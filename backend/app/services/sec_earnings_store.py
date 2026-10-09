"""Persistent earnings evidence and conservative existing-document reconciliation.

No research document, extracted event, alert or delivery is written here.
"""
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
from urllib.parse import urlsplit, urlunsplit

from sqlalchemy import or_, select

from app.models import ResearchSourceDocument, Security
from app.services.direct_feed_store import DirectFeedDocument, DirectFeedRevision, discover, dumps, record_document
from app.services.sec_earnings_materials import discover_earnings_filings, prepare_earnings_release, verify_filing_availability
from app.utils.symbols import canonical_symbol


def _url(value):
    try:
        parts = urlsplit(value or '')
        if parts.scheme not in {'http', 'https'} or not parts.hostname or parts.username or parts.password:
            return None
        # Fragments cannot identify a different source document. Retain query
        # parameters; stripping arbitrary provider parameters could merge pages.
        return urlunsplit((parts.scheme.lower(), parts.netloc.lower(), parts.path, parts.query, ''))
    except ValueError:
        return None


def reconcile_research_document(db, receipt, *, security_id):
    security = db.get(Security, security_id)
    if security is None or canonical_symbol(security.symbol) != receipt['symbol']:
        raise ValueError('Earnings evidence security mismatch')
    if receipt['status'] != 'prepared':
        return {'status': 'held', 'reason': receipt['reason'], 'existing_document_id': None}
    release = receipt['release']
    digest = release['text_sha256']
    if hashlib.sha256(release['text'].encode()).hexdigest() != digest:
        raise ValueError('Earnings normalized text hash mismatch')
    start = datetime.combine(date.fromisoformat(receipt['filing_date']) - timedelta(days=3), datetime.min.time(), tzinfo=timezone.utc)
    end = start + timedelta(days=7)
    # Exact identities can match outside the date window; near-date press
    # material without equality is held, never treated as proof of a new item.
    candidates = list(db.scalars(select(ResearchSourceDocument).where(
        ResearchSourceDocument.security_id == security_id,
        or_(ResearchSourceDocument.content_hash == digest,
            ResearchSourceDocument.source_url == release['url'],
            ResearchSourceDocument.external_id == receipt['canonical_key'],
            ResearchSourceDocument.source_url.like(receipt['source_url'].rsplit('/', 1)[0] + '/%'),
            (ResearchSourceDocument.document_type == 'press_release') &
            (ResearchSourceDocument.published_at >= start) & (ResearchSourceDocument.published_at < end),
            (ResearchSourceDocument.document_type == 'press_release') & ResearchSourceDocument.published_at.is_(None))
    ).order_by(ResearchSourceDocument.id).limit(501)))
    if len(candidates) > 500:
        return {'status': 'held', 'reason': 'existing_document_scan_limit', 'existing_document_id': None}
    identical, related = [], []
    for row in candidates:
        exact_text = row.content_hash == digest
        same_url = _url(row.source_url) == _url(release['url'])
        if exact_text:
            identical.append(row)
        elif same_url or row.external_id == receipt['canonical_key'] or row.document_type == 'press_release':
            related.append(row.id)
        elif row.source_url and row.source_url.startswith(receipt['source_url'].rsplit('/', 1)[0] + '/'):
            related.append(row.id)
    if len(identical) == 1 and not related:
        row = identical[0]
        state = {key: getattr(row, key) for key in ('id', 'security_id', 'source_provider', 'external_id',
                 'content_hash', 'source_url', 'published_at', 'document_type', 'processing_status', 'processing_version')}
        return {'status': 'matched', 'reason': None, 'existing_document_id': row.id,
                'existing_state_sha256': hashlib.sha256(dumps(state).encode()).hexdigest()}
    if identical or related:
        return {'status': 'held', 'reason': 'ambiguous_existing_material', 'existing_document_id': None,
                'candidate_ids': sorted(set(related + [row.id for row in identical]))}
    return {'status': 'new_candidate', 'reason': None, 'existing_document_id': None}


def _stage(db, feed, metadata, raw, parsed, *, source_text='', reasons=()):
    doc = discover(db, feed, metadata)
    record_document(db, doc, raw, source_text, parsed, reasons=reasons)
    return doc


def stage_earnings_material(db, *, company_raw, submission_raw, index_raw, symbol, cik, accession):
    """Stage a verified three-source bundle in the caller's transaction.

    This function never commits. Source dates remain separate from collection
    time. Every later use reparses exact saved bytes, not mutable parsed JSON.
    """
    discoveries = discover_earnings_filings(company_raw, symbol=symbol, cik=cik, limit=100)
    matches = [row for row in discoveries if row['accession_number'] == accession]
    if len(matches) != 1:
        raise ValueError('Earnings filing absent or ambiguous in company history')
    filing = matches[0]
    prepared = verify_filing_availability(prepare_earnings_release(submission_raw, filing=filing), index_raw)
    company_meta = {'key': filing['cik'], 'url': f'https://data.sec.gov/submissions/CIK{filing["cik"]}.json'}
    company = _stage(db, 'sec_earnings_company', company_meta, company_raw,
                     {'cik': filing['cik']}, source_text=company_raw.decode())
    index_meta = {'key': accession, 'url': prepared['index_url'], 'cik': filing['cik']}
    index = _stage(db, 'sec_earnings_index', index_meta, index_raw,
                   {'cik': filing['cik'], 'accession_number': accession})
    metadata = {'key': accession, 'url': filing['submission_url'], 'filing': filing,
                'company_document_id': company.id, 'company_sha256': hashlib.sha256(company_raw).hexdigest(),
                'index_document_id': index.id, 'index_sha256': hashlib.sha256(index_raw).hexdigest()}
    reasons = [prepared['reason']] if prepared['status'] != 'prepared' else []
    if prepared['availability_status'] != 'verified_sec_acceptance':
        reasons.append('SEC acceptance source clocks disagree')
    document = _stage(db, 'sec_earnings_release', metadata, submission_raw, prepared,
                      source_text=prepared.get('release', {}).get('text', ''), reasons=reasons)
    return document


def reconcile_staged_earnings(db, document_id, *, security_id):
    document = db.get(DirectFeedDocument, document_id)
    if document is None or document.feed != 'sec_earnings_release':
        raise ValueError('No staged SEC earnings release')
    metadata = json.loads(document.metadata_json)
    if metadata['key'] != document.source_key or metadata['url'] != document.source_url:
        raise ValueError('Staged earnings identity changed')

    def saved(doc_id, digest, feed):
        parent = db.get(DirectFeedDocument, doc_id)
        row = db.scalar(select(DirectFeedRevision).where(DirectFeedRevision.document_id == doc_id,
                                                       DirectFeedRevision.content_hash == digest))
        if parent is None or parent.feed != feed or row is None or row.source_bytes is None or hashlib.sha256(row.source_bytes).hexdigest() != digest:
            raise ValueError('Staged earnings source hash missing or changed')
        return row.source_bytes

    company_raw = saved(metadata['company_document_id'], metadata['company_sha256'], 'sec_earnings_company')
    index_raw = saved(metadata['index_document_id'], metadata['index_sha256'], 'sec_earnings_index')
    raw = saved(document.id, document.content_hash, 'sec_earnings_release')
    filing = metadata['filing']
    discovered = discover_earnings_filings(company_raw, symbol=filing['symbol'], cik=filing['cik'], limit=100)
    if filing not in discovered or filing['accession_number'] != document.source_key:
        raise ValueError('Staged filing no longer matches source discovery')
    parsed = verify_filing_availability(prepare_earnings_release(raw, filing=filing), index_raw)
    if document.parsed_json != dumps(parsed):
        raise ValueError('Staged parsed evidence changed; reparse required')
    result = reconcile_research_document(db, parsed, security_id=security_id)
    if parsed['status'] == 'prepared':
        digest = parsed['release']['text_sha256']
        others = list(db.scalars(select(DirectFeedDocument).where(
            DirectFeedDocument.feed == 'sec_earnings_release', DirectFeedDocument.id != document.id,
            DirectFeedDocument.parsed_json.contains('"text_sha256": "' + digest + '"')
        ).limit(501)))
        duplicates = [row.id for row in others if json.loads(row.parsed_json).get('cik') == parsed['cik']]
        if len(others) > 500 or duplicates:
            result = {'status': 'held', 'reason': 'duplicate_staged_content',
                      'existing_document_id': None, 'candidate_document_ids': sorted(duplicates)}
    if parsed['availability_status'] != 'verified_sec_acceptance':
        result = {**result, 'status': 'held', 'reason': 'source_clock_conflict',
                  'document_reconciliation': result}
    result.update({'document_id': document.id, 'source_sha256': document.content_hash,
                   'metadata_sha256': hashlib.sha256(document.metadata_json.encode()).hexdigest(),
                   'acceptance_status': parsed['availability_status'],
                   'publication_eligible': False})
    document.reconciliation_json = dumps(result)
    db.flush()
    return result
