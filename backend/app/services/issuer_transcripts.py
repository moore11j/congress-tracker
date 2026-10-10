"""Prepared issuer transcripts with fiscal identity and cross-provider deduplication."""
from datetime import date, datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
from urllib.parse import urlsplit

from sqlalchemy import or_, select

from app.clients.direct_sources import parse_issuer_material
from app.models import ResearchSourceDocument, Security
from app.services.direct_feed_store import DirectFeedDocument, DirectFeedRevision, dumps
from app.services.research_evidence import source_content_hash, upsert_source_document

REGISTRY = Path(__file__).resolve().parents[2] / 'config/direct_issuer_sources.json'


def selected_transcript_provider():
    value = os.getenv('TRANSCRIPT_PROVIDER', 'fmp').strip().lower()
    if value not in {'fmp', 'issuer'}:
        raise ValueError('Unsupported transcript provider')
    return value


def _reviewed_latest(symbol):
    companies = json.loads(REGISTRY.read_bytes())
    matches = [row for row in companies if row['symbol'] == symbol]
    if len(matches) != 1:
        return None
    company = matches[0]
    docs = [d for d in company.get('documents', []) if d['document_type'] == 'earnings_transcript']
    if not docs:
        return None
    latest = max((d['fiscal_year'], d['fiscal_quarter']) for d in docs)
    docs = [d for d in docs if (d['fiscal_year'], d['fiscal_quarter']) == latest]
    if len(docs) != 1:
        raise ValueError('Ambiguous reviewed issuer period')
    item = docs[0]
    url = urlsplit(item['url'])
    host = urlsplit(company['investor_website']).hostname
    if (url.scheme != 'https' or url.hostname != host or url.username or url.password
            or url.port not in (None, 443) or not 1 <= item['fiscal_quarter'] <= 4):
        raise ValueError('Unverified issuer identity')
    return {**item, 'symbol': symbol, 'company_website': company['company_website'],
            'investor_website': company['investor_website'],
            'key': f"{symbol}:earnings_transcript:{latest[0]}:Q{latest[1]}"}


def prepared_transcript(db, symbol, *, observed_by=None):
    """Read only; never fetch, substitute another quarter or fall back to FMP."""
    metadata = _reviewed_latest(symbol)
    unavailable = lambda reason: {'status': 'unavailable', 'reason': reason, 'provider': 'issuer',
                                  'coverage': 'reviewed_companies_and_periods_only'}
    if metadata is None:
        return unavailable('issuer_not_configured')
    row = db.scalar(select(DirectFeedDocument).where(DirectFeedDocument.feed == 'issuer_earnings',
                                                    DirectFeedDocument.source_key == metadata['key']))
    if row is None or row.status != 'parsed':
        return unavailable('reviewed_period_not_prepared')
    if row.source_url != metadata['url'] or json.loads(row.metadata_json) != metadata:
        return unavailable('reviewed_source_changed')
    revision = db.scalar(select(DirectFeedRevision).where(DirectFeedRevision.document_id == row.id,
                                                         DirectFeedRevision.content_hash == row.content_hash))
    raw = revision.source_bytes if revision is not None else None
    if not raw or len(raw) > 2_000_000 or hashlib.sha256(raw).hexdigest() != row.content_hash:
        raise ValueError('Issuer source checksum or size mismatch')
    observed = revision.fetched_at
    if observed.tzinfo is None:
        observed = observed.replace(tzinfo=timezone.utc)
    now = observed_by or datetime.now(timezone.utc)
    if now.tzinfo is None or observed > now:
        return unavailable('source_not_yet_observed')
    text, parsed = parse_issuer_material(raw, metadata)
    if (revision.source_text != text or json.loads(revision.parsed_json) != parsed
            or json.loads(row.parsed_json) != parsed or parsed['canonical_key'] != metadata['key']):
        raise ValueError('Issuer parser or fiscal identity changed')
    for field in ('company_pattern', 'publication_pattern'):
        if not metadata.get(field) or not re.search(metadata[field], text, re.I):
            return unavailable('reviewed_source_identity_incomplete')
    publication_date = date.fromisoformat(metadata['published_at'])
    if publication_date > observed.date():
        return unavailable('future_publication_date')
    return {'status': 'prepared', 'provider': 'issuer', 'symbol': symbol,
            'coverage': 'reviewed_companies_and_periods_only', 'year': metadata['fiscal_year'],
            'quarter': metadata['fiscal_quarter'], 'content': text, 'text_sha256': source_content_hash(text),
            'source_url': metadata['url'], 'source_sha256': row.content_hash, 'source_document_id': row.id,
            'source_revision_id': revision.id, 'observed_at': observed.isoformat(),
            'published_date': publication_date.isoformat(), 'publication_precision': 'date',
            'has_qa': parsed['has_qa'], 'canonical_key': metadata['key'],
            'transcript_publisher': metadata.get('transcript_publisher'),
            'source_format': metadata.get('source_format', 'html')}


def prepare_research_document(db, *, security_id, publish_since):
    """Caller owns commit and explicit date boundary; no models, matching or email."""
    if not isinstance(publish_since, date) or isinstance(publish_since, datetime):
        raise ValueError('Explicit issuer publication date boundary required')
    security = db.scalar(select(Security).where(Security.id == security_id).with_for_update())
    if security is None or not security.symbol:
        raise ValueError('Issuer security missing')
    source = prepared_transcript(db, security.symbol)
    if source['status'] != 'prepared':
        return source
    if date.fromisoformat(source['published_date']) < publish_since:
        return {'status': 'held', 'reason': 'before_issuer_publication_boundary'}
    period = f"Q{source['quarter']}-{source['year']}"
    legacy_key = f"earnings_transcript:{security.id}:{source['year']}:Q{source['quarter']}"
    # A different transcript rendering for one quarter is a reconciliation task,
    # not a second research document under the new provider's namespace.
    candidates = list(db.scalars(select(ResearchSourceDocument).where(
        ResearchSourceDocument.security_id == security.id,
        ResearchSourceDocument.document_type == 'earnings_transcript',
        or_(ResearchSourceDocument.filing_type == period,
            ResearchSourceDocument.external_id.in_([legacy_key, source['canonical_key']])))))
    if len(candidates) > 1 or (candidates and (candidates[0].content_hash != source['text_sha256']
            or candidates[0].filing_type not in (None, period))):
        return {'status': 'held', 'reason': 'existing_period_requires_reconciliation'}
    if candidates:
        document, created = candidates[0], False
    else:
        document, created = upsert_source_document(db, security_id=security.id,
            document_type='earnings_transcript', source_provider='issuer', external_id=source['canonical_key'],
            content=source['content'], title=f"{security.symbol} earnings call transcript Q{source['quarter']} {source['year']}",
            source_url=source['source_url'], published_at=None, filing_type=period)
    staged = db.get(DirectFeedDocument, source['source_document_id'])
    receipt = json.loads(staged.reconciliation_json or '{}')
    proof = {key: source[key] for key in ('source_sha256', 'text_sha256', 'source_revision_id',
              'source_url', 'observed_at', 'published_date', 'publication_precision', 'canonical_key')}
    proof.update(document_id=document.id, publish_since=publish_since.isoformat())
    if source.get('transcript_publisher'):
        proof['transcript_publisher'] = source['transcript_publisher']
    previous = receipt.get('issuer_transcript_research')
    if previous is not None:
        semantic_keys = ('text_sha256', 'source_url', 'published_date', 'publication_precision',
                         'canonical_key', 'document_id', 'publish_since')
        if (any(previous.get(key) != proof[key] for key in semantic_keys)
                or previous.get('transcript_publisher') != proof.get('transcript_publisher')):
            raise ValueError('Issuer publication identity or boundary changed')
        # Publisher HTML may change while the transcript remains identical.
        # Retain the first source receipt instead of refreshing availability.
        original = db.get(DirectFeedRevision, previous.get('source_revision_id'))
        if (original is None or original.document_id != staged.id or not original.source_bytes
                or len(original.source_bytes) > 2_000_000
                or hashlib.sha256(original.source_bytes).hexdigest() != previous.get('source_sha256')
                or source_content_hash(original.source_text) != previous['text_sha256']):
            raise ValueError('Original issuer publication evidence changed')
        original_text, _ = parse_issuer_material(original.source_bytes, json.loads(staged.metadata_json))
        if original_text != original.source_text or source_content_hash(original_text) != previous['text_sha256']:
            raise ValueError('Original issuer source text changed')
        first_observed = original.fetched_at
        if first_observed.tzinfo is None:
            first_observed = first_observed.replace(tzinfo=timezone.utc)
        if first_observed.isoformat() != previous.get('observed_at'):
            raise ValueError('Original issuer observation changed')
    if previous is None:
        receipt['issuer_transcript_research'] = proof
        staged.reconciliation_json = dumps(receipt)
    return {'status': 'prepared', 'document': document, 'created': created,
            'source_text': source['content'], 'source': source, 'publication_evidence': previous or proof}
