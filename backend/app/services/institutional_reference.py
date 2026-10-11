"""Bounded historical identifier evidence from the existing free reference API.

This adapter does not select a feed, publish holdings, request prices, or claim
that a newly retrieved reference was known when an older filing was released.
"""
from datetime import date, datetime, timezone
import hashlib
import json
import re
from urllib.parse import urlencode

from app.services.sec_directory import _symbol_key

SOURCE = 'massive_reference'
PATH = '/v3/reference/tickers'
FIELDS = ('ticker', 'name', 'cik', 'market', 'locale', 'currency_name', 'type',
          'primary_exchange', 'share_class_figi', 'composite_figi', 'last_updated_utc', 'active')


def _stamp(value):
    result = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    if result.tzinfo is None:
        raise ValueError('Reference observation must include a timezone')
    return result.astimezone(timezone.utc)


def reference_query(cusip, report_period):
    if not isinstance(cusip, str) or not re.fullmatch(r'[A-Z0-9]{9}', cusip):
        raise ValueError('Invalid CUSIP identity')
    period = date.fromisoformat(str(report_period))
    if (period.month, period.day) not in {(3, 31), (6, 30), (9, 30), (12, 31)}:
        raise ValueError('Reference date must match the reporting quarter end')
    return {'cusip': cusip, 'market': 'stocks', 'date': period.isoformat(), 'active': 'true', 'limit': 2}


def _digest(payload):
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def capture_reference(client, *, cusip, report_period, observed_at=None):
    """Exactly one API call; the caller owns quota, retry and scheduling policy."""
    query = reference_query(cusip, report_period)
    requested_at = _stamp(observed_at) if observed_at else datetime.now(timezone.utc)
    if date.fromisoformat(query['date']) > requested_at.date():
        raise ValueError('Reference period is in the future')
    payload = client.get(PATH, query)
    observed = _stamp(observed_at) if observed_at else datetime.now(timezone.utc)
    if not isinstance(payload, dict):
        raise ValueError('Malformed reference response')
    rows = payload.get('results', [])
    if not isinstance(rows, list) or len(rows) > 2 or any(not isinstance(r, dict) for r in rows):
        raise ValueError('Reference result exceeds the requested bound')
    # Retain only identity facts. Do not persist pagination URLs that might
    # carry credentials. This is canonical parsed JSON, not original wire bytes.
    response = {'status': payload.get('status'), 'request_id': payload.get('request_id'),
                'has_more': bool(payload.get('next_url')),
                'results': [{key: row.get(key) for key in FIELDS} for row in rows]}
    return {'provider': SOURCE, 'query': query,
            'source_url': 'https://api.massive.com' + PATH + '?' + urlencode(query),
            'representation': 'canonical_parsed_json', 'response': response,
            'response_sha256': _digest(response), 'observed_at': observed.isoformat()}


def reference_identity(document, *, cusip, report_period, observed_by):
    """Return one verified candidate or an explicit coverage state, never guess."""
    query = reference_query(cusip, report_period)
    expected_url = 'https://api.massive.com' + PATH + '?' + urlencode(query)
    if (document.get('provider') != SOURCE or document.get('query') != query
            or document.get('source_url') != expected_url
            or document.get('representation') != 'canonical_parsed_json'):
        raise ValueError('Reference query identity mismatch')
    response = document.get('response')
    if not isinstance(response, dict) or _digest(response) != document.get('response_sha256'):
        raise ValueError('Reference evidence checksum mismatch')
    observed = _stamp(document['observed_at'])
    if observed > _stamp(observed_by):
        return {'status': 'unavailable', 'reason': 'not_yet_observed'}
    if observed.date() < date.fromisoformat(query['date']):
        raise ValueError('Reference observation precedes requested period')
    rows = response.get('results')
    if response.get('status') != 'OK' or not isinstance(rows, list):
        return {'status': 'unavailable', 'reason': 'provider_response_unavailable'}
    if response.get('has_more') or len(rows) > 1:
        return {'status': 'held', 'reason': 'ambiguous_reference_identity'}
    if not rows:
        return {'status': 'unavailable', 'reason': 'no_reference_match'}
    row = rows[0]
    if not isinstance(row, dict):
        raise ValueError('Malformed reference row')
    symbol = _symbol_key(row.get('ticker'))
    figi_valid = bool(re.fullmatch(r'BBG[A-Z0-9]{9}', str(row.get('share_class_figi') or '')))
    # The issuer explicitly identifies this US ordinary-share CUSIP. Massive
    # returns a null FIGI for it in both captured 2026 reporting quarters.
    # Accept only the independently reviewed issuer/class/exchange/date tuple;
    # do not generalize a missing FIGI to other securities or the former ADR.
    reviewed_listing = (cusip == 'F92124100' and query['date'] >= '2025-12-31'
        and symbol == 'TTE' and str(row.get('cik') or '').lstrip('0') == '879764'
        and row.get('primary_exchange') == 'XNYS' and row.get('type') == 'CS'
        and row.get('share_class_figi') is None)
    if (not symbol or not re.fullmatch(r'[A-Z]{1,6}(?:-[A-Z])?', symbol)
            or row.get('market') != 'stocks' or row.get('locale') != 'us'
            or str(row.get('currency_name', '')).lower() != 'usd' or row.get('active') is not True
            or row.get('type') not in {'CS', 'ETF', 'ADRC'}
            or not (figi_valid or reviewed_listing)):
        return {'status': 'held', 'reason': 'unsupported_reference_security'}
    updated = _stamp(row['last_updated_utc']) if row.get('last_updated_utc') else None
    if updated and updated > observed:
        raise ValueError('Provider update is after the captured observation')
    identity = {'status': 'verified', 'cusip': cusip, 'symbol': symbol,
            'report_period': query['date'], 'provider': SOURCE,
            'source_url': expected_url, 'source_sha256': document['response_sha256'],
            'observed_at': observed.isoformat(), 'provider_updated_at': updated.isoformat() if updated else None,
            'share_class_figi': row['share_class_figi'], 'security_type': row['type'],
            'source_availability_basis': 'reference_observed_at'}
    if reviewed_listing:
        identity.update(identity_basis='reviewed_cusip_issuer_us_listing', reviewed_sources=[
            'https://totalenergies.com/investors/shares-and-dividends/totalenergies-shares/totalenergies-shares-listed-on-the-nyse',
            'https://www.sec.gov/Archives/edgar/data/879764/000110465925119004/tm2526657d2_6k.htm'])
    return identity


def resolve_reference_candidate(evidence, candidates):
    """Resolve observed evidence, retaining unreviewed identity conflicts."""
    if evidence.get('status') != 'verified':
        return None
    symbol = evidence['symbol']
    normalized = {_symbol_key(value) for value in candidates if _symbol_key(value)}
    if not normalized or symbol in normalized:
        return symbol
    from app.services.institutional_sec_snapshot import reviewed_us_listing
    # Only a verified, date-query-bound common-share reference may supply a
    # missing US candidate for an exact reviewed foreign listing. Availability
    # remains the real observation timestamp retained in the publication proof.
    try:
        period = date.fromisoformat(evidence['report_period'])
    except (KeyError, TypeError, ValueError):
        return None
    if (evidence.get('security_type') == 'CS'
            and reviewed_us_listing(evidence.get('cusip'), normalized,
                                    period.year, (period.month - 1) // 3 + 1) == symbol):
        return symbol
    return None


FEED = 'massive_reference'


def load_reference(db, *, cusip, report_period):
    """Read staged evidence only; public/canonical readers never call the API."""
    from sqlalchemy import select
    from app.services.direct_feed_store import DirectFeedDocument, DirectFeedRevision
    query = reference_query(cusip, report_period)
    key = query['cusip'] + ':' + query['date']
    row = db.scalar(select(DirectFeedDocument).where(
        DirectFeedDocument.feed == FEED, DirectFeedDocument.source_key == key))
    if row is None:
        return None
    revision = db.scalar(select(DirectFeedRevision).where(
        DirectFeedRevision.document_id == row.id, DirectFeedRevision.content_hash == row.content_hash))
    return _verified_staged(row, revision, cusip=cusip, report_period=report_period)


def _verified_staged(row, revision, *, cusip, report_period):
    if row.status != 'parsed':
        raise ValueError('Reference staging is not parsed')
    if (revision is None or not revision.source_bytes or len(revision.source_bytes) > 100_000
            or hashlib.sha256(revision.source_bytes).hexdigest() != row.content_hash):
        raise ValueError('Reference staging checksum mismatch')
    document = json.loads(revision.source_bytes)
    if row.source_url != document.get('source_url'):
        raise ValueError('Reference staging URL mismatch')
    reference_identity(document, cusip=cusip, report_period=report_period,
                       observed_by=document['observed_at'])
    return document


MAX_REFERENCE_SCOPES = 100_000
REFERENCE_QUERY_SCOPES = 5000
MAX_REFERENCE_BYTES = 20_000_000


def load_reference_batch(db, scopes):
    """Bounded saved evidence only, with no per-position query or source HTTP."""
    from sqlalchemy import select
    from app.services.direct_feed_store import DirectFeedDocument, DirectFeedRevision
    keys = {}
    for cusip, period in scopes:
        query = reference_query(cusip, period)
        keys[query['cusip'] + ':' + query['date']] = (cusip, period)
        if len(keys) > MAX_REFERENCE_SCOPES:
            raise ValueError('Reference scope exceeds identity bounds')
    if not keys:
        return []
    documents = []
    total = 0
    ordered_keys = sorted(keys)
    for start in range(0, len(ordered_keys), REFERENCE_QUERY_SCOPES):
        query = select(DirectFeedDocument, DirectFeedRevision).outerjoin(DirectFeedRevision,
            (DirectFeedRevision.document_id == DirectFeedDocument.id) &
            (DirectFeedRevision.content_hash == DirectFeedDocument.content_hash)).where(
                DirectFeedDocument.feed == FEED,
                DirectFeedDocument.source_key.in_(ordered_keys[start:start + REFERENCE_QUERY_SCOPES]))
        for row, revision in db.execute(query):
            cusip, period = keys[row.source_key]
            total += len(revision.source_bytes or b'') if revision else 0
            if total > MAX_REFERENCE_BYTES:
                raise ValueError('Reference evidence exceeds 20 MB')
            documents.append(_verified_staged(row, revision, cusip=cusip, report_period=period))
    return sorted(documents, key=lambda d: (d['query']['date'], d['query']['cusip']))


def stage_reference(db, document):
    """Durable internal staging; unchanged facts retain their first observation."""
    from app.services.direct_feed_store import discover, record_document, dumps
    if db.new or db.dirty or db.deleted:
        raise ValueError('Reference staging session has unrelated pending changes')
    query = document.get('query', {})
    cusip, period = query.get('cusip'), query.get('date')
    identity = reference_identity(document, cusip=cusip, report_period=period,
                                  observed_by=document['observed_at'])
    saved = load_reference(db, cusip=cusip, report_period=period)
    if saved is not None:
        def facts(item):
            return {key: value for key, value in item['response'].items() if key != 'request_id'}
        if facts(saved) == facts(document):
            # Re-evaluate only the derived interpretation of the original
            # observation. Keep original bytes/hash/revisions and checked_at.
            identity = reference_identity(saved, cusip=cusip, report_period=period,
                                          observed_by=document['observed_at'])
            from sqlalchemy import select
            from app.services.direct_feed_store import DirectFeedDocument
            row = db.scalar(select(DirectFeedDocument).where(
                DirectFeedDocument.feed == FEED, DirectFeedDocument.source_key == cusip + ':' + period))
            parsed = dumps({'identity': identity})
            if row.parsed_json != parsed:
                row.parsed_json = parsed
            return {'status': 'reused', 'identity_status': identity['status'], 'identity_reason': identity.get('reason'), 'public_writes': 0}
        if _stamp(document['observed_at']) <= _stamp(saved['observed_at']):
            raise ValueError('Conflicting reference cannot rewind source observation')
    raw = dumps(document).encode()
    metadata = {'key': cusip + ':' + period, 'url': document['source_url'],
                'cusip': cusip, 'report_period': period, 'representation': 'canonical_parsed_json'}
    row = discover(db, FEED, metadata)
    record_document(db, row, raw, raw.decode(), {'identity': identity})
    return {'status': 'staged', 'identity_status': identity['status'], 'identity_reason': identity.get('reason'),
            'document_id': row.id, 'content_hash': row.content_hash, 'public_writes': 0}
