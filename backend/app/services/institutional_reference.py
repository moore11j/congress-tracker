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
    if (not symbol or not re.fullmatch(r'[A-Z]{1,6}(?:-[A-Z])?', symbol)
            or row.get('market') != 'stocks' or row.get('locale') != 'us'
            or str(row.get('currency_name', '')).lower() != 'usd' or row.get('active') is not True
            or row.get('type') not in {'CS', 'ETF', 'ADRC'}
            or not re.fullmatch(r'BBG[A-Z0-9]{9}', str(row.get('share_class_figi') or ''))):
        return {'status': 'held', 'reason': 'unsupported_reference_security'}
    updated = _stamp(row['last_updated_utc']) if row.get('last_updated_utc') else None
    if updated and updated > observed:
        raise ValueError('Provider update is after the captured observation')
    return {'status': 'verified', 'cusip': cusip, 'symbol': symbol,
            'report_period': query['date'], 'provider': SOURCE,
            'source_url': expected_url, 'source_sha256': document['response_sha256'],
            'observed_at': observed.isoformat(), 'provider_updated_at': updated.isoformat() if updated else None,
            'share_class_figi': row['share_class_figi'], 'security_type': row['type'],
            'source_availability_basis': 'reference_observed_at'}


def resolve_reference_candidate(evidence, candidates):
    """Independent reference may disambiguate existing candidates, not overwrite a conflict."""
    if evidence.get('status') != 'verified':
        return None
    symbol = evidence['symbol']
    normalized = {_symbol_key(value) for value in candidates if _symbol_key(value)}
    return symbol if not normalized or symbol in normalized else None


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
    if row.status != 'parsed':
        raise ValueError('Reference staging is not parsed')
    revision = db.scalar(select(DirectFeedRevision).where(
        DirectFeedRevision.document_id == row.id, DirectFeedRevision.content_hash == row.content_hash))
    if (revision is None or not revision.source_bytes or len(revision.source_bytes) > 100_000
            or hashlib.sha256(revision.source_bytes).hexdigest() != row.content_hash):
        raise ValueError('Reference staging checksum mismatch')
    document = json.loads(revision.source_bytes)
    if row.source_url != document.get('source_url'):
        raise ValueError('Reference staging URL mismatch')
    reference_identity(document, cusip=cusip, report_period=report_period,
                       observed_by=document['observed_at'])
    return document


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
