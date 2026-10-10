"""Opt-in, cache-backed SEC earnings-release links for the press panel.

This feed does not create research events or label SEC acceptance as press time.
"""
from datetime import datetime, timedelta, timezone
import json
import logging
import os

from sqlalchemy import select, text

from app.db import SessionLocal
from app.models import Security, TickerContentCache
from app.services.direct_feed_store import dumps
from app.services.sec_earnings_materials import discover_earnings_filings
from app.services.sec_earnings_store import reconcile_staged_earnings, stage_earnings_material
from app.utils.symbols import canonical_symbol

logger = logging.getLogger(__name__)
PROVIDER = 'sec_edgar_earnings'
CONTENT_TYPE = 'sec_earnings_releases'
CACHE_VERSION = 'sec_earnings_links_v2'
TTL = timedelta(hours=6)
MAX_FILINGS = 5
MESSAGE = 'Selected earnings releases filed with SEC EDGAR. Other company releases and transcripts are not covered. Dates shown are filing dates.'


def selected_press_provider():
    return 'sec_edgar' if os.getenv('PRESS_RELEASE_PROVIDER', 'fmp').strip().lower() == 'sec_edgar' else 'fmp'


def _cache_row(db, symbol):
    return db.scalar(select(TickerContentCache).where(TickerContentCache.content_type == CONTENT_TYPE,
        TickerContentCache.symbol == symbol, TickerContentCache.window_key == 'latest',
        TickerContentCache.source == PROVIDER))


def _lock_issuer(db, security_id):
    if db.get_bind().dialect.name == 'postgresql':
        db.execute(text("SET LOCAL lock_timeout = '2s'"))
        db.execute(text("SET LOCAL statement_timeout = '20s'"))
        # No provider/model call occurs in these short staging transactions.
        # A killed cron process must not leave this row lock indefinitely.
        db.execute(text("SET LOCAL idle_in_transaction_session_timeout = '90s'"))
    return db.scalar(select(Security).where(Security.id == security_id).with_for_update())


def _save_payload(db, symbol, payload, now):
    row = _cache_row(db, symbol)
    if row is None:
        row = TickerContentCache(content_type=CONTENT_TYPE, symbol=symbol, window_key='latest',
            cache_key=f'{CONTENT_TYPE}:{symbol}:latest', source=PROVIDER)
        db.add(row)
    row.status, row.item_count = payload['status'], len(payload['items'])
    row.payload_json, row.fetched_at = dumps(payload), now


def _cached(symbol):
    with SessionLocal() as db:
        row = _cache_row(db, symbol)
        if row is None:
            return None
        stamp = row.fetched_at
        stamp = stamp.replace(tzinfo=timezone.utc) if stamp.tzinfo is None else stamp
        age = datetime.now(timezone.utc) - stamp
        if age < timedelta(0) or age > TTL:
            return None
        payload = json.loads(row.payload_json)
        return payload if payload.get('provider') == PROVIDER and payload.get('cache_version') == CACHE_VERSION else None


def _page(payload, page, limit):
    items = payload.get('items', [])
    start = page * limit
    return {**payload, 'items': items[start:start+limit], 'item_count': len(items[start:start+limit]),
            'page': page, 'limit': limit, 'has_next': start + limit < len(items)}


def refresh_sec_releases(symbol, *, client=None, directory=None, filing_limit=MAX_FILINGS, preparation_only=False):
    """Bounded existing-worker entry. End database transactions before HTTP."""
    from app.clients.direct_sources import DirectSourceClient, DirectSourceError
    from app.services.sec_directory import directory as company_directory

    def authorized():
        return (os.getenv('SEC_EARNINGS_WARMING_ENABLED', '0') == '1'
                if preparation_only else selected_press_provider() == 'sec_edgar')

    if not authorized():
        raise ValueError('Direct SEC press provider is not selected')
    if not 1 <= filing_limit <= MAX_FILINGS:
        raise ValueError('SEC earnings refresh limit outside 1..5')
    symbol = canonical_symbol(symbol)
    now = datetime.now(timezone.utc)
    client = client or DirectSourceClient()
    issuer = (directory if directory is not None else company_directory()).get(symbol)
    with SessionLocal() as db:
        security_id = db.scalar(select(Security.id).where(Security.symbol == symbol))
    if security_id is None:
        raise ValueError('Ticker identity has not been prepared')
    if issuer is None:
        payload = {'items': [], 'status': 'unavailable', 'provider': PROVIDER,
            'cache_version': CACHE_VERSION, 'item_count': 0, 'updated_at': now.isoformat(),
            'reason': 'symbol_absent_from_sec_directory',
            'message': MESSAGE + ' No SEC ticker-directory match is available for this security.',
            'coverage': {'kind': 'selected_sec_earnings_releases', 'complete': False,
                'filings_checked': 0, 'source_failures': 0,
                'reason': 'symbol_absent_from_sec_directory'}}
        with SessionLocal() as db:
            _lock_issuer(db, security_id)
            if not authorized():
                raise ValueError('Direct SEC provider changed during refresh')
            _save_payload(db, symbol, payload, now)
            db.commit()
        return payload
    cik = str(issuer['cik']).zfill(10)
    company_raw = client.get(f'https://data.sec.gov/submissions/CIK{cik}.json')
    all_filings = discover_earnings_filings(company_raw, symbol=symbol, cik=cik, limit=100)
    recent = [row for row in all_filings if now.date()-timedelta(days=365) <= datetime.fromisoformat(row['filing_date']).date() <= now.date()]
    filings = recent[:filing_limit]
    document_ids = []
    failures = 0
    for filing in filings:
        try:
            raw = client.get(filing['submission_url'])
            index_raw = client.get(filing['submission_url'][:-4] + '-index.html')
            with SessionLocal() as db:
                # Every collector for this feed uses the issuer lock. No lock
                # or open transaction is held while fetching provider bytes.
                _lock_issuer(db, security_id)
                doc = stage_earnings_material(db, company_raw=company_raw, submission_raw=raw,
                    index_raw=index_raw, symbol=symbol, cik=cik, accession=filing['accession_number'])
                document_ids.append(doc.id)
                db.commit()
        except Exception as exc:
            failures += 1
            logger.warning('sec_earnings_material_held symbol=%s error_type=%s', symbol, type(exc).__name__)
            if isinstance(exc, DirectSourceError):
                raise  # Let the scheduled batch stop on a source refusal.
            # Stop this issuer on a fetch/parse failure. No automatic burst of
            # requests after a refusal; remaining coverage is explicitly absent.
            break
    with SessionLocal() as db:
        _lock_issuer(db, security_id)
        items, held = [], 0
        for document_id in document_ids:
            result = reconcile_staged_earnings(db, document_id, security_id=security_id)
            if result['status'] not in {'matched', 'new_candidate'}:
                held += 1
                continue
            from app.services.direct_feed_store import DirectFeedDocument
            doc = db.get(DirectFeedDocument, document_id)
            parsed = json.loads(doc.parsed_json)
            release = parsed['release']
            items.append({'symbol': symbol, 'title': f'{symbol} earnings release filed {parsed["filing_date"]}',
                'site': 'SEC EDGAR', 'published_at': None, 'filing_date': parsed['filing_date'],
                'sec_accepted_at': parsed['sec_accepted_at'], 'url': release['url'],
                'acceptance_evidence': parsed.get('acceptance_evidence'),
                'summary': f'Filed {parsed["filing_date"]}. Read the issuer earnings release in its SEC filing.',
                'source': PROVIDER, 'canonical_key': parsed['canonical_key'],
                'accession_number': parsed['accession_number'], 'source_sha256': parsed['source_sha256']})
        payload = {'items': items, 'status': 'ok', 'provider': PROVIDER, 'cache_version': CACHE_VERSION, 'item_count': len(items),
            'updated_at': now.isoformat(), 'message': MESSAGE,
            'coverage': {'kind': 'selected_sec_earnings_releases', 'complete': False,
                'lookback_days': 365, 'filing_limit': filing_limit, 'filings_discovered': len(recent),
                'filings_checked': len(document_ids), 'held': held, 'source_failures': failures,
                'truncated': len(recent) > filing_limit or len(all_filings) == 100}}
        if not authorized():
            raise ValueError('Direct SEC provider changed during refresh')
        # A failed refresh must not replace previously prepared source links
        # with a misleading empty result. The caller reports unavailable.
        if failures:
            raise RuntimeError('SEC earnings collection incomplete')
        _save_payload(db, symbol, payload, now)
        db.commit()
    return payload


def prepare_sec_releases(symbol):
    """Prepare isolated source evidence without selecting any public writer."""
    if os.getenv('SEC_EARNINGS_WARMING_ENABLED', '0') != '1':
        raise ValueError('SEC earnings preparation is disabled')
    symbol = canonical_symbol(symbol)
    cached = _cached(symbol)
    return cached if cached is not None else refresh_sec_releases(symbol, preparation_only=True)


def get_sec_releases(*, symbol, page=0, limit=20, force_refresh=False, prepared_only=False):
    from app.services import fmp_news as news
    symbol = canonical_symbol(symbol)
    page, limit = max(0, int(page)), max(1, min(50, int(limit)))
    try:
        cached = _cached(symbol)
        if cached is not None and not force_refresh:
            return _page(cached, page, limit)
        if prepared_only:
            return {'items': [], 'status': 'unavailable', 'provider': PROVIDER,
                    'page': page, 'limit': limit, 'has_next': False, 'message': MESSAGE}
        if news._is_public_request_context():
            queued = news._enqueue_news_refresh(job_type='press_releases', symbol=symbol,
                reason='cache_miss', payload={'page': 0, 'limit': 20})
            return {'items': [], 'status': 'warming' if queued else 'unavailable',
                'provider': PROVIDER, 'page': page, 'limit': limit, 'has_next': False,
                'message': MESSAGE + ' Verified releases are being prepared.'}
        return _page(refresh_sec_releases(symbol), page, limit)
    except Exception as exc:
        logger.warning('sec_earnings_unavailable symbol=%s error_type=%s', symbol, type(exc).__name__)
        return {**news._unavailable_payload(page=page, limit=limit,
            message=MESSAGE + ' Source refresh is unavailable.', reason='sec_source_unavailable'),
            'provider': PROVIDER}
