"""SEC company identity replacement. No sector inference or market-data requests."""
from __future__ import annotations

import hashlib
import json
import os
import re
from datetime import datetime, timedelta, timezone

from app.clients.direct_sources import DirectSourceClient, DirectSourceError
from app.db import SessionLocal
from app.models import InsightsSnapshot
from app.utils.symbols import normalize_symbol

URL = 'https://www.sec.gov/files/company_tickers_exchange.json'
SOURCE = 'sec_edgar'
TTL = timedelta(days=1)


def _symbol_key(value):
    symbol = normalize_symbol(value)
    # Only explicit single-letter share-class separators are equivalent.
    return re.sub(r'^([A-Z]{1,6})[./-]([A-Z])$', r'\1-\2', symbol) if symbol else None


def selected() -> bool:
    return os.getenv('COMPANY_METADATA_PROVIDER', 'fmp').strip().lower() == SOURCE


def parse_directory(data: dict) -> dict:
    if not isinstance(data, dict) or data.get('fields') != ['cik', 'name', 'ticker', 'exchange']:
        raise DirectSourceError('invalid_sec_directory_schema')
    result = {}
    rows = data.get('data')
    if not isinstance(rows, list) or not rows or len(rows) > 100_000:
        raise DirectSourceError('invalid_sec_directory_rows')
    for row in rows:
        if not isinstance(row, list) or len(row) != 4:
            raise DirectSourceError('invalid_sec_directory_row')
        cik, name, raw_symbol, exchange = row
        symbol = _symbol_key(raw_symbol) if isinstance(raw_symbol, str) else None
        if not symbol or not str(cik).isdigit() or not 0 < int(cik) < 10**10 or not isinstance(name, str) or not name.strip():
            raise DirectSourceError('invalid_sec_identity')
        item = {'symbol': symbol, 'cik': str(cik).zfill(10), 'name': name.strip(),
                'exchange': exchange.strip() if isinstance(exchange, str) and exchange.strip() else None}
        if symbol in result and result[symbol] != item:
            raise DirectSourceError('ambiguous_sec_identity')
        result[symbol] = item
    return result


def parse_company(data: dict, *, cik: str, symbol: str | None = None) -> dict:
    if not isinstance(data, dict) or str(data.get('cik', '')).zfill(10) != cik:
        raise DirectSourceError('sec_company_cik_mismatch')
    name = data.get('name')
    if not isinstance(name, str) or not name.strip():
        raise DirectSourceError('sec_company_name_missing')
    tickers, exchanges = data.get('tickers') or [], data.get('exchanges') or []
    normalized = [_symbol_key(t) for t in tickers]
    symbol = _symbol_key(symbol)
    if symbol and (symbol not in normalized or normalized.count(symbol) != 1):
        raise DirectSourceError('sec_company_symbol_mismatch')
    exchange = exchanges[normalized.index(symbol)] if symbol and len(exchanges) == len(normalized) else None
    return {'cik': cik, 'name': name.strip(), 'exchange': exchange,
            'industry': data.get('sicDescription') or None, 'classification': 'SEC SIC',
            'sector': None, 'country': None,
            # SEC often leaves these empty. Do not synthesize domains from names.
            'website': data.get('website') or None, 'investor_website': data.get('investorWebsite') or None}


def _cached(key, url, project):
    """Worker-only persistent source cache, with one refresh owner on PostgreSQL."""
    now = datetime.now(timezone.utc)
    with SessionLocal() as db:
        if db.get_bind().dialect.name == 'postgresql':
            from sqlalchemy import text
            if not db.execute(text('SELECT pg_try_advisory_xact_lock(hashtext(:key))'), {'key': key}).scalar():
                raise DirectSourceError('sec_identity_refresh_in_progress')
        row = db.get(InsightsSnapshot, key)
        if row is not None and row.source == SOURCE:
            stamp = row.fetched_at.replace(tzinfo=timezone.utc) if row.fetched_at.tzinfo is None else row.fetched_at
            if timedelta(0) <= now - stamp < TTL:
                payload = json.loads(row.payload_json)
                if payload.get('source_url') == url and payload.get('source') == SOURCE:
                    return payload['data']
        raw = DirectSourceClient().get(url)
        result = project(json.loads(raw))
        payload = {'source': SOURCE, 'source_url': url, 'sha256': hashlib.sha256(raw).hexdigest(),
                   'observed_at': now.isoformat(), 'data': result}
        if row is None:
            row = InsightsSnapshot(kind=key, source=SOURCE, fetched_at=now, payload_json='{}')
            db.add(row)
        row.source, row.fetched_at, row.payload_json = SOURCE, now, json.dumps(payload, sort_keys=True)
        db.commit()
        return result


def directory() -> dict:
    return _cached('sec-directory:exchange:v1', URL, parse_directory)


def company(cik: str, *, symbol: str | None = None) -> dict:
    if not str(cik).isdigit() or not 0 < int(cik) < 10**10:
        raise DirectSourceError('invalid_cik')
    cik = str(cik).zfill(10)
    # Identity validation runs again on cache hits, rather than caching a result
    # from one share class as the exchange for every symbol belonging to an issuer.
    def project(data):
        parse_company(data, cik=cik)
        return {k: data.get(k) for k in ('cik', 'name', 'tickers', 'exchanges', 'sicDescription', 'website', 'investorWebsite')}
    data = _cached(f'sec-company:{cik}:v1', f'https://data.sec.gov/submissions/CIK{cik}.json', project)
    return parse_company(data, cik=cik, symbol=symbol)


def symbol_metadata(symbol: str):
    symbol = _symbol_key(symbol)
    item = directory().get(symbol)
    if item is None:
        raise DirectSourceError('symbol_absent_from_sec_directory')
    data = company(item['cik'], symbol=symbol)
    return data['name'], data['exchange'] or item['exchange'], None, data['industry'], None
