"""Prepared SEC identities without overwriting legacy identity caches."""
from datetime import datetime, timezone
import json
import re
from app.models import InsightsSnapshot
from app.services import sec_directory
from app.clients.direct_sources import DirectSourceError
from app.utils.symbols import normalize_symbol


def _prepared(db, key, url):
    row = db.get(InsightsSnapshot, key)
    if row is None or row.source != sec_directory.SOURCE:
        return None
    stamp = row.fetched_at.replace(tzinfo=row.fetched_at.tzinfo or timezone.utc)
    age = datetime.now(timezone.utc) - stamp
    if not 0 <= age.total_seconds() < sec_directory.TTL.total_seconds():
        return None
    try:
        payload = json.loads(row.payload_json)
        if (payload.get('source') != sec_directory.SOURCE or payload.get('source_url') != url
                or not re.fullmatch(r'[0-9a-f]{64}', str(payload.get('sha256', '')))
                or not isinstance(payload.get('data'), dict)):
            return None
        return payload['data'], stamp
    except (TypeError, ValueError):
        return None


def prepared_directory(db):
    saved = _prepared(db, 'sec-directory:exchange:v1', sec_directory.URL)
    if saved is None:
        return {}, None
    data, stamp = saved
    try:
        # Revalidate the projected identity schema and duplicate/share-class rules.
        parsed = sec_directory.parse_directory({'fields': ['cik','name','ticker','exchange'],
            'data': [[row['cik'],row['name'],symbol,row['exchange']] for symbol,row in data.items()]})
        if parsed != data:
            return {}, None
        return parsed, stamp
    except (KeyError, TypeError, DirectSourceError):
        return {}, None


def _company(db, cik, symbol=None):
    saved = _prepared(db, f'sec-company:{cik}:v1', f'https://data.sec.gov/submissions/CIK{cik}.json')
    if saved is None:
        return None
    data, stamp = saved
    try:
        return sec_directory.parse_company(data, cik=cik, symbol=symbol), stamp
    except (KeyError, TypeError, DirectSourceError):
        return None


def ticker_metadata(db, symbols, *, enqueue=False):
    directory, directory_at = prepared_directory(db)
    result = {}
    for requested in sorted({normalize_symbol(s) for s in symbols if normalize_symbol(s)}):
        symbol = sec_directory._symbol_key(requested)
        identity = directory.get(symbol)
        company = _company(db, identity['cik'], symbol) if identity else None
        if enqueue and (identity is None or company is None):
            from app.services.data_enrichment_queue import enqueue_data_enrichment_job
            enqueue_data_enrichment_job(job_type='ticker_meta', symbol=requested,
                source='page_load', reason='sec_identity_cache_miss', priority=60)
        if identity is None:
            continue
        data, observed = company if company else ({}, directory_at)
        result[requested] = {'company_name': data.get('name') or identity['name'],
            'exchange': data.get('exchange') or identity['exchange'], 'sector': None,
            'industry': data.get('industry'), 'country': None,
            'source': sec_directory.SOURCE, 'source_as_of': min(directory_at, observed).isoformat(),
            'classification': 'SEC SIC' if data.get('industry') else None}
    return result


def cik_metadata(db, ciks, *, enqueue=False):
    result = {}
    for raw in ciks:
        if not str(raw).isdigit() or not 0 < int(raw) < 10**10:
            continue
        cik = str(raw).zfill(10)
        company = _company(db, cik)
        if company:
            result[cik] = company[0]['name']
        elif enqueue:
            from app.services.data_enrichment_queue import enqueue_data_enrichment_job
            enqueue_data_enrichment_job(job_type='cik_meta', window_key=cik,
                source='page_load', reason='sec_identity_cache_miss', priority=60)
    return result
