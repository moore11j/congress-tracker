"""Prepare free SEC ratios without selecting a public fundamentals provider.

No market-data transport, canonical cache overwrite, score calculation or email.
"""
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from sqlalchemy import text
from app.db import SessionLocal
from app.models import InsightsSnapshot
from app.clients.direct_sources import DirectSourceClient, DirectSourceError
from app.services import sec_directory, sec_fundamentals

VERSION = 'sec_ratio_preparation_v2'
TTL = timedelta(hours=24)
METRICS = ('gross_margin', 'operating_margin', 'net_margin', 'revenue_growth',
    'operating_margin_expansion', 'current_ratio', 'roe', 'free_cash_flow', 'fcf_margin', 'fcf_growth')


def _cached(symbol, now):
    with SessionLocal() as db:
        row = db.get(InsightsSnapshot, f'sec-current-fundamentals:{symbol}:v1')
        if row is None or row.source != 'sec_edgar':
            return None
        stamp = row.fetched_at.replace(tzinfo=row.fetched_at.tzinfo or timezone.utc)
        if not timedelta(0) <= now-stamp < TTL:
            return None
        try:
            payload = json.loads(row.payload_json)
        except (ValueError, TypeError):
            return None
        if (payload.get('version') == VERSION and payload.get('symbol') == symbol
                and payload.get('source') == 'sec_edgar'):
            return payload
    return None


def prepare(symbol):
    if os.getenv('SEC_FUNDAMENTALS_WARMING_ENABLED', '0') != '1':
        raise ValueError('SEC ratio preparation is disabled')
    symbol = sec_directory._symbol_key(symbol)
    if not symbol:
        raise ValueError('Invalid symbol')
    now = datetime.now(timezone.utc)
    cached = _cached(symbol, now)
    if cached is not None:
        return cached
    identity = sec_directory.directory().get(symbol)
    payload = {'version': VERSION, 'symbol': symbol, 'source': 'sec_edgar',
        'observed_at': now.isoformat(), 'status': 'unavailable', 'values': {},
        'coverage': {'complete': False, 'market_data_included': False, 'available_metrics': []}}
    if identity is None:
        payload['reason'] = 'symbol_absent_from_sec_directory'
    else:
        cik = identity['cik']; client = DirectSourceClient()
        company_url = f'https://data.sec.gov/submissions/CIK{cik}.json'
        company = client.get(company_url)
        sec_directory.parse_company(json.loads(company), cik=cik, symbol=symbol)
        facts_url = f'https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json'
        payload['source_evidence'] = {'cik': cik, 'company_url': company_url,
            'company_sha256': hashlib.sha256(company).hexdigest(), 'facts_url': facts_url}
        try:
            raw = client.get(facts_url)
        except DirectSourceError as exc:
            if str(exc) != f'Source HTTP 404: {facts_url}':
                raise
            payload['reason'] = 'sec_company_facts_not_found'
            payload['source_evidence']['facts_status'] = 404
        else:
            payload['source_evidence']['facts_sha256'] = hashlib.sha256(raw).hexdigest()
            try:
                values = sec_fundamentals.project_fundamentals(raw, company,
                    symbol=symbol, cik=cik, observed_at=now)
            except sec_fundamentals.SecFundamentalsCoverageError as exc:
                payload.update(reason='unsupported_current_financial_coverage', detail=str(exc))
            else:
                payload['values'] = {name: values[name] for name in METRICS if name in values}
                payload['period_date'] = str(values['period_date'])
                payload['calculation_evidence'] = json.loads(values['source_evidence_json'])
                payload['coverage']['available_metrics'] = sorted(payload['values'])
                payload['status'] = 'partial' if payload['values'] else 'unavailable'
    # Network operations finish before opening the write transaction.
    with SessionLocal() as db:
        if db.get_bind().dialect.name == 'postgresql':
            db.execute(text("SET LOCAL statement_timeout='20s'"))
            db.execute(text("SET LOCAL lock_timeout='2s'"))
            db.execute(text("SET LOCAL idle_in_transaction_session_timeout='90s'"))
        if os.getenv('SEC_FUNDAMENTALS_WARMING_ENABLED', '0') != '1':
            raise ValueError('SEC ratio preparation changed during refresh')
        key = f'sec-current-fundamentals:{symbol}:v1'
        row = db.get(InsightsSnapshot, key)
        if row is None:
            row = InsightsSnapshot(kind=key, source='sec_edgar', fetched_at=now, payload_json='{}')
            db.add(row)
        row.source, row.fetched_at, row.payload_json = 'sec_edgar', now, json.dumps(payload, sort_keys=True)
        db.commit()
    return payload
