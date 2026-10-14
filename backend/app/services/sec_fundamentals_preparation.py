"""Prepare free SEC ratios without selecting a public fundamentals provider.

No market-data transport, canonical cache overwrite, score calculation or email.
"""
from app.services.issuer_financial_registry import PROFILES as ISSUER_PROFILES, key_for
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from sqlalchemy import text
from app.db import SessionLocal
from app.models import InsightsSnapshot
from app.clients.direct_sources import DirectSourceClient, DirectSourceError
from app.services import sec_directory, sec_fundamentals

VERSION = 'sec_ratio_preparation_v5'
TTL = timedelta(hours=24)
METRICS = ('gross_margin', 'operating_margin', 'net_margin', 'revenue_growth',
    'operating_margin_expansion', 'eps_growth', 'current_ratio', 'roe', 'free_cash_flow', 'fcf_margin', 'fcf_growth')


def version_for_symbol(symbol):
    from app.services.sec_currency_ratios import SYMBOL as currency_symbol, VERSION as currency_version
    if symbol == currency_symbol:
        return VERSION+'_'+currency_version
    from app.services.sec_parent_statement_income import SYMBOLS as parent_symbols, VERSION as parent_version
    if symbol in parent_symbols:
        return VERSION+'_'+parent_version
    from app.services.sec_statement_income import SYMBOLS, VERSION as statement_income_version
    if symbol in SYMBOLS:
        return VERSION+'_'+statement_income_version
    if symbol == 'ASML':
        from app.services.sec_asml_financials import VERSION as asml_version
        return VERSION+'_'+asml_version
    if symbol == 'TSM':
        from app.services.sec_tsm_financials import VERSION as tsm_version
        return VERSION+'_'+tsm_version
    if symbol == 'HCA':
        from app.services.sec_operating_components import VERSION as operating_version
        return VERSION+'_'+operating_version
    from app.services.sec_depreciation_components import SYMBOL as da_symbol, VERSION as da_version
    if symbol == da_symbol:
        return VERSION+'_'+da_version
    from app.services.sec_reporting_policy import PROFILES, VERSION as reporting_version
    if symbol in PROFILES:
        return VERSION+'_'+reporting_version
    if symbol in ISSUER_PROFILES:
        return VERSION+'_issuer_release_v1'
    if symbol in {'FITB', 'RF', 'EWBC'}:
        return VERSION+'_bank_contract_recovery_v1'
    if symbol == 'PROV':
        return VERSION+'_bank_revenue_v1'
    return VERSION+'_xom_continuity_v1' if symbol == 'XOM' else VERSION


def _issuer_dependency(db, symbol):
    if symbol not in ISSUER_PROFILES:
        return None
    row = db.get(InsightsSnapshot, key_for(symbol))
    return hashlib.sha256((row.source+'\0'+row.payload_json).encode()).hexdigest() if row else 'missing'


def _cached(symbol, now, max_cache_age=TTL):
    with SessionLocal() as db:
        row = db.get(InsightsSnapshot, f'sec-current-fundamentals:{symbol}:v1')
        if row is None or row.source != 'sec_edgar':
            return None
        stamp = row.fetched_at.replace(tzinfo=row.fetched_at.tzinfo or timezone.utc)
        if not timedelta(0) <= now-stamp < max_cache_age:
            return None
        try:
            payload = json.loads(row.payload_json)
            observed = datetime.fromisoformat(payload['observed_at'])
        except (ValueError, TypeError, KeyError):
            return None
        if symbol in ISSUER_PROFILES and payload.get('issuer_dependency') != _issuer_dependency(db, symbol):
            return None
        if (payload.get('version') == version_for_symbol(symbol) and payload.get('symbol') == symbol
                and payload.get('source') == 'sec_edgar' and observed == stamp):
            return payload
    return None


def prepare(symbol, *, max_cache_age=TTL):
    if os.getenv('SEC_FUNDAMENTALS_WARMING_ENABLED', '0') != '1':
        raise ValueError('SEC ratio preparation is disabled')
    symbol = sec_directory._symbol_key(symbol)
    if not symbol:
        raise ValueError('Invalid symbol')
    if not isinstance(max_cache_age, timedelta) or not timedelta(0) < max_cache_age <= TTL:
        raise ValueError('Invalid SEC preparation cache age')
    now = datetime.now(timezone.utc)
    cached = _cached(symbol, now, max_cache_age)
    if cached is not None:
        return cached
    dependency = None
    if symbol in ISSUER_PROFILES:
        with SessionLocal() as db:
            dependency = _issuer_dependency(db, symbol)
    identity = sec_directory.directory().get(symbol)
    payload = {'version': version_for_symbol(symbol), 'symbol': symbol, 'source': 'sec_edgar',
        'observed_at': now.isoformat(), 'status': 'unavailable', 'values': {},
        'coverage': {'complete': False, 'market_data_included': False, 'available_metrics': []}}
    if symbol in ISSUER_PROFILES:
        payload['issuer_dependency'] = dependency
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
            from app.services.sec_financial_continuity import fetch_history
            history = fetch_history(symbol,cik,client=client,observed_at=now)
            from app.services.sec_inline_financials import acquire
            filing = acquire(raw,company,symbol=symbol,cik=cik,observed_at=now,client=client)
            try:
                values = sec_fundamentals.project_fundamentals(raw, company,
                    symbol=symbol, cik=cik, observed_at=now, history_raw=history,filing_supplement=filing)
            except sec_fundamentals.SecFundamentalsCoverageError as exc:
                payload.update(reason='unsupported_current_financial_coverage', detail=str(exc))
            else:
                from app.services.sec_issuer_cash_flow import supplement
                values = supplement(values, raw, company, client=client)
                if symbol in ISSUER_PROFILES:
                    from app.services.issuer_financial_reader import supplement as issuer_financials
                    values = issuer_financials(values, company, observed_at=now)
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
        if symbol in ISSUER_PROFILES and _issuer_dependency(db, symbol) != dependency:
            raise ValueError('Issuer financial source changed during preparation')
        key = f'sec-current-fundamentals:{symbol}:v1'
        row = db.get(InsightsSnapshot, key)
        if row is None:
            row = InsightsSnapshot(kind=key, source='sec_edgar', fetched_at=now, payload_json='{}')
            db.add(row)
        row.source, row.fetched_at, row.payload_json = 'sec_edgar', now, json.dumps(payload, sort_keys=True)
        db.commit()
    return payload
