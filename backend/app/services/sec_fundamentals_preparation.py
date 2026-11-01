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
# Match the existing broad-source failure backoff; do not shorten cooldowns.
CASH_FLOW_RETRY_TTL = timedelta(hours=20)
CASH_FLOW_RECOVERY_VERSION = 'issuer_cash_flow_transport_isolation_v1'
DISCOVERY_RETRY_TTL = timedelta(minutes=20)
METRICS = ('gross_margin', 'operating_margin', 'net_margin', 'revenue_growth',
    'operating_margin_expansion', 'eps_growth', 'current_ratio', 'roe', 'free_cash_flow', 'fcf_margin', 'fcf_growth', 'net_debt_to_ebitda')


def version_for_symbol(symbol):
    from app.services.sec_parent_equity import PROFILES, VERSION as equity_version
    base = _base_version_for_symbol(symbol)
    from app.services.sec_revenue_concept_parity import PROFILES as revenue_profiles, VERSION as revenue_version
    if symbol in revenue_profiles:
        base += '_'+revenue_version
    from app.services.sec_nonrevenue_financials import PROFILES as partial_profiles, VERSION as partial_version
    if symbol in partial_profiles:
        base += '_'+partial_version
    if symbol == 'NVDA':
        base += '_'+CASH_FLOW_RECOVERY_VERSION
    return base+"_"+equity_version if symbol in PROFILES else base


def _base_version_for_symbol(symbol):
    if symbol == 'SHEL':
        from app.services.sec_shell_financials import VERSION as shell_version
        return VERSION+'_'+shell_version
    if symbol == 'VALE':
        from app.services.sec_vale_financials import VERSION as vale_version
        return VERSION+'_'+vale_version
    if symbol == 'UL':
        from app.services.sec_ul_financials import VERSION as ul_version
        return VERSION+'_'+ul_version
    if symbol == 'CNQ':
        from app.services.sec_cnq_financials import VERSION as cnq_version
        return VERSION+'_'+cnq_version
    if symbol == 'BABA':
        from app.services.sec_baba_financials import VERSION as baba_version
        return VERSION+'_'+baba_version
    if symbol == 'NVO':
        from app.services.sec_nvo_financials import VERSION as nvo_version
        return VERSION+'_'+nvo_version
    if symbol == 'SAP':
        from app.services.sec_sap_financials import VERSION as sap_version
        return VERSION+'_'+sap_version
    if symbol == 'NVS':
        from app.services.sec_nvs_financials import VERSION as nvs_version
        return VERSION+'_'+nvs_version
    from app.services.sec_bank_revenue import REVIEWED_SYMBOL, REVIEWED_VERSION
    if symbol == REVIEWED_SYMBOL:
        return VERSION+'_'+REVIEWED_VERSION
    if symbol == 'SPCX':
        from app.services.sec_registration_history import VERSION as registration_version
        return VERSION+'_'+registration_version
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
    if symbol == 'MGA':
        from app.services.sec_mga_financials import VERSION as mga_version
        return VERSION+'_'+mga_version
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
            if 'discovery' in payload:
                from app.services.sec_financial_discovery_reader import VERSION as discovery_version
                if (os.getenv('SEC_FINANCIAL_DISCOVERY_ENABLED','0')!='1'
                        or payload.get('discovery_version')!=discovery_version):return None
            if payload.get('reason') == 'financial_discovery_pending' and now-stamp >= DISCOVERY_RETRY_TTL:
                return None
            observed = datetime.fromisoformat(payload['observed_at'])
            cash_flow = payload.get('calculation_evidence', {}).get('issuer_cash_flow', {})
            if (symbol == 'NVDA' and cash_flow.get('reason') == 'cash_flow_source_unavailable'
                    and cash_flow.get('retryable') is True and now-stamp >= CASH_FLOW_RETRY_TTL):
                return None
        except (ValueError, TypeError, KeyError, AttributeError):
            return None
        from app.services.sec_tagged_foreign import cached_currency_valid
        if not cached_currency_valid(payload.get('calculation_evidence') or {}):
            return None
        if symbol in ISSUER_PROFILES and payload.get('issuer_dependency') != _issuer_dependency(db, symbol):
            return None
        if (payload.get('version') == version_for_symbol(symbol) and payload.get('symbol') == symbol
                and payload.get('source') == 'sec_edgar' and observed == stamp):
            return payload
    return None


def _continue_discovery(symbol,cik):
    """Only failed or already-discovered preparations enter the continuation."""
    from app.services import sec_financial_discovery_store as discovery_store
    if not discovery_store.enabled() or symbol in ISSUER_PROFILES:return False
    from app.services.sec_reporting_policy import reviewed
    from app.services.sec_currency_ratios import basis
    from app.services.sec_foreign_statements import profile
    if reviewed(symbol,cik) or basis(symbol,cik):return False
    annual_profile=profile(symbol,cik)
    with SessionLocal() as db:
        row=db.get(InsightsSnapshot,f'sec-current-fundamentals:{symbol}:v1')
        if row is None or row.source!='sec_edgar' or len(row.payload_json)>2_000_000:return False
        try:payload=json.loads(row.payload_json)
        except (ValueError,TypeError):return False
    if annual_profile:
        # An annual statement profile does not qualify a later interim report.
        # Permit only a current, identity-bound rollover hold (or its continued
        # discovery) to collect evidence. Restore still requires a complete
        # inventory and revalidation of the full original statements.
        from app.services.sec_financial_discovery_reader import VERSION as discovery_version
        if (not isinstance(payload,dict) or payload.get('symbol')!=symbol
                or payload.get('source')!='sec_edgar'
                or payload.get('version')!=version_for_symbol(symbol)
                or not isinstance(payload.get('source_evidence'),dict)
                or payload['source_evidence'].get('cik')!=str(cik).zfill(10)):
            return False
        continuation=(isinstance(payload.get('discovery'),dict)
            and payload.get('discovery_version')==discovery_version)
        rollover=(payload.get('status')=='unavailable' and not payload.get('values')
            and payload.get('reason')=='unsupported_current_financial_coverage'
            and payload.get('detail') in {
                f'{symbol.lower()}_new_or_amended_report_requires_review',
                f'{symbol.lower()}_newer_or_amended_report_requires_review'})
        return continuation or rollover
    return (isinstance(payload,dict) and payload.get('symbol')==symbol and
        (isinstance(payload.get('discovery'),dict) or (payload.get('status')=='unavailable'
        and not payload.get('values') and payload.get('reason') in {
            'unsupported_current_financial_coverage','unqualified_html_statement','unreviewed_foreign_statement_currency'})))


def _prepare_discovery(payload,company,raw,*,symbol,cik,client,now,dependency):
    from zoneinfo import ZoneInfo
    from app.services import sec_financial_discovery_store as store,sec_financial_discovery_reader as reader
    available=now.astimezone(ZoneInfo('America/New_York')).date()-timedelta(days=1)
    common=dict(symbol=symbol,cik=cik,available_by=available,observed_at=now)
    from app.services.sec_foreign_statements import profile
    annual_rollover=bool(profile(symbol,cik))
    progress=store.progress(company,raw,client=client,annual_rollover=annual_rollover,**common)
    payload['discovery']=progress
    payload['discovery_version']=reader.VERSION
    payload['reason']='financial_discovery_pending' if progress['status']=='pending' else 'financial_discovery_held'
    if progress['status']=='ready_for_original_revalidation':
        # Qualification and revalidation share one six-original worker budget.
        # A newly qualified final filing is restored on the next ordinary run.
        if progress['phase']!='complete':
            payload['reason']='financial_discovery_pending'
        else:
            filing=reader.restore(company,raw,checkpoint=store.read(symbol,cik),client=client,annual_rollover=annual_rollover,**common)
            try:
                values=sec_fundamentals.project_fundamentals(raw,company,symbol=symbol,cik=cik,
                    observed_at=now,filing_supplement=filing)
            except sec_fundamentals.SecFundamentalsCoverageError as exc:
                payload.update(reason='unsupported_current_financial_coverage',detail=str(exc))
            else:
                payload.pop('reason',None)
                payload['values']={name:values[name] for name in METRICS if name in values}
                payload['period_date']=str(values['period_date'])
                payload['calculation_evidence']=json.loads(values['source_evidence_json'])
                payload['coverage']['available_metrics']=sorted(payload['values'])
                payload['status']='partial' if payload['values'] else 'unavailable'
    return _save(symbol,payload,now,dependency)


def prepare(symbol, *, max_cache_age=TTL, continue_discovery=False):
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
            if continue_discovery and _continue_discovery(symbol,cik):
                from app.services.sec_financial_discovery import _inventory
                from zoneinfo import ZoneInfo
                available=now.astimezone(ZoneInfo('America/New_York')).date()-timedelta(days=1)
                if _inventory(company,symbol=symbol,cik=cik,available_by=available):
                    return _prepare_discovery(payload,company,raw,symbol=symbol,cik=cik,
                        client=client,now=now,dependency=dependency)
            from app.services.sec_financial_continuity import fetch_history
            history = fetch_history(symbol,cik,client=client,observed_at=now)
            from app.services.sec_inline_financials import acquire
            from app.services.sec_tagged_foreign import TaggedCurrencyError
            from app.services.sec_statement_currency import StatementCurrencyError
            from app.services.sec_tabular_reporting import TabularReportingError
            try:
                filing = acquire(raw,company,symbol=symbol,cik=cik,observed_at=now,client=client,include_debt_precision=True)
                values = sec_fundamentals.project_fundamentals(raw, company,
                    symbol=symbol, cik=cik, observed_at=now, history_raw=history,filing_supplement=filing)
            except (TaggedCurrencyError, StatementCurrencyError):
                payload.update(reason='unreviewed_foreign_statement_currency')
            except TabularReportingError as exc:
                payload.update(reason='unqualified_html_statement',detail=str(exc))
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
    return _save(symbol,payload,now,dependency)


def _save(symbol,payload,now,dependency):
    # Network operations finish before opening the write transaction.
    if 'discovery' in payload and os.getenv('SEC_FINANCIAL_DISCOVERY_ENABLED','0')!='1':
        raise ValueError('SEC financial discovery changed during preparation')
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
