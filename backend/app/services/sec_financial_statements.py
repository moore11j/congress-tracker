"""Partial historical US GAAP financial panels from SEC entity-wide facts.

Reported periods and aligned YTD differences only; no forecasts, invented EPS,
sector-specific extension mapping, or market-data requests.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import os
from zoneinfo import ZoneInfo

from app.clients.direct_sources import DirectSourceClient, DirectSourceError
from app.db import SessionLocal
from app.models import InsightsSnapshot
from app.services import sec_fundamentals as facts
from app.services.sec_directory import _symbol_key
from app.utils.symbols import normalize_symbol


def selected():
    return os.getenv('FINANCIAL_STATEMENTS_PROVIDER', 'fmp').strip().lower() == 'sec_edgar'


def period_value(records, start, end):
    """A filed conflicting direct fact cannot be bypassed with a derivation."""
    if (start, end) in records:
        fact = records[start, end]
        return {'value': fact['value'], 'method': 'reported', 'inputs': [fact]} if fact else None
    candidates = []
    for (year_start, ytd_end), total in records.items():
        if not total or ytd_end != end or not year_start or year_start >= start:
            continue
        prior = records.get((year_start, start-timedelta(days=1)))
        if prior and prior['tag'] == total['tag'] and (end-year_start).days < 380:
            candidates.append({'value': total['value']-prior['value'], 'method': 'aligned_ytd_difference', 'inputs': [total, prior]})
    return candidates[0] if len(candidates) == 1 else None


def project(facts_raw, company_raw, *, symbol, cik, observed_at):
    from app.services.ticker_financials import _unavailable, _subsection
    if observed_at.tzinfo is None:
        raise ValueError('Observation timestamp must be aware')
    data, company = json.loads(facts_raw), json.loads(company_raw)
    cik = str(cik).zfill(10)
    if any(str(obj.get('cik', '')).zfill(10) != cik for obj in (data, company)) or not company.get('name'):
        raise DirectSourceError('financial_company_identity_mismatch')
    matches = [s for s in company.get('tickers', []) if _symbol_key(s) == _symbol_key(symbol)]
    if len(matches) != 1:
        raise DirectSourceError('financial_symbol_identity_mismatch')
    available = observed_at.astimezone(ZoneInfo('America/New_York')).date()-timedelta(days=1)
    records = {name: facts._records(data, name, available) for name in facts.TAGS}
    eps_records = facts._records(data, 'eps', available, concepts=('EarningsPerShareDiluted',), unit='USD/shares')
    annual, quarters = set(), set()
    for (start, end), value in records['revenue'].items():
        if not value or not start:
            continue
        days = (end-start).days+1
        if 350 <= days <= 380:
            annual.add((start, end))
        if 70 <= days <= 110:
            quarters.add((start, end))
    # Fourth quarters may only appear as annual totals less nine-month YTD.
    for start, end in annual:
        for (ytd_start, ytd_end), value in records['revenue'].items():
            if value and ytd_start == start and 70 <= (end-ytd_end).days <= 110:
                quarters.add((ytd_end+timedelta(days=1), end))
    evidence = {}
    def rows(periods, count, kind):
        result = []
        # Multiple fiscal starts for one period end are ambiguous, not two
        # different quarters or years to chart under the same date.
        unique = {end: {start for start, candidate in periods if candidate == end} for _, end in periods}
        periods = {(next(iter(starts)), end) for end, starts in unique.items() if len(starts) == 1}
        for start, end in sorted(periods, key=lambda p: p[1])[-count:]:
            values = {name: period_value(series, start, end) for name, series in records.items() if name not in facts.INSTANT}
            if not values.get('revenue'):
                continue
            value = lambda name: values[name]['value'] if values.get(name) else None
            revenue, income, cash, capex = value('revenue'), value('net_income'), value('operating_cash_flow'), value('capex')
            margin = lambda name: value(name)/revenue*100 if value(name) is not None and revenue > 0 else None
            row = {'period': f'{"Year" if kind == "annual" else "Quarter"} ended {end}', 'date': str(end),
                'revenue': revenue, 'netIncome': income,
                'eps': eps_records[start, end]['value'] if eps_records.get((start, end)) else None,
                'grossMargin': margin('gross_profit'),
                'operatingMargin': margin('operating_income'), 'operatingCashFlow': cash,
                'capex': capex, 'freeCashFlow': cash-capex if cash is not None and capex is not None and capex >= 0 else None,
                'source': 'sec_edgar', 'periodStart': str(start), 'currency': 'USD'}
            evidence[f'{kind}:{end}'] = values
            evidence[f'{kind}:{end}']['reportedDilutedEps'] = eps_records.get((start, end))
            result.append(row)
        return result
    annual_rows, quarterly_rows = rows(annual, 6, 'annual'), rows(quarters, 12, 'quarterly')
    payload = _unavailable(symbol, message='SEC reported USD financials with partial coverage. Diluted EPS is shown only when directly reported. Analyst forecasts and valuation inputs are unavailable.', reason='partial_sec_coverage')
    payload.update(source='sec_edgar', companyName=company['name'], annual=annual_rows, quarterly=quarterly_rows,
        updatedAt=observed_at.isoformat(),
        status='partial' if annual_rows or quarterly_rows else 'unavailable', unavailable=not bool(annual_rows or quarterly_rows),
        sourceEvidence={'methodology': 'sec_statements_v1', 'availableBy': str(available), 'periods': evidence,
            'factsSha256': hashlib.sha256(facts_raw).hexdigest(), 'companySha256': hashlib.sha256(company_raw).hexdigest(),
            'factsUrl': f'https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json',
            'availabilityBasis': 'Current view uses latest filed facts available after completed New York filing day; not a historical tradable snapshot.'})
    if annual_rows or quarterly_rows:
        payload['sections']['income'] = 'partial'
        payload['subsections']['income'] = _subsection(status='limited', reason_code='partial_sec_coverage', data={'annual': annual_rows, 'quarterly': quarterly_rows})
    if any(row['operatingCashFlow'] is not None for row in annual_rows+quarterly_rows):
        payload['sections']['cashFlow'] = 'partial'
        payload['subsections']['cash_flow'] = _subsection(status='limited', reason_code='partial_sec_coverage', data={'annual': annual_rows, 'quarterly': quarterly_rows})
    try:
        current = facts.project_fundamentals(facts_raw, company_raw, symbol=matches[0], cik=cik, observed_at=observed_at)
    except facts.SecFundamentalsCoverageError:
        current = {}
    if current:
        source = json.loads(current['source_evidence_json'])
        def ttm(name):
            item = source['current'].get(name)
            return item['value'] if item else None
        payload['summary'].update(revenueTtm=ttm('revenue'), netIncomeTtm=ttm('net_income'),
            operatingCashFlowTtm=ttm('operating_cash_flow'), freeCashFlowTtm=current.get('free_cash_flow'),
            grossMargin=current.get('gross_margin'), operatingMargin=current.get('operating_margin'), currentRatio=current.get('current_ratio'))
        payload['health']['currentRatio'] = current.get('current_ratio')
        if current.get('current_ratio') is not None:
            payload['sections']['health'] = 'partial'
            payload['subsections']['health'] = _subsection(status='limited', reason_code='partial_sec_coverage', data=payload['health'])
    return payload


def cached_payload(db, symbol, *, now=None):
    """Read a current selected-source panel in the caller's transaction."""
    now = now or datetime.now(timezone.utc)
    row = db.get(InsightsSnapshot, f'sec-financials:{symbol}:v1')
    if row is None or row.source != 'sec_edgar':
        return None
    stamp = row.fetched_at.replace(tzinfo=timezone.utc) if row.fetched_at.tzinfo is None else row.fetched_at
    if not timedelta(0) <= now-stamp < timedelta(hours=24):
        return None
    try:
        payload = json.loads(row.payload_json)
    except (TypeError, ValueError):
        return None
    return payload if isinstance(payload, dict) and payload.get('source') == 'sec_edgar' and payload.get('symbol') == symbol else None


def prepared(symbol):
    from app.request_priority import get_request_context
    from app.services.ticker_financials import _warming
    from app.services.data_enrichment_queue import enqueue_data_enrichment_job
    from app.services.sec_directory import directory
    now = datetime.now(timezone.utc)
    key = f'sec-financials:{symbol}:v1'
    route = str((get_request_context() or {}).get('path') or '')
    public = route.startswith('/api/') and not route.startswith('/api/admin/')
    with SessionLocal() as db:
        if not public and db.get_bind().dialect.name == 'postgresql':
            from sqlalchemy import text
            if not db.execute(text('SELECT pg_try_advisory_xact_lock(hashtext(:key))'), {'key': key}).scalar():
                return _warming(symbol, reason='refresh_in_progress')
        row = db.get(InsightsSnapshot, key)
        payload = cached_payload(db, symbol, now=now)
        if payload is not None:
            return payload
        if public:
            enqueue_data_enrichment_job(job_type='ticker_financials', symbol=symbol, source='page_load', priority=45, reason='sec_financials_refresh')
            return _warming(symbol, reason='replacement_cache_miss')
        item = directory().get(_symbol_key(symbol))
        if not item:
            from app.services.ticker_financials import _unavailable
            payload = _unavailable(symbol, message='SEC financial statements are unavailable for this security.',
                                   reason='symbol_absent_from_sec_directory')
            payload.update(source='sec_edgar', updatedAt=now.isoformat())
        else:
            cik = item['cik']
            client = DirectSourceClient()
            company = client.get(f'https://data.sec.gov/submissions/CIK{cik}.json')
            facts_url = f'https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json'
            try:
                raw = client.get(facts_url)
            except DirectSourceError as exc:
                if str(exc) != f'Source HTTP 404: {facts_url}':
                    raise
                from app.services.ticker_financials import _unavailable
                payload = _unavailable(symbol, message='SEC company facts are unavailable for this security.',
                                       reason='sec_company_facts_not_found')
                payload.update(source='sec_edgar', updatedAt=now.isoformat(),
                               sourceEvidence={'factsUrl': facts_url, 'responseStatus': 404})
            else:
                payload = project(raw, company, symbol=symbol, cik=cik, observed_at=now)
        if row is None:
            row = InsightsSnapshot(kind=key, source='sec_edgar', fetched_at=now, payload_json='{}')
            db.add(row)
        row.source, row.fetched_at, row.payload_json = 'sec_edgar', now, json.dumps(payload, sort_keys=True)
        db.commit()
        return payload
