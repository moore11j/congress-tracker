"""Current US GAAP fundamentals with filing-bound, period-aligned calculations.

No consensus estimates, sector guesses, or substitutions for unavailable facts.
Companyfacts covers standard entity-wide concepts, not every issuer extension.
"""
from __future__ import annotations

from app.services.issuer_financial_registry import PROFILES as ISSUER_PROFILES
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import math
from functools import lru_cache
from zoneinfo import ZoneInfo

VERSION = "sec_fundamentals_v1"
TAGS = {
    "revenue": ("Revenues", "RevenuesNetOfInterestExpense", "RegulatedAndUnregulatedOperatingRevenue", "RevenueFromContractWithCustomerExcludingAssessedTax", "SalesRevenueNet",
                "RevenueFromContractWithCustomerIncludingAssessedTax"),
    "gross_profit": ("GrossProfit",),
    "operating_income": ("OperatingIncomeLoss",),
    "net_income": ("NetIncomeLoss",),
    "operating_cash_flow": ("NetCashProvidedByUsedInOperatingActivities",),
    "capex": ("PaymentsToAcquirePropertyPlantAndEquipment",),
    "equity": ("StockholdersEquity",),
    "current_assets": ("AssetsCurrent",),
    "current_liabilities": ("LiabilitiesCurrent",),
}
INSTANT = {"equity", "current_assets", "current_liabilities"}


class SecFundamentalsError(ValueError):
    pass


class SecFundamentalsCoverageError(SecFundamentalsError):
    """Verified issuer identity, but insufficient aligned financial facts."""


@lru_cache(maxsize=2)
def company_directory(day: str):
    from app.clients.direct_sources import DirectSourceClient
    from app.services.direct_feed_collection import DIRECTORY_URL, validate_directory
    raw = DirectSourceClient().get(DIRECTORY_URL)
    return {row['symbol']: row for row in validate_directory(json.loads(raw))}


def fetch_fundamentals(symbol: str):
    from app.clients.direct_sources import DirectSourceClient
    now = datetime.now(timezone.utc)
    item = company_directory(str(now.date())).get(symbol)
    if item is None:
        raise SecFundamentalsError('Symbol absent from SEC directory')
    cik = item['cik']
    client = DirectSourceClient()
    company = client.get(f'https://data.sec.gov/submissions/CIK{cik}.json')
    facts = client.get(f'https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json')
    from app.services.sec_financial_continuity import fetch_history
    history = fetch_history(symbol,cik,client=client,observed_at=now)
    from app.services.sec_inline_financials import acquire
    filing = acquire(facts,company,symbol=symbol,cik=cik,observed_at=now,client=client,include_debt_precision=True)
    try:
        values = project_fundamentals(facts, company, symbol=symbol, cik=cik, observed_at=now, history_raw=history, filing_supplement=filing)
    except SecFundamentalsCoverageError as exc:
        # Identity has already passed the projector's checks. Keep independently
        # verified market inputs usable for rankings even when financial facts
        # are unsupported. No stale financial metrics survive this replacement.
        metadata = json.loads(company)
        from app.utils.symbols import canonical_symbol
        tickers = [canonical_symbol(s) for s in metadata.get('tickers', [])]
        exchanges = metadata.get('exchanges', [])
        values = {'symbol': canonical_symbol(symbol), 'provider': 'sec_edgar',
            'company_name': metadata['name'], 'fetched_at': now, 'period_date': None,
            'exchange': exchanges[tickers.index(canonical_symbol(symbol))] if len(exchanges) == len(tickers) else None,
            'status': 'ok', 'error': None,
            'source_evidence_json': json.dumps({'methodology': VERSION,
                'financial_status': 'unavailable', 'financial_error': str(exc), 'cik': cik,
                'available_by': str(now.astimezone(ZoneInfo('America/New_York')).date() - timedelta(days=1)),
                'availability_basis': 'filed date, available after completed New York filing day',
                'facts': {'url': f'https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json', 'sha256': hashlib.sha256(facts).hexdigest()},
                'company': {'url': f'https://data.sec.gov/submissions/CIK{cik}.json', 'sha256': hashlib.sha256(company).hexdigest()},
                'current': {}, 'prior': {}, 'instant': {}, 'calculations': {},
                'limitations': ['Financial facts unavailable; market data is independently verified']})}
    from app.services.sec_issuer_cash_flow import supplement
    values = supplement(values, facts, company, client=client)
    if symbol in ISSUER_PROFILES:
        from app.services.issuer_financial_reader import supplement as issuer_financials
        values = issuer_financials(values, company, observed_at=now)
    # Financial facts remain useful when the separately sourced market data is
    # unavailable. Never carry old market ratios into a newly refreshed record.
    from app.clients.massive_stocks import MassiveStocksClient, MassiveStocksError, selected
    evidence = json.loads(values['source_evidence_json'])
    if selected():
        try:
            massive = MassiveStocksClient()
            details = massive.details(symbol)
            if str(details.get('cik', '')).zfill(10) != cik:
                raise MassiveStocksError('reference_cik_mismatch')
            cap = _number(details.get('market_cap'))
            if cap and cap > 0:
                values['market_cap'] = cap
                evidence['market'] = {'provider': 'massive', 'observed_at': now.isoformat(),
                    'basis': 'current reference, not historical point-in-time', 'reference': details}
                # The market cap is USD. Foreign reported amounts must never
                # be divided into it without a reviewed currency conversion.
                usd = evidence.get('reporting_currency', 'USD') == 'USD'
                revenue = evidence['current'].get('revenue') if usd else None
                from app.services.sec_common_income import resolve as common_income
                common = common_income(facts, company, evidence, symbol=symbol, cik=cik,
                    observed_at=now, history_raw=history, filing_supplement=filing)
                evidence['market_income'] = common
                income = common['point'] if usd and common['status'] == 'available' else None
                if income is None:
                    evidence['limitations'].append('P/E and earnings yield require verified income available to common shareholders.')
                if not usd:
                    evidence['limitations'].append('USD market-cap ratios are unavailable for non-USD reported amounts.')
                if revenue and revenue['value'] > 0:
                    values['price_to_sales'] = cap / revenue['value']
                    evidence['calculations']['price_to_sales'] = 'current Massive market cap / TTM revenue'
                if income:
                    income_basis = 'TTM income available to common shareholders'
                    values['earnings_yield'] = income['value'] / cap * 100
                    evidence['calculations']['earnings_yield'] = income_basis+' / current Massive market cap * 100'
                    if income['value'] > 0:
                        values['trailing_pe'] = cap / income['value']
                        evidence['calculations']['trailing_pe'] = 'current Massive market cap / '+income_basis
                if usd and 'free_cash_flow' in values:
                    values['fcf_yield'] = values['free_cash_flow'] / cap * 100
                    evidence['calculations']['fcf_yield'] = 'TTM free cash flow / current Massive market cap * 100'
            end = now.astimezone(timezone.utc).date() - timedelta(days=1)
            rows = massive.bars(symbol, str(end - timedelta(days=60)), str(end))
            bars = {}
            for row in rows:
                try:
                    day = datetime.fromtimestamp(float(row['t'])/1000, timezone.utc).astimezone(ZoneInfo('America/New_York')).date()
                except (KeyError, ValueError, TypeError, OverflowError, OSError):
                    continue
                close, volume = _number(row.get('c')), _number(row.get('v'))
                if end - timedelta(days=60) <= day <= end and close is not None and close > 0:
                    if day in bars and bars[day] != (close, volume):
                        raise MassiveStocksError('conflicting_daily_bar')
                    bars[day] = (close, volume)
            ordered = sorted(bars.items())
            if ordered and (end - ordered[-1][0]).days <= 4:
                values['price'], values['volume'] = ordered[-1][1]
                volumes = [v for _, (_, v) in ordered[-20:] if v is not None and v >= 0]
                if len(volumes) == 20:
                    values['avg_volume'] = sum(volumes) / 20
                evidence['daily_market'] = {'provider': 'massive', 'adjusted': True,
                    'as_of': str(ordered[-1][0]), 'bars': {str(d): {'close': c, 'volume': v} for d,(c,v) in ordered[-20:]}}
        except MassiveStocksError as exc:
            evidence['market_error'] = str(exc)
    values['source_evidence_json'] = json.dumps(evidence, sort_keys=True)
    return values


def _day(value):
    try:
        return date.fromisoformat(value)
    except (TypeError, ValueError):
        return None


def _number(value):
    if isinstance(value, bool):
        return None
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (TypeError, ValueError):
        return None


def _records(payload, metric, available_by, *, concepts=None, unit='USD', namespace='us-gaap', forms=None):
    """Latest available filing per exact period; equal-date conflicts are held."""
    periods = {}
    gaap = payload.get("facts", {}).get(namespace, {})
    concepts = TAGS[metric] if concepts is None else concepts
    for priority, tag in enumerate(concepts):
        for row in gaap.get(tag, {}).get("units", {}).get(unit, []):
            end, filed, start = _day(row.get("end")), _day(row.get("filed")), _day(row.get("start"))
            value = _number(row.get("val"))
            if (row.get("form") not in (forms if forms is not None else {"10-K", "10-Q", "10-K/A", "10-Q/A"})
                    or not end or not filed or end > filed or filed > available_by
                    or value is None or not row.get("accn")):
                continue
            if metric in INSTANT:
                if row.get("start"):
                    continue
            elif not start or not 1 <= (end - start).days + 1 <= 380:
                continue
            key = (start, end)
            fact = {"value": value, "tag": tag, "unit": unit, "start": str(start) if start else None,
                    "end": str(end), "filed": str(filed), "accession": row["accn"], "priority": priority}
            periods.setdefault(key, []).append(fact)
    results = {}
    # A company can report both total revenue and a smaller contract-revenue
    # component. Choose one current concept series, never merge their periods.
    present_tags = {row['tag'] for rows in periods.values() for row in rows}
    if not present_tags:
        return results
    chosen_tag = max(present_tags, key=lambda tag: (
        max(end for (_, end), rows in periods.items() if any(row['tag'] == tag for row in rows)),
        -concepts.index(tag)))
    for key, rows in periods.items():
        rows = [row for row in rows if row['tag'] == chosen_tag]
        if not rows:
            continue
        latest = max(row["filed"] for row in rows)
        rows = [row for row in rows if row["filed"] == latest]
        if len({row["value"] for row in rows}) != 1:
            results[key] = None
            continue
        results[key] = min(rows, key=lambda row: row["priority"])
    return results


def _ttm(records, end):
    annual = [(s, e, f) for (s, e), f in records.items() if f and s and 350 <= (e - s).days + 1 <= 380]
    direct = [(s, e, f) for s, e, f in annual if e == end]
    if len(direct) == 1:
        s, e, f = direct[0]
        return {"value": f["value"], "start": str(s), "end": str(e), "method": "derived_annual" if f.get("derived") else "reported_annual", "inputs": [f]}
    # Fiscal year plus this year's YTD minus comparable prior-year YTD.
    candidates = []
    for astart, aend, afact in annual:
        if not 1 <= (end - aend).days <= 300:
            continue
        current = records.get((aend + timedelta(days=1), end))
        if current is None:
            continue
        for (pstart, pend), previous in records.items():
            if (previous is None or pstart != astart or not 350 <= (end - pend).days <= 378
                    or abs((end - aend).days - ((pend - pstart).days + 1)) > 7):
                continue
            candidates.append({"value": afact["value"] + current["value"] - previous["value"],
                "start": str(pend + timedelta(days=1)), "end": str(end), "method": "annual_plus_ytd_minus_prior_ytd",
                "inputs": [afact, current, previous]})
    return candidates[0] if len(candidates) == 1 else None


def _prior_end(records, end):
    candidates = sorted({e for _, e in records if 350 <= (end - e).days <= 378}, reverse=True)
    return candidates[0] if len(candidates) == 1 else None


def project_fundamentals(facts_raw: bytes, company_raw: bytes, *, symbol: str, cik: str,
                         observed_at: datetime, history_raw=None, filing_supplement=None):
    """Return independently supported cache values with current filing evidence.

    Filed dates have day resolution. Today's filings are excluded until the day
    is complete, avoiding an invented intraday availability time.
    """
    if observed_at.tzinfo is None:
        raise SecFundamentalsError("Observation timestamp must be timezone aware")
    from app.utils.symbols import canonical_symbol
    symbol = canonical_symbol(symbol)
    cik = str(cik).zfill(10)
    payload, company = json.loads(facts_raw), json.loads(company_raw)
    if not cik.isdigit() or any(str(obj.get("cik", "")).zfill(10) != cik for obj in (payload, company)):
        raise SecFundamentalsError("SEC company identity mismatch")
    tickers = [canonical_symbol(x) for x in company.get("tickers", [])]
    if not symbol or tickers.count(symbol) != 1 or not company.get("name"):
        raise SecFundamentalsError("SEC symbol identity is missing or ambiguous")
    available_by = observed_at.astimezone(ZoneInfo('America/New_York')).date() - timedelta(days=1)
    if (filing_supplement or {}).get('evidence',{}).get('table_reporting'):
        from app.services.sec_tabular_reporting import core
        return core(facts_raw,company_raw,symbol=symbol,cik=cik,observed_at=observed_at,supplement=filing_supplement)
    if symbol == 'UL':
        from app.services.sec_ul_financials import project
        return project(facts_raw, company_raw, symbol=symbol, cik=cik, observed_at=observed_at, supplement=filing_supplement)
    if symbol == 'ASML':
        from app.services.sec_asml_financials import project
        return project(facts_raw, company_raw, symbol=symbol, cik=cik,
                       observed_at=observed_at, supplement=filing_supplement)
    if symbol == 'CNQ':
        from app.services.sec_cnq_financials import project
        return project(facts_raw, company_raw, symbol=symbol, cik=cik, observed_at=observed_at, supplement=filing_supplement)
    if symbol == 'BABA':
        from app.services.sec_baba_financials import project
        return project(facts_raw, company_raw, symbol=symbol, cik=cik, observed_at=observed_at, supplement=filing_supplement)
    if symbol == 'NVO':
        from app.services.sec_nvo_financials import project
        return project(facts_raw, company_raw, symbol=symbol, cik=cik, observed_at=observed_at, supplement=filing_supplement)
    if symbol == 'SAP':
        from app.services.sec_sap_financials import project
        return project(facts_raw, company_raw, symbol=symbol, cik=cik, observed_at=observed_at, supplement=filing_supplement)
    if symbol == 'MGA':
        from app.services.sec_mga_financials import project
        return project(facts_raw, company_raw, symbol=symbol, cik=cik,
                       observed_at=observed_at, supplement=filing_supplement)
    if symbol == 'TSM':
        from app.services.sec_tsm_financials import project
        return project(facts_raw, company_raw, symbol=symbol, cik=cik,
                       observed_at=observed_at, supplement=filing_supplement)
    from app.services.sec_financial_continuity import record_sets
    from app.services import sec_leverage, sec_parent_income, sec_depreciation_components
    from app.services import sec_parent_statement_income, sec_parent_equity
    from app.services.sec_currency_ratios import basis as currency_basis, REVIEW as currency_review
    from app.services import sec_statement_currency as native
    native_selection = native.authorized(payload,company,symbol=symbol,cik=cik,
        available_by=available_by,supplement=filing_supplement)
    native_basis = native.reporting_basis(filing_supplement) if native_selection else currency_basis(symbol, cik)
    currency = native_basis['currency'] if native_basis else 'USD'
    metric_tags = native_basis['tags'] if native_basis else {**TAGS, **sec_leverage.TAGS, **sec_parent_income.TAGS}
    if symbol in sec_parent_statement_income.SYMBOLS:
        metric_tags.update(sec_parent_statement_income.TAGS)
    from app.services import sec_operating_components
    if sec_operating_components.applies(symbol, cik):
        metric_tags.update(sec_operating_components.TAGS)
    if sec_depreciation_components.applies(symbol, cik):
        metric_tags.update(sec_depreciation_components.TAGS)
    equity_tags = sec_parent_equity.tags_for(symbol, cik)
    metric_tags.update(equity_tags)
    all_records, continuity = record_sets(payload,symbol=symbol,cik=cik,available_by=available_by,observed_at=observed_at,history_raw=history_raw,filing_supplement=filing_supplement,company=company,
        metrics={name:(tags, currency) for name,tags in metric_tags.items()}, instant_metrics=set(sec_leverage.INSTANT) | set(equity_tags))
    all_records, operating_evidence = sec_operating_components.apply(all_records, symbol=symbol, cik=cik)
    all_records, consolidated_income_evidence = sec_parent_income.consolidated_for_leverage(all_records)
    from app.services import sec_statement_income
    all_records, statement_income_evidence = sec_statement_income.apply(all_records, symbol=symbol, cik=cik)
    all_records, parent_income_evidence = sec_parent_income.apply(all_records)
    all_records, parent_statement_evidence = sec_parent_statement_income.apply(all_records, symbol=symbol, cik=cik)
    all_records, parent_equity_evidence = sec_parent_equity.apply(all_records, symbol=symbol, cik=cik, payload=payload)
    depreciation_evidence = sec_depreciation_components.reconcile(all_records, symbol=symbol, cik=cik)
    records = {name: all_records[name] for name in TAGS}
    ends = sorted({end for _, end in records["revenue"]}, reverse=True)
    end = next((e for e in ends if _ttm(records["revenue"], e) is not None), None)
    if end is None or (available_by - end).days > 180 or (ends and end != ends[0]):
        from app.services.sec_nonrevenue_financials import project as partial_financials
        return partial_financials(records, company, symbol=symbol, cik=cik,
            facts_raw=facts_raw, company_raw=company_raw, observed_at=observed_at, available_by=available_by,
            filing_supplement=filing_supplement, currency=currency,
            namespace=native_basis['namespace'] if native_basis else 'us-gaap',
            native_selection=native_selection,
            source_context={
                'financial_continuity': continuity,
                'parent_income_reconciliation': parent_income_evidence,
                'parent_statement_review': parent_statement_evidence,
                'parent_equity_reconciliation': parent_equity_evidence,
            })
    prior_end = _prior_end(records["revenue"], end)
    current = {name: _ttm(rows, end) for name, rows in records.items() if name not in INSTANT}
    prior = {name: _ttm(rows, prior_end) if prior_end else None for name, rows in records.items() if name not in INSTANT}
    reference_start = current["revenue"]["start"]
    for name, item in list(current.items()):
        if item and item["start"] != reference_start:
            current[name] = None
    if prior.get("revenue"):
        for name, item in list(prior.items()):
            if item and item["start"] != prior["revenue"]["start"]:
                prior[name] = None
    evidence = {"methodology": VERSION, "available_by": str(available_by),
        "availability_basis": "filed date, available after completed New York filing day", "cik": cik,
        "facts": {"url": f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json", "sha256": hashlib.sha256(facts_raw).hexdigest()},
        "company": {"url": f"https://data.sec.gov/submissions/CIK{cik}.json", "sha256": hashlib.sha256(company_raw).hexdigest()},
        "period_start": reference_start, "period_end": str(end), "current": current, "prior": prior,
        "instant": {}, "calculations": {}, "limitations": ["USD US GAAP standard concepts only",
            "No consensus estimates, EBITDA/debt proxies, or inferred per-share inputs",
            "SIC is not mapped to vendor sectors; exchange comes from SEC company metadata"]}
    if native_basis:
        evidence['reporting_currency'] = currency
        evidence['reviewed_currency_basis'] = dict(native_basis.get('review', currency_review))
        accounting = 'IFRS' if native_basis['namespace'] == 'ifrs-full' else 'US GAAP'
        evidence['limitations'][0] = currency+' '+accounting+(' standard concepts with explicit original-statement currency evidence' if native_selection else ' standard concepts under the reviewed issuer policy')
        if currency != 'USD':
            evidence['limitations'].append('Only dimensionless ratios are published; no FX conversion or USD cash amounts.')
        else:
            evidence['limitations'].extend([
                native_basis['review'].get('amount_basis', 'Reported USD cash amounts and ratios; no FX conversion or ADR per-share inference.'),
                native_basis['review'].get('margin_basis', 'Margin denominators use net sales, excluding separately reported other revenue.'),
                native_basis['review']['cash_flow_basis']])
    from app.services.sec_reporting_policy import reviewed
    reporting_policy = reviewed(symbol, cik)
    if reporting_policy:
        evidence['reviewed_reporting_policy'] = reporting_policy
    if operating_evidence is not None:
        evidence['operating_income_reconciliation'] = operating_evidence
        if operating_evidence['derived_periods']:
            evidence['limitations'].append('HCA operating income is derived from reviewed expenses with independent pretax reconciliation, not a reported subtotal.')
    if parent_income_evidence['derived_periods'] or parent_income_evidence['holds']:
        evidence['parent_income_reconciliation'] = parent_income_evidence
    if parent_equity_evidence is not None:
        evidence["parent_equity_reconciliation"] = parent_equity_evidence
    if parent_statement_evidence is not None:
        evidence['parent_statement_review'] = parent_statement_evidence
    from app.services.sec_bank_revenue import basis
    revenue_basis = basis(records)
    if revenue_basis:
        evidence['revenue_basis'] = revenue_basis
        evidence['limitations'].append('Bank revenue uses a derived net-interest basis, not gross interest income')
    if continuity is not None:
        evidence['financial_continuity'] = continuity
        if continuity.get('methodology') == 'sec_spcx_registration_v2':
            evidence['limitations'].extend(continuity['limitations'])
            evidence['limitations'].append('Historical amounts mapped from reviewed USD GAAP statement labels, with the original registration form retained; cash flow uses CFO minus reported PPE cash purchases, not issuer-adjusted FCF.')
    if filing_supplement is not None:
        evidence['inline_filing_supplement'] = filing_supplement['evidence']
    exchanges = company.get("exchanges", [])
    values = {"symbol": symbol, "provider": "sec_edgar", "company_name": company["name"],
        "exchange": exchanges[tickers.index(symbol)] if len(exchanges) == len(tickers) else None,
        "fetched_at": observed_at, "period_date": end, "status": "ok", "error": None}

    def val(name, older=False):
        fact = (prior if older else current).get(name)
        return fact["value"] if fact else None

    def save(name, value, formula):
        if value is not None and math.isfinite(value):
            values[name] = value
            evidence["calculations"][name] = formula

    def ratio(n, d, scale=1):
        return n / d * scale if n is not None and d is not None and d > 0 else None

    for name, numerator in (("gross_margin", "gross_profit"), ("operating_margin", "operating_income"), ("net_margin", "net_income")):
        label = ('parent net income (consolidated profit minus reported noncontrolling interest)'
            if numerator == 'net_income' and sec_parent_income.is_derived(current.get('net_income')) else numerator)
        if numerator == "net_income" and sec_parent_statement_income.is_reviewed(current.get("net_income")):
            label = "parent net income (reviewed original-statement reconciliation)"
        save(name, ratio(val(numerator), val("revenue"), 100), f"TTM {label} / TTM revenue * 100")
    growth = ratio(val("revenue"), val("revenue", True), 100)
    save("revenue_growth", growth - 100 if growth is not None else None, "(TTM revenue / prior TTM revenue - 1) * 100")
    old_margin = ratio(val("operating_income", True), val("revenue", True), 100)
    margin = values.get("operating_margin")
    save("operating_margin_expansion", margin - old_margin if margin is not None and old_margin is not None else None,
         "TTM operating margin minus prior TTM operating margin, percentage points")
    for metric in INSTANT:
        evidence["instant"][metric] = records[metric].get((None, end))
    def instant(name, at=end):
        fact = records[name].get((None, at))
        return fact["value"] if fact else None
    save("current_ratio", ratio(instant("current_assets"), instant("current_liabilities")), "period-end current assets / current liabilities")
    beginning = date.fromisoformat(reference_start) - timedelta(days=1)
    equity_then, equity_now = instant("equity", beginning), instant("equity")
    evidence["instant"]["beginning_equity"] = records["equity"].get((None, beginning))
    if equity_then is not None and equity_now is not None and min(equity_then, equity_now) > 0:
        income_label = ("parent net income (consolidated profit minus reported noncontrolling interest)"
            if sec_parent_income.is_derived(current.get("net_income")) else "net income")
        if sec_parent_statement_income.is_reviewed(current.get("net_income")):
            income_label = "parent net income (reviewed original-statement reconciliation)"
        equity_label = "equity"
        if any((evidence["instant"].get(k) or {}).get("derivation") == sec_parent_equity.VERSION
               for k in ("equity", "beginning_equity")):
            equity_label = "parent equity (reviewed component and balance-sheet reconciliation)"
        save("roe", ratio(val("net_income"), (equity_then + equity_now) / 2, 100), f"TTM {income_label} / average beginning and ending {equity_label} * 100")
    cash, capex = val("operating_cash_flow"), val("capex")
    old_cash, old_capex = val("operating_cash_flow", True), val("capex", True)
    fcf = cash - capex if cash is not None and capex is not None and capex >= 0 else None
    old_fcf = old_cash - old_capex if old_cash is not None and old_capex is not None and old_capex >= 0 else None
    if currency == 'USD':
        save("free_cash_flow", fcf, "TTM operating cash flow minus TTM cash paid for property, plant and equipment")
    else:
        evidence['native_cash_flow'] = {'currency': currency, 'current': fcf, 'prior': old_fcf,
            'formula': 'TTM operating cash flow minus TTM cash paid for property, plant and equipment',
            'canonical_usd_amount_available': False}
    save("fcf_margin", ratio(fcf, val("revenue"), 100), "TTM free cash flow / TTM revenue * 100")
    fcf_growth = ratio(fcf, old_fcf, 100)
    save("fcf_growth", fcf_growth - 100 if fcf_growth is not None else None, "(TTM free cash flow / prior TTM free cash flow - 1) * 100")
    evidence["leverage_preparation"] = sec_leverage.project(all_records, end=end, start=reference_start)
    if native_basis:
        evidence['leverage_preparation']['reporting_currency'] = currency
        evidence['leverage_preparation']['currency_policy'] = ('Reviewed IFRS leverage is not prepared by this ratios adapter.' if native_basis['namespace'] == 'ifrs-full' else 'Non-USD leverage is not prepared by this ratios adapter.')
        evidence['leverage_preparation']['ebitda']['unit'] = currency
    evidence['leverage_preparation']['income_reconciliation'] = consolidated_income_evidence
    if statement_income_evidence is not None:
        evidence['leverage_preparation']['statement_income_reconciliation'] = statement_income_evidence
    if depreciation_evidence is not None:
        evidence['leverage_preparation']['depreciation_reconciliation'] = depreciation_evidence
    from app.services.sec_net_debt_policy import select as select_net_debt
    policy = select_net_debt(evidence['leverage_preparation'], company)
    evidence['net_debt_policy'] = policy
    if policy['status'] == 'available':
        save('net_debt_to_ebitda', policy['value'], policy['formula'])
    values["source_evidence_json"] = json.dumps(evidence, sort_keys=True)
    return values
