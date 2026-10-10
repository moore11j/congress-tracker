"""Current US GAAP fundamentals with filing-bound, period-aligned calculations.

No consensus estimates, sector guesses, or substitutions for unavailable facts.
Companyfacts covers standard entity-wide concepts, not every issuer extension.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import math
from functools import lru_cache
from zoneinfo import ZoneInfo

VERSION = "sec_fundamentals_v1"
TAGS = {
    "revenue": ("Revenues", "RevenuesNetOfInterestExpense", "RevenueFromContractWithCustomerExcludingAssessedTax", "SalesRevenueNet",
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
    try:
        values = project_fundamentals(facts, company, symbol=symbol, cik=cik, observed_at=now)
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
                revenue = evidence['current'].get('revenue')
                income = evidence['current'].get('net_income')
                if revenue and revenue['value'] > 0:
                    values['price_to_sales'] = cap / revenue['value']
                    evidence['calculations']['price_to_sales'] = 'current Massive market cap / TTM revenue'
                if income:
                    values['earnings_yield'] = income['value'] / cap * 100
                    evidence['calculations']['earnings_yield'] = 'TTM net income / current Massive market cap * 100'
                    if income['value'] > 0:
                        values['trailing_pe'] = cap / income['value']
                        evidence['calculations']['trailing_pe'] = 'current Massive market cap / TTM net income'
                if 'free_cash_flow' in values:
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


def _records(payload, metric, available_by, *, concepts=None, unit='USD'):
    """Latest available filing per exact period; equal-date conflicts are held."""
    periods = {}
    gaap = payload.get("facts", {}).get("us-gaap", {})
    concepts = TAGS[metric] if concepts is None else concepts
    for priority, tag in enumerate(concepts):
        for row in gaap.get(tag, {}).get("units", {}).get(unit, []):
            end, filed, start = _day(row.get("end")), _day(row.get("filed")), _day(row.get("start"))
            value = _number(row.get("val"))
            if (row.get("form") not in {"10-K", "10-Q", "10-K/A", "10-Q/A"}
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
        return {"value": f["value"], "start": str(s), "end": str(e), "method": "reported_annual", "inputs": [f]}
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
                         observed_at: datetime):
    """Return cache values plus evidence; requires a current, coherent revenue period.

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
    records = {name: _records(payload, name, available_by) for name in TAGS}
    ends = sorted({end for _, end in records["revenue"]}, reverse=True)
    end = next((e for e in ends if _ttm(records["revenue"], e) is not None), None)
    if end is None or (available_by - end).days > 180:
        raise SecFundamentalsCoverageError("No current complete trailing revenue period")
    # Do not silently fall back to an older annual period when newer reported
    # revenue cannot be aligned into a complete trailing year.
    if ends and end != ends[0]:
        raise SecFundamentalsCoverageError("Latest revenue period has incomplete trailing coverage")
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
        save(name, ratio(val(numerator), val("revenue"), 100), f"TTM {numerator} / TTM revenue * 100")
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
        save("roe", ratio(val("net_income"), (equity_then + equity_now) / 2, 100), "TTM net income / average beginning and ending equity * 100")
    cash, capex = val("operating_cash_flow"), val("capex")
    old_cash, old_capex = val("operating_cash_flow", True), val("capex", True)
    fcf = cash - capex if cash is not None and capex is not None and capex >= 0 else None
    old_fcf = old_cash - old_capex if old_cash is not None and old_capex is not None and old_capex >= 0 else None
    save("free_cash_flow", fcf, "TTM operating cash flow minus TTM cash paid for property, plant and equipment")
    save("fcf_margin", ratio(fcf, val("revenue"), 100), "TTM free cash flow / TTM revenue * 100")
    fcf_growth = ratio(fcf, old_fcf, 100)
    save("fcf_growth", fcf_growth - 100 if fcf_growth is not None else None, "(TTM free cash flow / prior TTM free cash flow - 1) * 100")
    values["source_evidence_json"] = json.dumps(evidence, sort_keys=True)
    return values
