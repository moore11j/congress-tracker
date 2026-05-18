from __future__ import annotations

import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timezone
from threading import Lock
from typing import Any, Literal

import requests

from app.clients.fmp import FMP_BASE_URL

logger = logging.getLogger(__name__)

FinancialsStatus = Literal["ok", "partial", "unavailable"]
FINANCIALS_TTL_SECONDS = 6 * 60 * 60
PROVIDER_TIMEOUT_SECONDS = 5
AGGREGATE_TIMEOUT_SECONDS = 6
UNAVAILABLE_MESSAGE = "Financial data is not available for this ticker yet."
TEMPORARILY_UNAVAILABLE_MESSAGE = "Financial data is temporarily unavailable."

_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_CACHE_LOCK = Lock()


class TickerFinancialsUnavailable(RuntimeError):
    pass


def clear_financials_cache() -> None:
    with _CACHE_LOCK:
        _CACHE.clear()


def _cache_get(key: str) -> dict[str, Any] | None:
    now = time.time()
    with _CACHE_LOCK:
        cached = _CACHE.get(key)
        if not cached:
            return None
        expires_at, payload = cached
        if expires_at <= now:
            _CACHE.pop(key, None)
            return None
        return payload


def _cache_set(key: str, payload: dict[str, Any]) -> dict[str, Any]:
    with _CACHE_LOCK:
        _CACHE[key] = (time.time() + FINANCIALS_TTL_SECONDS, payload)
    return payload


def _api_key() -> str:
    key = os.getenv("FMP_API_KEY", "").strip()
    if not key:
        raise TickerFinancialsUnavailable(UNAVAILABLE_MESSAGE)
    return key


def _request_rows(endpoint: str, *, params: dict[str, Any], symbol: str) -> list[dict[str, Any]]:
    request_params = {"apikey": _api_key()}
    for key, value in params.items():
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        request_params[key] = value

    try:
        response = requests.get(f"{FMP_BASE_URL}/{endpoint}", params=request_params, timeout=PROVIDER_TIMEOUT_SECONDS)
    except requests.RequestException as exc:
        logger.info("ticker_financials request failed endpoint=%s symbol=%s error=%s", endpoint, symbol, exc)
        raise TickerFinancialsUnavailable(UNAVAILABLE_MESSAGE) from exc

    if response.status_code in {400, 404}:
        return []
    if response.status_code in {401, 402, 403, 429}:
        logger.info("ticker_financials unavailable endpoint=%s symbol=%s status=%s", endpoint, symbol, response.status_code)
        raise TickerFinancialsUnavailable(UNAVAILABLE_MESSAGE)

    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        logger.info("ticker_financials http error endpoint=%s symbol=%s status=%s", endpoint, symbol, response.status_code)
        raise TickerFinancialsUnavailable(UNAVAILABLE_MESSAGE) from exc

    try:
        payload = response.json()
    except ValueError as exc:
        raise TickerFinancialsUnavailable(UNAVAILABLE_MESSAGE) from exc

    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if isinstance(payload, dict):
        rows = payload.get("data")
        if isinstance(rows, list):
            return [row for row in rows if isinstance(row, dict)]
        return [payload] if payload else []
    return []


def _trimmed(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _numeric(row: dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        value = row.get(key)
        if isinstance(value, (int, float)):
            parsed = float(value)
            if parsed == parsed:
                return parsed
        if isinstance(value, str):
            cleaned = value.replace("$", "").replace(",", "").replace("%", "").strip()
            if not cleaned:
                continue
            try:
                parsed = float(cleaned)
            except ValueError:
                continue
            if parsed == parsed:
                return parsed
    return None


def _date_key(row: dict[str, Any]) -> str | None:
    raw = _trimmed(
        row.get("date")
        or row.get("filingDate")
        or row.get("acceptedDate")
        or row.get("calendarDate")
        or row.get("reportedDate")
    )
    if not raw:
        return None
    day = raw[:10]
    try:
        datetime.strptime(day, "%Y-%m-%d")
    except ValueError:
        return None
    return day


def _fiscal_year(row: dict[str, Any], row_date: str | None) -> str | None:
    for key in ("fiscalYear", "calendarYear", "year"):
        raw = _trimmed(row.get(key))
        if raw and raw[:4].isdigit():
            return raw[:4]
    return row_date[:4] if row_date else None


def _quarter_label(row: dict[str, Any], row_date: str | None) -> str | None:
    fiscal_year = _fiscal_year(row, row_date)
    period = (_trimmed(row.get("period") or row.get("fiscalPeriod")) or "").upper().replace("QUARTER", "Q")
    if period in {"Q1", "Q2", "Q3", "Q4"} and fiscal_year:
        return f"{period} {fiscal_year}"
    if row_date:
        month = int(row_date[5:7])
        quarter = ((month - 1) // 3) + 1
        return f"Q{quarter} {row_date[:4]}"
    return fiscal_year


def _margin(explicit: float | None, numerator: float | None, revenue: float | None) -> float | None:
    if explicit is not None:
        return explicit * 100 if abs(explicit) <= 1 else explicit
    if numerator is None or revenue in (None, 0):
        return None
    return (numerator / revenue) * 100


def _eps(row: dict[str, Any], net_income: float | None) -> float | None:
    explicit = _numeric(row, "eps", "epsDiluted", "epsdiluted", "reportedEPS", "reportedEps")
    if explicit is not None:
        return explicit
    shares = _numeric(row, "weightedAverageShsOutDil", "weightedAverageShsOut", "weightedAverageSharesDiluted")
    if net_income is not None and shares not in (None, 0):
        return net_income / shares
    return None


def _normalize_statement_row(row: dict[str, Any], *, period_type: Literal["annual", "quarterly"], cash_by_key: dict[str, dict[str, Any]]) -> dict[str, Any] | None:
    row_date = _date_key(row)
    fiscal_year = _fiscal_year(row, row_date)
    if period_type == "annual":
        period = fiscal_year
    else:
        period = _quarter_label(row, row_date)
    if not period and not row_date:
        return None

    revenue = _numeric(row, "revenue", "totalRevenue")
    gross_profit = _numeric(row, "grossProfit")
    operating_income = _numeric(row, "operatingIncome", "incomeFromOperations")
    net_income = _numeric(row, "netIncome", "netIncomeCommonStockholders", "netEarnings")
    cash_row = cash_by_key.get(row_date or "") or cash_by_key.get(period or "") or {}
    operating_cash_flow = _numeric(cash_row, "operatingCashFlow", "netCashProvidedByOperatingActivities")
    capex = _numeric(cash_row, "capitalExpenditure", "capitalExpenditures", "capitalExpense")
    free_cash_flow = _numeric(cash_row, "freeCashFlow")
    if free_cash_flow is None and operating_cash_flow is not None and capex is not None:
        free_cash_flow = operating_cash_flow + capex if capex < 0 else operating_cash_flow - capex

    return {
        "period": period or row_date,
        "date": row_date,
        "revenue": revenue,
        "netIncome": net_income,
        "eps": _eps(row, net_income),
        "grossMargin": _margin(_numeric(row, "grossProfitRatio", "grossMargin"), gross_profit, revenue),
        "operatingMargin": _margin(_numeric(row, "operatingIncomeRatio", "operatingMargin"), operating_income, revenue),
        "freeCashFlow": free_cash_flow,
        "operatingCashFlow": operating_cash_flow,
        "capex": capex,
    }


def _cash_lookup(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    lookup: dict[str, dict[str, Any]] = {}
    for row in rows:
        row_date = _date_key(row)
        if row_date:
            lookup[row_date] = row
        year = _fiscal_year(row, row_date)
        period = (_trimmed(row.get("period")) or "").upper()
        if year and period in {"FY", "ANNUAL"}:
            lookup[year] = row
        elif year and period in {"Q1", "Q2", "Q3", "Q4"}:
            lookup[f"{period} {year}"] = row
    return lookup


def _latest_rows(rows: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    sorted_rows = sorted(rows, key=lambda item: item.get("date") or item.get("period") or "")
    return sorted_rows[-limit:]


def _sum_latest(rows: list[dict[str, Any]], key: str, count: int = 4) -> float | None:
    values = [row.get(key) for row in _latest_rows(rows, count)]
    if len(values) < count or any(not isinstance(value, (int, float)) for value in values):
        return None
    return float(sum(values))


def _latest_value(rows: list[dict[str, Any]], key: str) -> float | None:
    for row in reversed(_latest_rows(rows, len(rows))):
        value = row.get(key)
        if isinstance(value, (int, float)):
            return float(value)
    return None


def _normalize_result(actual: float | None, estimate: float | None, surprise: float | None) -> str:
    delta = surprise
    if delta is None and actual is not None and estimate is not None:
        delta = actual - estimate
    if delta is None:
        return "unknown"
    if abs(delta) < 0.005:
        return "inline"
    return "beat" if delta > 0 else "miss"


def _normalize_earnings(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for row in rows:
        row_date = _date_key(row)
        if not row_date:
            continue
        actual = _numeric(row, "epsActual", "actualEarningResult", "actual", "reportedEPS", "reportedEps", "eps")
        estimate = _numeric(row, "epsEstimate", "estimatedEarning", "estimatedEPS", "estimatedEps", "estimate")
        surprise = _numeric(row, "surprise", "epsSurprise")
        if surprise is None and actual is not None and estimate is not None:
            surprise = actual - estimate
        surprise_pct = _numeric(row, "surprisePct", "surprisePercentage", "surprisePercent")
        if surprise_pct is None and surprise is not None and estimate not in (None, 0):
            surprise_pct = (surprise / abs(estimate)) * 100
        items.append(
            {
                "date": row_date,
                "period": _quarter_label(row, row_date) or row_date,
                "epsActual": actual,
                "epsEstimate": estimate,
                "surprise": surprise,
                "surprisePct": surprise_pct,
                "result": _normalize_result(actual, estimate, surprise),
            }
        )
    historical = [item for item in items if item["epsActual"] is not None or item["epsEstimate"] is not None]
    historical.sort(key=lambda item: item["date"])
    return historical[-8:]


def _normalize_forecast(row: dict[str, Any] | None, *, period_type: Literal["annual", "quarterly"]) -> dict[str, Any] | None:
    if not row:
        return None
    row_date = _date_key(row)
    period = _fiscal_year(row, row_date) if period_type == "annual" else _quarter_label(row, row_date)
    revenue = _numeric(
        row,
        "revenueAvg",
        "revenueAverage",
        "estimatedRevenueAvg",
        "estimatedRevenueAverage",
        "revenueEstimate",
        "estimatedRevenue",
    )
    eps = _numeric(row, "epsAvg", "epsAverage", "estimatedEpsAvg", "estimatedEPSAvg", "epsEstimate", "estimatedEPS", "estimatedEps")
    earnings = _numeric(
        row,
        "netIncomeAvg",
        "netIncomeAverage",
        "estimatedNetIncomeAvg",
        "estimatedNetIncome",
        "earningsAvg",
        "estimatedEarnings",
    )
    if revenue is None and eps is None and earnings is None:
        return None
    return {
        "period": period or row_date,
        "date": row_date,
        "revenueEstimate": revenue,
        "epsEstimate": eps,
        "earningsEstimate": earnings,
    }


def _next_estimate(rows: list[dict[str, Any]], *, period_type: Literal["annual", "quarterly"]) -> dict[str, Any] | None:
    today = date.today().isoformat()
    normalized = [_normalize_forecast(row, period_type=period_type) for row in rows]
    candidates = [item for item in normalized if item is not None]
    future = [item for item in candidates if item.get("date") and str(item["date"]) >= today]
    source = future or candidates
    if not source:
        return None
    return sorted(source, key=lambda item: item.get("date") or item.get("period") or "")[0 if future else -1]


def _forward_pe(quote_rows: list[dict[str, Any]], ratio_rows: list[dict[str, Any]], forecast: dict[str, Any] | None) -> float | None:
    for row in ratio_rows + quote_rows:
        value = _numeric(row, "forwardPE", "forwardPe", "forwardPERatio", "forwardPriceEarningsRatio", "forwardP/E")
        if value is not None and value > 0:
            return value

    price = None
    for row in quote_rows:
        price = _numeric(row, "price", "currentPrice", "close", "previousClose")
        if price is not None:
            break
    eps_estimate = forecast.get("epsEstimate") if forecast else None
    if price is not None and isinstance(eps_estimate, (int, float)) and eps_estimate > 0:
        return price / float(eps_estimate)
    return None


def _next_earnings_date(rows: list[dict[str, Any]]) -> str | None:
    today = date.today().isoformat()
    candidates: list[str] = []
    for row in rows:
        row_date = _date_key(row)
        if not row_date or row_date < today:
            continue
        if _numeric(row, "epsActual", "actualEarningResult", "actual", "reportedEPS", "reportedEps") is None:
            candidates.append(row_date)
    return min(candidates) if candidates else None


def _company_name(*row_groups: list[dict[str, Any]]) -> str | None:
    for rows in row_groups:
        for row in rows:
            value = _trimmed(row.get("companyName") or row.get("name"))
            if value:
                return value
    return None


def _unavailable(symbol: str, *, message: str = UNAVAILABLE_MESSAGE) -> dict[str, Any]:
    return {
        "symbol": symbol,
        "companyName": None,
        "status": "unavailable",
        "message": message,
        "summary": {
            "revenueTtm": None,
            "netIncomeTtm": None,
            "epsTtm": None,
            "forwardPE": None,
            "grossMargin": None,
            "operatingMargin": None,
            "nextEarningsDate": None,
            "latestQuarter": None,
            "freeCashFlowTtm": None,
            "operatingCashFlowTtm": None,
        },
        "annual": [],
        "quarterly": [],
        "earnings": [],
        "forecasts": {
            "nextQuarter": None,
            "nextFiscalYear": None,
        },
        "health": {},
        "sections": {
            "income": "unavailable",
            "earnings": "unavailable",
            "cashFlow": "unavailable",
            "forecasts": "unavailable",
            "valuation": "unavailable",
            "health": "unavailable",
        },
        "updatedAt": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
    }


def _section_status(*, failed: bool, has_rows: bool, partial: bool = False) -> str:
    if has_rows and failed:
        return "partial"
    if has_rows:
        return "ok"
    if partial:
        return "partial"
    return "unavailable"


def _fetch_financial_sections(normalized_symbol: str) -> tuple[dict[str, list[dict[str, Any]]], set[str]]:
    specs: dict[str, tuple[str, dict[str, Any]]] = {
        "annual_income": ("income-statement", {"period": "annual", "page": 0, "limit": 6}),
        "quarterly_income": ("income-statement", {"period": "quarter", "page": 0, "limit": 12}),
        "annual_cash": ("cash-flow-statement", {"period": "annual", "page": 0, "limit": 6}),
        "quarterly_cash": ("cash-flow-statement", {"period": "quarter", "page": 0, "limit": 12}),
        "earnings": ("earnings", {"page": 0, "limit": 16}),
        "quarterly_estimates": ("analyst-estimates", {"period": "quarter", "page": 0, "limit": 8}),
        "annual_estimates": ("analyst-estimates", {"period": "annual", "page": 0, "limit": 8}),
        "quote": ("quote", {}),
        "ratios_ttm": ("ratios-ttm", {}),
    }
    rows_by_key: dict[str, list[dict[str, Any]]] = {key: [] for key in specs}
    failed_keys: set[str] = set()

    executor = ThreadPoolExecutor(max_workers=len(specs))
    try:
        futures = {
            executor.submit(
                _request_rows,
                endpoint,
                params={"symbol": normalized_symbol, **params},
                symbol=normalized_symbol,
            ): key
            for key, (endpoint, params) in specs.items()
        }
        deadline = time.monotonic() + AGGREGATE_TIMEOUT_SECONDS
        for future, key in futures.items():
            remaining = max(0.01, deadline - time.monotonic())
            try:
                rows_by_key[key] = future.result(timeout=remaining)
            except Exception:
                failed_keys.add(key)
                rows_by_key[key] = []
    finally:
        executor.shutdown(wait=False, cancel_futures=True)

    return rows_by_key, failed_keys


def get_ticker_financials(symbol: str) -> dict[str, Any]:
    normalized_symbol = (symbol or "").strip().upper()
    if not normalized_symbol:
        return _unavailable("")
    cache_key = f"financials:{normalized_symbol}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    rows_by_key, failed_keys = _fetch_financial_sections(normalized_symbol)
    annual_income = rows_by_key["annual_income"]
    quarterly_income = rows_by_key["quarterly_income"]
    annual_cash = rows_by_key["annual_cash"]
    quarterly_cash = rows_by_key["quarterly_cash"]
    earnings_rows = rows_by_key["earnings"]
    quarterly_estimates = rows_by_key["quarterly_estimates"]
    annual_estimates = rows_by_key["annual_estimates"]
    quote_rows = rows_by_key["quote"]
    ratio_rows = rows_by_key["ratios_ttm"]

    annual = [
        item
        for item in (
            _normalize_statement_row(row, period_type="annual", cash_by_key=_cash_lookup(annual_cash))
            for row in annual_income
        )
        if item is not None
    ]
    quarterly = [
        item
        for item in (
            _normalize_statement_row(row, period_type="quarterly", cash_by_key=_cash_lookup(quarterly_cash))
            for row in quarterly_income
        )
        if item is not None
    ]
    annual = _latest_rows(annual, 5)
    quarterly = _latest_rows(quarterly, 8)
    earnings = _normalize_earnings(earnings_rows)
    forecasts = {
        "nextQuarter": _next_estimate(quarterly_estimates, period_type="quarterly"),
        "nextFiscalYear": _next_estimate(annual_estimates, period_type="annual"),
    }

    has_core_data = bool(annual or quarterly)
    if not has_core_data and not earnings:
        message = TEMPORARILY_UNAVAILABLE_MESSAGE if failed_keys else UNAVAILABLE_MESSAGE
        return _cache_set(cache_key, _unavailable(normalized_symbol, message=message))

    latest_quarter = quarterly[-1]["period"] if quarterly else None
    status: FinancialsStatus = "ok" if annual and quarterly else "partial"
    if failed_keys and status == "ok":
        status = "partial"
    income_failed = bool({"annual_income", "quarterly_income"} & failed_keys)
    cash_failed = bool({"annual_cash", "quarterly_cash"} & failed_keys)
    forecasts_failed = bool({"quarterly_estimates", "annual_estimates"} & failed_keys)
    valuation_failed = bool({"quote", "ratios_ttm"} & failed_keys)
    sections = {
        "income": _section_status(failed=income_failed, has_rows=has_core_data, partial=bool(annual) != bool(quarterly)),
        "earnings": _section_status(failed="earnings" in failed_keys, has_rows=bool(earnings)),
        "cashFlow": _section_status(failed=cash_failed, has_rows=bool(annual_cash or quarterly_cash)),
        "forecasts": _section_status(failed=forecasts_failed, has_rows=bool(forecasts["nextQuarter"] or forecasts["nextFiscalYear"])),
        "valuation": _section_status(
            failed=valuation_failed,
            has_rows=_forward_pe(quote_rows, ratio_rows, forecasts["nextFiscalYear"]) is not None,
        ),
        "health": "unavailable",
    }
    forward_pe = _forward_pe(quote_rows, ratio_rows, forecasts["nextFiscalYear"])

    payload = {
        "symbol": normalized_symbol,
        "companyName": _company_name(annual_income, quarterly_income, earnings_rows),
        "status": status,
        "summary": {
            "revenueTtm": _sum_latest(quarterly, "revenue"),
            "netIncomeTtm": _sum_latest(quarterly, "netIncome"),
            "epsTtm": _sum_latest(quarterly, "eps"),
            "forwardPE": forward_pe,
            "grossMargin": _latest_value(quarterly, "grossMargin") or _latest_value(annual, "grossMargin"),
            "operatingMargin": _latest_value(quarterly, "operatingMargin") or _latest_value(annual, "operatingMargin"),
            "nextEarningsDate": _next_earnings_date(earnings_rows),
            "latestQuarter": latest_quarter,
            "freeCashFlowTtm": _sum_latest(quarterly, "freeCashFlow"),
            "operatingCashFlowTtm": _sum_latest(quarterly, "operatingCashFlow"),
        },
        "annual": annual,
        "quarterly": quarterly,
        "earnings": earnings,
        "forecasts": forecasts,
        "health": {},
        "sections": sections,
        "updatedAt": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
    }
    return _cache_set(cache_key, payload)
