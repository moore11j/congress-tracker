from __future__ import annotations

import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, wait
from contextvars import copy_context
from datetime import date, datetime, timezone
from threading import Lock
from typing import Any, Literal

import requests

from app.clients.fmp import FMP_BASE_URL
from app.request_priority import get_request_context
from app.services.data_enrichment_queue import enqueue_data_enrichment_job
from app.services.provider_usage import (
    ProviderUnavailable,
    ensure_fmp_live_allowed,
    fallback_payload,
    reason_for_status,
    reason_from_exception,
    record_cache_hit,
    record_cache_miss,
    record_fallback,
    record_provider_response,
)

logger = logging.getLogger(__name__)

FinancialsStatus = Literal["ok", "partial", "unavailable"]
FINANCIALS_TTL_SECONDS = 6 * 60 * 60
FINANCIALS_STALE_TTL_SECONDS = 7 * 24 * 60 * 60
PROVIDER_TIMEOUT_SECONDS = 5
AGGREGATE_TIMEOUT_SECONDS = 6
FINANCIALS_MAX_WORKERS = 5
UNAVAILABLE_MESSAGE = "Financial data is not available for this ticker yet."
TEMPORARILY_UNAVAILABLE_MESSAGE = "Financial data is temporarily unavailable."

_CACHE: dict[str, tuple[float, float, float, dict[str, Any]]] = {}
_CACHE_LOCK = Lock()


class TickerFinancialsUnavailable(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        reason_code: str = "provider_unavailable",
        status_code: int | None = None,
    ) -> None:
        super().__init__(message)
        self.reason = reason_code
        self.reason_code = reason_code
        self.status_code = status_code


def clear_financials_cache() -> None:
    with _CACHE_LOCK:
        _CACHE.clear()


def _cache_get(key: str) -> dict[str, Any] | None:
    now = time.time()
    with _CACHE_LOCK:
        cached = _CACHE.get(key)
        if not cached:
            return None
        fetched_at, expires_at, stale_until, payload = cached
        if expires_at <= now:
            if stale_until <= now:
                _CACHE.pop(key, None)
            return None
        symbol = key.split(":", 1)[1] if ":" in key else None
        record_cache_hit(category="financials", symbol=symbol, cache_age_seconds=max(now - fetched_at, 0))
        return payload


def _cache_get_stale(key: str, *, symbol: str) -> tuple[dict[str, Any], float] | None:
    now = time.time()
    with _CACHE_LOCK:
        cached = _CACHE.get(key)
        if not cached:
            return None
        fetched_at, expires_at, stale_until, payload = cached
        if expires_at > now:
            return None
        if stale_until <= now:
            _CACHE.pop(key, None)
            return None
        age = max(now - fetched_at, 0)
        record_cache_hit(category="financials", symbol=symbol, cache_age_seconds=age)
        return payload, age


def _cache_set(key: str, payload: dict[str, Any]) -> dict[str, Any]:
    now = time.time()
    with _CACHE_LOCK:
        _CACHE[key] = (now, now + FINANCIALS_TTL_SECONDS, now + FINANCIALS_STALE_TTL_SECONDS, payload)
    return payload


def _api_key() -> str:
    key = os.getenv("FMP_API_KEY", "").strip()
    if not key:
        raise TickerFinancialsUnavailable(UNAVAILABLE_MESSAGE, reason_code="provider_disabled")
    return key


def _reason_code_for_status(status_code: int) -> str:
    if status_code == 402:
        return "provider_entitlement"
    if status_code in {401, 403}:
        return "provider_disabled"
    if status_code == 429:
        return "provider_rate_limited"
    if status_code >= 500:
        return "provider_unavailable"
    return "provider_error"


def _request_rows(endpoint: str, *, params: dict[str, Any], symbol: str) -> list[dict[str, Any]]:
    category = f"financials:{endpoint}"
    try:
        ensure_fmp_live_allowed(category=category, symbol=symbol)
    except ProviderUnavailable as exc:
        raise TickerFinancialsUnavailable(str(exc), reason_code=getattr(exc, "reason", "provider_unavailable")) from exc
    request_params = {"apikey": _api_key()}
    for key, value in params.items():
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        request_params[key] = value

    try:
        response = requests.get(f"{FMP_BASE_URL}/{endpoint}", params=request_params, timeout=PROVIDER_TIMEOUT_SECONDS)
        record_provider_response(category=category, symbol=symbol, status_code=response.status_code)
    except requests.RequestException as exc:
        logger.info("ticker_financials request failed endpoint=%s symbol=%s error=%s", endpoint, symbol, exc)
        raise TickerFinancialsUnavailable(UNAVAILABLE_MESSAGE) from exc

    if response.status_code in {400, 404}:
        return []
    if response.status_code in {401, 402, 403, 429}:
        logger.info("ticker_financials unavailable endpoint=%s symbol=%s status=%s", endpoint, symbol, response.status_code)
        raise TickerFinancialsUnavailable(
            reason_for_status(response.status_code),
            reason_code=_reason_code_for_status(response.status_code),
            status_code=response.status_code,
        )

    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        logger.info("ticker_financials http error endpoint=%s symbol=%s status=%s", endpoint, symbol, response.status_code)
        raise TickerFinancialsUnavailable(
            UNAVAILABLE_MESSAGE,
            reason_code=_reason_code_for_status(response.status_code),
            status_code=response.status_code,
        ) from exc

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


def _normalize_earnings(rows: list[dict[str, Any]], supplemental_rows: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for row in rows:
        row_date = _date_key(row)
        if not row_date:
            continue
        actual = _numeric(row, "epsActual", "actualEarningResult", "actual", "reportedEPS", "reportedEps", "eps")
        estimate = _numeric(row, "epsEstimate", "epsEstimated", "estimatedEarning", "estimatedEPS", "estimatedEps", "estimate")
        surprise = _numeric(row, "surprise", "epsSurprise")
        surprise_pct = _numeric(row, "surprisePct", "surprisePercentage", "surprisePercent")
        item = merged.setdefault(
            row_date,
            {
                "date": row_date,
                "period": _quarter_label(row, row_date) or row_date,
                "epsActual": None,
                "epsEstimate": None,
                "surprise": None,
                "surprisePct": None,
                "result": "unknown",
            },
        )
        item["epsActual"] = item["epsActual"] if item["epsActual"] is not None else actual
        item["epsEstimate"] = item["epsEstimate"] if item["epsEstimate"] is not None else estimate
        item["surprise"] = item["surprise"] if item["surprise"] is not None else surprise
        item["surprisePct"] = item["surprisePct"] if item["surprisePct"] is not None else surprise_pct

    for row in supplemental_rows or []:
        row_date = _date_key(row)
        if not row_date or row_date not in merged:
            continue
        item = merged[row_date]
        actual = _numeric(row, "epsActual", "actualEarningResult", "actual", "reportedEPS", "reportedEps", "eps")
        estimate = _numeric(row, "epsEstimate", "epsEstimated", "estimatedEarning", "estimatedEPS", "estimatedEps", "estimate")
        item["epsActual"] = item["epsActual"] if item["epsActual"] is not None else actual
        item["epsEstimate"] = item["epsEstimate"] if item["epsEstimate"] is not None else estimate

    items = list(merged.values())
    for item in items:
        actual = item["epsActual"]
        estimate = item["epsEstimate"]
        if item["surprise"] is None and actual is not None and estimate is not None:
            item["surprise"] = actual - estimate
        if item["surprisePct"] is None and item["surprise"] is not None and estimate not in (None, 0):
            item["surprisePct"] = (item["surprise"] / abs(estimate)) * 100
        item["result"] = _normalize_result(actual, estimate, item["surprise"])

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
    revenue_low = _numeric(row, "revenueLow", "revenueEstimateLow", "estimatedRevenueLow")
    revenue_high = _numeric(row, "revenueHigh", "revenueEstimateHigh", "estimatedRevenueHigh")
    eps = _numeric(row, "epsAvg", "epsAverage", "estimatedEpsAvg", "estimatedEPSAvg", "epsEstimate", "estimatedEPS", "estimatedEps")
    eps_low = _numeric(row, "epsLow", "epsEstimateLow", "estimatedEpsLow", "estimatedEPSLow")
    eps_high = _numeric(row, "epsHigh", "epsEstimateHigh", "estimatedEpsHigh", "estimatedEPSHigh")
    earnings = _numeric(
        row,
        "netIncomeAvg",
        "netIncomeAverage",
        "estimatedNetIncomeAvg",
        "estimatedNetIncome",
        "earningsAvg",
        "estimatedEarnings",
    )
    earnings_low = _numeric(row, "netIncomeLow", "netIncomeEstimateLow", "estimatedNetIncomeLow", "earningsLow", "estimatedEarningsLow")
    earnings_high = _numeric(row, "netIncomeHigh", "netIncomeEstimateHigh", "estimatedNetIncomeHigh", "earningsHigh", "estimatedEarningsHigh")
    if revenue is None and eps is None and earnings is None:
        return None
    return {
        "period": period or row_date,
        "date": row_date,
        "revenueEstimate": revenue,
        "revenueLow": revenue_low,
        "revenueHigh": revenue_high,
        "epsEstimate": eps,
        "epsLow": eps_low,
        "epsHigh": eps_high,
        "earningsEstimate": earnings,
        "earningsLow": earnings_low,
        "earningsHigh": earnings_high,
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


def _trailing_pe(quote_rows: list[dict[str, Any]], ratio_rows: list[dict[str, Any]]) -> float | None:
    for row in ratio_rows + quote_rows:
        value = _numeric(
            row,
            "priceToEarningsRatioTTM",
            "priceEarningsRatioTTM",
            "priceEarningsRatio",
            "peRatioTTM",
            "peRatio",
            "trailingPE",
            "trailing_pe",
            "peTTM",
            "pe",
        )
        if value is not None and value > 0:
            return value
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


def _latest_provider_row(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {}
    dated = [(row, _date_key(row) or "") for row in rows]
    dated.sort(key=lambda item: item[1])
    return dated[-1][0]


def _ratio_from_parts(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator in (None, 0):
        return None
    return numerator / denominator


def _normalize_health(ratio_rows: list[dict[str, Any]], metrics_rows: list[dict[str, Any]], balance_rows: list[dict[str, Any]]) -> dict[str, float | None]:
    ratio_row = _latest_provider_row(ratio_rows)
    metrics_row = _latest_provider_row(metrics_rows)
    balance_row = _latest_provider_row(balance_rows)

    total_debt = _numeric(balance_row, "totalDebt", "shortTermDebt", "longTermDebt")
    total_equity = _numeric(balance_row, "totalStockholdersEquity", "totalEquity", "totalShareholderEquity")
    total_assets = _numeric(balance_row, "totalAssets")
    total_liabilities = _numeric(balance_row, "totalLiabilities", "totalLiabilitiesAndStockholdersEquity")
    current_assets = _numeric(balance_row, "totalCurrentAssets")
    current_liabilities = _numeric(balance_row, "totalCurrentLiabilities")

    return {
        "debtToEquity": _numeric(
            ratio_row,
            "debtEquityRatioTTM",
            "debtToEquityTTM",
            "debtEquityRatio",
            "debtToEquity",
        )
        or _numeric(metrics_row, "debtToEquityTTM", "debtToEquity")
        or _ratio_from_parts(total_debt, total_equity),
        "currentRatio": _numeric(ratio_row, "currentRatioTTM", "currentRatio")
        or _numeric(metrics_row, "currentRatioTTM", "currentRatio")
        or _ratio_from_parts(current_assets, current_liabilities),
        "assetRatio": _numeric(ratio_row, "assetTurnoverTTM", "assetTurnover")
        or _numeric(metrics_row, "assetTurnoverTTM", "assetTurnover")
        or _ratio_from_parts(total_assets, total_liabilities),
    }


def _has_health_data(health: dict[str, Any]) -> bool:
    return any(isinstance(value, (int, float)) for value in health.values())


def _subsection_status(*, failed: bool, has_data: bool, limited: bool = False, loading: bool = False) -> str:
    if loading:
        return "loading"
    if has_data and (failed or limited):
        return "limited"
    if has_data:
        return "ok"
    return "unavailable"


def _subsection(
    *,
    status: str,
    data: Any,
    reason_code: str | None = None,
) -> dict[str, Any]:
    return {
        "status": status,
        "reason_code": reason_code,
        "data": data,
    }


def _unavailable_subsections(*, loading: bool = False, reason_code: str = "provider_unavailable") -> dict[str, dict[str, Any]]:
    status = "loading" if loading else "unavailable"
    return {
        "income": _subsection(status=status, reason_code=reason_code, data={"annual": [], "quarterly": []}),
        "cash_flow": _subsection(status=status, reason_code=reason_code, data={"annual": [], "quarterly": []}),
        "earnings": _subsection(status=status, reason_code=reason_code, data=[]),
        "analyst_estimates": _subsection(status=status, reason_code=reason_code, data={"nextQuarter": None, "nextFiscalYear": None}),
        "valuation": _subsection(status=status, reason_code=reason_code, data={"trailingPE": None, "forwardPE": None}),
        "health": _subsection(status=status, reason_code=reason_code, data={}),
    }


def _unavailable(symbol: str, *, message: str = UNAVAILABLE_MESSAGE, reason: str = "provider_unavailable") -> dict[str, Any]:
    return {
        "symbol": symbol,
        "companyName": None,
        "status": "unavailable",
        "message": message,
        **fallback_payload(reason=reason, message=message),
        "summary": {
            "revenueTtm": None,
            "netIncomeTtm": None,
            "epsTtm": None,
            "trailingPE": None,
            "forwardPE": None,
            "grossMargin": None,
            "operatingMargin": None,
            "nextEarningsDate": None,
            "latestQuarter": None,
            "freeCashFlowTtm": None,
            "operatingCashFlowTtm": None,
            "debtToEquity": None,
            "currentRatio": None,
            "assetRatio": None,
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
        "subsections": _unavailable_subsections(reason_code=reason),
        "updatedAt": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
    }


def _warming(symbol: str, *, message: str = TEMPORARILY_UNAVAILABLE_MESSAGE, reason: str = "cache_miss") -> dict[str, Any]:
    payload = _unavailable(symbol, message=message, reason=reason)
    payload["status"] = "warming"
    payload["subsections"] = _unavailable_subsections(loading=True, reason_code=reason)
    if _is_public_request_context():
        payload.pop("message", None)
        payload.pop("reason", None)
        payload.pop("unavailable", None)
        payload.pop("data", None)
        payload["cache_status"] = "warming"
    return payload


def _stale_financials(payload: dict[str, Any], *, reason: str, age_seconds: float) -> dict[str, Any]:
    stale = {
        **payload,
        "stale": True,
        "unavailable": False,
        "cache_status": "stale",
        "cache_age_seconds": round(age_seconds, 1),
    }
    if _is_public_request_context():
        stale.pop("message", None)
        stale.pop("reason", None)
        stale.pop("data", None)
    else:
        stale["reason"] = reason
    return stale


def _enqueue_financials_refresh(symbol: str, *, reason: str) -> None:
    if not _is_public_request_context():
        return
    enqueue_data_enrichment_job(
        job_type="ticker_financials",
        symbol=symbol,
        source="page_load",
        reason=reason,
        priority=45,
    )


def _is_public_request_context() -> bool:
    context = get_request_context() or {}
    route = str(context.get("path") or "")
    return route.startswith("/api/") and not route.startswith("/api/admin/")


def _section_status(*, failed: bool, has_rows: bool, partial: bool = False) -> str:
    if has_rows and failed:
        return "partial"
    if has_rows:
        return "ok"
    if partial:
        return "partial"
    return "unavailable"


def _section_for_key(key: str) -> str:
    if key.endswith("_income"):
        return "income"
    if key.endswith("_cash"):
        return "cash_flow"
    if key.endswith("_balance"):
        return "health"
    if key in {"earnings", "earnings_calendar"}:
        return "earnings"
    if key.endswith("_estimates"):
        return "analyst_estimates"
    if key in {"quote", "ratios_ttm", "key_metrics_ttm"}:
        return "valuation"
    return key


def _fetch_financial_sections(normalized_symbol: str) -> tuple[dict[str, list[dict[str, Any]]], set[str], dict[str, str]]:
    specs: dict[str, tuple[str, dict[str, Any]]] = {
        "annual_income": ("income-statement", {"period": "annual", "page": 0, "limit": 6}),
        "quarterly_income": ("income-statement", {"period": "quarter", "page": 0, "limit": 12}),
        "annual_cash": ("cash-flow-statement", {"period": "annual", "page": 0, "limit": 6}),
        "quarterly_cash": ("cash-flow-statement", {"period": "quarter", "page": 0, "limit": 12}),
        "annual_balance": ("balance-sheet-statement", {"period": "annual", "page": 0, "limit": 6}),
        "quarterly_balance": ("balance-sheet-statement", {"period": "quarter", "page": 0, "limit": 12}),
        "earnings": ("earnings", {"page": 0, "limit": 16}),
        "earnings_calendar": ("earnings-calendar", {"page": 0, "limit": 32}),
        "quarterly_estimates": ("analyst-estimates", {"period": "quarter", "page": 0, "limit": 8}),
        "annual_estimates": ("analyst-estimates", {"period": "annual", "page": 0, "limit": 8}),
        "quote": ("quote", {}),
        "ratios_ttm": ("ratios-ttm", {}),
        "key_metrics_ttm": ("key-metrics-ttm", {}),
    }
    rows_by_key: dict[str, list[dict[str, Any]]] = {key: [] for key in specs}
    failed_keys: set[str] = set()
    failed_section_reasons: dict[str, str] = {}

    executor = ThreadPoolExecutor(max_workers=min(FINANCIALS_MAX_WORKERS, len(specs)))
    try:
        futures = {
            executor.submit(
                copy_context().run,
                _request_rows,
                endpoint,
                params={"symbol": normalized_symbol, **params},
                symbol=normalized_symbol,
            ): key
            for key, (endpoint, params) in specs.items()
        }
        done, pending = wait(futures, timeout=AGGREGATE_TIMEOUT_SECONDS)
        for future in done:
            key = futures[future]
            try:
                rows_by_key[key] = future.result()
            except TickerFinancialsUnavailable as exc:
                failed_keys.add(key)
                rows_by_key[key] = []
                section = _section_for_key(key)
                failed_section_reasons.setdefault(section, exc.reason_code)
                logger.info(
                    "financials_subsection_unavailable symbol=%s section=%s status=%s",
                    normalized_symbol,
                    section,
                    exc.status_code or exc.reason_code,
                )
            except Exception as exc:
                failed_keys.add(key)
                rows_by_key[key] = []
                section = _section_for_key(key)
                failed_section_reasons.setdefault(section, "provider_unavailable")
                logger.info(
                    "financials_subsection_unavailable symbol=%s section=%s status=%s",
                    normalized_symbol,
                    section,
                    getattr(exc, "status_code", None) or "provider_unavailable",
                )
        for future in pending:
            key = futures[future]
            failed_keys.add(key)
            rows_by_key[key] = []
            section = _section_for_key(key)
            failed_section_reasons.setdefault(section, "timeout")
            logger.info(
                "financials_subsection_unavailable symbol=%s section=%s status=timeout",
                normalized_symbol,
                section,
            )
    finally:
        executor.shutdown(wait=False, cancel_futures=True)

    return rows_by_key, failed_keys, failed_section_reasons


def get_ticker_financials(symbol: str) -> dict[str, Any]:
    normalized_symbol = (symbol or "").strip().upper()
    if not normalized_symbol:
        return _unavailable("")
    cache_key = f"financials:{normalized_symbol}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    record_cache_miss(category="financials", symbol=normalized_symbol)
    if _is_public_request_context():
        reason = "cache_miss"
        _enqueue_financials_refresh(normalized_symbol, reason=reason)
        stale = _cache_get_stale(cache_key, symbol=normalized_symbol)
        if stale is not None:
            stale_payload, age = stale
            record_fallback(category="financials", symbol=normalized_symbol, reason=reason, cache_age_seconds=age)
            return _stale_financials(stale_payload, reason=reason, age_seconds=age)
        return _warming(normalized_symbol, message=TEMPORARILY_UNAVAILABLE_MESSAGE, reason=reason)
    if not os.getenv("FMP_API_KEY", "").strip():
        reason = "provider_disabled"
        record_fallback(category="financials", symbol=normalized_symbol, reason=reason)
        _enqueue_financials_refresh(normalized_symbol, reason=reason)
        stale = _cache_get_stale(cache_key, symbol=normalized_symbol)
        if stale is not None:
            stale_payload, age = stale
            record_fallback(category="financials", symbol=normalized_symbol, reason=reason, cache_age_seconds=age)
            return _stale_financials(stale_payload, reason=reason, age_seconds=age)
        if _is_public_request_context():
            return _warming(normalized_symbol, message=TEMPORARILY_UNAVAILABLE_MESSAGE, reason=reason)
        return _cache_set(cache_key, _unavailable(normalized_symbol, message=TEMPORARILY_UNAVAILABLE_MESSAGE, reason=reason))

    try:
        rows_by_key, failed_keys, failed_section_reasons = _fetch_financial_sections(normalized_symbol)
    except Exception as exc:
        reason = reason_from_exception(exc)
        record_fallback(category="financials", symbol=normalized_symbol, reason=reason)
        _enqueue_financials_refresh(normalized_symbol, reason=reason)
        stale = _cache_get_stale(cache_key, symbol=normalized_symbol)
        if stale is not None:
            stale_payload, age = stale
            record_fallback(category="financials", symbol=normalized_symbol, reason=reason, cache_age_seconds=age)
            return _stale_financials(stale_payload, reason=reason, age_seconds=age)
        if _is_public_request_context():
            return _warming(normalized_symbol, message=TEMPORARILY_UNAVAILABLE_MESSAGE, reason=reason)
        return _cache_set(cache_key, _unavailable(normalized_symbol, message=TEMPORARILY_UNAVAILABLE_MESSAGE, reason=reason))
    annual_income = rows_by_key["annual_income"]
    quarterly_income = rows_by_key["quarterly_income"]
    annual_cash = rows_by_key["annual_cash"]
    quarterly_cash = rows_by_key["quarterly_cash"]
    annual_balance = rows_by_key["annual_balance"]
    quarterly_balance = rows_by_key["quarterly_balance"]
    earnings_rows = rows_by_key["earnings"]
    earnings_calendar_rows = rows_by_key["earnings_calendar"]
    quarterly_estimates = rows_by_key["quarterly_estimates"]
    annual_estimates = rows_by_key["annual_estimates"]
    quote_rows = rows_by_key["quote"]
    ratio_rows = rows_by_key["ratios_ttm"]
    metrics_rows = rows_by_key["key_metrics_ttm"]

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
    earnings = _normalize_earnings(earnings_rows, earnings_calendar_rows)
    forecasts = {
        "nextQuarter": _next_estimate(quarterly_estimates, period_type="quarterly"),
        "nextFiscalYear": _next_estimate(annual_estimates, period_type="annual"),
    }
    forward_pe = _forward_pe(quote_rows, ratio_rows, forecasts["nextFiscalYear"])
    trailing_pe = _trailing_pe(quote_rows, ratio_rows)
    health = _normalize_health(ratio_rows, metrics_rows, quarterly_balance or annual_balance)
    has_health_data = _has_health_data(health)

    has_core_data = bool(annual or quarterly)
    has_valuation_data = forward_pe is not None or trailing_pe is not None
    has_any_financial_data = has_core_data or bool(earnings) or has_valuation_data or has_health_data
    if not has_any_financial_data:
        message = TEMPORARILY_UNAVAILABLE_MESSAGE if failed_keys else UNAVAILABLE_MESSAGE
        if failed_keys:
            reason = "provider_unavailable"
            _enqueue_financials_refresh(normalized_symbol, reason=reason)
            stale = _cache_get_stale(cache_key, symbol=normalized_symbol)
            if stale is not None:
                stale_payload, age = stale
                record_fallback(category="financials", symbol=normalized_symbol, reason=reason, cache_age_seconds=age)
                return _stale_financials(stale_payload, reason=reason, age_seconds=age)
            if _is_public_request_context():
                return _warming(normalized_symbol, message=message, reason=reason)
            return _cache_set(cache_key, _unavailable(normalized_symbol, message=message, reason=reason))
        return _cache_set(cache_key, _unavailable(normalized_symbol, message=message))

    latest_quarter = quarterly[-1]["period"] if quarterly else None
    status: FinancialsStatus = "ok" if annual and quarterly and not failed_keys else "partial"
    if failed_keys and status == "ok":
        status = "partial"
    income_failed = bool({"annual_income", "quarterly_income"} & failed_keys)
    cash_failed = bool({"annual_cash", "quarterly_cash"} & failed_keys)
    health_failed = bool({"annual_balance", "quarterly_balance", "ratios_ttm", "key_metrics_ttm"} & failed_keys)
    forecasts_failed = bool({"quarterly_estimates", "annual_estimates"} & failed_keys)
    valuation_failed = bool({"quote", "ratios_ttm", "key_metrics_ttm"} & failed_keys)
    forecasts_available = bool(forecasts["nextQuarter"] or forecasts["nextFiscalYear"])
    sections = {
        "income": _section_status(failed=income_failed, has_rows=has_core_data, partial=bool(annual) != bool(quarterly)),
        "earnings": _section_status(failed=bool({"earnings", "earnings_calendar"} & failed_keys), has_rows=bool(earnings)),
        "cashFlow": _section_status(failed=cash_failed, has_rows=bool(annual_cash or quarterly_cash)),
        "forecasts": _section_status(failed=forecasts_failed, has_rows=forecasts_available),
        "valuation": _section_status(
            failed=valuation_failed,
            has_rows=has_valuation_data,
        ),
        "health": _section_status(failed=health_failed, has_rows=has_health_data),
    }
    subsections = {
        "income": _subsection(
            status=_subsection_status(failed=income_failed, has_data=has_core_data, limited=bool(annual) != bool(quarterly)),
            reason_code=failed_section_reasons.get("income"),
            data={"annual": annual, "quarterly": quarterly},
        ),
        "cash_flow": _subsection(
            status=_subsection_status(failed=cash_failed, has_data=bool(annual_cash or quarterly_cash)),
            reason_code=failed_section_reasons.get("cash_flow"),
            data={"annual": annual_cash, "quarterly": quarterly_cash},
        ),
        "earnings": _subsection(
            status=_subsection_status(
                failed=bool({"earnings", "earnings_calendar"} & failed_keys),
                has_data=bool(earnings),
            ),
            reason_code=failed_section_reasons.get("earnings"),
            data=earnings,
        ),
        "analyst_estimates": _subsection(
            status=_subsection_status(failed=forecasts_failed, has_data=forecasts_available),
            reason_code=failed_section_reasons.get("analyst_estimates"),
            data=forecasts,
        ),
        "valuation": _subsection(
            status=_subsection_status(failed=valuation_failed, has_data=has_valuation_data),
            reason_code=failed_section_reasons.get("valuation"),
            data={"trailingPE": trailing_pe, "forwardPE": forward_pe},
        ),
        "health": _subsection(
            status=_subsection_status(failed=health_failed, has_data=has_health_data),
            reason_code=failed_section_reasons.get("health"),
            data=health,
        ),
    }

    if status == "partial":
        available_sections = [section for section, detail in subsections.items() if detail["status"] in {"ok", "limited"}]
        logger.info("financials_partial_success symbol=%s available_sections=%s", normalized_symbol, available_sections)

    payload = {
        "symbol": normalized_symbol,
        "companyName": _company_name(annual_income, quarterly_income, earnings_rows),
        "status": status,
        "summary": {
            "revenueTtm": _sum_latest(quarterly, "revenue"),
            "netIncomeTtm": _sum_latest(quarterly, "netIncome"),
            "epsTtm": _sum_latest(quarterly, "eps"),
            "trailingPE": trailing_pe,
            "forwardPE": forward_pe,
            "grossMargin": _latest_value(quarterly, "grossMargin") or _latest_value(annual, "grossMargin"),
            "operatingMargin": _latest_value(quarterly, "operatingMargin") or _latest_value(annual, "operatingMargin"),
            "nextEarningsDate": _next_earnings_date(earnings_rows),
            "latestQuarter": latest_quarter,
            "freeCashFlowTtm": _sum_latest(quarterly, "freeCashFlow"),
            "operatingCashFlowTtm": _sum_latest(quarterly, "operatingCashFlow"),
            "debtToEquity": health.get("debtToEquity"),
            "currentRatio": health.get("currentRatio"),
            "assetRatio": health.get("assetRatio"),
        },
        "annual": annual,
        "quarterly": quarterly,
        "earnings": earnings,
        "forecasts": forecasts,
        "health": health,
        "sections": sections,
        "subsections": subsections,
        "updatedAt": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
    }
    return _cache_set(cache_key, payload)
