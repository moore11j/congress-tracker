from __future__ import annotations

import logging
import os
import time
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from math import isfinite
from typing import Any

import requests
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.clients.fmp import FMP_BASE_URL, FMPClientError, fetch_company_screener
from app.models import Event, FundamentalsCache
from app.utils.symbols import normalize_symbol

logger = logging.getLogger(__name__)

PROVIDER = "fmp"

# Unit convention for screener fundamentals:
# margins and growth fields are stored as percentages, while valuation,
# leverage, and liquidity fields are plain ratios. Currency fields are USD.
FUNDAMENTAL_FIELD_NAMES = (
    "trailing_pe",
    "forward_pe",
    "price_to_sales",
    "ev_to_ebitda",
    "gross_margin",
    "operating_margin",
    "operating_margin_expansion",
    "net_margin",
    "roe",
    "roic",
    "revenue_growth",
    "eps_growth",
    "ebitda_growth",
    "free_cash_flow",
    "fcf_yield",
    "fcf_margin",
    "fcf_growth",
    "debt_to_equity",
    "current_ratio",
    "net_debt_to_ebitda",
    "eps_ttm",
    "earnings_yield",
)

SCREENER_ROW_FIELD_MAP = {
    "price_to_sales": "price_sales",
    "ev_to_ebitda": "ev_ebitda",
    "free_cash_flow": "fcf",
    "debt_to_equity": "debt_equity",
    "net_debt_to_ebitda": "net_debt_ebitda",
}

CACHE_ROW_FIELDS = (
    "company_name",
    "sector",
    "industry",
    "country",
    "exchange",
    "market_cap",
    "price",
    "volume",
    "avg_volume",
    "beta",
    "dividend_yield",
    *FUNDAMENTAL_FIELD_NAMES,
)

IDENTITY_CACHE_FIELDS = {"company_name", "sector", "industry", "country", "exchange"}


@dataclass(frozen=True)
class FundamentalsFetchResult:
    symbol: str
    values: dict[str, Any]
    status: str = "ok"
    error: str | None = None


def _api_key() -> str:
    key = os.getenv("FMP_API_KEY", "").strip()
    if not key:
        raise FMPClientError("Missing FMP_API_KEY")
    return key


def _request_rows(endpoint: str, *, params: dict[str, Any] | None = None, timeout_s: int = 30) -> list[dict[str, Any]]:
    request_params = {"apikey": _api_key(), **(params or {})}
    try:
        response = requests.get(f"{FMP_BASE_URL}/{endpoint}", params=request_params, timeout=timeout_s)
    except requests.RequestException as exc:
        raise FMPClientError(f"FMP fundamentals request failed endpoint={endpoint}: {exc}") from exc

    if response.status_code in {400, 404}:
        return []
    if response.status_code in {401, 403}:
        raise FMPClientError(f"FMP fundamentals auth failed endpoint={endpoint} status={response.status_code}")
    if response.status_code == 429:
        raise FMPClientError("FMP fundamentals rate-limited (429)")
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        raise FMPClientError(f"FMP fundamentals error endpoint={endpoint} status={response.status_code}") from exc

    payload = response.json()
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if isinstance(payload, dict):
        rows = payload.get("data")
        if isinstance(rows, list):
            return [row for row in rows if isinstance(row, dict)]
        return [payload] if payload else []
    return []


def _request_fundamentals_diagnostic(endpoint: str, *, params: dict[str, Any] | None = None, timeout_s: int = 30) -> dict[str, Any]:
    api_key_present = bool(os.getenv("FMP_API_KEY", "").strip())
    if not api_key_present:
        return {
            "endpoint": endpoint,
            "api_key_present": False,
            "status_code": None,
            "payload_shape": "unavailable",
            "row_count": 0,
            "first_row_keys": [],
            "error": "missing_api_key",
        }
    request_params = {"apikey": _api_key(), **(params or {})}
    try:
        response = requests.get(f"{FMP_BASE_URL}/{endpoint}", params=request_params, timeout=timeout_s)
    except requests.RequestException as exc:
        return {
            "endpoint": endpoint,
            "api_key_present": True,
            "status_code": None,
            "payload_shape": "unavailable",
            "row_count": 0,
            "first_row_keys": [],
            "error": str(exc)[:240],
        }
    try:
        payload = response.json()
    except ValueError:
        payload = None
    if isinstance(payload, list):
        rows = [row for row in payload if isinstance(row, dict)]
        payload_shape = "array"
    elif isinstance(payload, dict):
        data = payload.get("data")
        rows = [row for row in data if isinstance(row, dict)] if isinstance(data, list) else ([payload] if payload else [])
        payload_shape = "object"
    else:
        rows = []
        payload_shape = type(payload).__name__
    first_row = rows[0] if rows else {}
    return {
        "endpoint": endpoint,
        "api_key_present": True,
        "status_code": response.status_code,
        "payload_shape": payload_shape,
        "row_count": len(rows),
        "first_row_keys": sorted(str(key) for key in first_row.keys())[:80],
        "error": None if response.ok else f"http_{response.status_code}",
    }


def fundamentals_source_diagnostics(symbol: str) -> dict[str, Any]:
    normalized_symbol = normalize_symbol(symbol)
    api_key_present = bool(os.getenv("FMP_API_KEY", "").strip())
    if not normalized_symbol:
        return {"symbol": symbol, "status": "invalid_symbol", "api_key_present": api_key_present}
    endpoints = {
        "key_metrics_ttm": ("key-metrics-ttm", {"symbol": normalized_symbol}),
        "income_statement_growth": ("income-statement-growth", {"symbol": normalized_symbol, "limit": 1}),
        "ratios_ttm": ("ratios-ttm", {"symbol": normalized_symbol}),
        "ratios": ("ratios", {"symbol": normalized_symbol, "limit": 2}),
    }
    diagnostics = {
        key: _request_fundamentals_diagnostic(endpoint, params=params)
        for key, (endpoint, params) in endpoints.items()
    }
    if not api_key_present:
        return {
            "symbol": normalized_symbol,
            "status": "missing_api_key",
            "api_key_present": False,
            "endpoints": diagnostics,
            "missing_fields": list(FUNDAMENTALS_SUMMARY_METRIC_KEYS),
        }
    metrics_row = next(iter(_request_rows("key-metrics-ttm", params={"symbol": normalized_symbol})), {})
    growth_row = next(iter(_request_rows("income-statement-growth", params={"symbol": normalized_symbol, "limit": 1})), {})
    ratios_row = next(iter(_request_rows("ratios-ttm", params={"symbol": normalized_symbol})), {})
    ratios_history_rows = _request_rows("ratios", params={"symbol": normalized_symbol, "limit": 2})
    values = normalize_fundamentals_payload(
        symbol=normalized_symbol,
        ratios_row=ratios_row,
        metrics_row=metrics_row,
        growth_row=growth_row,
        ratios_history_rows=ratios_history_rows,
    )
    diagnostic_fields = {
        "revenue_growth": "revenue_growth",
        "return_on_equity": "roe",
        "ev_to_ebitda": "ev_to_ebitda",
        "operating_margin_expansion": "operating_margin_expansion",
        "net_debt_to_ebitda": "net_debt_to_ebitda",
    }
    missing = [name for name, cache_key in diagnostic_fields.items() if values.get(cache_key) is None]
    logger.info(
        "fundamentals diagnostics symbol=%s api_key_present=%s missing_fields=%s endpoint_statuses=%s",
        normalized_symbol,
        bool(os.getenv("FMP_API_KEY", "").strip()),
        missing,
        {key: item.get("status_code") for key, item in diagnostics.items()},
    )
    return {
        "symbol": normalized_symbol,
        "status": "ok",
        "api_key_present": bool(os.getenv("FMP_API_KEY", "").strip()),
        "endpoints": diagnostics,
        "missing_fields": missing,
    }


def _number(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        parsed = float(value)
    elif isinstance(value, str) and value.strip():
        cleaned = value.replace("$", "").replace("%", "").replace(",", "").strip()
        if not cleaned:
            return None
        try:
            parsed = float(cleaned)
        except ValueError:
            return None
    else:
        return None
    return parsed if isfinite(parsed) else None


def _first_number(*values: Any) -> float | None:
    for value in values:
        parsed = _number(value)
        if parsed is not None:
            return parsed
    return None


def _text(*values: Any) -> str | None:
    for value in values:
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _percent(value: Any) -> float | None:
    parsed = _number(value)
    if parsed is None:
        return None
    return parsed * 100 if abs(parsed) <= 1 else parsed


def _ratio_from_percentish(value: Any) -> float | None:
    parsed = _number(value)
    if parsed is None:
        return None
    return parsed if abs(parsed) <= 1 else parsed / 100


def _first_percent(*values: Any) -> float | None:
    for value in values:
        parsed = _percent(value)
        if parsed is not None:
            return parsed
    return None


def _profitability_percent(value: Any) -> float | None:
    parsed = _number(value)
    if parsed is None:
        return None
    return parsed * 100 if abs(parsed) <= 5 else parsed


def _first_profitability_percent(*values: Any) -> float | None:
    for value in values:
        parsed = _profitability_percent(value)
        if parsed is not None:
            return parsed
    return None


def _first_ratio_from_percentish(*values: Any) -> float | None:
    for value in values:
        parsed = _ratio_from_percentish(value)
        if parsed is not None:
            return parsed
    return None


def _margin_expansion_points(row: dict[str, Any]) -> float | None:
    direct = _first_ratio_from_percentish(
        row.get("operatingMarginExpansion"),
        row.get("operatingMarginExpansionTTM"),
        row.get("operatingMarginChange"),
        row.get("operatingMarginChangeTTM"),
        row.get("growthOperatingMargin"),
        row.get("growthOperatingIncomeRatio"),
        row.get("operatingMarginGrowth"),
        row.get("operatingProfitMarginGrowth"),
        row.get("operatingIncomeRatioGrowth"),
    )
    if direct is not None:
        return direct * 100
    current = _first_percent(
        row.get("operatingMargin"),
        row.get("operatingProfitMargin"),
        row.get("operatingIncomeRatio"),
        row.get("currentOperatingMargin"),
    )
    prior = _first_percent(
        row.get("priorOperatingMargin"),
        row.get("previousOperatingMargin"),
        row.get("previousOperatingProfitMargin"),
        row.get("priorOperatingIncomeRatio"),
    )
    if current is not None and prior is not None:
        return current - prior
    return None


def _computed_operating_margin_expansion_points(rows: list[dict[str, Any]] | None) -> float | None:
    if not rows or len(rows) < 2:
        return None
    margins: list[float] = []
    for row in rows[:2]:
        revenue = _first_number(row.get("revenue"), row.get("totalRevenue"))
        operating_income = _first_number(row.get("operatingIncome"), row.get("operating_income"))
        if revenue is None or revenue == 0 or operating_income is None:
            return None
        margins.append(operating_income / revenue)
    return (margins[0] - margins[1]) * 100 if len(margins) == 2 else None


def _operating_margin_expansion_from_ratios(
    ratios_row: dict[str, Any],
    ratios_history_rows: list[dict[str, Any]] | None,
) -> float | None:
    current = _first_ratio_from_percentish(
        ratios_row.get("operatingProfitMarginTTM"),
        ratios_row.get("operatingMarginTTM"),
    )
    if current is None:
        return None
    for row in ratios_history_rows or []:
        prior = _first_ratio_from_percentish(
            row.get("operatingProfitMargin"),
            row.get("operatingMargin"),
        )
        if prior is not None:
            return (current - prior) * 100
    return None


def _date_value(*values: Any):
    for value in values:
        if isinstance(value, str) and len(value) >= 10:
            try:
                return datetime.strptime(value[:10], "%Y-%m-%d").date()
            except ValueError:
                continue
    return None


def normalize_fundamentals_payload(
    *,
    symbol: str,
    screener_row: dict[str, Any] | None = None,
    quote_row: dict[str, Any] | None = None,
    ratios_row: dict[str, Any] | None = None,
    metrics_row: dict[str, Any] | None = None,
    growth_row: dict[str, Any] | None = None,
    ratios_history_rows: list[dict[str, Any]] | None = None,
    income_statement_rows: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    screener_row = screener_row or {}
    quote_row = quote_row or {}
    ratios_row = ratios_row or {}
    metrics_row = metrics_row or {}
    growth_row = growth_row or {}
    ratios_history_rows = ratios_history_rows or []
    income_statement_rows = income_statement_rows or []
    normalized_symbol = normalize_symbol(
        symbol
        or screener_row.get("symbol")
        or quote_row.get("symbol")
        or ratios_row.get("symbol")
        or metrics_row.get("symbol")
    )
    if not normalized_symbol:
        raise ValueError("symbol is required")
    ebitda_ttm = _first_number(
        metrics_row.get("ebitdaTTM"),
        metrics_row.get("EBITDATTM"),
        metrics_row.get("ebitda"),
    )
    net_debt_to_ebitda = _first_number(
        screener_row.get("netDebtToEBITDA"),
        metrics_row.get("netDebtToEBITDATTM"),
        metrics_row.get("netDebtToEbitdaTTM"),
        metrics_row.get("netDebtToEBITDA"),
    )
    if ebitda_ttm is not None and ebitda_ttm <= 0:
        net_debt_to_ebitda = None

    return {
        "symbol": normalized_symbol,
        "provider": PROVIDER,
        "fetched_at": datetime.now(timezone.utc),
        "period_date": _date_value(
            ratios_row.get("date"),
            metrics_row.get("date"),
            growth_row.get("date"),
            screener_row.get("date"),
        ),
        "status": "ok",
        "error": None,
        "company_name": _text(screener_row.get("companyName"), quote_row.get("name"), quote_row.get("companyName")),
        "sector": _text(screener_row.get("sector"), quote_row.get("sector")),
        "industry": _text(screener_row.get("industry"), quote_row.get("industry")),
        "country": _text(screener_row.get("country"), quote_row.get("country")),
        "exchange": _text(screener_row.get("exchangeShortName"), quote_row.get("exchangeShortName"), quote_row.get("exchange")),
        "market_cap": _first_number(screener_row.get("marketCap"), quote_row.get("marketCap")),
        "price": _first_number(screener_row.get("price"), quote_row.get("price")),
        "volume": _first_number(screener_row.get("volume"), quote_row.get("volume")),
        "avg_volume": _first_number(screener_row.get("avgVolume"), quote_row.get("avgVolume"), quote_row.get("averageVolume")),
        "beta": _first_number(screener_row.get("beta"), quote_row.get("beta")),
        "dividend_yield": _first_percent(screener_row.get("dividendYield"), quote_row.get("dividendYield")),
        "trailing_pe": _first_number(
            screener_row.get("pe"),
            screener_row.get("trailingPE"),
            ratios_row.get("priceToEarningsRatioTTM"),
            ratios_row.get("peRatioTTM"),
            metrics_row.get("peRatioTTM"),
        ),
        "forward_pe": _first_number(screener_row.get("forwardPE"), quote_row.get("forwardPE"), metrics_row.get("forwardPE")),
        "price_to_sales": _first_number(
            screener_row.get("priceToSalesRatio"),
            screener_row.get("priceToSales"),
            ratios_row.get("priceToSalesRatioTTM"),
            ratios_row.get("priceToSalesTTM"),
            metrics_row.get("priceToSalesRatioTTM"),
        ),
        "ev_to_ebitda": _first_number(
            screener_row.get("enterpriseValueOverEBITDA"),
            screener_row.get("evToEbitda"),
            ratios_row.get("enterpriseValueMultipleTTM"),
            metrics_row.get("enterpriseValueOverEBITDATTM"),
            metrics_row.get("evToEBITDATTM"),
            metrics_row.get("evToEbitdaTTM"),
            metrics_row.get("evToEBITDA"),
        ),
        "gross_margin": _first_percent(screener_row.get("grossMargin"), ratios_row.get("grossProfitMarginTTM"), ratios_row.get("grossProfitMargin")),
        "operating_margin": _first_percent(
            screener_row.get("operatingMargin"),
            ratios_row.get("operatingProfitMarginTTM"),
            ratios_row.get("operatingMarginTTM"),
        ),
        "operating_margin_expansion": _margin_expansion_points(growth_row)
        or _operating_margin_expansion_from_ratios(ratios_row, ratios_history_rows)
        or _computed_operating_margin_expansion_points(income_statement_rows),
        "net_margin": _first_percent(screener_row.get("netMargin"), ratios_row.get("netProfitMarginTTM"), ratios_row.get("netProfitMargin")),
        "roe": _first_profitability_percent(
            screener_row.get("returnOnEquity"),
            ratios_row.get("returnOnEquityTTM"),
            ratios_row.get("returnOnEquity"),
            ratios_row.get("roeTTM"),
            metrics_row.get("returnOnEquityTTM"),
            metrics_row.get("returnonequityTTM"),
            metrics_row.get("returnOnEquity"),
            metrics_row.get("roeTTM"),
        ),
        "roic": _first_percent(
            screener_row.get("returnOnInvestedCapital"),
            ratios_row.get("returnOnInvestedCapitalTTM"),
            metrics_row.get("roicTTM"),
        ),
        "revenue_growth": _first_percent(
            screener_row.get("revenueGrowth"),
            growth_row.get("revenueGrowth"),
            growth_row.get("growthRevenue"),
            growth_row.get("growth_revenue"),
        ),
        "eps_growth": _first_percent(screener_row.get("epsGrowth"), growth_row.get("epsgrowth"), growth_row.get("epsGrowth")),
        "ebitda_growth": _first_percent(screener_row.get("ebitdaGrowth"), growth_row.get("ebitdaGrowth"), growth_row.get("growthEBITDA")),
        "free_cash_flow": _first_number(screener_row.get("freeCashFlow"), metrics_row.get("freeCashFlowTTM"), metrics_row.get("freeCashFlow")),
        "fcf_yield": _first_percent(
            screener_row.get("freeCashFlowYield"),
            screener_row.get("fcfYield"),
            metrics_row.get("freeCashFlowYieldTTM"),
            metrics_row.get("freeCashFlowYield"),
            metrics_row.get("fcfYieldTTM"),
            metrics_row.get("fcfYield"),
        ),
        "fcf_margin": _first_percent(screener_row.get("freeCashFlowMargin"), metrics_row.get("freeCashFlowMarginTTM")),
        "fcf_growth": _first_percent(screener_row.get("freeCashFlowGrowth"), growth_row.get("freeCashFlowGrowth")),
        "debt_to_equity": _first_number(screener_row.get("debtToEquity"), ratios_row.get("debtEquityRatioTTM"), ratios_row.get("debtToEquityTTM")),
        "current_ratio": _first_number(screener_row.get("currentRatio"), ratios_row.get("currentRatioTTM"), ratios_row.get("currentRatio")),
        "net_debt_to_ebitda": net_debt_to_ebitda,
        "eps_ttm": _first_number(screener_row.get("epsTTM"), screener_row.get("eps"), quote_row.get("eps"), metrics_row.get("netIncomePerShareTTM")),
        "earnings_yield": _first_percent(screener_row.get("earningsYield"), metrics_row.get("earningsYieldTTM")),
    }


def fetch_fundamentals_for_symbol(symbol: str) -> FundamentalsFetchResult:
    normalized_symbol = normalize_symbol(symbol)
    if not normalized_symbol:
        return FundamentalsFetchResult(symbol=symbol, values={}, status="failed", error="invalid_symbol")

    try:
        screener_row = None
        for row in fetch_company_screener(filters={"symbol": normalized_symbol}, limit=10):
            if normalize_symbol(row.get("symbol")) == normalized_symbol:
                screener_row = row
                break
        quote_row = next(iter(_request_rows("historical-price-eod/light", params={"symbol": normalized_symbol})), {})
        ratios_row = next(iter(_request_rows("ratios-ttm", params={"symbol": normalized_symbol})), {})
        metrics_row = next(iter(_request_rows("key-metrics-ttm", params={"symbol": normalized_symbol})), {})
        growth_row = next(iter(_request_rows("income-statement-growth", params={"symbol": normalized_symbol, "limit": 1})), {})
        ratios_history_rows = _request_rows("ratios", params={"symbol": normalized_symbol, "limit": 2})
        income_statement_rows = _request_rows("income-statement", params={"symbol": normalized_symbol, "limit": 2})
        values = normalize_fundamentals_payload(
            symbol=normalized_symbol,
            screener_row=screener_row,
            quote_row=quote_row,
            ratios_row=ratios_row,
            metrics_row=metrics_row,
            growth_row=growth_row,
            ratios_history_rows=ratios_history_rows,
            income_statement_rows=income_statement_rows,
        )
        return FundamentalsFetchResult(symbol=normalized_symbol, values=values)
    except Exception as exc:
        logger.warning("fundamentals fetch failed symbol=%s error=%s", normalized_symbol, exc)
        return FundamentalsFetchResult(symbol=normalized_symbol, values={}, status="failed", error=str(exc)[:500])


def fetch_screener_universe_fundamentals(*, limit: int) -> list[FundamentalsFetchResult]:
    results: list[FundamentalsFetchResult] = []
    for row in fetch_company_screener(filters=None, limit=limit):
        symbol = normalize_symbol(row.get("symbol"))
        if not symbol:
            continue
        try:
            values = normalize_fundamentals_payload(symbol=symbol, screener_row=row)
            results.append(FundamentalsFetchResult(symbol=symbol, values=values))
        except Exception as exc:
            results.append(FundamentalsFetchResult(symbol=symbol, values={}, status="failed", error=str(exc)[:500]))
    return results


def upsert_fundamentals_cache(db: Session, values: dict[str, Any]) -> bool:
    symbol = normalize_symbol(values.get("symbol"))
    if not symbol:
        return False
    provider = values.get("provider") or PROVIDER
    row = db.execute(
        select(FundamentalsCache).where(FundamentalsCache.symbol == symbol, FundamentalsCache.provider == provider)
    ).scalar_one_or_none()
    payload = {key: values.get(key) for key in ("fetched_at", "period_date", "status", "error", *CACHE_ROW_FIELDS)}
    payload["symbol"] = symbol
    payload["provider"] = provider
    if row is None:
        db.add(FundamentalsCache(**payload))
        return True
    for key, value in payload.items():
        if key in IDENTITY_CACHE_FIELDS and value is None and getattr(row, key, None) is not None:
            continue
        setattr(row, key, value)
    return True


def cache_row_to_screener_row(row: FundamentalsCache) -> dict[str, Any]:
    payload = {
        "symbol": row.symbol,
        "company_name": row.company_name or row.symbol,
        "sector": row.sector,
        "industry": row.industry,
        "country": row.country,
        "exchange": row.exchange,
        "market_cap": row.market_cap,
        "price": row.price,
        "volume": row.volume,
        "avg_volume": row.avg_volume,
        "rel_volume": None,
        "price_move_pct": None,
        "rsi": None,
        "macd_state": None,
        "trend_state": None,
        "beta": row.beta,
        "dividend_yield": row.dividend_yield,
    }
    for cache_field in FUNDAMENTAL_FIELD_NAMES:
        payload[SCREENER_ROW_FIELD_MAP.get(cache_field, cache_field)] = getattr(row, cache_field)
    return payload


def _row_number(row: FundamentalsCache, field: str) -> float | None:
    value = getattr(row, field, None)
    return _number(value)


def _metric_state(value: float | None, *, kind: str) -> str:
    if value is None:
        return "unavailable"
    if kind == "roe":
        if value > 15:
            return "bullish"
        if value < 10:
            return "bearish"
        return "neutral"
    if kind == "revenue_growth":
        if value > 0:
            return "bullish"
        if value < 0:
            return "bearish"
        return "neutral"
    if kind == "fcf_yield":
        if value > 0:
            return "bullish"
        if value < 0:
            return "bearish"
        return "neutral"
    if kind == "net_debt_to_ebitda":
        if value < 2:
            return "bullish"
        if value > 4:
            return "bearish"
        return "neutral"
    if kind == "operating_margin_expansion":
        if value > 0:
            return "bullish"
        if value < 0:
            return "bearish"
        return "neutral"
    return "neutral"


FUNDAMENTALS_SUMMARY_METRIC_KEYS: tuple[str, ...] = (
    "revenue_growth",
    "return_on_equity",
    "ev_to_ebitda",
    "operating_margin_expansion",
    "net_debt_to_ebitda",
)


def _format_percent(value: float | None) -> str:
    return "\u2014" if value is None else f"{value:.1f}%"


def _format_multiple(value: float | None) -> str:
    return "\u2014" if value is None else f"{value:.1f}x"


def _format_points(value: float | None) -> str:
    return "\u2014" if value is None else f"{value:+.1f} pts"


def _metric_payload(
    *,
    value: float | None,
    display: str,
    state: str,
    direction: str | None = None,
) -> dict[str, Any]:
    payload = {
        "value": value,
        "display": display,
        "state": state,
    }
    if direction is not None:
        payload["direction"] = direction
    return payload


def _direction_from_value(value: float | None) -> str:
    if value is None:
        return "unknown"
    if value > 0:
        return "rising"
    if value < 0:
        return "falling"
    return "flat"


def _headline_for_status(status: str) -> str:
    if status == "bullish":
        return "Fundamental strength"
    if status == "bearish":
        return "Fundamental pressure"
    if status == "mixed":
        return "Mixed fundamental profile"
    return "Fundamentals unavailable"


def fundamentals_summary_from_cache_row(
    row: FundamentalsCache | None,
    *,
    now: datetime | None = None,
    stale_days: int = 45,
) -> dict[str, Any]:
    if row is None:
        return unavailable_fundamentals_summary()

    revenue_growth = _row_number(row, "revenue_growth")
    roe = _row_number(row, "roe")
    ev_to_ebitda = _row_number(row, "ev_to_ebitda")
    operating_margin_expansion = _row_number(row, "operating_margin_expansion")
    net_debt_to_ebitda = _row_number(row, "net_debt_to_ebitda")

    metric_values = {
        "revenue_growth": revenue_growth,
        "return_on_equity": roe,
        "ev_to_ebitda": ev_to_ebitda,
        "operating_margin_expansion": operating_margin_expansion,
        "net_debt_to_ebitda": net_debt_to_ebitda,
    }
    metrics = {
        "revenue_growth": _metric_payload(
            value=revenue_growth / 100 if revenue_growth is not None else None,
            display=_format_percent(revenue_growth),
            state=_metric_state(revenue_growth, kind="revenue_growth"),
            direction=_direction_from_value(revenue_growth),
        ),
        "return_on_equity": _metric_payload(
            value=roe / 100 if roe is not None else None,
            display=_format_percent(roe),
            state=_metric_state(roe, kind="roe"),
        ),
        "ev_to_ebitda": _metric_payload(
            value=ev_to_ebitda,
            display=_format_multiple(ev_to_ebitda),
            state="neutral" if ev_to_ebitda is not None else "unavailable",
        ),
        "operating_margin_expansion": _metric_payload(
            value=operating_margin_expansion / 100 if operating_margin_expansion is not None else None,
            display=_format_points(operating_margin_expansion),
            state=_metric_state(operating_margin_expansion, kind="operating_margin_expansion"),
        ),
        "net_debt_to_ebitda": _metric_payload(
            value=net_debt_to_ebitda,
            display=_format_multiple(net_debt_to_ebitda),
            state=_metric_state(net_debt_to_ebitda, kind="net_debt_to_ebitda"),
        ),
    }
    missing_fields = [key for key, value in metric_values.items() if value is None]
    scored_states = [
        metric.get("state")
        for metric in metrics.values()
        if metric.get("state") in {"bullish", "neutral", "bearish"}
    ]
    score = sum(1 if state == "bullish" else -1 if state == "bearish" else 0 for state in scored_states)
    scored_count = len(scored_states)
    if scored_count < 3:
        status = "unavailable"
    elif score >= 2:
        status = "bullish"
    elif score <= -2:
        status = "bearish"
    else:
        status = "mixed"

    observed_now = now or datetime.now(timezone.utc)
    fetched_at = row.fetched_at
    if fetched_at is not None and fetched_at.tzinfo is None:
        fetched_at = fetched_at.replace(tzinfo=timezone.utc)
    freshness_days = (
        max((observed_now - fetched_at).days, 0)
        if isinstance(fetched_at, datetime)
        else None
    )
    stale = freshness_days is not None and freshness_days > max(1, int(stale_days))
    return {
        "symbol": row.symbol,
        "status": status,
        "headline": _headline_for_status(status),
        "as_of": row.period_date.isoformat() if row.period_date is not None else None,
        "updated_at": row.fetched_at.isoformat() if isinstance(row.fetched_at, datetime) else None,
        "freshness_days": freshness_days,
        "data_state": "stale" if stale else "fresh",
        "metrics": metrics,
        "data_quality": {
            "available": scored_count >= 3,
            "missing_fields": missing_fields,
            "scored_metric_count": scored_count,
        },
    }


def unavailable_fundamentals_summary(symbol: str | None = None) -> dict[str, Any]:
    normalized = normalize_symbol(symbol) if symbol else None
    metrics = {
        key: _metric_payload(value=None, display="\u2014", state="unavailable")
        for key in FUNDAMENTALS_SUMMARY_METRIC_KEYS
    }
    metrics["revenue_growth"]["direction"] = "unknown"
    return {
        "symbol": normalized,
        "status": "unavailable",
        "headline": "Fundamentals unavailable",
        "as_of": None,
        "updated_at": None,
        "freshness_days": None,
        "data_state": "unavailable",
        "metrics": metrics,
        "data_quality": {
            "available": False,
            "missing_fields": list(metrics.keys()),
            "scored_metric_count": 0,
        },
    }


def cached_screener_rows(
    db: Session,
    *,
    provider: str = PROVIDER,
    limit: int | None = None,
    filters: Mapping[str, Any] | None = None,
) -> list[dict[str, Any]]:
    query = (
        select(FundamentalsCache)
        .where(FundamentalsCache.provider == provider)
        .where(FundamentalsCache.status == "ok")
    )
    query = _apply_screener_cache_filters(query, filters or {})
    query = query.order_by(FundamentalsCache.market_cap.desc().nullslast(), FundamentalsCache.symbol.asc())
    if limit is not None:
        query = query.limit(max(1, int(limit)))
    return [cache_row_to_screener_row(row) for row in db.execute(query).scalars().all()]


def _apply_screener_cache_filters(query, filters: Mapping[str, Any]):
    for field in ("market_cap", "price", "beta", "dividend_yield"):
        query = _apply_cache_range_filter(
            query,
            getattr(FundamentalsCache, field),
            filters.get(f"{field}_min"),
            filters.get(f"{field}_max"),
        )
    query = _apply_cache_range_filter(query, FundamentalsCache.volume, filters.get("volume_min"), None)

    for field in ("sector", "industry", "country", "exchange"):
        query = _apply_cache_text_filter(query, getattr(FundamentalsCache, field), filters.get(field))

    for field in FUNDAMENTAL_FIELD_NAMES:
        query = _apply_cache_range_filter(
            query,
            getattr(FundamentalsCache, field),
            filters.get(f"{field}_min"),
            filters.get(f"{field}_max"),
        )

    return query


def _apply_cache_range_filter(query, column, minimum: Any, maximum: Any):
    min_value = _number(minimum)
    max_value = _number(maximum)
    if min_value is not None:
        query = query.where(column >= min_value)
    if max_value is not None:
        query = query.where(column <= max_value)
    return query


def _apply_cache_text_filter(query, column, value: Any):
    expected_values = _normalized_filter_values(value)
    if not expected_values:
        return query
    return query.where(func.lower(column).in_(sorted(expected_values)))


def _normalized_filter_values(value: Any) -> set[str]:
    if not isinstance(value, str) or not value.strip():
        return set()
    return {
        cleaned
        for part in value.split(",")
        if (cleaned := part.strip().lower()) and cleaned != "any"
    }


def cached_fundamentals_by_symbol(db: Session, symbols: list[str], *, provider: str = PROVIDER) -> dict[str, FundamentalsCache]:
    normalized = sorted({symbol for symbol in (normalize_symbol(item) for item in symbols) if symbol})
    if not normalized:
        return {}
    rows = db.execute(
        select(FundamentalsCache)
        .where(FundamentalsCache.provider == provider)
        .where(FundamentalsCache.status == "ok")
        .where(FundamentalsCache.symbol.in_(normalized))
    ).scalars().all()
    return {row.symbol: row for row in rows}


def stale_or_missing_symbols(
    db: Session,
    symbols: list[str],
    *,
    stale_days: int | None,
    provider: str = PROVIDER,
) -> tuple[list[str], int]:
    normalized = list(dict.fromkeys(symbol for symbol in (normalize_symbol(item) for item in symbols) if symbol))
    if stale_days is None:
        return normalized, 0
    cutoff = datetime.now(timezone.utc) - timedelta(days=max(0, stale_days))
    rows = cached_fundamentals_by_symbol(db, normalized, provider=provider)
    stale: list[str] = []
    fresh = 0
    for symbol in normalized:
        row = rows.get(symbol)
        fetched_at = row.fetched_at if row is not None else None
        if fetched_at is None:
            stale.append(symbol)
            continue
        if fetched_at.tzinfo is None:
            fetched_at = fetched_at.replace(tzinfo=timezone.utc)
        if fetched_at < cutoff:
            stale.append(symbol)
        else:
            fresh += 1
    return stale, fresh


def recent_screener_universe_symbols(db: Session, *, limit: int) -> list[str]:
    symbols = [
        symbol
        for symbol in db.execute(
            select(func.upper(Event.symbol))
            .where(Event.symbol.is_not(None))
            .group_by(func.upper(Event.symbol))
            .order_by(func.max(func.coalesce(Event.event_date, Event.ts)).desc())
            .limit(max(1, int(limit)))
        ).scalars().all()
        if symbol
    ]
    return list(dict.fromkeys(symbols))


def sleep_between_provider_calls(seconds: float) -> None:
    if seconds > 0:
        time.sleep(seconds)
