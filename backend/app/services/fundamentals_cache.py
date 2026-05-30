from __future__ import annotations

import logging
import os
import time
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
    "net_margin",
    "roe",
    "roic",
    "revenue_growth",
    "eps_growth",
    "ebitda_growth",
    "free_cash_flow",
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


def _first_percent(*values: Any) -> float | None:
    for value in values:
        parsed = _percent(value)
        if parsed is not None:
            return parsed
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
) -> dict[str, Any]:
    screener_row = screener_row or {}
    quote_row = quote_row or {}
    ratios_row = ratios_row or {}
    metrics_row = metrics_row or {}
    growth_row = growth_row or {}
    normalized_symbol = normalize_symbol(
        symbol
        or screener_row.get("symbol")
        or quote_row.get("symbol")
        or ratios_row.get("symbol")
        or metrics_row.get("symbol")
    )
    if not normalized_symbol:
        raise ValueError("symbol is required")

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
            metrics_row.get("evToEbitdaTTM"),
        ),
        "gross_margin": _first_percent(screener_row.get("grossMargin"), ratios_row.get("grossProfitMarginTTM"), ratios_row.get("grossProfitMargin")),
        "operating_margin": _first_percent(
            screener_row.get("operatingMargin"),
            ratios_row.get("operatingProfitMarginTTM"),
            ratios_row.get("operatingMarginTTM"),
        ),
        "net_margin": _first_percent(screener_row.get("netMargin"), ratios_row.get("netProfitMarginTTM"), ratios_row.get("netProfitMargin")),
        "roe": _first_percent(screener_row.get("returnOnEquity"), ratios_row.get("returnOnEquityTTM"), ratios_row.get("roeTTM")),
        "roic": _first_percent(
            screener_row.get("returnOnInvestedCapital"),
            ratios_row.get("returnOnInvestedCapitalTTM"),
            metrics_row.get("roicTTM"),
        ),
        "revenue_growth": _first_percent(screener_row.get("revenueGrowth"), growth_row.get("revenueGrowth"), growth_row.get("growthRevenue")),
        "eps_growth": _first_percent(screener_row.get("epsGrowth"), growth_row.get("epsgrowth"), growth_row.get("epsGrowth")),
        "ebitda_growth": _first_percent(screener_row.get("ebitdaGrowth"), growth_row.get("ebitdaGrowth"), growth_row.get("growthEBITDA")),
        "free_cash_flow": _first_number(screener_row.get("freeCashFlow"), metrics_row.get("freeCashFlowTTM"), metrics_row.get("freeCashFlow")),
        "fcf_margin": _first_percent(screener_row.get("freeCashFlowMargin"), metrics_row.get("freeCashFlowMarginTTM")),
        "fcf_growth": _first_percent(screener_row.get("freeCashFlowGrowth"), growth_row.get("freeCashFlowGrowth")),
        "debt_to_equity": _first_number(screener_row.get("debtToEquity"), ratios_row.get("debtEquityRatioTTM"), ratios_row.get("debtToEquityTTM")),
        "current_ratio": _first_number(screener_row.get("currentRatio"), ratios_row.get("currentRatioTTM"), ratios_row.get("currentRatio")),
        "net_debt_to_ebitda": _first_number(screener_row.get("netDebtToEBITDA"), metrics_row.get("netDebtToEBITDATTM")),
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
        quote_row = next(iter(_request_rows("quote", params={"symbol": normalized_symbol})), {})
        ratios_row = next(iter(_request_rows("ratios-ttm", params={"symbol": normalized_symbol})), {})
        metrics_row = next(iter(_request_rows("key-metrics-ttm", params={"symbol": normalized_symbol})), {})
        growth_row = next(iter(_request_rows("financial-growth", params={"symbol": normalized_symbol, "limit": 1})), {})
        values = normalize_fundamentals_payload(
            symbol=normalized_symbol,
            screener_row=screener_row,
            quote_row=quote_row,
            ratios_row=ratios_row,
            metrics_row=metrics_row,
            growth_row=growth_row,
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


def cached_screener_rows(db: Session, *, provider: str = PROVIDER, limit: int | None = None) -> list[dict[str, Any]]:
    query = (
        select(FundamentalsCache)
        .where(FundamentalsCache.provider == provider)
        .where(FundamentalsCache.status == "ok")
        .order_by(FundamentalsCache.market_cap.desc().nullslast(), FundamentalsCache.symbol.asc())
    )
    if limit is not None:
        query = query.limit(max(1, int(limit)))
    return [cache_row_to_screener_row(row) for row in db.execute(query).scalars().all()]


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
