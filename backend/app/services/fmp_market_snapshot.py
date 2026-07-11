from __future__ import annotations

import csv
import io
import logging
import os
import time
from datetime import date, datetime, timedelta, timezone
from threading import Lock
from typing import Any

import requests

from app.clients.fmp import FMP_BASE_URL
from app.services.data_enrichment_queue import enqueue_data_enrichment_job
from app.services.provider_usage import (
    ProviderUnavailable,
    ensure_fmp_live_allowed,
    fallback_payload,
    reason_from_exception,
    record_cache_hit,
    record_cache_miss,
    record_fallback,
    record_provider_response,
)

logger = logging.getLogger(__name__)

MACRO_SNAPSHOT_TTL_SECONDS = 15 * 60
MACRO_SNAPSHOT_STALE_TTL_SECONDS = 24 * 60 * 60
PROVIDER_TIMEOUT_SECONDS = 8
PUBLIC_MACRO_CSV_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv"
INDEX_TARGETS = (
    {
        "label": "S&P 500",
        "symbols": ("^GSPC", "GSPC"),
        "names": ("S&P 500", "S&P500", "US 500"),
        "proxy_symbol": "SPY",
        "proxy_label": "S&P 500 proxy",
    },
    {
        "label": "Nasdaq",
        "symbols": ("^IXIC", "IXIC", "^NDX", "NDX"),
        "names": ("Nasdaq Composite", "NASDAQ Composite", "Nasdaq 100", "NASDAQ 100"),
        "proxy_symbol": "QQQ",
        "proxy_label": "Nasdaq proxy",
    },
    {
        "label": "Dow",
        "symbols": ("^DJI", "DJI", "^DJI30"),
        "names": ("Dow Jones Industrial Average", "Dow Jones", "DJIA"),
        "proxy_symbol": "DIA",
        "proxy_label": "Dow proxy",
    },
    {
        "label": "Russell 2000",
        "symbols": ("^RUT", "RUT"),
        "names": ("Russell 2000",),
        "proxy_symbol": "IWM",
        "proxy_label": "Russell 2000 proxy",
    },
    {
        "label": "VIX",
        "symbols": ("^VIX", "VIX"),
        "names": ("CBOE Volatility Index", "VIX"),
        "proxy_symbol": "VIXY",
        "proxy_label": "VIX proxy",
    },
)
WORLD_INDEX_TARGETS = (
    {
        "label": "Canada TSX",
        "symbols": (
            "^GSPTSE",
            "%5EGSPTSE",
            "GSPTSE",
            ".GSPTSE",
            "^TSX",
            "TSX",
            "^SPTSX",
            "SPTSX",
            "^TXCX",
            "TXCX",
            "CADINDEX",
            "S&P/TSX Composite",
            "S&P TSX Composite",
            "Canada TSX",
        ),
        "names": ("S&P/TSX Composite", "S&P TSX Composite", "TSX Composite", "Canada TSX"),
        "proxy_symbols": ("XIC.TO", "XIU.TO", "EWC"),
        "proxy_label": "Canada TSX",
    },
    {
        "label": "FTSE 100",
        "symbols": ("^FTSE", "FTSE"),
        "names": ("FTSE 100",),
    },
    {
        "label": "DAX",
        "symbols": ("^GDAXI", "GDAXI", ".GDAXI", "DAX", "DAX40", "GER40", "Germany 40"),
        "names": ("DAX", "Germany 40"),
    },
    {
        "label": "Nikkei 225",
        "symbols": ("^N225", "N225", "NI225"),
        "names": ("Nikkei 225", "Nikkei"),
    },
    {
        "label": "Hang Seng",
        "symbols": ("^HSI", "HSI"),
        "names": ("Hang Seng",),
    },
)
SECTOR_SNAPSHOT_ORDER = (
    "Basic Materials",
    "Communication Services",
    "Consumer Cyclical",
    "Consumer Defensive",
    "Energy",
    "Financial Services",
    "Healthcare",
    "Industrials",
    "Real Estate",
    "Technology",
    "Utilities",
)
SECTOR_NAME_ALIASES = {
    "basic materials": "Basic Materials",
    "materials": "Basic Materials",
    "communication services": "Communication Services",
    "communications": "Communication Services",
    "consumer cyclical": "Consumer Cyclical",
    "consumer discretionary": "Consumer Cyclical",
    "consumer defensive": "Consumer Defensive",
    "consumer staples": "Consumer Defensive",
    "energy": "Energy",
    "financial services": "Financial Services",
    "financials": "Financial Services",
    "health care": "Healthcare",
    "healthcare": "Healthcare",
    "industrials": "Industrials",
    "real estate": "Real Estate",
    "technology": "Technology",
    "utilities": "Utilities",
}
TREASURY_FIELD_ALIASES = {
    "3M Treasury": ("month3", "3M", "3m", "3Month", "3month", "month_3"),
    "2Y Treasury": ("year2", "2Y", "2y", "2Year", "2year", "year_2"),
    "5Y Treasury": ("year5", "5Y", "5y", "5Year", "5year", "year_5"),
    "10Y Treasury": ("year10", "10Y", "10y", "10Year", "10year", "year_10"),
    "30Y Treasury": ("year30", "30Y", "30y", "30Year", "30year", "year_30"),
}
FED_OVERNIGHT_RATE_CANDIDATES = (
    "federalFunds",
    "federal funds rate",
    "Federal Funds Rate",
    "effective federal funds rate",
    "Effective Federal Funds Rate",
)
CORE_CPI_CANDIDATES = (
    "Core CPI YoY",
    "Core CPI Year over Year",
    "Core Consumer Price Index YoY",
    "Core Inflation Rate",
    "core CPI",
    "Core CPI",
    "core cpi",
    "core_cpi",
    "core cpi yoy",
    "Core Consumer Price Index",
    "core inflation",
    "Core Inflation",
    "core inflation rate",
    "core consumer prices",
    "Core Consumer Prices",
    "inflation core",
    "Inflation Core",
    "consumer price index less food and energy",
    "Consumer Price Index Less Food and Energy",
    "Consumer Price Index excluding food and energy",
    "Consumer Price Index Excluding Food and Energy",
    "CPI less food and energy",
    "cpi less food and energy",
)
UNEMPLOYMENT_RATE_CANDIDATES = ("unemployment rate", "unemploymentRate", "unemployment")
DEBT_TO_GDP_CANDIDATES = (
    "debt to gdp",
    "Debt to GDP",
    "Debt/GDP",
    "US Debt/GDP",
    "debt-to-gdp",
    "Debt-to-GDP",
    "federal debt to gdp",
    "Federal Debt to GDP",
    "public debt to gdp",
    "Public Debt to GDP",
    "government debt to gdp",
    "Government Debt to GDP",
    "Federal Debt as Percent of GDP",
    "Total Public Debt as Percent of GDP",
)
FEDERAL_DEBT_CANDIDATES = (
    "federal debt",
    "Federal Debt",
    "gross federal debt",
    "Gross Federal Debt",
    "public debt",
    "Public Debt",
    "government debt",
    "Government Debt",
    "total public debt",
    "Total Public Debt",
    "total public debt outstanding",
    "Total Public Debt Outstanding",
)
NOMINAL_GDP_CANDIDATES = (
    "nominal GDP",
    "Nominal GDP",
    "GDP",
    "gross domestic product",
    "Gross Domestic Product",
)
PUBLIC_CORE_CPI_INDEX_SERIES = (
    {"id": "CPILFESL", "label": "Core Consumer Price Index"},
    {"id": "CUSR0000SA0L1E", "label": "CPI less food and energy"},
)
PUBLIC_DEBT_TO_GDP_RATIO_SERIES = (
    {"id": "GFDEGDQ188S", "label": "Federal Debt to GDP"},
)
PUBLIC_FEDERAL_DEBT_SERIES = (
    {"id": "GFDEBTN", "label": "Total Public Debt", "scale": 1_000_000.0},
)
PUBLIC_NOMINAL_GDP_SERIES = (
    {"id": "GDP", "label": "Nominal GDP", "scale": 1_000_000_000.0},
)
RETAIL_SALES_GROWTH_REQUESTS = (
    {
        "change_label": "YoY",
        "candidates": (
            "retail sales yoy",
            "Retail Sales YoY",
            "retail sales year over year",
            "Retail Sales Year Over Year",
        ),
    },
    {
        "change_label": "MoM",
        "candidates": (
            "retail sales mom",
            "Retail Sales MoM",
            "retail sales month over month",
            "Retail Sales Month Over Month",
        ),
    },
)
RETAIL_SALES_LEVEL_CANDIDATES = ("retail sales", "Retail Sales", "retailSales")
ECONOMIC_SNAPSHOT_ORDER = (
    "Fed Overnight Rate",
    "Core CPI",
    "Unemployment",
    "Debt/GDP",
    "Retail Sales",
)
COMMODITY_TARGETS = (
    {"label": "Gold", "symbols": ("GCUSD", "ZGUSD", "XAUUSD", "GC=F"), "unit_label": "USD"},
    {"label": "Silver", "symbols": ("SIUSD", "ZIUSD", "XAGUSD", "SI=F"), "unit_label": "USD"},
    {"label": "Copper", "symbols": ("HGUSD", "HGUSD.CMX", "HG=F", "COPPER", "Copper", "copper", "HG", "HGUUSD", "XCUUSD"), "unit_label": "USD"},
    {"label": "Brent Crude", "symbols": ("BZUSD", "BZ=F"), "unit_label": "USD"},
    {"label": "Wheat", "symbols": ("ZWUSD", "KEUSD", "WHEATUSD", "ZW=F", "WHEAT"), "unit_label": "USD"},
)
CURRENCY_TARGETS = (
    {"label": "USD/CAD", "symbols": ("USDCAD", "USDCAD=X"), "unit_label": "rate"},
    {"label": "EUR/USD", "symbols": ("EURUSD", "EURUSD=X"), "unit_label": "rate"},
    {"label": "GBP/USD", "symbols": ("GBPUSD", "GBPUSD=X"), "unit_label": "rate"},
    {"label": "USD/JPY", "symbols": ("USDJPY", "USDJPY=X"), "unit_label": "rate"},
    {"label": "EUR/CAD", "symbols": ("EURCAD", "EURCAD=X"), "unit_label": "rate"},
)
CRYPTO_TARGETS = (
    {"label": "BTC/USD", "symbols": ("BTCUSD", "BTCUSD=X"), "unit_label": "USD"},
    {"label": "ETH/USD", "symbols": ("ETHUSD", "ETHUSD=X"), "unit_label": "USD"},
    {"label": "SOL/USD", "symbols": ("SOLUSD", "SOLUSD=X"), "unit_label": "USD"},
    {"label": "XRP/USD", "symbols": ("XRPUSD", "XRPUSD=X"), "unit_label": "USD"},
    {"label": "BNB/USD", "symbols": ("BNBUSD", "BNBUSD=X"), "unit_label": "USD"},
)

_CACHE: dict[str, tuple[float, float, float, dict[str, Any]]] = {}
_CACHE_LOCK = Lock()
_TRUE_VALUES = {"1", "true", "yes", "on"}


def _debug_logs_enabled() -> bool:
    return os.getenv("PROVIDER_DEBUG_LOGS", "false").strip().lower() in _TRUE_VALUES


def clear_macro_snapshot_cache() -> None:
    with _CACHE_LOCK:
        _CACHE.clear()
    try:
        from app.services.provider_usage import reset_provider_usage

        reset_provider_usage()
    except Exception:
        pass


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
        record_cache_hit(category="macro-snapshot", cache_age_seconds=max(now - fetched_at, 0))
        return payload


def _cache_get_stale(key: str) -> tuple[dict[str, Any], float] | None:
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
        record_cache_hit(category="macro-snapshot", cache_age_seconds=age)
        return payload, age


def _cache_set(key: str, payload: dict[str, Any]) -> dict[str, Any]:
    now = time.time()
    with _CACHE_LOCK:
        _CACHE[key] = (now, now + MACRO_SNAPSHOT_TTL_SECONDS, now + MACRO_SNAPSHOT_STALE_TTL_SECONDS, payload)
    return payload


def _trimmed(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned or None


def _parse_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned = value.strip().replace("%", "").replace(",", "")
        if not cleaned:
            return None
        try:
            return float(cleaned)
        except ValueError:
            return None
    return None


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _empty_snapshot(*, status: str = "unavailable", include_fallback: bool = True) -> dict[str, Any]:
    payload = {
        "world_indexes": [],
        "indexes": [],
        "treasury": [],
        "economics": [],
        "commodities": [_unavailable_instrument(target) for target in COMMODITY_TARGETS],
        "currencies": [_unavailable_instrument(target) for target in CURRENCY_TARGETS],
        "crypto": [_unavailable_instrument(target) for target in CRYPTO_TARGETS],
        "sector_performance": [],
        "status": status,
        "generated_at": _now_iso(),
    }
    if include_fallback:
        payload.update(fallback_payload(reason="provider_unavailable", message="Insights block unavailable."))
    return payload


def _is_public_request_context() -> bool:
    from app.request_priority import get_request_context

    context = get_request_context() or {}
    route = str(context.get("path") or "")
    return route.startswith("/api/") and not route.startswith("/api/admin/")


def _stale_snapshot(payload: dict[str, Any], *, reason: str, age_seconds: float) -> dict[str, Any]:
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


def _enqueue_macro_refresh(*, reason: str) -> None:
    if not _is_public_request_context():
        return
    enqueue_data_enrichment_job(
        job_type="macro_snapshot",
        source="page_load",
        reason=reason,
        priority=55,
    )


def _api_key() -> str | None:
    key = os.getenv("FMP_API_KEY", "").strip()
    return key or None


def _request_payload_with_status(endpoint: str, *, params: dict[str, Any] | None = None) -> tuple[Any, int]:
    try:
        ensure_fmp_live_allowed(category=f"macro:{endpoint}")
    except ProviderUnavailable:
        raise
    api_key = _api_key()
    if not api_key:
        raise RuntimeError("Missing FMP_API_KEY")
    request_params = {"apikey": api_key}
    if params:
        for key, value in params.items():
            if value is None:
                continue
            if isinstance(value, str) and not value.strip():
                continue
            request_params[key] = value
    response = requests.get(
        f"{FMP_BASE_URL}/{endpoint}",
        params=request_params,
        timeout=PROVIDER_TIMEOUT_SECONDS,
    )
    record_provider_response(category=f"macro:{endpoint}", status_code=response.status_code)
    response.raise_for_status()
    return response.json(), response.status_code


def _request_payload(endpoint: str, *, params: dict[str, Any] | None = None) -> Any:
    payload, _status = _request_payload_with_status(endpoint, params=params)
    return payload


def _rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if isinstance(payload, dict):
        if any(key in payload for key in ("symbol", "price", "name", "sector")):
            return [payload]
        data = payload.get("data")
        if isinstance(data, list):
            return [row for row in data if isinstance(row, dict)]
        if isinstance(data, dict):
            return [row for row in data.values() if isinstance(row, dict)]
    return []


def _latest_row(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    dated_rows = [row for row in rows if _trimmed(row.get("date"))]
    if dated_rows:
        return max(dated_rows, key=lambda row: _trimmed(row.get("date")) or "")
    return rows[0] if rows else None


def _row_symbol(row: dict[str, Any]) -> str:
    return (_trimmed(row.get("symbol")) or _trimmed(row.get("ticker")) or "").upper()


def _row_name(row: dict[str, Any]) -> str:
    return (
        _trimmed(row.get("name"))
        or _trimmed(row.get("indexName"))
        or _trimmed(row.get("shortName"))
        or _trimmed(row.get("title"))
        or _trimmed(row.get("commodity"))
        or ""
    ).lower()


def _match_index_row(rows: list[dict[str, Any]], target: dict[str, Any]) -> dict[str, Any] | None:
    symbols = {symbol.upper() for symbol in target["symbols"]}
    for row in rows:
        if _row_symbol(row) in symbols:
            return row
    names = tuple(str(name).lower() for name in target["names"])
    for row in rows:
        row_name = _row_name(row)
        if row_name and any(name in row_name for name in names):
            return row
    return None


def _normalize_index_row(
    row: dict[str, Any],
    *,
    label: str,
    fallback_symbol: str,
    is_proxy: bool = False,
) -> dict[str, Any] | None:
    value = _pick_first_numeric(row, ("price", "value", "level", "close"))
    change_pct = _pick_first_numeric(
        row,
        ("changesPercentage", "changePercentage", "change_pct", "changePercent", "changesPercent"),
    )
    if change_pct is None:
        change = _pick_first_numeric(row, ("change", "changes"))
        previous_close = _pick_first_numeric(row, ("previousClose", "previous_close"))
        if change is not None and previous_close:
            change_pct = (change / previous_close) * 100
    if value is None:
        return None
    return {
        "label": label,
        "symbol": _row_symbol(row) or fallback_symbol,
        "value": value,
        "change_pct": change_pct,
        "is_proxy": is_proxy,
        "source": "etf_proxy" if is_proxy else "index",
    }


def _quote_row_has_value(row: dict[str, Any] | None) -> bool:
    if not row:
        return False
    return _pick_first_numeric(row, ("price", "value", "level", "close", "bid", "ask")) is not None


def _quote_row_has_change(row: dict[str, Any] | None) -> bool:
    if not row:
        return False
    return (
        _pick_first_numeric(
            row,
            ("changesPercentage", "changePercentage", "change_pct", "changePercent", "changesPercent"),
        )
        is not None
        or _pick_first_numeric(row, ("change", "changes", "priceChange")) is not None
    )


def _request_index_quote(symbol: str, *, debug_label: str | None = None) -> dict[str, Any] | None:
    for endpoint in ("historical-chart/1min", "historical-price-eod/light"):
        status: int | str | None = None
        rows: list[dict[str, Any]] = []
        try:
            payload, status = _request_payload_with_status(endpoint, params={"symbol": symbol})
            rows = _rows(payload)
            row = _latest_row(rows)
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else exc.__class__.__name__
            row = None
        except Exception as exc:
            status = exc.__class__.__name__
            row = None
        if debug_label == "Canada TSX" and _debug_logs_enabled():
            logger.info(
                "Market snapshot TSX alias attempt: label=%s alias=%s helper=%s status=%s rows=%s has_value=%s has_change=%s",
                debug_label,
                symbol,
                endpoint,
                status,
                len(rows),
                _quote_row_has_value(row),
                _quote_row_has_change(row),
            )
        if row and _quote_row_has_value(row):
            return row
    return None


def _proxy_rows(targets: tuple[dict[str, Any], ...]) -> dict[str, dict[str, Any]]:
    symbols = [str(target["proxy_symbol"]) for target in targets if target.get("proxy_symbol")]
    if not symbols:
        return {}
    rows: list[dict[str, Any]] = []
    try:
        rows = _rows(_request_payload("batch-quote", params={"symbols": ",".join(symbols)}))
    except Exception:
        rows = []
    by_symbol = {_row_symbol(row): row for row in rows}
    for symbol in symbols:
        if symbol.upper() in by_symbol:
            continue
        try:
            row = _latest_row(_rows(_request_payload("historical-price-eod/light", params={"symbol": symbol})))
        except Exception:
            row = None
        if row:
            by_symbol[symbol.upper()] = row
    return by_symbol


def _build_indexes(targets: tuple[dict[str, Any], ...] = INDEX_TARGETS) -> list[dict[str, Any]]:
    try:
        index_rows = _rows(_request_payload("batch-index-quotes"))
    except Exception:
        index_rows = []

    items: list[dict[str, Any]] = []
    found_targets: set[str] = set()
    for target in targets:
        primary_symbol = str(target["symbols"][0])
        row = _match_index_row(index_rows, target)
        normalized = (
            _normalize_index_row(row, label=str(target["label"]), fallback_symbol=primary_symbol)
            if row
            else None
        )
        if not normalized:
            for symbol in target["symbols"]:
                row = _request_index_quote(str(symbol), debug_label=str(target["label"]))
                normalized = (
                    _normalize_index_row(row, label=str(target["label"]), fallback_symbol=str(symbol))
                    if row
                    else None
                )
                if normalized:
                    if target["label"] == "Canada TSX" and _debug_logs_enabled():
                        logger.info(
                            "Market snapshot TSX resolved: label=%s resolved_symbol=%s source=index",
                            target["label"],
                            normalized.get("symbol"),
                        )
                    break
        if not normalized and target.get("proxy_symbols"):
            for proxy_symbol in target["proxy_symbols"]:
                row = _request_index_quote(str(proxy_symbol), debug_label=str(target["label"]))
                normalized = (
                    _normalize_index_row(
                        row,
                        label=str(target.get("proxy_label") or target["label"]),
                        fallback_symbol=str(proxy_symbol),
                        is_proxy=True,
                    )
                    if row
                    else None
                )
                if normalized:
                    resolved_symbol = str(normalized.get("symbol") or proxy_symbol)
                    normalized["symbol"] = f"{resolved_symbol} proxy"
                    if target["label"] == "Canada TSX" and _debug_logs_enabled():
                        logger.info(
                            "Market snapshot TSX resolved: label=%s resolved_symbol=%s source=etf_proxy",
                            target["label"],
                            normalized.get("symbol"),
                        )
                    break
        if normalized:
            items.append(normalized)
            found_targets.add(str(target["label"]))
        elif not target.get("proxy_symbol") and _debug_logs_enabled():
            logger.info(
                "Market snapshot index unavailable: label=%s attempted_symbols=%s attempted_proxies=%s helper=batch-index-quotes,historical-chart/1min,historical-price-eod/light",
                target["label"],
                ",".join(str(symbol) for symbol in target["symbols"]),
                ",".join(str(symbol) for symbol in target.get("proxy_symbols", ())),
            )

    missing_targets = [target for target in targets if str(target["label"]) not in found_targets and target.get("proxy_symbol")]
    if missing_targets:
        proxies = _proxy_rows(missing_targets)
        for target in missing_targets:
            proxy_symbol = str(target["proxy_symbol"]).upper()
            row = proxies.get(proxy_symbol)
            normalized = (
                _normalize_index_row(
                    row,
                    label=str(target["proxy_label"]),
                    fallback_symbol=proxy_symbol,
                    is_proxy=True,
                )
                if row
                else None
            )
            if normalized:
                items.append(normalized)
            elif _debug_logs_enabled():
                logger.info(
                    "Market snapshot index unavailable: label=%s attempted_symbols=%s helper=etf_proxy",
                    target["label"],
                    proxy_symbol,
                )

    return items


def _pick_first_numeric(row: dict[str, Any], aliases: tuple[str, ...]) -> float | None:
    for key in aliases:
        value = _parse_float(row.get(key))
        if value is not None:
            return value
    return None


def _unavailable_instrument(target: dict[str, Any]) -> dict[str, Any]:
    return {
        "label": str(target["label"]),
        "symbol": str(target["symbols"][0]),
        "value": None,
        "change": None,
        "change_pct": None,
        "timeframe_label": "1D change",
        "unit_label": target.get("unit_label"),
        "status": "unavailable",
    }


def _normalize_snapshot_quote(row: dict[str, Any], target: dict[str, Any]) -> dict[str, Any] | None:
    value = _pick_first_numeric(row, ("price", "value", "rate", "close", "bid", "ask"))
    if value is None:
        return None

    change = _pick_first_numeric(row, ("change", "changes", "priceChange"))
    change_pct = _pick_first_numeric(
        row,
        ("changesPercentage", "changePercentage", "change_pct", "changePercent", "changesPercent"),
    )
    if change_pct is None and change is not None:
        previous_close = _pick_first_numeric(row, ("previousClose", "previous_close", "prevClose"))
        if previous_close:
            change_pct = (change / previous_close) * 100

    return {
        "label": str(target["label"]),
        "symbol": _row_symbol(row) or str(target["symbols"][0]),
        "value": value,
        "change": change,
        "change_pct": change_pct,
        "timeframe_label": "1D change",
        "unit_label": target.get("unit_label"),
        "status": "ok",
    }


def _request_quote_rows(symbols: list[str], *, endpoint: str = "batch-quote") -> dict[str, dict[str, Any]]:
    by_symbol: dict[str, dict[str, Any]] = {}
    if not symbols and endpoint != "batch-commodity-quotes":
        return by_symbol
    try:
        params = None if endpoint == "batch-commodity-quotes" else {"symbols": ",".join(symbols)}
        rows = _rows(_request_payload(endpoint, params=params))
    except Exception as exc:
        logger.info(
            "Market snapshot quote batch unavailable: helper=%s attempted_symbols=%s error=%s",
            endpoint,
            ",".join(symbols),
            exc.__class__.__name__,
        )
        rows = []
    by_symbol.update({_row_symbol(row): row for row in rows if _row_symbol(row)})
    return by_symbol


def _request_single_quote_row(symbol: str) -> dict[str, Any] | None:
    for endpoint in ("historical-chart/1min", "historical-price-eod/light"):
        try:
            row = _latest_row(_rows(_request_payload(endpoint, params={"symbol": symbol})))
        except Exception:
            row = None
        if row:
            return row
    return None


def _build_snapshot_instruments(
    targets: tuple[dict[str, Any], ...],
    *,
    batch_endpoint: str = "batch-quote",
) -> list[dict[str, Any]]:
    symbols: list[str] = []
    for target in targets:
        symbols.extend(str(symbol) for symbol in target["symbols"])
    quote_rows = _request_quote_rows(symbols, endpoint=batch_endpoint)

    items: list[dict[str, Any]] = []
    for target in targets:
        normalized: dict[str, Any] | None = None
        for symbol in target["symbols"]:
            row = quote_rows.get(str(symbol).upper())
            if not row:
                continue
            normalized = _normalize_snapshot_quote(row, target)
            if normalized:
                break
        if not normalized:
            for symbol in target["symbols"]:
                row = _request_single_quote_row(str(symbol))
                normalized = _normalize_snapshot_quote(row, target) if row else None
                if normalized:
                    break
        if not normalized:
            logger.info(
                "Market snapshot instrument unavailable: label=%s attempted_symbols=%s helper=%s",
                target["label"],
                ",".join(str(symbol) for symbol in target["symbols"]),
                batch_endpoint,
            )
        items.append(normalized or _unavailable_instrument(target))
    return items


def _has_ok_instruments(items: list[dict[str, Any]]) -> bool:
    return any(item.get("status") == "ok" for item in items)


def _dated_rows_desc(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        [row for row in rows if _trimmed(row.get("date"))],
        key=lambda row: _trimmed(row.get("date")) or "",
        reverse=True,
    )


def _macro_point(
    *,
    label: str,
    value: float | None,
    value_format: str,
    date_value: str | None,
    change_value: float | None = None,
    change_format: str | None = None,
    change_label: str | None = None,
    context_label: str = "Latest available",
) -> dict[str, Any]:
    return {
        "label": label,
        "value": value,
        "value_format": value_format,
        "date": date_value,
        "change_value": change_value,
        "change_format": change_format,
        "change_label": change_label,
        "context_label": context_label,
    }


def _macro_unavailable(
    label: str,
    *,
    value_format: str = "percent",
    change_format: str | None = None,
    change_label: str | None = None,
    context_label: str = "Latest available",
) -> dict[str, Any]:
    return _macro_point(
        label=label,
        value=None,
        value_format=value_format,
        date_value=None,
        change_value=None,
        change_format=change_format,
        change_label=change_label,
        context_label=context_label,
    )


def _series_value(row: dict[str, Any]) -> float | None:
    value = _parse_float(row.get("value"))
    if value is None:
        value = _parse_float(row.get("indicator"))
    if value is None:
        value = _parse_float(row.get("close"))
    return value


def _indicator_series(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    points: list[dict[str, Any]] = []
    seen_dates: set[str] = set()
    for row in _dated_rows_desc(rows):
        as_of = _trimmed(row.get("date"))
        if not as_of or as_of in seen_dates:
            continue
        value = _series_value(row)
        if value is None:
            continue
        seen_dates.add(as_of)
        points.append({"date": as_of, "value": value, "raw": row})
    return points


def _request_indicator_series(candidates: tuple[str, ...]) -> tuple[list[dict[str, Any]], str | None, str | None]:
    last_error: str | None = None
    for name in candidates:
        try:
            series = _indicator_series(_rows(_request_payload("economic-indicators", params={"name": name})))
        except Exception as exc:
            last_error = exc.__class__.__name__
            series = []
        if series:
            return series, name, last_error
    return [], None, last_error


def _public_macro_csv_series(
    series_id: str,
    *,
    value_scale: float = 1.0,
) -> list[dict[str, Any]]:
    response = requests.get(
        PUBLIC_MACRO_CSV_URL,
        params={"id": series_id},
        timeout=PROVIDER_TIMEOUT_SECONDS,
    )
    response.raise_for_status()

    rows = csv.DictReader(io.StringIO(response.text))
    points: list[dict[str, Any]] = []
    seen_dates: set[str] = set()
    for row in rows:
        normalized_row = {str(key).strip().lstrip("\ufeff").lower(): value for key, value in row.items()}
        as_of = _trimmed(normalized_row.get("observation_date") or normalized_row.get("date"))
        if not as_of or as_of in seen_dates:
            continue
        raw_value = normalized_row.get(series_id.lower())
        if raw_value is None:
            raw_value = next((value for key, value in normalized_row.items() if key not in {"observation_date", "date"}), None)
        value = _parse_float(raw_value)
        if value is None:
            continue
        seen_dates.add(as_of)
        points.append({"date": as_of, "value": value * value_scale, "raw": {"series": series_id}})
    return sorted(points, key=lambda point: point["date"], reverse=True)


def _series_change_value(series: list[dict[str, Any]]) -> float | None:
    if len(series) < 2:
        return None
    return series[0]["value"] - series[1]["value"]


def _series_looks_like_percent(
    series: list[dict[str, Any]],
    *,
    upper_bound: float = 100.0,
    lower_bound: float = 0.0,
) -> bool:
    if not series:
        return False
    latest_value = abs(series[0]["value"])
    return lower_bound <= latest_value <= upper_bound


def _normalize_debt_to_gdp_series(
    series: list[dict[str, Any]],
    *,
    lower_bound: float = 50.0,
    upper_bound: float = 200.0,
) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for point in series:
        value = point["value"]
        absolute_value = abs(value)
        if lower_bound <= absolute_value <= upper_bound:
            normalized_value = value
        elif lower_bound / 100.0 <= absolute_value <= upper_bound / 100.0:
            normalized_value = value * 100.0
        else:
            continue
        normalized.append({**point, "value": normalized_value})
    return normalized


def _date_parts(date_value: str | None) -> tuple[int, int, int] | None:
    trimmed = _trimmed(date_value)
    if not trimmed:
        return None
    try:
        parsed = date.fromisoformat(trimmed[:10])
    except ValueError:
        return None
    return parsed.year, parsed.month, parsed.day


def _month_key(date_value: str | None) -> str | None:
    parts = _date_parts(date_value)
    if not parts:
        return None
    year, month, _day = parts
    return f"{year:04d}-{month:02d}"


def _parse_iso_date(date_value: str | None) -> date | None:
    trimmed = _trimmed(date_value)
    if not trimmed:
        return None
    try:
        return date.fromisoformat(trimmed[:10])
    except ValueError:
        return None


def _quarter_key(date_value: str | None) -> str | None:
    parts = _date_parts(date_value)
    if not parts:
        return None
    year, month, _day = parts
    quarter = ((month - 1) // 3) + 1
    return f"{year:04d}-Q{quarter}"


def _prior_year_reference_point(
    point: dict[str, Any],
    series: list[dict[str, Any]],
    *,
    tolerance_days: int = 45,
) -> dict[str, Any] | None:
    point_date = _parse_iso_date(point.get("date"))
    if not point_date:
        return None

    exact_key = f"{point_date.year - 1:04d}-{point_date.month:02d}"
    by_month = {_month_key(candidate["date"]): candidate for candidate in series}
    exact_point = by_month.get(exact_key)
    if exact_point:
        return exact_point

    try:
        target_date = point_date.replace(year=point_date.year - 1)
    except ValueError:
        target_date = date(point_date.year - 1, point_date.month, 28)

    closest: tuple[int, dict[str, Any]] | None = None
    for candidate in series:
        candidate_date = _parse_iso_date(candidate.get("date"))
        if not candidate_date:
            continue
        distance = abs((candidate_date - target_date).days)
        if distance > tolerance_days:
            continue
        if closest is None or distance < closest[0]:
            closest = (distance, candidate)
    return closest[1] if closest else None


def _build_yoy_series(series: list[dict[str, Any]]) -> list[dict[str, Any]]:
    yoy_points: list[dict[str, Any]] = []
    for point in series:
        previous_point = _prior_year_reference_point(point, series)
        if not previous_point:
            continue
        previous_value = previous_point["value"]
        if previous_value == 0:
            continue
        yoy_points.append(
            {
                "date": point["date"],
                "value": ((point["value"] / previous_value) - 1.0) * 100.0,
            }
        )
    return yoy_points


def _infer_scale_from_text(*values: Any) -> float:
    normalized = " ".join(str(value).lower() for value in values if value)
    if not normalized:
        return 1.0
    if "trillion" in normalized or "tn" in normalized:
        return 1_000_000_000_000.0
    if "billion" in normalized or normalized.endswith(" bn") or " bn " in normalized:
        return 1_000_000_000.0
    if "million" in normalized or normalized.endswith(" mn") or " mn " in normalized:
        return 1_000_000.0
    if "thousand" in normalized or "thousands" in normalized:
        return 1_000.0
    return 1.0


def _macro_level_scale(point: dict[str, Any], *, metric_label: str) -> float:
    raw = point.get("raw")
    inferred = _infer_scale_from_text(
        raw.get("unit") if isinstance(raw, dict) else None,
        raw.get("units") if isinstance(raw, dict) else None,
        raw.get("unitLabel") if isinstance(raw, dict) else None,
        raw.get("name") if isinstance(raw, dict) else None,
        raw.get("title") if isinstance(raw, dict) else None,
        raw.get("series") if isinstance(raw, dict) else None,
    )
    if inferred != 1.0:
        return inferred

    absolute_value = abs(point["value"])
    if metric_label == "Retail Sales":
        if 100.0 <= absolute_value < 5_000.0:
            return 1_000_000_000.0
        if 5_000.0 <= absolute_value < 10_000_000.0:
            return 1_000_000.0
    if metric_label in {"Federal Debt", "GDP"} and 100.0 <= absolute_value < 100_000.0:
        return 1_000_000_000.0
    return 1.0


def _scaled_macro_value(point: dict[str, Any], *, metric_label: str) -> float:
    return point["value"] * _macro_level_scale(point, metric_label=metric_label)


def _compute_ratio_percent(debt_point: dict[str, Any], gdp_point: dict[str, Any]) -> float | None:
    scaled_debt = _scaled_macro_value(debt_point, metric_label="Federal Debt")
    scaled_gdp = _scaled_macro_value(gdp_point, metric_label="GDP")
    if scaled_gdp == 0:
        return None

    ratio = (scaled_debt / scaled_gdp) * 100.0
    if 50.0 <= ratio <= 200.0:
        return ratio

    raw_ratio = (debt_point["value"] / gdp_point["value"]) * 100.0 if gdp_point["value"] else None
    if raw_ratio is None:
        return None
    candidates = [raw_ratio * (1000.0**exponent) for exponent in range(-4, 5)]
    plausible = [candidate for candidate in candidates if 50.0 <= candidate <= 200.0]
    if len(plausible) == 1:
        return plausible[0]
    return None


def _closest_series_point(
    point: dict[str, Any],
    series: list[dict[str, Any]],
    *,
    tolerance_days: int,
) -> dict[str, Any] | None:
    point_date = _parse_iso_date(point.get("date"))
    if not point_date:
        return None
    closest: tuple[int, dict[str, Any]] | None = None
    for candidate in series:
        candidate_date = _parse_iso_date(candidate.get("date"))
        if not candidate_date:
            continue
        distance = abs((candidate_date - point_date).days)
        if distance > tolerance_days:
            continue
        if closest is None or distance < closest[0]:
            closest = (distance, candidate)
    return closest[1] if closest else None


def _build_latest_per_quarter(series: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    latest_by_quarter: dict[str, dict[str, Any]] = {}
    for point in series:
        key = _quarter_key(point["date"])
        if not key or key in latest_by_quarter:
            continue
        latest_by_quarter[key] = point
    return latest_by_quarter


def _build_debt_to_gdp_series(
    debt_series: list[dict[str, Any]],
    gdp_series: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    debt_by_quarter = _build_latest_per_quarter(debt_series)
    gdp_by_quarter = _build_latest_per_quarter(gdp_series)
    points: list[dict[str, Any]] = []
    for quarter_key in sorted(set(debt_by_quarter).intersection(gdp_by_quarter), reverse=True):
        debt_point = debt_by_quarter[quarter_key]
        gdp_point = gdp_by_quarter[quarter_key]
        ratio = _compute_ratio_percent(debt_point, gdp_point)
        if ratio is None:
            continue
        points.append(
            {
                "date": max(debt_point["date"], gdp_point["date"]),
                "value": ratio,
            }
        )
    if points:
        return points

    for gdp_point in gdp_series:
        debt_point = _closest_series_point(gdp_point, debt_series, tolerance_days=120)
        if not debt_point:
            continue
        ratio = _compute_ratio_percent(debt_point, gdp_point)
        if ratio is None:
            continue
        points.append(
            {
                "date": max(debt_point["date"], gdp_point["date"]),
                "value": ratio,
            }
        )
    return points


def _has_ok_macro_points(items: list[dict[str, Any]]) -> bool:
    return any(item.get("value") is not None for item in items)


def _log_macro_unavailable(
    label: str,
    *,
    candidates: tuple[str, ...] | list[str],
    direct_series_found: bool,
    computed_fallback_attempted: bool,
    reason: str,
    details: dict[str, Any] | None = None,
) -> None:
    if not _debug_logs_enabled():
        return
    logger.info(
        "Market snapshot macro unavailable: label=%s candidates=%s direct_series_found=%s computed_fallback_attempted=%s reason=%s details=%s",
        label,
        ",".join(candidates),
        direct_series_found,
        computed_fallback_attempted,
        reason,
        details or {},
    )


def _build_fed_overnight_rate_point() -> dict[str, Any]:
    series, _selected_name, last_error = _request_indicator_series(FED_OVERNIGHT_RATE_CANDIDATES)
    if not series:
        if _debug_logs_enabled():
            logger.info(
                "Market snapshot macro unavailable: label=%s candidates=%s helper=economic-indicators error=%s",
                "Fed Overnight Rate",
                ",".join(FED_OVERNIGHT_RATE_CANDIDATES),
                last_error,
            )
        return _macro_unavailable("Fed Overnight Rate", value_format="percent", change_format="bps")

    return _macro_point(
        label="Fed Overnight Rate",
        value=series[0]["value"],
        value_format="percent",
        date_value=series[0]["date"],
        change_value=_series_change_value(series) * 100.0 if len(series) >= 2 else None,
        change_format="bps",
    )


def _build_core_cpi_point() -> dict[str, Any]:
    last_error: str | None = None
    index_series_candidates: list[tuple[str, list[dict[str, Any]]]] = []
    attempts: list[dict[str, Any]] = []
    for name in CORE_CPI_CANDIDATES:
        try:
            series = _indicator_series(_rows(_request_payload("economic-indicators", params={"name": name})))
        except Exception as exc:
            last_error = exc.__class__.__name__
            attempts.append({"candidate": name, "status": "error", "error": last_error})
            continue
        attempts.append({"candidate": name, "status": "series" if series else "empty", "points": len(series)})
        if not series:
            continue
        if _series_looks_like_percent(series, upper_bound=50.0):
            return _macro_point(
                label="Core CPI",
                value=series[0]["value"],
                value_format="percent",
                date_value=series[0]["date"],
                change_value=_series_change_value(series),
                change_format="percentage_points",
            )
        index_series_candidates.append((name, series))

    yoy_series: list[dict[str, Any]] = []
    for name, series in index_series_candidates:
        yoy_series = _build_yoy_series(series)
        attempts.append({"candidate": name, "status": "computed_yoy" if yoy_series else "missing_yoy_history", "points": len(yoy_series)})
        if yoy_series:
            break
    if not yoy_series:
        for public_series in PUBLIC_CORE_CPI_INDEX_SERIES:
            try:
                public_index_series = _public_macro_csv_series(str(public_series["id"]))
            except Exception as exc:
                attempts.append({"candidate": public_series["label"], "status": "error", "error": exc.__class__.__name__})
                continue
            attempts.append({"candidate": public_series["label"], "status": "series" if public_index_series else "empty", "points": len(public_index_series)})
            yoy_series = _build_yoy_series(public_index_series)
            if yoy_series:
                break

    if not yoy_series:
        _log_macro_unavailable(
            "Core CPI",
            candidates=list(CORE_CPI_CANDIDATES) + [str(item["label"]) for item in PUBLIC_CORE_CPI_INDEX_SERIES],
            direct_series_found=bool(index_series_candidates),
            computed_fallback_attempted=True,
            reason="missing_yoy_history" if index_series_candidates else (last_error or "no_series"),
            details={"attempts": attempts[:20]},
        )
        return _macro_unavailable("Core CPI", value_format="percent", change_format="percentage_points")

    return _macro_point(
        label="Core CPI",
        value=yoy_series[0]["value"],
        value_format="percent",
        date_value=yoy_series[0]["date"],
        change_value=_series_change_value(yoy_series),
        change_format="percentage_points",
    )


def _build_unemployment_point() -> dict[str, Any]:
    series, _selected_name, last_error = _request_indicator_series(UNEMPLOYMENT_RATE_CANDIDATES)
    if not series:
        if _debug_logs_enabled():
            logger.info(
                "Market snapshot macro unavailable: label=%s candidates=%s helper=economic-indicators error=%s",
                "Unemployment",
                ",".join(UNEMPLOYMENT_RATE_CANDIDATES),
                last_error,
            )
        return _macro_unavailable("Unemployment", value_format="percent", change_format="percentage_points")

    return _macro_point(
        label="Unemployment",
        value=series[0]["value"],
        value_format="percent",
        date_value=series[0]["date"],
        change_value=_series_change_value(series),
        change_format="percentage_points",
    )


def _build_debt_to_gdp_point() -> dict[str, Any]:
    attempts: list[dict[str, Any]] = []
    direct_series, _selected_name, last_error = _request_indicator_series(DEBT_TO_GDP_CANDIDATES)
    attempts.append({"candidate_group": "direct_ratio", "status": "series" if direct_series else "empty", "points": len(direct_series)})
    normalized_direct_series = _normalize_debt_to_gdp_series(direct_series)
    if normalized_direct_series:
        return _macro_point(
            label="Debt/GDP",
            value=normalized_direct_series[0]["value"],
            value_format="percent",
            date_value=normalized_direct_series[0]["date"],
            change_value=_series_change_value(normalized_direct_series),
            change_format="percentage_points",
        )

    for public_series in PUBLIC_DEBT_TO_GDP_RATIO_SERIES:
        try:
            public_direct_series = _public_macro_csv_series(str(public_series["id"]))
        except Exception as exc:
            attempts.append({"candidate": public_series["label"], "status": "error", "error": exc.__class__.__name__})
            continue
        attempts.append({"candidate": public_series["label"], "status": "series" if public_direct_series else "empty", "points": len(public_direct_series)})
        normalized_public_series = _normalize_debt_to_gdp_series(public_direct_series)
        if normalized_public_series:
            return _macro_point(
                label="Debt/GDP",
                value=normalized_public_series[0]["value"],
                value_format="percent",
                date_value=normalized_public_series[0]["date"],
                change_value=_series_change_value(normalized_public_series),
                change_format="percentage_points",
            )

    debt_series, _debt_name, debt_error = _request_indicator_series(FEDERAL_DEBT_CANDIDATES)
    gdp_series, _gdp_name, gdp_error = _request_indicator_series(NOMINAL_GDP_CANDIDATES)
    attempts.append({"candidate_group": "computed_primary_debt", "status": "series" if debt_series else "empty", "points": len(debt_series)})
    attempts.append({"candidate_group": "computed_primary_gdp", "status": "series" if gdp_series else "empty", "points": len(gdp_series)})
    computed_series = _build_debt_to_gdp_series(debt_series, gdp_series) if debt_series and gdp_series else []
    if computed_series:
        return _macro_point(
            label="Debt/GDP",
            value=computed_series[0]["value"],
            value_format="percent",
            date_value=computed_series[0]["date"],
            change_value=_series_change_value(computed_series),
            change_format="percentage_points",
        )

    public_debt_series: list[dict[str, Any]] = []
    public_gdp_series: list[dict[str, Any]] = []
    for public_series in PUBLIC_FEDERAL_DEBT_SERIES:
        try:
            public_debt_series = _public_macro_csv_series(str(public_series["id"]), value_scale=float(public_series["scale"]))
        except Exception as exc:
            attempts.append({"candidate": public_series["label"], "status": "error", "error": exc.__class__.__name__})
            continue
        attempts.append({"candidate": public_series["label"], "status": "series" if public_debt_series else "empty", "points": len(public_debt_series)})
        if public_debt_series:
            break
    for public_series in PUBLIC_NOMINAL_GDP_SERIES:
        try:
            public_gdp_series = _public_macro_csv_series(str(public_series["id"]), value_scale=float(public_series["scale"]))
        except Exception as exc:
            attempts.append({"candidate": public_series["label"], "status": "error", "error": exc.__class__.__name__})
            continue
        attempts.append({"candidate": public_series["label"], "status": "series" if public_gdp_series else "empty", "points": len(public_gdp_series)})
        if public_gdp_series:
            break

    computed_public_series = (
        _build_debt_to_gdp_series(public_debt_series, public_gdp_series)
        if public_debt_series and public_gdp_series
        else []
    )
    if computed_public_series:
        return _macro_point(
            label="Debt/GDP",
            value=computed_public_series[0]["value"],
            value_format="percent",
            date_value=computed_public_series[0]["date"],
            change_value=_series_change_value(computed_public_series),
            change_format="percentage_points",
        )

    reason = "computed_ratio_rejected"
    if not debt_series and not public_debt_series:
        reason = "missing_debt_series"
    elif not gdp_series and not public_gdp_series:
        reason = "missing_gdp_series"
    elif direct_series and not normalized_direct_series:
        reason = "direct_ratio_outside_expected_range"

    _log_macro_unavailable(
        "Debt/GDP",
        candidates=(
            list(DEBT_TO_GDP_CANDIDATES)
            + list(FEDERAL_DEBT_CANDIDATES)
            + list(NOMINAL_GDP_CANDIDATES)
            + [str(item["label"]) for item in PUBLIC_DEBT_TO_GDP_RATIO_SERIES]
            + [str(item["label"]) for item in PUBLIC_FEDERAL_DEBT_SERIES]
            + [str(item["label"]) for item in PUBLIC_NOMINAL_GDP_SERIES]
        ),
        direct_series_found=bool(direct_series),
        computed_fallback_attempted=True,
        reason=reason,
        details={"attempts": attempts, "errors": {"direct": last_error, "debt": debt_error, "gdp": gdp_error}},
    )
    return _macro_unavailable("Debt/GDP", value_format="percent", change_format="percentage_points")


def _build_retail_sales_point() -> dict[str, Any]:
    level_series, _selected_name, last_error = _request_indicator_series(RETAIL_SALES_LEVEL_CANDIDATES)
    if not level_series:
        if _debug_logs_enabled():
            logger.info(
                "Market snapshot macro unavailable: label=%s candidates=%s helper=economic-indicators error=%s",
                "Retail Sales",
                ",".join(RETAIL_SALES_LEVEL_CANDIDATES),
                last_error,
            )
        return _macro_unavailable("Retail Sales", value_format="currency", change_format="percent")

    latest = level_series[0]
    previous = level_series[1] if len(level_series) >= 2 else None
    previous_value = previous["value"] if previous else None
    change_pct = None
    change_label = None
    if previous_value not in (None, 0):
        change_pct = ((latest["value"] - previous_value) / previous_value) * 100.0
        change_label = "PoP"
    else:
        for request in RETAIL_SALES_GROWTH_REQUESTS:
            if request["change_label"] != "MoM":
                continue
            growth_series, _growth_name, _growth_error = _request_indicator_series(request["candidates"])
            if not growth_series or not _series_looks_like_percent(growth_series, upper_bound=100.0):
                continue
            change_pct = growth_series[0]["value"]
            change_label = request["change_label"]
            break

    return _macro_point(
        label="Retail Sales",
        value=_scaled_macro_value(latest, metric_label="Retail Sales"),
        value_format="currency",
        date_value=latest["date"],
        change_value=change_pct,
        change_format="percent",
        change_label=change_label,
    )


def _build_treasury() -> list[dict[str, Any]]:
    payload = _request_payload("treasury-rates")
    rows = _rows(payload)
    dated_rows = _dated_rows_desc(rows)
    row = dated_rows[0] if dated_rows else _latest_row(rows)
    previous_row = dated_rows[1] if len(dated_rows) > 1 else None
    if not row:
        return []
    as_of = _trimmed(row.get("date"))
    items: list[dict[str, Any]] = []
    for label, aliases in TREASURY_FIELD_ALIASES.items():
        value = _pick_first_numeric(row, aliases)
        if value is None:
            continue
        previous_value = _pick_first_numeric(previous_row, aliases) if previous_row else None
        change_bps = (value - previous_value) * 100 if previous_value is not None else None
        items.append(
            {
                "label": label,
                "value": value,
                "date": as_of,
                "change": change_bps,
                "change_unit": "bps",
                "timeframe_label": "1D change",
                "unit_label": "yield",
            }
        )
    return items


def get_treasury_rates_snapshot() -> list[dict[str, Any]]:
    if not _api_key():
        return []
    try:
        return _build_treasury()
    except Exception:
        logger.exception("treasury_rates_snapshot_failed")
        return []


def _build_economics() -> list[dict[str, Any]]:
    items = [
        _build_fed_overnight_rate_point(),
        _build_core_cpi_point(),
        _build_unemployment_point(),
        _build_debt_to_gdp_point(),
        _build_retail_sales_point(),
    ]
    by_label = {item["label"]: item for item in items}
    return [
        by_label.get(label)
        or _macro_unavailable(
            label,
            value_format="currency" if label == "Retail Sales" else "percent",
            change_format="percent" if label == "Retail Sales" else "percentage_points",
        )
        for label in ECONOMIC_SNAPSHOT_ORDER
    ]


def _normalize_sector_row(row: dict[str, Any]) -> dict[str, Any] | None:
    raw_sector = _trimmed(row.get("sector")) or _trimmed(row.get("name"))
    sector = _canonical_sector_name(raw_sector)
    change_pct = _pick_first_numeric(
        row,
        (
            "averageChange",
            "avgChange",
            "changesPercentage",
            "changePercentage",
            "change_percent",
            "changePct",
            "changePercent",
            "changesPercent",
        ),
    )
    if not sector or change_pct is None:
        return None
    item = {"sector": sector, "change_pct": change_pct}
    row_date = _trimmed(row.get("date")) or _trimmed(row.get("calendarDate")) or _trimmed(row.get("asOfDate"))
    if row_date:
        item["date"] = row_date
    return item


def _canonical_sector_name(value: str | None) -> str | None:
    if not value:
        return None
    cleaned = value.strip()
    if cleaned.endswith(")") and "(" in cleaned:
        cleaned = cleaned.rsplit("(", 1)[0].strip()
    key = " ".join(cleaned.replace("&", "and").lower().split())
    return SECTOR_NAME_ALIASES.get(key) or cleaned


def _build_sector_performance() -> list[dict[str, Any]]:
    for offset in range(0, 6):
        target_date = (date.today() - timedelta(days=offset)).isoformat()
        try:
            rows = _rows(_request_payload("sector-performance-snapshot", params={"date": target_date}))
        except Exception:
            continue
        items = list(filter(None, (_normalize_sector_row(row) for row in rows)))
        if items:
            by_sector: dict[str, dict[str, Any]] = {}
            for item in items:
                sector = str(item.get("sector") or "")
                if sector and sector not in by_sector:
                    by_sector[sector] = item
            ordered = [by_sector.pop(sector) for sector in SECTOR_SNAPSHOT_ORDER if sector in by_sector]
            return [*ordered, *by_sector.values()]
    return []


def get_sector_performance_snapshot() -> list[dict[str, Any]]:
    if not _api_key():
        return []
    try:
        return _build_sector_performance()
    except Exception:
        logger.exception("sector_performance_snapshot_failed")
        return []


def get_macro_snapshot() -> dict[str, Any]:
    cached = _cache_get("macro-snapshot")
    if cached is not None:
        return cached
    record_cache_miss(category="macro-snapshot")

    if not _api_key():
        record_fallback(category="macro-snapshot", reason="provider_disabled")
        _enqueue_macro_refresh(reason="provider_disabled")
        stale = _cache_get_stale("macro-snapshot")
        if stale is not None:
            stale_payload, age = stale
            record_fallback(category="macro-snapshot", reason="provider_disabled", cache_age_seconds=age)
            return _stale_snapshot(stale_payload, reason="provider_disabled", age_seconds=age)
        status = "warming" if _is_public_request_context() else "unavailable"
        payload = _empty_snapshot(status=status, include_fallback=status != "warming")
        if status != "warming":
            payload.update(fallback_payload(reason="provider_disabled", message="Insights block unavailable."))
        return payload if status == "warming" else _cache_set("macro-snapshot", payload)

    try:
        ensure_fmp_live_allowed(category="macro-snapshot")
    except ProviderUnavailable as exc:
        reason = reason_from_exception(exc)
        record_fallback(category="macro-snapshot", reason=reason)
        _enqueue_macro_refresh(reason=reason)
        stale = _cache_get_stale("macro-snapshot")
        if stale is not None:
            stale_payload, age = stale
            record_fallback(category="macro-snapshot", reason=reason, cache_age_seconds=age)
            return _stale_snapshot(stale_payload, reason=reason, age_seconds=age)
        status = "warming" if _is_public_request_context() else "unavailable"
        payload = _empty_snapshot(status=status, include_fallback=status != "warming")
        if status != "warming":
            payload.update(fallback_payload(reason=reason, message="Insights block unavailable."))
        return payload if status == "warming" else _cache_set("macro-snapshot", payload)

    world_indexes: list[dict[str, Any]] = []
    indexes: list[dict[str, Any]] = []
    treasury: list[dict[str, Any]] = []
    economics: list[dict[str, Any]] = []
    commodities: list[dict[str, Any]] = []
    currencies: list[dict[str, Any]] = []
    crypto: list[dict[str, Any]] = []
    sector_performance: list[dict[str, Any]] = []

    try:
        indexes = _build_indexes()
    except Exception:
        indexes = []

    try:
        world_indexes = _build_indexes(WORLD_INDEX_TARGETS)
    except Exception:
        world_indexes = []

    try:
        treasury = _build_treasury()
    except Exception:
        treasury = []

    try:
        economics = _build_economics()
    except Exception:
        economics = []

    try:
        commodities = _build_snapshot_instruments(COMMODITY_TARGETS, batch_endpoint="batch-commodity-quotes")
    except Exception:
        commodities = [_unavailable_instrument(target) for target in COMMODITY_TARGETS]

    try:
        currencies = _build_snapshot_instruments(CURRENCY_TARGETS)
    except Exception:
        currencies = [_unavailable_instrument(target) for target in CURRENCY_TARGETS]

    try:
        crypto = _build_snapshot_instruments(CRYPTO_TARGETS)
    except Exception:
        crypto = [_unavailable_instrument(target) for target in CRYPTO_TARGETS]

    try:
        sector_performance = _build_sector_performance()
    except Exception:
        sector_performance = []

    available_sections = sum(
        [
            bool(indexes),
            bool(world_indexes),
            bool(treasury),
            _has_ok_macro_points(economics),
            _has_ok_instruments(commodities),
            _has_ok_instruments(currencies),
            _has_ok_instruments(crypto),
            bool(sector_performance),
        ]
    )
    if available_sections == 0:
        status = "unavailable"
    elif available_sections == 8:
        status = "ok"
    else:
        status = "partial"

    payload = {
        "world_indexes": world_indexes,
        "indexes": indexes,
        "treasury": treasury,
        "economics": economics,
        "commodities": commodities,
        "currencies": currencies,
        "crypto": crypto,
        "sector_performance": sector_performance,
        "status": status,
        "generated_at": _now_iso(),
    }
    if status == "unavailable":
        record_fallback(category="macro-snapshot", reason="provider_unavailable")
        stale = _cache_get_stale("macro-snapshot")
        if stale is not None:
            stale_payload, age = stale
            record_fallback(category="macro-snapshot", reason="provider_unavailable", cache_age_seconds=age)
            return _stale_snapshot(stale_payload, reason="provider_unavailable", age_seconds=age)
        payload.update(fallback_payload(reason="provider_unavailable", message="Insights block unavailable."))
    return _cache_set("macro-snapshot", payload)
