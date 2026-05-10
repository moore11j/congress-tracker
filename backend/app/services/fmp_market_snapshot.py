from __future__ import annotations

import os
import time
from datetime import date, datetime, timedelta, timezone
from threading import Lock
from typing import Any

import requests

from app.clients.fmp import FMP_BASE_URL

MACRO_SNAPSHOT_TTL_SECONDS = 15 * 60
PROVIDER_TIMEOUT_SECONDS = 8
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
        "symbols": ("^GSPTSE", "GSPTSE", "TSX"),
        "names": ("S&P/TSX Composite", "S&P TSX Composite", "TSX Composite"),
    },
    {
        "label": "FTSE 100",
        "symbols": ("^FTSE", "FTSE"),
        "names": ("FTSE 100",),
    },
    {
        "label": "DAX",
        "symbols": ("^GDAXI", "GDAXI", "DAX"),
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
TREASURY_FIELD_ALIASES = {
    "2Y Treasury": ("year2", "2Y", "2y", "2Year", "2year", "year_2"),
    "5Y Treasury": ("year5", "5Y", "5y", "5Year", "5year", "year_5"),
    "10Y Treasury": ("year10", "10Y", "10y", "10Year", "10year", "year_10"),
    "30Y Treasury": ("year30", "30Y", "30y", "30Year", "30year", "year_30"),
    "3M Treasury": ("month3", "3M", "3m", "3Month", "3month", "month_3"),
}
ECONOMIC_REQUESTS = (
    {
        "label": "Fed Overnight Rate",
        "candidates": (
            "federal funds rate",
            "Federal Funds Rate",
            "effective federal funds rate",
            "Effective Federal Funds Rate",
        ),
        "context_label": "Latest available",
        "unit_label": "%",
    },
    {
        "label": "CPI",
        "candidates": ("CPI", "inflation", "consumer price index"),
        "context_label": "Latest release",
        "unit_label": "%",
    },
    {
        "label": "Unemployment",
        "candidates": ("unemployment rate", "unemploymentRate", "unemployment"),
        "context_label": "Latest release",
        "unit_label": "%",
    },
    {
        "label": "GDP",
        "candidates": ("GDP",),
        "context_label": "Latest release",
        "unit_label": "%",
    },
    {
        "label": "Retail Sales",
        "candidates": ("retail sales", "Retail Sales", "retailSales"),
        "context_label": "Latest release",
        "unit_label": "%",
    },
)
COMMODITY_TARGETS = (
    {"label": "Gold", "symbols": ("GCUSD", "XAUUSD", "GC=F"), "unit_label": "USD"},
    {"label": "Silver", "symbols": ("SIUSD", "XAGUSD", "SI=F"), "unit_label": "USD"},
    {"label": "Copper", "symbols": ("HGUSD", "XCUUSD", "HG=F", "COPPER"), "unit_label": "USD"},
    {"label": "Brent Crude", "symbols": ("BZUSD", "BZ=F"), "unit_label": "USD"},
    {"label": "Wheat", "symbols": ("ZWUSD", "ZW=F", "WHEAT"), "unit_label": "USD"},
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

_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_CACHE_LOCK = Lock()


def clear_macro_snapshot_cache() -> None:
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
        _CACHE[key] = (time.time() + MACRO_SNAPSHOT_TTL_SECONDS, payload)
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


def _empty_snapshot(*, status: str = "unavailable") -> dict[str, Any]:
    return {
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


def _api_key() -> str | None:
    key = os.getenv("FMP_API_KEY", "").strip()
    return key or None


def _request_payload(endpoint: str, *, params: dict[str, Any] | None = None) -> Any:
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
    response.raise_for_status()
    return response.json()


def _rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if isinstance(payload, dict):
        if any(key in payload for key in ("symbol", "price", "name")):
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
    return (_trimmed(row.get("name")) or _trimmed(row.get("indexName")) or "").lower()


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


def _request_index_quote(symbol: str) -> dict[str, Any] | None:
    for endpoint in ("quote", "quote-short"):
        try:
            row = _latest_row(_rows(_request_payload(endpoint, params={"symbol": symbol})))
        except Exception:
            row = None
        if row:
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
            row = _latest_row(_rows(_request_payload("quote", params={"symbol": symbol})))
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
        if not row:
            row = _request_index_quote(primary_symbol)
        normalized = _normalize_index_row(row, label=str(target["label"]), fallback_symbol=primary_symbol) if row else None
        if normalized:
            items.append(normalized)
            found_targets.add(str(target["label"]))

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


def _request_quote_rows(symbols: list[str]) -> dict[str, dict[str, Any]]:
    by_symbol: dict[str, dict[str, Any]] = {}
    if not symbols:
        return by_symbol
    try:
        rows = _rows(_request_payload("batch-quote", params={"symbols": ",".join(symbols)}))
    except Exception:
        rows = []
    by_symbol.update({_row_symbol(row): row for row in rows if _row_symbol(row)})
    return by_symbol


def _request_single_quote_row(symbol: str) -> dict[str, Any] | None:
    for endpoint in ("quote", "quote-short"):
        try:
            row = _latest_row(_rows(_request_payload(endpoint, params={"symbol": symbol})))
        except Exception:
            row = None
        if row:
            return row
    return None


def _build_snapshot_instruments(targets: tuple[dict[str, Any], ...]) -> list[dict[str, Any]]:
    symbols: list[str] = []
    for target in targets:
        symbols.extend(str(symbol) for symbol in target["symbols"])
    quote_rows = _request_quote_rows(symbols)

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


def _build_economics() -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for request in ECONOMIC_REQUESTS:
        selected_row: dict[str, Any] | None = None
        for name in request["candidates"]:
            try:
                rows = _rows(_request_payload("economic-indicators", params={"name": name}))
            except Exception:
                rows = []
            if rows:
                selected_row = _latest_row(rows)
                if selected_row:
                    break
        if not selected_row:
            continue
        value = _parse_float(selected_row.get("value"))
        if value is None:
            value = _parse_float(selected_row.get("indicator"))
        if value is None:
            value = _parse_float(selected_row.get("close"))
        if value is None:
            continue
        items.append(
            {
                "label": request["label"],
                "value": value,
                "date": _trimmed(selected_row.get("date")),
                "context_label": request["context_label"],
                "unit_label": request["unit_label"],
            }
        )
    return items


def _normalize_sector_row(row: dict[str, Any]) -> dict[str, Any] | None:
    sector = _trimmed(row.get("sector")) or _trimmed(row.get("name"))
    change_pct = _pick_first_numeric(
        row,
        (
            "averageChange",
            "avgChange",
            "changesPercentage",
            "changePercentage",
            "change_percent",
            "changePct",
        ),
    )
    if not sector or change_pct is None:
        return None
    return {"sector": sector, "change_pct": change_pct}


def _build_sector_performance() -> list[dict[str, Any]]:
    for offset in range(0, 6):
        target_date = (date.today() - timedelta(days=offset)).isoformat()
        try:
            rows = _rows(_request_payload("sector-performance-snapshot", params={"date": target_date}))
        except Exception:
            continue
        items = list(filter(None, (_normalize_sector_row(row) for row in rows)))
        if items:
            return items
    return []


def get_macro_snapshot() -> dict[str, Any]:
    cached = _cache_get("macro-snapshot")
    if cached is not None:
        return cached

    if not _api_key():
        return _cache_set("macro-snapshot", _empty_snapshot(status="unavailable"))

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
        commodities = _build_snapshot_instruments(COMMODITY_TARGETS)
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
            bool(economics),
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
    return _cache_set("macro-snapshot", payload)
