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
)
TREASURY_FIELD_ALIASES = {
    "5Y": ("year5", "5Y", "5y", "5Year", "5year", "year_5"),
    "10Y": ("year10", "10Y", "10y", "10Year", "10year", "year_10"),
    "30Y": ("year30", "30Y", "30y", "30Year", "30year", "year_30"),
}
ECONOMIC_REQUESTS = (
    ("GDP", ("GDP",)),
    ("Unemployment", ("unemployment rate", "unemploymentRate", "unemployment")),
    ("CPI", ("CPI", "inflation", "consumer price index")),
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
        "indexes": [],
        "treasury": [],
        "economics": [],
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


def _proxy_rows() -> dict[str, dict[str, Any]]:
    symbols = [str(target["proxy_symbol"]) for target in INDEX_TARGETS]
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


def _build_indexes() -> list[dict[str, Any]]:
    try:
        index_rows = _rows(_request_payload("batch-index-quotes"))
    except Exception:
        index_rows = []

    items: list[dict[str, Any]] = []
    found_targets: set[str] = set()
    for target in INDEX_TARGETS:
        primary_symbol = str(target["symbols"][0])
        row = _match_index_row(index_rows, target)
        if not row:
            row = _request_index_quote(primary_symbol)
        normalized = _normalize_index_row(row, label=str(target["label"]), fallback_symbol=primary_symbol) if row else None
        if normalized:
            items.append(normalized)
            found_targets.add(str(target["label"]))

    missing_targets = [target for target in INDEX_TARGETS if str(target["label"]) not in found_targets]
    if missing_targets:
        proxies = _proxy_rows()
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


def _build_treasury() -> list[dict[str, Any]]:
    payload = _request_payload("treasury-rates")
    row = _latest_row(_rows(payload))
    if not row:
        return []
    as_of = _trimmed(row.get("date"))
    items: list[dict[str, Any]] = []
    for label, aliases in TREASURY_FIELD_ALIASES.items():
        value = _pick_first_numeric(row, aliases)
        if value is None:
            continue
        items.append({"label": label, "value": value, "date": as_of})
    return items


def _build_economics() -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for label, candidates in ECONOMIC_REQUESTS:
        selected_row: dict[str, Any] | None = None
        for name in candidates:
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
        value = (
            _parse_float(selected_row.get("value"))
            or _parse_float(selected_row.get("indicator"))
            or _parse_float(selected_row.get("close"))
        )
        if value is None:
            continue
        items.append(
            {
                "label": label,
                "value": value,
                "date": _trimmed(selected_row.get("date")),
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

    indexes: list[dict[str, Any]] = []
    treasury: list[dict[str, Any]] = []
    economics: list[dict[str, Any]] = []
    sector_performance: list[dict[str, Any]] = []

    try:
        indexes = _build_indexes()
    except Exception:
        indexes = []

    try:
        treasury = _build_treasury()
    except Exception:
        treasury = []

    try:
        economics = _build_economics()
    except Exception:
        economics = []

    try:
        sector_performance = _build_sector_performance()
    except Exception:
        sector_performance = []

    available_sections = sum(bool(section) for section in [indexes, treasury, economics, sector_performance])
    if available_sections == 0:
        status = "unavailable"
    elif available_sections == 4:
        status = "ok"
    else:
        status = "partial"

    payload = {
        "indexes": indexes,
        "treasury": treasury,
        "economics": economics,
        "sector_performance": sector_performance,
        "status": status,
        "generated_at": _now_iso(),
    }
    return _cache_set("macro-snapshot", payload)
