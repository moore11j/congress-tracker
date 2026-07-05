from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Literal

import requests
from requests import RequestException
from sqlalchemy.orm import Session

from app.models import InsightsSnapshot

logger = logging.getLogger(__name__)

InsightQuoteGroup = Literal["global_markets", "commodities", "currencies", "crypto"]
InsightQuoteEndpoint = Literal["historical-chart/1min", "historical-price-eod/light"]

BASE_URL = "https://financialmodelingprep.com"
QUOTE_TTL = timedelta(minutes=10)
QUOTE_STALE_TTL = timedelta(hours=24)
CACHE_KIND_PREFIX = "insights-quote"


@dataclass(frozen=True)
class InsightQuoteConfig:
    label: str
    symbol: str
    display_symbol: str
    endpoint_type: InsightQuoteEndpoint


QUOTE_GROUPS: dict[InsightQuoteGroup, tuple[InsightQuoteConfig, ...]] = {
    "global_markets": (
        InsightQuoteConfig("China", "MCHI", "MCHI", "historical-chart/1min"),
        InsightQuoteConfig("Germany", "EWG", "EWG", "historical-chart/1min"),
        InsightQuoteConfig("Japan", "IJP.AX", "IJP", "historical-chart/1min"),
        InsightQuoteConfig("UK", "ISF.L", "ISF", "historical-chart/1min"),
        InsightQuoteConfig("Canada", "VFV.TO", "VFV", "historical-chart/1min"),
    ),
    "commodities": (
        InsightQuoteConfig("Gold", "GCUSD", "GCUSD", "historical-chart/1min"),
        InsightQuoteConfig("Silver", "SILUSD", "SILUSD", "historical-chart/1min"),
        InsightQuoteConfig("Brent Crude Oil", "BZUSD", "BZUSD", "historical-chart/1min"),
        InsightQuoteConfig("Copper", "HGUSD", "HGUSD", "historical-chart/1min"),
    ),
    "currencies": (
        InsightQuoteConfig("USD/CAD", "USDCAD", "USDCAD", "historical-chart/1min"),
        InsightQuoteConfig("EUR/USD", "EURUSD", "EURUSD", "historical-chart/1min"),
        InsightQuoteConfig("GBP/USD", "GBPUSD", "GBPUSD", "historical-chart/1min"),
        InsightQuoteConfig("USD/JPY", "USDJPY", "USDJPY", "historical-chart/1min"),
        InsightQuoteConfig("EUR/CAD", "EURCAD", "EURCAD", "historical-chart/1min"),
    ),
    "crypto": (
        InsightQuoteConfig("BTC/USD", "BTCUSD", "BTCUSD", "historical-chart/1min"),
        InsightQuoteConfig("ETH/USD", "ETHUSD", "ETHUSD", "historical-chart/1min"),
        InsightQuoteConfig("SOL/USD", "SOLUSD", "SOLUSD", "historical-chart/1min"),
        InsightQuoteConfig("XRP/USD", "XRPUSD", "XRPUSD", "historical-chart/1min"),
        InsightQuoteConfig("BNB/USD", "BNBUSD", "BNBUSD", "historical-chart/1min"),
    ),
}


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _aware(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _cache_kind(group: InsightQuoteGroup, config: InsightQuoteConfig) -> str:
    return f"{CACHE_KIND_PREFIX}:{group}:{config.symbol}:{config.endpoint_type}"


def _api_key() -> str:
    return os.getenv("FMP_API_KEY", "").strip()


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        parsed = float(value)
        return parsed if parsed == parsed else None
    if isinstance(value, str):
        cleaned = value.replace("$", "").replace(",", "").replace("%", "").strip()
        if not cleaned:
            return None
        try:
            parsed = float(cleaned)
        except ValueError:
            return None
        return parsed if parsed == parsed else None
    return None


def _number_from(record: dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        if key in record:
            value = _coerce_float(record.get(key))
            if value is not None:
                return value
    return None


def _first_record(payload: Any) -> dict[str, Any] | None:
    if isinstance(payload, list):
        for item in payload:
            if isinstance(item, dict):
                return item
        return None
    if isinstance(payload, dict):
        for key in ("data", "quote", "result", "results"):
            value = payload.get(key)
            if isinstance(value, list):
                nested = _first_record(value)
                if nested:
                    return nested
            if isinstance(value, dict):
                return value
        return payload
    return None


def _as_of_from_record(record: dict[str, Any], fallback: datetime) -> str:
    timestamp = _number_from(record, "timestamp")
    if timestamp is not None and timestamp > 0:
        try:
            return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()
        except (OverflowError, OSError, ValueError):
            pass
    for key in ("as_of", "asOf", "updated_at", "updatedAt", "date", "publishedDate"):
        raw = record.get(key)
        if raw is None:
            continue
        cleaned = str(raw).strip()
        if cleaned:
            return cleaned
    return fallback.isoformat()


def _unavailable_item(group: InsightQuoteGroup, config: InsightQuoteConfig) -> dict[str, Any]:
    return {
        "group": group,
        "label": config.label,
        "symbol": config.symbol,
        "display_symbol": config.display_symbol,
        "price": None,
        "change": None,
        "change_percent": None,
        "volume": None,
        "as_of": None,
        "status": "unavailable",
    }


def normalize_insights_quote_response(
    group: InsightQuoteGroup,
    config: InsightQuoteConfig,
    payload: Any,
    *,
    fetched_at: datetime | None = None,
) -> dict[str, Any]:
    record = _first_record(payload)
    if not record:
        return _unavailable_item(group, config)

    now = fetched_at or _utcnow()
    price = _number_from(record, "price", "close", "last", "lastPrice", "bid", "ask")
    item = {
        "group": group,
        "label": config.label,
        "symbol": config.symbol,
        "display_symbol": config.display_symbol,
        "price": price,
        "change": _number_from(record, "change", "changes", "dayChange", "changeValue"),
        "change_percent": _number_from(
            record,
            "changesPercentage",
            "changePercentage",
            "changePercent",
            "change_percent",
            "change_pct",
        ),
        "volume": _number_from(record, "volume"),
        "as_of": _as_of_from_record(record, now) if price is not None else None,
        "status": "ok" if price is not None else "unavailable",
    }
    return item


def _load_cached_item(db: Session, group: InsightQuoteGroup, config: InsightQuoteConfig) -> tuple[InsightsSnapshot | None, dict[str, Any] | None]:
    row = db.get(InsightsSnapshot, _cache_kind(group, config))
    if row is None:
        return None, None
    try:
        payload = json.loads(row.payload_json or "{}")
    except Exception:
        payload = None
    return row, payload if isinstance(payload, dict) else None


def _is_fresh(row: InsightsSnapshot | None) -> bool:
    fetched_at = _aware(row.fetched_at) if row else None
    return fetched_at is not None and (_utcnow() - fetched_at) <= QUOTE_TTL


def _is_stale_usable(row: InsightsSnapshot | None) -> bool:
    fetched_at = _aware(row.fetched_at) if row else None
    return fetched_at is not None and (_utcnow() - fetched_at) <= QUOTE_STALE_TTL


def _store_cached_item(db: Session, group: InsightQuoteGroup, config: InsightQuoteConfig, item: dict[str, Any]) -> None:
    now = _utcnow()
    kind = _cache_kind(group, config)
    row = db.get(InsightsSnapshot, kind)
    if row is None:
        db.add(
            InsightsSnapshot(
                kind=kind,
                payload_json=json.dumps(item),
                source="market_quote",
                fetched_at=now,
            )
        )
    else:
        row.payload_json = json.dumps(item)
        row.source = "market_quote"
        row.fetched_at = now
        row.updated_at = now
    db.commit()


def _fetch_quote(group: InsightQuoteGroup, config: InsightQuoteConfig) -> dict[str, Any]:
    api_key = _api_key()
    if not api_key:
        raise RuntimeError("missing market data API key")
    today = _utcnow().date()
    endpoints = (config.endpoint_type, "historical-price-eod/light")
    last_response: requests.Response | None = None
    for endpoint_type in dict.fromkeys(endpoints):
        params = {"symbol": config.symbol, "apikey": api_key}
        if endpoint_type == "historical-chart/1min":
            params["from"] = (today - timedelta(days=7)).isoformat()
            params["to"] = today.isoformat()
        response = requests.get(
            f"{BASE_URL}/stable/{endpoint_type}",
            params=params,
            timeout=10,
        )
        last_response = response
        response.raise_for_status()
        item = normalize_insights_quote_response(group, config, response.json())
        if item.get("status") == "ok":
            return item
    if last_response is not None:
        last_response.raise_for_status()
    return _unavailable_item(group, config)


def _quote_item(db: Session, group: InsightQuoteGroup, config: InsightQuoteConfig) -> dict[str, Any]:
    row, cached = _load_cached_item(db, group, config)
    if cached and _is_fresh(row):
        return cached
    try:
        item = _fetch_quote(group, config)
        _store_cached_item(db, group, config, item)
        return item
    except RequestException as exc:
        logger.warning(
            "insights_quote_fetch_failed group=%s symbol=%s endpoint=%s error=%s",
            group,
            config.symbol,
            config.endpoint_type,
            exc.__class__.__name__,
        )
    except Exception as exc:
        logger.warning(
            "insights_quote_unavailable group=%s symbol=%s endpoint=%s error=%s",
            group,
            config.symbol,
            config.endpoint_type,
            exc.__class__.__name__,
        )
    if cached and _is_stale_usable(row) and cached.get("status") == "ok":
        return cached
    return _unavailable_item(group, config)


def _latest_timestamp(items: list[dict[str, Any]]) -> str | None:
    latest: datetime | None = None
    latest_raw: str | None = None
    for item in items:
        raw = item.get("as_of")
        if not raw:
            continue
        try:
            parsed = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        except ValueError:
            continue
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        parsed = parsed.astimezone(timezone.utc)
        if latest is None or parsed > latest:
            latest = parsed
            latest_raw = parsed.isoformat()
    return latest_raw


def get_insights_quote_overview(db: Session) -> dict[str, Any]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    all_items: list[dict[str, Any]] = []
    for group, configs in QUOTE_GROUPS.items():
        items = [_quote_item(db, group, config) for config in configs]
        grouped[group] = items
        all_items.extend(items)
    return {
        "global_markets": grouped["global_markets"],
        "commodities": grouped["commodities"],
        "currencies": grouped["currencies"],
        "crypto": grouped["crypto"],
        "updated_at": _latest_timestamp(all_items),
    }
