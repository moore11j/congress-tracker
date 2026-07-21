from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Literal

import requests
from requests import RequestException
from sqlalchemy.orm import Session

from app.models import InsightsSnapshot

logger = logging.getLogger(__name__)

InsightQuoteGroup = Literal["global_markets", "commodities", "currencies", "crypto"]
InsightQuoteEndpoint = Literal["yahoo_chart", "frankfurter", "coingecko", "silv"]

YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart"
FRANKFURTER_SERIES_URL = "https://api.frankfurter.dev/v2/rates"
COINGECKO_PRICE_URL = "https://api.coingecko.com/api/v3/simple/price"
SILV_COMMODITIES_URL = "https://data.silv.app/commodities.json"
QUOTE_TTL = timedelta(minutes=10)
QUOTE_STALE_TTL = timedelta(hours=24)
CACHE_KIND_PREFIX = "insights-quote"


@dataclass(frozen=True)
class InsightQuoteConfig:
    label: str
    symbol: str
    display_symbol: str
    endpoint_type: InsightQuoteEndpoint
    provider_symbol: str | None = None


QUOTE_GROUPS: dict[InsightQuoteGroup, tuple[InsightQuoteConfig, ...]] = {
    "global_markets": (
        InsightQuoteConfig("MSCI ACWI", "ACWI", "ACWI", "yahoo_chart"),
        InsightQuoteConfig("China", "MCHI", "MCHI", "yahoo_chart"),
        InsightQuoteConfig("Germany", "EWG", "EWG", "yahoo_chart"),
        InsightQuoteConfig("Japan", "IJP", "IJP", "yahoo_chart", "IJP.AX"),
        InsightQuoteConfig("UK", "ISF", "ISF", "yahoo_chart", "ISF.L"),
        InsightQuoteConfig("Canada", "VFV", "VFV", "yahoo_chart", "VFV.TO"),
    ),
    "commodities": (
        InsightQuoteConfig("Gold", "GCUSD", "GCUSD", "silv", "gold"),
        InsightQuoteConfig("Silver", "SILUSD", "SILUSD", "silv", "silver"),
        InsightQuoteConfig("Copper", "HGUSD", "HGUSD", "silv", "copper"),
    ),
    "currencies": (
        InsightQuoteConfig("DXY", "DXY", "DXY", "frankfurter"),
        InsightQuoteConfig("USD/CAD", "USDCAD", "USDCAD", "frankfurter"),
        InsightQuoteConfig("EUR/USD", "EURUSD", "EURUSD", "frankfurter"),
        InsightQuoteConfig("GBP/USD", "GBPUSD", "GBPUSD", "frankfurter"),
        InsightQuoteConfig("USD/JPY", "USDJPY", "USDJPY", "frankfurter"),
        InsightQuoteConfig("EUR/CAD", "EURCAD", "EURCAD", "frankfurter"),
    ),
    "crypto": (
        InsightQuoteConfig("BTC/USD", "BTCUSD", "BTCUSD", "coingecko", "bitcoin"),
        InsightQuoteConfig("ETH/USD", "ETHUSD", "ETHUSD", "coingecko", "ethereum"),
        InsightQuoteConfig("SOL/USD", "SOLUSD", "SOLUSD", "coingecko", "solana"),
        InsightQuoteConfig("XRP/USD", "XRPUSD", "XRPUSD", "coingecko", "ripple"),
        InsightQuoteConfig("BNB/USD", "BNBUSD", "BNBUSD", "coingecko", "binancecoin"),
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


def _legacy_cache_kinds(group: InsightQuoteGroup, config: InsightQuoteConfig) -> tuple[str, ...]:
    candidates = [f"{CACHE_KIND_PREFIX}:{group}:{config.symbol}:historical-chart/1min"]
    if config.provider_symbol:
        candidates.append(f"{CACHE_KIND_PREFIX}:{group}:{config.provider_symbol}:historical-chart/1min")
    return tuple(dict.fromkeys(candidates))


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
    rows = [db.get(InsightsSnapshot, _cache_kind(group, config))]
    rows.extend(db.get(InsightsSnapshot, key) for key in _legacy_cache_kinds(group, config))
    row = next((candidate for candidate in rows if candidate is not None), None)
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


def _quote_item_payload(
    group: InsightQuoteGroup,
    config: InsightQuoteConfig,
    *,
    price: float | None,
    previous: float | None = None,
    change: float | None = None,
    change_percent: float | None = None,
    volume: float | None = None,
    as_of: str | None = None,
) -> dict[str, Any]:
    computed_change = change
    computed_change_percent = change_percent
    if price is not None and previous not in (None, 0):
        computed_change = price - previous
        computed_change_percent = (computed_change / previous) * 100
    return {
        "group": group,
        "label": config.label,
        "symbol": config.symbol,
        "display_symbol": config.display_symbol,
        "price": price,
        "change": computed_change,
        "change_percent": computed_change_percent,
        "volume": volume,
        "as_of": as_of,
        "status": "ok" if price is not None else "unavailable",
    }


def _fetch_yahoo_chart(group: InsightQuoteGroup, config: InsightQuoteConfig) -> dict[str, Any]:
    provider_symbol = config.provider_symbol or config.symbol
    response = requests.get(
        f"{YAHOO_CHART_URL}/{provider_symbol}",
        params={"range": "5d", "interval": "1d"},
        timeout=10,
        headers={"User-Agent": "WalnutMarkets/1.0"},
    )
    response.raise_for_status()
    payload = response.json()
    result = ((payload.get("chart") or {}).get("result") or [None])[0]
    if not isinstance(result, dict):
        return _unavailable_item(group, config)
    meta = result.get("meta") if isinstance(result.get("meta"), dict) else {}
    quote = (((result.get("indicators") or {}).get("quote") or [None])[0]) if isinstance(result.get("indicators"), dict) else {}
    closes = [value for value in (quote.get("close") if isinstance(quote, dict) else []) if _coerce_float(value) is not None]
    price = _coerce_float(meta.get("regularMarketPrice")) or (_coerce_float(closes[-1]) if closes else None)
    previous = _coerce_float(meta.get("chartPreviousClose")) or _coerce_float(meta.get("previousClose"))
    if previous is None and len(closes) >= 2:
        previous = _coerce_float(closes[-2])
    timestamps = result.get("timestamp") if isinstance(result.get("timestamp"), list) else []
    as_of = None
    if timestamps:
        timestamp = _coerce_float(timestamps[-1])
        if timestamp is not None:
            as_of = datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()
    return _quote_item_payload(group, config, price=price, previous=previous, as_of=as_of or _utcnow().isoformat())


def _frankfurter_series() -> list[tuple[str, dict[str, float]]]:
    end = _utcnow().date()
    start = end - timedelta(days=10)
    response = requests.get(
        FRANKFURTER_SERIES_URL,
        params={"from": start.isoformat(), "base": "USD", "quotes": "CAD,JPY,SEK,CHF,EUR,GBP"},
        timeout=10,
    )
    response.raise_for_status()
    rows: list[tuple[str, dict[str, float]]] = []
    payload = response.json()
    if isinstance(payload, list):
        for item in payload:
            if not isinstance(item, dict):
                continue
            date_key = str(item.get("date") or "")
            quote = str(item.get("quote") or "")
            rate = _coerce_float(item.get("rate"))
            if not date_key or not quote or rate is None:
                continue
            if not rows or rows[-1][0] != date_key:
                rows.append((date_key, {}))
            rows[-1][1][quote] = rate
        return [(date_key, rates) for date_key, rates in rows if rates]

    rates_by_date = payload.get("rates") if isinstance(payload, dict) else None
    if not isinstance(rates_by_date, dict):
        return []
    for date_key in sorted(rates_by_date):
        raw = rates_by_date.get(date_key)
        if not isinstance(raw, dict):
            continue
        parsed = {key: value for key, value in ((key, _coerce_float(raw.get(key))) for key in raw) if value is not None}
        if parsed:
            rows.append((date_key, parsed))
    return rows


def _dxy_value(rates: dict[str, float]) -> float | None:
    try:
        eur_usd = 1 / rates["EUR"]
        usd_jpy = rates["JPY"]
        gbp_usd = 1 / rates["GBP"]
        usd_cad = rates["CAD"]
        usd_sek = rates["SEK"]
        usd_chf = rates["CHF"]
    except (KeyError, ZeroDivisionError):
        return None
    return 50.14348112 * (eur_usd ** -0.576) * (usd_jpy ** 0.136) * (gbp_usd ** -0.119) * (usd_cad ** 0.091) * (usd_sek ** 0.042) * (usd_chf ** 0.036)


def _currency_value(symbol: str, rates: dict[str, float]) -> float | None:
    try:
        if symbol == "DXY":
            return _dxy_value(rates)
        if symbol == "USDCAD":
            return rates["CAD"]
        if symbol == "EURUSD":
            return 1 / rates["EUR"]
        if symbol == "GBPUSD":
            return 1 / rates["GBP"]
        if symbol == "USDJPY":
            return rates["JPY"]
        if symbol == "EURCAD":
            return rates["CAD"] / rates["EUR"]
    except (KeyError, ZeroDivisionError):
        return None
    return None


def _fetch_frankfurter_quote(group: InsightQuoteGroup, config: InsightQuoteConfig) -> dict[str, Any]:
    rows = _frankfurter_series()
    if not rows:
        return _unavailable_item(group, config)
    latest_date, latest_rates = rows[-1]
    previous_rates = rows[-2][1] if len(rows) >= 2 else {}
    price = _currency_value(config.symbol, latest_rates)
    previous = _currency_value(config.symbol, previous_rates) if previous_rates else None
    return _quote_item_payload(group, config, price=price, previous=previous, as_of=latest_date)


def _fetch_coingecko_quote(group: InsightQuoteGroup, config: InsightQuoteConfig) -> dict[str, Any]:
    coin_id = config.provider_symbol or config.symbol.lower()
    response = requests.get(
        COINGECKO_PRICE_URL,
        params={"ids": coin_id, "vs_currencies": "usd", "include_24hr_vol": "true", "include_24hr_change": "true"},
        timeout=10,
    )
    response.raise_for_status()
    record = response.json().get(coin_id)
    if not isinstance(record, dict):
        return _unavailable_item(group, config)
    price = _coerce_float(record.get("usd"))
    change_percent = _coerce_float(record.get("usd_24h_change"))
    change = (price * change_percent / 100) if price is not None and change_percent is not None else None
    return _quote_item_payload(
        group,
        config,
        price=price,
        change=change,
        change_percent=change_percent,
        volume=_coerce_float(record.get("usd_24h_vol")),
        as_of=_utcnow().isoformat(),
    )


def _fetch_silv_quote(group: InsightQuoteGroup, config: InsightQuoteConfig) -> dict[str, Any]:
    commodity_key = config.provider_symbol or config.symbol.lower()
    response = requests.get(SILV_COMMODITIES_URL, timeout=10)
    response.raise_for_status()
    payload = response.json()
    commodities = payload.get("commodities") if isinstance(payload, dict) else None
    record = commodities.get(commodity_key) if isinstance(commodities, dict) else None
    if not isinstance(record, dict):
        return _unavailable_item(group, config)
    change_24h = record.get("change_24h") if isinstance(record.get("change_24h"), dict) else {}
    return _quote_item_payload(
        group,
        config,
        price=_coerce_float(record.get("price")),
        change=_coerce_float(change_24h.get("amount")),
        change_percent=_coerce_float(change_24h.get("percent")),
        as_of=str(record.get("last_updated") or record.get("timestamp") or _utcnow().isoformat()),
    )


def _fetch_quote(group: InsightQuoteGroup, config: InsightQuoteConfig) -> dict[str, Any]:
    if config.endpoint_type == "yahoo_chart":
        return _fetch_yahoo_chart(group, config)
    if config.endpoint_type == "frankfurter":
        return _fetch_frankfurter_quote(group, config)
    if config.endpoint_type == "coingecko":
        return _fetch_coingecko_quote(group, config)
    if config.endpoint_type == "silv":
        return _fetch_silv_quote(group, config)
    return _unavailable_item(group, config)


def _quote_item(db: Session, group: InsightQuoteGroup, config: InsightQuoteConfig) -> dict[str, Any]:
    row, cached = _load_cached_item(db, group, config)
    if cached and _is_fresh(row) and cached.get("status") == "ok":
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
