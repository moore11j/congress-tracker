from __future__ import annotations

import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, time as datetime_time, timedelta, timezone
from types import SimpleNamespace
from typing import Any

import requests
from sqlalchemy import func, text
from sqlalchemy.dialects.postgresql import insert as postgres_insert
from sqlalchemy.exc import OperationalError
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from app.db import SessionLocal
from app.clients.fmp import FMP_BASE_URL
from app.services.provider_usage import (
    ProviderUnavailable,
    ensure_fmp_live_allowed,
    record_cache_hit,
    record_cache_miss,
    record_fallback,
    record_provider_response,
)
from app.services.provider_endpoints import build_fmp_endpoint_request, fmp_endpoint_requests_for_domain
from app.services.provider_registry import FMP_EOD_LIGHT_QUOTE_CONTRACT_JSON, FMP_INTRADAY_CHART_CONTRACT_JSON
from app.services.data_enrichment_queue import enqueue_data_enrichment_job
from app.models import PriceCache, QuoteCache
from app.utils.symbols import normalize_symbol

logger = logging.getLogger(__name__)

_QUOTE_CACHE: dict[str, tuple[float, float]] = {}
_QUOTE_META_CACHE: dict[str, tuple[dict[str, Any], float]] = {}
_MISS_CACHE: dict[str, float] = {}
_QUOTE_FETCH_LOCKS: dict[str, threading.Lock] = {}
_QUOTE_FETCH_LOCKS_GUARD = threading.Lock()
_QUOTE_BUDGET_LOCK = threading.Lock()
_QUOTE_CALL_TIMESTAMPS: list[float] = []
_last_paywall_log: datetime | None = None
_last_quotes_disable_log: datetime | None = None
_quotes_disabled_until: datetime | None = None
_quotes_disable_reason: str | None = None

_QUOTE_EOD_SANITY_MAX_AGE_DAYS = 10
_QUOTE_EOD_SANITY_MAX_DEVIATION = 0.50


def _miss_cache_hit(symbol: str) -> bool:
    exp = _MISS_CACHE.get(symbol)
    if not exp:
        return False
    if time.time() >= exp:
        _MISS_CACHE.pop(symbol, None)
        return False
    return True


def _miss_cache_set(symbol: str, seconds: int = 3600) -> None:
    _MISS_CACHE[symbol] = time.time() + max(60, seconds)


def _cache_ttl_seconds(*, lane: str | None = None, ttl_seconds: int | None = None) -> int:
    if ttl_seconds is not None:
        return max(int(ttl_seconds), 1)
    lane_env = {
        "ticker_quote": "QUOTE_LIVE_TTL_SECONDS",
        "feed_quote": "QUOTE_FEED_TTL_SECONDS",
        "pnl_quote": "QUOTE_FEED_TTL_SECONDS",
        "watchlist_quote": "QUOTE_WATCHLIST_TTL_SECONDS",
    }.get(lane or "")
    default_by_lane = {
        "ticker_quote": "30",
        "feed_quote": "60",
        "pnl_quote": "60",
        "watchlist_quote": "60",
    }.get(lane or "", "30")
    try:
        raw = os.getenv(lane_env) if lane_env else None
        ttl = int(raw or os.getenv("QUOTE_CACHE_TTL_SECONDS", default_by_lane) or default_by_lane)
    except ValueError:
        ttl = int(default_by_lane)
    return max(ttl, 1)


def _quote_process_budget_per_minute() -> int:
    raw = os.getenv("FMP_QUOTE_PROCESS_CALLS_PER_MINUTE") or os.getenv("FMP_QUOTE_CALLS_PER_MINUTE")
    if raw:
        try:
            return max(1, int(raw))
        except ValueError:
            logger.info("quote_budget invalid env value=%s", raw)
    try:
        global_budget = max(1, int(os.getenv("FMP_PLAN_CALLS_PER_MINUTE", os.getenv("FMP_CALLS_PER_MINUTE", "500"))))
    except ValueError:
        global_budget = 500
    try:
        machine_count = max(1, int(os.getenv("FMP_QUOTE_BUDGET_MACHINE_COUNT", "3")))
    except ValueError:
        machine_count = 3
    return max(1, int(global_budget * 0.9 / machine_count))


def _quote_budget_allows(*, lane: str, symbol: str) -> bool:
    now = time.time()
    cutoff = now - 60.0
    limit = _quote_process_budget_per_minute()
    with _QUOTE_BUDGET_LOCK:
        while _QUOTE_CALL_TIMESTAMPS and _QUOTE_CALL_TIMESTAMPS[0] < cutoff:
            _QUOTE_CALL_TIMESTAMPS.pop(0)
        if len(_QUOTE_CALL_TIMESTAMPS) >= limit:
            logger.info("quote_budget_exhausted lane=%s symbol=%s used=%s limit=%s", lane, symbol, len(_QUOTE_CALL_TIMESTAMPS), limit)
            record_fallback(category=lane, symbol=symbol, reason="provider_budget_exceeded")
            return False
        _QUOTE_CALL_TIMESTAMPS.append(now)
    return True


def _fetch_lock_for_symbol(symbol: str) -> threading.Lock:
    with _QUOTE_FETCH_LOCKS_GUARD:
        return _QUOTE_FETCH_LOCKS.setdefault(symbol, threading.Lock())


def _quotes_disabled() -> bool:
    return _quotes_disabled_until is not None and datetime.now(timezone.utc) < _quotes_disabled_until


def _quotes_disabled_status() -> str | None:
    if not _quotes_disabled():
        return None
    if _quotes_disable_reason and "402" in _quotes_disable_reason:
        return "provider_402"
    if _quotes_disable_reason and "429" in _quotes_disable_reason:
        return "provider_429"
    return "provider_unavailable"


def _disable_quotes(minutes: int, reason: str) -> None:
    global _quotes_disabled_until, _quotes_disable_reason, _last_quotes_disable_log
    now = datetime.now(timezone.utc)
    _quotes_disabled_until = now + timedelta(minutes=minutes)
    _quotes_disable_reason = reason
    if _last_quotes_disable_log is None or (now - _last_quotes_disable_log) > timedelta(hours=1):
        logger.warning(
            "quote_lookup quotes_disabled reason=%s until=%s",
            reason,
            _quotes_disabled_until.isoformat(),
        )
        _last_quotes_disable_log = now


def _enqueue_quote_refreshes(symbols: list[str], *, reason: str) -> None:
    for symbol in symbols:
        enqueue_data_enrichment_job(
            job_type="quote",
            symbol=symbol,
            source="page_load",
            reason=reason,
            priority=20,
        )


def _cache_get_meta(symbol: str) -> dict[str, Any] | None:
    cached_meta = _QUOTE_META_CACHE.get(symbol)
    if cached_meta:
        meta, expires_at = cached_meta
        if time.time() < expires_at:
            return dict(meta)
        _QUOTE_META_CACHE.pop(symbol, None)
    cached = _QUOTE_CACHE.get(symbol)
    if not cached:
        return None
    price, expires_at = cached
    if time.time() >= expires_at:
        _QUOTE_CACHE.pop(symbol, None)
        return None
    return {"symbol": symbol, "price": price, "asof_ts": None, "is_stale": False, "source": "cache"}


def cache_get(symbol: str) -> float | None:
    meta = _cache_get_meta(symbol)
    if not meta or meta.get("price") is None:
        return None
    return float(meta["price"])


def cache_set(symbol: str, price: float) -> None:
    expires_at = time.time() + _cache_ttl_seconds()
    _QUOTE_CACHE[symbol] = (price, expires_at)
    _QUOTE_META_CACHE[symbol] = (
        {"symbol": symbol, "price": price, "asof_ts": None, "is_stale": False, "source": "cache"},
        expires_at,
    )


def _cache_set_meta(symbol: str, meta: dict[str, Any], *, lane: str | None, ttl_seconds: int | None = None) -> None:
    price = meta.get("price")
    if price is None:
        return
    ttl = _cache_ttl_seconds(lane=lane, ttl_seconds=ttl_seconds)
    expires_at = time.time() + ttl
    normalized_meta = {**meta, "symbol": symbol, "is_stale": False, "source": meta.get("source") or "cache"}
    _QUOTE_CACHE[symbol] = (float(price), expires_at)
    _QUOTE_META_CACHE[symbol] = (normalized_meta, expires_at)






def _network_fetch_cap() -> int:
    try:
        cap = int(os.getenv("QUOTE_LOOKUP_MAX_FETCH", "25"))
    except ValueError:
        cap = 25
    return max(cap, 1)


def _bounded_network_fetch_cap(value: int | None) -> int:
    if value is None:
        return _network_fetch_cap()
    try:
        cap = int(value)
    except (TypeError, ValueError):
        return _network_fetch_cap()
    return max(cap, 1)


def _log_capped_fetch(
    *,
    requested: int,
    cached: int,
    cap: int,
    dropped_symbols: list[str],
) -> None:
    preview = dropped_symbols[:10]
    logger.info(
        "quote_lookup capped requested=%s cached=%s fetch_cap=%s dropped=%s symbols=%s capped",
        requested,
        cached,
        cap,
        len(dropped_symbols),
        preview,
    )


def _freshness_datetime(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is not None:
        return value.astimezone(timezone.utc).replace(tzinfo=None)
    return value


def _latest_cached_eod_for_quote_sanity(db: Session, symbol: str) -> tuple[float, datetime] | None:
    normalized = normalize_symbol(symbol)
    if not normalized:
        return None
    try:
        row = (
            db.query(PriceCache.date, PriceCache.close)
            .filter(PriceCache.symbol == normalized)
            .order_by(PriceCache.date.desc())
            .first()
        )
    except Exception:
        return None
    if row is None or row.close is None:
        return None
    try:
        close = float(row.close)
    except (TypeError, ValueError):
        return None
    if close <= 0:
        return None
    try:
        close_date = datetime.strptime(str(row.date)[:10], "%Y-%m-%d").date()
    except ValueError:
        return None
    if (datetime.now(timezone.utc).date() - close_date).days > _QUOTE_EOD_SANITY_MAX_AGE_DAYS:
        return None
    return close, datetime.combine(close_date, datetime_time.min, tzinfo=timezone.utc)


def _quote_meta_with_eod_sanity(
    db: Session,
    symbol: str,
    meta: dict[str, Any],
    *,
    lane: str,
) -> dict[str, Any]:
    try:
        price = float(meta.get("price"))
    except (TypeError, ValueError):
        return meta
    if price <= 0:
        return meta
    latest = _latest_cached_eod_for_quote_sanity(db, symbol)
    if latest is None:
        return meta
    eod_close, eod_asof = latest
    deviation = abs(price - eod_close) / eod_close
    if deviation <= _QUOTE_EOD_SANITY_MAX_DEVIATION:
        return meta

    logger.warning(
        "quote_sanity_eod_fallback lane=%s symbol=%s quote_price=%s eod_close=%s deviation=%.3f",
        lane,
        symbol,
        price,
        eod_close,
        deviation,
    )
    sanitized = {
        **meta,
        "price": eod_close,
        "asof_ts": eod_asof,
        "provider_timestamp": meta.get("provider_timestamp"),
        "quote_sanity_original_price": price,
        "quote_sanity_source": meta.get("source"),
        "source": "eod_sanity_fallback",
    }
    return sanitized


def quote_cache_get_many(db: Session, symbols: list[str]) -> dict[str, float]:
    return {sym: price for sym, (price, _asof) in quote_cache_get_many_with_age(db, symbols).items()}


def quote_cache_get_many_with_age(db: Session, symbols: list[str]) -> dict[str, tuple[float, datetime]]:
    if not symbols:
        return {}
    rows = (
        db.query(QuoteCache.symbol, QuoteCache.price, QuoteCache.asof_ts)
        .filter(QuoteCache.symbol.in_(symbols))
        .all()
    )
    result: dict[str, tuple[float, datetime]] = {}
    for sym, price, asof in rows:
        normalized_asof = _freshness_datetime(asof)
        if sym and price is not None and normalized_asof is not None:
            result[sym] = (float(price), normalized_asof)
    return result


def quote_cache_upsert_many(db: Session, prices: dict[str, float], market_caps: dict[str, float | None] | None = None) -> None:
    if not prices:
        return
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    rows = [
        {"symbol": sym, "price": float(price), "asof_ts": now, "market_cap": market_caps.get(sym) if market_caps else None}
        for sym, price in prices.items()
    ]
    insert_fn = postgres_insert if db.get_bind().dialect.name == "postgresql" else sqlite_insert
    stmt = insert_fn(QuoteCache.__table__).values(rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=["symbol"],
        set_={
            "price": stmt.excluded.price,
            "asof_ts": stmt.excluded.asof_ts,
            "market_cap": func.coalesce(stmt.excluded.market_cap, QuoteCache.market_cap),
        },
    )
    try:
        if db.bind and db.bind.dialect.name == "sqlite":
            # Fail fast under lock contention so read paths do not block on cache writes.
            db.execute(text("PRAGMA busy_timeout = 0"))
        db.execute(stmt)
        db.commit()
    except OperationalError as exc:
        db.rollback()
        logger.warning(
            "quote_lookup sqlite_upsert_skipped reason=lock_or_busy rows=%s error=%s",
            len(rows),
            exc.__class__.__name__,
        )
    except Exception:
        db.rollback()
        logger.exception("quote_lookup sqlite_upsert_failed rows=%s", len(rows))


def _payload_rows(payload: object) -> list[dict]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if isinstance(payload, dict):
        for key in ("data", "historical", "results", "quotes"):
            value = payload.get(key)
            if isinstance(value, list):
                return [row for row in value if isinstance(row, dict)]
            if isinstance(value, dict):
                return [value]
        return [payload] if payload else []
    return []


def _response_config(endpoint_contract: dict | None) -> dict:
    if isinstance(endpoint_contract, dict) and isinstance(endpoint_contract.get("response"), dict):
        return endpoint_contract["response"]
    return {}


def _price_fields(response_config: dict) -> tuple[str, ...]:
    fields: list[str] = []
    price_field = response_config.get("price_field")
    if isinstance(price_field, str) and price_field.strip():
        fields.append(price_field.strip())
    fallback_fields = response_config.get("fallback_price_fields")
    if isinstance(fallback_fields, list):
        fields.extend(str(field).strip() for field in fallback_fields if str(field).strip())
    fields.extend(["price", "close", "adjClose", "previousClose", "last", "bid", "ask"])
    return tuple(dict.fromkeys(fields))


def _python_date_format(value: object, fallback: str) -> str:
    aliases = {
        "YYYY-MM-DD": "%Y-%m-%d",
        "YYYY-MM-DD HH:MM:SS": "%Y-%m-%d %H:%M:%S",
    }
    if not value:
        return fallback
    text = str(value)
    return aliases.get(text, text)


def _parse_response_asof(row: dict, response_config: dict) -> datetime | None:
    date_field = str(response_config.get("date_field") or "date").strip()
    raw_value = row.get(date_field) if date_field else None
    if raw_value is None:
        return None
    if isinstance(raw_value, datetime):
        return raw_value.astimezone(timezone.utc).replace(tzinfo=None) if raw_value.tzinfo else raw_value
    raw_text = str(raw_value).strip()
    if not raw_text:
        return None
    configured_format = response_config.get("date_format")
    formats = [
        _python_date_format(configured_format, "%Y-%m-%d %H:%M:%S"),
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ]
    for fmt in dict.fromkeys(formats):
        try:
            return datetime.strptime(raw_text, fmt)
        except ValueError:
            continue
    try:
        parsed = datetime.fromisoformat(raw_text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(timezone.utc).replace(tzinfo=None) if parsed.tzinfo else parsed


def _numeric_price(row: dict, *, response_config: dict | None = None) -> float | None:
    for key in _price_fields(response_config or {}):
        value = row.get(key)
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            continue
        if parsed == parsed and parsed > 0:
            return parsed
    return None


def _numeric_field(row: dict, *keys: str) -> float | None:
    for key in keys:
        value = row.get(key)
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            continue
        if parsed == parsed:
            return parsed
    return None


def _rows_to_quote_payload(rows: list[dict], *, fallback_symbol: str, endpoint_contract: dict | None = None) -> list[dict]:
    response_config = _response_config(endpoint_contract)
    symbol_field = str(response_config.get("symbol_field") or "symbol").strip() or "symbol"
    payload: list[dict] = []
    for row in rows:
        price = _numeric_price(row, response_config=response_config)
        if price is None:
            continue
        symbol = normalize_symbol(row.get(symbol_field)) or fallback_symbol
        parsed = {"symbol": symbol, "price": price}
        asof_ts = _parse_response_asof(row, response_config)
        if asof_ts is not None:
            parsed["asof_ts"] = asof_ts
        change = _numeric_field(row, "change", "changes", "dayChange")
        if change is not None:
            parsed["change"] = change
        change_percent = _numeric_field(row, "changesPercentage", "changePercentage", "changePercent", "changes_pct")
        if change_percent is not None:
            parsed["change_percent"] = change_percent
        volume = _numeric_field(row, "volume")
        if volume is not None:
            parsed["volume"] = volume
        market_cap = _numeric_field(row, "marketCap", "market_cap", "mktCap")
        if market_cap is not None:
            parsed["market_cap"] = market_cap
        payload.append(parsed)
    return payload

def get_index_quote(symbol: str) -> float:
    """Fetch current index price (e.g. SPY) from FMP chart/EOD endpoints."""
    api_key = os.getenv("FMP_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("FMP_API_KEY not configured")

    ensure_fmp_live_allowed(category="quote:index", symbol=symbol)
    today = datetime.now(timezone.utc).date()
    last_response: requests.Response | None = None
    for endpoint, params in (
        (
            "historical-chart/1min",
            {
                "symbol": symbol,
                "from": (today - timedelta(days=7)).isoformat(),
                "to": today.isoformat(),
                "apikey": api_key,
            },
        ),
        ("historical-price-eod/light", {"symbol": symbol, "apikey": api_key}),
    ):
        response = requests.get(
            f"{FMP_BASE_URL}/{endpoint}",
            params=params,
            timeout=10,
        )
        last_response = response
        record_provider_response(category="quote:index", symbol=symbol, status_code=response.status_code)
        response.raise_for_status()
        data = response.json()
        rows = data if isinstance(data, list) else []
        if rows and isinstance(rows[0], dict):
            price = rows[0].get("close", rows[0].get("price"))
            if price is not None:
                return float(price)

    if last_response is not None:
        last_response.raise_for_status()
    raise RuntimeError(f"No quote data for {symbol}")


def get_current_prices_meta_db(
    db: Session | None,
    symbols: list[str],
    *,
    allow_cache_write: bool = True,
    release_connection_before_fetch: bool = False,
    lane: str = "background_quote",
    ttl_seconds: int | None = None,
    allow_live_user_fetch: bool = False,
    stale_while_revalidate: bool = True,
    coalesce_wait_seconds: float | None = None,
    force_quote_endpoint: bool = False,
    cache_only: bool = False,
    skip_db_sanity: bool = False,
    max_network_fetch: int | None = None,
    bypass_miss_cache: bool = False,
) -> dict[str, dict]:
    quote_meta: dict[str, dict] = {}
    try:
        normalized_symbols = sorted(
            {
                normalized
                for symbol in symbols
                for normalized in [normalize_symbol(symbol)]
                if normalized
            }
        )
        if not normalized_symbols:
            return {}

        mem_hits = 0
        sqlite_fresh_hits = 0
        sqlite_stale_hits = 0
        miss_skipped = 0

        remaining_symbols: list[str] = []
        for symbol in normalized_symbols:
            cached_meta = _cache_get_meta(symbol)
            if cached_meta is not None and cached_meta.get("price") is not None:
                if force_quote_endpoint:
                    record_cache_miss(category="quote", symbol=symbol)
                    remaining_symbols.append(symbol)
                    continue
                record_cache_hit(category="quote", symbol=symbol)
                raw_meta = {**cached_meta, "is_stale": False, "source": cached_meta.get("source") or "cache"}
                quote_meta[symbol] = (
                    raw_meta
                    if skip_db_sanity or db is None
                    else _quote_meta_with_eod_sanity(db, symbol, raw_meta, lane=lane)
                )
                if quote_meta[symbol].get("source") == "eod_sanity_fallback":
                    _cache_set_meta(symbol, quote_meta[symbol], lane=lane, ttl_seconds=ttl_seconds)
                logger.info("quote_cache_hit lane=%s symbol=%s source=memory", lane, symbol)
                mem_hits += 1
            else:
                record_cache_miss(category="quote", symbol=symbol)
                remaining_symbols.append(symbol)

        sqlite_fresh: dict[str, float] = {}
        sqlite_stale: dict[str, float] = {}
        if remaining_symbols and not force_quote_endpoint and db is not None:
            ttl = _cache_ttl_seconds(lane=lane, ttl_seconds=ttl_seconds)
            now = datetime.now(timezone.utc).replace(tzinfo=None)
            sqlite_map = quote_cache_get_many_with_age(db, remaining_symbols)
            for symbol, (price, asof_ts) in sqlite_map.items():
                freshness_asof = _freshness_datetime(asof_ts)
                if freshness_asof is None:
                    continue
                age_seconds = max((now - freshness_asof).total_seconds(), 0)
                if age_seconds <= ttl:
                    sqlite_fresh[symbol] = price
                else:
                    sqlite_stale[symbol] = price

            for symbol, price in sqlite_fresh.items():
                record_cache_hit(category="quote", symbol=symbol, cache_age_seconds=0)
                raw_meta = {
                    "symbol": symbol,
                    "price": price,
                    "asof_ts": sqlite_map[symbol][1],
                    "cached_at": sqlite_map[symbol][1],
                    "is_stale": False,
                    "source": "cache",
                }
                quote_meta[symbol] = (
                    raw_meta
                    if skip_db_sanity
                    else _quote_meta_with_eod_sanity(db, symbol, raw_meta, lane=lane)
                )
                _cache_set_meta(symbol, quote_meta[symbol], lane=lane, ttl_seconds=ttl_seconds)
                logger.info("quote_cache_hit lane=%s symbol=%s source=db", lane, symbol)
            sqlite_fresh_hits = len(sqlite_fresh)

            for symbol, price in sqlite_stale.items():
                record_cache_hit(category="quote", symbol=symbol)
                raw_meta = {
                    "symbol": symbol,
                    "price": price,
                    "asof_ts": sqlite_map[symbol][1],
                    "cached_at": sqlite_map[symbol][1],
                    "is_stale": True,
                    "source": "stale_cache",
                }
                quote_meta[symbol] = (
                    raw_meta
                    if skip_db_sanity
                    else _quote_meta_with_eod_sanity(db, symbol, raw_meta, lane=lane)
                )
                logger.info("quote_cache_stale_hit lane=%s symbol=%s", lane, symbol)
            sqlite_stale_hits = len(sqlite_stale)

        # Need fetch if missing entirely, plus we try to refresh stale quotes best-effort.
        missing_symbols = [
            s for s in remaining_symbols if (s not in sqlite_fresh and s not in sqlite_stale)
        ]
        stale_symbols = list(sqlite_stale.keys())

        # prioritize missing first, then stale refresh
        need_fetch_candidates = missing_symbols + ([] if stale_while_revalidate else stale_symbols)
        if stale_symbols and stale_while_revalidate:
            _enqueue_quote_refreshes(stale_symbols, reason=f"stale_{lane}")

        need_fetch: list[str] = []
        for symbol in need_fetch_candidates:
            if not bypass_miss_cache and _miss_cache_hit(symbol):
                miss_skipped += 1
                continue
            need_fetch.append(symbol)

        if cache_only:
            logger.info(
                "quote_lookup cache_only requested=%s mem=%s sqlite_fresh=%s sqlite_stale=%s skipped_fetch=%s miss_skipped=%s returned=%s",
                len(normalized_symbols),
                mem_hits,
                sqlite_fresh_hits,
                sqlite_stale_hits,
                len(need_fetch),
                miss_skipped,
                len(quote_meta),
            )
            return quote_meta

        if not need_fetch:
            logger.info(
                "quote_lookup requested=%s mem=%s sqlite_fresh=%s sqlite_stale=%s fetched=%s miss_skipped=%s returned=%s",
                len(normalized_symbols),
                mem_hits,
                sqlite_fresh_hits,
                sqlite_stale_hits,
                0,
                miss_skipped,
                len(quote_meta),
            )
            return quote_meta

        if release_connection_before_fetch and db is not None:
            db.close()

        fetch_cap = _bounded_network_fetch_cap(max_network_fetch)
        if len(need_fetch) > fetch_cap:
            dropped_symbols = need_fetch[fetch_cap:]
            if any(symbol in missing_symbols for symbol in dropped_symbols):
                logger.warning(
                    "quote_lookup cap_dropped_missing count=%s",
                    sum(1 for symbol in dropped_symbols if symbol in missing_symbols),
                )
            _log_capped_fetch(
                requested=len(normalized_symbols),
                cached=mem_hits + sqlite_fresh_hits + sqlite_stale_hits,
                cap=fetch_cap,
                dropped_symbols=dropped_symbols,
            )
            need_fetch = need_fetch[:fetch_cap]

        if _quotes_disabled():
            disabled_status = _quotes_disabled_status()
            if disabled_status:
                for symbol in need_fetch:
                    quote_meta.setdefault(
                        symbol,
                        {
                            "price": None,
                            "asof_ts": None,
                            "is_stale": False,
                            "status": disabled_status,
                        },
                    )
            logger.info(
                "quote_lookup requested=%s mem=%s sqlite_fresh=%s sqlite_stale=%s fetched=%s miss_skipped=%s returned=%s",
                len(normalized_symbols),
                mem_hits,
                sqlite_fresh_hits,
                sqlite_stale_hits,
                0,
                miss_skipped,
                len(quote_meta),
            )
            return quote_meta

        api_key = os.getenv("FMP_API_KEY", "").strip()
        if not api_key:
            logger.warning("quote_lookup skipped reason=missing_api_key")
            for symbol in need_fetch:
                record_fallback(category="quote", symbol=symbol, reason="provider_disabled")
            _enqueue_quote_refreshes(need_fetch, reason="missing_api_key")
            return quote_meta

        equities: list[str] = []
        crypto: list[str] = []
        for symbol in need_fetch:
            if symbol.endswith("USD") and len(symbol) <= 10:
                crypto.append(symbol)
            else:
                equities.append(symbol)

        payload: list[dict] = []
        miss_count = 0
        disable_triggered = False
        status_counts: dict[int, int] = {}
        attempted_symbols: list[str] = []
        mutation_lock = threading.Lock()

        def _record_miss(status_code: int, count: int = 1) -> None:
            nonlocal miss_count
            with mutation_lock:
                miss_count += count
                status_counts[status_code] = status_counts.get(status_code, 0) + count

        def _parse_quote_payload(quote_payload: object, *, fallback_symbol: str, endpoint_contract: dict | None = None) -> bool:
            rows = _payload_rows(quote_payload)
            parsed_rows = _rows_to_quote_payload(rows, fallback_symbol=fallback_symbol, endpoint_contract=endpoint_contract)
            if parsed_rows:
                with mutation_lock:
                    payload.extend(parsed_rows)
            return bool(parsed_rows)

        def _fetch_configured_quote(symbol: str, asset_type: str) -> bool:
            nonlocal disable_triggered
            with mutation_lock:
                attempted_symbols.append(symbol)
            lock = _fetch_lock_for_symbol(symbol)
            acquired = lock.acquire(blocking=False)
            if not acquired:
                wait_seconds = float(coalesce_wait_seconds if coalesce_wait_seconds is not None else os.getenv("QUOTE_COALESCE_WAIT_SECONDS", "0.75"))
                logger.info("quote_coalesced_waiter lane=%s symbol=%s wait_seconds=%s", lane, symbol, wait_seconds)
                if wait_seconds > 0 and lock.acquire(timeout=max(0.0, wait_seconds)):
                    lock.release()
                    deadline = time.monotonic() + max(0.05, wait_seconds)
                    while time.monotonic() < deadline:
                        refreshed = _cache_get_meta(symbol)
                        if refreshed is not None and refreshed.get("price") is not None:
                            payload.append({**refreshed, "symbol": symbol})
                            return True
                        time.sleep(0.01)
                return True
            if not _quote_budget_allows(lane=lane, symbol=symbol):
                with mutation_lock:
                    quote_meta.setdefault(
                        symbol,
                        {
                            "symbol": symbol,
                            "price": None,
                            "asof_ts": None,
                            "cached_at": None,
                            "is_stale": False,
                            "source": "unavailable",
                            "status": "provider_budget_exceeded",
                        },
                    )
                lock.release()
                return True
            try:
                ensure_fmp_live_allowed(category=lane, symbol=symbol, allow_user_request=allow_live_user_fetch)
            except ProviderUnavailable as exc:
                reason = getattr(exc, "reason", "provider_unavailable")
                record_fallback(category="quote", symbol=symbol, reason=reason)
                with mutation_lock:
                    quote_meta.setdefault(
                        symbol,
                        {
                            "price": None,
                            "asof_ts": None,
                            "is_stale": False,
                            "status": reason,
                        },
                    )
                _enqueue_quote_refreshes([symbol], reason=reason)
                lock.release()
                return True
            try:
                if force_quote_endpoint:
                    intraday_end_day = datetime.now(timezone.utc).date()
                    try:
                        intraday_lookback_days = max(1, int(os.getenv("QUOTE_INTRADAY_LOOKBACK_DAYS", "7") or 7))
                    except ValueError:
                        intraday_lookback_days = 7
                    intraday_start_day = intraday_end_day - timedelta(days=intraday_lookback_days)
                    intraday_request = build_fmp_endpoint_request(
                        role="primary",
                        provider="fmp",
                        endpoint_url=f"{FMP_BASE_URL}/historical-chart/1min?symbol={{symbol}}",
                        api_key=api_key,
                        symbol=symbol,
                        endpoint_contract_json=FMP_INTRADAY_CHART_CONTRACT_JSON,
                    )
                    intraday_request.request_params["from"] = intraday_start_day.isoformat()
                    intraday_request.request_params["to"] = intraday_end_day.isoformat()
                    eod_request = build_fmp_endpoint_request(
                        role="fallback",
                        provider="fmp",
                        endpoint_url=f"{FMP_BASE_URL}/historical-price-eod/light?symbol={{symbol}}",
                        api_key=api_key,
                        symbol=symbol,
                        endpoint_contract_json=FMP_EOD_LIGHT_QUOTE_CONTRACT_JSON,
                    )
                    endpoint_requests = [
                        intraday_request,
                        eod_request,
                    ]
                elif db is not None:
                    endpoint_requests = fmp_endpoint_requests_for_domain(
                        db,
                        "prices_intraday",
                        symbol=symbol,
                        api_key=api_key,
                        include_fallback=True,
                    )
                else:
                    endpoint_requests = []
            except Exception:
                logger.info("quote_lookup endpoint settings unavailable; using historical EOD light fallback", exc_info=True)
                endpoint_requests = []

            if not endpoint_requests:
                # Last-resort legacy behavior for incomplete settings; seeded settings should avoid this.
                endpoint_requests = [
                    SimpleNamespace(
                        role="legacy",
                        endpoint_name="historical-price-eod/light",
                        request_url=f"{FMP_BASE_URL}/historical-price-eod/light",
                        request_params={"symbol": symbol, "apikey": api_key},
                        endpoint_contract={},
                    )
                ]

            saw_402 = False
            saw_429 = False
            try:
                for endpoint_request in endpoint_requests:
                    started = time.monotonic()
                    try:
                        response = requests.get(
                            endpoint_request.request_url,
                            params=endpoint_request.request_params,
                            timeout=float(os.getenv("QUOTE_PROVIDER_TIMEOUT_SECONDS", "4")),
                        )
                    except requests.RequestException as exc:
                        _record_miss(599)
                        logger.warning("quote_live_error lane=%s symbol=%s error=%s", lane, symbol, exc.__class__.__name__)
                        _disable_quotes(minutes=1, reason="quote_provider_exception")
                        disable_triggered = True
                        return False
                    elapsed_ms = (time.monotonic() - started) * 1000
                    if elapsed_ms >= float(os.getenv("QUOTE_PROVIDER_SLOW_MS", "2500") or 2500):
                        logger.warning("quote_live_error lane=%s symbol=%s reason=slow_response elapsed_ms=%.1f", lane, symbol, elapsed_ms)
                        _disable_quotes(minutes=1, reason="quote_provider_slow")
                        disable_triggered = True
                    record_provider_response(
                        category=f"quote:{endpoint_request.endpoint_name}",
                        symbol=symbol,
                        status_code=response.status_code,
                    )
                    if response.status_code != 200:
                        _record_miss(response.status_code)
                        saw_402 = saw_402 or response.status_code == 402
                        saw_429 = saw_429 or response.status_code == 429
                        if response.status_code >= 500:
                            _disable_quotes(minutes=1, reason="quote_provider_5xx")
                            disable_triggered = True
                        if len(need_fetch) >= 5:
                            logger.debug(
                                "quote_lookup symbol_miss symbol=%s asset=%s endpoint=%s status=%s",
                                symbol,
                                asset_type,
                                endpoint_request.endpoint_name,
                                response.status_code,
                            )
                        continue

                    try:
                        parsed = response.json()
                    except ValueError:
                        _record_miss(502)
                        continue
                    if _parse_quote_payload(parsed, fallback_symbol=symbol, endpoint_contract=getattr(endpoint_request, "endpoint_contract", None)):
                        return True
                    _record_miss(204)
            finally:
                lock.release()

            if saw_402:
                global _last_paywall_log
                now = datetime.now(timezone.utc)
                if _last_paywall_log is None or (now - _last_paywall_log) > timedelta(hours=1):
                    logger.warning("quote_lookup configured_quote_paywalled status=402")
                    _last_paywall_log = now
                _disable_quotes(minutes=10, reason="paywalled_402_configured_quote")
                disable_triggered = True
                return False
            if saw_429:
                _disable_quotes(minutes=2, reason="rate_limited_429_configured_quote")
                disable_triggered = True
                return False
            return True

        if equities:
            equities_to_fetch = equities
            logger.info("quote_lookup fetching_equity_singles count=%s", len(equities_to_fetch))
            stop_fetching_equities = False
            if force_quote_endpoint and len(equities_to_fetch) > 1:
                try:
                    max_workers = max(1, int(os.getenv("QUOTE_FORCE_ENDPOINT_MAX_WORKERS", "4") or 4))
                except ValueError:
                    max_workers = 4
                with ThreadPoolExecutor(max_workers=min(max_workers, len(equities_to_fetch))) as executor:
                    futures = {
                        executor.submit(_fetch_configured_quote, symbol, "equity"): symbol
                        for symbol in equities_to_fetch
                    }
                    for future in as_completed(futures):
                        try:
                            should_continue = future.result()
                        except Exception:
                            _record_miss(599)
                            logger.warning(
                                "quote_live_error lane=%s symbol=%s error=worker_exception",
                                lane,
                                futures[future],
                            )
                            should_continue = True
                        if not should_continue:
                            stop_fetching_equities = True
            else:
                for symbol in equities_to_fetch:
                    should_continue = _fetch_configured_quote(symbol, asset_type="equity")
                    if not should_continue:
                        stop_fetching_equities = True
                        break

            if stop_fetching_equities and _quotes_disabled():
                disabled_status = _quotes_disabled_status()
                if disabled_status:
                    for unresolved_symbol in need_fetch:
                        quote_meta.setdefault(
                            unresolved_symbol,
                            {
                                "price": None,
                                "asof_ts": None,
                                "is_stale": False,
                                "status": disabled_status,
                            },
                        )
                logger.info(
                    "quote_lookup requested=%s mem=%s sqlite_fresh=%s sqlite_stale=%s fetched=%s miss_skipped=%s returned=%s",
                    len(normalized_symbols),
                    mem_hits,
                    sqlite_fresh_hits,
                    sqlite_stale_hits,
                    0,
                    miss_skipped,
                    len(quote_meta),
                )
                return quote_meta

        for symbol in crypto:
            logger.info("quote_lookup requesting crypto=%s", symbol)
            should_continue = _fetch_configured_quote(symbol, asset_type="crypto")
            if not should_continue:
                disabled_status = _quotes_disabled_status()
                if disabled_status:
                    for unresolved_symbol in need_fetch:
                        quote_meta.setdefault(
                            unresolved_symbol,
                            {
                                "price": None,
                                "asof_ts": None,
                                "is_stale": False,
                                "status": disabled_status,
                            },
                        )
                logger.info(
                    "quote_lookup requested=%s mem=%s sqlite_fresh=%s sqlite_stale=%s fetched=%s miss_skipped=%s returned=%s",
                    len(normalized_symbols),
                    mem_hits,
                    sqlite_fresh_hits,
                    sqlite_stale_hits,
                    0,
                    miss_skipped,
                    len(quote_meta),
                )
                return quote_meta

        new_prices: dict[str, float] = {}
        fetched_at = datetime.now(timezone.utc).replace(tzinfo=None)
        for row in payload:
            if not isinstance(row, dict):
                continue
            symbol = normalize_symbol(row.get("symbol"))
            if not symbol:
                continue
            try:
                price = float(row.get("price"))
            except (TypeError, ValueError):
                continue
            row_asof = row.get("asof_ts") if isinstance(row.get("asof_ts"), datetime) else fetched_at
            existing_asof = quote_meta.get(symbol, {}).get("asof_ts")
            if isinstance(existing_asof, datetime) and isinstance(row_asof, datetime) and row_asof <= existing_asof:
                continue
            raw_meta = {
                "symbol": symbol,
                "price": price,
                "asof_ts": row_asof,
                "provider_timestamp": row_asof if isinstance(row.get("asof_ts"), datetime) else None,
                "cached_at": fetched_at,
                "is_stale": False,
                "source": "live_quote" if force_quote_endpoint else "live_provider",
            }
            quote_meta[symbol] = (
                raw_meta
                if skip_db_sanity or db is None
                else _quote_meta_with_eod_sanity(db, symbol, raw_meta, lane=lane)
            )
            for field in ("change", "change_percent", "volume", "market_cap"):
                if row.get(field) is not None:
                    quote_meta[symbol][field] = row.get(field)
            _cache_set_meta(symbol, quote_meta[symbol], lane=lane, ttl_seconds=ttl_seconds)
            new_prices[symbol] = float(quote_meta[symbol]["price"])
            logger.info("quote_live_fetch lane=%s symbol=%s", lane, symbol)

        if allow_cache_write and db is not None:
            new_market_caps = {
                symbol: float(meta["market_cap"])
                for symbol, meta in quote_meta.items()
                if symbol in new_prices and meta.get("market_cap") is not None
            }
            quote_cache_upsert_many(db, new_prices, market_caps=new_market_caps)

        if attempted_symbols and not disable_triggered:
            returned_symbols = set(new_prices.keys())
            for symbol in attempted_symbols:
                if symbol not in returned_symbols:
                    _miss_cache_set(symbol, seconds=3600)

        if attempted_symbols:
            provider_status: str | None = None
            if status_counts.get(402):
                provider_status = "provider_402"
            elif status_counts.get(429):
                provider_status = "provider_429"
            if provider_status:
                returned_symbols = set(new_prices.keys())
                for symbol in attempted_symbols:
                    if symbol in returned_symbols:
                        continue
                    quote_meta.setdefault(
                        symbol,
                        {
                            "price": None,
                            "asof_ts": None,
                            "is_stale": False,
                            "status": provider_status,
                        },
                    )

        if miss_count:
            logger.warning(
                "quote_lookup partial_miss misses=%s statuses=%s fetched=%s",
                miss_count,
                status_counts,
                len(need_fetch),
            )

        logger.info(
            "quote_lookup requested=%s mem=%s sqlite_fresh=%s sqlite_stale=%s fetched=%s miss_skipped=%s returned=%s",
            len(normalized_symbols),
            mem_hits,
            sqlite_fresh_hits,
            sqlite_stale_hits,
            len(new_prices),
            miss_skipped,
            len(quote_meta),
        )

        return quote_meta
    except Exception:
        logger.exception("quote_lookup unexpected failure")
        return quote_meta


def _get_current_prices_with_db(db: Session, symbols: list[str], *, allow_cache_write: bool = False) -> dict[str, float]:
    quote_meta = get_current_prices_meta_db(db, symbols, allow_cache_write=allow_cache_write)
    return {
        symbol: float(meta["price"])
        for symbol, meta in quote_meta.items()
        if isinstance(meta, dict) and meta.get("price") is not None
    }


def get_current_prices(symbols: list[str]) -> dict[str, float]:
    """Returns {SYMBOL: price} for symbols. Safe, timeout, returns partial dict on failure."""
    with SessionLocal() as db:
        return _get_current_prices_with_db(db, symbols, allow_cache_write=False)


def get_current_prices_db(
    db: Session,
    symbols: list[str],
    *,
    lane: str = "background_quote",
    ttl_seconds: int | None = None,
    allow_live_user_fetch: bool = False,
) -> dict[str, float]:
    """Returns {SYMBOL: price} using memory + SQLite cache with network fallback."""
    quote_meta = get_current_prices_meta_db(
        db,
        symbols,
        allow_cache_write=False,
        lane=lane,
        ttl_seconds=ttl_seconds,
        allow_live_user_fetch=allow_live_user_fetch,
    )
    return {
        symbol: float(meta["price"])
        for symbol, meta in quote_meta.items()
        if isinstance(meta, dict) and meta.get("price") is not None
    }


def get_current_prices_meta(symbols: list[str]) -> dict[str, dict]:
    """Returns {SYMBOL: {price, asof_ts, is_stale}} for symbols."""
    with SessionLocal() as db:
        return get_current_prices_meta_db(db, symbols, allow_cache_write=False)
