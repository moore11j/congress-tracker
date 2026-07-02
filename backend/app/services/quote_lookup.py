from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import requests
from sqlalchemy import text
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
from app.services.provider_endpoints import fmp_endpoint_requests_for_domain
from app.services.data_enrichment_queue import enqueue_data_enrichment_job
from app.models import QuoteCache
from app.utils.symbols import normalize_symbol

logger = logging.getLogger(__name__)

_QUOTE_CACHE: dict[str, tuple[float, float]] = {}
_MISS_CACHE: dict[str, float] = {}
_last_paywall_log: datetime | None = None
_last_quotes_disable_log: datetime | None = None
_quotes_disabled_until: datetime | None = None
_quotes_disable_reason: str | None = None


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


def _cache_ttl_seconds() -> int:
    try:
        ttl = int(os.getenv("QUOTE_CACHE_TTL_SECONDS", "300"))
    except ValueError:
        ttl = 300
    return max(ttl, 1)


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


def cache_get(symbol: str) -> float | None:
    cached = _QUOTE_CACHE.get(symbol)
    if not cached:
        return None
    price, expires_at = cached
    if time.time() >= expires_at:
        _QUOTE_CACHE.pop(symbol, None)
        return None
    return price


def cache_set(symbol: str, price: float) -> None:
    _QUOTE_CACHE[symbol] = (price, time.time() + _cache_ttl_seconds())






def _network_fetch_cap() -> int:
    try:
        cap = int(os.getenv("QUOTE_LOOKUP_MAX_FETCH", "25"))
    except ValueError:
        cap = 25
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


def quote_cache_upsert_many(db: Session, prices: dict[str, float]) -> None:
    if not prices:
        return
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    rows = [
        {"symbol": sym, "price": float(price), "asof_ts": now}
        for sym, price in prices.items()
    ]
    insert_fn = postgres_insert if db.get_bind().dialect.name == "postgresql" else sqlite_insert
    stmt = insert_fn(QuoteCache.__table__).values(rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=["symbol"],
        set_={"price": stmt.excluded.price, "asof_ts": stmt.excluded.asof_ts},
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
        payload.append(parsed)
    return payload

def get_index_quote(symbol: str) -> float:
    """Fetch current index price (e.g. ^GSPC) from FMP stable quote endpoint."""
    api_key = os.getenv("FMP_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("FMP_API_KEY not configured")

    ensure_fmp_live_allowed(category="quote:index", symbol=symbol)
    response = requests.get(
        f"{FMP_BASE_URL}/quote",
        params={"symbol": symbol, "apikey": api_key},
        timeout=10,
    )
    record_provider_response(category="quote:index", symbol=symbol, status_code=response.status_code)
    response.raise_for_status()

    data = response.json()
    if not isinstance(data, list) or not data or "price" not in data[0]:
        raise RuntimeError(f"No quote data for {symbol}")

    return float(data[0]["price"])


def get_current_prices_meta_db(
    db: Session,
    symbols: list[str],
    *,
    allow_cache_write: bool = True,
    release_connection_before_fetch: bool = False,
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
            cached_price = cache_get(symbol)
            if cached_price is not None:
                record_cache_hit(category="quote", symbol=symbol)
                quote_meta[symbol] = {
                    "price": cached_price,
                    "asof_ts": None,
                    "is_stale": False,
                }
                mem_hits += 1
            else:
                record_cache_miss(category="quote", symbol=symbol)
                remaining_symbols.append(symbol)

        sqlite_fresh: dict[str, float] = {}
        sqlite_stale: dict[str, float] = {}
        if remaining_symbols:
            ttl = _cache_ttl_seconds()
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
                quote_meta[symbol] = {
                    "price": price,
                    "asof_ts": sqlite_map[symbol][1],
                    "is_stale": False,
                }
                cache_set(symbol, price)
            sqlite_fresh_hits = len(sqlite_fresh)

            for symbol, price in sqlite_stale.items():
                record_cache_hit(category="quote", symbol=symbol)
                quote_meta[symbol] = {
                    "price": price,
                    "asof_ts": sqlite_map[symbol][1],
                    "is_stale": True,
                }
                cache_set(symbol, price)
            sqlite_stale_hits = len(sqlite_stale)

        # Need fetch if missing entirely, plus we try to refresh stale quotes best-effort.
        missing_symbols = [
            s for s in remaining_symbols if (s not in sqlite_fresh and s not in sqlite_stale)
        ]
        stale_symbols = list(sqlite_stale.keys())

        # prioritize missing first, then stale refresh
        need_fetch_candidates = missing_symbols + stale_symbols

        need_fetch: list[str] = []
        for symbol in need_fetch_candidates:
            if _miss_cache_hit(symbol):
                miss_skipped += 1
                continue
            need_fetch.append(symbol)

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

        if release_connection_before_fetch:
            db.close()

        fetch_cap = _network_fetch_cap()
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

        try:
            for symbol in need_fetch:
                ensure_fmp_live_allowed(category="quote", symbol=symbol)
        except ProviderUnavailable as exc:
            reason = getattr(exc, "reason", "provider_unavailable")
            for symbol in need_fetch:
                record_fallback(category="quote", symbol=symbol, reason=reason)
                quote_meta.setdefault(
                    symbol,
                    {
                        "price": None,
                        "asof_ts": None,
                        "is_stale": False,
                        "status": reason,
                    },
                )
            _enqueue_quote_refreshes(need_fetch, reason=reason)
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

        def _record_miss(status_code: int, count: int = 1) -> None:
            nonlocal miss_count
            miss_count += count
            status_counts[status_code] = status_counts.get(status_code, 0) + count

        def _parse_quote_payload(quote_payload: object, *, fallback_symbol: str, endpoint_contract: dict | None = None) -> bool:
            rows = _payload_rows(quote_payload)
            parsed_rows = _rows_to_quote_payload(rows, fallback_symbol=fallback_symbol, endpoint_contract=endpoint_contract)
            payload.extend(parsed_rows)
            return bool(parsed_rows)

        def _fetch_configured_quote(symbol: str, asset_type: str) -> bool:
            nonlocal disable_triggered
            attempted_symbols.append(symbol)
            try:
                endpoint_requests = fmp_endpoint_requests_for_domain(
                    db,
                    "prices_intraday",
                    symbol=symbol,
                    api_key=api_key,
                    include_fallback=True,
                )
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
            for endpoint_request in endpoint_requests:
                response = requests.get(
                    endpoint_request.request_url,
                    params=endpoint_request.request_params,
                    timeout=10,
                )
                record_provider_response(
                    category=f"quote:{endpoint_request.endpoint_name}",
                    symbol=symbol,
                    status_code=response.status_code,
                )
                if response.status_code != 200:
                    _record_miss(response.status_code)
                    saw_402 = saw_402 or response.status_code == 402
                    saw_429 = saw_429 or response.status_code == 429
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
            quote_meta[symbol] = {
                "price": price,
                "asof_ts": row.get("asof_ts") if isinstance(row.get("asof_ts"), datetime) else fetched_at,
                "is_stale": False,
            }
            cache_set(symbol, price)
            new_prices[symbol] = price

        if allow_cache_write:
            quote_cache_upsert_many(db, new_prices)

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


def get_current_prices_db(db: Session, symbols: list[str]) -> dict[str, float]:
    """Returns {SYMBOL: price} using memory + SQLite cache with network fallback."""
    return _get_current_prices_with_db(db, symbols, allow_cache_write=False)


def get_current_prices_meta(symbols: list[str]) -> dict[str, dict]:
    """Returns {SYMBOL: {price, asof_ts, is_stale}} for symbols."""
    with SessionLocal() as db:
        return get_current_prices_meta_db(db, symbols, allow_cache_write=False)
