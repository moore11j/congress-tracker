from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timedelta, timezone

import requests
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from app.db import SessionLocal
from app.clients.fmp import FMP_BASE_URL
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
    return {
        sym: (float(price), asof)
        for sym, price, asof in rows
        if sym and price is not None and asof is not None
    }


def quote_cache_upsert_many(db: Session, prices: dict[str, float]) -> None:
    if not prices:
        return
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    rows = [
        {"symbol": sym, "price": float(price), "asof_ts": now}
        for sym, price in prices.items()
    ]
    stmt = sqlite_insert(QuoteCache.__table__).values(rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=["symbol"],
        set_={"price": stmt.excluded.price, "asof_ts": stmt.excluded.asof_ts},
    )
    try:
        db.execute(stmt)
        db.commit()
    except Exception:
        db.rollback()
        logger.exception("quote_lookup sqlite_upsert_failed rows=%s", len(rows))

def get_index_quote(symbol: str) -> float:
    """Fetch current index price (e.g. ^GSPC) from FMP stable quote endpoint."""
    api_key = os.getenv("FMP_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("FMP_API_KEY not configured")

    response = requests.get(
        f"{FMP_BASE_URL}/quote",
        params={"symbol": symbol, "apikey": api_key},
        timeout=10,
    )
    response.raise_for_status()

    data = response.json()
    if not isinstance(data, list) or not data or "price" not in data[0]:
        raise RuntimeError(f"No quote data for {symbol}")

    return float(data[0]["price"])


def get_current_prices_meta_db(db: Session, symbols: list[str]) -> dict[str, dict]:
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
                quote_meta[symbol] = {
                    "price": cached_price,
                    "asof_ts": None,
                    "is_stale": False,
                }
                mem_hits += 1
            else:
                remaining_symbols.append(symbol)

        sqlite_fresh: dict[str, float] = {}
        sqlite_stale: dict[str, float] = {}
        if remaining_symbols:
            ttl = _cache_ttl_seconds()
            now = datetime.now(timezone.utc).replace(tzinfo=None)
            sqlite_map = quote_cache_get_many_with_age(db, remaining_symbols)
            for symbol, (price, asof_ts) in sqlite_map.items():
                age_seconds = max((now - asof_ts).total_seconds(), 0)
                if age_seconds <= ttl:
                    sqlite_fresh[symbol] = price
                else:
                    sqlite_stale[symbol] = price

            for symbol, price in sqlite_fresh.items():
                quote_meta[symbol] = {
                    "price": price,
                    "asof_ts": sqlite_map[symbol][1],
                    "is_stale": False,
                }
                cache_set(symbol, price)
            sqlite_fresh_hits = len(sqlite_fresh)

            for symbol, price in sqlite_stale.items():
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

        def _parse_quote_payload(quote_payload: object) -> None:
            if isinstance(quote_payload, list):
                payload.extend(row for row in quote_payload if isinstance(row, dict))
            elif isinstance(quote_payload, dict):
                payload.append(quote_payload)

        def _fetch_quote_short(symbol: str, asset_type: str) -> bool:
            nonlocal disable_triggered
            attempted_symbols.append(symbol)
            response = requests.get(
                f"{FMP_BASE_URL}/quote-short?symbol={symbol}&apikey={api_key}",
                timeout=10,
            )
            if response.status_code != 200:
                _record_miss(response.status_code)
                if len(need_fetch) >= 5:
                    logger.debug(
                        "quote_lookup symbol_miss symbol=%s asset=%s status=%s",
                        symbol,
                        asset_type,
                        response.status_code,
                    )
                if response.status_code == 402:
                    global _last_paywall_log
                    now = datetime.now(timezone.utc)
                    if _last_paywall_log is None or (now - _last_paywall_log) > timedelta(hours=1):
                        logger.warning("quote_lookup quote_short_paywalled status=402")
                        _last_paywall_log = now
                    _disable_quotes(minutes=10, reason="paywalled_402_quote_short")
                    disable_triggered = True
                    return False
                if response.status_code == 429:
                    _disable_quotes(minutes=2, reason="rate_limited_429_quote_short")
                    disable_triggered = True
                    return False
                return True

            _parse_quote_payload(response.json())
            return True

        if equities:
            equities_to_fetch = equities
            logger.info("quote_lookup fetching_equity_singles count=%s", len(equities_to_fetch))
            stop_fetching_equities = False
            for symbol in equities_to_fetch:
                should_continue = _fetch_quote_short(symbol, asset_type="equity")
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
            should_continue = _fetch_quote_short(symbol, asset_type="crypto")
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
                "asof_ts": fetched_at,
                "is_stale": False,
            }
            cache_set(symbol, price)
            new_prices[symbol] = price

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


def _get_current_prices_with_db(db: Session, symbols: list[str]) -> dict[str, float]:
    quote_meta = get_current_prices_meta_db(db, symbols)
    return {
        symbol: float(meta["price"])
        for symbol, meta in quote_meta.items()
        if isinstance(meta, dict) and "price" in meta
    }


def get_current_prices(symbols: list[str]) -> dict[str, float]:
    """Returns {SYMBOL: price} for symbols. Safe, timeout, returns partial dict on failure."""
    with SessionLocal() as db:
        return _get_current_prices_with_db(db, symbols)


def get_current_prices_db(db: Session, symbols: list[str]) -> dict[str, float]:
    """Returns {SYMBOL: price} using memory + SQLite cache with network fallback."""
    return _get_current_prices_with_db(db, symbols)


def get_current_prices_meta(symbols: list[str]) -> dict[str, dict]:
    """Returns {SYMBOL: {price, asof_ts, is_stale}} for symbols."""
    with SessionLocal() as db:
        return get_current_prices_meta_db(db, symbols)
