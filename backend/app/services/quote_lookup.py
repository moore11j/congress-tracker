from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timedelta

import requests
from sqlalchemy.orm import Session

from app.clients.fmp import FMP_BASE_URL
from app.models import QuoteCache
from app.utils.symbols import canonical_symbol

logger = logging.getLogger(__name__)

_QUOTE_CACHE: dict[str, tuple[float, float]] = {}
_last_paywall_log: datetime | None = None
_last_quotes_disable_log: datetime | None = None
_quotes_disabled_until: datetime | None = None
_quotes_disable_reason: str | None = None


def _cache_ttl_seconds() -> int:
    try:
        ttl = int(os.getenv("QUOTE_CACHE_TTL_SECONDS", "300"))
    except ValueError:
        ttl = 300
    return max(ttl, 1)


def _quotes_disabled() -> bool:
    return _quotes_disabled_until is not None and datetime.utcnow() < _quotes_disabled_until


def _disable_quotes(minutes: int, reason: str) -> None:
    global _quotes_disabled_until, _quotes_disable_reason, _last_quotes_disable_log
    now = datetime.utcnow()
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




def quote_cache_get_many(db: Session, symbols: list[str]) -> dict[str, float]:
    if not symbols:
        return {}
    rows = (
        db.query(QuoteCache.symbol, QuoteCache.price)
        .filter(QuoteCache.symbol.in_(symbols))
        .all()
    )
    return {sym: float(price) for sym, price in rows if sym and price is not None}


def quote_cache_upsert_many(db: Session, prices: dict[str, float]) -> None:
    if not prices:
        return
    now = datetime.utcnow()
    syms = list(prices.keys())
    existing = (
        db.query(QuoteCache)
        .filter(QuoteCache.symbol.in_(syms))
        .all()
    )
    existing_map = {q.symbol: q for q in existing}

    for sym, price in prices.items():
        if sym in existing_map:
            q = existing_map[sym]
            q.price = float(price)
            q.asof_ts = now
        else:
            db.add(QuoteCache(symbol=sym, price=float(price), asof_ts=now))

    db.commit()

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


def get_current_prices(symbols: list[str]) -> dict[str, float]:
    """Returns {SYMBOL: price} for symbols. Safe, timeout, returns partial dict on failure."""
    prices: dict[str, float] = {}
    try:
        normalized_symbols = sorted(
            {
                normalized
                for symbol in symbols
                for normalized in [canonical_symbol(symbol)]
                if normalized
            }
        )
        if not normalized_symbols:
            return {}

        need_fetch: list[str] = []
        for symbol in normalized_symbols:
            cached_price = cache_get(symbol)
            if cached_price is not None:
                prices[symbol] = cached_price
            else:
                need_fetch.append(symbol)

        if not need_fetch:
            logger.info(
                "quote_lookup requested=%s cached=%s fetched=%s returned=%s",
                len(normalized_symbols),
                len(prices),
                0,
                len(prices),
            )
            return prices

        if _quotes_disabled():
            logger.info(
                "quote_lookup requested=%s cached=%s fetched=%s returned=%s reason=%s",
                len(normalized_symbols),
                len(prices),
                0,
                len(prices),
                _quotes_disable_reason,
            )
            return prices

        api_key = os.getenv("FMP_API_KEY", "").strip()
        if not api_key:
            logger.warning("quote_lookup skipped reason=missing_api_key")
            return prices

        equities: list[str] = []
        crypto: list[str] = []
        for symbol in need_fetch:
            if symbol.endswith("USD") and len(symbol) <= 10:
                crypto.append(symbol)
            else:
                equities.append(symbol)

        payload: list[dict] = []
        miss_count = 0
        status_counts: dict[int, int] = {}

        def _record_miss(status_code: int, count: int = 1) -> None:
            nonlocal miss_count
            miss_count += count
            status_counts[status_code] = status_counts.get(status_code, 0) + count

        def _fetch_quote_short(symbol: str, asset_type: str) -> bool:
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
                    now = datetime.utcnow()
                    if _last_paywall_log is None or (now - _last_paywall_log) > timedelta(hours=1):
                        logger.warning("quote_lookup quote_short_paywalled status=402")
                        _last_paywall_log = now
                    _disable_quotes(minutes=60, reason="paywalled_402_quote_short")
                    return False
                if response.status_code == 429:
                    _disable_quotes(minutes=5, reason="rate_limited_429_quote_short")
                    return False
                return True

            quote_payload = response.json()
            if isinstance(quote_payload, list):
                payload.extend(row for row in quote_payload if isinstance(row, dict))
            elif isinstance(quote_payload, dict):
                payload.append(quote_payload)
            return True

        def _fetch_batch_equities() -> requests.Response:
            return requests.get(
                f"{FMP_BASE_URL}/batch-quote-short?symbols={','.join(equities)}&apikey={api_key}",
                timeout=10,
            )

        if equities:
            logger.info("quote_lookup requesting equities=%s", ",".join(equities))
            response = _fetch_batch_equities()
            if response.status_code == 200:
                equities_payload = response.json()
                if isinstance(equities_payload, list):
                    payload.extend(row for row in equities_payload if isinstance(row, dict))
                else:
                    logger.warning("quote_lookup equities invalid_payload_type=%s", type(equities_payload).__name__)
            else:
                if response.status_code == 402:
                    global _last_paywall_log
                    now = datetime.utcnow()
                    if _last_paywall_log is None or (now - _last_paywall_log) > timedelta(hours=1):
                        logger.warning("quote_lookup batch_paywalled status=402")
                        _last_paywall_log = now
                    _disable_quotes(minutes=60, reason="paywalled_402_batch_quote_short")
                    logger.info(
                        "quote_lookup requested=%s cached=%s fetched=%s returned=%s",
                        len(normalized_symbols),
                        len(prices),
                        0,
                        len(prices),
                    )
                    return prices
                if response.status_code == 429:
                    _disable_quotes(minutes=5, reason="rate_limited_429_batch_quote_short")
                    logger.info(
                        "quote_lookup requested=%s cached=%s fetched=%s returned=%s",
                        len(normalized_symbols),
                        len(prices),
                        0,
                        len(prices),
                    )
                    return prices

                _record_miss(response.status_code, count=len(equities))
                for symbol in equities:
                    should_continue = _fetch_quote_short(symbol, asset_type="equity")
                    if not should_continue:
                        break

                if _quotes_disabled():
                    logger.info(
                        "quote_lookup requested=%s cached=%s fetched=%s returned=%s",
                        len(normalized_symbols),
                        len(normalized_symbols) - len(need_fetch),
                        len(need_fetch),
                        len(prices),
                    )
                    return prices

        for symbol in crypto:
            logger.info("quote_lookup requesting crypto=%s", symbol)
            should_continue = _fetch_quote_short(symbol, asset_type="crypto")
            if not should_continue:
                logger.info(
                    "quote_lookup requested=%s cached=%s fetched=%s returned=%s",
                    len(normalized_symbols),
                    len(normalized_symbols) - len(need_fetch),
                    len(need_fetch),
                    len(prices),
                )
                return prices

        for row in payload:
            if not isinstance(row, dict):
                continue
            symbol = canonical_symbol(row.get("symbol"))
            if not symbol:
                continue
            try:
                price = float(row.get("price"))
            except (TypeError, ValueError):
                continue
            prices[symbol] = price
            cache_set(symbol, price)

        if miss_count:
            logger.warning(
                "quote_lookup partial_miss misses=%s statuses=%s fetched=%s",
                miss_count,
                status_counts,
                len(need_fetch),
            )

        logger.info(
            "quote_lookup requested=%s cached=%s fetched=%s returned=%s",
            len(normalized_symbols),
            len(normalized_symbols) - len(need_fetch),
            len(need_fetch),
            len(prices),
        )

        return prices
    except Exception:
        logger.exception("quote_lookup unexpected failure")
        return prices


def get_current_prices_db(db: Session, symbols: list[str]) -> dict[str, float]:
    """Returns {SYMBOL: price} using memory + SQLite cache with network fallback."""
    prices: dict[str, float] = {}
    try:
        normalized_symbols = sorted(
            {
                normalized
                for symbol in symbols
                for normalized in [canonical_symbol(symbol)]
                if normalized
            }
        )
        if not normalized_symbols:
            return {}

        need_fetch: list[str] = []
        for symbol in normalized_symbols:
            cached_price = cache_get(symbol)
            if cached_price is not None:
                prices[symbol] = cached_price
            else:
                need_fetch.append(symbol)

        if not need_fetch:
            logger.info(
                "quote_lookup requested=%s cached=%s fetched=%s returned=%s",
                len(normalized_symbols),
                len(prices),
                0,
                len(prices),
            )
            return prices

        if _quotes_disabled():
            sqlite_prices = quote_cache_get_many(db, need_fetch)
            prices.update(sqlite_prices)
            logger.info(
                "quote_lookup sqlite_fallback symbols=%s returned=%s reason=%s",
                len(need_fetch),
                len(sqlite_prices),
                _quotes_disable_reason,
            )
            logger.info(
                "quote_lookup requested=%s cached=%s fetched=%s returned=%s reason=%s",
                len(normalized_symbols),
                len(normalized_symbols) - len(need_fetch),
                0,
                len(prices),
                _quotes_disable_reason,
            )
            return prices

        api_key = os.getenv("FMP_API_KEY", "").strip()
        if not api_key:
            logger.warning("quote_lookup skipped reason=missing_api_key")
            return prices

        equities: list[str] = []
        crypto: list[str] = []
        for symbol in need_fetch:
            if symbol.endswith("USD") and len(symbol) <= 10:
                crypto.append(symbol)
            else:
                equities.append(symbol)

        payload: list[dict] = []
        miss_count = 0
        status_counts: dict[int, int] = {}

        def _record_miss(status_code: int, count: int = 1) -> None:
            nonlocal miss_count
            miss_count += count
            status_counts[status_code] = status_counts.get(status_code, 0) + count

        def _sqlite_fallback(symbols_for_fallback: list[str], reason: str) -> dict[str, float]:
            sqlite_prices = quote_cache_get_many(db, symbols_for_fallback)
            prices.update(sqlite_prices)
            logger.info(
                "quote_lookup sqlite_fallback symbols=%s returned=%s reason=%s",
                len(symbols_for_fallback),
                len(sqlite_prices),
                reason,
            )
            return sqlite_prices

        def _fetch_quote_short(symbol: str, asset_type: str) -> str:
            response = requests.get(
                f"{FMP_BASE_URL}/quote-short?symbol={symbol}&apikey={api_key}",
                timeout=10,
            )
            if response.status_code != 200:
                if len(need_fetch) >= 5:
                    logger.debug(
                        "quote_lookup symbol_miss symbol=%s asset=%s status=%s",
                        symbol,
                        asset_type,
                        response.status_code,
                    )
                if response.status_code == 402:
                    global _last_paywall_log
                    now = datetime.utcnow()
                    if _last_paywall_log is None or (now - _last_paywall_log) > timedelta(hours=1):
                        logger.warning("quote_lookup quote_short_paywalled status=402")
                        _last_paywall_log = now
                    _disable_quotes(minutes=60, reason="paywalled_402_quote_short")
                    return "disabled"
                if response.status_code == 429:
                    _disable_quotes(minutes=5, reason="rate_limited_429_quote_short")
                    return "disabled"
                _record_miss(response.status_code)
                return "continue"

            quote_payload = response.json()
            if isinstance(quote_payload, list):
                payload.extend(row for row in quote_payload if isinstance(row, dict))
            elif isinstance(quote_payload, dict):
                payload.append(quote_payload)
            return "ok"

        if equities:
            logger.info("quote_lookup requesting equities=%s", ",".join(equities))
            response = requests.get(
                f"{FMP_BASE_URL}/batch-quote-short?symbols={','.join(equities)}&apikey={api_key}",
                timeout=10,
            )
            if response.status_code == 200:
                equities_payload = response.json()
                if isinstance(equities_payload, list):
                    payload.extend(row for row in equities_payload if isinstance(row, dict))
                else:
                    logger.warning(
                        "quote_lookup equities invalid_payload_type=%s",
                        type(equities_payload).__name__,
                    )
            else:
                if response.status_code == 402:
                    global _last_paywall_log
                    now = datetime.utcnow()
                    if _last_paywall_log is None or (now - _last_paywall_log) > timedelta(hours=1):
                        logger.warning("quote_lookup batch_paywalled status=402")
                        _last_paywall_log = now
                    _disable_quotes(minutes=60, reason="paywalled_402_batch_quote_short")
                    _sqlite_fallback(equities, "paywalled_402_batch_quote_short")
                    logger.info(
                        "quote_lookup requested=%s cached=%s fetched=%s returned=%s",
                        len(normalized_symbols),
                        len(normalized_symbols) - len(need_fetch),
                        0,
                        len(prices),
                    )
                    return prices
                if response.status_code == 429:
                    _disable_quotes(minutes=5, reason="rate_limited_429_batch_quote_short")
                    _sqlite_fallback(equities, "rate_limited_429_batch_quote_short")
                    logger.info(
                        "quote_lookup requested=%s cached=%s fetched=%s returned=%s",
                        len(normalized_symbols),
                        len(normalized_symbols) - len(need_fetch),
                        0,
                        len(prices),
                    )
                    return prices

                _record_miss(response.status_code, count=len(equities))
                for index, symbol in enumerate(equities):
                    result = _fetch_quote_short(symbol, asset_type="equity")
                    if result == "disabled":
                        _sqlite_fallback(equities[index:], _quotes_disable_reason or "quote_short_disabled")
                        logger.info(
                            "quote_lookup requested=%s cached=%s fetched=%s returned=%s",
                            len(normalized_symbols),
                            len(normalized_symbols) - len(need_fetch),
                            len(need_fetch),
                            len(prices),
                        )
                        return prices

        for index, symbol in enumerate(crypto):
            logger.info("quote_lookup requesting crypto=%s", symbol)
            result = _fetch_quote_short(symbol, asset_type="crypto")
            if result == "disabled":
                _sqlite_fallback(crypto[index:], _quotes_disable_reason or "quote_short_disabled")
                logger.info(
                    "quote_lookup requested=%s cached=%s fetched=%s returned=%s",
                    len(normalized_symbols),
                    len(normalized_symbols) - len(need_fetch),
                    len(need_fetch),
                    len(prices),
                )
                return prices

        new_prices: dict[str, float] = {}
        for row in payload:
            if not isinstance(row, dict):
                continue
            symbol = canonical_symbol(row.get("symbol"))
            if not symbol:
                continue
            try:
                price = float(row.get("price"))
            except (TypeError, ValueError):
                continue
            prices[symbol] = price
            cache_set(symbol, price)
            new_prices[symbol] = price

        quote_cache_upsert_many(db, new_prices)

        if miss_count:
            logger.warning(
                "quote_lookup partial_miss misses=%s statuses=%s fetched=%s",
                miss_count,
                status_counts,
                len(need_fetch),
            )

        logger.info(
            "quote_lookup requested=%s cached=%s fetched=%s returned=%s",
            len(normalized_symbols),
            len(normalized_symbols) - len(need_fetch),
            len(need_fetch),
            len(prices),
        )

        return prices
    except Exception:
        logger.exception("quote_lookup unexpected failure")
        return prices
