from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timedelta

import requests

from app.clients.fmp import FMP_BASE_URL
from app.utils.symbols import canonical_symbol

logger = logging.getLogger(__name__)

_QUOTE_CACHE: dict[str, tuple[float, float]] = {}
_last_paywall_log: datetime | None = None


def _cache_ttl_seconds() -> int:
    try:
        ttl = int(os.getenv("QUOTE_CACHE_TTL_SECONDS", "60"))
    except ValueError:
        ttl = 60
    return max(ttl, 1)


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

        def _fetch_quote_short(symbol: str, asset_type: str) -> None:
            response = requests.get(
                f"{FMP_BASE_URL}/quote-short?symbol={symbol}&apikey={api_key}",
                timeout=10,
            )
            if response.status_code != 200:
                _record_miss(response.status_code)
                if response.status_code == 402:
                    global _last_paywall_log
                    now = datetime.utcnow()
                    if _last_paywall_log is None or (now - _last_paywall_log) > timedelta(hours=1):
                        logger.warning("quote_lookup batch_paywalled status=402")
                        _last_paywall_log = now
                return

            quote_payload = response.json()
            if isinstance(quote_payload, list):
                payload.extend(row for row in quote_payload if isinstance(row, dict))
            elif isinstance(quote_payload, dict):
                payload.append(quote_payload)

        def _fetch_batch_equities() -> requests.Response:
            return requests.get(
                f"{FMP_BASE_URL}/batch-quote-short?symbols={','.join(equities)}&apikey={api_key}",
                timeout=10,
            )

        if equities:
            logger.info("quote_lookup requesting equities=%s", ",".join(equities))
            response = _fetch_batch_equities()
            if response.status_code == 429 and len(equities) < 5:
                time.sleep(0.5)
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
                _record_miss(response.status_code, count=len(equities))
                for symbol in equities:
                    _fetch_quote_short(symbol, asset_type="equity")

        for symbol in crypto:
            logger.info("quote_lookup requesting crypto=%s", symbol)
            _fetch_quote_short(symbol, asset_type="crypto")

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
