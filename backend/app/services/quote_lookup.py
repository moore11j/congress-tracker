from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

import requests

from app.clients.fmp import FMP_BASE_URL
from app.utils.symbols import canonical_symbol

logger = logging.getLogger(__name__)

_PRICE_CACHE: dict[str, tuple[float, datetime]] = {}
_PRICE_TTL = timedelta(seconds=60)
_last_paywall_log: datetime | None = None


def get_current_prices(symbols: list[str]) -> dict[str, float]:
    """Returns {SYMBOL: price} for symbols. Safe, timeout, returns empty dict on failure."""
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

        now = datetime.utcnow()
        prices: dict[str, float] = {}
        need_fetch: list[str] = []
        for symbol in normalized_symbols:
            cached = _PRICE_CACHE.get(symbol)
            if cached and (now - cached[1]) <= _PRICE_TTL:
                prices[symbol] = cached[0]
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

        def _fetch_quote_short(symbol: str, asset_type: str) -> None:
            response = requests.get(
                f"{FMP_BASE_URL}/quote-short?symbol={symbol}&apikey={api_key}",
                timeout=10,
            )
            if response.status_code != 200:
                logger.warning(
                    "quote_lookup quote_short failed asset_type=%s symbol=%s status=%s",
                    asset_type,
                    symbol,
                    response.status_code,
                )
                return

            quote_payload = response.json()
            if isinstance(quote_payload, list):
                payload.extend(row for row in quote_payload if isinstance(row, dict))
            elif isinstance(quote_payload, dict):
                payload.append(quote_payload)
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
                    logger.warning("quote_lookup equities invalid_payload_type=%s", type(equities_payload).__name__)
            else:
                global _last_paywall_log
                if response.status_code == 402:
                    now = datetime.utcnow()
                    if _last_paywall_log is None or (now - _last_paywall_log) > timedelta(hours=1):
                        logger.warning("quote_lookup batch_paywalled status=402")
                        _last_paywall_log = now
                else:
                    logger.warning("quote_lookup equities failed status=%s", response.status_code)
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
            _PRICE_CACHE[symbol] = (price, datetime.utcnow())

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
        return {}
