from __future__ import annotations

import logging
import os

import requests

from app.clients.fmp import FMP_BASE_URL
from app.utils.symbols import canonical_symbol

logger = logging.getLogger(__name__)


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

        api_key = os.getenv("FMP_API_KEY", "").strip()
        if not api_key:
            logger.warning("quote_lookup skipped reason=missing_api_key")
            return {}

        equities: list[str] = []
        crypto: list[str] = []
        for symbol in normalized_symbols:
            if symbol.endswith("USD") and len(symbol) <= 10:
                crypto.append(symbol)
            else:
                equities.append(symbol)

        payload: list[dict] = []
        if equities:
            logger.info("quote_lookup requesting equities=%s", ",".join(equities))
            response = requests.get(
                f"{FMP_BASE_URL}/batch-quote-short?symbols={','.join(equities)}&apikey={api_key}",
                timeout=10,
            )
            if response.status_code != 200:
                logger.warning("quote_lookup equities failed status=%s", response.status_code)
                return {}
            equities_payload = response.json()
            if not isinstance(equities_payload, list):
                return {}
            payload.extend(row for row in equities_payload if isinstance(row, dict))

        for symbol in crypto:
            logger.info("quote_lookup requesting crypto=%s", symbol)
            response = requests.get(
                f"{FMP_BASE_URL}/quote-short?symbol={symbol}&apikey={api_key}",
                timeout=10,
            )
            if response.status_code != 200:
                logger.warning("quote_lookup crypto failed symbol=%s status=%s", symbol, response.status_code)
                return {}
            crypto_payload = response.json()
            if isinstance(crypto_payload, list):
                payload.extend(row for row in crypto_payload if isinstance(row, dict))
            elif isinstance(crypto_payload, dict):
                payload.append(crypto_payload)

        prices: dict[str, float] = {}
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

        return prices
    except Exception:
        logger.exception("quote_lookup unexpected failure")
        return {}
