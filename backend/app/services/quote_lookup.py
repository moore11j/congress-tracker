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

        logger.info("quote_lookup requesting symbols=%s", ",".join(normalized_symbols))

        response = requests.get(
            f"{FMP_BASE_URL}/quote",
            params={"symbol": ",".join(normalized_symbols), "apikey": api_key},
            timeout=10,
        )
        if response.status_code != 200:
            logger.warning("quote_lookup failed status=%s", response.status_code)
            return {}

        payload = response.json()
        if not isinstance(payload, list):
            return {}

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
