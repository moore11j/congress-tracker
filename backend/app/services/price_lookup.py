from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any, Optional

import requests
from sqlalchemy.orm import Session

from app.clients.fmp import FMP_BASE_URL
from app.models import PriceCache

logger = logging.getLogger(__name__)


def _is_valid_yyyy_mm_dd(value: str) -> bool:
    try:
        parsed = datetime.strptime(value, "%Y-%m-%d")
    except ValueError:
        return False
    return parsed.strftime("%Y-%m-%d") == value


def _extract_close_from_payload(payload: Any, target_date: str) -> float | None:
    rows: list[Any]
    if isinstance(payload, list):
        rows = payload
    elif isinstance(payload, dict):
        data = payload.get("data")
        rows = data if isinstance(data, list) else []
    else:
        return None

    for row in rows:
        if not isinstance(row, dict):
            continue
        row_date = str(row.get("date") or "").strip()
        if row_date != target_date:
            continue
        close_raw = (
            row.get("close")
            or row.get("adjClose")
            or row.get("price")
        )
        try:
            close_value = float(close_raw)
        except (TypeError, ValueError):
            return None
        return close_value
    return None


def get_eod_close(db: Session, symbol: str, date: str) -> Optional[float]:
    """Get EOD close price with SQLite cache-first behavior.

    Returns float on success, otherwise None. Never raises.
    """
    try:
        normalized_symbol = (symbol or "").strip().upper()
        normalized_date = (date or "").strip()

        if not normalized_symbol or not _is_valid_yyyy_mm_dd(normalized_date):
            return None

        cached = db.get(PriceCache, (normalized_symbol, normalized_date))
        if cached is not None:
            logger.info("price_cache hit symbol=%s date=%s", normalized_symbol, normalized_date)
            return float(cached.close)

        logger.info("price_cache miss symbol=%s date=%s", normalized_symbol, normalized_date)

        api_key = os.getenv("FMP_API_KEY", "").strip()
        if not api_key:
            logger.warning("price_lookup upstream fail symbol=%s date=%s reason=missing_api_key", normalized_symbol, normalized_date)
            return None

        try:
            response = requests.get(
                f"{FMP_BASE_URL}/historical-price-eod/full",
                params={
                    "symbol": normalized_symbol,
                    "from": normalized_date,
                    "to": normalized_date,
                    "apikey": api_key,
                },
                timeout=10,
            )
        except requests.RequestException:
            logger.warning("price_lookup upstream fail symbol=%s date=%s reason=request_error", normalized_symbol, normalized_date)
            return None

        if response.status_code != 200:
            logger.warning(
                "price_lookup upstream fail symbol=%s date=%s status=%s",
                normalized_symbol,
                normalized_date,
                response.status_code,
            )
            return None

        try:
            payload = response.json()
        except ValueError:
            logger.warning("price_lookup upstream fail symbol=%s date=%s reason=invalid_json", normalized_symbol, normalized_date)
            return None

        close_value = _extract_close_from_payload(payload, normalized_date)
        if close_value is None:
            logger.info(
                "price_lookup miss with date filter; retrying full series symbol=%s date=%s",
                normalized_symbol,
                normalized_date,
            )

            try:
                response = requests.get(
                    f"{FMP_BASE_URL}/historical-price-eod/full",
                    params={
                        "symbol": normalized_symbol,
                        "apikey": api_key,
                    },
                    timeout=10,
                )
                if response.status_code == 200:
                    payload = response.json()
                    close_value = _extract_close_from_payload(payload, normalized_date)
            except requests.RequestException:
                close_value = None

            if close_value is None:
                logger.info("price_lookup upstream no_data symbol=%s date=%s", normalized_symbol, normalized_date)
                return None

        db.merge(
            PriceCache(
                symbol=normalized_symbol,
                date=normalized_date,
                close=close_value,
            )
        )
        db.commit()
        logger.info("price_lookup upstream success symbol=%s date=%s", normalized_symbol, normalized_date)
        return close_value
    except Exception:
        db.rollback()
        logger.exception("price_lookup unexpected failure")
        return None
