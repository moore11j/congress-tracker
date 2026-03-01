from __future__ import annotations

import logging
import os
from bisect import bisect_right
from datetime import datetime, timedelta
from typing import Any, Optional

import requests
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.clients.fmp import FMP_BASE_URL
from app.models import PriceCache
from app.utils.symbols import normalize_symbol

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



def get_index_eod_map(symbol: str, start_date: str, end_date: str) -> dict[str, float]:
    """Fetch historical EOD closes for index using FMP light endpoint."""
    api_key = os.getenv("FMP_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("FMP_API_KEY not configured")

    response = requests.get(
        f"{FMP_BASE_URL}/historical-price-eod/light",
        params={
            "symbol": symbol,
            "from": start_date,
            "to": end_date,
            "apikey": api_key,
        },
        timeout=15,
    )
    response.raise_for_status()

    data = response.json()
    price_map: dict[str, float] = {}
    if isinstance(data, list):
        rows = data
    elif isinstance(data, dict):
        rows = data.get("data") if isinstance(data.get("data"), list) else []
    else:
        rows = []

    for row in rows:
        if not isinstance(row, dict):
            continue
        date = row.get("date")
        close = row.get("close")
        if date and close is not None:
            price_map[str(date)] = float(close)

    return price_map


def get_close_for_date(date_str: str, price_map: dict[str, float]) -> float | None:
    """Returns close for same date or nearest prior trading day."""
    dt = datetime.strptime(date_str[:10], "%Y-%m-%d")

    for _ in range(7):
        key = dt.strftime("%Y-%m-%d")
        if key in price_map:
            return price_map[key]
        dt -= timedelta(days=1)

    return None


def get_index_series_with_dates(symbol: str, start_date: str, end_date: str) -> tuple[dict[str, float], list[str]]:
    """Fetch benchmark closes and return both date->close map and sorted dates."""
    series_map = get_index_eod_map(symbol=symbol, start_date=start_date, end_date=end_date)
    sorted_dates = sorted(series_map.keys())
    return series_map, sorted_dates


def get_close_for_date_or_prior(date_str: str, price_map: dict[str, float], sorted_dates: list[str]) -> float | None:
    """Return close on date, else closest prior available close."""
    if not price_map or not sorted_dates:
        return None

    target = (date_str or "")[:10]
    if not _is_valid_yyyy_mm_dd(target):
        return None

    if target in price_map:
        return price_map[target]

    idx = bisect_right(sorted_dates, target) - 1
    if idx < 0:
        return None
    return price_map.get(sorted_dates[idx])

def get_eod_close(db: Session, symbol: str, date: str) -> Optional[float]:
    """Get EOD close price with SQLite cache-first behavior.

    Returns float on success, otherwise None. Never raises.
    """
    try:
        normalized_symbol = normalize_symbol(symbol) or ""
        normalized_date = (date or "").strip()

        if not normalized_symbol or not _is_valid_yyyy_mm_dd(normalized_date):
            return None

        cached = db.get(PriceCache, (normalized_symbol, normalized_date))
        if cached is not None:
            logger.debug("price_cache hit symbol=%s date=%s", normalized_symbol, normalized_date)
            return float(cached.close)

        logger.debug("price_cache miss symbol=%s date=%s", normalized_symbol, normalized_date)

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

        stmt = sqlite_insert(PriceCache).values(
            symbol=normalized_symbol,
            date=normalized_date,
            close=close_value,
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=["symbol", "date"],
            set_={"close": close_value},
        )

        try:
            db.execute(stmt)
            db.commit()
        except IntegrityError:
            # Safety valve under concurrent access / stale transaction state.
            db.rollback()
            existing = db.get(PriceCache, (normalized_symbol, normalized_date))
            if existing is not None:
                close_value = float(existing.close)
            else:
                return None

        logger.debug("price_lookup upstream success symbol=%s date=%s", normalized_symbol, normalized_date)
        return close_value
    except Exception:
        db.rollback()
        logger.exception("price_lookup unexpected failure")
        return None
