from __future__ import annotations

import logging
import os
import time
from bisect import bisect_right
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional
from zoneinfo import ZoneInfo

import requests
from sqlalchemy import select as sqlalchemy_select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.orm import Session

from app.clients.fmp import FMP_BASE_URL
from app.models import PriceCache
from app.utils.symbols import classify_symbol, symbol_variants

logger = logging.getLogger(__name__)

_NEGATIVE_EOD_CACHE: dict[tuple[str, str], tuple[str, float]] = {}
_PROVIDER_EOD_SERIES_CACHE: dict[tuple[str, str, str, str], tuple[float, Any]] = {}
_PROVIDER_429_COOLDOWN_UNTIL: float = 0.0
_DEFAULT_APP_TIMEZONE = "America/Los_Angeles"
_DEFAULT_MAX_PRIOR_FALLBACK_DAYS = 7
_DEFAULT_DAILY_SERIES_MIN_DENSITY = 0.55
_PROVIDER_EOD_SERIES_CACHE_TTL_SECONDS = 15 * 60


def _max_prior_fallback_days() -> int:
    raw = os.getenv("PRICE_LOOKUP_MAX_PRIOR_FALLBACK_DAYS", "").strip()
    try:
        parsed = int(raw) if raw else _DEFAULT_MAX_PRIOR_FALLBACK_DAYS
    except ValueError:
        parsed = _DEFAULT_MAX_PRIOR_FALLBACK_DAYS
    return max(0, min(parsed, 14))


def _prior_fallback_within_bounds(requested_date: str, resolved_date: str, max_days: int) -> tuple[bool, int]:
    requested = datetime.strptime(requested_date, "%Y-%m-%d").date()
    resolved = datetime.strptime(resolved_date, "%Y-%m-%d").date()
    delta_days = (requested - resolved).days
    return 0 <= delta_days <= max_days, delta_days


def _safe_cache_upsert(db: Session, symbol: str, day: str, close_value: float) -> bool:
    stmt = sqlite_insert(PriceCache).values(symbol=symbol, date=day, close=close_value)
    stmt = stmt.on_conflict_do_update(index_elements=["symbol", "date"], set_={"close": close_value})
    try:
        with db.begin_nested():
            db.execute(stmt)
            db.flush()
        return True
    except (IntegrityError, OperationalError):
        logger.warning("price_cache upsert skipped symbol=%s date=%s", symbol, day, exc_info=True)
        return False


def _is_valid_yyyy_mm_dd(value: str) -> bool:
    try:
        parsed = datetime.strptime(value, "%Y-%m-%d")
    except ValueError:
        return False
    return parsed.strftime("%Y-%m-%d") == value


def effective_lookup_max_date(now_utc: datetime | None = None) -> date:
    """Return the latest allowed lookup date based on app-local calendar day."""
    tz_name = os.getenv("APP_TIMEZONE", _DEFAULT_APP_TIMEZONE).strip() or _DEFAULT_APP_TIMEZONE
    try:
        app_tz = ZoneInfo(tz_name)
    except Exception:
        app_tz = ZoneInfo(_DEFAULT_APP_TIMEZONE)
    current_utc = now_utc if now_utc is not None else datetime.now(timezone.utc)
    if current_utc.tzinfo is None:
        current_utc = current_utc.replace(tzinfo=timezone.utc)
    else:
        current_utc = current_utc.astimezone(timezone.utc)
    return current_utc.astimezone(app_tz).date()


def clamp_lookup_date(date_str: str) -> tuple[str, bool]:
    """Clamp input date to app-local max date to avoid future-day lookups."""
    parsed = datetime.strptime(date_str, "%Y-%m-%d").date()
    max_date = effective_lookup_max_date()
    if parsed <= max_date:
        return date_str, False
    return max_date.isoformat(), True


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
        close_raw = row.get("close") or row.get("adjClose") or row.get("price")
        try:
            close_value = float(close_raw)
        except (TypeError, ValueError):
            return None
        return close_value
    return None


def _extract_close_on_or_prior_from_payload(payload: Any, target_date: str) -> tuple[float, str] | None:
    rows: list[Any]
    if isinstance(payload, list):
        rows = payload
    elif isinstance(payload, dict):
        data = payload.get("data")
        rows = data if isinstance(data, list) else []
    else:
        return None

    closes_by_day: dict[str, float] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        row_date = str(row.get("date") or "").strip()[:10]
        if not _is_valid_yyyy_mm_dd(row_date) or row_date > target_date:
            continue
        close_raw = row.get("close") or row.get("adjClose") or row.get("price")
        try:
            close_value = float(close_raw)
        except (TypeError, ValueError):
            continue
        closes_by_day[row_date] = close_value

    if not closes_by_day:
        return None

    sorted_days = sorted(closes_by_day.keys())
    idx = bisect_right(sorted_days, target_date) - 1
    if idx < 0:
        return None
    resolved_date = sorted_days[idx]
    return closes_by_day[resolved_date], resolved_date


def _rows_from_series_payload(payload: Any) -> list[Any]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        data = payload.get("data")
        return data if isinstance(data, list) else []
    return []


def _extract_close_series_from_payload(payload: Any, start_date: str, end_date: str) -> dict[str, float]:
    price_map: dict[str, float] = {}
    for row in _rows_from_series_payload(payload):
        if not isinstance(row, dict):
            continue
        row_date = str(row.get("date") or "").strip()[:10]
        if not _is_valid_yyyy_mm_dd(row_date) or row_date < start_date or row_date > end_date:
            continue
        close_raw = row.get("close") or row.get("adjClose") or row.get("price")
        try:
            close_value = float(close_raw)
        except (TypeError, ValueError):
            continue
        if close_value <= 0:
            continue
        price_map[row_date] = close_value
    return dict(sorted(price_map.items()))


def _extract_volume_series_from_payload(payload: Any, start_date: str, end_date: str) -> dict[str, float]:
    volume_map: dict[str, float] = {}
    for row in _rows_from_series_payload(payload):
        if not isinstance(row, dict):
            continue
        row_date = str(row.get("date") or "").strip()[:10]
        if not _is_valid_yyyy_mm_dd(row_date) or row_date < start_date or row_date > end_date:
            continue
        volume_raw = row.get("volume")
        try:
            volume_value = float(volume_raw)
        except (TypeError, ValueError):
            continue
        if volume_value <= 0:
            continue
        volume_map[row_date] = volume_value
    return dict(sorted(volume_map.items()))


def _weekday_count(start_date: str, end_date: str) -> int:
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()
    if start > end:
        start, end = end, start

    days = (end - start).days + 1
    full_weeks, remainder = divmod(days, 7)
    weekdays = full_weeks * 5
    for offset in range(remainder):
        if (start + timedelta(days=offset)).weekday() < 5:
            weekdays += 1
    return weekdays


def _daily_series_min_density() -> float:
    raw = os.getenv("PRICE_LOOKUP_DAILY_SERIES_MIN_DENSITY", "").strip()
    try:
        parsed = float(raw) if raw else _DEFAULT_DAILY_SERIES_MIN_DENSITY
    except ValueError:
        parsed = _DEFAULT_DAILY_SERIES_MIN_DENSITY
    return max(0.1, min(parsed, 0.95))


def is_sparse_daily_close_series(price_map: dict[str, float], start_date: str, end_date: str) -> bool:
    expected_weekdays = _weekday_count(start_date, end_date)
    if expected_weekdays <= 0:
        return not price_map

    min_points = max(8, int(expected_weekdays * _daily_series_min_density()))
    return len(price_map) < min_points


def _negative_cache_get(symbol: str, date: str) -> str | None:
    cached = _NEGATIVE_EOD_CACHE.get((symbol, date))
    if not cached:
        return None
    status, expires_at = cached
    if time.time() >= expires_at:
        _NEGATIVE_EOD_CACHE.pop((symbol, date), None)
        return None
    return status


def _negative_cache_set(symbol: str, date: str, status: str, ttl_seconds: int) -> None:
    _NEGATIVE_EOD_CACHE[(symbol, date)] = (status, time.time() + max(ttl_seconds, 60))


def _fetch_with_backoff(url: str, params: dict[str, str], retries: int = 2) -> requests.Response | None:
    global _PROVIDER_429_COOLDOWN_UNTIL
    now = time.time()
    if now < _PROVIDER_429_COOLDOWN_UNTIL:
        return None

    for attempt in range(retries + 1):
        try:
            response = requests.get(url, params=params, timeout=10)
        except requests.RequestException:
            return None

        if response.status_code != 429:
            return response

        if attempt < retries:
            time.sleep(1 * (2 ** attempt))
            continue

        _PROVIDER_429_COOLDOWN_UNTIL = time.time() + 120
        return response

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


def get_eod_close_with_meta(db: Session, symbol: str, date: str) -> dict[str, Any]:
    normalized_date = (date or "").strip()
    status, normalized_symbol, classify_error = classify_symbol(symbol)
    if status != "eligible":
        return {"close": None, "status": status, "error": classify_error, "symbol": normalized_symbol}

    if not normalized_symbol or not _is_valid_yyyy_mm_dd(normalized_date):
        return {"close": None, "status": "unsupported_symbol", "error": "Invalid symbol/date", "symbol": normalized_symbol}
    normalized_date, was_clamped = clamp_lookup_date(normalized_date)
    if was_clamped:
        logger.info(
            "price_lookup future_date_clamped symbol=%s requested=%s effective=%s",
            normalized_symbol,
            (date or "").strip(),
            normalized_date,
        )

    saw_402 = False
    saw_429 = False
    saw_cooldown = False
    saw_stale_prior_candidate = False
    max_prior_days = _max_prior_fallback_days()

    for candidate_symbol in symbol_variants(normalized_symbol):
        negative_status = _negative_cache_get(candidate_symbol, normalized_date)
        if negative_status is not None:
            if negative_status == "provider_402":
                saw_402 = True
            elif negative_status == "provider_429":
                saw_429 = True
            continue

        cached = db.get(PriceCache, (candidate_symbol, normalized_date))
        if cached is not None:
            logger.debug("price_cache hit symbol=%s date=%s", candidate_symbol, normalized_date)
            return {"close": float(cached.close), "status": "ok", "error": None, "symbol": candidate_symbol}

        api_key = os.getenv("FMP_API_KEY", "").strip()
        if not api_key:
            return {"close": None, "status": "provider_unavailable", "error": "missing_api_key", "symbol": candidate_symbol}

        response = _fetch_with_backoff(
            f"{FMP_BASE_URL}/historical-price-eod/full",
            {
                "symbol": candidate_symbol,
                "from": normalized_date,
                "to": normalized_date,
                "apikey": api_key,
            },
        )
        if response is None:
            saw_cooldown = True
            continue

        if response.status_code == 402:
            saw_402 = True
            _negative_cache_set(candidate_symbol, normalized_date, "provider_402", ttl_seconds=86400)
            continue
        if response.status_code == 429:
            saw_429 = True
            _negative_cache_set(candidate_symbol, normalized_date, "provider_429", ttl_seconds=600)
            continue
        if response.status_code != 200:
            continue

        try:
            payload = response.json()
        except ValueError:
            continue

        close_value = _extract_close_from_payload(payload, normalized_date)
        resolved_close_date = normalized_date
        if close_value is None:
            logger.info(
                "price_lookup miss with date filter; retrying full series symbol=%s date=%s",
                candidate_symbol,
                normalized_date,
            )
            retry_response = _fetch_with_backoff(
                f"{FMP_BASE_URL}/historical-price-eod/full",
                {
                    "symbol": candidate_symbol,
                    "apikey": api_key,
                },
            )
            if retry_response is not None and retry_response.status_code == 200:
                try:
                    retry_payload = retry_response.json()
                    close_value = _extract_close_from_payload(retry_payload, normalized_date)
                    if close_value is None:
                        prior_close = _extract_close_on_or_prior_from_payload(retry_payload, normalized_date)
                        if prior_close is not None:
                            candidate_close_value, candidate_close_date = prior_close
                            within_bounds, delta_days = _prior_fallback_within_bounds(
                                normalized_date, candidate_close_date, max_prior_days
                            )
                            if within_bounds:
                                close_value, resolved_close_date = candidate_close_value, candidate_close_date
                                logger.info(
                                    "price_lookup resolved prior trading day symbol=%s requested_date=%s resolved_date=%s delta_days=%s max_days=%s",
                                    candidate_symbol,
                                    normalized_date,
                                    resolved_close_date,
                                    delta_days,
                                    max_prior_days,
                                )
                            else:
                                saw_stale_prior_candidate = True
                                logger.info(
                                    "price_lookup rejected stale prior trading day symbol=%s requested_date=%s resolved_date=%s delta_days=%s max_days=%s",
                                    candidate_symbol,
                                    normalized_date,
                                    candidate_close_date,
                                    delta_days,
                                    max_prior_days,
                                )
                                close_value = None
                except ValueError:
                    close_value = None

        if close_value is None:
            _negative_cache_set(candidate_symbol, normalized_date, "no_data", ttl_seconds=21600)
            logger.info("price_lookup upstream no_data symbol=%s date=%s", candidate_symbol, normalized_date)
            continue

        _safe_cache_upsert(db, candidate_symbol, normalized_date, close_value)
        return {
            "close": close_value,
            "status": "ok",
            "error": None,
            "symbol": candidate_symbol,
            "price_date": resolved_close_date,
            "fallback_days": (datetime.strptime(normalized_date, "%Y-%m-%d").date() - datetime.strptime(resolved_close_date, "%Y-%m-%d").date()).days,
        }

    if saw_402:
        return {"close": None, "status": "provider_402", "error": "Provider plan does not cover symbol", "symbol": normalized_symbol}
    if saw_429 or saw_cooldown:
        return {"close": None, "status": "provider_429", "error": "Provider rate-limited request", "symbol": normalized_symbol}
    return {
        "close": None,
        "status": "no_data",
        "error": (
            f"No EOD data for symbol variants: {symbol_variants(normalized_symbol)}"
            + (f"; prior candidate exceeded max fallback window ({max_prior_days} days)" if saw_stale_prior_candidate else "")
        ),
        "symbol": normalized_symbol,
    }


def get_eod_close(db: Session, symbol: str, date: str) -> Optional[float]:
    """Backward-compatible close-only lookup."""
    try:
        result = get_eod_close_with_meta(db, symbol, date)
        close = result.get("close")
        return float(close) if close is not None else None
    except Exception:
        db.rollback()
        logger.exception("price_lookup unexpected failure")
        return None


def get_eod_close_series(db: Session, symbol: str, start_date: str, end_date: str) -> dict[str, float]:
    """Return cached EOD history for a date window (emergency lean read path)."""
    status, normalized_symbol, _ = classify_symbol(symbol)
    if status != "eligible" or not normalized_symbol:
        return {}

    start_key = (start_date or "")[:10]
    end_key = (end_date or "")[:10]
    if not _is_valid_yyyy_mm_dd(start_key) or not _is_valid_yyyy_mm_dd(end_key):
        return {}
    if start_key > end_key:
        start_key, end_key = end_key, start_key

    rows = db.execute(
        sqlalchemy_select(PriceCache.date, PriceCache.close)
        .where(PriceCache.symbol == normalized_symbol)
        .where(PriceCache.date >= start_key)
        .where(PriceCache.date <= end_key)
    ).all()
    return dict(sorted((str(row[0]), float(row[1])) for row in rows))


def _fetch_provider_eod_payload(
    endpoint: str,
    candidate_symbol: str,
    start_date: str,
    end_date: str,
    api_key: str,
):
    cache_key = (endpoint, candidate_symbol, start_date, end_date)
    cached = _PROVIDER_EOD_SERIES_CACHE.get(cache_key)
    if cached and time.time() < cached[0]:
        return cached[1]

    response = _fetch_with_backoff(
        f"{FMP_BASE_URL}/{endpoint}",
        {
            "symbol": candidate_symbol,
            "from": start_date,
            "to": end_date,
            "apikey": api_key,
        },
        retries=1,
    )
    if response is None or response.status_code != 200:
        return response

    try:
        payload = response.json()
    except ValueError:
        return response

    _PROVIDER_EOD_SERIES_CACHE[cache_key] = (
        time.time() + _PROVIDER_EOD_SERIES_CACHE_TTL_SECONDS,
        payload,
    )
    return payload


def _fetch_provider_eod_close_series(symbol: str, start_date: str, end_date: str) -> tuple[dict[str, float], str | None]:
    api_key = os.getenv("FMP_API_KEY", "").strip()
    if not api_key:
        return {}, None

    best_map: dict[str, float] = {}
    best_symbol: str | None = None
    saw_402 = False
    saw_429 = False

    for candidate_symbol in symbol_variants(symbol):
        for endpoint in ("historical-price-eod/full", "historical-price-eod/light"):
            provider_payload = _fetch_provider_eod_payload(endpoint, candidate_symbol, start_date, end_date, api_key)
            if provider_payload is None:
                continue
            status_code = getattr(provider_payload, "status_code", 200)
            if status_code == 402:
                saw_402 = True
                break
            if status_code == 429:
                saw_429 = True
                break
            if status_code != 200:
                continue

            provider_map = _extract_close_series_from_payload(provider_payload, start_date, end_date)
            if len(provider_map) > len(best_map):
                best_map = provider_map
                best_symbol = candidate_symbol
            if provider_map and not is_sparse_daily_close_series(provider_map, start_date, end_date):
                return provider_map, candidate_symbol

    if saw_402:
        logger.info("price_lookup provider plan did not cover dense history symbol=%s", symbol)
    if saw_429:
        logger.info("price_lookup provider rate-limited dense history symbol=%s", symbol)
    return best_map, best_symbol


def get_daily_close_series_with_fallback(
    db: Session,
    symbol: str,
    start_date: str,
    end_date: str,
) -> dict[str, float]:
    """Return chart-grade daily EOD history, hydrating sparse cache from provider when possible."""
    status, normalized_symbol, _ = classify_symbol(symbol)
    if status != "eligible" or not normalized_symbol:
        return {}

    start_key = (start_date or "")[:10]
    end_key = (end_date or "")[:10]
    if not _is_valid_yyyy_mm_dd(start_key) or not _is_valid_yyyy_mm_dd(end_key):
        return {}
    if start_key > end_key:
        start_key, end_key = end_key, start_key

    cached_map = get_eod_close_series(db, normalized_symbol, start_key, end_key)
    if cached_map and not is_sparse_daily_close_series(cached_map, start_key, end_key):
        return cached_map

    provider_map, provider_symbol = _fetch_provider_eod_close_series(normalized_symbol, start_key, end_key)
    if provider_map:
        cache_symbol = provider_symbol or normalized_symbol
        wrote_any = False
        for day, close_value in provider_map.items():
            wrote_any = _safe_cache_upsert(db, cache_symbol, day, close_value) or wrote_any
        if wrote_any:
            try:
                db.commit()
            except Exception:
                db.rollback()
                logger.warning(
                    "price_lookup dense history cache commit failed symbol=%s provider_symbol=%s",
                    normalized_symbol,
                    cache_symbol,
                    exc_info=True,
                )
        if len(provider_map) >= len(cached_map):
            logger.info(
                "price_lookup dense history hydrated symbol=%s provider_symbol=%s cached_points=%s provider_points=%s start=%s end=%s",
                normalized_symbol,
                cache_symbol,
                len(cached_map),
                len(provider_map),
                start_key,
                end_key,
            )
            return provider_map

    return cached_map


def get_daily_volume_series_from_provider(
    symbol: str,
    start_date: str,
    end_date: str,
) -> dict[str, float]:
    """Return daily volume observations from the same FMP EOD history source used for chart hydration."""
    status, normalized_symbol, _ = classify_symbol(symbol)
    if status != "eligible" or not normalized_symbol:
        return {}

    start_key = (start_date or "")[:10]
    end_key = (end_date or "")[:10]
    if not _is_valid_yyyy_mm_dd(start_key) or not _is_valid_yyyy_mm_dd(end_key):
        return {}
    if start_key > end_key:
        start_key, end_key = end_key, start_key

    api_key = os.getenv("FMP_API_KEY", "").strip()
    if not api_key:
        return {}

    best_map: dict[str, float] = {}
    for candidate_symbol in symbol_variants(normalized_symbol):
        provider_payload = _fetch_provider_eod_payload(
            "historical-price-eod/full",
            candidate_symbol,
            start_key,
            end_key,
            api_key,
        )
        if provider_payload is None or getattr(provider_payload, "status_code", 200) != 200:
            continue
        provider_map = _extract_volume_series_from_payload(provider_payload, start_key, end_key)
        if len(provider_map) > len(best_map):
            best_map = provider_map
        if len(provider_map) >= 30:
            return provider_map
    return best_map
