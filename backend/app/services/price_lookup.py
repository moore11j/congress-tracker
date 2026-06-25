from __future__ import annotations

import logging
import os
import time
from bisect import bisect_right
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional
from urllib.parse import quote
from zoneinfo import ZoneInfo

import requests
from sqlalchemy import select as sqlalchemy_select
from sqlalchemy.dialects.postgresql import insert as postgres_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.orm import Session

from app.clients.fmp import FMP_BASE_URL
from app.models import PriceCache
from app.request_priority import get_request_context
from app.services.provider_usage import (
    ProviderUnavailable,
    ensure_fmp_live_allowed,
    record_cache_hit,
    record_cache_miss,
    record_fallback,
    record_provider_response,
)
from app.services.data_enrichment_queue import enqueue_data_enrichment_job
from app.utils.symbols import classify_symbol, symbol_variants

logger = logging.getLogger(__name__)

_NEGATIVE_EOD_CACHE: dict[tuple[str, str], tuple[str, float]] = {}
_PROVIDER_EOD_SERIES_CACHE: dict[tuple[str, str, str, str], tuple[float, Any]] = {}
_PROVIDER_429_COOLDOWN_UNTIL: float = 0.0


def _enqueue_eod_refresh(symbol: str | None, date_key: str | None = None, *, reason: str, window_key: str | None = None) -> None:
    if not symbol:
        return
    enqueue_data_enrichment_job(
        job_type="price_eod" if date_key else "price_series",
        symbol=symbol,
        date_key=date_key,
        window_key=window_key,
        source="page_load",
        reason=reason,
        priority=30,
    )
_DEFAULT_APP_TIMEZONE = "America/Los_Angeles"
_MARKET_TIMEZONE = "America/New_York"
_DEFAULT_MAX_PRIOR_FALLBACK_DAYS = 7
_DEFAULT_DAILY_SERIES_MIN_DENSITY = 0.55
_PROVIDER_EOD_SERIES_CACHE_TTL_SECONDS = 15 * 60
_DEFAULT_RECENT_REFRESH_TRADING_DAYS = 15
_DEFAULT_EOD_READY_HOUR_ET = 18


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


def _price_cache_insert(db: Session):
    get_bind = getattr(db, "get_bind", None)
    dialect_name = get_bind().dialect.name if callable(get_bind) else "sqlite"
    if dialect_name == "postgresql":
        return postgres_insert(PriceCache.__table__)
    return sqlite_insert(PriceCache.__table__)


def _safe_cache_upsert(
    db: Session,
    symbol: str,
    day: str,
    close_value: float,
    volume_value: float | None = None,
    day_volume_value: float | None = None,
) -> bool:
    values: dict[str, Any] = {"symbol": symbol, "date": day, "close": close_value}
    now = datetime.now(timezone.utc)
    update_values: dict[str, Any] = {"close": close_value, "updated_at": now}
    if volume_value is not None:
        values["volume"] = volume_value
        update_values["volume"] = volume_value
        values["day_volume"] = volume_value if day_volume_value is None else day_volume_value
        update_values["day_volume"] = values["day_volume"]

    stmt = _price_cache_insert(db).values(**values)
    stmt = stmt.on_conflict_do_update(index_elements=["symbol", "date"], set_=update_values)
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


def _observed_fixed_holiday(year: int, month: int, day: int) -> date:
    holiday = date(year, month, day)
    if holiday.weekday() == 5:
        return holiday - timedelta(days=1)
    if holiday.weekday() == 6:
        return holiday + timedelta(days=1)
    return holiday


def _nth_weekday(year: int, month: int, weekday: int, nth: int) -> date:
    cursor = date(year, month, 1)
    while cursor.weekday() != weekday:
        cursor += timedelta(days=1)
    return cursor + timedelta(days=7 * (nth - 1))


def _last_weekday(year: int, month: int, weekday: int) -> date:
    cursor = date(year, month + 1, 1) - timedelta(days=1) if month < 12 else date(year, 12, 31)
    while cursor.weekday() != weekday:
        cursor -= timedelta(days=1)
    return cursor


def _easter_date(year: int) -> date:
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return date(year, month, day)


def _us_market_holidays(year: int) -> set[date]:
    return {
        _observed_fixed_holiday(year, 1, 1),
        _nth_weekday(year, 1, 0, 3),
        _nth_weekday(year, 2, 0, 3),
        _easter_date(year) - timedelta(days=2),
        _last_weekday(year, 5, 0),
        _observed_fixed_holiday(year, 6, 19),
        _observed_fixed_holiday(year, 7, 4),
        _nth_weekday(year, 9, 0, 1),
        _nth_weekday(year, 11, 3, 4),
        _observed_fixed_holiday(year, 12, 25),
    }


def is_market_trading_day(day: date) -> bool:
    return day.weekday() < 5 and day not in _us_market_holidays(day.year)


def previous_market_trading_day(day: date) -> date:
    cursor = day - timedelta(days=1)
    while not is_market_trading_day(cursor):
        cursor -= timedelta(days=1)
    return cursor


def _recent_market_window_start(end_day: date, trading_days: int) -> date:
    remaining = max(1, trading_days)
    cursor = end_day
    while remaining > 1:
        cursor = previous_market_trading_day(cursor)
        remaining -= 1
    return cursor


def _eod_ready_hour_et() -> int:
    raw = os.getenv("PRICE_HISTORY_EOD_READY_HOUR_ET", "").strip()
    try:
        parsed = int(raw) if raw else _DEFAULT_EOD_READY_HOUR_ET
    except ValueError:
        parsed = _DEFAULT_EOD_READY_HOUR_ET
    return max(14, min(parsed, 23))


def get_expected_latest_market_date(now_utc: datetime | None = None) -> date:
    """Latest completed trading session expected to have EOD chart data."""
    try:
        market_tz = ZoneInfo(_MARKET_TIMEZONE)
    except Exception:
        market_tz = timezone.utc
    current_utc = now_utc if now_utc is not None else datetime.now(timezone.utc)
    if current_utc.tzinfo is None:
        current_utc = current_utc.replace(tzinfo=timezone.utc)
    local_now = current_utc.astimezone(market_tz)
    candidate = local_now.date()
    if is_market_trading_day(candidate) and local_now.hour < _eod_ready_hour_et():
        candidate = previous_market_trading_day(candidate)
    while not is_market_trading_day(candidate):
        candidate = previous_market_trading_day(candidate)
    return candidate


def _is_foreground_request_context() -> bool:
    context = get_request_context() or {}
    path = str(context.get("path") or "")
    return bool(path and path != "background")


def latest_price_history_row(db: Session, symbol: str) -> dict[str, Any]:
    status, normalized_symbol, classify_error = classify_symbol(symbol)
    if status != "eligible" or not normalized_symbol:
        return {
            "symbol": normalized_symbol,
            "status": status,
            "error": classify_error,
            "latest_date": None,
            "close": None,
            "updated_at": None,
        }

    best: dict[str, Any] | None = None
    for candidate_symbol in symbol_variants(normalized_symbol):
        row = db.execute(
            sqlalchemy_select(PriceCache.date, PriceCache.close, PriceCache.updated_at)
            .where(PriceCache.symbol == candidate_symbol)
            .order_by(PriceCache.date.desc())
            .limit(1)
        ).first()
        if row is None:
            continue
        payload = {
            "symbol": candidate_symbol,
            "status": "ok",
            "error": None,
            "latest_date": str(row[0]),
            "close": float(row[1]),
            "updated_at": row[2],
        }
        if best is None or str(payload["latest_date"]) > str(best["latest_date"]):
            best = payload

    if best is not None:
        return best
    return {
        "symbol": normalized_symbol,
        "status": "missing",
        "error": None,
        "latest_date": None,
        "close": None,
        "updated_at": None,
    }


def is_price_history_stale(
    db: Session,
    symbol: str,
    *,
    expected_date: date | str | None = None,
) -> dict[str, Any]:
    expected = date.fromisoformat(expected_date) if isinstance(expected_date, str) else expected_date
    expected = expected or get_expected_latest_market_date()
    latest = latest_price_history_row(db, symbol)
    latest_date = latest.get("latest_date")
    is_stale = latest_date is None or str(latest_date) < expected.isoformat()
    return {
        "symbol": latest.get("symbol"),
        "expected_latest_date": expected.isoformat(),
        "latest_date": latest_date,
        "latest_close": latest.get("close"),
        "updated_at": latest.get("updated_at"),
        "is_stale": is_stale,
        "status": "stale" if is_stale and latest_date else "missing" if is_stale else "ok",
    }


def refresh_recent_price_history(
    db: Session,
    symbol: str,
    *,
    lookback_days: int = _DEFAULT_RECENT_REFRESH_TRADING_DAYS,
    end_date: date | str | None = None,
) -> dict[str, Any]:
    status, normalized_symbol, classify_error = classify_symbol(symbol)
    if status != "eligible" or not normalized_symbol:
        return {
            "symbol": normalized_symbol,
            "status": status,
            "error": classify_error,
            "start_date": None,
            "end_date": None,
            "rows": 0,
        }

    end_day = date.fromisoformat(end_date) if isinstance(end_date, str) else end_date
    end_day = end_day or get_expected_latest_market_date()
    start_day = _recent_market_window_start(end_day, max(1, min(int(lookback_days or 1), 45)))
    start_key = start_day.isoformat()
    end_key = end_day.isoformat()

    provider_map, provider_volume_map, provider_symbol = _fetch_provider_eod_price_volume_series(
        normalized_symbol,
        start_key,
        end_key,
        allow_user_request=True,
    )
    if not provider_map:
        logger.warning(
            "price_history_recent_refresh_no_data symbol=%s start=%s end=%s",
            normalized_symbol,
            start_key,
            end_key,
        )
        return {
            "symbol": normalized_symbol,
            "status": "no_data",
            "error": "no_recent_price_history",
            "start_date": start_key,
            "end_date": end_key,
            "rows": 0,
        }

    cache_symbol = provider_symbol or normalized_symbol
    wrote_any = False
    for day, close_value in provider_map.items():
        wrote_any = _safe_cache_upsert(db, cache_symbol, day, close_value, provider_volume_map.get(day)) or wrote_any
    if wrote_any:
        try:
            db.commit()
        except Exception:
            db.rollback()
            logger.warning(
                "price_history_recent_refresh_commit_failed symbol=%s refresh_symbol=%s start=%s end=%s",
                normalized_symbol,
                cache_symbol,
                start_key,
                end_key,
                exc_info=True,
            )
            raise

    logger.info(
        "price_history_recent_refresh_done symbol=%s refresh_symbol=%s rows=%s start=%s end=%s latest=%s",
        normalized_symbol,
        cache_symbol,
        len(provider_map),
        start_key,
        end_key,
        max(provider_map) if provider_map else None,
    )
    return {
        "symbol": cache_symbol,
        "status": "ok",
        "error": None,
        "start_date": start_key,
        "end_date": end_key,
        "rows": len(provider_map),
        "latest_date": max(provider_map) if provider_map else None,
    }


def ensure_fresh_price_history(
    db: Session,
    symbol: str,
    *,
    expected_date: date | str | None = None,
    lookback_days: int = _DEFAULT_RECENT_REFRESH_TRADING_DAYS,
) -> dict[str, Any]:
    expected = date.fromisoformat(expected_date) if isinstance(expected_date, str) else expected_date
    expected = expected or get_expected_latest_market_date()
    before = is_price_history_stale(db, symbol, expected_date=expected)
    if not before["is_stale"]:
        return {
            **before,
            "refresh_attempted": False,
            "refresh_status": "not_needed",
            "message": f"Updated through {before['latest_date']}.",
        }

    logger.warning(
        "price_history_stale_detected symbol=%s latest=%s expected=%s",
        symbol,
        before.get("latest_date"),
        expected.isoformat(),
    )
    refresh_status = "failed"
    refresh_error = None
    try:
        refresh = refresh_recent_price_history(
            db,
            symbol,
            lookback_days=lookback_days,
            end_date=expected,
        )
        refresh_status = str(refresh.get("status") or "failed")
        refresh_error = refresh.get("error")
    except Exception as exc:
        db.rollback()
        refresh_error = exc.__class__.__name__
        logger.warning(
            "price_history_recent_refresh_failed symbol=%s latest=%s expected=%s",
            symbol,
            before.get("latest_date"),
            expected.isoformat(),
            exc_info=True,
        )

    after = is_price_history_stale(db, symbol, expected_date=expected)
    if after["is_stale"]:
        logger.warning(
            "price_history_stale_after_refresh symbol=%s latest=%s expected=%s refresh_status=%s",
            symbol,
            after.get("latest_date"),
            expected.isoformat(),
            refresh_status,
        )
    return {
        **after,
        "refresh_attempted": True,
        "refresh_status": refresh_status,
        "refresh_error": refresh_error,
        "message": (
            f"Updated through {after['latest_date']}."
            if not after["is_stale"] and after.get("latest_date")
            else "Latest market data is temporarily unavailable."
        ),
    }


def is_sparse_daily_close_series(price_map: dict[str, float], start_date: str, end_date: str) -> bool:
    expected_weekdays = _weekday_count(start_date, end_date)
    if expected_weekdays <= 0:
        return not price_map

    min_points = max(8, int(expected_weekdays * _daily_series_min_density()))
    return len(price_map) < min_points


def _series_has_stale_tail(price_map: dict[str, float], end_date: str) -> bool:
    if not price_map or not _is_valid_yyyy_mm_dd(end_date):
        return False
    latest_day = max(price_map)
    if latest_day >= end_date:
        return False
    # Allow the current trading day to be missing before the EOD provider has
    # published the close, but do not accept dense series with a multi-day stale tail.
    return _weekday_count(latest_day, end_date) > 2


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

    ensure_fmp_live_allowed(category="price:index-eod", symbol=symbol)
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
    record_provider_response(category="price:index-eod", symbol=symbol, status_code=response.status_code)
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


def get_eod_close_with_meta(
    db: Session,
    symbol: str,
    date: str,
    *,
    allow_cache_write: bool = True,
) -> dict[str, Any]:
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
            record_cache_hit(category="price:eod", symbol=candidate_symbol)
            return {"close": float(cached.close), "status": "ok", "error": None, "symbol": candidate_symbol}
        record_cache_miss(category="price:eod", symbol=candidate_symbol)

        api_key = os.getenv("FMP_API_KEY", "").strip()
        if not api_key:
            record_fallback(category="price:eod", symbol=candidate_symbol, reason="provider_disabled")
            _enqueue_eod_refresh(candidate_symbol, normalized_date, reason="missing_api_key")
            return {"close": None, "status": "provider_unavailable", "error": "missing_api_key", "symbol": candidate_symbol}

        try:
            ensure_fmp_live_allowed(category="price:eod", symbol=candidate_symbol)
        except ProviderUnavailable as exc:
            reason = getattr(exc, "reason", "provider_unavailable")
            record_fallback(category="price:eod", symbol=candidate_symbol, reason=reason)
            _enqueue_eod_refresh(candidate_symbol, normalized_date, reason=reason)
            return {"close": None, "status": reason, "error": reason, "symbol": candidate_symbol}

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
        record_provider_response(category="price:eod", symbol=candidate_symbol, status_code=response.status_code)

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
        volume_value = _extract_volume_series_from_payload(payload, normalized_date, normalized_date).get(normalized_date)
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
                    volume_value = _extract_volume_series_from_payload(retry_payload, normalized_date, normalized_date).get(normalized_date)
                    if close_value is None:
                        prior_close = _extract_close_on_or_prior_from_payload(retry_payload, normalized_date)
                        if prior_close is not None:
                            candidate_close_value, candidate_close_date = prior_close
                            within_bounds, delta_days = _prior_fallback_within_bounds(
                                normalized_date, candidate_close_date, max_prior_days
                            )
                            if within_bounds:
                                close_value, resolved_close_date = candidate_close_value, candidate_close_date
                                volume_value = None
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

        if allow_cache_write:
            _safe_cache_upsert(db, candidate_symbol, normalized_date, close_value, volume_value)
        return {
            "close": close_value,
            "status": "ok",
            "error": None,
            "symbol": candidate_symbol,
            "price_date": resolved_close_date,
            "fallback_days": (datetime.strptime(normalized_date, "%Y-%m-%d").date() - datetime.strptime(resolved_close_date, "%Y-%m-%d").date()).days,
        }

    if saw_402:
        _enqueue_eod_refresh(normalized_symbol, normalized_date, reason="provider_402")
        return {"close": None, "status": "provider_402", "error": "Provider plan does not cover symbol", "symbol": normalized_symbol}
    if saw_429 or saw_cooldown:
        _enqueue_eod_refresh(normalized_symbol, normalized_date, reason="provider_429")
        return {"close": None, "status": "provider_429", "error": "Provider rate-limited request", "symbol": normalized_symbol}
    _enqueue_eod_refresh(normalized_symbol, normalized_date, reason="no_data")
    return {
        "close": None,
        "status": "no_data",
        "error": (
            f"No EOD data for symbol variants: {symbol_variants(normalized_symbol)}"
            + (f"; prior candidate exceeded max fallback window ({max_prior_days} days)" if saw_stale_prior_candidate else "")
        ),
        "symbol": normalized_symbol,
    }


def get_eod_close(db: Session, symbol: str, date: str, *, allow_cache_write: bool = False) -> Optional[float]:
    """Backward-compatible close-only lookup."""
    try:
        result = get_eod_close_with_meta(db, symbol, date, allow_cache_write=allow_cache_write)
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
    if rows:
        record_cache_hit(category="price:series", symbol=normalized_symbol)
    else:
        record_cache_miss(category="price:series", symbol=normalized_symbol)
    return dict(sorted((str(row[0]), float(row[1])) for row in rows))


def _fetch_provider_eod_payload(
    endpoint: str,
    candidate_symbol: str,
    start_date: str,
    end_date: str,
    api_key: str,
    *,
    allow_user_request: bool = False,
):
    cache_key = (endpoint, candidate_symbol, start_date, end_date)
    cached = _PROVIDER_EOD_SERIES_CACHE.get(cache_key)
    if cached and time.time() < cached[0]:
        record_cache_hit(category=f"price:{endpoint}", symbol=candidate_symbol)
        return cached[1]
    record_cache_miss(category=f"price:{endpoint}", symbol=candidate_symbol)

    try:
        ensure_fmp_live_allowed(
            category=f"price:{endpoint}",
            symbol=candidate_symbol,
            allow_user_request=allow_user_request,
        )
    except ProviderUnavailable as exc:
        record_fallback(category=f"price:{endpoint}", symbol=candidate_symbol, reason=getattr(exc, "reason", "provider_unavailable"))
        return None

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
    if response is not None:
        record_provider_response(category=f"price:{endpoint}", symbol=candidate_symbol, status_code=response.status_code)
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


def _extract_close_series_from_massive_payload(payload: Any, start_date: str, end_date: str) -> dict[str, float]:
    if not isinstance(payload, dict):
        return {}
    rows = payload.get("results")
    if not isinstance(rows, list):
        return {}

    price_map: dict[str, float] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        timestamp = row.get("t")
        close_raw = row.get("c")
        try:
            close_value = float(close_raw)
            day = datetime.fromtimestamp(float(timestamp) / 1000, tz=timezone.utc).date().isoformat()
        except (TypeError, ValueError, OSError):
            continue
        if start_date <= day <= end_date and close_value > 0:
            price_map[day] = close_value
    return dict(sorted(price_map.items()))


def _extract_volume_series_from_massive_payload(payload: Any, start_date: str, end_date: str) -> dict[str, float]:
    if not isinstance(payload, dict):
        return {}
    rows = payload.get("results")
    if not isinstance(rows, list):
        return {}

    volume_map: dict[str, float] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        timestamp = row.get("t")
        volume_raw = row.get("v")
        try:
            volume_value = float(volume_raw)
            day = datetime.fromtimestamp(float(timestamp) / 1000, tz=timezone.utc).date().isoformat()
        except (TypeError, ValueError, OSError):
            continue
        if start_date <= day <= end_date and volume_value > 0:
            volume_map[day] = volume_value
    return dict(sorted(volume_map.items()))


def _fetch_massive_eod_price_volume_series(symbol: str, start_date: str, end_date: str) -> tuple[dict[str, float], dict[str, float], str | None]:
    api_key = (os.getenv("MASSIVE_API_KEY") or os.getenv("POLYGON_API_KEY") or "").strip()
    if not api_key:
        return {}, {}, None
    base_url = (
        os.getenv("MASSIVE_BASE_URL")
        or os.getenv("POLYGON_BASE_URL")
        or "https://api.massive.com"
    ).rstrip("/")

    best_map: dict[str, float] = {}
    best_volume_map: dict[str, float] = {}
    best_symbol: str | None = None
    for candidate_symbol in symbol_variants(symbol):
        path_symbol = quote(candidate_symbol, safe="")
        response = _fetch_with_backoff(
            f"{base_url}/v2/aggs/ticker/{path_symbol}/range/1/day/{start_date}/{end_date}",
            {
                "adjusted": "true",
                "sort": "asc",
                "limit": 50000,
                "apiKey": api_key,
            },
            retries=1,
        )
        if response is None or response.status_code != 200:
            continue
        try:
            payload = response.json()
        except ValueError:
            continue
        provider_map = _extract_close_series_from_massive_payload(payload, start_date, end_date)
        volume_map = _extract_volume_series_from_massive_payload(payload, start_date, end_date)
        if len(provider_map) > len(best_map):
            best_map = provider_map
            best_volume_map = volume_map
            best_symbol = candidate_symbol
        if provider_map and not is_sparse_daily_close_series(provider_map, start_date, end_date):
            return provider_map, volume_map, candidate_symbol
    return best_map, best_volume_map, best_symbol


def _fetch_massive_eod_close_series(symbol: str, start_date: str, end_date: str) -> tuple[dict[str, float], str | None]:
    price_map, _volume_map, provider_symbol = _fetch_massive_eod_price_volume_series(symbol, start_date, end_date)
    return price_map, provider_symbol


def _fetch_provider_eod_price_volume_series(
    symbol: str,
    start_date: str,
    end_date: str,
    *,
    allow_user_request: bool = False,
) -> tuple[dict[str, float], dict[str, float], str | None]:
    api_key = os.getenv("FMP_API_KEY", "").strip()
    if not api_key:
        return {}, {}, None

    best_map: dict[str, float] = {}
    best_volume_map: dict[str, float] = {}
    best_symbol: str | None = None
    saw_402 = False
    saw_429 = False

    for candidate_symbol in symbol_variants(symbol):
        for endpoint in ("historical-price-eod/full", "historical-price-eod/light"):
            provider_payload = _fetch_provider_eod_payload(
                endpoint,
                candidate_symbol,
                start_date,
                end_date,
                api_key,
                allow_user_request=allow_user_request,
            )
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
            volume_map = _extract_volume_series_from_payload(provider_payload, start_date, end_date)
            if len(provider_map) > len(best_map) or (
                len(provider_map) == len(best_map) and len(volume_map) > len(best_volume_map)
            ):
                best_map = provider_map
                best_volume_map = volume_map
                best_symbol = candidate_symbol
            if provider_map and not is_sparse_daily_close_series(provider_map, start_date, end_date):
                return provider_map, volume_map, candidate_symbol

    if saw_402:
        logger.info("price_lookup provider plan did not cover dense history symbol=%s", symbol)
    if saw_429:
        logger.info("price_lookup provider rate-limited dense history symbol=%s", symbol)
    massive_map, massive_volume_map, massive_symbol = _fetch_massive_eod_price_volume_series(symbol, start_date, end_date)
    if len(massive_map) > len(best_map):
        return massive_map, massive_volume_map, massive_symbol
    return best_map, best_volume_map, best_symbol


def _fetch_provider_eod_close_series(symbol: str, start_date: str, end_date: str) -> tuple[dict[str, float], str | None]:
    price_map, _volume_map, provider_symbol = _fetch_provider_eod_price_volume_series(symbol, start_date, end_date)
    return price_map, provider_symbol


def get_daily_close_series_with_fallback(
    db: Session,
    symbol: str,
    start_date: str,
    end_date: str,
    *,
    release_connection_before_provider: bool = False,
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
    cached_tail_stale = _series_has_stale_tail(cached_map, end_key)
    if cached_map and not cached_tail_stale and not is_sparse_daily_close_series(cached_map, start_key, end_key):
        return cached_map

    if _is_foreground_request_context():
        _enqueue_eod_refresh(
            normalized_symbol,
            reason="stale_or_missing_series",
            window_key=f"{start_key}:{end_key}",
        )
        return cached_map

    if release_connection_before_provider:
        db.close()

    provider_map, provider_volume_map, provider_symbol = _fetch_provider_eod_price_volume_series(normalized_symbol, start_key, end_key)
    if provider_map and _series_has_stale_tail(provider_map, end_key):
        tail_start = (date.fromisoformat(max(provider_map)) + timedelta(days=1)).isoformat()
        tail_window_start = max(tail_start, (date.fromisoformat(end_key) - timedelta(days=45)).isoformat())
        tail_map, tail_volume_map, tail_symbol = _fetch_provider_eod_price_volume_series(normalized_symbol, tail_window_start, end_key)
        if tail_map:
            provider_map = {**provider_map, **tail_map}
            provider_volume_map = {**provider_volume_map, **tail_volume_map}
            provider_symbol = tail_symbol or provider_symbol
    if provider_map:
        cache_symbol = provider_symbol or normalized_symbol
        wrote_any = False
        for day, close_value in provider_map.items():
            wrote_any = _safe_cache_upsert(db, cache_symbol, day, close_value, provider_volume_map.get(day)) or wrote_any
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
        if len(provider_map) >= len(cached_map) or cached_tail_stale:
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
