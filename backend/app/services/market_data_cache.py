from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable

from app.services.cache_policy import CachePolicy, cache_age_seconds, is_fresh, is_stale_but_usable, stale_age_label
from app.services.cache_store import hot_cache_get, hot_cache_set
from app.services.provider_usage import (
    ProviderUnavailable,
    ensure_fmp_live_allowed,
    fallback_payload,
    reason_from_exception,
    record_cache_hit,
    record_cache_miss,
    record_fallback,
)


def _data_from_record(record: Any) -> Any:
    if record is None:
        return None
    if isinstance(record, dict) and "data" in record:
        return record.get("data")
    return record


def _as_of(record: Any) -> str | None:
    if record is None:
        return None
    if isinstance(record, dict):
        value = record.get("as_of") or record.get("updated_at") or record.get("fetched_at")
    else:
        value = getattr(record, "as_of", None) or getattr(record, "updated_at", None) or getattr(record, "fetched_at", None)
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat() if value.tzinfo else value.replace(tzinfo=timezone.utc).isoformat()
    return str(value) if value else None


def _result(*, data: Any, cache_status: str, as_of: str | None, stale: bool, unavailable: bool, reason: str | None, age_seconds: float | None = None) -> dict[str, Any]:
    payload = {
        "data": data,
        "cache_status": cache_status,
        "as_of": as_of,
        "stale": stale,
        "unavailable": unavailable,
        "reason": reason,
    }
    if age_seconds is not None:
        payload["cache_age_seconds"] = round(age_seconds, 1)
        payload["age_label"] = stale_age_label({"as_of": as_of})
    return payload


def get_or_refresh_market_data(
    db: Any,
    *,
    key: str,
    category: str,
    cache_loader: Callable[[Any], Any],
    provider_fetcher: Callable[[], Any],
    cache_writer: Callable[[Any, Any], None],
    policy: CachePolicy,
    source: str,
    allow_live_fetch: bool = False,
    allow_stale: bool = True,
) -> dict[str, Any]:
    hot = hot_cache_get(key)
    if hot is not None:
        record_cache_hit(category=category)
        return _result(data=hot, cache_status="hit", as_of=None, stale=False, unavailable=False, reason=None)

    record = None
    try:
        record = cache_loader(db)
    except Exception:
        record = None

    age = cache_age_seconds(record)
    if record is not None and is_fresh(record, policy):
        record_cache_hit(category=category, cache_age_seconds=age)
        data = _data_from_record(record)
        hot_cache_set(key, data, policy.hot_ttl_seconds)
        return _result(data=data, cache_status="hit", as_of=_as_of(record), stale=False, unavailable=False, reason=None, age_seconds=age)

    if record is not None and allow_stale and is_stale_but_usable(record, policy):
        record_cache_hit(category=category, cache_age_seconds=age)
        data = _data_from_record(record)
        hot_cache_set(key, data, policy.hot_ttl_seconds)
        return _result(data=data, cache_status="stale", as_of=_as_of(record), stale=True, unavailable=False, reason=None, age_seconds=age)

    record_cache_miss(category=category)
    if not allow_live_fetch:
        reason = "cache_miss" if record is None else "provider_disabled"
        record_fallback(category=category, reason=reason, cache_age_seconds=age)
        fallback = fallback_payload(reason=reason, stale=record is not None, cache_age_seconds=age)
        return {**_result(data=None, cache_status="miss", as_of=_as_of(record), stale=record is not None, unavailable=True, reason=reason, age_seconds=age), **fallback}

    try:
        ensure_fmp_live_allowed(category=category)
        data = provider_fetcher()
        cache_writer(db, data)
        hot_cache_set(key, data, policy.hot_ttl_seconds)
        return _result(data=data, cache_status="refreshed", as_of=datetime.now(timezone.utc).isoformat(), stale=False, unavailable=False, reason=None)
    except ProviderUnavailable as exc:
        reason = reason_from_exception(exc)
    except Exception:
        reason = "provider_error"

    if record is not None and allow_stale:
        record_fallback(category=category, reason=reason, cache_age_seconds=age)
        return _result(data=_data_from_record(record), cache_status="stale", as_of=_as_of(record), stale=True, unavailable=False, reason=reason, age_seconds=age)

    record_fallback(category=category, reason=reason, cache_age_seconds=age)
    return {**_result(data=None, cache_status="unavailable", as_of=None, stale=False, unavailable=True, reason=reason), **fallback_payload(reason=reason)}
