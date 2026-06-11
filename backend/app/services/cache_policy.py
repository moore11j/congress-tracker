from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


@dataclass(frozen=True)
class CachePolicy:
    name: str
    ttl_seconds: int
    stale_seconds: int
    hot_ttl_seconds: int | None = None


CACHE_POLICIES: dict[str, CachePolicy] = {
    "quotes": CachePolicy("quotes", ttl_seconds=60, stale_seconds=15 * 60, hot_ttl_seconds=5),
    "chart_30d": CachePolicy("chart_30d", ttl_seconds=15 * 60, stale_seconds=24 * 60 * 60, hot_ttl_seconds=60),
    "chart_90d": CachePolicy("chart_90d", ttl_seconds=30 * 60, stale_seconds=24 * 60 * 60, hot_ttl_seconds=60),
    "chart_365d": CachePolicy("chart_365d", ttl_seconds=6 * 60 * 60, stale_seconds=24 * 60 * 60, hot_ttl_seconds=5 * 60),
    "financials": CachePolicy("financials", ttl_seconds=24 * 60 * 60, stale_seconds=7 * 24 * 60 * 60, hot_ttl_seconds=15 * 60),
    "fundamentals": CachePolicy("fundamentals", ttl_seconds=24 * 60 * 60, stale_seconds=7 * 24 * 60 * 60, hot_ttl_seconds=15 * 60),
    "technicals": CachePolicy("technicals", ttl_seconds=60 * 60, stale_seconds=24 * 60 * 60, hot_ttl_seconds=5 * 60),
    "insights": CachePolicy("insights", ttl_seconds=5 * 60, stale_seconds=24 * 60 * 60, hot_ttl_seconds=60),
    "news": CachePolicy("news", ttl_seconds=15 * 60, stale_seconds=24 * 60 * 60, hot_ttl_seconds=60),
    "provider_usage": CachePolicy("provider_usage", ttl_seconds=60, stale_seconds=24 * 60 * 60, hot_ttl_seconds=60),
}


def policy_for(category: str) -> CachePolicy:
    return CACHE_POLICIES.get(category, CachePolicy(category, ttl_seconds=5 * 60, stale_seconds=60 * 60, hot_ttl_seconds=60))


def _as_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        raw = str(value).strip().replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def cache_age_seconds(cache_record: Any) -> float | None:
    as_of = None
    if isinstance(cache_record, dict):
        as_of = cache_record.get("as_of") or cache_record.get("updated_at") or cache_record.get("fetched_at")
    else:
        as_of = getattr(cache_record, "as_of", None) or getattr(cache_record, "updated_at", None) or getattr(cache_record, "fetched_at", None)
    parsed = _as_datetime(as_of)
    if parsed is None:
        return None
    return max((datetime.now(timezone.utc) - parsed).total_seconds(), 0)


def is_fresh(cache_record: Any, policy: CachePolicy) -> bool:
    age = cache_age_seconds(cache_record)
    return age is not None and age <= policy.ttl_seconds


def is_stale_but_usable(cache_record: Any, policy: CachePolicy) -> bool:
    age = cache_age_seconds(cache_record)
    return age is not None and policy.ttl_seconds < age <= policy.stale_seconds


def should_refresh(cache_record: Any, policy: CachePolicy) -> bool:
    age = cache_age_seconds(cache_record)
    return age is None or age > policy.ttl_seconds


def stale_age_label(cache_record: Any) -> str | None:
    age = cache_age_seconds(cache_record)
    if age is None:
        return None
    if age < 60:
        return "just now"
    if age < 3600:
        minutes = int(age // 60)
        return f"{minutes} minute{'s' if minutes != 1 else ''} ago"
    if age < 86400:
        hours = int(age // 3600)
        return f"{hours} hour{'s' if hours != 1 else ''} ago"
    days = int(age // 86400)
    return f"{days} day{'s' if days != 1 else ''} ago"
