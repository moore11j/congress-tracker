from __future__ import annotations

import logging
import os
import threading
import time
from collections import Counter, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from app.request_priority import get_request_context

logger = logging.getLogger(__name__)

PROVIDER = "fmp"
DEFAULT_CALLS_PER_MINUTE = 750
_EVENT_LIMIT = 500
_WINDOW_SECONDS = 60.0
_TRUE_VALUES = {"1", "true", "yes", "on"}


class ProviderUnavailable(RuntimeError):
    reason = "provider_unavailable"


class ProviderDisabled(ProviderUnavailable):
    reason = "provider_disabled"


class ProviderBudgetExceeded(ProviderUnavailable):
    reason = "provider_budget_exceeded"


@dataclass
class _UsageState:
    started_at: float = field(default_factory=time.time)
    events: deque[dict[str, Any]] = field(default_factory=lambda: deque(maxlen=_EVENT_LIMIT))
    provider_call_timestamps: deque[float] = field(default_factory=deque)
    counters: Counter[str] = field(default_factory=Counter)
    route_counters: Counter[str] = field(default_factory=Counter)
    category_counters: Counter[str] = field(default_factory=Counter)
    reason_counters: Counter[str] = field(default_factory=Counter)


_STATE = _UsageState()
_LOCK = threading.Lock()
_PERSIST_FAILURE_LOGGED = False
_BUDGET_FALLBACK_LOG_COUNTERS: Counter[str] = Counter()
_BUDGET_FALLBACK_LOG_SUPPRESSED: Counter[str] = Counter()
_BUDGET_FALLBACK_LOG_SAMPLES: dict[str, list[str]] = {}


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in _TRUE_VALUES


def _int_env(*names: str, default: int) -> int:
    for name in names:
        raw = os.getenv(name)
        if raw is None or not str(raw).strip():
            continue
        try:
            return max(int(str(raw).strip()), 1)
        except ValueError:
            logger.info("provider_usage invalid integer env name=%s value=%s", name, raw)
    return max(int(default), 1)


def _budget_fallback_log_limit() -> int:
    return _int_env("FMP_PROVIDER_BUDGET_LOG_LIMIT_PER_CATEGORY", default=5)


def _plan_calls_per_minute() -> int:
    return _int_env("FMP_PLAN_CALLS_PER_MINUTE", "FMP_CALLS_PER_MINUTE", default=DEFAULT_CALLS_PER_MINUTE)


def _legacy_soft_limit_configured() -> bool:
    return bool(os.getenv("FMP_CALLS_PER_MINUTE_SOFT_LIMIT"))


def _soft_limit_per_minute() -> int:
    plan = _plan_calls_per_minute()
    return _int_env(
        "FMP_SOFT_LIMIT_PER_MINUTE",
        "FMP_CALLS_PER_MINUTE_SOFT_LIMIT",
        default=max(1, min(plan, int(plan * 0.8))),
    )


def _hard_limit_per_minute() -> int:
    plan = _plan_calls_per_minute()
    default = _soft_limit_per_minute() if _legacy_soft_limit_configured() else plan
    return _int_env("FMP_HARD_LIMIT_PER_MINUTE", "FMP_CALLS_PER_MINUTE_HARD_LIMIT", default=default)


def _calls_per_minute() -> int:
    return _hard_limit_per_minute()


def _budget_limits() -> dict[str, int]:
    try:
        plan = _plan_calls_per_minute()
        soft = _soft_limit_per_minute()
        hard = _hard_limit_per_minute()
        return {
            "plan_calls_per_minute": plan,
            "soft_limit_per_minute": soft,
            "hard_limit_per_minute": hard,
            "throttle_limit_per_minute": hard,
        }
    except Exception:
        return {
            "plan_calls_per_minute": DEFAULT_CALLS_PER_MINUTE,
            "soft_limit_per_minute": DEFAULT_CALLS_PER_MINUTE,
            "hard_limit_per_minute": DEFAULT_CALLS_PER_MINUTE,
            "throttle_limit_per_minute": DEFAULT_CALLS_PER_MINUTE,
        }


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _context_route() -> str:
    context = get_request_context() or {}
    return str(context.get("path") or context.get("walnut_route") or "background")


def _is_user_request() -> bool:
    route = _context_route()
    if route == "background":
        return False
    if route.startswith("/api/admin/"):
        return False
    return route.startswith("/api/")


def _prune_calls(now: float) -> None:
    cutoff = now - _WINDOW_SECONDS
    while _STATE.provider_call_timestamps and _STATE.provider_call_timestamps[0] < cutoff:
        _STATE.provider_call_timestamps.popleft()


def _record(
    kind: str,
    *,
    category: str,
    symbol: str | None = None,
    reason: str | None = None,
    cache_age_seconds: float | None = None,
    status_code: int | str | None = None,
    item_count: int | None = None,
    budget_tier: str | None = None,
) -> None:
    route = _context_route()
    source = _source_context(route)
    event = {
        "ts": _now_iso(),
        "provider": PROVIDER,
        "kind": kind,
        "route": route,
        "source": source,
        "category": category,
        "symbol": symbol,
        "reason": reason,
        "cache_age_seconds": round(cache_age_seconds, 1) if cache_age_seconds is not None else None,
        "status_code": status_code,
        "item_count": item_count,
        "budget_tier": budget_tier,
    }
    with _LOCK:
        _STATE.events.appendleft(event)
        _STATE.counters[kind] += 1
        _STATE.route_counters[f"{route}|{kind}"] += 1
        _STATE.category_counters[f"{category}|{kind}"] += 1
        if reason:
            _STATE.reason_counters[reason] += 1
    _persist_event(event)


def _source_context(route: str) -> str:
    if route == "background":
        return "scheduled_job"
    if route.startswith("/api/admin/"):
        return "admin_refresh"
    if _env_bool("FMP_EXPLICIT_USER_REFRESH", False):
        return "explicit_user_refresh"
    return "page_load"


def _persist_event(event: dict[str, Any]) -> None:
    if not _env_bool("FMP_PERSIST_USAGE_EVENTS", True):
        return
    global _PERSIST_FAILURE_LOGGED
    try:
        from app.db import SessionLocal
        from app.models import ProviderUsageEvent

        db = SessionLocal()
        try:
            kind = str(event.get("kind") or "")
            row = ProviderUsageEvent(
                provider=PROVIDER,
                category=str(event.get("category") or "") or None,
                endpoint=str(event.get("category") or "").split(":", 1)[-1] or None,
                symbol=event.get("symbol"),
                source=event.get("source"),
                route=event.get("route"),
                cache_status=kind if kind.startswith("cache_") else None,
                status_code=str(event.get("status_code") if event.get("status_code") is not None else event.get("item_count") if event.get("item_count") is not None else "") or None,
                duration_ms=None,
                success=kind in {"cache_hit", "provider_call", "content_write"},
                throttled=kind == "throttle",
                error=str(event.get("reason") or "") or None,
            )
            db.add(row)
            db.commit()
        finally:
            db.close()
    except Exception:
        if not _PERSIST_FAILURE_LOGGED:
            logger.info("provider_usage persistence unavailable; continuing with in-process counters", exc_info=True)
            _PERSIST_FAILURE_LOGGED = True


def reset_provider_usage() -> None:
    with _LOCK:
        _STATE.events.clear()
        _STATE.provider_call_timestamps.clear()
        _STATE.counters.clear()
        _STATE.route_counters.clear()
        _STATE.category_counters.clear()
        _STATE.reason_counters.clear()
        _STATE.started_at = time.time()
        _BUDGET_FALLBACK_LOG_COUNTERS.clear()
        _BUDGET_FALLBACK_LOG_SUPPRESSED.clear()
        _BUDGET_FALLBACK_LOG_SAMPLES.clear()


def live_fmp_user_routes_enabled() -> bool:
    if os.getenv("FMP_ALLOW_SYNC_USER_FETCH") is not None:
        return _env_bool("FMP_ALLOW_SYNC_USER_FETCH", False)
    if _env_bool("FMP_LIVE_USER_ROUTES_ENABLED", False):
        return True
    if os.getenv("FMP_CACHE_ONLY_USER_ROUTES") is not None:
        return not _env_bool("FMP_CACHE_ONLY_USER_ROUTES", True)
    return False


def ensure_fmp_live_allowed(*, category: str, symbol: str | None = None) -> None:
    if _env_bool("FMP_PROVIDER_DISABLED", False):
        reason = "background_provider_disabled" if not _is_user_request() else "provider_disabled"
        record_fallback(category=category, symbol=symbol, reason=reason)
        raise ProviderDisabled(reason)

    if _is_user_request() and not live_fmp_user_routes_enabled():
        reason = "page_fetch_blocked"
        record_fallback(category=category, symbol=symbol, reason=reason)
        raise ProviderDisabled(reason)

    now = time.time()
    with _LOCK:
        _prune_calls(now)
        limits = _budget_limits()
        call_limit = limits["throttle_limit_per_minute"]
        if len(_STATE.provider_call_timestamps) >= call_limit:
            budget_tier = "hard" if len(_STATE.provider_call_timestamps) >= limits["hard_limit_per_minute"] else "soft"
            _STATE.counters["throttle"] += 1
            _STATE.reason_counters["provider_budget_exceeded"] += 1
            _STATE.route_counters[f"{_context_route()}|throttle"] += 1
            _STATE.category_counters[f"{category}|throttle"] += 1
            _STATE.events.appendleft(
                {
                    "ts": _now_iso(),
                    "provider": PROVIDER,
                    "kind": "throttle",
                    "route": _context_route(),
                    "source": _source_context(_context_route()),
                    "category": category,
                    "symbol": symbol,
                    "reason": "provider_budget_exceeded",
                    "cache_age_seconds": None,
                    "status_code": None,
                    "item_count": None,
                    "budget_tier": budget_tier,
                    "budget_used": len(_STATE.provider_call_timestamps),
                    "budget_limit": call_limit,
                }
            )
            _persist_event(_STATE.events[0])
            raise ProviderBudgetExceeded("provider_budget_exceeded")
        _STATE.provider_call_timestamps.append(now)
        _STATE.counters["provider_call"] += 1
        _STATE.route_counters[f"{_context_route()}|provider_call"] += 1
        _STATE.category_counters[f"{category}|provider_call"] += 1
        _STATE.events.appendleft(
            {
                "ts": _now_iso(),
                "provider": PROVIDER,
                "kind": "provider_call",
                "route": _context_route(),
                "source": _source_context(_context_route()),
                "category": category,
                "symbol": symbol,
                "reason": None,
                "cache_age_seconds": None,
                "status_code": None,
            }
        )
        _persist_event(_STATE.events[0])


def record_cache_hit(*, category: str, symbol: str | None = None, cache_age_seconds: float | None = None) -> None:
    _record("cache_hit", category=category, symbol=symbol, cache_age_seconds=cache_age_seconds)


def record_cache_miss(*, category: str, symbol: str | None = None) -> None:
    _record("cache_miss", category=category, symbol=symbol)


def record_provider_response(*, category: str, symbol: str | None = None, status_code: int | str | None = None) -> None:
    if status_code == 429:
        _record("throttle", category=category, symbol=symbol, reason="provider_rate_limited", status_code=status_code)
    elif isinstance(status_code, int) and status_code >= 400:
        _record("provider_error", category=category, symbol=symbol, reason=_reason_for_status(status_code), status_code=status_code)


def _budget_log_key(*, route: str, category: str, reason: str) -> str:
    return f"{route}|{category}|{reason}"


def _record_budget_fallback_log(*, route: str, category: str, symbol: str | None, reason: str) -> bool:
    key = _budget_log_key(route=route, category=category, reason=reason)
    with _LOCK:
        _BUDGET_FALLBACK_LOG_COUNTERS[key] += 1
        samples = _BUDGET_FALLBACK_LOG_SAMPLES.setdefault(key, [])
        if symbol and symbol not in samples and len(samples) < 5:
            samples.append(symbol)
        count = _BUDGET_FALLBACK_LOG_COUNTERS[key]
        should_log = count <= _budget_fallback_log_limit()
        if not should_log:
            _BUDGET_FALLBACK_LOG_SUPPRESSED[key] += 1
        return should_log


def provider_budget_log_summary(*, reset: bool = False) -> list[dict[str, Any]]:
    with _LOCK:
        rows: list[dict[str, Any]] = []
        for key, count in _BUDGET_FALLBACK_LOG_COUNTERS.items():
            route, category, reason = key.split("|", 2)
            rows.append(
                {
                    "route": route,
                    "category": category,
                    "reason": reason,
                    "count": int(count),
                    "suppressed": int(_BUDGET_FALLBACK_LOG_SUPPRESSED.get(key, 0)),
                    "sample_symbols": list(_BUDGET_FALLBACK_LOG_SAMPLES.get(key, [])),
                }
            )
        rows.sort(key=lambda row: (row["count"], row["category"]), reverse=True)
        if reset:
            _BUDGET_FALLBACK_LOG_COUNTERS.clear()
            _BUDGET_FALLBACK_LOG_SUPPRESSED.clear()
            _BUDGET_FALLBACK_LOG_SAMPLES.clear()
        return rows


def log_provider_budget_summary(*, reset: bool = False) -> list[dict[str, Any]]:
    rows = provider_budget_log_summary(reset=reset)
    for row in rows:
        logger.info(
            "provider_budget_summary category=%s route=%s reason=%s count=%s suppressed=%s sample_symbols=%s",
            row["category"],
            row["route"],
            row["reason"],
            row["count"],
            row["suppressed"],
            row["sample_symbols"],
        )
    return rows


def record_fallback(*, category: str, symbol: str | None = None, reason: str, cache_age_seconds: float | None = None) -> None:
    route = _context_route()
    should_log = True
    if reason == "provider_budget_exceeded":
        should_log = _record_budget_fallback_log(route=route, category=category, symbol=symbol, reason=reason)
    if should_log:
        logger.info(
            "provider_fallback provider=%s route=%s category=%s symbol=%s reason=%s cache_age_seconds=%s",
            PROVIDER,
            route,
            category,
            symbol,
            reason,
            round(cache_age_seconds, 1) if cache_age_seconds is not None else None,
        )
    _record("fallback", category=category, symbol=symbol, reason=reason, cache_age_seconds=cache_age_seconds)


def record_content_write(*, category: str, symbol: str | None = None, item_count: int = 0) -> None:
    _record("content_write", category=category, symbol=symbol, item_count=max(0, int(item_count or 0)))


def fallback_payload(*, reason: str, message: str | None = None, stale: bool = True, cache_age_seconds: float | None = None) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "data": None,
        "stale": stale,
        "unavailable": True,
        "reason": reason,
    }
    if message:
        payload["message"] = message
    if cache_age_seconds is not None:
        payload["cache_age_seconds"] = round(cache_age_seconds, 1)
    return payload


def reason_from_exception(exc: BaseException) -> str:
    explicit = getattr(exc, "reason", None)
    if explicit:
        return explicit
    text = str(exc).strip()
    if text in {
        "provider_budget_exceeded",
        "cache_miss",
        "provider_disabled",
        "provider_error",
        "provider_rate_limited",
        "provider_unavailable",
        "page_fetch_blocked",
        "background_provider_disabled",
        "provider_timeout",
        "provider_entitlement",
    }:
        return text
    return "provider_unavailable"


def _reason_for_status(status_code: int) -> str:
    if status_code == 429:
        return "provider_rate_limited"
    if status_code in {401, 402, 403}:
        return "provider_entitlement"
    if status_code >= 500:
        return "provider_unavailable"
    return "provider_error"


def reason_for_status(status_code: int | str | None) -> str:
    if isinstance(status_code, int):
        return _reason_for_status(status_code)
    return "provider_unavailable"


def _top(counter: Counter[str], *, limit: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for key, count in counter.most_common(limit):
        name, kind = key.rsplit("|", 1)
        rows.append({"name": name, "kind": kind, "count": int(count)})
    return rows


def provider_usage_summary(*, limit: int = 20, db: Any | None = None) -> dict[str, Any]:
    now = time.time()
    with _LOCK:
        _prune_calls(now)
        calls_last_minute = len(_STATE.provider_call_timestamps)
        counters = dict(_STATE.counters)
        cache_hits = counters.get("cache_hit", 0)
        cache_misses = counters.get("cache_miss", 0)
        cache_total = cache_hits + cache_misses
        limits = _budget_limits()
        call_cap = limits["throttle_limit_per_minute"]
        warnings: list[str] = []
        if calls_last_minute >= call_cap * 0.8:
            warnings.append("FMP calls are above 80% of the configured per-minute budget.")
        if counters.get("throttle", 0):
            warnings.append("Provider throttles or internal budget throttles were observed.")
        if cache_total and (cache_hits / cache_total) < 0.85:
            warnings.append("Cache hit rate is below 85%; add refresh coverage before opening more traffic.")
        if counters.get("fallback", 0):
            warnings.append("Some user sections are serving controlled fallback payloads.")

        summary = {
            "provider": PROVIDER,
            "generated_at": _now_iso(),
            "started_at": datetime.fromtimestamp(_STATE.started_at, tz=timezone.utc).isoformat(),
            "configured_calls_per_minute": limits["plan_calls_per_minute"],
            "calls_last_minute": calls_last_minute,
            "budget": {
                **limits,
                "used_last_minute": calls_last_minute,
                "remaining_last_minute": max(call_cap - calls_last_minute, 0),
                "usage_pct": round((calls_last_minute / call_cap) * 100, 1) if call_cap else None,
                "soft_exceeded": calls_last_minute >= limits["soft_limit_per_minute"],
                "hard_exceeded": calls_last_minute >= limits["hard_limit_per_minute"],
            },
            "cache_hit_rate": round((cache_hits / cache_total) * 100, 1) if cache_total else None,
            "totals": {
                "provider_calls": int(counters.get("provider_call", 0)),
                "cache_hits": int(cache_hits),
                "cache_misses": int(cache_misses),
                "fallbacks": int(counters.get("fallback", 0)),
                "throttles": int(counters.get("throttle", 0)),
                "provider_errors": int(counters.get("provider_error", 0)),
            },
            "top_routes": _top(_STATE.route_counters, limit=limit),
            "top_categories": _top(_STATE.category_counters, limit=limit),
            "reasons": [{"reason": key, "count": int(value)} for key, value in _STATE.reason_counters.most_common(limit)],
            "fallback_reasons": [{"reason": key, "count": int(value)} for key, value in _STATE.reason_counters.most_common(limit)],
            "content_writes": _content_write_summary(list(_STATE.events), limit=limit),
            "recent_events": list(_STATE.events)[:limit],
            "warnings": warnings,
            "guardrails": {
                "cache_only_user_routes": not live_fmp_user_routes_enabled(),
                "provider_disabled": _env_bool("FMP_PROVIDER_DISABLED", False),
                "allow_sync_user_fetch": live_fmp_user_routes_enabled(),
            },
        }
    if db is not None:
        try:
            from sqlalchemy import func, select
            from app.models import DataEnrichmentJob, FundamentalsCache, PriceCache, ProviderUsageEvent
            from app.services.data_enrichment_queue import enrichment_queue_summary
            from app.services.fred_macro_cache import fred_macro_cache_diagnostics

            day_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
            now_dt = datetime.now(timezone.utc)
            call_filter = (
                ProviderUsageEvent.provider == PROVIDER,
                ProviderUsageEvent.cache_status.is_(None),
                ProviderUsageEvent.throttled.is_(False),
            )
            calls_today = db.execute(
                select(func.count(ProviderUsageEvent.id)).where(*call_filter, ProviderUsageEvent.created_at >= day_start)
            ).scalar_one()
            call_windows = {
                "last_1_min": calls_last_minute,
                "last_5_min": int(
                    db.execute(
                        select(func.count(ProviderUsageEvent.id)).where(
                            *call_filter,
                            ProviderUsageEvent.created_at >= now_dt - timedelta(minutes=5),
                        )
                    ).scalar_one()
                    or 0
                ),
                "last_1_hour": int(
                    db.execute(
                        select(func.count(ProviderUsageEvent.id)).where(
                            *call_filter,
                            ProviderUsageEvent.created_at >= now_dt - timedelta(hours=1),
                        )
                    ).scalar_one()
                    or 0
                ),
                "last_24_hours": int(
                    db.execute(
                        select(func.count(ProviderUsageEvent.id)).where(
                            *call_filter,
                            ProviderUsageEvent.created_at >= now_dt - timedelta(hours=24),
                        )
                    ).scalar_one()
                    or 0
                ),
            }
            recent_throttles = db.execute(
                select(ProviderUsageEvent)
                .where(ProviderUsageEvent.provider == PROVIDER, ProviderUsageEvent.throttled.is_(True))
                .order_by(ProviderUsageEvent.created_at.desc(), ProviderUsageEvent.id.desc())
                .limit(10)
            ).scalars().all()
            recent_errors = db.execute(
                select(ProviderUsageEvent)
                .where(ProviderUsageEvent.provider == PROVIDER, ProviderUsageEvent.error.is_not(None))
                .order_by(ProviderUsageEvent.created_at.desc(), ProviderUsageEvent.id.desc())
                .limit(10)
            ).scalars().all()
            summary["calls_today"] = int(calls_today or 0)
            summary["call_windows"] = call_windows
            summary["recent_throttles"] = [_event_row(row) for row in recent_throttles]
            summary["recent_errors"] = [_event_row(row) for row in recent_errors]
            enrichment_queue = enrichment_queue_summary(db, limit=limit)
            content_oldest_pending = db.execute(
                select(DataEnrichmentJob)
                .where(DataEnrichmentJob.job_type.in_(["news_stock", "press_releases", "sec_filings"]))
                .where(DataEnrichmentJob.status == "queued")
                .order_by(DataEnrichmentJob.created_at.asc(), DataEnrichmentJob.id.asc())
                .limit(1)
            ).scalar_one_or_none()
            if content_oldest_pending is not None:
                enrichment_queue["oldest_pending_content_job"] = {
                    "id": content_oldest_pending.id,
                    "job_type": content_oldest_pending.job_type,
                    "symbol": content_oldest_pending.symbol,
                    "status": content_oldest_pending.status,
                    "source": content_oldest_pending.source,
                    "reason": content_oldest_pending.reason,
                    "created_at": content_oldest_pending.created_at.isoformat() if content_oldest_pending.created_at else None,
                    "updated_at": content_oldest_pending.updated_at.isoformat() if content_oldest_pending.updated_at else None,
                }
            else:
                enrichment_queue["oldest_pending_content_job"] = None
            summary["enrichment_queue"] = enrichment_queue
            summary["content_diagnostics"] = _content_diagnostics(summary, enrichment_queue=enrichment_queue)
            summary["cache_coverage"] = {
                "fundamentals_rows": int(db.execute(select(func.count(FundamentalsCache.id))).scalar_one() or 0),
                "fundamentals_ok_rows": int(
                    db.execute(
                        select(func.count(FundamentalsCache.id)).where(FundamentalsCache.status == "ok")
                    ).scalar_one()
                    or 0
                ),
                "fundamentals_avg_volume_rows": int(
                    db.execute(
                        select(func.count(FundamentalsCache.id)).where(FundamentalsCache.avg_volume.is_not(None))
                    ).scalar_one()
                    or 0
                ),
                "technical_price_history_symbols": int(
                    db.execute(select(func.count(func.distinct(PriceCache.symbol)))).scalar_one() or 0
                ),
            }
            summary["fred_macro_cache"] = fred_macro_cache_diagnostics(db)
        except Exception:
            logger.info("provider_usage db summary unavailable", exc_info=True)
            summary["calls_today"] = summary["totals"]["provider_calls"]
            summary["recent_throttles"] = []
            summary["recent_errors"] = []
            summary["call_windows"] = {"last_1_min": calls_last_minute}
            summary["enrichment_queue"] = {"by_type_status": [], "failed_by_reason": [], "recent": [], "oldest_pending_content_job": None}
            summary["content_diagnostics"] = _content_diagnostics(summary, enrichment_queue=summary["enrichment_queue"])
            summary["cache_coverage"] = {}
            summary["fred_macro_cache"] = {"source": "fred", "status": "unavailable", "last_refresh_at": None, "missing_series": [], "stale_series": [], "series": []}
    else:
        summary["calls_today"] = summary["totals"]["provider_calls"]
        summary["call_windows"] = {"last_1_min": calls_last_minute}
        summary["recent_throttles"] = [event for event in summary["recent_events"] if event.get("kind") == "throttle"]
        summary["recent_errors"] = [event for event in summary["recent_events"] if event.get("reason")]
        summary["content_diagnostics"] = _content_diagnostics(summary, enrichment_queue=None)
        summary["fred_macro_cache"] = {"source": "fred", "status": "unavailable", "last_refresh_at": None, "missing_series": [], "stale_series": [], "series": []}
    status = "ok"
    warn_limit = _int_env("FMP_CALLS_PER_MINUTE_WARN_LIMIT", default=max(1, int(summary["budget"]["soft_limit_per_minute"] * 0.8)))
    soft_limit = summary["budget"]["soft_limit_per_minute"]
    if summary["calls_last_minute"] >= soft_limit or summary["totals"]["throttles"]:
        status = "critical"
    elif summary["calls_last_minute"] >= warn_limit or summary["warnings"]:
        status = "warning"
    summary["status"] = status
    summary["enabled"] = not _env_bool("FMP_PROVIDER_DISABLED", False)
    summary["live_page_fetch_enabled"] = live_fmp_user_routes_enabled()
    summary["cache_mode"] = os.getenv("FMP_CACHE_MODE") or os.getenv("CACHE_STORE_MODE", "memory")
    summary["recommendation"] = _recommendation(summary)
    return summary


def _event_row(row: Any) -> dict[str, Any]:
    item_count = None
    if row.cache_status == "content_write":
        try:
            item_count = int(row.status_code) if row.status_code is not None else None
        except (TypeError, ValueError):
            item_count = None
    return {
        "id": row.id,
        "provider": row.provider,
        "category": row.category,
        "endpoint": row.endpoint,
        "symbol": row.symbol,
        "source": row.source,
        "route": row.route,
        "cache_status": row.cache_status,
        "status_code": row.status_code,
        "duration_ms": row.duration_ms,
        "success": bool(row.success),
        "throttled": bool(row.throttled),
        "error": row.error,
        "item_count": item_count,
        "created_at": row.created_at.isoformat() if row.created_at else None,
    }


def _content_write_summary(events: list[dict[str, Any]], *, limit: int) -> list[dict[str, Any]]:
    rows: dict[tuple[str, str | None], dict[str, Any]] = {}
    for event in events:
        if event.get("kind") != "content_write":
            continue
        key = (str(event.get("category") or "unknown"), event.get("symbol"))
        current = rows.setdefault(
            key,
            {"category": key[0], "symbol": key[1], "writes": 0, "items_written": 0, "latest_at": event.get("ts")},
        )
        current["writes"] += 1
        current["items_written"] += int(event.get("item_count") or 0)
        current["latest_at"] = max(str(current.get("latest_at") or ""), str(event.get("ts") or "")) or None
    return sorted(rows.values(), key=lambda row: (row["writes"], row["items_written"]), reverse=True)[:limit]


def _content_diagnostics(summary: dict[str, Any], *, enrichment_queue: dict[str, Any] | None) -> list[dict[str, Any]]:
    content_types = {
        "news_stock": {"content_type": "news", "category": "news:stock"},
        "press_releases": {"content_type": "press_releases", "category": "news:press-releases"},
        "sec_filings": {"content_type": "sec_filings", "category": "news:sec-filings"},
    }
    rows = {
        job_type: {
            **detail,
            "cache_hits": 0,
            "cache_misses": 0,
            "jobs_done": 0,
            "jobs_queued": 0,
            "jobs_failed": 0,
            "items_written": 0,
            "oldest_pending_at": None,
        }
        for job_type, detail in content_types.items()
    }

    for item in summary.get("top_categories") or []:
        category = str(item.get("name") or "")
        kind = str(item.get("kind") or "")
        count = int(item.get("count") or 0)
        for row in rows.values():
            if category != row["category"]:
                continue
            if kind == "cache_hit":
                row["cache_hits"] += count
            elif kind == "cache_miss":
                row["cache_misses"] += count

    for item in summary.get("content_writes") or []:
        category = str(item.get("category") or "")
        for row in rows.values():
            if category == row["category"]:
                row["items_written"] += int(item.get("items_written") or 0)

    if enrichment_queue:
        for item in enrichment_queue.get("by_type_status") or []:
            job_type = str(item.get("job_type") or "")
            if job_type not in rows:
                continue
            status = str(item.get("status") or "")
            count = int(item.get("count") or 0)
            if status == "done":
                rows[job_type]["jobs_done"] += count
            elif status == "queued":
                rows[job_type]["jobs_queued"] += count
            elif status == "failed":
                rows[job_type]["jobs_failed"] += count
        oldest = enrichment_queue.get("oldest_pending_content_job")
        if isinstance(oldest, dict):
            job_type = str(oldest.get("job_type") or "")
            if job_type in rows:
                rows[job_type]["oldest_pending_at"] = oldest.get("created_at")

    return [rows[job_type] for job_type in ("news_stock", "press_releases", "sec_filings")]


def _recommendation(summary: dict[str, Any]) -> str:
    if summary.get("status") == "critical":
        return "Approaching FMP Premium limit. Reduce refresh frequency or upgrade bandwidth."
    if summary.get("totals", {}).get("fallbacks"):
        return "Some sections are falling back. Check refresh jobs and cache coverage before increasing traffic."
    if summary.get("cache_hit_rate") is not None and summary["cache_hit_rate"] < 85:
        return "Cache hit rate is low. Expand background refresh coverage for active symbols."
    return "Provider usage is within configured guardrails."
