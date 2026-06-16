from __future__ import annotations

import os
import time
from datetime import datetime, timedelta, timezone
from threading import Lock
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models import DataEnrichmentJob, FundamentalsCache, PriceCache, QuoteCache, Security, TickerFinancialsCache, TickerMeta, WatchlistItem
from app.request_priority import reset_request_context, set_request_context
from app.services.data_enrichment_queue import enqueue_data_enrichment_job, is_valid_enrichment_symbol
from app.services.ticker_content_cache import ticker_content_cache_has_items
from app.utils.symbols import normalize_symbol

HydrationState = str

ACTIVE_JOB_STATUSES = {"queued", "running"}
FINAL_JOB_STATUSES = {"done", "failed"}
CRITICAL_JOB_TYPES = {
    "profile": ("ticker_meta", "profile"),
    "quote": ("quote",),
    "chart_30d": ("price_series",),
    "chart_365d": ("price_series",),
    "fundamentals": ("fundamentals",),
    "technicals": ("technical_indicators",),
}
OPTIONAL_JOB_TYPES = {
    "news": ("news_stock",),
    "financials": ("ticker_financials",),
    "press_releases": ("press_releases",),
    "sec_filings": ("sec_filings",),
}
_SYMBOL_LOCKS: dict[str, float] = {}
_SYMBOL_LOCKS_GUARD = Lock()


def ticker_hydration_status(db: Session, symbol: str) -> dict[str, Any]:
    normalized = normalize_symbol(symbol)
    if not normalized:
        return _empty_hydration_status("")
    now = datetime.now(timezone.utc)
    active_jobs = _jobs_for_symbol(db, normalized, statuses=ACTIVE_JOB_STATUSES)
    final_jobs = _jobs_for_symbol(db, normalized, statuses=FINAL_JOB_STATUSES, limit=60)
    active_by_type = _group_jobs(active_jobs)
    final_by_type = _group_jobs(final_jobs)
    chart_counts = _chart_counts(db, normalized, now=now)
    critical = {
        "profile": _profile_state(db, normalized, active_by_type, final_by_type),
        "quote": _quote_state(db, normalized, active_by_type, final_by_type, now=now),
        "chart_30d": _chart_state("chart_30d", chart_counts["30d"], active_by_type, final_by_type, minimum=10),
        "chart_365d": _chart_state("chart_365d", chart_counts["365d"], active_by_type, final_by_type, minimum=60),
        "fundamentals": _fundamentals_state(db, normalized, active_by_type, final_by_type),
        "technicals": _technical_state(chart_counts["90d"], active_by_type, final_by_type),
    }
    optional = {
        "news": _content_state(db, normalized, "news", "news", active_by_type, final_by_type),
        "financials": _financials_content_state(db, normalized, active_by_type, final_by_type),
        "press_releases": _content_state(db, normalized, "press_releases", "press_releases", active_by_type, final_by_type),
        "sec_filings": _content_state(db, normalized, "sec_filings", "sec_filings", active_by_type, final_by_type),
    }
    states = {**critical, **optional}
    missing_sections = [
        section
        for section, state in states.items()
        if state in {"missing", "loading"}
    ]
    return {
        "symbol": normalized,
        "critical": critical,
        "optional": optional,
        "missing_sections": missing_sections,
        "should_request_hydration": bool(missing_sections),
        "queued_jobs_count": len(active_jobs),
        "queued_jobs": [_job_payload(job) for job in active_jobs],
        "queued_jobs_by_type": {job_type: len(jobs) for job_type, jobs in active_by_type.items()},
        "updated_at": now.isoformat(),
    }


def request_ticker_hydration(
    db: Session,
    symbol: str,
    *,
    reason: str = "ticker_page_view",
    priority: int = 25,
) -> dict[str, Any]:
    normalized = normalize_symbol(symbol)
    if not normalized:
        return {
            **_empty_hydration_status(""),
            "status": "skipped_invalid_symbol",
            "enqueued_jobs": [],
            "jobs_enqueued_by_type": {},
            "already_pending_count": 0,
            "skipped_invalid_count": 1,
            "refreshed": {},
        }
    before = ticker_hydration_status(db, normalized)
    enqueue_result = _enqueue_missing_jobs(normalized, before, reason=reason, priority=priority)
    refreshed = _bounded_refresh(db, normalized, before, reason=reason)
    after = ticker_hydration_status(db, normalized)
    return {
        **after,
        "status": "queued" if enqueue_result["enqueued_jobs"] else "already_pending" if enqueue_result["already_pending_count"] else "noop",
        "enqueued_jobs": enqueue_result["enqueued_jobs"],
        "jobs_enqueued_by_type": enqueue_result["jobs_enqueued_by_type"],
        "already_pending_count": enqueue_result["already_pending_count"],
        "skipped_invalid_count": enqueue_result["skipped_invalid_count"],
        "refreshed": refreshed,
    }


def _empty_hydration_status(symbol: str) -> dict[str, Any]:
    critical = {
        "profile": "missing",
        "quote": "missing",
        "chart_30d": "missing",
        "chart_365d": "missing",
        "fundamentals": "missing",
        "technicals": "missing",
    }
    optional = {
        "news": "missing",
        "financials": "missing",
        "press_releases": "missing",
        "sec_filings": "missing",
    }
    return {
        "symbol": symbol,
        "critical": critical,
        "optional": optional,
        "missing_sections": [],
        "should_request_hydration": False,
        "queued_jobs_count": 0,
        "queued_jobs": [],
        "queued_jobs_by_type": {},
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


def _jobs_for_symbol(
    db: Session,
    symbol: str,
    *,
    statuses: set[str],
    limit: int = 100,
) -> list[DataEnrichmentJob]:
    if not symbol:
        return []
    return list(
        db.execute(
            select(DataEnrichmentJob)
            .where(func.upper(DataEnrichmentJob.symbol) == symbol)
            .where(DataEnrichmentJob.status.in_(sorted(statuses)))
            .order_by(DataEnrichmentJob.updated_at.desc(), DataEnrichmentJob.id.desc())
            .limit(limit)
        ).scalars()
    )


def _group_jobs(jobs: list[DataEnrichmentJob]) -> dict[str, list[DataEnrichmentJob]]:
    grouped: dict[str, list[DataEnrichmentJob]] = {}
    for job in jobs:
        grouped.setdefault(job.job_type, []).append(job)
    return grouped


def _has_active_job(active_by_type: dict[str, list[DataEnrichmentJob]], job_types: tuple[str, ...]) -> bool:
    return any(active_by_type.get(job_type) for job_type in job_types)


def _latest_final_job(final_by_type: dict[str, list[DataEnrichmentJob]], job_types: tuple[str, ...]) -> DataEnrichmentJob | None:
    candidates = [job for job_type in job_types for job in final_by_type.get(job_type, [])]
    if not candidates:
        return None
    return max(candidates, key=lambda job: (job.updated_at or job.created_at or datetime.min.replace(tzinfo=timezone.utc), job.id or 0))


def _state_from_jobs(
    key: str,
    active_by_type: dict[str, list[DataEnrichmentJob]],
    final_by_type: dict[str, list[DataEnrichmentJob]],
) -> HydrationState:
    job_types = (*CRITICAL_JOB_TYPES.get(key, ()), *OPTIONAL_JOB_TYPES.get(key, ()))
    if _has_active_job(active_by_type, job_types):
        return "loading"
    latest = _latest_final_job(final_by_type, job_types)
    if latest is not None and latest.status == "failed":
        return "unavailable"
    return "missing"


def _profile_state(
    db: Session,
    symbol: str,
    active_by_type: dict[str, list[DataEnrichmentJob]],
    final_by_type: dict[str, list[DataEnrichmentJob]],
) -> HydrationState:
    security = db.execute(
        select(Security)
        .where(func.upper(Security.symbol) == symbol)
        .limit(1)
    ).scalar_one_or_none()
    meta = db.execute(
        select(TickerMeta)
        .where(func.upper(TickerMeta.symbol) == symbol)
        .limit(1)
    ).scalar_one_or_none()
    fundamentals = _latest_fundamentals(db, symbol)

    has_name = any(
        _profile_text(value)
        for value in (
            security.name if security is not None else None,
            meta.company_name if meta is not None else None,
            fundamentals.company_name if fundamentals is not None else None,
        )
    )
    has_classification = any(
        _profile_text(value)
        for value in (
            security.sector if security is not None else None,
            meta.sector if meta is not None else None,
            meta.industry if meta is not None else None,
            fundamentals.sector if fundamentals is not None else None,
            fundamentals.industry if fundamentals is not None else None,
        )
    )
    if has_name and has_classification:
        return "ok"
    return _state_from_jobs("profile", active_by_type, final_by_type)


def _profile_text(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    if not cleaned:
        return None
    if cleaned.lower() in {"n/a", "na", "none", "null", "unknown", "-", "--"}:
        return None
    return cleaned


def _quote_state(
    db: Session,
    symbol: str,
    active_by_type: dict[str, list[DataEnrichmentJob]],
    final_by_type: dict[str, list[DataEnrichmentJob]],
    *,
    now: datetime,
) -> HydrationState:
    fundamentals = _latest_fundamentals(db, symbol)
    if fundamentals is not None and fundamentals.price is not None:
        return "ok"
    quote = db.execute(select(QuoteCache).where(QuoteCache.symbol == symbol).limit(1)).scalar_one_or_none()
    if quote is not None:
        asof = quote.asof_ts
        if asof.tzinfo is None:
            asof = asof.replace(tzinfo=timezone.utc)
        if now - asof <= timedelta(days=3):
            return "ok"
    return _state_from_jobs("quote", active_by_type, final_by_type)


def _fundamentals_state(
    db: Session,
    symbol: str,
    active_by_type: dict[str, list[DataEnrichmentJob]],
    final_by_type: dict[str, list[DataEnrichmentJob]],
) -> HydrationState:
    row = _latest_fundamentals(db, symbol)
    if row is not None and any(
        value is not None
        for value in (row.market_cap, row.volume, row.avg_volume, row.beta, row.trailing_pe, row.forward_pe)
    ):
        return "ok"
    return _state_from_jobs("fundamentals", active_by_type, final_by_type)


def _chart_state(
    key: str,
    point_count: int,
    active_by_type: dict[str, list[DataEnrichmentJob]],
    final_by_type: dict[str, list[DataEnrichmentJob]],
    *,
    minimum: int,
) -> HydrationState:
    if point_count >= minimum:
        return "ok"
    return _state_from_jobs(key, active_by_type, final_by_type)


def _technical_state(
    point_count: int,
    active_by_type: dict[str, list[DataEnrichmentJob]],
    final_by_type: dict[str, list[DataEnrichmentJob]],
) -> HydrationState:
    if point_count >= 20:
        return "ok"
    return _state_from_jobs("technicals", active_by_type, final_by_type)


def _optional_state(
    key: str,
    active_by_type: dict[str, list[DataEnrichmentJob]],
    final_by_type: dict[str, list[DataEnrichmentJob]],
) -> HydrationState:
    return _state_from_jobs(key, active_by_type, final_by_type)


def _content_state(
    db: Session,
    symbol: str,
    key: str,
    content_type: str,
    active_by_type: dict[str, list[DataEnrichmentJob]],
    final_by_type: dict[str, list[DataEnrichmentJob]],
) -> HydrationState:
    if ticker_content_cache_has_items(db, content_type, symbol):
        return "ok"
    return _optional_state(key, active_by_type, final_by_type)


def _financials_content_state(
    db: Session,
    symbol: str,
    active_by_type: dict[str, list[DataEnrichmentJob]],
    final_by_type: dict[str, list[DataEnrichmentJob]],
) -> HydrationState:
    row = db.get(TickerFinancialsCache, symbol)
    if row is not None and row.status in {"ok", "partial"}:
        return "ok"
    return _optional_state("financials", active_by_type, final_by_type)


def _latest_fundamentals(db: Session, symbol: str) -> FundamentalsCache | None:
    return db.execute(
        select(FundamentalsCache)
        .where(FundamentalsCache.symbol == symbol)
        .where(FundamentalsCache.provider == "fmp")
        .where(FundamentalsCache.status == "ok")
        .order_by(FundamentalsCache.fetched_at.desc())
        .limit(1)
    ).scalar_one_or_none()


def _chart_counts(db: Session, symbol: str, *, now: datetime) -> dict[str, int]:
    today = now.date()
    starts = {
        "30d": (today - timedelta(days=29)).isoformat(),
        "90d": (today - timedelta(days=89)).isoformat(),
        "365d": (today - timedelta(days=364)).isoformat(),
    }
    rows = db.execute(
        select(PriceCache.date)
        .where(PriceCache.symbol == symbol)
        .where(PriceCache.date >= starts["365d"])
    ).scalars().all()
    return {
        label: sum(1 for day in rows if str(day) >= start_key)
        for label, start_key in starts.items()
    }


def _enqueue_missing_jobs(
    symbol: str,
    status: dict[str, Any],
    *,
    reason: str,
    priority: int,
) -> dict[str, Any]:
    today = datetime.now(timezone.utc).date()
    windows = {
        "chart_30d": f"{(today - timedelta(days=29)).isoformat()}:{today.isoformat()}",
        "chart_365d": f"{(today - timedelta(days=364)).isoformat()}:{today.isoformat()}",
    }
    specs: list[tuple[str, str, int, str | None, dict[str, Any] | None]] = [
        ("profile", "ticker_meta", priority, None, None),
        ("profile", "profile", priority + 1, None, None),
        ("quote", "quote", max(5, priority - 5), None, None),
        ("chart_30d", "price_series", priority, windows["chart_30d"], None),
        ("chart_365d", "price_series", priority + 10, windows["chart_365d"], None),
        ("fundamentals", "fundamentals", priority + 15, None, None),
        ("technicals", "technical_indicators", priority + 20, "technical:90d", None),
        ("news", "news_stock", priority + 40, None, {"page": 0, "limit": 20}),
        ("financials", "ticker_financials", priority + 45, None, None),
        ("press_releases", "press_releases", priority + 50, None, {"page": 0, "limit": 20}),
        ("sec_filings", "sec_filings", priority + 55, None, {"page": 0, "limit": 100}),
    ]
    enqueued: list[dict[str, Any]] = []
    enqueued_by_type: dict[str, int] = {}
    already_pending_count = 0
    skipped_invalid_count = 0
    states = {**status.get("critical", {}), **status.get("optional", {})}
    if not is_valid_enrichment_symbol(symbol):
        return {
            "enqueued_jobs": [],
            "jobs_enqueued_by_type": {},
            "already_pending_count": 0,
            "skipped_invalid_count": len([spec for spec in specs if states.get(spec[0]) != "ok"]),
        }
    for key, job_type, job_priority, window_key, payload in specs:
        if states.get(key) == "ok":
            continue
        if states.get(key) == "loading":
            already_pending_count += 1
            continue
        if enqueue_data_enrichment_job(
            job_type=job_type,
            symbol=symbol,
            window_key=window_key,
            source="ticker_hydration",
            reason=reason,
            priority=job_priority,
            payload=payload,
            max_attempts=3,
        ):
            enqueued.append({"job_type": job_type, "symbol": symbol, "window_key": window_key})
            enqueued_by_type[job_type] = enqueued_by_type.get(job_type, 0) + 1
        else:
            already_pending_count += 1
    return {
        "enqueued_jobs": enqueued,
        "jobs_enqueued_by_type": enqueued_by_type,
        "already_pending_count": already_pending_count,
        "skipped_invalid_count": skipped_invalid_count,
    }


def _bounded_refresh(db: Session, symbol: str, status: dict[str, Any], *, reason: str) -> dict[str, Any]:
    if os.getenv("FMP_ALLOW_BOUNDED_TICKER_REFRESH", "false").strip().lower() not in {"1", "true", "yes", "on"}:
        return {"attempted": False, "reason": "disabled", "calls": 0, "refreshed": []}
    if _watchlist_only() and not _is_watchlist_symbol(db, symbol):
        return {"attempted": False, "reason": "watchlist_only", "calls": 0, "refreshed": []}
    if not _acquire_symbol_lock(symbol):
        return {"attempted": False, "reason": "locked", "calls": 0, "refreshed": []}

    max_calls = _max_calls_per_symbol()
    calls = 0
    refreshed: list[str] = []
    critical = status.get("critical", {})
    token = set_request_context({})
    try:
        if calls < max_calls and critical.get("quote") != "ok":
            from app.services.quote_lookup import get_current_prices_meta_db

            get_current_prices_meta_db(db, [symbol], allow_cache_write=True)
            calls += 1
            refreshed.append("quote")
        if calls < max_calls and critical.get("profile") != "ok":
            from app.services.ticker_meta import get_ticker_meta

            get_ticker_meta(db, [symbol], allow_refresh=True)
            calls += 1
            refreshed.append("profile")
        if calls < max_calls and critical.get("chart_30d") != "ok":
            from app.services.price_lookup import get_daily_close_series_with_fallback

            today = datetime.now(timezone.utc).date()
            get_daily_close_series_with_fallback(db, symbol, (today - timedelta(days=29)).isoformat(), today.isoformat())
            calls += 1
            refreshed.append("chart_30d")
        if calls < max_calls and critical.get("chart_365d") != "ok":
            from app.services.price_lookup import get_daily_close_series_with_fallback

            today = datetime.now(timezone.utc).date()
            get_daily_close_series_with_fallback(db, symbol, (today - timedelta(days=364)).isoformat(), today.isoformat())
            calls += 1
            refreshed.append("chart_365d")
        if calls < max_calls and critical.get("fundamentals") != "ok":
            from app.services.fundamentals_cache import fetch_fundamentals_for_symbol, upsert_fundamentals_cache

            result = fetch_fundamentals_for_symbol(symbol)
            calls += 1
            if result.status == "ok":
                upsert_fundamentals_cache(db, result.values)
                db.commit()
            refreshed.append("fundamentals")
    finally:
        reset_request_context(token)
    return {"attempted": True, "reason": reason, "calls": calls, "refreshed": refreshed}


def _acquire_symbol_lock(symbol: str) -> bool:
    ttl = _lock_ttl_seconds()
    now = time.time()
    with _SYMBOL_LOCKS_GUARD:
        expires_at = _SYMBOL_LOCKS.get(symbol, 0)
        if expires_at > now:
            return False
        _SYMBOL_LOCKS[symbol] = now + ttl
        return True


def _lock_ttl_seconds() -> int:
    try:
        return max(1, int(os.getenv("FMP_TICKER_REFRESH_LOCK_TTL_SECONDS", "60") or 60))
    except ValueError:
        return 60


def _max_calls_per_symbol() -> int:
    try:
        return max(0, min(10, int(os.getenv("FMP_TICKER_REFRESH_MAX_CALLS_PER_SYMBOL", "4") or 4)))
    except ValueError:
        return 4


def _watchlist_only() -> bool:
    return os.getenv("FMP_TICKER_REFRESH_WATCHLIST_ONLY", "false").strip().lower() in {"1", "true", "yes", "on"}


def _is_watchlist_symbol(db: Session, symbol: str) -> bool:
    return db.execute(
        select(WatchlistItem.id)
        .join(Security, Security.id == WatchlistItem.security_id)
        .where(func.upper(Security.symbol) == symbol)
        .limit(1)
    ).scalar_one_or_none() is not None


def _job_payload(job: DataEnrichmentJob) -> dict[str, Any]:
    return {
        "id": job.id,
        "job_type": job.job_type,
        "symbol": job.symbol,
        "status": job.status,
        "window_key": job.window_key,
        "priority": job.priority,
        "reason": job.reason,
        "updated_at": job.updated_at.isoformat() if job.updated_at else None,
    }
