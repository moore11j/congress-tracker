from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import requests
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.orm import Session

from app.db import SessionLocal
from app.models import DataEnrichmentJob, Event, PageViewEvent, Security, WatchlistItem
from app.request_priority import reset_request_context, set_request_context
from app.utils.symbols import normalize_symbol

logger = logging.getLogger(__name__)

ACTIVE_STATUSES = {"queued", "running"}
DEFAULT_PREWARM_SYMBOLS = ("MSTR", "NBIS", "BMNR", "IBIT", "AAPL", "MSFT", "NVDA", "TSLA")
DONE_JOB_COOLDOWN_SECONDS = 60 * 60
SYMBOL_REQUIRED_JOB_TYPES = {
    "quote",
    "price_eod",
    "price_series",
    "fundamentals",
    "news_stock",
    "press_releases",
    "sec_filings",
    "ticker_financials",
    "ticker_meta",
    "technical_indicators",
    "profile",
}


class RetryableProviderTimeout(RuntimeError):
    def __init__(self, message: str = "provider_timeout") -> None:
        super().__init__(message)
        self.reason_code = "provider_timeout"
        self.retryable = True


def is_valid_enrichment_symbol(symbol: str | None) -> bool:
    normalized = normalize_symbol(symbol)
    return bool(normalized)


def _normalized_prewarm_symbol(raw: str | None, *, source: str) -> str | None:
    symbol = normalize_symbol(raw)
    if symbol and is_valid_enrichment_symbol(symbol):
        return symbol
    logger.info(
        "prewarm_ticker_invalid_symbol_skipped source=%s symbol=%s",
        source,
        "" if raw is None else str(raw).strip(),
    )
    return None


def _job_requires_symbol(job_type: str | None) -> bool:
    return (job_type or "").strip().lower() in SYMBOL_REQUIRED_JOB_TYPES


def build_dedupe_key(
    *,
    job_type: str,
    symbol: str | None = None,
    date_key: str | None = None,
    window_key: str | None = None,
) -> str:
    normalized_symbol = normalize_symbol(symbol) if symbol else ""
    return "|".join(
        [
            (job_type or "").strip().lower(),
            normalized_symbol or "",
            (date_key or "").strip(),
            (window_key or "").strip(),
        ]
    )


def enqueue_data_enrichment_job(
    *,
    job_type: str,
    symbol: str | None = None,
    date_key: str | None = None,
    window_key: str | None = None,
    source: str = "page_load",
    reason: str = "cache_miss",
    priority: int = 100,
    payload: dict[str, Any] | None = None,
    max_attempts: int = 5,
) -> bool:
    if symbol is not None and not is_valid_enrichment_symbol(symbol):
        logger.info(
            "data_enrichment_job_rejected reason=invalid_symbol job_type=%s symbol=%s",
            job_type,
            symbol,
        )
        return False
    if symbol is None and _job_requires_symbol(job_type):
        logger.info(
            "data_enrichment_job_rejected reason=invalid_symbol job_type=%s symbol=%s",
            job_type,
            symbol,
        )
        return False

    dedupe_key = build_dedupe_key(
        job_type=job_type,
        symbol=symbol,
        date_key=date_key,
        window_key=window_key,
    )
    if not job_type or not dedupe_key.strip("|"):
        return False
    normalized_symbol = normalize_symbol(symbol) if symbol else None
    payload_json = json.dumps(payload, sort_keys=True) if payload else None
    now = datetime.now(timezone.utc)
    db = SessionLocal()
    try:
        existing = db.execute(
            select(DataEnrichmentJob).where(DataEnrichmentJob.dedupe_key == dedupe_key)
        ).scalar_one_or_none()
        if existing is not None:
            if existing.status in ACTIVE_STATUSES:
                return False
            if existing.status == "done" and _job_completed_recently(existing, now):
                return False
            existing.status = "queued"
            existing.reason = reason
            existing.source = source
            existing.priority = min(int(existing.priority or priority), int(priority))
            existing.error = None
            existing.next_run_at = now
            existing.updated_at = now
            if payload_json:
                existing.payload_json = payload_json
            db.commit()
            return True

        db.add(
            DataEnrichmentJob(
                job_type=job_type,
                symbol=normalized_symbol,
                date_key=date_key,
                window_key=window_key,
                dedupe_key=dedupe_key,
                priority=int(priority),
                status="queued",
                attempts=0,
                max_attempts=int(max_attempts),
                source=source,
                reason=reason,
                payload_json=payload_json,
                next_run_at=now,
            )
        )
        db.commit()
        return True
    except IntegrityError:
        db.rollback()
        return False
    except OperationalError as exc:
        db.rollback()
        logger.info("data_enrichment_enqueue_skipped reason=db_busy job_type=%s symbol=%s error=%s", job_type, symbol, exc.__class__.__name__)
        return False
    except Exception:
        db.rollback()
        logger.exception("data_enrichment_enqueue_failed job_type=%s symbol=%s", job_type, symbol)
        return False
    finally:
        db.close()


def skip_invalid_symbol_jobs(db: Session) -> int:
    rows = db.execute(
        select(DataEnrichmentJob)
        .where(DataEnrichmentJob.status.in_(sorted(ACTIVE_STATUSES)))
        .where(DataEnrichmentJob.job_type.in_(sorted(SYMBOL_REQUIRED_JOB_TYPES)))
    ).scalars().all()
    skipped = 0
    now = datetime.now(timezone.utc)
    for row in rows:
        if is_valid_enrichment_symbol(row.symbol):
            continue
        row.status = "skipped"
        row.reason = "invalid_symbol"
        row.error = "invalid_symbol"
        row.updated_at = now
        skipped += 1
    if skipped:
        db.commit()
    return skipped


def _job_completed_recently(job: DataEnrichmentJob, now: datetime) -> bool:
    updated_at = job.updated_at or job.created_at
    if updated_at is None:
        return False
    if updated_at.tzinfo is None:
        updated_at = updated_at.replace(tzinfo=timezone.utc)
    return (now - updated_at).total_seconds() < DONE_JOB_COOLDOWN_SECONDS


def enrichment_queue_summary(db: Session, *, limit: int = 20) -> dict[str, Any]:
    status_rows = db.execute(
        select(DataEnrichmentJob.job_type, DataEnrichmentJob.status, func.count(DataEnrichmentJob.id))
        .group_by(DataEnrichmentJob.job_type, DataEnrichmentJob.status)
        .order_by(DataEnrichmentJob.job_type.asc(), DataEnrichmentJob.status.asc())
    ).all()
    failed_rows = db.execute(
        select(DataEnrichmentJob.job_type, DataEnrichmentJob.reason, DataEnrichmentJob.error, func.count(DataEnrichmentJob.id))
        .where(DataEnrichmentJob.status == "failed")
        .group_by(DataEnrichmentJob.job_type, DataEnrichmentJob.reason, DataEnrichmentJob.error)
        .order_by(func.count(DataEnrichmentJob.id).desc())
        .limit(limit)
    ).all()
    recent = db.execute(
        select(DataEnrichmentJob)
        .order_by(DataEnrichmentJob.updated_at.desc(), DataEnrichmentJob.id.desc())
        .limit(limit)
    ).scalars().all()
    oldest_pending = db.execute(
        select(DataEnrichmentJob)
        .where(DataEnrichmentJob.status == "queued")
        .order_by(DataEnrichmentJob.created_at.asc(), DataEnrichmentJob.id.asc())
        .limit(1)
    ).scalar_one_or_none()
    recent_success_rows = db.execute(
        select(DataEnrichmentJob.job_type, func.count(DataEnrichmentJob.id))
        .where(DataEnrichmentJob.status == "done")
        .where(DataEnrichmentJob.updated_at >= datetime.now(timezone.utc) - timedelta(hours=24))
        .group_by(DataEnrichmentJob.job_type)
        .order_by(func.count(DataEnrichmentJob.id).desc())
    ).all()
    return {
        "by_type_status": [
            {"job_type": job_type, "status": status, "count": int(count or 0)}
            for job_type, status, count in status_rows
        ],
        "failed_by_reason": [
            {
                "job_type": job_type,
                "reason": reason,
                "error": error,
                "count": int(count or 0),
            }
            for job_type, reason, error, count in failed_rows
        ],
        "recent": [
            {
                "id": row.id,
                "job_type": row.job_type,
                "symbol": row.symbol,
                "date_key": row.date_key,
                "window_key": row.window_key,
                "status": row.status,
                "attempts": row.attempts,
                "max_attempts": row.max_attempts,
                "source": row.source,
                "reason": row.reason,
                "error": row.error,
                "next_run_at": row.next_run_at.isoformat() if row.next_run_at else None,
                "updated_at": row.updated_at.isoformat() if row.updated_at else None,
            }
            for row in recent
        ],
        "oldest_pending_job": _job_summary(oldest_pending) if oldest_pending is not None else None,
        "recent_successes_by_type": [
            {"job_type": job_type, "count": int(count or 0)}
            for job_type, count in recent_success_rows
        ],
    }


def _job_summary(row: DataEnrichmentJob) -> dict[str, Any]:
    return {
        "id": row.id,
        "job_type": row.job_type,
        "symbol": row.symbol,
        "date_key": row.date_key,
        "window_key": row.window_key,
        "status": row.status,
        "source": row.source,
        "reason": row.reason,
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
    }


def _recently_viewed_ticker_symbols(db: Session, *, limit: int) -> list[str]:
    rows = db.execute(
        select(PageViewEvent.normalized_path)
        .where(PageViewEvent.normalized_path.like("/ticker/%"))
        .where(PageViewEvent.created_at >= datetime.now(timezone.utc) - timedelta(days=7))
        .order_by(PageViewEvent.created_at.desc())
        .limit(max(1, limit * 4))
    ).scalars().all()
    symbols: list[str] = []
    seen: set[str] = set()
    for path in rows:
        raw = str(path or "").split("?", 1)[0].removeprefix("/ticker/").strip()
        symbol = _normalized_prewarm_symbol(raw, source="recently_viewed")
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        symbols.append(symbol)
        if len(symbols) >= limit:
            break
    return symbols


def enqueue_priority_ticker_prewarm_jobs(
    db: Session,
    *,
    symbol_limit: int = 40,
    popular_limit: int = 15,
    source: str = "priority_ticker_prewarm",
) -> dict[str, Any]:
    normalized_limit = max(1, min(int(symbol_limit or 40), 100))
    normalized_popular_limit = max(0, min(int(popular_limit or 15), normalized_limit))
    raw_watchlist_symbols = db.execute(
        select(func.upper(Security.symbol))
        .select_from(WatchlistItem)
        .join(Security, Security.id == WatchlistItem.security_id)
        .where(Security.symbol.is_not(None))
        .where(func.length(func.trim(Security.symbol)) > 0)
        .group_by(func.upper(Security.symbol))
        .order_by(func.count(WatchlistItem.id).desc(), func.upper(Security.symbol))
        .limit(normalized_limit)
    ).scalars().all()
    watchlist_symbols = [
        symbol
        for raw in raw_watchlist_symbols
        if (symbol := _normalized_prewarm_symbol(raw, source="watchlist"))
    ]
    raw_popular_symbols = db.execute(
        select(func.upper(Event.symbol))
        .where(Event.symbol.is_not(None))
        .where(func.length(func.trim(Event.symbol)) > 0)
        .where(Event.event_type.in_(["congress_trade", "insider_trade", "government_contract"]))
        .group_by(func.upper(Event.symbol))
        .order_by(func.count(Event.id).desc(), func.upper(Event.symbol))
        .limit(normalized_popular_limit)
    ).scalars().all()
    popular_symbols = [
        symbol
        for raw in raw_popular_symbols
        if (symbol := _normalized_prewarm_symbol(raw, source="popular"))
    ]
    recently_viewed_symbols = _recently_viewed_ticker_symbols(db, limit=normalized_limit)
    landing_symbols = [
        symbol
        for raw in os.getenv("PRIORITY_TICKER_PREWARM_LANDING_SYMBOLS", "").replace("|", ",").split(",")
        if raw.strip()
        if (symbol := _normalized_prewarm_symbol(raw, source="landing"))
    ]
    symbols: list[str] = []
    seen: set[str] = set()
    ordered_sources = [
        *[(raw, "watchlist") for raw in watchlist_symbols],
        *[(raw, "recently_viewed") for raw in recently_viewed_symbols],
        *[(raw, "default") for raw in DEFAULT_PREWARM_SYMBOLS],
        *[(raw, "popular") for raw in popular_symbols],
        *[(raw, "landing") for raw in landing_symbols],
    ]
    for raw, raw_source in ordered_sources:
        symbol = _normalized_prewarm_symbol(raw, source=raw_source)
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        symbols.append(symbol)
        if len(symbols) >= normalized_limit:
            break

    today = datetime.now(timezone.utc).date()
    windows = {
        "30d": ((today - timedelta(days=29)).isoformat(), today.isoformat(), 25),
        "365d": ((today - timedelta(days=364)).isoformat(), today.isoformat(), 55),
    }
    enqueued = 0
    attempted = 0
    enqueued_by_type: dict[str, int] = {}
    attempted_by_type: dict[str, int] = {}
    skip_reasons: dict[str, int] = {}
    skip_reasons_by_type: dict[str, dict[str, int]] = {}

    def _enqueue(**kwargs) -> None:
        nonlocal attempted, enqueued
        job_type = str(kwargs.get("job_type") or "")
        attempted += 1
        attempted_by_type[job_type] = attempted_by_type.get(job_type, 0) + 1
        if enqueue_data_enrichment_job(**kwargs):
            enqueued += 1
            enqueued_by_type[job_type] = enqueued_by_type.get(job_type, 0) + 1
            return
        reason = _enqueue_skip_reason(
            job_type=job_type,
            symbol=kwargs.get("symbol"),
            date_key=kwargs.get("date_key"),
            window_key=kwargs.get("window_key"),
        )
        skip_reasons[reason] = skip_reasons.get(reason, 0) + 1
        by_type = skip_reasons_by_type.setdefault(job_type, {})
        by_type[reason] = by_type.get(reason, 0) + 1

    for symbol in symbols:
        for job_type, priority, payload in (
            ("quote", 10, None),
            ("ticker_meta", 15, None),
            ("fundamentals", 45, None),
            ("ticker_financials", 50, None),
            ("news_stock", 65, {"page": 0, "limit": 20}),
            ("press_releases", 70, {"page": 0, "limit": 20}),
            ("sec_filings", 75, {"page": 0, "limit": 50}),
        ):
            _enqueue(
                job_type=job_type,
                symbol=symbol,
                source=source,
                reason="enqueued_missing_profile" if job_type == "ticker_meta" else "priority_ticker_prewarm",
                priority=priority,
                payload=payload,
                max_attempts=3,
            )
        for label, (start_key, end_key, priority) in windows.items():
            _enqueue(
                job_type="price_series",
                symbol=symbol,
                window_key=f"{start_key}:{end_key}",
                source=source,
                reason="enqueued_missing_price_volume",
                priority=priority,
                max_attempts=3,
            )
        _enqueue(
            job_type="technical_indicators",
            symbol=symbol,
            window_key="technical:90d",
            source=source,
            reason="enqueued_missing_price_volume",
            priority=60,
            max_attempts=3,
        )

    return {
        "symbols": symbols,
        "symbol_count": len(symbols),
        "attempted": attempted,
        "enqueued": enqueued,
        "attempted_by_type": attempted_by_type,
        "enqueued_by_type": enqueued_by_type,
        "skip_reasons": skip_reasons,
        "skip_reasons_by_type": skip_reasons_by_type,
        "watchlist_symbol_count": len(watchlist_symbols),
        "recently_viewed_symbol_count": len(recently_viewed_symbols),
        "popular_symbol_count": len(popular_symbols),
        "landing_symbol_count": len(landing_symbols),
        "skipped_budget": 0,
        "skipped_fresh": skip_reasons.get("skipped_fresh", 0),
        "skipped_existing_pending": skip_reasons.get("skipped_existing_pending", 0),
    }


def _enqueue_skip_reason(
    *,
    job_type: str,
    symbol: object = None,
    date_key: object = None,
    window_key: object = None,
) -> str:
    dedupe_key = build_dedupe_key(
        job_type=job_type,
        symbol=str(symbol) if symbol is not None else None,
        date_key=str(date_key) if date_key is not None else None,
        window_key=str(window_key) if window_key is not None else None,
    )
    if not dedupe_key.strip("|"):
        return "skipped_invalid"
    now = datetime.now(timezone.utc)
    db = SessionLocal()
    try:
        existing = db.execute(
            select(DataEnrichmentJob).where(DataEnrichmentJob.dedupe_key == dedupe_key)
        ).scalar_one_or_none()
    except Exception:
        return "skipped_enqueue_failed"
    finally:
        db.close()
    if existing is None:
        return "skipped_enqueue_failed"
    if existing.status in ACTIVE_STATUSES:
        return "skipped_existing_pending"
    if existing.status == "done" and _job_completed_recently(existing, now):
        return "skipped_fresh"
    return "skipped_enqueue_failed"


def process_data_enrichment_jobs(*, limit: int = 25, max_seconds: int | None = None) -> dict[str, Any]:
    if os.getenv("FMP_BACKGROUND_REFRESH_ENABLED", "true").strip().lower() in {"0", "false", "no", "off"}:
        logger.info("data_enrichment_queue_skipped reason=background_refresh_disabled")
        return {"processed": 0, "succeeded": 0, "failed": 0, "skipped": 1}

    db = SessionLocal()
    processed = 0
    succeeded = 0
    failed = 0
    skipped = 0
    deadline = time.monotonic() + max_seconds if max_seconds and max_seconds > 0 else None
    try:
        now = datetime.now(timezone.utc)
        jobs = db.execute(
            select(DataEnrichmentJob)
            .where(DataEnrichmentJob.status == "queued")
            .where(DataEnrichmentJob.next_run_at <= now)
            .order_by(DataEnrichmentJob.priority.asc(), DataEnrichmentJob.created_at.asc(), DataEnrichmentJob.id.asc())
            .limit(max(1, int(limit)))
        ).scalars().all()
        for job in jobs:
            if deadline is not None and time.monotonic() >= deadline:
                skipped = len(jobs) - processed
                logger.info(
                    "data_enrichment_queue_time_limit_reached processed=%s skipped=%s max_seconds=%s",
                    processed,
                    skipped,
                    max_seconds,
                )
                break
            processed += 1
            if _job_requires_symbol(job.job_type) and not is_valid_enrichment_symbol(job.symbol):
                skipped += 1
                job.status = "skipped"
                job.reason = "invalid_symbol"
                job.error = "invalid_symbol"
                job.updated_at = datetime.now(timezone.utc)
                db.commit()
                logger.info(
                    "data_enrichment_job_rejected reason=invalid_symbol id=%s job_type=%s symbol=%s",
                    job.id,
                    job.job_type,
                    job.symbol,
                )
                continue
            job.status = "running"
            job.updated_at = datetime.now(timezone.utc)
            db.commit()
            try:
                token = set_request_context(
                    {
                        "path": "background",
                        "priority": "normal",
                        "job_type": job.job_type,
                        "source": job.source,
                    }
                )
                try:
                    _process_one(db, job)
                finally:
                    reset_request_context(token)
            except Exception as exc:
                db.rollback()
                failed += 1
                attempts = int(job.attempts or 0) + 1
                max_attempts = int(job.max_attempts or 5)
                reason_code = getattr(exc, "reason_code", None) or ("provider_timeout" if isinstance(exc, requests.Timeout) else None)
                job.attempts = attempts
                job.status = "failed" if attempts >= max_attempts else "queued"
                job.reason = str(reason_code or job.reason or "job_failed")[:100]
                job.error = str(reason_code or exc)[:500]
                job.next_run_at = datetime.now(timezone.utc) + timedelta(minutes=min(60, 2 ** min(attempts, 6)))
                job.updated_at = datetime.now(timezone.utc)
                db.commit()
                logger.warning(
                    "data_enrichment_job_failed id=%s type=%s symbol=%s attempts=%s reason=%s retryable=%s",
                    job.id,
                    job.job_type,
                    job.symbol,
                    attempts,
                    reason_code or "job_failed",
                    bool(getattr(exc, "retryable", False) or reason_code == "provider_timeout"),
                )
                continue
            succeeded += 1
            job.status = "done"
            job.error = None
            job.updated_at = datetime.now(timezone.utc)
            db.commit()
    finally:
        db.close()
    return {"processed": processed, "succeeded": succeeded, "failed": failed, "skipped": skipped}


def _process_one(db: Session, job: DataEnrichmentJob) -> None:
    if job.job_type == "quote":
        from app.services.quote_lookup import get_current_prices_meta_db

        get_current_prices_meta_db(db, [job.symbol or ""], allow_cache_write=True)
        return
    if job.job_type == "price_eod":
        from app.services.price_lookup import get_eod_close_with_meta

        get_eod_close_with_meta(db, job.symbol or "", job.date_key or "", allow_cache_write=True)
        return
    if job.job_type == "price_series":
        from app.services.price_lookup import get_daily_close_series_with_fallback

        start_key, end_key = _window_bounds(job.window_key)
        get_daily_close_series_with_fallback(db, job.symbol or "", start_key, end_key)
        return
    if job.job_type == "fundamentals":
        from app.services.fundamentals_cache import fetch_fundamentals_for_symbol, upsert_fundamentals_cache

        result = fetch_fundamentals_for_symbol(job.symbol or "")
        if result.status != "ok":
            raise RuntimeError(result.error or result.status)
        upsert_fundamentals_cache(db, result.values)
        return
    if job.job_type == "fundamentals_universe":
        from app.services.fundamentals_cache import fetch_screener_universe_fundamentals, upsert_fundamentals_cache

        limit = _payload_limit(job.payload_json) or _window_limit(job.window_key) or 500
        for result in fetch_screener_universe_fundamentals(limit=limit):
            if result.status == "ok":
                upsert_fundamentals_cache(db, result.values)
        return
    if job.job_type == "news_general":
        from app.services.fmp_news import get_general_news

        payload = _payload_dict(job.payload_json)
        get_general_news(page=_payload_int(payload, "page", 0), limit=_payload_int(payload, "limit", 20))
        return
    if job.job_type == "news_stock":
        from app.services.fmp_news import get_stock_news

        payload = _payload_dict(job.payload_json)
        result = get_stock_news(symbol=job.symbol or "", page=_payload_int(payload, "page", 0), limit=_payload_int(payload, "limit", 20))
        _raise_for_retryable_provider_result(result)
        return
    if job.job_type == "press_releases":
        from app.services.fmp_news import get_press_releases

        payload = _payload_dict(job.payload_json)
        result = get_press_releases(symbol=job.symbol or "", page=_payload_int(payload, "page", 0), limit=_payload_int(payload, "limit", 20))
        _raise_for_retryable_provider_result(result)
        return
    if job.job_type == "sec_filings":
        from app.services.fmp_news import get_sec_filings

        payload = _payload_dict(job.payload_json)
        result = get_sec_filings(
            symbol=job.symbol or "",
            from_date=_payload_str(payload, "from_date"),
            to_date=_payload_str(payload, "to_date"),
            page=_payload_int(payload, "page", 0),
            limit=_payload_int(payload, "limit", 100),
        )
        _raise_for_retryable_provider_result(result)
        return
    if job.job_type == "macro_snapshot":
        from app.services.fmp_market_snapshot import get_macro_snapshot

        get_macro_snapshot()
        return
    if job.job_type == "ticker_financials":
        from app.services.ticker_financials import get_ticker_financials

        result = get_ticker_financials(job.symbol or "")
        _raise_for_retryable_provider_result(result)
        return
    if job.job_type == "ticker_meta":
        from app.services.ticker_meta import get_ticker_meta

        get_ticker_meta(db, [job.symbol or ""], allow_refresh=True)
        return
    if job.job_type == "technical_indicators":
        from app.services.technical_indicators import build_ticker_technical_indicators

        build_ticker_technical_indicators(
            db,
            job.symbol or "",
            lookback_days=90,
            release_connection_before_provider=True,
            hydrate_provider=True,
        )
        return
    if job.job_type == "cik_meta":
        from app.services.ticker_meta import get_cik_meta

        get_cik_meta(db, [job.window_key or job.symbol or ""], allow_refresh=True)
        return
    if job.job_type == "trade_outcomes":
        from app.compute_trade_outcomes import run_compute

        payload = _payload_dict(job.payload_json)
        event_type = _payload_str(payload, "event_type") or "all"
        lookback_days = _payload_int(payload, "lookback_days", 30)
        limit = _payload_int(payload, "limit", 100)
        retry_statuses = _payload_str(payload, "retry_failed_statuses")
        run_compute(
            replace=False,
            limit=max(1, min(limit, 500)),
            member_id=None,
            event_type=event_type,
            benchmark_symbol=_payload_str(payload, "benchmark_symbol") or "^GSPC",
            lookback_days=max(1, min(lookback_days, 1095)),
            trade_date_after=None,
            only_missing=True,
            retry_failed_status=None,
            retry_failed_statuses=retry_statuses,
        )
        return
    if job.job_type == "priority_ticker_prewarm":
        payload = _payload_dict(job.payload_json)
        enqueue_priority_ticker_prewarm_jobs(
            db,
            symbol_limit=_payload_int(payload, "symbol_limit", 40),
            popular_limit=_payload_int(payload, "popular_limit", 15),
            source="priority_ticker_prewarm",
        )
        return
    if job.job_type == "profile":
        from app.main import _company_profile_snapshot_from_fmp

        _company_profile_snapshot_from_fmp(job.symbol or "")
        return
    raise RuntimeError(f"unsupported_job_type:{job.job_type}")


def _raise_for_retryable_provider_result(result: Any) -> None:
    if not isinstance(result, dict):
        return
    reason = str(result.get("reason") or "")
    if reason == "provider_timeout":
        raise RetryableProviderTimeout()
    subsections = result.get("subsections")
    if isinstance(subsections, dict):
        reasons = {
            str(detail.get("reason_code") or "")
            for detail in subsections.values()
            if isinstance(detail, dict)
        }
        if reasons == {"provider_timeout"}:
            raise RetryableProviderTimeout()


def _window_bounds(window_key: str | None) -> tuple[str, str]:
    today = datetime.now(timezone.utc).date()
    if isinstance(window_key, str) and ":" in window_key and not window_key.startswith("technical:"):
        start_key, end_key = window_key.split(":", 1)
        if len(start_key) >= 10 and len(end_key) >= 10:
            return start_key[:10], end_key[:10]
    if isinstance(window_key, str) and window_key.startswith("technical:"):
        try:
            days = int(window_key.split(":", 1)[1].removesuffix("d"))
        except (IndexError, ValueError):
            days = 120
        return (today - timedelta(days=max(days - 1, 0))).isoformat(), today.isoformat()
    return (today - timedelta(days=119)).isoformat(), today.isoformat()


def _window_limit(window_key: str | None) -> int | None:
    if not isinstance(window_key, str) or not window_key.startswith("limit:"):
        return None
    try:
        return max(1, int(window_key.split(":", 1)[1]))
    except (IndexError, ValueError):
        return None


def _payload_limit(payload_json: str | None) -> int | None:
    if not payload_json:
        return None
    try:
        payload = json.loads(payload_json)
    except ValueError:
        return None
    if not isinstance(payload, dict):
        return None
    raw = payload.get("limit")
    try:
        return max(1, int(raw))
    except (TypeError, ValueError):
        return None


def _payload_dict(payload_json: str | None) -> dict[str, Any]:
    if not payload_json:
        return {}
    try:
        payload = json.loads(payload_json)
    except ValueError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _payload_int(payload: dict[str, Any], key: str, default: int) -> int:
    try:
        return int(payload.get(key, default))
    except (TypeError, ValueError):
        return default


def _payload_str(payload: dict[str, Any], key: str) -> str | None:
    value = payload.get(key)
    if value is None:
        return None
    text = str(value).strip()
    return text or None
