from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.orm import Session

from app.db import SessionLocal
from app.models import DataEnrichmentJob
from app.utils.symbols import normalize_symbol

logger = logging.getLogger(__name__)

ACTIVE_STATUSES = {"queued", "running"}


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
    }


def process_data_enrichment_jobs(*, limit: int = 25) -> dict[str, Any]:
    db = SessionLocal()
    processed = 0
    succeeded = 0
    failed = 0
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
            processed += 1
            job.status = "running"
            job.updated_at = datetime.now(timezone.utc)
            db.commit()
            try:
                _process_one(db, job)
            except Exception as exc:
                db.rollback()
                failed += 1
                attempts = int(job.attempts or 0) + 1
                max_attempts = int(job.max_attempts or 5)
                job.attempts = attempts
                job.status = "failed" if attempts >= max_attempts else "queued"
                job.error = str(exc)[:500]
                job.next_run_at = datetime.now(timezone.utc) + timedelta(minutes=min(60, 2 ** min(attempts, 6)))
                job.updated_at = datetime.now(timezone.utc)
                db.commit()
                logger.warning("data_enrichment_job_failed id=%s type=%s symbol=%s attempts=%s", job.id, job.job_type, job.symbol, attempts)
                continue
            succeeded += 1
            job.status = "done"
            job.error = None
            job.updated_at = datetime.now(timezone.utc)
            db.commit()
    finally:
        db.close()
    return {"processed": processed, "succeeded": succeeded, "failed": failed}


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
    raise RuntimeError(f"unsupported_job_type:{job.job_type}")


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
