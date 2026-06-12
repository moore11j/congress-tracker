from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.orm import Session

from app.db import SessionLocal
from app.models import DataEnrichmentJob, Event, Security, WatchlistItem
from app.utils.symbols import normalize_symbol

logger = logging.getLogger(__name__)

ACTIVE_STATUSES = {"queued", "running"}
DEFAULT_PREWARM_SYMBOLS = ("MSTR", "AAPL", "MSFT", "NVDA", "TSLA")


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


def enqueue_priority_ticker_prewarm_jobs(
    db: Session,
    *,
    symbol_limit: int = 40,
    popular_limit: int = 15,
    source: str = "priority_ticker_prewarm",
) -> dict[str, Any]:
    normalized_limit = max(1, min(int(symbol_limit or 40), 100))
    normalized_popular_limit = max(0, min(int(popular_limit or 15), normalized_limit))
    watchlist_symbols = [
        symbol
        for symbol in db.execute(
            select(func.upper(Security.symbol))
            .select_from(WatchlistItem)
            .join(Security, Security.id == WatchlistItem.security_id)
            .where(Security.symbol.is_not(None))
            .where(func.length(func.trim(Security.symbol)) > 0)
            .group_by(func.upper(Security.symbol))
            .order_by(func.count(WatchlistItem.id).desc(), func.upper(Security.symbol))
            .limit(normalized_limit)
        ).scalars().all()
        if normalize_symbol(symbol)
    ]
    popular_symbols = [
        symbol
        for symbol in db.execute(
            select(func.upper(Event.symbol))
            .where(Event.symbol.is_not(None))
            .where(func.length(func.trim(Event.symbol)) > 0)
            .where(Event.event_type.in_(["congress_trade", "insider_trade", "government_contract"]))
            .group_by(func.upper(Event.symbol))
            .order_by(func.count(Event.id).desc(), func.upper(Event.symbol))
            .limit(normalized_popular_limit)
        ).scalars().all()
        if normalize_symbol(symbol)
    ]
    symbols: list[str] = []
    seen: set[str] = set()
    for raw in [*watchlist_symbols, *DEFAULT_PREWARM_SYMBOLS, *popular_symbols]:
        symbol = normalize_symbol(raw)
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
            attempted += 1
            if enqueue_data_enrichment_job(
                job_type=job_type,
                symbol=symbol,
                source=source,
                reason="priority_ticker_prewarm",
                priority=priority,
                payload=payload,
                max_attempts=3,
            ):
                enqueued += 1
        for label, (start_key, end_key, priority) in windows.items():
            attempted += 1
            if enqueue_data_enrichment_job(
                job_type="price_series",
                symbol=symbol,
                window_key=f"{start_key}:{end_key}",
                source=source,
                reason=f"priority_ticker_prewarm_{label}",
                priority=priority,
                max_attempts=3,
            ):
                enqueued += 1

    return {
        "symbols": symbols,
        "symbol_count": len(symbols),
        "attempted": attempted,
        "enqueued": enqueued,
        "watchlist_symbol_count": len(watchlist_symbols),
        "popular_symbol_count": len(popular_symbols),
    }


def process_data_enrichment_jobs(*, limit: int = 25, max_seconds: int | None = None) -> dict[str, Any]:
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
        get_stock_news(symbol=job.symbol or "", page=_payload_int(payload, "page", 0), limit=_payload_int(payload, "limit", 20))
        return
    if job.job_type == "press_releases":
        from app.services.fmp_news import get_press_releases

        payload = _payload_dict(job.payload_json)
        get_press_releases(symbol=job.symbol or "", page=_payload_int(payload, "page", 0), limit=_payload_int(payload, "limit", 20))
        return
    if job.job_type == "sec_filings":
        from app.services.fmp_news import get_sec_filings

        payload = _payload_dict(job.payload_json)
        get_sec_filings(
            symbol=job.symbol or "",
            from_date=_payload_str(payload, "from_date"),
            to_date=_payload_str(payload, "to_date"),
            page=_payload_int(payload, "page", 0),
            limit=_payload_int(payload, "limit", 100),
        )
        return
    if job.job_type == "macro_snapshot":
        from app.services.fmp_market_snapshot import get_macro_snapshot

        get_macro_snapshot()
        return
    if job.job_type == "ticker_financials":
        from app.services.ticker_financials import get_ticker_financials

        get_ticker_financials(job.symbol or "")
        return
    if job.job_type == "ticker_meta":
        from app.services.ticker_meta import get_ticker_meta

        get_ticker_meta(db, [job.symbol or ""], allow_refresh=True)
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
