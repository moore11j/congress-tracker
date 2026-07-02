from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import select, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from app.db import SessionLocal, engine, ensure_institutional_activity_schema
from app.models import InstitutionalIngestJobRun, InstitutionalIngestJobState

logger = logging.getLogger(__name__)

LATEST_FILINGS_JOB_NAME = "latest_filings"
DEFAULT_STALE_RUNNING_MINUTES = 90
DEFAULT_FEED_EVENTS_WARNING_THRESHOLD = 100
SCHEDULED_PAGES_PER_RUN = 1
SCHEDULED_LIMIT = 25
SCHEDULED_MAX_FILINGS = 10
SCHEDULED_ENABLED_ENV = "INSTITUTIONAL_SCHEDULED_INGEST_ENABLED"
SCHEDULED_START_PAGE_ENV = "INSTITUTIONAL_SCHEDULED_INGEST_START_PAGE"


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off", ""}


def _env_int(name: str, default: int, *, minimum: int, maximum: int) -> int:
    try:
        value = int(os.getenv(name, str(default)) or default)
    except (TypeError, ValueError):
        value = default
    return max(minimum, min(maximum, value))


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value if value is not None else default)
    except (TypeError, ValueError):
        return default


def latest_job_defaults() -> dict[str, int | bool]:
    return {
        "enabled": _env_bool(SCHEDULED_ENABLED_ENV, False),
        "cursor_page": _env_int(SCHEDULED_START_PAGE_ENV, 9, minimum=0, maximum=10_000),
        "pages_per_run": SCHEDULED_PAGES_PER_RUN,
        "limit": SCHEDULED_LIMIT,
        "max_filings_per_run": SCHEDULED_MAX_FILINGS,
    }


def _apply_scheduled_window(state: InstitutionalIngestJobState) -> None:
    state.pages_per_run = SCHEDULED_PAGES_PER_RUN
    state.limit = SCHEDULED_LIMIT
    state.max_filings_per_run = SCHEDULED_MAX_FILINGS


def get_or_create_latest_job_state(db: Session) -> InstitutionalIngestJobState:
    state = db.get(InstitutionalIngestJobState, LATEST_FILINGS_JOB_NAME)
    if state is not None:
        return state
    defaults = latest_job_defaults()
    state = InstitutionalIngestJobState(
        job_name=LATEST_FILINGS_JOB_NAME,
        enabled=bool(defaults["enabled"]),
        cursor_page=int(defaults["cursor_page"]),
        pages_per_run=int(defaults["pages_per_run"]),
        limit=int(defaults["limit"]),
        max_filings_per_run=int(defaults["max_filings_per_run"]),
        last_status="idle",
    )
    db.add(state)
    db.flush()
    return state


def initialize_latest_job_state(
    *,
    cursor_page: int | None = None,
    pages_per_run: int | None = None,
    limit: int | None = None,
    max_filings_per_run: int | None = None,
    enabled: bool | None = None,
) -> dict[str, Any]:
    ensure_institutional_activity_schema(engine)
    db = SessionLocal()
    try:
        state = get_or_create_latest_job_state(db)
        if cursor_page is not None:
            state.cursor_page = max(0, int(cursor_page))
        if pages_per_run is not None:
            state.pages_per_run = max(1, min(int(pages_per_run), 20))
        if limit is not None:
            state.limit = max(1, min(int(limit), 100))
        if max_filings_per_run is not None:
            state.max_filings_per_run = max(1, min(int(max_filings_per_run), 50))
        if enabled is not None:
            state.enabled = bool(enabled)
        state.updated_at = _now()
        db.commit()
        db.refresh(state)
        return job_state_payload(state)
    finally:
        db.close()


def _state_query_for_update(db: Session):
    statement = select(InstitutionalIngestJobState).where(InstitutionalIngestJobState.job_name == LATEST_FILINGS_JOB_NAME)
    if db.bind is not None and db.bind.dialect.name != "sqlite":
        statement = statement.with_for_update(nowait=True)
    return statement


def _load_state_for_update(db: Session) -> InstitutionalIngestJobState | None:
    return db.execute(_state_query_for_update(db)).scalar_one_or_none()


def _is_stale_running(state: InstitutionalIngestJobState, now: datetime) -> bool:
    if state.last_status != "running" or state.last_started_at is None:
        return False
    started = state.last_started_at
    if started.tzinfo is None:
        started = started.replace(tzinfo=timezone.utc)
    return started < now - timedelta(minutes=DEFAULT_STALE_RUNNING_MINUTES)


def _create_run(
    db: Session,
    state: InstitutionalIngestJobState,
    *,
    status: str,
    started_at: datetime,
    finished_at: datetime | None = None,
    error_message: str | None = None,
) -> InstitutionalIngestJobRun:
    run = InstitutionalIngestJobRun(
        job_name=state.job_name,
        started_at=started_at,
        finished_at=finished_at,
        status=status,
        start_page=state.cursor_page,
        pages_requested=state.pages_per_run,
        pages_scanned=0,
        limit=state.limit,
        max_filings=state.max_filings_per_run,
        next_cursor_page=state.cursor_page,
        error_message=error_message,
    )
    db.add(run)
    db.flush()
    return run


def _apply_counts_to_run(run: InstitutionalIngestJobRun, result: dict[str, Any]) -> None:
    for name in (
        "pages_scanned",
        "scanned",
        "parsed",
        "parse_failed",
        "already_processed_skipped",
        "processed_filings",
        "empty_extract_retryable",
        "empty_extract_processed_no_holdings",
        "skipped",
        "errors",
        "position_rows",
        "position_changes",
        "summaries",
        "activity_events",
        "feed_events",
    ):
        setattr(run, name, _as_int(result.get(name)))
    first_empty = result.get("first_empty_page_seen")
    run.first_empty_page_seen = int(first_empty) if first_empty is not None else None


def institutional_ingest_duplicate_checks(db: Session) -> dict[str, int]:
    event_date_expr = "coalesce(event_date::text, '')"
    if db.bind is not None and db.bind.dialect.name == "sqlite":
        event_date_expr = "coalesce(CAST(event_date AS TEXT), '')"
    queries = {
        "filings_by_accession": """
            SELECT count(*) FROM (
                SELECT accession_number
                FROM institutional_filings
                WHERE accession_number IS NOT NULL
                GROUP BY accession_number
                HAVING count(*) > 1
            ) d
        """,
        "active_canonical_filing_duplicates": """
            SELECT count(*) FROM (
                SELECT cik, report_year, report_quarter
                FROM institutional_filings
                WHERE superseded_by IS NULL
                GROUP BY cik, report_year, report_quarter
                HAVING count(*) > 1
            ) d
        """,
        "positions_by_canonical_filing_security_put_call": """
            SELECT count(*) FROM (
                SELECT filing_id, coalesce(cusip, ''), coalesce(normalized_symbol, ''), coalesce(put_call, '')
                FROM institutional_positions
                GROUP BY filing_id, coalesce(cusip, ''), coalesce(normalized_symbol, ''), coalesce(put_call, '')
                HAVING count(*) > 1
            ) d
        """,
        "position_changes": """
            SELECT count(*) FROM (
                SELECT cik, coalesce(normalized_symbol, ''), coalesce(cusip, ''), report_year, report_quarter, change_type
                FROM institutional_position_changes
                GROUP BY cik, coalesce(normalized_symbol, ''), coalesce(cusip, ''), report_year, report_quarter, change_type
                HAVING count(*) > 1
            ) d
        """,
        "summaries": """
            SELECT count(*) FROM (
                SELECT normalized_symbol, report_year, report_quarter
                FROM institutional_symbol_summary
                GROUP BY normalized_symbol, report_year, report_quarter
                HAVING count(*) > 1
            ) d
        """,
        "activity_events": """
            SELECT count(*) FROM (
                SELECT normalized_symbol, coalesce(cik, ''), event_type, report_year, report_quarter
                FROM institutional_activity_events
                GROUP BY normalized_symbol, coalesce(cik, ''), event_type, report_year, report_quarter
                HAVING count(*) > 1
            ) d
        """,
        "feed_events": f"""
            SELECT count(*) FROM (
                SELECT source, event_type, coalesce(symbol, ''), {event_date_expr}, coalesce(source_filing_id, ''), coalesce(payload_json, '')
                FROM events
                WHERE source = '13F filing'
                GROUP BY source, event_type, coalesce(symbol, ''), {event_date_expr}, coalesce(source_filing_id, ''), coalesce(payload_json, '')
                HAVING count(*) > 1
            ) d
        """,
    }
    return {name: _as_int(db.execute(text(query)).scalar()) for name, query in queries.items()}


def _finish_scheduled_run(
    db: Session,
    state: InstitutionalIngestJobState,
    run: InstitutionalIngestJobRun,
    result: dict[str, Any],
    *,
    duplicate_checks: dict[str, int],
    feed_events_warning_threshold: int,
) -> dict[str, Any]:
    now = _now()
    _apply_counts_to_run(run, result)
    run.finished_at = now
    duplicate_failures = {key: value for key, value in duplicate_checks.items() if value}
    first_empty = result.get("first_empty_page_seen")
    scanned = _as_int(result.get("scanned"))
    max_filings_reached = bool(_as_int(result.get("max_filings_reached")))
    stop_at_empty = first_empty is not None or scanned == 0
    metadata = {
        "ingest_result": result,
        "duplicate_checks": duplicate_checks,
        "scheduled_window": {
            "start_page": int(run.start_page),
            "pages": SCHEDULED_PAGES_PER_RUN,
            "limit": SCHEDULED_LIMIT,
            "max_filings": SCHEDULED_MAX_FILINGS,
        },
        "max_filings_reached": max_filings_reached,
    }

    if _as_int(result.get("errors")) > 0:
        run.status = "failed"
        run.error_message = "latest-filings ingest reported errors"
        run.next_cursor_page = int(state.cursor_page)
        state.enabled = False
        state.last_status = "failed"
        state.last_error = run.error_message
    elif duplicate_failures:
        run.status = "failed"
        run.error_message = f"duplicate checks failed: {duplicate_failures}"
        run.next_cursor_page = int(state.cursor_page)
        state.enabled = False
        state.last_status = "failed"
        state.last_error = run.error_message
    elif run.feed_events > feed_events_warning_threshold:
        run.status = "partial"
        run.error_message = f"feed event threshold exceeded: {run.feed_events}>{feed_events_warning_threshold}"
        run.next_cursor_page = int(state.cursor_page)
        state.enabled = False
        state.last_status = "paused"
        state.last_error = run.error_message
    else:
        run.status = "success"
        state.last_error = None
        if stop_at_empty:
            empty_page = int(first_empty) if first_empty is not None else int(state.cursor_page)
            run.next_cursor_page = int(state.cursor_page)
            state.first_empty_page = empty_page
            state.enabled = False
            state.last_status = "paused"
            metadata["stop_reason"] = "empty_page"
        elif max_filings_reached:
            # Keep the cursor on this page so the next hourly run can finish the
            # remaining latest-filings rows without skipping candidates.
            run.next_cursor_page = int(state.cursor_page)
            state.last_status = "success"
        else:
            run.next_cursor_page = int(state.cursor_page) + 1
            state.cursor_page = run.next_cursor_page
            state.last_status = "success"

        state.total_pages_scanned += run.pages_scanned
        state.total_filings_processed += run.processed_filings
        state.total_position_rows += run.position_rows
        state.total_activity_events += run.activity_events
        state.total_feed_events += run.feed_events

    state.last_finished_at = now
    state.updated_at = now
    run.metadata_json = json.dumps(metadata, sort_keys=True, default=str)
    return {"status": run.status, "result": result, "duplicate_checks": duplicate_checks}


def run_scheduled_latest_once() -> dict[str, Any]:
    started = _now()
    db = SessionLocal()
    try:
        try:
            state = _load_state_for_update(db)
        except OperationalError:
            db.rollback()
            state = get_or_create_latest_job_state(db)
            run = _create_run(
                db,
                state,
                status="skipped_locked",
                started_at=started,
                finished_at=_now(),
                error_message="job state row is locked by another runner",
            )
            db.commit()
            return {"status": "skipped_locked", "run": job_run_payload(run), "state": job_state_payload(state)}

        if state is None:
            state = get_or_create_latest_job_state(db)
        _apply_scheduled_window(state)
        now = _now()
        if _is_stale_running(state, now):
            state.last_status = "failed"
            state.last_error = "stale running job recovered"
            state.last_finished_at = now
        elif state.last_status == "running":
            run = _create_run(
                db,
                state,
                status="skipped_locked",
                started_at=started,
                finished_at=now,
                error_message="latest-filings job is already running",
            )
            db.commit()
            return {"status": "skipped_locked", "run": job_run_payload(run), "state": job_state_payload(state)}

        if not state.enabled:
            run = _create_run(
                db,
                state,
                status="paused",
                started_at=started,
                finished_at=now,
                error_message="scheduled latest-filings ingestion is disabled",
            )
            state.last_status = "paused"
            state.last_finished_at = now
            state.updated_at = now
            db.commit()
            return {"status": "paused", "run": job_run_payload(run), "state": job_state_payload(state)}

        run = _create_run(db, state, status="running", started_at=started)
        window = {
            "start_page": int(state.cursor_page),
            "pages": int(state.pages_per_run),
            "limit": int(state.limit),
            "max_filings": int(state.max_filings_per_run),
        }
        state.last_status = "running"
        state.last_started_at = started
        state.last_error = None
        state.updated_at = started
        db.commit()
        run_id = int(run.id)
    finally:
        db.close()

    logger.info(
        "institutional_scheduled_latest_run_start run_id=%s start_page=%s pages=%s limit=%s max_filings=%s",
        run_id,
        window["start_page"],
        window["pages"],
        window["limit"],
        window["max_filings"],
    )

    try:
        from app.ingest_institutional_activity import ingest_latest_institutional_filings

        result = ingest_latest_institutional_filings(
            start_page=window["start_page"],
            pages=SCHEDULED_PAGES_PER_RUN,
            limit=SCHEDULED_LIMIT,
            max_filings=SCHEDULED_MAX_FILINGS,
        )
    except Exception as exc:
        logger.exception("institutional_scheduled_latest_run_failed run_id=%s", run_id)
        db = SessionLocal()
        try:
            state = db.get(InstitutionalIngestJobState, LATEST_FILINGS_JOB_NAME)
            run = db.get(InstitutionalIngestJobRun, run_id)
            if state is not None:
                state.last_status = "failed"
                state.last_error = str(exc)
                state.last_finished_at = _now()
                state.enabled = False
            if run is not None:
                run.status = "failed"
                run.finished_at = _now()
                run.error_message = str(exc)
            db.commit()
            return {"status": "failed", "error": str(exc), "run": job_run_payload(run) if run else None}
        finally:
            db.close()

    db = SessionLocal()
    try:
        state = db.get(InstitutionalIngestJobState, LATEST_FILINGS_JOB_NAME)
        run = db.get(InstitutionalIngestJobRun, run_id)
        if state is None or run is None:
            raise RuntimeError("latest-filings job state disappeared during run")
        duplicate_checks = institutional_ingest_duplicate_checks(db)
        finish = _finish_scheduled_run(
            db,
            state,
            run,
            result,
            duplicate_checks=duplicate_checks,
            feed_events_warning_threshold=_env_int(
                "INSTITUTIONAL_SCHEDULED_INGEST_FEED_EVENTS_WARNING_THRESHOLD",
                DEFAULT_FEED_EVENTS_WARNING_THRESHOLD,
                minimum=1,
                maximum=10_000,
            ),
        )
        db.commit()
        db.refresh(state)
        db.refresh(run)
        logger.info(
            "institutional_scheduled_latest_run_finished run_id=%s status=%s start_page=%s pages_scanned=%s next_cursor=%s processed_filings=%s errors=%s feed_events=%s duplicate_checks=%s",
            run.id,
            run.status,
            run.start_page,
            run.pages_scanned,
            run.next_cursor_page,
            run.processed_filings,
            run.errors,
            run.feed_events,
            duplicate_checks,
        )
        return {"status": finish["status"], "state": job_state_payload(state), "run": job_run_payload(run), **finish}
    finally:
        db.close()


def run_latest_ingest_job_once(*, require_enabled: bool = False) -> dict[str, Any]:
    logger.warning(
        "institutional_latest_job_run_once_deprecated use --scheduled-latest-once; require_enabled=%s is ignored",
        require_enabled,
    )
    return run_scheduled_latest_once()


def update_latest_job_config(
    db: Session,
    *,
    pages_per_run: int | None = None,
    max_filings_per_run: int | None = None,
    limit: int | None = None,
) -> InstitutionalIngestJobState:
    state = get_or_create_latest_job_state(db)
    if pages_per_run is not None:
        state.pages_per_run = max(1, min(int(pages_per_run), 20))
    if max_filings_per_run is not None:
        state.max_filings_per_run = max(1, min(int(max_filings_per_run), 50))
    if limit is not None:
        state.limit = max(1, min(int(limit), 100))
    state.updated_at = _now()
    return state


def set_latest_job_enabled(db: Session, enabled: bool) -> InstitutionalIngestJobState:
    state = get_or_create_latest_job_state(db)
    state.enabled = bool(enabled)
    state.last_status = "idle" if enabled else "paused"
    state.updated_at = _now()
    if enabled:
        state.last_error = None
    return state


def set_latest_job_cursor(db: Session, cursor_page: int) -> InstitutionalIngestJobState:
    state = get_or_create_latest_job_state(db)
    state.cursor_page = max(0, int(cursor_page))
    state.first_empty_page = None
    state.updated_at = _now()
    return state


def recent_latest_job_runs(db: Session, *, limit: int = 10) -> list[InstitutionalIngestJobRun]:
    return list(
        db.execute(
            select(InstitutionalIngestJobRun)
            .where(InstitutionalIngestJobRun.job_name == LATEST_FILINGS_JOB_NAME)
            .order_by(InstitutionalIngestJobRun.started_at.desc(), InstitutionalIngestJobRun.id.desc())
            .limit(max(1, min(int(limit), 50)))
        )
        .scalars()
        .all()
    )


def latest_job_status_payload(db: Session) -> dict[str, Any]:
    state = get_or_create_latest_job_state(db)
    runs = recent_latest_job_runs(db, limit=10)
    return {
        "state": job_state_payload(state),
        "latest_run": job_run_payload(runs[0]) if runs else None,
        "recent_runs": [job_run_payload(run) for run in runs],
    }


def job_state_payload(state: InstitutionalIngestJobState) -> dict[str, Any]:
    return {
        "job_name": state.job_name,
        "enabled": bool(state.enabled),
        "cursor_page": int(state.cursor_page),
        "limit": int(state.limit),
        "pages_per_run": int(state.pages_per_run),
        "max_filings_per_run": int(state.max_filings_per_run),
        "first_empty_page": state.first_empty_page,
        "last_started_at": state.last_started_at.isoformat() if state.last_started_at else None,
        "last_finished_at": state.last_finished_at.isoformat() if state.last_finished_at else None,
        "last_status": state.last_status,
        "last_error": state.last_error,
        "total_pages_scanned": int(state.total_pages_scanned),
        "total_filings_processed": int(state.total_filings_processed),
        "total_position_rows": int(state.total_position_rows),
        "total_activity_events": int(state.total_activity_events),
        "total_feed_events": int(state.total_feed_events),
        "created_at": state.created_at.isoformat() if state.created_at else None,
        "updated_at": state.updated_at.isoformat() if state.updated_at else None,
    }


def job_run_payload(run: InstitutionalIngestJobRun | None) -> dict[str, Any] | None:
    if run is None:
        return None
    return {
        "id": run.id,
        "job_name": run.job_name,
        "started_at": run.started_at.isoformat() if run.started_at else None,
        "finished_at": run.finished_at.isoformat() if run.finished_at else None,
        "status": run.status,
        "start_page": int(run.start_page),
        "pages_requested": int(run.pages_requested),
        "pages_scanned": int(run.pages_scanned),
        "limit": int(run.limit),
        "max_filings": int(run.max_filings),
        "scanned": int(run.scanned),
        "parsed": int(run.parsed),
        "parse_failed": int(run.parse_failed),
        "already_processed_skipped": int(run.already_processed_skipped),
        "processed_filings": int(run.processed_filings),
        "empty_extract_retryable": int(run.empty_extract_retryable),
        "empty_extract_processed_no_holdings": int(run.empty_extract_processed_no_holdings),
        "skipped": int(run.skipped),
        "errors": int(run.errors),
        "position_rows": int(run.position_rows),
        "position_changes": int(run.position_changes),
        "summaries": int(run.summaries),
        "activity_events": int(run.activity_events),
        "feed_events": int(run.feed_events),
        "first_empty_page_seen": run.first_empty_page_seen,
        "next_cursor_page": int(run.next_cursor_page),
        "error_message": run.error_message,
        "metadata": json.loads(run.metadata_json) if run.metadata_json else None,
    }
