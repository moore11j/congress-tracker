from __future__ import annotations

import argparse
import json
import logging
import os
from dataclasses import asdict, dataclass
from typing import Any

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.db import DATABASE_URL, SessionLocal

logger = logging.getLogger(__name__)


def _truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "on"}


def _falsey(value: str | None) -> bool:
    return (value or "").strip().lower() in {"0", "false", "no", "off"}


def _int_env(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)) or default)
    except (TypeError, ValueError):
        return default


@dataclass
class BackgroundJobGuardResult:
    job: str
    proceed: bool
    reason: str
    active_connections: int | None = None
    total_connections: int | None = None
    active_limit: int | None = None
    total_limit: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def check_background_job_guard(job: str, *, db: Session | None = None) -> BackgroundJobGuardResult:
    job_name = (job or "background-job").strip() or "background-job"
    if _truthy(os.getenv("BACKGROUND_JOBS_PAUSED")):
        return BackgroundJobGuardResult(job=job_name, proceed=False, reason="background_jobs_paused")
    if _falsey(os.getenv("BACKGROUND_DB_PRESSURE_GUARD_ENABLED")):
        return BackgroundJobGuardResult(job=job_name, proceed=True, reason="guard_disabled")
    if DATABASE_URL.startswith("sqlite"):
        return BackgroundJobGuardResult(job=job_name, proceed=True, reason="sqlite_noop")

    active_limit = _int_env("BACKGROUND_DB_ACTIVE_CONNECTION_LIMIT", 16)
    total_limit = _int_env("BACKGROUND_DB_TOTAL_CONNECTION_LIMIT", 32)
    fail_closed = not _falsey(os.getenv("BACKGROUND_DB_PRESSURE_GUARD_FAIL_CLOSED", "true"))

    owns_session = db is None
    session = db or SessionLocal()
    try:
        row = session.execute(
            text(
                """
                SELECT
                  COUNT(*) FILTER (WHERE state = 'active') AS active_connections,
                  COUNT(*) AS total_connections
                FROM pg_stat_activity
                WHERE datname = current_database()
                """
            )
        ).mappings().one()
        active = int(row.get("active_connections") or 0)
        total = int(row.get("total_connections") or 0)
    except SQLAlchemyError as exc:
        logger.warning("background_job_guard_check_failed job=%s error=%s", job_name, exc.__class__.__name__)
        return BackgroundJobGuardResult(
            job=job_name,
            proceed=not fail_closed,
            reason="db_pressure_check_failed" if fail_closed else "db_pressure_check_failed_open",
            active_limit=active_limit,
            total_limit=total_limit,
        )
    finally:
        if owns_session:
            session.close()

    if active >= active_limit:
        return BackgroundJobGuardResult(
            job=job_name,
            proceed=False,
            reason="db_active_connection_pressure",
            active_connections=active,
            total_connections=total,
            active_limit=active_limit,
            total_limit=total_limit,
        )
    if total >= total_limit:
        return BackgroundJobGuardResult(
            job=job_name,
            proceed=False,
            reason="db_total_connection_pressure",
            active_connections=active,
            total_connections=total,
            active_limit=active_limit,
            total_limit=total_limit,
        )
    return BackgroundJobGuardResult(
        job=job_name,
        proceed=True,
        reason="ok",
        active_connections=active,
        total_connections=total,
        active_limit=active_limit,
        total_limit=total_limit,
    )


def background_job_skip_payload(job: str, guard: BackgroundJobGuardResult) -> dict[str, Any]:
    return {
        "job": job,
        "status": "skipped",
        "skipped": 1,
        "reason": guard.reason,
        "guard": guard.to_dict(),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Check whether a background job should run.")
    parser.add_argument("--job", required=True)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    result = check_background_job_guard(args.job)
    event = "background_job_guard_ok" if result.proceed else "background_job_guard_skipped"
    logger.info("%s job=%s reason=%s active=%s total=%s", event, result.job, result.reason, result.active_connections, result.total_connections)
    print(json.dumps(result.to_dict(), sort_keys=True))
    raise SystemExit(0 if result.proceed else 75)


if __name__ == "__main__":
    main()
