from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.auth import require_admin_user
from app.db import get_db
from app.rate_limit import rate_limit_admin_mutation
from app.services.institutional_ingest_job import (
    get_or_create_latest_job_state,
    get_or_create_historical_job_state,
    historical_job_state_payload,
    historical_job_status_payload,
    run_historical_backfill_once,
    job_state_payload,
    latest_job_status_payload,
    run_latest_ingest_job_once,
    set_historical_job_cursor,
    set_historical_job_enabled,
    set_latest_job_cursor,
    set_latest_job_enabled,
    update_historical_job_config,
    update_latest_job_config,
)

router = APIRouter(tags=["admin-institutional-ingest"])


class CursorPayload(BaseModel):
    cursor_page: int = Field(ge=0, le=10_000)


class ConfigPayload(BaseModel):
    pages_per_run: int | None = Field(default=None, ge=1, le=20)
    max_filings_per_run: int | None = Field(default=None, ge=1, le=50)
    limit: int | None = Field(default=None, ge=1, le=100)


class HistoricalConfigPayload(BaseModel):
    start_year: int | None = Field(default=None, ge=2000, le=2100)
    end_year: int | None = Field(default=None, ge=2000, le=2100)
    holder_ciks: list[str] | None = None
    max_filings_per_run: int | None = Field(default=None, ge=1, le=10)


@router.get("/admin/institutional-ingest/status")
def admin_institutional_ingest_status(request: Request, db: Session = Depends(get_db)) -> dict[str, Any]:
    require_admin_user(db, request)
    return latest_job_status_payload(db)


@router.post("/admin/institutional-ingest/enable", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_enable_institutional_ingest(request: Request, db: Session = Depends(get_db)) -> dict[str, Any]:
    require_admin_user(db, request)
    state = set_latest_job_enabled(db, True)
    db.commit()
    db.refresh(state)
    return {"state": job_state_payload(state)}


@router.post("/admin/institutional-ingest/disable", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_disable_institutional_ingest(request: Request, db: Session = Depends(get_db)) -> dict[str, Any]:
    require_admin_user(db, request)
    state = set_latest_job_enabled(db, False)
    db.commit()
    db.refresh(state)
    return {"state": job_state_payload(state)}


@router.post("/admin/institutional-ingest/run-once", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_run_institutional_ingest_once(request: Request, db: Session = Depends(get_db)) -> dict[str, Any]:
    require_admin_user(db, request)
    return run_latest_ingest_job_once(require_enabled=False)


@router.post("/admin/institutional-ingest/set-cursor", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_set_institutional_ingest_cursor(
    payload: CursorPayload,
    request: Request,
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    require_admin_user(db, request)
    state = set_latest_job_cursor(db, payload.cursor_page)
    db.commit()
    db.refresh(state)
    return {"state": job_state_payload(state)}


@router.post("/admin/institutional-ingest/config", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_configure_institutional_ingest(
    payload: ConfigPayload,
    request: Request,
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    require_admin_user(db, request)
    state = update_latest_job_config(
        db,
        pages_per_run=payload.pages_per_run,
        max_filings_per_run=payload.max_filings_per_run,
        limit=payload.limit,
    )
    db.commit()
    db.refresh(state)
    return {"state": job_state_payload(state)}


@router.post("/admin/institutional-ingest/init", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_init_institutional_ingest(request: Request, db: Session = Depends(get_db)) -> dict[str, Any]:
    require_admin_user(db, request)
    state = get_or_create_latest_job_state(db)
    db.commit()
    db.refresh(state)
    return {"state": job_state_payload(state)}


@router.get("/admin/institutional-ingest/historical/status")
def admin_historical_institutional_ingest_status(request: Request, db: Session = Depends(get_db)) -> dict[str, Any]:
    require_admin_user(db, request)
    return historical_job_status_payload(db)


@router.post("/admin/institutional-ingest/historical/init", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_init_historical_institutional_ingest(request: Request, db: Session = Depends(get_db)) -> dict[str, Any]:
    require_admin_user(db, request)
    state = get_or_create_historical_job_state(db)
    db.commit()
    db.refresh(state)
    return {"state": historical_job_state_payload(state)}


@router.post("/admin/institutional-ingest/historical/enable", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_enable_historical_institutional_ingest(request: Request, db: Session = Depends(get_db)) -> dict[str, Any]:
    require_admin_user(db, request)
    state = set_historical_job_enabled(db, True)
    db.commit()
    db.refresh(state)
    return {"state": historical_job_state_payload(state)}


@router.post("/admin/institutional-ingest/historical/disable", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_disable_historical_institutional_ingest(request: Request, db: Session = Depends(get_db)) -> dict[str, Any]:
    require_admin_user(db, request)
    state = set_historical_job_enabled(db, False)
    db.commit()
    db.refresh(state)
    return {"state": historical_job_state_payload(state)}


@router.post("/admin/institutional-ingest/historical/run-once", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_run_historical_institutional_ingest_once(request: Request, db: Session = Depends(get_db)) -> dict[str, Any]:
    require_admin_user(db, request)
    return run_historical_backfill_once(require_enabled=True)


@router.post("/admin/institutional-ingest/historical/set-cursor", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_set_historical_institutional_ingest_cursor(
    payload: CursorPayload,
    request: Request,
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    require_admin_user(db, request)
    state = set_historical_job_cursor(db, payload.cursor_page)
    db.commit()
    db.refresh(state)
    return {"state": historical_job_state_payload(state)}


@router.post("/admin/institutional-ingest/historical/config", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_configure_historical_institutional_ingest(
    payload: HistoricalConfigPayload,
    request: Request,
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    require_admin_user(db, request)
    state = update_historical_job_config(
        db,
        start_year=payload.start_year,
        end_year=payload.end_year,
        holder_ciks=payload.holder_ciks,
        max_filings_per_run=payload.max_filings_per_run,
    )
    db.commit()
    db.refresh(state)
    return {"state": historical_job_state_payload(state)}
