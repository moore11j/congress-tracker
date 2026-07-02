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
    job_state_payload,
    latest_job_status_payload,
    run_latest_ingest_job_once,
    set_latest_job_cursor,
    set_latest_job_enabled,
    update_latest_job_config,
)

router = APIRouter(tags=["admin-institutional-ingest"])


class CursorPayload(BaseModel):
    cursor_page: int = Field(ge=0, le=10_000)


class ConfigPayload(BaseModel):
    pages_per_run: int | None = Field(default=None, ge=1, le=20)
    max_filings_per_run: int | None = Field(default=None, ge=1, le=50)
    limit: int | None = Field(default=None, ge=1, le=100)


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
