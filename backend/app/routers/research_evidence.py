"""Admin-only Phase 2 verification surface; this is not a public evidence feed."""
from __future__ import annotations

from datetime import date, datetime

from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy.orm import Session

from app.auth import require_admin_user
from app.db import get_db
from app.services.research_evidence import (
    ensure_evidence_engine_enabled,
    query_events,
    research_evidence_engine_enabled,
    run_deterministic_adapters,
)

router = APIRouter(prefix="/admin/research-evidence", tags=["research-evidence"])


def _admin(db: Session, request: Request):
    admin = require_admin_user(db, request)
    ensure_evidence_engine_enabled(is_admin=True)
    return admin


@router.get("/status")
def status(request: Request, db: Session = Depends(get_db)):
    _admin(db, request)
    return {"enabled": research_evidence_engine_enabled(), "phase": 2, "processing_is_page_triggered": False}


@router.get("/events")
def events(
    request: Request,
    security_id: int = Query(ge=1),
    since: datetime | None = None,
    start: date | None = None,
    end: date | None = None,
    category: str | None = None,
    event_type: str | None = None,
    metric: str | None = None,
    source_type: str | None = None,
    limit: int = Query(default=100, ge=1, le=500),
    db: Session = Depends(get_db),
):
    _admin(db, request)
    return {"items": query_events(db, security_id=security_id, since=since, start=start, end=end, category=category, event_type=event_type, metric=metric, source_type=source_type, limit=limit)}


@router.post("/process")
def process(
    request: Request,
    security_id: int | None = Query(default=None, ge=1),
    limit: int = Query(default=200, ge=1, le=1000),
    db: Session = Depends(get_db),
):
    _admin(db, request)
    # Explicit operator action / worker-compatible entry point, never invoked by frontend render paths.
    return run_deterministic_adapters(db, security_id=security_id, limit=limit)
