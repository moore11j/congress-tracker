"""Admin-only Phase 3 match processing and inspection."""
from __future__ import annotations

from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy.orm import Session

from app.auth import require_admin_user
from app.db import get_db
from app.models import ResearchEvidenceEvent
from app.services.research_claim_matching import claim_matching_enabled, ensure_claim_matching_enabled, run_claim_matching

router = APIRouter(prefix="/admin/research-claim-matches", tags=["research-claim-matches"])


def _admin(db: Session, request: Request):
    admin = require_admin_user(db, request)
    ensure_claim_matching_enabled(is_admin=True)
    return admin


@router.get("/status")
def status(request: Request, db: Session = Depends(get_db)):
    _admin(db, request)
    return {"enabled": claim_matching_enabled(), "phase": 3, "processing_is_page_triggered": False}


@router.post("/process")
def process(request: Request, evidence_event_id: str | None = None, security_id: int | None = Query(default=None, ge=1), limit: int = Query(default=100, ge=1, le=500), db: Session = Depends(get_db)):
    _admin(db, request)
    return run_claim_matching(db, evidence_event_id=evidence_event_id, security_id=security_id, limit=limit)
