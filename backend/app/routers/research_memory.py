from __future__ import annotations

from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.auth import current_user
from app.db import get_db
from app.entitlements import current_entitlements, require_feature
from app.services.research_memory import (
    activate,
    compile_custom_thesis,
    create_draft,
    ensure_enabled,
    list_active_theses_for_security,
    list_templates,
    list_user_theses,
    owned_thesis,
    resolve_security,
    research_memory_enabled,
    suggestion_draft,
    suggestions_for_security,
    template_draft,
    update_draft,
    _serialize_thesis,
)
from app.services.research_claim_matching import ensure_claim_matching_enabled, query_invalidator_matches, query_matches

router = APIRouter(prefix="/research-memory", tags=["research-memory"])


class DraftPayload(BaseModel):
    security_id: int | None = None
    ticker: str | None = Field(default=None, max_length=20)
    structure: dict[str, Any]


class StructurePayload(BaseModel):
    structure: dict[str, Any]


class CustomCompilerPayload(BaseModel):
    security_id: int | None = None
    ticker: str | None = Field(default=None, max_length=20)
    original_text: str = Field(min_length=3, max_length=6000)


def _user_and_access(db: Session, request: Request, feature: str = "view_research_memory"):
    user = current_user(db, request, required=True)
    ensure_enabled(user)
    require_feature(current_entitlements(request, db), feature, message="Research Memory is not available on this plan.")
    return user


def _security(db: Session, *, security_id: int | None, ticker: str | None):
    if security_id:
        from app.models import Security
        security = db.get(Security, security_id)
        if not security:
            from fastapi import HTTPException
            raise HTTPException(status_code=404, detail="Ticker security not found.")
        return security
    return resolve_security(db, ticker or "")


@router.get("/status")
def status(request: Request, db: Session = Depends(get_db)):
    user = current_user(db, request, required=False)
    ensure_enabled(user)
    return {"enabled": research_memory_enabled(), "phase": 1}


@router.get("")
def index(request: Request, db: Session = Depends(get_db)):
    user = _user_and_access(db, request)
    return {"items": list_user_theses(db, user=user)}


@router.get("/templates")
def templates(request: Request, db: Session = Depends(get_db)):
    _user_and_access(db, request, "create_research_memory")
    return {"items": list_templates()}


@router.get("/templates/{template_id}/draft")
def template_preview(template_id: str, ticker: str, request: Request, db: Session = Depends(get_db)):
    _user_and_access(db, request, "create_research_memory")
    security = resolve_security(db, ticker)
    return {"security_id": security.id, "ticker": security.symbol, "structure": template_draft(template_id, symbol=security.symbol or ticker, company_name=security.name)}


@router.get("/suggestions/{ticker}")
def suggestions(ticker: str, request: Request, db: Session = Depends(get_db)):
    _user_and_access(db, request, "create_research_memory")
    return suggestions_for_security(db, security=resolve_security(db, ticker))


@router.get("/suggestions/{ticker}/{suggestion_id}/draft")
def suggestion_preview(ticker: str, suggestion_id: str, request: Request, db: Session = Depends(get_db)):
    _user_and_access(db, request, "create_research_memory")
    security = resolve_security(db, ticker)
    return {"security_id": security.id, "ticker": security.symbol, "structure": suggestion_draft(db, security=security, suggestion_id=suggestion_id)}


@router.post("/compile")
def compile(payload: CustomCompilerPayload, request: Request, db: Session = Depends(get_db)):
    _user_and_access(db, request, "use_custom_thesis_ai")
    security = _security(db, security_id=payload.security_id, ticker=payload.ticker)
    return {"security_id": security.id, "ticker": security.symbol, "structure": compile_custom_thesis(db, security=security, original_text=payload.original_text)}


@router.post("/drafts")
def create(payload: DraftPayload, request: Request, db: Session = Depends(get_db)):
    user = _user_and_access(db, request, "create_research_memory")
    return create_draft(db, user=user, security=_security(db, security_id=payload.security_id, ticker=payload.ticker), structure=payload.structure)


@router.get("/ticker/{ticker}/active")
def active_for_ticker(ticker: str, request: Request, db: Session = Depends(get_db)):
    user = _user_and_access(db, request)
    security = resolve_security(db, ticker)
    items = list_active_theses_for_security(db, user=user, security_id=security.id)
    return {"ticker": security.symbol, "count": len(items), "items": [{"id": item["id"], "title": item["title"], "status": item["status"]} for item in items]}


@router.get("/{thesis_id}")
def detail(thesis_id: str, request: Request, db: Session = Depends(get_db)):
    user = _user_and_access(db, request)
    return _serialize_thesis(db, owned_thesis(db, user=user, thesis_id=thesis_id))


@router.get("/{thesis_id}/matches")
def matches(thesis_id: str, request: Request, claim_id: str | None = None, relationship: str | None = None, since: datetime | None = None, limit: int = 100, db: Session = Depends(get_db)):
    user = _user_and_access(db, request, "monitor_research_memory")
    ensure_claim_matching_enabled(is_admin=user.role == "admin")
    owned_thesis(db, user=user, thesis_id=thesis_id)
    return {"items": query_matches(db, user=user, thesis_id=thesis_id, claim_id=claim_id, relationship=relationship, since=since, limit=limit)}


@router.get("/{thesis_id}/invalidator-matches")
def invalidator_matches(thesis_id: str, request: Request, since: datetime | None = None, limit: int = 100, db: Session = Depends(get_db)):
    user = _user_and_access(db, request, "monitor_research_memory")
    ensure_claim_matching_enabled(is_admin=user.role == "admin")
    owned_thesis(db, user=user, thesis_id=thesis_id)
    return {"items": query_invalidator_matches(db, user=user, thesis_id=thesis_id, since=since, limit=limit)}


@router.put("/{thesis_id}")
def update(thesis_id: str, payload: StructurePayload, request: Request, db: Session = Depends(get_db)):
    user = _user_and_access(db, request, "create_research_memory")
    return update_draft(db, user=user, thesis_id=thesis_id, structure=payload.structure)


@router.post("/{thesis_id}/activate")
def start_monitoring(thesis_id: str, request: Request, db: Session = Depends(get_db)):
    user = _user_and_access(db, request, "monitor_research_memory")
    return activate(db, user=user, thesis_id=thesis_id)
