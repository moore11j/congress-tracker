from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.auth import require_admin_user
from app.db import get_db
from app.rate_limit import rate_limit_admin_mutation
from app.services import reddit_ads_assistant as service

router = APIRouter(tags=["admin-reddit-ads-assistant"])


class RedditAdDraftPayload(BaseModel):
    campaign_objective: str = Field(max_length=120)
    audience: str = Field(max_length=120)
    geography: str = Field(max_length=120)
    custom_geography: str | None = Field(default=None, max_length=120)
    product_angle: str = Field(max_length=160)
    plan: str = Field(max_length=40)
    tone: str = Field(max_length=80)
    destination: str = Field(max_length=120)
    destination_url: str | None = Field(default=None, max_length=1200)
    ticker_symbols: list[str] = Field(default_factory=list, max_length=12)
    research_urls: list[str] = Field(default_factory=list, max_length=10)
    creative_reference: dict[str, Any] | None = None
    generate: bool = True


class RedditAdDraftPatchPayload(BaseModel):
    headline: str | None = Field(default=None, max_length=300)
    primary_text: str | None = Field(default=None, max_length=1400)
    short_description: str | None = Field(default=None, max_length=300)
    cta: str | None = Field(default=None, max_length=80)
    destination_url: str | None = Field(default=None, max_length=1200)
    suggested_image_concept: str | None = Field(default=None, max_length=500)
    optional_disclosure: str | None = Field(default=None, max_length=500)
    variations: list[dict[str, Any]] | None = None


class ExtensionFillActionPayload(BaseModel):
    fields: list[str] = Field(default_factory=list, max_length=20)


@router.get("/admin/reddit-ads/options")
def admin_reddit_ads_options(request: Request, db: Session = Depends(get_db)):
    require_admin_user(db, request)
    return service.options_payload()


@router.get("/admin/reddit-ads/drafts")
def admin_reddit_ads_drafts(
    status: str | None = None,
    limit: int = 50,
    request: Request = None,
    db: Session = Depends(get_db),
):
    require_admin_user(db, request)
    return service.list_drafts(db, status=status, limit=limit)


@router.post("/admin/reddit-ads/drafts", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_reddit_ads_create_draft(payload: RedditAdDraftPayload, request: Request, db: Session = Depends(get_db)):
    admin = require_admin_user(db, request)
    data = payload.model_dump() if hasattr(payload, "model_dump") else payload.dict()
    return service.create_or_generate_draft(db, admin, data, generate=payload.generate)


@router.get("/admin/reddit-ads/drafts/{draft_id}")
def admin_reddit_ads_draft(draft_id: int, request: Request, db: Session = Depends(get_db)):
    require_admin_user(db, request)
    return service.draft_to_dict(service.get_draft(db, draft_id))


@router.patch("/admin/reddit-ads/drafts/{draft_id}", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_reddit_ads_update_draft(draft_id: int, payload: RedditAdDraftPatchPayload, request: Request, db: Session = Depends(get_db)):
    admin = require_admin_user(db, request)
    data = payload.model_dump(exclude_unset=True) if hasattr(payload, "model_dump") else payload.dict(exclude_unset=True)
    return service.update_draft(db, admin, draft_id, data)


@router.post("/admin/reddit-ads/drafts/{draft_id}/approve", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_reddit_ads_approve_draft(draft_id: int, request: Request, db: Session = Depends(get_db)):
    admin = require_admin_user(db, request)
    return service.approve_draft(db, admin, draft_id)


@router.post("/admin/reddit-ads/drafts/{draft_id}/duplicate", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_reddit_ads_duplicate_draft(draft_id: int, request: Request, db: Session = Depends(get_db)):
    admin = require_admin_user(db, request)
    return service.duplicate_draft(db, admin, draft_id)


@router.post("/admin/reddit-ads/drafts/{draft_id}/regenerate", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_reddit_ads_regenerate_draft(draft_id: int, request: Request, db: Session = Depends(get_db)):
    admin = require_admin_user(db, request)
    return service.regenerate_draft(db, admin, draft_id)


@router.post("/admin/reddit-ads/drafts/{draft_id}/archive", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_reddit_ads_archive_draft(draft_id: int, request: Request, db: Session = Depends(get_db)):
    admin = require_admin_user(db, request)
    return service.set_draft_status(db, admin, draft_id, "archived")


@router.post("/admin/reddit-ads/drafts/{draft_id}/reject", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_reddit_ads_reject_draft(draft_id: int, request: Request, db: Session = Depends(get_db)):
    admin = require_admin_user(db, request)
    return service.set_draft_status(db, admin, draft_id, "rejected")


@router.post("/admin/reddit-ads/extension-token", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_reddit_ads_extension_token(request: Request, db: Session = Depends(get_db)):
    admin = require_admin_user(db, request)
    return service.create_extension_token(admin)


@router.get("/extension/reddit-ads/drafts")
def extension_reddit_ads_drafts(request: Request, db: Session = Depends(get_db)):
    service.extension_admin_from_request(db, request)
    return service.approved_extension_drafts(db)


@router.post("/extension/reddit-ads/drafts/{draft_id}/fill-action")
def extension_reddit_ads_fill_action(draft_id: int, payload: ExtensionFillActionPayload, request: Request, db: Session = Depends(get_db)):
    admin = service.extension_admin_from_request(db, request)
    data = payload.model_dump() if hasattr(payload, "model_dump") else payload.dict()
    return service.log_fill_action(db, admin, draft_id, data)
