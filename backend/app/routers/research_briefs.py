from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query, Request, Response, status
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.auth import require_admin_user
from app.db import get_db
from app.rate_limit import rate_limit_admin_mutation
from app.services.research_briefs import (
    DEFAULT_SECTIONS,
    ANGLE_OPTIONS,
    AUDIENCE_OPTIONS,
    EXTERNAL_RESEARCH_MODE_OPTIONS,
    JUDGMENT_OPTIONS,
    LENGTH_OPTIONS,
    SECTION_FORMAT_OPTIONS,
    TIME_HORIZON_OPTIONS,
    TONE_OPTIONS,
    assemble_research_context,
    delete_draft,
    enqueue_research_brief_generation_job,
    get_research_brief_generation_job,
    get_research_brief_generation_job_draft,
    get_draft,
    list_drafts,
    normalize_supported_symbol,
    publish_draft,
    published_article,
    published_cards,
    refresh_research_sources,
    research_brief_model_descriptions,
    research_brief_model_labels,
    research_brief_model_options,
    research_brief_model,
    unpublish_draft,
    update_draft,
    validate_config,
)

router = APIRouter(tags=["admin-research-briefs"])


class ResearchBriefGeneratePayload(BaseModel):
    ticker: str = Field(min_length=1, max_length=20)
    research_question: str = Field(min_length=12, max_length=3000)
    desired_angle: str = "Full company DD"
    comparison_ticker: str | None = Field(default=None, max_length=100)
    comparison_tickers: list[str] = Field(default_factory=list)
    time_horizon: str = "Near term"
    intended_audience: str = "Walnut Research Brief"
    judgment_preference: str = "Let the data decide"
    additional_context: str | None = Field(default=None, max_length=4000)
    include_sections: list[str] = Field(default_factory=lambda: list(DEFAULT_SECTIONS))
    length: str = "Standard: 1,500-2,500 words"
    tone: str = "Walnut market-native"
    external_research_mode: str = "Standard"
    section_format: str = "Walnut Research Brief"
    selected_model: str | None = Field(default=None, max_length=120)
    include_charts: bool = False
    include_source_links: bool = True
    generate_thumbnail: bool = True
    hero_image: str | None = Field(default=None, max_length=1000)
    client_request_id: str | None = Field(default=None, max_length=120)


class ResearchBriefUpdatePayload(BaseModel):
    status: str | None = Field(default=None, max_length=40)
    article: dict[str, Any] = Field(default_factory=dict)


class ConfirmPayload(BaseModel):
    confirm: bool = False
    confirm_text: str | None = None


@router.get("/admin/research-briefs/options")
def admin_research_brief_options(request: Request, db: Session = Depends(get_db)):
    require_admin_user(db, request)
    return {
        "angles": sorted(ANGLE_OPTIONS),
        "time_horizons": sorted(TIME_HORIZON_OPTIONS),
        "audiences": sorted(AUDIENCE_OPTIONS),
        "judgment_preferences": sorted(JUDGMENT_OPTIONS),
        "lengths": sorted(LENGTH_OPTIONS),
        "tones": sorted(TONE_OPTIONS),
        "external_research_modes": sorted(EXTERNAL_RESEARCH_MODE_OPTIONS),
        "section_formats": list(SECTION_FORMAT_OPTIONS),
        "model_options": research_brief_model_options(db),
        "model_default": research_brief_model(db),
        "model_descriptions": research_brief_model_descriptions(db),
        "model_labels": research_brief_model_labels(db),
        "sections": list(DEFAULT_SECTIONS),
        "publication_default": "draft",
        "storage": "local_json",
    }


@router.get("/admin/research-briefs/validate-ticker")
def admin_research_brief_validate_ticker(
    symbol: str = Query(..., min_length=1, max_length=20),
    request: Request = None,
    db: Session = Depends(get_db),
):
    require_admin_user(db, request)
    normalized, identity = normalize_supported_symbol(db, symbol)
    return {"symbol": normalized, "identity": identity}


@router.post("/admin/research-briefs/context", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_research_brief_context(payload: ResearchBriefGeneratePayload, request: Request, db: Session = Depends(get_db)):
    require_admin_user(db, request)
    config = validate_config(payload.model_dump())
    return {"config": config, "research_context": assemble_research_context(db, config)}


@router.post("/admin/research-briefs/generate", status_code=status.HTTP_202_ACCEPTED, dependencies=[Depends(rate_limit_admin_mutation)])
def admin_research_brief_generate(payload: ResearchBriefGeneratePayload, request: Request, response: Response, db: Session = Depends(get_db)):
    admin = require_admin_user(db, request)
    response.status_code = status.HTTP_202_ACCEPTED
    return enqueue_research_brief_generation_job(db, admin, payload.model_dump())


@router.get("/admin/research-briefs/jobs/{job_id}")
def admin_research_brief_job(job_id: str, request: Request, db: Session = Depends(get_db)):
    require_admin_user(db, request)
    return get_research_brief_generation_job(job_id)


@router.get("/admin/research-briefs/jobs/{job_id}/draft")
def admin_research_brief_job_draft(job_id: str, request: Request, db: Session = Depends(get_db)):
    require_admin_user(db, request)
    return get_research_brief_generation_job_draft(job_id)


@router.get("/admin/research-briefs/drafts")
def admin_research_brief_drafts(status: str | None = None, request: Request = None, db: Session = Depends(get_db)):
    require_admin_user(db, request)
    return list_drafts(status=status)


@router.get("/admin/research-briefs/drafts/{draft_id}")
def admin_research_brief_draft(draft_id: str, request: Request, db: Session = Depends(get_db)):
    require_admin_user(db, request)
    return get_draft(draft_id)


@router.patch("/admin/research-briefs/drafts/{draft_id}", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_research_brief_update(draft_id: str, payload: ResearchBriefUpdatePayload, request: Request, db: Session = Depends(get_db)):
    admin = require_admin_user(db, request)
    return update_draft(admin, draft_id, payload.article, status=payload.status)


@router.post("/admin/research-briefs/drafts/{draft_id}/refresh-sources", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_research_brief_refresh_sources(draft_id: str, request: Request, db: Session = Depends(get_db)):
    admin = require_admin_user(db, request)
    return refresh_research_sources(db, admin, draft_id)


@router.post("/admin/research-briefs/drafts/{draft_id}/publish", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_research_brief_publish(draft_id: str, payload: ConfirmPayload, request: Request, db: Session = Depends(get_db)):
    admin = require_admin_user(db, request)
    return publish_draft(admin, draft_id, confirm=payload.confirm)


@router.post("/admin/research-briefs/drafts/{draft_id}/unpublish", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_research_brief_unpublish(draft_id: str, payload: ConfirmPayload, request: Request, db: Session = Depends(get_db)):
    admin = require_admin_user(db, request)
    return unpublish_draft(admin, draft_id, confirm=payload.confirm)


@router.delete("/admin/research-briefs/drafts/{draft_id}", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_research_brief_delete(draft_id: str, payload: ConfirmPayload, request: Request, db: Session = Depends(get_db)):
    admin = require_admin_user(db, request)
    return delete_draft(admin, draft_id, confirm_text=payload.confirm_text or "")


@router.get("/research/briefs")
def public_research_brief_cards():
    return published_cards()


@router.get("/research/briefs/{slug}")
def public_research_brief(slug: str):
    return published_article(slug)
