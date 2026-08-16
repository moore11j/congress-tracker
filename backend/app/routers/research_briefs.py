from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query, Request, Response, status
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.auth import require_admin_user
from app.db import get_db
from app.entitlements import current_entitlements
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
    approve_scheduled_research_brief,
    create_research_campaign,
    delete_draft,
    delete_research_campaign,
    discover_research_keyword_opportunities,
    enqueue_research_brief_generation_job,
    get_research_brief_generation_job,
    get_research_brief_generation_job_draft,
    get_draft,
    get_research_campaign,
    list_drafts,
    list_research_campaigns,
    list_research_keyword_opportunities,
    normalize_supported_symbol,
    publish_draft,
    published_article,
    published_cards,
    refresh_research_sources,
    reject_scheduled_research_brief,
    research_campaign_themes,
    research_brief_model_descriptions,
    research_brief_model_labels,
    research_brief_model_options,
    research_brief_model,
    reschedule_research_brief,
    run_research_campaign_now,
    set_research_campaign_active,
    unpublish_draft,
    update_draft,
    update_research_keyword_opportunity_status,
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
    include_confirmation_score: bool = False
    include_cross_source_confirmations: bool = False
    premium_required: bool = False
    required_plan: str | None = Field(default=None, max_length=20)
    generate_thumbnail: bool = True
    hero_image: str | None = Field(default=None, max_length=1000)
    manual_source_url: str | None = Field(default=None, max_length=1000)
    target_keyword: str | None = Field(default=None, max_length=240)
    secondary_keywords: list[str] = Field(default_factory=list, max_length=12)
    search_intent: str | None = Field(default=None, max_length=240)
    content_type: str | None = Field(default=None, max_length=80)
    client_request_id: str | None = Field(default=None, max_length=120)


class ResearchBriefUpdatePayload(BaseModel):
    status: str | None = Field(default=None, max_length=40)
    article: dict[str, Any] = Field(default_factory=dict)
    config: dict[str, Any] = Field(default_factory=dict)


class ConfirmPayload(BaseModel):
    confirm: bool = False
    confirm_text: str | None = None


class ResearchCampaignPayload(BaseModel):
    name: str = Field(min_length=1, max_length=180)
    theme: str = Field(min_length=1, max_length=80)
    content_type: str = "ticker"
    tickers: list[str] = Field(default_factory=list)
    topic: str | None = Field(default=None, max_length=300)
    cadence: str = "one_time"
    publish_start_at: str | None = Field(default=None, max_length=80)
    publish_time: str | None = Field(default=None, max_length=20)
    article_count: int = Field(default=1, ge=1, le=50)
    window_days: int = Field(default=1, ge=1, le=30)
    active: bool = True
    target_keyword: str | None = Field(default=None, max_length=240)
    secondary_keywords: list[str] = Field(default_factory=list, max_length=12)
    search_intent: str | None = Field(default=None, max_length=240)
    target_keywords: dict[str, str] = Field(default_factory=dict)
    target_search_intents: dict[str, str] = Field(default_factory=dict)
    source_opportunity_ids: list[str] = Field(default_factory=list, max_length=50)


class ResearchKeywordDiscoveryPayload(BaseModel):
    seed_topics: list[str] = Field(default_factory=list, max_length=12)
    tickers: list[str] = Field(default_factory=list, max_length=12)
    theme: str | None = Field(default=None, max_length=80)
    max_candidates: int = Field(default=5, ge=1, le=8)


class ResearchKeywordOpportunityStatusPayload(BaseModel):
    status: str = Field(min_length=1, max_length=20)


class ActivePayload(BaseModel):
    active: bool


class ReschedulePayload(BaseModel):
    scheduled_at: str = Field(min_length=1, max_length=80)


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
        "storage": "database",
        "campaign_themes": research_campaign_themes()["items"],
    }


@router.get("/admin/research-briefs/campaign-themes")
def admin_research_campaign_themes(request: Request, db: Session = Depends(get_db)):
    require_admin_user(db, request)
    return research_campaign_themes()


@router.get("/admin/research-briefs/campaigns")
def admin_research_campaigns(request: Request, db: Session = Depends(get_db)):
    require_admin_user(db, request)
    return list_research_campaigns(db)


@router.get("/admin/research-briefs/keyword-opportunities")
def admin_research_keyword_opportunities(
    status: str | None = None,
    limit: int = Query(default=50, ge=1, le=100),
    request: Request = None,
    db: Session = Depends(get_db),
):
    require_admin_user(db, request)
    return list_research_keyword_opportunities(db, status=status, limit=limit)


@router.post("/admin/research-briefs/keyword-opportunities/discover", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_discover_research_keyword_opportunities(payload: ResearchKeywordDiscoveryPayload, request: Request, db: Session = Depends(get_db)):
    admin = require_admin_user(db, request)
    return discover_research_keyword_opportunities(db, admin, payload.model_dump())


@router.patch("/admin/research-briefs/keyword-opportunities/{opportunity_id}", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_update_research_keyword_opportunity(
    opportunity_id: str,
    payload: ResearchKeywordOpportunityStatusPayload,
    request: Request,
    db: Session = Depends(get_db),
):
    require_admin_user(db, request)
    return update_research_keyword_opportunity_status(db, opportunity_id, payload.status)


@router.get("/admin/research-briefs/health")
def admin_research_brief_health(request: Request, db: Session = Depends(get_db)):
    require_admin_user(db, request)
    from app.services.research_briefs import research_publishing_health

    return research_publishing_health(db)


@router.post("/admin/research-briefs/campaigns", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_create_research_campaign(payload: ResearchCampaignPayload, request: Request, db: Session = Depends(get_db)):
    admin = require_admin_user(db, request)
    return create_research_campaign(db, admin, payload.model_dump())


@router.get("/admin/research-briefs/campaigns/{campaign_id}")
def admin_research_campaign(campaign_id: str, request: Request, db: Session = Depends(get_db)):
    require_admin_user(db, request)
    return get_research_campaign(db, campaign_id)


@router.patch("/admin/research-briefs/campaigns/{campaign_id}/active", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_set_research_campaign_active(campaign_id: str, payload: ActivePayload, request: Request, db: Session = Depends(get_db)):
    require_admin_user(db, request)
    return set_research_campaign_active(db, campaign_id, payload.active)


@router.post("/admin/research-briefs/campaigns/{campaign_id}/run-now", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_run_research_campaign_now(campaign_id: str, request: Request, db: Session = Depends(get_db)):
    require_admin_user(db, request)
    return run_research_campaign_now(db, campaign_id)


@router.delete("/admin/research-briefs/campaigns/{campaign_id}", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_delete_research_campaign(campaign_id: str, request: Request, db: Session = Depends(get_db)):
    require_admin_user(db, request)
    return delete_research_campaign(db, campaign_id)


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
    return get_research_brief_generation_job(job_id, db)


@router.get("/admin/research-briefs/jobs/{job_id}/draft")
def admin_research_brief_job_draft(job_id: str, request: Request, db: Session = Depends(get_db)):
    require_admin_user(db, request)
    return get_research_brief_generation_job_draft(job_id, db)


@router.get("/admin/research-briefs/drafts")
def admin_research_brief_drafts(status: str | None = None, request: Request = None, db: Session = Depends(get_db)):
    require_admin_user(db, request)
    return list_drafts(status=status, db=db)


@router.get("/admin/research-briefs/drafts/{draft_id}")
def admin_research_brief_draft(draft_id: str, request: Request, db: Session = Depends(get_db)):
    require_admin_user(db, request)
    return get_draft(draft_id, db=db)


@router.patch("/admin/research-briefs/drafts/{draft_id}", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_research_brief_update(draft_id: str, payload: ResearchBriefUpdatePayload, request: Request, db: Session = Depends(get_db)):
    admin = require_admin_user(db, request)
    return update_draft(admin, draft_id, payload.article, status=payload.status, db=db, config_patch=payload.config)


@router.post("/admin/research-briefs/drafts/{draft_id}/refresh-sources", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_research_brief_refresh_sources(draft_id: str, request: Request, db: Session = Depends(get_db)):
    admin = require_admin_user(db, request)
    return refresh_research_sources(db, admin, draft_id)


@router.post("/admin/research-briefs/drafts/{draft_id}/publish", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_research_brief_publish(draft_id: str, payload: ConfirmPayload, request: Request, db: Session = Depends(get_db)):
    admin = require_admin_user(db, request)
    return publish_draft(admin, draft_id, confirm=payload.confirm, db=db)


@router.post("/admin/research-briefs/drafts/{draft_id}/publish-now", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_research_brief_publish_now(draft_id: str, payload: ConfirmPayload, request: Request, db: Session = Depends(get_db)):
    admin = require_admin_user(db, request)
    return publish_draft(admin, draft_id, confirm=payload.confirm, db=db)


@router.post("/admin/research-briefs/drafts/{draft_id}/approve-scheduled", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_research_brief_approve_scheduled(draft_id: str, request: Request, db: Session = Depends(get_db)):
    admin = require_admin_user(db, request)
    return approve_scheduled_research_brief(db, admin, draft_id)


@router.post("/admin/research-briefs/drafts/{draft_id}/reject", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_research_brief_reject(draft_id: str, request: Request, db: Session = Depends(get_db)):
    admin = require_admin_user(db, request)
    return reject_scheduled_research_brief(db, admin, draft_id)


@router.post("/admin/research-briefs/drafts/{draft_id}/reschedule", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_research_brief_reschedule(draft_id: str, payload: ReschedulePayload, request: Request, db: Session = Depends(get_db)):
    require_admin_user(db, request)
    return reschedule_research_brief(db, draft_id, payload.scheduled_at)


@router.post("/admin/research-briefs/drafts/{draft_id}/unpublish", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_research_brief_unpublish(draft_id: str, payload: ConfirmPayload, request: Request, db: Session = Depends(get_db)):
    admin = require_admin_user(db, request)
    return unpublish_draft(admin, draft_id, confirm=payload.confirm, db=db)


@router.delete("/admin/research-briefs/drafts/{draft_id}", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_research_brief_delete(draft_id: str, payload: ConfirmPayload, request: Request, db: Session = Depends(get_db)):
    admin = require_admin_user(db, request)
    return delete_draft(admin, draft_id, confirm_text=payload.confirm_text or "", db=db)


@router.get("/research/briefs")
def public_research_brief_cards(db: Session = Depends(get_db)):
    return published_cards(db=db)


@router.get("/research/briefs/{slug}")
def public_research_brief(slug: str, request: Request, db: Session = Depends(get_db)):
    return published_article(slug, db=db, entitlements=current_entitlements(request, db))
