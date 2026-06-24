from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field
from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from app.auth import require_admin_user
from app.db import get_db
from app.models import AiMarketingCampaign, AiMarketingOpportunity
from app.rate_limit import rate_limit_admin_mutation
from app.services.ai_marketing import (
    MissingMarketingCredential,
    OpenAISuggestionError,
    OPPORTUNITY_STATUSES,
    campaign_to_dict,
    config_status,
    create_campaign,
    create_manual_opportunity,
    generate_suggestion,
    latest_suggestions_by_opportunity,
    opportunity_to_dict,
    preview_digest,
    public_settings_payload,
    run_campaign,
    send_digest,
    suggestion_to_dict,
    test_openai_connection,
    test_reddit_connection,
    update_campaign,
    update_opportunity_status,
    update_settings,
)

router = APIRouter(tags=["admin-ai-marketing"])


class CampaignPayload(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    enabled: bool = True
    mode: str = Field(max_length=80)
    platforms: list[str] = Field(default_factory=lambda: ["reddit"])
    keywords: list[str] = Field(default_factory=list)
    tickers: list[str] = Field(default_factory=list)
    subreddits: list[str] = Field(default_factory=list)
    query_templates: list[str] = Field(default_factory=list)
    minimum_relevance_score: int = Field(default=60, ge=0, le=100)
    max_items_per_run: int = Field(default=10, ge=1, le=50)
    recency: str = Field(default="week", max_length=20)
    default_destination_page: str = Field(default="https://walnutmarkets.com", max_length=1000)
    include_disclosure: bool = True
    scheduled_digest_enabled: bool = False


class CampaignPatchPayload(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=200)
    enabled: bool | None = None
    mode: str | None = Field(default=None, max_length=80)
    platforms: list[str] | None = None
    keywords: list[str] | None = None
    tickers: list[str] | None = None
    subreddits: list[str] | None = None
    query_templates: list[str] | None = None
    minimum_relevance_score: int | None = Field(default=None, ge=0, le=100)
    max_items_per_run: int | None = Field(default=None, ge=1, le=50)
    recency: str | None = Field(default=None, max_length=20)
    default_destination_page: str | None = Field(default=None, max_length=1000)
    include_disclosure: bool | None = None
    scheduled_digest_enabled: bool | None = None


class OpportunityPatchPayload(BaseModel):
    status: str | None = Field(default=None, max_length=40)


class ManualUrlPayload(BaseModel):
    url: str | None = Field(default=None, max_length=1200)
    text: str | None = Field(default=None, max_length=4000)
    title: str | None = Field(default=None, max_length=300)
    campaign_id: int | None = None
    generate: bool = True


class EmailDigestPayload(BaseModel):
    send: bool = False
    opportunity_ids: list[int] | None = None
    statuses: list[str] | None = None
    limit: int = Field(default=25, ge=1, le=100)


class SettingsPatchPayload(BaseModel):
    updates: dict[str, str | None] = Field(default_factory=dict)
    clear: list[str] = Field(default_factory=list)


def _payload_dict(payload: BaseModel, *, exclude_unset: bool = False) -> dict[str, Any]:
    if hasattr(payload, "model_dump"):
        return payload.model_dump(exclude_unset=exclude_unset)
    return payload.dict(exclude_unset=exclude_unset)


def _campaign_or_404(db: Session, campaign_id: int) -> AiMarketingCampaign:
    campaign = db.get(AiMarketingCampaign, campaign_id)
    if not campaign:
        raise HTTPException(status_code=404, detail="Campaign not found.")
    return campaign


def _opportunity_or_404(db: Session, opportunity_id: int) -> AiMarketingOpportunity:
    opportunity = db.get(AiMarketingOpportunity, opportunity_id)
    if not opportunity:
        raise HTTPException(status_code=404, detail="Opportunity not found.")
    return opportunity


@router.get("/admin/ai-marketing/settings")
def admin_ai_marketing_settings(request: Request, db: Session = Depends(get_db)):
    require_admin_user(db, request)
    return public_settings_payload(db)


@router.patch("/admin/ai-marketing/settings", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_ai_marketing_update_settings(
    payload: SettingsPatchPayload,
    request: Request,
    db: Session = Depends(get_db),
):
    require_admin_user(db, request)
    try:
        return update_settings(db, updates=payload.updates, clear=payload.clear)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.post("/admin/ai-marketing/settings/test-openai", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_ai_marketing_test_openai(request: Request, db: Session = Depends(get_db)):
    require_admin_user(db, request)
    return test_openai_connection(db)


@router.post("/admin/ai-marketing/settings/test-reddit", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_ai_marketing_test_reddit(request: Request, db: Session = Depends(get_db)):
    require_admin_user(db, request)
    return test_reddit_connection(db)


@router.get("/admin/ai-marketing/campaigns")
def admin_ai_marketing_campaigns(request: Request, db: Session = Depends(get_db)):
    require_admin_user(db, request)
    campaigns = db.execute(select(AiMarketingCampaign).order_by(desc(AiMarketingCampaign.updated_at), desc(AiMarketingCampaign.id))).scalars().all()
    return {
        "items": [campaign_to_dict(campaign) for campaign in campaigns],
        "config": config_status(db),
    }


@router.post("/admin/ai-marketing/campaigns", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_ai_marketing_create_campaign(
    payload: CampaignPayload,
    request: Request,
    db: Session = Depends(get_db),
):
    require_admin_user(db, request)
    try:
        campaign = create_campaign(db, _payload_dict(payload))
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return campaign_to_dict(campaign)


@router.patch("/admin/ai-marketing/campaigns/{campaign_id}", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_ai_marketing_update_campaign(
    campaign_id: int,
    payload: CampaignPatchPayload,
    request: Request,
    db: Session = Depends(get_db),
):
    require_admin_user(db, request)
    campaign = _campaign_or_404(db, campaign_id)
    try:
        updated = update_campaign(db, campaign, _payload_dict(payload, exclude_unset=True))
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return campaign_to_dict(updated)


@router.post("/admin/ai-marketing/campaigns/{campaign_id}/run", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_ai_marketing_run_campaign(
    campaign_id: int,
    request: Request,
    db: Session = Depends(get_db),
):
    require_admin_user(db, request)
    campaign = _campaign_or_404(db, campaign_id)
    return run_campaign(db, campaign)


@router.get("/admin/ai-marketing/opportunities")
def admin_ai_marketing_opportunities(
    request: Request,
    db: Session = Depends(get_db),
    status: str | None = Query(default=None),
    campaign_id: int | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
):
    require_admin_user(db, request)
    query = select(AiMarketingOpportunity)
    if status and status.lower() != "all":
        statuses = [part.strip().lower() for part in status.split(",") if part.strip()]
        invalid = sorted(set(statuses) - OPPORTUNITY_STATUSES)
        if invalid:
            raise HTTPException(status_code=422, detail="Unsupported opportunity status.")
        query = query.where(AiMarketingOpportunity.status.in_(statuses))
    if campaign_id is not None:
        query = query.where(AiMarketingOpportunity.campaign_id == campaign_id)
    opportunities = db.execute(
        query.order_by(desc(AiMarketingOpportunity.relevance_score), desc(AiMarketingOpportunity.created_at)).limit(limit)
    ).scalars().all()
    latest = latest_suggestions_by_opportunity(db, [row.id for row in opportunities])
    return {
        "items": [opportunity_to_dict(row, suggestion=latest.get(row.id)) for row in opportunities],
        "config": config_status(db),
    }


@router.patch("/admin/ai-marketing/opportunities/{opportunity_id}", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_ai_marketing_update_opportunity(
    opportunity_id: int,
    payload: OpportunityPatchPayload,
    request: Request,
    db: Session = Depends(get_db),
):
    require_admin_user(db, request)
    opportunity = _opportunity_or_404(db, opportunity_id)
    try:
        updated = update_opportunity_status(db, opportunity, status=payload.status)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    latest = latest_suggestions_by_opportunity(db, [updated.id]).get(updated.id)
    return opportunity_to_dict(updated, suggestion=latest)


@router.post("/admin/ai-marketing/manual-url", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_ai_marketing_manual_url(
    payload: ManualUrlPayload,
    request: Request,
    db: Session = Depends(get_db),
):
    require_admin_user(db, request)
    campaign = _campaign_or_404(db, payload.campaign_id) if payload.campaign_id else None
    try:
        return create_manual_opportunity(
            db,
            url=payload.url,
            text=payload.text,
            title=payload.title,
            campaign=campaign,
            generate=payload.generate,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.post("/admin/ai-marketing/suggestions/{opportunity_id}/regenerate", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_ai_marketing_regenerate_suggestion(
    opportunity_id: int,
    request: Request,
    db: Session = Depends(get_db),
):
    require_admin_user(db, request)
    opportunity = _opportunity_or_404(db, opportunity_id)
    try:
        suggestion = generate_suggestion(db, opportunity)
    except MissingMarketingCredential as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except OpenAISuggestionError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.admin_message) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return suggestion_to_dict(suggestion)


@router.post("/admin/ai-marketing/email-digest", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_ai_marketing_email_digest(
    payload: EmailDigestPayload,
    request: Request,
    db: Session = Depends(get_db),
):
    admin = require_admin_user(db, request)
    body = _payload_dict(payload)
    if payload.send:
        return send_digest(
            db,
            opportunity_ids=body.get("opportunity_ids"),
            statuses=body.get("statuses"),
            limit=payload.limit,
            admin_user_id=admin.id,
        )
    return preview_digest(
        db,
        opportunity_ids=body.get("opportunity_ids"),
        statuses=body.get("statuses"),
        limit=payload.limit,
    )
