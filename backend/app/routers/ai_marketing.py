from __future__ import annotations

import html
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
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
    apply_email_action,
    archive_opportunity,
    campaign_to_dict,
    campaign_to_dict_with_runs,
    clear_ai_growth_draft_history,
    config_status,
    create_campaign,
    create_growth_draft,
    create_manual_opportunity,
    delete_campaign,
    generate_suggestion,
    latest_suggestions_by_opportunity,
    mark_opportunity_copied,
    mark_opportunity_opened,
    mark_opportunity_posted,
    opportunity_to_dict,
    process_postmark_ai_growth_inbound,
    preview_digest,
    public_settings_payload,
    reject_opportunity,
    regenerate_growth_draft,
    run_campaign,
    send_draft_email,
    send_digest,
    suggestion_to_dict,
    test_openai_connection,
    test_reddit_connection,
    update_campaign,
    update_opportunity_status,
    update_settings,
    x_account_status,
)

router = APIRouter(tags=["admin-ai-marketing"])


class CampaignPayload(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    enabled: bool = True
    mode: str = Field(max_length=80)
    campaign_type: str | None = Field(default=None, max_length=80)
    content_type: str | None = Field(default=None, max_length=80)
    status: str | None = Field(default=None, max_length=40)
    schedule_config: dict[str, Any] = Field(default_factory=dict)
    weekdays_only: bool = True
    run_time: str | None = Field(default=None, max_length=20)
    timezone: str = Field(default="America/Los_Angeles", max_length=80)
    recipient_email: str | None = Field(default=None, max_length=240)
    source_type: str | None = Field(default=None, max_length=80)
    source_reference_id: str | None = Field(default=None, max_length=200)
    filters: dict[str, Any] = Field(default_factory=dict)
    output_preferences: dict[str, Any] = Field(default_factory=dict)
    platforms: list[str] = Field(default_factory=lambda: ["reddit"])
    keywords: list[str] = Field(default_factory=list)
    tickers: list[str] = Field(default_factory=list)
    subreddits: list[str] = Field(default_factory=list)
    query_templates: list[str] = Field(default_factory=list)
    minimum_relevance_score: int = Field(default=60, ge=0, le=100)
    max_items_per_run: int = Field(default=10, ge=1, le=50)
    max_drafts_per_day: int = Field(default=1, ge=1, le=10)
    recency: str = Field(default="week", max_length=20)
    default_destination_page: str = Field(default="https://walnutmarkets.com", max_length=1000)
    include_disclosure: bool = True
    scheduled_digest_enabled: bool = False


class CampaignPatchPayload(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=200)
    enabled: bool | None = None
    mode: str | None = Field(default=None, max_length=80)
    campaign_type: str | None = Field(default=None, max_length=80)
    content_type: str | None = Field(default=None, max_length=80)
    status: str | None = Field(default=None, max_length=40)
    schedule_config: dict[str, Any] | None = None
    weekdays_only: bool | None = None
    run_time: str | None = Field(default=None, max_length=20)
    timezone: str | None = Field(default=None, max_length=80)
    recipient_email: str | None = Field(default=None, max_length=240)
    source_type: str | None = Field(default=None, max_length=80)
    source_reference_id: str | None = Field(default=None, max_length=200)
    filters: dict[str, Any] | None = None
    output_preferences: dict[str, Any] | None = None
    platforms: list[str] | None = None
    keywords: list[str] | None = None
    tickers: list[str] | None = None
    subreddits: list[str] | None = None
    query_templates: list[str] | None = None
    minimum_relevance_score: int | None = Field(default=None, ge=0, le=100)
    max_items_per_run: int | None = Field(default=None, ge=1, le=50)
    max_drafts_per_day: int | None = Field(default=None, ge=1, le=10)
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
    source_platform: str | None = Field(default=None, max_length=40)
    ticker_theme: str | None = Field(default=None, max_length=240)
    desired_output_type: str | None = Field(default=None, max_length=80)
    destination_url: str | None = Field(default=None, max_length=1200)
    campaign_type: str | None = Field(default=None, max_length=80)
    content_type: str | None = Field(default=None, max_length=80)
    campaign_id: int | None = None
    generate: bool = True


class GrowthAssetPayload(BaseModel):
    title: str | None = Field(default=None, max_length=200)
    asset_type: str | None = Field(default=None, max_length=40)
    url: str | None = Field(default=None, max_length=1200)
    path: str | None = Field(default=None, max_length=1200)
    reference: str | None = Field(default=None, max_length=1200)
    thumbnail_url: str | None = Field(default=None, max_length=1200)
    suggested_caption: str | None = Field(default=None, max_length=1000)
    source_data_notes: str | None = Field(default=None, max_length=1000)


class GrowthDraftPayload(BaseModel):
    campaign_type: str = Field(max_length=80)
    content_type: str | None = Field(default=None, max_length=80)
    source_platform: str | None = Field(default=None, max_length=40)
    title: str | None = Field(default=None, max_length=300)
    text: str | None = Field(default=None, max_length=6000)
    ticker_theme: str | None = Field(default=None, max_length=240)
    destination_url: str | None = Field(default=None, max_length=1200)
    audience: str | None = Field(default=None, max_length=500)
    tone: str | None = Field(default=None, max_length=80)
    assets: list[GrowthAssetPayload] = Field(default_factory=list)
    inputs: dict[str, Any] = Field(default_factory=dict)
    generate: bool = True


class GrowthDraftRegeneratePayload(BaseModel):
    change_request: str | None = Field(default=None, max_length=1000)


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


def _email_action_result_page(result: dict[str, Any], *, status_code: int = 200) -> HTMLResponse:
    status = str(result.get("status") or "updated").replace("_", " ")
    draft = result.get("draft") if isinstance(result.get("draft"), dict) else {}
    title = str(draft.get("title") or "AI Growth draft")
    draft_status = str(draft.get("status") or status)
    content_type = str(draft.get("content_type") or "")
    metadata = draft.get("metadata") if isinstance(draft.get("metadata"), dict) else {}
    posting = result.get("posting") if isinstance(result.get("posting"), dict) else {}
    posting_links = draft.get("posting_links") if isinstance(draft.get("posting_links"), dict) else {}
    x_compose = str(posting_links.get("open_x_compose") or "").strip()
    x_post_url = str(posting.get("x_post_url") or metadata.get("x_post_url") or "").strip()
    admin_url = "/admin/ai-marketing"
    if posting.get("ok"):
        heading = "Posted to X"
        message = "Walnut approved this draft and posted it to X."
        next_step = "Open the post to confirm how it appears publicly."
    elif posting.get("attempted"):
        heading = "Approved, but X post failed"
        message = str(posting.get("reason") or "Walnut approved the draft, but X rejected the posting request.")
        next_step = "Check the X token, app permissions, and tweet.write scope, then post from Walnut Admin or X Compose."
    elif status == "approved" and content_type == "x_post":
        heading = "Draft approved"
        message = "This draft was approved in Walnut, but it was not posted to X."
        next_step = str(posting.get("reason") or "Configure X_ACCESS_TOKEN with tweet.write scope, then approve a new draft to post automatically.")
    elif status == "approved":
        heading = "Draft approved"
        message = "This draft was approved in Walnut."
        next_step = "No X post was created because this draft is not an X post."
    elif "regeneration" in status:
        heading = "Regeneration requested"
        message = "Walnut recorded the rejection and requested a replacement draft."
        next_step = "A revised draft will appear in the AI Growth queue."
    elif status == "rejected":
        heading = "Draft rejected"
        message = "Walnut marked this draft as rejected."
        next_step = "No post was created."
    else:
        heading = "Draft updated"
        message = f"Walnut recorded the action: {status}."
        next_step = "Review the AI Growth queue for the current state."

    compose_button = (
        f'<a class="button primary" href="{html.escape(x_compose, quote=True)}">Open X Compose</a>'
        if x_compose
        else ""
    )
    post_button = (
        f'<a class="button primary" href="{html.escape(x_post_url, quote=True)}">Open X Post</a>'
        if x_post_url
        else ""
    )
    content = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html.escape(heading)}</title>
  <style>
    :root {{ color-scheme: dark; }}
    body {{
      margin: 0;
      min-height: 100vh;
      display: grid;
      place-items: center;
      background: #0f172a;
      color: #e2e8f0;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      line-height: 1.5;
    }}
    main {{
      width: min(92vw, 620px);
      padding: 28px;
      border: 1px solid rgba(148, 163, 184, 0.24);
      border-radius: 10px;
      background: #111827;
      box-shadow: 0 24px 80px rgba(0, 0, 0, 0.35);
    }}
    h1 {{ margin: 0 0 12px; font-size: 28px; line-height: 1.15; }}
    p {{ margin: 10px 0; color: #cbd5e1; }}
    .meta {{
      margin-top: 18px;
      padding: 12px;
      border-radius: 8px;
      background: rgba(15, 23, 42, 0.75);
      color: #94a3b8;
      font-size: 14px;
    }}
    .actions {{ display: flex; flex-wrap: wrap; gap: 10px; margin-top: 22px; }}
    .button {{
      display: inline-block;
      padding: 10px 13px;
      border-radius: 7px;
      border: 1px solid rgba(148, 163, 184, 0.25);
      color: #e2e8f0;
      text-decoration: none;
      font-weight: 700;
    }}
    .primary {{ background: #e2e8f0; color: #0f172a; }}
  </style>
</head>
<body>
  <main>
    <h1>{html.escape(heading)}</h1>
    <p>{html.escape(message)}</p>
    <p>{html.escape(next_step)}</p>
    <div class="meta">
      <div><strong>Draft:</strong> {html.escape(title)}</div>
      <div><strong>Status:</strong> {html.escape(draft_status.replace("_", " "))}</div>
    </div>
    <div class="actions">
      {post_button}
      {compose_button}
      <a class="button" href="{html.escape(admin_url, quote=True)}">Open Walnut Admin</a>
    </div>
  </main>
</body>
</html>"""
    return HTMLResponse(content=content, status_code=status_code, headers={"Cache-Control": "no-store"})


def _email_action_error_page(message: str, *, status_code: int) -> HTMLResponse:
    safe_message = str(message or "Unable to apply this action.")
    return HTMLResponse(
        content=f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Action not applied</title>
  <style>
    body {{ margin:0; min-height:100vh; display:grid; place-items:center; background:#0f172a; color:#e2e8f0; font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif; }}
    main {{ width:min(92vw,560px); padding:28px; border:1px solid rgba(248,113,113,.35); border-radius:10px; background:#111827; }}
    h1 {{ margin:0 0 12px; font-size:26px; }}
    p {{ color:#cbd5e1; line-height:1.5; }}
    a {{ color:#bbf7d0; font-weight:700; }}
  </style>
</head>
<body>
  <main>
    <h1>Action not applied</h1>
    <p>{html.escape(safe_message)}</p>
    <p><a href="/admin/ai-marketing">Open Walnut Admin</a></p>
  </main>
</body>
</html>""",
        status_code=status_code,
        headers={"Cache-Control": "no-store"},
    )


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
        "items": [campaign_to_dict_with_runs(db, campaign) for campaign in campaigns],
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


@router.delete("/admin/ai-marketing/campaigns/{campaign_id}", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_ai_marketing_delete_campaign(
    campaign_id: int,
    request: Request,
    db: Session = Depends(get_db),
):
    require_admin_user(db, request)
    campaign = _campaign_or_404(db, campaign_id)
    delete_campaign(db, campaign)
    return {"ok": True, "id": campaign_id}


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


@router.get("/admin/ai-growth/drafts")
def admin_ai_growth_drafts(
    request: Request,
    db: Session = Depends(get_db),
    status: str | None = Query(default=None),
    campaign_id: int | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
):
    requested_status = str(status or "").strip().lower()
    if not requested_status or requested_status == "all":
        status = ",".join(sorted(OPPORTUNITY_STATUSES - {"dismissed"}))
    if not isinstance(campaign_id, int):
        campaign_id = None
    return admin_ai_marketing_opportunities(request, db, status=status, campaign_id=campaign_id, limit=limit)


@router.post("/admin/ai-growth/drafts", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_ai_growth_create_draft(
    payload: GrowthDraftPayload,
    request: Request,
    db: Session = Depends(get_db),
):
    require_admin_user(db, request)
    try:
        return create_growth_draft(
            db,
            campaign_type=payload.campaign_type,
            content_type=payload.content_type,
            source_platform=payload.source_platform,
            title=payload.title,
            text=payload.text,
            ticker_theme=payload.ticker_theme,
            destination_url=payload.destination_url,
            audience=payload.audience,
            tone=payload.tone,
            assets=[_payload_dict(asset) for asset in payload.assets],
            inputs=payload.inputs,
            generate=payload.generate,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.post("/admin/ai-growth/drafts/clear-history", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_ai_growth_clear_draft_history(
    request: Request,
    db: Session = Depends(get_db),
):
    require_admin_user(db, request)
    return clear_ai_growth_draft_history(db)


@router.get("/admin/ai-growth/drafts/{draft_id}")
def admin_ai_growth_draft_detail(
    draft_id: int,
    request: Request,
    db: Session = Depends(get_db),
):
    require_admin_user(db, request)
    opportunity = _opportunity_or_404(db, draft_id)
    latest = latest_suggestions_by_opportunity(db, [opportunity.id]).get(opportunity.id)
    return opportunity_to_dict(opportunity, suggestion=latest)


@router.patch("/admin/ai-growth/drafts/{draft_id}", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_ai_growth_update_draft(
    draft_id: int,
    payload: OpportunityPatchPayload,
    request: Request,
    db: Session = Depends(get_db),
):
    return admin_ai_marketing_update_opportunity(draft_id, payload, request, db)


@router.post("/admin/ai-growth/drafts/{draft_id}/regenerate", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_ai_growth_regenerate_draft(
    draft_id: int,
    payload: GrowthDraftRegeneratePayload,
    request: Request,
    db: Session = Depends(get_db),
):
    require_admin_user(db, request)
    opportunity = _opportunity_or_404(db, draft_id)
    try:
        return regenerate_growth_draft(db, opportunity, change_request=payload.change_request)
    except MissingMarketingCredential as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except OpenAISuggestionError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.admin_message) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.post("/admin/ai-growth/drafts/{draft_id}/email", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_ai_growth_email_draft(
    draft_id: int,
    request: Request,
    db: Session = Depends(get_db),
):
    admin = require_admin_user(db, request)
    opportunity = _opportunity_or_404(db, draft_id)
    return send_draft_email(db, opportunity, admin_user_id=admin.id)


@router.post("/admin/ai-growth/drafts/{draft_id}/mark-copied", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_ai_growth_mark_copied(
    draft_id: int,
    request: Request,
    db: Session = Depends(get_db),
):
    require_admin_user(db, request)
    opportunity = mark_opportunity_copied(db, _opportunity_or_404(db, draft_id))
    latest = latest_suggestions_by_opportunity(db, [opportunity.id]).get(opportunity.id)
    return opportunity_to_dict(opportunity, suggestion=latest)


@router.post("/admin/ai-growth/drafts/{draft_id}/mark-posted", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_ai_growth_mark_posted(
    draft_id: int,
    request: Request,
    db: Session = Depends(get_db),
):
    require_admin_user(db, request)
    opportunity = mark_opportunity_posted(db, _opportunity_or_404(db, draft_id))
    latest = latest_suggestions_by_opportunity(db, [opportunity.id]).get(opportunity.id)
    return opportunity_to_dict(opportunity, suggestion=latest)


@router.post("/admin/ai-growth/drafts/{draft_id}/mark-opened", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_ai_growth_mark_opened(
    draft_id: int,
    request: Request,
    db: Session = Depends(get_db),
):
    require_admin_user(db, request)
    opportunity = mark_opportunity_opened(db, _opportunity_or_404(db, draft_id))
    latest = latest_suggestions_by_opportunity(db, [opportunity.id]).get(opportunity.id)
    return opportunity_to_dict(opportunity, suggestion=latest)


@router.post("/admin/ai-growth/drafts/{draft_id}/archive", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_ai_growth_archive_draft(
    draft_id: int,
    request: Request,
    db: Session = Depends(get_db),
):
    require_admin_user(db, request)
    opportunity = archive_opportunity(db, _opportunity_or_404(db, draft_id))
    latest = latest_suggestions_by_opportunity(db, [opportunity.id]).get(opportunity.id)
    return opportunity_to_dict(opportunity, suggestion=latest)


@router.post("/admin/ai-growth/drafts/{draft_id}/reject", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_ai_growth_reject_draft(
    draft_id: int,
    request: Request,
    db: Session = Depends(get_db),
):
    require_admin_user(db, request)
    opportunity = reject_opportunity(db, _opportunity_or_404(db, draft_id))
    latest = latest_suggestions_by_opportunity(db, [opportunity.id]).get(opportunity.id)
    return opportunity_to_dict(opportunity, suggestion=latest)


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
            source_platform=payload.source_platform,
            ticker_theme=payload.ticker_theme,
            desired_output_type=payload.desired_output_type,
            destination_url=payload.destination_url,
            campaign_type=payload.campaign_type,
            content_type=payload.content_type,
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


@router.get("/admin/ai-growth/email-action")
def admin_ai_growth_email_action(
    token: str,
    request: Request,
    db: Session = Depends(get_db),
):
    try:
        result = apply_email_action(
            db,
            token,
            ip_address=request.client.host if request.client else None,
            user_agent=request.headers.get("user-agent"),
        )
        return _email_action_result_page(result)
    except ValueError as exc:
        return _email_action_error_page(str(exc), status_code=400)
    except MissingMarketingCredential as exc:
        return _email_action_error_page(str(exc), status_code=422)


@router.get("/admin/ai-growth/x/oauth/start")
def admin_ai_growth_x_oauth_start(request: Request, db: Session = Depends(get_db)):
    require_admin_user(db, request)
    return {
        "ok": False,
        "status": x_account_status(),
        "message": "X OAuth PKCE setup requires X_CLIENT_ID, X_CLIENT_SECRET, and X_REDIRECT_URI in server env before redirect can start.",
        "required_scopes": ["tweet.read", "tweet.write", "users.read", "offline.access"],
    }


@router.get("/admin/ai-growth/x/oauth/callback")
def admin_ai_growth_x_oauth_callback(request: Request, db: Session = Depends(get_db)):
    require_admin_user(db, request)
    return {"ok": False, "message": "X OAuth callback is reserved for the encrypted token exchange flow; no token data is exposed."}


@router.get("/admin/ai-growth/x/status")
def admin_ai_growth_x_status(request: Request, db: Session = Depends(get_db)):
    require_admin_user(db, request)
    return x_account_status()


@router.post("/admin/ai-growth/x/disconnect", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_ai_growth_x_disconnect(request: Request, db: Session = Depends(get_db)):
    require_admin_user(db, request)
    return {"ok": False, "message": "X disconnect requires removing encrypted/server-side X credentials; no tokens are exposed here."}


@router.post("/admin/ai-growth/x/test", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_ai_growth_x_test(request: Request, db: Session = Depends(get_db)):
    require_admin_user(db, request)
    status = x_account_status()
    return {"ok": bool(status["connected"]), "status": status, "message": "X account connected." if status["connected"] else "X account is not connected."}


@router.post("/webhooks/postmark/inbound/ai-growth")
async def postmark_inbound_ai_growth(request: Request, db: Session = Depends(get_db)):
    try:
        payload = await request.json()
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid Postmark inbound payload.") from exc
    secret = request.headers.get("x-postmark-webhook-secret") or request.headers.get("x-ai-growth-webhook-secret")
    try:
        return process_postmark_ai_growth_inbound(db, payload, webhook_secret=secret)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
