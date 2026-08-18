from __future__ import annotations

import hashlib
import html
import json
import logging
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests
from fastapi import HTTPException, Request
from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from app.auth import sign_session_payload, verify_session_token
from app.models import RedditAdDraft, UserAccount
from app.services.ai_marketing import (
    DEFAULT_DESTINATION_URL,
    OPENAI_API_KEY,
    MissingMarketingCredential,
    OpenAISuggestionError,
    marketing_model,
    resolved_setting_value,
)
from app.services.openai_request_audit import audited_openai_request

logger = logging.getLogger(__name__)

APPROVED_WALNUT_DOMAINS = {"walnutmarkets.com", "www.walnutmarkets.com", "app.walnutmarkets.com"}
DESTINATION_PATHS = {
    "Landing page": "https://walnutmarkets.com",
    "Pricing page": "https://walnutmarkets.com/pricing",
    "Ticker page": "https://walnutmarkets.com/ticker/{ticker}",
    "Research brief": "https://walnutmarkets.com/research/{slug}",
    "Congress feed": "https://walnutmarkets.com/congress-trades",
    "Insider feed": "https://walnutmarkets.com/insider-trading-tracker",
    "Institutional page": "https://walnutmarkets.com/institutional-filings",
    "Market Pressure": "https://walnutmarkets.com/market-pressure",
    "Screener": "https://walnutmarkets.com/screener",
    "Custom Walnut URL": DEFAULT_DESTINATION_URL,
}
CAMPAIGN_OBJECTIVES = {
    "Free account registrations",
    "Premium subscriptions",
    "Pro subscriptions",
    "Retargeting",
    "Research brief promotion",
    "Ticker-page promotion",
    "Brand awareness",
}
AUDIENCES = {
    "General retail investors",
    "Active traders",
    "Long-term investors",
    "Options traders",
    "Semiconductor investors",
    "AI investors",
    "Crypto investors",
    "Congress-trading followers",
    "Insider-trading followers",
    "WallStreetBets-style audience",
    "ValueInvesting-style audience",
}
GEOGRAPHIES = {"United States", "Canada", "United Kingdom", "Custom"}
PRODUCT_ANGLES = {
    "Find stronger investment setups",
    "Avoid weaker entries",
    "Detect thesis deterioration",
    "See where multiple sources of evidence agree",
    "Track insiders and Congress",
    "See institutional positioning",
    "See options flow",
    "Research one ticker",
    "Compare two investments",
    "Monitor a watchlist",
}
PLANS = {"Free", "Premium", "Pro"}
TONES = {"Official Walnut", "WallStreetBets", "ValueInvesting"}
STATUSES = {"draft", "needs_review", "approved", "rejected", "archived"}
FORBIDDEN_CLAIM_PATTERNS = (
    r"\bguarantee[sd]?\b",
    r"\bbeat the market\b",
    r"\bwill make (you|users) money\b",
    r"\bguaranteed winner\b",
    r"\brisk[- ]?free\b",
    r"\bsure thing\b",
    r"\bprofit[s]? guaranteed\b",
)
TICKER_RE = re.compile(r"^[A-Z][A-Z0-9.-]{0,9}$")
PROMPT_VERSION = "reddit_ads_assistant_v1"
REDDIT_ADS_MANAGER_URL = "https://ads.reddit.com/"
OFFICIAL_LOGO_PATH = Path(__file__).resolve().parents[1] / "assets" / "walnut-markets-logo-lockup.png"


def options_payload() -> dict[str, Any]:
    logo = official_logo_payload()
    return {
        "campaign_objectives": sorted(CAMPAIGN_OBJECTIVES),
        "audiences": sorted(AUDIENCES),
        "geographies": sorted(GEOGRAPHIES),
        "product_angles": sorted(PRODUCT_ANGLES),
        "plans": sorted(PLANS),
        "tones": sorted(TONES),
        "destinations": list(DESTINATION_PATHS.keys()),
        "approved_walnut_domains": sorted(APPROVED_WALNUT_DOMAINS),
        "reddit_ads_manager_url": REDDIT_ADS_MANAGER_URL,
        "official_logo": logo,
    }


def official_logo_payload() -> dict[str, Any]:
    if not OFFICIAL_LOGO_PATH.exists():
        raise HTTPException(status_code=500, detail="Official Walnut logo asset is missing.")
    digest = hashlib.sha256(OFFICIAL_LOGO_PATH.read_bytes()).hexdigest()
    return {
        "path": "backend/app/assets/walnut-markets-logo-lockup.png",
        "sha256": digest,
        "extension_asset": "assets/walnut-markets-logo-lockup.png",
    }


def create_extension_token(admin: UserAccount) -> dict[str, Any]:
    expires_at = datetime.now(timezone.utc) + timedelta(hours=6)
    token = sign_session_payload(
        {
            "uid": admin.id,
            "email": admin.email,
            "scope": "reddit_ads_extension",
            "exp": int(expires_at.timestamp()),
        }
    )
    return {"token": token, "expires_at": expires_at.isoformat()}


def extension_admin_from_request(db: Session, request: Request) -> UserAccount:
    auth = request.headers.get("authorization", "")
    if not auth.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Extension token required.")
    parsed = verify_session_token(auth[7:].strip())
    if not parsed or parsed.get("scope") != "reddit_ads_extension":
        raise HTTPException(status_code=401, detail="Invalid or expired extension token.")
    user = db.get(UserAccount, parsed.get("uid")) if isinstance(parsed.get("uid"), int) else None
    if not user or user.role != "admin" or user.deleted_at is not None or user.is_suspended:
        raise HTTPException(status_code=403, detail="Admin extension access required.")
    return user


def list_drafts(db: Session, *, status: str | None = None, limit: int = 50) -> dict[str, Any]:
    query = select(RedditAdDraft).order_by(desc(RedditAdDraft.created_at)).limit(max(1, min(limit, 100)))
    if status and status != "all":
        query = query.where(RedditAdDraft.status == status)
    return {"items": [draft_to_dict(row) for row in db.execute(query).scalars().all()]}


def get_draft(db: Session, draft_id: int) -> RedditAdDraft:
    draft = db.get(RedditAdDraft, draft_id)
    if draft is None:
        raise HTTPException(status_code=404, detail="Reddit ad draft not found.")
    return draft


def create_or_generate_draft(db: Session, admin: UserAccount, payload: dict[str, Any], *, generate: bool = True) -> dict[str, Any]:
    inputs = normalize_inputs(payload)
    generated: dict[str, Any]
    raw: dict[str, Any] = {}
    if generate:
        generated, raw = generate_ad_copy(db, inputs)
    else:
        generated = fallback_draft(inputs)
    warnings = compliance_warnings(generated, inputs)
    status = "needs_review" if warnings else "draft"
    now = datetime.now(timezone.utc)
    draft = RedditAdDraft(
        status=status,
        campaign_objective=inputs["campaign_objective"],
        audience=inputs["audience"],
        geography=inputs["geography"],
        product_angle=inputs["product_angle"],
        plan=inputs["plan"],
        tone=inputs["tone"],
        destination_type=inputs["destination"],
        destination_url=inputs["destination_url"],
        ticker_symbols_json=_dump(inputs["ticker_symbols"]),
        research_urls_json=_dump(inputs["research_urls"]),
        input_settings_json=_dump(inputs),
        generated_draft_json=_dump(generated),
        final_draft_json=_dump(generated),
        compliance_warnings_json=_dump(warnings),
        creative_reference_json=_dump(inputs.get("creative_reference") or official_logo_payload()),
        raw_openai_response_json=_dump(raw),
        created_by=admin.id,
        updated_by=admin.id,
        audit_log_json=_dump([audit_event("created", admin, {"generated": generate, "warnings": warnings})]),
        created_at=now,
        updated_at=now,
    )
    db.add(draft)
    db.commit()
    db.refresh(draft)
    logger.info("reddit_ads_draft_created draft_id=%s admin_id=%s status=%s", draft.id, admin.id, draft.status)
    return draft_to_dict(draft)


def update_draft(db: Session, admin: UserAccount, draft_id: int, payload: dict[str, Any]) -> dict[str, Any]:
    draft = get_draft(db, draft_id)
    if draft.status == "archived":
        raise HTTPException(status_code=409, detail="Archived drafts cannot be edited.")
    final_draft = _load_object(draft.final_draft_json)
    for key in ("headline", "primary_text", "short_description", "cta", "destination_url", "suggested_image_concept", "optional_disclosure"):
        if key in payload and payload[key] is not None:
            final_draft[key] = sanitize_text(str(payload[key]), max_length=1200)
    if "variations" in payload and isinstance(payload["variations"], list):
        final_draft["variations"] = [normalize_variation(item) for item in payload["variations"][:3]]
    inputs = _load_object(draft.input_settings_json)
    if "destination_url" in final_draft:
        inputs["destination_url"] = validate_destination_url(str(final_draft["destination_url"]), inputs.get("destination") == "Custom Walnut URL")
        draft.destination_url = inputs["destination_url"]
    warnings = compliance_warnings(final_draft, inputs)
    draft.final_draft_json = _dump(final_draft)
    draft.compliance_warnings_json = _dump(warnings)
    draft.updated_by = admin.id
    draft.updated_at = datetime.now(timezone.utc)
    _append_audit(draft, audit_event("edited", admin, {"warnings": warnings}))
    db.commit()
    db.refresh(draft)
    return draft_to_dict(draft)


def approve_draft(db: Session, admin: UserAccount, draft_id: int) -> dict[str, Any]:
    draft = get_draft(db, draft_id)
    warnings = compliance_warnings(_load_object(draft.final_draft_json), _load_object(draft.input_settings_json))
    blocking = [warning for warning in warnings if warning.get("severity") == "error"]
    if blocking:
        raise HTTPException(status_code=422, detail={"message": "Resolve blocking compliance warnings before approval.", "warnings": blocking})
    now = datetime.now(timezone.utc)
    draft.status = "approved"
    draft.approved_by = admin.id
    draft.approver_email = admin.email
    draft.approved_at = now
    draft.updated_by = admin.id
    draft.updated_at = now
    draft.compliance_warnings_json = _dump(warnings)
    _append_audit(draft, audit_event("approved", admin, {"warnings": warnings}))
    db.commit()
    db.refresh(draft)
    logger.info("reddit_ads_draft_approved draft_id=%s admin_id=%s", draft.id, admin.id)
    return draft_to_dict(draft)


def set_draft_status(db: Session, admin: UserAccount, draft_id: int, status: str) -> dict[str, Any]:
    if status not in {"rejected", "archived", "needs_review", "draft"}:
        raise HTTPException(status_code=422, detail="Unsupported Reddit ad draft status.")
    draft = get_draft(db, draft_id)
    now = datetime.now(timezone.utc)
    draft.status = status
    draft.updated_by = admin.id
    draft.updated_at = now
    if status == "archived":
        draft.archived_at = now
    _append_audit(draft, audit_event(status, admin))
    db.commit()
    db.refresh(draft)
    return draft_to_dict(draft)


def duplicate_draft(db: Session, admin: UserAccount, draft_id: int) -> dict[str, Any]:
    source = get_draft(db, draft_id)
    inputs = _load_object(source.input_settings_json)
    generated = _load_object(source.final_draft_json)
    now = datetime.now(timezone.utc)
    duplicate = RedditAdDraft(
        status="draft",
        campaign_objective=source.campaign_objective,
        audience=source.audience,
        geography=source.geography,
        product_angle=source.product_angle,
        plan=source.plan,
        tone=source.tone,
        destination_type=source.destination_type,
        destination_url=source.destination_url,
        ticker_symbols_json=source.ticker_symbols_json,
        research_urls_json=source.research_urls_json,
        input_settings_json=_dump(inputs),
        generated_draft_json=_dump(generated),
        final_draft_json=_dump(generated),
        compliance_warnings_json=source.compliance_warnings_json,
        creative_reference_json=source.creative_reference_json,
        raw_openai_response_json=source.raw_openai_response_json,
        created_by=admin.id,
        updated_by=admin.id,
        audit_log_json=_dump([audit_event("duplicated", admin, {"source_id": source.id})]),
        created_at=now,
        updated_at=now,
    )
    db.add(duplicate)
    db.commit()
    db.refresh(duplicate)
    return draft_to_dict(duplicate)


def regenerate_draft(db: Session, admin: UserAccount, draft_id: int) -> dict[str, Any]:
    draft = get_draft(db, draft_id)
    if draft.status == "archived":
        raise HTTPException(status_code=409, detail="Archived drafts cannot be regenerated.")
    inputs = _load_object(draft.input_settings_json)
    generated, raw = generate_ad_copy(db, inputs)
    warnings = compliance_warnings(generated, inputs)
    draft.generated_draft_json = _dump(generated)
    draft.final_draft_json = _dump(generated)
    draft.raw_openai_response_json = _dump(raw)
    draft.compliance_warnings_json = _dump(warnings)
    draft.status = "needs_review" if warnings else "draft"
    draft.approved_by = None
    draft.approver_email = None
    draft.approved_at = None
    draft.updated_by = admin.id
    draft.updated_at = datetime.now(timezone.utc)
    _append_audit(draft, audit_event("regenerated", admin, {"warnings": warnings}))
    db.commit()
    db.refresh(draft)
    return draft_to_dict(draft)


def approved_extension_drafts(db: Session) -> dict[str, Any]:
    rows = db.execute(
        select(RedditAdDraft)
        .where(RedditAdDraft.status == "approved")
        .order_by(desc(RedditAdDraft.approved_at), desc(RedditAdDraft.created_at))
        .limit(50)
    ).scalars().all()
    return {"items": [extension_draft_payload(row) for row in rows], "reddit_ads_manager_url": REDDIT_ADS_MANAGER_URL}


def log_fill_action(db: Session, admin: UserAccount, draft_id: int, payload: dict[str, Any]) -> dict[str, Any]:
    draft = get_draft(db, draft_id)
    if draft.status != "approved":
        raise HTTPException(status_code=409, detail="Only approved drafts can be used for form assistance.")
    fields = payload.get("fields")
    if not isinstance(fields, list):
        fields = []
    safe_fields = [sanitize_text(str(item), max_length=80) for item in fields[:20]]
    _append_audit(draft, audit_event("form_fill_assisted", admin, {"fields": safe_fields, "submitted": False}))
    draft.updated_at = datetime.now(timezone.utc)
    db.commit()
    return {"ok": True, "submitted": False, "fields": safe_fields}


def normalize_inputs(payload: dict[str, Any]) -> dict[str, Any]:
    values = {
        "campaign_objective": require_choice(payload, "campaign_objective", CAMPAIGN_OBJECTIVES),
        "audience": require_choice(payload, "audience", AUDIENCES),
        "geography": require_choice(payload, "geography", GEOGRAPHIES),
        "product_angle": require_choice(payload, "product_angle", PRODUCT_ANGLES),
        "plan": require_choice(payload, "plan", PLANS),
        "tone": require_choice(payload, "tone", TONES),
        "destination": require_choice(payload, "destination", set(DESTINATION_PATHS)),
    }
    values["ticker_symbols"] = normalize_tickers(payload.get("ticker_symbols") or payload.get("tickers") or [])
    values["research_urls"] = normalize_research_urls(payload.get("research_urls") or [])
    destination_url = str(payload.get("destination_url") or "").strip()
    if not destination_url:
        destination_url = destination_url_for(values["destination"], values["ticker_symbols"], values["research_urls"])
    values["destination_url"] = validate_destination_url(destination_url, values["destination"] == "Custom Walnut URL")
    values["custom_geography"] = sanitize_text(str(payload.get("custom_geography") or ""), max_length=120)
    values["creative_reference"] = payload.get("creative_reference") if isinstance(payload.get("creative_reference"), dict) else official_logo_payload()
    return values


def generate_ad_copy(db: Session, inputs: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    api_key = resolved_setting_value(db, OPENAI_API_KEY)
    if not api_key:
        raise MissingMarketingCredential("OpenAI API key missing.")
    model = marketing_model(db)
    request_payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt()},
            {"role": "user", "content": json.dumps(inputs, sort_keys=True)},
        ],
        "store": False,
        "response_format": {"type": "json_object"},
    }
    try:
        response = audited_openai_request(
            feature="reddit_ads",
            operation="ad_copy_generation",
            method="POST",
            endpoint="https://api.openai.com/v1/chat/completions",
            payload=request_payload,
            model=str(request_payload.get("model") or "") or None,
            send=lambda: requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                json=request_payload,
                timeout=30,
            ),
        )
    except requests.RequestException as exc:
        raise OpenAISuggestionError("OpenAI ad generation request failed.", status_code=502) from exc
    if response.status_code >= 400:
        raise OpenAISuggestionError("OpenAI ad generation failed.", status_code=502)
    raw = response.json()
    try:
        content = raw["choices"][0]["message"]["content"]
        generated = json.loads(content)
    except Exception as exc:
        raise OpenAISuggestionError("OpenAI ad generation returned invalid JSON.", status_code=502) from exc
    return normalize_generated_draft(generated, inputs), raw


def fallback_draft(inputs: dict[str, Any]) -> dict[str, Any]:
    plan_line = "Make better stock decisions." if inputs["plan"] == "Premium" else "See the evidence most retail investors miss."
    if inputs["plan"] == "Free":
        plan_line = "Explore Walnut before upgrading."
    ticker = f" on {', '.join(inputs['ticker_symbols'])}" if inputs["ticker_symbols"] else ""
    headline = f"{inputs['product_angle']}{ticker}"
    primary = f"Walnut helps investors {inputs['product_angle'].lower()} with evidence across market data, filings, and risk signals."
    return normalize_generated_draft(
        {
            "headline": headline,
            "primary_text": primary,
            "short_description": plan_line,
            "cta": "Research with Walnut",
            "destination_url": inputs["destination_url"],
            "suggested_subreddit_interests": [inputs["audience"], inputs["product_angle"]],
            "suggested_image_concept": "Use an approved Walnut product screenshot with the official Walnut logo asset.",
            "optional_disclosure": "Investing involves risk. Walnut is a research tool, not investment advice.",
            "variations": [
                {"headline": headline, "primary_text": primary, "cta": "Open Walnut"},
                {"headline": "Invest with more evidence", "primary_text": primary, "cta": "Start researching"},
                {"headline": "Find stronger setups", "primary_text": primary, "cta": "Try Walnut"},
            ],
        },
        inputs,
    )


def normalize_generated_draft(value: dict[str, Any], inputs: dict[str, Any]) -> dict[str, Any]:
    variations = value.get("variations")
    if not isinstance(variations, list):
        variations = value.get("ad_variations") if isinstance(value.get("ad_variations"), list) else []
    normalized = {
        "headline": sanitize_text(str(value.get("headline") or ""), max_length=300),
        "primary_text": sanitize_text(str(value.get("primary_text") or value.get("body") or ""), max_length=1400),
        "short_description": sanitize_text(str(value.get("short_description") or ""), max_length=300),
        "cta": sanitize_text(str(value.get("cta") or value.get("cta_recommendation") or "Learn more"), max_length=80),
        "destination_url": validate_destination_url(str(value.get("destination_url") or inputs["destination_url"]), inputs["destination"] == "Custom Walnut URL"),
        "suggested_subreddit_interests": [sanitize_text(str(item), max_length=80) for item in _as_list(value.get("suggested_subreddit_interests") or value.get("keyword_themes"))[:12]],
        "suggested_image_concept": sanitize_text(str(value.get("suggested_image_concept") or ""), max_length=500),
        "optional_disclosure": sanitize_text(str(value.get("optional_disclosure") or value.get("disclosure") or ""), max_length=500),
        "variations": [normalize_variation(item) for item in variations[:3]],
        "prompt_version": PROMPT_VERSION,
    }
    while len(normalized["variations"]) < 3:
        normalized["variations"].append(
            normalize_variation(
                {
                    "headline": normalized["headline"],
                    "primary_text": normalized["primary_text"],
                    "cta": normalized["cta"],
                }
            )
        )
    return normalized


def normalize_variation(item: Any) -> dict[str, str]:
    value = item if isinstance(item, dict) else {}
    return {
        "headline": sanitize_text(str(value.get("headline") or ""), max_length=300),
        "primary_text": sanitize_text(str(value.get("primary_text") or value.get("body") or ""), max_length=1400),
        "cta": sanitize_text(str(value.get("cta") or "Learn more"), max_length=80),
    }


def compliance_warnings(draft: dict[str, Any], inputs: dict[str, Any]) -> list[dict[str, str]]:
    warnings: list[dict[str, str]] = []
    text = " ".join(str(draft.get(key) or "") for key in ("headline", "primary_text", "short_description", "optional_disclosure"))
    for pattern in FORBIDDEN_CLAIM_PATTERNS:
        if re.search(pattern, text, flags=re.IGNORECASE):
            warnings.append({"status": "Claim requires evidence", "severity": "error", "message": "Remove guaranteed-return or unsupported performance language."})
            break
    if not str(draft.get("destination_url") or "").strip():
        warnings.append({"status": "Missing destination URL", "severity": "error", "message": "Choose a destination before approval."})
    else:
        try:
            validate_destination_url(str(draft["destination_url"]), inputs.get("destination") == "Custom Walnut URL")
        except HTTPException as exc:
            warnings.append({"status": "Missing destination URL", "severity": "error", "message": str(exc.detail)})
    if len(str(draft.get("headline") or "")) > 300 or len(str(draft.get("primary_text") or "")) > 1400:
        warnings.append({"status": "Text too long", "severity": "warning", "message": "Copy exceeds the conservative Reddit ad preview length."})
    invalid_tickers = [ticker for ticker in inputs.get("ticker_symbols", []) if not TICKER_RE.match(ticker)]
    if invalid_tickers:
        warnings.append({"status": "Unsupported ticker", "severity": "error", "message": f"Unsupported ticker input: {', '.join(invalid_tickers)}"})
    creative = inputs.get("creative_reference") if isinstance(inputs.get("creative_reference"), dict) else {}
    if creative.get("status") == "unapproved":
        warnings.append({"status": "Unapproved image", "severity": "error", "message": "Use an approved Walnut creative before approval."})
    return warnings


def draft_to_dict(draft: RedditAdDraft) -> dict[str, Any]:
    return {
        "id": draft.id,
        "status": draft.status,
        "campaign_objective": draft.campaign_objective,
        "audience": draft.audience,
        "geography": draft.geography,
        "product_angle": draft.product_angle,
        "plan": draft.plan,
        "tone": draft.tone,
        "destination": draft.destination_type,
        "destination_url": draft.destination_url,
        "ticker_symbols": _load_list(draft.ticker_symbols_json),
        "research_urls": _load_list(draft.research_urls_json),
        "input_settings": _load_object(draft.input_settings_json),
        "generated_draft": _load_object(draft.generated_draft_json),
        "final_draft": _load_object(draft.final_draft_json),
        "compliance_warnings": _load_list(draft.compliance_warnings_json),
        "creative_reference": _load_object(draft.creative_reference_json),
        "created_by": draft.created_by,
        "updated_by": draft.updated_by,
        "approved_by": draft.approved_by,
        "approver_email": draft.approver_email,
        "approved_at": _iso(draft.approved_at),
        "created_at": _iso(draft.created_at),
        "updated_at": _iso(draft.updated_at),
        "archived_at": _iso(draft.archived_at),
        "audit_log": _load_list(draft.audit_log_json),
        "reddit_ads_manager_url": REDDIT_ADS_MANAGER_URL,
        "extension_open_url": f"walnut-reddit-ads-assistant://draft/{draft.id}",
    }


def extension_draft_payload(draft: RedditAdDraft) -> dict[str, Any]:
    value = draft_to_dict(draft)
    return {
        "id": value["id"],
        "approved_at": value["approved_at"],
        "approver_email": value["approver_email"],
        "campaign_objective": value["campaign_objective"],
        "audience": value["audience"],
        "geography": value["geography"],
        "plan": value["plan"],
        "tone": value["tone"],
        "destination_url": value["destination_url"],
        "final_draft": value["final_draft"],
        "compliance_warnings": value["compliance_warnings"],
        "creative_reference": value["creative_reference"],
    }


def require_choice(payload: dict[str, Any], key: str, allowed: set[str]) -> str:
    value = sanitize_text(str(payload.get(key) or ""), max_length=200)
    if value not in allowed:
        raise HTTPException(status_code=422, detail=f"Unsupported {key.replace('_', ' ')}.")
    return value


def normalize_tickers(values: Any) -> list[str]:
    raw = values if isinstance(values, list) else re.split(r"[\s,]+", str(values or ""))
    tickers: list[str] = []
    for item in raw:
        ticker = str(item or "").strip().upper().replace("$", "")
        if ticker and ticker not in tickers:
            tickers.append(ticker)
    return tickers[:12]


def normalize_research_urls(values: Any) -> list[str]:
    raw = values if isinstance(values, list) else [values]
    urls: list[str] = []
    for item in raw:
        url = str(item or "").strip()
        if not url:
            continue
        parsed = urlparse(url)
        if parsed.scheme != "https":
            raise HTTPException(status_code=422, detail="Research URLs must use HTTPS.")
        urls.append(url)
    return urls[:10]


def destination_url_for(destination: str, tickers: list[str], research_urls: list[str]) -> str:
    template = DESTINATION_PATHS.get(destination) or DEFAULT_DESTINATION_URL
    ticker = tickers[0] if tickers else "NVDA"
    if destination == "Research brief" and research_urls:
        return research_urls[0]
    return template.format(ticker=ticker, slug="ai-earnings-dd")


def validate_destination_url(url: str, custom_selected: bool) -> str:
    candidate = url.strip()
    parsed = urlparse(candidate)
    if parsed.scheme != "https" or not parsed.netloc:
        raise HTTPException(status_code=422, detail="Destination URL must be a valid HTTPS URL.")
    domain = parsed.netloc.lower().split(":", 1)[0]
    if not custom_selected and domain not in APPROVED_WALNUT_DOMAINS:
        raise HTTPException(status_code=422, detail="Destination URL must use an approved Walnut domain.")
    return candidate


def sanitize_text(value: str, *, max_length: int) -> str:
    cleaned = html.unescape(value or "")
    cleaned = re.sub(r"<[^>]*>", "", cleaned)
    cleaned = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned[:max_length]


def audit_event(action: str, admin: UserAccount, metadata: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "action": action,
        "admin_id": admin.id,
        "admin_email": admin.email,
        "at": datetime.now(timezone.utc).isoformat(),
        "metadata": metadata or {},
    }


def _append_audit(draft: RedditAdDraft, event: dict[str, Any]) -> None:
    events = _load_list(draft.audit_log_json)
    events.append(event)
    draft.audit_log_json = _dump(events[-100:])


def system_prompt() -> str:
    return """Generate compliant Reddit ad copy for Walnut Markets. Return JSON only with headline, primary_text, short_description, cta, destination_url, suggested_subreddit_interests, suggested_image_concept, optional_disclosure, and exactly three variations. Never guarantee returns, promise profits, fabricate statistics, impersonate retail investors, or imply automatic trading performance. Use Hook -> unexpected data or insight -> judgment -> tension/question where appropriate. For direct product ads, the final element may be a clear CTA. Premium positioning: Make better stock decisions. Pro positioning: See the evidence most retail investors miss. Keep the confirmation score separate from source data categories."""


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _dump(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _load_object(value: str | None) -> dict[str, Any]:
    try:
        parsed = json.loads(value or "{}")
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _load_list(value: str | None) -> list[Any]:
    try:
        parsed = json.loads(value or "[]")
    except Exception:
        return []
    return parsed if isinstance(parsed, list) else []


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value else None
