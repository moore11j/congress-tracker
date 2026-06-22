from __future__ import annotations

import hashlib
import html
import json
import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import requests
from sqlalchemy import desc, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.models import (
    AiMarketingCampaign,
    AiMarketingEmailLog,
    AiMarketingOpportunity,
    AiMarketingSetting,
    AiMarketingSuggestion,
)
from app.services.email_delivery import send_email

logger = logging.getLogger(__name__)

AI_MARKETING_RECIPIENT = "jarod@walnutmarkets.com"
AI_MARKETING_TEMPLATE_KEY = "ai_marketing.digest"
AI_MARKETING_PROMPT_VERSION = "ai_marketing_v1"
DEFAULT_DESTINATION_URL = "https://walnutmarkets.com"
DEFAULT_AI_MARKETING_MODEL = "gpt-5.4-mini"
OPENAI_API_KEY = "OPENAI_API_KEY"
AI_MARKETING_MODEL = "AI_MARKETING_MODEL"
REDDIT_CLIENT_ID = "REDDIT_CLIENT_ID"
REDDIT_CLIENT_SECRET = "REDDIT_CLIENT_SECRET"
REDDIT_USER_AGENT = "REDDIT_USER_AGENT"

CAMPAIGN_MODES = {
    "ticker_thread_assist",
    "congress_trade_angle",
    "insider_buying_angle",
    "unusual_signal_angle",
    "pain_point_tool_alternative",
    "manual_url_review",
}
PLATFORMS = {"reddit", "x_stub", "facebook_manual"}
OPPORTUNITY_STATUSES = {"new", "emailed", "dismissed", "copied", "archived"}
INTENTS = {"question", "complaint", "trade_idea", "tool_search", "news_reaction", "other"}
AI_MARKETING_SETTINGS: dict[str, dict[str, Any]] = {
    OPENAI_API_KEY: {"label": "OpenAI API Key", "is_secret": True, "required_for": "OpenAI suggestions"},
    AI_MARKETING_MODEL: {"label": "AI Marketing Model", "is_secret": False, "required_for": "OpenAI suggestions"},
    REDDIT_CLIENT_ID: {"label": "Reddit Client ID", "is_secret": True, "required_for": "Reddit discovery"},
    REDDIT_CLIENT_SECRET: {"label": "Reddit Client Secret", "is_secret": True, "required_for": "Reddit discovery"},
    REDDIT_USER_AGENT: {"label": "Reddit User Agent", "is_secret": False, "required_for": "Reddit discovery"},
}
SECRET_SETTING_KEYS = {key for key, meta in AI_MARKETING_SETTINGS.items() if meta["is_secret"]}
ENV_ONLY_PROVIDER_SETTING_KEYS = frozenset(
    {
        OPENAI_API_KEY,
        AI_MARKETING_MODEL,
        REDDIT_CLIENT_ID,
        REDDIT_CLIENT_SECRET,
        REDDIT_USER_AGENT,
    }
)
PROVIDER_ENV_ONLY_MESSAGE = "Provider credentials are managed through server environment variables."
MANUAL_REDDIT_CREDENTIALS_MESSAGE = (
    "Reddit API credentials are not configured. Paste the post/comment text manually or configure Reddit API credentials."
)
MANUAL_SUBREDDIT_LISTING_MESSAGE = (
    "Manual URL mode works best with a specific post/comment URL or pasted text. "
    "Subreddit listing URLs require Reddit API discovery."
)
MANUAL_TEXT_REQUIRED_MESSAGE = "Paste the post/comment text or thread excerpt before generating a manual suggestion."
MANUAL_SOURCE_URL = "https://walnutmarkets.com/admin/ai-marketing"
OPENAI_MISSING_KEY_MESSAGE = "OpenAI API key missing. Configure OPENAI_API_KEY, then regenerate."
OPENAI_INVALID_KEY_MESSAGE = "OpenAI API key was rejected. Check the OPENAI_API_KEY server environment variable, then regenerate."
OPENAI_BILLING_CREDITS_MESSAGE = (
    "OpenAI API billing/credits are unavailable. Add credits in the OpenAI Platform billing page, then regenerate."
)
OPENAI_RATE_LIMIT_MESSAGE = "OpenAI API rate limit reached. Wait a moment, then regenerate."
OPENAI_GENERIC_SUGGESTION_MESSAGE = "OpenAI suggestion request failed. Check OpenAI status and the configured model, then regenerate."

_TICKER_PATTERN = re.compile(r"(?<![A-Za-z0-9])\$?([A-Z]{1,5})(?![A-Za-z0-9])")
_COMMON_FALSE_TICKERS = {
    "A",
    "AI",
    "API",
    "CEO",
    "CFO",
    "DD",
    "ETF",
    "GDP",
    "IPO",
    "IR",
    "SEC",
    "US",
    "USA",
    "URL",
    "HTTP",
    "HTTPS",
    "WWW",
    "COM",
    "YOLO",
}


class MissingMarketingCredential(RuntimeError):
    pass


class OpenAISuggestionError(RuntimeError):
    def __init__(self, admin_message: str, *, status_code: int = 502):
        super().__init__(admin_message)
        self.admin_message = admin_message
        self.status_code = status_code


@dataclass(frozen=True)
class SourceItem:
    platform: str
    source_id: str | None
    source_url: str
    title: str
    excerpt: str | None = None
    author: str | None = None
    community: str | None = None
    source_score: int | None = None
    comment_count: int | None = None
    source_created_at: datetime | None = None
    metadata: dict[str, Any] | None = None


def marketing_model(db: Session | None = None) -> str:
    return resolved_setting_value(db, AI_MARKETING_MODEL) or DEFAULT_AI_MARKETING_MODEL


def resolved_setting_value(db: Session | None, key: str) -> str | None:
    resolved = resolve_setting(db, key)
    return resolved["value"] if isinstance(resolved["value"], str) and resolved["value"].strip() else None


def resolve_setting(db: Session | None, key: str) -> dict[str, Any]:
    if key not in AI_MARKETING_SETTINGS:
        raise KeyError(f"Unsupported AI marketing setting: {key}")

    row = db.get(AiMarketingSetting, key) if db is not None else None
    row_value = (row.value or "").strip() if row and row.value is not None else ""
    deprecated_admin_setting = key in ENV_ONLY_PROVIDER_SETTING_KEYS and bool(row_value)
    env_value = os.getenv(key, "").strip()

    if key in ENV_ONLY_PROVIDER_SETTING_KEYS:
        if env_value:
            return {
                "key": key,
                "value": env_value,
                "source": "server_env",
                "row": row,
                "deprecated_admin_setting": deprecated_admin_setting,
            }
        if key == AI_MARKETING_MODEL:
            return {
                "key": key,
                "value": DEFAULT_AI_MARKETING_MODEL,
                "source": "default",
                "row": row,
                "deprecated_admin_setting": deprecated_admin_setting,
            }
        return {
            "key": key,
            "value": None,
            "source": "missing",
            "row": row,
            "deprecated_admin_setting": deprecated_admin_setting,
        }

    if row_value:
        return {"key": key, "value": row_value, "source": "admin_settings", "row": row}

    if env_value:
        return {"key": key, "value": env_value, "source": "server_env", "row": row}

    return {"key": key, "value": None, "source": "missing", "row": row}


def public_setting_payload(db: Session, key: str) -> dict[str, Any]:
    meta = AI_MARKETING_SETTINGS[key]
    resolved = resolve_setting(db, key)
    row = resolved["row"]
    configured = bool(resolved["value"])
    is_secret = bool(meta["is_secret"])
    env_only = key in ENV_ONLY_PROVIDER_SETTING_KEYS
    payload = {
        "key": key,
        "label": meta["label"],
        "is_secret": is_secret,
        "configured": configured,
        "source": resolved["source"],
        "source_label": _setting_source_label(resolved["source"]),
        "required_for": meta["required_for"],
        "masked_value": None if env_only else _masked_secret() if is_secret and configured else None,
        "updated_at": None if env_only else _iso(row.updated_at) if row else None,
    }
    if resolved.get("deprecated_admin_setting"):
        payload["deprecated_admin_setting"] = True
    if not is_secret and not env_only:
        payload["value"] = resolved["value"] or (DEFAULT_AI_MARKETING_MODEL if key == AI_MARKETING_MODEL else "")
    return payload


def public_settings_payload(db: Session) -> dict[str, Any]:
    return {
        "items": [public_setting_payload(db, key) for key in AI_MARKETING_SETTINGS],
        "config": config_status(db),
    }


def update_settings(
    db: Session,
    *,
    updates: dict[str, Any] | None = None,
    clear: list[str] | None = None,
) -> dict[str, Any]:
    updates = updates or {}
    clear_keys = set(clear or [])
    unknown = sorted((set(updates) | clear_keys) - set(AI_MARKETING_SETTINGS))
    if unknown:
        raise ValueError(f"Unsupported AI marketing setting: {', '.join(unknown)}.")
    env_only = sorted((set(updates) | clear_keys) & ENV_ONLY_PROVIDER_SETTING_KEYS)
    if env_only:
        raise ValueError(PROVIDER_ENV_ONLY_MESSAGE)

    for key in clear_keys:
        _upsert_setting(db, key, None)

    for key, value in updates.items():
        if key in clear_keys:
            continue
        text = str(value or "").strip()
        if key in SECRET_SETTING_KEYS and not text:
            continue
        _upsert_setting(db, key, text or None)

    db.commit()
    return public_settings_payload(db)


def config_status(db: Session | None = None) -> dict[str, Any]:
    statuses = {
        key: public_setting_payload(db, key) if db is not None else _public_setting_payload_without_db(key)
        for key in AI_MARKETING_SETTINGS
    }
    warnings: list[str] = []
    if not statuses[OPENAI_API_KEY]["configured"]:
        warnings.append("OpenAI API key missing")
    if any(status.get("deprecated_admin_setting") for status in statuses.values()):
        warnings.append("Deprecated DB-stored provider credentials detected; ignored.")
    if not statuses[REDDIT_CLIENT_ID]["configured"]:
        warnings.append("Reddit client ID missing")
    if not statuses[REDDIT_CLIENT_SECRET]["configured"]:
        warnings.append("Reddit client secret missing")
    if not statuses[REDDIT_USER_AGENT]["configured"]:
        warnings.append("Reddit user agent missing")
    warnings.append("X is a stub only. No X API calls or posting are implemented.")
    warnings.append("Facebook is manual URL mode only. No Facebook scraping or posting is implemented.")
    return {
        "openai_configured": bool(statuses[OPENAI_API_KEY]["configured"]),
        "openai_model": resolved_setting_value(db, AI_MARKETING_MODEL) or DEFAULT_AI_MARKETING_MODEL,
        "reddit_configured": all(
            bool(statuses[key]["configured"])
            for key in (REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USER_AGENT)
        ),
        "reddit_missing": [
            key
            for key in (REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USER_AGENT)
            if not statuses[key]["configured"]
        ],
        "x_status": "stub",
        "facebook_status": "manual_url_only",
        "warnings": warnings,
        "recipient": AI_MARKETING_RECIPIENT,
        "settings": statuses,
    }


def normalize_campaign_input(payload: dict[str, Any]) -> dict[str, Any]:
    name = str(payload.get("name") or "").strip()
    if not name:
        raise ValueError("Campaign name is required.")

    mode = str(payload.get("mode") or "").strip()
    if mode not in CAMPAIGN_MODES:
        raise ValueError("Unsupported campaign mode.")

    platforms = _normalized_string_list(payload.get("platforms"), lowercase=True)
    if not platforms:
        platforms = ["reddit"]
    unknown_platforms = sorted(set(platforms) - PLATFORMS)
    if unknown_platforms:
        raise ValueError(f"Unsupported platform: {', '.join(unknown_platforms)}.")

    minimum_relevance_score = _clamp_int(payload.get("minimum_relevance_score", 60), 0, 100)
    max_items_per_run = _clamp_int(payload.get("max_items_per_run", 10), 1, 50)
    return {
        "name": name,
        "enabled": bool(payload.get("enabled", True)),
        "mode": mode,
        "platforms": platforms,
        "keywords": _normalized_string_list(payload.get("keywords")),
        "tickers": _normalized_tickers(payload.get("tickers")),
        "subreddits": _normalized_subreddits(payload.get("subreddits")),
        "minimum_relevance_score": minimum_relevance_score,
        "max_items_per_run": max_items_per_run,
        "default_destination_page": _walnut_url_or_default(str(payload.get("default_destination_page") or "")),
        "include_disclosure": bool(payload.get("include_disclosure", True)),
        "scheduled_digest_enabled": bool(payload.get("scheduled_digest_enabled", False)),
    }


def create_campaign(db: Session, payload: dict[str, Any]) -> AiMarketingCampaign:
    normalized = normalize_campaign_input(payload)
    campaign = AiMarketingCampaign(
        name=normalized["name"],
        enabled=normalized["enabled"],
        mode=normalized["mode"],
        platforms_json=_dump_list(normalized["platforms"]),
        keywords_json=_dump_list(normalized["keywords"]),
        tickers_json=_dump_list(normalized["tickers"]),
        subreddits_json=_dump_list(normalized["subreddits"]),
        minimum_relevance_score=normalized["minimum_relevance_score"],
        max_items_per_run=normalized["max_items_per_run"],
        default_destination_page=normalized["default_destination_page"],
        include_disclosure=normalized["include_disclosure"],
        scheduled_digest_enabled=normalized["scheduled_digest_enabled"],
    )
    db.add(campaign)
    db.commit()
    db.refresh(campaign)
    return campaign


def update_campaign(db: Session, campaign: AiMarketingCampaign, payload: dict[str, Any]) -> AiMarketingCampaign:
    current = campaign_to_dict(campaign)
    current.update(payload)
    normalized = normalize_campaign_input(current)
    campaign.name = normalized["name"]
    campaign.enabled = normalized["enabled"]
    campaign.mode = normalized["mode"]
    campaign.platforms_json = _dump_list(normalized["platforms"])
    campaign.keywords_json = _dump_list(normalized["keywords"])
    campaign.tickers_json = _dump_list(normalized["tickers"])
    campaign.subreddits_json = _dump_list(normalized["subreddits"])
    campaign.minimum_relevance_score = normalized["minimum_relevance_score"]
    campaign.max_items_per_run = normalized["max_items_per_run"]
    campaign.default_destination_page = normalized["default_destination_page"]
    campaign.include_disclosure = normalized["include_disclosure"]
    campaign.scheduled_digest_enabled = normalized["scheduled_digest_enabled"]
    db.commit()
    db.refresh(campaign)
    return campaign


def campaign_to_dict(campaign: AiMarketingCampaign) -> dict[str, Any]:
    return {
        "id": campaign.id,
        "name": campaign.name,
        "enabled": bool(campaign.enabled),
        "mode": campaign.mode,
        "platforms": _load_list(campaign.platforms_json),
        "keywords": _load_list(campaign.keywords_json),
        "tickers": _load_list(campaign.tickers_json),
        "subreddits": _load_list(campaign.subreddits_json),
        "minimum_relevance_score": int(campaign.minimum_relevance_score or 0),
        "max_items_per_run": int(campaign.max_items_per_run or 0),
        "default_destination_page": campaign.default_destination_page or DEFAULT_DESTINATION_URL,
        "include_disclosure": bool(campaign.include_disclosure),
        "scheduled_digest_enabled": bool(campaign.scheduled_digest_enabled),
        "created_at": _iso(campaign.created_at),
        "updated_at": _iso(campaign.updated_at),
    }


def latest_suggestions_by_opportunity(db: Session, opportunity_ids: list[int]) -> dict[int, AiMarketingSuggestion]:
    if not opportunity_ids:
        return {}
    rows = db.execute(
        select(AiMarketingSuggestion)
        .where(AiMarketingSuggestion.opportunity_id.in_(opportunity_ids))
        .order_by(AiMarketingSuggestion.opportunity_id, desc(AiMarketingSuggestion.created_at), desc(AiMarketingSuggestion.id))
    ).scalars()
    latest: dict[int, AiMarketingSuggestion] = {}
    for row in rows:
        latest.setdefault(row.opportunity_id, row)
    return latest


def opportunity_to_dict(
    opportunity: AiMarketingOpportunity,
    *,
    suggestion: AiMarketingSuggestion | None = None,
) -> dict[str, Any]:
    return {
        "id": opportunity.id,
        "campaign_id": opportunity.campaign_id,
        "platform": opportunity.platform,
        "source_id": opportunity.source_id,
        "source_url": opportunity.source_url,
        "title": opportunity.title,
        "excerpt": opportunity.excerpt,
        "author": opportunity.author,
        "community": opportunity.community,
        "source_score": opportunity.source_score,
        "comment_count": opportunity.comment_count,
        "source_created_at": _iso(opportunity.source_created_at),
        "status": opportunity.status,
        "matched_keywords": _load_list(opportunity.matched_keywords_json),
        "matched_tickers": _load_list(opportunity.matched_tickers_json),
        "relevance_score": opportunity.relevance_score,
        "spam_risk_score": opportunity.spam_risk_score,
        "intent": opportunity.intent,
        "suggested_destination_url": opportunity.suggested_destination_url,
        "short_reason": opportunity.short_reason,
        "compliance_notes": opportunity.compliance_notes,
        "metadata": _load_object(opportunity.raw_metadata_json),
        "created_at": _iso(opportunity.created_at),
        "updated_at": _iso(opportunity.updated_at),
        "last_seen_at": _iso(opportunity.last_seen_at),
        "suggestion": suggestion_to_dict(suggestion) if suggestion else None,
    }


def suggestion_to_dict(suggestion: AiMarketingSuggestion | None) -> dict[str, Any] | None:
    if suggestion is None:
        return None
    return {
        "id": suggestion.id,
        "opportunity_id": suggestion.opportunity_id,
        "campaign_id": suggestion.campaign_id,
        "model": suggestion.model,
        "relevance_score": suggestion.relevance_score,
        "spam_risk_score": suggestion.spam_risk_score,
        "detected_tickers": _load_list(suggestion.detected_tickers_json),
        "intent": suggestion.intent,
        "suggested_destination_url": suggestion.suggested_destination_url,
        "suggested_reply": suggestion.suggested_reply,
        "short_reason": suggestion.short_reason,
        "compliance_notes": suggestion.compliance_notes,
        "prompt_version": suggestion.prompt_version,
        "created_at": _iso(suggestion.created_at),
    }


def email_log_to_dict(log: AiMarketingEmailLog) -> dict[str, Any]:
    return {
        "id": log.id,
        "delivery_id": log.delivery_id,
        "to_email": log.to_email,
        "subject": log.subject,
        "opportunity_ids": _load_list(log.opportunity_ids_json),
        "status": log.status,
        "payload": _load_object(log.payload_json),
        "created_at": _iso(log.created_at),
        "sent_at": _iso(log.sent_at),
    }


def run_campaign(db: Session, campaign: AiMarketingCampaign) -> dict[str, Any]:
    warnings: list[str] = []
    if not campaign.enabled:
        warnings.append("Campaign is disabled; no discovery run was performed.")
        return {"created": 0, "deduped": 0, "suggested": 0, "warnings": warnings, "opportunities": []}

    items: list[SourceItem] = []
    platforms = set(_load_list(campaign.platforms_json))
    if "reddit" in platforms:
        try:
            items.extend(RedditSourceAdapter(db).search(campaign))
        except MissingMarketingCredential as exc:
            warnings.append(str(exc))
        except Exception:
            logger.exception("ai_marketing_reddit_search_failed campaign_id=%s", campaign.id)
            warnings.append("Reddit discovery failed. Check credentials, rate limits, and Reddit API availability.")

    if "x_stub" in platforms:
        warnings.append("X is configured as a future official API stub only; no X discovery ran.")
    if "facebook_manual" in platforms:
        warnings.append("Facebook is manual URL mode only; no Facebook discovery ran.")

    created = 0
    deduped = 0
    suggested = 0
    opportunities: list[AiMarketingOpportunity] = []
    for item in items[: max(1, int(campaign.max_items_per_run or 10))]:
        opportunity, was_created = upsert_source_item(db, campaign, item)
        opportunities.append(opportunity)
        if was_created:
            created += 1
        else:
            deduped += 1

        if resolved_setting_value(db, OPENAI_API_KEY):
            try:
                generate_suggestion(db, opportunity, campaign=campaign)
                suggested += 1
            except OpenAISuggestionError as exc:
                warnings.append(f"Suggestion generation failed for opportunity {opportunity.id}: {exc.admin_message}")
            except Exception:
                logger.exception("ai_marketing_suggestion_failed opportunity_id=%s", opportunity.id)
                warnings.append(f"Suggestion generation failed for opportunity {opportunity.id}.")
        else:
            warnings.append("OpenAI API key missing; run saved opportunities without AI suggestions.")
            break

    latest = latest_suggestions_by_opportunity(db, [row.id for row in opportunities])
    return {
        "created": created,
        "deduped": deduped,
        "suggested": suggested,
        "warnings": _dedupe_strings(warnings),
        "opportunities": [
            opportunity_to_dict(row, suggestion=latest.get(row.id))
            for row in opportunities
        ],
    }


def upsert_source_item(
    db: Session,
    campaign: AiMarketingCampaign | None,
    item: SourceItem,
) -> tuple[AiMarketingOpportunity, bool]:
    text_for_matching = " ".join(part for part in (item.title, item.excerpt or "", item.source_url) if part)
    campaign_keywords = _load_list(campaign.keywords_json) if campaign else []
    campaign_tickers = _load_list(campaign.tickers_json) if campaign else []
    matched_keywords = _matched_keywords(text_for_matching, campaign_keywords)
    matched_tickers = _matched_tickers(text_for_matching, campaign_tickers)
    source_dedupe_key = _dedupe_key(item.source_id or item.source_url)
    now = datetime.now(timezone.utc)

    opportunity = db.execute(
        select(AiMarketingOpportunity).where(
            AiMarketingOpportunity.platform == item.platform,
            AiMarketingOpportunity.source_dedupe_key == source_dedupe_key,
        )
    ).scalar_one_or_none()
    if opportunity:
        opportunity.campaign_id = campaign.id if campaign else opportunity.campaign_id
        opportunity.title = _truncate(item.title, 500) or opportunity.title
        opportunity.excerpt = _truncate(item.excerpt, 1500)
        opportunity.source_score = item.source_score
        opportunity.comment_count = item.comment_count
        opportunity.last_seen_at = now
        opportunity.matched_keywords_json = _dump_list(matched_keywords)
        opportunity.matched_tickers_json = _dump_list(matched_tickers)
        opportunity.raw_metadata_json = _dump_object(item.metadata or {})
        db.commit()
        db.refresh(opportunity)
        return opportunity, False

    opportunity = AiMarketingOpportunity(
        campaign_id=campaign.id if campaign else None,
        platform=item.platform,
        source_id=_truncate(item.source_id, 200),
        source_url=_truncate(item.source_url, 1000) or DEFAULT_DESTINATION_URL,
        source_dedupe_key=source_dedupe_key,
        title=_truncate(item.title, 500) or "Untitled opportunity",
        excerpt=_truncate(item.excerpt, 1500),
        author=_truncate(item.author, 200),
        community=_truncate(item.community, 200),
        source_score=item.source_score,
        comment_count=item.comment_count,
        source_created_at=item.source_created_at,
        status="new",
        matched_keywords_json=_dump_list(matched_keywords),
        matched_tickers_json=_dump_list(matched_tickers),
        raw_metadata_json=_dump_object(item.metadata or {}),
        created_at=now,
        updated_at=now,
        last_seen_at=now,
    )
    db.add(opportunity)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        existing = db.execute(
            select(AiMarketingOpportunity).where(
                AiMarketingOpportunity.platform == item.platform,
                AiMarketingOpportunity.source_dedupe_key == source_dedupe_key,
            )
        ).scalar_one()
        return existing, False
    db.refresh(opportunity)
    return opportunity, True


def create_manual_opportunity(
    db: Session,
    *,
    url: str | None,
    text: str | None,
    title: str | None = None,
    campaign: AiMarketingCampaign | None = None,
    generate: bool = True,
) -> dict[str, Any]:
    manual_text = str(text or "").strip()
    raw_url = str(url or "").strip()
    normalized_url = _normalize_source_url(raw_url) if raw_url else None
    if normalized_url and _is_reddit_subreddit_listing_url(normalized_url) and not manual_text:
        raise ValueError(MANUAL_SUBREDDIT_LISTING_MESSAGE)
    if normalized_url and _platform_from_url(normalized_url) == "reddit" and not manual_text and _missing_reddit_credentials(db):
        raise ValueError(MANUAL_REDDIT_CREDENTIALS_MESSAGE)
    if not manual_text:
        raise ValueError(MANUAL_TEXT_REQUIRED_MESSAGE)

    platform = _platform_from_url(normalized_url) if normalized_url else "manual"
    source_url = normalized_url or MANUAL_SOURCE_URL
    source_key = normalized_url or f"manual:text:{_dedupe_key(manual_text)}"
    source_item = SourceItem(
        platform=platform,
        source_id=f"manual:{_dedupe_key(source_key)}",
        source_url=source_url,
        title=title or "Manual URL review",
        excerpt=manual_text,
        metadata={
            "manual": True,
            "source": "admin_manual_url",
            "source_url_provided": bool(normalized_url),
        },
    )
    opportunity, _was_created = upsert_source_item(db, campaign, source_item)
    warning: str | None = None
    if generate:
        if resolved_setting_value(db, OPENAI_API_KEY):
            try:
                generate_suggestion(db, opportunity, campaign=campaign)
            except OpenAISuggestionError as exc:
                warning = exc.admin_message
        else:
            warning = "OpenAI API key missing; manual opportunity was saved without an AI suggestion."
            _record_suggestion_failure(db, opportunity, OPENAI_MISSING_KEY_MESSAGE, code="missing_key")
    latest = latest_suggestions_by_opportunity(db, [opportunity.id]).get(opportunity.id)
    return {
        "opportunity": opportunity_to_dict(opportunity, suggestion=latest),
        "warning": warning,
    }


def update_opportunity_status(
    db: Session,
    opportunity: AiMarketingOpportunity,
    *,
    status: str | None = None,
) -> AiMarketingOpportunity:
    if status is not None:
        normalized = status.strip().lower()
        if normalized not in OPPORTUNITY_STATUSES:
            raise ValueError("Unsupported opportunity status.")
        opportunity.status = normalized
    opportunity.updated_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(opportunity)
    return opportunity


def generate_suggestion(
    db: Session,
    opportunity: AiMarketingOpportunity,
    *,
    campaign: AiMarketingCampaign | None = None,
) -> AiMarketingSuggestion:
    api_key = resolved_setting_value(db, OPENAI_API_KEY)
    if not api_key:
        _record_suggestion_failure(db, opportunity, OPENAI_MISSING_KEY_MESSAGE, code="missing_key")
        raise MissingMarketingCredential("OpenAI API key missing.")
    if campaign is None and opportunity.campaign_id:
        campaign = db.get(AiMarketingCampaign, opportunity.campaign_id)

    model = marketing_model(db)
    campaign_payload = campaign_to_dict(campaign) if campaign else None
    platform = opportunity.platform
    destination_hint = recommended_destination_url(
        mode=campaign.mode if campaign else "manual_url_review",
        platform=platform,
        campaign_id=campaign.id if campaign else opportunity.campaign_id or 0,
        tickers=_load_list(opportunity.matched_tickers_json),
        fallback=(campaign.default_destination_page if campaign else DEFAULT_DESTINATION_URL),
    )
    request_payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": _suggestion_system_prompt()},
            {
                "role": "user",
                "content": json.dumps(
                    {
                        "campaign": campaign_payload,
                        "opportunity": {
                            "platform": opportunity.platform,
                            "source_url": opportunity.source_url,
                            "title": opportunity.title,
                            "excerpt": opportunity.excerpt,
                            "author": opportunity.author,
                            "community": opportunity.community,
                            "score": opportunity.source_score,
                            "comment_count": opportunity.comment_count,
                            "created_at": _iso(opportunity.source_created_at),
                            "matched_keywords": _load_list(opportunity.matched_keywords_json),
                            "matched_tickers": _load_list(opportunity.matched_tickers_json),
                        },
                        "routing_hint": destination_hint,
                    },
                    sort_keys=True,
                ),
            },
        ],
        "store": False,
        "response_format": {
            "type": "json_schema",
            "json_schema": {
                "name": "ai_marketing_suggestion",
                "strict": True,
                "schema": _suggestion_json_schema(),
            },
        },
    }
    try:
        response = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json=request_payload,
            timeout=30,
        )
    except requests.RequestException as exc:
        _record_suggestion_failure(db, opportunity, OPENAI_GENERIC_SUGGESTION_MESSAGE, code="request_error")
        raise OpenAISuggestionError(OPENAI_GENERIC_SUGGESTION_MESSAGE, status_code=502) from exc
    if response.status_code >= 400:
        message, code, status_code = _classify_openai_suggestion_error(response)
        _record_suggestion_failure(db, opportunity, message, code=code, status_code=response.status_code)
        raise OpenAISuggestionError(message, status_code=status_code)
    data = response.json()
    content = _extract_chat_completion_content(data)
    structured = _normalize_suggestion_payload(json.loads(content), destination_hint, platform, campaign.id if campaign else 0)
    suggestion = AiMarketingSuggestion(
        opportunity_id=opportunity.id,
        campaign_id=campaign.id if campaign else opportunity.campaign_id,
        model=model,
        relevance_score=structured["relevance_score"],
        spam_risk_score=structured["spam_risk_score"],
        detected_tickers_json=_dump_list(structured["detected_tickers"]),
        intent=structured["intent"],
        suggested_destination_url=structured["suggested_destination_url"],
        suggested_reply=structured["suggested_reply"],
        short_reason=structured["short_reason"],
        compliance_notes=structured["compliance_notes"],
        prompt_version=AI_MARKETING_PROMPT_VERSION,
        raw_response_json=_dump_object(data),
    )
    db.add(suggestion)
    opportunity.relevance_score = suggestion.relevance_score
    opportunity.spam_risk_score = suggestion.spam_risk_score
    opportunity.intent = suggestion.intent
    opportunity.suggested_destination_url = suggestion.suggested_destination_url
    opportunity.short_reason = suggestion.short_reason
    opportunity.compliance_notes = suggestion.compliance_notes
    _clear_suggestion_failure(opportunity)
    opportunity.matched_tickers_json = _dump_list(
        sorted(set(_load_list(opportunity.matched_tickers_json)) | set(structured["detected_tickers"]))
    )
    opportunity.updated_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(suggestion)
    db.refresh(opportunity)
    return suggestion


def recommended_destination_url(
    *,
    mode: str,
    platform: str,
    campaign_id: int,
    tickers: list[str],
    fallback: str | None = None,
) -> str:
    ticker = _normalized_tickers(tickers)[0] if _normalized_tickers(tickers) else None
    if ticker:
        base_url = f"https://walnutmarkets.com/ticker/{ticker}"
    elif mode == "congress_trade_angle":
        base_url = "https://walnutmarkets.com/feed?event_type=congress_trade"
    elif mode == "insider_buying_angle":
        base_url = "https://walnutmarkets.com/feed?event_type=insider_trade"
    elif mode in {"pain_point_tool_alternative", "unusual_signal_angle"}:
        base_url = "https://walnutmarkets.com/screener"
    else:
        base_url = _walnut_url_or_default(fallback or DEFAULT_DESTINATION_URL)
    return _with_utm(base_url, platform=platform, campaign_id=campaign_id)


def preview_digest(
    db: Session,
    *,
    opportunity_ids: list[int] | None = None,
    statuses: list[str] | None = None,
    limit: int = 25,
) -> dict[str, Any]:
    opportunities = _digest_opportunities(db, opportunity_ids=opportunity_ids, statuses=statuses, limit=limit)
    latest = latest_suggestions_by_opportunity(db, [row.id for row in opportunities])
    context = _digest_context(opportunities, latest)
    return {
        "to_email": AI_MARKETING_RECIPIENT,
        "subject": context["subject"],
        "items": [opportunity_to_dict(row, suggestion=latest.get(row.id)) for row in opportunities],
        "body_text": context["items_text"],
        "body_html": context["items_html"],
        "count": len(opportunities),
    }


def send_digest(
    db: Session,
    *,
    opportunity_ids: list[int] | None = None,
    statuses: list[str] | None = None,
    limit: int = 25,
    admin_user_id: int | None = None,
) -> dict[str, Any]:
    opportunities = _digest_opportunities(db, opportunity_ids=opportunity_ids, statuses=statuses, limit=limit)
    latest = latest_suggestions_by_opportunity(db, [row.id for row in opportunities])
    context = _digest_context(opportunities, latest)
    result = send_email(
        db,
        to_email=AI_MARKETING_RECIPIENT,
        template_key=AI_MARKETING_TEMPLATE_KEY,
        context=context,
        user_id=admin_user_id,
        category="admin_ai_marketing",
        idempotency_key=None,
    )
    status = str(result.get("status") or "queued")
    sent_at = datetime.now(timezone.utc) if status == "sent" else None
    log = AiMarketingEmailLog(
        delivery_id=result.get("id") if isinstance(result.get("id"), int) else None,
        to_email=AI_MARKETING_RECIPIENT,
        subject=context["subject"],
        opportunity_ids_json=_dump_list([str(row.id) for row in opportunities]),
        status=status,
        payload_json=_dump_object({"count": len(opportunities), "delivery": result}),
        sent_at=sent_at,
    )
    db.add(log)
    for opportunity in opportunities:
        if opportunity.status == "new":
            opportunity.status = "emailed"
            opportunity.updated_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(log)
    return {
        "delivery": result,
        "email_log": email_log_to_dict(log),
        "count": len(opportunities),
        "items": [opportunity_to_dict(row, suggestion=latest.get(row.id)) for row in opportunities],
    }


def test_openai_connection(db: Session) -> dict[str, Any]:
    api_key = resolved_setting_value(db, OPENAI_API_KEY)
    model = marketing_model(db)
    if not api_key:
        return {"ok": False, "message": "OpenAI API key missing.", "model": model}
    try:
        response = requests.get(
            f"https://api.openai.com/v1/models/{model}",
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=20,
        )
    except requests.RequestException:
        return {"ok": False, "message": "OpenAI connection request failed.", "model": model}
    if response.status_code >= 400:
        return {
            "ok": False,
            "message": "OpenAI connection failed for the configured key/model.",
            "model": model,
            "status_code": response.status_code,
        }
    return {"ok": True, "message": "OpenAI connection succeeded.", "model": model}


def test_reddit_connection(db: Session) -> dict[str, Any]:
    client_id = resolved_setting_value(db, REDDIT_CLIENT_ID)
    client_secret = resolved_setting_value(db, REDDIT_CLIENT_SECRET)
    user_agent = resolved_setting_value(db, REDDIT_USER_AGENT)
    missing = []
    if not client_id:
        missing.append("Reddit client ID missing.")
    if not client_secret:
        missing.append("Reddit client secret missing.")
    if not user_agent:
        missing.append("Reddit user agent missing.")
    if missing:
        return {"ok": False, "message": " ".join(missing)}
    try:
        RedditSourceAdapter._access_token(client_id, client_secret, user_agent)
    except Exception:
        return {"ok": False, "message": "Reddit OAuth test failed for the configured credentials."}
    return {"ok": True, "message": "Reddit OAuth connection succeeded."}


class RedditSourceAdapter:
    def __init__(self, db: Session | None = None) -> None:
        self.db = db

    def search(self, campaign: AiMarketingCampaign) -> list[SourceItem]:
        client_id = resolved_setting_value(self.db, REDDIT_CLIENT_ID) or ""
        client_secret = resolved_setting_value(self.db, REDDIT_CLIENT_SECRET) or ""
        user_agent = resolved_setting_value(self.db, REDDIT_USER_AGENT) or ""
        missing = [
            name
            for name, value in (
                ("Reddit client ID", client_id),
                ("Reddit client secret", client_secret),
                ("Reddit user agent", user_agent),
            )
            if not value
        ]
        if missing:
            raise MissingMarketingCredential(f"Reddit discovery disabled: {', '.join(missing)} missing.")

        token = self._access_token(client_id, client_secret, user_agent)
        query = self._query_for_campaign(campaign)
        if not query:
            return []

        subreddits = _load_list(campaign.subreddits_json) or [""]
        limit_per_request = min(max(int(campaign.max_items_per_run or 10), 1), 25)
        items: list[SourceItem] = []
        seen: set[str] = set()
        for subreddit in subreddits:
            endpoint = (
                f"https://oauth.reddit.com/r/{subreddit}/search"
                if subreddit
                else "https://oauth.reddit.com/search"
            )
            params = {
                "q": query,
                "sort": "new",
                "limit": limit_per_request,
                "t": "week",
                "restrict_sr": "true" if subreddit else "false",
            }
            response = requests.get(
                endpoint,
                headers={"Authorization": f"Bearer {token}", "User-Agent": user_agent},
                params=params,
                timeout=20,
            )
            if response.status_code == 429:
                raise RuntimeError("Reddit API rate limit reached.")
            if response.status_code >= 400:
                raise RuntimeError("Reddit API request failed.")
            for child in response.json().get("data", {}).get("children", []):
                data = child.get("data") if isinstance(child, dict) else None
                if not isinstance(data, dict):
                    continue
                source_id = str(data.get("name") or data.get("id") or "").strip() or None
                permalink = str(data.get("permalink") or "").strip()
                source_url = f"https://www.reddit.com{permalink}" if permalink.startswith("/") else str(data.get("url") or "")
                if not source_url:
                    continue
                dedupe = source_id or source_url
                if dedupe in seen:
                    continue
                seen.add(dedupe)
                title = str(data.get("title") or "Reddit discussion").strip()
                selftext = str(data.get("selftext") or "")
                excerpt = _truncate(selftext or title, 800)
                created_utc = _float_or_none(data.get("created_utc"))
                items.append(
                    SourceItem(
                        platform="reddit",
                        source_id=source_id,
                        source_url=source_url,
                        title=title,
                        excerpt=excerpt,
                        author=str(data.get("author") or "") or None,
                        community=str(data.get("subreddit") or "") or None,
                        source_score=_int_or_none(data.get("score")),
                        comment_count=_int_or_none(data.get("num_comments")),
                        source_created_at=datetime.fromtimestamp(created_utc, timezone.utc) if created_utc else None,
                        metadata={
                            "subreddit": data.get("subreddit"),
                            "over_18": bool(data.get("over_18")),
                            "url_hint": data.get("url"),
                        },
                    )
                )
                if len(items) >= int(campaign.max_items_per_run or 10):
                    return items
        return items

    @staticmethod
    def _access_token(client_id: str, client_secret: str, user_agent: str) -> str:
        response = requests.post(
            "https://www.reddit.com/api/v1/access_token",
            auth=(client_id, client_secret),
            headers={"User-Agent": user_agent},
            data={"grant_type": "client_credentials"},
            timeout=20,
        )
        if response.status_code >= 400:
            raise RuntimeError("Reddit OAuth token request failed.")
        token = str(response.json().get("access_token") or "").strip()
        if not token:
            raise RuntimeError("Reddit OAuth token response did not include an access token.")
        return token

    @staticmethod
    def _query_for_campaign(campaign: AiMarketingCampaign) -> str:
        terms = _load_list(campaign.keywords_json) + [f"${ticker}" for ticker in _load_list(campaign.tickers_json)]
        terms = [term for term in _dedupe_strings(terms) if term]
        if not terms:
            return ""
        return " OR ".join(terms[:12])


def _digest_opportunities(
    db: Session,
    *,
    opportunity_ids: list[int] | None,
    statuses: list[str] | None,
    limit: int,
) -> list[AiMarketingOpportunity]:
    query = select(AiMarketingOpportunity)
    if opportunity_ids:
        query = query.where(AiMarketingOpportunity.id.in_(opportunity_ids))
    else:
        normalized_statuses = [status for status in _normalized_string_list(statuses, lowercase=True) if status in OPPORTUNITY_STATUSES]
        if not normalized_statuses:
            normalized_statuses = ["new"]
        query = query.where(AiMarketingOpportunity.status.in_(normalized_statuses))
    query = query.order_by(desc(AiMarketingOpportunity.relevance_score), desc(AiMarketingOpportunity.created_at)).limit(max(1, min(limit, 100)))
    return list(db.execute(query).scalars().all())


def _digest_context(
    opportunities: list[AiMarketingOpportunity],
    latest: dict[int, AiMarketingSuggestion],
) -> dict[str, Any]:
    count = len(opportunities)
    subject = f"Walnut AI Outreach: {count} reply opportunit{'y' if count == 1 else 'ies'}"
    items_text: list[str] = []
    items_html: list[str] = []
    for index, opportunity in enumerate(opportunities, start=1):
        suggestion = latest.get(opportunity.id)
        reply = suggestion.suggested_reply if suggestion else "No AI suggestion generated yet."
        destination = (suggestion.suggested_destination_url if suggestion else opportunity.suggested_destination_url) or DEFAULT_DESTINATION_URL
        reason = (suggestion.short_reason if suggestion else opportunity.short_reason) or "No reasoning summary available."
        tickers = ", ".join(_load_list(opportunity.matched_tickers_json)) or "none"
        keywords = ", ".join(_load_list(opportunity.matched_keywords_json)) or "none"
        relevance = suggestion.relevance_score if suggestion else opportunity.relevance_score
        spam = suggestion.spam_risk_score if suggestion else opportunity.spam_risk_score
        items_text.append(
            "\n".join(
                [
                    f"{index}. {opportunity.title}",
                    f"Platform/source: {opportunity.platform} / {opportunity.community or 'manual'}",
                    f"Permalink: {opportunity.source_url}",
                    f"Matched ticker/keywords: {tickers} / {keywords}",
                    f"Relevance score: {relevance if relevance is not None else 'pending'}",
                    f"Spam risk score: {spam if spam is not None else 'pending'}",
                    f"Suggested Walnut link: {destination}",
                    "Suggested reply:",
                    reply,
                    f"Reasoning: {reason}",
                ]
            )
        )
        items_html.append(
            "<div style=\"margin:18px 0;padding:14px;border:1px solid #d8e6ea;border-radius:7px;background:#f8fafc;\">"
            f"<h3 style=\"margin:0 0 8px 0;font-size:16px;line-height:22px;color:#0f172a;\">{html.escape(opportunity.title)}</h3>"
            f"<p style=\"margin:0 0 8px 0;color:#475569;\">{html.escape(opportunity.platform)} / {html.escape(opportunity.community or 'manual')}</p>"
            f"<p style=\"margin:0 0 8px 0;\"><a href=\"{html.escape(opportunity.source_url, quote=True)}\">Open source thread</a></p>"
            f"<p style=\"margin:0 0 8px 0;color:#334155;\">Matched ticker/keywords: {html.escape(tickers)} / {html.escape(keywords)}</p>"
            f"<p style=\"margin:0 0 8px 0;color:#334155;\">Relevance: {html.escape(str(relevance if relevance is not None else 'pending'))} | Spam risk: {html.escape(str(spam if spam is not None else 'pending'))}</p>"
            f"<p style=\"margin:0 0 8px 0;\"><a href=\"{html.escape(destination, quote=True)}\">Suggested Walnut page</a></p>"
            f"<pre style=\"white-space:pre-wrap;margin:10px 0;padding:12px;background:#0f172a;color:#e2e8f0;border-radius:6px;font-size:13px;line-height:18px;\">{html.escape(reply)}</pre>"
            f"<p style=\"margin:0;color:#475569;\">{html.escape(reason)}</p>"
            "</div>"
        )
    return {
        "first_name": "Jarod",
        "subject": subject,
        "digest_title": subject,
        "summary": f"{count} human-reviewed outreach candidate{'s' if count != 1 else ''}. Review before posting. No auto-posting was performed.",
        "items_text": "\n\n".join(items_text) if items_text else "No matching opportunities are ready for digest.",
        "items_html": "".join(items_html) if items_html else "<p>No matching opportunities are ready for digest.</p>",
        "digest_url": "https://walnutmarkets.com/admin/ai-marketing",
    }


def _suggestion_system_prompt() -> str:
    return (
        "You draft human-reviewed outreach replies for Walnut Market Terminal. "
        "Return only JSON matching the supplied schema. Be helpful first. Do not write spammy, hypey, deceptive, or repetitive promotional replies. "
        "Do not imply endorsement by Reddit, X, Facebook, Congress, SEC, or any data provider. "
        "If promoting Walnut, disclose affiliation naturally, for example: \"I'm building Walnut, so obvious bias...\" "
        "Do not make investment advice claims, tell users to buy or sell a security, or guarantee returns. "
        "Prefer educational language such as \"this may be useful\", \"you can cross-check\", and \"one way to look at it\". "
        "Include at most one Walnut link unless the thread clearly needs more. Avoid replies when spam risk is high. "
        "Avoid replying to old or inactive threads unless relevance is very high."
    )


def _suggestion_json_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {
            "relevance_score": {"type": "integer", "minimum": 0, "maximum": 100},
            "spam_risk_score": {"type": "integer", "minimum": 0, "maximum": 100},
            "detected_tickers": {"type": "array", "items": {"type": "string"}},
            "intent": {"type": "string", "enum": sorted(INTENTS)},
            "suggested_destination_url": {"type": "string"},
            "suggested_reply": {"type": "string"},
            "short_reason": {"type": "string"},
            "compliance_notes": {"type": "string"},
        },
        "required": [
            "relevance_score",
            "spam_risk_score",
            "detected_tickers",
            "intent",
            "suggested_destination_url",
            "suggested_reply",
            "short_reason",
            "compliance_notes",
        ],
        "additionalProperties": False,
    }


def _normalize_suggestion_payload(
    payload: dict[str, Any],
    destination_hint: str,
    platform: str,
    campaign_id: int,
) -> dict[str, Any]:
    detected_tickers = _normalized_tickers(payload.get("detected_tickers"))
    destination = _normalize_destination_url(
        str(payload.get("suggested_destination_url") or destination_hint),
        platform=platform,
        campaign_id=campaign_id,
        fallback=destination_hint,
    )
    intent = str(payload.get("intent") or "other").strip().lower()
    if intent not in INTENTS:
        intent = "other"
    return {
        "relevance_score": _clamp_int(payload.get("relevance_score"), 0, 100),
        "spam_risk_score": _clamp_int(payload.get("spam_risk_score"), 0, 100),
        "detected_tickers": detected_tickers,
        "intent": intent,
        "suggested_destination_url": destination,
        "suggested_reply": _truncate(str(payload.get("suggested_reply") or "").strip(), 3000) or "No safe reply suggested.",
        "short_reason": _truncate(str(payload.get("short_reason") or "").strip(), 1000) or "No reason provided.",
        "compliance_notes": _truncate(str(payload.get("compliance_notes") or "").strip(), 1000) or "Review manually before posting.",
    }


def _classify_openai_suggestion_error(response: requests.Response) -> tuple[str, str, int]:
    payload: dict[str, Any] = {}
    try:
        data = response.json()
        payload = data if isinstance(data, dict) else {}
    except Exception:
        payload = {}

    error = payload.get("error") if isinstance(payload.get("error"), dict) else {}
    message = str(error.get("message") or payload.get("message") or "").lower()
    code = str(error.get("code") or "").lower()
    error_type = str(error.get("type") or "").lower()
    haystack = " ".join(part for part in (message, code, error_type) if part)

    if any(term in haystack for term in ("insufficient_quota", "quota", "billing", "credit")):
        return OPENAI_BILLING_CREDITS_MESSAGE, "insufficient_quota", 422
    if response.status_code in {401, 403} or any(term in haystack for term in ("invalid_api_key", "incorrect api key", "unauthorized")):
        return OPENAI_INVALID_KEY_MESSAGE, "invalid_key", 422
    if response.status_code == 429 or any(term in haystack for term in ("rate_limit", "rate limit")):
        return OPENAI_RATE_LIMIT_MESSAGE, "rate_limit", 429
    return OPENAI_GENERIC_SUGGESTION_MESSAGE, "openai_error", 502


def _record_suggestion_failure(
    db: Session,
    opportunity: AiMarketingOpportunity,
    message: str,
    *,
    code: str,
    status_code: int | None = None,
) -> None:
    now = datetime.now(timezone.utc)
    metadata = _load_object(opportunity.raw_metadata_json)
    metadata["ai_suggestion_error"] = message
    metadata["ai_suggestion_error_code"] = code
    metadata["ai_suggestion_error_at"] = _iso(now)
    if status_code is not None:
        metadata["ai_suggestion_error_status_code"] = status_code
    else:
        metadata.pop("ai_suggestion_error_status_code", None)
    opportunity.raw_metadata_json = _dump_object(metadata)
    opportunity.updated_at = now
    db.add(opportunity)
    db.commit()
    db.refresh(opportunity)


def _clear_suggestion_failure(opportunity: AiMarketingOpportunity) -> None:
    metadata = _load_object(opportunity.raw_metadata_json)
    for key in (
        "ai_suggestion_error",
        "ai_suggestion_error_code",
        "ai_suggestion_error_at",
        "ai_suggestion_error_status_code",
    ):
        metadata.pop(key, None)
    opportunity.raw_metadata_json = _dump_object(metadata)


def _extract_chat_completion_content(data: dict[str, Any]) -> str:
    choices = data.get("choices")
    if not isinstance(choices, list) or not choices:
        raise ValueError("OpenAI response did not include choices.")
    message = choices[0].get("message") if isinstance(choices[0], dict) else None
    content = message.get("content") if isinstance(message, dict) else None
    if not isinstance(content, str) or not content.strip():
        raise ValueError("OpenAI response did not include message content.")
    return content


def _upsert_setting(db: Session, key: str, value: str | None) -> AiMarketingSetting:
    # TODO: encrypt admin-stored API secrets at rest before multi-admin/team rollout.
    meta = AI_MARKETING_SETTINGS[key]
    row = db.get(AiMarketingSetting, key)
    now = datetime.now(timezone.utc)
    if row is None:
        row = AiMarketingSetting(
            key=key,
            value=value,
            is_secret=bool(meta["is_secret"]),
            created_at=now,
            updated_at=now,
        )
        db.add(row)
    else:
        row.value = value
        row.is_secret = bool(meta["is_secret"])
        row.updated_at = now
    return row


def _public_setting_payload_without_db(key: str) -> dict[str, Any]:
    meta = AI_MARKETING_SETTINGS[key]
    value = os.getenv(key, "").strip()
    env_only = key in ENV_ONLY_PROVIDER_SETTING_KEYS
    source = "server_env" if value else "default" if key == AI_MARKETING_MODEL else "missing"
    configured = bool(value) or key == AI_MARKETING_MODEL
    is_secret = bool(meta["is_secret"])
    payload = {
        "key": key,
        "label": meta["label"],
        "is_secret": is_secret,
        "configured": configured,
        "source": source,
        "source_label": _setting_source_label(source),
        "required_for": meta["required_for"],
        "masked_value": None if env_only else _masked_secret() if is_secret and configured else None,
        "updated_at": None,
    }
    if not is_secret and not env_only:
        payload["value"] = value or (DEFAULT_AI_MARKETING_MODEL if key == AI_MARKETING_MODEL else "")
    return payload


def _setting_source_label(source: str) -> str:
    if source == "admin_settings":
        return "Configured in admin settings"
    if source == "server_env":
        return "Configured via server env"
    if source == "default":
        return "Default"
    return "Missing"


def _masked_secret() -> str:
    return "\u2022\u2022\u2022\u2022\u2022\u2022\u2022\u2022 saved"


def _with_utm(url: str, *, platform: str, campaign_id: int) -> str:
    parsed = urlparse(url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query.update(
        {
            "utm_source": platform,
            "utm_medium": "organic_reply",
            "utm_campaign": "ai_outreach",
            "utm_content": str(campaign_id or "manual"),
        }
    )
    return urlunparse(parsed._replace(query=urlencode(query)))


def _normalize_destination_url(url: str, *, platform: str, campaign_id: int, fallback: str) -> str:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"} or parsed.netloc.lower() not in {"walnutmarkets.com", "www.walnutmarkets.com"}:
        url = fallback
    return _with_utm(_walnut_url_or_default(url), platform=platform, campaign_id=campaign_id)


def _walnut_url_or_default(value: str) -> str:
    value = value.strip()
    if not value:
        return DEFAULT_DESTINATION_URL
    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"} or parsed.netloc.lower() not in {"walnutmarkets.com", "www.walnutmarkets.com"}:
        return DEFAULT_DESTINATION_URL
    return value


def _normalize_source_url(value: str) -> str:
    parsed = urlparse(value.strip())
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("A valid Reddit, X, or Facebook URL is required.")
    host = parsed.netloc.lower()
    if not any(domain in host for domain in ("reddit.com", "x.com", "twitter.com", "facebook.com", "fb.com")):
        raise ValueError("Manual URL must be from Reddit, X, or Facebook.")
    return value.strip()


def _missing_reddit_credentials(db: Session | None) -> list[str]:
    return [
        key
        for key in (REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USER_AGENT)
        if not resolved_setting_value(db, key)
    ]


def _is_reddit_subreddit_listing_url(url: str) -> bool:
    parsed = urlparse(url)
    if "reddit.com" not in parsed.netloc.lower():
        return False
    parts = [part.lower() for part in parsed.path.split("/") if part]
    if len(parts) < 2 or parts[0] != "r":
        return False
    return "comments" not in parts


def _platform_from_url(url: str) -> str:
    host = urlparse(url).netloc.lower()
    if "reddit.com" in host:
        return "reddit"
    if "facebook.com" in host or "fb.com" in host:
        return "facebook_manual"
    return "x_stub"


def _matched_keywords(text: str, keywords: list[str]) -> list[str]:
    folded = text.lower()
    return [keyword for keyword in keywords if keyword.lower() in folded]


def _matched_tickers(text: str, campaign_tickers: list[str]) -> list[str]:
    detected = set()
    for match in _TICKER_PATTERN.finditer(text):
        raw = match.group(0)
        ticker = match.group(1).upper()
        if ticker in _COMMON_FALSE_TICKERS:
            continue
        if raw.startswith("$") or match.group(1).isupper():
            detected.add(ticker)
    campaign = set(_normalized_tickers(campaign_tickers))
    upper_text = text.upper()
    if campaign:
        detected |= {ticker for ticker in campaign if ticker in upper_text or f"${ticker}" in upper_text}
    return sorted(detected)


def _normalized_tickers(value: Any) -> list[str]:
    tickers = []
    for item in _normalized_string_list(value):
        ticker = item.strip().upper().lstrip("$")
        if re.fullmatch(r"[A-Z][A-Z0-9.\-]{0,7}", ticker) and ticker not in _COMMON_FALSE_TICKERS:
            tickers.append(ticker)
    return _dedupe_strings(tickers)


def _normalized_subreddits(value: Any) -> list[str]:
    result = []
    for item in _normalized_string_list(value):
        cleaned = item.strip().removeprefix("r/").strip("/")
        if re.fullmatch(r"[A-Za-z0-9_]{2,40}", cleaned):
            result.append(cleaned)
    return _dedupe_strings(result)


def _normalized_string_list(value: Any, *, lowercase: bool = False) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        raw = re.split(r"[\n,]", value)
    elif isinstance(value, list):
        raw = value
    else:
        raw = [value]
    result = []
    for item in raw:
        text = str(item or "").strip()
        if not text:
            continue
        result.append(text.lower() if lowercase else text)
    return _dedupe_strings(result)


def _load_list(raw: str | None) -> list[str]:
    try:
        parsed = json.loads(raw or "[]")
    except Exception:
        return []
    if not isinstance(parsed, list):
        return []
    return [str(item) for item in parsed if str(item or "").strip()]


def _load_object(raw: str | None) -> dict[str, Any]:
    try:
        parsed = json.loads(raw or "{}")
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _dump_list(values: list[Any]) -> str:
    return json.dumps(_dedupe_strings([str(value).strip() for value in values if str(value).strip()]), sort_keys=True)


def _dump_object(value: dict[str, Any]) -> str:
    return json.dumps(value, sort_keys=True, default=str)


def _dedupe_strings(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        key = value.lower()
        if key in seen:
            continue
        seen.add(key)
        result.append(value)
    return result


def _dedupe_key(value: str) -> str:
    return hashlib.sha256(value.strip().lower().encode("utf-8")).hexdigest()


def _clamp_int(value: Any, minimum: int, maximum: int) -> int:
    parsed = _int_or_none(value)
    if parsed is None:
        parsed = minimum
    return max(minimum, min(maximum, parsed))


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _float_or_none(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _truncate(value: str | None, limit: int) -> str | None:
    if value is None:
        return None
    cleaned = str(value).strip()
    if len(cleaned) <= limit:
        return cleaned
    return cleaned[: limit - 1].rstrip() + "..."


def _iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.isoformat()
