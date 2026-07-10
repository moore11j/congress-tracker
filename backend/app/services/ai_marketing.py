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

DEFAULT_AI_GROWTH_RECIPIENT = "jarod@walnutmarkets.com"
AI_GROWTH_DIGEST_RECIPIENT = "AI_GROWTH_DIGEST_RECIPIENT"
AI_MARKETING_TEMPLATE_KEY = "ai_marketing.digest"
AI_MARKETING_PROMPT_VERSION = "ai_growth_v1"
DEFAULT_DESTINATION_URL = "https://walnutmarkets.com"
DEFAULT_AI_MARKETING_MODEL = "gpt-5.4-mini"
X_POST_CHARACTER_LIMIT = 280
OPENAI_API_KEY = "OPENAI_API_KEY"
AI_MARKETING_MODEL = "AI_MARKETING_MODEL"
OPENAI_WEB_SEARCH_ENABLED = "OPENAI_WEB_SEARCH_ENABLED"
REDDIT_CLIENT_ID = "REDDIT_CLIENT_ID"
REDDIT_CLIENT_SECRET = "REDDIT_CLIENT_SECRET"
REDDIT_USER_AGENT = "REDDIT_USER_AGENT"
BING_SEARCH_API_KEY = "BING_SEARCH_API_KEY"
WEB_SEARCH_REDDIT_SOURCE_PROVIDER = "web_search_reddit"
OPENAI_WEB_SEARCH_NOT_CONFIGURED_MESSAGE = (
    "OpenAI web search is not configured. Enable OPENAI_WEB_SEARCH_ENABLED=true and confirm OPENAI_API_KEY is set."
)
OPENAI_WEB_SEARCH_FAILED_MESSAGE = "OpenAI web search discovery failed. Check OpenAI configuration, quota, and API availability."

LEGACY_CAMPAIGN_MODES = {
    "ticker_thread_assist",
    "congress_trade_angle",
    "insider_buying_angle",
    "unusual_signal_angle",
    "pain_point_tool_alternative",
}
GROWTH_CAMPAIGN_MODES = {
    "manual_url_review",
    "manual_research_input",
    "x_chart_drop",
    "reddit_research_thread",
}
CAMPAIGN_MODES = LEGACY_CAMPAIGN_MODES | GROWTH_CAMPAIGN_MODES
PLATFORMS = {"reddit", WEB_SEARCH_REDDIT_SOURCE_PROVIDER, "x_stub", "x", "facebook_manual", "facebook", "linkedin", "manual", "other"}
CAMPAIGN_RECENCIES = {"any", "day", "week", "month"}
OPPORTUNITY_STATUSES = {
    "new",
    "draft",
    "needs_review",
    "emailed",
    "opened",
    "copied",
    "approved",
    "posted_manually",
    "archived",
    "rejected",
    "dismissed",
    "regeneration_needed",
    "quality_failed",
}
INTENTS = {"question", "complaint", "trade_idea", "tool_search", "news_reaction", "other"}
RECOMMENDED_ACTIONS = {"reply", "skip", "monitor", "draft_post", "draft_ad"}
CONTENT_TYPES = {"reddit_reply", "reddit_thread", "x_post", "paid_ad"}
CAMPAIGN_TYPES = {
    "manual_research_input",
    "x_chart_drop",
    "reddit_research_thread",
    "legacy_outreach_campaign",
}
REPLY_ANGLES = {
    "margin_analysis",
    "ticker_context",
    "congress_activity",
    "insider_activity",
    "government_contracts",
    "screener_tool",
    "general_market_context",
    "other",
}
AI_MARKETING_SETTINGS: dict[str, dict[str, Any]] = {
    OPENAI_API_KEY: {"label": "OpenAI API Key", "is_secret": True, "required_for": "AI Growth suggestions"},
    AI_MARKETING_MODEL: {"label": "AI Growth Model", "is_secret": False, "required_for": "AI Growth suggestions"},
    OPENAI_WEB_SEARCH_ENABLED: {"label": "OpenAI Web Search", "is_secret": False, "required_for": "AI Growth web discovery"},
    REDDIT_CLIENT_ID: {"label": "Reddit Client ID", "is_secret": True, "required_for": "Reddit discovery"},
    REDDIT_CLIENT_SECRET: {"label": "Reddit Client Secret", "is_secret": True, "required_for": "Reddit discovery"},
    REDDIT_USER_AGENT: {"label": "Reddit User Agent", "is_secret": False, "required_for": "Reddit discovery"},
}
SECRET_SETTING_KEYS = {key for key, meta in AI_MARKETING_SETTINGS.items() if meta["is_secret"]}
ENV_ONLY_PROVIDER_SETTING_KEYS = frozenset(
    {
        OPENAI_API_KEY,
        AI_MARKETING_MODEL,
        OPENAI_WEB_SEARCH_ENABLED,
        REDDIT_CLIENT_ID,
        REDDIT_CLIENT_SECRET,
        REDDIT_USER_AGENT,
    }
)
BOOLEAN_ENV_ONLY_SETTING_KEYS = frozenset({OPENAI_WEB_SEARCH_ENABLED})
LEGACY_PROVIDER_SETTING_KEYS = frozenset({BING_SEARCH_API_KEY})
PROVIDER_ENV_ONLY_MESSAGE = "Provider credentials are managed through server environment variables."
MANUAL_REDDIT_CREDENTIALS_MESSAGE = (
    "Reddit API credentials are not configured. Paste the post/comment text manually or configure Reddit API credentials."
)
MANUAL_SUBREDDIT_LISTING_MESSAGE = (
    "Manual URL mode works best with a specific post/comment URL or pasted text. "
    "Subreddit listing URLs require Reddit API discovery."
)
MANUAL_TEXT_REQUIRED_MESSAGE = "Paste the post/comment text, thread excerpt, or research context before generating a manual suggestion."
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
DEFAULT_WEB_SEARCH_REDDIT_QUERY_TEMPLATES = [
    "site:reddit.com/r/{subreddit} {term}",
]
DEFAULT_WEB_SEARCH_REDDIT_SUBREDDITS = [
    "stocks",
    "investing",
    "SecurityAnalysis",
    "StockMarket",
    "options",
]
SHORT_SEARCH_SNIPPET_THRESHOLD = 80
REDDIT_RESEARCH_QUALITY_THRESHOLDS = {
    "research_depth_score": 75,
    "evidence_score": 70,
    "balance_score": 70,
    "promotional_risk_score": 35,
    "compliance_risk_score": 30,
}
REDDIT_RESEARCH_SECTIONS = [
    "TL;DR",
    "Why this name came up",
    "Company snapshot",
    "Walnut disclosure stack",
    "Technical picture",
    "Fundamental picture",
    "Recent news / filings / press releases",
    "Catalysts",
    "Bull case",
    "Bear case / risks",
    "What would confirm the setup",
    "What would weaken the setup",
    "Bottom line",
    "Suggested Reddit disclosure",
]


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
    source_provider: str | None = None
    campaign_type: str | None = None
    content_type: str | None = None
    source_platform: str | None = None
    ticker_theme: str | None = None
    recommended_action: str | None = None
    fit_score: int | None = None
    generated_content: str | None = None
    alternate_versions: dict[str, Any] | None = None
    assets: list[dict[str, Any]] | None = None
    excerpt: str | None = None
    author: str | None = None
    community: str | None = None
    source_score: int | None = None
    comment_count: int | None = None
    source_created_at: datetime | None = None
    metadata: dict[str, Any] | None = None


@dataclass(frozen=True)
class WebSearchResult:
    title: str
    url: str
    snippet: str | None = None
    provider: str = "web_search"


def ai_growth_recipient() -> str:
    return os.getenv(AI_GROWTH_DIGEST_RECIPIENT, "").strip() or DEFAULT_AI_GROWTH_RECIPIENT


def marketing_model(db: Session | None = None) -> str:
    return resolved_setting_value(db, AI_MARKETING_MODEL) or DEFAULT_AI_MARKETING_MODEL


def resolved_setting_value(db: Session | None, key: str) -> str | None:
    resolved = resolve_setting(db, key)
    return resolved["value"] if isinstance(resolved["value"], str) and resolved["value"].strip() else None


def _env_flag_enabled(key: str) -> bool:
    return os.getenv(key, "").strip().lower() in {"1", "true", "yes", "on"}


def resolve_setting(db: Session | None, key: str) -> dict[str, Any]:
    if key not in AI_MARKETING_SETTINGS:
        raise KeyError(f"Unsupported AI marketing setting: {key}")

    row = db.get(AiMarketingSetting, key) if db is not None else None
    row_value = (row.value or "").strip() if row and row.value is not None else ""
    deprecated_admin_setting = key in ENV_ONLY_PROVIDER_SETTING_KEYS and bool(row_value)
    env_value = os.getenv(key, "").strip()

    if key in ENV_ONLY_PROVIDER_SETTING_KEYS:
        if key in BOOLEAN_ENV_ONLY_SETTING_KEYS:
            if _env_flag_enabled(key):
                return {
                    "key": key,
                    "value": "true",
                    "source": "server_env",
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


def _has_legacy_db_provider_setting(db: Session | None) -> bool:
    if db is None:
        return False
    for key in LEGACY_PROVIDER_SETTING_KEYS:
        row = db.get(AiMarketingSetting, key)
        if row and str(row.value or "").strip():
            return True
    return False


def config_status(db: Session | None = None) -> dict[str, Any]:
    statuses = {
        key: public_setting_payload(db, key) if db is not None else _public_setting_payload_without_db(key)
        for key in AI_MARKETING_SETTINGS
    }
    web_search_status = web_search_provider_status(db)
    warnings: list[str] = []
    if not statuses[OPENAI_API_KEY]["configured"]:
        warnings.append("OpenAI API key missing")
    if any(status.get("deprecated_admin_setting") for status in statuses.values()) or _has_legacy_db_provider_setting(db):
        warnings.append("Deprecated DB-stored provider credentials detected; ignored.")
    if not statuses[REDDIT_CLIENT_ID]["configured"]:
        warnings.append("Reddit client ID missing")
    if not statuses[REDDIT_CLIENT_SECRET]["configured"]:
        warnings.append("Reddit client secret missing")
    if not statuses[REDDIT_USER_AGENT]["configured"]:
        warnings.append("Reddit user agent missing")
    if not web_search_status["configured"]:
        warnings.append(OPENAI_WEB_SEARCH_NOT_CONFIGURED_MESSAGE)
    warnings.append("X is a stub only. No X API calls or posting are implemented.")
    warnings.append("Facebook is manual URL mode only. No Facebook scraping or posting is implemented.")
    reddit_configured = all(
        bool(statuses[key]["configured"])
        for key in (REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USER_AGENT)
    )
    return {
        "openai_configured": bool(statuses[OPENAI_API_KEY]["configured"]),
        "openai_model": resolved_setting_value(db, AI_MARKETING_MODEL) or DEFAULT_AI_MARKETING_MODEL,
        "reddit_configured": reddit_configured,
        "reddit_status": "configured" if reddit_configured else "missing",
        "reddit_missing": [
            key
            for key in (REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USER_AGENT)
            if not statuses[key]["configured"]
        ],
        "web_search_reddit_configured": bool(web_search_status["configured"]),
        "web_search_reddit_status": "configured" if web_search_status["configured"] else "missing",
        "web_search_reddit_provider": web_search_status["provider"],
        "web_search_reddit_missing": web_search_status["missing"],
        "openai_web_search_configured": bool(web_search_status["configured"]),
        "openai_web_search_status": "enabled" if web_search_status["configured"] else "disabled",
        "openai_web_search_provider": web_search_status["provider"],
        "openai_web_search_missing": web_search_status["missing"],
        "manual_text_status": "available",
        "x_status": "stub",
        "facebook_status": "manual_url_only",
        "warnings": warnings,
        "recipient": ai_growth_recipient(),
        "settings": statuses,
    }


def _campaign_type_for_mode(mode: str | None) -> str:
    normalized = str(mode or "").strip().lower()
    if normalized in {
        "manual_url_review",
        "manual_research_input",
    }:
        return "manual_research_input"
    if normalized in {"x_chart_drop", "reddit_research_thread"}:
        return normalized
    return "legacy_outreach_campaign"


def _content_type_for_campaign_type(campaign_type: str | None, *, desired_output_type: str | None = None, platform: str | None = None) -> str:
    desired = str(desired_output_type or "").strip().lower().replace(" ", "_")
    if desired in {"x_post", "x", "post"}:
        return "x_post"
    if desired in {"reddit_research_thread", "reddit_thread", "thread"}:
        return "reddit_thread"
    if desired in {"paid_ad", "paid_ad_copy", "ad_copy", "draft_ad"}:
        return "paid_ad"
    if desired in {"reply", "reddit_reply", "comment"}:
        return "reddit_reply"

    normalized = str(campaign_type or "").strip().lower()
    if normalized == "x_chart_drop":
        return "x_post"
    if normalized == "reddit_research_thread":
        return "reddit_thread"
    if str(platform or "").strip().lower() in {"x", "x_stub", "twitter"}:
        return "x_post"
    return "reddit_reply"


def _default_action_for_content_type(content_type: str | None) -> str:
    return {
        "x_post": "draft_post",
        "reddit_thread": "draft_post",
        "paid_ad": "draft_ad",
        "reddit_reply": "reply",
    }.get(str(content_type or "").strip().lower(), "reply")


def _normalize_campaign_type(value: str | None, *, fallback_mode: str | None = None) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in CAMPAIGN_TYPES:
        return normalized
    return _campaign_type_for_mode(fallback_mode)


def _normalize_content_type(value: str | None, *, campaign_type: str | None = None, desired_output_type: str | None = None, platform: str | None = None) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in CONTENT_TYPES:
        return normalized
    return _content_type_for_campaign_type(campaign_type, desired_output_type=desired_output_type, platform=platform)


def _normalize_source_platform(value: str | None, *, fallback: str | None = None) -> str:
    normalized = str(value or fallback or "other").strip().lower()
    aliases = {
        "twitter": "x",
        "x_stub": "x",
        "facebook_manual": "facebook",
        "manual": "other",
    }
    normalized = aliases.get(normalized, normalized)
    if normalized in {"x", "reddit", "facebook", "linkedin", "other"}:
        return normalized
    return "other"


def _platform_for_content_type(content_type: str) -> str:
    if content_type == "x_post":
        return "x"
    if content_type in {"reddit_reply", "reddit_thread"}:
        return "reddit"
    return "other"


def _default_growth_title(campaign_type: str, ticker_theme: str | None = None) -> str:
    labels = {
        "manual_research_input": "Manual Research Input",
        "x_chart_drop": "X Campaign",
        "reddit_research_thread": "Reddit Research Thread",
    }
    label = labels.get(campaign_type, "AI Growth Draft")
    theme = str(ticker_theme or "").strip()
    return f"{label}: {theme}" if theme else label


def _growth_context_text(
    *,
    campaign_type: str,
    content_type: str,
    text: str | None,
    ticker_theme: str | None,
    audience: str | None,
    tone: str | None,
    inputs: dict[str, Any],
) -> str:
    parts = [
        f"Campaign type: {campaign_type}",
        f"Content type: {content_type}",
    ]
    if ticker_theme:
        parts.append(f"Ticker/theme: {ticker_theme}")
    if audience:
        parts.append(f"Audience: {audience}")
    if tone:
        parts.append(f"Tone: {tone}")
    for key, value in sorted((inputs or {}).items()):
        if value in (None, "", [], {}):
            continue
        parts.append(f"{key}: {value}")
    if text:
        parts.append(f"Research/input text: {text}")
    return "\n".join(parts)


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
    recency = str(payload.get("recency") or "week").strip().lower()
    if recency not in CAMPAIGN_RECENCIES:
        recency = "week"
    campaign_type = _normalize_campaign_type(payload.get("campaign_type"), fallback_mode=mode)
    content_type = _normalize_content_type(payload.get("content_type"), campaign_type=campaign_type, platform=platforms[0] if platforms else None)
    status = str(payload.get("status") or ("active" if bool(payload.get("enabled", True)) else "paused")).strip().lower()
    if status not in {"active", "paused"}:
        status = "active" if bool(payload.get("enabled", True)) else "paused"
    return {
        "name": name,
        "enabled": bool(payload.get("enabled", True)),
        "status": status,
        "mode": mode,
        "campaign_type": campaign_type,
        "content_type": content_type,
        "schedule_config": _load_object(payload.get("schedule_config") or payload.get("schedule_config_json")),
        "weekdays_only": bool(payload.get("weekdays_only", True)),
        "run_time": _truncate(str(payload.get("run_time") or "").strip(), 20) or None,
        "timezone": _truncate(str(payload.get("timezone") or "America/Los_Angeles").strip(), 80) or "America/Los_Angeles",
        "recipient_email": _truncate(str(payload.get("recipient_email") or ai_growth_recipient()).strip(), 240) or ai_growth_recipient(),
        "source_type": _truncate(str(payload.get("source_type") or "").strip(), 80) or None,
        "source_reference_id": _truncate(str(payload.get("source_reference_id") or "").strip(), 200) or None,
        "filters": _load_object(payload.get("filters") or payload.get("filters_json")),
        "output_preferences": _load_object(payload.get("output_preferences") or payload.get("output_preferences_json")),
        "created_by": _int_or_none(payload.get("created_by")),
        "updated_by": _int_or_none(payload.get("updated_by")),
        "platforms": platforms,
        "keywords": _normalized_string_list(payload.get("keywords")),
        "tickers": _normalized_tickers(payload.get("tickers")),
        "subreddits": _normalized_subreddits(payload.get("subreddits")),
        "query_templates": _normalized_query_templates(payload.get("query_templates")),
        "minimum_relevance_score": minimum_relevance_score,
        "max_items_per_run": max_items_per_run,
        "recency": recency,
        "default_destination_page": _walnut_url_or_default(str(payload.get("default_destination_page") or "")),
        "include_disclosure": bool(payload.get("include_disclosure", True)),
        "scheduled_digest_enabled": bool(payload.get("scheduled_digest_enabled", False)),
    }


def create_campaign(db: Session, payload: dict[str, Any]) -> AiMarketingCampaign:
    normalized = normalize_campaign_input(payload)
    campaign = AiMarketingCampaign(
        name=normalized["name"],
        enabled=normalized["enabled"],
        status=normalized["status"],
        mode=normalized["mode"],
        campaign_type=normalized["campaign_type"],
        content_type=normalized["content_type"],
        schedule_config_json=_dump_object(normalized["schedule_config"]),
        weekdays_only=normalized["weekdays_only"],
        run_time=normalized["run_time"],
        timezone=normalized["timezone"],
        recipient_email=normalized["recipient_email"],
        source_type=normalized["source_type"],
        source_reference_id=normalized["source_reference_id"],
        filters_json=_dump_object(normalized["filters"]),
        output_preferences_json=_dump_object(normalized["output_preferences"]),
        created_by=normalized["created_by"],
        updated_by=normalized["updated_by"],
        platforms_json=_dump_list(normalized["platforms"]),
        keywords_json=_dump_list(normalized["keywords"]),
        tickers_json=_dump_list(normalized["tickers"]),
        subreddits_json=_dump_list(normalized["subreddits"]),
        query_templates_json=_dump_list(normalized["query_templates"]),
        minimum_relevance_score=normalized["minimum_relevance_score"],
        max_items_per_run=normalized["max_items_per_run"],
        recency=normalized["recency"],
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
    campaign.status = normalized["status"]
    campaign.mode = normalized["mode"]
    campaign.campaign_type = normalized["campaign_type"]
    campaign.content_type = normalized["content_type"]
    campaign.schedule_config_json = _dump_object(normalized["schedule_config"])
    campaign.weekdays_only = normalized["weekdays_only"]
    campaign.run_time = normalized["run_time"]
    campaign.timezone = normalized["timezone"]
    campaign.recipient_email = normalized["recipient_email"]
    campaign.source_type = normalized["source_type"]
    campaign.source_reference_id = normalized["source_reference_id"]
    campaign.filters_json = _dump_object(normalized["filters"])
    campaign.output_preferences_json = _dump_object(normalized["output_preferences"])
    campaign.updated_by = normalized["updated_by"]
    campaign.platforms_json = _dump_list(normalized["platforms"])
    campaign.keywords_json = _dump_list(normalized["keywords"])
    campaign.tickers_json = _dump_list(normalized["tickers"])
    campaign.subreddits_json = _dump_list(normalized["subreddits"])
    campaign.query_templates_json = _dump_list(normalized["query_templates"])
    campaign.minimum_relevance_score = normalized["minimum_relevance_score"]
    campaign.max_items_per_run = normalized["max_items_per_run"]
    campaign.recency = normalized["recency"]
    campaign.default_destination_page = normalized["default_destination_page"]
    campaign.include_disclosure = normalized["include_disclosure"]
    campaign.scheduled_digest_enabled = normalized["scheduled_digest_enabled"]
    db.commit()
    db.refresh(campaign)
    return campaign


def campaign_to_dict(campaign: AiMarketingCampaign) -> dict[str, Any]:
    campaign_type = _normalize_campaign_type(campaign.campaign_type, fallback_mode=campaign.mode)
    content_type = _normalize_content_type(campaign.content_type, campaign_type=campaign_type, platform=(_load_list(campaign.platforms_json) or [None])[0])
    legacy = campaign.mode in LEGACY_CAMPAIGN_MODES or campaign_type == "legacy_outreach_campaign"
    return {
        "id": campaign.id,
        "name": "Legacy Outreach Campaign" if legacy and not campaign.name else campaign.name,
        "display_name": "Legacy Outreach Campaign" if legacy else campaign.name,
        "enabled": bool(campaign.enabled),
        "status": campaign.status or ("active" if campaign.enabled else "paused"),
        "mode": campaign.mode,
        "campaign_type": campaign_type,
        "content_type": content_type,
        "schedule_config": _load_object(campaign.schedule_config_json),
        "weekdays_only": bool(campaign.weekdays_only),
        "run_time": campaign.run_time,
        "timezone": campaign.timezone or "America/Los_Angeles",
        "recipient_email": campaign.recipient_email or ai_growth_recipient(),
        "source_type": campaign.source_type,
        "source_reference_id": campaign.source_reference_id,
        "filters": _load_object(campaign.filters_json),
        "output_preferences": _load_object(campaign.output_preferences_json),
        "created_by": campaign.created_by,
        "updated_by": campaign.updated_by,
        "last_run_at": _iso(campaign.last_run_at),
        "next_run_at": _iso(campaign.next_run_at),
        "legacy": legacy,
        "platforms": _load_list(campaign.platforms_json),
        "keywords": _load_list(campaign.keywords_json),
        "tickers": _load_list(campaign.tickers_json),
        "subreddits": _load_list(campaign.subreddits_json),
        "query_templates": _load_list(campaign.query_templates_json),
        "minimum_relevance_score": int(campaign.minimum_relevance_score or 0),
        "max_items_per_run": int(campaign.max_items_per_run or 0),
        "recency": campaign.recency or "week",
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
    campaign_type = _normalize_campaign_type(opportunity.campaign_type)
    content_type = _normalize_content_type(opportunity.content_type, campaign_type=campaign_type, platform=opportunity.platform)
    source_platform = _normalize_source_platform(opportunity.source_platform, fallback=opportunity.platform)
    generated_content = opportunity.generated_content or _generated_content_from_suggestion(suggestion)
    assets = _load_json_list(opportunity.asset_refs_json)
    if suggestion:
        assets = _normalize_assets(assets + _load_json_list(suggestion.assets_json))
    return {
        "id": opportunity.id,
        "campaign_id": opportunity.campaign_id,
        "campaign_type": campaign_type,
        "content_type": content_type,
        "platform": opportunity.platform,
        "source_platform": source_platform,
        "source_provider": opportunity.source_provider,
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
        "ticker_theme": opportunity.ticker_theme,
        "recommended_action": opportunity.recommended_action or (suggestion.recommended_action if suggestion else _default_action_for_content_type(content_type)),
        "matched_keywords": _load_list(opportunity.matched_keywords_json),
        "matched_tickers": _load_list(opportunity.matched_tickers_json),
        "fit_score": opportunity.fit_score if opportunity.fit_score is not None else opportunity.relevance_score,
        "relevance_score": opportunity.relevance_score,
        "spam_risk_score": opportunity.spam_risk_score,
        "intent": opportunity.intent,
        "suggested_destination_url": opportunity.suggested_destination_url,
        "short_reason": opportunity.short_reason,
        "compliance_notes": opportunity.compliance_notes,
        "generated_content": generated_content,
        "full_markdown": opportunity.full_markdown,
        "alternate_versions": _load_object(opportunity.alternate_versions_json),
        "quality_scores": _load_object(opportunity.quality_scores_json),
        "source_notes": _load_json_list(opportunity.source_notes_json),
        "missing_data_notes": _load_json_list(opportunity.missing_data_notes_json),
        "assets": assets,
        "posting_links": _posting_links(opportunity, suggestion=suggestion),
        "metadata": _load_object(opportunity.raw_metadata_json),
        "created_at": _iso(opportunity.created_at),
        "updated_at": _iso(opportunity.updated_at),
        "last_seen_at": _iso(opportunity.last_seen_at),
        "emailed_at": _iso(opportunity.emailed_at),
        "opened_at": _iso(opportunity.opened_at),
        "copied_at": _iso(opportunity.copied_at),
        "posted_manually_at": _iso(opportunity.posted_manually_at),
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
        "campaign_type": suggestion.campaign_type,
        "content_type": suggestion.content_type,
        "platform": suggestion.platform,
        "audience": suggestion.audience,
        "recommended_action": suggestion.recommended_action,
        "reply_angle": suggestion.reply_angle,
        "content_angle": suggestion.content_angle,
        "value_added_insight": suggestion.value_added_insight,
        "walnut_feature_to_mention": suggestion.walnut_feature_to_mention,
        "suggested_destination_url": suggestion.suggested_destination_url,
        "suggested_reply": suggestion.suggested_reply,
        "suggested_post": suggestion.suggested_post,
        "suggested_ad_variants": _load_json_list(suggestion.suggested_ad_variants_json),
        "alternate_hooks": _load_list(suggestion.alternate_hooks_json),
        "title_options": _load_list(suggestion.title_options_json),
        "disclosure_text": suggestion.disclosure_text,
        "assets": _load_json_list(suggestion.assets_json),
        "alternate_reply_more_direct": suggestion.alternate_reply_more_direct,
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


def _generated_content_from_suggestion(suggestion: AiMarketingSuggestion | None) -> str | None:
    if suggestion is None:
        return None
    for value in (
        suggestion.suggested_post,
        suggestion.suggested_reply,
    ):
        if value and value.strip():
            return value
    variants = _load_json_list(suggestion.suggested_ad_variants_json)
    if variants:
        return "\n\n".join(str(item) for item in variants if str(item or "").strip()) or None
    return None


def _posting_links(opportunity: AiMarketingOpportunity, *, suggestion: AiMarketingSuggestion | None = None) -> dict[str, str | None]:
    metadata = _load_object(opportunity.raw_metadata_json)
    source_platform = _normalize_source_platform(opportunity.source_platform, fallback=opportunity.platform)
    content_type = _normalize_content_type(opportunity.content_type, campaign_type=opportunity.campaign_type, platform=opportunity.platform)
    source_url = opportunity.source_url if opportunity.source_url and opportunity.source_url != MANUAL_SOURCE_URL else None
    destination = (
        suggestion.suggested_destination_url
        if suggestion and suggestion.suggested_destination_url
        else opportunity.suggested_destination_url
    )
    subreddit = str(metadata.get("subreddit") or opportunity.community or "").strip().lstrip("r/")
    x_text = _generated_content_from_suggestion(suggestion) or opportunity.generated_content or ""
    links: dict[str, str | None] = {
        "open_source_post": source_url,
        "open_walnut_link": destination or None,
        "open_x": "https://x.com/home",
        "open_x_compose": f"https://x.com/intent/post?{urlencode({'text': x_text[:260]})}" if content_type == "x_post" else None,
        "open_reddit": "https://www.reddit.com/",
        "open_reddit_thread": source_url if source_url and "reddit.com" in source_url.lower() else None,
        "open_reddit_submit": f"https://www.reddit.com/r/{subreddit}/submit" if subreddit and content_type == "reddit_thread" else None,
    }
    return links


def run_campaign(db: Session, campaign: AiMarketingCampaign) -> dict[str, Any]:
    warnings: list[str] = []
    if not campaign.enabled or str(campaign.status or "active").lower() == "paused":
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
    if WEB_SEARCH_REDDIT_SOURCE_PROVIDER in platforms:
        try:
            items.extend(WebSearchRedditSourceAdapter(db).search(campaign))
        except MissingMarketingCredential as exc:
            warnings.append(str(exc))
        except Exception:
            logger.exception("ai_marketing_web_search_reddit_failed campaign_id=%s", campaign.id)
            warnings.append(OPENAI_WEB_SEARCH_FAILED_MESSAGE)

    if "x_stub" in platforms:
        warnings.append("X is configured as a future official API stub only; no X discovery ran.")
    if "facebook_manual" in platforms:
        warnings.append("Facebook is manual URL mode only; no Facebook discovery ran.")

    created = 0
    deduped = 0
    suggested = 0
    opportunities: list[AiMarketingOpportunity] = []
    run_limit = max(1, int(campaign.max_items_per_run or 10))
    if _normalize_content_type(campaign.content_type, campaign_type=campaign.campaign_type, platform=(_load_list(campaign.platforms_json) or [None])[0]) == "reddit_thread":
        run_limit = min(run_limit, 1)
    for item in items[:run_limit]:
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
    campaign.last_run_at = datetime.now(timezone.utc)
    db.commit()
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
    fallback_mode = campaign.mode if campaign else None
    campaign_type = _normalize_campaign_type(item.campaign_type, fallback_mode=fallback_mode)
    content_type = _normalize_content_type(item.content_type, campaign_type=campaign_type, platform=item.platform)
    recommended_action = item.recommended_action or _default_action_for_content_type(content_type)
    source_platform = _normalize_source_platform(item.source_platform, fallback=item.platform)

    opportunity = db.execute(
        select(AiMarketingOpportunity).where(
            AiMarketingOpportunity.platform == item.platform,
            AiMarketingOpportunity.source_dedupe_key == source_dedupe_key,
        )
    ).scalar_one_or_none()
    if opportunity:
        opportunity.campaign_id = campaign.id if campaign else opportunity.campaign_id
        opportunity.source_provider = item.source_provider or opportunity.source_provider
        opportunity.title = _truncate(item.title, 500) or opportunity.title
        opportunity.excerpt = _truncate(item.excerpt, 1500)
        opportunity.source_score = item.source_score
        opportunity.comment_count = item.comment_count
        opportunity.last_seen_at = now
        opportunity.campaign_type = campaign_type
        opportunity.content_type = content_type
        opportunity.source_platform = source_platform
        opportunity.ticker_theme = _truncate(item.ticker_theme, 240)
        opportunity.recommended_action = recommended_action
        opportunity.fit_score = item.fit_score
        opportunity.generated_content = _truncate(item.generated_content, 5000)
        opportunity.alternate_versions_json = _dump_object(item.alternate_versions or _load_object(opportunity.alternate_versions_json))
        opportunity.asset_refs_json = _dump_json_list(_normalize_assets(item.assets) or _load_json_list(opportunity.asset_refs_json))
        opportunity.matched_keywords_json = _dump_list(matched_keywords)
        opportunity.matched_tickers_json = _dump_list(matched_tickers)
        opportunity.raw_metadata_json = _dump_object(item.metadata or {})
        db.commit()
        db.refresh(opportunity)
        return opportunity, False

    opportunity = AiMarketingOpportunity(
        campaign_id=campaign.id if campaign else None,
        platform=item.platform,
        source_provider=_truncate(item.source_provider, 100),
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
        campaign_type=campaign_type,
        content_type=content_type,
        source_platform=source_platform,
        ticker_theme=_truncate(item.ticker_theme, 240),
        recommended_action=recommended_action,
        fit_score=item.fit_score,
        matched_keywords_json=_dump_list(matched_keywords),
        matched_tickers_json=_dump_list(matched_tickers),
        generated_content=_truncate(item.generated_content, 5000),
        alternate_versions_json=_dump_object(item.alternate_versions or {}),
        asset_refs_json=_dump_json_list(_normalize_assets(item.assets)),
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
    source_platform: str | None = None,
    ticker_theme: str | None = None,
    desired_output_type: str | None = None,
    destination_url: str | None = None,
    campaign_type: str | None = None,
    content_type: str | None = None,
    assets: list[dict[str, Any]] | None = None,
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

    platform = _platform_from_url(normalized_url) if normalized_url else _normalize_source_platform(source_platform, fallback="other")
    source_platform_normalized = _normalize_source_platform(source_platform, fallback=platform)
    resolved_campaign_type = _normalize_campaign_type(campaign_type or (campaign.campaign_type if campaign else None), fallback_mode=campaign.mode if campaign else "manual_research_input")
    resolved_content_type = _normalize_content_type(
        content_type,
        campaign_type=resolved_campaign_type,
        desired_output_type=desired_output_type,
        platform=source_platform_normalized,
    )
    destination = _walnut_url_or_default(str(destination_url or campaign.default_destination_page if campaign else destination_url or ""))
    source_url = normalized_url or MANUAL_SOURCE_URL
    source_key = normalized_url or f"manual:text:{_dedupe_key(manual_text)}"
    source_item = SourceItem(
        platform=platform,
        source_id=f"manual:{_dedupe_key(source_key)}",
        source_url=source_url,
        source_provider="admin_manual_text",
        campaign_type=resolved_campaign_type,
        content_type=resolved_content_type,
        source_platform=source_platform_normalized,
        ticker_theme=ticker_theme,
        recommended_action=_default_action_for_content_type(resolved_content_type),
        assets=assets,
        title=title or "Manual Research Input",
        excerpt=manual_text,
        metadata={
            "manual": True,
            "source": "admin_manual_research_input",
            "source_url_provided": bool(normalized_url),
            "desired_output_type": desired_output_type,
            "ticker_theme": ticker_theme,
            "suggested_destination_url": destination,
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


def create_growth_draft(
    db: Session,
    *,
    campaign_type: str,
    content_type: str | None = None,
    source_platform: str | None = None,
    title: str | None = None,
    text: str | None = None,
    ticker_theme: str | None = None,
    destination_url: str | None = None,
    audience: str | None = None,
    tone: str | None = None,
    assets: list[dict[str, Any]] | None = None,
    inputs: dict[str, Any] | None = None,
    generate: bool = True,
) -> dict[str, Any]:
    if any("influencer" in str(value or "").strip().lower() for value in (campaign_type, content_type)):
        raise ValueError("Influencer growth workflows have been removed. Use X campaigns or Reddit Research Threads.")
    resolved_campaign_type = _normalize_campaign_type(campaign_type, fallback_mode=campaign_type)
    resolved_content_type = _normalize_content_type(content_type, campaign_type=resolved_campaign_type, platform=source_platform)
    resolved_platform = _normalize_source_platform(source_platform, fallback=_platform_for_content_type(resolved_content_type))
    destination = _walnut_url_or_default(str(destination_url or ""))
    draft_context = _growth_context_text(
        campaign_type=resolved_campaign_type,
        content_type=resolved_content_type,
        text=text,
        ticker_theme=ticker_theme,
        audience=audience,
        tone=tone,
        inputs=inputs or {},
    )
    source_key = f"growth:{resolved_campaign_type}:{resolved_content_type}:{_dedupe_key(draft_context)}"
    source_item = SourceItem(
        platform=resolved_platform,
        source_id=source_key,
        source_url=MANUAL_SOURCE_URL,
        source_provider="admin_growth_form",
        campaign_type=resolved_campaign_type,
        content_type=resolved_content_type,
        source_platform=resolved_platform,
        ticker_theme=ticker_theme,
        recommended_action=_default_action_for_content_type(resolved_content_type),
        assets=assets,
        title=title or _default_growth_title(resolved_campaign_type, ticker_theme),
        excerpt=draft_context,
        metadata={
            "manual": True,
            "source": "admin_growth_form",
            "inputs": inputs or {},
            "audience": audience,
            "tone": tone,
            "suggested_destination_url": destination,
            "source_url_provided": False,
        },
    )
    opportunity, _was_created = upsert_source_item(db, None, source_item)
    warning: str | None = None
    if generate:
        if resolved_setting_value(db, OPENAI_API_KEY):
            try:
                generate_suggestion(db, opportunity)
            except OpenAISuggestionError as exc:
                warning = exc.admin_message
        else:
            warning = "OpenAI API key missing; growth draft was saved without an AI suggestion."
            _record_suggestion_failure(db, opportunity, OPENAI_MISSING_KEY_MESSAGE, code="missing_key")
    latest = latest_suggestions_by_opportunity(db, [opportunity.id]).get(opportunity.id)
    return {"opportunity": opportunity_to_dict(opportunity, suggestion=latest), "warning": warning}


def regenerate_growth_draft(
    db: Session,
    opportunity: AiMarketingOpportunity,
    *,
    change_request: str | None = None,
) -> dict[str, Any]:
    change_text = _truncate(str(change_request or "").strip(), 1000) or ""
    if change_text:
        metadata = _load_object(opportunity.raw_metadata_json)
        history = _coerce_json_list(metadata.get("change_requests"))
        history.append({"requested_at": datetime.now(timezone.utc).isoformat(), "request": change_text})
        metadata["change_request"] = change_text
        metadata["change_requests"] = history[-10:]
        opportunity.raw_metadata_json = _dump_object(metadata)
    opportunity.status = "needs_review"
    opportunity.updated_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(opportunity)
    generate_suggestion(db, opportunity)
    latest = latest_suggestions_by_opportunity(db, [opportunity.id]).get(opportunity.id)
    return opportunity_to_dict(opportunity, suggestion=latest)


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
    now = datetime.now(timezone.utc)
    if opportunity.status == "emailed":
        opportunity.emailed_at = now
    elif opportunity.status == "opened":
        opportunity.opened_at = now
    elif opportunity.status == "copied":
        opportunity.copied_at = now
    elif opportunity.status == "posted_manually":
        opportunity.posted_manually_at = now
    opportunity.updated_at = now
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
    opportunity_metadata = _load_object(opportunity.raw_metadata_json)
    campaign_type = _normalize_campaign_type(opportunity.campaign_type or (campaign.campaign_type if campaign else None), fallback_mode=campaign.mode if campaign else None)
    content_type = _normalize_content_type(opportunity.content_type, campaign_type=campaign_type, platform=platform)
    source_platform = _normalize_source_platform(opportunity.source_platform, fallback=platform)
    destination_hint = recommended_destination_url(
        mode=campaign_type if campaign_type != "legacy_outreach_campaign" else (campaign.mode if campaign else "manual_url_review"),
        platform=platform,
        campaign_id=campaign.id if campaign else opportunity.campaign_id or 0,
        tickers=_load_list(opportunity.matched_tickers_json),
        fallback=str(opportunity_metadata.get("suggested_destination_url") or (campaign.default_destination_page if campaign else DEFAULT_DESTINATION_URL)),
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
                            "campaign_type": campaign_type,
                            "content_type": content_type,
                            "platform": opportunity.platform,
                            "source_platform": source_platform,
                            "source_provider": opportunity.source_provider,
                            "source_url": opportunity.source_url,
                            "title": opportunity.title,
                            "excerpt": opportunity.excerpt,
                            "ticker_theme": opportunity.ticker_theme,
                            "recommended_action": opportunity.recommended_action,
                            "author": opportunity.author,
                            "community": opportunity.community,
                            "score": opportunity.source_score,
                            "comment_count": opportunity.comment_count,
                            "created_at": _iso(opportunity.source_created_at),
                            "matched_keywords": _load_list(opportunity.matched_keywords_json),
                            "matched_tickers": _load_list(opportunity.matched_tickers_json),
                            "metadata": {
                                "web_search_provider": opportunity_metadata.get("web_search_provider"),
                                "discovery_query": opportunity_metadata.get("query"),
                                "snippet_only": opportunity_metadata.get("snippet_only"),
                                "snippet_character_count": opportunity_metadata.get("snippet_character_count"),
                                "needs_manual_review": opportunity_metadata.get("needs_manual_review"),
                                "inputs": opportunity_metadata.get("inputs"),
                                "audience": opportunity_metadata.get("audience"),
                                "tone": opportunity_metadata.get("tone"),
                                "desired_output_type": opportunity_metadata.get("desired_output_type"),
                                "change_request": opportunity_metadata.get("change_request"),
                                "change_requests": opportunity_metadata.get("change_requests"),
                            },
                            "assets": _load_json_list(opportunity.asset_refs_json),
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
    structured = _normalize_suggestion_payload(
        json.loads(content),
        destination_hint,
        platform,
        campaign.id if campaign else 0,
        opportunity=opportunity,
    )
    suggestion = AiMarketingSuggestion(
        opportunity_id=opportunity.id,
        campaign_id=campaign.id if campaign else opportunity.campaign_id,
        model=model,
        relevance_score=structured["relevance_score"],
        spam_risk_score=structured["spam_risk_score"],
        detected_tickers_json=_dump_list(structured["detected_tickers"]),
        intent=structured["intent"],
        campaign_type=structured["campaign_type"],
        content_type=structured["content_type"],
        platform=structured["platform"],
        audience=structured["audience"],
        recommended_action=structured["recommended_action"],
        reply_angle=structured["reply_angle"],
        content_angle=structured["content_angle"],
        value_added_insight=structured["value_added_insight"],
        walnut_feature_to_mention=structured["walnut_feature_to_mention"],
        suggested_destination_url=structured["suggested_destination_url"],
        suggested_reply=structured["suggested_reply"],
        suggested_post=structured["suggested_post"],
        suggested_ad_variants_json=_dump_json_list(structured["suggested_ad_variants"]),
        alternate_hooks_json=_dump_list(structured["alternate_hooks"]),
        title_options_json=_dump_list(structured["title_options"]),
        disclosure_text=structured["disclosure_text"],
        assets_json=_dump_json_list(structured["assets"]),
        alternate_reply_more_direct=structured["alternate_reply_more_direct"],
        short_reason=structured["short_reason"],
        compliance_notes=structured["compliance_notes"],
        prompt_version=AI_MARKETING_PROMPT_VERSION,
        raw_response_json=_dump_object(data),
    )
    db.add(suggestion)
    opportunity.relevance_score = suggestion.relevance_score
    opportunity.fit_score = suggestion.relevance_score
    opportunity.spam_risk_score = suggestion.spam_risk_score
    opportunity.intent = suggestion.intent
    opportunity.campaign_type = suggestion.campaign_type
    opportunity.content_type = suggestion.content_type
    opportunity.source_platform = _normalize_source_platform(suggestion.platform, fallback=opportunity.platform)
    opportunity.recommended_action = suggestion.recommended_action
    opportunity.suggested_destination_url = suggestion.suggested_destination_url
    opportunity.short_reason = suggestion.short_reason
    opportunity.compliance_notes = suggestion.compliance_notes
    opportunity.generated_content = structured["generated_content"]
    opportunity.full_markdown = structured["full_markdown"]
    opportunity.alternate_versions_json = _dump_object(structured["alternate_versions"])
    opportunity.quality_scores_json = _dump_object(structured["quality_scores"])
    opportunity.source_notes_json = _dump_json_list(structured["source_notes"])
    opportunity.missing_data_notes_json = _dump_json_list(structured["missing_data_notes"])
    opportunity.asset_refs_json = _dump_json_list(_normalize_assets(_load_json_list(opportunity.asset_refs_json) + structured["assets"]))
    if structured["content_type"] == "reddit_thread":
        opportunity.status = "new" if structured["quality_gate_passed"] else "regeneration_needed"
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
        "to_email": ai_growth_recipient(),
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
        to_email=ai_growth_recipient(),
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
        to_email=ai_growth_recipient(),
        subject=context["subject"],
        opportunity_ids_json=_dump_list([str(row.id) for row in opportunities]),
        status=status,
        payload_json=_dump_object({"count": len(opportunities), "delivery": result}),
        sent_at=sent_at,
    )
    db.add(log)
    now = datetime.now(timezone.utc)
    for opportunity in opportunities:
        if opportunity.status in {"new", "draft", "needs_review", "approved"}:
            opportunity.status = "emailed"
            opportunity.emailed_at = now
            opportunity.updated_at = now
    db.commit()
    db.refresh(log)
    return {
        "delivery": result,
        "email_log": email_log_to_dict(log),
        "count": len(opportunities),
        "items": [opportunity_to_dict(row, suggestion=latest.get(row.id)) for row in opportunities],
    }


def send_draft_email(
    db: Session,
    opportunity: AiMarketingOpportunity,
    *,
    admin_user_id: int | None = None,
) -> dict[str, Any]:
    return send_digest(db, opportunity_ids=[opportunity.id], statuses=None, limit=1, admin_user_id=admin_user_id)


def mark_opportunity_copied(db: Session, opportunity: AiMarketingOpportunity) -> AiMarketingOpportunity:
    now = datetime.now(timezone.utc)
    opportunity.status = "copied"
    opportunity.copied_at = now
    opportunity.updated_at = now
    db.commit()
    db.refresh(opportunity)
    return opportunity


def mark_opportunity_posted(db: Session, opportunity: AiMarketingOpportunity) -> AiMarketingOpportunity:
    now = datetime.now(timezone.utc)
    opportunity.status = "posted_manually"
    opportunity.posted_manually_at = now
    opportunity.updated_at = now
    db.commit()
    db.refresh(opportunity)
    return opportunity


def mark_opportunity_opened(db: Session, opportunity: AiMarketingOpportunity) -> AiMarketingOpportunity:
    now = datetime.now(timezone.utc)
    opportunity.status = "opened" if opportunity.status in {"new", "draft", "needs_review", "emailed"} else opportunity.status
    opportunity.opened_at = now
    opportunity.updated_at = now
    db.commit()
    db.refresh(opportunity)
    return opportunity


def archive_opportunity(db: Session, opportunity: AiMarketingOpportunity) -> AiMarketingOpportunity:
    return update_opportunity_status(db, opportunity, status="archived")


def reject_opportunity(db: Session, opportunity: AiMarketingOpportunity) -> AiMarketingOpportunity:
    return update_opportunity_status(db, opportunity, status="rejected")


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


def web_search_provider_status(db: Session | None = None) -> dict[str, Any]:
    missing: list[str] = []
    if not _env_flag_enabled(OPENAI_WEB_SEARCH_ENABLED):
        missing.append(OPENAI_WEB_SEARCH_ENABLED)
    if not resolved_setting_value(db, OPENAI_API_KEY):
        missing.append(OPENAI_API_KEY)
    return {
        "configured": not missing,
        "provider": "openai_web_search",
        "missing": missing,
    }


def resolve_web_search_provider(db: Session | None = None) -> "WebSearchProvider":
    status = web_search_provider_status(db)
    if not status["configured"]:
        raise MissingMarketingCredential(OPENAI_WEB_SEARCH_NOT_CONFIGURED_MESSAGE)
    api_key = resolved_setting_value(db, OPENAI_API_KEY)
    if not api_key:
        raise MissingMarketingCredential(OPENAI_WEB_SEARCH_NOT_CONFIGURED_MESSAGE)
    return OpenAIWebSearchProvider(api_key=api_key, model=marketing_model(db))


class WebSearchProvider:
    provider_name = "web_search"

    def search(self, query: str, *, max_results: int, recency: str = "week") -> list[WebSearchResult]:
        raise NotImplementedError


class OpenAIWebSearchProvider(WebSearchProvider):
    provider_name = "openai_web_search"
    endpoint = "https://api.openai.com/v1/responses"

    def __init__(self, *, api_key: str, model: str) -> None:
        self.api_key = api_key
        self.model = model

    def search(self, query: str, *, max_results: int, recency: str = "week") -> list[WebSearchResult]:
        result_limit = max(1, min(int(max_results or 10), 10))
        prompt = _openai_web_search_prompt(query, max_results=result_limit, recency=recency)
        request_payload = {
            "model": self.model,
            "tools": [{"type": "web_search"}],
            "input": prompt,
            "store": False,
        }
        response = requests.post(
            self.endpoint,
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            },
            json=request_payload,
            timeout=45,
        )
        if response.status_code >= 400:
            raise RuntimeError("OpenAI web search request failed.")
        data = response.json()
        return _parse_openai_web_search_results(data, max_results=result_limit, provider=self.provider_name)


def _openai_web_search_prompt(query: str, *, max_results: int, recency: str) -> str:
    recency_hint = {
        "day": "Prefer results from the past day when available.",
        "week": "Prefer results from the past week when available.",
        "month": "Prefer results from the past month when available.",
        "any": "Use the most relevant recent public results available.",
    }.get(str(recency or "week").lower(), "Prefer recent public results when available.")
    return "\n".join(
        [
            "You are Walnut's AI Growth Engine web discovery researcher.",
            "Use web search to find public market discussions, recent ticker/news context, and places where a human-reviewed Walnut reply or post could add useful context.",
            "Do not scrape Reddit pages directly. Reddit URLs are allowed only when surfaced by web search as title, URL, and snippet/citation context.",
            "Return only valid JSON with this shape:",
            '{"results":[{"title":"Result title","url":"https://example.com","snippet":"One concise sentence of relevant context."}]}',
            f"Return at most {max_results} results.",
            recency_hint,
            f"Search query: {query}",
        ]
    )


def _parse_openai_web_search_results(data: dict[str, Any], *, max_results: int, provider: str) -> list[WebSearchResult]:
    text = _extract_responses_text_content(data)
    parsed = _extract_json_payload(text)
    candidates: list[dict[str, Any]] = []
    if isinstance(parsed, dict):
        raw_results = parsed.get("results")
        if isinstance(raw_results, list):
            candidates.extend(item for item in raw_results if isinstance(item, dict))
    elif isinstance(parsed, list):
        candidates.extend(item for item in parsed if isinstance(item, dict))

    results = _normalize_web_search_candidates(candidates, provider=provider)
    if not results:
        results = _extract_openai_web_search_citations(data, provider=provider)
    return _dedupe_web_search_results(results)[: max(1, max_results)]


def _extract_responses_text_content(data: dict[str, Any]) -> str:
    output_text = data.get("output_text")
    if isinstance(output_text, str) and output_text.strip():
        return output_text.strip()
    texts: list[str] = []
    for item in _walk_dicts(data):
        item_type = str(item.get("type") or "")
        text = item.get("text")
        if item_type in {"output_text", "text"} and isinstance(text, str) and text.strip():
            texts.append(text.strip())
    return "\n".join(_dedupe_strings(texts))


def _extract_json_payload(text: str) -> Any:
    stripped = (text or "").strip()
    if not stripped:
        return None
    fenced = re.search(r"```(?:json)?\s*(.*?)```", stripped, flags=re.IGNORECASE | re.DOTALL)
    candidates = [fenced.group(1).strip()] if fenced else []
    candidates.append(stripped)
    start = stripped.find("{")
    end = stripped.rfind("}")
    if start >= 0 and end > start:
        candidates.append(stripped[start : end + 1])
    for candidate in candidates:
        try:
            return json.loads(candidate)
        except (TypeError, ValueError):
            continue
    return None


def _normalize_web_search_candidates(candidates: list[dict[str, Any]], *, provider: str) -> list[WebSearchResult]:
    results: list[WebSearchResult] = []
    for item in candidates:
        url = str(item.get("url") or item.get("link") or "").strip()
        title = str(item.get("title") or item.get("name") or "").strip()
        snippet = str(item.get("snippet") or item.get("summary") or item.get("description") or "").strip()
        parsed_url = urlparse(url)
        if parsed_url.scheme not in {"http", "https"} or not parsed_url.netloc:
            continue
        results.append(
            WebSearchResult(
                title=title or parsed_url.netloc,
                url=url,
                snippet=snippet or None,
                provider=provider,
            )
        )
    return results


def _extract_openai_web_search_citations(data: dict[str, Any], *, provider: str) -> list[WebSearchResult]:
    text = _truncate(_extract_responses_text_content(data), 500)
    candidates: list[dict[str, Any]] = []
    for item in _walk_dicts(data):
        item_type = str(item.get("type") or "")
        url = item.get("url")
        if not isinstance(url, str) or not url.strip():
            continue
        if "citation" not in item_type and "title" not in item:
            continue
        candidates.append(
            {
                "url": url,
                "title": item.get("title") or item.get("name") or url,
                "snippet": item.get("snippet") or item.get("summary") or text,
            }
        )
    return _normalize_web_search_candidates(candidates, provider=provider)


def _dedupe_web_search_results(results: list[WebSearchResult]) -> list[WebSearchResult]:
    deduped: list[WebSearchResult] = []
    seen: set[str] = set()
    for result in results:
        key = result.url.rstrip("/").lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(result)
    return deduped


def _walk_dicts(value: Any):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from _walk_dicts(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk_dicts(child)


class WebSearchRedditSourceAdapter:
    def __init__(self, db: Session | None = None) -> None:
        self.db = db

    def search(self, campaign: AiMarketingCampaign) -> list[SourceItem]:
        provider = resolve_web_search_provider(self.db)
        queries = self._queries_for_campaign(campaign)
        if not queries:
            return []

        max_items = max(1, min(int(campaign.max_items_per_run or 10), 50))
        items: list[SourceItem] = []
        seen_urls: set[str] = set()
        discovered_at = datetime.now(timezone.utc)
        recency = campaign.recency or "week"
        for query in queries:
            if len(items) >= max_items:
                break
            remaining = max_items - len(items)
            for result in provider.search(query, max_results=remaining, recency=recency):
                normalized_url = _normalize_reddit_search_result_url(result.url)
                if not normalized_url or normalized_url in seen_urls:
                    continue
                seen_urls.add(normalized_url)
                snippet = _truncate(result.snippet, 800)
                needs_manual_review = len(snippet or "") < SHORT_SEARCH_SNIPPET_THRESHOLD
                items.append(
                    SourceItem(
                        platform="reddit",
                        source_provider=WEB_SEARCH_REDDIT_SOURCE_PROVIDER,
                        source_id=normalized_url,
                        source_url=normalized_url,
                        title=result.title,
                        excerpt=snippet,
                        community=_subreddit_from_reddit_url(normalized_url),
                        metadata={
                            "source_provider": WEB_SEARCH_REDDIT_SOURCE_PROVIDER,
                            "web_search_provider": result.provider,
                            "query": query,
                            "snippet": snippet,
                            "snippet_only": True,
                            "snippet_character_count": len(snippet or ""),
                            "needs_manual_review": needs_manual_review,
                            "manual_review_reason": "short search-provider snippet" if needs_manual_review else None,
                            "discovered_at": _iso(discovered_at),
                            "stored_fields": ["title", "url", "snippet", "source/provider", "discovered_at"],
                            "compliance": "Search-provider snippets and URLs only; Reddit page HTML was not fetched.",
                        },
                    )
                )
                if len(items) >= max_items:
                    break
        return items

    @staticmethod
    def _queries_for_campaign(campaign: AiMarketingCampaign) -> list[str]:
        subreddits = _load_list(campaign.subreddits_json) or DEFAULT_WEB_SEARCH_REDDIT_SUBREDDITS
        keywords = _load_list(campaign.keywords_json)
        tickers = _load_list(campaign.tickers_json)
        templates = _load_list(campaign.query_templates_json) or DEFAULT_WEB_SEARCH_REDDIT_QUERY_TEMPLATES
        queries: list[str] = []
        for subreddit in subreddits:
            for keyword in keywords:
                queries.extend(
                    _render_search_query_template(template, subreddit=subreddit, keyword=keyword, ticker="", term=keyword)
                    for template in templates
                    if "{ticker}" not in template
                )
            for ticker in tickers:
                queries.extend(
                    _render_search_query_template(template, subreddit=subreddit, keyword="", ticker=ticker, term=ticker)
                    for template in templates
                    if "{keyword}" not in template
                )
        return [query for query in _dedupe_strings(queries) if query][:50]


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
                        source_provider="reddit_api",
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


def _growth_email_subject(opportunities: list[AiMarketingOpportunity], latest: dict[int, AiMarketingSuggestion]) -> str:
    count = len(opportunities)
    if count == 1 and opportunities:
        content_type = _normalize_content_type(opportunities[0].content_type, campaign_type=opportunities[0].campaign_type, platform=opportunities[0].platform)
        labels = {
            "x_post": "X campaign draft",
            "reddit_reply": "Reddit reply opportunity",
            "reddit_thread": "Reddit research thread draft",
            "paid_ad": "paid ad idea",
        }
        return f"Walnut AI Growth: {labels.get(content_type, 'draft')} ready"
    content_counts: dict[str, int] = {}
    for opportunity in opportunities:
        content_type = _normalize_content_type(opportunity.content_type, campaign_type=opportunity.campaign_type, platform=opportunity.platform)
        content_counts[content_type] = content_counts.get(content_type, 0) + 1
    if content_counts.get("x_post") == count:
        return f"Walnut AI Growth: {count} X campaign drafts ready"
    if content_counts.get("reddit_reply") == count:
        return f"Walnut AI Growth: {count} reply opportunities"
    return f"Walnut AI Growth: {count} drafts ready"


def _content_type_label(content_type: str) -> str:
    return {
        "x_post": "X post",
        "reddit_reply": "Reddit reply",
        "reddit_thread": "Reddit research thread",
        "paid_ad": "Paid ad copy",
    }.get(content_type, content_type.replace("_", " ").title())


def _source_platform_label(platform: str) -> str:
    return {"x": "X", "reddit": "Reddit", "facebook": "Facebook", "linkedin": "LinkedIn", "other": "Other"}.get(platform, platform.title())


def _draft_admin_url(opportunity_id: int) -> str:
    return f"https://walnutmarkets.com/admin/ai-marketing?draft={opportunity_id}"


def _default_disclosure_reminder(source_platform: str, content: str) -> str:
    if "walnut" in (content or "").lower() and source_platform in {"x", "reddit"}:
        return "Disclose Walnut affiliation naturally before posting."
    return "Human review required. No auto-posting."


def _assets_text(assets: list[dict[str, Any]]) -> str:
    if not assets:
        return "Assets to attach: none"
    lines = ["Assets to attach:"]
    for index, asset in enumerate(assets, start=1):
        lines.append(f"{index}. {asset.get('title') or 'Asset'} ({asset.get('asset_type') or 'asset'}): {asset.get('url') or asset.get('thumbnail_url') or 'no link'}")
        caption = str(asset.get("suggested_caption") or "").strip()
        if caption:
            lines.append(f"   Caption: {caption}")
    return "\n".join(lines)


def _assets_html(assets: list[dict[str, Any]]) -> str:
    if not assets:
        return "<p style=\"margin:0 0 8px 0;color:#334155;\">Assets to attach: none</p>"
    parts = ["<div style=\"margin:10px 0;color:#334155;\"><strong>Assets to attach</strong>"]
    for asset in assets:
        title = html.escape(str(asset.get("title") or "Asset"))
        url = html.escape(str(asset.get("url") or asset.get("thumbnail_url") or ""), quote=True)
        thumb = html.escape(str(asset.get("thumbnail_url") or ""), quote=True)
        caption = html.escape(str(asset.get("suggested_caption") or ""))
        link = f"<a href=\"{url}\">{title}</a>" if url else title
        image = f"<br><img src=\"{thumb}\" alt=\"{title}\" style=\"max-width:180px;height:auto;border-radius:6px;margin-top:6px;\">" if thumb else ""
        parts.append(f"<p style=\"margin:8px 0;\">{link}{image}<br><span>{caption}</span></p>")
    parts.append("</div>")
    return "".join(parts)


def _quality_scores_text(scores: dict[str, Any]) -> str:
    if not scores:
        return "Quality scores: pending"
    ordered = [
        "research_depth_score",
        "evidence_score",
        "catalyst_score",
        "balance_score",
        "reddit_native_score",
        "promotional_risk_score",
        "compliance_risk_score",
    ]
    return "Quality scores: " + ", ".join(f"{key}={scores.get(key, 'pending')}" for key in ordered)


def _quality_scores_html(scores: dict[str, Any]) -> str:
    return f"<p style=\"margin:0 0 8px 0;color:#334155;\">{html.escape(_quality_scores_text(scores))}</p>"


def _notes_text(label: str, notes: list[Any]) -> str:
    clean = [str(note).strip() for note in notes if str(note or "").strip()]
    if not clean:
        return f"{label}: none"
    return f"{label}:\n" + "\n".join(f"- {note}" for note in clean)


def _notes_html(label: str, notes: list[Any]) -> str:
    clean = [str(note).strip() for note in notes if str(note or "").strip()]
    if not clean:
        return f"<p style=\"margin:0 0 8px 0;color:#334155;\"><strong>{html.escape(label)}:</strong> none</p>"
    items = "".join(f"<li>{html.escape(note)}</li>" for note in clean)
    return f"<div style=\"margin:0 0 8px 0;color:#334155;\"><strong>{html.escape(label)}</strong><ul>{items}</ul></div>"


def _digest_context(
    opportunities: list[AiMarketingOpportunity],
    latest: dict[int, AiMarketingSuggestion],
) -> dict[str, Any]:
    count = len(opportunities)
    subject = _growth_email_subject(opportunities, latest)
    items_text: list[str] = []
    items_html: list[str] = []
    for index, opportunity in enumerate(opportunities, start=1):
        suggestion = latest.get(opportunity.id)
        metadata = _load_object(opportunity.raw_metadata_json)
        content_type = _normalize_content_type(opportunity.content_type, campaign_type=opportunity.campaign_type, platform=opportunity.platform)
        source_platform = _normalize_source_platform(opportunity.source_platform, fallback=opportunity.platform)
        draft_content = opportunity.generated_content or _generated_content_from_suggestion(suggestion) or "No AI suggestion generated yet."
        destination = suggestion.suggested_destination_url if suggestion else (opportunity.suggested_destination_url or DEFAULT_DESTINATION_URL)
        reason = (suggestion.short_reason if suggestion else opportunity.short_reason) or "No reasoning summary available."
        snippet = opportunity.excerpt or "none"
        query = str(metadata.get("query") or "manual").strip() or "manual"
        needs_manual_review = bool(metadata.get("needs_manual_review")) or (
            opportunity.source_provider == WEB_SEARCH_REDDIT_SOURCE_PROVIDER
            and len(opportunity.excerpt or "") < SHORT_SEARCH_SNIPPET_THRESHOLD
        )
        manual_review_note = "Needs manual review." if needs_manual_review else ""
        manual_review_html = (
            "<p style=\"margin:0 0 8px 0;color:#92400e;font-weight:600;\">Needs manual review.</p>"
            if needs_manual_review
            else ""
        )
        tickers = ", ".join(_load_list(opportunity.matched_tickers_json)) or "none"
        keywords = ", ".join(_load_list(opportunity.matched_keywords_json)) or "none"
        relevance = suggestion.relevance_score if suggestion else opportunity.relevance_score
        spam = suggestion.spam_risk_score if suggestion else opportunity.spam_risk_score
        quality_scores = _load_object(opportunity.quality_scores_json)
        source_notes = _load_json_list(opportunity.source_notes_json)
        missing_data_notes = _load_json_list(opportunity.missing_data_notes_json)
        action = (suggestion.recommended_action if suggestion else opportunity.recommended_action) or "pending"
        angle = (suggestion.content_angle if suggestion else None) or (suggestion.reply_angle if suggestion else "pending")
        disclosure = suggestion.disclosure_text if suggestion else ""
        compliance = (suggestion.compliance_notes if suggestion else opportunity.compliance_notes) or "Human review required. No auto-posting."
        assets = _normalize_assets(_load_json_list(opportunity.asset_refs_json) + (_load_json_list(suggestion.assets_json) if suggestion else []))
        posting_links = _posting_links(opportunity, suggestion=suggestion)
        admin_url = _draft_admin_url(opportunity.id)
        destination_html = (
            f"<p style=\"margin:0 0 8px 0;\"><a href=\"{html.escape(destination, quote=True)}\">Suggested Walnut link</a></p>"
            if destination
            else "<p style=\"margin:0 0 8px 0;color:#334155;\">Suggested Walnut link: none</p>"
        )
        assets_text = _assets_text(assets)
        assets_html = _assets_html(assets)
        quality_text = _quality_scores_text(quality_scores)
        notes_text = _notes_text("Source notes", source_notes)
        missing_text = _notes_text("Missing data notes", missing_data_notes)
        quality_html = _quality_scores_html(quality_scores)
        notes_html = _notes_html("Source notes", source_notes)
        missing_html = _notes_html("Missing data notes", missing_data_notes)
        checklist = [
            "Open source post",
            "Copy draft",
            "Paste into platform",
            "Attach image if relevant",
            "Review disclosure",
            "Post manually",
        ]
        items_text.append(
            "\n".join(
                [
                    f"{index}. {opportunity.title}",
                    f"Platform: {_source_platform_label(source_platform)}",
                    f"Content type: {_content_type_label(content_type)}",
                    f"Recommended action: {action}",
                    f"Fit score: {relevance if relevance is not None else 'pending'}",
                    f"Spam/compliance risk score: {spam if spam is not None else 'pending'}",
                    quality_text,
                    f"Source URL: {opportunity.source_url}",
                    f"Suggested destination URL: {destination or 'none'}",
                    f"Open in Walnut Admin: {admin_url}",
                    f"Snippet: {snippet}",
                    f"Search query: {query}",
                    f"Matched ticker/keywords: {tickers} / {keywords}",
                    manual_review_note,
                    f"Content angle: {angle}",
                    "Draft content:",
                    draft_content,
                    "Copy-ready markdown:",
                    opportunity.full_markdown or draft_content,
                    f"Disclosure reminder: {disclosure or _default_disclosure_reminder(source_platform, draft_content)}",
                    f"Compliance notes: {compliance}",
                    notes_text,
                    missing_text,
                    assets_text,
                    "Posting checklist:",
                    *[f"{number}. {item}" for number, item in enumerate(checklist, start=1)],
                    f"Reasoning: {reason}",
                ]
            )
        )
        items_html.append(
            "<div style=\"margin:18px 0;padding:14px;border:1px solid #d8e6ea;border-radius:7px;background:#f8fafc;\">"
            f"<h3 style=\"margin:0 0 8px 0;font-size:16px;line-height:22px;color:#0f172a;\">{html.escape(opportunity.title)}</h3>"
            f"<p style=\"margin:0 0 8px 0;color:#475569;\">{html.escape(_source_platform_label(source_platform))} / {html.escape(_content_type_label(content_type))}</p>"
            f"<p style=\"margin:0 0 8px 0;color:#334155;\">Recommended action: {html.escape(action)} | Fit: {html.escape(str(relevance if relevance is not None else 'pending'))} | Spam/compliance risk: {html.escape(str(spam if spam is not None else 'pending'))}</p>"
            f"{quality_html}"
            f"<p style=\"margin:0 0 8px 0;\"><a href=\"{html.escape(opportunity.source_url, quote=True)}\">Open source post</a></p>"
            f"<p style=\"margin:0 0 8px 0;\"><a href=\"{html.escape(admin_url, quote=True)}\">Open in Walnut Admin</a></p>"
            f"<p style=\"margin:0 0 8px 0;color:#334155;\">Snippet: {html.escape(snippet)}</p>"
            f"<p style=\"margin:0 0 8px 0;color:#334155;\">Search query: {html.escape(query)}</p>"
            f"<p style=\"margin:0 0 8px 0;color:#334155;\">Matched ticker/keywords: {html.escape(tickers)} / {html.escape(keywords)}</p>"
            f"<p style=\"margin:0 0 8px 0;color:#334155;\">Content angle: {html.escape(str(angle))}</p>"
            f"{manual_review_html}"
            f"{destination_html}"
            f"<pre style=\"white-space:pre-wrap;margin:10px 0;padding:12px;background:#0f172a;color:#e2e8f0;border-radius:6px;font-size:13px;line-height:18px;\">{html.escape(draft_content)}</pre>"
            f"<p style=\"margin:0 0 6px 0;color:#334155;\"><strong>Copy-ready markdown</strong></p>"
            f"<pre style=\"white-space:pre-wrap;margin:6px 0 10px 0;padding:12px;background:#0b1120;color:#d1fae5;border-radius:6px;font-size:13px;line-height:18px;\">{html.escape(opportunity.full_markdown or draft_content)}</pre>"
            f"<p style=\"margin:0 0 8px 0;color:#334155;\"><strong>Disclosure reminder:</strong> {html.escape(disclosure or _default_disclosure_reminder(source_platform, draft_content))}</p>"
            f"<p style=\"margin:0 0 8px 0;color:#334155;\"><strong>Compliance:</strong> {html.escape(compliance)}</p>"
            f"{notes_html}{missing_html}"
            f"{assets_html}"
            "<ol style=\"margin:12px 0 0 18px;color:#334155;\">"
            + "".join(f"<li>{html.escape(item)}</li>" for item in checklist)
            + "</ol>"
            f"<p style=\"margin:10px 0 0 0;color:#475569;\">{html.escape(reason)}</p>"
            "</div>"
        )
    return {
        "first_name": "Jarod",
        "subject": subject,
        "digest_title": subject,
        "summary": f"{count} AI Growth draft{'s' if count != 1 else ''} ready for human review. No auto-posting was performed.",
        "items_text": "\n\n".join(items_text) if items_text else "No matching opportunities are ready for digest.",
        "items_html": "".join(items_html) if items_html else "<p>No matching opportunities are ready for digest.</p>",
        "digest_url": "https://walnutmarkets.com/admin/ai-marketing",
    }


def _suggestion_system_prompt() -> str:
    return (
        "You draft human-reviewed AI Growth Engine assets for Walnut Market Terminal. "
        "Return only JSON matching the supplied schema. First decide whether the thread deserves a reply at all. "
        "Use recommended_action='skip' when the source is not clearly about investing, markets, public companies, trading, finance, or research tools. "
        "Use recommended_action='skip' or 'monitor' when Walnut cannot add a meaningful, specific angle. "
        "For skip, suggested_reply should be exactly or very close to: 'Skip - not relevant enough.' "
        "For monitor, explain what would make the thread worth replying to later. "
        "If opportunity.metadata.change_request is present, treat it as the highest-priority revision instruction while preserving compliance. "
        f"For x_post, write suggested_post plus alternate_hooks and a chart/report idea in value_added_insight. Keep suggested_post at or under {X_POST_CHARACTER_LIMIT} characters, including links and disclosure. "
        "For reddit_thread, write a serious, comprehensive Reddit-native DD post, not a promotional summary. "
        "A Reddit research thread must include: Title, TL;DR, Why this name came up, Company snapshot, Walnut disclosure stack, "
        "Technical picture, Fundamental picture, Recent news / filings / press releases, Catalysts, Bull case, Bear case / risks, "
        "What would confirm the setup, What would weaken the setup, Bottom line, and Suggested Reddit disclosure. "
        "For reddit_thread, fill the dedicated structured fields and full_reddit_post_markdown; include source_notes, missing_data_notes, "
        "quality_scores, suggested_image_asset, suggested_flair, and suggested_subreddits where appropriate. "
        "For paid_ad, write suggested_ad_variants as native paid ad headline/body/CTA variants. "
        "Walnut Market Terminal is a professional-grade market intelligence platform for sophisticated retail investors. "
        "It helps users find market tells by combining ticker context, price/volume confirmation, financials and filings, insider activity, "
        "Congress trading disclosures, government contracts, signal conviction, screener workflows, and evidence trail or why-now context. "
        "The brand idea is: 'The market has tells. Walnut finds them.' Do not describe Walnut as a casual stock app. "
        "When replying, lead with useful insight specific to the thread, then add nuance, then mention Walnut only if it has a strong natural angle. "
        "Sound like a sharp market participant and excellent founder/salesperson, while staying concise, helpful, and non-spammy. "
        "Explain Walnut concretely when mentioned; vague phrases like 'compare drivers in one place' are not enough. "
        "If the source mentions a specific public company or ticker, prefer the ticker page /ticker/{SYMBOL}. "
        "If it discusses screeners or research tooling without a ticker, prefer /screener or the homepage. "
        "If it discusses Congress trades, insider buying, or government contracts, use that as the reply_angle when relevant. "
        "Use reported/disclosed/filed/filing-date language for Congress, insider, and institutional disclosure data. "
        "For 13F or institutional activity, never imply live buying or exact trade dates; say reported holdings/activity, quarter-end holdings, and filing date context. "
        "Do not imply endorsement by Reddit, X, Facebook, Congress, SEC, or any data provider. "
        "If organic X or Reddit content mentions Walnut, disclose affiliation naturally, for example: \"I'm building Walnut, so obvious bias...\" "
        "Do not make investment advice claims, tell users to buy, sell, or short a security, guarantee returns, or use hype like 'about to explode'. "
        "Prefer educational language such as \"this may be useful\", \"you can cross-check\", and \"one way to look at it\". "
        "Include at most one Walnut link unless the thread clearly needs more. Never use spammy CTA language. "
        "Make Reddit research threads valuable even if nobody clicks Walnut; include technical context, fundamental context, catalysts, risks, and limitations. "
        "Do not fake personal experience or pretend to be unaffiliated. No automated posting is happening; a human will review and manually post if appropriate. "
        "Avoid replies when spam risk is high. "
        "Avoid replying to old or inactive threads unless relevance is very high. "
        "For source_provider='web_search_reddit', you only have a search-provider title, URL, and snippet. "
        "Do not invent unseen Reddit post or comment details. If the snippet is thin, vague, or missing, use recommended_action='monitor' "
        "and make suggested_reply start with 'Needs manual review -' instead of drafting a full reply."
    )


def _suggestion_json_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {
            "relevance_score": {"type": "integer", "minimum": 0, "maximum": 100},
            "spam_risk_score": {"type": "integer", "minimum": 0, "maximum": 100},
            "detected_tickers": {"type": "array", "items": {"type": "string"}},
            "intent": {"type": "string", "enum": sorted(INTENTS)},
            "campaign_type": {"type": "string", "enum": sorted(CAMPAIGN_TYPES)},
            "content_type": {"type": "string", "enum": sorted(CONTENT_TYPES)},
            "platform": {"type": "string"},
            "audience": {"type": "string"},
            "recommended_action": {"type": "string", "enum": sorted(RECOMMENDED_ACTIONS)},
            "reply_angle": {"type": "string", "enum": sorted(REPLY_ANGLES)},
            "content_angle": {"type": "string"},
            "value_added_insight": {"type": "string"},
            "walnut_feature_to_mention": {"type": "string"},
            "suggested_destination_url": {"type": "string"},
            "suggested_reply": {"type": "string"},
            "suggested_post": {"type": "string"},
            "suggested_ad_variants": {"type": "array", "items": {"type": "string"}},
            "title": {"type": "string"},
            "tldr_bullets": {"type": "array", "items": {"type": "string"}},
            "why_selected": {"type": "string"},
            "company_snapshot": {"type": "string"},
            "walnut_disclosure_stack": {"type": "string"},
            "technical_picture": {"type": "string"},
            "fundamental_picture": {"type": "string"},
            "recent_news_and_filings": {"type": "string"},
            "catalysts": {"type": "string"},
            "bull_case": {"type": "string"},
            "bear_case_and_risks": {"type": "string"},
            "what_would_confirm": {"type": "string"},
            "what_would_weaken": {"type": "string"},
            "bottom_line": {"type": "string"},
            "reddit_disclosure": {"type": "string"},
            "full_reddit_post_markdown": {"type": "string"},
            "source_notes": {"type": "array", "items": {"type": "string"}},
            "missing_data_notes": {"type": "array", "items": {"type": "string"}},
            "quality_scores": {
                "type": "object",
                "properties": {
                    "research_depth_score": {"type": "integer", "minimum": 0, "maximum": 100},
                    "evidence_score": {"type": "integer", "minimum": 0, "maximum": 100},
                    "catalyst_score": {"type": "integer", "minimum": 0, "maximum": 100},
                    "balance_score": {"type": "integer", "minimum": 0, "maximum": 100},
                    "reddit_native_score": {"type": "integer", "minimum": 0, "maximum": 100},
                    "promotional_risk_score": {"type": "integer", "minimum": 0, "maximum": 100},
                    "compliance_risk_score": {"type": "integer", "minimum": 0, "maximum": 100},
                },
                "required": [
                    "research_depth_score",
                    "evidence_score",
                    "catalyst_score",
                    "balance_score",
                    "reddit_native_score",
                    "promotional_risk_score",
                    "compliance_risk_score",
                ],
                "additionalProperties": False,
            },
            "suggested_image_asset": {
                "type": "object",
                "properties": {
                    "title": {"type": "string"},
                    "asset_type": {"type": "string", "enum": ["image", "chart", "csv", "pdf", "screenshot", "report"]},
                    "url": {"type": "string"},
                    "thumbnail_url": {"type": "string"},
                    "suggested_caption": {"type": "string"},
                    "source_data_notes": {"type": "string"},
                },
                "required": ["title", "asset_type", "url", "thumbnail_url", "suggested_caption", "source_data_notes"],
                "additionalProperties": False,
            },
            "suggested_flair": {"type": "string"},
            "suggested_subreddits": {"type": "array", "items": {"type": "string"}},
            "alternate_hooks": {"type": "array", "items": {"type": "string"}},
            "title_options": {"type": "array", "items": {"type": "string"}},
            "disclosure_text": {"type": "string"},
            "assets": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "title": {"type": "string"},
                        "asset_type": {"type": "string", "enum": ["image", "chart", "csv", "pdf", "screenshot", "report"]},
                        "url": {"type": "string"},
                        "thumbnail_url": {"type": "string"},
                        "suggested_caption": {"type": "string"},
                        "source_data_notes": {"type": "string"},
                    },
                    "required": ["title", "asset_type", "url", "thumbnail_url", "suggested_caption", "source_data_notes"],
                    "additionalProperties": False,
                },
            },
            "alternate_reply_more_direct": {"type": "string"},
            "short_reason": {"type": "string"},
            "compliance_notes": {"type": "string"},
        },
        "required": [
            "relevance_score",
            "spam_risk_score",
            "detected_tickers",
            "intent",
            "campaign_type",
            "content_type",
            "platform",
            "audience",
            "recommended_action",
            "reply_angle",
            "content_angle",
            "value_added_insight",
            "walnut_feature_to_mention",
            "suggested_destination_url",
            "suggested_reply",
            "suggested_post",
            "suggested_ad_variants",
            "title",
            "tldr_bullets",
            "why_selected",
            "company_snapshot",
            "walnut_disclosure_stack",
            "technical_picture",
            "fundamental_picture",
            "recent_news_and_filings",
            "catalysts",
            "bull_case",
            "bear_case_and_risks",
            "what_would_confirm",
            "what_would_weaken",
            "bottom_line",
            "reddit_disclosure",
            "full_reddit_post_markdown",
            "source_notes",
            "missing_data_notes",
            "quality_scores",
            "suggested_image_asset",
            "suggested_flair",
            "suggested_subreddits",
            "alternate_hooks",
            "title_options",
            "disclosure_text",
            "assets",
            "alternate_reply_more_direct",
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
    *,
    opportunity: AiMarketingOpportunity | None = None,
) -> dict[str, Any]:
    detected_tickers = _normalized_tickers(payload.get("detected_tickers"))
    intent = str(payload.get("intent") or "other").strip().lower()
    if intent not in INTENTS:
        intent = "other"
    fallback_campaign_type = opportunity.campaign_type if opportunity else None
    fallback_content_type = opportunity.content_type if opportunity else None
    campaign_type = _normalize_campaign_type(payload.get("campaign_type") or fallback_campaign_type)
    content_type = _normalize_content_type(payload.get("content_type") or fallback_content_type, campaign_type=campaign_type, platform=platform)
    output_platform = _normalize_source_platform(payload.get("platform"), fallback=opportunity.source_platform if opportunity else platform)
    opportunity_metadata = _load_object(opportunity.raw_metadata_json) if opportunity else {}
    audience = _truncate(str(payload.get("audience") or opportunity_metadata.get("audience") or "").strip(), 500) or ""
    recommended_action = str(payload.get("recommended_action") or "reply").strip().lower()
    if recommended_action not in RECOMMENDED_ACTIONS:
        recommended_action = _default_action_for_content_type(content_type)
    reply_angle = str(payload.get("reply_angle") or "other").strip().lower()
    if reply_angle not in REPLY_ANGLES:
        reply_angle = "other"
    raw_destination = str(payload.get("suggested_destination_url") or "").strip()
    if recommended_action == "skip":
        destination = ""
    else:
        if detected_tickers:
            raw_destination = f"https://walnutmarkets.com/ticker/{detected_tickers[0]}"
        elif not raw_destination and reply_angle == "screener_tool":
            raw_destination = "https://walnutmarkets.com/screener"
        destination = _normalize_destination_url(
            raw_destination or destination_hint,
            platform=platform,
            campaign_id=campaign_id,
            fallback=destination_hint,
        )
    suggested_reply = _truncate(str(payload.get("suggested_reply") or "").strip(), 3000) or ""
    alternate_reply = _truncate(str(payload.get("alternate_reply_more_direct") or "").strip(), 3000) or ""
    suggested_post = _truncate(str(payload.get("suggested_post") or "").strip(), 16000) or ""
    ad_variants = [_truncate(str(item), 1000) or "" for item in _coerce_json_list(payload.get("suggested_ad_variants"))]
    alternate_hooks = [_truncate(str(item), 400) or "" for item in _coerce_json_list(payload.get("alternate_hooks"))]
    title_options = [_truncate(str(item), 240) or "" for item in _coerce_json_list(payload.get("title_options"))]
    disclosure_text = _truncate(str(payload.get("disclosure_text") or "").strip(), 1000) or ""
    reddit_structured = _reddit_research_structured_payload(payload) if content_type == "reddit_thread" and recommended_action != "skip" else {}
    if reddit_structured:
        suggested_post = reddit_structured["full_markdown"]
        disclosure_text = reddit_structured["reddit_disclosure"] or disclosure_text
        if reddit_structured["title"]:
            title_options = _dedupe_strings([reddit_structured["title"], *title_options])
    if recommended_action == "skip":
        suggested_reply = suggested_reply or "Skip - not relevant enough."
        alternate_reply = alternate_reply or suggested_reply
    elif recommended_action == "monitor":
        suggested_reply = suggested_reply or "Monitor - relevant, but not worth replying yet."
        alternate_reply = alternate_reply or suggested_reply
    else:
        suggested_reply = _ensure_walnut_affiliation_disclosure(suggested_reply or "No safe reply suggested.")
        alternate_reply = _ensure_walnut_affiliation_disclosure(alternate_reply) if alternate_reply else ""
        suggested_post = _ensure_walnut_affiliation_disclosure(suggested_post) if content_type in {"x_post", "reddit_thread"} else suggested_post
    if content_type == "x_post":
        suggested_post = _fit_x_post_text(suggested_post)
        alternate_hooks = [_fit_x_post_text(item) for item in alternate_hooks]
    content_values = [suggested_reply, alternate_reply, suggested_post, *ad_variants]
    if any(_contains_direct_trade_advice(value) or _contains_hype_or_guarantee(value) for value in content_values):
        recommended_action = "monitor"
        destination = ""
        suggested_reply = "Monitor - generated draft contained direct trading advice, hype, or a guarantee; review manually before using."
        alternate_reply = suggested_reply
        suggested_post = ""
        ad_variants = []
    value_added_insight = _truncate(str(payload.get("value_added_insight") or "").strip(), 1500) or ""
    walnut_feature = _truncate(str(payload.get("walnut_feature_to_mention") or "").strip(), 500) or ""
    content_angle = _truncate(str(payload.get("content_angle") or payload.get("reply_angle") or "").strip(), 500) or reply_angle
    assets = _normalize_assets(payload.get("assets"))
    if reddit_structured and reddit_structured["suggested_image_asset"]:
        assets = _normalize_assets([*assets, reddit_structured["suggested_image_asset"]])
    generated_content = _generated_content_from_structured(
        content_type=content_type,
        suggested_reply=suggested_reply,
        suggested_post=suggested_post,
        ad_variants=ad_variants,
    )
    quality_scores = reddit_structured.get("quality_scores", {}) if reddit_structured else {}
    source_notes = reddit_structured.get("source_notes", []) if reddit_structured else []
    missing_data_notes = reddit_structured.get("missing_data_notes", []) if reddit_structured else []
    quality_gate = _reddit_research_quality_gate(
        generated_content,
        quality_scores,
        source_notes=source_notes,
        missing_data_notes=missing_data_notes,
    ) if content_type == "reddit_thread" and recommended_action != "skip" else {"passed": True, "notes": []}
    if content_type == "reddit_thread" and not quality_gate["passed"]:
        recommended_action = "monitor"
        missing_data_notes = _dedupe_strings([*missing_data_notes, *quality_gate["notes"]])
    alternate_versions = {
        "alternate_reply_more_direct": alternate_reply,
        "alternate_hooks": [item for item in alternate_hooks if item],
        "title_options": [item for item in title_options if item],
        "suggested_ad_variants": [item for item in ad_variants if item],
        "disclosure_text": disclosure_text,
        "suggested_flair": reddit_structured.get("suggested_flair", "") if reddit_structured else "",
        "suggested_subreddits": reddit_structured.get("suggested_subreddits", []) if reddit_structured else [],
    }
    return {
        "relevance_score": _clamp_int(payload.get("relevance_score"), 0, 100),
        "spam_risk_score": _clamp_int(payload.get("spam_risk_score"), 0, 100),
        "detected_tickers": detected_tickers,
        "intent": intent,
        "campaign_type": campaign_type,
        "content_type": content_type,
        "platform": output_platform,
        "audience": audience,
        "recommended_action": recommended_action,
        "reply_angle": reply_angle,
        "content_angle": content_angle,
        "value_added_insight": value_added_insight,
        "walnut_feature_to_mention": "" if recommended_action == "skip" else walnut_feature,
        "suggested_destination_url": destination,
        "suggested_reply": suggested_reply,
        "suggested_post": suggested_post,
        "suggested_ad_variants": [item for item in ad_variants if item],
        "alternate_hooks": [item for item in alternate_hooks if item],
        "title_options": [item for item in title_options if item],
        "disclosure_text": disclosure_text,
        "assets": assets,
        "alternate_reply_more_direct": alternate_reply,
        "generated_content": generated_content,
        "full_markdown": generated_content if content_type == "reddit_thread" else "",
        "alternate_versions": alternate_versions,
        "quality_scores": quality_scores,
        "source_notes": source_notes,
        "missing_data_notes": missing_data_notes,
        "quality_gate_passed": bool(quality_gate["passed"]),
        "short_reason": _truncate(str(payload.get("short_reason") or "").strip(), 1000) or value_added_insight or "No reason provided.",
        "compliance_notes": _truncate(str(payload.get("compliance_notes") or "").strip(), 2000) or "Review manually before posting.",
    }


def _ensure_walnut_affiliation_disclosure(reply: str) -> str:
    cleaned = reply.strip()
    if not cleaned or "walnut" not in cleaned.lower():
        return cleaned
    lowered = cleaned.lower()
    disclosure_markers = (
        "i'm building walnut",
        "i am building walnut",
        "we're building walnut",
        "we are building walnut",
        "bias disclosed",
        "obvious bias",
        "my company",
        "our platform",
    )
    if any(marker in lowered for marker in disclosure_markers):
        return cleaned
    return _truncate(f"Bias disclosed: I'm building Walnut. {cleaned}", 3000) or cleaned


def _contains_direct_trade_advice(reply: str) -> bool:
    lowered = reply.lower()
    patterns = (
        r"\b(you should|you need to|you can|i would|i'd|we would|must|should)\s+(buy|sell|short)\b",
        r"\b(buy|sell|short)\s+(this|the stock|the name|[a-z]{1,6})\b",
    )
    return any(re.search(pattern, lowered) for pattern in patterns)


def _contains_hype_or_guarantee(value: str) -> bool:
    lowered = value.lower()
    blocked = (
        "about to explode",
        "guaranteed",
        "guarantee returns",
        "can't lose",
        "risk-free",
        "sure thing",
        "will moon",
        "breakout guaranteed",
    )
    return any(term in lowered for term in blocked)


def _coerce_json_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return []
        try:
            parsed = json.loads(stripped)
            if isinstance(parsed, list):
                return parsed
        except Exception:
            pass
        return [stripped]
    return [value]


def _generated_content_from_structured(
    *,
    content_type: str,
    suggested_reply: str,
    suggested_post: str,
    ad_variants: list[str],
) -> str:
    if content_type == "paid_ad" and ad_variants:
        return "\n\n".join(ad_variants)
    if content_type in {"x_post", "reddit_thread"} and suggested_post:
        return suggested_post
    return suggested_reply


def _reddit_research_structured_payload(payload: dict[str, Any]) -> dict[str, Any]:
    tldr = [_truncate(str(item), 300) or "" for item in _coerce_json_list(payload.get("tldr_bullets"))]
    fields = {
        "title": _truncate(str(payload.get("title") or "").strip(), 240) or "",
        "why_selected": _truncate(str(payload.get("why_selected") or "").strip(), 2000) or "",
        "company_snapshot": _truncate(str(payload.get("company_snapshot") or "").strip(), 2000) or "",
        "walnut_disclosure_stack": _truncate(str(payload.get("walnut_disclosure_stack") or "").strip(), 2500) or "",
        "technical_picture": _truncate(str(payload.get("technical_picture") or "").strip(), 2200) or "",
        "fundamental_picture": _truncate(str(payload.get("fundamental_picture") or "").strip(), 2200) or "",
        "recent_news_and_filings": _truncate(str(payload.get("recent_news_and_filings") or "").strip(), 2500) or "",
        "catalysts": _truncate(str(payload.get("catalysts") or "").strip(), 2200) or "",
        "bull_case": _truncate(str(payload.get("bull_case") or "").strip(), 1800) or "",
        "bear_case_and_risks": _truncate(str(payload.get("bear_case_and_risks") or "").strip(), 2200) or "",
        "what_would_confirm": _truncate(str(payload.get("what_would_confirm") or "").strip(), 1800) or "",
        "what_would_weaken": _truncate(str(payload.get("what_would_weaken") or "").strip(), 1800) or "",
        "bottom_line": _truncate(str(payload.get("bottom_line") or "").strip(), 1800) or "",
        "reddit_disclosure": _truncate(str(payload.get("reddit_disclosure") or payload.get("disclosure_text") or "").strip(), 1000) or "",
    }
    markdown = _truncate(str(payload.get("full_reddit_post_markdown") or "").strip(), 20000) or ""
    if not markdown:
        markdown = _build_reddit_research_markdown(tldr_bullets=tldr, **fields)
    quality_scores = _normalize_quality_scores(payload.get("quality_scores"))
    image_asset = _normalize_assets([payload.get("suggested_image_asset")])[0] if isinstance(payload.get("suggested_image_asset"), dict) else None
    return {
        **fields,
        "tldr_bullets": [item for item in tldr if item],
        "full_markdown": markdown,
        "source_notes": [_truncate(str(item), 500) or "" for item in _coerce_json_list(payload.get("source_notes")) if str(item or "").strip()],
        "missing_data_notes": [_truncate(str(item), 500) or "" for item in _coerce_json_list(payload.get("missing_data_notes")) if str(item or "").strip()],
        "quality_scores": quality_scores,
        "suggested_image_asset": image_asset,
        "suggested_flair": _truncate(str(payload.get("suggested_flair") or "").strip(), 80) or "",
        "suggested_subreddits": _dedupe_strings([str(item).strip().lstrip("r/") for item in _coerce_json_list(payload.get("suggested_subreddits")) if str(item or "").strip()]),
    }


def _build_reddit_research_markdown(*, tldr_bullets: list[str], **sections: str) -> str:
    title = sections.get("title") or "Research thread"
    lines = [f"# {title}", "", "## TL;DR"]
    bullets = [item for item in tldr_bullets if item][:5]
    lines.extend([f"- {item}" for item in bullets] or ["- No TL;DR supplied by the model."])
    section_map = [
        ("Why this name came up", "why_selected"),
        ("Company snapshot", "company_snapshot"),
        ("Walnut disclosure stack", "walnut_disclosure_stack"),
        ("Technical picture", "technical_picture"),
        ("Fundamental picture", "fundamental_picture"),
        ("Recent news / filings / press releases", "recent_news_and_filings"),
        ("Catalysts", "catalysts"),
        ("Bull case", "bull_case"),
        ("Bear case / risks", "bear_case_and_risks"),
        ("What would confirm the setup", "what_would_confirm"),
        ("What would weaken the setup", "what_would_weaken"),
        ("Bottom line", "bottom_line"),
        ("Suggested Reddit disclosure", "reddit_disclosure"),
    ]
    for heading, key in section_map:
        lines.extend(["", f"## {heading}", sections.get(key) or "Not verified from available data."])
    return "\n".join(lines).strip()


def _normalize_quality_scores(value: Any) -> dict[str, int]:
    raw = _load_object(value)
    return {
        "research_depth_score": _clamp_int(raw.get("research_depth_score"), 0, 100),
        "evidence_score": _clamp_int(raw.get("evidence_score"), 0, 100),
        "catalyst_score": _clamp_int(raw.get("catalyst_score"), 0, 100),
        "balance_score": _clamp_int(raw.get("balance_score"), 0, 100),
        "reddit_native_score": _clamp_int(raw.get("reddit_native_score"), 0, 100),
        "promotional_risk_score": _clamp_int(raw.get("promotional_risk_score"), 0, 100),
        "compliance_risk_score": _clamp_int(raw.get("compliance_risk_score"), 0, 100),
    }


def _reddit_research_quality_gate(
    markdown: str,
    quality_scores: dict[str, int],
    *,
    source_notes: list[str],
    missing_data_notes: list[str],
) -> dict[str, Any]:
    notes: list[str] = []
    lowered = markdown.lower()
    if len(markdown.strip()) < 1800:
        notes.append("Reddit DD draft is too short for a comprehensive research thread.")
    for section in REDDIT_RESEARCH_SECTIONS:
        if section.lower() not in lowered:
            notes.append(f"Missing required section: {section}.")
    for required in ("catalyst", "risk", "technical", "fundamental", "disclosed", "reported"):
        if required not in lowered:
            notes.append(f"Missing required research language/context: {required}.")
    if _contains_direct_trade_advice(markdown) or _contains_hype_or_guarantee(markdown):
        notes.append("Draft contains direct trading advice, hype, or guarantee language.")
    if _implies_live_institutional_trading(markdown):
        notes.append("Institutional/13F wording implies live trading instead of reported holdings or filing-date context.")
    for key, threshold in REDDIT_RESEARCH_QUALITY_THRESHOLDS.items():
        score = quality_scores.get(key, 0)
        if key.endswith("_risk_score"):
            if score > threshold:
                notes.append(f"{key} is above threshold: {score} > {threshold}.")
        elif score < threshold:
            notes.append(f"{key} is below threshold: {score} < {threshold}.")
    if "could not be verified" not in lowered and missing_data_notes and not source_notes:
        notes.append("Missing-data limitations are not clearly reflected in the draft.")
    return {"passed": not notes, "notes": _dedupe_strings(notes)}


def _implies_live_institutional_trading(value: str) -> bool:
    lowered = value.lower()
    if "13f" not in lowered and "institutional" not in lowered:
        return False
    if "not live buying" in lowered or "never imply live buying" in lowered:
        lowered = lowered.replace("not live buying", "").replace("never imply live buying", "")
    risky = ("bought today", "buying today", "is buying", "are buying", "just bought", "live buying", "current buying")
    return any(term in lowered for term in risky)


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
    if key in BOOLEAN_ENV_ONLY_SETTING_KEYS:
        value = "true" if _env_flag_enabled(key) else ""
    else:
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
        raise ValueError("A valid source URL is required.")
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
    if "linkedin.com" in host:
        return "linkedin"
    if "x.com" in host or "twitter.com" in host:
        return "x_stub"
    if "walnutmarkets.com" in host:
        return "manual"
    return "other"


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


def _normalized_query_templates(value: Any) -> list[str]:
    templates = []
    for item in _normalized_string_list(value):
        cleaned = re.sub(r"\s+", " ", item).strip()
        if not cleaned:
            continue
        templates.append(_truncate(cleaned, 240) or "")
    return [template for template in _dedupe_strings(templates) if template]


def _normalize_assets(value: Any) -> list[dict[str, Any]]:
    if not value:
        return []
    raw_items = value if isinstance(value, list) else [value]
    assets: list[dict[str, Any]] = []
    allowed_types = {"image", "chart", "csv", "pdf", "screenshot", "report"}
    for raw in raw_items:
        if not isinstance(raw, dict):
            continue
        asset_url = _truncate(str(raw.get("url") or raw.get("path") or raw.get("reference") or "").strip(), 1200) or ""
        thumbnail_url = _truncate(str(raw.get("thumbnail_url") or "").strip(), 1200) or ""
        if not asset_url and not thumbnail_url:
            continue
        asset_type = str(raw.get("asset_type") or "image").strip().lower()
        if asset_type not in allowed_types:
            asset_type = "image"
        title = _truncate(str(raw.get("title") or asset_type.title()).strip(), 200) or asset_type.title()
        assets.append(
            {
                "title": title,
                "asset_type": asset_type,
                "url": asset_url,
                "thumbnail_url": thumbnail_url,
                "suggested_caption": _truncate(str(raw.get("suggested_caption") or "").strip(), 1000) or "",
                "source_data_notes": _truncate(str(raw.get("source_data_notes") or "").strip(), 1000) or "",
            }
        )
    return assets[:10]


def _render_search_query_template(template: str, *, subreddit: str, keyword: str, ticker: str, term: str) -> str:
    rendered = template
    replacements = {
        "{subreddit}": subreddit,
        "{keyword}": keyword,
        "{ticker}": ticker,
        "{term}": term,
    }
    for placeholder, value in replacements.items():
        rendered = rendered.replace(placeholder, value)
    return re.sub(r"\s+", " ", rendered).strip()


def _normalize_reddit_search_result_url(value: str) -> str | None:
    parsed = urlparse(value.strip())
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return None
    host = parsed.netloc.lower()
    if host == "redd.it" or host.endswith(".redd.it"):
        path = "/" + "/".join(part for part in parsed.path.split("/") if part)
        return urlunparse(("https", "redd.it", path.rstrip("/") or "/", "", "", ""))
    if host != "reddit.com" and not host.endswith(".reddit.com"):
        return None
    path_parts = [part for part in parsed.path.split("/") if part]
    if not path_parts:
        return None
    if path_parts[0].lower() != "r":
        return None
    normalized_path = "/" + "/".join(path_parts)
    return urlunparse(("https", "www.reddit.com", normalized_path.rstrip("/") or "/", "", "", ""))


def _subreddit_from_reddit_url(value: str) -> str | None:
    parsed = urlparse(value)
    parts = [part for part in parsed.path.split("/") if part]
    if len(parts) >= 2 and parts[0].lower() == "r":
        return parts[1]
    return None


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


def _load_json_list(raw: str | None) -> list[Any]:
    try:
        parsed = json.loads(raw or "[]")
    except Exception:
        return []
    return parsed if isinstance(parsed, list) else []


def _load_object(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return dict(raw)
    try:
        parsed = json.loads(raw or "{}")
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _dump_list(values: list[Any]) -> str:
    return json.dumps(_dedupe_strings([str(value).strip() for value in values if str(value).strip()]), sort_keys=True)


def _dump_json_list(values: list[Any]) -> str:
    return json.dumps(values if isinstance(values, list) else [], sort_keys=True, default=str)


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


def _fit_x_post_text(value: str | None) -> str:
    cleaned = re.sub(r"\s+", " ", str(value or "").strip())
    if len(cleaned) <= X_POST_CHARACTER_LIMIT:
        return cleaned
    suffix = "..."
    max_body = X_POST_CHARACTER_LIMIT - len(suffix)
    trimmed = cleaned[:max_body].rstrip()
    word_boundary = trimmed.rsplit(" ", 1)[0].rstrip(" ,;:-") if " " in trimmed else trimmed
    if len(word_boundary) >= 160:
        trimmed = word_boundary
    return f"{trimmed.rstrip(' ,;:-')}{suffix}"[:X_POST_CHARACTER_LIMIT]


def _iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.isoformat()
