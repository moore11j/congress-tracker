from __future__ import annotations

import hashlib
import hmac
import html
import json
import logging
import os
import re
import secrets
import base64
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from io import BytesIO
from typing import Any
from urllib.parse import parse_qsl, quote, unquote_to_bytes, urlencode, urlparse, urlunparse
from xml.etree import ElementTree
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import requests
from sqlalchemy import desc, func, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.models import (
    AiMarketingArticleCandidate,
    AiMarketingCampaign,
    AiMarketingCampaignRun,
    AiMarketingEmailLog,
    AiMarketingOpportunity,
    AiMarketingSetting,
    AiMarketingSuggestion,
    AiGrowthEmailActionToken,
    ConfirmationMonitoringEvent,
    ConfirmationMonitoringSnapshot,
    Event,
    GovernmentContract,
    GovernmentContractAction,
    InstitutionalActivityEvent,
    SavedScreen,
    SavedScreenEvent,
    SavedScreenSnapshot,
    Security,
    TickerMeta,
    Watchlist,
    WatchlistItem,
)
from app.services.confirmation_score import get_confirmation_score_bundles_for_tickers
from app.services.email_delivery import send_email

logger = logging.getLogger(__name__)

DEFAULT_AI_GROWTH_RECIPIENT = "jarod@walnutmarkets.com"
AI_GROWTH_DIGEST_RECIPIENT = "AI_GROWTH_DIGEST_RECIPIENT"
AI_MARKETING_TEMPLATE_KEY = "ai_marketing.digest"
AI_MARKETING_PROMPT_VERSION = "ai_growth_v2"
DEFAULT_DESTINATION_URL = "https://walnutmarkets.com"
DEFAULT_AI_MARKETING_MODEL = "gpt-5.6"
DEFAULT_AI_MARKETING_IMAGE_MODEL = "gpt-image-2"
DEFAULT_AI_MARKETING_IMAGE_SIZE = "1536x1024"
DEFAULT_AI_MARKETING_IMAGE_QUALITY = "high"
WALNUT_THUMBNAIL_LOGO_LOCKUP_PATH = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    "assets",
    "walnut-markets-logo-lockup.png",
)
X_POST_CHARACTER_LIMIT = 280
SOCIAL_CARD_WIDTH = 1600
SOCIAL_CARD_HEIGHT = 900
SOCIAL_CARD_TYPES = {
    "article_reactive",
    "ticker_signal",
    "congress_insider_activity",
    "research_cover",
}
SOCIAL_CARD_TEMPLATES = SOCIAL_CARD_TYPES | {
    "auto",
    "fast_reaction",
    "signal_card",
    "activity_card",
    "dd_cover",
}
SOCIAL_CARD_TONES = {"sharp", "market-native", "neutral", "educational"}
SOCIAL_CARD_SENTIMENTS = {"bullish", "bearish", "neutral", "notable", "active"}
OPENAI_API_KEY = "OPENAI_API_KEY"
AI_MARKETING_MODEL = "AI_MARKETING_MODEL"
AI_MARKETING_IMAGE_MODEL = "AI_MARKETING_IMAGE_MODEL"
AI_MARKETING_IMAGE_GENERATION_ENABLED = "AI_MARKETING_IMAGE_GENERATION_ENABLED"
AI_MARKETING_IMAGE_SIZE = "AI_MARKETING_IMAGE_SIZE"
AI_MARKETING_IMAGE_QUALITY = "AI_MARKETING_IMAGE_QUALITY"
OPENAI_WEB_SEARCH_ENABLED = "OPENAI_WEB_SEARCH_ENABLED"
AI_GROWTH_EMAIL_TONE = "AI_GROWTH_EMAIL_TONE"
AI_GROWTH_VOICE_CHARACTERISTICS = "AI_GROWTH_VOICE_CHARACTERISTICS"
OPENAI_CREDITS_LOW_WATERMARK_USD = "OPENAI_CREDITS_LOW_WATERMARK_USD"
FMP_API_KEY = "FMP_API_KEY"
AI_GROWTH_ARTICLE_AUTOMATION_ENABLED = "AI_GROWTH_ARTICLE_AUTOMATION_ENABLED"
AI_GROWTH_ARTICLE_MAX_DAILY_DRAFTS = "AI_GROWTH_ARTICLE_MAX_DAILY_DRAFTS"
AI_GROWTH_INBOUND_REPLY_ADDRESS = "AI_GROWTH_INBOUND_REPLY_ADDRESS"
POSTMARK_INBOUND_WEBHOOK_SECRET = "POSTMARK_INBOUND_WEBHOOK_SECRET"
POSTMARK_INBOUND_BASIC_AUTH_SECRET = "POSTMARK_INBOUND_BASIC_AUTH_SECRET"
REDDIT_CLIENT_ID = "REDDIT_CLIENT_ID"
REDDIT_CLIENT_SECRET = "REDDIT_CLIENT_SECRET"
REDDIT_USER_AGENT = "REDDIT_USER_AGENT"
X_CLIENT_ID = "X_CLIENT_ID"
X_CLIENT_SECRET = "X_CLIENT_SECRET"
X_REDIRECT_URI = "X_REDIRECT_URI"
X_ACCESS_TOKEN = "X_ACCESS_TOKEN"
X_REFRESH_TOKEN = "X_REFRESH_TOKEN"
X_API_BASE_URL = "X_API_BASE_URL"
X_OAUTH2_TOKEN_URL = "X_OAUTH2_TOKEN_URL"
X_CURRENT_ACCESS_TOKEN_SETTING = "X_OAUTH2_CURRENT_ACCESS_TOKEN"
X_CURRENT_REFRESH_TOKEN_SETTING = "X_OAUTH2_CURRENT_REFRESH_TOKEN"
X_TOKEN_REFRESHED_AT_SETTING = "X_OAUTH2_LAST_REFRESHED_AT"
BING_SEARCH_API_KEY = "BING_SEARCH_API_KEY"
WEB_SEARCH_REDDIT_SOURCE_PROVIDER = "web_search_reddit"
OPENAI_WEB_SEARCH_NOT_CONFIGURED_MESSAGE = (
    "OpenAI web search is not configured. Enable OPENAI_WEB_SEARCH_ENABLED=true and confirm OPENAI_API_KEY is set."
)
OPENAI_WEB_SEARCH_FAILED_MESSAGE = "OpenAI web search discovery failed. Check OpenAI configuration, quota, and API availability."
DEFAULT_OPENAI_CREDITS_LOW_WATERMARK_USD = 25.0
DEFAULT_AI_GROWTH_EMAIL_TONE = "market-native"
DEFAULT_AI_GROWTH_VOICE_CHARACTERISTICS = "\n".join(
    [
        "Professional-grade market intelligence for sophisticated retail investors.",
        "Sharp market participant voice: useful, concrete, concise, and non-spammy.",
        "Lead with ticker-specific data, then explain why it matters.",
        "For X replies, mimic the strongest Walnut reply behavior: concise market judgment under high-reach posts.",
        "Prefer a one-line or two-line take that names the actual market tell instead of explaining the product.",
        "Use data, not stack, as the public-facing language: price/volume, fundamentals, reported institutional activity, Congress/insider activity, contracts, and technicals.",
        "Keep the distinction clear: confirmation score is Walnut's proprietary score; underlying data is the evidence behind the situation.",
        "Posting formula: assess the situation, identify the issues, analyze the data, then conclude.",
        "Do not say cross-check this on Walnut pages; state what the data says, then provide the ticker link only when it adds useful context.",
        "Avoid generic one-word replies unless the prompt explicitly calls for a one-word answer.",
        "Use reported/disclosed/filed language for Congress, insider, and institutional data.",
        "No hype, guarantees, buy/sell/short instructions, or spammy CTA language.",
        "Brand idea: The market has tells. We help find them.",
    ]
)
AI_GROWTH_SEO_KEYWORD_GUIDANCE = " ".join(
    [
        "Use search-led Walnut language consistently across AI Growth emails, X campaigns, X replies, Reddit drafts, and social cards.",
        "Prioritize phrases people already search for: Congress trades, congressional stock trades, insider activity, insider trading tracker, stock research, ticker intelligence, market signals, options flow, institutional activity, government contracts, fundamentals, technicals, confirmation score, and underlying data.",
        "Use confirmation score only for Walnut's proprietary score; use underlying data for price/volume, fundamentals, reported institutional activity, Congress/insider activity, contracts, and technicals.",
        "Do not use stack as public-facing campaign language when data, underlying data, or data sources is clearer.",
        "Avoid the headline phrase 'Confirmation-Stack Market Intelligence' and the stale phrase 'Market Intelligence from Political Trades and Insider Activity'.",
        "Prefer title and hook patterns like 'Congress Trades & Insider Activity Research', 'Congressional Stock Trades and Insider Signals', and 'Stock Research from Congress Trades, Insider Activity, and Market Signals'.",
        "For X campaigns, use the same keyword language naturally in the post, card headline, card chips, and visual_brief when relevant.",
        "Options Flow is still coming soon; do not describe it as an available Walnut feature unless a future context explicitly says it is live.",
    ]
)
DEFAULT_OPENAI_CREDITS_LEDGER_START_USD = 9.91
OPENAI_CREDITS_LEDGER_START_USD = "OPENAI_CREDITS_LEDGER_START_USD"
OPENAI_CREDITS_LEDGER_SPENT_USD = "OPENAI_CREDITS_LEDGER_SPENT_USD"
OPENAI_CREDITS_LEDGER_LAST_COST_USD = "OPENAI_CREDITS_LEDGER_LAST_COST_USD"
OPENAI_CREDITS_LEDGER_LAST_MODEL = "OPENAI_CREDITS_LEDGER_LAST_MODEL"
OPENAI_CREDITS_LEDGER_LAST_USAGE_JSON = "OPENAI_CREDITS_LEDGER_LAST_USAGE_JSON"
OPENAI_INPUT_USD_PER_1M = "OPENAI_INPUT_USD_PER_1M"
OPENAI_CACHED_INPUT_USD_PER_1M = "OPENAI_CACHED_INPUT_USD_PER_1M"
OPENAI_OUTPUT_USD_PER_1M = "OPENAI_OUTPUT_USD_PER_1M"
OPENAI_WEB_SEARCH_USD_PER_1K_CALLS = "OPENAI_WEB_SEARCH_USD_PER_1K_CALLS"
OPENAI_CREDIT_GRANTS_URL = "https://api.openai.com/dashboard/billing/credit_grants"
OPENAI_CREDITS_CACHE_SECONDS = 300
_OPENAI_CREDITS_CACHE: dict[str, Any] = {"expires_at": 0.0, "api_key": None, "payload": None}

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
    "article_reactive_x",
    "scheduled_x_campaign",
    "x_reply_campaign",
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
    "posted",
    "posted_manually",
    "archived",
    "rejected",
    "rejected_regenerate_requested",
    "superseded",
    "dismissed",
    "regeneration_needed",
    "quality_failed",
}
INTENTS = {"question", "complaint", "trade_idea", "tool_search", "news_reaction", "other"}
RECOMMENDED_ACTIONS = {"reply", "skip", "monitor", "draft_post", "draft_ad"}
CONTENT_TYPES = {"reddit_reply", "reddit_thread", "x_post", "x_reply", "paid_ad"}
CAMPAIGN_TYPES = {
    "manual_research_input",
    "x_chart_drop",
    "reddit_research_thread",
    "article_reactive_x",
    "scheduled_x_campaign",
    "x_reply_campaign",
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
ARTICLE_REACTIVE_PROVIDER = "fmp_articles"
ARTICLE_REACTIVE_CAMPAIGN_TYPE = "article_reactive_x"
SCHEDULED_X_CAMPAIGN_TYPE = "scheduled_x_campaign"
X_REPLY_CAMPAIGN_TYPE = "x_reply_campaign"
X_REPLY_PROVIDER = "x_api"
FMP_ARTICLES_URL = "https://financialmodelingprep.com/stable/fmp-articles"
ARTICLE_RUN_DEFAULT_LIMIT = 20
ARTICLE_DEDUPE_DAYS = 14
ARTICLE_MIN_FINAL_SCORE = 58
ARTICLE_RELEVANT_THEMES = {
    "ai": ("ai", "artificial intelligence", "machine learning", "data center", "datacenter"),
    "semiconductors": ("semiconductor", "chip", "hbm", "gpu", "wafer", "memory"),
    "defense": ("defense", "pentagon", "dod", "missile", "aerospace"),
    "energy": ("energy", "oil", "gas", "nuclear", "uranium", "solar", "power grid"),
    "crypto": ("crypto", "bitcoin", "ethereum", "blockchain", "stablecoin"),
    "macro": ("fed", "inflation", "rates", "treasury", "jobs report", "cpi", "ppi"),
}
AI_MARKETING_SETTINGS: dict[str, dict[str, Any]] = {
    OPENAI_API_KEY: {"label": "OpenAI API Key", "is_secret": True, "required_for": "AI Growth suggestions"},
    AI_MARKETING_MODEL: {"label": "AI Growth Model", "is_secret": False, "required_for": "AI Growth suggestions"},
    OPENAI_WEB_SEARCH_ENABLED: {"label": "OpenAI Web Search", "is_secret": False, "required_for": "AI Growth web discovery"},
    AI_GROWTH_EMAIL_TONE: {"label": "AI Growth Email Tone", "is_secret": False, "required_for": "AI Growth messaging"},
    AI_GROWTH_VOICE_CHARACTERISTICS: {"label": "AI Growth Voice Characteristics", "is_secret": False, "required_for": "AI Growth messaging"},
    FMP_API_KEY: {"label": "FMP Articles API", "is_secret": True, "required_for": "Article-Reactive X campaigns"},
    X_CLIENT_ID: {"label": "X Client ID", "is_secret": True, "required_for": "X OAuth status"},
    X_CLIENT_SECRET: {"label": "X Client Secret", "is_secret": True, "required_for": "X OAuth status"},
    X_REDIRECT_URI: {"label": "X Redirect URI", "is_secret": False, "required_for": "X OAuth status"},
    X_ACCESS_TOKEN: {"label": "X Access Token", "is_secret": True, "required_for": "X posting on approval"},
    X_REFRESH_TOKEN: {"label": "X Refresh Token", "is_secret": True, "required_for": "X token refresh"},
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
        FMP_API_KEY,
        X_CLIENT_ID,
        X_CLIENT_SECRET,
        X_REDIRECT_URI,
        X_ACCESS_TOKEN,
        X_REFRESH_TOKEN,
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
_CASHTAG_TICKER_PATTERN = re.compile(
    r"(?<![A-Za-z0-9])\$([A-Za-z][A-Za-z0-9]{0,4}(?:\.[A-Za-z]{1,2})?)(?![A-Za-z0-9])"
)
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
    "Walnut disclosure data",
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


def marketing_image_model() -> str:
    return os.getenv(AI_MARKETING_IMAGE_MODEL, "").strip() or DEFAULT_AI_MARKETING_IMAGE_MODEL


def marketing_image_generation_enabled() -> bool:
    return _env_flag_enabled(AI_MARKETING_IMAGE_GENERATION_ENABLED)


def ai_growth_email_tone(db: Session | None = None) -> str:
    return resolved_setting_value(db, AI_GROWTH_EMAIL_TONE) or DEFAULT_AI_GROWTH_EMAIL_TONE


def ai_growth_voice_characteristics(db: Session | None = None) -> str:
    return resolved_setting_value(db, AI_GROWTH_VOICE_CHARACTERISTICS) or DEFAULT_AI_GROWTH_VOICE_CHARACTERISTICS


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

    if key == AI_GROWTH_EMAIL_TONE:
        return {"key": key, "value": DEFAULT_AI_GROWTH_EMAIL_TONE, "source": "default", "row": row}
    if key == AI_GROWTH_VOICE_CHARACTERISTICS:
        return {"key": key, "value": DEFAULT_AI_GROWTH_VOICE_CHARACTERISTICS, "source": "default", "row": row}

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


def _env_float(key: str) -> float | None:
    raw = os.getenv(key)
    if raw is None:
        return None
    cleaned = raw.strip().replace("$", "").replace(",", "")
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def _request_openai_credit_grants(api_key: str) -> dict[str, Any]:
    now = time.monotonic()
    cache_key = api_key[-12:]
    if _OPENAI_CREDITS_CACHE.get("api_key") == cache_key and float(_OPENAI_CREDITS_CACHE.get("expires_at") or 0) > now:
        cached = _OPENAI_CREDITS_CACHE.get("payload")
        if isinstance(cached, dict):
            return cached
    try:
        response = requests.get(
            OPENAI_CREDIT_GRANTS_URL,
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=8,
        )
    except requests.RequestException as exc:
        payload = {"ok": False, "status_code": None, "error": str(exc)}
    else:
        status_code = getattr(response, "status_code", None)
        response_ok = bool(getattr(response, "ok", False)) or (isinstance(status_code, int) and 200 <= status_code < 300)
        if response_ok:
            try:
                payload = {"ok": True, "status_code": status_code, "data": response.json()}
            except ValueError:
                payload = {"ok": False, "status_code": status_code, "error": "OpenAI billing response was not JSON."}
        else:
            payload = {
                "ok": False,
                "status_code": status_code,
                "error": "OpenAI billing credits request failed.",
            }
    _OPENAI_CREDITS_CACHE.update({"api_key": cache_key, "expires_at": now + OPENAI_CREDITS_CACHE_SECONDS, "payload": payload})
    return payload


def _openai_credits_status(db: Session | None) -> dict[str, Any]:
    api_key = resolved_setting_value(db, OPENAI_API_KEY)
    low_watermark = _env_float(OPENAI_CREDITS_LOW_WATERMARK_USD)
    if low_watermark is None or low_watermark < 0:
        low_watermark = DEFAULT_OPENAI_CREDITS_LOW_WATERMARK_USD
    if not api_key:
        return {
            "left_usd": None,
            "low_watermark_usd": low_watermark,
            "status": "missing",
            "label": "OpenAI API key missing",
            "source": "openai_billing",
            "error": "OPENAI_API_KEY is not configured.",
        }
    starting_balance = _openai_ledger_start_usd(db)
    spent = _openai_ledger_spent_usd(db)
    credits_left = max(starting_balance - spent, 0.0)
    status = "low" if credits_left <= low_watermark else "ok"
    return {
        "left_usd": credits_left,
        "starting_balance_usd": starting_balance,
        "spent_usd": spent,
        "last_response_cost_usd": _openai_ledger_float(db, OPENAI_CREDITS_LEDGER_LAST_COST_USD, default=0.0),
        "last_model": _private_setting_value(db, OPENAI_CREDITS_LEDGER_LAST_MODEL),
        "low_watermark_usd": low_watermark,
        "status": status,
        "label": f"${credits_left:,.2f}",
        "source": "local_usage_ledger",
    }


def _private_setting_value(db: Session | None, key: str) -> str | None:
    if db is None:
        return None
    row = db.get(AiMarketingSetting, key)
    value = row.value if row is not None else None
    return value.strip() if isinstance(value, str) and value.strip() else None


def _upsert_private_setting(db: Session, key: str, value: str | None, *, is_secret: bool = False) -> AiMarketingSetting:
    row = db.get(AiMarketingSetting, key)
    now = datetime.now(timezone.utc)
    if row is None:
        row = AiMarketingSetting(key=key, value=value, is_secret=is_secret, created_at=now, updated_at=now)
        db.add(row)
    else:
        row.value = value
        row.is_secret = is_secret
        row.updated_at = now
    return row


def _openai_ledger_float(db: Session | None, key: str, *, default: float) -> float:
    value = _private_setting_value(db, key)
    if value is None:
        env_value = os.getenv(key, "").strip()
        value = env_value or None
    if value is None:
        return default
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return max(number, 0.0)


def _openai_ledger_start_usd(db: Session | None) -> float:
    env_start = _env_float(OPENAI_CREDITS_LEDGER_START_USD)
    default = env_start if env_start is not None and env_start >= 0 else DEFAULT_OPENAI_CREDITS_LEDGER_START_USD
    return _openai_ledger_float(db, OPENAI_CREDITS_LEDGER_START_USD, default=default)


def _openai_ledger_spent_usd(db: Session | None) -> float:
    return _openai_ledger_float(db, OPENAI_CREDITS_LEDGER_SPENT_USD, default=0.0)


def _model_pricing_usd_per_1m(model: str) -> dict[str, float]:
    normalized = str(model or "").strip().lower()
    if "sol" in normalized:
        pricing = {"input": 5.0, "cached_input": 0.5, "output": 30.0}
    elif "terra" in normalized:
        pricing = {"input": 2.5, "cached_input": 0.25, "output": 15.0}
    else:
        pricing = {"input": 1.0, "cached_input": 0.1, "output": 6.0}
    overrides = {
        "input": _env_float(OPENAI_INPUT_USD_PER_1M),
        "cached_input": _env_float(OPENAI_CACHED_INPUT_USD_PER_1M),
        "output": _env_float(OPENAI_OUTPUT_USD_PER_1M),
    }
    for key, value in overrides.items():
        if value is not None and value >= 0:
            pricing[key] = value
    return pricing


def _number_from_mapping(mapping: Any, *keys: str) -> int:
    if not isinstance(mapping, dict):
        return 0
    for key in keys:
        value = mapping.get(key)
        if isinstance(value, (int, float)):
            return max(int(value), 0)
    return 0


def _openai_usage_tokens(data: dict[str, Any]) -> dict[str, int]:
    usage = data.get("usage") if isinstance(data, dict) else {}
    input_tokens = _number_from_mapping(usage, "prompt_tokens", "input_tokens")
    output_tokens = _number_from_mapping(usage, "completion_tokens", "output_tokens")
    details = usage.get("prompt_tokens_details") if isinstance(usage, dict) else None
    if not isinstance(details, dict) and isinstance(usage, dict):
        details = usage.get("input_tokens_details")
    cached_tokens = _number_from_mapping(details, "cached_tokens")
    return {
        "input_tokens": input_tokens,
        "cached_input_tokens": min(cached_tokens, input_tokens),
        "output_tokens": output_tokens,
    }


def _estimate_openai_response_cost(model: str, data: dict[str, Any], *, web_search_calls: int = 0) -> dict[str, Any]:
    tokens = _openai_usage_tokens(data)
    pricing = _model_pricing_usd_per_1m(model)
    uncached_input = max(tokens["input_tokens"] - tokens["cached_input_tokens"], 0)
    search_rate = _env_float(OPENAI_WEB_SEARCH_USD_PER_1K_CALLS)
    if search_rate is None or search_rate < 0:
        search_rate = 10.0
    token_cost = (
        (uncached_input * pricing["input"])
        + (tokens["cached_input_tokens"] * pricing["cached_input"])
        + (tokens["output_tokens"] * pricing["output"])
    ) / 1_000_000
    search_cost = (max(web_search_calls, 0) * search_rate) / 1_000
    return {
        "model": model,
        "cost_usd": round(token_cost + search_cost, 8),
        "token_cost_usd": round(token_cost, 8),
        "web_search_cost_usd": round(search_cost, 8),
        "web_search_calls": max(web_search_calls, 0),
        "pricing_usd_per_1m": pricing,
        "web_search_usd_per_1k_calls": search_rate,
        **tokens,
    }


def _record_openai_usage_cost(
    db: Session | None,
    *,
    model: str,
    data: dict[str, Any],
    feature: str,
    web_search_calls: int = 0,
    commit: bool = False,
) -> dict[str, Any]:
    estimate = _estimate_openai_response_cost(model, data, web_search_calls=web_search_calls)
    if db is None:
        return estimate
    spent = _openai_ledger_spent_usd(db) + float(estimate["cost_usd"])
    _upsert_private_setting(db, OPENAI_CREDITS_LEDGER_START_USD, f"{_openai_ledger_start_usd(db):.2f}")
    _upsert_private_setting(db, OPENAI_CREDITS_LEDGER_SPENT_USD, f"{spent:.8f}")
    _upsert_private_setting(db, OPENAI_CREDITS_LEDGER_LAST_COST_USD, f"{float(estimate['cost_usd']):.8f}")
    _upsert_private_setting(db, OPENAI_CREDITS_LEDGER_LAST_MODEL, model)
    _upsert_private_setting(
        db,
        OPENAI_CREDITS_LEDGER_LAST_USAGE_JSON,
        _dump_object({**estimate, "feature": feature, "recorded_at": datetime.now(timezone.utc).isoformat()}),
    )
    if commit:
        db.commit()
    return estimate


def config_status(db: Session | None = None) -> dict[str, Any]:
    statuses = {
        key: public_setting_payload(db, key) if db is not None else _public_setting_payload_without_db(key)
        for key in AI_MARKETING_SETTINGS
    }
    web_search_status = web_search_provider_status(db)
    openai_credits = _openai_credits_status(db)
    x_status = x_account_status(db)
    warnings: list[str] = []
    if not statuses[OPENAI_API_KEY]["configured"]:
        warnings.append("OpenAI API key missing")
    if openai_credits["status"] == "low":
        warnings.append(
            f"OpenAI credits low: {openai_credits['label']} remaining. Repurchase before AI Growth generation stalls."
        )
    if not statuses[FMP_API_KEY]["configured"]:
        warnings.append("FMP Articles API key missing")
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
    if not x_status["oauth_configured"]:
        warnings.append("X API OAuth credentials missing")
    if not x_status["connected"]:
        warnings.append("X access token missing; approval will not post to X")
    elif x_status.get("refresh_token_status") != "configured":
        warnings.append("X refresh token missing; expired access tokens will require manual replacement")
    reddit_configured = all(
        bool(statuses[key]["configured"])
        for key in (REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USER_AGENT)
    )
    return {
        "openai_configured": bool(statuses[OPENAI_API_KEY]["configured"]),
        "openai_model": resolved_setting_value(db, AI_MARKETING_MODEL) or DEFAULT_AI_MARKETING_MODEL,
        "ai_growth_email_tone": ai_growth_email_tone(db),
        "ai_growth_voice_characteristics": ai_growth_voice_characteristics(db),
        "openai_credits_left_usd": openai_credits["left_usd"],
        "openai_credits_starting_balance_usd": openai_credits.get("starting_balance_usd"),
        "openai_credits_spent_usd": openai_credits.get("spent_usd"),
        "openai_credits_last_response_cost_usd": openai_credits.get("last_response_cost_usd"),
        "openai_credits_last_model": openai_credits.get("last_model"),
        "openai_credits_low_watermark_usd": openai_credits["low_watermark_usd"],
        "openai_credits_status": openai_credits["status"],
        "openai_credits_label": openai_credits["label"],
        "openai_credits_source": openai_credits.get("source"),
        "openai_credits_error": openai_credits.get("error"),
        "fmp_articles_configured": bool(statuses[FMP_API_KEY]["configured"]),
        "fmp_articles_status": "configured" if statuses[FMP_API_KEY]["configured"] else "missing",
        "fmp_articles_missing": [] if statuses[FMP_API_KEY]["configured"] else [FMP_API_KEY],
        "fmp_articles_provider": "FMP Articles",
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
        "x_configured": bool(x_status["oauth_configured"]),
        "x_status": x_status["status"],
        "x_oauth_configured": bool(x_status["oauth_configured"]),
        "x_connected": bool(x_status["connected"]),
        "x_refresh_token_configured": x_status.get("refresh_token_status") == "configured",
        "x_missing": x_status["missing"],
        "x_handle": x_status["handle"],
        "x_posting_status": "approve_posts_to_x" if x_status["connected"] else "approval_only",
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
    if normalized in {"x_chart_drop", "reddit_research_thread", ARTICLE_REACTIVE_CAMPAIGN_TYPE, SCHEDULED_X_CAMPAIGN_TYPE}:
        return normalized
    return "legacy_outreach_campaign"


def _content_type_for_campaign_type(campaign_type: str | None, *, desired_output_type: str | None = None, platform: str | None = None) -> str:
    desired = str(desired_output_type or "").strip().lower().replace(" ", "_")
    if desired in {"x_reply", "x_comment", "reply_to_x", "twitter_reply"}:
        return "x_reply"
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
    if normalized == X_REPLY_CAMPAIGN_TYPE:
        return "x_reply"
    if str(platform or "").strip().lower() in {"x", "x_stub", "twitter"}:
        return "x_post"
    return "reddit_reply"


def _default_action_for_content_type(content_type: str | None) -> str:
    return {
        "x_post": "draft_post",
        "x_reply": "reply",
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
    if content_type in {"x_post", "x_reply"}:
        return "x"
    if content_type in {"reddit_reply", "reddit_thread"}:
        return "reddit"
    return "other"


def _default_growth_title(campaign_type: str, ticker_theme: str | None = None) -> str:
    labels = {
        "manual_research_input": "Manual Research Input",
        "x_chart_drop": "X Campaign",
        "reddit_research_thread": "Reddit Research Thread",
        "article_reactive_x": "Article-Reactive X Campaign",
        "scheduled_x_campaign": "Scheduled X Campaign",
        "x_reply_campaign": "X Reply Campaign",
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
    if campaign_type == ARTICLE_REACTIVE_CAMPAIGN_TYPE:
        mode = ARTICLE_REACTIVE_CAMPAIGN_TYPE
        platforms = ["x"]
        content_type = "x_post"
        max_items_per_run = min(max_items_per_run, 20)
    if campaign_type == X_REPLY_CAMPAIGN_TYPE:
        mode = X_REPLY_CAMPAIGN_TYPE
        platforms = ["x"]
        content_type = "x_reply"
        max_items_per_run = min(max_items_per_run, 50)
    max_drafts_limit = 10 if campaign_type in {SCHEDULED_X_CAMPAIGN_TYPE, X_REPLY_CAMPAIGN_TYPE} else 2
    max_drafts_per_day = _clamp_int(payload.get("max_drafts_per_day", payload.get("max_items_per_run", 1)), 1, max_drafts_limit)
    status = str(payload.get("status") or ("active" if bool(payload.get("enabled", True)) else "paused")).strip().lower()
    if status not in {"active", "paused", "stopped"}:
        status = "active" if bool(payload.get("enabled", True)) else "paused"
    enabled = bool(payload.get("enabled", True))
    if status in {"paused", "stopped"}:
        enabled = False
    return {
        "name": name,
        "enabled": enabled,
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
        "max_drafts_per_day": max_drafts_per_day,
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
        max_drafts_per_day=normalized["max_drafts_per_day"],
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
    campaign.max_drafts_per_day = normalized["max_drafts_per_day"]
    campaign.recency = normalized["recency"]
    campaign.default_destination_page = normalized["default_destination_page"]
    campaign.include_disclosure = normalized["include_disclosure"]
    campaign.scheduled_digest_enabled = normalized["scheduled_digest_enabled"]
    db.commit()
    db.refresh(campaign)
    return campaign


def delete_campaign(db: Session, campaign: AiMarketingCampaign) -> None:
    db.delete(campaign)
    db.commit()


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
        "max_drafts_per_day": int(getattr(campaign, "max_drafts_per_day", 1) or 1),
        "recency": campaign.recency or "week",
        "default_destination_page": campaign.default_destination_page or DEFAULT_DESTINATION_URL,
        "include_disclosure": bool(campaign.include_disclosure),
        "scheduled_digest_enabled": bool(campaign.scheduled_digest_enabled),
        "created_at": _iso(campaign.created_at),
        "updated_at": _iso(campaign.updated_at),
    }


def campaign_run_to_dict(run: AiMarketingCampaignRun) -> dict[str, Any]:
    return {
        "id": run.id,
        "campaign_id": run.campaign_id,
        "campaign_type": run.campaign_type,
        "run_at": _iso(run.run_at),
        "status": run.status,
        "candidates_considered": int(run.candidates_considered or 0),
        "drafts_generated": int(run.drafts_generated or 0),
        "emails_sent": int(run.emails_sent or 0),
        "failure_reason": run.failure_reason,
        "payload": _load_object(run.payload_json),
    }


def campaign_to_dict_with_runs(db: Session, campaign: AiMarketingCampaign, *, limit: int = 5) -> dict[str, Any]:
    payload = campaign_to_dict(campaign)
    runs = db.execute(
        select(AiMarketingCampaignRun)
        .where(AiMarketingCampaignRun.campaign_id == campaign.id)
        .order_by(desc(AiMarketingCampaignRun.run_at), desc(AiMarketingCampaignRun.id))
        .limit(max(1, min(limit, 20)))
    ).scalars().all()
    payload["recent_runs"] = [campaign_run_to_dict(run) for run in runs]
    payload["last_status"] = runs[0].status if runs else None
    return payload


def record_campaign_run(
    db: Session,
    campaign: AiMarketingCampaign,
    summary: dict[str, Any],
    *,
    candidates_considered: int | None = None,
    drafts_generated: int | None = None,
    emails_sent: int | None = None,
    failure_reason: str | None = None,
) -> AiMarketingCampaignRun:
    warnings = _coerce_json_list(summary.get("warnings"))
    errors = _coerce_json_list(summary.get("errors"))
    status = str(summary.get("status") or "ok")
    reason = failure_reason or (str(errors[0]) if errors else str(warnings[0]) if warnings and status not in {"ok", "success"} else None)
    run = AiMarketingCampaignRun(
        campaign_id=campaign.id,
        campaign_type=_normalize_campaign_type(campaign.campaign_type, fallback_mode=campaign.mode),
        run_at=datetime.now(timezone.utc),
        status=status,
        candidates_considered=int(candidates_considered if candidates_considered is not None else summary.get("candidates_considered") or summary.get("articles_considered") or 0),
        drafts_generated=int(drafts_generated if drafts_generated is not None else summary.get("drafts_generated") or summary.get("created") or 0),
        emails_sent=int(emails_sent if emails_sent is not None else summary.get("emails_sent") or 0),
        failure_reason=_truncate(reason or "", 1000) or None,
        payload_json=_dump_object({key: value for key, value in summary.items() if key != "opportunities"}),
    )
    db.add(run)
    db.commit()
    db.refresh(run)
    return run


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
    assets = _opportunity_assets(opportunity, suggestion=suggestion, include_download_urls=True)
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


def _opportunity_assets(
    opportunity: AiMarketingOpportunity,
    *,
    suggestion: AiMarketingSuggestion | None = None,
    include_download_urls: bool = False,
) -> list[dict[str, Any]]:
    assets = _normalize_assets(_load_json_list(opportunity.asset_refs_json) + (_load_json_list(suggestion.assets_json) if suggestion else []))
    assets = _prefer_generated_thumbnail_assets(assets)
    if not include_download_urls:
        return assets
    decorated: list[dict[str, Any]] = []
    for index, asset in enumerate(assets):
        next_asset = dict(asset)
        if _asset_data_uri(next_asset):
            next_asset["download_url"] = f"/api/admin/ai-growth/drafts/{opportunity.id}/assets/{index}/download"
        decorated.append(next_asset)
    return decorated


def _prefer_generated_thumbnail_assets(assets: list[dict[str, Any]]) -> list[dict[str, Any]]:
    generated = [
        asset
        for asset in assets
        if str(asset.get("template") or "") == "generated_thumbnail"
        and _asset_data_uri(asset).lower().startswith("data:image/jpeg")
    ]
    if not generated:
        return assets
    generated_keys = {(str(asset.get("url") or ""), str(asset.get("thumbnail_url") or "")) for asset in generated}
    filtered = []
    for asset in assets:
        key = (str(asset.get("url") or ""), str(asset.get("thumbnail_url") or ""))
        if key in generated_keys:
            continue
        if _is_legacy_generated_draft_card(asset):
            continue
        filtered.append(asset)
    return [*generated, *filtered]


def _is_legacy_generated_draft_card(asset: dict[str, Any]) -> bool:
    template = str(asset.get("template") or "")
    card_type = str(asset.get("card_type") or "")
    data_uri = _asset_data_uri(asset).lower()
    return (
        data_uri.startswith("data:image/svg+xml")
        and template in SOCIAL_CARD_TEMPLATES
        and card_type in SOCIAL_CARD_TYPES
    )


def ai_growth_asset_download(
    db: Session,
    opportunity: AiMarketingOpportunity,
    asset_index: int,
) -> dict[str, Any]:
    latest = latest_suggestions_by_opportunity(db, [opportunity.id]).get(opportunity.id)
    assets = _opportunity_assets(opportunity, suggestion=latest)
    if asset_index < 0 or asset_index >= len(assets):
        raise ValueError("Asset not found.")
    asset = assets[asset_index]
    payload = _decode_data_uri_asset(asset)
    if not payload:
        raise ValueError("Asset is not a generated downloadable image.")
    return payload


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
        "open_x_reply_source": source_url if content_type == "x_reply" and source_url else None,
        "open_reddit": "https://www.reddit.com/",
        "open_reddit_thread": source_url if source_url and "reddit.com" in source_url.lower() else None,
        "open_reddit_submit": f"https://www.reddit.com/r/{subreddit}/submit" if subreddit and content_type == "reddit_thread" else None,
    }
    return links


def _article_provider_status(db: Session | None = None) -> dict[str, Any]:
    configured = bool(resolved_setting_value(db, FMP_API_KEY))
    return {
        "provider": ARTICLE_REACTIVE_PROVIDER,
        "label": "FMP Articles API",
        "configured": configured,
        "status": "configured" if configured else "missing",
        "managed_by": "server_env",
        "admin_message": "Managed outside the admin UI",
    }


def fetch_fmp_articles(db: Session | None = None, *, page: int = 0, limit: int = ARTICLE_RUN_DEFAULT_LIMIT) -> list[dict[str, Any]]:
    api_key = resolved_setting_value(db, FMP_API_KEY)
    if not api_key:
        raise MissingMarketingCredential("FMP Articles API key missing. Configure FMP_API_KEY on the server.")
    response = requests.get(
        FMP_ARTICLES_URL,
        params={"page": page, "limit": limit, "apikey": api_key},
        timeout=25,
    )
    response.raise_for_status()
    data = response.json()
    if isinstance(data, dict):
        for key in ("items", "articles", "data", "results"):
            value = data.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
        return []
    return [item for item in data if isinstance(item, dict)] if isinstance(data, list) else []


def _parse_article_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    text_value = str(value).strip()
    if not text_value:
        return None
    for candidate in (text_value, text_value.replace("Z", "+00:00")):
        try:
            parsed = datetime.fromisoformat(candidate)
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(text_value[: len(fmt)], fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def _article_value(article: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = article.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return ""


def _extract_article_tickers(article: dict[str, Any]) -> list[str]:
    raw_values: list[Any] = []
    for key in ("tickers", "symbols", "symbol", "ticker", "stock", "stocks"):
        value = article.get(key)
        if isinstance(value, list):
            raw_values.extend(value)
        elif isinstance(value, str):
            raw_values.extend(re.split(r"[\s,;|]+", value))
    text = " ".join(
        _article_value(article, key)
        for key in ("title", "text", "content", "summary", "snippet", "description")
    )
    raw_values.extend(_matched_tickers(text, []))
    return _normalized_tickers(raw_values)


def _normalize_fmp_article(article: dict[str, Any]) -> dict[str, Any] | None:
    title = _article_value(article, "title", "headline", "name")
    url = _article_value(article, "url", "link", "articleUrl")
    if not title or not url:
        return None
    published_at = _parse_article_datetime(
        article.get("publishedDate")
        or article.get("published_at")
        or article.get("date")
        or article.get("created_at")
    )
    site = _article_value(article, "site", "source", "publisher")
    summary = _truncate(_article_value(article, "text", "summary", "snippet", "description"), 1500)
    provider_article_id = _article_value(article, "id", "articleId", "uuid") or _dedupe_key(url)
    dedupe_hash = _dedupe_key(provider_article_id or url or title)
    return {
        "provider": "fmp",
        "provider_article_id": provider_article_id,
        "title": title,
        "url": url,
        "site": site or None,
        "published_at": published_at,
        "tickers": _extract_article_tickers(article),
        "image_url": _article_value(article, "image", "imageUrl", "thumbnail") or None,
        "summary": summary,
        "raw": article,
        "dedupe_hash": dedupe_hash,
    }


def upsert_article_candidate(db: Session, normalized: dict[str, Any]) -> tuple[AiMarketingArticleCandidate, bool]:
    now = datetime.now(timezone.utc)
    existing = db.execute(
        select(AiMarketingArticleCandidate).where(
            AiMarketingArticleCandidate.provider == normalized["provider"],
            AiMarketingArticleCandidate.dedupe_hash == normalized["dedupe_hash"],
        )
    ).scalar_one_or_none()
    if existing:
        existing.title = _truncate(normalized["title"], 500) or existing.title
        existing.url = _truncate(normalized["url"], 1200) or existing.url
        existing.site = _truncate(normalized.get("site"), 200)
        existing.published_at = normalized.get("published_at")
        existing.tickers_json = _dump_list(normalized.get("tickers") or [])
        existing.image_url = _truncate(normalized.get("image_url"), 1200)
        existing.summary = _truncate(normalized.get("summary"), 1500)
        existing.raw_metadata_json = _dump_object(normalized.get("raw") or {})
        existing.last_seen_at = now
        db.commit()
        db.refresh(existing)
        return existing, False
    candidate = AiMarketingArticleCandidate(
        provider=normalized["provider"],
        provider_article_id=_truncate(normalized.get("provider_article_id"), 240),
        title=_truncate(normalized["title"], 500) or "Untitled article",
        url=_truncate(normalized["url"], 1200) or DEFAULT_DESTINATION_URL,
        site=_truncate(normalized.get("site"), 200),
        published_at=normalized.get("published_at"),
        tickers_json=_dump_list(normalized.get("tickers") or []),
        image_url=_truncate(normalized.get("image_url"), 1200),
        summary=_truncate(normalized.get("summary"), 1500),
        raw_metadata_json=_dump_object(normalized.get("raw") or {}),
        first_seen_at=now,
        last_seen_at=now,
        dedupe_hash=normalized["dedupe_hash"],
    )
    db.add(candidate)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        existing = db.execute(
            select(AiMarketingArticleCandidate).where(
                AiMarketingArticleCandidate.provider == normalized["provider"],
                AiMarketingArticleCandidate.dedupe_hash == normalized["dedupe_hash"],
            )
        ).scalar_one()
        return existing, False
    db.refresh(candidate)
    return candidate, True


def _event_count_for_ticker(db: Session, ticker: str, event_types: set[str], since: datetime) -> int:
    return int(
        db.execute(
            select(func.count())
            .select_from(Event)
            .where(
                func.upper(Event.symbol) == ticker.upper(),
                Event.event_type.in_(sorted(event_types)),
                func.coalesce(Event.event_date, Event.ts) >= since,
            )
        ).scalar()
        or 0
    )


def _walnut_context_for_article(db: Session, candidate: AiMarketingArticleCandidate, tickers: list[str]) -> dict[str, Any]:
    since = datetime.now(timezone.utc) - timedelta(days=90)
    context: dict[str, Any] = {
        "tickers": tickers,
        "themes": _article_themes(candidate.title, candidate.summary),
        "watchlists": [],
        "saved_screens": [],
        "confirmation": [],
        "signals": [],
        "congress_activity": [],
        "insider_activity": [],
        "institutional_activity": [],
        "government_contracts": [],
        "ticker_context": [],
    }
    for ticker in tickers[:8]:
        watchlists = db.execute(
            select(Watchlist.name)
            .join(WatchlistItem, WatchlistItem.watchlist_id == Watchlist.id)
            .join(Security, Security.id == WatchlistItem.security_id)
            .where(func.upper(Security.symbol) == ticker.upper())
            .limit(5)
        ).scalars().all()
        if watchlists:
            context["watchlists"].append({"ticker": ticker, "names": list(watchlists)})

        saved_screen_rows = db.execute(
            select(SavedScreen.name)
            .join(SavedScreenSnapshot, SavedScreenSnapshot.saved_screen_id == SavedScreen.id)
            .where(func.upper(SavedScreenSnapshot.ticker) == ticker.upper())
            .limit(5)
        ).scalars().all()
        if saved_screen_rows:
            context["saved_screens"].append({"ticker": ticker, "names": list(saved_screen_rows)})

        snapshots = db.execute(
            select(ConfirmationMonitoringSnapshot)
            .where(func.upper(ConfirmationMonitoringSnapshot.ticker) == ticker.upper())
            .order_by(desc(ConfirmationMonitoringSnapshot.observed_at))
            .limit(3)
        ).scalars().all()
        if snapshots:
            context["confirmation"].append(
                {
                    "ticker": ticker,
                    "latest": [
                        {
                            "score": row.score,
                            "band": row.band,
                            "direction": row.direction,
                            "source_count": row.source_count,
                        }
                        for row in snapshots
                    ],
                }
            )
        confirmation_events = db.execute(
            select(ConfirmationMonitoringEvent.event_type, ConfirmationMonitoringEvent.title)
            .where(func.upper(ConfirmationMonitoringEvent.ticker) == ticker.upper(), ConfirmationMonitoringEvent.created_at >= since)
            .order_by(desc(ConfirmationMonitoringEvent.created_at))
            .limit(5)
        ).all()
        if confirmation_events:
            context["signals"].append({"ticker": ticker, "events": [{"type": row[0], "title": row[1]} for row in confirmation_events]})

        congress_count = _event_count_for_ticker(db, ticker, {"congress_trade", "congress_treasury_trade", "congress_crypto_trade"}, since)
        if congress_count:
            context["congress_activity"].append({"ticker": ticker, "recent_count": congress_count})

        insider_count = _event_count_for_ticker(db, ticker, {"insider_trade"}, since)
        if insider_count:
            context["insider_activity"].append({"ticker": ticker, "recent_count": insider_count})

        institutional_rows = db.execute(
            select(InstitutionalActivityEvent.title, InstitutionalActivityEvent.direction, InstitutionalActivityEvent.materiality_score)
            .where(func.upper(InstitutionalActivityEvent.normalized_symbol) == ticker.upper())
            .order_by(desc(InstitutionalActivityEvent.filing_date))
            .limit(3)
        ).all()
        if institutional_rows:
            context["institutional_activity"].append(
                {
                    "ticker": ticker,
                    "items": [
                        {"title": row[0], "direction": row[1], "materiality_score": row[2]}
                        for row in institutional_rows
                    ],
                }
            )

        contract_count = int(
            db.execute(
                select(func.count())
                .select_from(GovernmentContract)
                .where(func.upper(GovernmentContract.symbol) == ticker.upper(), GovernmentContract.award_date >= since.date())
            ).scalar()
            or 0
        )
        action_count = int(
            db.execute(
                select(func.count())
                .select_from(GovernmentContractAction)
                .where(func.upper(GovernmentContractAction.symbol) == ticker.upper(), GovernmentContractAction.action_date >= since.date())
            ).scalar()
            or 0
        )
        if contract_count or action_count:
            context["government_contracts"].append({"ticker": ticker, "awards": contract_count, "actions": action_count})

        meta = db.get(TickerMeta, ticker.upper())
        if meta:
            context["ticker_context"].append(
                {
                    "ticker": ticker,
                    "company_name": meta.company_name,
                    "sector": meta.sector,
                    "industry": meta.industry,
                }
            )
    return context


def _walnut_context_for_research_tickers(
    db: Session,
    tickers: list[str],
    *,
    title: str | None = None,
    excerpt: str | None = None,
) -> dict[str, Any]:
    context = _walnut_context_for_article(
        db,
        AiMarketingArticleCandidate(
            provider="internal",
            title=title or "Reddit research thread",
            summary=excerpt,
            url=DEFAULT_DESTINATION_URL,
            dedupe_hash="",
        ),
        _normalized_tickers(tickers)[:8],
    )
    context["research_thread"] = True
    context["ticker_pages"] = [
        {"ticker": ticker, "url": f"https://walnutmarkets.com/ticker/{ticker}"}
        for ticker in context.get("tickers", [])
    ]
    return context


def _article_themes(title: str | None, summary: str | None) -> list[str]:
    text = f"{title or ''} {summary or ''}".lower()
    return [theme for theme, terms in ARTICLE_RELEVANT_THEMES.items() if any(term in text for term in terms)]


def score_article_candidate(
    db: Session,
    candidate: AiMarketingArticleCandidate,
    *,
    campaign: AiMarketingCampaign | None = None,
) -> dict[str, Any]:
    tickers = _normalized_tickers(_load_list(candidate.tickers_json) + _matched_tickers(f"{candidate.title} {candidate.summary or ''}", []))
    context = _walnut_context_for_article(db, candidate, tickers)
    now = datetime.now(timezone.utc)
    age_hours = None
    if candidate.published_at:
        published = candidate.published_at if candidate.published_at.tzinfo else candidate.published_at.replace(tzinfo=timezone.utc)
        age_hours = max(0.0, (now - published).total_seconds() / 3600)
    freshness_score = 80 if age_hours is None else 100 if age_hours <= 12 else 85 if age_hours <= 24 else 65 if age_hours <= 72 else 25
    campaign_tickers = set(_load_list(campaign.tickers_json) if campaign else [])
    ticker_relevance_score = 70 if tickers else 15
    if campaign_tickers and set(tickers) & campaign_tickers:
        ticker_relevance_score = 95
    walnut_signal_keys = [
        "watchlists",
        "saved_screens",
        "confirmation",
        "signals",
        "congress_activity",
        "insider_activity",
        "institutional_activity",
        "government_contracts",
        "ticker_context",
    ]
    walnut_context_hits = sum(1 for key in walnut_signal_keys if context.get(key))
    walnut_context_score = min(100, walnut_context_hits * 18)
    theme_score = 25 if context["themes"] else 0
    if context["themes"] and tickers:
        theme_score = 55
    news_relevance_score = max(theme_score, 60 if tickers else 20)
    recent_duplicate = _recent_article_draft_exists(db, candidate, days=ARTICLE_DEDUPE_DAYS)
    duplicate_risk_score = 100 if recent_duplicate else 0
    promotional_risk_score = 35 if _looks_promotional(candidate.title, candidate.summary) else 10
    uniqueness_score = 100 - duplicate_risk_score
    audience_fit_score = min(100, 45 + walnut_context_score // 2 + theme_score // 2)
    final_score = int(
        freshness_score * 0.14
        + ticker_relevance_score * 0.16
        + walnut_context_score * 0.26
        + news_relevance_score * 0.14
        + uniqueness_score * 0.12
        + audience_fit_score * 0.14
        - duplicate_risk_score * 0.10
        - promotional_risk_score * 0.04
    )
    clear_walnut_angle = walnut_context_hits > 0 or (bool(tickers) and bool(context["themes"]))
    rejected_reasons: list[str] = []
    if not tickers and not context["themes"]:
        rejected_reasons.append("No ticker or Walnut-relevant theme detected.")
    if not clear_walnut_angle:
        rejected_reasons.append("No clear Walnut angle.")
    if age_hours is not None and age_hours > 96:
        rejected_reasons.append("Article is stale.")
    if recent_duplicate:
        rejected_reasons.append("Recent duplicate article draft exists.")
    if promotional_risk_score >= 35:
        rejected_reasons.append("Promotional/clickbait risk is elevated.")
    if final_score < ARTICLE_MIN_FINAL_SCORE:
        rejected_reasons.append("Score is below article-reactive threshold.")
    return {
        "freshness_score": int(freshness_score),
        "ticker_relevance_score": int(ticker_relevance_score),
        "walnut_context_score": int(walnut_context_score),
        "news_relevance_score": int(news_relevance_score),
        "uniqueness_score": int(uniqueness_score),
        "audience_fit_score": int(audience_fit_score),
        "duplicate_risk_score": int(duplicate_risk_score),
        "promotional_risk_score": int(promotional_risk_score),
        "final_score": max(0, min(100, final_score)),
        "clear_walnut_angle": clear_walnut_angle,
        "rejected": bool(rejected_reasons),
        "rejected_reasons": _dedupe_strings(rejected_reasons),
        "tickers": tickers,
        "themes": context["themes"],
        "walnut_context": context,
    }


def _looks_promotional(title: str | None, summary: str | None) -> bool:
    text = f"{title or ''} {summary or ''}".lower()
    return any(term in text for term in ("penny stock", "explode", "guaranteed", "load up", "millionaire", "secret stock"))


def _recent_article_draft_exists(db: Session, candidate: AiMarketingArticleCandidate, *, days: int) -> bool:
    since = datetime.now(timezone.utc) - timedelta(days=days)
    source_id = f"fmp:{candidate.dedupe_hash}"
    return bool(
        db.execute(
            select(AiMarketingOpportunity.id)
            .where(
                AiMarketingOpportunity.source_provider == ARTICLE_REACTIVE_PROVIDER,
                AiMarketingOpportunity.source_id == source_id,
                AiMarketingOpportunity.created_at >= since,
            )
            .limit(1)
        ).scalar_one_or_none()
    )


def _article_card_asset(candidate: AiMarketingArticleCandidate, scoring: dict[str, Any]) -> dict[str, Any]:
    tickers = scoring.get("tickers") or []
    themes = scoring.get("themes") or []
    title_subject = ", ".join([*(f"${ticker}" for ticker in tickers[:3]), *themes[:2]]) or "Market context"
    bullets = _article_context_bullets(scoring)[:3]
    spec = _normalize_social_card_spec(
        {
            "card_type": "article_reactive",
            "template": "article_reactive",
            "ticker": tickers[0] if tickers else "",
            "tickers": tickers,
            "sentiment": "notable",
            "headline": title_subject,
            "subheadline": candidate.title,
            "bullets": bullets,
            "key_stats": [
                {"label": "Fit", "value": f"{scoring.get('final_score', 0)}/100"},
                {"label": "Signal", "value": "Review"},
            ],
            "chips": [*(themes[:2]), "Article", "Signals"],
            "cta": "View the data on Walnut",
            "url": "https://walnutmarkets.com",
            "visual_emphasis": "mini chart",
            "source_label": str(candidate.site or "FMP / linked article"),
            "tone": "market-native",
            "include_chart": True,
            "include_cta": True,
            "include_source_tag": True,
            "include_walnut_url": True,
        },
        fallback_card_type="article_reactive",
        fallback_tickers=tickers,
        fallback_url="https://walnutmarkets.com",
    )
    asset = _social_card_asset(spec, source_note="Source: FMP / linked article. Do not reuse article thumbnails unless redistribution is explicitly allowed.")
    asset["suggested_caption"] = "Walnut-branded 16:9 article reaction card generated from structured metadata."
    return asset


def _article_context_bullets(scoring: dict[str, Any]) -> list[str]:
    context = scoring.get("walnut_context") or {}
    bullets: list[str] = []
    if context.get("signals") or context.get("confirmation"):
        bullets.append("Signal and confirmation context available")
    if context.get("congress_activity"):
        bullets.append("Recent Congress disclosure activity to cross-check")
    if context.get("insider_activity"):
        bullets.append("Recent insider disclosure activity to cross-check")
    if context.get("institutional_activity"):
        bullets.append("Reported institutional activity adds a filing-date lens")
    if context.get("government_contracts"):
        bullets.append("Government contract activity adds policy/procurement context")
    if context.get("watchlists") or context.get("saved_screens"):
        bullets.append("Ticker appears in Walnut watchlists or saved screens")
    if not bullets and context.get("themes"):
        bullets.append(f"Theme: {', '.join(context['themes'][:2])}")
    return bullets or ["Clear market hook", "Walnut context required before posting"]


def _social_card_json_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {
            "card_type": {"type": "string", "enum": sorted(SOCIAL_CARD_TYPES)},
            "template": {"type": "string", "enum": sorted(SOCIAL_CARD_TEMPLATES)},
            "ticker": {"type": "string"},
            "tickers": {"type": "array", "items": {"type": "string"}},
            "sentiment": {"type": "string", "enum": sorted(SOCIAL_CARD_SENTIMENTS)},
            "headline": {"type": "string"},
            "subheadline": {"type": "string"},
            "bullets": {"type": "array", "items": {"type": "string"}},
            "key_stats": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "label": {"type": "string"},
                        "value": {"type": "string"},
                    },
                    "required": ["label", "value"],
                    "additionalProperties": False,
                },
            },
            "chips": {"type": "array", "items": {"type": "string"}},
            "cta": {"type": "string"},
            "url": {"type": "string"},
            "visual_emphasis": {"type": "string"},
            "source_label": {"type": "string"},
            "tone": {"type": "string", "enum": sorted(SOCIAL_CARD_TONES)},
            "include_chart": {"type": "boolean"},
            "include_cta": {"type": "boolean"},
            "include_source_tag": {"type": "boolean"},
            "include_walnut_url": {"type": "boolean"},
        },
        "required": [
            "card_type",
            "template",
            "ticker",
            "tickers",
            "sentiment",
            "headline",
            "subheadline",
            "bullets",
            "key_stats",
            "chips",
            "cta",
            "url",
            "visual_emphasis",
            "source_label",
            "tone",
            "include_chart",
            "include_cta",
            "include_source_tag",
            "include_walnut_url",
        ],
        "additionalProperties": False,
    }


def _social_card_preferences(opportunity_metadata: dict[str, Any]) -> dict[str, Any]:
    inputs = opportunity_metadata.get("inputs") if isinstance(opportunity_metadata.get("inputs"), dict) else {}
    raw = inputs.get("social_card") if isinstance(inputs.get("social_card"), dict) else {}
    return {
        "template": str(raw.get("template") or raw.get("card_template") or "auto").strip().lower(),
        "tone": str(raw.get("tone") or inputs.get("tone") or opportunity_metadata.get("tone") or "market-native").strip().lower(),
        "include_chart": bool(raw.get("include_chart", True)),
        "include_cta": bool(raw.get("include_cta", True)),
        "include_source_tag": bool(raw.get("include_source_tag", True)),
        "include_walnut_url": bool(raw.get("include_walnut_url", True)),
        "include_article_thumbnail": bool(raw.get("include_article_thumbnail", False)),
    }


def _social_card_type_for_context(campaign_type: str | None, content_type: str | None, preferences: dict[str, Any]) -> str:
    template = str(preferences.get("template") or "").strip().lower()
    if template in {"article_reactive", "fast_reaction"}:
        return "article_reactive"
    if template in {"ticker_signal", "signal_card"}:
        return "ticker_signal"
    if template in {"congress_insider_activity", "activity_card"}:
        return "congress_insider_activity"
    if template in {"research_cover", "dd_cover"}:
        return "research_cover"
    if campaign_type == "article_reactive_x":
        return "article_reactive"
    if campaign_type == "reddit_research_thread" or content_type == "reddit_thread":
        return "research_cover"
    return "ticker_signal"


def _normalize_social_card_spec(
    value: Any,
    *,
    fallback_card_type: str,
    fallback_tickers: list[str],
    fallback_url: str,
    preferences: dict[str, Any] | None = None,
    visual_brief: Any = None,
) -> dict[str, Any]:
    raw = value if isinstance(value, dict) else {}
    prefs = preferences or {}
    card_type = str(raw.get("card_type") or fallback_card_type or "ticker_signal").strip().lower()
    if card_type not in SOCIAL_CARD_TYPES:
        card_type = fallback_card_type if fallback_card_type in SOCIAL_CARD_TYPES else "ticker_signal"
    template = str(raw.get("template") or prefs.get("template") or card_type).strip().lower()
    if template == "auto" or template not in SOCIAL_CARD_TEMPLATES:
        template = card_type
    tone = str(raw.get("tone") or prefs.get("tone") or "market-native").strip().lower()
    if tone not in SOCIAL_CARD_TONES:
        tone = "market-native"
    tickers = _normalized_tickers(fallback_tickers) or _normalized_tickers(raw.get("tickers"))
    raw_ticker = _normalized_tickers([raw.get("ticker")])[0] if _normalized_tickers([raw.get("ticker")]) else ""
    ticker = tickers[0] if tickers else raw_ticker
    if ticker and ticker not in tickers:
        tickers = [ticker, *tickers]
    sentiment = str(raw.get("sentiment") or "notable").strip().lower()
    if sentiment not in SOCIAL_CARD_SENTIMENTS:
        sentiment = "notable"
    headline = _truncate(str(raw.get("headline") or "").strip(), 120) or _fallback_social_card_headline(card_type, ticker)
    subheadline = _truncate(str(raw.get("subheadline") or "").strip(), 180) or "Walnut data worth reviewing before the market narrative gets too clean."
    bullets = [_truncate(str(item or "").strip(), 110) or "" for item in _coerce_json_list(raw.get("bullets"))]
    bullets = [item for item in bullets if item][:5]
    if not bullets:
        bullets = _social_card_bullets_from_visual_brief(visual_brief) or [
            "One signal is noise. Data is intelligence.",
            "Watch price, disclosure, and confirmation data together.",
            "Human review required before posting.",
        ]
    key_stats: list[dict[str, str]] = []
    for item in _coerce_json_list(raw.get("key_stats")):
        if not isinstance(item, dict):
            continue
        label = _truncate(str(item.get("label") or "").strip(), 22) or ""
        stat_value = _truncate(str(item.get("value") or "").strip(), 24) or ""
        if label or stat_value:
            key_stats.append({"label": label or "Signal", "value": stat_value or "Review"})
    if not key_stats:
        key_stats = _social_card_stats_from_visual_brief(visual_brief) or [{"label": "Confirmation", "value": "Review"}, {"label": "Mode", "value": sentiment.title()}]
    chips = [_truncate(str(item or "").strip(), 24) or "" for item in _coerce_json_list(raw.get("chips"))]
    chips = [item for item in _dedupe_strings(chips) if item][:5]
    if not chips:
        chips = _default_social_card_chips(card_type)
    url = _truncate(str(raw.get("url") or fallback_url or DEFAULT_DESTINATION_URL).strip(), 1200) or DEFAULT_DESTINATION_URL
    return {
        "card_type": card_type,
        "template": template,
        "ticker": ticker,
        "tickers": tickers[:5],
        "sentiment": sentiment,
        "headline": headline,
        "subheadline": subheadline,
        "bullets": bullets[:5],
        "key_stats": key_stats[:4],
        "chips": chips[:5],
        "cta": _truncate(str(raw.get("cta") or "View the data on Walnut").strip(), 80) or "View the data on Walnut",
        "url": url,
        "visual_emphasis": _truncate(str(raw.get("visual_emphasis") or "underlying data").strip(), 80) or "underlying data",
        "source_label": _truncate(str(raw.get("source_label") or "Walnut intelligence").strip(), 48) or "Walnut intelligence",
        "tone": tone,
        "include_chart": bool(raw.get("include_chart", prefs.get("include_chart", True))),
        "include_cta": bool(raw.get("include_cta", prefs.get("include_cta", True))),
        "include_source_tag": bool(raw.get("include_source_tag", prefs.get("include_source_tag", True))),
        "include_walnut_url": bool(raw.get("include_walnut_url", prefs.get("include_walnut_url", True))),
    }


def _fallback_social_card_headline(card_type: str, ticker: str) -> str:
    if card_type == "article_reactive":
        return f"{ticker or 'This headline'} needs a signal check"
    if card_type == "congress_insider_activity":
        return f"{ticker or 'Disclosure'} activity just hit the data"
    if card_type == "research_cover":
        return f"{ticker or 'Market'} research data"
    return f"{ticker or 'Ticker'} data is active"


def _default_social_card_chips(card_type: str) -> list[str]:
    if card_type == "article_reactive":
        return ["Article", "Why now", "Signals"]
    if card_type == "congress_insider_activity":
        return ["Disclosure", "Activity", "Why it matters"]
    if card_type == "research_cover":
        return ["Research", "DD", "Evidence trail"]
    return ["Signals", "Confirmation", "Price/Volume"]


def _social_card_bullets_from_visual_brief(value: Any) -> list[str]:
    if not isinstance(value, dict):
        return []
    bullets = []
    for row in _coerce_json_list(value.get("rows"))[:4]:
        if not isinstance(row, dict):
            continue
        label = str(row.get("label") or "").strip()
        note = str(row.get("note") or "").strip()
        row_value = str(row.get("value") or "").strip()
        line = " - ".join(part for part in (label, row_value, note) if part)
        if line:
            bullets.append(_truncate(line, 110) or "")
    return [item for item in bullets if item]


def _social_card_stats_from_visual_brief(value: Any) -> list[dict[str, str]]:
    if not isinstance(value, dict):
        return []
    stats = []
    for row in _coerce_json_list(value.get("rows"))[:3]:
        if not isinstance(row, dict):
            continue
        label = _truncate(str(row.get("label") or "").strip(), 22) or ""
        row_value = _truncate(str(row.get("value") or "").strip(), 24) or ""
        if label or row_value:
            stats.append({"label": label or "Signal", "value": row_value or "Active"})
    return stats


def _social_card_asset(spec: dict[str, Any], *, source_note: str | None = None) -> dict[str, Any]:
    card_url = _social_card_data_uri(spec)
    card_type = str(spec.get("card_type") or "ticker_signal")
    template = str(spec.get("template") or card_type)
    tone = str(spec.get("tone") or "market-native")
    title = _truncate(str(spec.get("headline") or "Walnut social card").strip(), 90) or "Walnut social card"
    return {
        "title": f"Walnut {card_type.replace('_', ' ')} card: {title}",
        "asset_type": "image",
        "url": card_url,
        "thumbnail_url": card_url,
        "suggested_caption": _truncate(str(spec.get("headline") or title), 180) or title,
        "source_data_notes": source_note or f"Template: {template}; tone: {tone}; deterministic 1600x900 Walnut social-card render.",
        "template": template,
        "card_type": card_type,
        "tone": tone,
        "card_spec": spec,
        "width": SOCIAL_CARD_WIDTH,
        "height": SOCIAL_CARD_HEIGHT,
    }


def _generated_thumbnail_asset(
    *,
    api_key: str,
    card_spec: dict[str, Any],
    suggested_post: str,
    visual_brief: Any,
) -> dict[str, Any] | None:
    if not marketing_image_generation_enabled():
        return None
    prompt = _generated_thumbnail_prompt(card_spec=card_spec, suggested_post=suggested_post, visual_brief=visual_brief)
    model = marketing_image_model()
    size = os.getenv(AI_MARKETING_IMAGE_SIZE, "").strip() or DEFAULT_AI_MARKETING_IMAGE_SIZE
    quality = os.getenv(AI_MARKETING_IMAGE_QUALITY, "").strip() or DEFAULT_AI_MARKETING_IMAGE_QUALITY
    payload = {
        "model": model,
        "prompt": prompt,
        "size": size,
        "quality": quality,
        "output_format": "jpeg",
        "output_compression": 86,
        "moderation": "auto",
    }
    try:
        response = requests.post(
            "https://api.openai.com/v1/images/generations",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json=payload,
            timeout=130,
        )
    except requests.RequestException:
        logger.exception("ai_growth_image_generation_request_failed")
        return None
    if response.status_code >= 400:
        logger.warning("ai_growth_image_generation_failed status=%s body=%s", response.status_code, response.text[:500])
        return None
    data = response.json()
    image_data = (data.get("data") or [{}])[0] if isinstance(data, dict) else {}
    b64_image = str(image_data.get("b64_json") or "").strip()
    if not b64_image:
        logger.warning("ai_growth_image_generation_missing_b64")
        return None
    branded_b64_image, branded = _apply_walnut_thumbnail_branding(b64_image)
    ticker = str(card_spec.get("ticker") or "").upper()
    headline = str(card_spec.get("headline") or "Walnut Markets").strip()
    title = _truncate(f"Walnut generated thumbnail: {headline}", 120) or "Walnut generated thumbnail"
    revised_prompt = _truncate(str(image_data.get("revised_prompt") or "").strip(), 2000) or ""
    data_uri = f"data:image/jpeg;base64,{branded_b64_image}"
    return {
        "title": title,
        "asset_type": "image",
        "url": data_uri,
        "thumbnail_url": data_uri,
        "suggested_caption": _truncate(headline, 180) or title,
        "source_data_notes": f"Generated 16:9 Walnut Markets thumbnail with {model}.",
        "template": "generated_thumbnail",
        "card_type": str(card_spec.get("card_type") or "ticker_signal"),
        "tone": str(card_spec.get("tone") or "market-native"),
        "image_prompt": prompt,
        "image_revised_prompt": revised_prompt,
        "image_model": model,
        "brand_overlay": "walnut_markets_logo_lockup" if branded else "",
        "ticker": ticker,
        "width": SOCIAL_CARD_WIDTH,
        "height": SOCIAL_CARD_HEIGHT,
    }


def _apply_walnut_thumbnail_branding(b64_image: str) -> tuple[str, bool]:
    try:
        from PIL import Image, ImageDraw
    except Exception:
        logger.warning("Pillow is unavailable; AI Growth thumbnail logo overlay skipped.")
        return b64_image, False
    if not os.path.exists(WALNUT_THUMBNAIL_LOGO_LOCKUP_PATH):
        logger.warning("Walnut thumbnail logo lockup asset is missing: %s", WALNUT_THUMBNAIL_LOGO_LOCKUP_PATH)
        return b64_image, False
    try:
        image_bytes = base64.b64decode(b64_image, validate=True)
        base_image = Image.open(BytesIO(image_bytes)).convert("RGBA")
        logo = Image.open(WALNUT_THUMBNAIL_LOGO_LOCKUP_PATH).convert("RGBA")
    except Exception:
        logger.exception("ai_growth_thumbnail_logo_overlay_decode_failed")
        return b64_image, False

    width, height = base_image.size
    if width < 600 or height < 320:
        return b64_image, False
    logo_target_width = max(320, min(int(width * 0.34), 540))
    logo_target_height = max(1, int(logo.height * (logo_target_width / logo.width)))
    try:
        resampling = Image.Resampling.LANCZOS
    except AttributeError:
        resampling = Image.LANCZOS
    logo = logo.resize((logo_target_width, logo_target_height), resampling)

    x = max(48, int(width * 0.055))
    y = max(46, int(height * 0.065))
    pad_x = max(22, int(width * 0.018))
    pad_y = max(18, int(height * 0.016))
    panel_box = (x - pad_x, y - pad_y, x + logo.width + pad_x, y + logo.height + pad_y)
    overlay = Image.new("RGBA", base_image.size, (0, 0, 0, 0))
    draw = ImageDraw.Draw(overlay)
    draw.rounded_rectangle(panel_box, radius=max(14, int(height * 0.018)), fill=(2, 9, 14, 218))
    base_image = Image.alpha_composite(base_image, overlay)
    base_image.paste(logo, (x, y), logo)

    output = BytesIO()
    base_image.convert("RGB").save(output, format="JPEG", quality=90, optimize=True)
    return base64.b64encode(output.getvalue()).decode("ascii"), True


def _generated_thumbnail_prompt(*, card_spec: dict[str, Any], suggested_post: str, visual_brief: Any) -> str:
    ticker = str(card_spec.get("ticker") or "").upper()
    headline = _thumbnail_headline(card_spec, ticker=ticker)
    reason_line = _thumbnail_reason_line(card_spec, visual_brief=visual_brief)
    visual = _truncate(str(card_spec.get("visual_emphasis") or "").strip(), 120) or "market signal"
    rows = []
    if isinstance(visual_brief, dict):
        for row in _coerce_json_list(visual_brief.get("rows"))[:4]:
            if isinstance(row, dict):
                label = str(row.get("label") or "").strip()
                value = str(row.get("value") or "").strip()
                if label or value:
                    rows.append(f"{label}: {value}".strip(": "))
    context = "; ".join(rows) if rows else _truncate(suggested_post, 220) or headline
    ticker_text = f"${ticker}" if ticker else "Market signal"
    return (
        "Create a polished 16:9 finance-media thumbnail for Walnut Markets, in the same quality tier as a premium ChatGPT-generated market visual. "
        "Style: cinematic dark navy/black studio background, teal/emerald glow, realistic 3D product-render lighting, high contrast, crisp depth of field, premium fintech editorial look. "
        f"Core idea: {visual}. Context: {context}. "
        "Composition: reserve the upper-left 35% as a clean dark logo-safe area; do not draw any logo, icon, tree, brain, mark, or Walnut Markets wordmark yourself because the official Walnut logo will be overlaid after generation. "
        "Use the remaining canvas for one large striking visual metaphor tied to the market story, such as a semiconductor package, filing archive, bank tower, trading terminal glow, disclosure folder, or market infrastructure object. "
        f"Text to render, if any: '{ticker_text}', the headline '{headline}', and one short reason line: '{reason_line}'. "
        "The headline must be a complete, grammatical market statement. Never write vague fragments like 'bearish confirmation is leading'. For bearish setups, use phrasing like 'Bearish trend confirmed' or 'Bearish signal identified' and name the underlying data that confirms it. "
        "Do not render a source line, footer, citation strip, watermark, or any text beginning with 'Source:'. "
        "Avoid: dashboard cards, evidence panels, bullet lists, tiny text, clipped text, charts as the main design, crowded UI, generic stock photos, fake news branding, fake official company logos, fake Walnut logos, and imitation third-party trademarks. "
        "If company names are needed, render them as simple clean text labels, not official logos. The final image should be post-worthy at X/Reddit thumbnail size."
    )


def _thumbnail_headline(card_spec: dict[str, Any], *, ticker: str) -> str:
    raw = _truncate(str(card_spec.get("headline") or "").strip(), 96) or ""
    sentiment = str(card_spec.get("sentiment") or "").strip().lower()
    if re.search(r"\b(bearish|bullish)\s+confirmation\s+(?:is\s+)?leading\b", raw, flags=re.IGNORECASE):
        raw = ""
    if not raw and sentiment == "bearish":
        raw = "Bearish signal identified"
    elif not raw and sentiment == "bullish":
        raw = "Bullish signal identified"
    elif not raw:
        raw = f"${ticker} market data" if ticker else "Market data identified"
    return raw


def _thumbnail_reason_line(card_spec: dict[str, Any], *, visual_brief: Any) -> str:
    candidates = [
        str(card_spec.get("subheadline") or "").strip(),
        *[str(item or "").strip() for item in _coerce_json_list(card_spec.get("bullets"))[:3]],
    ]
    if isinstance(visual_brief, dict):
        rows = []
        for row in _coerce_json_list(visual_brief.get("rows"))[:4]:
            if isinstance(row, dict):
                label = str(row.get("label") or "").strip()
                note = str(row.get("note") or "").strip()
                if label:
                    rows.append(label if not note else f"{label}: {note}")
        if rows:
            candidates.append("; ".join(rows))
    for candidate in candidates:
        candidate = _truncate(candidate, 118) or ""
        if candidate:
            return candidate
    return "Underlying data should explain the move."


def ai_growth_social_card_demo_assets() -> list[dict[str, Any]]:
    demos = [
        {
            "card_type": "ticker_signal",
            "template": "ticker_signal",
            "ticker": "NVDA",
            "tickers": ["NVDA"],
            "sentiment": "bullish",
            "headline": "NVDA's data is still leading",
            "subheadline": "The tell is not one data point. It is confirmation across price, filings, and disclosure context.",
            "bullets": ["Price/volume remains the first confirmation layer.", "Disclosure and filing context keep the move on watch.", "We are watching whether the data broadens or fades."],
            "key_stats": [{"label": "Confirm", "value": "82/100"}, {"label": "RSI", "value": "Active"}, {"label": "Flow", "value": "Watch"}],
            "chips": ["Signals", "Price/Volume", "Filings"],
            "cta": "View the data on Walnut",
            "url": "https://walnutmarkets.com/ticker/NVDA",
            "visual_emphasis": "confirmation score and underlying data",
            "source_label": "Demo signal",
            "tone": "sharp",
            "include_chart": True,
            "include_cta": True,
            "include_source_tag": True,
            "include_walnut_url": True,
        },
        {
            "card_type": "ticker_signal",
            "template": "ticker_signal",
            "ticker": "SPY",
            "tickers": ["SPY"],
            "sentiment": "bullish",
            "headline": "SPY breadth needs confirmation, not vibes",
            "subheadline": "A bullish read gets stronger when macro, price, and underlying data move together.",
            "bullets": ["Confirmation score is the first read, not the final answer.", "Price and macro context should validate the move.", "Watch whether the signal survives the next risk window."],
            "key_stats": [{"label": "Confirm", "value": "76/100"}, {"label": "Mode", "value": "Bullish"}, {"label": "Stack", "value": "3 layers"}],
            "chips": ["Macro", "Signals", "Confirmation"],
            "cta": "View the data on Walnut",
            "url": "https://walnutmarkets.com/ticker/SPY",
            "visual_emphasis": "bullish confirmation",
            "source_label": "Demo confirmation",
            "tone": "market-native",
            "include_chart": True,
            "include_cta": True,
            "include_source_tag": True,
            "include_walnut_url": True,
        },
        {
            "card_type": "article_reactive",
            "template": "article_reactive",
            "ticker": "IBM",
            "tickers": ["IBM"],
            "sentiment": "bearish",
            "headline": "IBM's AI story just got a harder read",
            "subheadline": "The market is repricing where AI spend concentrates and which data points can defend demand.",
            "bullets": ["Software and infrastructure durability need validation.", "AI capex can grow while spend concentrates elsewhere.", "Watch institutional and confirmation data for follow-through."],
            "key_stats": [{"label": "Move", "value": "Down"}, {"label": "Signal", "value": "Watch"}],
            "chips": ["Article", "Financials", "Institutional"],
            "cta": "View the data on Walnut",
            "url": "https://walnutmarkets.com/ticker/IBM",
            "visual_emphasis": "reaction data",
            "source_label": "Demo article",
            "tone": "sharp",
            "include_chart": True,
            "include_cta": True,
            "include_source_tag": True,
            "include_walnut_url": True,
        },
        {
            "card_type": "article_reactive",
            "template": "fast_reaction",
            "ticker": "NBIS",
            "tickers": ["NBIS"],
            "sentiment": "active",
            "headline": "AI infrastructure is turning into a capacity trade",
            "subheadline": "Nebius-style compute headlines need demand, margin, and funding data before the story is clean.",
            "bullets": ["Compute deals can be catalysts and capital-intensity warnings.", "Contract quality matters more than headline size.", "Price/volume tells whether the market believes the ramp."],
            "key_stats": [{"label": "Theme", "value": "AI infra"}, {"label": "Mode", "value": "Active"}],
            "chips": ["AI Infra", "Contracts", "Why now"],
            "cta": "View the data on Walnut",
            "url": "https://walnutmarkets.com/ticker/NBIS",
            "visual_emphasis": "capacity data",
            "source_label": "Demo article",
            "tone": "market-native",
            "include_chart": True,
            "include_cta": True,
            "include_source_tag": True,
            "include_walnut_url": True,
        },
        {
            "card_type": "congress_insider_activity",
            "template": "activity_card",
            "ticker": "MSFT",
            "tickers": ["MSFT"],
            "sentiment": "notable",
            "headline": "A Congress disclosure is only the first tell",
            "subheadline": "The useful read comes from trade timing, filing lag, ticker context, and confirmation after disclosure.",
            "bullets": ["Reported activity needs trade date and disclosure date together.", "Filing lag changes how fresh the signal really is.", "Ticker confirmation decides whether it matters now."],
            "key_stats": [{"label": "Type", "value": "Buy"}, {"label": "Lag", "value": "Review"}],
            "chips": ["Congress", "Disclosure", "Why it matters"],
            "cta": "View the data on Walnut",
            "url": "https://walnutmarkets.com/feed?event_type=congress_trade",
            "visual_emphasis": "disclosure timeline",
            "source_label": "Demo disclosure",
            "tone": "neutral",
            "include_chart": True,
            "include_cta": True,
            "include_source_tag": True,
            "include_walnut_url": True,
        },
        {
            "card_type": "congress_insider_activity",
            "template": "activity_card",
            "ticker": "PLTR",
            "tickers": ["PLTR"],
            "sentiment": "active",
            "headline": "Insider activity needs context before it becomes signal",
            "subheadline": "Form 4 activity is strongest when role, size, timing, and price behavior line up.",
            "bullets": ["Role and transaction type change the interpretation.", "Reported date is not the same thing as market confirmation.", "Watch whether price/volume validates the disclosure."],
            "key_stats": [{"label": "Type", "value": "Sell"}, {"label": "Signal", "value": "Context"}],
            "chips": ["Insider", "Form 4", "Signals"],
            "cta": "View the data on Walnut",
            "url": "https://walnutmarkets.com/feed?event_type=insider_trade",
            "visual_emphasis": "activity data",
            "source_label": "Demo insider",
            "tone": "educational",
            "include_chart": True,
            "include_cta": True,
            "include_source_tag": True,
            "include_walnut_url": True,
        },
    ]
    return [_social_card_asset(_normalize_social_card_spec(item, fallback_card_type=str(item["card_type"]), fallback_tickers=item.get("tickers", []), fallback_url=str(item.get("url") or DEFAULT_DESTINATION_URL))) for item in demos]


def _social_card_data_uri(spec: dict[str, Any]) -> str:
    accent = _social_card_accent(str(spec.get("sentiment") or "notable"))
    ticker = str(spec.get("ticker") or "").upper()
    card_type = str(spec.get("card_type") or "ticker_signal")
    chip_text = [str(item) for item in spec.get("chips", []) if str(item).strip()][:3]
    stats = [item for item in spec.get("key_stats", []) if isinstance(item, dict)][:3]
    bullets = [str(item) for item in spec.get("bullets", []) if str(item).strip()][:3]
    source_label = str(spec.get("source_label") or "Walnut intelligence")
    cta = str(spec.get("cta") or "View the data on Walnut")
    url = str(spec.get("url") or DEFAULT_DESTINATION_URL)
    include_chart = bool(spec.get("include_chart", True))
    include_cta = bool(spec.get("include_cta", True))
    include_source_tag = False
    include_url = bool(spec.get("include_walnut_url", True))
    visual_label = str(spec.get("visual_emphasis") or "underlying data")
    headline_lines = _svg_line_tspans(spec.get("headline"), max_chars=23 if card_type == "research_cover" else 24, max_lines=3, x=92, y=274, font_size=52, line_height=59)
    subheadline_lines = _svg_line_tspans(spec.get("subheadline"), max_chars=47, max_lines=2, x=94, y=485, font_size=25, line_height=34, fill="#b8cbd0", weight="600")
    bullet_markup = _social_card_bullet_markup(bullets, x=102, y=600, max_chars=43)
    chip_markup = _social_card_chip_markup(chip_text, x=92, y=164, accent=accent, max_total_width=820)
    stat_markup = _social_card_stat_markup(stats, x=1086, y=294, accent=accent)
    chart_markup = _social_card_chart_markup(stats, x=1086, y=548, accent=accent, label=visual_label) if include_chart else _social_card_signal_panel(x=1086, y=548, accent=accent, label=visual_label)
    ticker_markup = (
        f"<rect x=\"1242\" y=\"74\" width=\"236\" height=\"58\" rx=\"14\" fill=\"{accent}\" opacity=\"0.20\" stroke=\"{accent}\" stroke-width=\"2\"/>"
        f"<text x=\"1266\" y=\"113\" fill=\"#f8fafc\" font-size=\"32\" font-family=\"Arial\" font-weight=\"700\">${html.escape(_truncate(ticker, 8) or 'WMT')}</text>"
        if ticker
        else ""
    )
    source_markup = ""
    cta_markup = ""
    if include_cta:
        cta_markup = (
            f"<rect x=\"72\" y=\"770\" width=\"1456\" height=\"74\" rx=\"18\" fill=\"#071e21\" stroke=\"{accent}\" stroke-width=\"2\" opacity=\"0.96\"/>"
            f"<text x=\"104\" y=\"817\" fill=\"#f8fafc\" font-size=\"29\" font-family=\"Arial\" font-weight=\"700\">{html.escape(_truncate(cta, 40) or 'View the data on Walnut')}</text>"
            f"<text x=\"1102\" y=\"817\" fill=\"#8ff5c6\" font-size=\"23\" font-family=\"Arial\" font-weight=\"600\">{html.escape(_short_card_url(url) if include_url else 'walnutmarkets.com')}</text>"
        )
    footer = f"{card_type.replace('_', ' ').title()} / Walnut Markets"
    svg = (
        f"<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"{SOCIAL_CARD_WIDTH}\" height=\"{SOCIAL_CARD_HEIGHT}\" viewBox=\"0 0 {SOCIAL_CARD_WIDTH} {SOCIAL_CARD_HEIGHT}\">"
        "<rect width=\"1600\" height=\"900\" fill=\"#061114\"/>"
        "<rect x=\"0\" y=\"0\" width=\"1600\" height=\"900\" fill=\"#071a1f\" opacity=\"0.88\"/>"
        f"<rect x=\"48\" y=\"48\" width=\"1504\" height=\"804\" rx=\"24\" fill=\"#0a1d22\" stroke=\"{accent}\" stroke-width=\"2\" opacity=\"0.98\"/>"
        f"<rect x=\"72\" y=\"72\" width=\"1456\" height=\"92\" rx=\"18\" fill=\"#102b30\" opacity=\"0.92\"/>"
        f"<rect x=\"84\" y=\"92\" width=\"44\" height=\"44\" rx=\"11\" fill=\"{accent}\" opacity=\"0.22\" stroke=\"{accent}\" stroke-width=\"2\"/>"
        f"<text x=\"97\" y=\"124\" fill=\"#f8fafc\" font-size=\"25\" font-family=\"Arial\" font-weight=\"700\">W</text>"
        f"<text x=\"144\" y=\"124\" fill=\"#f8fafc\" font-size=\"30\" font-family=\"Arial\" font-weight=\"700\">Walnut Markets</text>"
        f"<rect x=\"1036\" y=\"204\" width=\"454\" height=\"516\" rx=\"22\" fill=\"#07181c\" stroke=\"#1b4148\" stroke-width=\"1\" opacity=\"0.96\"/>"
        f"<text x=\"1086\" y=\"250\" fill=\"#f8fafc\" font-size=\"25\" font-family=\"Arial\" font-weight=\"700\">Evidence panel</text>"
        f"<rect x=\"92\" y=\"724\" width=\"820\" height=\"2\" fill=\"{accent}\" opacity=\"0.72\"/>"
        f"<text x=\"92\" y=\"744\" fill=\"#6e858a\" font-size=\"18\" font-family=\"Arial\">{html.escape(footer)}</text>"
        f"{ticker_markup}{source_markup}{chip_markup}{headline_lines}{subheadline_lines}{bullet_markup}{stat_markup}{chart_markup}{cta_markup}"
        "</svg>"
    )
    return "data:image/svg+xml;charset=utf-8," + quote(svg, safe=":/,;=+-_.'()#")


def _social_card_accent(sentiment: str) -> str:
    return {
        "bullish": "#25d889",
        "bearish": "#f87171",
        "active": "#38bdf8",
        "neutral": "#8ff5c6",
        "notable": "#2dd4bf",
    }.get(sentiment, "#2dd4bf")


def _svg_line_tspans(
    value: Any,
    *,
    max_chars: int,
    max_lines: int,
    x: int,
    y: int,
    font_size: int,
    line_height: int,
    fill: str = "#f8fafc",
    weight: str = "700",
) -> str:
    lines = _wrap_card_text(str(value or ""), max_chars=max_chars, max_lines=max_lines)
    return "".join(
        f"<text x=\"{x}\" y=\"{y + index * line_height}\" fill=\"{fill}\" font-size=\"{font_size}\" font-family=\"Arial\" font-weight=\"{weight}\">{html.escape(line)}</text>"
        for index, line in enumerate(lines)
    )


def _wrap_card_text(value: str, *, max_chars: int, max_lines: int) -> list[str]:
    cleaned = re.sub(r"\s+", " ", value).strip()
    if not cleaned:
        return []
    words = cleaned.split(" ")
    lines: list[str] = []
    current = ""
    for word in words:
        if len(word) > max_chars:
            word = f"{word[: max(1, max_chars - 1)]}..."
        candidate = f"{current} {word}".strip()
        if len(candidate) <= max_chars:
            current = candidate
            continue
        if current:
            lines.append(current)
        current = word
        if len(lines) >= max_lines:
            break
    if current and len(lines) < max_lines:
        lines.append(current)
    if len(lines) > max_lines:
        lines = lines[:max_lines]
    consumed = " ".join(lines).replace("...", "")
    if len(lines) == max_lines and len(consumed) < len(cleaned):
        lines[-1] = lines[-1][: max(4, max_chars - 3)].rstrip(". ") + "..."
    return lines


def _social_card_chip_markup(chips: list[str], *, x: int, y: int, accent: str, max_total_width: int = 820) -> str:
    markup: list[str] = []
    cursor = x
    for chip in chips[:3]:
        label = _truncate(chip, 18) or ""
        width = max(112, min(238, 36 + len(label) * 12))
        if cursor + width > x + max_total_width:
            break
        markup.append(
            f"<rect x=\"{cursor}\" y=\"{y}\" width=\"{width}\" height=\"38\" rx=\"10\" fill=\"{accent}\" opacity=\"0.16\" stroke=\"{accent}\" stroke-width=\"1\"/>"
            f"<text x=\"{cursor + 16}\" y=\"{y + 25}\" fill=\"#d8fff0\" font-size=\"18\" font-family=\"Arial\" font-weight=\"700\">{html.escape(label)}</text>"
        )
        cursor += width + 12
    return "".join(markup)


def _social_card_bullet_markup(bullets: list[str], *, x: int, y: int, max_chars: int) -> str:
    markup: list[str] = []
    cursor_y = y
    for bullet in bullets[:3]:
        lines = _wrap_card_text(bullet, max_chars=max_chars, max_lines=2)
        if not lines:
            continue
        markup.append(f"<rect x=\"{x}\" y=\"{cursor_y - 24}\" width=\"10\" height=\"10\" rx=\"3\" fill=\"#2dd4bf\"/>")
        for index, line in enumerate(lines):
            markup.append(
                f"<text x=\"{x + 28}\" y=\"{cursor_y + index * 29}\" fill=\"#d7e5e8\" font-size=\"25\" font-family=\"Arial\">{html.escape(line)}</text>"
            )
        cursor_y += 48 + (len(lines) - 1) * 29
    return "".join(markup)


def _social_card_stat_markup(stats: list[dict[str, Any]], *, x: int, y: int, accent: str) -> str:
    markup: list[str] = []
    for index, stat in enumerate(stats[:3]):
        stat_x = x
        stat_y = y + index * 78
        label = html.escape(_truncate(str(stat.get("label") or "Signal"), 24) or "Signal")
        value = html.escape(_truncate(str(stat.get("value") or "Review"), 22) or "Review")
        markup.append(
            f"<rect x=\"{stat_x}\" y=\"{stat_y}\" width=\"354\" height=\"66\" rx=\"14\" fill=\"#10292e\" stroke=\"#1e444b\" stroke-width=\"1\"/>"
            f"<text x=\"{stat_x + 20}\" y=\"{stat_y + 26}\" fill=\"#8ea5aa\" font-size=\"17\" font-family=\"Arial\" font-weight=\"700\">{label}</text>"
            f"<text x=\"{stat_x + 20}\" y=\"{stat_y + 53}\" fill=\"{accent}\" font-size=\"24\" font-family=\"Arial\" font-weight=\"700\">{value}</text>"
        )
    return "".join(markup)


def _social_card_chart_markup(stats: list[dict[str, Any]], *, x: int, y: int, accent: str, label: str) -> str:
    markup = [
        f"<rect x=\"{x}\" y=\"{y}\" width=\"354\" height=\"132\" rx=\"18\" fill=\"#08181c\" stroke=\"#1d3d43\" stroke-width=\"1\"/>",
        f"<text x=\"{x + 22}\" y=\"{y + 34}\" fill=\"#f8fafc\" font-size=\"20\" font-family=\"Arial\" font-weight=\"700\">{html.escape(_truncate(label, 25) or 'Data sources')}</text>",
    ]
    values = [_numeric_value_from_label(str(stat.get("value") or "")) for stat in stats[:4]]
    max_value = max([value for value in values if value is not None] or [100])
    for index in range(6):
        height = 26 + ((index * 29 + len(stats) * 11) % 62)
        if index < len(values) and values[index] is not None and max_value:
            height = max(24, min(76, int(22 + (values[index] or 0) / max_value * 58)))
        bar_x = x + 30 + index * 50
        bar_y = y + 104 - height
        opacity = "0.95" if index < len(stats) else "0.28"
        markup.append(f"<rect x=\"{bar_x}\" y=\"{bar_y}\" width=\"32\" height=\"{height}\" rx=\"8\" fill=\"{accent}\" opacity=\"{opacity}\"/>")
    markup.append(f"<text x=\"{x + 22}\" y=\"{y + 120}\" fill=\"#6e858a\" font-size=\"15\" font-family=\"Arial\">Data sources</text>")
    return "".join(markup)


def _social_card_signal_panel(*, x: int, y: int, accent: str, label: str) -> str:
    safe_label = html.escape(_truncate(label, 36) or "Evidence trail")
    return (
        f"<rect x=\"{x}\" y=\"{y}\" width=\"554\" height=\"246\" rx=\"22\" fill=\"#08181c\" stroke=\"#1d3d43\" stroke-width=\"1\"/>"
        f"<text x=\"{x + 30}\" y=\"{y + 52}\" fill=\"#f8fafc\" font-size=\"26\" font-family=\"Arial\" font-weight=\"700\">{safe_label}</text>"
        f"<rect x=\"{x + 34}\" y=\"{y + 92}\" width=\"486\" height=\"28\" rx=\"9\" fill=\"{accent}\" opacity=\"0.72\"/>"
        f"<rect x=\"{x + 34}\" y=\"{y + 142}\" width=\"386\" height=\"28\" rx=\"9\" fill=\"#8ff5c6\" opacity=\"0.42\"/>"
        f"<rect x=\"{x + 34}\" y=\"{y + 192}\" width=\"446\" height=\"28\" rx=\"9\" fill=\"#38bdf8\" opacity=\"0.34\"/>"
    )


def _short_card_url(value: str) -> str:
    parsed = urlparse(value)
    if parsed.netloc:
        path = parsed.path.rstrip("/")
        if path:
            return f"{parsed.netloc}{_truncate(path, 28) or ''}"
        return parsed.netloc
    return _truncate(value, 42) or "walnutmarkets.com"


def _x_visual_brief_asset(value: Any, *, detected_tickers: list[str]) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    rows: list[dict[str, str]] = []
    for row in _coerce_json_list(value.get("rows")):
        if not isinstance(row, dict):
            continue
        label = _truncate(str(row.get("label") or "").strip(), 28) or ""
        row_value = _truncate(str(row.get("value") or "").strip(), 18) or ""
        note = _truncate(str(row.get("note") or "").strip(), 60) or ""
        if label:
            rows.append({"label": label, "value": row_value, "note": note})
    if not rows:
        return None
    title = _truncate(str(value.get("title") or "").strip(), 72) or "Walnut market breakdown"
    metric_label = _truncate(str(value.get("metric_label") or "").strip(), 56) or "Metric"
    chart_type = str(value.get("chart_type") or "ranked_bars").strip().lower()
    source_note = _truncate(str(value.get("source_note") or "").strip(), 110) or "Source: Walnut context and linked source; human reviewed."
    missing_note = _truncate(str(value.get("missing_data_note") or "").strip(), 90) or ""
    subtitle = ", ".join(f"${ticker}" for ticker in detected_tickers[:5]) or metric_label
    card_url = _x_visual_brief_data_uri(
        title=title,
        subtitle=subtitle,
        metric_label=metric_label,
        chart_type=chart_type,
        rows=rows[:8],
        source_note=source_note,
        missing_note=missing_note,
    )
    return {
        "title": f"Walnut visual: {title}",
        "asset_type": "chart",
        "url": card_url,
        "thumbnail_url": card_url,
        "suggested_caption": f"{title} - {metric_label}",
        "source_data_notes": f"{source_note}" + (f" Missing data: {missing_note}" if missing_note else ""),
    }


def _x_visual_brief_data_uri(
    *,
    title: str,
    subtitle: str,
    metric_label: str,
    chart_type: str,
    rows: list[dict[str, str]],
    source_note: str,
    missing_note: str,
) -> str:
    safe_title = html.escape(_truncate(title, 72) or "Walnut market breakdown")
    safe_subtitle = html.escape(_truncate(subtitle, 90) or "")
    safe_metric = html.escape(_truncate(metric_label, 56) or "Metric")
    safe_source = html.escape(_truncate(source_note, 110) or "")
    safe_missing = html.escape(_truncate(missing_note, 90) or "")
    numeric_values = [_numeric_value_from_label(row.get("value")) for row in rows]
    max_value = max([value for value in numeric_values if value is not None] or [0])
    row_markup: list[str] = []
    start_y = 282
    row_gap = 68 if len(rows) <= 6 else 58
    for index, row in enumerate(rows):
        y = start_y + index * row_gap
        label = html.escape(_truncate(row.get("label"), 28) or "")
        value_text = html.escape(_truncate(row.get("value"), 18) or "")
        note = html.escape(_truncate(row.get("note"), 60) or "")
        numeric_value = numeric_values[index] if index < len(numeric_values) else None
        if numeric_value is not None and max_value > 0:
            bar_width = int(220 + (numeric_value / max_value) * 520)
            bar = (
                f"<rect x=\"430\" y=\"{y - 28}\" width=\"{bar_width}\" height=\"34\" rx=\"8\" fill=\"#22c55e\" opacity=\"0.82\"/>"
                f"<text x=\"{min(1380, 450 + bar_width)}\" y=\"{y - 5}\" fill=\"#d1fae5\" font-size=\"28\" font-family=\"Arial\" font-weight=\"700\">{value_text}</text>"
            )
        else:
            bucket_width = 650 if chart_type in {"bucket_breakdown", "data_sources", "comparison_card"} else 520
            bar = (
                f"<rect x=\"430\" y=\"{y - 28}\" width=\"{bucket_width}\" height=\"34\" rx=\"8\" fill=\"#1f6f55\" opacity=\"0.72\"/>"
                f"<text x=\"452\" y=\"{y - 5}\" fill=\"#d1fae5\" font-size=\"28\" font-family=\"Arial\" font-weight=\"700\">{value_text or 'Review'}</text>"
            )
        row_markup.append(
            f"<text x=\"86\" y=\"{y}\" fill=\"#f8fafc\" font-size=\"31\" font-family=\"Arial\" font-weight=\"700\">{label}</text>"
            f"{bar}"
            f"<text x=\"430\" y=\"{y + 32}\" fill=\"#94a3b8\" font-size=\"22\" font-family=\"Arial\">{note}</text>"
        )
    missing_markup = (
        f"<text x=\"86\" y=\"750\" fill=\"#fbbf24\" font-size=\"22\" font-family=\"Arial\">Missing: {safe_missing}</text>"
        if safe_missing
        else ""
    )
    svg = (
        "<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"1600\" height=\"900\" viewBox=\"0 0 1600 900\">"
        "<rect width=\"1600\" height=\"900\" fill=\"#071315\"/>"
        "<rect x=\"48\" y=\"48\" width=\"1504\" height=\"804\" rx=\"20\" fill=\"#0d1f22\" stroke=\"#1f6f55\" stroke-width=\"3\"/>"
        "<text x=\"84\" y=\"118\" fill=\"#22c55e\" font-size=\"30\" font-family=\"Arial\" font-weight=\"700\">Walnut Markets</text>"
        f"<text x=\"84\" y=\"190\" fill=\"#f8fafc\" font-size=\"54\" font-family=\"Arial\" font-weight=\"700\">{safe_title}</text>"
        f"<text x=\"86\" y=\"232\" fill=\"#94a3b8\" font-size=\"25\" font-family=\"Arial\">{safe_subtitle} | {safe_metric}</text>"
        f"{''.join(row_markup)}"
        f"{missing_markup}"
        f"<text x=\"86\" y=\"805\" fill=\"#64748b\" font-size=\"22\" font-family=\"Arial\">{safe_source}</text>"
        "</svg>"
    )
    return "data:image/svg+xml;charset=utf-8," + quote(svg, safe=":/,;=+-_.'()#")


def _numeric_value_from_label(value: str | None) -> float | None:
    match = re.search(r"[-+]?\d+(?:\.\d+)?", str(value or "").replace(",", ""))
    if not match:
        return None
    try:
        return float(match.group(0))
    except ValueError:
        return None


def _article_candidate_to_source_item(
    candidate: AiMarketingArticleCandidate,
    scoring: dict[str, Any],
    campaign: AiMarketingCampaign,
) -> SourceItem:
    tickers = scoring.get("tickers") or []
    themes = scoring.get("themes") or []
    destination = recommended_destination_url(
        mode=ARTICLE_REACTIVE_CAMPAIGN_TYPE,
        platform="x",
        campaign_id=campaign.id,
        tickers=tickers,
        fallback=campaign.default_destination_page or DEFAULT_DESTINATION_URL,
    )
    ticker_theme = ", ".join([*(f"${ticker}" for ticker in tickers[:4]), *themes[:3]]) or "Market article"
    compliance = "Human review required. No investment advice, no buy/sell recommendations, no article thumbnail reuse."
    preferences = _load_object(campaign.output_preferences_json)
    metadata = {
        "article_reactive": True,
        "article_candidate_id": candidate.id,
        "provider": "fmp",
        "article_title": candidate.title,
        "article_source": candidate.site,
        "article_url": candidate.url,
        "article_published_at": _iso(candidate.published_at),
        "article_tickers": tickers,
        "themes": themes,
        "walnut_context": scoring.get("walnut_context") or {},
        "scoring": {key: value for key, value in scoring.items() if key.endswith("_score") or key in {"clear_walnut_angle", "rejected_reasons"}},
        "suggested_destination_url": destination,
        "email_recipient": campaign.recipient_email or ai_growth_recipient(),
        "inputs": {
            "social_card": {
                "template": preferences.get("card_template") or preferences.get("template") or "article_reactive",
                "tone": preferences.get("card_tone") or preferences.get("tone") or "market-native",
                "include_chart": bool(preferences.get("include_chart", True)),
                "include_cta": bool(preferences.get("include_cta", True)),
                "include_source_tag": bool(preferences.get("include_source_tag", True)),
                "include_walnut_url": bool(preferences.get("include_walnut_url", True)),
                "include_article_thumbnail": bool(preferences.get("include_article_thumbnail", False)),
            }
        },
    }
    should_seed_legacy_card = (
        _article_campaign_pref(campaign, "include_image_card", True)
        and not marketing_image_generation_enabled()
    )
    return SourceItem(
        platform="x",
        source_id=f"fmp:{candidate.dedupe_hash}",
        source_url=candidate.url,
        title=f"Article-Reactive X: {candidate.title}",
        source_provider=ARTICLE_REACTIVE_PROVIDER,
        campaign_type=ARTICLE_REACTIVE_CAMPAIGN_TYPE,
        content_type="x_post",
        source_platform="x",
        ticker_theme=ticker_theme,
        recommended_action="draft_post",
        fit_score=int(scoring["final_score"]),
        assets=[_article_card_asset(candidate, scoring)] if should_seed_legacy_card else [],
        excerpt=candidate.summary,
        source_created_at=candidate.published_at,
        metadata=metadata,
        generated_content=None,
        alternate_versions={
            "short_version": "",
            "more_direct_version": "",
            "copy_hashtags_cashtags": " ".join([*(f"${ticker}" for ticker in tickers[:4]), *(f"#{theme.replace(' ', '')}" for theme in themes[:2])]),
        },
    )


def _article_campaign_pref(campaign: AiMarketingCampaign, key: str, default: Any) -> Any:
    prefs = _load_object(campaign.output_preferences_json)
    if key in prefs:
        return prefs[key]
    return default


def _article_daily_count(db: Session, campaign: AiMarketingCampaign) -> int:
    start = _campaign_today_start_utc(campaign)
    return int(
        db.execute(
            select(func.count())
            .select_from(AiMarketingOpportunity)
            .where(
                AiMarketingOpportunity.campaign_id == campaign.id,
                AiMarketingOpportunity.campaign_type == ARTICLE_REACTIVE_CAMPAIGN_TYPE,
                AiMarketingOpportunity.created_at >= start,
            )
        ).scalar()
        or 0
    )


def _article_env_daily_cap() -> int:
    raw = os.getenv(AI_GROWTH_ARTICLE_MAX_DAILY_DRAFTS, "").strip()
    if not raw:
        return 2
    return _clamp_int(raw, 1, 2)


def _campaign_today_start_utc(campaign: AiMarketingCampaign) -> datetime:
    tz_name = campaign.timezone or "America/Los_Angeles"
    try:
        tz = ZoneInfo(tz_name)
    except ZoneInfoNotFoundError:
        tz = ZoneInfo("America/Los_Angeles")
    local_now = datetime.now(tz)
    local_start = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
    return local_start.astimezone(timezone.utc)


def run_article_reactive_campaign(db: Session, campaign: AiMarketingCampaign, *, send_email_on_create: bool = True) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "status": "ok",
        "articles_fetched": 0,
        "articles_considered": 0,
        "articles_rejected": 0,
        "drafts_generated": 0,
        "emails_sent": 0,
        "errors": [],
        "warnings": [],
        "created": 0,
        "deduped": 0,
        "suggested": 0,
        "opportunities": [],
    }
    status = str(campaign.status or ("active" if campaign.enabled else "paused")).lower()
    if not campaign.enabled or status != "active":
        summary["status"] = status if status in {"paused", "stopped"} else "paused"
        summary["warnings"].append("Campaign is disabled; no article run was performed.")
        record_campaign_run(db, campaign, summary)
        return summary
    if not resolved_setting_value(db, FMP_API_KEY):
        summary["status"] = "configuration_failed"
        summary["errors"].append("FMP Articles API key missing. Configure FMP_API_KEY on the server.")
        campaign.last_run_at = datetime.now(timezone.utc)
        db.commit()
        record_campaign_run(db, campaign, summary)
        return summary
    try:
        articles = fetch_fmp_articles(db, page=0, limit=min(max(int(campaign.max_items_per_run or 20), 1), 50))
    except MissingMarketingCredential as exc:
        summary["status"] = "configuration_failed"
        summary["errors"].append(str(exc))
        campaign.last_run_at = datetime.now(timezone.utc)
        db.commit()
        record_campaign_run(db, campaign, summary)
        return summary
    except Exception:
        logger.exception("ai_growth_article_fmp_fetch_failed campaign_id=%s", campaign.id)
        summary["status"] = "provider_failed"
        summary["errors"].append("FMP Articles API request failed.")
        campaign.last_run_at = datetime.now(timezone.utc)
        db.commit()
        record_campaign_run(db, campaign, summary)
        return summary

    summary["articles_fetched"] = len(articles)
    candidates: list[tuple[AiMarketingArticleCandidate, bool, dict[str, Any]]] = []
    for raw_article in articles:
        normalized = _normalize_fmp_article(raw_article)
        if not normalized:
            summary["articles_rejected"] += 1
            continue
        candidate, was_created = upsert_article_candidate(db, normalized)
        scoring = score_article_candidate(db, candidate, campaign=campaign)
        candidates.append((candidate, was_created, scoring))
    summary["articles_considered"] = len(candidates)
    ranked = sorted(candidates, key=lambda item: item[2]["final_score"], reverse=True)
    max_daily = min(_article_env_daily_cap(), max(1, int(getattr(campaign, "max_drafts_per_day", 1) or 1)))
    remaining = max(0, max_daily - _article_daily_count(db, campaign))
    selected: list[AiMarketingOpportunity] = []
    for candidate, was_created, scoring in ranked:
        if remaining <= 0:
            break
        if scoring["rejected"]:
            summary["articles_rejected"] += 1
            continue
        source_item = _article_candidate_to_source_item(candidate, scoring, campaign)
        opportunity, opportunity_created = upsert_source_item(db, campaign, source_item)
        opportunity.relevance_score = int(scoring["final_score"])
        opportunity.fit_score = int(scoring["final_score"])
        opportunity.spam_risk_score = int(scoring["promotional_risk_score"])
        opportunity.quality_scores_json = _dump_object({key: value for key, value in scoring.items() if key.endswith("_score")})
        opportunity.source_notes_json = _dump_json_list(_article_context_bullets(scoring))
        opportunity.compliance_notes = "No posting before approval. Review for investment advice, unsupported claims, and article-image licensing."
        db.commit()
        db.refresh(opportunity)
        if opportunity_created:
            summary["created"] += 1
        else:
            summary["deduped"] += 1
        if resolved_setting_value(db, OPENAI_API_KEY):
            try:
                generate_suggestion(db, opportunity, campaign=campaign)
                summary["suggested"] += 1
            except OpenAISuggestionError as exc:
                summary["warnings"].append(f"Suggestion generation failed for draft {opportunity.id}: {exc.admin_message}")
            except Exception:
                logger.exception("ai_growth_article_suggestion_failed opportunity_id=%s", opportunity.id)
                summary["warnings"].append(f"Suggestion generation failed for draft {opportunity.id}.")
        else:
            summary["warnings"].append("OpenAI API key missing; article draft saved without generated copy.")
            _record_suggestion_failure(db, opportunity, OPENAI_MISSING_KEY_MESSAGE, code="missing_key")
        latest = latest_suggestions_by_opportunity(db, [opportunity.id]).get(opportunity.id)
        if send_email_on_create and opportunity_created:
            try:
                send_draft_email(db, opportunity)
                summary["emails_sent"] += 1
                latest = latest_suggestions_by_opportunity(db, [opportunity.id]).get(opportunity.id)
            except Exception:
                logger.exception("ai_growth_article_email_failed opportunity_id=%s", opportunity.id)
                summary["warnings"].append(f"Email failed for draft {opportunity.id}.")
        selected.append(opportunity)
        remaining -= 1

    summary["drafts_generated"] = len(selected)
    latest = latest_suggestions_by_opportunity(db, [row.id for row in selected])
    summary["opportunities"] = [opportunity_to_dict(row, suggestion=latest.get(row.id)) for row in selected]
    if not selected and not summary["errors"]:
        summary["status"] = "no_clear_angle"
        summary["warnings"].append("No article had a clear Walnut angle after scoring.")
    campaign.last_run_at = datetime.now(timezone.utc)
    db.commit()
    record_campaign_run(db, campaign, summary)
    return summary


def article_campaign_due(campaign: AiMarketingCampaign, *, now: datetime | None = None) -> bool:
    if not campaign.enabled or str(campaign.status or "active").lower() != "active":
        return False
    if _normalize_campaign_type(campaign.campaign_type, fallback_mode=campaign.mode) != ARTICLE_REACTIVE_CAMPAIGN_TYPE:
        return False
    tz_name = campaign.timezone or "America/Los_Angeles"
    try:
        tz = ZoneInfo(tz_name)
    except ZoneInfoNotFoundError:
        tz = ZoneInfo("America/Los_Angeles")
    local_now = (now or datetime.now(timezone.utc)).astimezone(tz)
    if campaign.weekdays_only and local_now.weekday() >= 5:
        return False
    if campaign.last_run_at:
        last_local = (campaign.last_run_at if campaign.last_run_at.tzinfo else campaign.last_run_at.replace(tzinfo=timezone.utc)).astimezone(tz)
        if last_local.date() == local_now.date():
            return False
    run_time = str(campaign.run_time or "07:30").strip()
    match = re.fullmatch(r"(\d{1,2}):(\d{2})", run_time)
    if match:
        hour = max(0, min(23, int(match.group(1))))
        minute = max(0, min(59, int(match.group(2))))
        if local_now.time() < datetime.combine(date.today(), datetime.min.time()).replace(hour=hour, minute=minute).time():
            return False
    return True


def _campaign_run_timezone(campaign: AiMarketingCampaign) -> ZoneInfo:
    try:
        return ZoneInfo(campaign.timezone or "America/Los_Angeles")
    except ZoneInfoNotFoundError:
        return ZoneInfo("America/Los_Angeles")


def _campaign_run_time(campaign: AiMarketingCampaign, *, default: str = "07:00") -> tuple[int, int]:
    run_time = str(campaign.run_time or default).strip()
    match = re.fullmatch(r"(\d{1,2}):(\d{2})", run_time)
    if not match:
        run_time = default
        match = re.fullmatch(r"(\d{1,2}):(\d{2})", run_time)
    if not match:
        return 7, 0
    return max(0, min(23, int(match.group(1)))), max(0, min(59, int(match.group(2))))


def _campaign_last_run_local(campaign: AiMarketingCampaign, tz: ZoneInfo) -> datetime | None:
    if not campaign.last_run_at:
        return None
    value = campaign.last_run_at if campaign.last_run_at.tzinfo else campaign.last_run_at.replace(tzinfo=timezone.utc)
    return value.astimezone(tz)


def _weekly_campaign_day(schedule_config: dict[str, Any]) -> int | None:
    raw = schedule_config.get("weekday") or schedule_config.get("day_of_week") or schedule_config.get("day")
    if raw in (None, ""):
        return None
    if isinstance(raw, int):
        return raw if 0 <= raw <= 6 else None
    text = str(raw).strip().lower()
    names = {
        "monday": 0,
        "mon": 0,
        "tuesday": 1,
        "tue": 1,
        "wednesday": 2,
        "wed": 2,
        "thursday": 3,
        "thu": 3,
        "friday": 4,
        "fri": 4,
        "saturday": 5,
        "sat": 5,
        "sunday": 6,
        "sun": 6,
    }
    if text.isdigit():
        value = int(text)
        return value if 0 <= value <= 6 else None
    return names.get(text)


def scheduled_x_campaign_due(campaign: AiMarketingCampaign, *, now: datetime | None = None) -> bool:
    if not campaign.enabled or str(campaign.status or "active").lower() != "active":
        return False
    if _normalize_campaign_type(campaign.campaign_type, fallback_mode=campaign.mode) != SCHEDULED_X_CAMPAIGN_TYPE:
        return False
    tz = _campaign_run_timezone(campaign)
    local_now = (now or datetime.now(timezone.utc)).astimezone(tz)
    schedule_config = _load_object(campaign.schedule_config_json)
    cadence = str(schedule_config.get("cadence") or ("weekdays" if campaign.weekdays_only else "daily")).strip().lower()
    if cadence not in {"daily", "weekdays", "weekly"}:
        cadence = "weekdays" if campaign.weekdays_only else "daily"
    if cadence == "weekdays" and local_now.weekday() >= 5:
        return False
    if cadence == "weekly":
        configured_day = _weekly_campaign_day(schedule_config)
        if configured_day is not None and local_now.weekday() != configured_day:
            return False
    hour, minute = _campaign_run_time(campaign, default="07:00")
    scheduled_local = local_now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if local_now < scheduled_local:
        return False
    last_local = _campaign_last_run_local(campaign, tz)
    if not last_local:
        return True
    if last_local.date() == local_now.date():
        return False
    if cadence == "weekly" and (local_now.date() - last_local.date()).days < 6:
        return False
    return True


def x_reply_campaign_due(campaign: AiMarketingCampaign, *, now: datetime | None = None) -> bool:
    if not campaign.enabled or str(campaign.status or "active").lower() != "active":
        return False
    if _normalize_campaign_type(campaign.campaign_type, fallback_mode=campaign.mode) != X_REPLY_CAMPAIGN_TYPE:
        return False
    tz = _campaign_run_timezone(campaign)
    local_now = (now or datetime.now(timezone.utc)).astimezone(tz)
    schedule_config = _load_object(campaign.schedule_config_json)
    start_date = str(schedule_config.get("start_date") or "").strip()
    if start_date:
        try:
            if local_now.date() < date.fromisoformat(start_date):
                return False
        except ValueError:
            pass
    cadence = str(schedule_config.get("cadence") or ("weekdays" if campaign.weekdays_only else "daily")).strip().lower()
    if cadence not in {"daily", "weekdays", "weekly"}:
        cadence = "weekdays" if campaign.weekdays_only else "daily"
    if cadence == "weekdays" and local_now.weekday() >= 5:
        return False
    if cadence == "weekly":
        configured_day = _weekly_campaign_day(schedule_config)
        if configured_day is not None and local_now.weekday() != configured_day:
            return False
    hour, minute = _campaign_run_time(campaign, default="07:15")
    scheduled_local = local_now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if local_now < scheduled_local:
        return False
    last_local = _campaign_last_run_local(campaign, tz)
    if not last_local:
        return True
    if last_local.date() == local_now.date():
        return False
    if cadence == "weekly" and (local_now.date() - last_local.date()).days < 6:
        return False
    return True


def run_due_article_reactive_campaigns(db: Session, *, force: bool = False, dry_run: bool = False) -> dict[str, Any]:
    campaigns = db.execute(
        select(AiMarketingCampaign).where(
            or_(AiMarketingCampaign.campaign_type == ARTICLE_REACTIVE_CAMPAIGN_TYPE, AiMarketingCampaign.mode == ARTICLE_REACTIVE_CAMPAIGN_TYPE),
            AiMarketingCampaign.enabled == True,  # noqa: E712
        )
    ).scalars().all()
    results: list[dict[str, Any]] = []
    for campaign in campaigns:
        due = force or article_campaign_due(campaign)
        if not due:
            results.append({"campaign_id": campaign.id, "campaign_name": campaign.name, "status": "not_due"})
            continue
        if dry_run:
            results.append({"campaign_id": campaign.id, "campaign_name": campaign.name, "status": "due_dry_run"})
            continue
        result = run_article_reactive_campaign(db, campaign)
        result["campaign_id"] = campaign.id
        result["campaign_name"] = campaign.name
        results.append(result)
    return {
        "campaigns_checked": len(campaigns),
        "campaigns_run": sum(1 for item in results if item.get("drafts_generated") is not None),
        "dry_run": dry_run,
        "items": results,
    }


def run_due_scheduled_x_campaigns(db: Session, *, force: bool = False, dry_run: bool = False) -> dict[str, Any]:
    campaigns = db.execute(
        select(AiMarketingCampaign).where(
            or_(AiMarketingCampaign.campaign_type == SCHEDULED_X_CAMPAIGN_TYPE, AiMarketingCampaign.mode == SCHEDULED_X_CAMPAIGN_TYPE),
            AiMarketingCampaign.enabled == True,  # noqa: E712
        )
    ).scalars().all()
    results: list[dict[str, Any]] = []
    for campaign in campaigns:
        due = force or scheduled_x_campaign_due(campaign)
        if not due:
            results.append({"campaign_id": campaign.id, "campaign_name": campaign.name, "status": "not_due"})
            continue
        if dry_run:
            results.append({"campaign_id": campaign.id, "campaign_name": campaign.name, "status": "due_dry_run"})
            continue
        result = run_scheduled_x_campaign(db, campaign)
        result["campaign_id"] = campaign.id
        result["campaign_name"] = campaign.name
        results.append(result)
    return {
        "campaigns_checked": len(campaigns),
        "campaigns_run": sum(1 for item in results if item.get("drafts_generated") is not None),
        "dry_run": dry_run,
        "items": results,
    }


def run_due_x_reply_campaigns(db: Session, *, force: bool = False, dry_run: bool = False) -> dict[str, Any]:
    campaigns = db.execute(
        select(AiMarketingCampaign).where(
            or_(AiMarketingCampaign.campaign_type == X_REPLY_CAMPAIGN_TYPE, AiMarketingCampaign.mode == X_REPLY_CAMPAIGN_TYPE),
            AiMarketingCampaign.enabled == True,  # noqa: E712
        )
    ).scalars().all()
    results: list[dict[str, Any]] = []
    for campaign in campaigns:
        due = force or x_reply_campaign_due(campaign)
        if not due:
            results.append({"campaign_id": campaign.id, "campaign_name": campaign.name, "status": "not_due"})
            continue
        if dry_run:
            results.append({"campaign_id": campaign.id, "campaign_name": campaign.name, "status": "due_dry_run"})
            continue
        result = run_x_reply_campaign(db, campaign)
        result["campaign_id"] = campaign.id
        result["campaign_name"] = campaign.name
        results.append(result)
    return {
        "campaigns_checked": len(campaigns),
        "campaigns_run": sum(1 for item in results if item.get("drafts_generated") is not None),
        "dry_run": dry_run,
        "items": results,
    }


def run_due_ai_growth_campaigns(db: Session, *, force: bool = False, dry_run: bool = False) -> dict[str, Any]:
    article = run_due_article_reactive_campaigns(db, force=force, dry_run=dry_run)
    scheduled_x = run_due_scheduled_x_campaigns(db, force=force, dry_run=dry_run)
    x_replies = run_due_x_reply_campaigns(db, force=force, dry_run=dry_run)
    return {
        "campaigns_checked": int(article.get("campaigns_checked") or 0) + int(scheduled_x.get("campaigns_checked") or 0) + int(x_replies.get("campaigns_checked") or 0),
        "campaigns_run": int(article.get("campaigns_run") or 0) + int(scheduled_x.get("campaigns_run") or 0) + int(x_replies.get("campaigns_run") or 0),
        "dry_run": dry_run,
        "article_reactive_x": article,
        "scheduled_x_campaigns": scheduled_x,
        "x_reply_campaigns": x_replies,
    }


def _scheduled_x_source_label(source_type: str | None, source_reference_id: str | None) -> str:
    source = str(source_type or "watchlist").strip().replace("_", " ")
    selector = str(source_reference_id or "").strip()
    return f"{source}: {selector}" if selector else source


def _compact_money(value: float | int | None) -> str:
    if value is None:
        return "unknown amount"
    try:
        amount = float(value)
    except (TypeError, ValueError):
        return "unknown amount"
    sign = "-" if amount < 0 else ""
    amount = abs(amount)
    if amount >= 1_000_000_000:
        return f"{sign}${amount / 1_000_000_000:.1f}B"
    if amount >= 1_000_000:
        return f"{sign}${amount / 1_000_000:.1f}M"
    if amount >= 1_000:
        return f"{sign}${amount / 1_000:.1f}K"
    return f"{sign}${amount:,.0f}"


def _ticker_link(symbol: str | None) -> str | None:
    ticker = str(symbol or "").strip().upper()
    if not ticker:
        return None
    return f"https://app.walnutmarkets.com/ticker/{quote(ticker)}"


def _trigger_payload(
    *,
    ticker: str | None,
    reason: str,
    source: str,
    actor: str | None = None,
    amount: str | None = None,
    date_value: date | datetime | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_ticker = str(ticker or "").strip().upper()
    payload: dict[str, Any] = {
        "ticker": normalized_ticker,
        "ticker_url": _ticker_link(normalized_ticker),
        "relevant_url": _ticker_link(normalized_ticker),
        "source": source,
        "reason": reason,
    }
    if actor:
        payload["actor"] = actor
    if amount:
        payload["amount"] = amount
    if date_value:
        payload["date"] = date_value.isoformat()
    if extra:
        payload.update({key: value for key, value in extra.items() if value not in (None, "", [], {})})
    return payload


def _trigger_line(trigger: dict[str, Any]) -> str:
    ticker = trigger.get("ticker") or "UNKNOWN"
    parts = [f"${ticker}: {trigger.get('reason') or 'Walnut trigger'}"]
    source_stack = trigger.get("source_stack")
    if isinstance(source_stack, list) and source_stack:
        stack_lines = []
        for item in source_stack[:6]:
            if not isinstance(item, dict):
                continue
            label = str(item.get("label") or item.get("key") or "").strip()
            detail = str(item.get("detail") or item.get("direction") or "").strip()
            if label and detail:
                stack_lines.append(f"{label}: {detail}")
            elif label:
                stack_lines.append(label)
        if stack_lines:
            parts.append(f"Underlying data: {'; '.join(stack_lines)}")
    if trigger.get("actor"):
        parts.append(f"Actor: {trigger['actor']}")
    if trigger.get("amount"):
        parts.append(f"Amount: {trigger['amount']}")
    if trigger.get("date"):
        parts.append(f"Date: {trigger['date']}")
    relevant_url = trigger.get("relevant_url") or trigger.get("ticker_url")
    if relevant_url:
        parts.append(f"Relevant Walnut page: {relevant_url}")
    return " | ".join(parts)


def _recent_since(days: int = 14) -> datetime:
    return datetime.now(timezone.utc) - timedelta(days=days)


CONFIRMATION_SOURCE_LABELS = {
    "price_volume": "Price / Volume",
    "institutional_activity": "Institutional Activity",
    "macro_positioning": "Macro Positioning",
    "fundamentals": "Fundamentals",
    "signals": "Signals",
    "congress": "Congress",
    "insiders": "Insiders",
    "government_contracts": "Government Contracts",
    "options_flow": "Options Flow",
}

CONFIRMATION_SOURCE_PRIORITY = (
    "price_volume",
    "institutional_activity",
    "macro_positioning",
    "fundamentals",
    "signals",
    "congress",
    "insiders",
    "government_contracts",
    "options_flow",
)


def _confirmation_source_stack(bundle: dict[str, Any] | None, source_states: dict[str, Any]) -> list[dict[str, Any]]:
    raw_sources = bundle.get("sources") if isinstance(bundle, dict) and isinstance(bundle.get("sources"), dict) else {}
    source_details = bundle.get("source_details") if isinstance(bundle, dict) and isinstance(bundle.get("source_details"), dict) else {}
    keys = list(CONFIRMATION_SOURCE_PRIORITY)
    for key in list(raw_sources.keys()) + list(source_states.keys()):
        if key not in keys:
            keys.append(key)

    stack: list[dict[str, Any]] = []
    for key in keys:
        source = raw_sources.get(key) if isinstance(raw_sources.get(key), dict) else {}
        state = source_states.get(key) if isinstance(source_states.get(key), dict) else {}
        source_present = source.get("present") is True
        state_present = state.get("present") is True
        present = source_present or state_present
        if not present:
            continue
        label = str(CONFIRMATION_SOURCE_LABELS.get(key) or source.get("label") or state.get("label") or key.replace("_", " ")).strip()
        direction = str(source.get("direction") or state.get("direction") or "").strip()
        if source_present:
            detail_value = source_details.get(key) or source.get("detail") or source.get("summary") or state.get("detail") or state.get("summary") or state.get("label")
        else:
            detail_value = state.get("detail") or state.get("summary") or state.get("label") or source_details.get(key) or source.get("detail") or source.get("summary")
        detail = str(detail_value or "").strip()
        if key == "institutional_activity" and not detail and direction == "bullish":
            detail = "net reported accumulation"
        if key == "institutional_activity" and direction == "bullish" and "accumulation" not in detail.lower():
            detail = f"{detail}; institutional accumulation".strip("; ")
        if key == "price_volume" and not detail and direction in {"bullish", "bearish"}:
            detail = f"{direction} tape confirmation"
        if key == "macro_positioning" and not detail:
            detail = "macro positioning support" if direction in {"bullish", "neutral"} else "macro positioning headwind"
        stack.append({"key": key, "label": label, "direction": direction, "detail": detail})
    return stack


def _top_confirmation_triggers(db: Session, *, direction: str | None = None, limit: int = 5) -> list[dict[str, Any]]:
    query = select(ConfirmationMonitoringSnapshot).where(ConfirmationMonitoringSnapshot.ticker.is_not(None))
    if direction:
        query = query.where(ConfirmationMonitoringSnapshot.direction == direction)
    rows = db.execute(
        query.order_by(desc(ConfirmationMonitoringSnapshot.score), desc(ConfirmationMonitoringSnapshot.observed_at)).limit(limit)
    ).scalars().all()
    symbols = [str(row.ticker or "").strip().upper() for row in rows if str(row.ticker or "").strip()]
    fresh_bundles: dict[str, dict[str, Any]] = {}
    if symbols:
        try:
            fresh_bundles = get_confirmation_score_bundles_for_tickers(db, symbols, lookback_days=30)
        except Exception:
            logger.exception("ai_growth_confirmation_bundle_refresh_failed")
            fresh_bundles = {}
    triggers: list[dict[str, Any]] = []
    for row in rows:
        source_states = _load_object(row.source_states_json)
        bundle = fresh_bundles.get(str(row.ticker or "").strip().upper()) or {}
        source_stack = _confirmation_source_stack(bundle, source_states)
        raw_bundle_sources = bundle.get("sources") if isinstance(bundle, dict) and isinstance(bundle.get("sources"), dict) else {}
        bundle_has_active_sources = any(isinstance(source, dict) and source.get("present") is True for source in raw_bundle_sources.values())
        bundle_score = int(bundle.get("score") or 0) if isinstance(bundle, dict) else 0
        bundle_is_authoritative = bundle_has_active_sources or bundle_score > 0
        score = bundle_score or int(row.score or 0)
        bundle_direction = bundle.get("direction") if isinstance(bundle, dict) else None
        resolved_direction = bundle_direction if bundle_is_authoritative and bundle_direction in {"bullish", "bearish", "mixed", "neutral"} else row.direction
        status = str((bundle.get("status") if bundle_is_authoritative else None) or row.status or row.band or "confirmation").strip() if isinstance(bundle, dict) else str(row.status or row.band or "confirmation").strip()
        source_labels = [str(item.get("label") or item.get("key") or "").strip() for item in source_stack if item.get("label") or item.get("key")]
        source_count = len(source_stack) or int(row.source_count or 0)
        if source_count >= 2 and resolved_direction in {"bullish", "bearish"}:
            status = f"{source_count}-source {resolved_direction} confirmation"
        reason = f"{status}; {score}/100 confirmation score"
        if source_labels:
            reason = f"{reason}; active sources: {', '.join(source_labels[:6])}"
        triggers.append(
            _trigger_payload(
                ticker=row.ticker,
                source="confirmation_monitoring",
                reason=reason,
                date_value=row.observed_at,
                extra={
                    "direction": resolved_direction,
                    "source_count": source_count,
                    "confirmation_score": score,
                    "source_stack": source_stack,
                },
            )
        )
    return triggers


def _recent_trade_triggers(db: Session, *, event_type: str, limit: int = 8) -> list[dict[str, Any]]:
    rows = db.execute(
        select(Event)
        .where(Event.event_type == event_type)
        .where(Event.symbol.is_not(None))
        .where(Event.ts >= _recent_since(30))
        .order_by(desc(Event.ts), desc(Event.amount_max))
        .limit(limit)
    ).scalars().all()
    triggers: list[dict[str, Any]] = []
    for row in rows:
        payload = _load_object(row.payload_json)
        actor = (
            row.member_name
            or payload.get("member_name")
            or payload.get("memberName")
            or payload.get("insider_name")
            or payload.get("reporting_owner_name")
            or payload.get("reportingOwnerName")
        )
        trade_type = row.transaction_type or row.trade_type or payload.get("transaction_type") or payload.get("transactionType") or "trade"
        amount = _compact_money(row.amount_max or row.amount_min or payload.get("value") or payload.get("amount"))
        source = "congress_activity" if event_type == "congress_trade" else "insider_activity"
        reason = f"{trade_type} reported"
        if actor:
            reason = f"{actor} {reason}"
        triggers.append(
            _trigger_payload(
                ticker=row.symbol,
                source=source,
                reason=reason,
                actor=actor,
                amount=amount,
                date_value=row.event_date or row.ts,
                extra={"trade_type": trade_type, "event_id": row.id},
            )
        )
    return triggers


def _institutional_triggers(db: Session, *, limit: int = 6) -> list[dict[str, Any]]:
    rows = db.execute(
        select(InstitutionalActivityEvent)
        .where(InstitutionalActivityEvent.feed_visible == True)  # noqa: E712
        .where(InstitutionalActivityEvent.direction == "bullish")
        .order_by(desc(InstitutionalActivityEvent.materiality_score), desc(InstitutionalActivityEvent.filing_date))
        .limit(limit)
    ).scalars().all()
    triggers: list[dict[str, Any]] = []
    for row in rows:
        amount_value = row.value_delta_usd if row.value_delta_usd is not None else row.reported_value_usd
        reason = row.summary or row.title or row.event_type.replace("_", " ")
        triggers.append(
            _trigger_payload(
                ticker=row.normalized_symbol or row.symbol,
                source="institutional_activity",
                reason=reason,
                actor=row.holder_name,
                amount=_compact_money(amount_value),
                date_value=row.filing_date,
                extra={
                    "event_type": row.event_type,
                    "materiality_score": row.materiality_score,
                    "confirmation_score": row.confirmation_score,
                },
            )
        )
    return triggers


def _government_contract_triggers(db: Session, *, limit: int = 6) -> list[dict[str, Any]]:
    action_rows = db.execute(
        select(GovernmentContractAction)
        .where(GovernmentContractAction.symbol.is_not(None))
        .where(GovernmentContractAction.action_date >= (date.today() - timedelta(days=45)))
        .order_by(desc(GovernmentContractAction.obligated_amount), desc(GovernmentContractAction.action_date))
        .limit(limit)
    ).scalars().all()
    triggers: list[dict[str, Any]] = []
    for row in action_rows:
        agency = row.awarding_sub_agency or row.awarding_agency or "government agency"
        description = row.description or row.action_type or "contract action"
        triggers.append(
            _trigger_payload(
                ticker=row.symbol,
                source="government_contracts",
                reason=f"{agency}: {description}",
                actor=row.company_name or row.recipient_name,
                amount=_compact_money(row.obligated_amount),
                date_value=row.action_date,
                extra={"award_id": row.parent_award_id, "action_type": row.action_type},
            )
        )
    if triggers:
        return triggers
    contract_rows = db.execute(
        select(GovernmentContract)
        .where(GovernmentContract.symbol.is_not(None))
        .where(GovernmentContract.award_date >= (date.today() - timedelta(days=45)))
        .order_by(desc(GovernmentContract.award_amount), desc(GovernmentContract.award_date))
        .limit(limit)
    ).scalars().all()
    for row in contract_rows:
        agency = row.awarding_sub_agency or row.awarding_agency or "government agency"
        description = row.description or row.contract_type or "contract award"
        triggers.append(
            _trigger_payload(
                ticker=row.symbol,
                source="government_contracts",
                reason=f"{agency}: {description}",
                actor=row.recipient_name or row.raw_recipient_name,
                amount=_compact_money(row.award_amount),
                date_value=row.award_date,
                extra={"award_id": row.award_id, "contract_type": row.contract_type},
            )
        )
    return triggers


def _watchlist_triggers(db: Session, campaign: AiMarketingCampaign, *, limit: int = 5) -> list[dict[str, Any]]:
    selector = str(campaign.source_reference_id or "").strip()
    tickers: list[str] = []
    if selector:
        watchlist = db.execute(select(Watchlist).where(func.lower(Watchlist.name) == selector.lower())).scalar_one_or_none()
        if watchlist:
            rows = db.execute(
                select(Security.symbol)
                .join(WatchlistItem, WatchlistItem.security_id == Security.id)
                .where(WatchlistItem.watchlist_id == watchlist.id)
                .where(Security.symbol.is_not(None))
                .limit(100)
            ).all()
            tickers = [str(row[0]).upper() for row in rows if row[0]]
    query = select(ConfirmationMonitoringSnapshot).where(ConfirmationMonitoringSnapshot.ticker.is_not(None))
    if tickers:
        query = query.where(func.upper(ConfirmationMonitoringSnapshot.ticker).in_(tickers))
    rows = db.execute(
        query.order_by(desc(ConfirmationMonitoringSnapshot.score), desc(ConfirmationMonitoringSnapshot.source_count), desc(ConfirmationMonitoringSnapshot.observed_at)).limit(limit)
    ).scalars().all()
    return [
        _trigger_payload(
            ticker=row.ticker,
            source="watchlist",
            reason=f"{row.status or row.band}; score {row.score}; {row.source_count} active sources",
            date_value=row.observed_at,
            extra={"watchlist": selector, "direction": row.direction, "confirmation_score": row.score},
        )
        for row in rows
    ]


def _saved_screen_triggers(db: Session, campaign: AiMarketingCampaign, *, limit: int = 5) -> list[dict[str, Any]]:
    selector = str(campaign.source_reference_id or "").strip()
    screen_ids: list[int] = []
    if selector:
        screens = db.execute(select(SavedScreen).where(func.lower(SavedScreen.name) == selector.lower())).scalars().all()
        screen_ids = [row.id for row in screens]
    query = select(SavedScreenSnapshot).where(SavedScreenSnapshot.ticker.is_not(None))
    if screen_ids:
        query = query.where(SavedScreenSnapshot.saved_screen_id.in_(screen_ids))
    rows = db.execute(
        query.order_by(desc(SavedScreenSnapshot.confirmation_score), desc(SavedScreenSnapshot.source_count), desc(SavedScreenSnapshot.observed_at)).limit(limit)
    ).scalars().all()
    return [
        _trigger_payload(
            ticker=row.ticker,
            source="saved_screen",
            reason=f"{row.confirmation_band} {row.direction}; score {row.confirmation_score}; {row.source_count} active sources",
            date_value=row.observed_at,
            extra={"saved_screen": selector, "why_now_state": row.why_now_state},
        )
        for row in rows
    ]


def _scheduled_x_triggers(db: Session, campaign: AiMarketingCampaign) -> list[dict[str, Any]]:
    source_type = str(campaign.source_type or "watchlist").strip()
    if source_type == "watchlist":
        return _watchlist_triggers(db, campaign)
    if source_type in {"saved_screen", "saved_view"}:
        return _saved_screen_triggers(db, campaign)
    if source_type == "bullish_confirmation":
        return _top_confirmation_triggers(db, direction="bullish")
    if source_type == "bearish_confirmation":
        return _top_confirmation_triggers(db, direction="bearish")
    if source_type == "congress_activity":
        return _recent_trade_triggers(db, event_type="congress_trade")
    if source_type == "insider_activity":
        return _recent_trade_triggers(db, event_type="insider_trade")
    if source_type == "institutional_activity":
        return _institutional_triggers(db)
    if source_type == "government_contracts":
        return _government_contract_triggers(db)
    if source_type in {"signal_feed", "ticker_context"}:
        return _top_confirmation_triggers(db, limit=6)
    return _watchlist_triggers(db, campaign)


def _scheduled_x_context(db: Session, campaign: AiMarketingCampaign, *, index: int = 1) -> tuple[str, dict[str, Any]]:
    filters = _load_object(campaign.filters_json)
    preferences = _load_object(campaign.output_preferences_json)
    schedule = _load_object(campaign.schedule_config_json)
    source_type = campaign.source_type or "watchlist"
    source_reference_id = campaign.source_reference_id or ""
    source_label = _scheduled_x_source_label(source_type, source_reference_id)
    triggers = _scheduled_x_triggers(db, campaign)
    tickers = _dedupe_strings([str(trigger.get("ticker") or "").upper() for trigger in triggers if trigger.get("ticker")])
    ticker_theme = ", ".join(f"${ticker}" for ticker in tickers[:5]) or source_reference_id or ", ".join(_load_list(campaign.tickers_json)) or source_label
    trigger_lines = [_trigger_line(trigger) for trigger in triggers[:8]]
    if not trigger_lines:
        trigger_lines = [
            "No fresh Walnut trigger rows were found for this campaign. Draft should say Walnut is monitoring this source and avoid inventing ticker examples."
        ]
    text = "\n".join(
        [
            f"Saved Walnut scheduled X campaign: {campaign.name}",
            f"Source type: {source_type}",
            f"Source selector: {source_reference_id or 'default'}",
            f"Schedule: {schedule.get('cadence') or ('weekdays' if campaign.weekdays_only else 'daily')} at {campaign.run_time or 'scheduled time'} {campaign.timezone or 'America/Los_Angeles'}",
            "Strategy: make the draft data-led. Mention the actual tickers/entities below, explain why Walnut flagged them, and include the relevant Walnut page URL for each example when possible. Do not write generic product marketing unless there are no fresh triggers.",
            "Walnut triggers:",
            *trigger_lines,
            f"Filters/preferences JSON: {json.dumps(filters, sort_keys=True)}",
            f"Draft preferences: {json.dumps(preferences, sort_keys=True)}",
            f"Draft slot: {index}",
            "Create a human-reviewed X draft for Walnut. Save to Draft Queue and email the recipient for approval/posting actions.",
        ]
    )
    inputs = {
        "scheduled_campaign": True,
        "source_type": source_type,
        "source_reference_id": source_reference_id,
        "filters": filters,
        "schedule": schedule,
        "preferences": preferences,
        "walnut_triggers": triggers,
        "trigger_tickers": tickers,
        "draft_slot": index,
        "include_image_card": bool(preferences.get("include_image_card", True)),
        "include_walnut_link": bool(preferences.get("include_walnut_link", True)),
        "cta_mode": preferences.get("cta_mode", "soft"),
        "hashtag_mode": preferences.get("hashtag_mode", "ticker/theme only"),
        "social_card": {
            "template": preferences.get("card_template") or preferences.get("template") or "ticker_signal",
            "tone": preferences.get("card_tone") or preferences.get("tone") or "market-native",
            "include_chart": bool(preferences.get("include_chart", True)),
            "include_cta": bool(preferences.get("include_cta", True)),
            "include_source_tag": bool(preferences.get("include_source_tag", True)),
            "include_walnut_url": bool(preferences.get("include_walnut_url", True)),
            "include_article_thumbnail": bool(preferences.get("include_article_thumbnail", False)),
        },
    }
    return ticker_theme, {"text": text, "inputs": inputs, "preferences": preferences, "source_label": source_label}


def _x_reply_handle_list(filters: dict[str, Any], campaign: AiMarketingCampaign, key: str) -> list[str]:
    values = filters.get(key)
    if not values and key == "target_handles":
        values = campaign.source_reference_id
    return _dedupe_strings([item.strip().lstrip("@") for item in _normalized_string_list(values) if item.strip()])


def _x_reply_search_terms(filters: dict[str, Any], campaign: AiMarketingCampaign) -> list[str]:
    terms = _normalized_string_list(filters.get("keywords") or filters.get("search_terms"))
    terms.extend(_load_list(campaign.keywords_json))
    terms.extend(f"${ticker}" for ticker in _load_list(campaign.tickers_json))
    return _dedupe_strings([term for term in terms if term])


def _x_author_map(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    includes = payload.get("includes") if isinstance(payload.get("includes"), dict) else {}
    users = includes.get("users") if isinstance(includes.get("users"), list) else []
    return {str(user.get("id")): user for user in users if isinstance(user, dict) and user.get("id")}


def _x_metric(metrics: dict[str, Any], key: str) -> int:
    try:
        return int(metrics.get(key) or 0)
    except (TypeError, ValueError):
        return 0


def _x_reply_candidate_score(tweet: dict[str, Any], author: dict[str, Any], *, matched_tickers: list[str], target_handles: list[str]) -> int:
    metrics = tweet.get("public_metrics") if isinstance(tweet.get("public_metrics"), dict) else {}
    user_metrics = author.get("public_metrics") if isinstance(author.get("public_metrics"), dict) else {}
    score = 35
    score += min(30, _x_metric(metrics, "like_count") // 10)
    score += min(20, _x_metric(metrics, "reply_count") // 5)
    score += min(20, _x_metric(metrics, "retweet_count") // 5)
    score += min(15, _x_metric(metrics, "quote_count") // 3)
    score += min(25, _x_metric(user_metrics, "followers_count") // 50000)
    if matched_tickers:
        score += 15
    if str(author.get("username") or "").lower() in {handle.lower() for handle in target_handles}:
        score += 20
    if author.get("verified"):
        score += 5
    return max(0, min(score, 100))


def _x_tweet_to_source_item(
    db: Session,
    campaign: AiMarketingCampaign,
    tweet: dict[str, Any],
    author: dict[str, Any],
    *,
    source_mode: str,
    target_handles: list[str],
) -> SourceItem | None:
    tweet_id = str(tweet.get("id") or "").strip()
    text = _truncate(str(tweet.get("text") or "").strip(), 1500) or ""
    if not tweet_id or not text:
        return None
    username = str(author.get("username") or "").strip()
    name = str(author.get("name") or username or "X user").strip()
    source_url = _x_author_public_url(username, tweet_id)
    tickers = _matched_tickers(text, _load_list(campaign.tickers_json), require_cashtag=True)
    walnut_context = _walnut_context_for_research_tickers(db, tickers[:3], title=text[:140], excerpt=text) if tickers else {}
    metrics = tweet.get("public_metrics") if isinstance(tweet.get("public_metrics"), dict) else {}
    score = _x_reply_candidate_score(tweet, author, matched_tickers=tickers, target_handles=target_handles)
    created_at = _parse_article_datetime(tweet.get("created_at"))
    ticker_theme = ", ".join(f"${ticker}" for ticker in tickers[:4]) or (f"@{username}" if username else "X reply")
    title = f"Reply candidate: @{username}" if username else "X reply candidate"
    return SourceItem(
        platform="x",
        source_id=f"x-reply:{tweet_id}",
        source_url=source_url,
        source_provider=X_REPLY_PROVIDER,
        campaign_type=X_REPLY_CAMPAIGN_TYPE,
        content_type="x_reply",
        source_platform="x",
        ticker_theme=ticker_theme,
        recommended_action="reply",
        title=title,
        excerpt=text,
        author=f"@{username}" if username else name,
        community="X",
        source_score=score,
        comment_count=_x_metric(metrics, "reply_count"),
        source_created_at=created_at,
        metadata={
            "x_reply_campaign": True,
            "x_source_mode": source_mode,
            "x_tweet_id": tweet_id,
            "x_author_id": tweet.get("author_id"),
            "x_author_username": username,
            "x_author_name": name,
            "x_public_metrics": metrics,
            "x_author_metrics": author.get("public_metrics") if isinstance(author.get("public_metrics"), dict) else {},
            "x_conversation_id": tweet.get("conversation_id"),
            "walnut_context": walnut_context,
            "article_tickers": tickers,
            "suggested_destination_url": f"https://walnutmarkets.com/ticker/{tickers[0]}" if tickers else campaign.default_destination_page,
            "inputs": {
                "reply_candidate_text": text,
                "reply_to_author": f"@{username}" if username else name,
                "reply_to_url": source_url,
                "source_mode": source_mode,
                "public_metrics": metrics,
                "campaign_preferences": _load_object(campaign.output_preferences_json),
                "filters": _load_object(campaign.filters_json),
                "instruction": "Generate a reply suggestion only. Do not post. Match Walnut's concise high-performing X reply voice.",
            },
        },
    )


def _x_reply_candidate_items(db: Session, campaign: AiMarketingCampaign) -> list[SourceItem]:
    filters = _load_object(campaign.filters_json)
    source_mode = str(campaign.source_type or filters.get("source_mode") or "home_feed").strip().lower()
    target_handles = _x_reply_handle_list(filters, campaign, "target_handles")
    ignore_handles = {handle.lower() for handle in _x_reply_handle_list(filters, campaign, "ignore_handles")}
    own_handle = (os.getenv("X_CONNECTED_HANDLE", "").strip().lstrip("@") or "WalnutMarkets").lower()
    ignore_handles.add(own_handle)
    limit = max(10, min(int(campaign.max_items_per_run or 25), 100))
    payloads: list[tuple[str, dict[str, Any]]] = []
    if source_mode in {"home_feed", "feed", "timeline"}:
        payloads.append(("home_feed", _x_home_timeline(db, limit=limit)))
    if source_mode in {"mentions", "account_mentions"}:
        payloads.append(("mentions", _x_mentions(db, limit=limit)))
    if source_mode in {"target_handles", "handles", "watch_handles"}:
        for handle in target_handles[:10]:
            query = f"from:{handle} -from:{own_handle} -is:retweet -is:reply lang:en"
            payloads.append((f"from:{handle}", _x_recent_search(db, query, limit=limit)))
    if source_mode in {"keyword_search", "search", "cashtag_search"}:
        terms = _x_reply_search_terms(filters, campaign)
        query_body = " OR ".join(terms[:12]) if terms else "$SPY OR $QQQ OR $NVDA OR $AAPL"
        query = f"({query_body}) -from:{own_handle} -is:retweet -is:reply lang:en"
        payloads.append(("keyword_search", _x_recent_search(db, query, limit=limit)))
    if not payloads:
        payloads.append(("home_feed", _x_home_timeline(db, limit=limit)))

    items: list[SourceItem] = []
    seen_ids: set[str] = set()
    min_score = _clamp_int(filters.get("minimum_candidate_score") or campaign.minimum_relevance_score or 50, 0, 100)
    require_ticker = bool(filters.get("require_ticker", False))
    for mode, payload in payloads:
        authors = _x_author_map(payload)
        tweets = payload.get("data") if isinstance(payload.get("data"), list) else []
        for tweet in tweets:
            if not isinstance(tweet, dict):
                continue
            tweet_id = str(tweet.get("id") or "")
            if not tweet_id or tweet_id in seen_ids:
                continue
            author = authors.get(str(tweet.get("author_id") or ""), {})
            username = str(author.get("username") or "").strip().lstrip("@")
            if username.lower() in ignore_handles:
                continue
            text = str(tweet.get("text") or "")
            if require_ticker and not _matched_tickers(text, _load_list(campaign.tickers_json), require_cashtag=True):
                continue
            item = _x_tweet_to_source_item(db, campaign, tweet, author, source_mode=mode, target_handles=target_handles)
            if not item or int(item.source_score or 0) < min_score:
                continue
            seen_ids.add(tweet_id)
            items.append(item)
    return sorted(items, key=lambda item: int(item.source_score or 0), reverse=True)


def run_x_reply_campaign(db: Session, campaign: AiMarketingCampaign) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "status": "ok",
        "candidates_considered": 0,
        "drafts_generated": 0,
        "emails_sent": 0,
        "created": 0,
        "deduped": 0,
        "suggested": 0,
        "warnings": [],
        "errors": [],
        "opportunities": [],
    }
    status = str(campaign.status or ("active" if campaign.enabled else "paused")).lower()
    if not campaign.enabled or status != "active":
        summary["status"] = status if status in {"paused", "stopped"} else "paused"
        summary["warnings"].append("Campaign is disabled; no X reply discovery run was performed.")
        record_campaign_run(db, campaign, summary)
        return summary
    try:
        candidates = _x_reply_candidate_items(db, campaign)
    except MissingMarketingCredential as exc:
        summary["status"] = "config_error"
        summary["errors"].append(str(exc))
        record_campaign_run(db, campaign, summary)
        return summary
    except Exception as exc:
        logger.exception("x_reply_discovery_failed campaign_id=%s", campaign.id)
        summary["status"] = "error"
        summary["errors"].append(f"X reply discovery failed: {exc}")
        record_campaign_run(db, campaign, summary)
        return summary

    summary["candidates_considered"] = len(candidates)
    max_drafts = max(1, min(int(campaign.max_drafts_per_day or 5), 10))
    selected: list[AiMarketingOpportunity] = []
    for item in candidates[:max_drafts]:
        opportunity, was_created = upsert_source_item(db, campaign, item)
        selected.append(opportunity)
        if was_created:
            summary["created"] += 1
        else:
            summary["deduped"] += 1
        if resolved_setting_value(db, OPENAI_API_KEY):
            try:
                generate_suggestion(db, opportunity, campaign=campaign)
                summary["suggested"] += 1
            except OpenAISuggestionError as exc:
                summary["warnings"].append(f"Reply suggestion failed for draft {opportunity.id}: {exc.admin_message}")
            except Exception:
                logger.exception("x_reply_suggestion_failed opportunity_id=%s", opportunity.id)
                summary["warnings"].append(f"Reply suggestion failed for draft {opportunity.id}.")
        else:
            summary["warnings"].append("OpenAI API key missing; X reply candidate saved without generated copy.")
            _record_suggestion_failure(db, opportunity, OPENAI_MISSING_KEY_MESSAGE, code="missing_key")
        try:
            send_draft_email(db, opportunity, to_email=campaign.recipient_email or ai_growth_recipient())
            summary["emails_sent"] += 1
        except Exception:
            logger.exception("x_reply_email_failed opportunity_id=%s", opportunity.id)
            summary["warnings"].append(f"Email failed for X reply draft {opportunity.id}.")
    summary["drafts_generated"] = len(selected)
    latest = latest_suggestions_by_opportunity(db, [row.id for row in selected])
    summary["opportunities"] = [opportunity_to_dict(row, suggestion=latest.get(row.id)) for row in selected]
    campaign.last_run_at = datetime.now(timezone.utc)
    db.commit()
    record_campaign_run(db, campaign, summary)
    return summary


def run_scheduled_x_campaign(db: Session, campaign: AiMarketingCampaign) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "status": "ok",
        "candidates_considered": 0,
        "drafts_generated": 0,
        "emails_sent": 0,
        "created": 0,
        "deduped": 0,
        "suggested": 0,
        "warnings": [],
        "errors": [],
        "opportunities": [],
    }
    status = str(campaign.status or ("active" if campaign.enabled else "paused")).lower()
    if not campaign.enabled or status != "active":
        summary["status"] = status if status in {"paused", "stopped"} else "paused"
        summary["warnings"].append("Campaign is disabled; no scheduled X run was performed.")
        record_campaign_run(db, campaign, summary)
        return summary
    max_drafts = max(1, min(int(campaign.max_drafts_per_day or 1), 10))
    selected: list[AiMarketingOpportunity] = []
    for index in range(1, max_drafts + 1):
        ticker_theme, context = _scheduled_x_context(db, campaign, index=index)
        source_key = f"scheduled-x:{campaign.id}:{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}:{index}"
        source_item = SourceItem(
            platform="x",
            source_id=source_key,
            source_url=MANUAL_SOURCE_URL,
            source_provider="scheduled_x_campaign",
            campaign_type=SCHEDULED_X_CAMPAIGN_TYPE,
            content_type="x_post",
            source_platform="x",
            ticker_theme=ticker_theme,
            recommended_action="draft_post",
            title=f"{campaign.name}: {context['source_label']}",
            excerpt=context["text"],
            source_score=80,
            metadata={
                "scheduled_campaign": True,
                "source_type": campaign.source_type,
                "source_reference_id": campaign.source_reference_id,
                "inputs": context["inputs"],
                "tone": context["preferences"].get("tone"),
                "article_tickers": context["inputs"].get("trigger_tickers") or [],
                "walnut_context": context["inputs"].get("walnut_triggers") or [],
                "suggested_destination_url": campaign.default_destination_page or DEFAULT_DESTINATION_URL,
            },
        )
        opportunity, was_created = upsert_source_item(db, campaign, source_item)
        selected.append(opportunity)
        summary["candidates_considered"] += 1
        if was_created:
            summary["created"] += 1
        else:
            summary["deduped"] += 1
        if resolved_setting_value(db, OPENAI_API_KEY):
            try:
                generate_suggestion(db, opportunity, campaign=campaign)
                summary["suggested"] += 1
            except OpenAISuggestionError as exc:
                summary["warnings"].append(f"Suggestion generation failed for draft {opportunity.id}: {exc.admin_message}")
            except Exception:
                logger.exception("scheduled_x_suggestion_failed opportunity_id=%s", opportunity.id)
                summary["warnings"].append(f"Suggestion generation failed for draft {opportunity.id}.")
        else:
            summary["warnings"].append("OpenAI API key missing; scheduled X draft saved without generated copy.")
            _record_suggestion_failure(db, opportunity, OPENAI_MISSING_KEY_MESSAGE, code="missing_key")
        try:
            send_draft_email(db, opportunity, to_email=campaign.recipient_email or ai_growth_recipient())
            summary["emails_sent"] += 1
        except Exception:
            logger.exception("scheduled_x_email_failed opportunity_id=%s", opportunity.id)
            summary["warnings"].append(f"Email failed for draft {opportunity.id}.")
    summary["drafts_generated"] = len(selected)
    latest = latest_suggestions_by_opportunity(db, [row.id for row in selected])
    summary["opportunities"] = [opportunity_to_dict(row, suggestion=latest.get(row.id)) for row in selected]
    campaign.last_run_at = datetime.now(timezone.utc)
    db.commit()
    record_campaign_run(db, campaign, summary)
    return summary


def run_campaign(db: Session, campaign: AiMarketingCampaign) -> dict[str, Any]:
    warnings: list[str] = []
    status = str(campaign.status or ("active" if campaign.enabled else "paused")).lower()
    if not campaign.enabled or status != "active":
        warnings.append("Campaign is disabled; no discovery run was performed.")
        return {"status": status if status in {"paused", "stopped"} else "paused", "created": 0, "deduped": 0, "suggested": 0, "warnings": warnings, "opportunities": []}
    if _normalize_campaign_type(campaign.campaign_type, fallback_mode=campaign.mode) == ARTICLE_REACTIVE_CAMPAIGN_TYPE:
        return run_article_reactive_campaign(db, campaign)
    if _normalize_campaign_type(campaign.campaign_type, fallback_mode=campaign.mode) == SCHEDULED_X_CAMPAIGN_TYPE:
        return run_scheduled_x_campaign(db, campaign)
    if _normalize_campaign_type(campaign.campaign_type, fallback_mode=campaign.mode) == X_REPLY_CAMPAIGN_TYPE:
        return run_x_reply_campaign(db, campaign)

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
    matched_tickers = _matched_tickers(
        text_for_matching,
        campaign_tickers,
        require_cashtag=item.source_provider == X_REPLY_PROVIDER,
    )
    metadata_tickers = _normalized_tickers((item.metadata or {}).get("article_tickers") if item.metadata else [])
    matched_tickers = _dedupe_strings([*matched_tickers, *metadata_tickers])
    item_metadata = dict(item.metadata or {})
    source_dedupe_key = _dedupe_key(item.source_id or item.source_url)
    now = datetime.now(timezone.utc)
    fallback_mode = campaign.mode if campaign else None
    campaign_type = _normalize_campaign_type(item.campaign_type, fallback_mode=fallback_mode)
    content_type = _normalize_content_type(item.content_type, campaign_type=campaign_type, platform=item.platform)
    recommended_action = item.recommended_action or _default_action_for_content_type(content_type)
    source_platform = _normalize_source_platform(item.source_platform, fallback=item.platform)
    if content_type == "reddit_thread" and matched_tickers and not item_metadata.get("walnut_context"):
        item_metadata["walnut_context"] = _walnut_context_for_research_tickers(
            db,
            matched_tickers,
            title=item.title,
            excerpt=item.excerpt,
        )

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
        opportunity.raw_metadata_json = _dump_object(item_metadata)
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
        raw_metadata_json=_dump_object(item_metadata),
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
    card_options: dict[str, Any] | None = None,
) -> dict[str, Any]:
    change_text = _truncate(str(change_request or "").strip(), 1000) or ""
    if change_text or card_options:
        metadata = _load_object(opportunity.raw_metadata_json)
        if change_text:
            history = _coerce_json_list(metadata.get("change_requests"))
            history.append({"requested_at": datetime.now(timezone.utc).isoformat(), "request": change_text})
            metadata["change_request"] = change_text
            metadata["change_requests"] = history[-10:]
        if card_options:
            inputs = metadata.get("inputs") if isinstance(metadata.get("inputs"), dict) else {}
            existing = inputs.get("social_card") if isinstance(inputs.get("social_card"), dict) else {}
            inputs["social_card"] = {**existing, **card_options}
            metadata["inputs"] = inputs
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
    social_card_preferences = _social_card_preferences(opportunity_metadata)
    social_card_type = _social_card_type_for_context(campaign_type, content_type, social_card_preferences)
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
            {"role": "system", "content": _suggestion_system_prompt(db)},
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
                                "article_reactive": opportunity_metadata.get("article_reactive"),
                                "article_title": opportunity_metadata.get("article_title"),
                                "article_source": opportunity_metadata.get("article_source"),
                                "article_url": opportunity_metadata.get("article_url"),
                                "article_published_at": opportunity_metadata.get("article_published_at"),
                                "article_tickers": opportunity_metadata.get("article_tickers"),
                                "web_market_context": opportunity_metadata.get("web_market_context"),
                                "themes": opportunity_metadata.get("themes"),
                                "walnut_context": opportunity_metadata.get("walnut_context"),
                                "scoring": opportunity_metadata.get("scoring"),
                                "social_card_preferences": social_card_preferences,
                            },
                            "assets": _load_json_list(opportunity.asset_refs_json),
                        },
                        "routing_hint": destination_hint,
                        "content_constraints": {
                            "social_card": {
                                "required": content_type in {"x_post", "reddit_thread"},
                                "preferred_card_type": social_card_type,
                                "allowed_card_types": sorted(SOCIAL_CARD_TYPES),
                                "allowed_tones": sorted(SOCIAL_CARD_TONES),
                                "x_landscape_ratio": "1600x900",
                                "hard_requirements": [
                                    "structured JSON only",
                                    "short headline and subheadline",
                                    "2-4 concise bullets",
                                    "no invented data",
                                    "CTA/url fields obey include flags",
                                ],
                                "preferences": social_card_preferences,
                            },
                            "x_post": {
                                "max_characters": X_POST_CHARACTER_LIMIT,
                                "applies_to": ["suggested_post", "alternate_hooks"],
                                "hard_requirement": True,
                            },
                            "x_reply": {
                                "max_characters": X_POST_CHARACTER_LIMIT,
                                "applies_to": ["suggested_reply", "alternate_reply_more_direct"],
                                "hard_requirement": True,
                            },
                        },
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
    _record_openai_usage_cost(db, model=model, data=data, feature="ai_growth_suggestion")
    content = _extract_chat_completion_content(data)
    structured = _normalize_suggestion_payload(
        json.loads(content),
        destination_hint,
        platform,
        campaign.id if campaign else 0,
        opportunity=opportunity,
        openai_api_key=api_key,
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
    opportunity.asset_refs_json = _dump_json_list(
        _prefer_generated_thumbnail_assets(_normalize_assets(_load_json_list(opportunity.asset_refs_json) + structured["assets"]))
    )
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


EMAIL_ACTIONS = {"approve", "reject", "reject_and_regenerate", "reply"}
X_POST_ENDPOINT_PATH = "/2/tweets"
X_CASHTAG_RE = re.compile(r"(?<![\w$])\$([A-Za-z][A-Za-z0-9.]{0,14})(?![A-Za-z0-9_])")


def _normalize_x_post_text_for_api(text: str) -> str:
    seen_cashtag = False

    def replace_extra_cashtag(match: re.Match[str]) -> str:
        nonlocal seen_cashtag
        if not seen_cashtag:
            seen_cashtag = True
            return match.group(0)
        return match.group(1)

    return X_CASHTAG_RE.sub(replace_extra_cashtag, text)


def _public_app_base_url() -> str:
    return os.getenv("WALNUT_APP_URL", "").strip().rstrip("/") or "https://walnutmarkets.com"


def _email_action_secret() -> str:
    return os.getenv("AI_GROWTH_EMAIL_ACTION_SECRET", "").strip() or os.getenv("APP_SESSION_SECRET", "dev-session-secret")


def _b64_json(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _unb64_json(value: str) -> dict[str, Any]:
    padding = "=" * (-len(value) % 4)
    return json.loads(base64.urlsafe_b64decode((value + padding).encode("ascii")).decode("utf-8"))


def _sign_email_action_payload(payload_b64: str) -> str:
    return hmac.new(_email_action_secret().encode("utf-8"), payload_b64.encode("ascii"), hashlib.sha256).hexdigest()


def create_email_action_token(
    db: Session,
    draft_id: int,
    action: str,
    *,
    actor_email: str | None = None,
    ttl_hours: int = 72,
) -> str:
    normalized_action = str(action or "").strip().lower()
    if normalized_action not in EMAIL_ACTIONS:
        raise ValueError("Unsupported AI Growth email action.")
    now = datetime.now(timezone.utc)
    token_id = secrets.token_urlsafe(12)
    nonce = secrets.token_urlsafe(18)
    expires_at = now + timedelta(hours=max(1, min(ttl_hours, 168)))
    payload = {"draft_id": int(draft_id), "action": normalized_action, "exp": int(expires_at.timestamp()), "nonce": nonce, "tid": token_id}
    payload_b64 = _b64_json(payload)
    token = f"{payload_b64}.{_sign_email_action_payload(payload_b64)}"
    db.add(
        AiGrowthEmailActionToken(
            token_id=token_id,
            draft_id=int(draft_id),
            action=normalized_action,
            actor_email=actor_email,
            nonce_hash=_dedupe_key(nonce),
            expires_at=expires_at,
        )
    )
    db.commit()
    return token


def email_action_url(db: Session, draft_id: int, action: str, *, actor_email: str | None = None) -> str:
    token = create_email_action_token(db, draft_id, action, actor_email=actor_email)
    return f"{_public_app_base_url()}/api/admin/ai-growth/email-action?token={quote(token)}"


def reply_to_address_for_draft(db: Session, draft_id: int, *, actor_email: str | None = None) -> str:
    address = os.getenv(AI_GROWTH_INBOUND_REPLY_ADDRESS, "ai-growth@walnutmarkets.com").strip() or "ai-growth@walnutmarkets.com"
    local, _, domain = address.partition("@")
    token = create_email_action_token(db, draft_id, "reply", actor_email=actor_email)
    payload = _unb64_json(token.split(".", 1)[0])
    return f"{local}+draft_{draft_id}_{payload['tid']}@{domain}"


def verify_email_action_token(
    db: Session,
    token: str,
    *,
    consume: bool = True,
    ip_address: str | None = None,
    user_agent: str | None = None,
) -> tuple[AiMarketingOpportunity, AiGrowthEmailActionToken, dict[str, Any]]:
    try:
        payload_b64, signature = token.split(".", 1)
        expected = _sign_email_action_payload(payload_b64)
        if not hmac.compare_digest(signature, expected):
            raise ValueError("Invalid token signature.")
        payload = _unb64_json(payload_b64)
    except Exception as exc:
        raise ValueError("Invalid AI Growth action token.") from exc
    action = str(payload.get("action") or "").strip().lower()
    if action not in EMAIL_ACTIONS:
        raise ValueError("Unsupported AI Growth action.")
    if datetime.now(timezone.utc).timestamp() > int(payload.get("exp") or 0):
        raise ValueError("AI Growth action token expired.")
    row = db.execute(
        select(AiGrowthEmailActionToken).where(
            AiGrowthEmailActionToken.token_id == str(payload.get("tid") or ""),
            AiGrowthEmailActionToken.draft_id == int(payload.get("draft_id") or 0),
            AiGrowthEmailActionToken.action == action,
        )
    ).scalar_one_or_none()
    if not row or row.nonce_hash != _dedupe_key(str(payload.get("nonce") or "")):
        raise ValueError("AI Growth action token not recognized.")
    if row.used_at:
        raise ValueError("AI Growth action token already used.")
    draft = db.get(AiMarketingOpportunity, int(payload.get("draft_id") or 0))
    if not draft:
        raise ValueError("Draft not found.")
    if consume:
        row.used_at = datetime.now(timezone.utc)
        row.ip_address = _truncate(ip_address, 120)
        row.user_agent = _truncate(user_agent, 500)
        db.commit()
    return draft, row, payload


def apply_email_action(
    db: Session,
    token: str,
    *,
    ip_address: str | None = None,
    user_agent: str | None = None,
) -> dict[str, Any]:
    draft, token_row, payload = verify_email_action_token(db, token, ip_address=ip_address, user_agent=user_agent)
    action = str(payload["action"])
    if draft.status in {"posted", "posted_manually", "archived", "rejected", "superseded"} and action != "reply":
        token_row.result = "rejected_status_not_allowed"
        db.commit()
        raise ValueError("Draft status no longer allows this action.")
    if action == "approve":
        latest = latest_suggestions_by_opportunity(db, [draft.id]).get(draft.id)
        posting = post_approved_draft_to_x(db, draft, suggestion=latest)
        token_row.result = "posted" if posting.get("ok") else "approved"
    elif action == "reject":
        draft.status = "rejected"
        token_row.result = "rejected"
        posting = None
    elif action == "reject_and_regenerate":
        draft.status = "rejected_regenerate_requested"
        token_row.result = "regeneration_requested"
        regenerate_growth_draft(db, draft, change_request="Reject this idea and regenerate a replacement angle. Preserve compliance and no-investment-advice framing.")
        posting = None
    else:
        token_row.result = "unsupported_action"
        posting = None
    draft.updated_at = datetime.now(timezone.utc)
    db.commit()
    latest = latest_suggestions_by_opportunity(db, [draft.id]).get(draft.id)
    result = {"status": token_row.result, "draft": opportunity_to_dict(draft, suggestion=latest)}
    if posting is not None:
        result["posting"] = posting
    return result


def _x_api_base_url() -> str:
    return (os.getenv(X_API_BASE_URL, "").strip().rstrip("/") or "https://api.x.com")


def _x_oauth2_token_url() -> str:
    return os.getenv(X_OAUTH2_TOKEN_URL, "").strip() or f"{_x_api_base_url()}/2/oauth2/token"


def _x_post_public_url(post_id: str | None) -> str | None:
    cleaned = str(post_id or "").strip()
    if not cleaned:
        return None
    handle = os.getenv("X_CONNECTED_HANDLE", "").strip().lstrip("@")
    if handle:
        return f"https://x.com/{quote(handle)}/status/{quote(cleaned)}"
    return f"https://x.com/i/web/status/{quote(cleaned)}"


def _x_error_message(response: requests.Response) -> str:
    try:
        payload = response.json()
    except ValueError:
        payload = {}
    if isinstance(payload, dict):
        errors = payload.get("errors")
        if isinstance(errors, list) and errors:
            messages = []
            for error in errors:
                if isinstance(error, dict):
                    detail = error.get("detail") or error.get("message") or error.get("title")
                    if detail:
                        messages.append(str(detail))
            if messages:
                return "; ".join(messages[:3])
        detail = payload.get("detail") or payload.get("title") or payload.get("message")
        if detail:
            return str(detail)
    return f"X API returned HTTP {response.status_code}."


def _x_author_public_url(username: str | None, tweet_id: str | None) -> str:
    clean_id = str(tweet_id or "").strip()
    handle = str(username or "").strip().lstrip("@")
    if handle and clean_id:
        return f"https://x.com/{quote(handle)}/status/{quote(clean_id)}"
    if clean_id:
        return f"https://x.com/i/web/status/{quote(clean_id)}"
    return "https://x.com/home"


def _x_current_access_token(db: Session | None = None) -> str:
    return (_private_setting_value(db, X_CURRENT_ACCESS_TOKEN_SETTING) or os.getenv(X_ACCESS_TOKEN, "")).strip()


def _x_current_refresh_token(db: Session | None = None) -> str:
    return (_private_setting_value(db, X_CURRENT_REFRESH_TOKEN_SETTING) or os.getenv(X_REFRESH_TOKEN, "")).strip()


def _post_x_tweet(access_token: str, text: str) -> requests.Response:
    return requests.post(
        f"{_x_api_base_url()}{X_POST_ENDPOINT_PATH}",
        headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
        json={"text": text},
        timeout=12,
    )


def _x_get_json(db: Session, path: str, *, params: dict[str, Any] | None = None) -> dict[str, Any]:
    access_token = _x_current_access_token(db)
    if not access_token:
        raise MissingMarketingCredential("X access token missing. Connect X OAuth before running reply suggestions.")

    url = f"{_x_api_base_url()}{path}"
    try:
        response = requests.get(url, headers={"Authorization": f"Bearer {access_token}"}, params=params or {}, timeout=18)
    except requests.RequestException as exc:
        raise RuntimeError(f"X API request failed: {exc}") from exc

    if response.status_code == 401:
        refresh_result = _refresh_x_oauth2_access_token(db)
        if refresh_result.get("ok"):
            try:
                response = requests.get(
                    url,
                    headers={"Authorization": f"Bearer {refresh_result['access_token']}"},
                    params=params or {},
                    timeout=18,
                )
            except requests.RequestException as exc:
                raise RuntimeError(f"X API request failed after token refresh: {exc}") from exc
        elif refresh_result.get("attempted"):
            raise MissingMarketingCredential(f"X token refresh failed: {refresh_result.get('reason')}")

    if not (200 <= response.status_code < 300):
        raise RuntimeError(_x_error_message(response))
    try:
        data = response.json()
    except ValueError as exc:
        raise RuntimeError("X API returned a non-JSON response.") from exc
    return data if isinstance(data, dict) else {}


def _x_connected_user_id(db: Session) -> str:
    configured = os.getenv("X_CONNECTED_USER_ID", "").strip()
    if configured:
        return configured
    data = _x_get_json(db, "/2/users/me", params={"user.fields": "username,name"})
    user = data.get("data") if isinstance(data.get("data"), dict) else {}
    user_id = str(user.get("id") or "").strip()
    if not user_id:
        raise MissingMarketingCredential("X OAuth is connected, but /2/users/me did not return a user id.")
    os.environ["X_CONNECTED_USER_ID"] = user_id
    if user.get("username"):
        os.environ["X_CONNECTED_HANDLE"] = str(user.get("username") or "").strip()
    return user_id


def _x_recent_search(db: Session, query: str, *, limit: int) -> dict[str, Any]:
    return _x_get_json(
        db,
        "/2/tweets/search/recent",
        params={
            "query": query,
            "max_results": max(10, min(limit, 100)),
            "tweet.fields": "created_at,public_metrics,conversation_id,author_id,entities,referenced_tweets,lang",
            "expansions": "author_id",
            "user.fields": "username,name,verified,public_metrics",
        },
    )


def _x_home_timeline(db: Session, *, limit: int) -> dict[str, Any]:
    user_id = _x_connected_user_id(db)
    return _x_get_json(
        db,
        f"/2/users/{quote(user_id)}/timelines/reverse_chronological",
        params={
            "max_results": max(5, min(limit, 100)),
            "exclude": "retweets,replies",
            "tweet.fields": "created_at,public_metrics,conversation_id,author_id,entities,referenced_tweets,lang",
            "expansions": "author_id",
            "user.fields": "username,name,verified,public_metrics",
        },
    )


def _x_mentions(db: Session, *, limit: int) -> dict[str, Any]:
    user_id = _x_connected_user_id(db)
    return _x_get_json(
        db,
        f"/2/users/{quote(user_id)}/mentions",
        params={
            "max_results": max(5, min(limit, 100)),
            "tweet.fields": "created_at,public_metrics,conversation_id,author_id,entities,referenced_tweets,lang",
            "expansions": "author_id",
            "user.fields": "username,name,verified,public_metrics",
        },
    )


def _x_user_lookup(db: Session, handle: str) -> dict[str, Any] | None:
    cleaned = str(handle or "").strip().lstrip("@")
    if not cleaned:
        return None
    data = _x_get_json(db, f"/2/users/by/username/{quote(cleaned)}", params={"user.fields": "username,name,verified,public_metrics"})
    user = data.get("data")
    return user if isinstance(user, dict) else None


def _refresh_x_oauth2_access_token(db: Session) -> dict[str, Any]:
    refresh_token = _x_current_refresh_token(db)
    client_id = os.getenv(X_CLIENT_ID, "").strip()
    client_secret = os.getenv(X_CLIENT_SECRET, "").strip()
    if not refresh_token:
        return {"ok": False, "attempted": False, "reason": "X_REFRESH_TOKEN is not configured on the server."}
    if not client_id or not client_secret:
        missing = [key for key, value in ((X_CLIENT_ID, client_id), (X_CLIENT_SECRET, client_secret)) if not value]
        return {"ok": False, "attempted": False, "reason": f"X token refresh is missing: {', '.join(missing)}."}

    try:
        response = requests.post(
            _x_oauth2_token_url(),
            auth=(client_id, client_secret),
            data={"grant_type": "refresh_token", "refresh_token": refresh_token, "client_id": client_id},
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=12,
        )
    except requests.RequestException as exc:
        return {"ok": False, "attempted": True, "reason": f"X token refresh failed: {exc}"}

    if not (200 <= response.status_code < 300):
        return {
            "ok": False,
            "attempted": True,
            "status_code": response.status_code,
            "reason": f"X token refresh failed: {_x_error_message(response)}",
        }

    try:
        payload = response.json()
    except ValueError:
        payload = {}
    if not isinstance(payload, dict):
        payload = {}
    access_token = str(payload.get("access_token") or "").strip()
    rotated_refresh_token = str(payload.get("refresh_token") or "").strip() or refresh_token
    if not access_token:
        return {
            "ok": False,
            "attempted": True,
            "status_code": response.status_code,
            "reason": "X token refresh response did not include an access token.",
        }

    refreshed_at = datetime.now(timezone.utc).isoformat()
    _upsert_private_setting(db, X_CURRENT_ACCESS_TOKEN_SETTING, access_token, is_secret=True)
    _upsert_private_setting(db, X_CURRENT_REFRESH_TOKEN_SETTING, rotated_refresh_token, is_secret=True)
    _upsert_private_setting(db, X_TOKEN_REFRESHED_AT_SETTING, refreshed_at, is_secret=False)
    os.environ[X_ACCESS_TOKEN] = access_token
    os.environ[X_REFRESH_TOKEN] = rotated_refresh_token
    db.flush()
    return {"ok": True, "attempted": True, "access_token": access_token, "refreshed_at": refreshed_at}


def post_approved_draft_to_x(
    db: Session,
    opportunity: AiMarketingOpportunity,
    *,
    suggestion: AiMarketingSuggestion | None = None,
) -> dict[str, Any]:
    content_type = _normalize_content_type(opportunity.content_type, campaign_type=opportunity.campaign_type, platform=opportunity.platform)
    if content_type != "x_post":
        opportunity.status = "approved"
        return {"attempted": False, "ok": False, "reason": "Draft is not an X post."}

    access_token = _x_current_access_token(db)
    if not access_token:
        opportunity.status = "approved"
        return {"attempted": False, "ok": False, "reason": "X_ACCESS_TOKEN is not configured on the server."}

    text = (_generated_content_from_suggestion(suggestion) or opportunity.generated_content or opportunity.full_markdown or "").strip()
    text = _normalize_x_post_text_for_api(text)
    if not text:
        opportunity.status = "approved"
        return {"attempted": False, "ok": False, "reason": "Draft has no post text."}
    if len(text) > X_POST_CHARACTER_LIMIT:
        opportunity.status = "approved"
        return {"attempted": False, "ok": False, "reason": f"Draft is {len(text)} characters; X posts must be {X_POST_CHARACTER_LIMIT} characters or fewer."}

    refresh_result: dict[str, Any] | None = None
    try:
        response = _post_x_tweet(access_token, text)
    except requests.RequestException as exc:
        opportunity.status = "approved"
        return {"attempted": True, "ok": False, "reason": f"X API request failed: {exc}"}

    if response.status_code == 401:
        refresh_result = _refresh_x_oauth2_access_token(db)
        if refresh_result.get("ok"):
            try:
                response = _post_x_tweet(str(refresh_result["access_token"]), text)
            except requests.RequestException as exc:
                opportunity.status = "approved"
                return {
                    "attempted": True,
                    "ok": False,
                    "refreshed": True,
                    "reason": f"X API request failed after token refresh: {exc}",
                }

    if not (200 <= response.status_code < 300):
        opportunity.status = "approved"
        reason = _x_error_message(response)
        if response.status_code in {401, 403}:
            reason = f"{reason} Confirm the X access token has tweet.write scope."
        if refresh_result and not refresh_result.get("ok"):
            reason = f"{reason} Token refresh did not complete: {refresh_result.get('reason')}"
        return {
            "attempted": True,
            "ok": False,
            "status_code": response.status_code,
            "refreshed": bool(refresh_result and refresh_result.get("ok")),
            "reason": reason,
        }

    try:
        payload = response.json()
    except ValueError:
        payload = {}
    data = payload.get("data") if isinstance(payload, dict) else {}
    post_id = str(data.get("id") or "").strip() if isinstance(data, dict) else ""
    now = datetime.now(timezone.utc)
    metadata = _load_object(opportunity.raw_metadata_json)
    metadata["x_post_id"] = post_id
    metadata["x_post_url"] = _x_post_public_url(post_id)
    metadata["x_posted_at"] = now.isoformat()
    opportunity.raw_metadata_json = _dump_object(metadata)
    opportunity.status = "posted"
    opportunity.posted_manually_at = now
    opportunity.updated_at = now
    db.flush()
    return {
        "attempted": True,
        "ok": True,
        "status_code": response.status_code,
        "x_post_id": post_id or None,
        "x_post_url": metadata.get("x_post_url"),
        "text": data.get("text") if isinstance(data, dict) else text,
        "refreshed": bool(refresh_result and refresh_result.get("ok")),
    }


def x_account_status(db: Session | None = None) -> dict[str, Any]:
    access_token = _x_current_access_token(db)
    refresh_token = _x_current_refresh_token(db)
    client_id = os.getenv(X_CLIENT_ID, "").strip()
    client_secret = os.getenv(X_CLIENT_SECRET, "").strip()
    redirect_uri = os.getenv(X_REDIRECT_URI, "").strip()
    missing = [
        key
        for key, value in (
            (X_CLIENT_ID, client_id),
            (X_CLIENT_SECRET, client_secret),
            (X_REDIRECT_URI, redirect_uri),
        )
        if not value
    ]
    oauth_configured = not missing
    return {
        "connected": bool(access_token),
        "oauth_configured": oauth_configured,
        "configured": oauth_configured,
        "status": "connected" if access_token else "configured" if oauth_configured else "missing",
        "missing": missing,
        "handle": os.getenv("X_CONNECTED_HANDLE", "").strip() or None,
        "user_id": os.getenv("X_CONNECTED_USER_ID", "").strip() or None,
        "token_status": "configured" if access_token else "missing",
        "refresh_token_status": "configured" if refresh_token else "missing",
        "last_successful_post": None,
        "last_token_refresh": _private_setting_value(db, X_TOKEN_REFRESHED_AT_SETTING),
        "secrets_managed_by": "server_env_or_encrypted_store",
        "posting_mode": "approve_posts_to_x" if access_token else "approval_only_until_x_access_token_configured",
        "required_scopes": ["tweet.read", "tweet.write", "users.read", "offline.access"],
    }


def process_postmark_ai_growth_inbound(
    db: Session,
    payload: dict[str, Any],
    *,
    webhook_secret: str | None = None,
) -> dict[str, Any]:
    expected_secret = os.getenv(POSTMARK_INBOUND_WEBHOOK_SECRET, "").strip() or os.getenv(POSTMARK_INBOUND_BASIC_AUTH_SECRET, "").strip()
    if expected_secret and webhook_secret != expected_secret:
        raise ValueError("Invalid inbound webhook secret.")
    from_email = str(payload.get("From") or (payload.get("FromFull") or {}).get("Email") or "").strip().lower()
    if from_email and from_email != ai_growth_recipient().lower():
        raise ValueError("Inbound AI Growth reply sender is not authorized.")
    mailbox_hash = str(payload.get("MailboxHash") or "").strip()
    if not mailbox_hash:
        to_value = str(payload.get("To") or "").strip()
        match_to = re.search(r"\+draft_(\d+)_([^@>\s]+)", to_value)
        mailbox_hash = f"draft_{match_to.group(1)}_{match_to.group(2)}" if match_to else ""
    match = re.fullmatch(r"draft_(\d+)_([A-Za-z0-9_-]+)", mailbox_hash)
    if not match:
        raise ValueError("Inbound AI Growth reply token was not found.")
    draft_id = int(match.group(1))
    token_id = match.group(2)
    token_row = db.execute(
        select(AiGrowthEmailActionToken).where(
            AiGrowthEmailActionToken.token_id == token_id,
            AiGrowthEmailActionToken.draft_id == draft_id,
            AiGrowthEmailActionToken.action == "reply",
        )
    ).scalar_one_or_none()
    if not token_row or token_row.used_at:
        raise ValueError("Inbound AI Growth reply token is invalid or already used.")
    expires_at = token_row.expires_at if token_row.expires_at.tzinfo else token_row.expires_at.replace(tzinfo=timezone.utc)
    if datetime.now(timezone.utc) > expires_at:
        raise ValueError("Inbound AI Growth reply token expired.")
    instruction = str(payload.get("StrippedTextReply") or payload.get("TextBody") or "").strip()
    if not instruction:
        raise ValueError("Inbound AI Growth reply did not include revision instructions.")
    draft = db.get(AiMarketingOpportunity, draft_id)
    if not draft:
        raise ValueError("Draft not found.")
    token_row.used_at = datetime.now(timezone.utc)
    token_row.result = "reply_regeneration_requested"
    db.commit()
    result = regenerate_growth_draft(db, draft, change_request=instruction[:1000])
    send_draft_email(db, draft)
    return {"status": "regenerated", "draft": result}


def preview_digest(
    db: Session,
    *,
    opportunity_ids: list[int] | None = None,
    statuses: list[str] | None = None,
    limit: int = 25,
    to_email: str | None = None,
) -> dict[str, Any]:
    opportunities = _digest_opportunities(db, opportunity_ids=opportunity_ids, statuses=statuses, limit=limit)
    latest = latest_suggestions_by_opportunity(db, [row.id for row in opportunities])
    context = _digest_context(db, opportunities, latest)
    return {
        "to_email": to_email or ai_growth_recipient(),
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
    to_email: str | None = None,
) -> dict[str, Any]:
    opportunities = _digest_opportunities(db, opportunity_ids=opportunity_ids, statuses=statuses, limit=limit)
    latest = latest_suggestions_by_opportunity(db, [row.id for row in opportunities])
    context = _digest_context(db, opportunities, latest)
    recipient = to_email or ai_growth_recipient()
    reply_to = reply_to_address_for_draft(db, opportunities[0].id, actor_email=recipient) if len(opportunities) == 1 else None
    result = send_email(
        db,
        to_email=recipient,
        template_key=AI_MARKETING_TEMPLATE_KEY,
        context=context,
        user_id=admin_user_id,
        category="admin_ai_marketing",
        idempotency_key=None,
        reply_to=reply_to,
        attachments=_email_asset_attachments(opportunities, latest),
    )
    status = str(result.get("status") or "queued")
    sent_at = datetime.now(timezone.utc) if status == "sent" else None
    log = AiMarketingEmailLog(
        delivery_id=result.get("id") if isinstance(result.get("id"), int) else None,
        to_email=recipient,
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
    to_email: str | None = None,
) -> dict[str, Any]:
    return send_digest(db, opportunity_ids=[opportunity.id], statuses=None, limit=1, admin_user_id=admin_user_id, to_email=to_email)


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


def clear_ai_growth_draft_history(db: Session) -> dict[str, Any]:
    rows = db.execute(
        select(AiMarketingOpportunity).where(AiMarketingOpportunity.status != "dismissed")
    ).scalars().all()
    now = datetime.now(timezone.utc)
    for row in rows:
        row.status = "dismissed"
        row.updated_at = now
    db.commit()
    return {"ok": True, "cleared": len(rows)}


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
    return OpenAIWebSearchProvider(api_key=api_key, model=marketing_model(db), db=db)


class WebSearchProvider:
    provider_name = "web_search"

    def search(self, query: str, *, max_results: int, recency: str = "week") -> list[WebSearchResult]:
        raise NotImplementedError


class OpenAIWebSearchProvider(WebSearchProvider):
    provider_name = "openai_web_search"
    endpoint = "https://api.openai.com/v1/responses"

    def __init__(self, *, api_key: str, model: str, db: Session | None = None) -> None:
        self.api_key = api_key
        self.model = model
        self.db = db

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
        _record_openai_usage_cost(
            self.db,
            model=self.model,
            data=data,
            feature="openai_web_search",
            web_search_calls=1,
            commit=self.db is not None,
        )
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
        market_context_cache: dict[str, list[dict[str, str]]] = {}
        discovered_at = datetime.now(timezone.utc)
        recency = campaign.recency or "week"
        campaign_tickers = _normalized_tickers(_load_list(campaign.tickers_json))
        content_type = _normalize_content_type(campaign.content_type, campaign_type=campaign.campaign_type, platform="reddit")
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
                ticker_text = " ".join(part for part in (result.title, snippet, normalized_url) if part)
                result_tickers = _dedupe_strings([*_matched_tickers(ticker_text, campaign_tickers), *campaign_tickers])[:5]
                web_market_context: list[dict[str, str]] = []
                if content_type == "reddit_thread":
                    web_market_context = self._market_context_for_tickers(
                        provider,
                        result_tickers,
                        recency=recency,
                        cache=market_context_cache,
                    )
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
                            "article_tickers": result_tickers,
                            "web_market_context": web_market_context,
                            "snippet_character_count": len(snippet or ""),
                            "needs_manual_review": needs_manual_review,
                            "manual_review_reason": "short search-provider snippet" if needs_manual_review else None,
                            "discovered_at": _iso(discovered_at),
                            "stored_fields": ["title", "url", "snippet", "source/provider", "discovered_at", "web_market_context"],
                            "compliance": "Search-provider snippets and URLs only; Reddit page HTML was not fetched.",
                        },
                    )
                )
                if len(items) >= max_items:
                    break
        return items

    @staticmethod
    def _market_context_for_tickers(
        provider: WebSearchProvider,
        tickers: list[str],
        *,
        recency: str,
        cache: dict[str, list[dict[str, str]]],
    ) -> list[dict[str, str]]:
        context: list[dict[str, str]] = []
        for ticker in _normalized_tickers(tickers)[:3]:
            if ticker not in cache:
                query = (
                    f"{ticker} stock earnings product business risks catalysts fundamentals "
                    "latest filings analyst investor presentation"
                )
                try:
                    rows = provider.search(query, max_results=4, recency=recency)
                except Exception:
                    rows = []
                cache[ticker] = [
                    {
                        "ticker": ticker,
                        "title": _truncate(row.title, 240),
                        "url": _truncate(row.url, 800),
                        "snippet": _truncate(row.snippet, 500),
                        "provider": row.provider,
                    }
                    for row in rows
                    if row.url
                ][:4]
            context.extend(cache[ticker])
        return context[:10]

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
        if opportunities[0].campaign_type == ARTICLE_REACTIVE_CAMPAIGN_TYPE:
            metadata = _load_object(opportunities[0].raw_metadata_json)
            tickers = _load_list(opportunities[0].matched_tickers_json)
            theme = ", ".join([*(f"${ticker}" for ticker in tickers[:2]), *(_coerce_json_list(metadata.get("themes"))[:2])])
            return f"Walnut AI Growth: Article-reactive X draft ready - {theme or 'market theme'}"
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
    return "Human review required before approval/posting."


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
    parts = ["<div style=\"margin:10px 0;color:#334155;\"><strong>Assets attached</strong>"]
    for asset in assets:
        title = html.escape(str(asset.get("title") or "Asset"))
        url = html.escape(str(asset.get("url") or asset.get("thumbnail_url") or ""), quote=True)
        caption = html.escape(str(asset.get("suggested_caption") or ""))
        if _asset_data_uri(asset):
            link = f"{title} <span style=\"color:#64748b;\">(attached image file)</span>"
        else:
            link = f"<a href=\"{url}\">{title}</a>" if url else title
        parts.append(f"<p style=\"margin:8px 0;\">{link}<br><span>{caption}</span></p>")
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
    db: Session,
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
        article_title = str(metadata.get("article_title") or "").strip()
        article_source = str(metadata.get("article_source") or opportunity.source_provider or "").strip()
        article_url = str(metadata.get("article_url") or opportunity.source_url or "").strip()
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
        compliance = (suggestion.compliance_notes if suggestion else opportunity.compliance_notes) or "Human review required before approval/posting."
        assets = _normalize_assets(_load_json_list(opportunity.asset_refs_json) + (_load_json_list(suggestion.assets_json) if suggestion else []))
        posting_links = _posting_links(opportunity, suggestion=suggestion)
        x_status = x_account_status(db)
        approve_posts_to_x = content_type == "x_post" and bool(x_status.get("connected"))
        approve_label = "Approve & Post to X" if approve_posts_to_x else "Approve"
        approve_behavior_text = (
            "Approve behavior: posts directly to X via the configured X API token."
            if approve_posts_to_x
            else "Approve behavior: saves approval in Walnut; no X post will be created until X_ACCESS_TOKEN with tweet.write scope is configured."
            if content_type == "x_post"
            else "Approve behavior: saves approval in Walnut."
        )
        alternate_versions = _load_object(opportunity.alternate_versions_json)
        short_version = str(alternate_versions.get("short_version") or "").strip()
        direct_version = str(alternate_versions.get("more_direct_version") or alternate_versions.get("alternate_reply_more_direct") or "").strip()
        hashtag_block = str(alternate_versions.get("copy_hashtags_cashtags") or "").strip()
        admin_url = _draft_admin_url(opportunity.id)
        approve_url = email_action_url(db, opportunity.id, "approve", actor_email=ai_growth_recipient())
        reject_url = email_action_url(db, opportunity.id, "reject", actor_email=ai_growth_recipient())
        reject_regenerate_url = email_action_url(db, opportunity.id, "reject_and_regenerate", actor_email=ai_growth_recipient())
        destination_html = (
            f"<p style=\"margin:0 0 8px 0;\"><a href=\"{html.escape(destination, quote=True)}\">Suggested Walnut link</a></p>"
            if destination
            else "<p style=\"margin:0 0 8px 0;color:#334155;\">Suggested Walnut link: none</p>"
        )
        source_article_html = ""
        article_button_html = ""
        if opportunity.campaign_type == ARTICLE_REACTIVE_CAMPAIGN_TYPE and (article_url or opportunity.source_url):
            article_link_url = article_url or opportunity.source_url
            source_article_html = (
                f"<p style=\"margin:0 0 8px 0;color:#334155;\"><strong>Source article:</strong> "
                f"{html.escape(article_title or opportunity.title)}"
                f"{' from ' + html.escape(article_source) if article_source else ''}</p>"
            )
            article_button_html = (
                f"<a href=\"{html.escape(article_link_url, quote=True)}\" "
                "style=\"display:inline-block;padding:10px 12px;background:#14b8a6;color:#0f172a;text-decoration:none;border-radius:6px;font-weight:700;\">Open Article</a>"
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
            "Click Approve & Post to X" if approve_posts_to_x else "Paste into platform",
            "Attach image if relevant",
            "Review disclosure",
            "Confirm posted status" if approve_posts_to_x else "Post manually",
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
                    f"Article title: {article_title or opportunity.title}",
                    f"Article source: {article_source or 'unknown'}",
                    f"Article URL: {article_url or opportunity.source_url}",
                    f"Suggested destination URL: {destination or 'none'}",
                    f"Open in Walnut Admin: {admin_url}",
                    f"Open X: {posting_links.get('open_x') or 'none'}",
                    f"Open X Compose: {posting_links.get('open_x_compose') or 'none'}",
                    f"Snippet: {snippet}",
                    f"Search query: {query}",
                    f"Matched ticker/keywords: {tickers} / {keywords}",
                    manual_review_note,
                    f"Content angle: {angle}",
                    "Draft content:",
                    draft_content,
                    f"Short version: {short_version or 'none'}",
                    f"Direct version: {direct_version or 'none'}",
                    f"Suggested hashtags/cashtags: {hashtag_block or tickers}",
                    f"Approve: {approve_url}",
                    approve_behavior_text,
                    f"Reject: {reject_url}",
                    f"Reject + Regenerate: {reject_regenerate_url}",
                    "Copy-ready markdown:",
                    opportunity.full_markdown or draft_content,
                    "Reply instructions: Reply to this email with edits like 'make it sharper,' 'shorten it,' or 'focus more on the HBM angle.' Walnut will regenerate the draft and email you a revised version.",
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
            f"<p style=\"margin:0 0 8px 0;color:#334155;\">Article source: {html.escape(article_source or 'unknown')}</p>"
            f"<p style=\"margin:0 0 8px 0;\"><a href=\"{html.escape(article_url or opportunity.source_url, quote=True)}\">Open article</a></p>"
            f"<p style=\"margin:0 0 8px 0;\"><a href=\"{html.escape(str(posting_links.get('open_x') or 'https://x.com/home'), quote=True)}\">Open X</a> | <a href=\"{html.escape(str(posting_links.get('open_x_compose') or 'https://x.com/compose/post'), quote=True)}\">Open X Compose</a></p>"
            f"<p style=\"margin:0 0 8px 0;color:#334155;\">Snippet: {html.escape(snippet)}</p>"
            f"<p style=\"margin:0 0 8px 0;color:#334155;\">Search query: {html.escape(query)}</p>"
            f"<p style=\"margin:0 0 8px 0;color:#334155;\">Matched ticker/keywords: {html.escape(tickers)} / {html.escape(keywords)}</p>"
            f"<p style=\"margin:0 0 8px 0;color:#334155;\">Content angle: {html.escape(str(angle))}</p>"
            f"{manual_review_html}"
            f"{destination_html}"
            f"{source_article_html}"
            f"<pre style=\"white-space:pre-wrap;margin:10px 0;padding:12px;background:#0f172a;color:#e2e8f0;border-radius:6px;font-size:13px;line-height:18px;\">{html.escape(draft_content)}</pre>"
            f"<p style=\"margin:0 0 8px 0;color:#334155;\"><strong>Alternate versions:</strong> short={html.escape(short_version or 'none')} | direct={html.escape(direct_version or 'none')}</p>"
            f"<p style=\"margin:0 0 8px 0;color:#334155;\"><strong>Hashtags/cashtags:</strong> {html.escape(hashtag_block or tickers)}</p>"
            "<div style=\"margin:12px 0;display:flex;flex-wrap:wrap;gap:8px;\">"
            f"{article_button_html}"
            f"<a href=\"{html.escape(approve_url, quote=True)}\" style=\"display:inline-block;padding:10px 12px;background:#e2e8f0;color:#0f172a;text-decoration:none;border-radius:6px;font-weight:700;\">{html.escape(approve_label)}</a>"
            f"<a href=\"{html.escape(reject_url, quote=True)}\" style=\"display:inline-block;padding:10px 12px;background:#475569;color:#ffffff;text-decoration:none;border-radius:6px;font-weight:700;\">Reject</a>"
            f"<a href=\"{html.escape(reject_regenerate_url, quote=True)}\" style=\"display:inline-block;padding:10px 12px;background:#f59e0b;color:#111827;text-decoration:none;border-radius:6px;font-weight:700;\">Reject + Regenerate</a>"
            f"<a href=\"{html.escape(admin_url, quote=True)}\" style=\"display:inline-block;padding:10px 12px;background:#0f172a;color:#ffffff;text-decoration:none;border-radius:6px;font-weight:700;\">Open Draft in Walnut Admin</a>"
            f"<a href=\"{html.escape(str(posting_links.get('open_x_compose') or 'https://x.com/compose/post'), quote=True)}\" style=\"display:inline-block;padding:10px 12px;background:#0f172a;color:#ffffff;text-decoration:none;border-radius:6px;font-weight:700;\">Open X Compose</a>"
            "</div>"
            f"<p style=\"margin:0 0 6px 0;color:#334155;\"><strong>Copy-ready markdown</strong></p>"
            f"<pre style=\"white-space:pre-wrap;margin:6px 0 10px 0;padding:12px;background:#0b1120;color:#d1fae5;border-radius:6px;font-size:13px;line-height:18px;\">{html.escape(opportunity.full_markdown or draft_content)}</pre>"
            "<p style=\"margin:0 0 8px 0;color:#334155;\"><strong>Reply instructions:</strong> Reply to this email with edits like 'make it sharper,' 'shorten it,' or 'focus more on the HBM angle.' Walnut will regenerate the draft and email you a revised version.</p>"
            f"<p style=\"margin:0 0 8px 0;color:#334155;\"><strong>Disclosure reminder:</strong> {html.escape(disclosure or _default_disclosure_reminder(source_platform, draft_content))}</p>"
            f"<p style=\"margin:0 0 8px 0;color:#334155;\"><strong>Compliance:</strong> {html.escape(compliance)}</p>"
            f"<p style=\"margin:0 0 8px 0;color:#334155;\"><strong>Approve behavior:</strong> {html.escape(approve_behavior_text)}</p>"
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
        "summary": f"{count} AI Growth draft{'s' if count != 1 else ''} ready for human review.",
        "items_text": "\n\n".join(items_text) if items_text else "No matching opportunities are ready for digest.",
        "items_html": "".join(items_html) if items_html else "<p>No matching opportunities are ready for digest.</p>",
        "digest_url": "https://walnutmarkets.com/admin/ai-marketing",
    }


def _suggestion_system_prompt(db: Session | None = None) -> str:
    configured_tone = ai_growth_email_tone(db)
    characteristics = ai_growth_voice_characteristics(db)
    return (
        "You draft human-reviewed AI Growth Engine assets for Walnut Market Terminal. "
        f"Default email and campaign tone: {configured_tone}. "
        f"Saved Walnut voice characteristics to apply consistently: {characteristics}. "
        f"Saved SEO/search keyword guidance to apply consistently: {AI_GROWTH_SEO_KEYWORD_GUIDANCE}. "
        "Return only JSON matching the supplied schema. First decide whether the thread deserves a reply at all. "
        "Use recommended_action='skip' when the source is not clearly about investing, markets, public companies, trading, finance, or research tools. "
        "Use recommended_action='skip' or 'monitor' when Walnut cannot add a meaningful, specific angle. "
        "For skip, suggested_reply should be exactly or very close to: 'Skip - not relevant enough.' "
        "For monitor, explain what would make the thread worth replying to later. "
        "If opportunity.metadata.change_request is present, treat it as the highest-priority revision instruction while preserving compliance. "
        "For campaign_type='article_reactive_x', use the article only as a trigger and source reference. Do not summarize or repost it. "
        "Use this posting formula for article-reactive X: assess the situation, identify the issues, analyze the data, then conclude. "
        "Use data, underlying data, or data sources as the public-facing language; do not use stack as shorthand in public X copy. "
        "Keep this distinction clear: confirmation score is Walnut's proprietary score, while underlying data means price/volume, fundamentals, reported institutional activity, Congress/insider activity, contracts, technicals, and other cited evidence. "
        "Double-check numbers and dates against the provided context before using them; do not use stale figures, and say what freshness is missing when the context does not establish recency. "
        "For all X campaign types, including manual X drafts, scheduled X campaigns, article-reactive X, and X reply campaigns, apply the saved Walnut voice characteristics while staying market-native and terse. "
        "Draft X posts like high-signal market tape: one factual hook, one sourced stat or event, and Walnut-native context only when it adds something concrete. "
        "Prefer this shape: '[Ticker/company/stat/event], per [source].' Add a second sentence only for Walnut signal context, a material caveat, or a data-backed why-it-matters line. "
        "Use BREAKING only when the source event is truly breaking. Prefer cashtags over hashtags and avoid hashtags by default. "
        "Do not force first-person plural; use 'we' or 'our' only when describing Walnut's own signal process. Do not add promotional CTA language to X posts. "
        "High-quality X output should pair concise analysis with a real Walnut-generated thumbnail: a premium finance-media visual, not a dashboard screenshot or generic chart card. "
        "For x_post and reddit_thread, fill social_card as compact art direction for a generated thumbnail, not final post copy and not a text-heavy layout. "
        "The social_card should describe one scroll-stopping finance-media visual idea with a short hook, primary ticker, tone, and visual emphasis. Avoid source/footer text, bullets, evidence panels, cramped UI, charts as the main design, and long copy. "
        "Thumbnail headlines must be complete market statements, not fragments. For example, use 'Bearish trend confirmed' or 'Bearish signal identified' rather than vague lines like 'bearish confirmation is leading'. "
        "Every bullish or bearish thumbnail claim must name the underlying data behind it: price/volume, fundamentals, reported institutional activity, Congress/insider activity, contracts, technicals, macro, or other cited evidence. "
        "Use card_type='article_reactive' for news/article reactions, 'ticker_signal' for ticker confirmation or data-source views, 'congress_insider_activity' for Congress or insider transactions, and 'research_cover' for Reddit/DD covers. "
        "Keep thumbnail art direction simple enough for a 16:9 image: official Walnut logo area, ticker, one large visual metaphor, generous negative space, no source line, no generic hype, and no invented numbers. "
        "Respect opportunity.metadata.social_card_preferences for template, tone, chart, CTA, source tag, and Walnut URL inclusion. "
        "For x_post, always fill visual_brief with a chart-ready concept: title, chart_type, metric_label, 3-8 rows, and source_note. "
        "Only use numeric values when they are present in the provided context; otherwise use qualitative buckets and say what data is missing. "
        "For x_post, do not tell readers to 'cross-check', 'review', or 'check' ticker pages. State what the data says, explain the takeaway or limitation, then provide the relevant ticker link for more info. "
        "For bullish/bearish confirmation X posts, use opportunity.metadata.walnut_context.source_stack as underlying data when present. Name the active data sources directly, especially Price / Volume, Fundamentals, reported Institutional Activity, Congress/Insider Activity, Contracts, Technicals, and Macro Positioning, and include the confirmation score when supplied. "
        "Do not make buy/sell recommendations, price targets unless clearly sourced and framed, or unsupported factual claims. "
        "Do not reuse article thumbnails; any image should be a Walnut-branded original generated thumbnail. Keep source attribution in metadata/copy, not as visible thumbnail text. "
        f"For x_post, write suggested_post plus alternate_hooks and make value_added_insight explain the analysis behind the visual. Keep suggested_post at or under {X_POST_CHARACTER_LIMIT} characters, including links and cashtags. "
        "For x_post published from @WalnutMarkets, do not include self-disclosure like 'bias disclosed' or 'I'm building Walnut'; the account identity is already clear. "
        "For x_post, include relevant cashtags such as $NVDA or $JPM when useful. Do not append generic hashtags like #Markets. "
        "For x_reply, produce suggested_reply as a concise reply to the source post, not a standalone post. Mimic the strongest Walnut replies: one or two lines, direct market judgment, anchored to the original author's point, and no generic product pitch. "
        "For x_reply, do not add self-disclosure, do not say 'cross-check this on Walnut', and do not force a Walnut link unless the reply names a specific ticker where the link adds useful context. "
        "For x_reply, if the original post invites a one-word answer, one word is acceptable; otherwise avoid generic one-word replies. "
        "For reddit_thread, write a serious, comprehensive Reddit-native DD post, not a promotional summary. "
        "Use the Reddit/search result only as the discovery hook, then combine opportunity.metadata.web_market_context "
        "with opportunity.metadata.walnut_context to build the thesis. "
        "The web_market_context may include product, earnings, filings, risks, catalysts, and current public-news snippets. "
        "The walnut_context may include ticker pages, confirmation signals, Congress disclosures, insider disclosures, "
        "institutional reported activity, government contracts, watchlists, saved screens, and ticker metadata. "
        "Include concrete tickers and Walnut ticker links when available. "
        "If a section lacks enough evidence, say what is missing in missing_data_notes rather than inventing details. "
        "A Reddit research thread must include: Title, TL;DR, Why this name came up, Company snapshot, Walnut disclosure data, "
        "Technical picture, Fundamental picture, Recent news / filings / press releases, Catalysts, Bull case, Bear case / risks, "
        "What would confirm the setup, What would weaken the setup, Bottom line, and Suggested Reddit disclosure. "
        "For reddit_thread, fill the dedicated structured fields and full_reddit_post_markdown; include source_notes, missing_data_notes, "
        "quality_scores, suggested_image_asset, suggested_flair, and suggested_subreddits where appropriate. "
        "For paid_ad, write suggested_ad_variants as native paid ad headline/body/CTA variants. "
        "Walnut Market Terminal is a professional-grade market intelligence platform for sophisticated retail investors. "
        "It helps users find market tells by combining ticker context, price/volume confirmation, financials and filings, insider activity, "
        "Congress trading disclosures, government contracts, signal conviction, screener workflows, and evidence trail or why-now context. "
        "The brand idea is: 'The market has tells. We help find them.' Do not describe Walnut as a casual stock app. "
        "When replying, lead with useful insight specific to the thread, then add nuance, then mention Walnut only if it has a strong natural angle. "
        "Sound like a sharp market participant and excellent founder/salesperson, while staying concise, helpful, and non-spammy. "
        "Explain Walnut concretely when mentioned; vague phrases like 'compare drivers in one place' are not enough. "
        "If the source mentions a specific public company or ticker, prefer the ticker page /ticker/{SYMBOL}. "
        "If it discusses screeners or research tooling without a ticker, prefer /screener or the homepage. "
        "If it discusses Congress trades, insider buying, or government contracts, use that as the reply_angle when relevant. "
        "Use reported/disclosed/filed/filing-date language for Congress, insider, and institutional disclosure data. "
        "For 13F or institutional activity, never imply live buying or exact trade dates; say reported holdings/activity, quarter-end holdings, and filing date context. "
        "Do not imply endorsement by Reddit, X, Facebook, Congress, SEC, or any data provider. "
        "If organic Reddit content mentions Walnut, disclose affiliation naturally, for example: \"I'm building Walnut, so obvious bias...\" "
        "Do not make investment advice claims, tell users to buy, sell, or short a security, guarantee returns, or use hype like 'about to explode'. "
        "Prefer educational language such as \"this may be useful\", \"you can cross-check\", and \"one way to look at it\". "
        "Include at most one Walnut link unless the thread clearly needs more. Never use spammy CTA language. "
        "Make Reddit research threads valuable even if nobody clicks Walnut; include technical context, fundamental context, catalysts, risks, and limitations. "
        "Do not fake personal experience or pretend to be unaffiliated. No automated posting is happening; a human will review and manually post if appropriate. "
        "Avoid replies when spam risk is high. "
        "Avoid replying to old or inactive threads unless relevance is very high. "
        "For source_provider='web_search_reddit', you only have a search-provider title, URL, and snippet. "
        "Do not invent unseen Reddit post or comment details. For reddit replies, if the snippet is thin, vague, or missing, use recommended_action='monitor' "
        "and make suggested_reply start with 'Needs manual review -' instead of drafting a full reply. "
        "For reddit_thread, a thin snippet can still be a discovery seed when ticker, web_market_context, or walnut_context is strong; "
        "write the DD from those evidence fields and call out Reddit-specific gaps in missing_data_notes."
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
            "suggested_post": {"type": "string", "maxLength": X_POST_CHARACTER_LIMIT},
            "suggested_ad_variants": {"type": "array", "items": {"type": "string"}},
            "visual_brief": {
                "type": "object",
                "properties": {
                    "title": {"type": "string"},
                    "chart_type": {"type": "string", "enum": ["ranked_bars", "bucket_breakdown", "data_sources", "comparison_card"]},
                    "metric_label": {"type": "string"},
                    "rows": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "label": {"type": "string"},
                                "value": {"type": "string"},
                                "note": {"type": "string"},
                            },
                            "required": ["label", "value", "note"],
                            "additionalProperties": False,
                        },
                    },
                    "source_note": {"type": "string"},
                    "missing_data_note": {"type": "string"},
                },
                "required": ["title", "chart_type", "metric_label", "rows", "source_note", "missing_data_note"],
                "additionalProperties": False,
            },
            "social_card": _social_card_json_schema(),
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
            "alternate_hooks": {"type": "array", "items": {"type": "string", "maxLength": X_POST_CHARACTER_LIMIT}},
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
            "visual_brief",
            "social_card",
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
    openai_api_key: str | None = None,
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
    elif content_type == "x_reply":
        disclosure_text = ""
        suggested_reply = _fit_x_post_text(_strip_x_self_disclosure(suggested_reply or "No safe reply suggested."))
        alternate_reply = _fit_x_post_text(_strip_x_self_disclosure(alternate_reply)) if alternate_reply else ""
    else:
        suggested_reply = _ensure_walnut_affiliation_disclosure(suggested_reply or "No safe reply suggested.")
        alternate_reply = _ensure_walnut_affiliation_disclosure(alternate_reply) if alternate_reply else ""
        suggested_post = _ensure_walnut_affiliation_disclosure(suggested_post) if content_type == "reddit_thread" else suggested_post
    if content_type == "x_post":
        disclosure_text = ""
        suggested_post = _ensure_x_hashtags(_strip_x_self_disclosure(suggested_post), detected_tickers)
        alternate_hooks = [_ensure_x_hashtags(_strip_x_self_disclosure(item), detected_tickers) for item in alternate_hooks]
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
    if content_type in {"x_post", "reddit_thread"}:
        preferences = _social_card_preferences(opportunity_metadata)
        fallback_card_type = _social_card_type_for_context(campaign_type, content_type, preferences)
        card_spec = _normalize_social_card_spec(
            payload.get("social_card"),
            fallback_card_type=fallback_card_type,
            fallback_tickers=detected_tickers,
            fallback_url=destination or destination_hint,
            preferences=preferences,
            visual_brief=payload.get("visual_brief"),
        )
        generated_asset = (
            _generated_thumbnail_asset(
                api_key=openai_api_key,
                card_spec=card_spec,
                suggested_post=suggested_post,
                visual_brief=payload.get("visual_brief"),
            )
            if openai_api_key
            else None
        )
        generated_assets = [generated_asset] if generated_asset else []
        assets = _normalize_assets([*generated_assets, *assets])
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
        "short_version": alternate_hooks[0] if alternate_hooks else "",
        "more_direct_version": alternate_reply or (alternate_hooks[1] if len(alternate_hooks) > 1 else ""),
        "copy_hashtags_cashtags": " ".join([*(f"${ticker}" for ticker in detected_tickers[:4])]),
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
        ("Walnut disclosure data", "walnut_disclosure_stack"),
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
    default_values = {
        AI_MARKETING_MODEL: DEFAULT_AI_MARKETING_MODEL,
        AI_GROWTH_EMAIL_TONE: DEFAULT_AI_GROWTH_EMAIL_TONE,
        AI_GROWTH_VOICE_CHARACTERISTICS: DEFAULT_AI_GROWTH_VOICE_CHARACTERISTICS,
    }
    source = "server_env" if value else "default" if key in default_values else "missing"
    configured = bool(value) or key in default_values
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
        payload["value"] = value or default_values.get(key, "")
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


def _matched_tickers(text: str, campaign_tickers: list[str], *, require_cashtag: bool = False) -> list[str]:
    detected = set()
    pattern = _CASHTAG_TICKER_PATTERN if require_cashtag else _TICKER_PATTERN
    for match in pattern.finditer(text):
        raw = match.group(0)
        ticker = match.group(1).upper()
        if ticker in _COMMON_FALSE_TICKERS:
            continue
        if require_cashtag or raw.startswith("$") or match.group(1).isupper():
            detected.add(ticker)
    campaign = set(_normalized_tickers(campaign_tickers))
    upper_text = text.upper()
    if campaign and not require_cashtag:
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
    seen: set[tuple[str, str, str]] = set()
    allowed_types = {"image", "chart", "csv", "pdf", "screenshot", "report"}
    for raw in raw_items:
        if not isinstance(raw, dict):
            continue
        raw_asset_url = str(raw.get("url") or raw.get("path") or raw.get("reference") or "").strip()
        raw_thumbnail_url = str(raw.get("thumbnail_url") or "").strip()
        asset_url = _truncate(raw_asset_url, 8_000_000 if raw_asset_url.startswith("data:image/") else 1200) or ""
        thumbnail_url = _truncate(raw_thumbnail_url, 8_000_000 if raw_thumbnail_url.startswith("data:image/") else 1200) or ""
        asset_type = str(raw.get("asset_type") or "image").strip().lower()
        if asset_type not in allowed_types:
            asset_type = "image"
        if asset_type in {"image", "chart", "screenshot"}:
            if asset_url and not _is_media_asset_url(asset_url):
                asset_url = ""
            if thumbnail_url and not _is_media_asset_url(thumbnail_url):
                thumbnail_url = ""
        if not asset_url and not thumbnail_url:
            continue
        title = _truncate(str(raw.get("title") or asset_type.title()).strip(), 200) or asset_type.title()
        dedupe_key = (asset_type, asset_url or thumbnail_url, title.lower())
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        assets.append(
            {
                "title": title,
                "asset_type": asset_type,
                "url": asset_url,
                "thumbnail_url": thumbnail_url,
                "suggested_caption": _truncate(str(raw.get("suggested_caption") or "").strip(), 1000) or "",
                "source_data_notes": _truncate(str(raw.get("source_data_notes") or "").strip(), 1000) or "",
                "template": _truncate(str(raw.get("template") or "").strip(), 80) or "",
                "card_type": _truncate(str(raw.get("card_type") or "").strip(), 80) or "",
                "tone": _truncate(str(raw.get("tone") or "").strip(), 80) or "",
                "card_spec": raw.get("card_spec") if isinstance(raw.get("card_spec"), dict) else {},
                "image_prompt": _truncate(str(raw.get("image_prompt") or "").strip(), 4000) or "",
                "image_revised_prompt": _truncate(str(raw.get("image_revised_prompt") or "").strip(), 4000) or "",
                "image_model": _truncate(str(raw.get("image_model") or "").strip(), 80) or "",
                "width": _clamp_int(raw.get("width") or 0, 0, 4000),
                "height": _clamp_int(raw.get("height") or 0, 0, 4000),
            }
        )
    return assets[:10]


def _asset_data_uri(asset: dict[str, Any]) -> str:
    for key in ("url", "thumbnail_url"):
        value = str(asset.get(key) or "").strip()
        if value.lower().startswith("data:image/"):
            return value
    return ""


def _decode_data_uri_asset(asset: dict[str, Any], *, fallback_name: str | None = None) -> dict[str, Any] | None:
    data_uri = _asset_data_uri(asset)
    if not data_uri or "," not in data_uri:
        return None
    header, payload = data_uri.split(",", 1)
    if not header.lower().startswith("data:"):
        return None
    media_type = header[5:].split(";", 1)[0].strip().lower() or "application/octet-stream"
    if not media_type.startswith("image/"):
        return None
    try:
        content = base64.b64decode(payload, validate=True) if ";base64" in header.lower() else unquote_to_bytes(payload)
    except Exception:
        return None
    if not content:
        return None
    if media_type == "image/svg+xml":
        png_content = _svg_bytes_to_png(content)
        if not png_content:
            return None
        content = png_content
        media_type = "image/png"
    title = fallback_name or str(asset.get("title") or "walnut-asset")
    extension = _image_extension_for_media_type(media_type)
    filename = _safe_asset_filename(title, extension)
    return {"name": filename, "content": content, "content_type": media_type}


def _svg_bytes_to_png(content: bytes) -> bytes | None:
    try:
        from PIL import Image, ImageColor, ImageDraw, ImageFont
    except Exception:
        logger.warning("Pillow is unavailable; AI Growth SVG asset will not be converted to PNG.")
        return None
    try:
        root = ElementTree.fromstring(content.decode("utf-8", errors="replace"))
    except Exception:
        logger.warning("Unable to parse AI Growth SVG asset for PNG conversion.")
        return None
    width, height = _svg_dimensions(root)
    if width <= 0 or height <= 0:
        return None
    image = Image.new("RGBA", (width, height), (6, 20, 23, 255))
    draw = ImageDraw.Draw(image)
    for element in root.iter():
        tag = _svg_local_name(element.tag)
        if tag == "rect":
            fill = _svg_color(element.get("fill"), default=(0, 0, 0, 0), image_color=ImageColor)
            stroke = _svg_color(element.get("stroke"), default=None, image_color=ImageColor)
            opacity = _svg_float(element.get("opacity"), 1.0)
            if opacity < 1 and fill:
                fill = (*fill[:3], max(0, min(255, int(fill[3] * opacity))))
            x = _svg_float(element.get("x"), 0)
            y = _svg_float(element.get("y"), 0)
            rect_width = _svg_float(element.get("width"), width)
            rect_height = _svg_float(element.get("height"), height)
            radius = _svg_float(element.get("rx"), 0)
            stroke_width = max(1, int(_svg_float(element.get("stroke-width"), 1)))
            box = [x, y, x + rect_width, y + rect_height]
            if radius > 0:
                draw.rounded_rectangle(box, radius=radius, fill=fill, outline=stroke, width=stroke_width)
            else:
                draw.rectangle(box, fill=fill, outline=stroke, width=stroke_width)
        elif tag == "text":
            text_value = "".join(element.itertext()).strip()
            if not text_value:
                continue
            x = _svg_float(element.get("x"), 0)
            y = _svg_float(element.get("y"), 0)
            font_size = max(8, int(_svg_float(element.get("font-size"), 24)))
            is_bold = str(element.get("font-weight") or "").lower() in {"600", "700", "bold", "bolder"}
            font = _svg_font(font_size, is_bold=is_bold, image_font=ImageFont)
            fill = _svg_color(element.get("fill"), default=(248, 250, 252, 255), image_color=ImageColor)
            draw.text((x, y - font_size), text_value, fill=fill, font=font)
    output = BytesIO()
    image.convert("RGB").save(output, format="PNG", optimize=True)
    return output.getvalue()


def _svg_dimensions(root: ElementTree.Element) -> tuple[int, int]:
    width = _svg_float(root.get("width"), 0)
    height = _svg_float(root.get("height"), 0)
    if (width <= 0 or height <= 0) and root.get("viewBox"):
        parts = re.split(r"[\s,]+", str(root.get("viewBox") or "").strip())
        if len(parts) == 4:
            width = _svg_float(parts[2], width)
            height = _svg_float(parts[3], height)
    return max(1, int(width)), max(1, int(height))


def _svg_local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1].lower()


def _svg_float(value: Any, default: float) -> float:
    match = re.search(r"[-+]?\d+(?:\.\d+)?", str(value or ""))
    if not match:
        return default
    try:
        return float(match.group(0))
    except ValueError:
        return default


def _svg_color(value: Any, *, default: Any, image_color: Any) -> tuple[int, int, int, int] | None:
    raw = str(value or "").strip()
    if not raw:
        return default
    if raw.lower() == "none":
        return None
    try:
        parsed = image_color.getrgb(raw)
    except Exception:
        return default
    if len(parsed) == 4:
        return parsed
    return (*parsed, 255)


def _svg_font(size: int, *, is_bold: bool, image_font: Any) -> Any:
    font_names = (
        ("DejaVuSans-Bold.ttf", "arialbd.ttf") if is_bold else ("DejaVuSans.ttf", "arial.ttf")
    )
    for font_name in font_names:
        try:
            return image_font.truetype(font_name, size=size)
        except Exception:
            continue
    return image_font.load_default()


def _email_asset_attachments(
    opportunities: list[AiMarketingOpportunity],
    latest: dict[int, AiMarketingSuggestion],
) -> list[dict[str, Any]]:
    attachments: list[dict[str, Any]] = []
    seen: set[str] = set()
    for opportunity in opportunities:
        assets = _opportunity_assets(opportunity, suggestion=latest.get(opportunity.id))
        for index, asset in enumerate(assets):
            decoded = _decode_data_uri_asset(asset, fallback_name=f"{opportunity.title}-{index + 1}")
            if not decoded:
                continue
            key = f"{decoded['name']}:{len(decoded['content'])}"
            if key in seen:
                continue
            seen.add(key)
            attachments.append(decoded)
            if len(attachments) >= 10:
                return attachments
    return attachments


def _image_extension_for_media_type(media_type: str) -> str:
    normalized = media_type.lower().split(";", 1)[0]
    if normalized == "image/svg+xml":
        return "svg"
    if normalized == "image/jpeg":
        return "jpg"
    if normalized == "image/png":
        return "png"
    if normalized == "image/webp":
        return "webp"
    if normalized == "image/gif":
        return "gif"
    return "img"


def _safe_asset_filename(value: str, extension: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "-", str(value or "walnut-asset")).strip(".-")
    cleaned = cleaned or "walnut-asset"
    if not cleaned.lower().endswith(f".{extension.lower()}"):
        cleaned = f"{cleaned}.{extension}"
    return cleaned[:140]


def _is_media_asset_url(value: str | None) -> bool:
    url = str(value or "").strip()
    if not url:
        return False
    lower = url.split("?", 1)[0].lower()
    if lower.startswith("data:image/") or lower.startswith("blob:"):
        return True
    media_extensions = (".png", ".jpg", ".jpeg", ".webp", ".gif", ".svg")
    document_extensions = (".pdf", ".csv")
    return lower.endswith(media_extensions + document_extensions)


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


def _strip_x_self_disclosure(value: str | None) -> str:
    cleaned = re.sub(r"\s+", " ", str(value or "").strip())
    if not cleaned:
        return ""
    patterns = (
        r"^(?:i'm|i am)\s+building\s+walnut,\s*so\s+obvious\s+bias,\s*but\s+",
        r"^(?:bias disclosed:\s*)?(?:i'm|i am)\s+building\s+walnut(?:[^:\.\n]*[:\.])\s*",
        r"^bias disclosed:\s*",
    )
    for pattern in patterns:
        cleaned = re.sub(pattern, "", cleaned, count=1, flags=re.IGNORECASE).strip()
    return cleaned


def _ensure_x_hashtags(value: str | None, tickers: list[str]) -> str:
    cleaned = re.sub(r"\s+", " ", str(value or "").strip())
    cleaned = re.sub(r"(?<!\w)#[A-Za-z0-9_]+\b", "", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    if not cleaned:
        return cleaned
    existing_cashtags = {match.upper() for match in re.findall(r"\$([A-Za-z][A-Za-z0-9.\-]{0,9})", cleaned)}
    missing: list[str] = []
    for ticker in tickers[:2]:
        symbol = re.sub(r"[^A-Za-z0-9.\-]", "", str(ticker or "").upper())
        if not symbol or symbol in existing_cashtags:
            continue
        missing.append(symbol)
    for count in range(len(missing), 0, -1):
        suffix = " " + " ".join(f"${symbol}" for symbol in missing[:count])
        body_limit = max(40, X_POST_CHARACTER_LIMIT - len(suffix))
        candidate = f"{_fit_x_post_text(cleaned, limit=body_limit)}{suffix}"
        if len(candidate) <= X_POST_CHARACTER_LIMIT:
            return candidate
    return _fit_x_post_text(cleaned)


def _fit_x_post_text(value: str | None, *, limit: int = X_POST_CHARACTER_LIMIT) -> str:
    cleaned = re.sub(r"\s+", " ", str(value or "").strip())
    bounded_limit = max(4, min(limit, X_POST_CHARACTER_LIMIT))
    if len(cleaned) <= bounded_limit:
        return cleaned
    suffix = "..."
    max_body = bounded_limit - len(suffix)
    trimmed = cleaned[:max_body].rstrip()
    word_boundary = trimmed.rsplit(" ", 1)[0].rstrip(" ,;:-") if " " in trimmed else trimmed
    if len(word_boundary) >= min(160, max_body):
        trimmed = word_boundary
    return f"{trimmed.rstrip(' ,;:-')}{suffix}"[:bounded_limit]


def _iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.isoformat()
