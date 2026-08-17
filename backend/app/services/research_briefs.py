from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
import uuid
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests
from fastapi import HTTPException
from sqlalchemy import desc, func, select, text
from sqlalchemy.orm import Session

from app.db import SessionLocal
from app.models import Event, FundamentalsCache, GovernmentContract, QuoteCache, Security, TickerFinancialsCache, TickerMeta, UserAccount
from app.services.ai_marketing import (
    AI_MARKETING_IMAGE_GENERATION_ENABLED,
    AI_MARKETING_IMAGE_MODEL,
    AI_MARKETING_IMAGE_QUALITY,
    AI_MARKETING_IMAGE_SIZE,
    DEFAULT_AI_MARKETING_IMAGE_MODEL,
    DEFAULT_AI_MARKETING_IMAGE_QUALITY,
    DEFAULT_AI_MARKETING_IMAGE_SIZE,
    OPENAI_API_KEY,
    resolved_setting_value,
)
from app.services.confirmation_score import get_confirmation_score_bundles_for_tickers
from app.services.email_delivery import send_email
from app.utils.symbols import normalize_symbol

RESEARCH_BRIEF_PROMPT_VERSION = "research_brief_v3_search_intent_walnut_context"
RESEARCH_BRIEF_GENERATOR_MODEL = "RESEARCH_BRIEF_GENERATOR_MODEL"
RESEARCH_BRIEF_MODEL_DEFAULT = "RESEARCH_BRIEF_MODEL_DEFAULT"
RESEARCH_BRIEF_MODEL_OPTIONS = "RESEARCH_BRIEF_MODEL_OPTIONS"
RESPONSES_ENDPOINT = "https://api.openai.com/v1/responses"
IMAGES_ENDPOINT = "https://api.openai.com/v1/images/generations"
STORE_ENV = "RESEARCH_BRIEF_DRAFT_STORE_PATH"
MOCK_ENV = "RESEARCH_BRIEF_GENERATOR_MOCK"
DEFAULT_RESEARCH_BRIEF_MODEL = "gpt-5.6-terra"
DEFAULT_RESEARCH_BRIEF_MODEL_OPTIONS = ["gpt-5.6-luna", "gpt-5.6-terra", "gpt-5.6-sol"]
RESEARCH_BRIEF_MODEL_LABELS = {
    "gpt-5.6-luna": "GPT-5.6 Luna",
    "gpt-5.6-terra": "GPT-5.6 Terra",
    "gpt-5.6-sol": "GPT-5.6 Sol",
}
logger = logging.getLogger(__name__)
RESEARCH_BRIEF_JOB_SAFE_ERROR = "Research brief generation failed. Try again or reduce research depth."
RESEARCH_BRIEF_JOB_STALE_ERROR = "Research brief generation timed out. Please start a fresh draft."
RESEARCH_BRIEF_OPENAI_TIMEOUT_SECONDS = "RESEARCH_BRIEF_OPENAI_TIMEOUT_SECONDS"
RESEARCH_BRIEF_THUMBNAIL_TIMEOUT_SECONDS = "RESEARCH_BRIEF_THUMBNAIL_TIMEOUT_SECONDS"
RESEARCH_BRIEF_JOB_STALE_SECONDS = "RESEARCH_BRIEF_JOB_STALE_SECONDS"
RESEARCH_BRIEF_MODEL_DESCRIPTIONS = {
    "gpt-5.6-luna": "Fast / cheaper",
    "gpt-5.6-terra": "Balanced",
    "gpt-5.6-sol": "Deep research / highest quality",
}

ANGLE_OPTIONS = {
    "Full company DD",
    "Bull case",
    "Bear case",
    "Earnings setup",
    "Post-earnings review",
    "Momentum analysis",
    "Fundamental analysis",
    "Valuation analysis",
    "Technical setup",
    "Congress activity",
    "Insider activity",
    "Institutional activity",
    "Government contracts",
    "Macro or sector impact",
    "Peer comparison",
    "Custom",
}
TIME_HORIZON_OPTIONS = {"Near term", "3-6 months", "6-12 months", "Long term", "Custom"}
AUDIENCE_OPTIONS = {"General investors", "Active traders", "Long-term investors", "Professional / advanced", "Reddit DD", "Walnut Research Brief"}
JUDGMENT_OPTIONS = {"Let the data decide", "Bull case", "Bear case", "Balanced debate"}
LENGTH_OPTIONS = {"Short: 800-1,200 words", "Standard: 1,500-2,500 words", "Deep dive: 3,000-5,000 words"}
TONE_OPTIONS = {"Walnut market-native", "Institutional research", "Reddit DD", "Concise executive brief"}
EXTERNAL_RESEARCH_MODE_OPTIONS = {"Off", "Standard", "Deep"}
SECTION_FORMAT_OPTIONS = [
    "Walnut Research Brief",
    "Reddit DD - Issue / Risk / Data / Conclusion",
    "Reddit DD - Bull Case / Bear Case / The Data / The Call",
    "ValueInvesting - Business / Valuation / Risks / Margin of Safety",
    "X Thread",
    "Internal Analyst Note",
]
STATUS_OPTIONS = {"generating", "draft", "ready_for_review", "scheduled_review", "approved_scheduled", "published", "unpublished", "rejected", "failed"}
CAMPAIGN_ITEM_STATUS_OPTIONS = {"pending", "generating", "generated", "failed"}
RESEARCH_CAMPAIGN_REVIEW_TEMPLATE_KEY = "research_brief.scheduled_review"
RESEARCH_CAMPAIGN_DEFAULT_GENERATOR_VERSION = "research_campaign_v1"
RESEARCH_DAILY_PUBLISH_CAP = "RESEARCH_DAILY_PUBLISH_CAP"
RESEARCH_DAILY_PUBLISH_CAP_DEFAULT = 1
RESEARCH_CAMPAIGNS_SCHEDULE_ENABLED = "RESEARCH_CAMPAIGNS_SCHEDULE_ENABLED"
RESEARCH_KEYWORD_DISCOVERY_MODEL = "RESEARCH_KEYWORD_DISCOVERY_MODEL"
RESEARCH_KEYWORD_DISCOVERY_MAX_CANDIDATES = 8
RESEARCH_KEYWORD_OPPORTUNITY_STATUSES = {"new", "used", "dismissed"}
INDEX_STATUSES = {"indexed", "crawled_not_indexed", "discovered", "unknown"}
RESEARCH_CAMPAIGN_THEMES: list[dict[str, Any]] = [
    {"key": "good_buy_now", "label": "Good Buy Now", "content_type": "ticker", "intent": "Is [TICKER] a Good Stock to Buy Right Now?"},
    {"key": "why_is_it_moving", "label": "Why Is It Moving", "content_type": "ticker", "intent": "Why Is [TICKER] Stock Moving?"},
    {"key": "insider_activity", "label": "Insider Buying / Selling", "content_type": "ticker", "intent": "What does the latest insider activity mean for [TICKER]?"},
    {"key": "who_is_buying", "label": "Who Is Buying", "content_type": "ticker", "intent": "Who is buying [TICKER] stock?"},
    {"key": "institutional_ownership", "label": "Institutional Ownership", "content_type": "ticker", "intent": "Are institutions accumulating [TICKER]?"},
    {"key": "congress_activity", "label": "Congress Activity", "content_type": "ticker", "intent": "Which members of Congress are buying or selling [TICKER]?"},
    {"key": "government_contracts", "label": "Government Contracts", "content_type": "ticker", "intent": "How much government contract exposure does [TICKER] have?"},
    {"key": "analysts_vs_fundamentals", "label": "Analysts vs Fundamentals", "content_type": "ticker", "intent": "[TICKER] price targets vs fundamentals: does Wall Street's view hold up?"},
    {"key": "bullish_or_bearish", "label": "Bullish or Bearish", "content_type": "ticker", "intent": "Is [TICKER] bullish or bearish right now?"},
    {"key": "what_changed", "label": "What Changed", "content_type": "ticker", "intent": "What changed in [TICKER] stock?"},
    {"key": "insider_purchases_predict_returns", "label": "Do Insider Purchases Predict Returns?", "content_type": "non_ticker", "intent": "Do insider purchases predict stock returns?"},
    {"key": "congress_semiconductor_buying", "label": "Congress Semiconductor Buying", "content_type": "non_ticker", "intent": "Which Congress members are buying semiconductor stocks?"},
    {"key": "insider_and_institutional_buying", "label": "Insider and Institutional Buying", "content_type": "non_ticker", "intent": "Which stocks have both insider and institutional buying?"},
    {"key": "dod_contract_leaders", "label": "DoD Contract Leaders", "content_type": "non_ticker", "intent": "Which public companies are receiving the most Department of Defense contracts?"},
    {"key": "ai_institutional_accumulation", "label": "AI Institutional Accumulation", "content_type": "non_ticker", "intent": "Are institutions accumulating AI stocks?"},
    {"key": "meaningful_insider_buying", "label": "Meaningful Insider Buying", "content_type": "non_ticker", "intent": "How do you tell if insider buying is meaningful?"},
    {"key": "interpret_congress_trades", "label": "Interpret Congress Trades", "content_type": "non_ticker", "intent": "How should investors interpret Congress stock trades?"},
    {"key": "increasing_institutional_ownership", "label": "Increasing Institutional Ownership", "content_type": "non_ticker", "intent": "What does increasing institutional ownership mean?"},
    {"key": "confirm_bullish_thesis", "label": "Confirm a Bullish Thesis", "content_type": "non_ticker", "intent": "How do you confirm a bullish stock thesis?"},
    {"key": "conflicting_stock_research_data", "label": "Conflicting Stock Research Data", "content_type": "non_ticker", "intent": "What does conflicting stock research data mean?"},
]
JUDGMENT_VALUES = {"bullish", "bearish", "mixed", "macro", "policy", "neutral"}
RESEARCH_BRIEF_REQUIRED_PLAN_VALUES = {"premium", "pro"}
WALNUT_CALL_VALUES = [
    "Very bullish",
    "Bullish",
    "Bullish with capex risk",
    "Bullish but expensive",
    "Neutral",
    "Neutral but expensive",
    "Neutral with capex risk",
    "Mixed with capex risk",
    "Mixed",
    "Bearish",
    "Very bearish",
    "Insufficient data to make a call",
]
MAX_COMPARISON_TICKERS = 5
MAJOR_EARNINGS_SOURCE_TICKERS = {"AAPL", "META", "MSFT", "GOOGL", "GOOG", "AMZN", "NVDA", "MU", "JPM"}
OFFICIAL_SOURCE_PROFILES: dict[str, dict[str, Any]] = {
    "AAPL": {
        "company_earnings_sources": [
            {
                "label": "Apple Newsroom Q2 FY2026 results",
                "url": "https://www.apple.com/ca/newsroom/2026/04/apple-reports-second-quarter-results/",
                "source_type": "official_company_earnings",
            },
            {
                "label": "Apple Investor Relations earnings",
                "url": "https://investor.apple.com/earnings-results/default.aspx",
                "source_type": "official_company_ir",
            },
            {
                "label": "Apple SEC filings",
                "url": "https://investor.apple.com/sec-filings/default.aspx",
                "source_type": "official_company_filings",
            },
        ],
        "source_notes": [
            "For AAPL earnings setup, use Apple official Q2 FY2026 results and SEC filings before generic market sources.",
        ],
        "official_facts": {
            "latest_official_quarter": "Q2 FY2026",
            "quarter_end": "2026-03-28",
            "revenue": {"value": 111.2, "unit": "USD billions", "period": "Q2 FY2026", "source": "Apple official Q2 FY2026 results release"},
            "revenue_growth": {"value": 17, "unit": "% YoY", "period": "Q2 FY2026", "source": "Apple official Q2 FY2026 results release"},
            "diluted_eps": {"value": 2.01, "unit": "USD", "period": "Q2 FY2026", "source": "Apple official Q2 FY2026 results release"},
            "eps_growth": {"value": 22, "unit": "% YoY", "period": "Q2 FY2026", "source": "Apple official Q2 FY2026 results release"},
        },
    },
    "META": {"company_earnings_sources": [{"label": "Meta Investor Relations earnings", "url": "https://investor.fb.com/financials/default.aspx", "source_type": "official_company_earnings"}]},
    "MSFT": {"company_earnings_sources": [{"label": "Microsoft Investor Relations earnings", "url": "https://www.microsoft.com/en-us/Investor/earnings", "source_type": "official_company_earnings"}]},
    "GOOGL": {"company_earnings_sources": [{"label": "Alphabet Investor Relations earnings", "url": "https://abc.xyz/investor/", "source_type": "official_company_earnings"}]},
    "GOOG": {"company_earnings_sources": [{"label": "Alphabet Investor Relations earnings", "url": "https://abc.xyz/investor/", "source_type": "official_company_earnings"}]},
    "AMZN": {"company_earnings_sources": [{"label": "Amazon Investor Relations quarterly results", "url": "https://ir.aboutamazon.com/quarterly-results/default.aspx", "source_type": "official_company_earnings"}]},
    "NVDA": {"company_earnings_sources": [{"label": "NVIDIA Investor Relations financial reports", "url": "https://investor.nvidia.com/financial-info/financial-reports/default.aspx", "source_type": "official_company_earnings"}]},
    "MU": {"company_earnings_sources": [{"label": "Micron Investor Relations financial releases", "url": "https://investors.micron.com/news-releases", "source_type": "official_company_earnings"}]},
    "JPM": {"company_earnings_sources": [{"label": "JPMorgan Chase Investor Relations earnings", "url": "https://www.jpmorganchase.com/ir/quarterly-earnings", "source_type": "official_company_earnings"}]},
    "NBIS": {
        "company_earnings_sources": [
            {
                "label": "Nebius financial results archive",
                "url": "https://nebius.com/financials",
                "source_type": "official_company_earnings",
            },
            {
                "label": "Nebius Q1 2026 financial results",
                "url": "https://nebius.com/newsroom/nebius-reports-first-quarter-2026-financial-results",
                "source_type": "official_company_earnings",
            },
            {
                "label": "Nebius Investor Relations",
                "url": "https://nebius.com/investor-hub",
                "source_type": "official_company_ir",
            },
            {
                "label": "Zacks NBIS detailed estimates",
                "url": "https://stage.zacks.com/stock/quote/NBIS/detailed-earning-estimates",
                "source_type": "reputable_estimate_source",
            },
        ],
        "source_notes": [
            "For NBIS earnings setup, separate Nebius primary operating data from CRWV or other AI infrastructure peers unless they are selected comparison tickers.",
        ],
        "official_facts": {
            "upcoming_earnings_date": {"value": "2026-08-06", "period": "Q2 2026", "source": "Zacks/MarketBeat estimated earnings calendar"},
            "current_revenue_consensus": {"value": 535.03, "unit": "USD millions", "period": "Q2 2026", "source": "Zacks detailed estimates"},
            "current_eps_consensus": {"value": -0.67, "unit": "USD/share", "period": "Q2 2026", "source": "Zacks detailed estimates"},
            "latest_official_quarter": "Q1 2026",
            "previous_quarter_revenue": {"value": 399, "unit": "USD millions", "period": "Q1 2026", "source": "Nebius Q1 2026 financial results"},
            "previous_quarter_eps": {"value": -0.23, "unit": "USD/share", "period": "Q1 2026", "source": "MarketBeat/Zacks earnings history"},
            "previous_quarter_revenue_consensus": {"value": 389, "unit": "USD millions", "period": "Q1 2026", "source": "Drillr earnings history"},
            "previous_quarter_eps_consensus": {"value": -0.81, "unit": "USD/share", "period": "Q1 2026", "source": "MarketBeat/Zacks earnings history"},
            "previous_quarter_result": {"value": "beat", "period": "Q1 2026", "source": "MarketBeat/Zacks earnings history"},
            "guidance": {"value": "FY 2026 revenue guidance of $3.0B-$3.4B", "period": "FY 2026", "source": "MarketBeat company guidance summary"},
            "material_catalysts": ["capacity deployment", "ARR growth", "capex funding", "AI cloud customer wins"],
        },
    },
}
KEY_RESEARCH_FIELDS = [
    "revenue",
    "revenue growth",
    "guidance",
    "gross margin",
    "operating margin",
    "EBITDA / adjusted EBITDA",
    "free cash flow",
    "capex",
    "cash",
    "debt",
    "share count",
    "backlog / orders / RPO",
    "dilution / ATM / offering history",
    "major customer concentration",
    "government contracts",
    "reported institutional activity",
    "insider activity",
    "Congress activity",
    "price/volume and technicals",
    "peer comparison data",
]
DEFAULT_SECTIONS = [
    "Executive thesis",
    "What changed",
    "Business and fundamentals",
    "Valuation",
    "Price / volume and technicals",
    "Congress activity",
    "Insider activity",
    "Reported institutional activity",
    "Government contracts",
    "Options flow",
    "Catalysts",
    "Risks",
    "What to watch next",
    "Final Walnut judgment",
    "Data freshness and limitations",
]
CONFIRMATION_SCORE_SECTION_HEADING = "Our confirmation score"
CROSS_SOURCE_CONFIRMATIONS_SECTION_HEADING = "Cross-source confirmations"
PUBLISHED_STATIC_SLUGS = {"mu-dd"}
UNSUPPORTED_LANGUAGE = [
    "buy now",
    "better buy",
    "guaranteed return",
    "guaranteed returns",
    "beat the market",
    "risk-free",
    "can't lose",
    "will moon",
]
PUBLISH_COPY_FORBIDDEN_PATTERNS = [
    r"\bresearch request\b",
    r"\bsupplied research request\b",
    r"\bsupplied research context\b",
    r"\bsupplied materials?\b",
    r"\bsupplied context\b",
    r"\bsupplied q[1-4] figures?\b",
    r"\bresearch configuration\b",
    r"\bmarked available in the research configuration\b",
    r"\bprovided comparison confirmation\b",
    r"\bno reviewed consensus source was supplied\b",
    r"\breviewed materials do not provide\b",
    r"\bthis configuration\b",
    r"\bin this research configuration\b",
    r"\bpublication context\b",
    r"\buser request\b",
    r"\bmodel was asked\b",
    r"\bgenerated from\b",
    r"\bthe prompt\b",
    r"\bprompt\b",
    r"\bwe do not publish our proprietary confirmation score\b",
]
PUBLISH_COPY_FORBIDDEN_RE = re.compile("|".join(PUBLISH_COPY_FORBIDDEN_PATTERNS), re.IGNORECASE)
MISSING_DATA_AWKWARD_RE = re.compile(
    r"\b(not supplied|was supplied|were supplied|no .* was supplied|reviewed materials do not provide|supplied materials|supplied context|research configuration)\b",
    re.IGNORECASE,
)
STYLE_TIC_PATTERNS = [
    ("reviewed record supplied", r"\bthe reviewed record supplied\b"),
    ("available evidence does not permit", r"\bthe available evidence does not permit\b"),
    ("we reserve judgment", r"\bwe (?:therefore )?reserve judgment\b"),
    ("appropriate next step", r"\bthe appropriate next step\b"),
    ("credible bull case requires", r"\ba credible bull case requires\b"),
    ("central question", r"\bthe central question\b"),
    ("not about x it is about y", r"\bnot (?:really )?(?:about|whether)\b.{0,80}\bit is (?:about|whether)\b"),
    ("market is asking", r"\bthe market is (?:not )?asking\b"),
    ("against this backdrop", r"\bagainst this backdrop\b"),
    ("at this stage", r"\bat this stage\b"),
    ("in other words", r"\bin other words\b"),
    ("from an investor perspective", r"\bfrom an investor perspective\b"),
    ("key takeaway", r"\bthe key takeaway\b"),
    ("on balance", r"\bon balance\b"),
    ("evidence suggests", r"\bevidence suggests\b"),
    ("would weaken the thesis", r"\bwould weaken the thesis\b"),
    ("remains to be seen", r"\bremains to be seen\b"),
    ("warrants caution", r"\bwarrants caution\b"),
    ("investment intensity", r"\binvestment intensity\b"),
    ("operating cadence", r"\boperating cadence\b"),
    ("demand narrative", r"\bdemand narrative\b"),
    ("viewed through this lens", r"\bviewed through this lens\b"),
]
COMPANY_IDENTITY_GUARDS = {
    "AAPL": ["Apple"],
    "AMD": ["Advanced Micro Devices", "AMD"],
    "AMZN": ["Amazon"],
    "CRWV": ["CoreWeave", "CRWV"],
    "GOOG": ["Alphabet", "Google", "GOOG"],
    "GOOGL": ["Alphabet", "Google", "GOOGL"],
    "META": ["Meta", "Facebook"],
    "MSFT": ["Microsoft"],
    "MU": ["Micron"],
    "NBIS": ["Nebius", "NBIS"],
    "NVDA": ["Nvidia", "NVIDIA", "NVDA"],
}
PLACEHOLDER_HEADINGS = {"intro", "hook", "intro / hook"}
SINGLETON_HEADINGS = {
    "the call": "The call",
    "sources": "Sources",
    "what to watch next": "What to watch next",
    "data freshness and limitations": "Data freshness and limitations",
}
REDDIT_BULL_BEAR_OUTLINE = [
    "Executive thesis",
    "Bull case",
    "Bear case",
    "The data",
    "The call",
    "What to watch next",
    "Sources",
    "Data freshness and limitations",
]

_STORE_LOCK = threading.Lock()
_ACTIVE_GENERATIONS: set[str] = set()
_JOB_WORKER_LOCK = threading.Lock()
_JOB_WORKERS: dict[str, threading.Thread] = {}


def research_brief_model(db: Session | None = None) -> str:
    configured = os.getenv(RESEARCH_BRIEF_MODEL_DEFAULT, "").strip() or os.getenv(RESEARCH_BRIEF_GENERATOR_MODEL, "").strip()
    if configured:
        return configured
    return DEFAULT_RESEARCH_BRIEF_MODEL


def research_brief_model_options(db: Session | None = None) -> list[str]:
    configured = [item.strip() for item in os.getenv(RESEARCH_BRIEF_MODEL_OPTIONS, "").split(",") if item.strip()]
    default = research_brief_model(db)
    options = configured or list(DEFAULT_RESEARCH_BRIEF_MODEL_OPTIONS)
    if default not in options:
        options.insert(0, default)
    return list(dict.fromkeys(options))


def research_brief_model_descriptions(db: Session | None = None) -> dict[str, str]:
    options = research_brief_model_options(db)
    labels = ["Fast / cheaper", "Balanced", "Deep research / highest quality"]
    return {model: RESEARCH_BRIEF_MODEL_DESCRIPTIONS.get(model) or labels[min(index, len(labels) - 1)] for index, model in enumerate(options)}


def research_brief_model_labels(db: Session | None = None) -> dict[str, str]:
    return {model: RESEARCH_BRIEF_MODEL_LABELS.get(model) or model for model in research_brief_model_options(db)}


def _selected_research_model(config: dict[str, Any], db: Session | None = None, *, strict: bool = True) -> str:
    options = research_brief_model_options(db)
    selected = str(config.get("selected_model") or "").strip()
    if selected:
        if selected not in options:
            if not strict:
                return ""
            raise HTTPException(status_code=422, detail="Selected research model is not configured for this environment.")
        return selected
    serious = (
        str(config.get("section_format") or "") != "X Thread"
        and str(config.get("intended_audience") or "") != "General investors"
        and (
            "valuation" in str(config.get("desired_angle") or "").lower()
            or "dcf" in str(config.get("research_question") or "").lower()
            or "dd" in str(config.get("section_format") or "").lower()
            or str(config.get("intended_audience") or "") in {"Reddit DD", "Walnut Research Brief", "Professional / advanced"}
        )
    )
    return options[-1] if serious else research_brief_model(db)


def draft_store_path() -> Path:
    configured = os.getenv(STORE_ENV, "").strip()
    if configured:
        return Path(configured)
    return Path(__file__).resolve().parents[2] / ".local" / "research_brief_drafts.json"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _iso(value: Any) -> str | None:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat() if value.tzinfo else value.replace(tzinfo=timezone.utc).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _load_json(value: str | None) -> Any:
    if not value:
        return None
    try:
        return json.loads(value)
    except Exception:
        return None


def _compact(value: Any, *, limit: int = 5000) -> Any:
    if isinstance(value, dict):
        return {str(k): _compact(v, limit=limit) for k, v in value.items() if not _is_internal_key(str(k))}
    if isinstance(value, list):
        return [_compact(item, limit=limit) for item in value[:25]]
    if isinstance(value, str):
        return value[:limit]
    return value


def _is_internal_key(key: str) -> bool:
    lowered = key.lower()
    return any(term in lowered for term in ("provider", "cache", "secret", "token", "credential", "diagnostic", "raw"))


def sanitize_research_brief_copy(markdown: str) -> str:
    text = str(markdown or "")
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = text.replace("**", "")
    repaired_blocks: list[str] = []
    for block in re.split(r"(\n{2,})", text):
        if not block or block.startswith("\n"):
            repaired_blocks.append(block)
            continue
        repaired_blocks.append(_sanitize_copy_block(block))
    cleaned = "".join(repaired_blocks)
    cleaned = re.sub(r"[ \t]+\n", "\n", cleaned)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    cleaned = format_research_numeric_claims(cleaned)
    return _rewrite_public_walnut_voice(cleaned).strip()


def format_research_numeric_claims(markdown: str) -> str:
    text = str(markdown or "")

    def percentage(match: re.Match[str]) -> str:
        prefix = match.group(1) or ""
        value = _safe_float(match.group(2).replace(",", ""))
        if value is None:
            return match.group(0)
        return f"{prefix}{_format_compact_decimal(value, max_decimals=1)}%"

    def ratio(match: re.Match[str]) -> str:
        prefix = match.group(1) or ""
        value = _safe_float(match.group(2).replace(",", ""))
        if value is None:
            return match.group(0)
        return f"{prefix}{_format_compact_decimal(value, max_decimals=1)}x"

    def currency(match: re.Match[str]) -> str:
        raw = match.group(1).replace(",", "")
        value = _safe_float(raw)
        if value is None:
            return match.group(0)
        if abs(value) >= 1_000_000_000_000:
            return f"${value / 1_000_000_000_000:.1f} trillion"
        if abs(value) >= 1_000_000_000:
            return f"${value / 1_000_000_000:.1f} billion"
        if abs(value) >= 1_000_000:
            return f"${value / 1_000_000:.1f} million"
        if "." in raw:
            return f"${value:.2f}".rstrip("0").rstrip(".")
        return match.group(0)

    text = re.sub(r"(?<![\w/])([+−-]?)(\d[\d,]*\.\d{3,})\s*%", percentage, text)
    text = re.sub(r"(?<![\w/])([+−-]?)(\d[\d,]*\.\d{3,})\s*x\b", ratio, text, flags=re.IGNORECASE)
    text = re.sub(r"\$(-?\d{7,}(?:\.\d+)?)\b", currency, text)
    text = re.sub(r"\$(-?\d+\.\d{3,})\b", currency, text)
    return text


def _format_compact_decimal(value: float, *, max_decimals: int) -> str:
    return f"{value:.{max_decimals}f}".rstrip("0").rstrip(".")


def _safe_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _rewrite_public_walnut_voice(text: str) -> str:
    replacements = (
        (r"\bWalnut[’']s proprietary confirmation score\b", "our proprietary confirmation score"),
        (r"\bWalnut[’']s confirmation score\b", "our confirmation score"),
        (r"\bWalnut confirmation score\b", "our confirmation score"),
        (r"\bWalnut[’']s take\b", "our take"),
        (r"\bWalnut take\b", "our take"),
        (r"\bWalnut[’']s view\b", "our view"),
        (r"\bWalnut[’']s read\b", "our read"),
        (r"\bWalnut[’']s judgment\b", "our judgment"),
        (r"\bWalnut[’']s cross-source evidence\b", "our cross-source evidence"),
        (r"\bWalnut[’']s cross-source confirmation\b", "our cross-source confirmation"),
        (r"\bWalnut[’']s cross-source confirmations\b", "our cross-source confirmations"),
        (r"\bWalnut[’']s data\b", "our data"),
        (r"\bWalnut[’']s signal\b", "our signal"),
        (r"\bWalnut[’']s signals\b", "our signals"),
        (r"\bWalnut data\b", "our data"),
        (r"\bWalnut signal\b", "our signal"),
        (r"\bWalnut signals\b", "our signals"),
    )
    rewritten = str(text or "")
    for pattern, replacement in replacements:
        def replace_match(match: re.Match[str], repl: str = replacement) -> str:
            prefix = rewritten[: match.start()]
            line_prefix = prefix[prefix.rfind("\n") + 1 :]
            starts_sentence = not prefix.strip() or not line_prefix.strip() or line_prefix.lstrip().startswith("#") or prefix.rstrip().endswith((".", "!", "?", ":"))
            return repl[:1].upper() + repl[1:] if starts_sentence else repl

        rewritten = re.sub(pattern, replace_match, rewritten, flags=re.IGNORECASE)
    return rewritten


def _sanitize_copy_block(block: str) -> str:
    if re.search(r"(?m)^#{1,6}\s+", block):
        lines = block.splitlines()
        output: list[str] = []
        pending: list[str] = []

        def flush_pending() -> None:
            if not pending:
                return
            cleaned = _sanitize_copy_text_block("\n".join(pending))
            if cleaned:
                output.append(cleaned)
            pending.clear()

        for line in lines:
            if re.match(r"^#{1,6}\s+", line):
                flush_pending()
                output.append(line)
            else:
                pending.append(line)
        flush_pending()
        return "\n".join(output).strip()
    return _sanitize_copy_text_block(block)


def _sanitize_copy_text_block(block: str) -> str:
    if block.lstrip().startswith("|"):
        return "" if PUBLISH_COPY_FORBIDDEN_RE.search(block) else block
    pieces = re.split(r"(?<=[.!?])(\s+)", block)
    output: list[str] = []
    index = 0
    while index < len(pieces):
        sentence = pieces[index]
        separator = pieces[index + 1] if index + 1 < len(pieces) else ""
        replacement = _rewrite_internal_workflow_sentence(sentence)
        if replacement is not None:
            if replacement and replacement not in output:
                output.append(replacement)
                output.append(separator)
        else:
            output.append(_rewrite_human_research_phrases(_rewrite_internal_phrases(sentence)))
            output.append(separator)
        index += 2
    return "".join(output).strip()


def _rewrite_internal_workflow_sentence(sentence: str) -> str | None:
    lowered = sentence.lower()
    if not PUBLISH_COPY_FORBIDDEN_RE.search(sentence):
        return None
    if "eps" in lowered and any(term in lowered for term in ("estimate", "consensus")):
        return "Current EPS consensus estimates were not verified in reviewed sources, so they are omitted from the analysis."
    if "revenue" in lowered and any(term in lowered for term in ("estimate", "consensus")):
        return "Current revenue consensus estimates were not verified in reviewed sources, so they are omitted from the analysis."
    if "price" in lowered and ("volume" in lowered or "technical" in lowered):
        return "Current Walnut data does not provide enough ticker-specific price/volume detail to support a full technical read, so price action is not central to this brief."
    if "tax" in lowered:
        return "Diluted EPS should be read with caution if one-time tax items affected the quarter."
    if "guidance" in lowered:
        return "Current guidance was not verified in reviewed sources, so the analysis focuses on reported results and the questions management needs to answer."
    if "options" in lowered or "implied move" in lowered:
        return "Options flow is omitted because no reliable options-implied move was verified in reviewed sources."
    if "congress" in lowered or "insider" in lowered:
        return "No ticker-specific Congress or insider activity was material enough to affect this setup."
    if "q1" in lowered and ("figures" in lowered or "operating" in lowered):
        return "Q1 operating figures should be read alongside official earnings materials and SEC filings."
    return ""


def _rewrite_internal_phrases(sentence: str) -> str:
    replacements = {
        r"\breferenced in the research request\b": "not independently verified",
        r"\bfrom the supplied research context\b": "from reviewed Walnut data",
        r"\bfrom supplied research context\b": "from reviewed Walnut data",
        r"\bsupplied research context\b": "reviewed Walnut data",
        r"\bsupplied materials?\b": "reviewed sources",
        r"\bsupplied context\b": "reviewed Walnut data",
        r"\bavailable Walnut context\b": "available Walnut data",
        r"\bWalnut context\b": "Walnut data",
        r"\bresearch configuration\b": "reviewed data",
        r"\bprovided comparison confirmation\b": "comparison data",
        r"\bno reviewed consensus source was supplied\b": "no reliable consensus source was verified",
        r"\breviewed materials do not provide\b": "reviewed sources did not verify",
    }
    rewritten = sentence
    for pattern, replacement in replacements.items():
        rewritten = re.sub(pattern, replacement, rewritten, flags=re.IGNORECASE)
    return rewritten


def _rewrite_human_research_phrases(sentence: str) -> str:
    original = sentence
    replacements = {
        r"\bIt is important to note that\s+": "",
        r"\bIt should be noted that\s+": "",
        r"\bIn conclusion,\s+": "",
        r"\bOverall,\s+": "",
        r"\bThis article will examine\b": "This brief examines",
        r"\bThis section will examine\b": "This section examines",
        r"\bThis brief will examine\b": "This brief examines",
        r"\bIt remains to be seen whether\b": "The open question is whether",
        r"\bInvestors should monitor\b": "Watch",
        r"\bInvestors should watch\b": "Watch",
        r"\bInvestors should keep an eye on\b": "Watch",
        r"\bThere are several factors that\b": "Several factors",
        r"\bplays a crucial role in\b": "matters for",
        r"\bis a key factor in determining\b": "helps determine",
    }
    rewritten = sentence
    for pattern, replacement in replacements.items():
        rewritten = re.sub(pattern, replacement, rewritten, flags=re.IGNORECASE)
    if rewritten != original:
        match = re.search(r"[A-Za-z]", rewritten)
        if match and rewritten[match.start()].islower():
            rewritten = rewritten[: match.start()] + rewritten[match.start()].upper() + rewritten[match.start() + 1 :]
    return rewritten


def clean_research_brief_markdown(markdown: str, section_format: str, section_heading: str | None = None) -> str:
    text = sanitize_research_brief_copy(markdown)
    if not text:
        return ""
    text = _remove_redundant_leading_heading(text, section_heading)
    parts = re.split(r"(?m)^(##\s+.+?)\s*$", text)
    if len(parts) == 1:
        return text.strip()
    intro = parts[0].strip()
    sections: list[dict[str, str]] = []
    pending_empty_heading: str | None = None
    for index in range(1, len(parts), 2):
        heading = re.sub(r"^##\s+", "", parts[index]).strip()
        body = parts[index + 1].strip() if index + 1 < len(parts) else ""
        normalized = _canonical_heading(heading)
        if _is_placeholder_heading(normalized):
            if body:
                intro = "\n\n".join(part for part in [intro, body] if part).strip()
            pending_empty_heading = None
            continue
        if not body:
            pending_empty_heading = normalized
            continue
        if pending_empty_heading:
            normalized = pending_empty_heading
            pending_empty_heading = None
        if sections and _heading_key(sections[-1]["heading"]) == _heading_key(normalized):
            sections[-1]["body"] = _merge_markdown_bodies(sections[-1]["body"], body)
            continue
        sections.append({"heading": normalized, "body": body})
    sections = _merge_singleton_sections(sections)
    if section_format == "Reddit DD - Bull Case / Bear Case / The Data / The Call":
        sections = _order_outline_sections(sections, REDDIT_BULL_BEAR_OUTLINE)
        intro = _merge_intro_into_first_section(intro, sections)
    output: list[str] = []
    if intro:
        output.append(intro)
    for section in sections:
        output.append(f"## {section['heading']}\n\n{section['body'].strip()}")
    return "\n\n".join(output).strip()


def _remove_redundant_leading_heading(markdown: str, section_heading: str | None) -> str:
    if not section_heading:
        return markdown
    match = re.match(r"^\s*##\s+(.+?)\s*\n+", markdown)
    if not match:
        return markdown
    if _heading_key(match.group(1)) == _heading_key(section_heading):
        return markdown[match.end() :].strip()
    return markdown


def _canonical_heading(heading: str) -> str:
    cleaned = re.sub(r"\s+", " ", str(heading or "").strip().strip("#")).strip()
    key = _heading_key(cleaned)
    for singleton_key, canonical in SINGLETON_HEADINGS.items():
        if key == singleton_key or key.startswith(f"{singleton_key}:"):
            return canonical
    return cleaned


def _heading_key(heading: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(heading or "").lower()).strip()


def _is_placeholder_heading(heading: str) -> bool:
    return _heading_key(heading) in PLACEHOLDER_HEADINGS


def _merge_markdown_bodies(left: str, right: str) -> str:
    lines: list[str] = []
    seen = set()
    for line in [*str(left or "").splitlines(), "", *str(right or "").splitlines()]:
        key = line.strip()
        if key and key in seen:
            continue
        if key:
            seen.add(key)
        lines.append(line)
    return re.sub(r"\n{3,}", "\n\n", "\n".join(lines)).strip()


def _merge_singleton_sections(sections: list[dict[str, str]]) -> list[dict[str, str]]:
    merged: list[dict[str, str]] = []
    singleton_indexes: dict[str, int] = {}
    for section in sections:
        key = _heading_key(section["heading"])
        if key in SINGLETON_HEADINGS:
            section["heading"] = SINGLETON_HEADINGS[key]
            if key in singleton_indexes:
                target = merged[singleton_indexes[key]]
                target["body"] = _merge_markdown_bodies(target["body"], section["body"])
                continue
            singleton_indexes[key] = len(merged)
        merged.append(section)
    return merged


def _order_outline_sections(sections: list[dict[str, str]], outline: list[str]) -> list[dict[str, str]]:
    order = {_heading_key(heading): index for index, heading in enumerate(outline)}
    return [item[1] for item in sorted(enumerate(sections), key=lambda item: (order.get(_heading_key(item[1]["heading"]), len(order) + item[0]), item[0]))]


def _merge_intro_into_first_section(intro: str, sections: list[dict[str, str]]) -> str:
    if intro and sections:
        sections[0]["body"] = _merge_markdown_bodies(intro, sections[0]["body"])
        return ""
    return intro


def sanitize_research_brief_article(
    article: dict[str, Any],
    config: dict[str, Any],
    context: dict[str, Any] | None = None,
    *,
    repair_generated_sections: bool = True,
) -> dict[str, Any]:
    section_format = str(config.get("section_format") or "Walnut Research Brief")
    sanitized = deepcopy(article)
    before = json.dumps(sanitized, sort_keys=True, default=str)
    for key in ("title", "subtitle", "summary", "preview_body"):
        if isinstance(sanitized.get(key), str):
            sanitized[key] = sanitize_research_brief_copy(sanitized[key]).lstrip("# ").strip()
    for key in ("key_points", "catalysts", "risks", "watch_items", "data_freshness", "missing_data_notes"):
        if isinstance(sanitized.get(key), list):
            sanitized[key] = _dedupe_strings([sanitize_research_brief_copy(str(item)) for item in sanitized[key]])
    sections = sanitized.get("sections") if isinstance(sanitized.get("sections"), list) else []
    cleaned_sections: list[dict[str, Any]] = []
    for index, section in enumerate(sections):
        if not isinstance(section, dict):
            continue
        heading = sanitize_research_brief_copy(str(section.get("heading") or "")).lstrip("# ").strip() or f"Section {index + 1}"
        if not repair_generated_sections:
            body = sanitize_research_brief_copy(str(section.get("body_markdown") or ""))
            if not body:
                continue
            cleaned_sections.append(
                {
                    **section,
                    "heading": heading,
                    "key": str(section.get("key") or _slugify(heading, fallback=f"section-{index + 1}")),
                    "body_markdown": body,
                }
            )
            continue
        heading = _canonical_heading(heading)
        if _is_placeholder_heading(heading):
            heading = "Executive thesis"
        body = clean_research_brief_markdown(str(section.get("body_markdown") or ""), section_format, section_heading=heading)
        if not body:
            continue
        cleaned_sections.extend(_article_sections_from_clean_markdown(body, heading, section, index))
    sanitized["sections"] = _merge_article_sections(cleaned_sections, section_format) if repair_generated_sections else cleaned_sections
    preview_count = _coerce_preview_section_count(sanitized.get("preview_section_count"), len(sanitized["sections"]))
    if preview_count is None:
        sanitized.pop("preview_section_count", None)
    else:
        sanitized["preview_section_count"] = preview_count
    if repair_generated_sections:
        sanitized = _apply_confirmation_preferences(sanitized, config, context or {})
        sanitized = _apply_earnings_setup_judgment(sanitized, config, context or {})
        # A model can still carry an obsolete "not found" sentence into the
        # final copy even when Walnut's assembled context confirms that field
        # is available. Remove only those contradictory sentences; validation
        # remains the backstop for every other unsupported claim.
        sanitized = _remove_available_data_missing_claims_from_article(sanitized, context or {})
    sanitized = _apply_walnut_call_metadata(sanitized)
    sanitized = _apply_research_access_metadata(sanitized, config)
    after = json.dumps(sanitized, sort_keys=True, default=str)
    if after != before:
        sanitized["_copy_sanitizer_repairs"] = 1 + int(sanitized.get("_copy_sanitizer_repairs") or 0)
    return sanitized


def _article_sections_from_clean_markdown(body: str, fallback_heading: str, source_section: dict[str, Any], index: int) -> list[dict[str, Any]]:
    parts = re.split(r"(?m)^(##\s+.+?)\s*$", body)
    if len(parts) == 1:
        return [
            {
                **source_section,
                "heading": fallback_heading,
                "key": str(source_section.get("key") or _slugify(fallback_heading, fallback=f"section-{index + 1}")),
                "body_markdown": body.strip(),
            }
        ]
    sections: list[dict[str, Any]] = []
    intro = parts[0].strip()
    if intro:
        sections.append(
            {
                **source_section,
                "heading": fallback_heading,
                "key": str(source_section.get("key") or _slugify(fallback_heading, fallback=f"section-{index + 1}")),
                "body_markdown": intro,
            }
        )
    for part_index in range(1, len(parts), 2):
        heading = _canonical_heading(re.sub(r"^##\s+", "", parts[part_index]).strip())
        section_body = parts[part_index + 1].strip() if part_index + 1 < len(parts) else ""
        if not section_body:
            continue
        sections.append(
            {
                **source_section,
                "heading": heading,
                "key": _slugify(heading, fallback=f"section-{index + 1}-{part_index}"),
                "body_markdown": section_body,
            }
        )
    return sections


def _merge_article_sections(sections: list[dict[str, Any]], section_format: str) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    by_heading: dict[str, int] = {}
    for section in sections:
        heading = _canonical_heading(str(section.get("heading") or ""))
        key = _heading_key(heading)
        if key in by_heading:
            existing = merged[by_heading[key]]
            existing["body_markdown"] = _merge_markdown_bodies(str(existing.get("body_markdown") or ""), str(section.get("body_markdown") or ""))
            continue
        section["heading"] = heading
        by_heading[key] = len(merged)
        merged.append(section)
    if section_format == "Reddit DD - Bull Case / Bear Case / The Data / The Call":
        order = {_heading_key(heading): index for index, heading in enumerate(REDDIT_BULL_BEAR_OUTLINE)}
        merged = [item[1] for item in sorted(enumerate(merged), key=lambda item: (order.get(_heading_key(item[1].get("heading")), len(order) + item[0]), item[0]))]
    return merged


def _apply_confirmation_preferences(article: dict[str, Any], config: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    include_score = bool(config.get("include_confirmation_score"))
    include_cross_source = bool(config.get("include_cross_source_confirmations"))
    sanitized = deepcopy(article)
    if not include_score:
        sanitized = _strip_confirmation_score_from_article(sanitized)
    sections = sanitized.get("sections") if isinstance(sanitized.get("sections"), list) else []
    sections = [_strip_confirmation_content_from_section(section, include_score=include_score, include_cross_source=include_cross_source) for section in sections if isinstance(section, dict)]
    sections = [_remove_confirmation_data_conflation_from_section(section) for section in sections]
    sections = [section for section in sections if str(section.get("body_markdown") or "").strip()]
    sanitized["sections"] = sections
    sanitized["confirmation_score_included"] = bool(include_score and _confirmation_score_value(context) is not None)

    if include_score:
        score_text = _confirmation_score_sentence(context)
        if score_text and not _article_includes_confirmation_score_value(sanitized, context):
            sanitized["sections"] = _append_or_merge_generated_section(
                sanitized["sections"],
                CONFIRMATION_SCORE_SECTION_HEADING,
                score_text,
                key="walnut_confirmation_score",
            )
    if include_cross_source:
        commentary = _cross_source_confirmation_commentary(context)
        if commentary and not _article_mentions_cross_source_confirmations(sanitized):
            sanitized["sections"] = _append_or_merge_generated_section(
                sanitized["sections"],
                CROSS_SOURCE_CONFIRMATIONS_SECTION_HEADING,
                commentary,
                key="cross_source_confirmations",
            )
    return _remove_confirmation_data_conflation(sanitized)


def _remove_confirmation_data_conflation(article: dict[str, Any]) -> dict[str, Any]:
    """Apply the confirmation-score guard to every public text field, not only sections."""
    cleaned = deepcopy(article)
    for key in ("title", "subtitle", "summary", "preview_body"):
        if isinstance(cleaned.get(key), str):
            cleaned[key] = _remove_confirmation_data_conflation_from_text(cleaned[key])
    for key in ("key_points", "catalysts", "risks", "watch_items", "data_freshness", "missing_data_notes"):
        if isinstance(cleaned.get(key), list):
            cleaned[key] = [
                value
                for value in (_remove_confirmation_data_conflation_from_text(str(item)) for item in cleaned[key])
                if value
            ]
    suggested = cleaned.get("suggested_card") if isinstance(cleaned.get("suggested_card"), dict) else None
    if suggested:
        for key in ("title", "description"):
            if isinstance(suggested.get(key), str):
                suggested[key] = _remove_confirmation_data_conflation_from_text(suggested[key])
    sections = cleaned.get("sections") if isinstance(cleaned.get("sections"), list) else []
    cleaned["sections"] = [_remove_confirmation_data_conflation_from_section(section) for section in sections if isinstance(section, dict)]
    return cleaned


def _remove_confirmation_data_conflation_from_section(section: dict[str, Any]) -> dict[str, Any]:
    """Drop a model sentence that incorrectly equates qualitative data with the score."""
    cleaned = dict(section)
    cleaned["heading"] = _remove_confirmation_data_conflation_from_text(str(cleaned.get("heading") or "")) or "Supporting context"
    cleaned["body_markdown"] = _remove_confirmation_data_conflation_from_text(str(cleaned.get("body_markdown") or ""))
    return cleaned


def _remove_confirmation_data_conflation_from_text(text: str) -> str:
    body = str(text or "")
    data_terms = r"(?:price/?volume|price and volume|fundamentals|reported institutional activity|congress activity|insider activity|government contracts|options flow|macro positioning|underlying data|(?:cross[ -]source )?data categories)"
    score_inputs = r"(?:input|inputs|component|components|driver|drivers|factor|factors|basis|bases|source|sources)"
    patterns = (
        rf"\bconfirmation score\s+(?:is|equals|represents|is derived from|is based on|comes from)\s+.{{0,160}}{data_terms}",
        rf"{data_terms}.{{0,160}}\s+(?:are|is)\s+the\s+confirmation score",
        rf"\bconfirmation score.{{0,160}}{score_inputs}.{{0,120}}{data_terms}",
        rf"{data_terms}.{{0,160}}{score_inputs}.{{0,120}}\bconfirmation score",
        r"\bconfirmation score\s+and\s+underlying data\s+are\s+the\s+same",
    )
    for pattern in patterns:
        body = _remove_sentences_matching(body, pattern)
    return body.strip()


def _remove_available_data_missing_claims_from_article(article: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    """Remove a generated sentence that contradicts Walnut's available-data context."""
    availability = context.get("data_availability") if isinstance(context.get("data_availability"), dict) else {}
    missing_terms = r"(?:not found|not available|unavailable|missing|could not find|couldn't find|not directly reviewed|not independently verified(?: in reviewed primary sources)?)"
    checks = {
        "current price": [r"current\s+\w*\s*price", r"share\s+price", r"stock\s+price"],
        "volume": [r"\bvolume\b"],
        "price/volume and technicals": [r"price\s*/\s*volume", r"\btechnicals?\b", r"technical\s+levels?"],
        "revenue consensus": [r"revenue\s+consensus", r"q[1-4]\s+revenue"],
        "eps consensus": [r"eps\s+consensus", r"q[1-4]\s+eps"],
        "gross margin": [r"gross\s+margin"],
        "free cash flow": [r"free\s+cash\s+flow", r"\bfcf\b"],
        "reported institutional activity": [r"reported\s+institutional\s+activity", r"institutional\s+activity"],
        "insider activity": [r"insider\s+activity"],
        "congress activity": [r"congress\s+activity"],
        "government contracts": [r"government\s+contracts?"],
        "valuation data": [r"valuation\s+data", r"\bvaluation\b"],
    }
    patterns = [
        rf"(?:{'|'.join(synonyms)}).{{0,90}}{missing_terms}|{missing_terms}.{{0,90}}(?:{'|'.join(synonyms)})"
        for field, synonyms in checks.items()
        if availability.get(field)
    ]
    if not patterns:
        return article

    def clean(value: str) -> str:
        cleaned = str(value or "")
        for pattern in patterns:
            cleaned = _remove_sentences_matching(cleaned, pattern)
        return cleaned.strip()

    sanitized = deepcopy(article)
    for key in ("title", "subtitle", "summary", "preview_body"):
        if isinstance(sanitized.get(key), str):
            sanitized[key] = clean(sanitized[key])
    for key in ("key_points", "catalysts", "risks", "watch_items", "data_freshness", "missing_data_notes"):
        if isinstance(sanitized.get(key), list):
            sanitized[key] = [clean(str(item)) for item in sanitized[key] if clean(str(item))]
    for section in sanitized.get("sections") or []:
        if isinstance(section, dict) and isinstance(section.get("body_markdown"), str):
            section["body_markdown"] = clean(section["body_markdown"])
    return sanitized


def _strip_confirmation_score_from_article(article: dict[str, Any]) -> dict[str, Any]:
    cleaned = deepcopy(article)
    for key in ("title", "subtitle", "summary", "preview_body"):
        if isinstance(cleaned.get(key), str):
            stripped = _remove_sentences_matching(str(cleaned[key]), r"\bconfirmation score\b").strip()
            cleaned[key] = stripped
    for key in ("key_points", "catalysts", "risks", "watch_items", "data_freshness", "missing_data_notes"):
        if isinstance(cleaned.get(key), list):
            cleaned[key] = [
                item
                for item in cleaned[key]
                if not re.search(r"\bconfirmation score\b", str(item), flags=re.IGNORECASE)
            ]
    suggested = cleaned.get("suggested_card") if isinstance(cleaned.get("suggested_card"), dict) else None
    if suggested:
        for key in ("title", "description"):
            if isinstance(suggested.get(key), str):
                suggested[key] = _remove_sentences_matching(str(suggested[key]), r"\bconfirmation score\b").strip()
    return cleaned


def _is_earnings_setup_config(config: dict[str, Any]) -> bool:
    text = " ".join(str(config.get(key) or "") for key in ("desired_angle", "research_question", "section_format")).lower()
    return "earnings setup" in text or ("earnings" in text and "setup" in text)


def _apply_earnings_setup_judgment(article: dict[str, Any], config: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    if not _is_earnings_setup_config(config):
        return article
    sanitized = deepcopy(article)
    walnut_call = _infer_earnings_walnut_call(sanitized, context)
    explanation = _earnings_setup_judgment_explanation(walnut_call, sanitized, context)
    call_line = f"Our call: {walnut_call}"
    judgment_block = format_research_numeric_claims("\n".join([call_line, "", explanation]).strip())
    sections = sanitized.get("sections") if isinstance(sanitized.get("sections"), list) else []
    if not sections:
        sanitized["sections"] = [{"key": "the_call", "heading": "The call", "body_markdown": judgment_block}]
    else:
        target_index = _earnings_judgment_section_index(sections)
        if target_index is None:
            sections.append({"key": "the_call", "heading": "The call", "body_markdown": judgment_block})
        else:
            target = dict(sections[target_index])
            target["heading"] = "The call"
            target["key"] = "the_call"
            target["body_markdown"] = _replace_earnings_judgment_block(str(target.get("body_markdown") or ""), call_line, judgment_block)
            sections[target_index] = target
        sanitized["sections"] = sections

    sanitized["walnut_call"] = walnut_call
    sanitized["confirmation_score_included"] = bool(config.get("include_confirmation_score"))
    sanitized["confidence"] = _earnings_setup_confidence(walnut_call, str(sanitized.get("confidence") or ""), context)
    if walnut_call in {"Bearish", "Very bearish"}:
        sanitized["judgment"] = "bearish"
    elif walnut_call in {"Mixed", "Mixed with capex risk", "Neutral", "Neutral but expensive", "Neutral with capex risk", "Insufficient data to make a call"}:
        sanitized["judgment"] = "mixed"
    else:
        sanitized["judgment"] = "bullish"
    return sanitized


def _apply_walnut_call_metadata(article: dict[str, Any]) -> dict[str, Any]:
    sanitized = deepcopy(article)
    call = _walnut_call_from_body(sanitized) or _normalize_walnut_call(sanitized.get("walnut_call"))
    if call is None:
        call = _walnut_call_from_judgment(str(sanitized.get("judgment") or ""))
    sanitized["walnut_call"] = call
    sanitized.pop("setup_label", None)
    if "confirmation_score_included" not in sanitized:
        sanitized["confirmation_score_included"] = _article_confirmation_score_value(sanitized) is not None
    elif _article_confirmation_score_value(sanitized) is not None:
        sanitized["confirmation_score_included"] = True
    return sanitized


def _normalize_walnut_call(value: Any) -> str | None:
    text = re.sub(r"\s+", " ", str(value or "").strip()).lower()
    for allowed in WALNUT_CALL_VALUES:
        if text == allowed.lower():
            return allowed
    return None


def _walnut_call_from_judgment(value: str) -> str:
    lowered = str(value or "").lower()
    if lowered == "bullish":
        return "Bullish"
    if lowered == "bearish":
        return "Bearish"
    if lowered in {"neutral", "macro", "policy"}:
        return "Neutral"
    return "Mixed"


def _earnings_judgment_section_index(sections: list[dict[str, Any]]) -> int | None:
    fallback: int | None = None
    for index, section in enumerate(sections):
        heading_key = _heading_key(str(section.get("heading") or ""))
        if "final walnut judgment" in heading_key:
            return index
        if heading_key in {"the call", "conclusion"} or "judgment" in heading_key:
            fallback = index
    return fallback


def _replace_earnings_judgment_block(body: str, call_line: str, fallback_judgment_block: str) -> str:
    cleaned = re.sub(
        r"(?is)(?:\*\*)?Walnut judgment:\s*(?:clean bullish setup|constructive but expensive|expensive defensive setup|capex-risk setup|mixed\s*/\s*wait for the print|bearish setup|insufficient data)(?:\*\*)?\.?(?:\s+.*?)(?=\n{2,}|$)",
        "",
        body,
    ).strip()
    call_pattern = r"(?im)^(?:\*\*)?(?:Walnut|Our) call:\s*.*?(?:\*\*)?\s*$"
    if re.search(call_pattern, cleaned):
        cleaned = re.sub(call_pattern, call_line, cleaned, count=1).strip()
        output_lines: list[str] = []
        saw_call = False
        for line in cleaned.splitlines():
            if re.match(r"(?i)^\s*(?:Walnut|Our) call:\s*", line):
                if saw_call:
                    continue
                saw_call = True
            else:
                line = re.sub(r"(?i)\s*\b(?:Walnut|Our) call:\s*[^\n.]+\.?", "", line).rstrip()
            output_lines.append(line)
        cleaned = "\n".join(output_lines).strip()
        return format_research_numeric_claims(cleaned.replace("**", "")).strip()
    cleaned = re.sub(r"(?im)^\*\*Setup:\s*.*?\*\*\s*$", "", cleaned).strip()
    cleaned = re.sub(r"(?im)^#+\s*The call:\s*.*wait for the print.*$", "", cleaned).strip()
    cleaned = re.sub(r"(?im)^The call:\s*.*wait for the print.*$", "", cleaned).strip()
    if not cleaned:
        return fallback_judgment_block
    if "wait for the print" in cleaned.lower() and "Mixed" not in fallback_judgment_block:
        cleaned = _remove_sentences_matching(cleaned, r"\bwait for the print\b")
    return format_research_numeric_claims(_merge_markdown_bodies(fallback_judgment_block, cleaned).replace("**", "")).strip()


def _infer_earnings_walnut_call(article: dict[str, Any], context: dict[str, Any]) -> str:
    text = f"{_article_body_text(article)}\n{json.dumps(context, default=str)}".lower()
    primary = context.get("primary") if isinstance(context.get("primary"), dict) else {}
    fundamentals = primary.get("fundamentals") if isinstance(primary.get("fundamentals"), dict) else {}
    financials = primary.get("financials") if isinstance(primary.get("financials"), dict) else {}
    confirmation = primary.get("confirmation") if isinstance(primary.get("confirmation"), dict) else {}
    availability = context.get("data_availability") if isinstance(context.get("data_availability"), dict) else {}

    has_required_primary = bool(primary.get("quote") or primary.get("market_state")) and bool(fundamentals or financials or confirmation)
    if not has_required_primary:
        return "Insufficient data to make a call"

    bullish_business = _earnings_business_strength(fundamentals, confirmation, text)
    bearish_business = _earnings_business_weakness(fundamentals, confirmation, text)
    expensive = _earnings_setup_expensive(fundamentals, financials, text)
    capex_risk = bool(re.search(r"\bcapex\b|capital expenditures?|free cash flow|fcf|reality labs|ai spend|ai infrastructure", text))
    defensive = bool(re.search(r"\bdefensive\b|resilien\w+|services|buybacks?|institutional safety|safe-haven|installed base|franchise", text))
    balanced = bool(re.search(r"\bgenuinely balanced\b|balanced evidence|two-sided|offsetting evidence", text))

    if bearish_business and not bullish_business:
        return "Very bearish" if _earnings_business_very_weak(fundamentals, confirmation, text) else "Bearish"
    if bullish_business and capex_risk:
        return "Mixed with capex risk"
    if bullish_business and expensive and defensive:
        return "Neutral but expensive"
    if bullish_business and expensive:
        return "Bullish but expensive"
    if bullish_business:
        return "Very bullish" if _earnings_business_very_strong(fundamentals, confirmation, text) else "Bullish"
    if balanced or _missing_required_earnings_data(availability):
        return "Mixed"
    if bearish_business:
        return "Bearish"
    return "Neutral"


def _earnings_business_strength(fundamentals: dict[str, Any], confirmation: dict[str, Any], text: str) -> bool:
    direction = str(confirmation.get("direction") or "").lower()
    sources = confirmation.get("sources") if isinstance(confirmation.get("sources"), dict) else {}
    fundamentals_source = sources.get("fundamentals") if isinstance(sources.get("fundamentals"), dict) else {}
    if direction == "bullish" or str(fundamentals_source.get("direction") or "").lower() == "bullish":
        return True
    for key, threshold in (("revenue_growth", 5), ("gross_margin", 35), ("operating_margin", 20), ("roe", 15), ("roic", 10)):
        try:
            value = fundamentals.get(key)
            if value is not None and float(value) >= threshold:
                return True
        except (TypeError, ValueError):
            continue
    return bool(re.search(r"\b(strong|constructive|high-quality|quality franchise|resilient|core business|ad business is strong|services growth)\b", text))


def _earnings_business_weakness(fundamentals: dict[str, Any], confirmation: dict[str, Any], text: str) -> bool:
    direction = str(confirmation.get("direction") or "").lower()
    if direction == "bearish":
        return True
    try:
        revenue_growth = fundamentals.get("revenue_growth")
        if revenue_growth is not None and float(revenue_growth) <= -5:
            return True
    except (TypeError, ValueError):
        pass
    return bool(re.search(r"\bdeteriorat\w+|negative growth|margin pressure|demand weakness|bearish setup\b", text))


def _earnings_business_very_strong(fundamentals: dict[str, Any], confirmation: dict[str, Any], text: str) -> bool:
    try:
        revenue_growth = fundamentals.get("revenue_growth")
        gross_margin = fundamentals.get("gross_margin")
        if revenue_growth is not None and gross_margin is not None and float(revenue_growth) >= 15 and float(gross_margin) >= 45:
            return True
    except (TypeError, ValueError):
        pass
    score = _safe_float(confirmation.get("score") or confirmation.get("confirmation_score"))
    return bool(score is not None and score >= 85 and re.search(r"\b(strong|accelerat\w+|upside|beat and raise)\b", text))


def _earnings_business_very_weak(fundamentals: dict[str, Any], confirmation: dict[str, Any], text: str) -> bool:
    try:
        revenue_growth = fundamentals.get("revenue_growth")
        if revenue_growth is not None and float(revenue_growth) <= -15:
            return True
    except (TypeError, ValueError):
        pass
    score = _safe_float(confirmation.get("score") or confirmation.get("confirmation_score"))
    return bool(score is not None and score <= 25 and re.search(r"\b(deteriorat\w+|guide down|demand weakness|margin collapse)\b", text))


def _earnings_setup_expensive(fundamentals: dict[str, Any], financials: dict[str, Any], text: str) -> bool:
    for key, threshold in (("forward_pe", 25), ("trailing_pe", 30), ("price_to_sales", 6), ("ev_to_ebitda", 20)):
        try:
            value = fundamentals.get(key)
            if value is not None and float(value) >= threshold:
                return True
        except (TypeError, ValueError):
            continue
    return bool(re.search(r"\b(expensive|valuation|multiple|priced for|high expectations|bar is high|earnings bar|premium)\b", text))


def _missing_required_earnings_data(availability: dict[str, Any]) -> bool:
    required = ["current price", "revenue", "eps consensus"]
    missing_count = sum(1 for field in required if availability.get(field) is False)
    return missing_count >= 3


def _earnings_setup_confidence(walnut_call: str, current: str, context: dict[str, Any]) -> str:
    current = current if current in {"low", "medium", "high"} else "medium"
    missing = context.get("missing_data_notes") if isinstance(context.get("missing_data_notes"), list) else []
    if walnut_call == "Insufficient data to make a call" or len(missing) >= 4:
        return "low"
    if missing:
        return "medium" if current == "high" else current
    return current


def _earnings_setup_judgment_explanation(walnut_call: str, article: dict[str, Any], context: dict[str, Any]) -> str:
    symbol = (((context.get("primary") or {}).get("identity") or {}).get("symbol") or article.get("primary_ticker") or "The company")
    symbol = str(symbol).upper()
    business_sentence = _earnings_business_sentence(symbol, context)
    issue = {
        "Very bullish": "The market issue is whether results and guidance can confirm an already strong operating read.",
        "Bullish": "The market issue is whether results and guidance can confirm that strength without a valuation reset.",
        "Bullish but expensive": "The market issue is that valuation and expectations already price in a lot of the resilience.",
        "Neutral": "The market issue is that the business is not broken, but the setup does not justify a cleaner directional call.",
        "Neutral but expensive": "The market issue is that investors are already paying for resilience, recurring revenue quality, buybacks, and institutional safety.",
        "Neutral with capex risk": "The market issue is whether capex intensity and free cash flow conversion can keep pace with the core business strength.",
        "Mixed with capex risk": "The market issue is whether capex intensity and free cash flow conversion overwhelm the stronger parts of the business.",
        "Mixed": "The market issue is that bull and bear evidence are close enough that the print needs to resolve the setup.",
        "Bearish": "The market issue is that the available data leans negative into the print rather than merely uncertain.",
        "Very bearish": "The market issue is that both operating evidence and market setup lean against the stock into the print.",
        "Insufficient data to make a call": "The market issue is that required primary data is unavailable, so a directional call would be false precision.",
    }[walnut_call]
    confirm = {
        "Very bullish": "Confirmation would be upside in the core operating metrics plus guidance that extends the strength; the call breaks if demand or margins roll over.",
        "Bullish": "Confirmation would be upside in the core operating metrics plus supportive guidance; the call breaks if demand or margins roll over.",
        "Bullish but expensive": "Confirmation would be upside to growth or margins that justifies the multiple; the call breaks if guidance is merely in line while expectations stay high.",
        "Neutral": "Confirmation would require a cleaner growth, margin, or guidance signal; the call breaks lower if the print shows demand or margin pressure.",
        "Neutral but expensive": "Confirmation would be durable growth, margin discipline, and measurable AI or product contribution; the call breaks if the print shows safety was already fully priced.",
        "Neutral with capex risk": "Confirmation would be stronger revenue momentum with capex and free cash flow guidance that does not worsen; the call breaks if spending absorbs the upside.",
        "Mixed with capex risk": "Confirmation would be stronger revenue momentum with capex and free cash flow guidance that does not worsen; the call breaks if spending absorbs the upside.",
        "Mixed": "Confirmation would require the print to break the tie in growth, margins, guidance, or cash flow; a weak guide would tilt it bearish.",
        "Bearish": "Confirmation would require management to reverse the weak operating signal with guidance or margin improvement; otherwise the bear case remains in control.",
        "Very bearish": "Confirmation would require a material reversal in operating trends or guidance; otherwise the bear case remains in control.",
        "Insufficient data to make a call": "The call can be revisited once official results, estimate context, current market state, and our confirmation context are available.",
    }[walnut_call]
    return " ".join([business_sentence, issue, confirm])


def _earnings_business_sentence(symbol: str, context: dict[str, Any]) -> str:
    primary = context.get("primary") if isinstance(context.get("primary"), dict) else {}
    fundamentals = primary.get("fundamentals") if isinstance(primary.get("fundamentals"), dict) else {}
    pieces: list[str] = []
    for key, label, suffix in (
        ("revenue_growth", "revenue growth", "%"),
        ("gross_margin", "gross margin", "%"),
        ("operating_margin", "operating margin", "%"),
        ("forward_pe", "forward P/E", "x"),
    ):
        value = fundamentals.get(key)
        if value is not None:
            numeric_value = _safe_float(value)
            formatted = _format_compact_decimal(numeric_value, max_decimals=1) if numeric_value is not None else str(value)
            pieces.append(f"{label} {formatted}{suffix}")
    if pieces:
        return f"The business data for {symbol} is anchored by {', '.join(pieces[:3])}."
    return f"The business data for {symbol} points to the core operating setup, but confidence depends on the available earnings, guidance, margin, and cash flow evidence."


def _strip_confirmation_content_from_section(section: dict[str, Any], *, include_score: bool, include_cross_source: bool) -> dict[str, Any]:
    cleaned = dict(section)
    heading = str(cleaned.get("heading") or "")
    heading_key = _heading_key(heading)
    body = str(cleaned.get("body_markdown") or "")
    if not include_score and ("confirmation score" in heading_key or "walnut confirmation score" in heading_key):
        cleaned["body_markdown"] = ""
        return cleaned
    if not include_cross_source and ("cross source confirmation" in heading_key or "cross source confirmations" in heading_key):
        cleaned["body_markdown"] = ""
        return cleaned
    if not include_score:
        body = _remove_sentences_matching(body, r"\bconfirmation score\b")
    if not include_cross_source:
        body = _remove_sentences_matching(body, r"\bcross-source confirmations?\b|\bcross source confirmations?\b")
    cleaned["body_markdown"] = body.strip()
    return cleaned


def _remove_sentences_matching(text: str, pattern: str) -> str:
    blocks: list[str] = []
    for block in re.split(r"(\n{2,})", str(text or "")):
        if not block or block.startswith("\n"):
            blocks.append(block)
            continue
        if block.lstrip().startswith(("- ", "* ", "1. ")):
            lines = [line for line in block.splitlines() if not re.search(pattern, line, flags=re.IGNORECASE)]
            blocks.append("\n".join(lines))
            continue
        pieces = re.split(r"(?<=[.!?])(\s+)", block)
        kept: list[str] = []
        index = 0
        while index < len(pieces):
            sentence = pieces[index]
            separator = pieces[index + 1] if index + 1 < len(pieces) else ""
            if not re.search(pattern, sentence, flags=re.IGNORECASE):
                kept.append(sentence)
                kept.append(separator)
            index += 2
        blocks.append("".join(kept).strip())
    return re.sub(r"\n{3,}", "\n\n", "".join(blocks)).strip()


def _append_or_merge_generated_section(sections: list[dict[str, Any]], heading: str, body: str, *, key: str) -> list[dict[str, Any]]:
    next_sections = [dict(section) for section in sections]
    target_key = _heading_key(heading)
    for section in next_sections:
        if _heading_key(str(section.get("heading") or "")) == target_key:
            section["body_markdown"] = _merge_markdown_bodies(str(section.get("body_markdown") or ""), body)
            return next_sections
    next_sections.append({"key": key, "heading": heading, "body_markdown": body})
    return next_sections


def _primary_confirmation(context: dict[str, Any]) -> dict[str, Any]:
    primary = context.get("primary") if isinstance(context.get("primary"), dict) else {}
    confirmation = primary.get("confirmation") if isinstance(primary.get("confirmation"), dict) else {}
    return confirmation


def _confirmation_score_value(context: dict[str, Any]) -> int | None:
    confirmation = _primary_confirmation(context)
    value = confirmation.get("score", confirmation.get("confirmation_score"))
    try:
        score = int(round(float(value)))
    except (TypeError, ValueError):
        return None
    return score if score > 0 else None


def _confirmation_score_sentence(context: dict[str, Any]) -> str:
    score = _confirmation_score_value(context)
    if score is None:
        return ""
    confirmation = _primary_confirmation(context)
    direction = str(confirmation.get("direction") or confirmation.get("confirmation_direction") or "").strip().lower()
    direction_text = f" The score direction is {direction}." if direction in {"bullish", "bearish", "neutral", "mixed"} else ""
    return f"Our proprietary confirmation score is {score}/100.{direction_text} This score is separate from the underlying data."


def _cross_source_confirmation_commentary(context: dict[str, Any]) -> str:
    confirmation = _primary_confirmation(context)
    sources = confirmation.get("sources") if isinstance(confirmation.get("sources"), dict) else {}
    supported: list[str] = []
    contradicted: list[str] = []
    mixed: list[str] = []
    for key, source in sources.items():
        if not isinstance(source, dict) or source.get("present") is False or source.get("locked") is True:
            continue
        label = _confirmation_source_label(str(key))
        direction = str(source.get("direction") or "").lower()
        if direction == "bullish":
            supported.append(label)
        elif direction == "bearish":
            contradicted.append(label)
        elif direction == "mixed":
            mixed.append(label)
    clauses: list[str] = []
    if supported:
        clauses.append(f"supported by {', '.join(supported[:4])}")
    if contradicted:
        clauses.append(f"contradicted by {', '.join(contradicted[:4])}")
    if mixed:
        clauses.append(f"mixed in {', '.join(mixed[:4])}")
    if not clauses:
        return "Cross-source confirmations are not strong enough to change the thesis; no single data category should be treated as decisive."
    return f"The setup is {', and '.join(clauses)}. These are underlying data categories and should be read qualitatively."


def _confirmation_source_label(key: str) -> str:
    labels = {
        "price_volume": "price/volume",
        "fundamentals": "fundamentals",
        "institutional_activity": "reported institutional activity",
        "congress": "Congress activity",
        "insiders": "insider activity",
        "government_contracts": "government contracts",
        "options_flow": "options flow",
        "macro_positioning": "macro positioning",
        "signals": "signal data",
    }
    return labels.get(key, key.replace("_", " "))


def _article_body_text(article: dict[str, Any]) -> str:
    sections = article.get("sections") if isinstance(article.get("sections"), list) else []
    return "\n\n".join(
        f"{section.get('heading') or ''}\n{section.get('body_markdown') or ''}"
        for section in sections
        if isinstance(section, dict)
    )


def _article_public_text(article: dict[str, Any]) -> str:
    parts: list[str] = []
    for key in ("title", "subtitle", "summary", "preview_body"):
        if isinstance(article.get(key), str):
            parts.append(str(article[key]))
    for key in ("key_points", "catalysts", "risks", "watch_items", "data_freshness", "missing_data_notes"):
        if isinstance(article.get(key), list):
            parts.extend(str(item) for item in article[key])
    suggested = article.get("suggested_card") if isinstance(article.get("suggested_card"), dict) else {}
    for key in ("title", "description"):
        if isinstance(suggested.get(key), str):
            parts.append(str(suggested[key]))
    parts.append(_article_body_text(article))
    return "\n".join(parts)


def _article_mentions_confirmation_score(article: dict[str, Any]) -> bool:
    return bool(re.search(r"\bconfirmation score\b", _article_public_text(article), flags=re.IGNORECASE))


def _article_confirmation_score_value(article: dict[str, Any]) -> int | None:
    text = _article_public_text(article)
    match = re.search(
        r"\bconfirmation score\b.{0,200}?\b([1-9][0-9]?|100)\s*/\s*100\b|\b([1-9][0-9]?|100)\s*/\s*100\b.{0,200}?\bconfirmation score\b",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not match:
        return None
    value = match.group(1) or match.group(2)
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _article_includes_confirmation_score_value(article: dict[str, Any], context: dict[str, Any]) -> bool:
    score = _confirmation_score_value(context)
    if score is None:
        return _article_confirmation_score_value(article) is not None
    text = _article_public_text(article)
    score_pattern = rf"\b{score}\s*/\s*100\b"
    if not re.search(score_pattern, text):
        return False
    return bool(re.search(rf"confirmation score.{{0,160}}{score_pattern}|{score_pattern}.{{0,160}}confirmation score", text, flags=re.IGNORECASE | re.DOTALL))


def _article_mentions_cross_source_confirmations(article: dict[str, Any]) -> bool:
    return bool(re.search(r"\bcross[- ]source confirmations?\b", _article_body_text(article), flags=re.IGNORECASE))


def _is_earnings_setup_context(context: dict[str, Any]) -> bool:
    text = " ".join(str(context.get(key) or "") for key in ("desired_angle", "research_question", "section_format")).lower()
    if "earnings setup" in text or ("earnings" in text and "setup" in text):
        return True
    config = context.get("config") if isinstance(context.get("config"), dict) else {}
    return _is_earnings_setup_config(config)


def _is_thematic_research(article: dict[str, Any], context: dict[str, Any]) -> bool:
    analytics = article.get("analytics") if isinstance(article.get("analytics"), dict) else {}
    config = context.get("config") if isinstance(context.get("config"), dict) else {}
    text = " ".join(
        str(value or "")
        for value in (
            analytics.get("research_type"),
            analytics.get("theme"),
            context.get("research_type"),
            context.get("theme"),
            config.get("research_type"),
            config.get("theme"),
            context.get("desired_angle"),
            context.get("research_question"),
            article.get("category"),
            article.get("primary_ticker"),
        )
    ).lower()
    return any(
        marker in text
        for marker in (
            "thematic",
            "macro",
            "sector",
            "industry",
            "semiconductor_memory",
            "semiconductor memory",
            "memory shortage",
        )
    )


def _article_earnings_judgment_label(article: dict[str, Any]) -> str | None:
    return _article_walnut_call(article)


def _article_walnut_call(article: dict[str, Any]) -> str | None:
    call = _normalize_walnut_call(article.get("walnut_call"))
    if call:
        return call
    return _walnut_call_from_body(article)


def _walnut_call_from_body(article: dict[str, Any]) -> str | None:
    match = re.search(r"\b(?:Walnut|Our) call:\s*([^\n*]+)", _article_body_text(article), flags=re.IGNORECASE)
    return _normalize_walnut_call(match.group(1) if match else None)


def _read_store() -> dict[str, Any]:
    path = draft_store_path()
    if not path.exists():
        return {"drafts": [], "audit": [], "jobs": []}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"drafts": [], "audit": [], "jobs": []}
    if not isinstance(payload, dict):
        return {"drafts": [], "audit": [], "jobs": []}
    drafts = payload.get("drafts") if isinstance(payload.get("drafts"), list) else []
    audit = payload.get("audit") if isinstance(payload.get("audit"), list) else []
    jobs = payload.get("jobs") if isinstance(payload.get("jobs"), list) else []
    return {"drafts": drafts, "audit": audit, "jobs": jobs}


def _write_store(payload: dict[str, Any]) -> None:
    path = draft_store_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


def _json_dump(value: Any) -> str:
    return json.dumps(value, sort_keys=True, default=str)


def ensure_research_brief_store_schema(db: Session) -> None:
    db.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS research_brief_generation_jobs (
                id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                client_request_id TEXT,
                created_by_admin_id INTEGER,
                created_by_admin_email TEXT,
                ticker TEXT,
                request_payload_json TEXT,
                model TEXT,
                external_research_mode TEXT,
                section_format TEXT,
                generate_thumbnail BOOLEAN,
                progress_step TEXT,
                progress_message TEXT,
                source_links_count INTEGER DEFAULT 0,
                numeric_claims_count INTEGER DEFAULT 0,
                validation_status TEXT,
                draft_id TEXT,
                draft_payload_json TEXT,
                error_message_safe TEXT,
                error_details_internal TEXT,
                duration_ms INTEGER,
                created_at TEXT,
                started_at TEXT,
                updated_at TEXT,
                completed_at TEXT,
                failed_at TEXT
            )
            """
        )
    )
    db.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS research_brief_drafts (
                id TEXT PRIMARY KEY,
                status TEXT,
                created_by INTEGER,
                primary_ticker TEXT,
                slug TEXT,
                campaign_id TEXT,
                campaign_item_id TEXT,
                scheduled_at TEXT,
                approved_at TEXT,
                data_as_of TEXT,
                earnings_period_used TEXT,
                generator_version TEXT,
                last_publish_error TEXT,
                target_keyword TEXT,
                search_intent TEXT,
                index_status TEXT,
                first_seen_indexed_at TEXT,
                last_checked_at TEXT,
                search_console_impressions REAL,
                search_console_clicks REAL,
                average_position REAL,
                updated_at TEXT,
                published_at TEXT,
                payload_json TEXT NOT NULL
            )
            """
        )
    )
    db.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS research_campaigns (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                theme TEXT NOT NULL,
                content_type TEXT NOT NULL,
                active BOOLEAN NOT NULL DEFAULT TRUE,
                cadence TEXT NOT NULL DEFAULT 'one_time',
                config_json TEXT NOT NULL,
                created_by INTEGER,
                created_by_email TEXT,
                generated_count INTEGER NOT NULL DEFAULT 0,
                approved_count INTEGER NOT NULL DEFAULT 0,
                rejected_count INTEGER NOT NULL DEFAULT 0,
                published_count INTEGER NOT NULL DEFAULT 0,
                created_at TEXT,
                updated_at TEXT
            )
            """
        )
    )
    db.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS research_keyword_opportunities (
                id TEXT PRIMARY KEY,
                status TEXT NOT NULL DEFAULT 'new',
                created_by INTEGER,
                created_by_email TEXT,
                target_keyword TEXT NOT NULL,
                opportunity_score INTEGER,
                ticker TEXT,
                topic TEXT,
                discovered_at TEXT,
                updated_at TEXT,
                payload_json TEXT NOT NULL
            )
            """
        )
    )
    db.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS research_campaign_items (
                id TEXT PRIMARY KEY,
                campaign_id TEXT NOT NULL,
                ticker TEXT,
                topic TEXT,
                generate_at TEXT,
                publish_at TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                research_article_id TEXT,
                idempotency_key TEXT,
                generated_at TEXT,
                last_error TEXT,
                target_keyword TEXT,
                created_at TEXT,
                updated_at TEXT
            )
            """
        )
    )
    for column, ddl in {
        "campaign_id": "ALTER TABLE research_brief_drafts ADD COLUMN campaign_id TEXT",
        "campaign_item_id": "ALTER TABLE research_brief_drafts ADD COLUMN campaign_item_id TEXT",
        "scheduled_at": "ALTER TABLE research_brief_drafts ADD COLUMN scheduled_at TEXT",
        "approved_at": "ALTER TABLE research_brief_drafts ADD COLUMN approved_at TEXT",
        "data_as_of": "ALTER TABLE research_brief_drafts ADD COLUMN data_as_of TEXT",
        "earnings_period_used": "ALTER TABLE research_brief_drafts ADD COLUMN earnings_period_used TEXT",
        "generator_version": "ALTER TABLE research_brief_drafts ADD COLUMN generator_version TEXT",
        "last_publish_error": "ALTER TABLE research_brief_drafts ADD COLUMN last_publish_error TEXT",
        "target_keyword": "ALTER TABLE research_brief_drafts ADD COLUMN target_keyword TEXT",
        "search_intent": "ALTER TABLE research_brief_drafts ADD COLUMN search_intent TEXT",
        "index_status": "ALTER TABLE research_brief_drafts ADD COLUMN index_status TEXT",
        "first_seen_indexed_at": "ALTER TABLE research_brief_drafts ADD COLUMN first_seen_indexed_at TEXT",
        "last_checked_at": "ALTER TABLE research_brief_drafts ADD COLUMN last_checked_at TEXT",
        "search_console_impressions": "ALTER TABLE research_brief_drafts ADD COLUMN search_console_impressions REAL",
        "search_console_clicks": "ALTER TABLE research_brief_drafts ADD COLUMN search_console_clicks REAL",
        "average_position": "ALTER TABLE research_brief_drafts ADD COLUMN average_position REAL",
    }.items():
        try:
            db.execute(text(f"ALTER TABLE research_brief_drafts ADD COLUMN IF NOT EXISTS {column} TEXT"))
        except Exception:
            try:
                db.execute(text(ddl))
            except Exception:
                pass
    try:
        db.execute(text("ALTER TABLE research_brief_generation_jobs ADD COLUMN IF NOT EXISTS updated_at TEXT"))
    except Exception:
        try:
            db.execute(text("ALTER TABLE research_brief_generation_jobs ADD COLUMN updated_at TEXT"))
        except Exception:
            pass
    try:
        db.execute(text("ALTER TABLE research_campaign_items ADD COLUMN IF NOT EXISTS target_keyword TEXT"))
    except Exception:
        try:
            db.execute(text("ALTER TABLE research_campaign_items ADD COLUMN target_keyword TEXT"))
        except Exception:
            pass
    # All legacy columns must exist before an index can reference them. In
    # PostgreSQL, a failed CREATE INDEX aborts the transaction and previously
    # published cards would otherwise be hidden by the fallback store.
    db.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS ix_research_brief_jobs_admin_request ON research_brief_generation_jobs (created_by_admin_id, client_request_id)"))
    db.execute(text("CREATE INDEX IF NOT EXISTS ix_research_brief_jobs_status_created ON research_brief_generation_jobs (status, created_at)"))
    db.execute(text("CREATE INDEX IF NOT EXISTS ix_research_brief_drafts_status_updated ON research_brief_drafts (status, updated_at)"))
    db.execute(text("CREATE INDEX IF NOT EXISTS ix_research_brief_drafts_scheduled ON research_brief_drafts (status, scheduled_at)"))
    db.execute(text("CREATE INDEX IF NOT EXISTS ix_research_brief_drafts_keyword ON research_brief_drafts (target_keyword, primary_ticker)"))
    db.execute(text("CREATE INDEX IF NOT EXISTS ix_research_campaigns_active ON research_campaigns (active, updated_at)"))
    db.execute(text("CREATE INDEX IF NOT EXISTS ix_research_campaign_items_due ON research_campaign_items (status, generate_at)"))
    db.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS ix_research_campaign_items_idempotency ON research_campaign_items (idempotency_key)"))
    db.execute(text("CREATE INDEX IF NOT EXISTS ix_research_keyword_opportunities_status_created ON research_keyword_opportunities (status, discovered_at)"))
    db.commit()


def _job_from_row(row: Any) -> dict[str, Any]:
    data = dict(row or {})
    data["request_payload_json"] = _load_json(data.get("request_payload_json")) or {}
    data["draft_payload_json"] = _load_json(data.get("draft_payload_json")) if data.get("draft_payload_json") else None
    data["generate_thumbnail"] = bool(data.get("generate_thumbnail"))
    return data


def _db_job(db: Session, job_id: str) -> dict[str, Any] | None:
    ensure_research_brief_store_schema(db)
    row = db.execute(text("SELECT * FROM research_brief_generation_jobs WHERE id = :id"), {"id": job_id}).mappings().first()
    return _job_from_row(row) if row else None


def _upsert_db_job(db: Session, job: dict[str, Any]) -> None:
    ensure_research_brief_store_schema(db)
    defaults = {
        "source_links_count": 0,
        "numeric_claims_count": 0,
        "validation_status": None,
        "draft_id": None,
        "draft_payload_json": None,
        "error_message_safe": None,
        "error_details_internal": None,
        "duration_ms": None,
        "started_at": None,
        "updated_at": None,
        "completed_at": None,
        "failed_at": None,
    }
    normalized_job = {**defaults, **job}
    normalized_job["updated_at"] = _now()
    params = {
        **normalized_job,
        "request_payload_json": _json_dump(normalized_job.get("request_payload_json") or {}),
        "draft_payload_json": _json_dump(normalized_job.get("draft_payload_json")) if normalized_job.get("draft_payload_json") else None,
        "generate_thumbnail": bool(normalized_job.get("generate_thumbnail")),
    }
    db.execute(
        text(
            """
            INSERT INTO research_brief_generation_jobs (
                id, status, client_request_id, created_by_admin_id, created_by_admin_email, ticker,
                request_payload_json, model, external_research_mode, section_format, generate_thumbnail,
                progress_step, progress_message, source_links_count, numeric_claims_count, validation_status,
                draft_id, draft_payload_json, error_message_safe, error_details_internal, duration_ms,
                created_at, started_at, updated_at, completed_at, failed_at
            ) VALUES (
                :id, :status, :client_request_id, :created_by_admin_id, :created_by_admin_email, :ticker,
                :request_payload_json, :model, :external_research_mode, :section_format, :generate_thumbnail,
                :progress_step, :progress_message, :source_links_count, :numeric_claims_count, :validation_status,
                :draft_id, :draft_payload_json, :error_message_safe, :error_details_internal, :duration_ms,
                :created_at, :started_at, :updated_at, :completed_at, :failed_at
            )
            ON CONFLICT(id) DO UPDATE SET
                status = excluded.status,
                progress_step = excluded.progress_step,
                progress_message = excluded.progress_message,
                source_links_count = excluded.source_links_count,
                numeric_claims_count = excluded.numeric_claims_count,
                validation_status = excluded.validation_status,
                draft_id = excluded.draft_id,
                draft_payload_json = excluded.draft_payload_json,
                error_message_safe = excluded.error_message_safe,
                error_details_internal = excluded.error_details_internal,
                duration_ms = excluded.duration_ms,
                started_at = excluded.started_at,
                updated_at = excluded.updated_at,
                completed_at = excluded.completed_at,
                failed_at = excluded.failed_at
            """
        ),
        params,
    )
    db.commit()


def _upsert_db_draft(db: Session, draft: dict[str, Any]) -> None:
    ensure_research_brief_store_schema(db)
    article = draft.get("article") or {}
    params = {
        "id": draft.get("id"),
        "status": draft.get("status"),
        "created_by": draft.get("created_by"),
        "primary_ticker": draft.get("primary_ticker"),
        "slug": article.get("slug"),
        "campaign_id": draft.get("campaign_id"),
        "campaign_item_id": draft.get("campaign_item_id"),
        "scheduled_at": draft.get("scheduled_at"),
        "approved_at": draft.get("approved_at"),
        "data_as_of": draft.get("data_as_of") or draft.get("research_context_timestamp"),
        "earnings_period_used": draft.get("earnings_period_used"),
        "generator_version": draft.get("generator_version") or draft.get("prompt_version"),
        "last_publish_error": draft.get("last_publish_error"),
        "target_keyword": draft.get("target_keyword") or (draft.get("config") or {}).get("target_keyword"),
        "search_intent": draft.get("search_intent") or (draft.get("config") or {}).get("search_intent"),
        "index_status": draft.get("index_status") or "unknown",
        "first_seen_indexed_at": draft.get("first_seen_indexed_at"),
        "last_checked_at": draft.get("last_checked_at"),
        "search_console_impressions": draft.get("search_console_impressions"),
        "search_console_clicks": draft.get("search_console_clicks"),
        "average_position": draft.get("average_position"),
        "updated_at": draft.get("updated_at"),
        "published_at": draft.get("published_at"),
        "payload_json": _json_dump(draft),
    }
    db.execute(
        text(
            """
            INSERT INTO research_brief_drafts (
                id, status, created_by, primary_ticker, slug, campaign_id, campaign_item_id,
                scheduled_at, approved_at, data_as_of, earnings_period_used, generator_version,
                last_publish_error, target_keyword, search_intent, index_status, first_seen_indexed_at,
                last_checked_at, search_console_impressions, search_console_clicks, average_position,
                updated_at, published_at, payload_json
            )
            VALUES (
                :id, :status, :created_by, :primary_ticker, :slug, :campaign_id, :campaign_item_id,
                :scheduled_at, :approved_at, :data_as_of, :earnings_period_used, :generator_version,
                :last_publish_error, :target_keyword, :search_intent, :index_status, :first_seen_indexed_at,
                :last_checked_at, :search_console_impressions, :search_console_clicks, :average_position,
                :updated_at, :published_at, :payload_json
            )
            ON CONFLICT(id) DO UPDATE SET
                status = excluded.status,
                primary_ticker = excluded.primary_ticker,
                slug = excluded.slug,
                campaign_id = excluded.campaign_id,
                campaign_item_id = excluded.campaign_item_id,
                scheduled_at = excluded.scheduled_at,
                approved_at = excluded.approved_at,
                data_as_of = excluded.data_as_of,
                earnings_period_used = excluded.earnings_period_used,
                generator_version = excluded.generator_version,
                last_publish_error = excluded.last_publish_error,
                target_keyword = excluded.target_keyword,
                search_intent = excluded.search_intent,
                index_status = excluded.index_status,
                first_seen_indexed_at = excluded.first_seen_indexed_at,
                last_checked_at = excluded.last_checked_at,
                search_console_impressions = excluded.search_console_impressions,
                search_console_clicks = excluded.search_console_clicks,
                average_position = excluded.average_position,
                updated_at = excluded.updated_at,
                published_at = excluded.published_at,
                payload_json = excluded.payload_json
            """
        ),
        params,
    )
    db.commit()


def _db_draft(db: Session, draft_id: str) -> dict[str, Any] | None:
    ensure_research_brief_store_schema(db)
    row = db.execute(text("SELECT payload_json FROM research_brief_drafts WHERE id = :id"), {"id": draft_id}).mappings().first()
    payload = _load_json(row["payload_json"]) if row else None
    return payload if isinstance(payload, dict) else None


def _db_drafts(db: Session, status: str | None = None) -> list[dict[str, Any]]:
    ensure_research_brief_store_schema(db)
    if status and status != "all":
        rows = db.execute(text("SELECT payload_json FROM research_brief_drafts WHERE status = :status ORDER BY updated_at DESC"), {"status": status}).mappings().all()
    else:
        rows = db.execute(text("SELECT payload_json FROM research_brief_drafts ORDER BY updated_at DESC")).mappings().all()
    drafts: list[dict[str, Any]] = []
    for row in rows:
        payload = _load_json(row["payload_json"])
        if isinstance(payload, dict):
            drafts.append(payload)
    return drafts


def research_campaign_themes() -> dict[str, Any]:
    return {"items": deepcopy(RESEARCH_CAMPAIGN_THEMES)}


def _keyword_opportunity_from_row(row: Any) -> dict[str, Any]:
    opportunity = _load_json((dict(row or {})).get("payload_json")) or {}
    if not isinstance(opportunity, dict):
        opportunity = {}
    source = dict(row or {})
    opportunity.update(
        {
            "id": source.get("id"),
            "status": source.get("status") or opportunity.get("status") or "new",
            "created_by": source.get("created_by"),
            "created_by_email": source.get("created_by_email"),
            "target_keyword": source.get("target_keyword") or opportunity.get("target_keyword"),
            "opportunity_score": source.get("opportunity_score") if source.get("opportunity_score") is not None else opportunity.get("opportunity_score"),
            "ticker": source.get("ticker") or opportunity.get("ticker"),
            "topic": source.get("topic") or opportunity.get("topic"),
            "discovered_at": source.get("discovered_at") or opportunity.get("discovered_at"),
            "updated_at": source.get("updated_at") or opportunity.get("updated_at"),
        }
    )
    return opportunity


def list_research_keyword_opportunities(db: Session, *, status: str | None = None, limit: int = 50) -> dict[str, Any]:
    ensure_research_brief_store_schema(db)
    normalized_status = str(status or "").strip().lower()
    params: dict[str, Any] = {"limit": max(1, min(100, int(limit or 50)))}
    if normalized_status and normalized_status != "all":
        if normalized_status not in RESEARCH_KEYWORD_OPPORTUNITY_STATUSES:
            raise HTTPException(status_code=422, detail="Unsupported keyword opportunity status.")
        rows = db.execute(
            text("SELECT * FROM research_keyword_opportunities WHERE status = :status ORDER BY discovered_at DESC LIMIT :limit"),
            {**params, "status": normalized_status},
        ).mappings().all()
    else:
        rows = db.execute(text("SELECT * FROM research_keyword_opportunities ORDER BY discovered_at DESC LIMIT :limit"), params).mappings().all()
    return {"items": [_keyword_opportunity_from_row(row) for row in rows]}


def _keyword_discovery_model(db: Session) -> str:
    configured = os.getenv(RESEARCH_KEYWORD_DISCOVERY_MODEL, "").strip()
    return configured or research_brief_model(db)


def _keyword_discovery_prompt(payload: dict[str, Any]) -> str:
    seeds = _dedupe_strings([str(item).strip()[:120] for item in (payload.get("seed_topics") or []) if str(item).strip()])[:12]
    tickers = _dedupe_strings([normalize_symbol(item) for item in (payload.get("tickers") or []) if normalize_symbol(item)])[:12]
    requested_count = max(1, min(RESEARCH_KEYWORD_DISCOVERY_MAX_CANDIDATES, int(payload.get("max_candidates") or 5)))
    theme = next((item for item in RESEARCH_CAMPAIGN_THEMES if item["key"] == str(payload.get("theme") or "").strip().lower()), None)
    seed_text = ", ".join(seeds) if seeds else "No fixed topic; find timely US public-market investor questions."
    ticker_text = ", ".join(tickers) if tickers else "No fixed tickers; prefer names or themes Walnut can support with its data."
    theme_text = f"{theme['label']}: {theme['intent']}" if theme else "No fixed campaign theme; choose the best-fitting research angle."
    return "\n".join(
        [
            "You are Walnut Markets' SEO and answer-engine research strategist.",
            "Use web search before answering. Look for fresh investor attention and explainable search intent using a mix of current reporting, Google Trends pages/results when available, and relevant Reddit discussion. Prefer primary company, filing, regulatory, or reputable market sources for factual claims.",
            "Do not claim exact Google search volume, keyword difficulty, CPC, or Reddit engagement unless a source explicitly provides it. Google Trends is relative-interest evidence only. Treat competition as a directional SERP assessment, not a verified commercial keyword metric.",
            "Only suggest queries Walnut can answer with an original angle from its own evidence: congressional trades, insider activity, institutional ownership, government contracts, confirmations, fundamentals, or price/volume context.",
            "Avoid generic stock-picking queries, unsupported financial promises, and candidates that would merely rewrite a current news headline. Return distinct, answerable long-tail opportunities that have a clear investor question.",
            f"SEED_TOPICS: {seed_text}",
            f"MANUAL_TICKERS: {ticker_text}",
            f"CAMPAIGN_THEME: {theme_text}",
            f"Return up to {requested_count} candidates, ordered from strongest to weakest by editorial opportunity score. Return JSON matching the requested schema. Include 2-4 source URLs per candidate from pages actually used. Give each candidate a 0-100 editorial opportunity score, not a prediction of traffic.",
        ]
    )


def _keyword_opportunity_schema(max_candidates: int = RESEARCH_KEYWORD_DISCOVERY_MAX_CANDIDATES) -> dict[str, Any]:
    candidate = {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "target_keyword", "secondary_keywords", "search_intent", "content_type", "ticker", "topic",
            "recommended_theme", "trend_signal", "competition_assessment", "opportunity_score", "rationale",
            "walnut_angle", "source_urls", "metric_note",
        ],
        "properties": {
            "target_keyword": {"type": "string"},
            "secondary_keywords": {"type": "array", "items": {"type": "string"}, "maxItems": 6},
            "search_intent": {"type": "string", "maxLength": 120},
            "content_type": {"type": "string", "enum": ["ticker", "non_ticker"]},
            "ticker": {"type": "string"},
            "topic": {"type": "string"},
            "recommended_theme": {"type": "string"},
            "trend_signal": {"type": "string", "enum": ["rising", "recent", "evergreen", "unclear"]},
            "competition_assessment": {"type": "string", "enum": ["lower", "moderate", "higher", "unknown"]},
            "opportunity_score": {"type": "integer", "minimum": 0, "maximum": 100},
            "rationale": {"type": "string"},
            "walnut_angle": {"type": "string"},
            "source_urls": {"type": "array", "items": {"type": "string"}, "minItems": 1, "maxItems": 5},
            "metric_note": {"type": "string"},
        },
    }
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["candidates", "market_note"],
        "properties": {
            "candidates": {"type": "array", "items": candidate, "maxItems": max(1, min(RESEARCH_KEYWORD_DISCOVERY_MAX_CANDIDATES, max_candidates))},
            "market_note": {"type": "string"},
        },
    }


def _openai_error_code(response: Any) -> str:
    try:
        payload = response.json()
    except Exception:
        return ""
    error = payload.get("error") if isinstance(payload, dict) else None
    if not isinstance(error, dict):
        return ""
    return str(error.get("code") or error.get("type") or "").strip().lower()


def _raise_openai_response_error(response: Any, *, operation: str) -> None:
    error_code = _openai_error_code(response)
    if response.status_code == 429:
        if error_code in {"credit_balance_exhausted", "insufficient_quota", "billing_hard_limit_reached"}:
            raise HTTPException(
                status_code=503,
                detail="OpenAI API credit balance is exhausted. Add API billing credit or raise the project budget, then try again.",
            )
        headers = getattr(response, "headers", {}) or {}
        retry_after = headers.get("retry-after") if hasattr(headers, "get") else None
        retry_suffix = f" Retry after {retry_after} seconds." if retry_after else " Try again shortly."
        raise HTTPException(status_code=429, detail=f"OpenAI is rate limiting {operation}.{retry_suffix}")
    if response.status_code >= 400:
        raise HTTPException(status_code=502, detail=f"OpenAI {operation} failed. Check the configured model, account access, and request size.")


def _normalize_keyword_candidate(candidate: dict[str, Any]) -> dict[str, Any] | None:
    target_keyword = str(candidate.get("target_keyword") or "").strip()[:240]
    if not target_keyword:
        return None
    content_type = str(candidate.get("content_type") or "non_ticker").strip().lower()
    content_type = content_type if content_type in {"ticker", "non_ticker"} else "non_ticker"
    ticker = normalize_symbol(candidate.get("ticker")) if content_type == "ticker" else ""
    topic = str(candidate.get("topic") or target_keyword).strip()[:300]
    if content_type == "ticker" and not ticker:
        content_type = "non_ticker"
    theme = str(candidate.get("recommended_theme") or "").strip().lower()
    if theme not in {item["key"] for item in RESEARCH_CAMPAIGN_THEMES}:
        theme = "good_buy_now" if content_type == "ticker" else "conflicting_stock_research_data"
    try:
        score = max(0, min(100, int(candidate.get("opportunity_score") or 0)))
    except (TypeError, ValueError):
        score = 0
    return {
        "target_keyword": target_keyword,
        "secondary_keywords": _dedupe_strings([str(item).strip()[:120] for item in (candidate.get("secondary_keywords") or []) if str(item).strip()])[:6],
        "search_intent": str(candidate.get("search_intent") or target_keyword).strip()[:120],
        "content_type": content_type,
        "ticker": ticker or None,
        "topic": topic,
        "recommended_theme": theme,
        "trend_signal": str(candidate.get("trend_signal") or "unclear").strip().lower() if str(candidate.get("trend_signal") or "").strip().lower() in {"rising", "recent", "evergreen", "unclear"} else "unclear",
        "competition_assessment": str(candidate.get("competition_assessment") or "unknown").strip().lower() if str(candidate.get("competition_assessment") or "").strip().lower() in {"lower", "moderate", "higher", "unknown"} else "unknown",
        "opportunity_score": score,
        "rationale": str(candidate.get("rationale") or "").strip()[:1200],
        "walnut_angle": str(candidate.get("walnut_angle") or "").strip()[:800],
        "source_urls": _dedupe_strings([str(item).strip()[:1000] for item in (candidate.get("source_urls") or []) if str(item).strip().startswith(("http://", "https://"))])[:5],
        "metric_note": str(candidate.get("metric_note") or "Directional assessment only; no verified keyword-volume provider is connected.").strip()[:500],
    }


def discover_research_keyword_opportunities(db: Session, admin: UserAccount, payload: dict[str, Any]) -> dict[str, Any]:
    ensure_research_brief_store_schema(db)
    requested_count = max(1, min(RESEARCH_KEYWORD_DISCOVERY_MAX_CANDIDATES, int(payload.get("max_candidates") or 5)))
    api_key = resolved_setting_value(db, OPENAI_API_KEY)
    if not api_key:
        raise HTTPException(status_code=503, detail="OpenAI API key missing. Configure OPENAI_API_KEY before discovering keyword opportunities.")
    response = requests.post(
        RESPONSES_ENDPOINT,
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json={
            "model": _keyword_discovery_model(db),
            "input": _keyword_discovery_prompt(payload),
            "tools": [{"type": "web_search", "search_context_size": "medium"}],
            "tool_choice": "required",
            "store": False,
            "max_output_tokens": 5000,
            "text": {"format": {"type": "json_schema", "name": "walnut_keyword_opportunities", "schema": _keyword_opportunity_schema(requested_count), "strict": True}},
        },
        timeout=_env_float(RESEARCH_BRIEF_OPENAI_TIMEOUT_SECONDS, 90.0),
    )
    if response.status_code >= 400:
        logger.warning("research_keyword_discovery_failed status=%s body=%s", response.status_code, response.text[:500])
        _raise_openai_response_error(response, operation="keyword discovery")
    try:
        parsed = json.loads(_response_text(response.json()))
    except Exception as exc:
        raise HTTPException(status_code=502, detail="OpenAI returned invalid keyword opportunity JSON.") from exc
    if not isinstance(parsed, dict):
        raise HTTPException(status_code=502, detail="OpenAI returned an invalid keyword opportunity payload.")
    created_at = _now()
    opportunities: list[dict[str, Any]] = []
    for raw_candidate in (parsed.get("candidates") or [])[:requested_count]:
        candidate = _normalize_keyword_candidate(raw_candidate) if isinstance(raw_candidate, dict) else None
        if not candidate:
            continue
        opportunity_id = f"rko_{uuid.uuid4().hex}"
        candidate.update({"id": opportunity_id, "status": "new", "discovered_at": created_at, "updated_at": created_at})
        db.execute(
            text(
                """
                INSERT INTO research_keyword_opportunities (
                    id, status, created_by, created_by_email, target_keyword, opportunity_score, ticker, topic,
                    discovered_at, updated_at, payload_json
                ) VALUES (
                    :id, :status, :created_by, :created_by_email, :target_keyword, :opportunity_score, :ticker, :topic,
                    :discovered_at, :updated_at, :payload_json
                )
                """
            ),
            {
                **candidate,
                "created_by": admin.id,
                "created_by_email": getattr(admin, "email", None),
                "payload_json": _json_dump(candidate),
            },
        )
        opportunities.append(candidate)
    db.commit()
    return {
        "items": opportunities,
        "market_note": str(parsed.get("market_note") or ""),
        "metric_provider_configured": False,
        "metric_provider_note": "Google Trends and web search provide relative demand and SERP evidence. Connect a licensed keyword-metrics provider before treating competition or volume as verified.",
    }


def _keyword_opportunity_regeneration_prompt(opportunity: dict[str, Any], instructions: str) -> str:
    """Ask for one replacement opportunity while retaining the useful original context."""
    original = {
        key: opportunity.get(key)
        for key in (
            "target_keyword", "secondary_keywords", "search_intent", "content_type", "ticker", "topic",
            "recommended_theme", "trend_signal", "competition_assessment", "opportunity_score", "rationale",
            "walnut_angle", "source_urls", "metric_note",
        )
    }
    requested_changes = instructions.strip() or "Find a meaningfully different, stronger angle while preserving the relevant ticker or topic when it is still justified by current signals."
    return "\n".join(
        [
            "You are revising one saved Walnut Markets SEO and answer-engine keyword opportunity.",
            "Use web search before answering. Re-check current reporting, Google Trends pages/results when available, and relevant Reddit discussions. Prefer primary company, filing, regulatory, or reputable market sources for factual claims.",
            "Return exactly one replacement candidate. It must be a distinct, answerable long-tail investor query that Walnut can support with original evidence (congressional trades, insider activity, institutional ownership, government contracts, confirmations, fundamentals, or price/volume context).",
            "Do not claim exact search volume, keyword difficulty, CPC, or Reddit engagement unless a source explicitly provides it. Treat Google Trends and competition as directional evidence only.",
            "Keep search_intent at 120 characters or fewer. Include 2-4 source URLs from pages actually used. Return JSON matching the requested schema.",
            f"SAVED_OPPORTUNITY: {_json_dump(original)}",
            f"EDITOR_INSTRUCTIONS: {requested_changes[:2000]}",
        ]
    )


def regenerate_research_keyword_opportunity(
    db: Session,
    admin: UserAccount,
    opportunity_id: str,
    instructions: str | None = None,
) -> dict[str, Any]:
    """Replace a saved, uncommitted opportunity in place after a fresh web-grounded pass."""
    ensure_research_brief_store_schema(db)
    row = db.execute(text("SELECT * FROM research_keyword_opportunities WHERE id = :id"), {"id": opportunity_id}).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="Keyword opportunity not found.")
    existing = _keyword_opportunity_from_row(row)
    if existing.get("status") != "new":
        raise HTTPException(status_code=409, detail="Only saved, unused keyword opportunities can be regenerated.")
    api_key = resolved_setting_value(db, OPENAI_API_KEY)
    if not api_key:
        raise HTTPException(status_code=503, detail="OpenAI API key missing. Configure OPENAI_API_KEY before regenerating a keyword opportunity.")
    response = requests.post(
        RESPONSES_ENDPOINT,
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json={
            "model": _keyword_discovery_model(db),
            "input": _keyword_opportunity_regeneration_prompt(existing, str(instructions or "")),
            "tools": [{"type": "web_search", "search_context_size": "medium"}],
            "tool_choice": "required",
            "store": False,
            "max_output_tokens": 2600,
            "text": {"format": {"type": "json_schema", "name": "walnut_keyword_opportunity_revision", "schema": _keyword_opportunity_schema(1), "strict": True}},
        },
        timeout=_env_float(RESEARCH_BRIEF_OPENAI_TIMEOUT_SECONDS, 90.0),
    )
    if response.status_code >= 400:
        logger.warning("research_keyword_regeneration_failed opportunity_id=%s status=%s body=%s", opportunity_id, response.status_code, response.text[:500])
        _raise_openai_response_error(response, operation="keyword opportunity regeneration")
    try:
        parsed = json.loads(_response_text(response.json()))
    except Exception as exc:
        raise HTTPException(status_code=502, detail="OpenAI returned invalid regenerated keyword opportunity JSON.") from exc
    candidates = parsed.get("candidates") if isinstance(parsed, dict) else None
    candidate = _normalize_keyword_candidate(candidates[0]) if isinstance(candidates, list) and candidates and isinstance(candidates[0], dict) else None
    if not candidate:
        raise HTTPException(status_code=502, detail="OpenAI did not return a usable regenerated keyword opportunity.")
    now = _now()
    revision_count = max(0, int(existing.get("revision_count") or 0)) + 1
    candidate.update(
        {
            "id": opportunity_id,
            "status": "new",
            "created_by": existing.get("created_by") or admin.id,
            "created_by_email": existing.get("created_by_email") or getattr(admin, "email", None),
            "discovered_at": existing.get("discovered_at") or now,
            "updated_at": now,
            "revision_count": revision_count,
            "last_revision_instructions": str(instructions or "").strip()[:2000] or None,
        }
    )
    db.execute(
        text(
            """
            UPDATE research_keyword_opportunities
            SET target_keyword = :target_keyword, opportunity_score = :opportunity_score, ticker = :ticker, topic = :topic,
                updated_at = :updated_at, payload_json = :payload_json
            WHERE id = :id
            """
        ),
        {**candidate, "payload_json": _json_dump(candidate)},
    )
    db.commit()
    return candidate


def update_research_keyword_opportunity_status(db: Session, opportunity_id: str, status: str) -> dict[str, Any]:
    ensure_research_brief_store_schema(db)
    normalized_status = str(status or "").strip().lower()
    if normalized_status not in RESEARCH_KEYWORD_OPPORTUNITY_STATUSES:
        raise HTTPException(status_code=422, detail="Keyword opportunity status must be new, used, or dismissed.")
    result = db.execute(
        text("UPDATE research_keyword_opportunities SET status = :status, updated_at = :updated_at WHERE id = :id"),
        {"id": opportunity_id, "status": normalized_status, "updated_at": _now()},
    )
    db.commit()
    if getattr(result, "rowcount", 0) != 1:
        raise HTTPException(status_code=404, detail="Keyword opportunity not found.")
    row = db.execute(text("SELECT * FROM research_keyword_opportunities WHERE id = :id"), {"id": opportunity_id}).mappings().first()
    return _keyword_opportunity_from_row(row)


def _campaign_theme(theme: Any) -> dict[str, Any]:
    key = str(theme or "").strip().lower()
    for item in RESEARCH_CAMPAIGN_THEMES:
        if item["key"] == key:
            return item
    raise HTTPException(status_code=422, detail="Unsupported research campaign theme.")


def _campaign_from_row(row: Any) -> dict[str, Any]:
    campaign = dict(row or {})
    campaign["active"] = bool(campaign.get("active"))
    campaign["config"] = _load_json(campaign.pop("config_json", None)) or {}
    return campaign


def _campaign_item_from_row(row: Any) -> dict[str, Any]:
    return dict(row or {})


def _normalize_campaign_payload(payload: dict[str, Any]) -> dict[str, Any]:
    theme = _campaign_theme(payload.get("theme"))
    content_type = str(payload.get("content_type") or theme.get("content_type") or "ticker").strip().lower()
    if content_type not in {"ticker", "non_ticker"}:
        raise HTTPException(status_code=422, detail="Campaign content type must be ticker or non_ticker.")
    cadence = str(payload.get("cadence") or "one_time").strip().lower()
    if cadence not in {"one_time", "daily", "weekly", "custom"}:
        raise HTTPException(status_code=422, detail="Unsupported campaign cadence.")
    tickers = [normalize_symbol(item) for item in (payload.get("tickers") or []) if normalize_symbol(item)]
    topic = str(payload.get("topic") or "").strip()
    if content_type == "ticker" and not tickers:
        raise HTTPException(status_code=422, detail="At least one ticker is required.")
    if content_type == "non_ticker" and not topic:
        topic = str(theme.get("intent") or theme.get("label") or "Walnut research topic").strip()
    try:
        window_days = max(1, min(30, int(payload.get("window_days") or 1)))
    except (TypeError, ValueError):
        window_days = 1
    try:
        article_count = max(1, min(50, int(payload.get("article_count") or (len(tickers) if content_type == "ticker" else 1))))
    except (TypeError, ValueError):
        article_count = len(tickers) if content_type == "ticker" else 1
    publish_start_at = _parse_schedule_datetime(payload.get("publish_start_at")) or datetime.now(timezone.utc)
    target_keyword = str(payload.get("target_keyword") or "").strip()[:240]
    secondary_keywords = _dedupe_strings([str(item).strip()[:120] for item in (payload.get("secondary_keywords") or []) if str(item).strip()])[:12]
    target_keywords = {
        normalize_symbol(symbol): str(keyword).strip()[:240]
        for symbol, keyword in (payload.get("target_keywords") or {}).items()
        if normalize_symbol(symbol) and str(keyword).strip()
    }
    target_search_intents = {
        normalize_symbol(symbol): str(intent).strip()[:120]
        for symbol, intent in (payload.get("target_search_intents") or {}).items()
        if normalize_symbol(symbol) and str(intent).strip()
    }
    source_opportunity_ids = _dedupe_strings(
        [str(opportunity_id).strip()[:100] for opportunity_id in (payload.get("source_opportunity_ids") or []) if str(opportunity_id).strip()]
    )[:50]
    return {
        "name": str(payload.get("name") or theme["label"]).strip()[:180],
        "theme": theme["key"],
        "theme_label": theme["label"],
        "content_type": content_type,
        "active": bool(payload.get("active", True)),
        "cadence": cadence,
        "tickers": tickers,
        "topic": topic,
        "article_count": article_count,
        "window_days": window_days,
        "publish_start_at": publish_start_at.isoformat(),
        "publish_time": str(payload.get("publish_time") or "").strip()[:20],
        "target_keyword": target_keyword,
        "secondary_keywords": secondary_keywords,
        "search_intent": str(payload.get("search_intent") or theme.get("intent") or "").strip()[:120],
        "target_keywords": target_keywords,
        "target_search_intents": target_search_intents,
        "source_opportunity_ids": source_opportunity_ids,
    }


def _parse_schedule_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if not isinstance(value, str) or not value.strip():
        return None
    raw = value.strip()
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        try:
            parsed = datetime.fromisoformat(f"{raw}T09:00:00+00:00")
        except ValueError:
            return None
    return parsed.astimezone(timezone.utc) if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _distributed_publish_times(start: datetime, count: int, window_days: int) -> list[datetime]:
    count = max(1, count)
    window_days = max(1, window_days)
    if count == 1:
        return [start]
    day_span = max(1, window_days - 1)
    return [start + timedelta(days=round(index * day_span / max(1, count - 1))) for index in range(count)]


def create_research_campaign(db: Session, admin: UserAccount, payload: dict[str, Any]) -> dict[str, Any]:
    ensure_research_brief_store_schema(db)
    config = _normalize_campaign_payload(payload)
    source_opportunity_ids = config["source_opportunity_ids"]
    opportunity_params = {f"opportunity_{index}": opportunity_id for index, opportunity_id in enumerate(source_opportunity_ids)}
    placeholders = ", ".join(f":{key}" for key in opportunity_params)
    if source_opportunity_ids:
        selected_rows = db.execute(
            text(
                f"SELECT id FROM research_keyword_opportunities "
                f"WHERE id IN ({placeholders}) AND status = 'new' AND created_by = :created_by"
            ),
            {**opportunity_params, "created_by": admin.id},
        ).mappings().all()
        if {str(row["id"]) for row in selected_rows} != set(source_opportunity_ids):
            raise HTTPException(status_code=409, detail="One or more selected keyword opportunities are no longer available. Refresh the campaign plan and try again.")
    now = _now()
    campaign_id = f"rc_{uuid.uuid4().hex}"
    db.execute(
        text(
            """
            INSERT INTO research_campaigns (
                id, name, theme, content_type, active, cadence, config_json,
                created_by, created_by_email, created_at, updated_at
            ) VALUES (
                :id, :name, :theme, :content_type, :active, :cadence, :config_json,
                :created_by, :created_by_email, :created_at, :updated_at
            )
            """
        ),
        {
            "id": campaign_id,
            "name": config["name"],
            "theme": config["theme"],
            "content_type": config["content_type"],
            "active": config["active"],
            "cadence": config["cadence"],
            "config_json": _json_dump(config),
            "created_by": admin.id,
            "created_by_email": getattr(admin, "email", None),
            "created_at": now,
            "updated_at": now,
        },
    )
    targets = config["tickers"] if config["content_type"] == "ticker" else [config["topic"]]
    targets = targets[: config["article_count"]]
    publish_times = _distributed_publish_times(_parse_schedule_datetime(config["publish_start_at"]) or datetime.now(timezone.utc), len(targets), config["window_days"])
    for index, target in enumerate(targets):
        publish_at = publish_times[index]
        generate_at = min(datetime.now(timezone.utc), publish_at - timedelta(hours=18))
        item_id = f"rci_{uuid.uuid4().hex}"
        ticker = target if config["content_type"] == "ticker" else None
        topic = None if config["content_type"] == "ticker" else target
        target_keyword = (
            config["target_keywords"].get(ticker)
            if ticker
            else config["target_keyword"]
        ) or (f"{ticker} stock buy now" if ticker and config["theme"] == "good_buy_now" else config["target_keyword"])
        db.execute(
            text(
                """
                INSERT INTO research_campaign_items (
                    id, campaign_id, ticker, topic, generate_at, publish_at, status,
                    idempotency_key, target_keyword, created_at, updated_at
                ) VALUES (
                    :id, :campaign_id, :ticker, :topic, :generate_at, :publish_at, 'pending',
                    :idempotency_key, :target_keyword, :created_at, :updated_at
                )
                """
            ),
            {
                "id": item_id,
                "campaign_id": campaign_id,
                "ticker": ticker,
                "topic": topic,
                "generate_at": generate_at.isoformat(),
                "publish_at": publish_at.isoformat(),
                "idempotency_key": f"{campaign_id}:{ticker or topic}:{publish_at.isoformat()}",
                "target_keyword": target_keyword,
                "created_at": now,
                "updated_at": now,
            },
        )
    if source_opportunity_ids:
        db.execute(
            text(f"UPDATE research_keyword_opportunities SET status = 'used', updated_at = :updated_at WHERE id IN ({placeholders})"),
            {**opportunity_params, "updated_at": now},
        )
    db.commit()
    return get_research_campaign(db, campaign_id)


def get_research_campaign(db: Session, campaign_id: str) -> dict[str, Any]:
    ensure_research_brief_store_schema(db)
    row = db.execute(text("SELECT * FROM research_campaigns WHERE id = :id"), {"id": campaign_id}).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="Research campaign not found.")
    campaign = _campaign_from_row(row)
    items = db.execute(text("SELECT * FROM research_campaign_items WHERE campaign_id = :id ORDER BY publish_at, created_at"), {"id": campaign_id}).mappings().all()
    campaign["items"] = [_campaign_item_from_row(item) for item in items]
    return campaign


def list_research_campaigns(db: Session) -> dict[str, Any]:
    ensure_research_brief_store_schema(db)
    rows = db.execute(text("SELECT * FROM research_campaigns ORDER BY updated_at DESC, created_at DESC")).mappings().all()
    items = []
    for row in rows:
        campaign = _campaign_from_row(row)
        item_rows = db.execute(
            text("SELECT * FROM research_campaign_items WHERE campaign_id = :id ORDER BY publish_at, created_at"),
            {"id": campaign["id"]},
        ).mappings().all()
        campaign["item_count"] = len(item_rows)
        campaign["pending_count"] = sum(1 for item in item_rows if item.get("status") == "pending")
        campaign["items"] = [_campaign_item_from_row(item) for item in item_rows]
        items.append(campaign)
    return {"items": items}


def set_research_campaign_active(db: Session, campaign_id: str, active: bool) -> dict[str, Any]:
    ensure_research_brief_store_schema(db)
    result = db.execute(text("UPDATE research_campaigns SET active = :active, updated_at = :updated_at WHERE id = :id"), {"active": bool(active), "updated_at": _now(), "id": campaign_id})
    db.commit()
    if getattr(result, "rowcount", 0) == 0:
        raise HTTPException(status_code=404, detail="Research campaign not found.")
    return get_research_campaign(db, campaign_id)


def delete_research_campaign(db: Session, campaign_id: str) -> dict[str, Any]:
    ensure_research_brief_store_schema(db)
    db.execute(text("DELETE FROM research_campaign_items WHERE campaign_id = :id AND status = 'pending'"), {"id": campaign_id})
    result = db.execute(text("DELETE FROM research_campaigns WHERE id = :id"), {"id": campaign_id})
    db.commit()
    if getattr(result, "rowcount", 0) == 0:
        raise HTTPException(status_code=404, detail="Research campaign not found.")
    return {"ok": True, "deleted": campaign_id}


def run_research_campaign_now(db: Session, campaign_id: str, *, limit: int = 10) -> dict[str, Any]:
    ensure_research_brief_store_schema(db)
    db.execute(text("UPDATE research_campaign_items SET generate_at = :now, updated_at = :now WHERE campaign_id = :campaign_id AND status = 'pending'"), {"now": _now(), "campaign_id": campaign_id})
    db.commit()
    return run_due_research_campaign_generation(db, limit=limit, campaign_id=campaign_id)


def reschedule_research_campaign_item(db: Session, campaign_id: str, item_id: str, publish_at: str) -> dict[str, Any]:
    ensure_research_brief_store_schema(db)
    parsed = _parse_schedule_datetime(publish_at)
    if not parsed:
        raise HTTPException(status_code=422, detail="A valid publish_at timestamp is required.")
    row = db.execute(
        text("SELECT * FROM research_campaign_items WHERE id = :id AND campaign_id = :campaign_id"),
        {"id": item_id, "campaign_id": campaign_id},
    ).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="Campaign item not found.")
    if str(row.get("status") or "") != "pending":
        raise HTTPException(status_code=409, detail="Only pending campaign items can be rescheduled here. Open the generated draft to change its schedule.")
    now = datetime.now(timezone.utc)
    generate_at = min(now, parsed - timedelta(hours=18))
    db.execute(
        text("UPDATE research_campaign_items SET publish_at = :publish_at, generate_at = :generate_at, updated_at = :updated_at WHERE id = :id"),
        {"id": item_id, "publish_at": parsed.isoformat(), "generate_at": generate_at.isoformat(), "updated_at": _now()},
    )
    db.commit()
    updated = db.execute(text("SELECT * FROM research_campaign_items WHERE id = :id"), {"id": item_id}).mappings().first()
    return _campaign_item_from_row(updated)


def run_research_campaign_item_now(db: Session, campaign_id: str, item_id: str) -> dict[str, Any]:
    ensure_research_brief_store_schema(db)
    result = db.execute(
        text(
            "UPDATE research_campaign_items SET generate_at = :now, updated_at = :now "
            "WHERE id = :id AND campaign_id = :campaign_id AND status = 'pending'"
        ),
        {"id": item_id, "campaign_id": campaign_id, "now": _now()},
    )
    db.commit()
    if getattr(result, "rowcount", 0) != 1:
        raise HTTPException(status_code=409, detail="This campaign item is no longer pending. Refresh the schedule and try again.")
    return run_due_research_campaign_generation(db, limit=1, campaign_id=campaign_id, item_id=item_id)


def run_due_research_campaign_generation(
    db: Session,
    *,
    limit: int = 10,
    campaign_id: str | None = None,
    item_id: str | None = None,
) -> dict[str, Any]:
    ensure_research_brief_store_schema(db)
    where_campaign = "AND c.id = :campaign_id" if campaign_id else ""
    where_item = "AND i.id = :item_id" if item_id else ""
    rows = db.execute(
        text(
            f"""
            SELECT i.*, c.name AS campaign_name, c.theme AS campaign_theme, c.content_type AS campaign_content_type,
                   c.config_json AS campaign_config_json, c.created_by AS campaign_created_by, c.created_by_email AS campaign_created_by_email
            FROM research_campaign_items i
            JOIN research_campaigns c ON c.id = i.campaign_id
            WHERE i.status = 'pending' AND c.active = TRUE AND i.generate_at <= :now {where_campaign} {where_item}
            ORDER BY i.generate_at ASC
            LIMIT :limit
            """
        ),
        {"now": _now(), "limit": max(1, min(50, limit)), "campaign_id": campaign_id, "item_id": item_id},
    ).mappings().all()
    generated = 0
    failed = 0
    skipped = 0
    for row in rows:
        item_id = str(row["id"])
        claim = db.execute(text("UPDATE research_campaign_items SET status = 'generating', updated_at = :now WHERE id = :id AND status = 'pending'"), {"now": _now(), "id": item_id})
        db.commit()
        if getattr(claim, "rowcount", 0) != 1:
            skipped += 1
            continue
        try:
            _generate_research_campaign_item(db, row)
            generated += 1
        except Exception as exc:
            failed += 1
            db.execute(
                text("UPDATE research_campaign_items SET status = 'failed', last_error = :error, updated_at = :now WHERE id = :id"),
                {"id": item_id, "error": f"{exc.__class__.__name__}: {str(exc)[:500]}", "now": _now()},
            )
            db.commit()
            logger.warning("research_campaign_item_failed item_id=%s error=%s", item_id, exc.__class__.__name__, exc_info=True)
    return {"generated": generated, "failed": failed, "skipped": skipped, "checked": len(rows)}


def _generate_research_campaign_item(db: Session, row: Any) -> dict[str, Any]:
    item = _campaign_item_from_row(row)
    if item.get("research_article_id"):
        return get_draft(str(item["research_article_id"]), db=db)
    admin = db.get(UserAccount, item.get("campaign_created_by")) if item.get("campaign_created_by") else None
    if not admin:
        raise HTTPException(status_code=404, detail="Campaign admin account not found.")
    campaign_config = _load_json(item.get("campaign_config_json")) or {}
    content_type = str(item.get("campaign_content_type") or campaign_config.get("content_type") or "ticker")
    if content_type != "ticker":
        draft = _generate_non_ticker_campaign_stub(db, admin, item, campaign_config)
    else:
        config = _campaign_item_generation_config(item, campaign_config)
        draft, correction_notes = _generate_campaign_brief_with_corrections(db, admin, config)
        if correction_notes:
            draft["quality_gate_correction_note"] = correction_notes[-1]
    draft = _mark_draft_scheduled_review(draft, item, campaign_config)
    _upsert_db_draft(db, draft)
    db.execute(
        text(
            """
            UPDATE research_campaign_items
            SET status = 'generated', research_article_id = :draft_id, generated_at = :generated_at, updated_at = :updated_at, last_error = NULL
            WHERE id = :id
            """
        ),
        {"id": item["id"], "draft_id": draft["id"], "generated_at": draft["generated_at"], "updated_at": _now()},
    )
    db.execute(text("UPDATE research_campaigns SET generated_count = generated_count + 1, updated_at = :now WHERE id = :id"), {"id": item["campaign_id"], "now": _now()})
    db.commit()
    send_research_campaign_review_email(db, admin, draft, item, campaign_config)
    return draft


def _quality_gate_correction_note(validation_error: str) -> str:
    detail = str(validation_error or "").strip()[:500]
    return (
        "Walnut quality-gate correction note: the previous draft was rejected before review. "
        f"Correct this specific validation issue: {detail}. "
        "Regenerate the entire brief using only supported source-backed claims, and do not repeat the rejected wording."
    )


def _campaign_generation_correction_note(exc: HTTPException) -> str | None:
    detail = str(exc.detail or "").strip()
    if exc.status_code == 422 and detail.startswith("Draft generation failed validation."):
        return _quality_gate_correction_note(detail)
    if exc.status_code == 502 and detail == "OpenAI returned invalid structured research JSON.":
        return (
            "Walnut structured-output correction note: the previous response could not be parsed as JSON. "
            "Return exactly one complete RFC 8259 JSON object that conforms to the supplied schema. "
            "Do not include Markdown fences, commentary, or unescaped newlines or quotes inside string values."
        )
    return None


def _generate_campaign_brief_with_corrections(
    db: Session,
    admin: UserAccount,
    config: dict[str, Any],
) -> tuple[dict[str, Any], list[str]]:
    """Generate a campaign draft with at most two feedback-driven corrective retries."""
    retry_config = deepcopy(config)
    correction_notes: list[str] = []
    for attempt in range(3):
        try:
            return generate_research_brief(db, admin, retry_config), correction_notes
        except HTTPException as exc:
            correction_note = _campaign_generation_correction_note(exc)
            if not correction_note or attempt == 2:
                raise
            correction_notes.append(correction_note)
            prior_context = str(retry_config.get("additional_context") or "").strip()
            retry_config["additional_context"] = f"{prior_context}\n\n{correction_note}".strip()[:4000]
            # Corrective campaign attempts need room to complete the full strict JSON payload.
            retry_config["retry_output_tokens"] = 10000
    raise RuntimeError("Campaign generation retry loop unexpectedly completed.")


def _campaign_item_generation_config(item: dict[str, Any], campaign_config: dict[str, Any]) -> dict[str, Any]:
    ticker = normalize_symbol(item.get("ticker"))
    theme = _campaign_theme(campaign_config.get("theme"))
    title_intent = str(theme.get("intent") or "").replace("[TICKER]", ticker)
    if theme["key"] == "good_buy_now":
        question = f"Is {ticker} a good stock to buy right now after the latest earnings and current Walnut data?"
        angle = "Post-earnings review"
    else:
        question = title_intent or f"What does current Walnut data say about {ticker}?"
        angle = "Full company DD"
    return {
        "ticker": ticker,
        "research_question": question,
        "desired_angle": angle,
        "time_horizon": "Near term",
        "intended_audience": "Walnut Research Brief",
        "judgment_preference": "Let the data decide",
        "additional_context": (
            f"Campaign theme: {theme['label']}. Write a concise SEO/AEO research brief. "
            "Use the latest earnings/company data available at generation time, current Walnut-native data, the current confirmation score, fundamentals, technical context, and only relevant optional datasets. "
            "Do not force sections for unavailable or irrelevant data."
        ),
        "include_sections": [
            "Executive thesis",
            "What changed",
            "Business and fundamentals",
            "Valuation",
            "Price / volume and technicals",
            "Catalysts",
            "Risks",
            "What to watch next",
            "Final Walnut judgment",
        ],
        "length": "Standard: 1,500-2,500 words",
        "tone": "Walnut market-native",
        "external_research_mode": "Standard",
        "section_format": "Walnut Research Brief",
        "include_charts": False,
        "include_source_links": True,
        "include_confirmation_score": True,
        "include_cross_source_confirmations": True,
        "premium_required": False,
        "required_plan": None,
        "generate_thumbnail": True,
        "selected_model": "",
        "manual_source_url": "",
        "target_keyword": item.get("target_keyword") or campaign_config.get("target_keyword") or f"{ticker} stock buy now",
        "secondary_keywords": campaign_config.get("secondary_keywords") or [],
        "search_intent": campaign_config.get("target_search_intents", {}).get(ticker) or campaign_config.get("search_intent") or title_intent,
        "content_type": "ticker",
    }


def _generate_non_ticker_campaign_stub(db: Session, admin: UserAccount, item: dict[str, Any], campaign_config: dict[str, Any]) -> dict[str, Any]:
    created = _now()
    topic = str(item.get("topic") or campaign_config.get("topic") or campaign_config.get("theme_label") or "Walnut research").strip()
    slug = _slugify(topic, fallback="walnut-research-brief")
    article = {
        "title": topic,
        "slug": slug,
        "subtitle": "A Walnut research brief queued for editorial generation.",
        "summary": "This non-ticker campaign item is queued for Walnut-native dataset research and editorial completion.",
        "preview_body": "This non-ticker campaign item is queued for Walnut-native dataset research and editorial completion.",
        "judgment": "neutral",
        "walnut_call": "Neutral",
        "confidence": "medium",
        "confirmation_score_included": False,
        "primary_ticker": "",
        "comparison_tickers": [],
        "category": "Research",
        "reading_minutes": 4,
        "sections": [{"key": "editorial-note", "heading": "Editorial note", "body_markdown": "This non-ticker campaign item has been scheduled for review. Add the Walnut-native dataset query output before approval."}],
        "key_points": [],
        "catalysts": [],
        "risks": [],
        "watch_items": [],
        "data_freshness": [created],
        "missing_data_notes": [],
        "source_links": [],
        "suggested_card": {"title": topic, "description": "Walnut research brief.", "judgment": "neutral", "tickers": []},
        "seo": {"title": f"{topic} | Walnut Research", "description": "Walnut research brief."},
    }
    draft = {
        "id": f"rb_{int(time.time() * 1000)}_{uuid.uuid4().hex[:6]}",
        "status": "draft",
        "created_by": admin.id,
        "created_by_email": getattr(admin, "email", None),
        "created_at": created,
        "updated_at": created,
        "published_at": None,
        "model": "manual-dataset-brief",
        "prompt_version": RESEARCH_BRIEF_PROMPT_VERSION,
        "research_context_timestamp": created,
        "primary_ticker": "",
        "comparison_ticker": None,
        "comparison_tickers": [],
        "config": {"ticker": "", "research_question": topic, "theme": campaign_config.get("theme"), "content_type": "non_ticker"},
        "article": article,
        "validation": {"status": "passed", "warnings": [], "numeric_claims": [], "source_link_count": 0, "estimated_reading_minutes": 4},
        "diagnostics": {"elapsed_ms": 0, "storage": "database", "usage": {}},
        "research_context": {"generated_at": created, "topic": topic, "campaign_theme": campaign_config.get("theme")},
    }
    return draft


def _mark_draft_scheduled_review(draft: dict[str, Any], item: dict[str, Any], campaign_config: dict[str, Any]) -> dict[str, Any]:
    updated = deepcopy(draft)
    now = _now()
    context = updated.get("research_context") if isinstance(updated.get("research_context"), dict) else {}
    article = updated.get("article") if isinstance(updated.get("article"), dict) else {}
    updated["status"] = "scheduled_review"
    updated["campaign_id"] = item.get("campaign_id")
    updated["campaign_item_id"] = item.get("id")
    updated["campaign_name"] = item.get("campaign_name") or campaign_config.get("name")
    updated["campaign_theme"] = campaign_config.get("theme")
    updated["scheduled_at"] = item.get("publish_at")
    updated["generated_at"] = now
    updated["data_as_of"] = context.get("generated_at") or now
    updated["earnings_period_used"] = _earnings_period_from_context(context)
    updated["generator_version"] = RESEARCH_CAMPAIGN_DEFAULT_GENERATOR_VERSION
    config = updated.get("config") if isinstance(updated.get("config"), dict) else {}
    updated["target_keyword"] = str(config.get("target_keyword") or item.get("target_keyword") or "").strip() or None
    updated["secondary_keywords"] = config.get("secondary_keywords") or []
    updated["search_intent"] = str(config.get("search_intent") or campaign_config.get("search_intent") or "").strip() or None
    updated["content_type"] = str(config.get("content_type") or campaign_config.get("content_type") or "ticker")
    updated["index_status"] = str(updated.get("index_status") or "unknown")
    updated["updated_at"] = now
    article["current_data_as_of"] = updated["data_as_of"]
    seo = article.setdefault("seo", {})
    seo.setdefault("canonical", f"/research/{article.get('slug') or updated['id']}")
    article["schema"] = {
        "@type": "Article",
        "headline": article.get("title"),
        "dateModified": now,
        "datePublished": updated.get("scheduled_at"),
    }
    article["target_keyword"] = updated["target_keyword"]
    updated["article"] = article
    return updated


def _earnings_period_from_context(context: dict[str, Any]) -> str | None:
    primary = context.get("primary") if isinstance(context.get("primary"), dict) else {}
    financials = primary.get("financials") if isinstance(primary.get("financials"), dict) else {}
    summary = financials.get("summary") if isinstance(financials.get("summary"), dict) else {}
    external = context.get("external_research") if isinstance(context.get("external_research"), dict) else {}
    official_facts = external.get("official_facts") if isinstance(external.get("official_facts"), dict) else {}
    return str(summary.get("latestQuarter") or official_facts.get("latest_official_quarter") or "") or None


def send_research_campaign_review_email(db: Session, admin: UserAccount, draft: dict[str, Any], item: dict[str, Any], campaign_config: dict[str, Any]) -> dict[str, Any] | None:
    to_email = str(getattr(admin, "email", "") or item.get("campaign_created_by_email") or "").strip()
    if not to_email:
        return None
    article = draft.get("article") if isinstance(draft.get("article"), dict) else {}
    app_base = os.getenv("APP_BASE_URL", "https://app.walnutmarkets.com").strip().rstrip("/") or "https://app.walnutmarkets.com"
    review_url = f"{app_base}/admin/research-briefs?draft={draft['id']}"
    context = {
        "title": article.get("title") or draft["id"],
        "ticker_or_topic": draft.get("primary_ticker") or item.get("topic") or "",
        "scheduled_at": draft.get("scheduled_at") or "",
        "campaign_name": draft.get("campaign_name") or campaign_config.get("name") or "",
        "data_as_of": draft.get("data_as_of") or "",
        "review_url": review_url,
        "approve_url": review_url,
        "revision_note": draft.get("revision_request") or draft.get("quality_gate_correction_note") or "",
    }
    return send_email(
        db,
        to_email=to_email,
        template_key=RESEARCH_CAMPAIGN_REVIEW_TEMPLATE_KEY,
        context=context,
        user_id=getattr(admin, "id", None),
        category="alerts",
        idempotency_key=f"research-campaign-review:{draft['id']}",
    )


def approve_scheduled_research_brief(db: Session, admin: UserAccount, draft_id: str) -> dict[str, Any]:
    draft = get_draft(draft_id, db=db)
    draft["status"] = "approved_scheduled"
    draft["approved_at"] = _now()
    draft["approved_by"] = admin.id
    draft["updated_at"] = _now()
    _upsert_db_draft(db, draft)
    if draft.get("campaign_id"):
        db.execute(text("UPDATE research_campaigns SET approved_count = approved_count + 1, updated_at = :now WHERE id = :id"), {"id": draft["campaign_id"], "now": _now()})
        db.commit()
    return deepcopy(draft)


def reject_scheduled_research_brief(
    db: Session,
    admin: UserAccount,
    draft_id: str,
    correction_instructions: str | None = None,
) -> dict[str, Any]:
    draft = get_draft(draft_id, db=db)
    correction_note = str(correction_instructions or "").strip()
    draft["status"] = "rejected"
    draft["rejected_at"] = _now()
    draft["rejected_by"] = admin.id
    draft["rejection_request"] = correction_note or None
    draft["updated_at"] = _now()
    _upsert_db_draft(db, draft)
    if draft.get("campaign_id"):
        db.execute(text("UPDATE research_campaigns SET rejected_count = rejected_count + 1, updated_at = :now WHERE id = :id"), {"id": draft["campaign_id"], "now": _now()})
        db.commit()
    if not draft.get("campaign_id") or not draft.get("campaign_item_id"):
        return deepcopy(draft)

    row = db.execute(
        text(
            """
            SELECT i.*, c.name AS campaign_name, c.theme AS campaign_theme, c.content_type AS campaign_content_type,
                   c.config_json AS campaign_config_json, c.created_by AS campaign_created_by, c.created_by_email AS campaign_created_by_email
            FROM research_campaign_items i
            JOIN research_campaigns c ON c.id = i.campaign_id
            WHERE i.id = :item_id AND i.campaign_id = :campaign_id
            """
        ),
        {"item_id": draft["campaign_item_id"], "campaign_id": draft["campaign_id"]},
    ).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="Campaign item for this draft was not found.")

    item = _campaign_item_from_row(row)
    campaign_config = _load_json(item.get("campaign_config_json")) or {}
    revision_config = deepcopy(draft.get("config") or {})
    prior_context = str(revision_config.get("additional_context") or "").strip()
    user_note = correction_note or "Revise the brief substantially while preserving supported claims and source attribution."
    revision_note = (
        "Editor correction instructions: "
        f"{user_note}\n\nCreate a replacement draft. Address these instructions directly, "
        "retain only evidence supported by the supplied data and sources, and do not reuse the rejected copy verbatim."
    )
    revision_config["additional_context"] = f"{prior_context}\n\n{revision_note}".strip()[:4000]
    replacement = generate_research_brief(db, admin, revision_config)
    replacement = _mark_draft_scheduled_review(replacement, item, campaign_config)
    replacement["revision_of"] = draft["id"]
    replacement["revision_number"] = int(draft.get("revision_number") or 0) + 1
    replacement["revision_request"] = user_note
    replacement["updated_at"] = _now()
    _upsert_db_draft(db, replacement)
    db.execute(
        text(
            """
            UPDATE research_campaign_items
            SET status = 'generated', research_article_id = :draft_id, generated_at = :generated_at,
                updated_at = :updated_at, last_error = NULL
            WHERE id = :id
            """
        ),
        {"id": item["id"], "draft_id": replacement["id"], "generated_at": replacement["generated_at"], "updated_at": _now()},
    )
    db.commit()
    send_research_campaign_review_email(db, admin, replacement, item, campaign_config)
    return deepcopy(replacement)


def reschedule_research_brief(db: Session, draft_id: str, scheduled_at: str) -> dict[str, Any]:
    parsed = _parse_schedule_datetime(scheduled_at)
    if not parsed:
        raise HTTPException(status_code=422, detail="A valid scheduled_at timestamp is required.")
    draft = get_draft(draft_id, db=db)
    draft["scheduled_at"] = parsed.isoformat()
    draft["updated_at"] = _now()
    _upsert_db_draft(db, draft)
    if draft.get("campaign_item_id"):
        db.execute(text("UPDATE research_campaign_items SET publish_at = :publish_at, updated_at = :now WHERE id = :id"), {"publish_at": draft["scheduled_at"], "now": _now(), "id": draft["campaign_item_id"]})
        db.commit()
    return deepcopy(draft)


def run_due_scheduled_research_publications(db: Session, *, limit: int = 20) -> dict[str, Any]:
    ensure_research_brief_store_schema(db)
    cap = _research_daily_publish_cap()
    today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
    published_today = db.execute(
        text("SELECT COUNT(*) FROM research_brief_drafts WHERE status = 'published' AND published_at >= :today_start"),
        {"today_start": today_start},
    ).scalar() or 0
    remaining = max(0, cap - int(published_today)) if cap > 0 else max(1, min(100, limit))
    if cap > 0 and remaining == 0:
        return {"published": 0, "failed": 0, "skipped": 0, "checked": 0, "warning": f"Daily automated publishing cap ({cap}) reached."}
    rows = db.execute(
        text(
            """
            SELECT id, payload_json
            FROM research_brief_drafts
            WHERE status = 'approved_scheduled' AND scheduled_at <= :now
            ORDER BY scheduled_at ASC
            LIMIT :limit
            """
        ),
        {"now": _now(), "limit": min(max(1, min(100, limit)), remaining)},
    ).mappings().all()
    published = 0
    failed = 0
    skipped = 0
    system_admin = _system_admin_for_scheduler(db)
    for row in rows:
        draft = _load_json(row["payload_json"])
        if not isinstance(draft, dict) or draft.get("status") != "approved_scheduled":
            skipped += 1
            continue
        try:
            published_draft = publish_draft(system_admin, str(row["id"]), confirm=True, db=db)
            published += 1
            if published_draft.get("campaign_id"):
                db.execute(text("UPDATE research_campaigns SET published_count = published_count + 1, updated_at = :now WHERE id = :id"), {"id": published_draft["campaign_id"], "now": _now()})
                db.commit()
        except Exception as exc:
            failed += 1
            draft["last_publish_error"] = f"{exc.__class__.__name__}: {str(exc)[:500]}"
            draft["updated_at"] = _now()
            _upsert_db_draft(db, draft)
            logger.warning("scheduled_research_publish_failed draft_id=%s error=%s", row["id"], exc.__class__.__name__, exc_info=True)
    return {"published": published, "failed": failed, "skipped": skipped, "checked": len(rows), "daily_cap": cap, "published_today": int(published_today)}


def _research_daily_publish_cap() -> int:
    try:
        return max(0, int(os.getenv(RESEARCH_DAILY_PUBLISH_CAP, str(RESEARCH_DAILY_PUBLISH_CAP_DEFAULT))))
    except (TypeError, ValueError):
        return RESEARCH_DAILY_PUBLISH_CAP_DEFAULT


def research_publishing_health(db: Session) -> dict[str, Any]:
    """Small operator view; Search Console values remain ready for a later connector."""
    now = datetime.now(timezone.utc)
    cutoff = (now - timedelta(days=30)).isoformat()
    drafts = _db_drafts(db, status="published")
    recent = [draft for draft in drafts if str(draft.get("published_at") or draft.get("updated_at") or "") >= cutoff]
    statuses = {status: sum(1 for draft in recent if str(draft.get("index_status") or "unknown") == status) for status in INDEX_STATUSES}
    indexed = statuses["indexed"]
    index_rate = round((indexed / len(recent) * 100), 1) if recent else None
    days_to_index: list[float] = []
    impressions: list[float] = []
    positions: list[float] = []
    for draft in recent:
        published_at = _parse_iso_datetime(draft.get("published_at"))
        first_seen = _parse_iso_datetime(draft.get("first_seen_indexed_at"))
        if published_at and first_seen:
            days_to_index.append((first_seen - published_at).total_seconds() / 86400)
        if str(draft.get("index_status") or "unknown") == "indexed":
            if _safe_float(draft.get("search_console_impressions")) is not None:
                impressions.append(float(draft["search_console_impressions"]))
            if _safe_float(draft.get("average_position")) is not None:
                positions.append(float(draft["average_position"]))
    median_days = sorted(days_to_index)[len(days_to_index) // 2] if days_to_index else None
    return {
        "published_last_30_days": len(recent),
        "indexed": indexed,
        "crawled_not_indexed": statuses["crawled_not_indexed"],
        "discovered": statuses["discovered"],
        "unknown": statuses["unknown"],
        "indexation_rate": index_rate,
        "median_days_to_index": round(median_days, 1) if median_days is not None else None,
        "average_impressions_per_indexed_brief": round(sum(impressions) / len(impressions), 1) if impressions else None,
        "average_position": round(sum(positions) / len(positions), 1) if positions else None,
        "daily_automated_publish_cap": _research_daily_publish_cap(),
        "campaign_schedule_enabled": _env_flag_enabled(RESEARCH_CAMPAIGNS_SCHEDULE_ENABLED),
        "cadence_warning": f"Only {index_rate}% of Research Briefs published in the last 30 days are currently indexed. Consider slowing publication or improving internal linking." if index_rate is not None and index_rate < 50 else None,
    }


def _system_admin_for_scheduler(db: Session) -> UserAccount:
    admin = db.execute(select(UserAccount).where(UserAccount.role == "admin").order_by(UserAccount.id.asc())).scalar_one_or_none()
    if admin:
        return admin
    fallback = UserAccount(id=0, email="scheduler@walnutmarkets.com", role="admin")  # type: ignore[call-arg]
    return fallback


def _append_audit(store: dict[str, Any], *, action: str, admin: UserAccount, draft_id: str | None, metadata: dict[str, Any] | None = None) -> None:
    audit = store.setdefault("audit", [])
    audit.append(
        {
            "action": action,
            "draft_id": draft_id,
            "admin_id": admin.id,
            "admin_email": getattr(admin, "email", None),
            "at": _now(),
            "metadata": metadata or {},
        }
    )
    del audit[:-250]


def _slugify(value: str, fallback: str) -> str:
    raw = (value or fallback).strip().lower()
    raw = raw.replace("$", "")
    slug = re.sub(r"[^a-z0-9]+", "-", raw).strip("-")
    return slug[:96] or fallback.lower()


def normalize_supported_symbol(db: Session, raw: str | None) -> tuple[str, dict[str, Any]]:
    symbol = normalize_symbol(raw) if raw else None
    if not symbol:
        raise HTTPException(status_code=422, detail="Ticker symbol is required.")
    meta = db.get(TickerMeta, symbol)
    security = db.execute(select(Security).where(func.upper(Security.symbol) == symbol)).scalar_one_or_none()
    fundamentals = (
        db.execute(
            select(FundamentalsCache)
            .where(func.upper(FundamentalsCache.symbol) == symbol)
            .order_by(desc(FundamentalsCache.fetched_at))
            .limit(1)
        ).scalar_one_or_none()
    )
    quote = db.get(QuoteCache, symbol)
    if not any([meta, security, fundamentals, quote]):
        raise HTTPException(status_code=422, detail=f"{symbol} is not a supported Walnut ticker yet.")
    identity = {
        "symbol": symbol,
        "company_name": (meta.company_name if meta else None) or (fundamentals.company_name if fundamentals else None) or (security.name if security else None) or symbol,
        "exchange": (meta.exchange if meta else None) or (fundamentals.exchange if fundamentals else None),
        "sector": (meta.sector if meta else None) or (fundamentals.sector if fundamentals else None) or (security.sector if security else None),
        "industry": (meta.industry if meta else None) or (fundamentals.industry if fundamentals else None),
        "country": (meta.country if meta else None) or (fundamentals.country if fundamentals else None),
    }
    return symbol, identity


def normalize_comparison_tickers(config: dict[str, Any], *, primary_ticker: str | None = None) -> list[str]:
    values: list[Any] = []
    raw_list = config.get("comparison_tickers")
    if isinstance(raw_list, list):
        values.extend(raw_list)
    elif raw_list:
        values.append(raw_list)
    if config.get("comparison_ticker"):
        values.append(config.get("comparison_ticker"))

    symbols: list[str] = []
    seen: set[str] = set()
    for value in values:
        for part in str(value or "").split(","):
            symbol = normalize_symbol(part)
            if not symbol or symbol in seen:
                continue
            symbols.append(symbol)
            seen.add(symbol)

    if len(symbols) > MAX_COMPARISON_TICKERS:
        raise HTTPException(status_code=422, detail=f"Comparison tickers are limited to {MAX_COMPARISON_TICKERS} symbols.")

    primary = normalize_symbol(primary_ticker)
    if primary and primary in seen:
        raise HTTPException(status_code=422, detail="Primary ticker cannot appear in comparison tickers.")

    return symbols


def _latest_fundamentals(db: Session, symbol: str) -> dict[str, Any] | None:
    row = (
        db.execute(
            select(FundamentalsCache)
            .where(func.upper(FundamentalsCache.symbol) == symbol)
            .order_by(desc(FundamentalsCache.fetched_at))
            .limit(1)
        ).scalar_one_or_none()
    )
    if not row:
        return None
    return {
        "as_of": _iso(row.fetched_at),
        "period_date": _iso(row.period_date),
        "status": row.status,
        "market_cap": row.market_cap,
        "price": row.price,
        "volume": row.volume,
        "avg_volume": row.avg_volume,
        "revenue_growth": row.revenue_growth,
        "eps_growth": row.eps_growth,
        "gross_margin": row.gross_margin,
        "operating_margin": row.operating_margin,
        "net_margin": row.net_margin,
        "roe": row.roe,
        "roic": row.roic,
        "trailing_pe": row.trailing_pe,
        "forward_pe": row.forward_pe,
        "price_to_sales": row.price_to_sales,
        "ev_to_ebitda": row.ev_to_ebitda,
        "debt_to_equity": row.debt_to_equity,
        "net_debt_to_ebitda": row.net_debt_to_ebitda,
        "free_cash_flow": row.free_cash_flow,
        "fcf_yield": row.fcf_yield,
        "eps_ttm": row.eps_ttm,
    }


def _quote(db: Session, symbol: str) -> dict[str, Any] | None:
    row = db.get(QuoteCache, symbol)
    if not row:
        return None
    return {"price": row.price, "market_cap": row.market_cap, "as_of": _iso(row.asof_ts)}


def _current_market_state(quote: dict[str, Any] | None, fundamentals: dict[str, Any] | None) -> dict[str, Any]:
    quote = quote if isinstance(quote, dict) else {}
    fundamentals = fundamentals if isinstance(fundamentals, dict) else {}
    price = quote.get("price")
    if price is None:
        price = fundamentals.get("price")
    volume = fundamentals.get("volume")
    avg_volume = fundamentals.get("avg_volume")
    volume_vs_avg = None
    try:
        if volume is not None and avg_volume not in (None, 0):
            volume_vs_avg = round(float(volume) / float(avg_volume), 4)
    except (TypeError, ValueError, ZeroDivisionError):
        volume_vs_avg = None
    return _compact(
        {
            "price": price,
            "market_cap": quote.get("market_cap") if quote.get("market_cap") is not None else fundamentals.get("market_cap"),
            "price_as_of": quote.get("as_of") or fundamentals.get("as_of"),
            "volume": volume,
            "avg_volume": avg_volume,
            "volume_vs_avg": volume_vs_avg,
        }
    )


def _cached_financials_snapshot(db: Session, symbol: str) -> dict[str, Any] | None:
    row = (
        db.execute(
            select(TickerFinancialsCache)
            .where(func.upper(TickerFinancialsCache.symbol) == symbol)
            .order_by(desc(TickerFinancialsCache.fetched_at))
            .limit(1)
        ).scalar_one_or_none()
    )
    if not row:
        return None
    payload = _load_json(row.payload_json)
    if not isinstance(payload, dict):
        return None
    subsections = payload.get("subsections") if isinstance(payload.get("subsections"), dict) else {}
    sections = payload.get("sections") if isinstance(payload.get("sections"), dict) else {}
    analyst_estimates = subsections.get("analyst_estimates") if isinstance(subsections.get("analyst_estimates"), dict) else {}
    valuation = subsections.get("valuation") if isinstance(subsections.get("valuation"), dict) else {}
    income = subsections.get("income") if isinstance(subsections.get("income"), dict) else {}
    cash_flow = subsections.get("cash_flow") if isinstance(subsections.get("cash_flow"), dict) else {}
    health = subsections.get("health") if isinstance(subsections.get("health"), dict) else {}
    earnings_subsection = subsections.get("earnings") if isinstance(subsections.get("earnings"), dict) else {}
    return _compact(
        {
            "status": payload.get("status") or row.status,
            "as_of": _iso(row.fetched_at),
            "latest_quarter": (payload.get("summary") or {}).get("latestQuarter") if isinstance(payload.get("summary"), dict) else None,
            "forecasts": payload.get("forecasts") or analyst_estimates.get("data") or sections.get("analyst_estimates"),
            "earnings": payload.get("earnings") or sections.get("earnings") or earnings_subsection.get("data"),
            "valuation": valuation.get("data") or sections.get("valuation") or payload.get("valuation_metrics"),
            "income": income.get("data") or sections.get("income"),
            "cash_flow": cash_flow.get("data") or sections.get("cashFlow"),
            "health": health.get("data") or sections.get("health"),
        },
        limit=2500,
    )


def _recent_events(db: Session, symbol: str, event_types: list[str], *, limit: int = 8) -> list[dict[str, Any]]:
    rows = (
        db.execute(
            select(Event)
            .where(func.upper(Event.symbol) == symbol)
            .where(Event.event_type.in_(event_types))
            .order_by(desc(Event.ts))
            .limit(limit)
        )
        .scalars()
        .all()
    )
    items: list[dict[str, Any]] = []
    for row in rows:
        payload = _load_json(row.payload_json) or {}
        items.append(
            {
                "event_type": row.event_type,
                "date": _iso(row.event_date or row.ts),
                "member_name": row.member_name,
                "trade_type": row.trade_type or row.transaction_type,
                "amount_range_min": row.amount_min,
                "amount_range_max": row.amount_max,
                "title": payload.get("title") or payload.get("headline"),
                "summary": payload.get("summary") or payload.get("description"),
                "source_document_url": row.source_document_url,
            }
        )
    return items


def _government_contracts(db: Session, symbol: str) -> dict[str, Any]:
    rows = (
        db.execute(
            select(GovernmentContract)
            .where(func.upper(GovernmentContract.symbol) == symbol)
            .order_by(desc(GovernmentContract.award_date))
            .limit(8)
        )
        .scalars()
        .all()
    )
    total = sum(float(row.award_amount or 0) for row in rows)
    return {
        "recent_count": len(rows),
        "recent_award_amount": total,
        "items": [
            {
                "award_date": _iso(row.award_date),
                "award_amount": row.award_amount,
                "awarding_agency": row.awarding_agency,
                "description": row.description,
                "source_url": row.source_url,
            }
            for row in rows
        ],
    }


def _has_value(mapping: Any, keys: list[str]) -> bool:
    if not isinstance(mapping, dict):
        return False
    for key in keys:
        value = mapping.get(key)
        if value is not None and value != "":
            return True
    return False


def _has_nested_value(mapping: Any, paths: list[tuple[str, ...]]) -> bool:
    if not isinstance(mapping, dict):
        return False
    for path in paths:
        value: Any = mapping
        for key in path:
            if not isinstance(value, dict):
                value = None
                break
            value = value.get(key)
        if isinstance(value, list) and value:
            return True
        if isinstance(value, dict) and any(item is not None and item != "" for item in value.values()):
            return True
        if value is not None and value != "":
            return True
    return False


def _research_data_availability(primary: dict[str, Any], external_research: dict[str, Any]) -> dict[str, bool]:
    quote = primary.get("quote") if isinstance(primary.get("quote"), dict) else {}
    market_state = primary.get("market_state") if isinstance(primary.get("market_state"), dict) else {}
    fundamentals = primary.get("fundamentals") if isinstance(primary.get("fundamentals"), dict) else {}
    financials = primary.get("financials") if isinstance(primary.get("financials"), dict) else {}
    confirmation = primary.get("confirmation") if isinstance(primary.get("confirmation"), dict) else {}
    official_facts = (external_research.get("official_facts") or {}) if isinstance(external_research, dict) else {}
    government_contracts = primary.get("government_contracts") if isinstance(primary.get("government_contracts"), dict) else {}

    has_price = _has_value(quote, ["price"]) or _has_value(market_state, ["price"]) or _has_value(fundamentals, ["price"])
    has_volume = _has_value(market_state, ["volume", "avg_volume"]) or _has_value(fundamentals, ["volume", "avg_volume"])
    has_confirmation = bool(confirmation)
    has_forecast_revenue = _has_nested_value(
        financials,
        [
            ("forecasts", "nextQuarter", "revenueEstimate"),
            ("forecasts", "nextQuarter", "estimatedRevenueAvg"),
            ("forecasts", "nextFiscalYear", "revenueEstimate"),
        ],
    ) or "current_revenue_consensus" in official_facts
    has_forecast_eps = _has_nested_value(
        financials,
        [
            ("forecasts", "nextQuarter", "epsEstimate"),
            ("forecasts", "nextFiscalYear", "epsEstimate"),
        ],
    ) or "current_eps_consensus" in official_facts
    return {
        "revenue": _has_value(fundamentals, ["revenue_growth"]) or "revenue" in official_facts or has_forecast_revenue,
        "revenue growth": _has_value(fundamentals, ["revenue_growth"]) or "revenue_growth" in official_facts,
        "revenue consensus": has_forecast_revenue,
        "eps consensus": has_forecast_eps,
        "upcoming earnings date": "upcoming_earnings_date" in official_facts,
        "previous quarter revenue": "previous_quarter_revenue" in official_facts or "revenue" in official_facts,
        "previous quarter eps": "previous_quarter_eps" in official_facts or "diluted_eps" in official_facts,
        "previous quarter consensus": "previous_quarter_revenue_consensus" in official_facts or "previous_quarter_eps_consensus" in official_facts,
        "previous quarter result": "previous_quarter_result" in official_facts,
        "guidance": "guidance" in official_facts,
        "diluted eps": "diluted_eps" in official_facts,
        "gross margin": _has_value(fundamentals, ["gross_margin"]) or "gross_margin" in official_facts,
        "operating margin": _has_value(fundamentals, ["operating_margin"]) or "operating_margin" in official_facts,
        "free cash flow": _has_value(fundamentals, ["free_cash_flow", "fcf_yield"]) or "free_cash_flow" in official_facts,
        "capex": "capex" in official_facts,
        "cash": "cash" in official_facts or _has_nested_value(financials, [("health", "cashAndCashEquivalents"), ("health", "cash")]),
        "debt": _has_value(fundamentals, ["debt_to_equity", "net_debt_to_ebitda"]) or "debt" in official_facts,
        "share count": "shares" in official_facts,
        "price": has_price,
        "current price": has_price,
        "volume": has_volume,
        "price/volume and technicals": has_price or has_volume or has_confirmation,
        "technical levels": has_confirmation,
        "reported institutional activity": bool(primary.get("institutional_activity")),
        "insider activity": bool(primary.get("insider_activity")),
        "congress activity": bool(primary.get("congress_activity")),
        "government contracts": bool((government_contracts or {}).get("recent_count") or (government_contracts or {}).get("items")),
        "valuation data": _has_value(fundamentals, ["forward_pe", "trailing_pe", "price_to_sales", "ev_to_ebitda"])
        or _has_nested_value(financials, [("valuation", "forwardPE"), ("valuation", "trailingPE"), ("valuation", "forward_pe")]),
        "peer comparison data": False,
    }


def _missing_note_field(note: str) -> str:
    return str(note or "").split(":", 1)[0].strip().lower()


def _filter_missing_data_notes(notes: list[str], availability: dict[str, bool]) -> list[str]:
    filtered: list[str] = []
    for note in notes:
        field = _missing_note_field(note)
        if field and availability.get(field):
            continue
        filtered.append(note)
    return _dedupe_strings(filtered)


def _primary_ticker_prompt_context(primary: dict[str, Any]) -> dict[str, Any]:
    confirmation = primary.get("confirmation") if isinstance(primary.get("confirmation"), dict) else {}
    sources = confirmation.get("sources") if isinstance(confirmation.get("sources"), dict) else {}
    return _compact(
        {
            "symbol": ((primary.get("identity") or {}).get("symbol") if isinstance(primary.get("identity"), dict) else None),
            "confirmation_score": confirmation.get("score") or confirmation.get("confirmation_score"),
            "confirmation_score_label": confirmation.get("status") or confirmation.get("label") or confirmation.get("direction"),
            "confirmation_score_window": "30 days",
            "price_volume_summary": sources.get("price_volume") or (primary.get("market_state") if isinstance(primary.get("market_state"), dict) else {}),
            "fundamentals_summary": primary.get("fundamentals"),
            "reported_institutional_summary": primary.get("institutional_activity"),
            "congress_summary": primary.get("congress_activity"),
            "insider_summary": primary.get("insider_activity"),
            "options_flow_summary": sources.get("options_flow"),
        },
        limit=2500,
    )


def _validated_confirmation_context(symbol: str, value: Any, *, role: str, strict: bool = True) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    embedded_symbol = normalize_symbol(value.get("symbol") or value.get("ticker") or value.get("primary_ticker"))
    if embedded_symbol and embedded_symbol != symbol:
        if strict:
            raise HTTPException(status_code=422, detail=f"Primary ticker context mismatch: expected {symbol}, received {embedded_symbol}.")
        return {}
    cleaned = dict(value)
    cleaned.setdefault("symbol", symbol)
    cleaned["context_role"] = role
    return cleaned


def retrieve_walnut_site_context(db: Session, *, symbol: str, target_keyword: str, search_intent: str) -> dict[str, Any]:
    """Return a deliberately small, crawler-safe slice of Walnut context for a brief."""
    related: list[dict[str, str]] = []
    keyword_terms = {term for term in re.findall(r"[a-z0-9]{3,}", f"{target_keyword} {search_intent}".lower())}
    try:
        drafts = _db_drafts(db, status="published")
    except Exception:
        drafts = []
    for draft in drafts:
        article = draft.get("article") if isinstance(draft.get("article"), dict) else {}
        title = str(article.get("title") or "")
        article_symbol = normalize_symbol(article.get("primary_ticker") or draft.get("primary_ticker"))
        words = set(re.findall(r"[a-z0-9]{3,}", title.lower()))
        if article_symbol == symbol or len(keyword_terms & words) >= 2:
            slug = str(article.get("slug") or "").strip()
            if slug:
                related.append({"title": title or slug, "url": f"/research/{slug}", "source_type": "related_research"})
    links = [
        {"title": f"{symbol} ticker research", "url": f"/ticker/{symbol}", "source_type": "ticker_page"},
        {"title": "Research Briefs", "url": "/research", "source_type": "research_hub"},
        {"title": "Walnut Confirmation Score", "url": "/stock-confirmation-score", "source_type": "methodology"},
        *related[:3],
    ]
    return {"query": target_keyword, "search_intent": search_intent, "links": links[:5], "related_research": related[:3]}


def potential_research_overlap(db: Session, *, symbol: str, target_keyword: str, search_intent: str, exclude_draft_id: str | None = None) -> list[dict[str, str]]:
    matches: list[dict[str, str]] = []
    normalized_keyword = re.sub(r"\s+", " ", target_keyword.lower()).strip()
    intent_terms = set(re.findall(r"[a-z0-9]{4,}", search_intent.lower()))
    try:
        drafts = _db_drafts(db)
    except Exception:
        drafts = []
    for draft in drafts:
        if exclude_draft_id and draft.get("id") == exclude_draft_id:
            continue
        article = draft.get("article") if isinstance(draft.get("article"), dict) else {}
        config = draft.get("config") if isinstance(draft.get("config"), dict) else {}
        candidate_keyword = str(draft.get("target_keyword") or config.get("target_keyword") or "").lower().strip()
        candidate_intent = str(draft.get("search_intent") or config.get("search_intent") or config.get("research_question") or "").lower()
        same_symbol = normalize_symbol(draft.get("primary_ticker") or article.get("primary_ticker")) == symbol
        same_keyword = bool(normalized_keyword and candidate_keyword == normalized_keyword)
        same_intent = bool(intent_terms and len(intent_terms & set(re.findall(r"[a-z0-9]{4,}", candidate_intent))) >= min(2, len(intent_terms)))
        if same_keyword or (same_symbol and same_intent):
            slug = str(article.get("slug") or "").strip()
            matches.append({"id": str(draft.get("id") or ""), "title": str(article.get("title") or draft.get("id")), "url": f"/research/{slug}" if slug else "", "status": str(draft.get("status") or "draft")})
    return matches[:3]


def assemble_research_context(db: Session, payload: dict[str, Any]) -> dict[str, Any]:
    symbol, identity = normalize_supported_symbol(db, payload.get("ticker"))
    comparison_symbols = normalize_comparison_tickers(payload, primary_ticker=symbol)
    comparison_identities: dict[str, dict[str, Any]] = {}
    for comparison_symbol in comparison_symbols:
        try:
            normalized_comparison, comparison_identity = normalize_supported_symbol(db, comparison_symbol)
        except HTTPException as exc:
            if exc.status_code == 422:
                raise HTTPException(status_code=422, detail=f"{comparison_symbol} is not currently supported as a comparison ticker.") from exc
            raise
        comparison_identities[normalized_comparison] = comparison_identity

    symbols = [symbol] + list(comparison_identities.keys())
    fundamentals = {item: _latest_fundamentals(db, item) for item in symbols}
    quotes = {item: _quote(db, item) for item in symbols}
    financials = {item: _cached_financials_snapshot(db, item) for item in symbols}
    try:
        confirmation = get_confirmation_score_bundles_for_tickers(db, symbols, lookback_days=30)
    except Exception:
        confirmation = {}
    primary_confirmation = _validated_confirmation_context(symbol, confirmation.get(symbol), role="primary")

    missing: list[str] = []
    for item in symbols:
        if not fundamentals.get(item):
            missing.append(f"{item}: fundamentals unavailable")
        if not quotes.get(item):
            missing.append(f"{item}: quote unavailable")
        if not confirmation.get(item):
            missing.append(f"{item}: confirmation score unavailable")
    external_research = discover_external_research(
        symbol,
        identity,
        mode=payload.get("external_research_mode") or "Standard",
        manual_source_url=payload.get("manual_source_url"),
        desired_angle=payload.get("desired_angle"),
        research_question=payload.get("research_question"),
    )
    primary_context = {
        "identity": identity,
        "quote": quotes.get(symbol),
        "market_state": _current_market_state(quotes.get(symbol), fundamentals.get(symbol)),
        "fundamentals": fundamentals.get(symbol),
        "financials": financials.get(symbol),
        "confirmation": _compact(primary_confirmation),
        "congress_activity": _recent_events(db, symbol, ["congress_trade", "congress_treasury_trade", "congress_crypto_trade"]),
        "insider_activity": _recent_events(db, symbol, ["insider_trade"]),
        "institutional_activity": _recent_events(
            db,
            symbol,
            [
                "institutional_accumulation",
                "institutional_distribution",
                "new_institutional_position",
                "major_holder_reduction",
                "major_holder_exit",
                "cluster_accumulation",
                "cluster_distribution",
                "smart_money_confirmation",
                "crowded_long",
                "contrarian_accumulation",
            ],
        ),
        "government_contracts": _government_contracts(db, symbol),
    }
    data_availability = _research_data_availability(primary_context, external_research)
    if external_research.get("missing_data_notes"):
        missing.extend(_filter_missing_data_notes(external_research["missing_data_notes"], data_availability))

    context = {
        "generated_at": _now(),
        "external_research_mode": payload.get("external_research_mode") or "Standard",
        "desired_angle": payload.get("desired_angle") or "",
        "research_question": payload.get("research_question") or "",
        "section_format": payload.get("section_format") or "Walnut Research Brief",
        "include_confirmation_score": bool(payload.get("include_confirmation_score")),
        "include_cross_source_confirmations": bool(payload.get("include_cross_source_confirmations")),
        "primary": primary_context,
        "primary_ticker_context": _primary_ticker_prompt_context(primary_context),
        "external_research": external_research,
        "data_availability": data_availability,
        "comparison": None,
        "comparisons": [],
        "missing_data_notes": _dedupe_strings(missing),
        "source_discovery": external_research.get("source_discovery") or {},
        "limitations": [
            "13F activity is reported with filing lag and is not real-time.",
            "Congress and insider activity should not be interpreted as intent or wrongdoing.",
            "Missing Walnut data is unavailable, not zero and not automatically bearish.",
        ],
    }
    for comparison_symbol, comparison_identity in comparison_identities.items():
        comparison_confirmation = _validated_confirmation_context(comparison_symbol, confirmation.get(comparison_symbol), role="comparison", strict=False)
        comparison_context = {
            "identity": comparison_identity,
            "quote": quotes.get(comparison_symbol),
            "market_state": _current_market_state(quotes.get(comparison_symbol), fundamentals.get(comparison_symbol)),
            "fundamentals": fundamentals.get(comparison_symbol),
            "financials": financials.get(comparison_symbol),
            "confirmation": _compact(comparison_confirmation),
            "congress_activity": _recent_events(db, comparison_symbol, ["congress_trade", "congress_treasury_trade", "congress_crypto_trade"]),
            "insider_activity": _recent_events(db, comparison_symbol, ["insider_trade"]),
            "institutional_activity": _recent_events(
                db,
                comparison_symbol,
                [
                    "institutional_accumulation",
                    "institutional_distribution",
                    "new_institutional_position",
                    "major_holder_reduction",
                    "major_holder_exit",
                    "cluster_accumulation",
                    "cluster_distribution",
                    "smart_money_confirmation",
                    "crowded_long",
                    "contrarian_accumulation",
                ],
            ),
            "government_contracts": _government_contracts(db, comparison_symbol),
        }
        context["comparisons"].append(comparison_context)
    if context["comparisons"]:
        context["comparison"] = context["comparisons"][0]
    context["walnut_site_context"] = retrieve_walnut_site_context(
        db,
        symbol=symbol,
        target_keyword=str(payload.get("target_keyword") or payload.get("research_question") or ""),
        search_intent=str(payload.get("search_intent") or payload.get("research_question") or ""),
    )
    context["potential_overlap"] = potential_research_overlap(
        db,
        symbol=symbol,
        target_keyword=str(payload.get("target_keyword") or payload.get("research_question") or ""),
        search_intent=str(payload.get("search_intent") or payload.get("research_question") or ""),
    )
    context["research_packet"] = _research_packet(context)
    context["research_readiness"] = research_readiness(context)
    return context


def _research_packet(context: dict[str, Any]) -> dict[str, Any]:
    """Stable packet boundary: omit unavailable data instead of narrating its absence."""
    primary = context.get("primary") if isinstance(context.get("primary"), dict) else {}
    identity = primary.get("identity") if isinstance(primary.get("identity"), dict) else {}
    confirmation = primary.get("confirmation") if isinstance(primary.get("confirmation"), dict) else {}
    external = context.get("external_research") if isinstance(context.get("external_research"), dict) else {}
    official = external.get("official_facts") if isinstance(external.get("official_facts"), dict) else {}
    packet = {
        "ticker": identity.get("symbol"),
        "company": identity.get("company_name"),
        "latest_price": (primary.get("quote") or {}).get("price") if isinstance(primary.get("quote"), dict) else None,
        "latest_earnings": official.get("latest_official_quarter") or (primary.get("financials") or {}).get("latest_quarter") if isinstance(primary.get("financials"), dict) else official.get("latest_official_quarter"),
        "guidance": official.get("guidance"),
        "valuation": (primary.get("fundamentals") or {}) if isinstance(primary.get("fundamentals"), dict) else None,
        "confirmation_score": confirmation.get("score") or confirmation.get("confirmation_score"),
        "directional_judgment": confirmation.get("direction") or confirmation.get("status"),
        "score_components": confirmation.get("sources"),
        "what_changed": confirmation.get("what_changed"),
        "fundamentals": primary.get("fundamentals"),
        "technicals": primary.get("market_state"),
        "insiders": primary.get("insider_activity"),
        "congress": primary.get("congress_activity"),
        "institutions": primary.get("institutional_activity"),
        "government_contracts": primary.get("government_contracts"),
        "analysts": (primary.get("financials") or {}).get("forecasts") if isinstance(primary.get("financials"), dict) else None,
        "relevant_recent_news": external.get("reviewed_sources"),
        "related_research": (context.get("walnut_site_context") or {}).get("related_research") if isinstance(context.get("walnut_site_context"), dict) else None,
        "source_timestamps": {"data_as_of": context.get("generated_at")},
    }
    return {key: value for key, value in packet.items() if value not in (None, {}, [], "")}


def discover_external_research(
    symbol: str,
    identity: dict[str, Any],
    *,
    mode: str,
    manual_source_url: str | None = None,
    desired_angle: str | None = None,
    research_question: str | None = None,
) -> dict[str, Any]:
    normalized_mode = _choice(mode, EXTERNAL_RESEARCH_MODE_OPTIONS, "Standard")
    manual_source = _normalize_manual_source_url(manual_source_url)
    source_discovery = _source_discovery_status(symbol, [], {}, desired_angle=desired_angle, research_question=research_question)
    if normalized_mode == "Off":
        reviewed_sources = []
        if manual_source:
            reviewed_sources.append({"label": "Admin-added source URL", "url": manual_source, "source_type": "manual_official_source"})
            source_discovery = _source_discovery_status(symbol, reviewed_sources, {}, desired_angle=desired_angle, research_question=research_question)
            source_discovery["manual_source_url"] = {"status": "found", "url": manual_source}
        return {
            "mode": "Off",
            "reviewed_sources": reviewed_sources,
            "source_notes": ["External research mode is off; only Walnut data was reviewed."],
            "official_facts": {},
            "missing_data_notes": [],
            "source_discovery": source_discovery,
        }
    reviewed_sources = [
        {
            "label": "SEC EDGAR company search",
            "url": f"https://www.sec.gov/edgar/search/#/q={symbol}&dateRange=all",
            "source_type": "filing_search",
        },
        {
            "label": f"{symbol} Nasdaq market activity",
            "url": f"https://www.nasdaq.com/market-activity/stocks/{symbol.lower()}",
            "source_type": "reputable_market_source",
        }
    ]
    source_notes = [
        "Reviewed Walnut data and public/official source discovery. Report missing values as 'Not found in reviewed sources' when they cannot be supported.",
    ]
    official_facts: dict[str, Any] = {}
    profile = OFFICIAL_SOURCE_PROFILES.get(symbol.upper()) or {}
    profile_sources = profile.get("company_earnings_sources") if isinstance(profile.get("company_earnings_sources"), list) else []
    reviewed_sources.extend([source for source in profile_sources if isinstance(source, dict)])
    profile_facts = profile.get("official_facts") if isinstance(profile.get("official_facts"), dict) else {}
    official_facts.update(profile_facts)
    source_notes.extend(str(note) for note in profile.get("source_notes") or [] if str(note).strip())
    if manual_source:
        reviewed_sources.insert(0, {"label": "Admin-added source URL", "url": manual_source, "source_type": "manual_official_source"})
        source_notes.append("Admin-added manual source URL is included for extraction and citation.")
    sec_record = _sec_company_record(symbol)
    if sec_record:
        cik = str(sec_record.get("cik_str") or "").zfill(10)
        company = str(sec_record.get("title") or identity.get("company_name") or symbol).strip()
        reviewed_sources.extend(
            [
                {
                    "label": f"{company} SEC company filings",
                    "url": f"https://www.sec.gov/edgar/browse/?CIK={cik}&owner=exclude",
                    "source_type": "official_filing",
                },
                {
                    "label": f"{company} SEC company facts",
                    "url": f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json",
                    "source_type": "official_filing_data",
                },
            ]
        )
        official_facts = {**_sec_company_facts(cik), **official_facts}
        source_notes.append(f"Matched {symbol} to SEC CIK {cik} for official filings and company-facts review.")
    else:
        source_notes.append(f"SEC ticker mapping did not return a CIK for {symbol}; EDGAR symbol search remains attached for manual review.")
    if normalized_mode == "Deep":
        source_notes.append("Deep mode also attaches a reputable public market reference for price/volume review.")
    missing_fields = _missing_key_fields(official_facts)
    source_discovery = _source_discovery_status(symbol, reviewed_sources, official_facts, desired_angle=desired_angle, research_question=research_question)
    return {
        "mode": normalized_mode,
        "reviewed_sources": _dedupe_source_links(reviewed_sources),
        "source_notes": source_notes,
        "official_facts": official_facts,
        "missing_data_notes": [f"{field}: Not found in reviewed sources" for field in missing_fields],
        "source_discovery": source_discovery,
    }


def _is_earnings_setup_text(*values: Any) -> bool:
    text = " ".join(str(value or "") for value in values).lower()
    return "earnings setup" in text or ("earnings" in text and "setup" in text)


def _source_discovery_status(
    symbol: str,
    reviewed_sources: list[dict[str, Any]],
    official_facts: dict[str, Any],
    *,
    desired_angle: str | None = None,
    research_question: str | None = None,
) -> dict[str, Any]:
    sources = [source for source in reviewed_sources if isinstance(source, dict)]
    source_types = {str(source.get("source_type") or "") for source in sources}
    urls = [str(source.get("url") or "") for source in sources]
    official_earnings = any(source_type in {"official_company_earnings", "official_company_ir", "manual_official_source"} for source_type in source_types)
    sec_filing = any(source_type in {"official_filing", "official_filing_data", "filing_search", "official_company_filings"} for source_type in source_types)
    profile = OFFICIAL_SOURCE_PROFILES.get(symbol.upper()) or {}
    required = _is_earnings_setup_text(desired_angle, research_question)
    latest_quarter = official_facts.get("latest_official_quarter") if isinstance(official_facts, dict) else None
    return {
        "official_earnings_release": {
            "status": "found" if official_earnings else "missing",
            "required": required,
            "url": next((url for url in urls if url and ("earnings" in url.lower() or "newsroom" in url.lower() or "quarterly" in url.lower())), None),
        },
        "sec_filing": {
            "status": "found" if sec_filing else "missing",
            "required": required,
            "url": next((url for url in urls if "sec.gov" in url.lower() or "sec-filings" in url.lower()), None),
        },
        "company_ir": {
            "status": "found" if profile.get("company_earnings_sources") or any("investor" in url.lower() or "ir." in url.lower() for url in urls) else "missing",
            "required": required,
        },
        "consensus_estimates": {
            "status": "found" if "current_revenue_consensus" in official_facts and "current_eps_consensus" in official_facts else "missing",
            "required": required,
        },
        "prior_quarter_results": {
            "status": "found" if ("previous_quarter_revenue" in official_facts or "revenue" in official_facts) and ("previous_quarter_eps" in official_facts or "diluted_eps" in official_facts) else "missing",
            "required": required,
        },
        "upcoming_earnings_date": {
            "status": "found" if "upcoming_earnings_date" in official_facts else "missing",
            "required": required,
        },
        "latest_official_quarter": latest_quarter,
        "required_for_major_earnings_setup": required,
        "required_for_earnings_setup": required,
    }


def _readiness_row(label: str, found: bool, *, required: bool = True, detail: Any | None = None) -> dict[str, Any]:
    row: dict[str, Any] = {"label": label, "status": "found" if found else "missing", "required": required}
    if detail is not None:
        row["detail"] = detail
    return row


def research_readiness(context: dict[str, Any]) -> dict[str, Any]:
    primary = context.get("primary") if isinstance(context.get("primary"), dict) else {}
    identity = primary.get("identity") if isinstance(primary.get("identity"), dict) else {}
    external = context.get("external_research") if isinstance(context.get("external_research"), dict) else {}
    official_facts = external.get("official_facts") if isinstance(external.get("official_facts"), dict) else {}
    discovery = context.get("source_discovery") if isinstance(context.get("source_discovery"), dict) else {}
    availability = context.get("data_availability") if isinstance(context.get("data_availability"), dict) else {}
    comparisons = context.get("comparisons") if isinstance(context.get("comparisons"), list) else []
    is_earnings = _is_earnings_setup_context(context)

    official = discovery.get("official_earnings_release") if isinstance(discovery.get("official_earnings_release"), dict) else {}
    sec = discovery.get("sec_filing") if isinstance(discovery.get("sec_filing"), dict) else {}
    rows = [
        _readiness_row("Primary company", bool(identity.get("symbol") and identity.get("company_name")), detail=identity.get("company_name") or identity.get("symbol")),
        _readiness_row("Upcoming earnings date", bool(official_facts.get("upcoming_earnings_date") or availability.get("upcoming earnings date")), required=is_earnings),
        _readiness_row("Official earnings release", official.get("status") == "found", required=is_earnings, detail=official.get("url")),
        _readiness_row("SEC filing", sec.get("status") == "found", required=is_earnings, detail=sec.get("url")),
        _readiness_row("Consensus estimates", bool(availability.get("revenue consensus") and availability.get("eps consensus")), required=is_earnings),
        _readiness_row("Prior quarter results", bool(availability.get("previous quarter revenue") and availability.get("previous quarter eps")), required=is_earnings),
        _readiness_row("Prior quarter consensus", bool(availability.get("previous quarter consensus")), required=False),
        _readiness_row("Prior earnings result", bool(availability.get("previous quarter result")), required=is_earnings),
        _readiness_row("Company guidance", bool(availability.get("guidance")), required=False),
        _readiness_row("Walnut ticker data", bool(primary.get("quote") or primary.get("fundamentals") or primary.get("confirmation"))),
        _readiness_row("Comparison context", bool(comparisons), required=False, detail=", ".join(str(((item.get("identity") or {}).get("symbol") or "")) for item in comparisons if isinstance(item, dict))),
    ]
    missing_required = [row["label"] for row in rows if row.get("required") and row.get("status") != "found"]
    return {
        "status": "ready" if not missing_required else "not_ready",
        "required_for_earnings_setup": is_earnings,
        "rows": rows,
        "missing_requirements": missing_required,
    }


def enforce_research_readiness(context: dict[str, Any]) -> None:
    readiness = research_readiness(context)
    context["research_readiness"] = readiness
    if not readiness.get("required_for_earnings_setup") or readiness.get("status") == "ready":
        return
    missing = ", ".join(str(item) for item in readiness.get("missing_requirements") or [])
    raise HTTPException(
        status_code=422,
        detail=f"Brief not generated: current earnings release or primary financial source could not be verified. Missing: {missing}.",
    )


def _sec_company_record(symbol: str) -> dict[str, Any] | None:
    try:
        response = requests.get(
            "https://www.sec.gov/files/company_tickers.json",
            headers={"User-Agent": "Walnut Markets research generator contact@walnutmarkets.com"},
            timeout=8,
        )
    except requests.RequestException:
        return None
    if response.status_code >= 400:
        return None
    try:
        payload = response.json()
    except ValueError:
        return None
    records = payload.values() if isinstance(payload, dict) else payload if isinstance(payload, list) else []
    for record in records:
        if isinstance(record, dict) and str(record.get("ticker") or "").upper() == symbol.upper():
            return record
    return None


def _sec_company_facts(cik: str) -> dict[str, Any]:
    try:
        response = requests.get(
            f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json",
            headers={"User-Agent": "Walnut Markets research generator contact@walnutmarkets.com"},
            timeout=10,
        )
    except requests.RequestException:
        return {}
    if response.status_code >= 400:
        return {}
    try:
        payload = response.json()
    except ValueError:
        return {}
    us_gaap = ((payload.get("facts") or {}).get("us-gaap") or {}) if isinstance(payload, dict) else {}
    facts = {
        "revenue": _latest_sec_fact(us_gaap, ["Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax", "SalesRevenueNet"]),
        "gross_profit": _latest_sec_fact(us_gaap, ["GrossProfit"]),
        "operating_income": _latest_sec_fact(us_gaap, ["OperatingIncomeLoss"]),
        "net_income": _latest_sec_fact(us_gaap, ["NetIncomeLoss"]),
        "operating_cash_flow": _latest_sec_fact(us_gaap, ["NetCashProvidedByUsedInOperatingActivities"]),
        "capex": _latest_sec_fact(us_gaap, ["PaymentsToAcquirePropertyPlantAndEquipment"]),
        "cash": _latest_sec_fact(us_gaap, ["CashAndCashEquivalentsAtCarryingValue", "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents"]),
        "debt": _latest_sec_fact(us_gaap, ["LongTermDebt", "LongTermDebtAndFinanceLeaseObligationsCurrentAndNoncurrent"]),
        "shares": _latest_sec_fact(us_gaap, ["EntityCommonStockSharesOutstanding", "CommonStocksIncludingAdditionalPaidInCapital"]),
    }
    revenue = facts.get("revenue", {}).get("value") if isinstance(facts.get("revenue"), dict) else None
    gross_profit = facts.get("gross_profit", {}).get("value") if isinstance(facts.get("gross_profit"), dict) else None
    operating_income = facts.get("operating_income", {}).get("value") if isinstance(facts.get("operating_income"), dict) else None
    if revenue:
        if gross_profit is not None:
            facts["gross_margin"] = {"value": round((float(gross_profit) / float(revenue)) * 100, 2), "unit": "%", "derived_from": ["gross_profit", "revenue"]}
        if operating_income is not None:
            facts["operating_margin"] = {"value": round((float(operating_income) / float(revenue)) * 100, 2), "unit": "%", "derived_from": ["operating_income", "revenue"]}
    ocf = facts.get("operating_cash_flow", {}).get("value") if isinstance(facts.get("operating_cash_flow"), dict) else None
    capex = facts.get("capex", {}).get("value") if isinstance(facts.get("capex"), dict) else None
    if ocf is not None and capex is not None:
        facts["free_cash_flow"] = {"value": float(ocf) - abs(float(capex)), "unit": "USD", "derived_from": ["operating_cash_flow", "capex"]}
    return {key: value for key, value in facts.items() if value}


def _latest_sec_fact(us_gaap: dict[str, Any], names: list[str]) -> dict[str, Any] | None:
    for name in names:
        units = ((us_gaap.get(name) or {}).get("units") or {}) if isinstance(us_gaap.get(name), dict) else {}
        rows: list[dict[str, Any]] = []
        for unit, values in units.items():
            if isinstance(values, list):
                rows.extend({**row, "unit": unit, "taxonomy": name} for row in values if isinstance(row, dict) and row.get("val") is not None)
        if rows:
            row = sorted(rows, key=lambda item: (str(item.get("end") or ""), str(item.get("filed") or "")), reverse=True)[0]
            return {
                "value": row.get("val"),
                "unit": row.get("unit"),
                "period_end": row.get("end"),
                "filed": row.get("filed"),
                "form": row.get("form"),
                "taxonomy": row.get("taxonomy"),
            }
    return None


def _missing_key_fields(facts: dict[str, Any]) -> list[str]:
    fact_keys = set(facts.keys())
    required = {
        "revenue",
        "gross_margin",
        "operating_margin",
        "free_cash_flow",
        "capex",
        "cash",
        "debt",
        "shares",
    }
    missing = [field for field in required if field not in fact_keys]
    missing.extend(["guidance", "EBITDA / adjusted EBITDA", "backlog / orders / RPO", "dilution / ATM / offering history", "major customer concentration"])
    return missing


def _dedupe_strings(values: list[str]) -> list[str]:
    return list(dict.fromkeys(str(value) for value in values if str(value).strip()))


def validate_config(config: dict[str, Any], *, strict_selected_model: bool = True) -> dict[str, Any]:
    ticker = config.get("ticker")
    prompt = str(config.get("research_question") or "").strip()
    if not ticker:
        raise HTTPException(status_code=422, detail="Ticker is required.")
    if len(prompt) < 12:
        raise HTTPException(status_code=422, detail="Research question must be more specific.")
    normalized_ticker = normalize_symbol(ticker)
    comparison_tickers = normalize_comparison_tickers(config, primary_ticker=normalized_ticker)
    required_plan = _research_required_plan(config.get("required_plan"), premium_required=bool(config.get("premium_required")))
    normalized = {
        "ticker": normalized_ticker or ticker,
        "research_question": prompt[:3000],
        "desired_angle": _choice(config.get("desired_angle"), ANGLE_OPTIONS, "Full company DD"),
        "comparison_ticker": comparison_tickers[0] if comparison_tickers else None,
        "comparison_tickers": comparison_tickers,
        "time_horizon": _choice(config.get("time_horizon"), TIME_HORIZON_OPTIONS, "Near term"),
        "intended_audience": _choice(config.get("intended_audience"), AUDIENCE_OPTIONS, "Walnut Research Brief"),
        "judgment_preference": _choice(config.get("judgment_preference"), JUDGMENT_OPTIONS, "Let the data decide"),
        "additional_context": str(config.get("additional_context") or "")[:4000],
        "include_sections": _sections(config.get("include_sections")),
        "length": _choice(config.get("length"), LENGTH_OPTIONS, "Standard: 1,500-2,500 words"),
        "tone": _choice(config.get("tone"), TONE_OPTIONS, "Walnut market-native"),
        "external_research_mode": _choice(config.get("external_research_mode"), EXTERNAL_RESEARCH_MODE_OPTIONS, "Standard"),
        "section_format": _choice_from_list(config.get("section_format"), SECTION_FORMAT_OPTIONS, "Walnut Research Brief"),
        "include_charts": bool(config.get("include_charts")),
        "include_source_links": bool(config.get("include_source_links")),
        "include_confirmation_score": bool(config.get("include_confirmation_score")),
        "include_cross_source_confirmations": bool(config.get("include_cross_source_confirmations")),
        "premium_required": required_plan is not None,
        "required_plan": required_plan,
        "generate_thumbnail": bool(config.get("generate_thumbnail", _default_generate_thumbnail(config))),
        "selected_model": str(config.get("selected_model") or "").strip(),
        "hero_image": config.get("hero_image") or "",
        "manual_source_url": _normalize_manual_source_url(config.get("manual_source_url")),
        "target_keyword": str(config.get("target_keyword") or "").strip()[:240],
        "secondary_keywords": _dedupe_strings([str(item).strip()[:120] for item in (config.get("secondary_keywords") or []) if str(item).strip()])[:12],
        "search_intent": str(config.get("search_intent") or "").strip()[:120],
        "content_type": str(config.get("content_type") or "ticker").strip()[:80],
        "retry_output_tokens": _retry_output_tokens(config.get("retry_output_tokens")),
    }
    normalized["selected_model"] = _selected_research_model(normalized, strict=strict_selected_model)
    if not normalized["target_keyword"]:
        normalized["target_keyword"] = normalized["research_question"][:240]
    if not normalized["search_intent"]:
        normalized["search_intent"] = normalized["research_question"][:120]
    if normalized["desired_angle"] == "Peer comparison" and not normalized["comparison_tickers"]:
        raise HTTPException(status_code=422, detail="Comparison tickers are required for peer comparison briefs.")
    return normalized


def _retry_output_tokens(value: Any) -> int:
    try:
        return max(0, min(10000, int(value or 0)))
    except (TypeError, ValueError):
        return 0


def _research_required_plan(value: Any, *, premium_required: bool = False) -> str | None:
    text = str(value or "").strip().lower().replace("_", "-")
    if text in RESEARCH_BRIEF_REQUIRED_PLAN_VALUES:
        return text
    return "premium" if premium_required else None


def _apply_research_access_metadata(article: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
    next_article = deepcopy(article)
    required_plan = _research_required_plan(next_article.get("required_plan"), premium_required=bool(next_article.get("premium_required")))
    if not required_plan:
        required_plan = _research_required_plan(config.get("required_plan"), premium_required=bool(config.get("premium_required")))
    next_article["premium_required"] = required_plan is not None
    next_article["required_plan"] = required_plan
    return next_article


def _research_article_required_plan(article: dict[str, Any]) -> str | None:
    return _research_required_plan(article.get("required_plan"), premium_required=bool(article.get("premium_required")))


def _can_read_full_research_article(article: dict[str, Any], entitlements: Any | None) -> bool:
    required_plan = _research_article_required_plan(article)
    if not required_plan:
        return True
    tier = str(getattr(entitlements, "tier", "") or "").lower()
    rank = int(getattr(entitlements, "rank", 0) or 0)
    if tier == "admin" or rank >= 100:
        return True
    return rank >= (20 if required_plan == "pro" else 10)


def _research_access_payload(article: dict[str, Any], entitlements: Any | None) -> dict[str, Any]:
    required_plan = _research_article_required_plan(article)
    return {
        "premium_required": required_plan is not None,
        "required_plan": required_plan,
        "full_article_visible": _can_read_full_research_article(article, entitlements),
    }


PAYWALL_MARKER_PATTERN = re.compile(r"(?im)^\s*(?:<!--\s*walnut:paywall\s*-->|::walnut-paywall::|\[\[WALNUT_PAYWALL\]\])\s*$")


def _strip_paywall_markers(value: Any) -> str:
    text = str(value or "")
    return PAYWALL_MARKER_PATTERN.sub("", text).strip()


def _split_at_paywall_marker(value: Any) -> tuple[str, str] | None:
    text = str(value or "")
    match = PAYWALL_MARKER_PATTERN.search(text)
    if not match:
        return None
    return text[: match.start()].strip(), text[match.end() :].strip()


def _coerce_preview_section_count(value: Any, section_count: int) -> int | None:
    try:
        count = int(value)
    except (TypeError, ValueError):
        return None
    return max(0, min(count, max(0, section_count)))


def _article_without_paywall_markers(article: dict[str, Any]) -> dict[str, Any]:
    cleaned = deepcopy(article)
    sections = cleaned.get("sections") if isinstance(cleaned.get("sections"), list) else []
    for section in sections:
        if isinstance(section, dict) and isinstance(section.get("body_markdown"), str):
            section["body_markdown"] = _strip_paywall_markers(section["body_markdown"])
    return cleaned


def _preview_sections_for_article(article: dict[str, Any]) -> list[dict[str, Any]]:
    sections = [deepcopy(section) for section in article.get("sections") or [] if isinstance(section, dict)]
    if not sections:
        return []
    for index, section in enumerate(sections):
        split = _split_at_paywall_marker(section.get("body_markdown"))
        if not split:
            continue
        public_body, _gated_body = split
        preview_sections = _article_without_paywall_markers({"sections": sections[:index]}).get("sections") or []
        if public_body:
            public_section = deepcopy(section)
            public_section["body_markdown"] = public_body
            preview_sections.append(public_section)
        return preview_sections
    preview_count = _coerce_preview_section_count(article.get("preview_section_count"), len(sections))
    if preview_count is None:
        preview_count = max(1, min(2, len(sections)))
    return _article_without_paywall_markers({"sections": sections[:preview_count]})["sections"]


def _choice(value: Any, choices: set[str], fallback: str) -> str:
    text = str(value or "").strip()
    return text if text in choices else fallback


def _choice_from_list(value: Any, choices: list[str], fallback: str) -> str:
    text = str(value or "").strip()
    return text if text in choices else fallback


def _default_generate_thumbnail(config: dict[str, Any]) -> bool:
    text = " ".join(str(config.get(key) or "") for key in ("section_format", "intended_audience", "tone"))
    if "Internal Analyst Note" in text:
        return False
    return True


def _sections(value: Any) -> list[str]:
    if not isinstance(value, list):
        return list(DEFAULT_SECTIONS)
    cleaned = [str(item).strip() for item in value if str(item).strip() in DEFAULT_SECTIONS]
    return cleaned or list(DEFAULT_SECTIONS)


def _normalize_manual_source_url(value: Any) -> str:
    url = str(value or "").strip()
    if not url:
        return ""
    if not url.startswith(("https://", "http://")):
        raise HTTPException(status_code=422, detail="Manual source URL must start with https:// or http://.")
    if len(url) > 1000:
        raise HTTPException(status_code=422, detail="Manual source URL is too long.")
    return url


def generate_research_brief(db: Session, admin: UserAccount, config: dict[str, Any], progress_callback: Any | None = None) -> dict[str, Any]:
    normalized_config = validate_config(config)
    normalized_config["selected_model"] = _selected_research_model(normalized_config, db)
    if progress_callback:
        progress_callback("loading_walnut_data", "Loading Walnut data.")
        if normalized_config.get("external_research_mode") != "Off":
            progress_callback("finding_sources", "Finding source context.")
    context = assemble_research_context(db, normalized_config)
    if progress_callback:
        progress_callback("validating_research_readiness", "Validating research readiness.")
    enforce_research_readiness(context)
    actor_key = f"admin:{admin.id}"
    if actor_key in _ACTIVE_GENERATIONS:
        raise HTTPException(status_code=429, detail="A research brief generation is already running for this Admin session.")
    _ACTIVE_GENERATIONS.add(actor_key)
    try:
        started = time.perf_counter()
        if progress_callback:
            progress_callback("generating_brief", "Generating research brief.")
        article = _mock_article(normalized_config, context) if os.getenv(MOCK_ENV) == "1" else _call_openai(db, normalized_config, context)
        article = sanitize_research_brief_article(article, normalized_config, context)
        article["source_links"] = _dedupe_source_links([*(article.get("source_links") or []), *((context.get("external_research") or {}).get("reviewed_sources") or [])])
        article = enrich_internal_links(article, context)
        # Enrichment can add or reshape prose after the initial sanitizer pass.
        # Run the confirmation-score guard once more on the final public copy.
        article = _remove_confirmation_data_conflation(article)
        article["missing_data_notes"] = _filter_missing_data_notes([*(article.get("missing_data_notes") or []), *(context.get("missing_data_notes") or [])], context.get("data_availability") or {})
        if normalized_config.get("generate_thumbnail"):
            if progress_callback:
                progress_callback("generating_thumbnail", "Generating thumbnail.")
            try:
                article["thumbnail_asset"] = generate_thumbnail_asset(db, normalized_config, article)
                if article["thumbnail_asset"].get("url") and not article.get("hero_image"):
                    article["hero_image"] = article["thumbnail_asset"]["url"]
            except Exception as exc:
                logger.warning("research_brief_thumbnail_failed ticker=%s error=%s", normalized_config.get("ticker"), exc.__class__.__name__)
                article["thumbnail_asset"] = {
                    "image_title": str(article.get("title") or normalized_config.get("ticker") or "Walnut research")[:120],
                    "image_prompt": "",
                    "asset_type": _thumbnail_asset_type(normalized_config),
                    "url": "",
                    "thumbnail_url": "",
                    "source_notes": "Thumbnail generation failed; text draft was saved.",
                    "created_at": _now(),
                }
        if progress_callback:
            progress_callback("validating_claims", "Validating generated draft.")
        validation = validate_article(article, context)
        if validation.get("status") == "failed":
            blocking_messages = [
                str(warning.get("message") or warning.get("code"))
                for warning in validation.get("warnings") or []
                if isinstance(warning, dict) and warning.get("blocking")
            ]
            message = "Draft generation failed validation."
            if blocking_messages:
                message = f"{message} {blocking_messages[0]}"
            raise HTTPException(status_code=422, detail=message[:300])
        if progress_callback:
            progress_callback("saving_draft", "Saving generated draft.")
        draft = _new_draft(admin, normalized_config, context, article, validation, elapsed_ms=int((time.perf_counter() - started) * 1000))
        with _STORE_LOCK:
            store = _read_store()
            store["drafts"].append(draft)
            _append_audit(store, action="generate", admin=admin, draft_id=draft["id"], metadata={"ticker": normalized_config["ticker"]})
            _write_store(store)
        try:
            _upsert_db_draft(db, draft)
        except Exception as exc:
            logger.warning("research_brief_draft_db_persist_failed draft_id=%s error=%s", draft.get("id"), exc.__class__.__name__)
        return draft
    finally:
        _ACTIVE_GENERATIONS.discard(actor_key)


def enqueue_research_brief_generation_job(db: Session, admin: UserAccount, config: dict[str, Any]) -> dict[str, Any]:
    normalized_config = validate_config(config)
    normalized_config["selected_model"] = _selected_research_model(normalized_config, db)
    client_request_id = str(config.get("client_request_id") or uuid.uuid4()).strip()[:120]
    now = _now()
    ensure_research_brief_store_schema(db)
    existing = db.execute(
        text(
            """
            SELECT * FROM research_brief_generation_jobs
            WHERE created_by_admin_id = :admin_id AND client_request_id = :client_request_id
            ORDER BY created_at DESC
            LIMIT 1
            """
        ),
        {"admin_id": admin.id, "client_request_id": client_request_id},
    ).mappings().first()
    if existing:
        existing_job = _job_from_row(existing)
        payload = _job_response_payload(existing_job)
        if payload.get("status") == "queued":
            _start_research_brief_job_worker(str(payload["job_id"]))
        return payload
    with _STORE_LOCK:
        store = _read_store()
        jobs = store.setdefault("jobs", [])
        for job in jobs:
            if job.get("created_by_admin_id") == admin.id and job.get("client_request_id") == client_request_id:
                payload = _job_response_payload(job)
                if job.get("status") in {"queued", "running"}:
                    _start_research_brief_job_worker(str(job["id"]))
                return payload
        job = {
            "id": f"rbj_{uuid.uuid4().hex}",
            "status": "queued",
            "client_request_id": client_request_id,
            "created_by_admin_id": admin.id,
            "created_by_admin_email": getattr(admin, "email", None),
            "ticker": normalized_config["ticker"],
            "request_payload_json": normalized_config,
            "model": normalized_config.get("selected_model"),
            "external_research_mode": normalized_config.get("external_research_mode"),
            "section_format": normalized_config.get("section_format"),
            "generate_thumbnail": bool(normalized_config.get("generate_thumbnail")),
            "progress_step": "queued",
            "progress_message": "Research brief generation queued.",
            "source_links_count": 0,
            "numeric_claims_count": 0,
            "validation_status": None,
            "draft_id": None,
            "error_message_safe": None,
            "error_details_internal": None,
            "created_at": now,
            "started_at": None,
            "updated_at": now,
            "completed_at": None,
            "failed_at": None,
        }
        jobs.append(job)
        _write_store(store)
        _upsert_db_job(db, job)
        payload = _job_response_payload(job)
    logger.info(
        "research_brief_job_created job_id=%s ticker=%s model=%s external_research_mode=%s generate_thumbnail=%s",
        payload["job_id"],
        payload.get("ticker"),
        payload.get("model"),
        payload.get("external_research_mode"),
        payload.get("generate_thumbnail"),
    )
    _start_research_brief_job_worker(payload["job_id"])
    return payload


def get_research_brief_generation_job(job_id: str, db: Session | None = None) -> dict[str, Any]:
    if db is not None:
        job = _db_job(db, job_id)
        if job:
            job = _fail_stale_running_job(job_id, job, db)
            payload = _job_response_payload(job)
            if payload["status"] == "queued":
                _start_research_brief_job_worker(job_id)
            return payload
    with _STORE_LOCK:
        job = _find_job(_read_store(), job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Research brief generation job not found.")
        if _job_is_stale_running(job):
            job["status"] = "failed"
            job["progress_step"] = "failed"
            job["progress_message"] = RESEARCH_BRIEF_JOB_STALE_ERROR
            job["error_message_safe"] = RESEARCH_BRIEF_JOB_STALE_ERROR
            job["error_details_internal"] = "Stale running research brief job heartbeat expired."
            job["failed_at"] = _now()
            _write_store(store)
        payload = _job_response_payload(job)
    if payload["status"] == "queued":
        _start_research_brief_job_worker(job_id)
    return payload


def list_generation_jobs(status: str | None = None, db: Session | None = None) -> dict[str, Any]:
    if db is not None:
        ensure_research_brief_store_schema(db)
        if status and status != "all":
            rows = db.execute(text("SELECT * FROM research_brief_generation_jobs WHERE status = :status ORDER BY created_at DESC"), {"status": status}).mappings().all()
        else:
            rows = db.execute(text("SELECT * FROM research_brief_generation_jobs ORDER BY created_at DESC")).mappings().all()
        return {"items": [_job_response_payload(_job_from_row(row)) for row in rows]}
    with _STORE_LOCK:
        jobs = [_job_response_payload(job) for job in (_read_store().get("jobs") or [])]
    if status and status != "all":
        jobs = [job for job in jobs if job.get("status") == status]
    return {"items": sorted(jobs, key=lambda item: item.get("created_at") or "", reverse=True)}


def get_research_brief_generation_job_draft(job_id: str, db: Session | None = None) -> dict[str, Any]:
    job = get_research_brief_generation_job(job_id, db)
    if job["status"] != "completed" or not job.get("draft_id"):
        raise HTTPException(status_code=409, detail="Research brief generation is not complete yet.")
    if db is not None:
        db_job = _db_job(db, job_id)
        draft_payload = (db_job or {}).get("draft_payload_json")
        if isinstance(draft_payload, dict):
            return _draft_with_comparison_tickers(deepcopy(draft_payload))
    return get_draft(str(job["draft_id"]), db=db)


def run_research_brief_generation_job(job_id: str, db: Session | None = None) -> None:
    owns_db = db is None
    session = db or SessionLocal()
    started = time.perf_counter()
    try:
        job = _mark_job_running(job_id, session)
        if job.get("status") == "completed" or job.get("_skip_worker"):
            return
        admin = session.get(UserAccount, job.get("created_by_admin_id"))
        if not admin:
            raise HTTPException(status_code=404, detail="Admin account not found for research brief generation job.")

        def progress(step: str, message: str) -> None:
            _update_job_progress(job_id, step, message, session)

        draft = generate_research_brief(session, admin, dict(job.get("request_payload_json") or {}), progress_callback=progress)
        validation = draft.get("validation") or {}
        _complete_job(job_id, draft, duration_ms=int((time.perf_counter() - started) * 1000), db=session)
        logger.info(
            "research_brief_job_completed job_id=%s ticker=%s model=%s external_research_mode=%s generate_thumbnail=%s duration_ms=%s source_links_count=%s numeric_claims_count=%s validation_status=%s",
            job_id,
            draft.get("primary_ticker"),
            draft.get("model"),
            (draft.get("config") or {}).get("external_research_mode"),
            (draft.get("config") or {}).get("generate_thumbnail"),
            int((time.perf_counter() - started) * 1000),
            validation.get("source_link_count"),
            len(validation.get("numeric_claims") or []),
            validation.get("status"),
        )
    except Exception as exc:
        _fail_job(job_id, exc, duration_ms=int((time.perf_counter() - started) * 1000), db=session)
    finally:
        if owns_db:
            session.close()


def _start_research_brief_job_worker(job_id: str) -> None:
    with _JOB_WORKER_LOCK:
        existing = _JOB_WORKERS.get(job_id)
        if existing and existing.is_alive():
            return
        thread = threading.Thread(target=run_research_brief_generation_job, args=(job_id,), name=f"research-brief-job-{job_id}", daemon=True)
        _JOB_WORKERS[job_id] = thread
        thread.start()


def _mark_job_running(job_id: str, db: Session | None = None) -> dict[str, Any]:
    if db is not None:
        job = _db_job(db, job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Research brief generation job not found.")
        if job.get("status") == "completed":
            return deepcopy(job)
        if job.get("status") in {"failed", "cancelled"}:
            raise HTTPException(status_code=409, detail="Research brief generation job is not active.")
        if job.get("status") == "running" and _job_is_stale_running(job):
            raise TimeoutError("Stale running research brief job heartbeat expired.")
        if job.get("status") == "running":
            payload = deepcopy(job)
            payload["_skip_worker"] = True
            return payload
        job["status"] = "running"
        job["started_at"] = job.get("started_at") or _now()
        job["progress_step"] = "loading_walnut_data"
        job["progress_message"] = "Starting research brief generation."
        _upsert_db_job(db, job)
        payload = deepcopy(job)
        logger.info(
            "research_brief_job_started job_id=%s ticker=%s model=%s external_research_mode=%s generate_thumbnail=%s",
            job_id,
            payload.get("ticker"),
            payload.get("model"),
            payload.get("external_research_mode"),
            payload.get("generate_thumbnail"),
        )
        return payload
    with _STORE_LOCK:
        store = _read_store()
        job = _find_job(store, job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Research brief generation job not found.")
        if job.get("status") == "completed":
            return deepcopy(job)
        if job.get("status") in {"failed", "cancelled"}:
            raise HTTPException(status_code=409, detail="Research brief generation job is not active.")
        if job.get("status") == "running" and _job_is_stale_running(job):
            raise TimeoutError("Stale running research brief job heartbeat expired.")
        job["status"] = "running"
        job["started_at"] = job.get("started_at") or _now()
        job["updated_at"] = _now()
        job["progress_step"] = "loading_walnut_data"
        job["progress_message"] = "Starting research brief generation."
        _write_store(store)
        payload = deepcopy(job)
    logger.info(
        "research_brief_job_started job_id=%s ticker=%s model=%s external_research_mode=%s generate_thumbnail=%s",
        job_id,
        payload.get("ticker"),
        payload.get("model"),
        payload.get("external_research_mode"),
        payload.get("generate_thumbnail"),
    )
    return payload


def _update_job_progress(job_id: str, step: str, message: str, db: Session | None = None) -> None:
    if db is not None:
        job = _db_job(db, job_id)
        if not job or job.get("status") not in {"queued", "running"}:
            return
        job["status"] = "running"
        job["progress_step"] = step
        job["progress_message"] = message
        job["updated_at"] = _now()
        _upsert_db_job(db, job)
        payload = deepcopy(job)
        logger.info(
            "research_brief_job_step job_id=%s ticker=%s model=%s external_research_mode=%s generate_thumbnail=%s progress_step=%s",
            job_id,
            payload.get("ticker"),
            payload.get("model"),
            payload.get("external_research_mode"),
            payload.get("generate_thumbnail"),
            step,
        )
        return
    with _STORE_LOCK:
        store = _read_store()
        job = _find_job(store, job_id)
        if not job or job.get("status") not in {"queued", "running"}:
            return
        job["status"] = "running"
        job["progress_step"] = step
        job["progress_message"] = message
        job["updated_at"] = _now()
        _write_store(store)
        payload = deepcopy(job)
    logger.info(
        "research_brief_job_step job_id=%s ticker=%s model=%s external_research_mode=%s generate_thumbnail=%s progress_step=%s",
        job_id,
        payload.get("ticker"),
        payload.get("model"),
        payload.get("external_research_mode"),
        payload.get("generate_thumbnail"),
        step,
    )


def _complete_job(job_id: str, draft: dict[str, Any], *, duration_ms: int, db: Session | None = None) -> None:
    validation = draft.get("validation") or {}
    if db is not None:
        job = _db_job(db, job_id)
        if not job:
            return
        job["status"] = "completed"
        job["progress_step"] = "completed"
        job["progress_message"] = "Research brief draft generated."
        job["draft_id"] = draft.get("id")
        job["draft_payload_json"] = draft
        job["source_links_count"] = validation.get("source_link_count") or 0
        job["numeric_claims_count"] = len(validation.get("numeric_claims") or [])
        job["validation_status"] = validation.get("status")
        job["completed_at"] = _now()
        job["updated_at"] = job["completed_at"]
        job["failed_at"] = None
        job["error_message_safe"] = None
        job["error_details_internal"] = None
        job["duration_ms"] = duration_ms
        _upsert_db_draft(db, draft)
        _upsert_db_job(db, job)
        return
    with _STORE_LOCK:
        store = _read_store()
        job = _find_job(store, job_id)
        if not job:
            return
        job["status"] = "completed"
        job["progress_step"] = "completed"
        job["progress_message"] = "Research brief draft generated."
        job["draft_id"] = draft.get("id")
        job["source_links_count"] = validation.get("source_link_count") or 0
        job["numeric_claims_count"] = len(validation.get("numeric_claims") or [])
        job["validation_status"] = validation.get("status")
        job["completed_at"] = _now()
        job["updated_at"] = job["completed_at"]
        job["failed_at"] = None
        job["error_message_safe"] = None
        job["error_details_internal"] = None
        job["duration_ms"] = duration_ms
        _write_store(store)


def _fail_job(job_id: str, exc: Exception, *, duration_ms: int, db: Session | None = None) -> None:
    safe_error = _safe_job_error(exc)
    if db is not None:
        job = _db_job(db, job_id)
        if job:
            job["status"] = "failed"
            job["progress_step"] = "failed"
            job["progress_message"] = safe_error
            job["error_message_safe"] = safe_error
            job["error_details_internal"] = f"{exc.__class__.__name__}: {str(exc)[:1000]}"
            job["failed_at"] = _now()
            job["updated_at"] = job["failed_at"]
            job["duration_ms"] = duration_ms
            _upsert_db_job(db, job)
            payload = deepcopy(job)
        else:
            payload = {"ticker": None, "model": None, "external_research_mode": None, "generate_thumbnail": None}
        logger.warning(
            "research_brief_job_failed job_id=%s ticker=%s model=%s external_research_mode=%s generate_thumbnail=%s duration_ms=%s error=%s",
            job_id,
            payload.get("ticker"),
            payload.get("model"),
            payload.get("external_research_mode"),
            payload.get("generate_thumbnail"),
            duration_ms,
            exc.__class__.__name__,
        )
        return
    with _STORE_LOCK:
        store = _read_store()
        job = _find_job(store, job_id)
        if job:
            job["status"] = "failed"
            job["progress_step"] = "failed"
            job["progress_message"] = safe_error
            job["error_message_safe"] = safe_error
            job["error_details_internal"] = f"{exc.__class__.__name__}: {str(exc)[:1000]}"
            job["failed_at"] = _now()
            job["updated_at"] = job["failed_at"]
            job["duration_ms"] = duration_ms
            _write_store(store)
            payload = deepcopy(job)
        else:
            payload = {"ticker": None, "model": None, "external_research_mode": None, "generate_thumbnail": None}
    logger.warning(
        "research_brief_job_failed job_id=%s ticker=%s model=%s external_research_mode=%s generate_thumbnail=%s duration_ms=%s error=%s",
        job_id,
        payload.get("ticker"),
        payload.get("model"),
        payload.get("external_research_mode"),
        payload.get("generate_thumbnail"),
        duration_ms,
        exc.__class__.__name__,
    )


def _safe_job_error(exc: Exception) -> str:
    if isinstance(exc, TimeoutError):
        return RESEARCH_BRIEF_JOB_STALE_ERROR
    if isinstance(exc, HTTPException) and exc.status_code == 422:
        detail = str(exc.detail or "").strip()
        if detail and not re.search(r"\b(provider|internal|cache|raw|token|credential|diagnostic)s?\b", detail, flags=re.IGNORECASE):
            return detail[:300]
    return RESEARCH_BRIEF_JOB_SAFE_ERROR


def _fail_stale_running_job(job_id: str, job: dict[str, Any], db: Session) -> dict[str, Any]:
    if not _job_is_stale_running(job):
        return job
    job = deepcopy(job)
    job["status"] = "failed"
    job["progress_step"] = "failed"
    job["progress_message"] = RESEARCH_BRIEF_JOB_STALE_ERROR
    job["error_message_safe"] = RESEARCH_BRIEF_JOB_STALE_ERROR
    job["error_details_internal"] = "Stale running research brief job heartbeat expired."
    job["failed_at"] = _now()
    job["updated_at"] = job["failed_at"]
    _upsert_db_job(db, job)
    logger.warning(
        "research_brief_job_failed job_id=%s ticker=%s model=%s external_research_mode=%s generate_thumbnail=%s duration_ms=%s error=%s",
        job_id,
        job.get("ticker"),
        job.get("model"),
        job.get("external_research_mode"),
        job.get("generate_thumbnail"),
        job.get("duration_ms"),
        "StaleRunningJob",
    )
    return job


def _job_is_stale_running(job: dict[str, Any]) -> bool:
    if job.get("status") != "running":
        return False
    heartbeat = _parse_iso_datetime(job.get("updated_at") or job.get("started_at") or job.get("created_at"))
    if heartbeat is None:
        return True
    return (datetime.now(timezone.utc) - heartbeat).total_seconds() > _env_float(RESEARCH_BRIEF_JOB_STALE_SECONDS, 300.0)


def _parse_iso_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(timezone.utc) if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _find_job(store: dict[str, Any], job_id: str) -> dict[str, Any] | None:
    for job in store.get("jobs") or []:
        if job.get("id") == job_id:
            return job
    return None


def _job_response_payload(job: dict[str, Any]) -> dict[str, Any]:
    return {
        "job_id": job.get("id"),
        "status": job.get("status"),
        "client_request_id": job.get("client_request_id"),
        "ticker": job.get("ticker"),
        "model": job.get("model"),
        "external_research_mode": job.get("external_research_mode"),
        "section_format": job.get("section_format"),
        "generate_thumbnail": job.get("generate_thumbnail"),
        "progress_step": job.get("progress_step"),
        "progress_message": job.get("progress_message"),
        "source_links_count": job.get("source_links_count") or 0,
        "numeric_claims_count": job.get("numeric_claims_count") or 0,
        "validation_status": job.get("validation_status"),
        "draft_id": job.get("draft_id"),
        "error_message_safe": job.get("error_message_safe"),
        "created_at": job.get("created_at"),
        "started_at": job.get("started_at"),
        "updated_at": job.get("updated_at"),
        "completed_at": job.get("completed_at"),
        "failed_at": job.get("failed_at"),
    }


def _call_openai(db: Session, config: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    api_key = resolved_setting_value(db, OPENAI_API_KEY)
    if not api_key:
        raise HTTPException(status_code=503, detail="OpenAI API key missing. Configure OPENAI_API_KEY before generating.")
    model = _selected_research_model(config, db)
    max_output_tokens = _retry_output_tokens(config.get("retry_output_tokens")) or _max_output_tokens(config["length"])
    response = requests.post(
        RESPONSES_ENDPOINT,
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json={
            "model": model,
            "input": _prompt(config, context),
            "store": False,
            "max_output_tokens": max_output_tokens,
            "text": {"format": {"type": "json_schema", "name": "walnut_research_brief", "schema": article_schema(), "strict": True}},
        },
        timeout=_env_float(RESEARCH_BRIEF_OPENAI_TIMEOUT_SECONDS, 90.0),
    )
    if response.status_code >= 400:
        _raise_openai_response_error(response, operation="research brief generation")
    data = response.json()
    text = _response_text(data)
    try:
        parsed = json.loads(text)
    except Exception:
        raise HTTPException(status_code=502, detail="OpenAI returned invalid structured research JSON.")
    if not isinstance(parsed, dict):
        raise HTTPException(status_code=502, detail="OpenAI returned an invalid article payload.")
    parsed["_generation_usage"] = data.get("usage") if isinstance(data.get("usage"), dict) else {}
    parsed["_model"] = model
    return parsed


def _response_text(data: dict[str, Any]) -> str:
    if isinstance(data.get("output_text"), str):
        return data["output_text"]
    for output in data.get("output") or []:
        for content in output.get("content") or []:
            if isinstance(content.get("text"), str):
                return content["text"]
    return ""


def _max_output_tokens(length: str) -> int:
    if length.startswith("Short"):
        return 3500
    if length.startswith("Deep"):
        return 9000
    return 6000


def _env_float(name: str, fallback: float) -> float:
    try:
        return max(float(os.getenv(name, "") or fallback), 1.0)
    except (TypeError, ValueError):
        return fallback


def _prompt(config: dict[str, Any], context: dict[str, Any]) -> str:
    section_format = _section_format_instructions(config.get("section_format") or "Walnut Research Brief")
    prompt_config = dict(config)
    prompt_config.pop("comparison_ticker", None)
    primary_identity = ((context.get("primary") or {}).get("identity") or {}) if isinstance(context.get("primary"), dict) else {}
    primary_symbol = str(primary_identity.get("symbol") or config.get("ticker") or "").upper()
    primary_company = str(primary_identity.get("company_name") or primary_symbol or "").strip()
    comparison_symbols = [
        str(((item.get("identity") or {}).get("symbol") or "")).upper()
        for item in (context.get("comparisons") or [])
        if isinstance(item, dict)
    ]
    return "\n".join(
        [
            "You are Walnut's senior market research editor writing a publishable research brief in Walnut's investor-to-investor voice.",
            f"PRIMARY_TICKER: {primary_symbol}",
            f"PRIMARY_COMPANY: {primary_company}",
            f"COMPARISON_TICKERS: {', '.join(comparison_symbols) if comparison_symbols else 'None'}",
            "Every company-specific statement must be about PRIMARY_COMPANY unless it is explicitly framed as comparison, industry, or macro context. Do not analyze Nvidia, AMD, CoreWeave, or any other company as the subject unless that ticker is listed in COMPARISON_TICKERS.",
            "Use Walnut data, external research notes, and reviewed public source links. Do not invent metrics, quotes, filings, historical changes, catalysts, or source links.",
            "The target search query is the organizing question, not a phrase to repeat. Answer it immediately, then earn the conclusion with Walnut-native evidence.",
            "Walnut site context contains only approved first-party pages. Use 2-4 relevant internal links where they genuinely help a reader navigate; do not create random keyword links or link every sentence.",
            "If core earnings research is unavailable, do not write around it. The backend should stop generation before this prompt. Never write paragraphs saying Walnut needs to go find the data.",
            "Treat data_availability as authoritative. Do not say price, volume, price/volume and technicals, revenue consensus, EPS consensus, gross margin, free cash flow, valuation, reported institutional activity, insider activity, Congress activity, or government contracts are missing when data_availability marks that field available.",
            "Do not say an item was 'not independently verified in reviewed primary sources' when the item is present in Walnut context or marked available in data_availability.",
            "Only list fields from missing_data_notes as missing. If a dataset is available but empty or limited, describe the actual availability/result instead of calling the whole category not found.",
            "Only include our proprietary confirmation score if include_confirmation_score is true. If include_confirmation_score is true, publish the primary ticker score from Walnut context; do not look for it in external reviewed sources. If include_confirmation_score is false, omit it entirely and do not explain that we do not publish it.",
            "Only include cross-source confirmation commentary if include_cross_source_confirmations is true. Keep confirmation score and cross-source confirmations separate.",
            "The confirmation score is our proprietary score. Cross-source confirmations are qualitative supporting or contradicting data categories such as price/volume, fundamentals, reported institutional activity, Congress activity, insider activity, government contracts, options flow, and macro positioning. Use 'data,' not 'stack.'",
            "Never cite the admin prompt, user request, research request, supplied materials, supplied context, research configuration, or model instructions as a source. User-provided numbers are leads to verify, not sources.",
            "Any publishable research/DD post must include at least two credible source links, and valuation/DD work should include an official/company/filing source when possible.",
            "For external research, verify official company data, estimates, and guidance with official company materials, SEC filings, or credible market/estimate sources before using them.",
            "For major-ticker earnings setup briefs, official company earnings and SEC/IR sources in source_discovery are required. If official_facts includes a latest_official_quarter, use those reported values first and do not substitute stale prior-year data.",
            "Write numeric claims cleanly. Round percentages and ratios to at most two decimal places, strip useless trailing zeros, and never output raw provider decimals such as 17.123456%.",
            "Separate underlying data from our confirmation score. Missing data is unavailable, not zero and not bearish.",
            "For earnings setup briefs, do not default to 'mixed / wait for the print.' Missing one or two data categories should lower confidence, not automatically force a no-call judgment.",
            "The Walnut call must be the full final judgment. Do not output a separate setup label. Allowed Walnut calls are: " + ", ".join(WALNUT_CALL_VALUES) + ".",
            "For earnings setup briefs, use this plain-text call format in the final call section: 'Our call: [allowed call]'. Do not wrap it in markdown bold markers. Mixed should be rare; use a more specific call such as Bullish but expensive, Neutral but expensive, Neutral with capex risk, or Mixed with capex risk when that is what the evidence says.",
            "For earnings setup briefs, if the business is strong but valuation or expectations are high, use Bullish but expensive or Neutral but expensive. If the business is strong but capex/free cash flow is the main market risk, use Neutral with capex risk or Mixed with capex risk. Use Insufficient data to make a call only when required primary data is unavailable.",
            "For earnings previews, lead with numbers: consensus revenue/EPS, prior quarter revenue/EPS versus consensus, prior reaction if available, and the main setup. Avoid broad industry throat-clearing.",
            "For earnings previews, prefer this structure: Opening setup; What changed since last earnings; The numbers that matter; Business and fundamentals; Price / positioning; Bull case; Bear case; What we're watching; The call.",
            "Selected sections are conditional: include a selected section only when meaningful supported data exists. If a selected data area is empty, use one short factual line instead of filler.",
            "After the call line, write 2-4 sentences covering what the business data says, what the market issue is, and what would confirm or break the call.",
            "Use 'data', not 'stack'. Use 'reported' or 'disclosed' for Congress, insider, and institutional activity. For 13F data, say 'reported institutional activity', 'filing date', and 'quarter-end holdings'; never imply live institutional buying.",
            "Never expose provider, internal, cache, raw, token, credential, or diagnostic wording in user-facing copy.",
            "For DCF/valuation briefs, do not produce a fake DCF when inputs are missing. Separate reported numbers from assumptions and say when a DCF cannot be anchored.",
            "Do not imply financial advice, guaranteed returns, congressional intent, insider wrongdoing, or real-time 13F activity.",
            "Core Walnut tone: Assess the data, not the hype.",
            "Write like an experienced investor explaining the setup to another experienced investor: concise, human, skeptical, data-first, specific, conversational without getting sloppy, and opinionated only where the evidence supports it.",
            "Answer the headline question in the opening 2-4 short paragraphs. Get to the data quickly. Use real numbers. Acknowledge conflicting evidence. Clearly separate fact from interpretation. Do not pretend every stock has a strong conclusion.",
            "For ticker question briefs, prefer natural sections such as Quick answer, What earnings changed, What our data is seeing, Fundamentals, Price / technical context, Bull case, Bear case, What to watch next, and Bottom line. Do not force every Walnut dataset into the article.",
            "If Congress, insider, institutional, contracts, options, macro, or analyst data is unavailable or irrelevant, omit that section. Do not turn missing data into paragraphs.",
            "Strict copy rules: never write 'The reviewed record supplied for this brief does not contain', 'The available information is insufficient to assess', 'Investors should carefully consider', 'In today's rapidly evolving market', 'Unlock', 'Delve', 'Robust', 'Comprehensive', 'Holistic', 'Investment case', or 'Vibes'.",
            "Avoid generic AI phrasing, throat-clearing, and template transitions such as 'the central question,' 'against this backdrop,' 'on balance,' 'evidence suggests,' 'the appropriate next step,' 'credible bull case requires,' 'we reserve judgment,' 'It is important to note,' 'Looking ahead,' 'Overall,' 'In conclusion,' 'This article will examine,' and repeated 'investors should monitor.'",
            "Prefer active sentences that sound like a senior analyst wrote them after reading the data. Do not become promotional, cute, or chatty.",
            "Use comparison_tickers only where relevant. Do not force every comparison ticker into every section. If comparison data is unavailable, say so clearly. Do not invent data. Use the comparisons to compare growth, margins, capex, valuation, cash flow, and market setup where available.",
            "End with a clear judgment. Do not add generic investment disclaimers inside the article body; Walnut's public legal/footer language handles that.",
            "Use first-person plural for our own views, data, takes, and confirmation score. Say 'our take' or 'our confirmation score,' not 'Walnut's take' or 'Walnut's confirmation score.'",
            "The JSON summary is the Insights preview body. Keep it 1-3 sentences and do not duplicate the full post body.",
            "Return metadata fields walnut_call and confirmation_score_included. confirmation_score_included must reflect whether the score appears in the body.",
            "Section format instructions:",
            section_format,
            "Key missing-field search checklist:",
            json.dumps(KEY_RESEARCH_FIELDS, indent=2),
            "Return only JSON matching the provided schema.",
            "Admin configuration:",
            json.dumps(prompt_config, indent=2, sort_keys=True),
            "Walnut research context:",
            json.dumps(context, indent=2, sort_keys=True, default=str)[:18000],
        ]
    )


def _section_format_instructions(section_format: str) -> str:
    if section_format == "Reddit DD - Issue / Risk / Data / Conclusion":
        return (
            "Use this markdown structure: Intro / hook; The issue; The risk / opportunity; The data; Conclusion; Sources; optional What to watch next. "
            "The issue explains what the market is debating and why now. The risk / opportunity names what can go right, what can go wrong, what kills the thesis, and what confirms it. "
            "The data uses concrete sourced numbers without burying unavailable fields. Conclusion makes a bullish, bearish, mixed, or insufficient-data call. Research only. Not investment advice."
        )
    if section_format == "Reddit DD - Bull Case / Bear Case / The Data / The Call":
        return "Use this markdown structure: Executive thesis; Bull case; Bear case; The data; The call; What to watch next; Sources; Data freshness and limitations. Do not include an Intro / hook heading."
    if section_format == "ValueInvesting - Business / Valuation / Risks / Margin of Safety":
        return "Use this markdown structure: Business; Valuation; Risks; Margin of safety; Sources; What to watch next. Emphasize cash flow, downside case, assumptions, and valuation limits."
    if section_format == "X Thread":
        return "Write as a concise X thread draft with numbered posts, each source-backed and readable without hype."
    if section_format == "Internal Analyst Note":
        return "Write as an internal analyst note. Thumbnail generation is optional and the tone can be more terse, but unsupported claims still fail validation."
    return "Use Walnut Research Brief sections with a clear thesis, data, risks, catalysts, conclusion, sources, and data limitations."


def article_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "title",
            "slug",
            "subtitle",
            "summary",
            "preview_body",
            "judgment",
            "walnut_call",
            "confidence",
            "confirmation_score_included",
            "primary_ticker",
            "comparison_tickers",
            "category",
            "reading_minutes",
            "preview_section_count",
            "hero_image",
            "current_data_as_of",
            "premium_required",
            "required_plan",
            "paywall_copy",
            "analytics",
            "reddit_post",
            "thumbnail_asset",
            "sections",
            "key_points",
            "catalysts",
            "risks",
            "watch_items",
            "data_freshness",
            "missing_data_notes",
            "source_links",
            "suggested_card",
            "seo",
        ],
        "properties": {
            "title": {"type": "string"},
            "slug": {"type": "string"},
            "subtitle": {"type": "string"},
            "summary": {"type": "string"},
            "preview_body": {"type": "string"},
            "judgment": {"type": "string", "enum": sorted(JUDGMENT_VALUES)},
            "walnut_call": {"type": "string", "enum": WALNUT_CALL_VALUES},
            "confidence": {"type": "string", "enum": ["low", "medium", "high"]},
            "confirmation_score_included": {"type": "boolean"},
            "primary_ticker": {"type": "string"},
            "comparison_tickers": {"type": "array", "items": {"type": "string"}},
            "category": {"type": "string"},
            "reading_minutes": {"type": "integer"},
            "preview_section_count": {"type": "integer", "minimum": 0},
            "hero_image": {"type": ["string", "null"]},
            "current_data_as_of": {"type": "string"},
            "premium_required": {"type": "boolean"},
            "required_plan": {"type": ["string", "null"]},
            "paywall_copy": {
                "type": "object",
                "additionalProperties": False,
                "required": ["heading", "description", "cta_label"],
                "properties": {
                    "heading": {"type": "string"},
                    "description": {"type": "string"},
                    "cta_label": {"type": "string"},
                },
            },
            "analytics": {
                "type": "object",
                "additionalProperties": False,
                "required": [],
                "properties": {},
            },
            "reddit_post": {"type": "string"},
            "thumbnail_asset": {
                "type": "object",
                "additionalProperties": False,
                "required": [],
                "properties": {},
            },
            "sections": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["key", "heading", "body_markdown"],
                    "properties": {"key": {"type": "string"}, "heading": {"type": "string"}, "body_markdown": {"type": "string"}},
                },
            },
            "key_points": {"type": "array", "items": {"type": "string"}},
            "catalysts": {"type": "array", "items": {"type": "string"}},
            "risks": {"type": "array", "items": {"type": "string"}},
            "watch_items": {"type": "array", "items": {"type": "string"}},
            "data_freshness": {"type": "array", "items": {"type": "string"}},
            "missing_data_notes": {"type": "array", "items": {"type": "string"}},
            "source_links": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["label", "url", "source_type"],
                    "properties": {
                        "label": {"type": "string"},
                        "url": {"type": "string"},
                        "source_type": {"type": "string"},
                    },
                },
            },
            "suggested_card": {
                "type": "object",
                "additionalProperties": False,
                "required": ["title", "description", "judgment", "tickers"],
                "properties": {
                    "title": {"type": "string"},
                    "description": {"type": "string"},
                    "judgment": {"type": "string"},
                    "tickers": {"type": "array", "items": {"type": "string"}},
                },
            },
            "seo": {
                "type": "object",
                "additionalProperties": False,
                "required": ["title", "description"],
                "properties": {"title": {"type": "string"}, "description": {"type": "string"}},
            },
        },
    }


def validate_article(article: dict[str, Any], context: dict[str, Any], draft_id: str | None = None) -> dict[str, Any]:
    warnings: list[dict[str, Any]] = []
    blocking = False
    title = str(article.get("title") or "").strip()
    body = "\n\n".join(str(section.get("body_markdown") or "") for section in article.get("sections") or [] if isinstance(section, dict))
    rendered_markdown = "\n\n".join(
        f"## {section.get('heading')}\n\n{section.get('body_markdown') or ''}"
        for section in article.get("sections") or []
        if isinstance(section, dict)
    )
    slug = _slugify(str(article.get("slug") or title), fallback=f"{context['primary']['identity']['symbol'].lower()}-research-brief")
    repair_count = int(article.get("_copy_sanitizer_repairs") or 0)
    labels = {
        "structure": "repaired" if repair_count else "passed",
        "internal_language": "repaired" if repair_count else "passed",
        "source_support": "passed",
        "missing_data_language": "repaired" if repair_count else "passed",
        "research_readiness": "passed",
        "company_identity": "passed",
        "numeric_validation": "passed",
        "style": "passed",
    }
    readiness = context.get("research_readiness") if isinstance(context.get("research_readiness"), dict) else research_readiness(context)
    if readiness.get("status") == "not_ready":
        missing = ", ".join(str(item) for item in readiness.get("missing_requirements") or [])
        warnings.append(_warning("research_not_ready", f"Research readiness is not ready. Missing: {missing}.", blocking=True))
        labels["research_readiness"] = "failed"
        blocking = True
    source_discovery = context.get("source_discovery") if isinstance(context.get("source_discovery"), dict) else {}
    source_diagnostic_warnings = _source_discovery_validation_warnings(context)
    if source_diagnostic_warnings:
        warnings.extend(source_diagnostic_warnings)
        labels["source_support"] = "failed"
        blocking = True
    if _required_official_source_omitted(article, context):
        warnings.append(_warning("official_source_link_omitted", "Official company earnings source must remain in source links before publishing.", blocking=True))
        labels["source_support"] = "failed"
        blocking = True
    primary_match_warning = _primary_context_match_warning(context)
    if primary_match_warning:
        warnings.append(primary_match_warning)
        labels["source_support"] = "failed"
        labels["company_identity"] = "failed"
        blocking = True
    company_identity_warnings = _company_identity_warnings(article, context)
    if company_identity_warnings:
        warnings.extend(company_identity_warnings)
        labels["company_identity"] = "failed"
        blocking = True
    if not title:
        warnings.append(_warning("missing_title", "Title is required.", blocking=True))
        blocking = True
    if len(body) < 800:
        warnings.append(_warning("thin_body", "Article body appears too short for a professional research brief.", blocking=True))
        blocking = True
    summary_text = f"{article.get('summary') or ''}\n{article.get('preview_body') or ''}"
    if "not investment advice" not in body.lower() and "not investment advice" not in summary_text.lower():
        warnings.append(_warning("missing_disclaimer", "Research-only / not-investment-advice language is missing from the body; rely on the Walnut legal/footer disclaimer when appropriate.", blocking=False))
    lowered = f"{title}\n{body}".lower()
    article_score_value = _article_confirmation_score_value(article)
    thematic_research = _is_thematic_research(article, context)
    include_confirmation_score = bool(context.get("include_confirmation_score") or article.get("confirmation_score_included"))
    include_cross_source_confirmations = bool(context.get("include_cross_source_confirmations"))
    source_link_count = _source_link_count(article, body)
    if source_link_count == 0:
        warnings.append(_warning("missing_source_links", "This draft has no source links. Regenerate with External Research Mode enabled or add sources manually.", blocking=True))
        labels["source_support"] = "failed"
        blocking = True
    elif source_link_count < 2:
        warnings.append(_warning("insufficient_source_links", "Research briefs need at least 2 credible source links before publishing.", blocking=True))
        labels["source_support"] = "failed"
        blocking = True
    for phrase in UNSUPPORTED_LANGUAGE:
        if phrase in lowered:
            warnings.append(_warning("unsupported_language", f"Unsupported language detected: {phrase}", blocking=True))
            blocking = True
    if "not supplied" in lowered:
        warnings.append(_warning("not_supplied_language", "Use 'Not found in reviewed sources' once in Data limitations instead of repeated 'not supplied' language.", blocking=True))
        labels["missing_data_language"] = "failed"
        blocking = True
    missing_language_hits = _missing_data_language_hits(lowered)
    if missing_language_hits:
        warnings.append(_warning("awkward_missing_data_language", f"Missing-data wording needs cleanup: {', '.join(missing_language_hits)}.", blocking=True))
        labels["missing_data_language"] = "failed"
        blocking = True
    internal_language_hits = _internal_language_hits(lowered)
    if internal_language_hits:
        warnings.append(_warning("internal_workflow_language", f"Internal workflow language remains: {', '.join(internal_language_hits)}.", blocking=True))
        labels["internal_language"] = "failed"
        blocking = True
    structure_issues = _markdown_structure_issues(rendered_markdown)
    if structure_issues:
        warnings.append(_warning("markdown_structure", f"Markdown structure needs cleanup: {', '.join(structure_issues)}.", blocking=True))
        labels["structure"] = "failed"
        blocking = True
    if re.search(r"\b(internal|token|credential|diagnostic)s?\b", lowered) or re.search(
        r"\b(?:price_cache|confirmation_monitoring_events|provider\s+cache|raw\s+(?:response|payload|json|context|source))\b",
        lowered,
    ):
        warnings.append(_warning("internal_wording", "Provider/internal/cache/source-system wording must not appear in user-facing output.", blocking=True))
        labels["internal_language"] = "failed"
        blocking = True
    if "confirmation score equals" in lowered or "confirmation stack" in lowered:
        warnings.append(_warning("confirmation_score_blended", "Confirmation score must remain separate from underlying data.", blocking=True))
        blocking = True
    if include_confirmation_score and _confirmation_score_value(context) is None and article_score_value is None:
        symbol = (((context.get("primary") or {}).get("identity") or {}).get("symbol") or "the primary ticker")
        warnings.append(_warning("confirmation_score_unavailable", f"Primary ticker confirmation score could not be loaded for {symbol}.", blocking=True))
        blocking = True
    elif include_confirmation_score and not (_article_includes_confirmation_score_value(article, context) or article_score_value is not None):
        warnings.append(_warning("confirmation_score_missing_from_body", "Walnut confirmation score is checked, but the post body does not include the primary ticker score.", blocking=True))
        blocking = True
    if not include_confirmation_score and _article_mentions_confirmation_score(article) and not thematic_research:
        warnings.append(_warning("confirmation_score_not_requested", "Walnut confirmation score is unchecked, but the article mentions the confirmation score.", blocking=True))
        blocking = True
    if not include_cross_source_confirmations and _article_mentions_cross_source_confirmations(article):
        warnings.append(_warning("cross_source_confirmations_not_requested", "Cross-source confirmations are unchecked, but the post body includes cross-source confirmation commentary.", blocking=True))
        blocking = True
    if include_cross_source_confirmations and _conflates_confirmation_score_with_data(lowered):
        warnings.append(_warning("confirmation_score_conflated", "Cross-source data categories must not be described as the proprietary confirmation score.", blocking=True))
        blocking = True
    has_call_metadata = "walnut_call" in article or _is_earnings_setup_context(context)
    if has_call_metadata and _normalize_walnut_call(article.get("walnut_call")) is None and not thematic_research:
        warnings.append(_warning("invalid_walnut_call", f"Walnut call must be one of: {', '.join(WALNUT_CALL_VALUES)}.", blocking=True))
        blocking = True
    if str(article.get("walnut_call") or "").strip().lower().endswith(" setup"):
        warnings.append(_warning("setup_label_used_as_call", "Do not use setup labels; Walnut call must be the full final judgment.", blocking=True))
        blocking = True
    if _is_earnings_setup_context(context):
        earnings_call = _article_walnut_call(article)
        if earnings_call not in WALNUT_CALL_VALUES:
            warnings.append(
                _warning(
                    "earnings_walnut_call",
                    "Earnings setup briefs must include a single expanded Walnut call.",
                    blocking=True,
                )
            )
            blocking = True
        if _uses_stale_earnings_year(lowered, context):
            warnings.append(_warning("stale_year_substitution", "Draft appears to use stale earnings-year data while newer official earnings data is available.", blocking=True))
            labels["source_support"] = "failed"
            blocking = True
    contradicted_fields = _available_data_missing_claims(lowered, context)
    if contradicted_fields:
        warnings.append(
            _warning(
                "available_data_marked_missing",
                f"Draft says available Walnut data is missing: {', '.join(contradicted_fields)}.",
                blocking=True,
            )
        )
        blocking = True
    numeric_claims = sorted(set(re.findall(r"(?<![A-Za-z])(?:\$?\d[\d,]*(?:\.\d+)?%?|\d+\s?bps)(?![A-Za-z])", body)))[:80]
    numeric_format_issues = _numeric_format_issues(body)
    if numeric_format_issues:
        warnings.append(_warning("numeric_formatting", f"Numeric formatting needs cleanup: {', '.join(numeric_format_issues)}.", blocking=True))
        labels["numeric_validation"] = "failed"
        blocking = True
    if numeric_claims and not _context_has_numbers(context):
        warnings.append(_warning("numeric_claims_without_context", "Numeric claims detected while source context has few numeric fields.", blocking=True))
        labels["source_support"] = "failed"
        labels["numeric_validation"] = "failed"
        blocking = True
    style_warnings = _style_validation_warnings(article, context)
    if style_warnings:
        warnings.extend(style_warnings)
        labels["style"] = "failed"
        blocking = True
    if _duplicate_slug(slug, draft_id=draft_id):
        warnings.append(_warning("duplicate_slug", f"Slug '{slug}' is already published or reserved.", blocking=True))
        blocking = True
    site_context = context.get("walnut_site_context") if isinstance(context.get("walnut_site_context"), dict) else {}
    internal_links = _internal_link_count(article, body)
    inbound_opportunities = len(site_context.get("links") or []) if isinstance(site_context.get("links"), list) else 0
    labels["search_intent"] = "passed" if context.get("search_intent") or context.get("research_question") else "failed"
    labels["walnut_native_data"] = "passed" if _context_has_numbers(context) else "failed"
    labels["internal_links"] = "passed" if internal_links else "failed"
    if not internal_links:
        warnings.append(_warning("missing_internal_links", "No Walnut internal links found. Add the relevant ticker page or research hub before publishing.", blocking=False))
    overlaps = context.get("potential_overlap") if isinstance(context.get("potential_overlap"), list) else []
    if overlaps:
        warnings.append(_warning("potential_overlap", "Potential overlap with existing research. Review the linked brief and choose whether to update or publish a distinct angle.", blocking=False))
    if not article.get("hero_image"):
        warnings.append(_warning("missing_hero_image", "No hero image selected; the public page will use the polished fallback.", blocking=False))
    return {
        "status": "failed" if blocking else "passed",
        "warnings": warnings,
        "numeric_claims": numeric_claims,
        "source_link_count": source_link_count,
        "estimated_reading_minutes": max(1, round(len(body.split()) / 220)),
        "labels": labels,
        "source_discovery": source_discovery,
        "research_readiness": readiness,
        "publication_readiness": {
            "search_intent_identified": bool(context.get("search_intent") or context.get("research_question")),
            "walnut_native_data_included": _context_has_numbers(context),
            "confirmation_score_included": include_confirmation_score,
            "unique_thesis": not bool(overlaps),
            "internal_links_out": internal_links,
            "inbound_link_opportunities": inbound_opportunities,
            "data_freshness": context.get("generated_at") or "unknown",
            "potential_cannibalization": overlaps,
        },
    }


def _internal_link_count(article: dict[str, Any], body: str) -> int:
    links = article.get("source_links") if isinstance(article.get("source_links"), list) else []
    linked_urls = [str(link.get("url") or "") for link in links if isinstance(link, dict)]
    inline_urls = re.findall(r"\]\((/[^)\s]+)\)", body)
    return len({url for url in [*linked_urls, *inline_urls] if url.startswith("/")})


def _source_discovery_validation_warnings(context: dict[str, Any]) -> list[dict[str, Any]]:
    source_discovery = context.get("source_discovery") if isinstance(context.get("source_discovery"), dict) else {}
    primary = context.get("primary") if isinstance(context.get("primary"), dict) else {}
    symbol = str(((primary.get("identity") or {}).get("symbol") or "")).upper()
    if not source_discovery.get("required_for_earnings_setup") and not source_discovery.get("required_for_major_earnings_setup"):
        return []
    warnings: list[dict[str, Any]] = []
    official = source_discovery.get("official_earnings_release") if isinstance(source_discovery.get("official_earnings_release"), dict) else {}
    sec = source_discovery.get("sec_filing") if isinstance(source_discovery.get("sec_filing"), dict) else {}
    if official.get("status") != "found":
        warnings.append(_warning("missing_official_earnings_source", "Official company earnings source was not found. Regenerate or add source manually.", blocking=True))
        if symbol:
            warnings.append(_warning("official_earnings_retrieval_failed", f"Official earnings release could not be retrieved for {symbol}.", blocking=True))
    if sec.get("status") != "found":
        warnings.append(_warning("missing_sec_or_ir_source", "At least one SEC or company IR source is required for earnings setup briefs.", blocking=True))
    official_facts = ((context.get("external_research") or {}).get("official_facts") or {}) if isinstance(context.get("external_research"), dict) else {}
    if symbol == "AAPL" and official_facts.get("latest_official_quarter") != "Q2 FY2026":
        warnings.append(_warning("stale_official_earnings_source", "AAPL earnings setup must use Apple Q2 FY2026 official results when available.", blocking=True))
    return warnings


def _required_official_source_omitted(article: dict[str, Any], context: dict[str, Any]) -> bool:
    source_discovery = context.get("source_discovery") if isinstance(context.get("source_discovery"), dict) else {}
    if not source_discovery.get("required_for_earnings_setup") and not source_discovery.get("required_for_major_earnings_setup"):
        return False
    source_links = article.get("source_links") if isinstance(article.get("source_links"), list) else []
    return not any(
        isinstance(source, dict)
        and str(source.get("source_type") or "") in {"official_company_earnings", "official_company_ir", "manual_official_source"}
        for source in source_links
    )


def _uses_stale_earnings_year(lowered_text: str, context: dict[str, Any]) -> bool:
    primary = context.get("primary") if isinstance(context.get("primary"), dict) else {}
    symbol = str(((primary.get("identity") or {}).get("symbol") or "")).upper()
    official_facts = ((context.get("external_research") or {}).get("official_facts") or {}) if isinstance(context.get("external_research"), dict) else {}
    latest_quarter = str(official_facts.get("latest_official_quarter") or "")
    if symbol == "AAPL" and latest_quarter == "Q2 FY2026":
        return "q2 2025" in lowered_text or "fy2025" in lowered_text
    return False


def _primary_context_match_warning(context: dict[str, Any]) -> dict[str, Any] | None:
    primary = context.get("primary") if isinstance(context.get("primary"), dict) else {}
    identity = primary.get("identity") if isinstance(primary.get("identity"), dict) else {}
    expected = normalize_symbol(identity.get("symbol"))
    embedded = normalize_symbol((primary.get("confirmation") or {}).get("symbol") if isinstance(primary.get("confirmation"), dict) else None)
    if expected and embedded and expected != embedded:
        return _warning("primary_ticker_context_mismatch", f"Primary ticker context mismatch: expected {expected}, received {embedded}.", blocking=True)
    return None


def _article_full_text(article: dict[str, Any]) -> str:
    pieces = [
        str(article.get("title") or ""),
        str(article.get("subtitle") or ""),
        str(article.get("summary") or ""),
        str(article.get("preview_body") or ""),
        str((article.get("suggested_card") or {}).get("title") or "") if isinstance(article.get("suggested_card"), dict) else "",
        str((article.get("suggested_card") or {}).get("description") or "") if isinstance(article.get("suggested_card"), dict) else "",
    ]
    for section in article.get("sections") or []:
        if isinstance(section, dict):
            pieces.append(str(section.get("heading") or ""))
            pieces.append(str(section.get("body_markdown") or ""))
    return "\n\n".join(piece for piece in pieces if piece)


def _company_identity_warnings(article: dict[str, Any], context: dict[str, Any]) -> list[dict[str, Any]]:
    primary = context.get("primary") if isinstance(context.get("primary"), dict) else {}
    identity = primary.get("identity") if isinstance(primary.get("identity"), dict) else {}
    expected_symbol = normalize_symbol(identity.get("symbol"))
    expected_company = str(identity.get("company_name") or expected_symbol or "").strip()
    article_symbol = normalize_symbol(article.get("primary_ticker"))
    comparison_symbols = {
        normalize_symbol((((item or {}).get("identity") or {}).get("symbol"))) for item in (context.get("comparisons") or []) if isinstance(item, dict)
    }
    comparison_symbols.update(normalize_symbol(item) for item in article.get("comparison_tickers") or [])
    comparison_symbols.discard(None)
    allowed_symbols = {expected_symbol, *comparison_symbols}
    allowed_symbols.discard(None)
    text = _article_full_text(article)
    warnings: list[dict[str, Any]] = []
    if expected_symbol and article_symbol and article_symbol != expected_symbol:
        warnings.append(_warning("article_primary_ticker_mismatch", f"Article primary ticker mismatch: expected {expected_symbol}, received {article_symbol}.", blocking=True))
    if expected_symbol and expected_company and expected_symbol not in text.upper() and expected_company.lower() not in text.lower():
        warnings.append(_warning("article_subject_missing", f"Article does not clearly anchor on {expected_company} ({expected_symbol}).", blocking=True))
    for symbol, aliases in COMPANY_IDENTITY_GUARDS.items():
        if symbol in allowed_symbols:
            continue
        matched_alias = next((alias for alias in aliases if re.search(rf"\b{re.escape(alias)}\b", text, flags=re.IGNORECASE)), None)
        if matched_alias:
            warnings.append(
                _warning(
                    "company_identity_contamination",
                    f"Draft mentions {matched_alias} as company-specific context, but {symbol} is not the primary ticker or an allowed comparison.",
                    blocking=True,
                )
            )
    return warnings


def _style_validation_warnings(article: dict[str, Any], context: dict[str, Any]) -> list[dict[str, Any]]:
    text = _article_full_text(article)
    lowered = text.lower()
    warnings: list[dict[str, Any]] = []
    hits = [label for label, pattern in STYLE_TIC_PATTERNS if re.search(pattern, lowered, flags=re.IGNORECASE | re.DOTALL)]
    if hits:
        warnings.append(_warning("ai_style_tics", f"Generic AI-writing patterns detected: {', '.join(hits[:6])}.", blocking=True))
    title = str(article.get("title") or "")
    if re.search(r"\b(primary .*data needed|data needed before|insufficient data|missing data|reserve judgment)\b", title, flags=re.IGNORECASE):
        warnings.append(_warning("weak_missing_data_title", "Title describes missing research instead of an investment question.", blocking=True))
    if text.count(";") > 8:
        warnings.append(_warning("excessive_semicolons", "Draft overuses semicolons; rewrite in a more natural Walnut voice.", blocking=True))
    if text.count("--") + text.count("—") > 6:
        warnings.append(_warning("excessive_dashes", "Draft overuses dashes; rewrite with cleaner sentence variation.", blocking=True))
    primary = context.get("primary") if isinstance(context.get("primary"), dict) else {}
    identity = primary.get("identity") if isinstance(primary.get("identity"), dict) else {}
    company_terms = [str(identity.get("symbol") or ""), str(identity.get("company_name") or "")]
    company_terms.extend(alias for symbol, aliases in COMPANY_IDENTITY_GUARDS.items() for alias in aliases if symbol == normalize_symbol(identity.get("symbol")))
    generic_paragraphs = 0
    for paragraph in re.split(r"\n{2,}", text):
        cleaned = paragraph.strip()
        if len(cleaned.split()) < 18:
            continue
        has_number = bool(re.search(r"(?<![A-Za-z])\$?\d", cleaned))
        has_company = any(term and re.search(rf"\b{re.escape(term)}\b", cleaned, flags=re.IGNORECASE) for term in company_terms)
        has_date_or_source = bool(re.search(r"\b(?:20\d{2}|q[1-4]|fy\s?20\d{2}|http|sec|guidance|consensus|revenue|eps|arr|capex|margin)\b", cleaned, flags=re.IGNORECASE))
        if not (has_number or has_company or has_date_or_source):
            generic_paragraphs += 1
    if generic_paragraphs >= 3:
        warnings.append(_warning("low_information_density", "Multiple paragraphs lack company-specific facts, metrics, dated events, sources, or investor implications.", blocking=True))
    return warnings


def _internal_language_hits(lowered_text: str) -> list[str]:
    hits: list[str] = []
    labels = [
        ("research request", r"\bresearch request\b"),
        ("supplied research context", r"\bsupplied research context\b"),
        ("supplied materials", r"\bsupplied materials?\b"),
        ("supplied context", r"\bsupplied context\b"),
        ("research configuration", r"\bresearch configuration\b|\bthis configuration\b|\bin this research configuration\b"),
        ("provided comparison confirmation", r"\bprovided comparison confirmation\b"),
        ("user request", r"\buser request\b"),
        ("prompt", r"\bprompt\b"),
        ("generated from", r"\bgenerated from\b"),
        ("model was asked", r"\bmodel was asked\b"),
        ("confirmation score publish filler", r"\bwe do not publish our proprietary confirmation score\b"),
    ]
    for label, pattern in labels:
        if re.search(pattern, lowered_text):
            hits.append(label)
    return hits


def _missing_data_language_hits(lowered_text: str) -> list[str]:
    hits: list[str] = []
    labels = [
        ("not supplied", r"\bnot supplied\b"),
        ("was supplied", r"\b(?:was|were)\s+supplied\b"),
        ("no reviewed consensus source was supplied", r"\bno reviewed consensus source was supplied\b"),
        ("reviewed materials do not provide", r"\breviewed materials do not provide\b"),
    ]
    for label, pattern in labels:
        if re.search(pattern, lowered_text):
            hits.append(label)
    return hits


def _markdown_structure_issues(markdown: str) -> list[str]:
    issues: list[str] = []
    sections = _markdown_h2_sections(markdown)
    previous_had_body = True
    counts: dict[str, int] = {}
    for section in sections:
        key = _heading_key(section["heading"])
        counts[key] = counts.get(key, 0) + 1
        if _is_placeholder_heading(section["heading"]):
            issues.append("placeholder heading")
        if not section["body"].strip():
            issues.append(f"empty heading: {section['heading']}")
        if not previous_had_body:
            issues.append("consecutive H2 headings without body text")
        previous_had_body = bool(section["body"].strip())
    if any(count > 1 for count in counts.values()):
        issues.append("duplicate headings")
    return _dedupe_strings(issues)


def _markdown_h2_sections(markdown: str) -> list[dict[str, str]]:
    lines = str(markdown or "").replace("\r\n", "\n").replace("\r", "\n").splitlines()
    sections: list[dict[str, str]] = []
    current: dict[str, Any] | None = None
    for line in lines:
        match = re.match(r"^##\s+(.+?)\s*$", line)
        if match:
            if current is not None:
                sections.append({"heading": current["heading"], "body": "\n".join(current["body_lines"]).strip()})
            current = {"heading": match.group(1).strip(), "body_lines": []}
            continue
        if current is not None:
            current["body_lines"].append(line)
    if current is not None:
        sections.append({"heading": current["heading"], "body": "\n".join(current["body_lines"]).strip()})
    return sections


def _available_data_missing_claims(lowered_text: str, context: dict[str, Any]) -> list[str]:
    availability = context.get("data_availability") if isinstance(context.get("data_availability"), dict) else {}
    missing_terms = r"(?:not found|not available|unavailable|missing|could not find|couldn't find|not directly reviewed|not independently verified(?: in reviewed primary sources)?)"
    checks = {
        "current price": [r"current\s+\w*\s*price", r"share\s+price", r"stock\s+price"],
        "volume": [r"\bvolume\b"],
        "price/volume and technicals": [r"price\s*/\s*volume", r"\btechnicals?\b", r"technical\s+levels?"],
        "revenue consensus": [r"revenue\s+consensus", r"q[1-4]\s+revenue"],
        "eps consensus": [r"eps\s+consensus", r"q[1-4]\s+eps"],
        "gross margin": [r"gross\s+margin"],
        "free cash flow": [r"free\s+cash\s+flow", r"\bfcf\b"],
        "reported institutional activity": [r"reported\s+institutional\s+activity", r"institutional\s+activity"],
        "insider activity": [r"insider\s+activity"],
        "congress activity": [r"congress\s+activity"],
        "government contracts": [r"government\s+contracts?"],
        "valuation data": [r"valuation\s+data", r"\bvaluation\b"],
    }
    contradicted: list[str] = []
    for field, synonyms in checks.items():
        if not availability.get(field):
            continue
        for synonym in synonyms:
            if re.search(rf"{synonym}.{{0,90}}{missing_terms}|{missing_terms}.{{0,90}}{synonym}", lowered_text):
                contradicted.append(field)
                break
    return contradicted


def _numeric_format_issues(body: str) -> list[str]:
    text = re.sub(r"https?://\S+", "", str(body or ""))
    issues: list[str] = []
    if re.search(r"(?<![\w/])-?\d[\d,]*\.\d{3,}\s*%", text):
        issues.append("percentage has more than 2 decimal places")
    if re.search(r"(?<![\w/])-?\d[\d,]*\.\d{3,}\s*x\b", text, flags=re.IGNORECASE):
        issues.append("ratio has raw decimal precision")
    if re.search(r"\$-?\d{7,}(?:\.\d+)?\b", text):
        issues.append("large currency value should be compacted")
    if re.search(r"(?<![\w/])-?\d+\.\d{6,}(?![\w/])", text):
        issues.append("raw floating point artifact")
    return _dedupe_strings(issues)


def _conflates_confirmation_score_with_data(lowered_text: str) -> bool:
    data_terms = r"(?:price/?volume|price and volume|fundamentals|reported institutional activity|congress activity|insider activity|government contracts|options flow|macro positioning|underlying data|(?:cross[ -]source )?data categories)"
    score_inputs = r"(?:input|inputs|component|components|driver|drivers|factor|factors|basis|bases|source|sources)"
    patterns = [
        rf"confirmation score\s+(?:is|equals|represents|is derived from|is based on|comes from)\s+.{{0,160}}{data_terms}",
        rf"{data_terms}.{{0,160}}\s+(?:are|is)\s+the\s+confirmation score",
        rf"confirmation score.{{0,160}}{score_inputs}.{{0,120}}{data_terms}",
        rf"{data_terms}.{{0,160}}{score_inputs}.{{0,120}}confirmation score",
        r"confirmation score\s+and\s+underlying data\s+are\s+the\s+same",
    ]
    return any(re.search(pattern, lowered_text) for pattern in patterns)


def _source_link_count(article: dict[str, Any], body: str) -> int:
    urls = set(re.findall(r"https?://[^\s)\]>\"']+", body))
    source_links = article.get("source_links") if isinstance(article.get("source_links"), list) else []
    for item in source_links:
        if isinstance(item, dict) and str(item.get("url") or "").startswith(("http://", "https://")):
            urls.add(str(item["url"]))
    return len(urls)


def _warning(code: str, message: str, *, blocking: bool) -> dict[str, Any]:
    return {"code": code, "message": message, "blocking": blocking}


def _context_has_numbers(context: dict[str, Any]) -> bool:
    return bool(re.search(r"\d", json.dumps(context, default=str)))


def _duplicate_slug(slug: str, draft_id: str | None = None) -> bool:
    if slug in PUBLISHED_STATIC_SLUGS:
        return True
    store = _read_store()
    for draft in store.get("drafts", []):
        if draft.get("id") == draft_id:
            continue
        if draft.get("status") == "published" and draft.get("article", {}).get("slug") == slug:
            return True
    return False


def _new_draft(admin: UserAccount, config: dict[str, Any], context: dict[str, Any], article: dict[str, Any], validation: dict[str, Any], *, elapsed_ms: int) -> dict[str, Any]:
    created = _now()
    slug = _slugify(str(article.get("slug") or article.get("title") or config["ticker"]), fallback=f"{config['ticker'].lower()}-research-brief")
    article = deepcopy(article)
    article["slug"] = slug
    return {
        "id": f"rb_{int(time.time() * 1000)}_{uuid.uuid4().hex[:6]}",
        "status": "draft",
        "created_by": admin.id,
        "created_by_email": getattr(admin, "email", None),
        "created_at": created,
        "updated_at": created,
        "published_at": None,
        "target_keyword": config.get("target_keyword"),
        "secondary_keywords": config.get("secondary_keywords") or [],
        "search_intent": config.get("search_intent"),
        "content_type": config.get("content_type") or "ticker",
        "index_status": "unknown",
        "first_seen_indexed_at": None,
        "last_checked_at": None,
        "model": article.get("_model") or config.get("selected_model") or research_brief_model(None),
        "prompt_version": RESEARCH_BRIEF_PROMPT_VERSION,
        "research_context_timestamp": context.get("generated_at"),
        "primary_ticker": context["primary"]["identity"]["symbol"],
        "comparison_ticker": (config.get("comparison_tickers") or [None])[0],
        "comparison_tickers": list(config.get("comparison_tickers") or []),
        "config": config,
        "article": {k: v for k, v in article.items() if not str(k).startswith("_")},
        "validation": validation,
        "diagnostics": {
            "elapsed_ms": elapsed_ms,
            "storage": "database",
            "usage": article.get("_generation_usage") or {},
        },
        "research_context": context,
    }


def enrich_internal_links(article: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    """Attach only contextually relevant first-party links and a compact related module."""
    enriched = deepcopy(article)
    site_context = context.get("walnut_site_context") if isinstance(context.get("walnut_site_context"), dict) else {}
    candidates = site_context.get("links") if isinstance(site_context.get("links"), list) else []
    internal = [item for item in candidates if isinstance(item, dict) and str(item.get("url") or "").startswith("/")]
    enriched["source_links"] = _dedupe_source_links([*(enriched.get("source_links") or []), *internal])
    body = "\n".join(str(section.get("body_markdown") or "") for section in (enriched.get("sections") or []) if isinstance(section, dict))
    useful = [item for item in internal if item.get("source_type") in {"ticker_page", "research_hub", "related_research"}][:3]
    if useful and not _internal_link_count(enriched, body):
        links = " · ".join(f"[{item.get('title')}]({item.get('url')})" for item in useful)
        enriched.setdefault("sections", []).append({"key": "related-walnut-research", "heading": "Related Walnut research", "body_markdown": f"{links}"})
    enriched["internal_linking"] = {
        "outbound_links": useful,
        "inbound_opportunities": [
            {"page": item.get("url"), "module": "Related Research" if item.get("source_type") in {"ticker_page", "related_research"} else "Research hub"}
            for item in useful
        ],
    }
    return enriched


def generate_thumbnail_asset(db: Session, config: dict[str, Any], article: dict[str, Any]) -> dict[str, Any]:
    title = str(article.get("title") or config.get("ticker") or "Walnut research").strip()
    conclusion = _article_conclusion(article)
    asset_type = _thumbnail_asset_type(config)
    prompt = _research_thumbnail_prompt(title=title, ticker=str(config.get("ticker") or ""), conclusion=conclusion, asset_type=asset_type)
    model = os.getenv(AI_MARKETING_IMAGE_MODEL, "").strip() or DEFAULT_AI_MARKETING_IMAGE_MODEL
    asset = {
        "image_title": title[:120],
        "image_prompt": prompt,
        "asset_type": asset_type,
        "url": "",
        "thumbnail_url": "",
        "source_notes": f"Prompt generated from final post conclusion. Image model: {model}. Official Walnut logo should be overlaid or preserved.",
        "created_at": _now(),
    }
    if not _env_flag_enabled(AI_MARKETING_IMAGE_GENERATION_ENABLED):
        return asset
    api_key = resolved_setting_value(db, OPENAI_API_KEY)
    if not api_key:
        asset["source_notes"] += " Image generation skipped because OPENAI_API_KEY is not configured."
        return asset
    try:
        response = requests.post(
            IMAGES_ENDPOINT,
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={
                "model": model,
                "prompt": prompt,
                "size": os.getenv(AI_MARKETING_IMAGE_SIZE, "").strip() or DEFAULT_AI_MARKETING_IMAGE_SIZE,
                "quality": os.getenv(AI_MARKETING_IMAGE_QUALITY, "").strip() or DEFAULT_AI_MARKETING_IMAGE_QUALITY,
                "output_format": "jpeg",
            },
            timeout=_env_float(RESEARCH_BRIEF_THUMBNAIL_TIMEOUT_SECONDS, 45.0),
        )
    except requests.RequestException:
        asset["source_notes"] += " Image generation request failed; prompt is saved for retry."
        return asset
    if response.status_code >= 400:
        asset["source_notes"] += " Image generation failed; prompt is saved for retry."
        return asset
    data = response.json()
    image_data = (data.get("data") or [{}])[0] if isinstance(data, dict) else {}
    b64_image = str(image_data.get("b64_json") or "").strip()
    if b64_image:
        data_uri = f"data:image/jpeg;base64,{b64_image}"
        asset["url"] = data_uri
        asset["thumbnail_url"] = data_uri
    return asset


def _env_flag_enabled(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in {"1", "true", "yes", "on"}


def _article_conclusion(article: dict[str, Any]) -> str:
    sections = article.get("sections") if isinstance(article.get("sections"), list) else []
    for section in sections:
        heading = str((section or {}).get("heading") or "").lower()
        if "conclusion" in heading or "judgment" in heading or "call" in heading:
            return str((section or {}).get("body_markdown") or "")[:700]
    if sections:
        return str((sections[-1] or {}).get("body_markdown") or "")[:700]
    return str(article.get("summary") or article.get("preview_body") or "")[:700]


def _thumbnail_asset_type(config: dict[str, Any]) -> str:
    section_format = str(config.get("section_format") or "")
    audience = str(config.get("intended_audience") or "")
    if "X Thread" in section_format:
        return "X thumbnail"
    if "Reddit" in section_format or "Reddit" in audience:
        return "Reddit DD cover image"
    if "Internal Analyst Note" in section_format:
        return "research hero image"
    return "Insights card image"


def _research_thumbnail_prompt(*, title: str, ticker: str, conclusion: str, asset_type: str) -> str:
    ticker_text = f"${ticker.upper()}" if ticker else "the ticker"
    return (
        f"Create a clean, readable dark Walnut Markets {asset_type} for {ticker_text}. "
        "Use the real Walnut logo area only; do not invent a Walnut logo, company logo, fake data, fake chart, source footer, or tiny unreadable text. "
        "Branding: dark finance editorial background, emerald/teal accents, generous negative space, mobile-readable title. "
        f"Readable title: {title}. "
        f"Generate the image from this final post conclusion, not the raw prompt: {conclusion[:600]}. "
        "The image should look premium and specific to the market story without generic AI finance art or clutter."
    )


def _mock_article(config: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    symbol = context["primary"]["identity"]["symbol"]
    company = context["primary"]["identity"].get("company_name") or symbol
    question = config["research_question"]
    source_links = (context.get("external_research") or {}).get("reviewed_sources") or [
        {"label": "SEC EDGAR company search", "url": f"https://www.sec.gov/edgar/search/#/q={symbol}&dateRange=all", "source_type": "filing_search"},
        {"label": f"{symbol} Nasdaq market activity", "url": f"https://www.nasdaq.com/market-activity/stocks/{symbol.lower()}", "source_type": "reputable_market_source"},
    ]
    body = (
        f"{company} ({symbol}) deserves a focused research review because the current question is specific: {question}\n\n"
        "The available Walnut data should be read as evidence, not as a recommendation. The confirmation score is a separate Walnut signal, while fundamentals, price action, public filings, reported institutional activity, government contracts, and event history are the underlying data.\n\n"
        "The strongest constructive case is that available company and market data still support a credible thesis. The strongest risk case is that missing or stale data can hide a change in the cycle, and unavailable data should not be treated as bearish or bullish by itself.\n\n"
        "What matters next is whether the observable data improves or deteriorates: fundamentals, tape confirmation, public filings, reported activity, catalysts, and risk signals. Research only. Not investment advice.\n\n"
        "Sources:\n"
        + "\n".join(f"- [{item.get('label')}]({item.get('url')})" for item in source_links[:2])
    )
    return {
        "title": f"{symbol} DD: {question.rstrip('?')}",
        "slug": f"{symbol.lower()}-dd-draft",
        "subtitle": f"A Walnut research brief on {company}.",
        "summary": f"Draft research brief for {symbol}. Research only. Not investment advice.",
        "preview_body": f"{symbol} has a mixed setup: available data supports review, but missing fields keep the conclusion cautious. Research only. Not investment advice.",
        "judgment": "mixed",
        "walnut_call": "Mixed",
        "confidence": "medium",
        "confirmation_score_included": False,
        "primary_ticker": symbol,
        "comparison_tickers": list(config.get("comparison_tickers") or []),
        "category": context["primary"]["identity"].get("sector") or "Research",
        "reading_minutes": 4,
        "sections": [
            {"key": "thesis", "heading": "Executive thesis", "body_markdown": body},
            {"key": "watch", "heading": "What to watch next", "body_markdown": "Watch the next fundamentals refresh, price/volume confirmation, and new public filings before changing the thesis."},
        ],
        "key_points": ["Separate confirmation score from underlying data.", "Treat missing data as unavailable, not directional."],
        "catalysts": ["Next earnings update", "Material public filing or contract update"],
        "risks": ["Cycle deterioration", "Stale or incomplete data"],
        "watch_items": ["Fundamentals refresh", "Price/volume confirmation", "Public filings"],
        "data_freshness": [context.get("generated_at") or ""],
        "missing_data_notes": context.get("missing_data_notes") or [],
        "source_links": source_links[:4],
        "suggested_card": {
            "title": f"{symbol} DD research brief",
            "description": f"A research-only Walnut DD brief for {symbol}.",
            "judgment": "mixed",
            "tickers": [symbol],
        },
        "seo": {"title": f"{symbol} DD | Walnut Research", "description": f"Walnut research brief for {symbol}. Not investment advice."},
    }


def list_drafts(status: str | None = None, db: Session | None = None) -> dict[str, Any]:
    if db is not None:
        try:
            drafts = [_draft_with_comparison_tickers(draft) for draft in _db_drafts(db, status=status)]
            return {"items": sorted(drafts, key=lambda item: item.get("updated_at") or "", reverse=True)}
        except Exception as exc:
            logger.warning("research_brief_db_list_drafts_failed error=%s", exc.__class__.__name__)
    with _STORE_LOCK:
        drafts = deepcopy(_read_store().get("drafts", []))
    drafts = [_draft_with_comparison_tickers(draft) for draft in drafts]
    if status and status != "all":
        drafts = [draft for draft in drafts if draft.get("status") == status]
    return {"items": sorted(drafts, key=lambda item: item.get("updated_at") or "", reverse=True)}


def get_draft(draft_id: str, db: Session | None = None) -> dict[str, Any]:
    if db is not None:
        try:
            draft = _db_draft(db, draft_id)
            if draft:
                return _draft_with_comparison_tickers(deepcopy(draft))
        except Exception as exc:
            logger.warning("research_brief_db_get_draft_failed draft_id=%s error=%s", draft_id, exc.__class__.__name__)
    for draft in _read_store().get("drafts", []):
        if draft.get("id") == draft_id:
            return _draft_with_comparison_tickers(deepcopy(draft))
    raise HTTPException(status_code=404, detail="Research brief draft not found.")


def _draft_with_comparison_tickers(draft: dict[str, Any]) -> dict[str, Any]:
    config = draft.setdefault("config", {})
    source_config = dict(config)
    if draft.get("comparison_tickers") and not source_config.get("comparison_tickers"):
        source_config["comparison_tickers"] = draft.get("comparison_tickers")
    if draft.get("comparison_ticker") and not source_config.get("comparison_ticker"):
        source_config["comparison_ticker"] = draft.get("comparison_ticker")
    comparison_tickers = normalize_comparison_tickers(source_config)
    config["comparison_tickers"] = comparison_tickers
    config["comparison_ticker"] = comparison_tickers[0] if comparison_tickers else None
    draft["comparison_tickers"] = comparison_tickers
    draft["comparison_ticker"] = comparison_tickers[0] if comparison_tickers else None
    article = draft.get("article")
    if isinstance(article, dict) and not isinstance(article.get("comparison_tickers"), list):
        article["comparison_tickers"] = comparison_tickers
    return draft


def _apply_draft_config_patch(draft: dict[str, Any], config_patch: dict[str, Any] | None) -> dict[str, Any]:
    if not config_patch:
        return validate_config(draft.get("config") or {}, strict_selected_model=False)
    merged_config = {**(draft.get("config") or {}), **config_patch}
    config = validate_config(merged_config, strict_selected_model=False)
    comparison_tickers = list(config.get("comparison_tickers") or [])
    draft["config"] = config
    draft["comparison_tickers"] = comparison_tickers
    draft["comparison_ticker"] = comparison_tickers[0] if comparison_tickers else None
    return config


def _sync_article_comparison_metadata(article: dict[str, Any], draft: dict[str, Any], config: dict[str, Any]) -> None:
    comparison_tickers = list(config.get("comparison_tickers") or [])
    primary_ticker = normalize_symbol(article.get("primary_ticker") or draft.get("primary_ticker") or config.get("ticker")) or str(
        article.get("primary_ticker") or draft.get("primary_ticker") or config.get("ticker") or ""
    ).upper()
    article["comparison_tickers"] = comparison_tickers
    if primary_ticker:
        article["primary_ticker"] = primary_ticker
    suggested_card = article.get("suggested_card") if isinstance(article.get("suggested_card"), dict) else {}
    suggested_card["tickers"] = _dedupe_strings([primary_ticker, *comparison_tickers])
    article["suggested_card"] = suggested_card


def update_draft(
    admin: UserAccount,
    draft_id: str,
    article_patch: dict[str, Any],
    status: str | None = None,
    db: Session | None = None,
    config_patch: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if db is not None:
        draft = _db_draft(db, draft_id)
        if draft:
            config = _apply_draft_config_patch(draft, config_patch)
            article = draft.setdefault("article", {})
            article.update({k: v for k, v in article_patch.items() if k in article_schema()["properties"] or k in {"hero_image", "thumbnail_asset", "premium_required", "required_plan"}})
            if config_patch:
                _sync_article_comparison_metadata(article, draft, config)
            article["slug"] = _slugify(str(article.get("slug") or article.get("title") or draft.get("primary_ticker")), fallback=f"{draft.get('primary_ticker', 'brief').lower()}-research-brief")
            draft["article"] = sanitize_research_brief_article(
                article,
                config,
                draft.get("research_context") or {},
                repair_generated_sections=False,
            )
            if status:
                draft["status"] = _normalize_update_status(draft.get("status"), status)
            draft["validation"] = validate_article(draft["article"], draft.get("research_context") or {}, draft_id=draft_id)
            draft["updated_at"] = _now()
            _upsert_db_draft(db, draft)
            return deepcopy(draft)
    with _STORE_LOCK:
        store = _read_store()
        for draft in store.get("drafts", []):
            if draft.get("id") == draft_id:
                config = _apply_draft_config_patch(draft, config_patch)
                article = draft.setdefault("article", {})
                article.update({k: v for k, v in article_patch.items() if k in article_schema()["properties"] or k in {"hero_image", "thumbnail_asset", "premium_required", "required_plan"}})
                if config_patch:
                    _sync_article_comparison_metadata(article, draft, config)
                article["slug"] = _slugify(str(article.get("slug") or article.get("title") or draft.get("primary_ticker")), fallback=f"{draft.get('primary_ticker', 'brief').lower()}-research-brief")
                draft["article"] = sanitize_research_brief_article(
                    article,
                    config,
                    draft.get("research_context") or {},
                    repair_generated_sections=False,
                )
                if status:
                    draft["status"] = _normalize_update_status(draft.get("status"), status)
                draft["validation"] = validate_article(draft["article"], draft.get("research_context") or {}, draft_id=draft_id)
                draft["updated_at"] = _now()
                _append_audit(store, action="save", admin=admin, draft_id=draft_id)
                _write_store(store)
                return deepcopy(draft)
    raise HTTPException(status_code=404, detail="Research brief draft not found.")


def refresh_research_sources(db: Session, admin: UserAccount, draft_id: str) -> dict[str, Any]:
    draft = _db_draft(db, draft_id)
    if draft:
        config = validate_config(draft.get("config") or {})
        symbol = str(draft.get("primary_ticker") or config.get("ticker") or "").upper()
        identity = ((draft.get("research_context") or {}).get("primary") or {}).get("identity") or {"symbol": symbol}
        external = discover_external_research(
            symbol,
            identity,
            mode=config.get("external_research_mode") or "Standard",
            manual_source_url=config.get("manual_source_url"),
            desired_angle=config.get("desired_angle"),
            research_question=config.get("research_question"),
        )
        context = draft.setdefault("research_context", {})
        context["external_research"] = external
        context["external_research_mode"] = external.get("mode")
        context["generated_at"] = _now()
        context["data_availability"] = _research_data_availability(context.get("primary") or {}, external)
        context["source_discovery"] = external.get("source_discovery") or {}
        filtered_missing = _filter_missing_data_notes(external.get("missing_data_notes") or [], context["data_availability"])
        existing_missing = context.get("missing_data_notes") if isinstance(context.get("missing_data_notes"), list) else []
        context["missing_data_notes"] = _filter_missing_data_notes([*existing_missing, *filtered_missing], context["data_availability"])
        article = draft.setdefault("article", {})
        article["missing_data_notes"] = _filter_missing_data_notes([*(article.get("missing_data_notes") or []), *filtered_missing], context["data_availability"])
        article["source_links"] = _dedupe_source_links([*(article.get("source_links") or []), *(external.get("reviewed_sources") or [])])
        draft["article"] = sanitize_research_brief_article(article, config, context, repair_generated_sections=False)
        draft["validation"] = validate_article(draft["article"], context, draft_id=draft_id)
        draft["updated_at"] = _now()
        _upsert_db_draft(db, draft)
        return deepcopy(draft)

    with _STORE_LOCK:
        store = _read_store()
        for draft in store.get("drafts", []):
            if draft.get("id") != draft_id:
                continue
            config = validate_config(draft.get("config") or {})
            symbol = str(draft.get("primary_ticker") or config.get("ticker") or "").upper()
            identity = ((draft.get("research_context") or {}).get("primary") or {}).get("identity") or {"symbol": symbol}
            external = discover_external_research(
                symbol,
                identity,
                mode=config.get("external_research_mode") or "Standard",
                manual_source_url=config.get("manual_source_url"),
                desired_angle=config.get("desired_angle"),
                research_question=config.get("research_question"),
            )
            context = draft.setdefault("research_context", {})
            context["external_research"] = external
            context["external_research_mode"] = external.get("mode")
            context["generated_at"] = _now()
            context["data_availability"] = _research_data_availability(context.get("primary") or {}, external)
            context["source_discovery"] = external.get("source_discovery") or {}
            filtered_missing = _filter_missing_data_notes(external.get("missing_data_notes") or [], context["data_availability"])
            existing_missing = context.get("missing_data_notes") if isinstance(context.get("missing_data_notes"), list) else []
            context["missing_data_notes"] = _filter_missing_data_notes([*existing_missing, *filtered_missing], context["data_availability"])
            article = draft.setdefault("article", {})
            article["missing_data_notes"] = _filter_missing_data_notes([*(article.get("missing_data_notes") or []), *filtered_missing], context["data_availability"])
            article["source_links"] = _dedupe_source_links([*(article.get("source_links") or []), *(external.get("reviewed_sources") or [])])
            draft["article"] = sanitize_research_brief_article(article, config, context, repair_generated_sections=False)
            draft["validation"] = validate_article(draft["article"], context, draft_id=draft_id)
            draft["updated_at"] = _now()
            _append_audit(store, action="refresh_sources", admin=admin, draft_id=draft_id, metadata={"mode": external.get("mode")})
            _write_store(store)
            return deepcopy(draft)
    raise HTTPException(status_code=404, detail="Research brief draft not found.")


def _dedupe_source_links(values: list[Any]) -> list[dict[str, str]]:
    links: list[dict[str, str]] = []
    seen: set[str] = set()
    for item in values:
        if not isinstance(item, dict):
            continue
        url = str(item.get("url") or "").strip()
        if not url or url in seen:
            continue
        seen.add(url)
        links.append(
            {
                "label": str(item.get("label") or url).strip()[:180],
                "url": url,
                "source_type": str(item.get("source_type") or "source").strip()[:80],
            }
        )
    return links[:12]


PUBLISH_HARD_STOP_WARNING_CODES = {
    "missing_title",
    "thin_body",
    "missing_disclaimer",
    "missing_source_links",
    "insufficient_source_links",
    "missing_official_earnings_source",
    "official_earnings_retrieval_failed",
    "missing_sec_or_ir_source",
    "official_source_link_omitted",
    "primary_ticker_context_mismatch",
    "duplicate_slug",
    "internal_wording",
    "internal_workflow_language",
    "unsupported_language",
    "markdown_structure",
}


def _publish_hard_stop_warnings(validation: dict[str, Any]) -> list[dict[str, Any]]:
    warnings = validation.get("warnings") if isinstance(validation.get("warnings"), list) else []
    return [
        warning
        for warning in warnings
        if isinstance(warning, dict)
        and warning.get("blocking")
        and str(warning.get("code") or "") in PUBLISH_HARD_STOP_WARNING_CODES
    ]


def publish_draft(admin: UserAccount, draft_id: str, *, confirm: bool, db: Session | None = None) -> dict[str, Any]:
    if not confirm:
        raise HTTPException(status_code=422, detail="Publish requires explicit confirmation.")
    if db is not None:
        draft = _db_draft(db, draft_id)
        if draft:
            draft["article"] = sanitize_research_brief_article(
                draft.get("article") or {},
                draft.get("config") or {},
                draft.get("research_context") or {},
                repair_generated_sections=False,
            )
            validation = validate_article(draft.get("article") or {}, draft.get("research_context") or {}, draft_id=draft_id)
            hard_stop_warnings = _publish_hard_stop_warnings(validation)
            if hard_stop_warnings:
                draft["validation"] = validation
                _upsert_db_draft(db, draft)
                raise HTTPException(status_code=422, detail="Resolve validation failures before publishing.")
            draft["status"] = "published"
            draft["published_at"] = draft.get("published_at") or _now()
            draft["updated_at"] = _now()
            draft["validation"] = validation
            _unpublish_other_db_drafts_for_slug(db, draft_id, str((draft.get("article") or {}).get("slug") or ""))
            _upsert_db_draft(db, draft)
            return deepcopy(draft)
    with _STORE_LOCK:
        store = _read_store()
        for draft in store.get("drafts", []):
            if draft.get("id") == draft_id:
                draft["article"] = sanitize_research_brief_article(
                    draft.get("article") or {},
                    draft.get("config") or {},
                    draft.get("research_context") or {},
                    repair_generated_sections=False,
                )
                validation = validate_article(draft.get("article") or {}, draft.get("research_context") or {}, draft_id=draft_id)
                hard_stop_warnings = _publish_hard_stop_warnings(validation)
                if hard_stop_warnings:
                    draft["validation"] = validation
                    _write_store(store)
                    raise HTTPException(status_code=422, detail="Resolve validation failures before publishing.")
                draft["status"] = "published"
                draft["published_at"] = draft.get("published_at") or _now()
                draft["updated_at"] = _now()
                draft["validation"] = validation
                _unpublish_other_store_drafts_for_slug(store, draft_id, str((draft.get("article") or {}).get("slug") or ""))
                _append_audit(store, action="publish", admin=admin, draft_id=draft_id)
                _write_store(store)
                return deepcopy(draft)
    raise HTTPException(status_code=404, detail="Research brief draft not found.")


def unpublish_draft(admin: UserAccount, draft_id: str, *, confirm: bool, db: Session | None = None) -> dict[str, Any]:
    if not confirm:
        raise HTTPException(status_code=422, detail="Unpublish requires explicit confirmation.")
    if db is not None:
        draft = _db_draft(db, draft_id)
        if draft:
            draft["status"] = "unpublished"
            draft["updated_at"] = _now()
            _upsert_db_draft(db, draft)
            return deepcopy(draft)
    with _STORE_LOCK:
        store = _read_store()
        for draft in store.get("drafts", []):
            if draft.get("id") == draft_id:
                draft["status"] = "unpublished"
                draft["updated_at"] = _now()
                _append_audit(store, action="unpublish", admin=admin, draft_id=draft_id)
                _write_store(store)
                return deepcopy(draft)
    raise HTTPException(status_code=404, detail="Research brief draft not found.")


def delete_draft(admin: UserAccount, draft_id: str, *, confirm_text: str, db: Session | None = None) -> dict[str, Any]:
    if confirm_text != "DELETE":
        raise HTTPException(status_code=422, detail="Delete requires typing DELETE.")
    if db is not None:
        ensure_research_brief_store_schema(db)
        result = db.execute(text("DELETE FROM research_brief_drafts WHERE id = :id"), {"id": draft_id})
        db.commit()
        if getattr(result, "rowcount", 0):
            return {"ok": True, "deleted": draft_id}
    with _STORE_LOCK:
        store = _read_store()
        before = len(store.get("drafts", []))
        store["drafts"] = [draft for draft in store.get("drafts", []) if draft.get("id") != draft_id]
        if len(store["drafts"]) == before:
            raise HTTPException(status_code=404, detail="Research brief draft not found.")
        _append_audit(store, action="delete", admin=admin, draft_id=draft_id)
        _write_store(store)
    return {"ok": True, "deleted": draft_id}


def _normalize_status(status: str) -> str:
    normalized = str(status or "").strip().lower().replace("-", "_")
    if normalized not in STATUS_OPTIONS:
        raise HTTPException(status_code=422, detail="Unsupported draft status.")
    return normalized


def _normalize_update_status(current_status: Any, requested_status: str) -> str:
    requested = _normalize_status(requested_status)
    current = str(current_status or "").strip().lower()
    if current == "published" and requested in {"draft", "ready_for_review"}:
        return "published"
    return requested


def _unpublish_other_db_drafts_for_slug(db: Session, draft_id: str, slug: str) -> None:
    if not slug:
        return
    ensure_research_brief_store_schema(db)
    rows = db.execute(
        text("SELECT id, payload_json FROM research_brief_drafts WHERE status = 'published' AND slug = :slug AND id != :id"),
        {"slug": slug, "id": draft_id},
    ).mappings().all()
    now = _now()
    for row in rows:
        payload = _load_json(row["payload_json"])
        if not isinstance(payload, dict):
            continue
        payload["status"] = "unpublished"
        payload["updated_at"] = now
        db.execute(
            text("UPDATE research_brief_drafts SET status = 'unpublished', updated_at = :updated_at, payload_json = :payload_json WHERE id = :id"),
            {"id": row["id"], "updated_at": now, "payload_json": _json_dump(payload)},
        )


def _unpublish_other_store_drafts_for_slug(store: dict[str, Any], draft_id: str, slug: str) -> None:
    if not slug:
        return
    now = _now()
    for draft in store.get("drafts", []):
        if draft.get("id") == draft_id or draft.get("status") != "published":
            continue
        if (draft.get("article") or {}).get("slug") != slug:
            continue
        draft["status"] = "unpublished"
        draft["updated_at"] = now


def published_cards(db: Session | None = None) -> dict[str, Any]:
    if db is not None:
        try:
            drafts = [draft for draft in _db_drafts(db, status="published") if draft.get("status") == "published"]
        except Exception as exc:
            logger.warning("research_brief_db_published_cards_failed error=%s", exc.__class__.__name__)
            drafts = [draft for draft in _read_store().get("drafts", []) if draft.get("status") == "published"]
    else:
        drafts = [draft for draft in _read_store().get("drafts", []) if draft.get("status") == "published"]
    drafts = sorted(drafts, key=lambda item: item.get("updated_at") or item.get("published_at") or "", reverse=True)
    cards = []
    seen_slugs: set[str] = set()
    for draft in drafts:
        article = draft.get("article") or {}
        suggested = article.get("suggested_card") if isinstance(article.get("suggested_card"), dict) else {}
        slug = article.get("slug")
        if not slug or slug in seen_slugs:
            continue
        seen_slugs.add(slug)
        required_plan = _research_article_required_plan(article)
        cards.append(
            {
                "slug": slug,
                "route": f"/research/{slug}",
                "title": suggested.get("title") or article.get("title") or slug,
                "description": suggested.get("description") or article.get("summary") or "",
                "tickers": suggested.get("tickers") or [draft.get("primary_ticker")],
                "category": article.get("category") or "Research",
                "judgment": None if required_plan else suggested.get("judgment") or article.get("judgment") or "mixed",
                "publishedAt": (draft.get("published_at") or draft.get("updated_at") or "")[:10],
                "readingMinutes": article.get("reading_minutes") or draft.get("validation", {}).get("estimated_reading_minutes") or 8,
                "generated": True,
                "premium": required_plan is not None,
                "requiredPlan": required_plan,
            }
        )
    return {"items": cards}


def _preview_research_article(article: dict[str, Any]) -> dict[str, Any]:
    preview = deepcopy(article)
    preview["sections"] = _preview_sections_for_article(article)
    preview["key_points"] = (preview.get("key_points") or [])[:2] if isinstance(preview.get("key_points"), list) else []
    preview["catalysts"] = []
    preview["risks"] = []
    preview["watch_items"] = []
    preview["source_links"] = []
    preview["judgment"] = ""
    preview["walnut_call"] = ""
    preview["confidence"] = ""
    if isinstance(preview.get("suggested_card"), dict):
        preview["suggested_card"] = {**preview["suggested_card"], "judgment": ""}
    if isinstance(preview.get("seo"), dict):
        preview["seo"] = {
            **preview["seo"],
            "description": str(preview.get("preview_body") or preview.get("summary") or preview["seo"].get("description") or ""),
        }
    preview["access"] = {"premium_required": True, "required_plan": _research_article_required_plan(article), "full_article_visible": False}
    return preview


def _research_payload_for_entitlements(draft: dict[str, Any], entitlements: Any | None) -> dict[str, Any]:
    payload = deepcopy(draft)
    article = payload.get("article") if isinstance(payload.get("article"), dict) else {}
    access = _research_access_payload(article, entitlements)
    if access["premium_required"] and not access["full_article_visible"]:
        payload["article"] = _preview_research_article(article)
        payload["research_context"] = None
        payload["diagnostics"] = {"storage": (draft.get("diagnostics") or {}).get("storage")}
        payload["validation"] = {
            "status": payload.get("validation", {}).get("status", "passed"),
            "warnings": [],
            "numeric_claims": [],
            "source_link_count": 0,
            "estimated_reading_minutes": payload.get("validation", {}).get("estimated_reading_minutes") or article.get("reading_minutes") or 1,
        }
    else:
        payload["article"] = _article_without_paywall_markers(article)
        payload["article"]["access"] = access
    return payload


def published_article(slug: str, db: Session | None = None, entitlements: Any | None = None) -> dict[str, Any]:
    normalized = _slugify(slug, fallback=slug)
    if db is not None:
        try:
            ensure_research_brief_store_schema(db)
            row = db.execute(
                text("SELECT payload_json FROM research_brief_drafts WHERE status = 'published' AND slug = :slug ORDER BY updated_at DESC, published_at DESC LIMIT 1"),
                {"slug": normalized},
            ).mappings().first()
            payload = _load_json(row["payload_json"]) if row else None
            if isinstance(payload, dict):
                return _research_payload_for_entitlements(payload, entitlements)
        except Exception as exc:
            logger.warning("research_brief_db_published_article_failed slug=%s error=%s", normalized, exc.__class__.__name__)
    store_drafts = sorted(_read_store().get("drafts", []), key=lambda item: item.get("updated_at") or item.get("published_at") or "", reverse=True)
    for draft in store_drafts:
        article = draft.get("article") or {}
        if draft.get("status") == "published" and article.get("slug") == normalized:
            return _research_payload_for_entitlements(draft, entitlements)
    raise HTTPException(status_code=404, detail="Research brief not found.")
