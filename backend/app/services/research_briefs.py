from __future__ import annotations

import json
import os
import re
import threading
import time
from copy import deepcopy
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import requests
from fastapi import HTTPException
from sqlalchemy import desc, func, select
from sqlalchemy.orm import Session

from app.models import Event, FundamentalsCache, GovernmentContract, QuoteCache, Security, TickerMeta, UserAccount
from app.services.ai_marketing import (
    AI_MARKETING_MODEL,
    DEFAULT_AI_MARKETING_MODEL,
    OPENAI_API_KEY,
    resolved_setting_value,
)
from app.services.confirmation_score import get_confirmation_score_bundles_for_tickers
from app.utils.symbols import normalize_symbol

RESEARCH_BRIEF_PROMPT_VERSION = "research_brief_v1"
RESEARCH_BRIEF_GENERATOR_MODEL = "RESEARCH_BRIEF_GENERATOR_MODEL"
RESPONSES_ENDPOINT = "https://api.openai.com/v1/responses"
STORE_ENV = "RESEARCH_BRIEF_DRAFT_STORE_PATH"
MOCK_ENV = "RESEARCH_BRIEF_GENERATOR_MOCK"

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
STATUS_OPTIONS = {"generating", "draft", "ready_for_review", "published", "unpublished", "failed"}
JUDGMENT_VALUES = {"bullish", "bearish", "mixed", "macro", "policy", "neutral"}
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

_STORE_LOCK = threading.Lock()
_ACTIVE_GENERATIONS: set[str] = set()


def research_brief_model(db: Session | None = None) -> str:
    configured = os.getenv(RESEARCH_BRIEF_GENERATOR_MODEL, "").strip()
    if configured:
        return configured
    return resolved_setting_value(db, AI_MARKETING_MODEL) or DEFAULT_AI_MARKETING_MODEL


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


def _read_store() -> dict[str, Any]:
    path = draft_store_path()
    if not path.exists():
        return {"drafts": [], "audit": []}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"drafts": [], "audit": []}
    if not isinstance(payload, dict):
        return {"drafts": [], "audit": []}
    drafts = payload.get("drafts") if isinstance(payload.get("drafts"), list) else []
    audit = payload.get("audit") if isinstance(payload.get("audit"), list) else []
    return {"drafts": drafts, "audit": audit}


def _write_store(payload: dict[str, Any]) -> None:
    path = draft_store_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


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


def assemble_research_context(db: Session, payload: dict[str, Any]) -> dict[str, Any]:
    symbol, identity = normalize_supported_symbol(db, payload.get("ticker"))
    comparison_raw = payload.get("comparison_ticker")
    comparison_symbol = None
    comparison_identity = None
    if comparison_raw:
        comparison_symbol, comparison_identity = normalize_supported_symbol(db, comparison_raw)
        if comparison_symbol == symbol:
            raise HTTPException(status_code=422, detail="Comparison ticker must be different from the primary ticker.")

    symbols = [symbol] + ([comparison_symbol] if comparison_symbol else [])
    fundamentals = {item: _latest_fundamentals(db, item) for item in symbols}
    quotes = {item: _quote(db, item) for item in symbols}
    try:
        confirmation = get_confirmation_score_bundles_for_tickers(db, symbols, lookback_days=30)
    except Exception:
        confirmation = {}

    missing: list[str] = []
    for item in symbols:
        if not fundamentals.get(item):
            missing.append(f"{item}: fundamentals unavailable")
        if not quotes.get(item):
            missing.append(f"{item}: quote unavailable")
        if not confirmation.get(item):
            missing.append(f"{item}: confirmation score unavailable")

    context = {
        "generated_at": _now(),
        "primary": {
            "identity": identity,
            "quote": quotes.get(symbol),
            "fundamentals": fundamentals.get(symbol),
            "confirmation": _compact(confirmation.get(symbol)),
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
        },
        "comparison": None,
        "missing_data_notes": missing,
        "limitations": [
            "13F activity is reported with filing lag and is not real-time.",
            "Congress and insider activity should not be interpreted as intent or wrongdoing.",
            "Missing Walnut data is unavailable, not zero and not automatically bearish.",
        ],
    }
    if comparison_symbol and comparison_identity:
        context["comparison"] = {
            "identity": comparison_identity,
            "quote": quotes.get(comparison_symbol),
            "fundamentals": fundamentals.get(comparison_symbol),
            "confirmation": _compact(confirmation.get(comparison_symbol)),
            "congress_activity": _recent_events(db, comparison_symbol, ["congress_trade", "congress_treasury_trade", "congress_crypto_trade"]),
            "insider_activity": _recent_events(db, comparison_symbol, ["insider_trade"]),
            "government_contracts": _government_contracts(db, comparison_symbol),
        }
    return context


def validate_config(config: dict[str, Any]) -> dict[str, Any]:
    ticker = config.get("ticker")
    prompt = str(config.get("research_question") or "").strip()
    if not ticker:
        raise HTTPException(status_code=422, detail="Ticker is required.")
    if len(prompt) < 12:
        raise HTTPException(status_code=422, detail="Research question must be more specific.")
    normalized = {
        "ticker": ticker,
        "research_question": prompt[:3000],
        "desired_angle": _choice(config.get("desired_angle"), ANGLE_OPTIONS, "Full company DD"),
        "comparison_ticker": config.get("comparison_ticker") or None,
        "time_horizon": _choice(config.get("time_horizon"), TIME_HORIZON_OPTIONS, "Near term"),
        "intended_audience": _choice(config.get("intended_audience"), AUDIENCE_OPTIONS, "Walnut Research Brief"),
        "judgment_preference": _choice(config.get("judgment_preference"), JUDGMENT_OPTIONS, "Let the data decide"),
        "additional_context": str(config.get("additional_context") or "")[:4000],
        "include_sections": _sections(config.get("include_sections")),
        "length": _choice(config.get("length"), LENGTH_OPTIONS, "Standard: 1,500-2,500 words"),
        "tone": _choice(config.get("tone"), TONE_OPTIONS, "Walnut market-native"),
        "include_charts": bool(config.get("include_charts")),
        "include_source_links": bool(config.get("include_source_links")),
        "hero_image": config.get("hero_image") or "",
    }
    if normalized["desired_angle"] == "Peer comparison" and not normalized["comparison_ticker"]:
        raise HTTPException(status_code=422, detail="Comparison ticker is required for peer comparison briefs.")
    return normalized


def _choice(value: Any, choices: set[str], fallback: str) -> str:
    text = str(value or "").strip()
    return text if text in choices else fallback


def _sections(value: Any) -> list[str]:
    if not isinstance(value, list):
        return list(DEFAULT_SECTIONS)
    cleaned = [str(item).strip() for item in value if str(item).strip() in DEFAULT_SECTIONS]
    return cleaned or list(DEFAULT_SECTIONS)


def generate_research_brief(db: Session, admin: UserAccount, config: dict[str, Any]) -> dict[str, Any]:
    normalized_config = validate_config(config)
    context = assemble_research_context(db, normalized_config)
    actor_key = f"admin:{admin.id}"
    if actor_key in _ACTIVE_GENERATIONS:
        raise HTTPException(status_code=429, detail="A research brief generation is already running for this Admin session.")
    _ACTIVE_GENERATIONS.add(actor_key)
    try:
        started = time.perf_counter()
        article = _mock_article(normalized_config, context) if os.getenv(MOCK_ENV) == "1" else _call_openai(db, normalized_config, context)
        validation = validate_article(article, context)
        draft = _new_draft(admin, normalized_config, context, article, validation, elapsed_ms=int((time.perf_counter() - started) * 1000))
        with _STORE_LOCK:
            store = _read_store()
            store["drafts"].append(draft)
            _append_audit(store, action="generate", admin=admin, draft_id=draft["id"], metadata={"ticker": normalized_config["ticker"]})
            _write_store(store)
        return draft
    finally:
        _ACTIVE_GENERATIONS.discard(actor_key)


def _call_openai(db: Session, config: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    api_key = resolved_setting_value(db, OPENAI_API_KEY)
    if not api_key:
        raise HTTPException(status_code=503, detail="OpenAI API key missing. Configure OPENAI_API_KEY before generating.")
    model = research_brief_model(db)
    response = requests.post(
        RESPONSES_ENDPOINT,
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json={
            "model": model,
            "input": _prompt(config, context),
            "store": False,
            "max_output_tokens": _max_output_tokens(config["length"]),
            "text": {"format": {"type": "json_schema", "name": "walnut_research_brief", "schema": article_schema(), "strict": True}},
        },
        timeout=90,
    )
    if response.status_code == 429:
        raise HTTPException(status_code=429, detail="OpenAI rate limit hit. Try again later.")
    if response.status_code >= 400:
        raise HTTPException(status_code=502, detail="OpenAI generation failed. Check model, quota, and prompt size.")
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


def _prompt(config: dict[str, Any], context: dict[str, Any]) -> str:
    return "\n".join(
        [
            "You are Walnut's senior market research editor writing a publishable due-diligence brief.",
            "Use only the supplied Walnut research context. Do not invent metrics, quotes, filings, historical changes, catalysts, or source links.",
            "Separate underlying data from Walnut confirmation score. Missing data is unavailable, not zero and not bearish.",
            "Do not imply financial advice, guaranteed returns, congressional intent, insider wrongdoing, or real-time 13F activity.",
            "Write directly, specifically, and professionally. Avoid generic AI phrasing and marketing filler.",
            "End with a clear Walnut judgment plus a brief research-only disclaimer.",
            "Return only JSON matching the provided schema.",
            "Admin configuration:",
            json.dumps(config, indent=2, sort_keys=True),
            "Walnut research context:",
            json.dumps(context, indent=2, sort_keys=True, default=str)[:18000],
        ]
    )


def article_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "title",
            "slug",
            "subtitle",
            "summary",
            "judgment",
            "confidence",
            "primary_ticker",
            "comparison_tickers",
            "category",
            "reading_minutes",
            "sections",
            "key_points",
            "catalysts",
            "risks",
            "watch_items",
            "data_freshness",
            "missing_data_notes",
            "suggested_card",
            "seo",
        ],
        "properties": {
            "title": {"type": "string"},
            "slug": {"type": "string"},
            "subtitle": {"type": "string"},
            "summary": {"type": "string"},
            "judgment": {"type": "string", "enum": sorted(JUDGMENT_VALUES)},
            "confidence": {"type": "string", "enum": ["low", "medium", "high"]},
            "primary_ticker": {"type": "string"},
            "comparison_tickers": {"type": "array", "items": {"type": "string"}},
            "category": {"type": "string"},
            "reading_minutes": {"type": "integer"},
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
    slug = _slugify(str(article.get("slug") or title), fallback=f"{context['primary']['identity']['symbol'].lower()}-research-brief")
    if not title:
        warnings.append(_warning("missing_title", "Title is required.", blocking=True))
        blocking = True
    if len(body) < 800:
        warnings.append(_warning("thin_body", "Article body appears too short for a professional research brief.", blocking=True))
        blocking = True
    if "not investment advice" not in body.lower() and "not investment advice" not in str(article.get("summary") or "").lower():
        warnings.append(_warning("missing_disclaimer", "Research-only / not-investment-advice language is missing.", blocking=True))
        blocking = True
    lowered = f"{title}\n{body}".lower()
    for phrase in UNSUPPORTED_LANGUAGE:
        if phrase in lowered:
            warnings.append(_warning("unsupported_language", f"Unsupported language detected: {phrase}", blocking=True))
            blocking = True
    numeric_claims = sorted(set(re.findall(r"(?<![A-Za-z])(?:\$?\d[\d,]*(?:\.\d+)?%?|\d+\s?bps)(?![A-Za-z])", body)))[:80]
    if numeric_claims and not _context_has_numbers(context):
        warnings.append(_warning("numeric_claims_without_context", "Numeric claims detected while source context has few numeric fields.", blocking=True))
        blocking = True
    if _duplicate_slug(slug, draft_id=draft_id):
        warnings.append(_warning("duplicate_slug", f"Slug '{slug}' is already published or reserved.", blocking=True))
        blocking = True
    if not article.get("hero_image"):
        warnings.append(_warning("missing_hero_image", "No hero image selected; the public page will use the polished fallback.", blocking=False))
    return {
        "status": "failed" if blocking else "passed",
        "warnings": warnings,
        "numeric_claims": numeric_claims,
        "source_link_count": body.count("http://") + body.count("https://"),
        "estimated_reading_minutes": max(1, round(len(body.split()) / 220)),
    }


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
        "id": f"rb_{int(time.time() * 1000)}",
        "status": "draft",
        "created_by": admin.id,
        "created_by_email": getattr(admin, "email", None),
        "created_at": created,
        "updated_at": created,
        "published_at": None,
        "model": article.get("_model") or research_brief_model(None),
        "prompt_version": RESEARCH_BRIEF_PROMPT_VERSION,
        "research_context_timestamp": context.get("generated_at"),
        "primary_ticker": context["primary"]["identity"]["symbol"],
        "comparison_ticker": config.get("comparison_ticker"),
        "config": config,
        "article": {k: v for k, v in article.items() if not str(k).startswith("_")},
        "validation": validation,
        "diagnostics": {
            "elapsed_ms": elapsed_ms,
            "storage": "local_json",
            "usage": article.get("_generation_usage") or {},
        },
        "research_context": context,
    }


def _mock_article(config: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    symbol = context["primary"]["identity"]["symbol"]
    company = context["primary"]["identity"].get("company_name") or symbol
    question = config["research_question"]
    body = (
        f"{company} ({symbol}) deserves a focused research review because the current question is specific: {question}\n\n"
        "The available Walnut context should be read as evidence, not as a recommendation. The confirmation score is a separate Walnut signal, while fundamentals, price context, public filings, reported institutional activity, government contracts, and event history are the underlying data.\n\n"
        "The strongest constructive case is that available company and market data still support a credible thesis. The strongest risk case is that missing or stale data can hide a change in the cycle, and unavailable data should not be treated as bearish or bullish by itself.\n\n"
        "What matters next is whether the observable data improves or deteriorates: fundamentals, tape confirmation, public filings, reported activity, catalysts, and risk signals. Research only. Not investment advice."
    )
    return {
        "title": f"{symbol} DD: {question.rstrip('?')}",
        "slug": f"{symbol.lower()}-dd-draft",
        "subtitle": f"A Walnut research brief on {company}.",
        "summary": f"Draft research brief for {symbol}. Research only. Not investment advice.",
        "judgment": "mixed",
        "confidence": "medium",
        "primary_ticker": symbol,
        "comparison_tickers": [config["comparison_ticker"]] if config.get("comparison_ticker") else [],
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
        "suggested_card": {
            "title": f"{symbol} DD research brief",
            "description": f"A research-only Walnut DD brief for {symbol}.",
            "judgment": "mixed",
            "tickers": [symbol],
        },
        "seo": {"title": f"{symbol} DD | Walnut Research", "description": f"Walnut research brief for {symbol}. Not investment advice."},
    }


def list_drafts(status: str | None = None) -> dict[str, Any]:
    with _STORE_LOCK:
        drafts = deepcopy(_read_store().get("drafts", []))
    if status and status != "all":
        drafts = [draft for draft in drafts if draft.get("status") == status]
    return {"items": sorted(drafts, key=lambda item: item.get("updated_at") or "", reverse=True)}


def get_draft(draft_id: str) -> dict[str, Any]:
    for draft in _read_store().get("drafts", []):
        if draft.get("id") == draft_id:
            return deepcopy(draft)
    raise HTTPException(status_code=404, detail="Research brief draft not found.")


def update_draft(admin: UserAccount, draft_id: str, article_patch: dict[str, Any], status: str | None = None) -> dict[str, Any]:
    with _STORE_LOCK:
        store = _read_store()
        for draft in store.get("drafts", []):
            if draft.get("id") == draft_id:
                article = draft.setdefault("article", {})
                article.update({k: v for k, v in article_patch.items() if k in article_schema()["properties"] or k == "hero_image"})
                article["slug"] = _slugify(str(article.get("slug") or article.get("title") or draft.get("primary_ticker")), fallback=f"{draft.get('primary_ticker', 'brief').lower()}-research-brief")
                if status:
                    draft["status"] = _normalize_status(status)
                draft["validation"] = validate_article(article, draft.get("research_context") or {}, draft_id=draft_id)
                draft["updated_at"] = _now()
                _append_audit(store, action="save", admin=admin, draft_id=draft_id)
                _write_store(store)
                return deepcopy(draft)
    raise HTTPException(status_code=404, detail="Research brief draft not found.")


def publish_draft(admin: UserAccount, draft_id: str, *, confirm: bool) -> dict[str, Any]:
    if not confirm:
        raise HTTPException(status_code=422, detail="Publish requires explicit confirmation.")
    with _STORE_LOCK:
        store = _read_store()
        for draft in store.get("drafts", []):
            if draft.get("id") == draft_id:
                validation = validate_article(draft.get("article") or {}, draft.get("research_context") or {}, draft_id=draft_id)
                if validation["status"] != "passed":
                    draft["validation"] = validation
                    _write_store(store)
                    raise HTTPException(status_code=422, detail="Resolve validation failures before publishing.")
                draft["status"] = "published"
                draft["published_at"] = draft.get("published_at") or _now()
                draft["updated_at"] = _now()
                draft["validation"] = validation
                _append_audit(store, action="publish", admin=admin, draft_id=draft_id)
                _write_store(store)
                return deepcopy(draft)
    raise HTTPException(status_code=404, detail="Research brief draft not found.")


def unpublish_draft(admin: UserAccount, draft_id: str, *, confirm: bool) -> dict[str, Any]:
    if not confirm:
        raise HTTPException(status_code=422, detail="Unpublish requires explicit confirmation.")
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


def delete_draft(admin: UserAccount, draft_id: str, *, confirm_text: str) -> dict[str, Any]:
    if confirm_text != "DELETE":
        raise HTTPException(status_code=422, detail="Delete requires typing DELETE.")
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


def published_cards() -> dict[str, Any]:
    drafts = [draft for draft in _read_store().get("drafts", []) if draft.get("status") == "published"]
    cards = []
    for draft in drafts:
        article = draft.get("article") or {}
        suggested = article.get("suggested_card") if isinstance(article.get("suggested_card"), dict) else {}
        slug = article.get("slug")
        if not slug:
            continue
        cards.append(
            {
                "slug": slug,
                "route": f"/research/{slug}",
                "title": suggested.get("title") or article.get("title") or slug,
                "description": suggested.get("description") or article.get("summary") or "",
                "tickers": suggested.get("tickers") or [draft.get("primary_ticker")],
                "category": article.get("category") or "Research",
                "judgment": suggested.get("judgment") or article.get("judgment") or "mixed",
                "publishedAt": (draft.get("published_at") or draft.get("updated_at") or "")[:10],
                "readingMinutes": article.get("reading_minutes") or draft.get("validation", {}).get("estimated_reading_minutes") or 8,
                "generated": True,
            }
        )
    return {"items": cards}


def published_article(slug: str) -> dict[str, Any]:
    normalized = _slugify(slug, fallback=slug)
    for draft in _read_store().get("drafts", []):
        article = draft.get("article") or {}
        if draft.get("status") == "published" and article.get("slug") == normalized:
            return deepcopy(draft)
    raise HTTPException(status_code=404, detail="Research brief not found.")
