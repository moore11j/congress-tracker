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
    AI_MARKETING_IMAGE_GENERATION_ENABLED,
    AI_MARKETING_IMAGE_MODEL,
    AI_MARKETING_IMAGE_QUALITY,
    AI_MARKETING_IMAGE_SIZE,
    AI_MARKETING_MODEL,
    DEFAULT_AI_MARKETING_IMAGE_MODEL,
    DEFAULT_AI_MARKETING_IMAGE_QUALITY,
    DEFAULT_AI_MARKETING_IMAGE_SIZE,
    DEFAULT_AI_MARKETING_MODEL,
    OPENAI_API_KEY,
    resolved_setting_value,
)
from app.services.confirmation_score import get_confirmation_score_bundles_for_tickers
from app.utils.symbols import normalize_symbol

RESEARCH_BRIEF_PROMPT_VERSION = "research_brief_v1"
RESEARCH_BRIEF_GENERATOR_MODEL = "RESEARCH_BRIEF_GENERATOR_MODEL"
RESEARCH_BRIEF_MODEL_DEFAULT = "RESEARCH_BRIEF_MODEL_DEFAULT"
RESEARCH_BRIEF_MODEL_OPTIONS = "RESEARCH_BRIEF_MODEL_OPTIONS"
RESPONSES_ENDPOINT = "https://api.openai.com/v1/responses"
IMAGES_ENDPOINT = "https://api.openai.com/v1/images/generations"
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
EXTERNAL_RESEARCH_MODE_OPTIONS = {"Off", "Standard", "Deep"}
SECTION_FORMAT_OPTIONS = [
    "Walnut Research Brief",
    "Reddit DD - Issue / Risk / Data / Conclusion",
    "Reddit DD - Bull Case / Bear Case / The Data / The Call",
    "ValueInvesting - Business / Valuation / Risks / Margin of Safety",
    "X Thread",
    "Internal Analyst Note",
]
STATUS_OPTIONS = {"generating", "draft", "ready_for_review", "published", "unpublished", "failed"}
JUDGMENT_VALUES = {"bullish", "bearish", "mixed", "macro", "policy", "neutral"}
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
    configured = os.getenv(RESEARCH_BRIEF_MODEL_DEFAULT, "").strip() or os.getenv(RESEARCH_BRIEF_GENERATOR_MODEL, "").strip()
    if configured:
        return configured
    return resolved_setting_value(db, AI_MARKETING_MODEL) or DEFAULT_AI_MARKETING_MODEL


def research_brief_model_options(db: Session | None = None) -> list[str]:
    configured = [item.strip() for item in os.getenv(RESEARCH_BRIEF_MODEL_OPTIONS, "").split(",") if item.strip()]
    default = research_brief_model(db)
    options = configured or [default]
    if default not in options:
        options.insert(0, default)
    return list(dict.fromkeys(options))


def research_brief_model_descriptions(db: Session | None = None) -> dict[str, str]:
    options = research_brief_model_options(db)
    labels = ["Fast / cheaper", "Balanced", "Deep research / highest quality"]
    return {model: labels[min(index, len(labels) - 1)] for index, model in enumerate(options)}


def _selected_research_model(config: dict[str, Any], db: Session | None = None) -> str:
    options = research_brief_model_options(db)
    selected = str(config.get("selected_model") or "").strip()
    if selected:
        if selected not in options:
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
    external_research = discover_external_research(symbol, identity, mode=payload.get("external_research_mode") or "Standard")
    if external_research.get("missing_data_notes"):
        missing.extend(external_research["missing_data_notes"])

    context = {
        "generated_at": _now(),
        "external_research_mode": payload.get("external_research_mode") or "Standard",
        "section_format": payload.get("section_format") or "Walnut Research Brief",
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
        "external_research": external_research,
        "comparison": None,
        "missing_data_notes": _dedupe_strings(missing),
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


def discover_external_research(symbol: str, identity: dict[str, Any], *, mode: str) -> dict[str, Any]:
    normalized_mode = _choice(mode, EXTERNAL_RESEARCH_MODE_OPTIONS, "Standard")
    if normalized_mode == "Off":
        return {
            "mode": "Off",
            "reviewed_sources": [],
            "source_notes": ["External research mode is off; only Walnut data was reviewed."],
            "official_facts": {},
            "missing_data_notes": [],
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
        official_facts = _sec_company_facts(cik)
        source_notes.append(f"Matched {symbol} to SEC CIK {cik} for official filings and company-facts review.")
    else:
        source_notes.append(f"SEC ticker mapping did not return a CIK for {symbol}; EDGAR symbol search remains attached for manual review.")
    if normalized_mode == "Deep":
        source_notes.append("Deep mode also attaches a reputable public market reference for price/volume review.")
    missing_fields = _missing_key_fields(official_facts)
    return {
        "mode": normalized_mode,
        "reviewed_sources": reviewed_sources,
        "source_notes": source_notes,
        "official_facts": official_facts,
        "missing_data_notes": [f"{field}: Not found in reviewed sources" for field in missing_fields],
    }


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
        "external_research_mode": _choice(config.get("external_research_mode"), EXTERNAL_RESEARCH_MODE_OPTIONS, "Standard"),
        "section_format": _choice_from_list(config.get("section_format"), SECTION_FORMAT_OPTIONS, "Walnut Research Brief"),
        "include_charts": bool(config.get("include_charts")),
        "include_source_links": bool(config.get("include_source_links")),
        "generate_thumbnail": bool(config.get("generate_thumbnail", _default_generate_thumbnail(config))),
        "selected_model": str(config.get("selected_model") or "").strip(),
        "hero_image": config.get("hero_image") or "",
    }
    normalized["selected_model"] = _selected_research_model(normalized)
    if normalized["desired_angle"] == "Peer comparison" and not normalized["comparison_ticker"]:
        raise HTTPException(status_code=422, detail="Comparison ticker is required for peer comparison briefs.")
    return normalized


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


def generate_research_brief(db: Session, admin: UserAccount, config: dict[str, Any]) -> dict[str, Any]:
    normalized_config = validate_config(config)
    normalized_config["selected_model"] = _selected_research_model(normalized_config, db)
    context = assemble_research_context(db, normalized_config)
    actor_key = f"admin:{admin.id}"
    if actor_key in _ACTIVE_GENERATIONS:
        raise HTTPException(status_code=429, detail="A research brief generation is already running for this Admin session.")
    _ACTIVE_GENERATIONS.add(actor_key)
    try:
        started = time.perf_counter()
        article = _mock_article(normalized_config, context) if os.getenv(MOCK_ENV) == "1" else _call_openai(db, normalized_config, context)
        if normalized_config.get("generate_thumbnail"):
            article["thumbnail_asset"] = generate_thumbnail_asset(db, normalized_config, article)
            if article["thumbnail_asset"].get("url") and not article.get("hero_image"):
                article["hero_image"] = article["thumbnail_asset"]["url"]
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
    model = _selected_research_model(config, db)
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
    section_format = _section_format_instructions(config.get("section_format") or "Walnut Research Brief")
    return "\n".join(
        [
            "You are Walnut's senior market research editor writing a publishable due-diligence brief.",
            "Use supplied Walnut research context plus the attached external research notes and reviewed public source links. Do not invent metrics, quotes, filings, historical changes, catalysts, or source links.",
            "When Walnut data misses a key field, use official/public reviewed sources first. If still unavailable, say 'Not found in reviewed sources' once in Data limitations, not repeatedly field by field.",
            "Any publishable research/DD post must include at least two credible source links, and valuation/DD work should include an official/company/filing source when possible.",
            "Separate underlying data from Walnut confirmation score. Missing data is unavailable, not zero and not bearish.",
            "Use 'data', not 'stack'. Use 'reported' or 'disclosed' for Congress, insider, and institutional activity. For 13F data, say 'reported institutional activity', 'filing date', and 'quarter-end holdings'; never imply live institutional buying.",
            "Never expose provider, internal, cache, raw, token, credential, or diagnostic wording in user-facing copy.",
            "For DCF/valuation briefs, do not produce a fake DCF when inputs are missing. Separate reported numbers from assumptions and say when a DCF cannot be anchored.",
            "Do not imply financial advice, guaranteed returns, congressional intent, insider wrongdoing, or real-time 13F activity.",
            "Write directly, specifically, and professionally. Avoid generic AI phrasing and marketing filler.",
            "End with a clear Walnut judgment plus a brief research-only disclaimer.",
            "The JSON summary is the Insights preview body. Keep it 1-3 sentences and do not duplicate the full post body.",
            "Section format instructions:",
            section_format,
            "Key missing-field search checklist:",
            json.dumps(KEY_RESEARCH_FIELDS, indent=2),
            "Return only JSON matching the provided schema.",
            "Admin configuration:",
            json.dumps(config, indent=2, sort_keys=True),
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
        return "Use this markdown structure: Intro / hook; Bull case; Bear case; The data; The call; Sources; What to watch next."
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
    slug = _slugify(str(article.get("slug") or title), fallback=f"{context['primary']['identity']['symbol'].lower()}-research-brief")
    if not title:
        warnings.append(_warning("missing_title", "Title is required.", blocking=True))
        blocking = True
    if len(body) < 800:
        warnings.append(_warning("thin_body", "Article body appears too short for a professional research brief.", blocking=True))
        blocking = True
    summary_text = f"{article.get('summary') or ''}\n{article.get('preview_body') or ''}"
    if "not investment advice" not in body.lower() and "not investment advice" not in summary_text.lower():
        warnings.append(_warning("missing_disclaimer", "Research-only / not-investment-advice language is missing.", blocking=True))
        blocking = True
    lowered = f"{title}\n{body}".lower()
    source_link_count = _source_link_count(article, body)
    if source_link_count == 0:
        warnings.append(_warning("missing_source_links", "This draft has no source links. Regenerate with External Research Mode enabled or add sources manually.", blocking=True))
        blocking = True
    elif source_link_count < 2:
        warnings.append(_warning("insufficient_source_links", "Research briefs need at least 2 credible source links before publishing.", blocking=True))
        blocking = True
    for phrase in UNSUPPORTED_LANGUAGE:
        if phrase in lowered:
            warnings.append(_warning("unsupported_language", f"Unsupported language detected: {phrase}", blocking=True))
            blocking = True
    if "not supplied" in lowered:
        warnings.append(_warning("not_supplied_language", "Use 'Not found in reviewed sources' once in Data limitations instead of repeated 'not supplied' language.", blocking=True))
        blocking = True
    if re.search(r"\b(provider|internal|cache|raw|token|credential|diagnostic)s?\b", lowered):
        warnings.append(_warning("internal_wording", "Provider/internal/cache/source-system wording must not appear in user-facing output.", blocking=True))
        blocking = True
    if "confirmation score equals" in lowered or "confirmation stack" in lowered:
        warnings.append(_warning("confirmation_score_blended", "Confirmation score must remain separate from underlying data.", blocking=True))
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
        "source_link_count": source_link_count,
        "estimated_reading_minutes": max(1, round(len(body.split()) / 220)),
    }


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
        "id": f"rb_{int(time.time() * 1000)}",
        "status": "draft",
        "created_by": admin.id,
        "created_by_email": getattr(admin, "email", None),
        "created_at": created,
        "updated_at": created,
        "published_at": None,
        "model": article.get("_model") or config.get("selected_model") or research_brief_model(None),
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
            timeout=130,
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
        "The available Walnut context should be read as evidence, not as a recommendation. The confirmation score is a separate Walnut signal, while fundamentals, price context, public filings, reported institutional activity, government contracts, and event history are the underlying data.\n\n"
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
        "source_links": source_links[:4],
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
                article.update({k: v for k, v in article_patch.items() if k in article_schema()["properties"] or k in {"hero_image", "thumbnail_asset"}})
                article["slug"] = _slugify(str(article.get("slug") or article.get("title") or draft.get("primary_ticker")), fallback=f"{draft.get('primary_ticker', 'brief').lower()}-research-brief")
                if status:
                    draft["status"] = _normalize_status(status)
                draft["validation"] = validate_article(article, draft.get("research_context") or {}, draft_id=draft_id)
                draft["updated_at"] = _now()
                _append_audit(store, action="save", admin=admin, draft_id=draft_id)
                _write_store(store)
                return deepcopy(draft)
    raise HTTPException(status_code=404, detail="Research brief draft not found.")


def refresh_research_sources(db: Session, admin: UserAccount, draft_id: str) -> dict[str, Any]:
    with _STORE_LOCK:
        store = _read_store()
        for draft in store.get("drafts", []):
            if draft.get("id") != draft_id:
                continue
            config = validate_config(draft.get("config") or {})
            symbol = str(draft.get("primary_ticker") or config.get("ticker") or "").upper()
            identity = ((draft.get("research_context") or {}).get("primary") or {}).get("identity") or {"symbol": symbol}
            external = discover_external_research(symbol, identity, mode=config.get("external_research_mode") or "Standard")
            context = draft.setdefault("research_context", {})
            context["external_research"] = external
            context["external_research_mode"] = external.get("mode")
            context["generated_at"] = _now()
            existing_missing = context.get("missing_data_notes") if isinstance(context.get("missing_data_notes"), list) else []
            context["missing_data_notes"] = _dedupe_strings([*existing_missing, *(external.get("missing_data_notes") or [])])
            article = draft.setdefault("article", {})
            article["missing_data_notes"] = _dedupe_strings([*(article.get("missing_data_notes") or []), *(external.get("missing_data_notes") or [])])
            article["source_links"] = _dedupe_source_links([*(article.get("source_links") or []), *(external.get("reviewed_sources") or [])])
            draft["validation"] = validate_article(article, context, draft_id=draft_id)
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
