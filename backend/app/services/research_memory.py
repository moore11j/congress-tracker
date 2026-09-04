"""Phase 1 private Research Memory services.

This module intentionally stops at creating structured, user-confirmed thesis
objects. It does not schedule, evaluate, or notify on any evidence.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import requests
from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.models import (
    ConfirmationScoreSnapshot,
    FundamentalsCache,
    ResearchThesis,
    ResearchThesisCatalyst,
    ResearchThesisClaim,
    ResearchThesisInvalidator,
    ResearchThesisRisk,
    Security,
    TickerMeta,
    TickerThesisSuggestion,
    UserAccount,
)
from app.services.ai_marketing import OPENAI_API_KEY, resolved_setting_value
from app.services.openai_request_audit import audited_openai_request
from app.utils.symbols import normalize_symbol

logger = logging.getLogger(__name__)

RESEARCH_MEMORY_COMPILER_PROMPT_VERSION = "research_memory_compiler_v1"
RESEARCH_MEMORY_SUGGESTION_PROMPT_VERSION = "research_memory_suggestions_v1"
RESEARCH_MEMORY_SCHEMA_VERSION = "research_memory_schema_v1"
RESEARCH_MEMORY_MODEL = os.getenv("RESEARCH_MEMORY_MODEL", "gpt-5.4-mini")
RESPONSES_ENDPOINT = "https://api.openai.com/v1/responses"

ORIENTATIONS = {"bullish", "bearish", "neutral"}
SOURCE_TYPES = {"walnut_suggested", "template", "custom"}
THESIS_STATUSES = {"draft", "active", "paused", "archived"}
MONITORING_MODES = {"structured_metric", "event", "semantic", "hybrid", "manual"}
COVERAGE_LEVELS = {"fully_monitored", "partially_monitored", "news_monitored", "manual_review_required"}
IMPORTANCE = {"low", "medium", "high"}
SEVERITY = {"low", "medium", "high", "critical"}


def research_memory_enabled() -> bool:
    return os.getenv("RESEARCH_MEMORY_ENABLED", "true").strip().lower() not in {"0", "false", "off", "no"}


def ensure_enabled(user: UserAccount | None) -> None:
    # Existing Walnut convention keeps admin access during a feature rollout.
    if research_memory_enabled() or (user and user.role == "admin"):
        return
    raise HTTPException(status_code=404, detail="Research Memory is not enabled.")


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _json(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), default=str)


def _canonical_evidence_json(value: dict[str, Any]) -> str:
    """Serialize the shared ticker evidence state deterministically for cache identity."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str)


def _parse_json(value: str | None, fallback: Any) -> Any:
    try:
        parsed = json.loads(value or "")
        return parsed if isinstance(parsed, type(fallback)) else fallback
    except (TypeError, ValueError):
        return fallback


def _id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


def _clean_text(value: Any, *, field: str, required: bool = True, limit: int = 1200) -> str | None:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    if required and not text:
        raise HTTPException(status_code=422, detail=f"{field} is required.")
    if len(text) > limit:
        raise HTTPException(status_code=422, detail=f"{field} is too long.")
    return text or None


def _choice(value: Any, allowed: set[str], *, field: str, default: str | None = None) -> str | None:
    cleaned = str(value or default or "").strip().lower()
    if not cleaned and default is None:
        return None
    if cleaned not in allowed:
        raise HTTPException(status_code=422, detail=f"Unsupported {field}.")
    return cleaned


def _date_or_none(value: Any, *, field: str) -> str | None:
    if value in (None, ""):
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).date().isoformat()
    except ValueError:
        raise HTTPException(status_code=422, detail=f"Invalid {field}.")


def _templates() -> list[dict[str, Any]]:
    # Controlled definitions: adding a template only changes this registry, not storage.
    catalog = [
        ("revenue_growth", "Revenue Growth", "Revenue can compound as demand expands.", "revenue", "increase", "structured_metric", "fully_monitored"),
        ("earnings_acceleration", "Earnings Acceleration", "Earnings can accelerate if operating leverage improves.", "eps", "increase", "structured_metric", "fully_monitored"),
        ("margin_expansion", "Margin Expansion", "Margins can improve through mix, pricing, or operating leverage.", "operating margin", "increase", "structured_metric", "partially_monitored"),
        ("product_launch", "Product Launch", "A product launch can create incremental demand.", "product adoption", "increase", "semantic", "partially_monitored"),
        ("commercialization", "Commercialization", "Commercial execution can convert a product opportunity into revenue.", "commercial revenue", "increase", "structured_metric", "partially_monitored"),
        ("market_share_gain", "Market Share Gain", "The company can gain share in its served market.", "market share", "increase", "semantic", "partially_monitored"),
        ("supply_constraint", "Supply Constraint", "Tight supply can support pricing and utilization.", "supply availability", "decrease", "semantic", "partially_monitored"),
        ("pricing_power", "Pricing Power", "Pricing can remain resilient if demand outpaces supply.", "pricing", "increase", "semantic", "partially_monitored"),
        ("government_contract_growth", "Government Contract Growth", "Government contract activity can support growth.", "contract awards", "increase", "event", "news_monitored"),
        ("customer_wins", "Customer Wins", "New customer wins can support future demand.", "customer wins", "increase", "event", "news_monitored"),
        ("regulatory_approval", "Regulatory Approval", "Regulatory progress can unlock commercialization.", "regulatory status", "increase", "event", "news_monitored"),
        ("capacity_expansion", "Capacity Expansion", "Capacity expansion can support higher volume.", "production capacity", "increase", "semantic", "partially_monitored"),
        ("balance_sheet_improvement", "Balance Sheet Improvement", "Lower leverage can improve financial flexibility.", "net debt", "decrease", "structured_metric", "fully_monitored"),
        ("financing_dilution_risk", "Financing / Dilution Risk", "Funding needs can dilute existing shareholders.", "share count", "increase", "structured_metric", "partially_monitored"),
        ("turnaround", "Turnaround", "Operating execution can reverse a deterioration.", "operating performance", "increase", "hybrid", "partially_monitored"),
        ("valuation_rerating", "Valuation Rerating", "Improving fundamentals can support a valuation rerating.", "valuation multiple", "increase", "hybrid", "manual_review_required"),
        ("valuation_compression", "Valuation Compression", "A premium valuation can compress if expectations reset.", "valuation multiple", "decrease", "hybrid", "manual_review_required"),
        ("insider_institutional_accumulation", "Insider / Institutional Accumulation", "Ownership activity can reinforce the investment case.", "ownership activity", "increase", "event", "partially_monitored"),
        ("bear_fundamental_deterioration", "Bear Thesis / Fundamental Deterioration", "Fundamentals can weaken as demand, margins, or balance-sheet health deteriorate.", "fundamentals", "decrease", "hybrid", "partially_monitored"),
    ]
    return [
        {"id": key, "title": title, "description": description, "orientation": "bearish" if key in {"valuation_compression", "bear_fundamental_deterioration", "financing_dilution_risk"} else "bullish", "claim": {"claim_type": key, "subject": "Company", "metric": metric, "expected_direction": direction, "expected_magnitude": None, "expected_timeframe": None, "importance": "high", "monitoring_mode": mode, "coverage_level": coverage}, "catalyst": {"title": "Next earnings and company updates", "catalyst_type": "earnings", "status": "upcoming", "monitoring_mode": "event", "importance": "medium"}, "risk": {"title": "Execution differs from the thesis", "risk_type": "execution", "severity": "high", "monitoring_mode": "semantic"}, "invalidator": {"description": "The core assumption does not develop as expected.", "condition_type": "semantic", "metric": metric, "operator": None, "threshold": None, "time_window": None, "severity": "high", "monitoring_mode": "semantic"}}
        for key, title, description, metric, direction, mode, coverage in catalog
    ]


def list_templates() -> list[dict[str, Any]]:
    return [{k: v for k, v in item.items() if k not in {"claim", "catalyst", "risk", "invalidator"}} for item in _templates()]


def template_draft(template_id: str, *, symbol: str, company_name: str | None = None) -> dict[str, Any]:
    template = next((row for row in _templates() if row["id"] == template_id), None)
    if not template:
        raise HTTPException(status_code=404, detail="Research Memory template not found.")
    company = company_name or symbol
    claim = {**template["claim"], "subject": company}
    return {
        "title": f"{company}: {template['title']}", "summary": template["description"], "orientation": template["orientation"], "target_horizon": None,
        "original_text": None, "source_type": "template", "template_id": template_id,
        "claims": [claim], "catalysts": [template["catalyst"]], "risks": [template["risk"]], "invalidators": [template["invalidator"]],
    }


def _validate_claim(value: dict[str, Any]) -> dict[str, Any]:
    return {
        "claim_type": _clean_text(value.get("claim_type"), field="claim type", limit=120),
        "subject": _clean_text(value.get("subject"), field="claim subject", limit=240),
        "metric": _clean_text(value.get("metric"), field="claim metric", required=False, limit=240),
        "expected_direction": _clean_text(value.get("expected_direction"), field="expected direction", required=False, limit=80),
        "expected_magnitude": _clean_text(value.get("expected_magnitude"), field="expected magnitude", required=False, limit=160),
        "expected_timeframe": _clean_text(value.get("expected_timeframe"), field="expected timeframe", required=False, limit=160),
        "importance": _choice(value.get("importance"), IMPORTANCE, field="claim importance", default="medium"),
        "monitoring_mode": _choice(value.get("monitoring_mode"), MONITORING_MODES, field="claim monitoring mode", default="manual"),
        "coverage_level": _choice(value.get("coverage_level"), COVERAGE_LEVELS, field="claim coverage level", default="manual_review_required"),
    }


def _validate_catalyst(value: dict[str, Any]) -> dict[str, Any]:
    return {"title": _clean_text(value.get("title"), field="catalyst title"), "catalyst_type": _clean_text(value.get("catalyst_type"), field="catalyst type", limit=120), "expected_date": _date_or_none(value.get("expected_date"), field="expected date"), "expected_window_start": _date_or_none(value.get("expected_window_start"), field="expected window start"), "expected_window_end": _date_or_none(value.get("expected_window_end"), field="expected window end"), "status": _clean_text(value.get("status") or "upcoming", field="catalyst status", limit=80), "monitoring_mode": _choice(value.get("monitoring_mode"), MONITORING_MODES, field="catalyst monitoring mode", default="manual"), "importance": _choice(value.get("importance"), IMPORTANCE, field="catalyst importance", default="medium")}


def _validate_risk(value: dict[str, Any]) -> dict[str, Any]:
    return {"title": _clean_text(value.get("title"), field="risk title"), "risk_type": _clean_text(value.get("risk_type"), field="risk type", limit=120), "severity": _choice(value.get("severity"), SEVERITY, field="risk severity", default="medium"), "monitoring_mode": _choice(value.get("monitoring_mode"), MONITORING_MODES, field="risk monitoring mode", default="manual")}


def _validate_invalidator(value: dict[str, Any], *, original_text: str | None = None, enforce_source_threshold: bool = False) -> dict[str, Any]:
    threshold = _clean_text(value.get("threshold"), field="threshold", required=False, limit=120)
    if enforce_source_threshold and threshold and (not original_text or threshold not in original_text):
        raise HTTPException(status_code=422, detail="A generated invalidation threshold must be explicitly present in the original thesis.")
    return {"description": _clean_text(value.get("description"), field="invalidation description"), "condition_type": _clean_text(value.get("condition_type"), field="condition type", limit=120), "metric": _clean_text(value.get("metric"), field="invalidation metric", required=False, limit=240), "operator": _clean_text(value.get("operator"), field="operator", required=False, limit=32), "threshold": threshold, "time_window": _clean_text(value.get("time_window"), field="time window", required=False, limit=160), "severity": _choice(value.get("severity"), SEVERITY, field="invalidation severity", default="high"), "monitoring_mode": _choice(value.get("monitoring_mode"), MONITORING_MODES, field="invalidation monitoring mode", default="manual")}


def validate_draft(value: dict[str, Any], *, source_type: str | None = None, compiler_output: bool = False) -> dict[str, Any]:
    source = _choice(source_type or value.get("source_type"), SOURCE_TYPES, field="source type")
    original_text = _clean_text(value.get("original_text"), field="original thesis", required=False, limit=6000)
    lists = {key: value.get(key) or [] for key in ("claims", "catalysts", "risks", "invalidators")}
    if not isinstance(lists["claims"], list) or not lists["claims"]:
        raise HTTPException(status_code=422, detail="At least one core assumption is required.")
    if any(not isinstance(row, dict) for rows in lists.values() for row in rows):
        raise HTTPException(status_code=422, detail="Research Memory structure is invalid.")
    return {"title": _clean_text(value.get("title"), field="thesis title", limit=240), "summary": _clean_text(value.get("summary"), field="thesis summary", limit=2000), "orientation": _choice(value.get("orientation"), ORIENTATIONS, field="orientation", default="neutral"), "target_horizon": _clean_text(value.get("target_horizon"), field="target horizon", required=False, limit=160), "original_text": original_text, "source_type": source, "template_id": _clean_text(value.get("template_id"), field="template id", required=False, limit=120), "claims": [_validate_claim(row) for row in lists["claims"]], "catalysts": [_validate_catalyst(row) for row in lists["catalysts"]], "risks": [_validate_risk(row) for row in lists["risks"]], "invalidators": [_validate_invalidator(row, original_text=original_text, enforce_source_threshold=compiler_output) for row in lists["invalidators"]]}


def _serialize_thesis(db: Session, thesis: ResearchThesis) -> dict[str, Any]:
    claims = db.execute(select(ResearchThesisClaim).where(ResearchThesisClaim.thesis_id == thesis.id).order_by(ResearchThesisClaim.created_at)).scalars().all()
    catalysts = db.execute(select(ResearchThesisCatalyst).where(ResearchThesisCatalyst.thesis_id == thesis.id).order_by(ResearchThesisCatalyst.created_at)).scalars().all()
    risks = db.execute(select(ResearchThesisRisk).where(ResearchThesisRisk.thesis_id == thesis.id).order_by(ResearchThesisRisk.created_at)).scalars().all()
    invalidators = db.execute(select(ResearchThesisInvalidator).where(ResearchThesisInvalidator.thesis_id == thesis.id).order_by(ResearchThesisInvalidator.created_at)).scalars().all()
    security = db.get(Security, thesis.security_id)
    def iso(v: Any) -> str | None: return v.isoformat() if v else None
    return {"id": thesis.id, "security_id": thesis.security_id, "ticker": thesis.ticker_at_creation, "company_name": security.name if security else thesis.ticker_at_creation, "title": thesis.title, "original_text": thesis.original_text, "summary": thesis.summary, "orientation": thesis.orientation, "target_horizon": thesis.target_horizon, "status": thesis.status, "source_type": thesis.source_type, "template_id": thesis.template_id, "created_at": iso(thesis.created_at), "updated_at": iso(thesis.updated_at), "started_monitoring_at": iso(thesis.started_monitoring_at), "paused_at": iso(thesis.paused_at), "claims": [{"id": x.id, "claim_type": x.claim_type, "subject": x.subject, "metric": x.metric, "expected_direction": x.expected_direction, "expected_magnitude": x.expected_magnitude, "expected_timeframe": x.expected_timeframe, "importance": x.importance, "monitoring_mode": x.monitoring_mode, "coverage_level": x.coverage_level, "user_confirmed": x.user_confirmed} for x in claims], "catalysts": [{"id": x.id, "title": x.title, "catalyst_type": x.catalyst_type, "expected_date": iso(x.expected_date), "expected_window_start": iso(x.expected_window_start), "expected_window_end": iso(x.expected_window_end), "status": x.status, "monitoring_mode": x.monitoring_mode, "importance": x.importance} for x in catalysts], "risks": [{"id": x.id, "title": x.title, "risk_type": x.risk_type, "severity": x.severity, "monitoring_mode": x.monitoring_mode} for x in risks], "invalidators": [{"id": x.id, "description": x.description, "condition_type": x.condition_type, "metric": x.metric, "operator": x.operator, "threshold": x.threshold, "time_window": x.time_window, "severity": x.severity, "monitoring_mode": x.monitoring_mode} for x in invalidators], "phase_one_notice": "Research Memory created. Continuous evidence monitoring will be added in the next phase."}


def _persist_children(db: Session, thesis_id: str, structure: dict[str, Any]) -> None:
    for model in (ResearchThesisClaim, ResearchThesisCatalyst, ResearchThesisRisk, ResearchThesisInvalidator):
        db.query(model).filter(model.thesis_id == thesis_id).delete(synchronize_session=False)
    db.add_all([ResearchThesisClaim(id=_id("rtc"), thesis_id=thesis_id, user_confirmed=False, **row) for row in structure["claims"]])
    db.add_all([ResearchThesisCatalyst(id=_id("rtcat"), thesis_id=thesis_id, expected_date=datetime.fromisoformat(row["expected_date"]).date() if row["expected_date"] else None, expected_window_start=datetime.fromisoformat(row["expected_window_start"]).date() if row["expected_window_start"] else None, expected_window_end=datetime.fromisoformat(row["expected_window_end"]).date() if row["expected_window_end"] else None, **{k: v for k, v in row.items() if k not in {"expected_date", "expected_window_start", "expected_window_end"}}) for row in structure["catalysts"]])
    db.add_all([ResearchThesisRisk(id=_id("rtr"), thesis_id=thesis_id, **row) for row in structure["risks"]])
    db.add_all([ResearchThesisInvalidator(id=_id("rti"), thesis_id=thesis_id, **row) for row in structure["invalidators"]])


def resolve_security(db: Session, symbol: str) -> Security:
    normalized = normalize_symbol(symbol)
    if not normalized:
        raise HTTPException(status_code=422, detail="A valid ticker is required.")
    security = db.execute(select(Security).where(Security.symbol == normalized)).scalar_one_or_none()
    if not security:
        raise HTTPException(status_code=404, detail="Ticker security not found.")
    return security


def create_draft(db: Session, *, user: UserAccount, security: Security, structure: dict[str, Any]) -> dict[str, Any]:
    valid = validate_draft(structure)
    thesis = ResearchThesis(id=_id("rt"), user_id=user.id, security_id=security.id, ticker_at_creation=security.symbol or "", title=valid["title"], original_text=valid["original_text"], summary=valid["summary"], orientation=valid["orientation"], target_horizon=valid["target_horizon"], status="draft", source_type=valid["source_type"], template_id=valid["template_id"])
    db.add(thesis); db.flush(); _persist_children(db, thesis.id, valid); db.commit()
    return _serialize_thesis(db, thesis)


def update_draft(db: Session, *, user: UserAccount, thesis_id: str, structure: dict[str, Any]) -> dict[str, Any]:
    thesis = owned_thesis(db, user=user, thesis_id=thesis_id)
    if thesis.status == "archived": raise HTTPException(status_code=409, detail="Archived Research Memory cannot be edited.")
    valid = validate_draft(structure, source_type=thesis.source_type)
    for field in ("title", "summary", "orientation", "target_horizon", "original_text", "template_id"):
        setattr(thesis, field, valid[field])
    _persist_children(db, thesis.id, valid); db.commit()
    return _serialize_thesis(db, thesis)


def activate(db: Session, *, user: UserAccount, thesis_id: str) -> dict[str, Any]:
    thesis = owned_thesis(db, user=user, thesis_id=thesis_id)
    if thesis.status not in {"draft", "paused"}: raise HTTPException(status_code=409, detail="This Research Memory cannot be activated from its current status.")
    thesis.status, thesis.started_monitoring_at, thesis.paused_at = "active", _now(), None
    for claim in db.execute(select(ResearchThesisClaim).where(ResearchThesisClaim.thesis_id == thesis.id)).scalars(): claim.user_confirmed = True
    db.commit(); return _serialize_thesis(db, thesis)


def owned_thesis(db: Session, *, user: UserAccount, thesis_id: str) -> ResearchThesis:
    thesis = db.execute(select(ResearchThesis).where(ResearchThesis.id == thesis_id, ResearchThesis.user_id == user.id)).scalar_one_or_none()
    if not thesis: raise HTTPException(status_code=404, detail="Research Memory not found.")
    return thesis


def list_user_theses(db: Session, *, user: UserAccount) -> list[dict[str, Any]]:
    rows = db.execute(select(ResearchThesis).where(ResearchThesis.user_id == user.id).order_by(ResearchThesis.updated_at.desc())).scalars().all()
    return [_serialize_thesis(db, row) for row in rows]


def list_active_theses_for_security(db: Session, *, user: UserAccount, security_id: int) -> list[dict[str, Any]]:
    rows = db.execute(
        select(ResearchThesis)
        .where(
            ResearchThesis.user_id == user.id,
            ResearchThesis.security_id == security_id,
            ResearchThesis.status == "active",
        )
        .order_by(ResearchThesis.updated_at.desc())
    ).scalars().all()
    return [_serialize_thesis(db, row) for row in rows]


def _evidence_state(db: Session, security: Security) -> dict[str, Any]:
    symbol = security.symbol or ""
    meta = db.get(TickerMeta, symbol)
    fundamentals = db.execute(select(FundamentalsCache).where(FundamentalsCache.symbol == symbol).order_by(FundamentalsCache.fetched_at.desc(), FundamentalsCache.id.desc()).limit(1)).scalar_one_or_none()
    score = db.execute(select(ConfirmationScoreSnapshot).where(ConfirmationScoreSnapshot.security_id == security.id).order_by(ConfirmationScoreSnapshot.calculated_at.desc(), ConfirmationScoreSnapshot.id.desc()).limit(1)).scalar_one_or_none()
    return {
        "security_id": security.id,
        "symbol": symbol,
        "company_name": security.name,
        "sector": meta.sector if meta else None,
        "industry": meta.industry if meta else None,
        "fundamentals": {
            "period_date": fundamentals.period_date.isoformat() if fundamentals and fundamentals.period_date else None,
            "revenue_growth": fundamentals.revenue_growth if fundamentals else None,
            "eps_growth": fundamentals.eps_growth if fundamentals else None,
            "operating_margin_expansion": fundamentals.operating_margin_expansion if fundamentals else None,
            "net_debt_to_ebitda": fundamentals.net_debt_to_ebitda if fundamentals else None,
        },
        "confirmation": {
            "market_date": score.market_date.isoformat() if score else None,
            "direction": score.direction if score else None,
            "active_sources": score.active_source_count if score else 0,
            "score": score.score if score else None,
            "input_hash": score.input_hash if score else None,
            "methodology_version_id": score.methodology_version_id if score else None,
            "calculation_type": score.calculation_type if score else None,
        },
    }


def _suggestions_from_evidence(state: dict[str, Any]) -> list[dict[str, Any]]:
    symbol, company = state["symbol"], state["company_name"] or state["symbol"]
    f, c = state["fundamentals"], state["confirmation"]
    result: list[dict[str, Any]] = []
    if isinstance(f.get("revenue_growth"), (float, int)) and f["revenue_growth"] > 0:
        draft = template_draft("revenue_growth", symbol=symbol, company_name=company); draft.update({"title": f"{company}: Revenue Growth", "summary": f"{company} may outperform if revenue growth remains positive and execution sustains demand."}); result.append({"suggestion_type": "revenue_growth", "evidence_basis": [f"Walnut fundamentals show positive revenue growth ({f['revenue_growth']:.1f}%)."], "structured": draft})
    if isinstance(f.get("operating_margin_expansion"), (float, int)) and f["operating_margin_expansion"] > 0:
        draft = template_draft("margin_expansion", symbol=symbol, company_name=company); draft.update({"title": f"{company}: Margin Expansion"}); result.append({"suggestion_type": "margin_expansion", "evidence_basis": [f"Walnut fundamentals show positive operating-margin expansion ({f['operating_margin_expansion']:.1f} percentage points)."], "structured": draft})
    if c.get("direction") == "bullish" and int(c.get("active_sources") or 0) >= 2:
        draft = template_draft("earnings_acceleration", symbol=symbol, company_name=company); draft.update({"title": f"{company}: Cross-Source Growth Setup", "summary": f"{company} may outperform if the current bullish Walnut cross-source setup is supported by continued fundamentals."}); result.append({"suggestion_type": "cross_source_growth", "evidence_basis": [f"Walnut's latest confirmation snapshot is bullish across {c['active_sources']} active sources."], "structured": draft})
    return result[:5]


def suggestions_for_security(db: Session, *, security: Security) -> dict[str, Any]:
    state = _evidence_state(db, security); evidence_hash = hashlib.sha256(_canonical_evidence_json(state).encode("utf-8")).hexdigest(); now = _now()
    rows = db.execute(select(TickerThesisSuggestion).where(TickerThesisSuggestion.security_id == security.id, TickerThesisSuggestion.evidence_state_hash == evidence_hash, TickerThesisSuggestion.prompt_version == RESEARCH_MEMORY_SUGGESTION_PROMPT_VERSION, TickerThesisSuggestion.schema_version == RESEARCH_MEMORY_SCHEMA_VERSION, (TickerThesisSuggestion.expires_at.is_(None)) | (TickerThesisSuggestion.expires_at > now)).order_by(TickerThesisSuggestion.title)).scalars().all()
    if not rows:
        generated = _suggestions_from_evidence(state)
        for item in generated:
            structure = validate_draft(item["structured"], source_type="template")
            structure["source_type"] = "walnut_suggested"
            try:
                db.add(TickerThesisSuggestion(id=_id("rts"), security_id=security.id, suggestion_type=item["suggestion_type"], title=structure["title"], summary=structure["summary"], orientation=structure["orientation"], evidence_basis_json=_json(item["evidence_basis"]), structured_thesis_json=_json(structure), evidence_state_hash=evidence_hash, model_version="deterministic-v1", prompt_version=RESEARCH_MEMORY_SUGGESTION_PROMPT_VERSION, schema_version=RESEARCH_MEMORY_SCHEMA_VERSION, generated_at=now, expires_at=now + timedelta(hours=24)))
                db.flush()
            except IntegrityError:
                db.rollback()
        db.commit()
        rows = db.execute(select(TickerThesisSuggestion).where(TickerThesisSuggestion.security_id == security.id, TickerThesisSuggestion.evidence_state_hash == evidence_hash, TickerThesisSuggestion.prompt_version == RESEARCH_MEMORY_SUGGESTION_PROMPT_VERSION, TickerThesisSuggestion.schema_version == RESEARCH_MEMORY_SCHEMA_VERSION).order_by(TickerThesisSuggestion.title)).scalars().all()
    return {"security_id": security.id, "ticker": security.symbol, "evidence_state_hash": evidence_hash, "items": [{"id": row.id, "suggestion_type": row.suggestion_type, "title": row.title, "summary": row.summary, "orientation": row.orientation, "evidence_basis": _parse_json(row.evidence_basis_json, []), "structured_thesis": _parse_json(row.structured_thesis_json, {}), "monitoring_coverage": [claim.get("coverage_level") for claim in _parse_json(row.structured_thesis_json, {}).get("claims", []) if isinstance(claim, dict)]} for row in rows]}


def suggestion_draft(db: Session, *, security: Security, suggestion_id: str) -> dict[str, Any]:
    row = db.execute(select(TickerThesisSuggestion).where(TickerThesisSuggestion.id == suggestion_id, TickerThesisSuggestion.security_id == security.id)).scalar_one_or_none()
    if not row: raise HTTPException(status_code=404, detail="Suggested thesis not found.")
    draft = _parse_json(row.structured_thesis_json, {})
    draft["source_type"] = "walnut_suggested"; return validate_draft(draft)


def _compiler_schema() -> dict[str, Any]:
    # Strict schema and server normalization deliberately keep numeric thresholds nullable.
    leaf = {"type": "object", "additionalProperties": False}
    return {"type": "object", "additionalProperties": False, "required": ["title", "summary", "orientation", "target_horizon", "claims", "catalysts", "risks", "invalidators"], "properties": {"title": {"type": "string"}, "summary": {"type": "string"}, "orientation": {"type": "string", "enum": sorted(ORIENTATIONS)}, "target_horizon": {"type": ["string", "null"]}, "claims": {"type": "array", "items": {**leaf, "required": ["claim_type", "subject", "metric", "expected_direction", "expected_magnitude", "expected_timeframe", "importance", "monitoring_mode", "coverage_level"], "properties": {"claim_type": {"type": "string"}, "subject": {"type": "string"}, "metric": {"type": ["string", "null"]}, "expected_direction": {"type": ["string", "null"]}, "expected_magnitude": {"type": ["string", "null"]}, "expected_timeframe": {"type": ["string", "null"]}, "importance": {"type": "string", "enum": sorted(IMPORTANCE)}, "monitoring_mode": {"type": "string", "enum": sorted(MONITORING_MODES)}, "coverage_level": {"type": "string", "enum": sorted(COVERAGE_LEVELS)}}}}, "catalysts": {"type": "array", "items": {"type": "object"}}, "risks": {"type": "array", "items": {"type": "object"}}, "invalidators": {"type": "array", "items": {"type": "object"}}}}


def _response_text(data: dict[str, Any]) -> str:
    if isinstance(data.get("output_text"), str): return data["output_text"]
    for output in data.get("output") or []:
        for content in output.get("content") or []:
            if isinstance(content, dict) and isinstance(content.get("text"), str): return content["text"]
    return ""


def compile_custom_thesis(db: Session, *, security: Security, original_text: str) -> dict[str, Any]:
    original = _clean_text(original_text, field="custom thesis", limit=6000)
    api_key = resolved_setting_value(db, OPENAI_API_KEY)
    if not api_key: raise HTTPException(status_code=503, detail="Thesis interpretation is temporarily unavailable. Please try again later.")
    state = _evidence_state(db, security)
    prompt = "\n".join(["You compile a private investment thesis into Walnut Research Memory structure.", "Return only the strict schema. Preserve uncertainty. Do not invent facts, catalysts, risks, dates, or numerical thresholds. A threshold must be null unless the user explicitly gave that number.", f"SECURITY: {security.symbol} / {security.name}", f"WALNUT_CONTEXT: {_json(state)}", f"USER_THESIS: {original}"])
    payload = {"model": RESEARCH_MEMORY_MODEL, "input": prompt, "store": False, "max_output_tokens": 2400, "text": {"format": {"type": "json_schema", "name": "research_memory_thesis", "strict": True, "schema": _compiler_schema()}}}
    try:
        response = audited_openai_request(feature="research_memory", operation="custom_thesis_compile", method="POST", endpoint=RESPONSES_ENDPOINT, payload=payload, model=RESEARCH_MEMORY_MODEL, send=lambda: requests.post(RESPONSES_ENDPOINT, headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}, json=payload, timeout=45))
    except requests.Timeout as exc:
        raise HTTPException(status_code=504, detail="Thesis interpretation timed out. Please retry.") from exc
    except requests.RequestException as exc:
        raise HTTPException(status_code=502, detail="Thesis interpretation is temporarily unavailable. Please retry.") from exc
    if response.status_code >= 400:
        logger.warning("research_memory_compiler_provider_error status=%s", response.status_code)
        raise HTTPException(status_code=502, detail="Thesis interpretation is temporarily unavailable. Please retry.")
    try:
        parsed = json.loads(_response_text(response.json()))
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=502, detail="Thesis interpretation returned an invalid structure. Please retry.") from exc
    if not isinstance(parsed, dict): raise HTTPException(status_code=502, detail="Thesis interpretation returned an invalid structure. Please retry.")
    parsed.update({"original_text": original, "source_type": "custom", "template_id": None})
    return validate_draft(parsed, source_type="custom", compiler_output=True)
