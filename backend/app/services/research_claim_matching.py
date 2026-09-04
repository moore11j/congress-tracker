"""Phase 3 private matching between global Evidence Events and active user claims.

No global evidence is changed and this module never aggregates matches into a
thesis conclusion.  Phase 4 is responsible for any thesis-health interpretation.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import uuid
from datetime import date, datetime, time, timezone
from typing import Any, Callable

import requests
from fastapi import HTTPException
from sqlalchemy import and_, func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.models import (
    ResearchClaimEvidenceMatch, ResearchClaimMatchCheckpoint, ResearchEvidenceEvent,
    ResearchInvalidatorEvidenceMatch, ResearchThesis, ResearchThesisClaim,
    ResearchThesisInvalidator, Security, UserAccount,
)
from app.services.ai_marketing import OPENAI_API_KEY, resolved_setting_value
from app.services.openai_request_audit import audited_openai_request

logger = logging.getLogger(__name__)

CLAIM_MATCHING_PROMPT_VERSION = "claim_matching_v1"
CLAIM_MATCHING_SCHEMA_VERSION = "claim_matching_schema_v1"
CLAIM_MATCHING_VERSION = "claim_matching_engine_v1"
CLAIM_MATCHING_MODEL = os.getenv("RESEARCH_CLAIM_MATCHING_MODEL", "gpt-5.4-mini")
RESPONSES_ENDPOINT = "https://api.openai.com/v1/responses"

RELATIONSHIPS = {"supports", "contradicts", "related", "potential_invalidator"}
SEMANTIC_RELATIONSHIPS = RELATIONSHIPS | {"unrelated"}
LEVELS = {"high", "medium", "low"}
METHODS = {"deterministic", "semantic", "hybrid", "manual"}

METRIC_ALIASES = {
    "gross_margin": {"gross margin", "gross_margin"},
    "operating_margin": {"operating margin", "operating_margin", "margin"},
    "revenue_growth": {"revenue growth", "revenue_growth", "sales growth"},
    "eps_growth": {"eps growth", "eps_growth", "earnings growth"},
    "free_cash_flow": {"free cash flow", "free_cash_flow", "fcf"},
    "net_debt_to_ebitda": {"net debt", "net debt to ebitda", "net_debt_to_ebitda", "leverage", "debt"},
    "government_contract_award": {"government contract", "government contracts", "contract award", "contract awards"},
    "institutional_shares": {"institutional shares", "institutional accumulation", "institutional ownership"},
    "insider_transaction_value": {"insider purchase", "insider sale", "insider accumulation", "insider ownership"},
    "confirmation_score": {"confirmation score", "cross source alignment", "walnut signal"},
}


def claim_matching_enabled() -> bool:
    return os.getenv("RESEARCH_CLAIM_MATCHING_ENABLED", "true").strip().lower() not in {"0", "false", "off", "no"}


def ensure_claim_matching_enabled(*, is_admin: bool = False) -> None:
    if claim_matching_enabled() or is_admin:
        return
    raise HTTPException(status_code=404, detail="Research Claim Matching is not enabled.")


def _id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str)


def _hash(value: Any) -> str:
    return hashlib.sha256(_json(value).encode("utf-8")).hexdigest()


def _text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def normalize_metric(value: str | None) -> str | None:
    clean = _text(value).lower().replace("-", " ").replace("_", " ")
    if not clean:
        return None
    for canonical, aliases in METRIC_ALIASES.items():
        if clean in {alias.replace("_", " ") for alias in aliases}:
            return canonical
    return clean.replace(" ", "_")


def _claim_snapshot(claim: ResearchThesisClaim) -> dict[str, Any]:
    return {"id": claim.id, "claim_type": claim.claim_type, "subject": claim.subject, "metric": claim.metric, "expected_direction": claim.expected_direction, "expected_magnitude": claim.expected_magnitude, "expected_timeframe": claim.expected_timeframe, "importance": claim.importance, "monitoring_mode": claim.monitoring_mode, "coverage_level": claim.coverage_level, "updated_at": claim.updated_at.isoformat() if claim.updated_at else None}


def _invalidator_snapshot(invalidator: ResearchThesisInvalidator) -> dict[str, Any]:
    return {"id": invalidator.id, "description": invalidator.description, "condition_type": invalidator.condition_type, "metric": invalidator.metric, "operator": invalidator.operator, "threshold": invalidator.threshold, "time_window": invalidator.time_window, "severity": invalidator.severity, "monitoring_mode": invalidator.monitoring_mode, "updated_at": invalidator.updated_at.isoformat() if invalidator.updated_at else None}


def _evidence_snapshot(event: ResearchEvidenceEvent) -> dict[str, Any]:
    return {"id": event.id, "content_hash": event.content_hash, "category": event.category, "event_type": event.event_type, "subject": event.subject, "metric": event.metric, "direction": event.direction, "previous_value": event.previous_value, "current_value": event.current_value, "previous_text": event.previous_text, "current_text": event.current_text, "event_date": event.event_date.isoformat() if event.event_date else None, "effective_date": event.effective_date.isoformat() if event.effective_date else None, "published_at": event.published_at.isoformat() if event.published_at else None, "confidence": event.confidence, "materiality": event.materiality, "source_type": event.source_type, "source_provider": event.source_provider, "source_id": event.source_id, "source_url": event.source_url, "source_locator": event.source_locator}


def _availability(event: ResearchEvidenceEvent) -> datetime:
    if event.published_at:
        return event.published_at if event.published_at.tzinfo else event.published_at.replace(tzinfo=timezone.utc)
    for day in (event.effective_date, event.event_date):
        if day:
            return datetime.combine(day, time.min, tzinfo=timezone.utc)
    return event.created_at if event.created_at and event.created_at.tzinfo else (event.created_at or _now()).replace(tzinfo=timezone.utc)


def _expected_direction(value: str | None) -> str | None:
    value = _text(value).lower()
    if any(token in value for token in ("increase", "expand", "accelerate", "rise", "improve", "higher", "tighten")):
        return "increase"
    if any(token in value for token in ("decrease", "compress", "decelerate", "decline", "lower", "reduce", "ease")):
        return "decrease"
    return None


def _event_direction(event: ResearchEvidenceEvent) -> str | None:
    if event.event_type in {"metric_increased", "growth_accelerated", "margin_expanded", "confirmation_strengthened", "institutional_position_increased", "institutional_position_opened", "insider_purchase"}:
        return "increase"
    if event.event_type in {"metric_decreased", "growth_decelerated", "margin_compressed", "confirmation_weakened", "institutional_position_decreased", "institutional_position_closed", "insider_sale"}:
        return "decrease"
    if event.current_value is not None and event.previous_value is not None:
        return "increase" if event.current_value > event.previous_value else "decrease" if event.current_value < event.previous_value else None
    return None


def _tokens(*values: str | None) -> set[str]:
    ignored = {"company", "growth", "increase", "decrease", "expected", "will", "remain", "your", "this", "with", "from", "that", "the", "and", "for"}
    return {token for value in values for token in re.findall(r"[a-z0-9]{3,}", _text(value).lower()) if token not in ignored}


def _event_category_compatible(claim: ResearchThesisClaim, event: ResearchEvidenceEvent) -> bool:
    metric = normalize_metric(claim.metric)
    if metric:
        return True
    claim_tokens = _tokens(claim.claim_type, claim.subject)
    if event.category == "government_contract": return bool(claim_tokens & {"government", "contract", "customer", "revenue"})
    if event.category == "ownership": return bool(claim_tokens & {"insider", "institutional", "ownership", "accumulation"})
    if event.category == "walnut_signal": return bool(claim_tokens & {"confirmation", "alignment", "walnut", "signal"})
    if event.category == "financial": return bool(claim_tokens & {"revenue", "earnings", "margin", "debt", "leverage", "cash", "financial"})
    return bool(claim_tokens & _tokens(event.subject, event.metric, event.summary, event.evidence_excerpt))


def _semantic_candidate(claim: ResearchThesisClaim, event: ResearchEvidenceEvent) -> bool:
    if claim.monitoring_mode not in {"semantic", "hybrid", "event"}:
        return False
    if claim.coverage_level == "manual_review_required" or claim.monitoring_mode == "manual":
        return False
    return bool(_tokens(claim.subject, claim.metric, claim.claim_type) & _tokens(event.subject, event.metric, event.summary, event.evidence_excerpt))


def candidate_claims(db: Session, event: ResearchEvidenceEvent) -> list[tuple[ResearchThesis, ResearchThesisClaim]]:
    """Security/status/time/coverage/category filtering before any semantic comparison."""
    availability = _availability(event)
    rows = db.execute(
        select(ResearchThesis, ResearchThesisClaim)
        .join(ResearchThesisClaim, ResearchThesisClaim.thesis_id == ResearchThesis.id)
        .where(ResearchThesis.security_id == event.security_id, ResearchThesis.status == "active")
        .where(ResearchThesis.started_monitoring_at.is_not(None), ResearchThesis.started_monitoring_at <= availability)
        .where(ResearchThesisClaim.monitoring_mode != "manual", ResearchThesisClaim.coverage_level != "manual_review_required")
        .order_by(ResearchThesis.id, ResearchThesisClaim.id)
    ).all()
    return [(thesis, claim) for thesis, claim in rows if _event_category_compatible(claim, event)]


def _eligible_theses(db: Session, event: ResearchEvidenceEvent) -> list[ResearchThesis]:
    availability = _availability(event)
    return db.execute(
        select(ResearchThesis)
        .where(ResearchThesis.security_id == event.security_id, ResearchThesis.status == "active")
        .where(ResearchThesis.started_monitoring_at.is_not(None), ResearchThesis.started_monitoring_at <= availability)
        .order_by(ResearchThesis.id)
    ).scalars().all()


def deterministic_match(claim: ResearchThesisClaim, event: ResearchEvidenceEvent) -> dict[str, str] | None:
    claim_metric, event_metric = normalize_metric(claim.metric), normalize_metric(event.metric)
    if not claim_metric or claim_metric != event_metric:
        return None
    expected, observed = _expected_direction(claim.expected_direction), _event_direction(event)
    if not expected or not observed:
        return None
    relationship = "supports" if expected == observed else "contradicts"
    return {"relationship": relationship, "relevance": "high", "confidence": "high", "reason": f"{event.headline} directly {'aligns with' if relationship == 'supports' else 'runs against'} the claim's expected {claim_metric.replace('_', ' ')} direction.", "match_method": "deterministic"}


def _matching_hash(claim: ResearchThesisClaim, event: ResearchEvidenceEvent) -> str:
    return _hash({"claim": _claim_snapshot(claim), "evidence_id": event.id, "evidence_hash": event.content_hash, "matching_version": CLAIM_MATCHING_VERSION, "prompt_version": CLAIM_MATCHING_PROMPT_VERSION, "schema_version": CLAIM_MATCHING_SCHEMA_VERSION})


def _existing_pair(db: Session, claim: ResearchThesisClaim, event: ResearchEvidenceEvent) -> bool:
    return bool(db.execute(select(ResearchClaimEvidenceMatch.id).where(ResearchClaimEvidenceMatch.claim_id == claim.id, ResearchClaimEvidenceMatch.evidence_event_id == event.id, ResearchClaimEvidenceMatch.matching_version == CLAIM_MATCHING_VERSION).limit(1)).scalar_one_or_none() or db.execute(select(ResearchClaimMatchCheckpoint.id).where(ResearchClaimMatchCheckpoint.claim_id == claim.id, ResearchClaimMatchCheckpoint.evidence_event_id == event.id, ResearchClaimMatchCheckpoint.matching_version == CLAIM_MATCHING_VERSION).limit(1)).scalar_one_or_none())


def _persist_match(db: Session, *, thesis: ResearchThesis, claim: ResearchThesisClaim, event: ResearchEvidenceEvent, result: dict[str, str], model_version: str | None = None) -> tuple[ResearchClaimEvidenceMatch | None, bool]:
    if result["relationship"] == "unrelated":
        checkpoint = ResearchClaimMatchCheckpoint(id=_id("rcmc"), user_id=thesis.user_id, thesis_id=thesis.id, claim_id=claim.id, evidence_event_id=event.id, outcome="unrelated", matching_hash=_matching_hash(claim, event), matching_version=CLAIM_MATCHING_VERSION)
        try:
            with db.begin_nested(): db.add(checkpoint); db.flush()
        except IntegrityError: return None, False
        return None, True
    if result["relationship"] not in RELATIONSHIPS or result["relevance"] not in LEVELS or result["confidence"] not in LEVELS or result["match_method"] not in METHODS:
        raise ValueError("invalid match result")
    row = ResearchClaimEvidenceMatch(id=_id("rcem"), user_id=thesis.user_id, thesis_id=thesis.id, claim_id=claim.id, evidence_event_id=event.id, relationship=result["relationship"], relevance=result["relevance"], confidence=result["confidence"], reason=_text(result["reason"])[:1200], match_method=result["match_method"], claim_snapshot_json=_json(_claim_snapshot(claim)), evidence_snapshot_json=_json(_evidence_snapshot(event)), model_version=model_version, prompt_version=CLAIM_MATCHING_PROMPT_VERSION if model_version else None, schema_version=CLAIM_MATCHING_SCHEMA_VERSION, matching_version=CLAIM_MATCHING_VERSION, matching_hash=_matching_hash(claim, event))
    try:
        with db.begin_nested(): db.add(row); db.flush()
    except IntegrityError: return None, False
    return row, True


def _semantic_schema() -> dict[str, Any]:
    return {"type": "object", "additionalProperties": False, "required": ["relationship", "relevance", "confidence", "reason"], "properties": {"relationship": {"type": "string", "enum": sorted(SEMANTIC_RELATIONSHIPS)}, "relevance": {"type": "string", "enum": sorted(LEVELS)}, "confidence": {"type": "string", "enum": sorted(LEVELS)}, "reason": {"type": "string", "maxLength": 800}}}


def _response_text(data: dict[str, Any]) -> str:
    if isinstance(data.get("output_text"), str): return data["output_text"]
    for output in data.get("output") or []:
        for content in output.get("content") or []:
            if isinstance(content, dict) and isinstance(content.get("text"), str): return content["text"]
    return ""


def semantic_match(db: Session, *, claim: ResearchThesisClaim, event: ResearchEvidenceEvent, security: Security, request_sender: Callable[[], requests.Response] | None = None) -> dict[str, str]:
    api_key = resolved_setting_value(db, OPENAI_API_KEY) if request_sender is None else None
    if not api_key and request_sender is None: raise HTTPException(status_code=503, detail="Claim matching is temporarily unavailable.")
    context = {"security": {"ticker": security.symbol, "company_name": security.name}, "claim": _claim_snapshot(claim), "evidence": _evidence_snapshot(event)}
    prompt = "\n".join(["Match exactly one private claim to one global evidence event. Do not assess overall thesis health or investment quality.", "Choose unrelated unless the evidence directly supports, contradicts, materially relates to, or is a potential invalidator for this specific claim.", "Use only supplied facts. Return a concise claim-level reason without health language.", _json(context)])
    payload = {"model": CLAIM_MATCHING_MODEL, "input": prompt, "store": False, "max_output_tokens": 700, "text": {"format": {"type": "json_schema", "name": "research_claim_match", "strict": True, "schema": _semantic_schema()}}}
    try:
        response = request_sender() if request_sender else audited_openai_request(feature="research_claim_matching", operation="semantic_match", method="POST", endpoint=RESPONSES_ENDPOINT, payload=payload, model=CLAIM_MATCHING_MODEL, send=lambda: requests.post(RESPONSES_ENDPOINT, headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}, json=payload, timeout=45))
        if response.status_code >= 400: raise RuntimeError("provider_error")
        parsed = json.loads(_response_text(response.json()))
        if not isinstance(parsed, dict) or parsed.get("relationship") not in SEMANTIC_RELATIONSHIPS or parsed.get("relevance") not in LEVELS or parsed.get("confidence") not in LEVELS or not _text(parsed.get("reason")):
            raise ValueError("invalid semantic match output")
        return {"relationship": parsed["relationship"], "relevance": parsed["relevance"], "confidence": parsed["confidence"], "reason": _text(parsed["reason"])[:800], "match_method": "semantic"}
    except requests.Timeout as exc: raise HTTPException(status_code=504, detail="Claim matching timed out.") from exc
    except (requests.RequestException, RuntimeError) as exc: raise HTTPException(status_code=502, detail="Claim matching is temporarily unavailable.") from exc
    except (ValueError, json.JSONDecodeError) as exc: raise HTTPException(status_code=502, detail="Claim matching returned an invalid structure.") from exc


def _invalidator_match(db: Session, thesis: ResearchThesis, invalidator: ResearchThesisInvalidator, event: ResearchEvidenceEvent) -> bool:
    if normalize_metric(invalidator.metric) != normalize_metric(event.metric) or not invalidator.metric:
        return False
    expected = _expected_direction(invalidator.description)
    observed = _event_direction(event)
    if expected and observed and expected != observed:
        return False
    snapshot = _invalidator_snapshot(invalidator)
    row = ResearchInvalidatorEvidenceMatch(id=_id("riem"), user_id=thesis.user_id, thesis_id=thesis.id, invalidator_id=invalidator.id, evidence_event_id=event.id, relationship="potential_invalidator", confidence="high" if expected and observed else "medium", reason=f"{event.headline} is relevant to this invalidation condition; Phase 3 does not evaluate whether it is satisfied.", match_method="deterministic", invalidator_snapshot_json=_json(snapshot), evidence_snapshot_json=_json(_evidence_snapshot(event)), model_version=None, prompt_version=None, schema_version=CLAIM_MATCHING_SCHEMA_VERSION, matching_version=CLAIM_MATCHING_VERSION, matching_hash=_hash({"invalidator": snapshot, "event": event.content_hash, "version": CLAIM_MATCHING_VERSION}))
    try:
        with db.begin_nested(): db.add(row); db.flush()
    except IntegrityError: return False
    return True


def process_event_matches(db: Session, *, event: ResearchEvidenceEvent, request_sender: Callable[[], requests.Response] | None = None) -> dict[str, int]:
    """One bounded event pass. Paused/draft/archived theses and historical evidence are excluded."""
    if not claim_matching_enabled(): return {"candidates": 0, "deterministic": 0, "semantic": 0, "matches": 0, "unrelated": 0, "invalidators": 0, "status": "disabled"}
    security = db.get(Security, event.security_id)
    if not security: return {"candidates": 0, "deterministic": 0, "semantic": 0, "matches": 0, "unrelated": 0, "invalidators": 0}
    result = {"candidates": 0, "deterministic": 0, "semantic": 0, "matches": 0, "unrelated": 0, "invalidators": 0}
    candidates = candidate_claims(db, event); result["candidates"] = len(candidates)
    for thesis, claim in candidates:
        if _existing_pair(db, claim, event): continue
        deterministic = deterministic_match(claim, event)
        if deterministic:
            _row, created = _persist_match(db, thesis=thesis, claim=claim, event=event, result=deterministic)
            result["deterministic"] += int(created); result["matches"] += int(created)
        elif _semantic_candidate(claim, event):
            semantic = semantic_match(db, claim=claim, event=event, security=security, request_sender=request_sender)
            _row, created = _persist_match(db, thesis=thesis, claim=claim, event=event, result=semantic, model_version=CLAIM_MATCHING_MODEL)
            result["semantic"] += 1
            if semantic["relationship"] == "unrelated": result["unrelated"] += int(created)
            else: result["matches"] += int(created)
    # Invalidation is separate from claim health and may be relevant even if no claim matched.
    for thesis in _eligible_theses(db, event):
        invalidators = db.execute(select(ResearchThesisInvalidator).where(ResearchThesisInvalidator.thesis_id == thesis.id)).scalars().all()
        for invalidator in invalidators: result["invalidators"] += int(_invalidator_match(db, thesis, invalidator, event))
    db.commit()
    return result


def run_claim_matching(db: Session, *, evidence_event_id: str | None = None, security_id: int | None = None, limit: int = 100) -> dict[str, int]:
    if not claim_matching_enabled(): return {"events_seen": 0, "events_processed": 0, "matches": 0, "status": "disabled"}
    stmt = select(ResearchEvidenceEvent)
    if evidence_event_id: stmt = stmt.where(ResearchEvidenceEvent.id == evidence_event_id)
    if security_id: stmt = stmt.where(ResearchEvidenceEvent.security_id == security_id)
    events = db.execute(stmt.order_by(ResearchEvidenceEvent.created_at.asc(), ResearchEvidenceEvent.id.asc()).limit(max(1, min(limit, 500)))).scalars().all()
    result = {"events_seen": len(events), "events_processed": 0, "candidates": 0, "deterministic": 0, "semantic": 0, "matches": 0, "unrelated": 0, "invalidators": 0}
    for event in events:
        outcome = process_event_matches(db, event=event)
        result["events_processed"] += 1
        for key in ("candidates", "deterministic", "semantic", "matches", "unrelated", "invalidators"): result[key] += int(outcome.get(key, 0))
    return result


def _serialize_match(row: ResearchClaimEvidenceMatch) -> dict[str, Any]:
    return {"id": row.id, "thesis_id": row.thesis_id, "claim_id": row.claim_id, "evidence_event_id": row.evidence_event_id, "relationship": row.relationship, "relevance": row.relevance, "confidence": row.confidence, "reason": row.reason, "match_method": row.match_method, "claim_snapshot": json.loads(row.claim_snapshot_json), "evidence_snapshot": json.loads(row.evidence_snapshot_json), "model_version": row.model_version, "prompt_version": row.prompt_version, "schema_version": row.schema_version, "matching_version": row.matching_version, "created_at": row.created_at.isoformat() if row.created_at else None}


def query_matches(db: Session, *, user: UserAccount, thesis_id: str, claim_id: str | None = None, relationship: str | None = None, since: datetime | None = None, limit: int = 100) -> list[dict[str, Any]]:
    # Ownership scope is in the SQL predicate; hidden links cannot bypass it.
    stmt = select(ResearchClaimEvidenceMatch).where(ResearchClaimEvidenceMatch.user_id == user.id, ResearchClaimEvidenceMatch.thesis_id == thesis_id)
    if claim_id: stmt = stmt.where(ResearchClaimEvidenceMatch.claim_id == claim_id)
    if relationship:
        if relationship not in RELATIONSHIPS: raise HTTPException(status_code=422, detail="Unsupported match relationship.")
        stmt = stmt.where(ResearchClaimEvidenceMatch.relationship == relationship)
    if since: stmt = stmt.where(ResearchClaimEvidenceMatch.created_at >= since)
    rows = db.execute(stmt.order_by(ResearchClaimEvidenceMatch.created_at.desc(), ResearchClaimEvidenceMatch.id.desc()).limit(max(1, min(limit, 500)))).scalars().all()
    return [_serialize_match(row) for row in rows]


def query_invalidator_matches(db: Session, *, user: UserAccount, thesis_id: str, since: datetime | None = None, limit: int = 100) -> list[dict[str, Any]]:
    stmt = select(ResearchInvalidatorEvidenceMatch).where(ResearchInvalidatorEvidenceMatch.user_id == user.id, ResearchInvalidatorEvidenceMatch.thesis_id == thesis_id)
    if since: stmt = stmt.where(ResearchInvalidatorEvidenceMatch.created_at >= since)
    rows = db.execute(stmt.order_by(ResearchInvalidatorEvidenceMatch.created_at.desc(), ResearchInvalidatorEvidenceMatch.id.desc()).limit(max(1, min(limit, 500)))).scalars().all()
    return [{"id": row.id, "thesis_id": row.thesis_id, "invalidator_id": row.invalidator_id, "evidence_event_id": row.evidence_event_id, "relationship": row.relationship, "confidence": row.confidence, "reason": row.reason, "match_method": row.match_method, "invalidator_snapshot": json.loads(row.invalidator_snapshot_json), "evidence_snapshot": json.loads(row.evidence_snapshot_json), "created_at": row.created_at.isoformat() if row.created_at else None} for row in rows]
