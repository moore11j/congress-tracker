"""Phase 2 company-level Evidence Event Engine.

This module deliberately has no dependency on ResearchThesis or user-owned data.
It turns one retained source record/document into reusable, idempotent evidence
events; Claim Matching begins in Phase 3.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import uuid
from datetime import date, datetime, timezone
from typing import Any, Callable

import requests
from fastapi import HTTPException
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.models import (
    ConfirmationScoreSnapshot,
    FundamentalsSnapshot,
    GovernmentContract,
    GovernmentContractAction,
    InsiderTransactionNormalized,
    InstitutionalPositionChange,
    ResearchEvidenceEvent,
    ResearchSourceDocument,
    Security,
)
from app.services.ai_marketing import OPENAI_API_KEY, resolved_setting_value
from app.services.openai_request_audit import audited_openai_request

logger = logging.getLogger(__name__)

EVIDENCE_EXTRACTION_PROMPT_VERSION = "evidence_extraction_v1"
EVIDENCE_SCHEMA_VERSION = "research_evidence_schema_v1"
EVIDENCE_PROCESSING_VERSION = "research_evidence_processing_v1"
EVIDENCE_MODEL = os.getenv("RESEARCH_EVIDENCE_MODEL", "gpt-5.4-mini")
RESPONSES_ENDPOINT = "https://api.openai.com/v1/responses"

CATEGORIES = {"financial", "government_contract", "ownership", "walnut_signal", "other_material_company_event"}
EVENT_TYPES = {
    "metric_increased", "metric_decreased", "growth_accelerated", "growth_decelerated", "margin_expanded", "margin_compressed",
    "contract_awarded", "contract_modified", "insider_purchase", "insider_sale",
    "institutional_position_increased", "institutional_position_decreased", "institutional_position_opened", "institutional_position_closed",
    "confirmation_strengthened", "confirmation_weakened", "confirmation_direction_changed", "cross_source_alignment_changed",
}
DIRECTIONS = {"positive", "negative", "neutral", "mixed", "unknown"}
CONFIDENCE = {"high", "medium", "low"}
MATERIALITY = {"low", "medium", "high"}
DOCUMENT_STATUSES = {"pending", "processing", "processed", "failed", "skipped"}


def research_evidence_engine_enabled() -> bool:
    return os.getenv("RESEARCH_EVIDENCE_ENGINE_ENABLED", "true").strip().lower() not in {"0", "false", "off", "no"}


def ensure_evidence_engine_enabled(*, is_admin: bool = False) -> None:
    if research_evidence_engine_enabled() or is_admin:
        return
    raise HTTPException(status_code=404, detail="Research Evidence Engine is not enabled.")


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


def canonical_json(value: Any) -> str:
    """Canonical hash input: stable key ordering, compact separators, no ingest timestamps."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str)


def evidence_hash(value: dict[str, Any]) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def source_content_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _clean_text(value: Any, *, field: str, required: bool = False, limit: int = 1600) -> str | None:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    if required and not text:
        raise ValueError(f"{field} is required")
    if len(text) > limit:
        raise ValueError(f"{field} is too long")
    return text or None


def _choice(value: Any, allowed: set[str], *, field: str) -> str:
    cleaned = str(value or "").strip().lower()
    if cleaned not in allowed:
        raise ValueError(f"unsupported {field}")
    return cleaned


def _date(value: Any) -> date | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")).date()


def _datetime(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def _float(value: Any, *, field: str) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid {field}") from exc


def _security_for_symbol(db: Session, symbol: str | None) -> Security | None:
    normalized = str(symbol or "").strip().upper()
    if not normalized:
        return None
    return db.execute(select(Security).where(func.upper(Security.symbol) == normalized)).scalar_one_or_none()


def _iso(value: date | datetime | None) -> str | None:
    return value.isoformat() if value else None


def _event_identity(value: dict[str, Any]) -> dict[str, Any]:
    # Intentionally excludes created/updated/ingestion timestamps.
    return {
        "security_id": value["security_id"], "source_provider": value["source_provider"], "source_id": value["source_id"],
        "source_document_id": value.get("source_document_id"), "event_type": value["event_type"], "category": value["category"],
        "subject": value.get("subject"), "metric": value.get("metric"), "event_date": _iso(value.get("event_date")),
        "effective_date": _iso(value.get("effective_date")), "previous_value": value.get("previous_value"),
        "current_value": value.get("current_value"), "previous_text": value.get("previous_text"),
        "current_text": value.get("current_text"), "source_locator": value.get("source_locator"),
    }


def validate_event(value: dict[str, Any]) -> dict[str, Any]:
    """Server-side validation for both deterministic and semantic event candidates."""
    if not isinstance(value, dict):
        raise ValueError("event must be an object")
    security_id = value.get("security_id")
    if not isinstance(security_id, int) or security_id <= 0:
        raise ValueError("security_id is required")
    event_type = _choice(value.get("event_type"), EVENT_TYPES, field="event type")
    category = _choice(value.get("category"), CATEGORIES, field="category")
    source_type = _clean_text(value.get("source_type"), field="source type", required=True, limit=120)
    source_provider = _clean_text(value.get("source_provider"), field="source provider", required=True, limit=120)
    source_id = _clean_text(value.get("source_id"), field="source id", required=True, limit=240)
    headline = _clean_text(value.get("headline"), field="headline", required=True, limit=320)
    summary = _clean_text(value.get("summary"), field="summary", required=True, limit=1200)
    result = {
        "security_id": security_id, "event_type": event_type, "category": category,
        "subject": _clean_text(value.get("subject"), field="subject", limit=320),
        "metric": _clean_text(value.get("metric"), field="metric", limit=160),
        "direction": _choice(value.get("direction"), DIRECTIONS, field="direction"),
        "magnitude": _float(value.get("magnitude"), field="magnitude"),
        "unit": _clean_text(value.get("unit"), field="unit", limit=80),
        "period": _clean_text(value.get("period"), field="period", limit=160),
        "previous_value": _float(value.get("previous_value"), field="previous value"),
        "current_value": _float(value.get("current_value"), field="current value"),
        "expected_value": _float(value.get("expected_value"), field="expected value"),
        "actual_value": _float(value.get("actual_value"), field="actual value"),
        "previous_text": _clean_text(value.get("previous_text"), field="previous text", limit=400),
        "current_text": _clean_text(value.get("current_text"), field="current text", limit=400),
        "event_date": _date(value.get("event_date")), "effective_date": _date(value.get("effective_date")),
        "published_at": _datetime(value.get("published_at")), "source_type": source_type, "source_provider": source_provider,
        "source_id": source_id, "source_url": _clean_text(value.get("source_url"), field="source url", limit=2000),
        "source_document_id": _clean_text(value.get("source_document_id"), field="source document id", limit=240),
        "source_locator": _clean_text(value.get("source_locator"), field="source locator", limit=320),
        "headline": headline, "summary": summary,
        "evidence_excerpt": _clean_text(value.get("evidence_excerpt"), field="evidence excerpt", limit=800),
        "confidence": _choice(value.get("confidence"), CONFIDENCE, field="confidence"),
        "materiality": _choice(value.get("materiality"), MATERIALITY, field="materiality"),
        "extraction_method": _clean_text(value.get("extraction_method"), field="extraction method", required=True, limit=80),
        "model_version": _clean_text(value.get("model_version"), field="model version", limit=160),
        "prompt_version": _clean_text(value.get("prompt_version"), field="prompt version", limit=160),
        "schema_version": _clean_text(value.get("schema_version") or EVIDENCE_SCHEMA_VERSION, field="schema version", required=True, limit=160),
        "processing_version": _clean_text(value.get("processing_version") or EVIDENCE_PROCESSING_VERSION, field="processing version", required=True, limit=160),
        "related_event_id": _clean_text(value.get("related_event_id"), field="related event id", limit=240),
    }
    if result["category"] == "financial" and not result["metric"]:
        raise ValueError("financial events require a metric")
    if result["extraction_method"] == "semantic" and not (result["source_document_id"] and result["evidence_excerpt"]):
        raise ValueError("semantic events require source document linkage and evidence excerpt")
    return result


def persist_event(db: Session, value: dict[str, Any]) -> tuple[ResearchEvidenceEvent, bool]:
    event = validate_event(value)
    content_hash = evidence_hash(_event_identity(event))
    existing = db.execute(select(ResearchEvidenceEvent).where(ResearchEvidenceEvent.content_hash == content_hash)).scalar_one_or_none()
    if existing:
        return existing, False
    row = ResearchEvidenceEvent(id=_id("ree"), content_hash=content_hash, **event)
    try:
        # A duplicate race must not roll back other source events in this batch.
        with db.begin_nested():
            db.add(row)
            db.flush()
    except IntegrityError:
        db.rollback()
        existing = db.execute(select(ResearchEvidenceEvent).where(ResearchEvidenceEvent.content_hash == content_hash)).scalar_one()
        return existing, False
    return row, True


def _numeric_event(*, security: Security, source_type: str, source_provider: str, source_id: str, event_type: str, metric: str, previous: float, current: float, event_date: date | None, unit: str, direction: str, materiality: str = "medium", source_url: str | None = None, source_locator: str | None = None, subject: str | None = None) -> dict[str, Any]:
    return {
        "security_id": security.id, "event_type": event_type, "category": "financial", "subject": subject or security.name,
        "metric": metric, "direction": direction, "magnitude": current - previous, "unit": unit,
        "previous_value": previous, "current_value": current, "event_date": event_date, "effective_date": event_date,
        "source_type": source_type, "source_provider": source_provider, "source_id": source_id, "source_url": source_url,
        "source_locator": source_locator, "headline": f"{metric.replace('_', ' ').title()} {'increased' if current > previous else 'decreased'}",
        "summary": f"{metric.replace('_', ' ')} changed from {previous:g} to {current:g}.", "confidence": "high", "materiality": materiality,
        "extraction_method": "deterministic", "schema_version": EVIDENCE_SCHEMA_VERSION, "processing_version": EVIDENCE_PROCESSING_VERSION,
    }


def events_from_fundamentals_snapshot(db: Session, snapshot: FundamentalsSnapshot) -> int:
    """Emit only comparative metric changes; an unchanged refresh produces zero events."""
    if snapshot.status != "ok":
        return 0
    security = _security_for_symbol(db, snapshot.symbol)
    if not security:
        return 0
    previous = db.execute(
        select(FundamentalsSnapshot)
        .where(func.upper(FundamentalsSnapshot.symbol) == (snapshot.symbol or "").upper(), FundamentalsSnapshot.provider == snapshot.provider, FundamentalsSnapshot.status == "ok")
        .where((FundamentalsSnapshot.snapshot_date < snapshot.snapshot_date) | ((FundamentalsSnapshot.snapshot_date == snapshot.snapshot_date) & (FundamentalsSnapshot.observed_at < snapshot.observed_at)))
        .order_by(FundamentalsSnapshot.snapshot_date.desc(), FundamentalsSnapshot.observed_at.desc(), FundamentalsSnapshot.id.desc()).limit(1)
    ).scalar_one_or_none()
    if previous is None:
        return 0
    fields = {
        "revenue_growth": ("growth_accelerated", "growth_decelerated", "percentage_points"),
        "eps_growth": ("growth_accelerated", "growth_decelerated", "percentage_points"),
        "gross_margin": ("margin_expanded", "margin_compressed", "percentage_points"),
        "operating_margin": ("margin_expanded", "margin_compressed", "percentage_points"),
        "free_cash_flow": ("metric_increased", "metric_decreased", "currency"),
        "net_debt_to_ebitda": ("metric_decreased", "metric_increased", "ratio"),
    }
    written = 0
    for metric, (up_type, down_type, unit) in fields.items():
        old, new = getattr(previous, metric, None), getattr(snapshot, metric, None)
        if old is None or new is None or float(old) == float(new):
            continue
        is_up = float(new) > float(old)
        row, created = persist_event(db, _numeric_event(
            security=security, source_type="fundamentals_snapshot", source_provider=snapshot.provider, source_id=str(snapshot.id),
            event_type=up_type if is_up else down_type, metric=metric, previous=float(old), current=float(new),
            event_date=snapshot.period_date or snapshot.snapshot_date, unit=unit,
            direction="positive" if (is_up and metric != "net_debt_to_ebitda") or (not is_up and metric == "net_debt_to_ebitda") else "negative",
            source_locator=f"fundamentals_snapshots:{snapshot.id}",
        ))
        written += int(created)
    return written


def events_from_contract(db: Session, contract: GovernmentContract) -> int:
    security = _security_for_symbol(db, contract.symbol)
    if not security:
        return 0
    _, created = persist_event(db, {
        "security_id": security.id, "event_type": "contract_awarded", "category": "government_contract", "subject": contract.recipient_name or security.name,
        "metric": "government_contract_award", "direction": "positive", "magnitude": contract.award_amount, "unit": "currency",
        "current_value": contract.award_amount, "event_date": contract.award_date, "effective_date": contract.award_date,
        "source_type": "government_contract", "source_provider": contract.source, "source_id": contract.award_id or contract.dedupe_key or str(contract.id),
        "source_url": contract.source_url, "source_locator": f"government_contracts:{contract.id}",
        "headline": "Government contract awarded", "summary": f"Government contract award recorded for {contract.award_amount:g}.",
        # The contract dataset has no established materiality methodology; retain the fact without inventing a dollar threshold.
        "confidence": "high", "materiality": "medium", "extraction_method": "deterministic",
        "schema_version": EVIDENCE_SCHEMA_VERSION, "processing_version": EVIDENCE_PROCESSING_VERSION,
    })
    return int(created)


def events_from_contract_action(db: Session, action: GovernmentContractAction) -> int:
    security = _security_for_symbol(db, action.symbol)
    if not security:
        return 0
    _, created = persist_event(db, {
        "security_id": security.id, "event_type": "contract_modified", "category": "government_contract", "subject": action.recipient_name or action.company_name or security.name,
        "metric": "government_contract_obligation", "direction": "unknown", "magnitude": action.obligated_amount, "unit": "currency",
        "current_value": action.obligated_amount, "event_date": action.action_date, "effective_date": action.action_date,
        "source_type": "government_contract_action", "source_provider": action.source, "source_id": action.dedupe_key,
        "source_url": action.source_url, "source_locator": f"government_contract_actions:{action.id}",
        "headline": "Government contract modified", "summary": f"Government contract action recorded for {action.obligated_amount:g}.",
        "confidence": "high", "materiality": "medium", "extraction_method": "deterministic",
        "schema_version": EVIDENCE_SCHEMA_VERSION, "processing_version": EVIDENCE_PROCESSING_VERSION,
    })
    return int(created)


def events_from_insider_transaction(db: Session, transaction: InsiderTransactionNormalized) -> int:
    if transaction.is_duplicate or transaction.is_derivative:
        return 0
    code = (transaction.transaction_code or transaction.transaction_type_normalized or "").strip().upper()
    if code not in {"P", "S", "PURCHASE", "SALE"}:
        return 0
    security = _security_for_symbol(db, transaction.ticker_normalized)
    if not security:
        return 0
    is_purchase = code in {"P", "PURCHASE"}
    _, created = persist_event(db, {
        "security_id": security.id, "event_type": "insider_purchase" if is_purchase else "insider_sale", "category": "ownership",
        "subject": transaction.reporting_owner_name or security.name, "metric": "insider_transaction_value", "direction": "positive" if is_purchase else "unknown",
        "magnitude": transaction.value, "unit": "currency", "current_value": transaction.value, "event_date": transaction.transaction_date or transaction.filing_date,
        "effective_date": transaction.transaction_date, "source_type": "sec_form4", "source_provider": "sec", "source_id": transaction.normalized_hash,
        "source_url": None, "source_locator": f"form4:{transaction.accession_number}",
        "headline": "Insider purchase" if is_purchase else "Insider sale", "summary": f"Form 4 {('purchase' if is_purchase else 'sale')} by {transaction.reporting_owner_name or 'reported insider'}.",
        "confidence": "high", "materiality": "medium", "extraction_method": "deterministic",
        "schema_version": EVIDENCE_SCHEMA_VERSION, "processing_version": EVIDENCE_PROCESSING_VERSION,
    })
    return int(created)


def events_from_institutional_change(db: Session, change: InstitutionalPositionChange) -> int:
    if not change.is_material:
        return 0
    security = _security_for_symbol(db, change.normalized_symbol)
    if not security:
        return 0
    normalized = (change.change_type or "").strip().lower()
    mapping = {"new": "institutional_position_opened", "opened": "institutional_position_opened", "increase": "institutional_position_increased", "increased": "institutional_position_increased", "reduce": "institutional_position_decreased", "reduced": "institutional_position_decreased", "decrease": "institutional_position_decreased", "decreased": "institutional_position_decreased", "exit": "institutional_position_closed", "closed": "institutional_position_closed"}
    event_type = mapping.get(normalized)
    if not event_type:
        return 0
    direction = "positive" if event_type in {"institutional_position_opened", "institutional_position_increased"} else "negative"
    _, created = persist_event(db, {
        "security_id": security.id, "event_type": event_type, "category": "ownership", "subject": change.holder_name or "Institutional holder",
        "metric": "institutional_shares", "direction": direction, "magnitude": change.shares_delta, "unit": "shares", "previous_value": change.prev_shares,
        "current_value": change.curr_shares, "event_date": change.filing_date, "effective_date": change.filing_date,
        "source_type": "institutional_filing", "source_provider": "sec_13f", "source_id": str(change.id),
        "source_locator": f"institutional_position_changes:{change.id}", "headline": "Institutional position change",
        "summary": f"Material institutional position {normalized} reported for the filing period.", "confidence": "high", "materiality": "high",
        "extraction_method": "deterministic", "schema_version": EVIDENCE_SCHEMA_VERSION, "processing_version": EVIDENCE_PROCESSING_VERSION,
    })
    return int(created)


def _json_list(value: str | None) -> list[str]:
    try:
        parsed = json.loads(value or "[]")
        return sorted(str(x) for x in parsed) if isinstance(parsed, list) else []
    except (TypeError, ValueError):
        return []


def events_from_confirmation_snapshot(db: Session, snapshot: ConfirmationScoreSnapshot) -> int:
    previous = db.execute(
        select(ConfirmationScoreSnapshot)
        .where(ConfirmationScoreSnapshot.security_id == snapshot.security_id, ConfirmationScoreSnapshot.calculation_type == snapshot.calculation_type)
        .where((ConfirmationScoreSnapshot.calculated_at < snapshot.calculated_at) | ((ConfirmationScoreSnapshot.calculated_at == snapshot.calculated_at) & (ConfirmationScoreSnapshot.id < snapshot.id)))
        .order_by(ConfirmationScoreSnapshot.calculated_at.desc(), ConfirmationScoreSnapshot.id.desc()).limit(1)
    ).scalar_one_or_none()
    if previous is None:
        return 0
    security = db.get(Security, snapshot.security_id)
    if not security:
        return 0
    common = {"security_id": security.id, "category": "walnut_signal", "subject": security.name, "metric": "confirmation_score", "unit": "score",
              "previous_value": float(previous.score), "current_value": float(snapshot.score), "event_date": snapshot.market_date, "effective_date": snapshot.market_date,
              "published_at": snapshot.calculated_at, "source_type": "walnut_confirmation", "source_provider": "walnut", "source_id": str(snapshot.id),
              "source_locator": f"confirmation_score_snapshots:{snapshot.id}", "confidence": "high", "materiality": "medium", "extraction_method": "deterministic",
              "schema_version": EVIDENCE_SCHEMA_VERSION, "processing_version": EVIDENCE_PROCESSING_VERSION}
    written = 0
    if previous.direction != snapshot.direction:
        _, created = persist_event(db, {**common, "event_type": "confirmation_direction_changed", "direction": "mixed", "magnitude": float(snapshot.score - previous.score), "headline": "Confirmation direction changed", "summary": f"Walnut Confirmation direction changed from {previous.direction} to {snapshot.direction}."})
        written += int(created)
    elif previous.strength != snapshot.strength:
        upward = snapshot.score > previous.score
        _, created = persist_event(db, {**common, "event_type": "confirmation_strengthened" if upward else "confirmation_weakened", "direction": "positive" if upward else "negative", "magnitude": float(snapshot.score - previous.score), "headline": "Confirmation strength changed", "summary": f"Walnut Confirmation score changed from {previous.score} to {snapshot.score} with a strength-band change."})
        written += int(created)
    if _json_list(previous.active_sources_json) != _json_list(snapshot.active_sources_json):
        _, created = persist_event(db, {**common, "event_type": "cross_source_alignment_changed", "direction": "neutral", "magnitude": None, "headline": "Confirmation source set changed", "summary": "Walnut Confirmation active source set changed."})
        written += int(created)
    return written


def run_deterministic_adapters(db: Session, *, security_id: int | None = None, limit: int = 200) -> dict[str, int]:
    """Explicit worker/admin entry point. Never invoked by a ticker or Research Memory page render."""
    if not research_evidence_engine_enabled():
        return {"fundamentals": 0, "government_contracts": 0, "government_contract_actions": 0, "insiders": 0, "institutions": 0, "confirmation": 0, "events_written": 0, "status": "disabled"}
    max_rows = max(1, min(int(limit), 1000))
    result = {"fundamentals": 0, "government_contracts": 0, "government_contract_actions": 0, "insiders": 0, "institutions": 0, "confirmation": 0}
    security = db.get(Security, security_id) if security_id else None
    symbol = security.symbol if security else None
    snapshot_stmt = select(FundamentalsSnapshot)
    if symbol:
        snapshot_stmt = snapshot_stmt.where(func.upper(FundamentalsSnapshot.symbol) == symbol.upper())
    snapshots = db.execute(snapshot_stmt.order_by(FundamentalsSnapshot.snapshot_date.desc(), FundamentalsSnapshot.observed_at.desc()).limit(max_rows)).scalars().all()
    for row in snapshots[:max_rows]: result["fundamentals"] += events_from_fundamentals_snapshot(db, row)
    contracts = db.execute((select(GovernmentContract).where(func.upper(GovernmentContract.symbol) == symbol.upper()) if symbol else select(GovernmentContract)).order_by(GovernmentContract.award_date.desc()).limit(max_rows)).scalars().all()
    for row in contracts: result["government_contracts"] += events_from_contract(db, row)
    actions = db.execute((select(GovernmentContractAction).where(func.upper(GovernmentContractAction.symbol) == symbol.upper()) if symbol else select(GovernmentContractAction)).order_by(GovernmentContractAction.action_date.desc()).limit(max_rows)).scalars().all()
    for row in actions: result["government_contract_actions"] += events_from_contract_action(db, row)
    insiders = db.execute((select(InsiderTransactionNormalized).where(func.upper(InsiderTransactionNormalized.ticker_normalized) == symbol.upper()) if symbol else select(InsiderTransactionNormalized)).order_by(InsiderTransactionNormalized.transaction_date.desc()).limit(max_rows)).scalars().all()
    for row in insiders: result["insiders"] += events_from_insider_transaction(db, row)
    institutions = db.execute((select(InstitutionalPositionChange).where(func.upper(InstitutionalPositionChange.normalized_symbol) == symbol.upper()) if symbol else select(InstitutionalPositionChange)).order_by(InstitutionalPositionChange.filing_date.desc()).limit(max_rows)).scalars().all()
    for row in institutions: result["institutions"] += events_from_institutional_change(db, row)
    scores = db.execute((select(ConfirmationScoreSnapshot).where(ConfirmationScoreSnapshot.security_id == security_id) if security_id else select(ConfirmationScoreSnapshot)).order_by(ConfirmationScoreSnapshot.calculated_at.desc()).limit(max_rows)).scalars().all()
    for row in scores: result["confirmation"] += events_from_confirmation_snapshot(db, row)
    db.commit()
    result["events_written"] = sum(result.values())
    return result


def upsert_source_document(db: Session, *, security_id: int, document_type: str, source_provider: str, external_id: str, content: str, title: str | None = None, source_url: str | None = None, published_at: datetime | None = None, period_end: date | None = None, filing_type: str | None = None) -> tuple[ResearchSourceDocument, bool]:
    text_value = _clean_text(content, field="source text", required=True, limit=2_000_000) or ""
    content_hash = source_content_hash(text_value)
    document = db.execute(select(ResearchSourceDocument).where(ResearchSourceDocument.source_provider == source_provider, ResearchSourceDocument.external_id == external_id)).scalar_one_or_none()
    changed = document is None or document.content_hash != content_hash
    if document is None:
        document = ResearchSourceDocument(id=_id("rsd"), security_id=security_id, document_type=_clean_text(document_type, field="document type", required=True, limit=120) or "", source_provider=_clean_text(source_provider, field="source provider", required=True, limit=120) or "", external_id=_clean_text(external_id, field="external id", required=True, limit=240) or "", title=_clean_text(title, field="title", limit=500), source_url=_clean_text(source_url, field="source url", limit=2000), published_at=published_at, period_end=period_end, filing_type=_clean_text(filing_type, field="filing type", limit=80), content_hash=content_hash, processing_status="pending", processing_version=EVIDENCE_PROCESSING_VERSION)
        db.add(document)
    elif changed:
        document.security_id, document.document_type, document.title, document.source_url = security_id, document_type, title, source_url
        document.published_at, document.period_end, document.filing_type = published_at, period_end, filing_type
        document.content_hash, document.processing_status, document.failure_reason = content_hash, "pending", None
    db.flush()
    return document, changed


def _semantic_schema() -> dict[str, Any]:
    # Deliberately text-only for source-derived facts: model cannot invent numeric values/dates.
    event = {"type": "object", "additionalProperties": False, "required": ["category", "event_type", "subject", "metric", "direction", "previous_text", "current_text", "headline", "summary", "evidence_excerpt", "confidence", "materiality"], "properties": {
        "category": {"type": "string", "enum": sorted(CATEGORIES)}, "event_type": {"type": "string", "enum": sorted(EVENT_TYPES)}, "subject": {"type": ["string", "null"]}, "metric": {"type": ["string", "null"]}, "direction": {"type": "string", "enum": sorted(DIRECTIONS)}, "previous_text": {"type": ["string", "null"]}, "current_text": {"type": ["string", "null"]}, "headline": {"type": "string"}, "summary": {"type": "string"}, "evidence_excerpt": {"type": "string"}, "confidence": {"type": "string", "enum": sorted(CONFIDENCE)}, "materiality": {"type": "string", "enum": sorted(MATERIALITY)}}}
    return {"type": "object", "additionalProperties": False, "required": ["events"], "properties": {"events": {"type": "array", "items": event}}}


def _response_text(data: dict[str, Any]) -> str:
    if isinstance(data.get("output_text"), str):
        return data["output_text"]
    for output in data.get("output") or []:
        for content in output.get("content") or []:
            if isinstance(content, dict) and isinstance(content.get("text"), str):
                return content["text"]
    return ""


def parse_semantic_events(*, parsed: Any, document: ResearchSourceDocument, source_text: str) -> list[dict[str, Any]]:
    if not isinstance(parsed, dict) or not isinstance(parsed.get("events"), list):
        raise ValueError("invalid evidence extraction response")
    normalized_source = re.sub(r"\s+", " ", source_text).strip()
    results: list[dict[str, Any]] = []
    for raw in parsed["events"]:
        if not isinstance(raw, dict):
            raise ValueError("invalid evidence event")
        excerpt = _clean_text(raw.get("evidence_excerpt"), field="evidence excerpt", required=True, limit=800) or ""
        if excerpt not in normalized_source:
            raise ValueError("evidence excerpt is not present in source text")
        # Numeric/date fields are not in the model schema and remain null by construction.
        results.append(validate_event({
            **raw, "security_id": document.security_id, "event_date": None, "effective_date": None, "published_at": document.published_at,
            "source_type": document.document_type, "source_provider": document.source_provider, "source_id": document.external_id,
            "source_url": document.source_url, "source_document_id": document.id, "source_locator": "document_text",
            "extraction_method": "semantic", "model_version": EVIDENCE_MODEL, "prompt_version": EVIDENCE_EXTRACTION_PROMPT_VERSION,
            "schema_version": EVIDENCE_SCHEMA_VERSION, "processing_version": EVIDENCE_PROCESSING_VERSION,
        }))
    return results


def extract_document_events(db: Session, *, document: ResearchSourceDocument, source_text: str, request_sender: Callable[[], requests.Response] | None = None) -> dict[str, Any]:
    """Process one changed document once; caller owns source ingestion and never passes user data."""
    text_value = _clean_text(source_text, field="source text", required=True, limit=2_000_000) or ""
    if source_content_hash(text_value) != document.content_hash:
        raise ValueError("source text does not match source document content hash")
    if document.processing_status == "processed" and document.processing_version == EVIDENCE_PROCESSING_VERSION:
        return {"status": "reused", "events_written": 0, "document_id": document.id}
    api_key = resolved_setting_value(db, OPENAI_API_KEY) if request_sender is None else None
    if not api_key and request_sender is None:
        document.processing_status, document.failure_reason = "failed", "provider_configuration_missing"
        db.commit()
        raise HTTPException(status_code=503, detail="Evidence extraction is temporarily unavailable.")
    document.processing_status, document.failure_reason = "processing", None
    db.flush()
    prompt = "\n".join([
        "Extract only discrete factual company developments from this source. Do not assess any thesis.",
        "Return no event for generic promotion or unsupported inference. Quote a short exact evidence_excerpt from the source for every event.",
        "Do not return numerical values or dates: keep them absent; do not invent facts, source locations, customers, or prior baselines.",
        f"DOCUMENT TYPE: {document.document_type}; TITLE: {document.title or ''}; SOURCE: {document.source_provider}",
        f"SOURCE TEXT:\n{text_value}",
    ])
    payload = {"model": EVIDENCE_MODEL, "input": prompt, "store": False, "max_output_tokens": 3000, "text": {"format": {"type": "json_schema", "name": "research_evidence_events", "strict": True, "schema": _semantic_schema()}}}
    try:
        response = request_sender() if request_sender else audited_openai_request(feature="research_evidence", operation="document_extract", method="POST", endpoint=RESPONSES_ENDPOINT, payload=payload, model=EVIDENCE_MODEL, send=lambda: requests.post(RESPONSES_ENDPOINT, headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}, json=payload, timeout=60))
        if response.status_code >= 400:
            raise RuntimeError("provider_error")
        events = parse_semantic_events(parsed=json.loads(_response_text(response.json())), document=document, source_text=text_value)
        written = sum(int(persist_event(db, row)[1]) for row in events)
        document.processing_status, document.processing_version, document.last_processed_at, document.failure_reason = "processed", EVIDENCE_PROCESSING_VERSION, _now(), None
        db.commit()
        return {"status": "processed", "events_written": written, "events_seen": len(events), "document_id": document.id}
    except requests.Timeout as exc:
        document.processing_status, document.failure_reason = "failed", "timeout"; db.commit()
        raise HTTPException(status_code=504, detail="Evidence extraction timed out.") from exc
    except (requests.RequestException, RuntimeError) as exc:
        document.processing_status, document.failure_reason = "failed", "provider_error"; db.commit()
        raise HTTPException(status_code=502, detail="Evidence extraction is temporarily unavailable.") from exc
    except (ValueError, json.JSONDecodeError) as exc:
        document.processing_status, document.failure_reason = "failed", "validation_error"; db.commit()
        logger.warning("research_evidence_extraction_validation_failed document_id=%s error=%s", document.id, type(exc).__name__)
        raise HTTPException(status_code=502, detail="Evidence extraction returned an invalid structure.") from exc


def serialize_event(row: ResearchEvidenceEvent) -> dict[str, Any]:
    return {name: (_iso(getattr(row, name)) if name in {"event_date", "effective_date", "published_at", "created_at", "updated_at"} else getattr(row, name)) for name in (
        "id", "security_id", "event_type", "category", "subject", "metric", "direction", "magnitude", "unit", "period", "previous_value", "current_value", "expected_value", "actual_value", "previous_text", "current_text", "event_date", "effective_date", "published_at", "source_type", "source_provider", "source_id", "source_url", "source_document_id", "source_locator", "headline", "summary", "evidence_excerpt", "confidence", "materiality", "extraction_method", "model_version", "prompt_version", "schema_version", "processing_version", "created_at", "updated_at")}


def query_events(db: Session, *, security_id: int, since: datetime | None = None, start: date | None = None, end: date | None = None, category: str | None = None, event_type: str | None = None, metric: str | None = None, source_type: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
    stmt = select(ResearchEvidenceEvent).where(ResearchEvidenceEvent.security_id == security_id)
    if since: stmt = stmt.where(ResearchEvidenceEvent.created_at >= since)
    if start: stmt = stmt.where(ResearchEvidenceEvent.event_date >= start)
    if end: stmt = stmt.where(ResearchEvidenceEvent.event_date <= end)
    if category: stmt = stmt.where(ResearchEvidenceEvent.category == _choice(category, CATEGORIES, field="category"))
    if event_type: stmt = stmt.where(ResearchEvidenceEvent.event_type == _choice(event_type, EVENT_TYPES, field="event type"))
    if metric: stmt = stmt.where(ResearchEvidenceEvent.metric == metric)
    if source_type: stmt = stmt.where(ResearchEvidenceEvent.source_type == source_type)
    rows = db.execute(stmt.order_by(ResearchEvidenceEvent.event_date.desc(), ResearchEvidenceEvent.created_at.desc(), ResearchEvidenceEvent.id.desc()).limit(max(1, min(limit, 500)))).scalars().all()
    return [serialize_event(row) for row in rows]
