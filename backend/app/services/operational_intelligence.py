"""Reusable operating-company evidence for ticker intelligence and Research Memory.

This worker intentionally fetches authoritative provider content before asking a
model to classify it.  The model never scrapes URLs, and it runs once per
changed source document rather than once per viewer or private thesis.
"""
from __future__ import annotations

import hashlib
import logging
import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import requests
from sqlalchemy import func, select, or_, case
from sqlalchemy.orm import Session

from app.clients.fmp import FMP_BASE_URL
from app.models import ResearchEvidenceEvent, ResearchThesis, ResearchSourceCoverage, Security, WatchlistItem
from app.services.fmp_news import get_press_releases, get_stock_news
from app.services.provider_usage import ProviderUnavailable, ensure_fmp_live_allowed, record_provider_response
from app.services.research_claim_matching import claim_matching_enabled, process_event_matches, MatchBudget
from app.services.research_evidence import EVIDENCE_PROCESSING_VERSION, extract_document_events, upsert_source_document

logger = logging.getLogger(__name__)

OPERATIONAL_INTELLIGENCE_VERSION = "operational_intelligence_v1"
OPERATIONAL_SOURCE_VERSION = "operational_sources_v1"
OPERATIONAL_EVENT_TYPES = {
    "guidance_raised", "guidance_lowered", "product_launch", "product_delay", "commercial_milestone",
    "operational_milestone", "operational_setback", "customer_win", "customer_loss", "supply_constraint",
    "supply_relief", "pricing_increased", "pricing_decreased", "m_and_a_announced", "m_and_a_completed",
    "regulatory_approval", "regulatory_setback",
}


@dataclass
class _ExtractionBudget:
    remaining: int
    attempts: int = 0
    deferred: int = 0

    def consume(self) -> bool:
        if self.remaining <= 0:
            self.deferred += 1
            return False
        self.remaining -= 1
        self.attempts += 1
        return True


def _extraction_limit() -> int:
    try:
        value = int(os.getenv("RESEARCH_OPERATIONAL_MAX_EXTRACTIONS_PER_RUN", "50"))
    except ValueError:
        value = 50
    return max(0, min(value, 500))


def operational_intelligence_enabled() -> bool:
    return os.getenv("RESEARCH_OPERATIONAL_INTELLIGENCE_ENABLED", "true").strip().lower() not in {"0", "false", "off", "no"}


def transcript_analysis_enabled() -> bool:
    # Transcript content has distinct provider licensing/third-party processing
    # implications, so an operator must explicitly enable it after confirming
    # the FMP plan permits this use.
    return os.getenv("RESEARCH_TRANSCRIPT_ANALYSIS_ENABLED", "false").strip().lower() in {"1", "true", "on", "yes"}


def _clean(value: Any, limit: int = 20_000) -> str:
    return " ".join(str(value or "").split())[:limit]


def _source_url(value: Any) -> str | None:
    try:
        parsed = urlsplit(str(value or "").strip())
        if parsed.scheme.lower() not in {"https", "http"} or not parsed.hostname or parsed.username or parsed.password:
            return None
        return urlunsplit((parsed.scheme.lower(), parsed.netloc.lower(), parsed.path, parsed.query, ""))[:2000]
    except ValueError:
        return None


def _source_id(kind: str, item: dict[str, Any], *, security_id: int) -> str:
    # Provider news can cover multiple symbols; each retained document targets
    # one security. Title edits must update a URL's content, not create duplicates.
    identity = _source_url(item.get("url")) or "|".join(str(item.get(key) or "") for key in ("title", "published_at", "period", "year"))
    return f"{kind}:{security_id}:{hashlib.sha256(identity.encode('utf-8')).hexdigest()}"


def _as_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except ValueError:
        try:
            return datetime.combine(date.fromisoformat(text[:10]), datetime.min.time(), tzinfo=timezone.utc)
        except ValueError:
            return None


def _article_text(item: dict[str, Any]) -> str:
    title = _clean(item.get("title"), 500)
    summary = _clean(item.get("summary"), 20_000)
    return "\n\n".join(part for part in (title, summary) if part)


def _ingest_article(db: Session, *, security: Security, item: dict[str, Any], document_type: str, budget: _ExtractionBudget | None = None) -> dict[str, int]:
    source_text = _article_text(item)
    if len(source_text) < 20:
        return {"documents": 0, "events": 0, "matches": 0, "skipped": 1}
    document, changed = upsert_source_document(
        db,
        security_id=security.id,
        document_type=document_type,
        source_provider="fmp",
        external_id=_source_id(document_type, item, security_id=security.id),
        content=source_text,
        title=_clean(item.get("title"), 500) or None,
        source_url=_source_url(item.get("url")),
        published_at=_as_datetime(item.get("published_at")),
    )
    if not changed and document.processing_status == "processed" and document.processing_version == EVIDENCE_PROCESSING_VERSION:
        return {"documents": 0, "events": 0, "matches": _match_document_events(db, document.id), "skipped": 1}
    try:
        result = extract_document_events(db, document=document, source_text=source_text, consume_call=budget.consume if budget else None)
    except Exception as exc:
        db.rollback()
        logger.info("operational_document_extraction_failed security_id=%s document_id=%s error=%s", security.id, document.id, type(exc).__name__)
        return {"documents": int(changed), "events": 0, "matches": 0, "skipped": 0}
    matches = _match_document_events(db, document.id) if result.get("status") in {"processed", "reused"} else 0
    return {"documents": int(changed), "events": int(result.get("events_written") or 0), "matches": matches, "skipped": 0}


def _match_document_events(db: Session, document_id: str) -> int:
    if not claim_matching_enabled():
        return 0
    matches = 0
    rows = db.execute(select(ResearchEvidenceEvent).where(ResearchEvidenceEvent.source_document_id == document_id, ResearchEvidenceEvent.superseded_at.is_(None))).scalars().all()
    for event in rows:
        try:
            matches += int(process_event_matches(db, event=event, budget=db.info.get("research_match_budget")).get("matches") or 0)
        except Exception as exc:
            db.rollback()
            # Evidence is durable even if a later private-match attempt fails.
            logger.info("operational_claim_matching_failed document_id=%s event_id=%s error=%s", document_id, event.id, type(exc).__name__)
    db.commit()
    return matches


def _fmp_rows(endpoint: str, params: dict[str, Any]) -> list[dict[str, Any]]:
    api_key = os.getenv("FMP_API_KEY", "").strip()
    if not api_key:
        raise ProviderUnavailable("provider_configuration_missing")
    category = "research:operational_transcripts"
    try:
        ensure_fmp_live_allowed(category=category, allow_user_request=False)
    except ProviderUnavailable:
        raise
    try:
        response = requests.get(f"{FMP_BASE_URL}/{endpoint}", params={**params, "apikey": api_key}, timeout=45)
        record_provider_response(category=category, status_code=response.status_code)
        if response.status_code != 200:
            raise RuntimeError("provider_unavailable")
        data = response.json()
    except (requests.RequestException, ValueError):
        raise RuntimeError("provider_unavailable") from None
    return [row for row in data if isinstance(row, dict)] if isinstance(data, list) else []


def _ingest_latest_transcript(db: Session, *, security: Security, budget: _ExtractionBudget | None = None) -> dict[str, int]:
    if not transcript_analysis_enabled() or not security.symbol:
        return {"documents": 0, "events": 0, "matches": 0, "skipped": 0}
    dates = _fmp_rows("earning-call-transcript-dates", {"symbol": security.symbol})
    candidates: list[tuple[int, int, dict[str, Any]]] = []
    for row in dates:
        try:
            year, quarter = int(row.get("year") or row.get("fiscalYear")), int(str(row.get("quarter") or row.get("period") or "").upper().replace("Q", ""))
        except (TypeError, ValueError):
            continue
        if year > 1990 and 1 <= quarter <= 4:
            candidates.append((year, quarter, row))
    if not candidates:
        return {"documents": 0, "events": 0, "matches": 0, "skipped": 0}
    year, quarter, metadata = max(candidates, key=lambda item: (item[0], item[1]))
    rows = _fmp_rows("earning-call-transcript", {"symbol": security.symbol, "year": year, "quarter": quarter})
    if not rows:
        return {"documents": 0, "events": 0, "matches": 0, "skipped": 0}
    transcript = rows[0]
    content = str(transcript.get("content") or "").strip()
    # Full transcript, including Q&A. Sections are cached and resumed within the
    # per-run model budget. Oversized documents fail visibly instead of truncating.
    if len(content) > 2_000_000:
        raise ValueError("transcript_too_large")
    if len(content) < 200:
        return {"documents": 0, "events": 0, "matches": 0, "skipped": 1}
    item = {"title": f"{security.symbol} earnings call transcript — Q{quarter} {year}", "url": None, "published_at": transcript.get("date") or metadata.get("date"), "period": quarter, "year": year}
    document, changed = upsert_source_document(
        db,
        security_id=security.id,
        document_type="earnings_transcript",
        source_provider="fmp",
        external_id=f"earnings_transcript:{security.id}:{year}:Q{quarter}",
        content=content,
        title=item["title"],
        published_at=_as_datetime(item["published_at"]),
        period_end=None,
        filing_type=f"Q{quarter}-{year}",
    )
    if not changed and document.processing_status == "processed" and document.processing_version == EVIDENCE_PROCESSING_VERSION:
        return {"documents": 0, "events": 0, "matches": _match_document_events(db, document.id), "skipped": 1}
    try:
        result = extract_document_events(db, document=document, source_text=content, consume_call=budget.consume if budget else None)
    except Exception as exc:
        db.rollback()
        logger.info("operational_transcript_extraction_failed security_id=%s document_id=%s error=%s", security.id, document.id, type(exc).__name__)
        return {"documents": int(changed), "events": 0, "matches": 0, "skipped": 0}
    return {"documents": int(changed), "events": int(result.get("events_written") or 0), "matches": _match_document_events(db, document.id) if result.get("status") in {"processed", "reused"} else 0, "skipped": 0}


def candidate_securities(db: Session, *, limit: int = 50) -> list[Security]:
    """Oldest attempted ticker first; active theses break ties for new symbols."""
    bounded = max(1, min(int(limit), 250))
    active = select(ResearchThesis.security_id).where(ResearchThesis.status == "active")
    saved = select(WatchlistItem.security_id).where(WatchlistItem.security_id.is_not(None))
    attempts = select(ResearchSourceCoverage.security_id, func.max(ResearchSourceCoverage.last_attempt_at).label("attempted")).group_by(ResearchSourceCoverage.security_id).subquery()
    return db.scalars(select(Security).outerjoin(attempts, attempts.c.security_id == Security.id).where(or_(Security.id.in_(active), Security.id.in_(saved)), Security.symbol.is_not(None)).order_by(attempts.c.attempted.asc().nullsfirst(), case((Security.id.in_(active), 0), else_=1), Security.id).limit(bounded)).all()


def _coverage(db: Session, security_id: int, source_type: str) -> ResearchSourceCoverage:
    row = db.get(ResearchSourceCoverage, (security_id, source_type))
    if row is None:
        row = ResearchSourceCoverage(security_id=security_id, source_type=source_type, status="pending")
        db.add(row)
        db.flush()
    return row


def refresh_operational_intelligence(db: Session, *, security_id: int | None = None, limit: int = 50) -> dict[str, int | str]:
    if not operational_intelligence_enabled():
        return {"status": "disabled", "securities": 0, "documents": 0, "events": 0, "matches": 0, "skipped": 0}
    securities = [db.get(Security, security_id)] if security_id else candidate_securities(db, limit=limit)
    budget = _ExtractionBudget(remaining=_extraction_limit())
    match_budget = MatchBudget.configured()
    db.info["research_match_budget"] = match_budget
    totals = {"status": "ok", "securities": 0, "documents": 0, "events": 0, "matches": 0, "skipped": 0}
    for security in securities:
        if not security or not security.symbol:
            continue
        totals["securities"] += 1
        for document_type, loader in (("news_article", get_stock_news), ("press_release", get_press_releases), ("earnings_transcript", None)):
            coverage = _coverage(db, security.id, document_type)
            if document_type == "earnings_transcript" and not transcript_analysis_enabled():
                coverage.status = "disabled"
                db.commit()
                continue
            coverage.last_attempt_at = datetime.now(timezone.utc)
            coverage.status, coverage.failure_reason = "refreshing", None
            db.commit()
            deferred_before = budget.deferred
            try:
                if loader is None:
                    result = _ingest_latest_transcript(db, security=security, budget=budget)
                    count = result["documents"] + result["skipped"]
                    results = [result]
                else:
                    payload = loader(symbol=security.symbol, limit=20, force_refresh=True)
                    if payload.get("status") not in {"ok", "empty", "no_data"} or payload.get("cache_status") == "stale" or payload.get("is_stale"):
                        raise RuntimeError("source_unavailable")
                    items = [item for item in payload.get("items", []) if isinstance(item, dict)]
                    count = len(items)
                    results = [_ingest_article(db, security=security, item=item, document_type=document_type, budget=budget) for item in items]
                for result in results:
                    for key in ("documents", "events", "matches", "skipped"):
                        totals[key] += result[key]
                coverage = _coverage(db, security.id, document_type)
                coverage.documents_seen = count
                # A successful fetch is distinct from completed analysis.
                from app.models import ResearchSourceDocument
                unfinished = db.scalar(select(ResearchSourceDocument.id).where(ResearchSourceDocument.security_id == security.id, ResearchSourceDocument.document_type == document_type, ResearchSourceDocument.processing_status != "processed").limit(1))
                coverage.status = "partial" if unfinished or budget.deferred > deferred_before else "ready" if count else "empty"
                coverage.last_success_at = datetime.now(timezone.utc)
                coverage.failure_reason = None
                db.commit()
            except Exception as exc:
                db.rollback()
                coverage = _coverage(db, security.id, document_type)
                coverage.status, coverage.failure_reason = "unavailable", type(exc).__name__
                db.commit()
                logger.warning("research_source_refresh_failed security_id=%s source=%s error=%s", security.id, document_type, type(exc).__name__)
        if budget.remaining <= 0:
            break
    db.info.pop("research_match_budget", None)
    return {**totals, "extraction_attempts": budget.attempts, "extractions_deferred": budget.deferred, "matching_attempts": match_budget.attempts, "matching_deferred": match_budget.deferred}


def ticker_operational_intelligence(db: Session, *, security: Security, limit: int = 4) -> dict[str, Any]:
    """Read a prepared, public global signal summary. No providers or models run here."""
    bounded = max(1, min(int(limit), 10))
    rows = db.execute(
        select(ResearchEvidenceEvent)
        .where(ResearchEvidenceEvent.security_id == security.id)
        .where(ResearchEvidenceEvent.event_type.in_(sorted(OPERATIONAL_EVENT_TYPES)))
        .where(ResearchEvidenceEvent.superseded_at.is_(None))
        .where(func.coalesce(ResearchEvidenceEvent.published_at, ResearchEvidenceEvent.created_at) >= datetime.now(timezone.utc) - timedelta(days=120))
        .order_by(ResearchEvidenceEvent.published_at.desc().nullslast(), ResearchEvidenceEvent.created_at.desc())
        .limit(60)
    ).scalars().all()
    result: dict[str, list[dict[str, Any]]] = {"catalysts": [], "risks": [], "opportunities": [], "watch_next": []}
    seen: dict[str, set[str]] = {key: set() for key in result}
    for event in rows:
        item = {
            "id": event.id,
            "title": event.headline,
            "summary": event.summary,
            "event_type": event.event_type,
            "source_type": event.source_type,
            "source_url": _source_url(event.source_url),
            "published_at": event.published_at.isoformat() if event.published_at else None,
            "materiality": event.materiality,
            "confidence": event.confidence,
            "evidence_excerpt": _clean(event.evidence_excerpt, 800) or None,
            "source_locator": event.source_locator,
            "watch_item": event.watch_item,
        }
        bucket = "catalysts" if event.direction == "positive" else "risks" if event.direction == "negative" else "watch_next"
        signature = f"{event.event_type}|{event.headline}"
        if bucket != "watch_next" and signature not in seen[bucket] and len(result[bucket]) < bounded:
            result[bucket].append(item); seen[bucket].add(signature)
        if event.direction == "positive" and event.watch_item and signature not in seen["opportunities"] and len(result["opportunities"]) < bounded:
            result["opportunities"].append({**item, "next_step": event.watch_item})
            seen["opportunities"].add(signature)
        if event.watch_item:
            watch = {**item, "title": event.watch_item, "summary": event.summary}
            watch_signature = f"watch|{event.watch_item}"
            if watch_signature not in seen["watch_next"] and len(result["watch_next"]) < bounded:
                result["watch_next"].append(watch); seen["watch_next"].add(watch_signature)
    coverage_rows = db.scalars(select(ResearchSourceCoverage).where(ResearchSourceCoverage.security_id == security.id)).all()
    by_source = {row.source_type: row for row in coverage_rows}
    now = datetime.now(timezone.utc)
    coverage = []
    for source in ("news_article", "press_release", "earnings_transcript"):
        row = by_source.get(source)
        status = row.status if row else "not_checked"
        if source == "earnings_transcript" and not transcript_analysis_enabled():
            status = "disabled"
        last = row.last_success_at.replace(tzinfo=timezone.utc) if row and row.last_success_at else None
        if status in {"ready", "empty"} and last and now - last > timedelta(hours=6):
            status = "stale"
        coverage.append({"source_type": source, "status": status, "last_checked_at": row.last_attempt_at.isoformat() if row and row.last_attempt_at else None, "last_success_at": last.isoformat() if last else None, "documents_seen": row.documents_seen if row else 0})
    return {"symbol": security.symbol, "status": "ok" if rows else "empty", "source_version": OPERATIONAL_SOURCE_VERSION, "coverage": coverage, "lookback_days": 120, **result}
