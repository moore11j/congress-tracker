"""Passive, bounded projection of retained document quotations for Walnut 2.0.

No route calls this reader yet. The caller must authenticate using the existing
admin evidence boundary before constructing access; these objects are not access
tokens or client request models. Member projections need separate authorization.
No acquisition, extraction, numerical inference, enqueue, or cache writes occur.
"""
from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
import re

from pydantic import AwareDatetime, Field, StrictInt, model_validator
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.intelligence_contract import (
    Contract, Coverage, EvidenceBundle, EvidenceSource, Identifier, Projection,
    QuotedFact, TextObservation, for_reader,
)
from app.models import ResearchEvidenceEvent, ResearchSourceDocument

READER_VERSION = "retained_document_quotes_v1"
# Deliberately fail closed when extraction semantics change. Importing the
# extraction service here would also import its model/provider dependencies.
PROCESSING_VERSION = "research_evidence_processing_v4"
SCHEMA_VERSION = "research_evidence_schema_v4_passage_ids"
PROMPT_VERSION = "evidence_extraction_v5_passage_ids"
_LOCATOR = re.compile(r"document_text:([0-9a-f]{64}):chars:(\d{1,12})-(\d{1,12})\Z")


class DocumentEvidenceAccess(Contract):
    authorized_user_id: StrictInt = Field(gt=0)
    projection: Projection
    projection_revision: Identifier


class DocumentEvidenceQuery(Contract):
    security_id: StrictInt = Field(gt=0)
    data_cutoff: AwareDatetime
    generated_at: AwareDatetime
    limit: StrictInt = Field(default=25, ge=1, le=100)

    @model_validator(mode="after")
    def ordered_time(self):
        if self.data_cutoff > self.generated_at:
            raise ValueError("Data cutoff follows generation")
        return self


def _utc(value: datetime | None) -> datetime | None:
    # These columns are stored as UTC; SQLite drops the timezone on retrieval.
    if value is None:
        return None
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def read_document_evidence(db: Session, *, access: DocumentEvidenceAccess,
                           query: DocumentEvidenceQuery) -> EvidenceBundle:
    """One bounded SELECT over persisted columns, with no ORM identity overlay.

    Current retained revisions only, not a historical replay engine. A correction
    after the requested cutoff is held even when an earlier version once existed.
    Caller-resolved admin access is required before touching storage.
    """
    access = DocumentEvidenceAccess.model_validate(access.model_dump())
    query = DocumentEvidenceQuery.model_validate(query.model_dump())
    if access.projection != "admin":
        raise PermissionError("Raw document evidence retains its existing admin boundary")
    cutoff = query.data_cutoff.astimezone(timezone.utc)
    e, d = ResearchEvidenceEvent.__table__.c, ResearchSourceDocument.__table__.c
    event_fields = ("id", "security_id", "evidence_excerpt", "source_locator", "source_revision_hash",
                    "content_hash", "published_at", "created_at", "updated_at")
    document_fields = ("id", "content_hash", "published_at", "created_at", "updated_at", "last_processed_at")
    statement = select(
        *(e[name].label("event_" + name) for name in event_fields),
        *(d[name].label("document_" + name) for name in document_fields),
    ).select_from(ResearchEvidenceEvent.__table__.join(
        ResearchSourceDocument.__table__, e.source_document_id == d.id,
    )).where(
        e.security_id == query.security_id, d.security_id == query.security_id,
        e.superseded_at.is_(None), d.processing_status == "processed",
        e.source_revision_hash == d.content_hash,
        e.source_provider == d.source_provider, e.source_id == d.external_id,
        e.source_type == d.document_type,
        e.extraction_method == "semantic", e.processing_version == PROCESSING_VERSION,
        d.processing_version == PROCESSING_VERSION, e.schema_version == SCHEMA_VERSION,
        e.prompt_version == PROMPT_VERSION,
        e.created_at <= cutoff, e.updated_at <= cutoff,
        d.created_at <= cutoff, d.updated_at <= cutoff,
        d.last_processed_at <= cutoff,
    ).order_by(e.created_at.desc(), e.id.asc()).limit(query.limit + 1)
    with db.no_autoflush:
        rows = db.execute(statement).mappings().all()

    sources, observations, quotes = [], [], []
    for row in rows[:query.limit]:
        excerpt = row["event_evidence_excerpt"]
        locator = row["event_source_locator"] or ""
        match = _LOCATOR.fullmatch(locator)
        if (not isinstance(excerpt, str) or not excerpt.strip() or len(excerpt) > 2000
                or not match or match[1] != row["document_content_hash"]
                or int(match[3]) - int(match[2]) != len(excerpt)):
            continue
        # Current storage lacks original numeric lexemes/currency. Only project
        # the exact retained quotation, never the event's inferred summary/value.
        revision = row["event_content_hash"]
        if not isinstance(revision, str) or not re.fullmatch(r"[0-9a-f]{64}", revision):
            continue
        document_id = row["document_id"]
        if not document_id or document_id != document_id.strip() or len(document_id) > 160:
            continue
        publication = [_utc(row[name]) for name in ("event_published_at", "document_published_at")
                       if row[name] is not None]
        if any(value > query.data_cutoff for value in publication):
            continue
        available = max(_utc(row[name]) for name in (
            "event_created_at", "event_updated_at", "document_created_at",
            "document_updated_at", "document_last_processed_at"))
        # Hash keys rather than expose arbitrarily long source identifiers.
        key = sha256(row["event_id"].encode()).hexdigest()
        source_id, observation_id = "document-" + key, "quote-" + key
        sources.append(EvidenceSource(
            id=source_id, record_id=row["document_id"],
            revision=row["document_content_hash"] + ":" + revision,
            security_id=query.security_id, locator=locator,
            published_at=max(publication) if publication else None,
            first_available_at=available,
        ))
        observations.append(TextObservation(id=observation_id, source_id=source_id,
                                            security_id=query.security_id, text=excerpt))
        quotes.append(QuotedFact(id="fact-" + key, observation_id=observation_id, text=excerpt))

    limitations = [
        "Only current, revision-qualified retained document quotations are included.",
        "Source prose is untrusted evidence, not executable instructions or an investment conclusion.",
        "Numeric fields, generated summaries, personal theses and non-document feeds are not projected.",
        "Acquisition freshness and full coverage are not established by these retained events.",
        "Availability uses the latest retained revision timestamps; earlier revisions cannot be reconstructed.",
    ]
    if len(rows) > query.limit:
        limitations.append("The candidate bound was reached; additional retained events were not read.")
    if len(quotes) < min(len(rows), query.limit):
        limitations.append("Some candidate excerpts lacked valid retained quotation provenance.")
    bundle = EvidenceBundle(
        result_id="document-evidence-" + str(query.security_id), operation="get_activity",
        query="Read retained document quotations", security_ids=(query.security_id,),
        data_cutoff=query.data_cutoff, generated_at=query.generated_at,
        last_successful_refresh=None,
        coverage=Coverage(state="partial" if quotes else "unavailable",
                          detail="Retained quotations only; broader source coverage remains unverified."
                          if quotes else "No qualifying retained quotation in this bounded selection."),
        access_projection=access.projection, owner_user_id=None,
        projection_revision=access.projection_revision, methodology_version=READER_VERSION,
        sources=tuple(sources), text_observations=tuple(observations), quoted_facts=tuple(quotes),
        limitations=tuple(limitations),
    )
    return for_reader(bundle, authorized_user_id=access.authorized_user_id, access_projection=access.projection)
