"""Isolated direct-source staging, revisions and cross-provider reconciliation.

These tables are deliberately absent from public feed/profile queries. A source
receipt is evidence of collection, not permission to publish or disable FMP.
"""
from __future__ import annotations

import hashlib
import json
import re
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Optional

from sqlalchemy import DateTime, LargeBinary, Text, UniqueConstraint, inspect, select, text
from sqlalchemy.orm import Mapped, mapped_column

from app.db import Base
from app.models import Event, InsiderTransactionNormalized, InstitutionalFiling, InstitutionalPosition


def utcnow():
    return datetime.now(timezone.utc)


class DirectFeedDocument(Base):
    __tablename__ = "direct_feed_documents"
    __table_args__ = (UniqueConstraint("feed", "source_key", name="uq_direct_feed_document"),)
    id: Mapped[int] = mapped_column(primary_key=True)
    feed: Mapped[str] = mapped_column(index=True)
    source_key: Mapped[str]
    source_url: Mapped[str] = mapped_column(Text)
    metadata_json: Mapped[str] = mapped_column(Text, default="{}")
    content_hash: Mapped[Optional[str]]
    parsed_json: Mapped[Optional[str]] = mapped_column(Text)
    status: Mapped[str] = mapped_column(default="pending", index=True)
    error: Mapped[Optional[str]] = mapped_column(Text)
    attempts: Mapped[int] = mapped_column(default=0)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    checked_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    reconciliation_json: Mapped[Optional[str]] = mapped_column(Text)


class DirectFeedRevision(Base):
    __tablename__ = "direct_feed_revisions"
    __table_args__ = (UniqueConstraint("document_id", "content_hash", name="uq_direct_feed_revision"),)
    id: Mapped[int] = mapped_column(primary_key=True)
    document_id: Mapped[int] = mapped_column(index=True)
    content_hash: Mapped[str]
    # Government XML/JSON or extracted text, never HTML executable by a browser.
    source_text: Mapped[str] = mapped_column(Text)
    # Exact transport bytes bind publication to the captured SHA, including
    # the SEC header. Older revisions remain unavailable until re-collected.
    source_bytes: Mapped[Optional[bytes]] = mapped_column(LargeBinary)
    parsed_json: Mapped[str] = mapped_column(Text)
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class DirectFeedRun(Base):
    __tablename__ = "direct_feed_runs"
    id: Mapped[int] = mapped_column(primary_key=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    finished_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    status: Mapped[str] = mapped_column(default="running")
    report_json: Mapped[str] = mapped_column(Text, default="{}")


def ensure_direct_feed_schema(bind):
    from app.services.feed_source_control import FeedSourceControl
    from app.services.direct_feed_worker import DirectFeedPublication
    from app.services.direct_congress_repair import CongressRepairArchive, CongressRepairReceipt, CongressRowBinding
    from app.services.direct_sec_repair import SecRepairReceipt
    def create_table(model):
        with bind.begin() as conn:
            if conn.dialect.name == 'postgresql':
                conn.execute(text("SET LOCAL lock_timeout = '2s'"))
                conn.execute(text("SET LOCAL statement_timeout = '10s'"))
            model.__table__.create(conn, checkfirst=True)
    for model in (DirectFeedDocument, DirectFeedRevision, DirectFeedRun):
        create_table(model)
    with bind.begin() as conn:
        if 'source_bytes' not in {c['name'] for c in inspect(conn).get_columns('direct_feed_revisions')}:
            binary_type = 'BYTEA' if conn.dialect.name == 'postgresql' else 'BLOB'
            if conn.dialect.name == 'postgresql':
                conn.execute(text("SET LOCAL lock_timeout = '2s'"))
                conn.execute(text("SET LOCAL statement_timeout = '10s'"))
            conn.execute(text(f'ALTER TABLE direct_feed_revisions ADD COLUMN source_bytes {binary_type}'))
    for model in (FeedSourceControl, DirectFeedPublication, CongressRepairArchive, CongressRepairReceipt, CongressRowBinding, SecRepairReceipt):
        create_table(model)


def dumps(value):
    return json.dumps(value, default=str, sort_keys=True, ensure_ascii=False)


def discover(db, feed, metadata):
    key = metadata["key"]
    row = db.scalar(select(DirectFeedDocument).where(DirectFeedDocument.feed == feed, DirectFeedDocument.source_key == key))
    if row is None:
        row = DirectFeedDocument(feed=feed, source_key=key, source_url=metadata["url"], metadata_json=dumps(metadata))
        db.add(row)
        db.flush()
    elif row.metadata_json != dumps(metadata):
        row.metadata_json = dumps(metadata)
        row.source_url = metadata["url"]
        row.status = "pending"
    return row


def record_document(db, row, raw: bytes, source_text: str, parsed: dict, *, reasons=()):
    if len(raw) > 30_000_000:
        raise ValueError('Source exceeds size limit')
    digest = hashlib.sha256(raw).hexdigest()
    parsed_json = dumps(parsed)
    existing = db.scalar(select(DirectFeedRevision).where(DirectFeedRevision.document_id == row.id, DirectFeedRevision.content_hash == digest))
    if existing is None:
        db.add(DirectFeedRevision(document_id=row.id, content_hash=digest, source_text=source_text,
                                  source_bytes=raw, parsed_json=parsed_json))
    elif existing.source_bytes is None:
        existing.source_bytes = raw
    row.content_hash = digest
    row.parsed_json = parsed_json
    row.status = "quarantined" if reasons else "parsed"
    row.error = "; ".join(reasons) or None
    row.checked_at = utcnow()
    # Issuer publication evidence is immutable across page refreshes. The reader
    # validates it against the original revision and holds semantic changes.
    issuer_receipt = _payload(row.reconciliation_json).get("issuer_transcript_research") if row.feed == "issuer_earnings" else None
    row.reconciliation_json = dumps({"issuer_transcript_research": issuer_receipt}) if issuer_receipt is not None else None
    db.flush()


def _text(value):
    return re.sub(r"[^a-z0-9]", "", str(value or "").lower())


def _number(value):
    if value is None or value == "":
        return None
    try:
        return str(Decimal(str(value)).normalize())
    except InvalidOperation:
        return None


def insider_identity(row):
    get = row.get if isinstance(row, dict) else lambda key: getattr(row, key, None)
    return (str(get("transaction_date") or "")[:10], _text(get("ticker_normalized")),
            str(get("reporting_owner_cik") or "").lstrip("0") or _text(get("reporting_owner_name")),
            _text(get("transaction_code")), _number(get("shares")), _number(get("price")), bool(get("is_derivative")))


def _insider_details(row):
    get = row.get if isinstance(row, dict) else lambda key: getattr(row, key, None)
    return (str(get("filing_date") or "")[:10], str(get("issuer_cik") or "").lstrip("0"),
            _text(get("security_title")), _text(get("acquired_disposed")),
            _text(get("direct_or_indirect")))


def reconcile_insider(db, parsed):
    """Consume matches one-to-one; similar trades without filing proof are held."""
    matched, new, ambiguous = [], [], []
    consumed = set()
    accession = parsed["filing"]["accession_number"]
    filing_rows = db.scalars(select(InsiderTransactionNormalized).where(
        InsiderTransactionNormalized.is_duplicate.is_(False),
        InsiderTransactionNormalized.accession_number == accession,
    ).order_by(InsiderTransactionNormalized.id)).all()
    for index, item in enumerate(parsed["transactions"]):
        candidates = db.scalars(select(InsiderTransactionNormalized).where(
            InsiderTransactionNormalized.is_duplicate.is_(False),
            InsiderTransactionNormalized.ticker_normalized == item["ticker_normalized"],
            InsiderTransactionNormalized.transaction_date == datetime.fromisoformat(str(item["transaction_date"])).date(),
        )).all() if item.get("transaction_date") and item.get("ticker_normalized") else []
        exact = [row for row in candidates if row.id not in consumed and insider_identity(row) == insider_identity(item)
                 and _insider_details(row) == _insider_details(item)
                 and row.accession_number == item["accession_number"]]
        if len(exact) == 1:
            consumed.add(exact[0].id)
            matched.append({"row": index + 1, "existing_id": exact[0].id})
        elif exact or filing_rows or any(insider_identity(row) == insider_identity(item) for row in candidates):
            # Provider projections can lose zeros, derivative flags or lots.
            # A mismatch in an already stored filing is not proof of a new
            # transaction. Hold it instead of inserting a second representation.
            ambiguous.append(index + 1)
        else:
            new.append(index + 1)
    return {"matched": matched, "unmatched": new, "ambiguous": ambiguous,
            "existing_only_ids": [row.id for row in filing_rows if row.id not in consumed]}


def _payload(value):
    try:
        result = json.loads(value or "{}")
        return result if isinstance(result, dict) else {}
    except ValueError:
        return {}


def reconcile_institutional(db, parsed):
    """Compare an exact filing without creating holdings or guessing tickers.

    Existing ingestion aggregates repeated CUSIP/option rows; compare totals
    at that granularity while retaining every source row in the document.
    """
    metadata = parsed["metadata"]
    filing = db.scalar(select(InstitutionalFiling).where(InstitutionalFiling.accession_number == metadata["key"]))
    if filing is None:
        return {"matched": [], "unmatched": [row["source_line_ref"] for row in parsed["positions"]], "ambiguous": [], "filing_id": None}
    if (filing.cik.lstrip("0") != metadata["cik"].lstrip("0") or
            str(filing.report_period_end) != metadata["report_period"] or
            str(filing.filing_date) != metadata["filing_date"]):
        return {"matched": [], "unmatched": [], "ambiguous": [row["source_line_ref"] for row in parsed["positions"]], "filing_id": filing.id}
    groups = {}
    for row in parsed["positions"]:
        key = (row["cusip"].strip().upper(), (row.get("putCall") or "").strip().upper())
        group = groups.setdefault(key, {"rows": [], "shares": Decimal(0), "value": Decimal(0)})
        group["rows"].append(row["source_line_ref"])
        group["shares"] += Decimal(str(row["shares"]))
        group["value"] += Decimal(str(row["valueUsd"]))
    existing = {}
    for row in db.scalars(select(InstitutionalPosition).where(InstitutionalPosition.filing_id == filing.id)):
        existing.setdefault(((row.cusip or "").strip().upper(), (row.put_call or "").strip().upper()), []).append(row)
    matched, unmatched, ambiguous = [], [], []
    for key, group in groups.items():
        candidates = existing.pop(key, [])
        if not candidates:
            unmatched.extend(group["rows"])
        elif (len(candidates) == 1 and _number(candidates[0].shares) == _number(group["shares"])
              and _number(candidates[0].value_usd) == _number(group["value"])):
            matched.extend({"row": ref, "existing_id": candidates[0].id} for ref in group["rows"])
        else:
            ambiguous.extend(group["rows"])
    return {"matched": matched, "unmatched": unmatched, "ambiguous": ambiguous, "filing_id": filing.id,
            "existing_only_ids": [row.id for rows in existing.values() for row in rows]}


def reconcile_congress(db, parsed):
    from app.services.official_congress import normalize_congress_owner, normalize_congress_transaction_type
    matched, new, ambiguous, consumed = [], [], [], set()
    for index, item in enumerate(parsed["transactions"]):
        candidates = db.scalars(select(Event).where(Event.event_type == "congress_trade", Event.symbol == item.get("ticker_normalized"))).all() if item.get("ticker_normalized") else []
        exact, similar = [], []
        for event in candidates:
            payload = _payload(event.payload_json)
            raw = payload.get("raw") or {}
            trade_date = payload.get("transaction_date") or payload.get("trade_date") or raw.get("transactionDate")
            owner = payload.get("owner_type") or raw.get("owner")
            name = event.member_bioguide_id if item.get("member_id") else event.member_name
            member = item.get("member_id") or item.get("member_name_raw")
            # Filing URL plus every economic/owner field, not fuzzy name alone.
            equal = (str(trade_date or "")[:10] == str(item.get("transaction_date") or "")[:10]
                     and normalize_congress_owner(owner) == item.get("owner_normalized")
                     and normalize_congress_transaction_type(event.trade_type or event.transaction_type) == item.get("transaction_type_normalized")
                     and _number(event.amount_min) == _number(item.get("amount_low"))
                     and _number(event.amount_max) == _number(item.get("amount_high")))
            if not equal:
                continue
            document = event.source_document_url or payload.get("document_url") or payload.get("link") or raw.get("link")
            if document == item.get("document_url") and event.chamber == item.get("chamber") and event.id not in consumed:
                exact.append(event.id)
            elif _text(name) == _text(member):
                similar.append(event.id)
        if len(exact) == 1:
            consumed.add(exact[0])
            matched.append({"row": index + 1, "existing_id": exact[0]})
        elif exact or similar:
            ambiguous.append(index + 1)
        else:
            new.append(index + 1)
    return {"matched": matched, "unmatched": new, "ambiguous": ambiguous}


def readiness_report(db):
    statuses = Counter()
    feeds = {}
    for row in db.scalars(select(DirectFeedDocument)):
        statuses[row.status] += 1
        feed = feeds.setdefault(row.feed, {"documents": 0, "parsed": 0, "pending": 0, "failed": 0, "quarantined": 0, "matched_rows": 0, "unmatched_rows": 0, "ambiguous_rows": 0, "existing_only_rows": 0})
        feed["documents"] += 1
        feed[row.status] = feed.get(row.status, 0) + 1
        receipt = _payload(row.reconciliation_json)
        for name in ("matched", "unmatched", "ambiguous"):
            feed[name + "_rows"] += len(receipt.get(name, []))
        feed["existing_only_rows"] += len(receipt.get("existing_only_ids", []))
    runs = db.scalars(select(DirectFeedRun).order_by(DirectFeedRun.id.desc()).limit(20)).all()
    return {"mode": "shadow", "cutover_ready": False, "fmp_disabled": False, "feeds": feeds,
            "recent_runs": [{"id": row.id, "status": row.status, "started_at": str(row.started_at), "finished_at": str(row.finished_at), "report": _payload(row.report_json)} for row in runs],
            "remaining_gates": ["Observed source freshness and complete retained-universe coverage", "Resolve quarantined and ambiguous rows",
                                "No-send monitoring/watchlist/daily/weekly digest parity, delivery identity and preference preservation",
                                "Fresh quote/reference-close validation and rebuilt Top Stocks input coverage with FMP blocked",
                                "Public canonical table/endpoint parity and per-feed switch rehearsal", "Replacement prices/corporate actions and documented provider-use constraints",
                                "Explicit disposition of other retained FMP dependencies"]}
