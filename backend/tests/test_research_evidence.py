from datetime import date, datetime, timezone

import pytest
import requests
from fastapi import HTTPException
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from app.db import Base
from app.models import (
    ConfirmationScoreSnapshot,
    FundamentalsSnapshot,
    GovernmentContract,
    InsiderTransactionNormalized,
    InstitutionalPositionChange,
    ResearchEvidenceEvent,
    ResearchSourceDocument,
    Security,
)
from app.services.research_evidence import (
    EVIDENCE_PROCESSING_VERSION,
    _semantic_schema,
    canonical_json,
    events_from_confirmation_snapshot,
    events_from_contract,
    events_from_fundamentals_snapshot,
    events_from_insider_transaction,
    events_from_institutional_change,
    extract_document_events,
    parse_semantic_events,
    persist_event,
    query_events,
    upsert_source_document,
    validate_event,
)


@pytest.fixture()
def db():
    engine = create_engine("sqlite+pysqlite:///:memory:", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    Base.metadata.create_all(engine, tables=[
        Security.__table__, FundamentalsSnapshot.__table__, GovernmentContract.__table__, InsiderTransactionNormalized.__table__,
        InstitutionalPositionChange.__table__, ConfirmationScoreSnapshot.__table__, ResearchSourceDocument.__table__, ResearchEvidenceEvent.__table__,
    ])
    session = Session(engine)
    try:
        yield session
    finally:
        session.close()
        engine.dispose()


def seed_security(db):
    security = Security(symbol="MU", name="Micron Technology", asset_class="Equity", sector="Technology")
    db.add(security); db.commit()
    return security


def test_evidence_model_is_global_and_rejects_invalid_taxonomy():
    assert "user_id" not in ResearchEvidenceEvent.__table__.c
    valid = validate_event({"security_id": 1, "event_type": "metric_increased", "category": "financial", "metric": "revenue", "direction": "positive", "source_type": "fundamentals_snapshot", "source_provider": "fmp", "source_id": "row-1", "headline": "Revenue increased", "summary": "Revenue increased.", "confidence": "high", "materiality": "medium", "extraction_method": "deterministic"})
    assert valid["previous_value"] is None
    with pytest.raises(ValueError):
        validate_event({**valid, "event_type": "made_up_event"})
    with pytest.raises(ValueError):
        validate_event({**valid, "category": "made_up_category"})


def test_canonical_hash_is_stable_and_ignores_unrelated_ingestion_timestamp(db):
    security = seed_security(db)
    base = {"security_id": security.id, "event_type": "metric_increased", "category": "financial", "metric": "revenue", "direction": "positive", "source_type": "fundamentals_snapshot", "source_provider": "fmp", "source_id": "row-1", "headline": "Revenue increased", "summary": "Revenue increased.", "confidence": "high", "materiality": "medium", "extraction_method": "deterministic", "current_value": 10.0}
    first, first_created = persist_event(db, base)
    second, second_created = persist_event(db, {**base, "published_at": datetime.now(timezone.utc)})
    assert first_created is True
    assert second_created is False
    assert first.id == second.id
    assert canonical_json({"b": 2, "a": 1}) == canonical_json({"a": 1, "b": 2})


def test_deterministic_financial_event_and_query_filters(db):
    security = seed_security(db)
    previous = FundamentalsSnapshot(symbol="MU", provider="fmp", snapshot_date=date(2026, 8, 1), observed_at=datetime(2026, 8, 1, tzinfo=timezone.utc), source_fetched_at=datetime(2026, 8, 1, tzinfo=timezone.utc), status="ok", revenue_growth=10.0, gross_margin=40.0)
    current = FundamentalsSnapshot(symbol="MU", provider="fmp", snapshot_date=date(2026, 8, 2), observed_at=datetime(2026, 8, 2, tzinfo=timezone.utc), source_fetched_at=datetime(2026, 8, 2, tzinfo=timezone.utc), status="ok", revenue_growth=15.0, gross_margin=42.0)
    db.add_all([previous, current]); db.commit()
    assert events_from_fundamentals_snapshot(db, current) == 2
    db.commit()
    assert events_from_fundamentals_snapshot(db, current) == 0
    rows = query_events(db, security_id=security.id, category="financial", metric="gross_margin")
    assert len(rows) == 1
    assert rows[0]["event_type"] == "margin_expanded"
    assert rows[0]["previous_value"] == 40.0
    assert rows[0]["current_value"] == 42.0


def test_available_structured_adapters_create_traceable_events(db):
    security = seed_security(db)
    contract = GovernmentContract(symbol="MU", award_date=date(2026, 8, 1), award_amount=15_000_000, award_id="award-1", source="usaspending", source_url="https://example.test/award-1")
    insider = InsiderTransactionNormalized(accession_number="0001", ticker_normalized="MU", transaction_code="P", transaction_date=date(2026, 8, 2), reporting_owner_name="Executive", normalized_hash="hash-1", is_duplicate=False, is_derivative=False)
    institution = InstitutionalPositionChange(cik="holder", normalized_symbol="MU", report_year=2026, report_quarter=2, filing_date=date(2026, 8, 3), change_type="increased", is_material=True, prev_shares=10, curr_shares=20, shares_delta=10)
    db.add_all([contract, insider, institution]); db.commit()
    assert events_from_contract(db, contract) == 1
    assert events_from_insider_transaction(db, insider) == 1
    assert events_from_institutional_change(db, institution) == 1
    db.commit()
    kinds = {row.event_type for row in db.query(ResearchEvidenceEvent).all()}
    assert {"contract_awarded", "insider_purchase", "institutional_position_increased"} <= kinds
    assert all(row.source_id and row.source_provider for row in db.query(ResearchEvidenceEvent).all())


def test_confirmation_only_emits_meaningful_change(db):
    security = seed_security(db)
    first = ConfirmationScoreSnapshot(security_id=security.id, ticker_at_time="MU", calculated_at=datetime(2026, 8, 1, tzinfo=timezone.utc), market_date=date(2026, 8, 1), score=50, direction="neutral", strength="neutral", active_source_count=1, active_sources_json='["financials"]', input_hash="a", methodology_version_id=1, calculation_type="live")
    same = ConfirmationScoreSnapshot(security_id=security.id, ticker_at_time="MU", calculated_at=datetime(2026, 8, 2, tzinfo=timezone.utc), market_date=date(2026, 8, 2), score=50, direction="neutral", strength="neutral", active_source_count=1, active_sources_json='["financials"]', input_hash="b", methodology_version_id=1, calculation_type="live")
    changed = ConfirmationScoreSnapshot(security_id=security.id, ticker_at_time="MU", calculated_at=datetime(2026, 8, 3, tzinfo=timezone.utc), market_date=date(2026, 8, 3), score=70, direction="bullish", strength="strong", active_source_count=2, active_sources_json='["financials", "insiders"]', input_hash="c", methodology_version_id=1, calculation_type="live")
    db.add_all([first, same, changed]); db.commit()
    assert events_from_confirmation_snapshot(db, same) == 0
    assert events_from_confirmation_snapshot(db, changed) == 2  # direction change + source-set change; no arbitrary score threshold.


def test_semantic_schema_forbids_numeric_or_date_output_and_requires_real_excerpt(db):
    security = seed_security(db)
    text = "We now expect commercial service to begin in the first quarter of 2027, compared with our previous target of the fourth quarter of 2026."
    document, changed = upsert_source_document(db, security_id=security.id, document_type="earnings_transcript", source_provider="company_ir", external_id="doc-1", content=text)
    assert changed
    schema = _semantic_schema()["properties"]["events"]["items"]["properties"]
    assert "current_value" not in schema and "event_date" not in schema
    parsed = {"events": [{"category": "other_material_company_event", "event_type": "metric_decreased", "subject": "commercial service", "metric": "commercialization timing", "direction": "negative", "previous_text": "fourth quarter of 2026", "current_text": "first quarter of 2027", "headline": "Commercial service delayed", "summary": "Commercial service timing moved later.", "evidence_excerpt": text, "confidence": "high", "materiality": "high"}]}
    events = parse_semantic_events(parsed=parsed, document=document, source_text=text)
    assert events[0]["previous_value"] is None and events[0]["event_date"] is None
    parsed["events"][0]["evidence_excerpt"] = "Invented excerpt"
    with pytest.raises(ValueError):
        parse_semantic_events(parsed=parsed, document=document, source_text=text)


class Response:
    status_code = 200
    def __init__(self, payload): self.payload = payload
    def json(self): return {"output_text": self.payload}


def test_unchanged_document_does_not_repeat_semantic_extraction(db):
    security = seed_security(db)
    text = "Management launched a product today."
    document, _ = upsert_source_document(db, security_id=security.id, document_type="press_release", source_provider="company_ir", external_id="doc-2", content=text)
    calls = []
    payload = '{"events":[{"category":"other_material_company_event","event_type":"metric_increased","subject":"product","metric":"product availability","direction":"positive","previous_text":null,"current_text":"launched","headline":"Product launched","summary":"Product launched.","evidence_excerpt":"Management launched a product today.","confidence":"medium","materiality":"medium"}]}'
    first = extract_document_events(db, document=document, source_text=text, request_sender=lambda: (calls.append(1) or Response(payload)))
    second = extract_document_events(db, document=document, source_text=text, request_sender=lambda: (calls.append(1) or Response(payload)))
    assert first["events_written"] == 1
    assert second["status"] == "reused"
    assert len(calls) == 1
    assert document.processing_version == EVIDENCE_PROCESSING_VERSION


def test_changed_document_is_marked_pending_for_controlled_reprocessing(db):
    security = seed_security(db)
    document, _ = upsert_source_document(db, security_id=security.id, document_type="press_release", source_provider="company_ir", external_id="amended-doc", content="Original statement.")
    document.processing_status = "processed"; db.commit()
    amended, changed = upsert_source_document(db, security_id=security.id, document_type="press_release", source_provider="company_ir", external_id="amended-doc", content="Corrected statement.")
    assert amended.id == document.id
    assert changed is True
    assert amended.processing_status == "pending"


def test_document_extraction_failure_does_not_persist_partial_events(db):
    security = seed_security(db)
    text = "Management made an update."
    document, _ = upsert_source_document(db, security_id=security.id, document_type="press_release", source_provider="company_ir", external_id="failure-doc", content=text)
    with pytest.raises(HTTPException) as provider_error:
        extract_document_events(db, document=document, source_text=text, request_sender=lambda: Response('{"events":[{"bad":"shape"}]}'))
    assert provider_error.value.status_code == 502
    assert document.processing_status == "failed"
    assert db.query(ResearchEvidenceEvent).count() == 0
    document.processing_status = "pending"; db.commit()
    with pytest.raises(HTTPException) as timeout_error:
        extract_document_events(db, document=document, source_text=text, request_sender=lambda: (_ for _ in ()).throw(requests.Timeout()))
    assert timeout_error.value.status_code == 504
    assert document.failure_reason == "timeout"
