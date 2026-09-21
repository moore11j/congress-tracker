import json
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
    ResearchExtractionChunk,
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
        InstitutionalPositionChange.__table__, ConfirmationScoreSnapshot.__table__, ResearchSourceDocument.__table__, ResearchEvidenceEvent.__table__, ResearchExtractionChunk.__table__,
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
    parsed = {"events": [{"category": "product_commercial", "event_type": "product_delay", "subject": "commercial service", "metric": "commercialization timing", "direction": "negative", "previous_text": "fourth quarter of 2026", "current_text": "first quarter of 2027", "headline": "Commercial service delayed", "summary": "Commercial service timing moved later.", "evidence_excerpt": text, "watch_item": "Commercial service timing", "confidence": "high", "materiality": "high"}]}
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
    payload = '{"events":[{"category":"product_commercial","event_type":"product_launch","subject":"product","metric":"product availability","direction":"positive","previous_text":null,"current_text":"launched","headline":"Product launched","summary":"Product launched.","evidence_excerpt":"Management launched a product today.","watch_item":null,"confidence":"medium","materiality":"medium"}]}'
    first = extract_document_events(db, document=document, source_text=text, request_sender=lambda: (calls.append(1) or Response(payload)))
    second = extract_document_events(db, document=document, source_text=text, request_sender=lambda: (calls.append(1) or Response(payload)))
    assert first["events_written"] == 1
    assert second["status"] == "reused"
    assert len(calls) == 1
    assert document.processing_version == EVIDENCE_PROCESSING_VERSION


def test_source_passage_choices_are_verbatim_bounded_and_cover_full_section():
    from app.services.research_evidence import _source_excerpt_choices, _semantic_schema
    for source in (("Management expects growth, subject to approval. " * 400)[:18000], "x" * 18000):
        source = source.strip()
        choices = _source_excerpt_choices(source)
        assert len(choices) < 100
        assert sum(map(len, choices)) < 50000
        assert all(0 < len(value) <= 500 and value in source for value in choices)
        assert source.startswith(choices[0]) and source.endswith(choices[-1])
        schema = _semantic_schema(source)
        assert schema["properties"]["events"]["items"]["properties"]["evidence_excerpt"]["enum"] == choices


def test_extraction_constrains_model_to_source_passages_and_reuses_result(db, monkeypatch):
    from app.services import research_evidence as service
    source = "Management expects the launch next quarter, subject to approval."
    security = seed_security(db)
    document, _ = upsert_source_document(db, security_id=security.id, document_type="earnings_transcript", source_provider="fmp", external_id="passage-call", content=source)
    calls = []
    monkeypatch.setattr(service, "resolved_setting_value", lambda *_: "test-key")
    def send(**kwargs):
        payload = kwargs["payload"]
        calls.append(payload)
        assert payload["text"]["format"]["strict"] is True
        choices = payload["text"]["format"]["schema"]["properties"]["events"]["items"]["properties"]["evidence_excerpt"]["enum"]
        event = {"category": "product_commercial", "event_type": "product_launch", "subject": "Product", "metric": None, "direction": "positive", "previous_text": None, "current_text": None, "headline": source, "summary": source, "evidence_excerpt": choices[0], "watch_item": "Approval next quarter", "confidence": "medium", "materiality": "medium"}
        return Response(json.dumps({"events": [event]}))
    monkeypatch.setattr(service, "audited_openai_request", send)
    assert extract_document_events(db, document=document, source_text=source)["events_written"] == 1
    assert extract_document_events(db, document=document, source_text=source)["status"] == "reused"
    assert len(calls) == 1
    row = db.query(ResearchEvidenceEvent).one()
    assert row.evidence_excerpt == source
    assert row.source_locator.endswith(f"chars:0-{len(source)}")
    assert row.prompt_version == service.EVIDENCE_EXTRACTION_PROMPT_VERSION


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


def test_semantic_output_cannot_inject_numeric_facts(db):
    security = seed_security(db)
    document, _ = upsert_source_document(db, security_id=security.id, document_type="press_release", source_provider="company_ir", external_id="unsafe-fields", content="Management announced revenue growth.")
    with pytest.raises(ValueError, match="unsupported evidence fields"):
        parse_semantic_events(parsed={"events": [{"evidence_excerpt": "Management announced revenue growth.", "current_value": 9000000}]}, document=document, source_text="Management announced revenue growth.")


def test_incomplete_response_cannot_be_marked_processed(db):
    security = seed_security(db)
    text = "Management made an update."
    document, _ = upsert_source_document(db, security_id=security.id, document_type="press_release", source_provider="company_ir", external_id="incomplete-doc", content=text)
    class IncompleteResponse(Response):
        def json(self): return {"status": "incomplete", "output_text": '{"events":[]}'}
    with pytest.raises(HTTPException):
        extract_document_events(db, document=document, source_text=text, request_sender=lambda: IncompleteResponse(""))
    assert document.processing_status == "failed"
    assert db.query(ResearchEvidenceEvent).count() == 0


def test_stale_worker_instance_rechecks_persisted_processing_status(db):
    security = seed_security(db)
    text = "Management made an update."
    document, _ = upsert_source_document(db, security_id=security.id, document_type="press_release", source_provider="company_ir", external_id="concurrent-doc", content=text)
    db.commit()
    # Keep the stale pending instance held by a worker while another completes.
    from sqlalchemy import update
    db.execute(update(ResearchSourceDocument).where(ResearchSourceDocument.id == document.id).values(processing_status="processed").execution_options(synchronize_session=False))
    assert document.processing_status == "pending"
    calls = []
    result = extract_document_events(db, document=document, source_text=text, request_sender=lambda: (calls.append(1) or Response('{"events":[]}')))
    assert result["status"] == "reused"
    assert calls == []


def test_in_progress_document_does_not_trigger_duplicate_request(db):
    security = seed_security(db)
    text = "Management made an update."
    document, _ = upsert_source_document(db, security_id=security.id, document_type="press_release", source_provider="company_ir", external_id="busy-doc", content=text)
    document.processing_status = "processing"
    db.commit()
    calls = []
    result = extract_document_events(db, document=document, source_text=text, request_sender=lambda: (calls.append(1) or Response('{"events":[]}')))
    assert result["status"] == "busy"
    assert calls == []


def test_source_identity_cannot_reassign_existing_document(db):
    security = seed_security(db)
    upsert_source_document(db, security_id=security.id, document_type="press_release", source_provider="company_ir", external_id="shared-url", content="Company A source statement.")
    with pytest.raises(ValueError, match="another security"):
        upsert_source_document(db, security_id=security.id + 1, document_type="press_release", source_provider="company_ir", external_id="shared-url", content="Company B source statement.")


def test_distinct_semantic_developments_with_same_metric_are_preserved(db):
    security = seed_security(db)
    text = "Factory A launched. Factory B launched."
    document, _ = upsert_source_document(db, security_id=security.id, document_type="press_release", source_provider="company_ir", external_id="two-launches", content=text)
    common = {"category": "company_operations", "event_type": "operational_milestone", "subject": "Micron", "metric": "production", "direction": "positive", "previous_text": None, "current_text": None, "headline": "Factory launched", "summary": "Production began.", "watch_item": None, "confidence": "high", "materiality": "medium"}
    events = parse_semantic_events(parsed={"events": [{**common, "evidence_excerpt": "Factory A launched."}, {**common, "evidence_excerpt": "Factory B launched."}]}, document=document, source_text=text)
    assert all(persist_event(db, event)[1] for event in events)
    assert db.query(ResearchEvidenceEvent).count() == 2
    assert events[0]["source_locator"] == f"document_text:{document.content_hash}"


def test_overlapping_workers_pay_for_only_one_extraction(tmp_path):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier

    engine = create_engine(f"sqlite+pysqlite:///{tmp_path / 'concurrent-evidence.db'}", connect_args={"check_same_thread": False, "timeout": 10})
    Base.metadata.create_all(engine, tables=[Security.__table__, ResearchSourceDocument.__table__, ResearchEvidenceEvent.__table__, ResearchExtractionChunk.__table__])
    text = "Management made an update."
    with Session(engine) as session:
        security = seed_security(session)
        document, _ = upsert_source_document(session, security_id=security.id, document_type="press_release", source_provider="company_ir", external_id="parallel-doc", content=text)
        session.commit()
        document_id = document.id
    ready = Barrier(2)
    calls = []
    def worker():
        with Session(engine) as session:
            document = session.get(ResearchSourceDocument, document_id)
            ready.wait(timeout=5)
            return extract_document_events(session, document=document, source_text=text, request_sender=lambda: (calls.append(1) or Response('{"events":[]}')))["status"]
    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(lambda _: worker(), range(2)))
        assert sorted(results) == ["processed", "reused"]
        assert calls == [1]
    finally:
        engine.dispose()


def test_full_source_sections_resume_without_repaying_and_publish_atomically(db):
    security = seed_security(db)
    source = ("Prepared remarks and financial context. " * 550) + "Question and answer: The product is delayed until next year."
    document, _ = upsert_source_document(db, security_id=security.id, document_type="earnings_transcript", source_provider="fmp", external_id="full-call", content=source)
    from app.services.research_evidence import source_sections
    assert source_sections(" ".join(source.split()))[-1][1].endswith("The product is delayed until next year.")
    allowed = iter([True, False])
    calls = []
    first = extract_document_events(db, document=document, source_text=source, consume_call=lambda: next(allowed), request_sender=lambda: calls.append(1) or Response('{"events":[]}'))
    assert first["status"] == "deferred"
    assert db.query(ResearchEvidenceEvent).count() == 0
    assert db.query(ResearchExtractionChunk).count() == 1
    final = extract_document_events(db, document=document, source_text=source, consume_call=lambda: True, request_sender=lambda: calls.append(1) or Response('{"events":[]}'))
    assert final["status"] == "processed"
    assert len(calls) == len(source_sections(" ".join(source.split())))


def test_corrected_document_supersedes_only_after_valid_replacement(db):
    security = seed_security(db)
    source = "Product launched today."
    document, _ = upsert_source_document(db, security_id=security.id, document_type="press_release", source_provider="fmp", external_id="corrected-release", content=source)
    value = {"category": "product_commercial", "event_type": "product_launch", "subject": "Product", "metric": None, "direction": "positive", "previous_text": None, "current_text": None, "headline": "Product launched", "summary": source, "evidence_excerpt": source, "watch_item": None, "confidence": "high", "materiality": "medium"}
    extract_document_events(db, document=document, source_text=source, request_sender=lambda: Response(json.dumps({"events": [value]})))
    original = db.query(ResearchEvidenceEvent).one()
    corrected = "Correction: the product has not launched."
    document, _ = upsert_source_document(db, security_id=security.id, document_type="press_release", source_provider="fmp", external_id="corrected-release", content=corrected)
    with pytest.raises(HTTPException):
        extract_document_events(db, document=document, source_text=corrected, request_sender=lambda: Response('{"events":[{"invalid":true}]}'))
    assert original.superseded_at is None
    extract_document_events(db, document=document, source_text=corrected, request_sender=lambda: Response('{"events":[]}'))
    db.refresh(original)
    assert original.superseded_at is not None
    assert query_events(db, security_id=security.id) == []
