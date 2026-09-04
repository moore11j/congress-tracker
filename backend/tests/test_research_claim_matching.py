from datetime import datetime, timedelta, timezone

import pytest
import requests
from fastapi import HTTPException
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from app.db import Base
from app.models import (
    ResearchClaimEvidenceMatch, ResearchClaimMatchCheckpoint, ResearchEvidenceEvent,
    ResearchInvalidatorEvidenceMatch, ResearchThesis, ResearchThesisClaim,
    ResearchThesisInvalidator, Security, UserAccount,
)
from app.services.research_claim_matching import process_event_matches, query_matches, semantic_match


@pytest.fixture()
def db():
    engine = create_engine("sqlite+pysqlite:///:memory:", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    Base.metadata.create_all(engine, tables=[
        UserAccount.__table__, Security.__table__, ResearchThesis.__table__, ResearchThesisClaim.__table__, ResearchThesisInvalidator.__table__,
        ResearchEvidenceEvent.__table__, ResearchClaimEvidenceMatch.__table__, ResearchClaimMatchCheckpoint.__table__, ResearchInvalidatorEvidenceMatch.__table__,
    ])
    session = Session(engine)
    try: yield session
    finally: session.close(); engine.dispose()


def seed(db):
    user, other = UserAccount(email="owner@example.test"), UserAccount(email="other@example.test")
    mu, nvda = Security(symbol="MU", name="Micron", asset_class="Equity", sector="Technology"), Security(symbol="NVDA", name="Nvidia", asset_class="Equity", sector="Technology")
    db.add_all([user, other, mu, nvda]); db.commit()
    return user, other, mu, nvda


def thesis(db, user, security, *, status="active", started=None):
    row = ResearchThesis(id=f"thesis-{user.id}-{security.id}-{status}-{db.query(ResearchThesis).count()}", user_id=user.id, security_id=security.id, ticker_at_creation=security.symbol, title="Margin thesis", summary="Structured claim", orientation="bullish", status=status, source_type="custom", started_monitoring_at=started)
    db.add(row); db.flush(); return row


def claim(db, thesis_row, *, metric="gross margin", direction="increase", mode="structured_metric", coverage="fully_monitored", subject="Micron"):
    row = ResearchThesisClaim(id=f"claim-{db.query(ResearchThesisClaim).count()}", thesis_id=thesis_row.id, claim_type="margin_expansion", subject=subject, metric=metric, expected_direction=direction, importance="high", monitoring_mode=mode, coverage_level=coverage, user_confirmed=True)
    db.add(row); db.flush(); return row


def event(db, security, *, metric="gross_margin", event_type="margin_expanded", direction="positive", published=None, summary="Gross margin expanded from 40 to 42."):
    row = ResearchEvidenceEvent(id=f"event-{db.query(ResearchEvidenceEvent).count()}", security_id=security.id, event_type=event_type, category="financial", subject=security.name, metric=metric, direction=direction, previous_value=40, current_value=42, source_type="fundamentals_snapshot", source_provider="fmp", source_id=f"source-{db.query(ResearchEvidenceEvent).count()}", source_locator="test", headline="Gross margin expanded", summary=summary, confidence="high", materiality="medium", extraction_method="deterministic", schema_version="evidence-v1", processing_version="evidence-processing-v1", content_hash=f"hash-{db.query(ResearchEvidenceEvent).count()}", published_at=published or datetime.now(timezone.utc))
    db.add(row); db.commit(); return row


def test_deterministic_support_contradiction_and_unrelated_metric(db):
    user, _other, mu, _nvda = seed(db); started = datetime.now(timezone.utc) - timedelta(days=1)
    first = thesis(db, user, mu, started=started); supports = claim(db, first, direction="increase")
    second = thesis(db, user, mu, started=started); contradicts = claim(db, second, direction="decrease")
    third = thesis(db, user, mu, started=started); claim(db, third, metric="revenue growth", direction="increase")
    db.commit(); evidence = event(db, mu)
    result = process_event_matches(db, event=evidence)
    assert result["matches"] == 2
    rows = {row.claim_id: row.relationship for row in db.query(ResearchClaimEvidenceMatch).all()}
    assert rows[supports.id] == "supports"
    assert rows[contradicts.id] == "contradicts"
    assert db.query(ResearchClaimMatchCheckpoint).count() == 0


def test_security_status_and_time_filters(db):
    user, _other, mu, nvda = seed(db); now = datetime.now(timezone.utc)
    active = thesis(db, user, mu, started=now - timedelta(days=1)); claim(db, active)
    draft = thesis(db, user, mu, status="draft", started=now - timedelta(days=1)); claim(db, draft)
    paused = thesis(db, user, mu, status="paused", started=now - timedelta(days=1)); claim(db, paused)
    old = thesis(db, user, mu, started=now + timedelta(days=1)); claim(db, old)
    wrong_security = thesis(db, user, nvda, started=now - timedelta(days=1)); claim(db, wrong_security)
    db.commit(); evidence = event(db, mu, published=now)
    process_event_matches(db, event=evidence)
    assert [row.thesis_id for row in db.query(ResearchClaimEvidenceMatch).all()] == [active.id]


class Response:
    status_code = 200
    def __init__(self, value): self.value = value
    def json(self): return {"output_text": self.value}


def test_semantic_match_checkpoint_and_error_handling(db):
    user, _other, mu, _nvda = seed(db); active = thesis(db, user, mu, started=datetime.now(timezone.utc) - timedelta(days=1))
    semantic_claim = claim(db, active, metric="HBM demand", direction="increase", mode="semantic", coverage="partially_monitored", subject="HBM demand")
    db.commit(); evidence = event(db, mu, metric="capacity_commitment", event_type="metric_increased", summary="HBM capacity commitments now extend through 2027.")
    calls = []
    payload = '{"relationship":"supports","relevance":"high","confidence":"high","reason":"HBM capacity commitments directly support sustained HBM demand."}'
    first = process_event_matches(db, event=evidence, request_sender=lambda: (calls.append(1) or Response(payload)))
    second = process_event_matches(db, event=evidence, request_sender=lambda: (calls.append(1) or Response(payload)))
    assert first["matches"] == 1 and second["semantic"] == 0 and len(calls) == 1
    assert db.query(ResearchClaimEvidenceMatch).filter_by(claim_id=semantic_claim.id).one().match_method == "semantic"
    with pytest.raises(HTTPException) as malformed:
        semantic_match(db, claim=semantic_claim, event=evidence, security=mu, request_sender=lambda: Response('{"relationship":"bogus"}'))
    assert malformed.value.status_code == 502
    with pytest.raises(HTTPException) as timeout:
        semantic_match(db, claim=semantic_claim, event=evidence, security=mu, request_sender=lambda: (_ for _ in ()).throw(requests.Timeout()))
    assert timeout.value.status_code == 504


def test_unrelated_semantic_result_is_checkpointed_not_persisted(db):
    user, _other, mu, _nvda = seed(db); active = thesis(db, user, mu, started=datetime.now(timezone.utc) - timedelta(days=1))
    claim(db, active, metric="HBM demand", direction="increase", mode="semantic", coverage="partially_monitored", subject="HBM demand")
    db.commit(); evidence = event(db, mu, metric="capacity_commitment", summary="HBM capacity commitments now extend through 2027.")
    process_event_matches(db, event=evidence, request_sender=lambda: Response('{"relationship":"unrelated","relevance":"low","confidence":"medium","reason":"No direct connection."}'))
    assert db.query(ResearchClaimEvidenceMatch).count() == 0
    assert db.query(ResearchClaimMatchCheckpoint).one().outcome == "unrelated"


def test_potential_invalidator_preserves_null_threshold_and_never_changes_thesis_state(db):
    user, _other, mu, _nvda = seed(db); active = thesis(db, user, mu, started=datetime.now(timezone.utc) - timedelta(days=1))
    invalidator = ResearchThesisInvalidator(id="invalidator-1", thesis_id=active.id, description="Gross margin decreases materially.", condition_type="semantic", metric="gross margin", severity="high", monitoring_mode="structured_metric")
    db.add(invalidator); db.commit()
    evidence = event(db, mu, event_type="margin_compressed", direction="negative", summary="Gross margin compressed.")
    result = process_event_matches(db, event=evidence)
    match = db.query(ResearchInvalidatorEvidenceMatch).one()
    assert result["invalidators"] == 1
    assert match.relationship == "potential_invalidator"
    assert '"threshold":null' in match.invalidator_snapshot_json
    assert db.get(ResearchThesis, active.id).status == "active"


def test_private_queries_and_snapshots_remain_owner_scoped(db):
    user, other, mu, _nvda = seed(db); active = thesis(db, user, mu, started=datetime.now(timezone.utc) - timedelta(days=1)); original = claim(db, active, direction="increase"); db.commit()
    evidence = event(db, mu); process_event_matches(db, event=evidence)
    original.expected_direction = "decrease"; db.commit()
    owner_rows = query_matches(db, user=user, thesis_id=active.id)
    other_rows = query_matches(db, user=other, thesis_id=active.id)
    assert len(owner_rows) == 1 and other_rows == []
    assert owner_rows[0]["claim_snapshot"]["expected_direction"] == "increase"
    assert "user_id" not in ResearchEvidenceEvent.__table__.c
