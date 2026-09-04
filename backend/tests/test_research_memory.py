from datetime import datetime, timezone

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from fastapi import HTTPException
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from app.auth import SESSION_COOKIE_NAME, sign_session_payload
from app.db import Base
from app.entitlements import seed_feature_gates
from app.models import (
    AppSetting,
    ConfirmationScoreSnapshot,
    FeatureGate,
    FundamentalsCache,
    PlanLimit,
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
from app.routers.research_memory import router as research_memory_router
from app.services.openai_request_audit import _request_metadata
from app.services.research_memory import (
    activate,
    create_draft,
    owned_thesis,
    suggestions_for_security,
    template_draft,
    validate_draft,
)


@pytest.fixture()
def db():
    engine = create_engine("sqlite+pysqlite:///:memory:", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    tables = [
        UserAccount.__table__,
        AppSetting.__table__,
        FeatureGate.__table__,
        PlanLimit.__table__,
        Security.__table__,
        TickerMeta.__table__,
        FundamentalsCache.__table__,
        ConfirmationScoreSnapshot.__table__,
        ResearchThesis.__table__,
        ResearchThesisClaim.__table__,
        ResearchThesisCatalyst.__table__,
        ResearchThesisRisk.__table__,
        ResearchThesisInvalidator.__table__,
        TickerThesisSuggestion.__table__,
    ]
    Base.metadata.create_all(engine, tables=tables)
    session = Session(engine)
    try:
        yield session
    finally:
        session.close()
        engine.dispose()


def _seed_user_and_security(db: Session):
    first = UserAccount(email="first@example.com")
    second = UserAccount(email="second@example.com")
    security = Security(symbol="MU", name="Micron Technology", asset_class="Equity", sector="Technology")
    db.add_all([first, second, security])
    db.commit()
    return first, second, security


def test_template_draft_is_structured_and_has_no_invented_threshold():
    draft = template_draft("margin_expansion", symbol="MU", company_name="Micron")
    valid = validate_draft(draft, source_type="template")
    assert valid["claims"][0]["coverage_level"] == "partially_monitored"
    assert valid["invalidators"][0]["threshold"] is None


def test_compiler_output_rejects_a_threshold_not_present_in_user_text():
    draft = template_draft("margin_expansion", symbol="MU", company_name="Micron")
    draft["source_type"] = "custom"
    draft["original_text"] = "Margins should improve materially."
    draft["invalidators"][0]["threshold"] = "45%"
    try:
        validate_draft(draft, source_type="custom", compiler_output=True)
    except HTTPException as exc:
        assert exc.status_code == 422
    else:
        raise AssertionError("Generated numerical thresholds must not be invented.")


def test_unknown_coverage_is_rejected():
    draft = template_draft("revenue_growth", symbol="MU", company_name="Micron")
    draft["claims"][0]["coverage_level"] = "always_monitored"
    try:
        validate_draft(draft, source_type="template")
    except HTTPException as exc:
        assert exc.status_code == 422
    else:
        raise AssertionError("Unsupported coverage must fail validation.")


def test_draft_persists_children_and_explicit_activation(db: Session):
    owner, _other, security = _seed_user_and_security(db)
    structure = template_draft("margin_expansion", symbol="MU", company_name="Micron Technology")

    draft = create_draft(db, user=owner, security=security, structure=structure)

    assert draft["status"] == "draft"
    assert draft["security_id"] == security.id
    assert draft["invalidators"][0]["threshold"] is None
    assert db.query(ResearchThesisClaim).filter_by(thesis_id=draft["id"]).count() == 1
    assert db.query(ResearchThesisCatalyst).filter_by(thesis_id=draft["id"]).count() == 1
    assert db.query(ResearchThesisRisk).filter_by(thesis_id=draft["id"]).count() == 1
    assert db.query(ResearchThesisInvalidator).filter_by(thesis_id=draft["id"]).count() == 1

    active = activate(db, user=owner, thesis_id=draft["id"])
    assert active["status"] == "active"
    assert active["started_monitoring_at"]
    assert all(claim["user_confirmed"] for claim in active["claims"])


def test_ownership_scope_prevents_cross_user_detail_or_activation(db: Session):
    owner, other, security = _seed_user_and_security(db)
    draft = create_draft(db, user=owner, security=security, structure=template_draft("revenue_growth", symbol="MU"))

    with pytest.raises(HTTPException) as detail_error:
        owned_thesis(db, user=other, thesis_id=draft["id"])
    assert detail_error.value.status_code == 404

    with pytest.raises(HTTPException) as activation_error:
        activate(db, user=other, thesis_id=draft["id"])
    assert activation_error.value.status_code == 404


def test_suggestions_reuse_cache_and_change_when_evidence_changes(db: Session):
    _owner, _other, security = _seed_user_and_security(db)
    now = datetime.now(timezone.utc)
    db.add_all([
        TickerMeta(symbol="MU", company_name="Micron Technology", sector="Technology", industry="Semiconductors"),
        FundamentalsCache(symbol="MU", provider="test", fetched_at=now, revenue_growth=12.5, operating_margin_expansion=2.0),
    ])
    db.commit()

    first = suggestions_for_security(db, security=security)
    second = suggestions_for_security(db, security=security)
    assert first["items"]
    assert [item["id"] for item in first["items"]] == [item["id"] for item in second["items"]]
    assert first["evidence_state_hash"] == second["evidence_state_hash"]
    assert db.query(TickerThesisSuggestion).count() == len(first["items"])
    assert all("Walnut fundamentals show" in item["evidence_basis"][0] for item in first["items"])

    fundamentals = db.query(FundamentalsCache).one()
    fundamentals.fetched_at = datetime.now(timezone.utc)
    db.commit()
    unchanged_refresh = suggestions_for_security(db, security=security)
    assert unchanged_refresh["evidence_state_hash"] == first["evidence_state_hash"]

    fundamentals.revenue_growth = -1.0
    fundamentals.operating_margin_expansion = None
    fundamentals.fetched_at = datetime.now(timezone.utc)
    db.commit()
    changed = suggestions_for_security(db, security=security)
    assert changed["evidence_state_hash"] != first["evidence_state_hash"]
    assert changed["items"] == []


def test_research_memory_indexes_and_duplicate_constraint_are_declared():
    assert {index.name for index in ResearchThesis.__table__.indexes} == {
        "ix_research_theses_user_status_updated",
        "ix_research_theses_user_security_status",
    }
    assert {index.name for index in TickerThesisSuggestion.__table__.indexes} == {
        "ix_ticker_thesis_suggestions_lookup",
    }
    constraint_names = {constraint.name for constraint in TickerThesisSuggestion.__table__.constraints}
    assert "uq_ticker_thesis_suggestions_evidence" in constraint_names


def test_api_create_activate_and_cross_user_access_are_server_scoped(db: Session):
    owner, other, security = _seed_user_and_security(db)
    seed_feature_gates(db)
    api = FastAPI()
    api.include_router(research_memory_router)

    def get_test_db():
        yield db

    from app.db import get_db
    api.dependency_overrides[get_db] = get_test_db
    client = TestClient(api)
    structure = template_draft("revenue_growth", symbol="MU", company_name="Micron Technology")
    client.cookies.set(SESSION_COOKIE_NAME, sign_session_payload({"uid": owner.id, "email": owner.email}))
    created = client.post("/research-memory/drafts", json={"security_id": security.id, "structure": structure})
    assert created.status_code == 200
    thesis_id = created.json()["id"]
    assert created.json()["status"] == "draft"

    client.cookies.set(SESSION_COOKIE_NAME, sign_session_payload({"uid": other.id, "email": other.email}))
    assert client.get(f"/research-memory/{thesis_id}").status_code == 404
    assert client.post(f"/research-memory/{thesis_id}/activate").status_code == 404

    client.cookies.set(SESSION_COOKIE_NAME, sign_session_payload({"uid": owner.id, "email": owner.email}))
    activated = client.post(f"/research-memory/{thesis_id}/activate")
    assert activated.status_code == 200
    assert activated.json()["status"] == "active"


def test_openai_audit_metadata_does_not_store_private_thesis_prose():
    metadata = _request_metadata({"input": "My private thesis text must not be logged.", "model": "test", "store": False})
    assert metadata["input_chars"] > 0
    assert "private thesis" not in str(metadata).lower()
    assert "input" not in metadata
    FeatureGate,
    PlanLimit,
