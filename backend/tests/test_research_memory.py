import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

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
    InsightsSnapshot,
    PriceCache,
    QuoteCache,
    ResearchThesisMarketBaseline,
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
from app.services import research_memory
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
        InsightsSnapshot.__table__,
        PriceCache.__table__,
        QuoteCache.__table__,
        ResearchThesisMarketBaseline.__table__,
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


def _assert_strict_schema(schema):
    if schema.get("type") == "object":
        assert schema.get("additionalProperties") is False
        assert schema.get("properties")
        assert set(schema["required"]) == set(schema["properties"])
        for child in schema["properties"].values():
            _assert_strict_schema(child)
    elif schema.get("type") == "array":
        _assert_strict_schema(schema["items"])


def test_compiler_schema_defines_every_nested_object():
    schema = research_memory._compiler_schema()
    _assert_strict_schema(schema)
    # Each nested contract must cover the fields accepted by persistence.
    template = research_memory.validate_draft(template_draft("revenue_growth", symbol="MU"))
    for collection in ("claims", "catalysts", "risks", "invalidators"):
        item_schema = schema["properties"][collection]["items"]
        assert set(item_schema["required"]) == set(template[collection][0])
    assert schema["properties"]["invalidators"]["items"]["properties"]["threshold"]["type"] == ["string", "null"]


@pytest.mark.parametrize("quote_age", [None, timedelta(minutes=5), timedelta(days=2)])
@pytest.mark.parametrize("ticker,company,asset,symbol,group,provider_name,original", [
    ("BMNR", "BitMine Immersion Technologies", "Ethereum", "ETHUSD", "crypto", "coingecko", "Price of etheruem will continue in an uptrend"),
    ("NEM", "Newmont", "Gold", "GCUSD", "commodities", "silv", "Gold prices will continue in an uptrend"),
    ("FCX", "Freeport-McMoRan", "Copper", "HGUSD", "commodities", "silv", "Copper prices will rise and support the company"),
    ("PAAS", "Pan American Silver", "Silver", "SILUSD", "commodities", "silv", "Silver prices will continue rising"),
])
def test_external_asset_compile_reviews_and_saves_without_market_provider_calls(
    db, monkeypatch, quote_age, ticker, company, asset, symbol, group, provider_name, original,
):
    owner, _, security = _seed_user_and_security(db)
    security.symbol, security.name = ticker, company
    owner.manual_tier_override = "premium"
    if quote_age is not None:
        as_of = datetime.now(timezone.utc) - quote_age
        db.add(InsightsSnapshot(
            kind=f"insights-quote:{group}:{symbol}:{provider_name}", source="market_quote", fetched_at=as_of,
            payload_json=json.dumps({"symbol": symbol, "label": asset, "price": 2500,
                                     "change_percent": 2.5, "as_of": as_of.isoformat(), "status": "ok"}),
        ))
    db.commit()
    seed_feature_gates(db)
    output = {
        "title": f"{ticker}: {asset} uptrend", "summary": original, "orientation": "bullish", "target_horizon": None,
        "claims": [{"claim_type": "asset_price", "subject": asset, "metric": f"{symbol} price",
                    "expected_direction": "increase", "expected_magnitude": None, "expected_timeframe": None,
                    "importance": "high", "monitoring_mode": "manual", "coverage_level": "manual_review_required"}],
        "catalysts": [], "risks": [], "invalidators": [],
    }
    monkeypatch.setattr(research_memory, "resolved_setting_value", lambda *_: "test-key")

    def no_market_calls(*args, **kwargs):
        raise AssertionError("Compile must only read the prepared Insights cache")

    monkeypatch.setattr(research_memory.requests, "get", no_market_calls)

    def provider(**kwargs):
        payload = kwargs["payload"]
        _assert_strict_schema(payload["text"]["format"]["schema"])
        context = json.loads(payload["input"])
        assert context["user_thesis"] == original
        assert context["security"]["symbol"] == ticker
        quote = next(q for q in context["walnut_context"]["market_context"]["quotes"] if q["symbol"] == symbol)
        fresh = quote_age == timedelta(minutes=5)
        assert quote["group"] == group
        assert quote["price"] == (2500 if fresh else None)
        assert quote["status"] == ("available" if fresh else "unavailable")
        assert "Commodity and crypto price assumptions" in payload["instructions"]
        assert "manual_review_required" in payload["instructions"]
        assert payload["store"] is False
        return SimpleNamespace(status_code=200, json=lambda: {"status": "completed", "output": [
            {"type": "reasoning", "summary": []},
            {"type": "message", "content": [{"type": "output_text", "text": json.dumps(output)}]},
        ]})

    monkeypatch.setattr(research_memory, "audited_openai_request", provider)
    api = FastAPI()
    api.include_router(research_memory_router)
    from app.db import get_db
    api.dependency_overrides[get_db] = lambda: db
    client = TestClient(api)
    client.cookies.set(SESSION_COOKIE_NAME, sign_session_payload({"uid": owner.id, "email": owner.email}))
    response = client.post("/research-memory/compile", json={"ticker": ticker, "original_text": original})
    assert response.status_code == 200, response.text
    structure = response.json()["structure"]
    assert structure["original_text"] == original
    assert structure["claims"][0]["subject"] == asset
    assert structure["claims"][0]["metric"] == f"{symbol} price"
    assert structure["target_horizon"] is None
    assert structure["invalidators"] == []
    assert db.query(ResearchThesis).count() == 0  # Review precedes saving/activation.
    saved = client.post("/research-memory/drafts", json={"ticker": ticker, "structure": structure})
    assert saved.status_code == 200, saved.text
    assert saved.json()["status"] == "draft"
    assert saved.json()["claims"][0]["metric"] == f"{symbol} price"


@pytest.mark.parametrize("provider_output", [
    {"status": "incomplete", "output_text": "{}"},
    {"status": "completed", "output_text": "not json"},
    {"status": "completed", "output": [{"content": [{"type": "refusal", "refusal": "Unavailable"}]}]},
])
def test_compiler_unusable_response_is_retryable(db, monkeypatch, provider_output):
    _, _, security = _seed_user_and_security(db)
    monkeypatch.setattr(research_memory, "resolved_setting_value", lambda *_: "test-key")
    monkeypatch.setattr(research_memory, "audited_openai_request", lambda **_: SimpleNamespace(
        status_code=200, json=lambda: provider_output,
    ))
    with pytest.raises(HTTPException) as error:
        research_memory.compile_custom_thesis(db, security=security, original_text="Ethereum should rise")
    assert error.value.status_code == 502
    assert "Please retry" in error.value.detail


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
    owner.manual_tier_override = other.manual_tier_override = "premium"
    db.commit()
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
