import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.auth import SESSION_COOKIE_NAME, sign_session_payload
from app.db import get_db
from app.entitlements import ENTITLEMENTS, entitlements_for_user, seed_feature_gates
from app.models import FeatureGate, ResearchThesis
from app.routers.operational_intelligence import router as operational_router
from app.routers.research_memory import router as memory_router
from app.services.research_memory import create_draft, template_draft
from test_research_memory import db, _seed_user_and_security


@pytest.mark.parametrize("tier,status", [(None, 401), ("free", 402), ("premium", 200), ("pro", 200), ("admin", 200), ("suspended", 402)])
def test_research_and_operational_api_require_premium(db, monkeypatch, tier, status):
    from app.routers import operational_intelligence as route
    owner, _other, security = _seed_user_and_security(db)
    if tier in {"premium", "pro"}: owner.manual_tier_override = tier
    if tier == "admin": owner.role = "admin"
    if tier == "suspended":
        owner.manual_tier_override = "premium"
        owner.is_suspended = True
    db.commit()
    seed_feature_gates(db)
    # Simulate already-seeded Phase 1 Free gates: the Premium floor must win.
    for feature in ["view_research_memory", "create_research_memory", "use_custom_thesis_ai", "monitor_research_memory"]:
        db.get(FeatureGate, feature).required_tier = "free"
    db.commit()
    called = []
    monkeypatch.setattr(route, "ticker_operational_intelligence", lambda *_args, **_kwargs: called.append(True) or {"symbol": "MU", "catalysts": [{"title": "Premium finding"}]})
    monkeypatch.setattr(route, "operational_intelligence_enabled", lambda: True)
    api = FastAPI()
    api.include_router(memory_router)
    api.include_router(operational_router)
    api.dependency_overrides[get_db] = lambda: db
    with TestClient(api) as client:
        if tier:
            client.cookies.set(SESSION_COOKIE_NAME, sign_session_payload({"uid": owner.id, "email": owner.email}))
        result = client.get("/tickers/MU/operational-intelligence")
        # Suspended sessions may be rejected by authentication before the gate.
        expected = {401, 402, 403} if tier == "suspended" else {status}
        assert result.status_code in expected
        assert client.get("/research-memory").status_code in expected
        if status == 200:
            assert called == [True]
            assert result.headers["cache-control"] == "private, no-store"
        else:
            assert not called
            assert "Premium finding" not in result.text


def test_free_cannot_bypass_gate_through_creation_or_owned_detail(db):
    owner, _other, security = _seed_user_and_security(db)
    structure = template_draft("revenue_growth", symbol="MU", company_name="Micron")
    existing = create_draft(db, user=owner, security=security, structure=structure)
    seed_feature_gates(db)
    api = FastAPI()
    api.include_router(memory_router)
    api.dependency_overrides[get_db] = lambda: db
    with TestClient(api) as client:
        client.cookies.set(SESSION_COOKIE_NAME, sign_session_payload({"uid": owner.id, "email": owner.email}))
        paths = ["", "/templates", "/templates/revenue_growth/draft?ticker=MU", "/suggestions/MU", "/suggestions/MU/fake/draft", "/ticker/MU/active", f"/{existing['id']}", f"/{existing['id']}/matches", f"/{existing['id']}/invalidator-matches"]
        for path in paths:
            assert client.get(f"/research-memory{path}").status_code == 402, path
        for path, payload in [("/compile", {"ticker": "MU", "original_text": "Margins improve"}), ("/drafts", {"security_id": security.id, "structure": structure}), (f"/{existing['id']}/activate", {})]:
            assert client.post(f"/research-memory{path}", json=payload).status_code == 402, path
        assert client.put(f"/research-memory/{existing['id']}", json={"structure": structure}).status_code == 402
    assert db.query(ResearchThesis).count() == 1
    assert db.get(ResearchThesis, existing["id"]).status == "draft"


def test_default_and_seeded_entitlements_keep_research_premium(db):
    owner, _other, _security = _seed_user_and_security(db)
    seed_feature_gates(db)
    for key in ["view_research_memory", "create_research_memory", "use_custom_thesis_ai", "monitor_research_memory", "receive_thesis_alerts"]:
        db.get(FeatureGate, key).required_tier = "free"
        assert not ENTITLEMENTS["free"].has_feature(key)
        assert ENTITLEMENTS["premium"].has_feature(key)
    db.commit()
    assert not entitlements_for_user(db, owner).has_feature("view_research_memory")
