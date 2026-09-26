import json
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.db import get_db
from app.entitlements import ENTITLEMENTS
from app.services.ranking_access import project_ranking


def snapshot():
    rows = [{"rank": i, "symbol": f"STOCK{i}", "company_name": f"Company {i}", "name": f"Person {i}",
             "key_drivers": ["Insiders"], "why_ranked": "Strong confirmation in available evidence",
             "confirmation_score": 98.7654, "why_this_ranked": [{"source": "Insiders", "summary": "PAID_EVIDENCE"}],
             "institutional_data": "PRO_SECRET"} for i in range(1, 31)]
    return {"items": rows, "filter_items": {"all": rows}, "candidate_rows": rows,
            "metadata": {"winner": "STOCK1"}, "generated_at": "2026-09-25T20:00:00Z"}


@pytest.mark.parametrize("tier,expected", [(None, [3, 4, 5]), ("free", list(range(1, 6))), ("premium", list(range(1, 11))), ("pro", list(range(1, 26)))])
@pytest.mark.parametrize("path", ["/top-stocks", "/leaderboards/dashboard", "/leaderboards/top-stocks"])
def test_all_stock_http_routes_enforce_identity_rank_and_evidence_access(monkeypatch, tier, expected, path):
    from app.routers import leaderboards, top_stocks
    app = FastAPI()
    app.include_router(leaderboards.router)
    app.include_router(top_stocks.router)
    app.dependency_overrides[get_db] = lambda: None
    for module in [leaderboards, top_stocks]:
        monkeypatch.setattr(module, "current_user", lambda *a: SimpleNamespace(id=1) if tier else None)
        monkeypatch.setattr(module, "current_entitlements", lambda *a: ENTITLEMENTS[tier or "free"])
        monkeypatch.setattr(module, "build_top_stocks_response", lambda *a, **k: snapshot())
    monkeypatch.setattr(leaderboards, "read_leaderboard_snapshot", lambda *a: snapshot())
    response = TestClient(app).get(path)
    assert response.status_code == 200
    assert response.headers["cache-control"] == "private, no-store"
    payload = response.json()
    data = payload["top_stocks"] if path.endswith("dashboard") else payload
    assert [r["rank"] for r in data["items"]] == expected
    if tier in {None, "free"}:
        assert "PAID_EVIDENCE" not in json.dumps(payload)
        assert "PRO_SECRET" not in json.dumps(payload)
        assert "confirmation_score" not in json.dumps(payload)
        assert "filter_items" not in json.dumps(payload)
    if tier is None:
        assert "STOCK1\"" not in json.dumps(payload)
        assert "STOCK2\"" not in json.dumps(payload)
        assert "Person 1\"" not in json.dumps(payload)
        assert data["locked_ranks"] == [1, 2]


def test_public_preview_drops_winners_from_every_section_and_unknown_metadata(monkeypatch):
    from app.routers import leaderboards
    app = FastAPI()
    app.include_router(leaderboards.router)
    app.dependency_overrides[get_db] = lambda: None
    monkeypatch.setattr(leaderboards, "build_top_stocks_response", lambda *a: snapshot())
    monkeypatch.setattr(leaderboards, "read_leaderboard_snapshot", lambda *a: snapshot())
    response = TestClient(app).get("/leaderboards/preview")
    for section in ["top_stocks", "congress", "insiders", "institutions"]:
        result = response.json()[section]
        assert [row["rank"] for row in result["items"]] == [3, 4, 5]
        assert "metadata" not in result
        assert "candidate_rows" not in result
        assert "filter_items" not in result
    assert response.headers["cache-control"] == "no-store"
    assert "STOCK1\"" not in response.text
    assert "PAID_EVIDENCE" not in response.text


def test_sparse_and_malformed_ranks_are_not_renumbered_or_backfilled():
    data = {"items": [{"rank": True, "symbol": "HIDDEN"}, {"rank": 4, "symbol": "FOUR"}, {"rank": 6, "symbol": "SIX"}]}
    assert project_ranking(data, authenticated=False, stocks=True)["items"] == [{"rank": 4, "symbol": "FOUR"}]


@pytest.mark.parametrize("section", ["congress_members", "insiders", "institutions"])
@pytest.mark.parametrize("tier", [None, "free"])
def test_direct_participant_endpoints_apply_discovery_projection(monkeypatch, section, tier):
    from app.routers import leaderboards
    app = FastAPI()
    app.include_router(leaderboards.router)
    app.dependency_overrides[get_db] = lambda: None
    monkeypatch.setattr(leaderboards, "current_user", lambda *a: SimpleNamespace(id=1) if tier else None)
    monkeypatch.setattr(leaderboards, "current_entitlements", lambda *a: ENTITLEMENTS["free"])
    monkeypatch.setattr(leaderboards, "read_leaderboard_snapshot", lambda *a: snapshot())
    data = TestClient(app).get(f"/leaderboards/{section}").json()
    assert [r["rank"] for r in data["items"]] == ([1, 2, 3, 4, 5] if tier else [3, 4, 5])
    assert "PRO_SECRET" not in json.dumps(data)


def test_real_signed_session_unlocks_free_but_invalid_or_expired_sessions_do_not(monkeypatch):
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session
    from sqlalchemy.pool import StaticPool
    from app.auth import SESSION_COOKIE_NAME, sign_session_payload
    from app.db import Base
    from app.models import UserAccount
    from app.routers import top_stocks
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    Base.metadata.create_all(engine)
    app = FastAPI()
    app.include_router(top_stocks.router)
    monkeypatch.setattr(top_stocks, "build_top_stocks_response", lambda *a, **k: snapshot())
    with Session(engine) as db:
        user = UserAccount(email="session@example.test")
        db.add(user)
        db.commit()
        app.dependency_overrides[get_db] = lambda: db
        for token, expected in [("invalid", [3, 4, 5]), (sign_session_payload({"uid": user.id, "exp": 1}), [3, 4, 5]), (sign_session_payload({"uid": user.id}), [1, 2, 3, 4, 5])]:
            with TestClient(app) as client:
                client.cookies.set(SESSION_COOKIE_NAME, token)
                response = client.get("/top-stocks")
                assert response.status_code == 200
                assert [r["rank"] for r in response.json()["items"]] == expected
                assert "PAID_EVIDENCE" not in response.text
