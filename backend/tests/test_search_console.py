import json
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qs, urlparse

import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from app.db import Base
from app.models import UserAccount
from app.services import search_console as gsc
from app.services.research_seo import rank_candidates

@pytest.fixture
def db(monkeypatch):
    monkeypatch.setenv("APP_SESSION_SECRET", "test-secret-" * 5)
    monkeypatch.setenv("GOOGLE_CLIENT_ID", "test-client")
    monkeypatch.setenv("GOOGLE_CLIENT_SECRET", "test-client-secret")
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        session.add(UserAccount(id=1, email="admin@example.com", role="admin"))
        session.commit()
        gsc.ensure_schema(session)
        yield session
    engine.dispose()


def connect(db, monkeypatch, **token_changes):
    admin = db.get(UserAccount, 1)
    url = gsc.start_connection(db, admin)["authorization_url"]
    state = parse_qs(urlparse(url).query)["state"][0]
    token = {"access_token": "access-secret", "refresh_token": "refresh-secret", "scope": gsc.SCOPE, **token_changes}
    def request(method, url, **kwargs):
        if url.endswith("/token"):
            assert kwargs["data"]["code_verifier"]
            return token
        if url.endswith("/userinfo"):
            return {"email_verified": True, "email": "moore11j@gmail.com"}
        assert url == gsc.API
        return {"permissionLevel": "siteOwner"}
    monkeypatch.setattr(gsc, "_request", request)
    return gsc.complete_connection(db, admin, "test-code", state), state


def test_encrypted_readonly_offline_admin_bound_one_use_flow(db, monkeypatch):
    url = gsc.start_connection(db, db.get(UserAccount, 1))["authorization_url"]
    query = parse_qs(urlparse(url).query)
    assert query["scope"] == ["openid email " + gsc.SCOPE]
    assert query["access_type"] == ["offline"]
    assert query["code_challenge_method"] == ["S256"]
    assert "gmail" not in query["scope"][0]
    other = UserAccount(id=2, email="other@example.com", role="admin")
    db.add(other); db.commit()
    with pytest.raises(HTTPException):
        gsc.complete_connection(db, other, "code", query["state"][0])
    status, state = connect(db, monkeypatch)
    assert status["connected"] and status["email"] == "moore11j@gmail.com"
    stored = db.execute(text("SELECT refresh_encrypted FROM research_gsc_connection")).scalar()
    assert "refresh-secret" not in stored
    assert gsc._cipher().decrypt(stored.encode()) == b"refresh-secret"
    assert "secret" not in json.dumps(status)
    with pytest.raises(HTTPException):
        gsc.complete_connection(db, db.get(UserAccount, 1), "code", state)


def test_missing_scope_cannot_replace_connection(db, monkeypatch):
    with pytest.raises(HTTPException):
        connect(db, monkeypatch, scope="openid email")
    assert not gsc.get_status(db)["connected"]


def test_expired_state_fails_before_network(db, monkeypatch):
    admin = db.get(UserAccount, 1)
    state = parse_qs(urlparse(gsc.start_connection(db, admin)["authorization_url"]).query)["state"][0]
    db.execute(text("UPDATE research_gsc_oauth SET expires_at='2000-01-01'")); db.commit()
    monkeypatch.setattr(gsc, "_request", lambda *a, **k: pytest.fail("expired OAuth must not call Google"))
    with pytest.raises(HTTPException):
        gsc.complete_connection(db, admin, "code", state)


def test_sync_final_dates_once_daily_and_keep_good_snapshot_on_failure(db, monkeypatch):
    connect(db, monkeypatch)
    now = datetime.now(timezone.utc)
    calls = []
    def request(method, url, **kwargs):
        calls.append((url, kwargs))
        if url.endswith("/token"):
            assert kwargs["data"]["refresh_token"] == "refresh-secret"
            return {"access_token": "access"}
        payload = kwargs["json"]
        assert payload["dataState"] == "final" and payload["type"] == "web"
        assert payload["endDate"] <= str(now.astimezone(gsc.ZoneInfo("America/Los_Angeles")).date()-timedelta(days=3))
        key = "nvda institutional holders" if payload["dimensions"] == ["query"] else "https://walnutmarkets.com/research/nvda"
        return {"rows": [{"keys": [key], "impressions": 500, "clicks": 5, "ctr": .01, "position": 8}]}
    monkeypatch.setattr(gsc, "_request", request)
    assert gsc.sync(db, now=now)["status"] == "synced"
    assert len(calls) == 4
    assert gsc.sync(db, now=now)["status"] == "already_attempted"
    assert gsc.sync(db, now=now, force=True)["status"] == "already_attempted"
    status = gsc.get_status(db)
    assert len(status["recommendations"]) == 1
    assert status["keyword_volume_connected"] is False
    assert gsc.planning_signals(db)["queries"][0]["impressions"] == 500
    def fail(*a, **k):
        raise HTTPException(409, "Reconnect Google")
    monkeypatch.setattr(gsc, "_request", fail)
    assert gsc.sync(db, now=now+timedelta(days=1))["status"] == "failed"
    assert gsc.get_status(db)["queries"] == status["queries"]
    assert gsc.planning_signals(db) == {}
    gsc.disconnect(db)
    assert gsc.sync(db)["status"] == "not_connected"


def test_http_errors_do_not_expose_tokens_or_provider_body(monkeypatch):
    class Response:
        status_code = 401
        def json(self): return {"secret": "do-not-leak"}
    monkeypatch.setattr(gsc.requests, "request", lambda *a, **k: Response())
    with pytest.raises(HTTPException) as error:
        gsc._request("GET", "https://example.test")
    assert "do-not-leak" not in str(error.value.detail)


def test_measured_bonus_requires_exact_query_and_editorial_threshold():
    candidate = {"content_type": "ticker", "ticker": "NVDA", "target_keyword": "NVDA holders", "source_urls": ["https://sec.gov"], "walnut_angle": "Named buyers", "opportunity_score": 75}
    google = {"queries": [{"query": "nvda holders", "impressions": 500}]}
    assert rank_candidates([candidate], [], [], 70, google)[0]["priority_score"] == 85
    assert rank_candidates([{**candidate, "opportunity_score": 65}], [], [], 70, google) == []
    assert rank_candidates([{**candidate, "target_keyword": "AVGO holders"}], [], [], 70, google)[0]["priority_score"] == 75


def test_stale_snapshot_is_not_used(db, monkeypatch):
    connect(db, monkeypatch)
    db.execute(text("UPDATE research_gsc_connection SET last_sync_at='2000-01-01T00:00:00+00:00', snapshot_json=:s"), {"s": json.dumps({"queries": [{"query": "nvda", "impressions": 100}]})}); db.commit()
    assert gsc.planning_signals(db) == {}


def test_revoked_admin_cannot_sync(db, monkeypatch):
    connect(db, monkeypatch)
    db.get(UserAccount, 1).role = "user"
    db.commit()
    monkeypatch.setattr(gsc, "_request", lambda *a, **k: pytest.fail("No calls after admin revocation"))
    assert gsc.sync(db)["status"] == "failed"
    assert "inactive" in gsc.get_status(db)["error"]


def test_key_rotation_requires_reconnect(db, monkeypatch):
    connect(db, monkeypatch)
    monkeypatch.setenv("APP_SESSION_SECRET", "changed-secret-" * 5)
    assert gsc.sync(db)["status"] == "failed"
    assert "encryption key changed" in gsc.get_status(db)["error"]


def test_pagination_is_bounded_and_reports_cap(monkeypatch):
    calls = []
    row = {"keys": ["NVDA"], "impressions": 10, "clicks": 1, "ctr": .1, "position": 2}
    def request(*args, **kwargs):
        calls.append(kwargs["json"]["startRow"])
        return {"rows": [row] * 25000}
    monkeypatch.setattr(gsc, "_request", request)
    rows, capped = gsc._rows({}, "2026-08-01", "2026-08-28", "query")
    assert calls == [0, 25000] and len(rows) == 50000 and capped


def test_admin_routes_deny_anonymous_access(db):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from app.db import get_db
    from app.routers.research_briefs import router
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: db
    with TestClient(app) as client:
        for path, body in [("connect", None), ("sync", None), ("callback", {"code": "code", "state": "gsc_" + "x" * 40})]:
            response = client.post("/admin/research-briefs/search-console/" + path, json=body)
            assert response.status_code in {401, 403}
        assert client.delete("/admin/research-briefs/search-console").status_code in {401, 403}
