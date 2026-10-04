import json
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qs, urlparse

import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from app.db import Base
from app.models import UserAccount
from app.services import keyword_planner as kp
from app.services.research_seo import rank_candidates


@pytest.fixture
def db(monkeypatch):
    monkeypatch.setenv("APP_SESSION_SECRET", "test-secret-" * 5)
    monkeypatch.setenv("GOOGLE_CLIENT_ID", "test-client")
    monkeypatch.setenv("GOOGLE_CLIENT_SECRET", "test-secret")
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        session.add(UserAccount(id=1, email="admin@example.com", role="admin"))
        session.commit()
        kp.ensure_schema(session)
        yield session
    engine.dispose()


def result(keyword="institutional ownership", volume="1000"):
    return {"results": [{"text": keyword, "closeVariants": ["institutional owners"],
                         "keywordMetrics": {"avgMonthlySearches": volume, "monthlySearchVolumes": [
                             {"year": "2026", "month": "AUGUST", "monthlySearches": "1200"},
                             {"year": "2026", "month": "JULY"}]}}]}


def seed(db):
    db.execute(text("INSERT INTO research_ads_connection (id,owner_id,email,refresh_encrypted,connected_at) VALUES (1,1,'admin@example.com',:token,:now)"),
               {"token": kp._cipher().encrypt(b"refresh-secret").decode(), "now": datetime.now(timezone.utc).isoformat()})
    db.commit()


def test_oauth_separate_encrypted_admin_bound_single_use(db, monkeypatch):
    admin = db.get(UserAccount, 1)
    query = parse_qs(urlparse(kp.start_connection(db, admin)["authorization_url"]).query)
    state = query["state"][0]
    assert state.startswith("gads_") and query["scope"] == ["openid email " + kp.SCOPE]
    assert query["code_challenge_method"] == ["S256"] and query["access_type"] == ["offline"]
    row = db.execute(text("SELECT * FROM research_ads_oauth")).mappings().one()
    assert state not in str(row) and row["verifier_encrypted"] != kp._decrypt(row["verifier_encrypted"])
    with pytest.raises(HTTPException):
        kp.complete_connection(db, UserAccount(id=2), "code", state)
    def request(method, url, **kwargs):
        if url.endswith("/token"):
            return {"access_token": "access", "refresh_token": "refresh-secret", "scope": kp.SCOPE}
        if url.endswith("/userinfo"):
            return {"email_verified": True, "email": "moore11j@gmail.com"}
        if url == kp.ACCOUNT:
            return {"results": [{"customer": {"id": kp.CUSTOMER}}]}
        assert url == kp.HISTORICAL and kwargs["json"]["keywordPlanNetwork"] == "GOOGLE_SEARCH"
        assert kwargs["json"]["geoTargetConstants"] == ["geoTargetConstants/2840"]
        return result()
    monkeypatch.setattr(kp, "_request", request)
    status = kp.complete_connection(db, admin, "code", state)
    assert status["connected"] and status["last_sync_at"] and "refresh-secret" not in json.dumps(status)
    assert "refresh-secret" not in str(db.execute(text("SELECT * FROM research_ads_connection")).first())
    with pytest.raises(HTTPException):
        kp.complete_connection(db, admin, "code", state)


@pytest.mark.parametrize("failure", ["scope", "account", "keyword_access"])
def test_connection_requires_scope_account_and_keyword_access(db, monkeypatch, failure):
    admin = db.get(UserAccount, 1)
    state = parse_qs(urlparse(kp.start_connection(db, admin)["authorization_url"]).query)["state"][0]
    def request(method, url, **kwargs):
        if url.endswith("/token"):
            return {"access_token": "access", "refresh_token": "refresh", "scope": "openid" if failure == "scope" else kp.SCOPE}
        if url.endswith("/userinfo"):
            return {"email_verified": True, "email": "admin@example.com"}
        if url == kp.ACCOUNT:
            return {"results": [{"customer": {"id": "other" if failure == "account" else kp.CUSTOMER}}]}
        raise HTTPException(403, "Denied")
    monkeypatch.setattr(kp, "_request", request)
    with pytest.raises(HTTPException):
        kp.complete_connection(db, admin, "code", state)
    assert not kp.get_status(db)["connected"]


def test_variants_null_zero_and_metadata_are_preserved():
    now = datetime.now(timezone.utc)
    rows = kp.parse_metrics(result(), ["institutional owners", "unrelated"], now)
    assert rows["institutional owners"]["avg_monthly_searches"] == 1000
    assert rows["unrelated"]["avg_monthly_searches"] is None
    assert rows["institutional owners"]["monthly_searches"][1]["searches"] is None
    assert kp.parse_metrics(result(volume="0"), ["institutional ownership"], now)["institutional ownership"]["avg_monthly_searches"] == 0
    assert kp.parse_metrics(result(volume=None), ["institutional ownership"], now)["institutional ownership"]["avg_monthly_searches"] is None


def test_cache_bounds_retries_and_excludes_stale(db, monkeypatch):
    seed(db)
    now = datetime.now(timezone.utc)
    calls = []
    def request(method, url, **kwargs):
        calls.append(url)
        return {"access_token": "access"} if url.endswith("/token") else result()
    monkeypatch.setattr(kp, "_request", request)
    assert kp.lookup(db, ["institutional ownership"], now=now)["institutional ownership"]["avg_monthly_searches"] == 1000
    kp.lookup(db, ["institutional ownership"], now=now+timedelta(days=1))
    assert len(calls) == 2
    assert kp.lookup(db, ["new keyword"], now=now+timedelta(seconds=1)) == {}
    assert len(calls) == 2
    monkeypatch.setattr(kp, "_request", lambda *a, **k: (_ for _ in ()).throw(HTTPException(429, "quota")))
    assert kp.lookup(db, ["institutional ownership"], now=now+timedelta(days=31)) == {}
    assert kp.get_status(db)["error"] == "quota"
    assert db.execute(text("SELECT count(*) FROM research_keyword_metrics")).scalar() == 1


def test_disconnect_inflight_cannot_repopulate(db, monkeypatch):
    seed(db)
    def request(method, url, **kwargs):
        if url.endswith("/token"):
            return {"access_token": "access"}
        kp.disconnect(db)
        return result()
    monkeypatch.setattr(kp, "_request", request)
    assert kp.lookup(db, ["institutional ownership"]) == {}
    assert kp.get_status(db)["metrics"] == []


def test_inactive_owner_no_requests(db, monkeypatch):
    seed(db)
    db.get(UserAccount, 1).role = "user"
    db.commit()
    monkeypatch.setattr(kp, "_request", lambda *a, **k: pytest.fail("must not call Google"))
    assert kp.lookup(db, ["institutional ownership"]) == {}


def test_transport_rejects_mutation_and_sanitizes_provider_errors(monkeypatch):
    with pytest.raises(ValueError):
        kp._request("POST", f"{kp.API}/customers/{kp.CUSTOMER}/campaigns:mutate")
    class Response:
        status_code = 403
        def json(self):
            return {"error": "access-secret"}
    monkeypatch.setattr(kp.requests, "request", lambda *a, **k: Response())
    with pytest.raises(HTTPException) as exc:
        kp._request("POST", kp.ACCOUNT)
    assert "access-secret" not in exc.value.detail


def test_keyword_bounds_and_ranking_quality_gate():
    assert kp._keywords([" Test   Phrase ", "test phrase", "a@b.com", "https://x.com", "x"*81]) == ["test phrase"]
    c = {"content_type": "ticker", "ticker": "NVDA", "target_keyword": "institutional ownership",
         "source_urls": ["https://sec.gov"], "walnut_angle": "Named holders", "opportunity_score": 80,
         "keyword_metrics": {"avg_monthly_searches": 1000}}
    assert rank_candidates([c], [], [], 70)[0]["priority_score"] == 86
    assert rank_candidates([{**c, "opportunity_score": 20}], [], [], 70) == []
    assert kp.priority_bonus({"secondary_keyword_metrics": [{"avg_monthly_searches": 99999999}]}) == 0


def test_admin_routes_deny_anonymous_access(db):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from app.db import get_db
    from app.routers.research_briefs import router
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: db
    with TestClient(app) as client:
        for path, body in [("connect", None), ("lookup", {"keywords": ["insider buying"]}),
                           ("callback", {"code": "code", "state": "gads_" + "x"*40})]:
            assert client.post("/admin/research-briefs/keyword-planner/"+path, json=body).status_code in {401,403}
        assert client.delete("/admin/research-briefs/keyword-planner").status_code in {401,403}


def test_discovery_signals_keep_unknown_and_stale_volume_honest(db):
    seed(db)
    now = datetime.now(timezone.utc)
    for keyword, stamp, volume in [("earnings growth", now, 1000), ("old ownership", now-timedelta(days=31), 99999),
                                   ("unknown", now, None)]:
        metric = {"keyword": keyword, "avg_monthly_searches": volume, "fetched_at": stamp.isoformat()}
        db.execute(text("INSERT INTO research_keyword_metrics VALUES (:k,:t,:m)"), {"k": keyword, "t": stamp.isoformat(), "m": json.dumps(metric)})
    db.commit()
    signals = kp.planning_signals(db)
    assert [m["keyword"] for m in signals["keywords"]] == ["earnings growth"]
    assert "Advertising competition is not organic SEO difficulty" in signals["note"]


def test_planning_keeps_fresh_seed_demand_outside_admin_recent_fifty(db):
    seed(db)
    now = datetime.now(timezone.utc)
    for n in range(51):
        stamp = (now - timedelta(days=1 if n == 50 else 0)).isoformat()
        metric = {"keyword": f"phrase {n}", "avg_monthly_searches": 1000 if n == 50 else None, "fetched_at": stamp}
        db.execute(text("INSERT INTO research_keyword_metrics VALUES (:k,:t,:m)"),
                   {"k": metric["keyword"], "t": stamp, "m": json.dumps(metric)})
    db.commit()
    assert [m["keyword"] for m in kp.planning_signals(db)["keywords"]] == ["phrase 50"]


def test_recent_demand_excludes_current_month_and_compares_completed_windows():
    history = [{"year": 2026, "month": month, "searches": 100 if month < 7 else 200} for month in range(4, 10)]
    history.append({"year": 2026, "month": "OCTOBER", "searches": 999999})
    result = kp.recent_demand({"monthly_searches": history}, now=datetime(2026, 10, 4, tzinfo=timezone.utc))
    assert result["period_end"] == "2026-09"
    assert result["change_pct"] == 100
    assert result["recent_three_month_average"] == 200
    assert "not live" in result["note"]


@pytest.mark.parametrize("case", ["gap", "missing", "stale", "empty"])
def test_recent_demand_does_not_invent_trends(case):
    history = [{"year": 2026, "month": month, "searches": 100} for month in range(4, 10)]
    if case == "gap": history.pop(3)
    if case == "missing": history[3]["searches"] = None
    if case == "stale":
        for row in history: row["year"] = 2025
    if case == "empty": history = []
    assert kp.recent_demand({"monthly_searches": history}, now=datetime(2026, 10, 4, tzinfo=timezone.utc))["status"] == "unknown"


def test_recent_demand_zero_base_is_not_infinite_growth_and_crosses_year():
    history = [{"year": 2025, "month": month, "searches": 0} for month in (10, 11, 12)]
    history += [{"year": 2026, "month": month, "searches": 10} for month in (1, 2, 3)]
    result = kp.recent_demand({"monthly_searches": history}, now=datetime(2026, 4, 4, tzinfo=timezone.utc))
    assert result["change_pct"] is None
    assert result["recent_three_month_average"] == 10
