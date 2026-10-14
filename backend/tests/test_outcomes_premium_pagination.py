from types import SimpleNamespace
import copy
import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from app import main


@pytest.fixture
def client(monkeypatch):
    payload = {"items": [{"id": n, "ticker": f"T{n}"} for n in range(500)],
               "page": 0, "limit": 500, "total": 800, "has_next": True}
    monkeypatch.setattr(main, "_outcomes_full_overview", lambda *a, **k: {"snapshots": payload, "status": {}, "summaries": {"30D": {"count": 800}}})
    def full(*a, **kwargs):
        a[0].headers["Cache-Control"] = "public, max-age=43200"
        return {**payload, "page": kwargs["page"]}
    monkeypatch.setattr(main, "_outcomes_full_snapshots", full)
    def user(db, request, required):
        tier = request.headers.get("test-tier")
        if tier is None:
            raise HTTPException(401, "Sign in")
        return SimpleNamespace(tier=tier)
    monkeypatch.setattr(main, "current_user", user)
    monkeypatch.setattr(main, "entitlements_for_user", lambda db, user: user)
    app = FastAPI()
    for route in main.app.routes:
        if getattr(route, "path", "") in {"/api/outcomes/overview", "/api/outcomes/snapshots", "/api/outcomes/premium/snapshots"}:
            app.router.routes.append(route)
    app.dependency_overrides[main.get_db] = lambda: None
    with TestClient(app) as test_client:
        yield test_client, payload


@pytest.mark.parametrize("path", ["overview", "snapshots"])
def test_public_preview_projects_even_existing_full_cache(client, path):
    client, shared = client
    original = copy.deepcopy(shared)
    response = client.get(f"/api/outcomes/{path}?limit=5000")
    assert response.status_code == 200
    result = response.json()
    rows = result["snapshots"] if path == "overview" else result
    assert [row["id"] for row in rows["items"]] == list(range(10))
    assert rows["limit"] == 10 and rows["page"] == 0
    assert rows["preview_locked"] and not rows["has_next"]
    assert shared == original
    if path == "overview":
        assert result["summaries"]["30D"]["count"] == 800


@pytest.mark.parametrize("page", [1, 49, 999])
def test_public_pagination_cannot_bypass_preview(client, page):
    client, _ = client
    assert client.get(f"/api/outcomes/snapshots?page={page}&limit=10").status_code == 402


@pytest.mark.parametrize("tier,expected", [(None, 401), ("free", 402), ("premium", 200), ("pro", 200), ("admin", 200)])
def test_full_table_requires_verified_paid_user_and_is_never_publicly_cached(client, tier, expected):
    client, _ = client
    headers = {"test-tier": tier} if tier else {"x-ct-entitlement-tier": "admin"}
    response = client.get("/api/outcomes/premium/snapshots?page=1&limit=500", headers=headers)
    assert response.status_code == expected
    if expected == 200:
        assert len(response.json()["items"]) == 500
        assert response.json()["page"] == 1
        assert response.headers["cache-control"] == "private, no-store"
    assert not main._is_public_get_cacheable_path("/api/outcomes/premium/snapshots")
