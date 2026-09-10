from __future__ import annotations
import json
from sqlalchemy import select
from starlette.requests import Request
from app.models import PageViewEvent
from app.routers import accounts as api
from test_accounts_admin_stripe import _session, _user, _request_for_user


def request(origin="https://app.walnutmarkets.com"):
    return Request({"type": "http", "method": "POST", "path": "/api/analytics/event", "headers": [(b"origin", origin.encode())]})


def test_dynamic_visits_dedupe_exact_delivery_not_normalized_route():
    with _session() as db:
        for path, event_id in [("/ticker/AAPL", "one"), ("/ticker/AAPL", "one"), ("/ticker/MSFT", "two"), ("/ticker/AAPL", "three")]:
            api.record_page_view(api.PageViewPayload(path=path, session_id="session", event_id=event_id), request(), db)
        rows = db.execute(select(PageViewEvent)).scalars().all()
        assert [r.path for r in rows] == ["/ticker/AAPL", "/ticker/MSFT", "/ticker/AAPL"]
        assert len({r.normalized_path for r in rows}) == 1


def test_anonymous_to_account_identity_retains_session_and_safe_source():
    with _session() as db:
        user = _user(db, "funnel-test@example.com")
        api.record_product_event(api.ProductEventPayload(event_name="signup_started", path="/login?token=secret", session_id="session", properties={"utm_source": "reddit", "email": "private@example.com"}), request(), db)
        api.record_product_event(api.ProductEventPayload(event_name="signup_completed", path="/login", session_id="session"), _request_for_user(user), db)
        rows = db.execute(select(PageViewEvent)).scalars().all()
        assert rows[0].session_id_hash == rows[1].session_id_hash
        assert rows[0].user_id is None and rows[1].user_id == user.id
        assert rows[0].path == "/login"
        assert json.loads(rows[0].metadata_json)["properties"] == {"utm_source": "reddit", "authenticated": False, "current_plan": "free"}


def test_production_rejects_local_preview_file_and_missing_origin(monkeypatch):
    monkeypatch.setenv("APP_ENV", "production")
    with _session() as db:
        for origin in ["http://localhost:3000", "https://preview.vercel.app", "null", "https://app.walnutmarkets.com.evil.test"]:
            api.record_product_event(api.ProductEventPayload(event_name="ticker_viewed", path="/ticker/AAPL"), request(origin), db)
        api.record_product_event(api.ProductEventPayload(event_name="ticker_viewed", path="C:/My Web Sites/page.html"), request(), db)
        assert db.execute(select(PageViewEvent)).scalars().all() == []
        assert not api._analytics_request_allowed(Request({"type": "http", "headers": []}))


def test_server_rejects_fabricated_browser_payment_completion():
    with _session() as db:
        api.record_product_event(api.ProductEventPayload(event_name="subscription_completed", path="/pricing"), request(), db)
        assert db.execute(select(PageViewEvent)).scalars().all() == []


def test_paid_completion_requires_live_invoice_and_prior_consented_checkout():
    with _session() as db:
        user = _user(db, "paid-test@example.com", tier="premium")
        invoice = {"id": "in_test_invoice"}
        api._record_paid_funnel_event(db, user, invoice, {"livemode": True})
        assert db.execute(select(PageViewEvent)).scalars().all() == []
        api.record_product_event(api.ProductEventPayload(event_name="checkout_started", path="/pricing", session_id="s", properties={"acquisition_source": "reddit"}), _request_for_user(user), db)
        api._record_paid_funnel_event(db, user, invoice, {"livemode": False})
        assert len(db.execute(select(PageViewEvent)).scalars().all()) == 1
        api._record_paid_funnel_event(db, user, invoice, {"livemode": True})
        db.commit()
        api._record_paid_funnel_event(db, user, invoice, {"livemode": True})
        db.commit()
        rows = db.execute(select(PageViewEvent)).scalars().all()
        assert len(rows) == 2
        assert rows[1].session_id_hash == rows[0].session_id_hash
        assert json.loads(rows[1].metadata_json)["properties"]["acquisition_source"] == "reddit"


def test_canonical_schema_matches_browser():
    from pathlib import Path
    import re
    source = (Path(__file__).parents[2] / "frontend/lib/funnelEvents.ts").read_text()
    names = set(re.findall(r'"([a-z_]+)"', source.split("] as const;")[0]))
    assert api._CANONICAL_FUNNEL_EVENTS == names
