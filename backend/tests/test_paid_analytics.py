import asyncio
import json
import time
from unittest.mock import Mock

import pytest
from sqlalchemy import select
from starlette.requests import Request
from app import paid_analytics as paid
from app.models import PageViewEvent
from app.routers import accounts as api
from test_accounts_admin_stripe import _session, _user, _request_for_user


@pytest.fixture(autouse=True)
def session_secret(monkeypatch):
    monkeypatch.setenv("APP_SESSION_SECRET", "qa-isolated-session-signing-only-" * 2)


def paid_row(db):
    user = _user(db, "provider-fixture@example.com", tier="premium")
    request = _request_for_user(user)
    request.scope["headers"].append((b"origin", b"https://app.walnutmarkets.com"))
    api.record_product_event(api.ProductEventPayload(event_name="checkout_started", path="/pricing", session_id="qa-session",
        properties={"acquisition_source": "reddit"}, ga_context={"client_id": "123.456", "session_id": "1789010000"}),
        request, db)
    api._record_paid_funnel_event(db, user, {"id": "in_provider_fixture"}, {"livemode": True})
    db.commit()
    return db.execute(select(PageViewEvent).where(PageViewEvent.normalized_path == paid.PAID_PATH)).scalar_one()


def test_paid_claim_is_durable_one_time_per_provider_and_keeps_ga_identity():
    with _session() as db:
        row = paid_row(db)
        first = paid.claim_delivery(db, row.id, "ga4")
        assert first["ga_context"] == {"client_id": "123.456", "session_id": "1789010000"}
        assert paid.claim_delivery(db, row.id, "ga4") is None
        assert paid.claim_delivery(db, row.id, "heycatch") is not None
        assert paid.claim_delivery(db, row.id, "heycatch") is None
        payload = paid.ga4_payload(first)
        assert payload["events"][0]["name"] == "subscription_completed"
        assert payload["events"][0]["params"]["acquisition_source"] == "reddit"
        assert payload["events"][0]["params"]["engagement_time_msec"] == 0
        assert "provider-fixture" not in json.dumps(payload)
        assert payload["consent"]["ad_user_data"] == "DENIED"


def test_ga_context_rejects_pii_and_never_invents_identifiers():
    for value in [None, {}, {"client_id": "name@example.com", "session_id": "1"}, {"client_id": "123.456", "session_id": "uuid"}]:
        assert paid.safe_ga_context(value) == {}


def test_claim_does_not_accept_browser_events():
    with _session() as db:
        user = _user(db, "not-paid@example.com")
        api.record_product_event(api.ProductEventPayload(event_name="checkout_started", path="/pricing"), _request_for_user(user), db)
        row = db.execute(select(PageViewEvent)).scalar_one()
        assert paid.claim_delivery(db, row.id, "heycatch") is None


def test_bridge_requires_production_fresh_signature_and_correct_scope(monkeypatch):
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("ANALYTICS_FORWARDING_SECRET", "test-secret-" * 4)
    body, timestamp = b'{"event_id":1}', str(int(time.time()))
    signature = paid.bridge_signature(body, timestamp, "claim")
    assert paid.verify_bridge(body, timestamp, signature, "claim")
    assert not paid.verify_bridge(body, timestamp, signature, "dispatch")
    assert not paid.verify_bridge(b'{"event_id":2}', timestamp, signature, "claim")
    assert not paid.verify_bridge(body, str(int(time.time()) - 120), signature, "claim")
    monkeypatch.setenv("APP_ENV", "staging")
    monkeypatch.setenv("FLY_APP_NAME", "fixture")
    assert not paid.verify_bridge(body, timestamp, signature, "claim")


def test_claim_endpoint_rejects_unsigned_and_consumes_signed_replays(monkeypatch):
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("ANALYTICS_FORWARDING_SECRET", "test-secret-" * 4)
    with _session() as db:
        row = paid_row(db)
        body = json.dumps({"event_id": row.id}).encode()
        stamp = str(int(time.time()))
        async def receive():
            return {"type": "http.request", "body": body}
        def request(signature):
            return Request({"type": "http", "headers": [(b"x-walnut-timestamp", stamp.encode()),
                (b"x-walnut-signature", signature.encode())]}, receive)
        with pytest.raises(api.HTTPException) as error:
            asyncio.run(api.claim_paid_analytics(request("invalid"), db))
        assert error.value.status_code == 401
        signed = paid.bridge_signature(body, stamp, "claim")
        assert asyncio.run(api.claim_paid_analytics(request(signed), db))["user_id"]
        assert asyncio.run(api.claim_paid_analytics(request(signed), db)).status_code == 204


def test_provider_timeout_does_not_retry_or_affect_committed_payment(monkeypatch):
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("GA4_API_SECRET", "test-only")
    monkeypatch.delenv("ANALYTICS_FORWARDING_SECRET", raising=False)
    with _session() as db:
        row = paid_row(db)
        event_id = row.id
        # Keep the test's isolated database; the dispatcher owns its own session in production.
        class Scope:
            def __enter__(self): return db
            def __exit__(self, *args): pass
        monkeypatch.setattr(paid, "SessionLocal", Scope)
        post = Mock(side_effect=TimeoutError("ambiguous send"))
        monkeypatch.setattr(paid.requests, "post", post)
        paid.forward_paid_event(event_id)
        paid.forward_paid_event(event_id)
        assert post.call_count == 1
        assert paid.metadata(db.get(PageViewEvent, event_id).metadata_json)["forwarding"]["ga4"] == "attempted"
        assert db.get(PageViewEvent, event_id).normalized_path == paid.PAID_PATH


def test_background_dispatch_database_failure_cannot_escape(monkeypatch):
    monkeypatch.setenv("APP_ENV", "production")
    def unavailable():
        raise RuntimeError("database unavailable")
    monkeypatch.setattr(paid, "SessionLocal", unavailable)
    paid.forward_paid_event(123)
