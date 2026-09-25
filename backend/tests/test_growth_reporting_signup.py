import json
from datetime import datetime, timedelta, timezone

import pytest
from fastapi import HTTPException, Response
from sqlalchemy import select

from app.growth_reporting import page_analytics
from app.models import BillingTransaction, PageViewEvent, UserAccount
from app.routers import accounts as api
from test_accounts_admin_stripe import _session, _user, _request_for_user


def test_email_only_registration_preserves_security_and_login(monkeypatch):
    sent = []
    monkeypatch.setattr(api, "_send_verification_email", lambda *args: sent.append("verification"))
    monkeypatch.setattr(api, "_send_welcome_email", lambda *args: sent.append("welcome"))
    with _session() as db:
        response = Response()
        result = api.register(api.RegisterPayload(email="new@example.com", password="Password123!"), response, db)
        user = db.scalar(select(UserAccount).where(UserAccount.email == "new@example.com"))
        assert result["is_new_user"] is True
        assert result["email_verification_required"] is True
        assert user.address_line1 is None and user.first_name is None
        assert user.password_hash and user.password_hash != "Password123!"
        assert user.email_verification_token_hash
        assert sent == ["verification", "welcome"]
        assert "ct_session=" in response.headers["set-cookie"]
        assert api.login(api.LoginPayload(email=user.email, password="Password123!"), db)["user"]["id"] == user.id
        with pytest.raises(HTTPException) as error:
            api.create_checkout_session(_request_for_user(user), db=db)
        assert error.value.status_code == 403  # verification is still required for upgrade
        with pytest.raises(HTTPException) as error:
            api.register(api.RegisterPayload(email=user.email, password="Password123!"), db)
        assert error.value.status_code == 409
        with pytest.raises(HTTPException) as error:
            api.register(api.RegisterPayload(email="weak@example.com", password="onlyletters"), db)
        assert error.value.status_code == 422


def test_optional_registration_fields_do_not_erase_google_profile(monkeypatch):
    monkeypatch.setattr(api, "_send_verification_email", lambda *args: None)
    monkeypatch.setattr(api, "_send_welcome_email", lambda *args: pytest.fail("Not a new account"))
    with _session() as db:
        user = _user(db, "google@example.com")
        user.auth_provider = "google"
        user.first_name, user.last_name, user.name = "A", "Reader", "A Reader"
        user.country, user.address_line1 = "CA", "123 Existing Street"
        db.commit()
        result = api.register(api.RegisterPayload(email=user.email, password="Password123!"), db)
        assert result["is_new_user"] is False
        assert (user.name, user.country, user.address_line1) == ("A Reader", "CA", "123 Existing Street")


def add_event(db, path, *, user=None, session=None, event=False, created=None):
    db.add(PageViewEvent(
        path=path, normalized_path=path if event else "/ticker/[symbol]" if path.startswith("/ticker/") else path,
        route_group="events" if event else "other", user_id=user.id if user else None,
        session_id_hash=session, is_authenticated=user is not None,
        created_at=created or datetime.now(timezone.utc),
    ))


def test_report_separates_events_accounts_sessions_and_excludes_internal(monkeypatch):
    with _session() as db:
        admin = _user(db, "admin@example.com", role="admin")
        test_user = _user(db, "fixture@example.com")
        reader = _user(db, "reader@example.com")
        monkeypatch.setenv("ANALYTICS_EXCLUDED_USER_IDS", str(test_user.id))
        for i in range(5):
            add_event(db, "/ticker/NVDA", user=admin, session="internal")
        add_event(db, "/ticker/NVDA", session="internal")  # pre-login same internal session
        add_event(db, "/ticker/NVDA", user=test_user, session="test")
        add_event(db, "/ticker/NVDA", session="reader")
        add_event(db, "/ticker/NVDA", user=reader, session="reader")
        add_event(db, "/ticker/AMD")  # unidentified visit must not become a fictitious user
        add_event(db, "/research/example", session="another")
        add_event(db, "/events/signup_completed", user=reader, session="reader", event=True)
        add_event(db, "/old", created=datetime.now(timezone.utc)-timedelta(days=60))
        db.commit()
        args = dict(start=datetime.now(timezone.utc)-timedelta(days=7), period="7d", limit=1)
        report = page_analytics(db, **args)
        assert report["totals"] == {"views": 4, "accounts": 1, "sessions": 2, "views_without_session": 1, "pages": 3}
        assert sum(row["views"] for row in report["trend_by_day"]) == 4
        assert len(report["top_pages"]) == 1
        assert report["low_usage_pages"][0]["page"] == "/research/example"  # outside top 1
        assert report["event_reach"] == [{"event": "signup_completed", "events": 1, "accounts": 1, "sessions": 1}]
        assert report["accounts"]["current_accounts"] == 1
        assert page_analytics(db, **args, include_internal=True)["totals"]["views"] == 11


def test_report_payments_require_live_positive_evidence_not_paid_access():
    with _session() as db:
        comped = _user(db, "comped@example.com", tier="pro")
        buyer = _user(db, "buyer@example.com")
        now = datetime.now(timezone.utc)
        for index, (user, payload) in enumerate([
            (buyer, {"livemode": True, "amount_paid": 2495}),
            (buyer, {"livemode": True, "amount_paid": 2495}),  # two invoices, one paying account
            (comped, {"livemode": True, "amount_paid": 0}),
            (comped, {"livemode": False, "amount_paid": 2495}),
            (comped, {"amount_paid": 2495}),
        ]):
            db.add(BillingTransaction(user_id=user.id, stripe_invoice_id=f"in_{index}", payment_status="paid", charged_at=now, total_amount=2495, payload_json=json.dumps(payload)))
        db.commit()
        report = page_analytics(db, start=now-timedelta(days=7), period="7d", limit=10)
        assert report["payments"]["live_paid_invoices"] == 2
        assert report["payments"]["live_paying_accounts"] == 1
        assert report["payments"]["zero_paid_invoices"] == 1
        assert report["payments"]["test_paid_invoices"] == 1
        assert report["payments"]["unverified_paid_invoices"] == 1
