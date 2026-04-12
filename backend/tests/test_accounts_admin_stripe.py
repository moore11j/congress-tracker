from __future__ import annotations

from fastapi import HTTPException
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from starlette.requests import Request

from app.auth import sign_session_payload
from app.db import Base
from app.entitlements import current_entitlements, seed_feature_gates
from app.main import WatchlistPayload, create_watchlist
from app.models import UserAccount
from app.routers.accounts import (
    FeatureGatePayload,
    LoginPayload,
    ManualPremiumPayload,
    SuspendPayload,
    admin_delete_user,
    admin_set_premium,
    admin_settings,
    admin_suspend_user,
    admin_update_feature_gate,
    login,
    process_stripe_event,
    upsert_google_user,
)
from app.routers.notifications import NotificationSubscriptionPayload, put_notification_subscription


def _session():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    Base.metadata.create_all(engine)
    db = Session()
    seed_feature_gates(db)
    return db


def _request_for_user(user: UserAccount) -> Request:
    token = sign_session_payload({"uid": user.id, "email": user.email})
    return Request({"type": "http", "method": "GET", "path": "/", "headers": [(b"authorization", f"Bearer {token}".encode())]})


def _user(db, email: str, *, role: str = "user", tier: str = "free") -> UserAccount:
    user = UserAccount(email=email, role=role, entitlement_tier=tier)
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


def _google_claims(email: str, sub: str = "google-sub", name: str = "Google User") -> dict:
    return {
        "iss": "https://accounts.google.com",
        "aud": "google-client",
        "exp": 4_102_444_800,
        "email": email,
        "email_verified": True,
        "sub": sub,
        "name": name,
        "picture": "https://example.com/avatar.png",
    }


def test_successful_stripe_checkout_grants_premium_access(monkeypatch):
    monkeypatch.setenv("CT_DEFAULT_TIER", "free")
    db = _session()
    try:
        user = _user(db, "paid@example.com")
        event = {
            "id": "evt_checkout",
            "type": "checkout.session.completed",
            "data": {
                "object": {
                    "object": "checkout.session",
                    "customer": "cus_123",
                    "subscription": "sub_123",
                    "customer_email": user.email,
                    "metadata": {"user_id": str(user.id), "email": user.email},
                }
            },
        }

        result = process_stripe_event(db, event)
        db.refresh(user)

        assert result["status"] == "processed"
        assert user.entitlement_tier == "premium"
        assert user.stripe_customer_id == "cus_123"
        assert current_entitlements(_request_for_user(user), db).tier == "premium"
    finally:
        db.close()


def test_failed_and_deleted_subscription_remove_premium_access(monkeypatch):
    monkeypatch.setenv("CT_DEFAULT_TIER", "free")
    db = _session()
    try:
        user = _user(db, "failed@example.com", tier="premium")
        user.stripe_customer_id = "cus_456"
        user.stripe_subscription_id = "sub_456"
        db.commit()

        process_stripe_event(
            db,
            {
                "id": "evt_failed",
                "type": "invoice.payment_failed",
                "data": {"object": {"customer": "cus_456", "subscription": "sub_456"}},
            },
        )
        db.refresh(user)
        assert user.entitlement_tier == "free"
        assert current_entitlements(_request_for_user(user), db).tier == "free"

        user.entitlement_tier = "premium"
        db.commit()
        process_stripe_event(
            db,
            {
                "id": "evt_deleted",
                "type": "customer.subscription.deleted",
                "data": {"object": {"object": "subscription", "id": "sub_456", "customer": "cus_456"}},
            },
        )
        db.refresh(user)
        assert user.subscription_status == "canceled"
        assert user.entitlement_tier == "free"
    finally:
        db.close()


def test_admin_account_gets_premium_without_stripe_and_can_save_digest(monkeypatch):
    monkeypatch.setenv("ADMIN_EMAILS", "owner@example.com")
    monkeypatch.setenv("ADMIN_TOKEN", "secret")
    db = _session()
    try:
        response = login(LoginPayload(email="owner@example.com", name="Owner", admin_token="secret"), db)
        admin = db.get(UserAccount, response["user"]["id"])
        assert admin is not None

        entitlements = current_entitlements(_request_for_user(admin), db)
        assert entitlements.tier == "premium"
        assert "notification_digests" in entitlements.features

        subscription = put_notification_subscription(
            NotificationSubscriptionPayload(
                email="owner@example.com",
                source_type="saved_view",
                source_id="view-1",
                source_name="Admin view",
                only_if_new=True,
                active=True,
                alert_triggers=["cross_source_confirmation"],
            ),
            _request_for_user(admin),
            db,
        )
        assert subscription["email"] == "owner@example.com"
    finally:
        db.close()


def test_admin_settings_lists_registered_accounts_without_sensitive_fields(monkeypatch):
    monkeypatch.setenv("ADMIN_EMAILS", "admin@example.com")
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        _user(db, "reader@example.com")

        response = admin_settings(_request_for_user(admin), db)

        assert len(response["users"]) == 2
        assert {"email", "name", "created_at", "last_seen_at"}.issubset(response["users"][0].keys())
        forbidden = {"password", "password_hash", "card", "payment_method"}
        assert forbidden.isdisjoint(response["users"][0].keys())
        assert response["stripe"]["secret_key"] in {"configured", "missing"}
    finally:
        db.close()


def test_admin_can_upgrade_downgrade_suspend_and_delete_user(monkeypatch):
    monkeypatch.setenv("ADMIN_EMAILS", "admin@example.com")
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        reader = _user(db, "reader@example.com")
        request = _request_for_user(admin)

        upgraded = admin_set_premium(reader.id, ManualPremiumPayload(tier="premium"), request, db)
        assert upgraded["manual_tier_override"] == "premium"

        downgraded = admin_set_premium(reader.id, ManualPremiumPayload(tier="free"), request, db)
        assert downgraded["manual_tier_override"] == "free"

        suspended = admin_suspend_user(reader.id, SuspendPayload(suspended=True), request, db)
        assert suspended["is_suspended"] is True

        result = admin_delete_user(reader.id, request, db)
        assert result["status"] == "deleted"
        assert db.get(UserAccount, reader.id) is None
    finally:
        db.close()


def test_admin_feature_gate_change_is_backend_authoritative(monkeypatch):
    monkeypatch.setenv("ADMIN_EMAILS", "admin@example.com")
    monkeypatch.setenv("CT_DEFAULT_TIER", "free")
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        request = _request_for_user(admin)
        admin_update_feature_gate("watchlists", FeatureGatePayload(required_tier="premium"), request, db)

        try:
            create_watchlist(WatchlistPayload(name="Blocked"), Request({"type": "http", "method": "POST", "path": "/", "headers": []}), db)
        except HTTPException as exc:
            assert exc.status_code == 402
            assert exc.detail["feature"] == "watchlists"
        else:
            raise AssertionError("Expected premium-required response")

        admin_update_feature_gate("watchlists", FeatureGatePayload(required_tier="free"), request, db)
        response = create_watchlist(WatchlistPayload(name="Allowed"), Request({"type": "http", "method": "POST", "path": "/", "headers": []}), db)
        assert response["name"] == "Allowed"
    finally:
        db.close()


def test_google_sign_in_maps_normal_user_without_admin_access(monkeypatch):
    monkeypatch.setenv("GOOGLE_CLIENT_ID", "google-client")
    db = _session()
    try:
        user = upsert_google_user(db, _google_claims("reader@gmail.com"))
        db.commit()
        db.refresh(user)

        assert user.email == "reader@gmail.com"
        assert user.auth_provider == "google"
        assert user.google_sub == "google-sub"
        assert user.role == "user"
        assert current_entitlements(_request_for_user(user), db).tier == "free"
        assert admin_settings_raises_for_user(user, db)
    finally:
        db.close()


def admin_settings_raises_for_user(user: UserAccount, db) -> bool:
    try:
        admin_settings(_request_for_user(user), db)
    except HTTPException as exc:
        return exc.status_code == 403
    return False


def test_google_sign_in_admin_email_gets_admin_and_premium_without_payment(monkeypatch):
    monkeypatch.setenv("GOOGLE_CLIENT_ID", "google-client")
    db = _session()
    try:
        user = upsert_google_user(db, _google_claims("moore11j@gmail.com", sub="admin-sub", name="Moore"))
        db.commit()
        db.refresh(user)

        assert user.role == "admin"
        entitlements = current_entitlements(_request_for_user(user), db)
        assert entitlements.tier == "premium"
        assert "notification_digests" in entitlements.features
        assert admin_settings(_request_for_user(user), db)["users"][0]["email"] == "moore11j@gmail.com"
    finally:
        db.close()


def test_google_account_linking_preserves_stripe_state(monkeypatch):
    monkeypatch.setenv("GOOGLE_CLIENT_ID", "google-client")
    db = _session()
    try:
        user = _user(db, "paid-reader@gmail.com", tier="premium")
        user.stripe_customer_id = "cus_google"
        user.stripe_subscription_id = "sub_google"
        user.subscription_status = "active"
        db.commit()

        linked = upsert_google_user(db, _google_claims("paid-reader@gmail.com", sub="linked-sub"))
        db.commit()
        db.refresh(linked)

        assert linked.id == user.id
        assert linked.google_sub == "linked-sub"
        assert linked.stripe_customer_id == "cus_google"
        assert current_entitlements(_request_for_user(linked), db).tier == "premium"
    finally:
        db.close()
