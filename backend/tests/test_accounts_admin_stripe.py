from __future__ import annotations

import base64
import json
import logging
import threading
import time
import zipfile
from datetime import datetime, timedelta, timezone
from io import BytesIO
from zoneinfo import ZoneInfo

import pytest
from fastapi import HTTPException, Response
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import sessionmaker
from starlette.requests import Request

from app.auth import SESSION_COOKIE_NAME, admin_emails, sign_session_payload
from app.db import Base
from app.entitlements import current_entitlements, seed_feature_gates, seed_plan_prices
from app.main import WatchlistPayload, create_watchlist
from app.models import AdminBillingOverrideAuditLog, AppSetting, BillingTransaction, EmailDelivery, PageViewEvent, StripeWebhookEvent, UserAccount, Watchlist
from app.routers import accounts as accounts_module
from app.services.billing_reminders import run_billing_expiry_reminders
from app.routers.accounts import (
    CheckoutSessionPayload,
    FeatureGatePayload,
    GoogleCallbackPayload,
    LoginPayload,
    ManualPremiumPayload,
    NotificationSettingsPayload,
    OAuthSettingsPayload,
    PageViewPayload,
    ProductEventPayload,
    PasswordResetConfirmPayload,
    PasswordResetRequestPayload,
    PasswordChangePayload,
    DeleteAccountPayload,
    PriceOverridePayload,
    ReactivateAccountPayload,
    PlanLimitPayload,
    PlanPricePayload,
    ProfileUpdatePayload,
    RegisterPayload,
    StripeTaxSettingsPayload,
    AdminSubscriptionSyncPayload,
    SuspendPayload,
    VerifyEmailPayload,
    admin_set_premium,
    admin_set_user_price_override,
    admin_send_password_reset,
    admin_subscription_debug,
    admin_sync_stripe_subscription,
    admin_settings,
    admin_sales_ledger,
    admin_page_analytics,
    admin_reports_summary,
    admin_sales_ledger_export,
    admin_suspend_user,
    admin_email_deliveries,
    admin_delete_user,
    admin_users,
    admin_users_export,
    admin_update_feature_gate,
    admin_update_oauth_settings,
    admin_update_plan_limit,
    admin_update_plan_price,
    admin_update_stripe_tax_settings,
    account_billing_history,
    account_settings,
    billing_readiness,
    cancel_subscription_at_period_end,
    delete_account,
    create_checkout_session,
    create_customer_portal_session,
    confirm_password_reset,
    google_auth_callback,
    google_auth_start,
    login,
    me,
    process_stripe_event,
    record_product_event,
    record_page_view,
    public_plan_config,
    refresh_subscription_from_stripe,
    reactivate_subscription_before_expiry,
    reactivate_deleted_account,
    register,
    request_password_reset,
    update_account_notifications,
    update_account_password,
    update_account_profile,
    stripe_tax_billing_readiness,
    _google_client_id,
    format_expiry_duration,
    _reset_url,
    _verification_url,
    resend_email_verification,
    upsert_google_user,
    verify_email,
)
from app.routers.notifications import NotificationSubscriptionPayload, put_notification_subscription


def _session():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    Base.metadata.create_all(engine)
    db = Session()
    seed_feature_gates(db)
    seed_plan_prices(db)
    return db


def _request_for_user(user: UserAccount) -> Request:
    token = sign_session_payload({"uid": user.id, "email": user.email})
    return Request({"type": "http", "method": "GET", "path": "/", "headers": [(b"cookie", f"{SESSION_COOKIE_NAME}={token}".encode())]})


def _user(db, email: str, *, role: str = "user", tier: str = "free") -> UserAccount:
    user = UserAccount(email=email, role=role, entitlement_tier=tier)
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


def _email_delivery(
    db,
    *,
    to_email: str,
    status: str = "sent",
    template_key: str = "account.welcome",
    created_at: datetime | None = None,
) -> EmailDelivery:
    row = EmailDelivery(
        to_email=to_email,
        from_email="support@example.com",
        template_key=template_key,
        category=template_key.split(".", 1)[0],
        subject=f"Subject for {template_key}",
        provider="postmark",
        status=status,
        created_at=created_at or datetime.now(timezone.utc),
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


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


def _google_id_token(claims: dict) -> str:
    def encode(value: dict) -> str:
        return base64.urlsafe_b64encode(json.dumps(value).encode("utf-8")).rstrip(b"=").decode("ascii")

    return f"{encode({'alg': 'none'})}.{encode(claims)}.signature"


def _register_payload(email: str, *, password: str = "Password123!") -> RegisterPayload:
    return RegisterPayload(
        first_name="Reader",
        last_name="One",
        email=email,
        password=password,
        country="US",
        state_province="CA",
        postal_code="94105",
        city="San Francisco",
        address_line1="1 Market St",
        address_line2="Suite 200",
    )


def test_auth_email_links_use_walnut_markets_app_host_in_production(monkeypatch):
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("APP_BASE_URL", "https://app.walnutmarkets.com")
    monkeypatch.setenv("FRONTEND_BASE_URL", "https://www.walnut-intel.com")

    verification_url = _verification_url("verify-token")
    assert verification_url == "https://app.walnutmarkets.com/account/verify-email?token=verify-token"
    assert "/api/account/verify-email" not in verification_url
    assert "walnut-intel.com" not in verification_url
    assert _reset_url("reset-token") == "https://app.walnutmarkets.com/reset-password?token=reset-token"


def test_checkout_completed_links_stripe_ids_without_granting_paid_access(monkeypatch):
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
        assert user.stripe_customer_id == "cus_123"
        assert user.stripe_subscription_id == "sub_123"
        assert user.subscription_status == "checkout_completed"
        assert user.entitlement_tier == "free"
        assert current_entitlements(_request_for_user(user), db).tier == "free"
    finally:
        db.close()


def test_failed_unpaid_and_deleted_subscription_remove_premium_access(monkeypatch):
    monkeypatch.setenv("CT_DEFAULT_TIER", "free")
    monkeypatch.delenv("STRIPE_PAYMENT_FAILURE_GRACE_DAYS", raising=False)
    db = _session()
    try:
        user = _user(db, "failed@example.com", tier="premium")
        user.stripe_customer_id = "cus_456"
        user.stripe_subscription_id = "sub_456"
        user.subscription_status = "active"
        user.subscription_plan = "premium"
        user.access_expires_at = datetime.now(timezone.utc) + timedelta(days=25)
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
        assert user.subscription_status == "payment_failed"
        failed_expiry = user.access_expires_at.replace(tzinfo=timezone.utc) if user.access_expires_at and user.access_expires_at.tzinfo is None else user.access_expires_at
        assert failed_expiry <= datetime.now(timezone.utc)
        assert user.entitlement_tier == "free"
        assert current_entitlements(_request_for_user(user), db).tier == "free"

        user.entitlement_tier = "premium"
        user.subscription_status = "active"
        user.subscription_plan = "premium"
        user.access_expires_at = datetime.now(timezone.utc) + timedelta(days=25)
        db.commit()
        process_stripe_event(
            db,
            {
                "id": "evt_unpaid",
                "type": "customer.subscription.updated",
                "data": {"object": {"object": "subscription", "id": "sub_456", "customer": "cus_456", "status": "unpaid"}},
            },
        )
        db.refresh(user)
        assert user.subscription_status == "unpaid"
        assert user.access_expires_at is None
        assert user.entitlement_tier == "free"

        user.entitlement_tier = "premium"
        user.subscription_status = "active"
        user.subscription_plan = "premium"
        user.access_expires_at = datetime.now(timezone.utc) + timedelta(days=25)
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
        assert user.subscription_status == "deleted"
        assert user.access_expires_at is None
        assert user.entitlement_tier == "free"
    finally:
        db.close()


def test_cancel_at_period_end_preserves_access_until_period_end(monkeypatch):
    monkeypatch.setenv("CT_DEFAULT_TIER", "free")
    monkeypatch.setenv("STRIPE_PRICE_ID_PREMIUM_MONTHLY", "price_cancel_period")
    db = _session()
    try:
        user = _user(db, "cancel-period@example.com", tier="premium")
        user.stripe_customer_id = "cus_cancel_period"
        db.commit()

        process_stripe_event(
            db,
            {
                "id": "evt_cancel_period",
                "type": "customer.subscription.updated",
                "data": {
                    "object": {
                        "object": "subscription",
                        "id": "sub_cancel_period",
                        "customer": "cus_cancel_period",
                        "status": "active",
                        "cancel_at_period_end": True,
                        "current_period_end": int((datetime.now(timezone.utc) + timedelta(days=12)).timestamp()),
                        "items": {"data": [{"price": {"id": "price_cancel_period", "recurring": {"interval": "month"}}}]},
                    }
                },
            },
        )
        db.refresh(user)

        assert user.subscription_cancel_at_period_end is True
        assert user.entitlement_tier == "premium"
        assert current_entitlements(_request_for_user(user), db).tier == "premium"
    finally:
        db.close()


def test_invoice_payment_failed_can_use_explicit_grace(monkeypatch):
    monkeypatch.setenv("CT_DEFAULT_TIER", "free")
    monkeypatch.setenv("STRIPE_PAYMENT_FAILURE_GRACE_DAYS", "3")
    db = _session()
    try:
        user = _user(db, "failed-grace@example.com", tier="premium")
        user.stripe_customer_id = "cus_failed_grace"
        user.stripe_subscription_id = "sub_failed_grace"
        user.subscription_status = "active"
        user.subscription_plan = "premium"
        user.access_expires_at = datetime.now(timezone.utc) + timedelta(days=30)
        db.commit()

        process_stripe_event(
            db,
            {
                "id": "evt_failed_grace",
                "type": "invoice.payment_failed",
                "data": {"object": {"id": "in_failed_grace", "customer": "cus_failed_grace", "subscription": "sub_failed_grace"}},
            },
        )
        db.refresh(user)

        assert user.subscription_status == "payment_failed"
        assert user.entitlement_tier == "premium"
        assert user.access_expires_at is not None
        grace_expiry = user.access_expires_at.replace(tzinfo=timezone.utc) if user.access_expires_at.tzinfo is None else user.access_expires_at
        assert grace_expiry <= datetime.now(timezone.utc) + timedelta(days=3, minutes=1)
        assert current_entitlements(_request_for_user(user), db).tier == "premium"
    finally:
        db.close()


def test_invoice_voided_and_uncollectible_remove_paid_access(monkeypatch):
    monkeypatch.setenv("CT_DEFAULT_TIER", "free")
    db = _session()
    try:
        for event_type, status, email, customer, subscription in (
            ("invoice.voided", "voided", "voided@example.com", "cus_voided", "sub_voided"),
            ("invoice.marked_uncollectible", "uncollectible", "uncollectible@example.com", "cus_uncollectible", "sub_uncollectible"),
        ):
            user = _user(db, email, tier="premium")
            user.stripe_customer_id = customer
            user.stripe_subscription_id = subscription
            user.subscription_status = "active"
            user.subscription_plan = "premium"
            user.access_expires_at = datetime.now(timezone.utc) + timedelta(days=20)
            db.commit()

            process_stripe_event(
                db,
                {
                    "id": f"evt_{status}",
                    "type": event_type,
                    "data": {"object": {"id": f"in_{status}", "customer": customer, "subscription": subscription}},
                },
            )
            db.refresh(user)

            assert user.subscription_status == status
            assert user.access_expires_at is None
            assert user.entitlement_tier == "free"
            assert current_entitlements(_request_for_user(user), db).tier == "free"
    finally:
        db.close()


def test_full_refund_revokes_paid_access_and_duplicate_is_idempotent(monkeypatch):
    monkeypatch.setenv("CT_DEFAULT_TIER", "free")
    db = _session()
    try:
        user = _user(db, "refund@example.com", tier="premium")
        user.stripe_customer_id = "cus_refund"
        user.stripe_subscription_id = "sub_refund"
        user.subscription_status = "active"
        user.subscription_plan = "premium"
        user.access_expires_at = datetime.now(timezone.utc) + timedelta(days=20)
        db.add(
            BillingTransaction(
                user_id=user.id,
                stripe_customer_id="cus_refund",
                stripe_subscription_id="sub_refund",
                stripe_invoice_id="in_refund",
                stripe_charge_id="ch_refund",
                total_amount=1995,
                payment_status="paid",
                refund_status="none",
                charged_at=datetime.now(timezone.utc),
            )
        )
        db.commit()

        event = {
            "id": "evt_full_refund",
            "type": "charge.refunded",
            "data": {
                "object": {
                    "object": "charge",
                    "id": "ch_refund",
                    "customer": "cus_refund",
                    "invoice": "in_refund",
                    "amount": 1995,
                    "amount_refunded": 1995,
                    "refunded": True,
                }
            },
        }
        first = process_stripe_event(db, event)
        db.refresh(user)
        assert user.subscription_status == "refunded"
        assert user.access_expires_at is None
        assert user.entitlement_tier == "free"
        assert current_entitlements(_request_for_user(user), db).tier == "free"

        user.entitlement_tier = "premium"
        user.subscription_status = "active"
        user.subscription_plan = "premium"
        user.access_expires_at = datetime.now(timezone.utc) + timedelta(days=20)
        db.commit()
        second = process_stripe_event(db, event)
        db.refresh(user)
        tx = db.execute(select(BillingTransaction).where(BillingTransaction.stripe_charge_id == "ch_refund")).scalar_one()

        assert first["status"] == "processed"
        assert second == {"status": "already_processed", "event_type": "charge.refunded"}
        assert tx.refund_status == "refunded"
        assert user.subscription_status == "active"
        assert user.entitlement_tier == "premium"
    finally:
        db.close()


def test_partial_refund_updates_transaction_without_revoking_access(monkeypatch):
    monkeypatch.setenv("CT_DEFAULT_TIER", "free")
    db = _session()
    try:
        user = _user(db, "partial-refund@example.com", tier="premium")
        user.stripe_customer_id = "cus_partial_refund"
        user.stripe_subscription_id = "sub_partial_refund"
        user.subscription_status = "active"
        user.subscription_plan = "premium"
        user.access_expires_at = datetime.now(timezone.utc) + timedelta(days=20)
        db.add(
            BillingTransaction(
                user_id=user.id,
                stripe_customer_id="cus_partial_refund",
                stripe_subscription_id="sub_partial_refund",
                stripe_invoice_id="in_partial_refund",
                stripe_charge_id="ch_partial_refund",
                total_amount=1995,
                payment_status="paid",
                refund_status="none",
                charged_at=datetime.now(timezone.utc),
            )
        )
        db.commit()

        process_stripe_event(
            db,
            {
                "id": "evt_partial_refund",
                "type": "refund.updated",
                "data": {
                    "object": {
                        "object": "refund",
                        "id": "re_partial",
                        "amount": 500,
                        "status": "succeeded",
                        "charge": {"id": "ch_partial_refund", "amount": 1995, "amount_refunded": 500, "refunded": False},
                    }
                },
            },
        )
        db.refresh(user)
        tx = db.execute(select(BillingTransaction).where(BillingTransaction.stripe_charge_id == "ch_partial_refund")).scalar_one()

        assert tx.refund_status == "partially_refunded"
        assert user.entitlement_tier == "premium"
        assert current_entitlements(_request_for_user(user), db).tier == "premium"
    finally:
        db.close()


def test_admin_account_gets_premium_without_stripe_and_can_save_digest(monkeypatch):
    monkeypatch.setenv("ADMIN_EMAILS", "owner@example.com")
    db = _session()
    try:
        admin = _user(db, "owner@example.com", role="admin")

        entitlements = current_entitlements(_request_for_user(admin), db)
        assert entitlements.tier == "admin"
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


def test_email_password_register_login_and_reset_flow(monkeypatch):
    monkeypatch.setenv("APP_ENV", "test")
    monkeypatch.setenv("CT_ALLOW_INSECURE_RESET_LINK_RESPONSE", "1")
    db = _session()
    try:
        registered = register(_register_payload("reader-one@example.com"), db)
        user = db.get(UserAccount, registered["user"]["id"])
        assert user is not None
        assert registered["authenticated"] is True
        assert "token" not in registered
        assert user.name == "Reader One"
        assert user.country == "US"
        assert user.postal_code == "94105"
        assert "billing_profile_complete" not in registered["user"]
        browser_auth_forbidden = {
            "country",
            "state_province",
            "postal_code",
            "city",
            "address_line1",
            "address_line2",
            "deleted_at",
            "deleted_by_user",
            "deletion_reason",
            "deletion_plan",
            "reactivation_expires_at",
            "is_deleted",
            "email_verified_at",
        }
        assert browser_auth_forbidden.isdisjoint(registered["user"].keys())
        assert user.password_hash
        assert registered["email_verification_required"] is True
        assert registered["dev_verification_url"].startswith("http://localhost:3000/account/verify-email?token=")
        assert user.email_verified_at is None
        assert user.email_verification_token_hash
        verification_delivery = db.execute(
            select(EmailDelivery).where(EmailDelivery.template_key == "account.verify_email")
        ).scalar_one()
        assert verification_delivery.user_id == user.id
        assert verification_delivery.to_email == "reader-one@example.com"
        assert verification_delivery.idempotency_key == f"verify-email:{user.id}:{user.email_verification_token_hash}"
        welcome_delivery = db.execute(
            select(EmailDelivery).where(EmailDelivery.template_key == "account.welcome")
        ).scalar_one()
        assert welcome_delivery.user_id == user.id
        assert welcome_delivery.to_email == "reader-one@example.com"
        assert welcome_delivery.idempotency_key == f"account.welcome:user:{user.id}"

        signed_in = login(LoginPayload(email="reader-one@example.com", password="Password123!"), db)
        assert signed_in["user"]["email"] == "reader-one@example.com"
        assert signed_in["authenticated"] is True
        assert "token" not in signed_in
        assert browser_auth_forbidden.isdisjoint(signed_in["user"].keys())

        reset = request_password_reset(PasswordResetRequestPayload(email="reader-one@example.com"), db)
        assert reset["reset_path"].startswith("/reset-password?token=")
        token = reset["reset_path"].split("token=", 1)[1]

        response = Response()
        confirmed = confirm_password_reset(
            PasswordResetConfirmPayload(token=token, password="Newpassword123!", confirm_password="Newpassword123!"),
            response,
            db,
        )
        assert confirmed == {"ok": True, "authenticated": False, "redirect_to": "/login?reset=success"}
        assert "token" not in confirmed
        assert "user" not in confirmed
        set_cookie = response.headers.get("set-cookie", "")
        assert SESSION_COOKIE_NAME not in set_cookie or "Max-Age=0" in set_cookie
        db.refresh(user)
        assert user.password_reset_token_hash is None
        assert user.password_reset_expires_at is None
        delivery = db.execute(
            select(EmailDelivery).where(EmailDelivery.template_key == "account.password_changed")
        ).scalar_one()
        assert delivery.user_id == user.id
        assert delivery.to_email == "reader-one@example.com"
        assert login(LoginPayload(email="reader-one@example.com", password="Newpassword123!"), db)["user"]["id"] == user.id

        try:
            confirm_password_reset(
                PasswordResetConfirmPayload(token=token, password="Anotherpass123!", confirm_password="Anotherpass123!"),
                db,
            )
        except HTTPException as exc:
            assert exc.status_code == 400
            assert "Invalid or expired reset link." in str(exc.detail)
        else:
            raise AssertionError("Expected used password reset token rejection")
    finally:
        db.close()


def test_duplicate_email_password_register_does_not_send_welcome():
    db = _session()
    try:
        registered = register(_register_payload("duplicate-register@example.com"), db)
        user = db.get(UserAccount, registered["user"]["id"])
        assert user is not None
        assert len(db.execute(select(EmailDelivery)).scalars().all()) == 2

        with pytest.raises(HTTPException) as exc:
            register(_register_payload("duplicate-register@example.com"), db)

        assert exc.value.status_code == 409
        assert len(db.execute(select(EmailDelivery)).scalars().all()) == 2
        welcome_deliveries = db.execute(
            select(EmailDelivery).where(EmailDelivery.template_key == "account.welcome")
        ).scalars().all()
        assert len(welcome_deliveries) == 1
        assert welcome_deliveries[0].user_id == user.id
    finally:
        db.close()


def test_public_login_wrong_password_is_generic_for_admin_and_missing_accounts():
    db = _session()
    try:
        admin = _user(db, "admin-login@example.com", role="admin")
        admin.password_hash = accounts_module.hash_password("Correctpass123!")
        reader = _user(db, "reader-login@example.com")
        reader.password_hash = accounts_module.hash_password("Readerpass123!")
        db.commit()

        for email in ("missing-login@example.com", "reader-login@example.com", "admin-login@example.com"):
            with pytest.raises(HTTPException) as exc:
                login(LoginPayload(email=email, password="Wrongpass123!"), db)
            assert exc.value.status_code == 401
            assert exc.value.detail == "Incorrect email or password."

        signed_in = login(LoginPayload(email="admin-login@example.com", password="Correctpass123!"), db)
        assert signed_in["authenticated"] is True
        assert signed_in["user"]["email"] == "admin-login@example.com"
    finally:
        db.close()


def test_failed_email_password_register_does_not_send_welcome(monkeypatch):
    sent: list[dict] = []
    monkeypatch.setattr("app.routers.accounts.send_email", lambda *_args, **kwargs: sent.append(kwargs) or {"status": "sent"})
    db = _session()
    try:
        with pytest.raises(HTTPException) as exc:
            register(_register_payload("weak-register-no-welcome@example.com", password="password"), db)

        assert exc.value.status_code == 422
        assert sent == []
        user = db.execute(
            select(UserAccount).where(UserAccount.email == "weak-register-no-welcome@example.com")
        ).scalar_one_or_none()
        assert user is None
    finally:
        db.close()


def test_user_delete_soft_deletes_paid_account_and_sends_reactivation(monkeypatch):
    monkeypatch.setenv("FRONTEND_BASE_URL", "https://app.walnut-intel.com")
    db = _session()
    sent: list[dict] = []
    stripe_calls: list[tuple[str, dict]] = []
    try:
        user = _user(db, "paid-delete@example.com", tier="premium")
        user.password_hash = "pbkdf2_sha256$210000$bad$bad"
        user.name = "Paid Reader"
        user.first_name = "Paid"
        user.last_name = "Reader"
        user.country = "US"
        user.state_province = "CA"
        user.postal_code = "94105"
        user.city = "San Francisco"
        user.address_line1 = "1 Market St"
        user.stripe_customer_id = "cus_delete"
        user.stripe_subscription_id = "sub_delete"
        user.stripe_price_id = "price_premium"
        user.subscription_status = "active"
        user.subscription_plan = "premium"
        user.access_expires_at = datetime.now(timezone.utc) + timedelta(days=12)
        db.commit()

        def fake_send_email(*_args, **kwargs):
            sent.append(kwargs)
            return {"status": "sent"}

        def fake_stripe_post(path, data):
            stripe_calls.append((path, data))
            assert path == "subscriptions/sub_delete"
            assert data == {"cancel_at_period_end": "true"}
            return {
                "object": "subscription",
                "id": "sub_delete",
                "customer": "cus_delete",
                "status": "active",
                "current_period_end": int((datetime.now(timezone.utc) + timedelta(days=12)).timestamp()),
                "cancel_at_period_end": True,
            }

        monkeypatch.setattr("app.routers.accounts.send_email", fake_send_email)
        monkeypatch.setattr("app.routers.accounts._stripe_post", fake_stripe_post)

        response = Response()
        result = delete_account(DeleteAccountPayload(confirmation="DELETE"), _request_for_user(user), response, db)
        db.refresh(user)

        assert result["status"] == "deleted"
        assert result["is_paid"] is True
        assert user.deleted_at is not None
        assert user.deleted_by_user is True
        assert user.is_suspended is True
        assert user.first_name == "Paid"
        assert user.last_name == "Reader"
        assert user.country == "US"
        assert user.city == "San Francisco"
        assert user.address_line1 == "1 Market St"
        assert user.original_email == "paid-delete@example.com"
        assert user.email.startswith("deleted+")
        assert user.stripe_customer_id == "cus_delete"
        assert user.stripe_subscription_id == "sub_delete"
        assert user.subscription_cancel_at_period_end is True
        assert stripe_calls == [("subscriptions/sub_delete", {"cancel_at_period_end": "true"})]
        assert user.reactivation_token_hash
        assert user.reactivation_expires_at is not None
        assert "Max-Age=0" in response.headers.get("set-cookie", "")
        assert sent[0]["template_key"] == "account.account_deleted_reactivation"
        assert sent[0]["to_email"] == "paid-delete@example.com"
        assert sent[0]["context"]["reactivate_url"].startswith("https://app.walnutmarkets.com/account/reactivate?token=")

        try:
            login(LoginPayload(email="paid-delete@example.com", password="Password123!"), db)
        except HTTPException as exc:
            assert exc.status_code == 403
            assert "deleted" in str(exc.detail).lower()
        else:
            raise AssertionError("Expected deleted account login rejection")

        reset = request_password_reset(PasswordResetRequestPayload(email="paid-delete@example.com"), db)
        assert reset["status"] == "ok"

        try:
            register(_register_payload("paid-delete@example.com"), db)
        except HTTPException as exc:
            assert exc.status_code == 403
            assert "recently deleted" in str(exc.detail).lower()
        else:
            raise AssertionError("Expected registration to be blocked during reactivation window")

        user.reactivation_expires_at = datetime.now(timezone.utc) - timedelta(seconds=1)
        db.commit()
        registered = register(_register_payload("paid-delete@example.com"), db)
        assert registered["user"]["id"] != user.id
        assert registered["user"]["email"] == "paid-delete@example.com"

        admin = _user(db, "admin-delete@example.com", role="admin")
        admin_result = admin_users(_request_for_user(admin), db, status="deleted", page=1, page_size=25)
        assert admin_result["total"] == 1
        assert admin_result["items"][0]["status"] == "deleted"
        assert admin_result["items"][0]["original_email"] == "paid-delete@example.com"
        assert admin_result["items"][0]["subscription_cancel_at_period_end"] is True
        assert admin_result["items"][0]["reactivation_expired"] is True
    finally:
        db.close()


def test_paid_account_delete_blocks_when_stripe_cancel_schedule_fails(monkeypatch):
    db = _session()
    try:
        user = _user(db, "paid-delete-fail@example.com", tier="premium")
        user.stripe_subscription_id = "sub_delete_fail"
        user.subscription_status = "active"
        user.subscription_plan = "premium"
        user.access_expires_at = datetime.now(timezone.utc) + timedelta(days=10)
        db.commit()

        monkeypatch.setattr(
            "app.routers.accounts._stripe_post",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(HTTPException(status_code=502, detail="Stripe down")),
        )

        try:
            delete_account(DeleteAccountPayload(confirmation="DELETE"), _request_for_user(user), Response(), db)
        except HTTPException as exc:
            assert exc.status_code == 502
            assert "could not schedule your subscription cancellation" in str(exc.detail).lower()
        else:
            raise AssertionError("Expected paid deletion to fail when Stripe cancellation scheduling fails")

        db.refresh(user)
        assert user.deleted_at is None
        assert user.email == "paid-delete-fail@example.com"
        assert user.subscription_cancel_at_period_end is False
    finally:
        db.close()


def test_past_due_paid_account_delete_schedules_stripe_cancellation(monkeypatch):
    db = _session()
    calls: list[tuple[str, dict]] = []
    try:
        user = _user(db, "past-due-delete@example.com", tier="premium")
        user.stripe_subscription_id = "sub_past_due_delete"
        user.subscription_status = "past_due"
        user.subscription_plan = "premium"
        user.access_expires_at = datetime.now(timezone.utc) + timedelta(days=5)
        db.commit()

        def fake_stripe_post(path, data):
            calls.append((path, data))
            return {
                "object": "subscription",
                "id": "sub_past_due_delete",
                "status": "past_due",
                "current_period_end": int((datetime.now(timezone.utc) + timedelta(days=5)).timestamp()),
                "cancel_at_period_end": True,
            }

        monkeypatch.setattr("app.routers.accounts._stripe_post", fake_stripe_post)
        monkeypatch.setattr("app.routers.accounts.send_email", lambda *_args, **_kwargs: {"status": "sent"})

        delete_account(DeleteAccountPayload(confirmation="DELETE"), _request_for_user(user), Response(), db)

        assert calls == [("subscriptions/sub_past_due_delete", {"cancel_at_period_end": "true"})]
        assert user.subscription_cancel_at_period_end is True
        assert user.deleted_at is not None
    finally:
        db.close()


def test_reactivation_token_restores_paid_or_free_and_is_single_use(monkeypatch):
    monkeypatch.setenv("FRONTEND_BASE_URL", "https://app.walnut-intel.com")
    db = _session()
    sent: list[dict] = []
    try:
        user = _user(db, "reactivate@example.com", tier="premium")
        user.first_name = "Rhea"
        user.last_name = "Activated"
        user.country = "US"
        user.state_province = "NY"
        user.postal_code = "10001"
        user.city = "New York"
        user.address_line1 = "11 Broadway"
        user.subscription_status = "active"
        user.subscription_plan = "premium"
        user.access_expires_at = datetime.now(timezone.utc) + timedelta(days=6)
        db.commit()

        monkeypatch.setattr("app.routers.accounts.send_email", lambda *_args, **kwargs: sent.append(kwargs) or {"status": "sent"})
        delete_account(DeleteAccountPayload(confirmation="DELETE"), _request_for_user(user), Response(), db)
        token = sent[0]["context"]["reactivate_url"].split("token=", 1)[1]

        result = reactivate_deleted_account(ReactivateAccountPayload(token=token), db)
        db.refresh(user)
        assert result["status"] == "reactivated"
        assert user.email == "reactivate@example.com"
        assert user.deleted_at is None
        assert user.is_suspended is False
        assert user.first_name == "Rhea"
        assert user.last_name == "Activated"
        assert user.country == "US"
        assert user.state_province == "NY"
        assert user.postal_code == "10001"
        assert user.city == "New York"
        assert user.address_line1 == "11 Broadway"
        assert user.entitlement_tier == "premium"
        assert user.reactivation_token_hash is None

        try:
            reactivate_deleted_account(ReactivateAccountPayload(token=token), db)
        except HTTPException as exc:
            assert exc.status_code == 400
        else:
            raise AssertionError("Expected single-use token rejection")

        expired = _user(db, "expired-reactivation@example.com", tier="premium")
        expired.subscription_status = "canceled"
        expired.subscription_plan = "premium"
        expired.access_expires_at = datetime.now(timezone.utc) - timedelta(days=1)
        db.commit()
        delete_account(DeleteAccountPayload(confirmation="DELETE"), _request_for_user(expired), Response(), db)
        expired_token = sent[-1]["context"]["reactivate_url"].split("token=", 1)[1]
        expired.reactivation_expires_at = datetime.now(timezone.utc) - timedelta(seconds=1)
        db.commit()
        try:
            reactivate_deleted_account(ReactivateAccountPayload(token=expired_token), db)
        except HTTPException as exc:
            assert exc.status_code == 400
            assert "Invalid or expired" in str(exc.detail)
        else:
            raise AssertionError("Expected expired token rejection")
    finally:
        db.close()


def test_reactivation_reconciles_active_stripe_subscription(monkeypatch):
    monkeypatch.setenv("FRONTEND_BASE_URL", "https://app.walnut-intel.com")
    monkeypatch.setenv("STRIPE_PRICE_ID_PREMIUM_MONTHLY", "price_reactivate_active")
    db = _session()
    sent: list[dict] = []

    def fake_stripe_get(path, params=None):
        if path == "subscriptions/sub_reactivate_active":
            return {
                "object": "subscription",
                "id": "sub_reactivate_active",
                "customer": "cus_reactivate_active",
                "status": "active",
                "current_period_end": 1_893_456_000,
                "cancel_at_period_end": True,
                "items": {"data": [{"price": {"id": "price_reactivate_active", "recurring": {"interval": "month"}}}]},
            }
        raise AssertionError(f"Unexpected Stripe GET path {path}")

    def fake_stripe_post(path, data):
        assert path == "subscriptions/sub_reactivate_active"
        assert data == {"cancel_at_period_end": "true"}
        return {
            "object": "subscription",
            "id": "sub_reactivate_active",
            "customer": "cus_reactivate_active",
            "status": "active",
            "current_period_end": 1_893_456_000,
            "cancel_at_period_end": True,
            "items": {"data": [{"price": {"id": "price_reactivate_active", "recurring": {"interval": "month"}}}]},
        }

    monkeypatch.setattr("app.routers.accounts._stripe_get", fake_stripe_get)
    monkeypatch.setattr("app.routers.accounts._stripe_post", fake_stripe_post)
    monkeypatch.setattr("app.routers.accounts.send_email", lambda *_args, **kwargs: sent.append(kwargs) or {"status": "sent"})
    try:
        user = _user(db, "reactivate-stripe@example.com", tier="free")
        user.stripe_customer_id = "cus_reactivate_active"
        user.stripe_subscription_id = "sub_reactivate_active"
        user.subscription_status = "canceled"
        user.subscription_plan = "free"
        user.access_expires_at = datetime.now(timezone.utc) + timedelta(days=6)
        db.commit()

        delete_account(DeleteAccountPayload(confirmation="DELETE"), _request_for_user(user), Response(), db)
        token = sent[0]["context"]["reactivate_url"].split("token=", 1)[1]
        result = reactivate_deleted_account(ReactivateAccountPayload(token=token), db)
        db.refresh(user)

        assert result["status"] == "reactivated"
        assert user.entitlement_tier == "premium"
        assert user.subscription_plan == "premium"
        assert user.subscription_status == "active"
        assert user.subscription_cancel_at_period_end is True
        assert result["subscription_cancel_at_period_end"] is True
        assert user.stripe_price_id == "price_reactivate_active"
        assert current_entitlements(_request_for_user(user), db).tier == "premium"
    finally:
        db.close()


def test_reactivation_reconciles_expired_stripe_subscription_to_free(monkeypatch):
    monkeypatch.setenv("FRONTEND_BASE_URL", "https://app.walnut-intel.com")
    monkeypatch.setenv("STRIPE_PRICE_ID_PREMIUM_MONTHLY", "price_reactivate_expired")
    db = _session()
    sent: list[dict] = []

    def fake_stripe_get(path, params=None):
        if path == "subscriptions/sub_reactivate_expired":
            return {
                "object": "subscription",
                "id": "sub_reactivate_expired",
                "customer": "cus_reactivate_expired",
                "status": "canceled",
                "current_period_end": 1_700_000_000,
                "cancel_at_period_end": False,
                "items": {"data": [{"price": {"id": "price_reactivate_expired", "recurring": {"interval": "month"}}}]},
            }
        raise AssertionError(f"Unexpected Stripe GET path {path}")

    def fake_stripe_post(path, data):
        assert path == "subscriptions/sub_reactivate_expired"
        assert data == {"cancel_at_period_end": "true"}
        return {
            "object": "subscription",
            "id": "sub_reactivate_expired",
            "customer": "cus_reactivate_expired",
            "status": "active",
            "current_period_end": int((datetime.now(timezone.utc) + timedelta(days=6)).timestamp()),
            "cancel_at_period_end": True,
        }

    monkeypatch.setattr("app.routers.accounts._stripe_get", fake_stripe_get)
    monkeypatch.setattr("app.routers.accounts._stripe_post", fake_stripe_post)
    monkeypatch.setattr("app.routers.accounts.send_email", lambda *_args, **kwargs: sent.append(kwargs) or {"status": "sent"})
    try:
        user = _user(db, "reactivate-expired-stripe@example.com", tier="premium")
        user.stripe_customer_id = "cus_reactivate_expired"
        user.stripe_subscription_id = "sub_reactivate_expired"
        user.subscription_status = "active"
        user.subscription_plan = "premium"
        user.access_expires_at = datetime.now(timezone.utc) + timedelta(days=6)
        db.commit()

        delete_account(DeleteAccountPayload(confirmation="DELETE"), _request_for_user(user), Response(), db)
        token = sent[0]["context"]["reactivate_url"].split("token=", 1)[1]
        result = reactivate_deleted_account(ReactivateAccountPayload(token=token), db)
        db.refresh(user)

        assert result["status"] == "reactivated"
        assert user.entitlement_tier == "free"
        assert user.subscription_status == "canceled"
        assert current_entitlements(_request_for_user(user), db).tier == "free"
    finally:
        db.close()


def test_billing_expiry_reminders_send_once_per_window():
    db = _session()
    now = datetime(2026, 6, 1, 12, tzinfo=timezone.utc)
    try:
        seven_day = _user(db, "seven-day-reminder@example.com", tier="premium")
        seven_day.first_name = "Seven"
        seven_day.subscription_status = "active"
        seven_day.subscription_plan = "premium"
        seven_day.stripe_subscription_id = "sub_7d"
        seven_day.subscription_cancel_at_period_end = True
        seven_day.access_expires_at = now + timedelta(days=7)

        renewing = _user(db, "renewing-reminder@example.com", tier="premium")
        renewing.subscription_status = "active"
        renewing.subscription_plan = "premium"
        renewing.stripe_subscription_id = "sub_renewing"
        renewing.subscription_cancel_at_period_end = False
        renewing.access_expires_at = now + timedelta(days=7)

        free = _user(db, "free-reminder@example.com", tier="free")
        free.subscription_status = "free"
        free.subscription_plan = "free"
        free.subscription_cancel_at_period_end = True
        free.access_expires_at = now + timedelta(days=7)
        db.commit()

        first = run_billing_expiry_reminders(db, window="7d", now=now)
        assert len(first) == 1
        assert first[0]["status"] == "skipped"
        assert first[0]["idempotency_key"] == f"billing_expiry_reminder:user:{seven_day.id}:subscription:sub_7d:window:7d"

        delivery = db.execute(select(EmailDelivery).where(EmailDelivery.template_key == "billing.subscription_expiry_reminder")).scalar_one()
        assert delivery.to_email == "seven-day-reminder@example.com"
        assert delivery.category == "billing"

        duplicate = run_billing_expiry_reminders(db, window="7d", now=now)
        assert duplicate == [
            {
                "user_id": seven_day.id,
                "email": "seven-day-reminder@example.com",
                "window": "7d",
                "idempotency_key": f"billing_expiry_reminder:user:{seven_day.id}:subscription:sub_7d:window:7d",
                "access_expires_at": seven_day.access_expires_at,
                "status": "duplicate",
                "delivery_id": delivery.id,
            }
        ]
    finally:
        db.close()


def test_billing_expiry_reminders_find_24_hour_window():
    db = _session()
    now = datetime(2026, 6, 1, 12, tzinfo=timezone.utc)
    try:
        user = _user(db, "day-before-reminder@example.com", tier="pro")
        user.subscription_status = "trialing"
        user.subscription_plan = "pro"
        user.stripe_subscription_id = "sub_24h"
        user.subscription_cancel_at_period_end = True
        user.access_expires_at = now + timedelta(hours=23)
        db.commit()

        result = run_billing_expiry_reminders(db, window="24h", now=now, dry_run=True)
        assert len(result) == 1
        assert result[0]["status"] == "dry_run"
        assert result[0]["window"] == "24h"
    finally:
        db.close()


def test_non_renewing_paid_access_downgrades_after_period_end():
    db = _session()
    try:
        user = _user(db, "expired-non-renewing@example.com", tier="premium")
        user.subscription_status = "active"
        user.subscription_plan = "premium"
        user.subscription_cancel_at_period_end = True
        user.access_expires_at = datetime.now(timezone.utc) - timedelta(seconds=1)
        db.commit()

        assert current_entitlements(_request_for_user(user), db).tier == "free"
    finally:
        db.close()


def test_register_verification_email_failure_does_not_fail_account_creation(monkeypatch):
    db = _session()
    try:
        def fake_send_email(*args, **kwargs):
            if kwargs.get("template_key") == "account.verify_email":
                raise RuntimeError("provider unavailable")
            return None

        monkeypatch.setattr("app.routers.accounts.send_email", fake_send_email)

        registered = register(_register_payload("reader-verification-failure@example.com"), db)

        user = db.get(UserAccount, registered["user"]["id"])
        assert user is not None
        assert user.email == "reader-verification-failure@example.com"
        assert user.password_hash
        assert user.email_verified_at is None
        assert user.email_verification_token_hash
        assert registered["email_verification_required"] is True
    finally:
        db.close()


def test_email_verification_link_resend_and_safe_states(monkeypatch):
    monkeypatch.setenv("APP_ENV", "test")
    db = _session()
    try:
        registered = register(_register_payload("reader-verify@example.com"), db)
        user = db.get(UserAccount, registered["user"]["id"])
        assert user is not None
        token = registered["dev_verification_url"].split("token=", 1)[1]

        verified = verify_email(token, db)
        db.refresh(user)
        settings = account_settings(_request_for_user(user), db)["user"]

        assert verified["status"] == "verified"
        assert user.email_verified_at is not None
        assert settings["email_verified"] is True
        assert settings["email_verification_required"] is False

        second_click = verify_email(token, db)
        assert second_click["status"] == "already_verified"
        already_verified_resend = resend_email_verification(_request_for_user(user), None, db)
        assert already_verified_resend["status"] == "ok"
        assert "dev_verification_url" not in already_verified_resend

        body_token_response = register(_register_payload("reader-body-verify@example.com"), db)
        body_token = body_token_response["dev_verification_url"].split("token=", 1)[1]
        assert verify_email("", db, VerifyEmailPayload(token=body_token))["status"] == "verified"

        resent_user_response = register(_register_payload("reader-resend-verify@example.com"), db)
        resent_user = db.get(UserAccount, resent_user_response["user"]["id"])
        assert resent_user is not None
        resend = resend_email_verification(_request_for_user(resent_user), None, db)
        assert resend["email_verification_required"] is True
        resent_token = resend["dev_verification_url"].split("token=", 1)[1]
        assert verify_email(resent_token, db)["status"] == "verified"

        with pytest.raises(HTTPException) as invalid:
            verify_email("short", db)
        assert invalid.value.status_code == 400
        assert invalid.value.detail["code"] == "invalid_verification_link"
        assert invalid.value.detail["message"] == "This verification link is invalid. Please request a new one."

        expired_response = register(_register_payload("reader-expired-verify@example.com"), db)
        expired_user = db.get(UserAccount, expired_response["user"]["id"])
        assert expired_user is not None
        expired_token = expired_response["dev_verification_url"].split("token=", 1)[1]
        expired_user.email_verification_expires_at = datetime.now(timezone.utc) - timedelta(seconds=1)
        db.commit()
        with pytest.raises(HTTPException) as expired:
            verify_email(expired_token, db)
        assert expired.value.status_code == 400
        assert expired.value.detail["code"] == "expired_verification_link"
        assert expired.value.detail["message"] == "This verification link has expired. Please request a new one."
    finally:
        db.close()


def test_verification_expiry_duration_copy_is_human_readable():
    assert format_expiry_duration(60) == "1 hour"
    assert format_expiry_duration(90) == "90 minutes"
    assert format_expiry_duration(1440) == "24 hours"
    assert format_expiry_duration(2880) == "48 hours"


def test_password_reset_mismatch_returns_422_and_keeps_token(monkeypatch):
    monkeypatch.setenv("APP_ENV", "test")
    monkeypatch.setenv("CT_ALLOW_INSECURE_RESET_LINK_RESPONSE", "1")
    db = _session()
    try:
        registered = register(_register_payload("reader-mismatch@example.com"), db)
        user = db.get(UserAccount, registered["user"]["id"])
        assert user is not None
        reset = request_password_reset(PasswordResetRequestPayload(email="reader-mismatch@example.com"), db)
        token = reset["reset_path"].split("token=", 1)[1]
        original_hash = user.password_reset_token_hash

        try:
            confirm_password_reset(
                PasswordResetConfirmPayload(token=token, password="Resetpass123!", confirm_password="Different123!"),
                db,
            )
        except HTTPException as exc:
            assert exc.status_code == 422
            assert "Passwords do not match." in str(exc.detail)
        else:
            raise AssertionError("Expected password reset mismatch rejection")

        db.refresh(user)
        assert user.password_reset_token_hash == original_hash
        assert user.password_reset_expires_at is not None
    finally:
        db.close()


def test_password_reset_invalid_and_expired_tokens_fail_safely(monkeypatch):
    monkeypatch.setenv("APP_ENV", "test")
    monkeypatch.setenv("CT_ALLOW_INSECURE_RESET_LINK_RESPONSE", "1")
    db = _session()
    try:
        registered = register(_register_payload("reader-expired@example.com"), db)
        user = db.get(UserAccount, registered["user"]["id"])
        assert user is not None

        try:
            confirm_password_reset(
                PasswordResetConfirmPayload(token="invalid-token-value", password="Resetpass123!", confirm_password="Resetpass123!"),
                db,
            )
        except HTTPException as exc:
            assert exc.status_code == 400
            assert "Invalid or expired reset link." in str(exc.detail)
        else:
            raise AssertionError("Expected invalid password reset token rejection")

        reset = request_password_reset(PasswordResetRequestPayload(email="reader-expired@example.com"), db)
        token = reset["reset_path"].split("token=", 1)[1]
        user.password_reset_expires_at = datetime.now(timezone.utc).replace(year=datetime.now(timezone.utc).year - 1)
        original_hash = user.password_reset_token_hash
        db.commit()

        try:
            confirm_password_reset(
                PasswordResetConfirmPayload(token=token, password="Resetpass123!", confirm_password="Resetpass123!"),
                db,
            )
        except HTTPException as exc:
            assert exc.status_code == 400
            assert "Invalid or expired reset link." in str(exc.detail)
        else:
            raise AssertionError("Expected expired password reset token rejection")

        db.refresh(user)
        assert user.password_reset_token_hash == original_hash
        assert user.password_reset_expires_at is not None
    finally:
        db.close()


def test_password_changed_email_failure_does_not_rollback_reset(monkeypatch):
    monkeypatch.setenv("APP_ENV", "test")
    monkeypatch.setenv("CT_ALLOW_INSECURE_RESET_LINK_RESPONSE", "1")
    db = _session()
    try:
        registered = register(_register_payload("reader-email-failure@example.com"), db)
        user = db.get(UserAccount, registered["user"]["id"])
        assert user is not None
        reset = request_password_reset(PasswordResetRequestPayload(email="reader-email-failure@example.com"), db)
        token = reset["reset_path"].split("token=", 1)[1]

        def fake_send_email(*args, **kwargs):
            if kwargs.get("template_key") == "account.password_changed":
                raise RuntimeError("provider unavailable")
            return None

        monkeypatch.setattr("app.routers.accounts.send_email", fake_send_email)

        confirmed = confirm_password_reset(
            PasswordResetConfirmPayload(token=token, password="Resetpass123!", confirm_password="Resetpass123!"),
            db,
        )

        assert confirmed["authenticated"] is False
        db.refresh(user)
        assert user.password_reset_token_hash is None
        assert user.password_reset_expires_at is None
        assert login(LoginPayload(email="reader-email-failure@example.com", password="Resetpass123!"), db)["user"]["id"] == user.id
    finally:
        db.close()


def test_password_reset_request_does_not_return_token_by_default(monkeypatch):
    monkeypatch.delenv("CT_ALLOW_INSECURE_RESET_LINK_RESPONSE", raising=False)
    db = _session()
    try:
        register(_register_payload("reader-reset@example.com"), db)

        reset = request_password_reset(PasswordResetRequestPayload(email="reader-reset@example.com"), db)

        assert reset == {
            "status": "ok",
            "message": "If an account exists for that email, reset instructions have been sent.",
        }
        user = db.execute(select(UserAccount).where(UserAccount.email == "reader-reset@example.com")).scalar_one()
        assert user.password_reset_token_hash
        assert user.password_reset_expires_at
    finally:
        db.close()


def test_account_profile_password_and_notification_settings_update():
    db = _session()
    try:
        registered = register(_register_payload("reader-settings@example.com"), db)
        user = db.get(UserAccount, registered["user"]["id"])
        assert user is not None
        request = _request_for_user(user)

        profile = update_account_profile(
            ProfileUpdatePayload(
                first_name="Reader",
                last_name="Updated",
                country="CA",
                state_province="ON",
                postal_code="M5V 2T6",
                city="Toronto",
                address_line1="100 King St W",
                address_line2="Floor 3",
            ),
            request,
            db,
        )
        assert profile["first_name"] == "Reader"
        assert profile["last_name"] == "Updated"
        assert profile["country"] == "CA"
        assert profile["state_province"] == "ON"
        assert profile["city"] == "Toronto"
        assert profile["address_line1"] == "100 King St W"
        assert profile["address_line2"] == "Floor 3"
        assert profile["email"] == "reader-settings@example.com"

        notifications = update_account_notifications(
            NotificationSettingsPayload(
                alerts_enabled=False,
                email_notifications_enabled=True,
                watchlist_activity_notifications=False,
                signals_notifications=True,
            ),
            request,
            db,
        )
        assert notifications["alerts_enabled"] is False
        assert account_settings(request, db)["notifications"]["watchlist_activity_notifications"] is False

        try:
            update_account_password(
                PasswordChangePayload(
                    current_password="wrongpassword",
                    new_password="Newpass1!",
                    confirm_password="Newpass1!",
                ),
                request,
                db,
            )
        except HTTPException as exc:
            assert exc.status_code == 401
        else:
            raise AssertionError("Expected current password verification failure")

        changed = update_account_password(
            PasswordChangePayload(
                current_password="Password123!",
                new_password="Newpass1!",
                confirm_password="Newpass1!",
            ),
            request,
            db,
        )
        assert changed["status"] == "ok"
        assert login(LoginPayload(email="reader-settings@example.com", password="Newpass1!"), db)["user"]["id"] == user.id
    finally:
        db.close()


def test_existing_account_without_billing_location_can_load_and_complete_profile():
    db = _session()
    try:
        user = _user(db, "legacy-reader@example.com")
        request = _request_for_user(user)

        settings = account_settings(request, db)
        assert settings["user"]["billing_profile_complete"] is False
        assert "country" in settings["user"]["billing_profile_missing_fields"]

        profile = update_account_profile(
            ProfileUpdatePayload(
                first_name="Legacy",
                last_name="Reader",
                country="US",
                state_province="NY",
                postal_code="10001",
                city="New York",
                address_line1="10 Broadway",
            ),
            request,
            db,
        )

        assert profile["billing_profile_complete"] is True
        assert profile["email"] == "legacy-reader@example.com"
    finally:
        db.close()


def test_normal_user_serializers_exclude_admin_and_stripe_identifiers():
    db = _session()
    try:
        registered = register(_register_payload("minimized@example.com"), db)
        user = db.get(UserAccount, registered["user"]["id"])
        assert user is not None
        user.stripe_customer_id = "cus_hidden"
        user.stripe_subscription_id = "sub_hidden"
        user.manual_tier_override = "premium"
        user.monthly_price_override = 1495
        user.override_currency = "USD"
        user.override_note = "support-only"
        user.original_email = "minimized-original@example.com"
        user.country = "US"
        user.state_province = "CA"
        user.postal_code = "94105"
        user.city = "San Francisco"
        user.address_line1 = "1 Market St"
        user.address_line2 = "Suite 200"
        user.deleted_by_user = True
        user.deletion_reason = "self_service"
        user.deletion_plan = "reactivation_window"
        user.reactivation_expires_at = datetime.now(timezone.utc)
        db.commit()
        request = _request_for_user(user)

        me_payload = me(request, db)
        auth_user = me_payload["user"]
        account_user = account_settings(request, db)["user"]

        forbidden = {
            "stripe_customer_id",
            "stripe_subscription_id",
            "manual_tier_override",
            "monthly_price_override",
            "annual_price_override",
            "override_currency",
            "override_note",
        }
        auth_only_forbidden = forbidden | {
            "original_email",
            "country",
            "state_province",
            "postal_code",
            "city",
            "address_line1",
            "address_line2",
            "deleted_at",
            "deleted_by_user",
            "deletion_reason",
            "deletion_plan",
            "reactivation_expires_at",
            "is_deleted",
            "email_verified_at",
        }
        assert auth_only_forbidden.isdisjoint(auth_user.keys())
        assert forbidden.isdisjoint(account_user.keys())
        assert {"id", "email", "name", "role", "is_admin", "entitlement_tier"}.issubset(auth_user.keys())
        assert {"email_verified", "email_verification_required", "current_plan", "subscription_status"}.issubset(auth_user.keys())
        assert "billing_profile_complete" not in auth_user
        assert account_user["country"] == "US"
        assert account_user["address_line1"] == "1 Market St"
        assert {"billing_location", "billing_profile_complete", "billing_profile_missing_fields"}.issubset(account_user.keys())
        assert "manual_tier_override" not in (me_payload["entitlements"]["user"] or {})
    finally:
        db.close()


def test_auth_me_returns_admin_effective_entitlements_without_sensitive_fields():
    db = _session()
    try:
        admin = _user(db, "admin-me@example.com", role="admin", tier="free")
        admin.stripe_customer_id = "cus_secret"
        admin.stripe_subscription_id = "sub_secret"
        db.commit()

        payload = me(_request_for_user(admin), db)

        assert payload["user"]["is_admin"] is True
        assert payload["user"]["role"] == "admin"
        assert payload["entitlements"]["tier"] == "admin"
        assert payload["entitlements"]["effective_tier"] == "admin"
        assert payload["entitlements"]["is_admin"] is True
        assert "signals" in payload["entitlements"]["features"]
        assert "options_flow_feed" in payload["entitlements"]["features"]
        assert "institutional_feed" in payload["entitlements"]["features"]
        forbidden = {
            "stripe_customer_id",
            "stripe_subscription_id",
            "stripe_price_id",
            "country",
            "state_province",
            "postal_code",
            "city",
            "address_line1",
            "address_line2",
            "deleted_at",
            "deleted_by_user",
            "deletion_reason",
            "deletion_plan",
            "reactivation_expires_at",
            "is_deleted",
            "email_verified_at",
        }
        assert forbidden.isdisjoint(payload["user"].keys())
    finally:
        db.close()


def test_password_policy_is_consistent_for_register_reset_and_change(monkeypatch):
    monkeypatch.setenv("APP_ENV", "test")
    monkeypatch.setenv("CT_ALLOW_INSECURE_RESET_LINK_RESPONSE", "1")
    db = _session()
    try:
        try:
            register(_register_payload("weak-register@example.com", password="password"), db)
        except HTTPException as exc:
            assert exc.status_code == 422
            assert "at least 3 of 4" in str(exc.detail)
        else:
            raise AssertionError("Expected weak registration password rejection")

        allowed = register(_register_payload("three-of-four@example.com", password="password123"), db)
        assert allowed["user"]["email"] == "three-of-four@example.com"

        registered = register(_register_payload("strong-flow@example.com", password="Password123!"), db)
        user = db.get(UserAccount, registered["user"]["id"])
        assert user is not None

        reset = request_password_reset(PasswordResetRequestPayload(email="strong-flow@example.com"), db)
        token = reset["reset_path"].split("token=", 1)[1]
        confirmed = confirm_password_reset(
            PasswordResetConfirmPayload(token=token, password="password123", confirm_password="password123"),
            db,
        )
        assert confirmed["authenticated"] is False
        assert confirmed["redirect_to"] == "/login?reset=success"
        assert login(LoginPayload(email="strong-flow@example.com", password="password123"), db)["user"]["id"] == user.id

        db.refresh(user)
        request = _request_for_user(user)
        changed = update_account_password(
            PasswordChangePayload(
                current_password="password123",
                new_password="Changedpass123",
                confirm_password="Changedpass123",
            ),
            request,
            db,
        )
        assert changed["status"] == "ok"
        assert login(LoginPayload(email="strong-flow@example.com", password="Changedpass123"), db)["user"]["id"] == user.id
    finally:
        db.close()


def test_password_policy_blocks_below_three_requirements(monkeypatch):
    monkeypatch.setenv("APP_ENV", "test")
    monkeypatch.setenv("CT_ALLOW_INSECURE_RESET_LINK_RESPONSE", "1")
    db = _session()
    try:
        registered = register(_register_payload("weak-flow@example.com", password="Password123!"), db)
        user = db.get(UserAccount, registered["user"]["id"])
        assert user is not None

        reset = request_password_reset(PasswordResetRequestPayload(email="weak-flow@example.com"), db)
        token = reset["reset_path"].split("token=", 1)[1]
        original_hash = user.password_reset_token_hash
        try:
            confirm_password_reset(
                PasswordResetConfirmPayload(token=token, password="password", confirm_password="password"),
                db,
            )
        except HTTPException as exc:
            assert exc.status_code == 422
            assert "at least 3 of 4" in str(exc.detail)
        else:
            raise AssertionError("Expected weak reset password rejection")
        db.refresh(user)
        assert user.password_reset_token_hash == original_hash
        assert user.password_reset_expires_at is not None

        confirmed = confirm_password_reset(
            PasswordResetConfirmPayload(token=token, password="Resetpass123!", confirm_password="Resetpass123!"),
            db,
        )
        assert confirmed["authenticated"] is False
        assert confirmed["redirect_to"] == "/login?reset=success"
        assert login(LoginPayload(email="weak-flow@example.com", password="Resetpass123!"), db)["user"]["id"] == user.id

        request = _request_for_user(user)
        try:
            update_account_password(
                PasswordChangePayload(
                    current_password="Resetpass123!",
                    new_password="password",
                    confirm_password="password",
                ),
                request,
                db,
            )
        except HTTPException as exc:
            assert exc.status_code == 422
            assert "at least 3 of 4" in str(exc.detail)
        else:
            raise AssertionError("Expected weak account password rejection")

        changed = update_account_password(
            PasswordChangePayload(
                current_password="Resetpass123!",
                new_password="Changedpass123!",
                confirm_password="Changedpass123!",
            ),
            request,
            db,
        )
        assert changed["status"] == "ok"
        assert login(LoginPayload(email="weak-flow@example.com", password="Changedpass123!"), db)["user"]["id"] == user.id
    finally:
        db.close()


def test_legacy_watchlist_attaches_to_moore_account_on_registration():
    db = _session()
    try:
        legacy = Watchlist(name="Legacy")
        db.add(legacy)
        db.commit()
        db.refresh(legacy)
        assert legacy.owner_user_id is None

        response = register(_register_payload("moore11j@gmail.com"), db)
        db.refresh(legacy)
        assert legacy.owner_user_id == response["user"]["id"]
        user = db.get(UserAccount, response["user"]["id"])
        assert user is not None
        assert user.role == "user"
        assert response["user"]["is_admin"] is False
    finally:
        db.close()


def test_register_configured_admin_email_creates_normal_user(monkeypatch):
    monkeypatch.setenv("ADMIN_EMAILS", "configured-admin@example.com")
    db = _session()
    try:
        response = register(_register_payload("configured-admin@example.com"), db)

        assert response["user"]["role"] == "user"
        assert response["user"]["is_admin"] is False
        user = db.get(UserAccount, response["user"]["id"])
        assert user is not None
        assert user.role == "user"
    finally:
        db.close()


def test_admin_emails_has_no_hardcoded_personal_address(monkeypatch):
    monkeypatch.delenv("ADMIN_EMAILS", raising=False)

    assert "moore11j@gmail.com" not in admin_emails()


def test_admin_settings_lists_registered_accounts_without_sensitive_fields(monkeypatch):
    monkeypatch.setenv("ADMIN_EMAILS", "admin@example.com")
    monkeypatch.delenv("STRIPE_SECRET_KEY", raising=False)
    monkeypatch.delenv("STRIPE_WEBHOOK_SECRET", raising=False)
    monkeypatch.setenv("STRIPE_PRICE_ID_PREMIUM_MONTHLY", "price_premium_monthly")
    monkeypatch.setenv("STRIPE_PRICE_ID_PREMIUM_ANNUAL", "price_premium_annual")
    monkeypatch.setenv("STRIPE_PRICE_ID_PRO_MONTHLY", "price_pro_monthly")
    monkeypatch.setenv("STRIPE_PRICE_ID_PRO_ANNUAL", "price_pro_annual")
    monkeypatch.setenv("FRONTEND_BASE_URL", "https://www.walnut-intel.com")
    monkeypatch.delenv("FRONTEND_APP_URL", raising=False)
    monkeypatch.delenv("APP_BASE_URL", raising=False)
    monkeypatch.delenv("STRIPE_CUSTOMER_PORTAL_RETURN_URL", raising=False)
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        _user(db, "reader@example.com")

        default_response = admin_settings(_request_for_user(admin), db)
        assert default_response["users"] == []
        assert default_response["users_limit"] == 0
        assert default_response["users_truncated"] is False

        response = admin_settings(_request_for_user(admin), db, include_users=True)

        assert len(response["users"]) == 2
        assert response["users_limit"] == 100
        assert response["users_truncated"] is False
        assert {"email", "name", "created_at", "last_seen_at"}.issubset(response["users"][0].keys())
        forbidden = {"password", "password_hash", "card", "payment_method", "stripe_customer_id", "stripe_subscription_id"}
        assert forbidden.isdisjoint(response["users"][0].keys())
        assert response["stripe"]["secret_key"] in {"configured", "missing"}
        assert response["stripe"]["price_ids"] == {
            "premium_monthly": "price_premium_monthly",
            "premium_annual": "price_premium_annual",
            "pro_monthly": "price_pro_monthly",
            "pro_annual": "price_pro_annual",
        }
        assert response["stripe"]["missing_price_ids"] == []
        assert response["stripe"]["missing_price_env_vars"] == []
        assert set(response["stripe"]["missing_env_vars"]) == {"STRIPE_SECRET_KEY", "STRIPE_WEBHOOK_SECRET"}
        assert response["stripe"]["webhook_url"] == "https://congress-tracker-api.fly.dev/api/billing/stripe/webhook"
        assert response["stripe"]["portal_return_url"] == "https://app.walnutmarkets.com/account/billing?portal_return=1"
        assert response["stripe"]["success_url"] == "https://app.walnutmarkets.com/account/billing?checkout=success"
        assert response["stripe"]["cancel_url"] == "https://app.walnutmarkets.com/pricing?checkout=cancelled"
        assert admin_settings(_request_for_user(admin), db, include_users=False)["users"] == []
        assert admin_settings(_request_for_user(admin), db, include_users=False)["users_limit"] == 0
    finally:
        db.close()


def test_admin_settings_reports_billing_readiness_and_missing_price_ids(monkeypatch):
    monkeypatch.setenv("ADMIN_EMAILS", "admin@example.com")
    monkeypatch.setenv("STRIPE_SECRET_KEY", "sk_test_hidden")
    monkeypatch.setenv("STRIPE_WEBHOOK_SECRET", "whsec_hidden")
    monkeypatch.setenv("STRIPE_PRICE_ID_PREMIUM_MONTHLY", "price_premium_monthly")
    monkeypatch.delenv("STRIPE_PRICE_ID_PREMIUM_ANNUAL", raising=False)
    monkeypatch.delenv("STRIPE_PRICE_ID_PRO_MONTHLY", raising=False)
    monkeypatch.delenv("STRIPE_PRICE_ID_PRO_ANNUAL", raising=False)
    for legacy_name in (
        "STRIPE_PRICE_ID",
        "STRIPE_PRICE_ID_MONTHLY",
        "STRIPE_PRICE_ID_ANNUAL",
        "STRIPE_PRO_PRICE_ID",
        "STRIPE_PRO_PRICE_ID_MONTHLY",
        "STRIPE_PRO_PRICE_ID_ANNUAL",
    ):
        monkeypatch.delenv(legacy_name, raising=False)
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")

        response = admin_settings(_request_for_user(admin), db, include_users=False)
        stripe = response["stripe"]

        assert stripe["configured"] is False
        assert stripe["billing_enabled"] is False
        assert stripe["overall"]["ready"] is False
        assert stripe["checkout"]["ready"] is False
        assert stripe["webhooks"]["ready"] is True
        assert "customer.deleted" in stripe["webhooks"]["recommended_events"]
        assert "customer.deleted" in stripe["webhook_events"]
        assert stripe["secret_key"] == "configured"
        assert stripe["webhook_secret"] == "configured"
        assert stripe["price_ids"] == {
            "premium_monthly": "price_premium_monthly",
            "premium_annual": "missing",
            "pro_monthly": "missing",
            "pro_annual": "missing",
        }
        assert stripe["missing_price_ids"] == ["premium_annual", "pro_monthly", "pro_annual"]
        assert stripe["missing_price_env_vars"] == [
            "STRIPE_PRICE_ID_PREMIUM_ANNUAL",
            "STRIPE_PRICE_ID_PRO_MONTHLY",
            "STRIPE_PRICE_ID_PRO_ANNUAL",
        ]
        assert stripe["admin_free_grants"]["ready"] is False
        assert stripe["admin_free_grants"]["prices"]["premium"]["configured"] is False
        assert stripe["admin_free_grants"]["prices"]["pro"]["configured"] is False
        assert stripe["missing_admin_free_price_env_vars"] == [
            "STRIPE_PREMIUM_ADMIN_FREE_PRICE_ID",
            "STRIPE_PRO_ADMIN_FREE_PRICE_ID",
        ]
        assert "sk_test_hidden" not in json.dumps(stripe)
        assert "whsec_hidden" not in json.dumps(stripe)
    finally:
        db.close()


def test_billing_readiness_uses_selected_checkout_plan(monkeypatch):
    monkeypatch.setenv("STRIPE_SECRET_KEY", "sk_test_hidden")
    monkeypatch.delenv("STRIPE_WEBHOOK_SECRET", raising=False)
    monkeypatch.setenv("STRIPE_PRICE_ID_PREMIUM_MONTHLY", "price_premium_monthly")
    monkeypatch.delenv("STRIPE_PRICE_ID_PREMIUM_ANNUAL", raising=False)
    monkeypatch.delenv("STRIPE_PRICE_ID_PRO_MONTHLY", raising=False)
    monkeypatch.delenv("STRIPE_PRICE_ID_PRO_ANNUAL", raising=False)
    for legacy_name in (
        "STRIPE_PRICE_ID",
        "STRIPE_PRICE_ID_MONTHLY",
        "STRIPE_PRICE_ID_ANNUAL",
        "STRIPE_PRO_PRICE_ID",
        "STRIPE_PRO_PRICE_ID_MONTHLY",
        "STRIPE_PRO_PRICE_ID_ANNUAL",
    ):
        monkeypatch.delenv(legacy_name, raising=False)

    premium_monthly = billing_readiness(checkout_tier="premium", checkout_interval="monthly")
    pro_annual = billing_readiness(checkout_tier="pro", checkout_interval="annual")

    assert premium_monthly["checkout"]["ready"] is True
    assert premium_monthly["overall"]["ready"] is False
    assert premium_monthly["webhooks"]["ready"] is False
    assert pro_annual["checkout"]["ready"] is False
    assert pro_annual["checkout"]["missing_env_vars"] == ["STRIPE_PRICE_ID_PRO_ANNUAL"]


def test_live_billing_readiness_ignores_legacy_price_aliases(monkeypatch):
    monkeypatch.setenv("STRIPE_SECRET_KEY", "sk_live_hidden")
    monkeypatch.setenv("STRIPE_WEBHOOK_SECRET", "whsec_hidden")
    monkeypatch.delenv("STRIPE_PRICE_ID_PREMIUM_MONTHLY", raising=False)
    monkeypatch.setenv("STRIPE_PRICE_ID_MONTHLY", "price_legacy_test_monthly")
    monkeypatch.setenv("STRIPE_PRICE_ID_PREMIUM_ANNUAL", "price_premium_annual")
    monkeypatch.setenv("STRIPE_PRICE_ID_PRO_MONTHLY", "price_pro_monthly")
    monkeypatch.setenv("STRIPE_PRICE_ID_PRO_ANNUAL", "price_pro_annual")

    readiness = billing_readiness(checkout_tier="premium", checkout_interval="monthly")

    assert readiness["secret_key_mode"] == "live"
    assert readiness["price_ids"]["premium_monthly"] == "missing"
    assert readiness["checkout"]["ready"] is False
    assert readiness["checkout"]["missing_env_vars"] == ["STRIPE_PRICE_ID_PREMIUM_MONTHLY"]
    assert readiness["legacy_price_env_vars_present"] == ["STRIPE_PRICE_ID_MONTHLY"]
    assert {error["code"] for error in readiness["live_mode_errors"]} == {
        "live_missing_required_env_vars",
        "live_legacy_price_env_vars_present",
    }


def test_admin_reports_summary_requires_admin(monkeypatch):
    monkeypatch.setenv("ADMIN_EMAILS", "admin@example.com")
    db = _session()
    try:
        user = _user(db, "reader@example.com")
        try:
            admin_reports_summary(_request_for_user(user), db)
        except HTTPException as exc:
            assert exc.status_code == 403
        else:
            raise AssertionError("Expected admin access failure")
    finally:
        db.close()


def test_admin_reports_summary_returns_expected_metrics_and_keys(monkeypatch):
    monkeypatch.setenv("ADMIN_EMAILS", "admin@example.com")
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        free_user = _user(db, "free@example.com")
        free_user.created_at = datetime.now(timezone.utc)
        free_user.last_seen_at = datetime.now(timezone.utc)

        monthly_user = _user(db, "monthly@example.com")
        monthly_user.subscription_status = "active"
        monthly_user.subscription_plan = "premium"
        monthly_user.access_expires_at = datetime(2030, 1, 1, tzinfo=timezone.utc)
        monthly_user.last_seen_at = datetime.now(timezone.utc)

        annual_user = _user(db, "annual@example.com")
        annual_user.subscription_status = "trialing"
        annual_user.subscription_plan = "premium"
        annual_user.access_expires_at = datetime(2030, 1, 1, tzinfo=timezone.utc)
        annual_user.last_seen_at = datetime.now(timezone.utc)

        db.add(BillingTransaction(user_id=monthly_user.id, billing_period_type="monthly", total_amount=1995, payment_status="paid", charged_at=datetime.now(timezone.utc)))
        db.add(BillingTransaction(user_id=annual_user.id, billing_period_type="annual", total_amount=19995, payment_status="paid", charged_at=datetime.now(timezone.utc)))
        db.commit()

        summary = admin_reports_summary(_request_for_user(admin), db)

        assert set(summary.keys()) >= {
            "active_free_users",
            "active_premium_users",
            "monthly_recurring_revenue",
            "revenue_ytd",
            "new_users_last_30_days",
            "total_users",
            "currency",
            "generated_at",
        }
        assert summary["active_free_users"] == 1
        assert summary["active_premium_users"] == 2
        assert summary["monthly_recurring_revenue"] == 36.61
        assert summary["revenue_ytd"] == 219.9
        assert summary["new_users_last_30_days"] >= 4
        assert summary["total_users"] == 4
        assert summary["currency"] == "USD"

        generated_at = datetime.fromisoformat(summary["generated_at"])
        assert generated_at.tzinfo is not None
    finally:
        db.close()


def test_admin_reports_summary_uses_created_at_and_revenue_fallback_notes(monkeypatch):
    monkeypatch.setenv("ADMIN_EMAILS", "admin@example.com")
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        free_user = _user(db, "free@example.com")
        free_user.created_at = datetime.now(timezone.utc)
        free_user.last_seen_at = None
        db.commit()

        summary = admin_reports_summary(_request_for_user(admin), db)

        assert summary["active_free_users"] == 1
        assert summary["revenue_ytd"] == 0
        assert "notes" in summary
        assert any("created_at fallback" in note for note in summary["notes"])
        assert any(note == "Revenue collection data not connected yet." for note in summary["notes"])
    finally:
        db.close()


def test_admin_users_filters_and_paginates(monkeypatch):
    monkeypatch.setenv("ADMIN_EMAILS", "admin@example.com")
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        premium = _user(db, "premium@example.com", tier="premium")
        premium.name = "Premium Reader"
        premium.country = "CA"
        premium.state_province = "ON"
        premium.subscription_status = "active"
        premium.access_expires_at = datetime(2026, 5, 1, tzinfo=timezone.utc)
        free = _user(db, "free@example.com")
        free.name = "Free Reader"
        free.country = "US"
        free.state_province = "CA"
        suspended = _user(db, "suspended@example.com")
        suspended.country = "US"
        suspended.is_suspended = True
        db.commit()

        premium_response = admin_users(
            _request_for_user(admin),
            db,
            plan="premium",
            status=None,
            country=None,
            admin="non_admin",
            sort_by="email",
            sort_dir="asc",
            page=1,
            page_size=1,
        )
        assert premium_response["total"] == 1
        assert premium_response["total_pages"] == 1
        assert premium_response["items"][0]["email"] == "premium@example.com"
        assert premium_response["items"][0]["user_display_id"] == f"U-{premium.id:06d}"
        assert premium_response["items"][0]["user_id_display"] == f"U-{premium.id:06d}"
        assert "@" not in premium_response["items"][0]["user_display_id"]
        assert premium_response["items"][0]["plan"] == "premium"
        assert premium_response["items"][0]["status"] == "active"
        assert premium_response["items"][0]["admin_flag"] == "no"
        assert "password_hash" not in premium_response["items"][0]
        assert "stripe_customer_id" not in premium_response["items"][0]
        assert "stripe_subscription_id" not in premium_response["items"][0]

        suspended_response = admin_users(
            _request_for_user(admin),
            db,
            plan="all",
            status="suspended",
            country="US",
            admin="non_admin",
            sort_by="created_at",
            sort_dir="desc",
            page=1,
            page_size=25,
        )
        assert suspended_response["total"] == 1
        assert suspended_response["items"][0]["email"] == "suspended@example.com"
        assert suspended_response["has_previous"] is False
        assert suspended_response["has_next"] is False

        admin_response = admin_users(
            _request_for_user(admin),
            db,
            plan="all",
            status=None,
            country=None,
            admin="admin",
            sort_by="created_at",
            sort_dir="desc",
            page=1,
            page_size=25,
        )
        assert admin_response["total"] == 1
        assert admin_response["items"][0]["is_admin"] is True
    finally:
        db.close()


def test_admin_users_excludes_deleted_by_default_and_exports(monkeypatch):
    monkeypatch.setenv("ADMIN_EMAILS", "admin@example.com")
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        active = _user(db, "active-default@example.com")
        deleted = _user(db, "deleted-default@example.com")
        deleted.deleted_at = datetime.now(timezone.utc)
        deleted.original_email = "deleted-original@example.com"
        db.commit()

        default_response = admin_users(
            _request_for_user(admin),
            db,
            plan="all",
            status=None,
            country=None,
            admin="non_admin",
            sort_by="email",
            sort_dir="asc",
            page=1,
            page_size=25,
        )
        deleted_response = admin_users(
            _request_for_user(admin),
            db,
            plan="all",
            status="deleted",
            country=None,
            admin="non_admin",
            sort_by="email",
            sort_dir="asc",
            page=1,
            page_size=25,
        )
        all_response = admin_users(
            _request_for_user(admin),
            db,
            plan="all",
            status="all_with_deleted",
            country=None,
            admin="non_admin",
            sort_by="email",
            sort_dir="asc",
            page=1,
            page_size=25,
        )
        export = admin_users_export(
            "xlsx",
            _request_for_user(admin),
            db,
            plan="all",
            status=None,
            country=None,
            admin="non_admin",
            sort_by="email",
            sort_dir="asc",
        )

        assert default_response["total"] == 1
        assert [item["email"] for item in default_response["items"]] == [active.email]
        assert deleted_response["total"] == 1
        assert deleted_response["items"][0]["status"] == "deleted"
        assert deleted_response["items"][0]["original_email"] == "deleted-original@example.com"
        assert all_response["total"] == 2
        with zipfile.ZipFile(BytesIO(export.body)) as workbook:
            sheet_xml = workbook.read("xl/worksheets/sheet1.xml").decode("utf-8")
        assert "active-default@example.com" in sheet_xml
        assert "deleted-default@example.com" not in sheet_xml
        assert "deleted-original@example.com" not in sheet_xml
    finally:
        db.close()


def test_admin_users_search_matches_email_name_ids_and_filters(monkeypatch):
    monkeypatch.setenv("ADMIN_EMAILS", "admin@example.com")
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        nancy = _user(db, "nancy@example.com", tier="premium")
        nancy.name = "Nancy Pelosi"
        nancy.first_name = "Nancy"
        nancy.last_name = "Pelosi"
        nancy.subscription_status = "active"
        nancy.country = "US"
        other = _user(db, "other@example.com", tier="premium")
        other.name = "Other Reader"
        other.subscription_status = "active"
        suspended = _user(db, "suspended-reader@example.com", tier="premium")
        suspended.name = "Nancy Suspended"
        suspended.is_suspended = True
        db.commit()

        base_params = {
            "plan": "all",
            "status": None,
            "country": None,
            "admin": "non_admin",
            "sort_by": "email",
            "sort_dir": "asc",
            "page": 1,
            "page_size": 25,
        }

        email_response = admin_users(_request_for_user(admin), db, search="nancy@example.com", **base_params)
        assert [item["email"] for item in email_response["items"]] == ["nancy@example.com"]
        assert email_response["filters"]["search"] == "nancy@example.com"

        name_response = admin_users(_request_for_user(admin), db, search="pelosi", **base_params)
        assert [item["email"] for item in name_response["items"]] == ["nancy@example.com"]

        raw_id_response = admin_users(_request_for_user(admin), db, search=str(nancy.id), **base_params)
        assert "nancy@example.com" in {item["email"] for item in raw_id_response["items"]}

        display_id_response = admin_users(_request_for_user(admin), db, search=f"U-{nancy.id:06d}", **base_params)
        assert [item["email"] for item in display_id_response["items"]] == ["nancy@example.com"]

        filtered_response = admin_users(
            _request_for_user(admin),
            db,
            search="nancy",
            plan="premium",
            status="active",
            country=None,
            admin="non_admin",
            sort_by="email",
            sort_dir="asc",
            page=1,
            page_size=25,
        )
        assert [item["email"] for item in filtered_response["items"]] == ["nancy@example.com"]
    finally:
        db.close()


def test_admin_users_requires_admin(monkeypatch):
    monkeypatch.setenv("ADMIN_EMAILS", "admin@example.com")
    db = _session()
    try:
        user = _user(db, "user@example.com")
        with pytest.raises(HTTPException) as exc:
            admin_users(_request_for_user(user), db)
        assert exc.value.status_code == 403
    finally:
        db.close()


def test_admin_email_deliveries_paginates_and_caps_page_size(monkeypatch):
    monkeypatch.setenv("ADMIN_EMAILS", "admin@example.com")
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        base = datetime.now(timezone.utc)
        for index in range(12):
            _email_delivery(db, to_email=f"user{index}@example.com", created_at=base + timedelta(seconds=index))

        response = admin_email_deliveries(_request_for_user(admin), db, date_window="all_time", page=1, page_size=5)
        assert response["total"] == 12
        assert response["page_size"] == 5
        assert response["total_pages"] == 3
        assert len(response["items"]) == 5
        assert response["items"][0]["to_email"] == "user11@example.com"

        capped = admin_email_deliveries(_request_for_user(admin), db, date_window="all_time", page=1, page_size=250)
        assert capped["page_size"] == 100
        assert capped["total"] == 12
    finally:
        db.close()


def test_admin_email_deliveries_filters_recipient_status_and_template(monkeypatch):
    monkeypatch.setenv("ADMIN_EMAILS", "admin@example.com")
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        _email_delivery(db, to_email="moore11j@gmail.com", status="sent", template_key="alerts.watchlist_activity")
        _email_delivery(db, to_email="MOORE11J+failed@gmail.com", status="failed", template_key="alerts.watchlist_activity")
        _email_delivery(db, to_email="nancy@example.com", status="sent", template_key="account.welcome")

        recipient_response = admin_email_deliveries(
            _request_for_user(admin),
            db,
            recipient="MOORE11J",
            date_window="all_time",
            page=1,
            page_size=10,
        )
        assert recipient_response["total"] == 2
        assert {item["to_email"] for item in recipient_response["items"]} == {
            "moore11j@gmail.com",
            "MOORE11J+failed@gmail.com",
        }

        combined = admin_email_deliveries(
            _request_for_user(admin),
            db,
            recipient="moore11j",
            status="sent",
            template_key="alerts.watchlist_activity",
            date_window="all_time",
            page=1,
            page_size=10,
        )
        assert combined["total"] == 1
        assert combined["items"][0]["to_email"] == "moore11j@gmail.com"
        assert combined["filters"] == {
            "recipient": "moore11j",
            "status": "sent",
            "template_key": "alerts.watchlist_activity",
            "date_window": "all_time",
        }
    finally:
        db.close()


def test_admin_email_deliveries_date_windows(monkeypatch):
    monkeypatch.setenv("ADMIN_EMAILS", "admin@example.com")
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        now = datetime.now(timezone.utc)
        _email_delivery(db, to_email="today@example.com", created_at=now)
        _email_delivery(db, to_email="three@example.com", created_at=now - timedelta(days=3))
        _email_delivery(db, to_email="ten@example.com", created_at=now - timedelta(days=10))
        _email_delivery(db, to_email="twenty@example.com", created_at=now - timedelta(days=20))
        _email_delivery(db, to_email="forty@example.com", created_at=now - timedelta(days=40))

        today = admin_email_deliveries(_request_for_user(admin), db, date_window="today", page=1, page_size=25)
        assert "today@example.com" in {item["to_email"] for item in today["items"]}

        last_7 = admin_email_deliveries(_request_for_user(admin), db, date_window="last_7", page=1, page_size=25)
        assert last_7["total"] == 2
        last_14 = admin_email_deliveries(_request_for_user(admin), db, date_window="last_14", page=1, page_size=25)
        assert last_14["total"] == 3
        last_30 = admin_email_deliveries(_request_for_user(admin), db, date_window="last_30", page=1, page_size=25)
        assert last_30["total"] == 4
    finally:
        db.close()


def test_admin_email_deliveries_last_month_uses_previous_calendar_month(monkeypatch):
    monkeypatch.setenv("ADMIN_EMAILS", "admin@example.com")
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        pacific = ZoneInfo("America/Los_Angeles")
        current_local = datetime.now(timezone.utc).astimezone(pacific).date()
        first_this_month = current_local.replace(day=1)
        if first_this_month.month == 1:
            previous_month = first_this_month.replace(year=first_this_month.year - 1, month=12)
        else:
            previous_month = first_this_month.replace(month=first_this_month.month - 1)
        previous_month_midpoint = previous_month.replace(day=min(15, previous_month.day))
        last_month_ts = datetime.combine(previous_month_midpoint, datetime.min.time(), tzinfo=pacific).replace(hour=12).astimezone(timezone.utc)

        _email_delivery(db, to_email="last-month@example.com", created_at=last_month_ts)
        _email_delivery(db, to_email="this-month@example.com", created_at=datetime.now(timezone.utc))

        response = admin_email_deliveries(_request_for_user(admin), db, date_window="last_month", page=1, page_size=10)
        assert response["total"] == 1
        assert response["items"][0]["to_email"] == "last-month@example.com"
    finally:
        db.close()


def test_admin_email_deliveries_requires_admin(monkeypatch):
    monkeypatch.setenv("ADMIN_EMAILS", "admin@example.com")
    db = _session()
    try:
        user = _user(db, "user@example.com")
        with pytest.raises(HTTPException) as exc:
            admin_email_deliveries(_request_for_user(user), db)
        assert exc.value.status_code == 403
    finally:
        db.close()


def test_admin_users_include_display_safe_price_and_billing_fields(monkeypatch):
    monkeypatch.setenv("ADMIN_EMAILS", "admin@example.com")
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        free = _user(db, "free@example.com")

        premium_monthly = _user(db, "premium-monthly@example.com", tier="premium")
        premium_monthly.subscription_status = "active"
        premium_monthly.subscription_interval = "monthly"
        premium_monthly.stripe_customer_id = "cus_monthly"
        premium_monthly.stripe_subscription_id = "sub_monthly"
        db.add(
            BillingTransaction(
                user_id=premium_monthly.id,
                stripe_customer_id="cus_monthly",
                stripe_subscription_id="sub_monthly",
                stripe_invoice_id="in_monthly",
                billing_period_type="monthly",
                subtotal_amount=1995,
                total_amount=2170,
                currency="USD",
                charged_at=datetime(2026, 4, 1, tzinfo=timezone.utc),
                payment_status="paid",
                payload_json='{"payment_method": "hidden"}',
            )
        )

        premium_annual = _user(db, "premium-annual@example.com", tier="premium")
        premium_annual.subscription_status = "active"
        premium_annual.subscription_interval = "annual"
        db.add(
            BillingTransaction(
                user_id=premium_annual.id,
                stripe_invoice_id="in_annual",
                billing_period_type="annual",
                subtotal_amount=19995,
                total_amount=19995,
                currency="USD",
                charged_at=datetime(2026, 4, 2, tzinfo=timezone.utc),
                payment_status="paid",
            )
        )

        pro_monthly = _user(db, "pro-monthly@example.com", tier="pro")
        pro_monthly.subscription_status = "active"
        pro_monthly.subscription_interval = "monthly"

        pro_annual = _user(db, "pro-annual@example.com", tier="pro")
        pro_annual.subscription_status = "active"
        pro_annual.subscription_interval = "annual"
        db.add(
            BillingTransaction(
                user_id=pro_annual.id,
                billing_period_type="annual",
                subtotal_amount=49995,
                currency="USD",
                charged_at=datetime(2026, 4, 3, tzinfo=timezone.utc),
                payment_status="paid",
            )
        )

        override = _user(db, "override@example.com", tier="premium")
        override.subscription_status = "active"
        override.monthly_price_override = 1495
        override.override_currency = "USD"
        db.commit()

        response = admin_users(
            _request_for_user(admin),
            db,
            plan="all",
            status=None,
            country=None,
            admin="non_admin",
            sort_by="email",
            sort_dir="asc",
            page=1,
            page_size=25,
        )
        rows = {item["email"]: item for item in response["items"]}

        assert rows["free@example.com"]["billing_price_amount"] is None
        assert rows["free@example.com"]["billing_frequency"] is None
        assert rows["premium-monthly@example.com"]["subscription_price_amount"] == 2495
        assert rows["premium-monthly@example.com"]["subscription_currency"] == "USD"
        assert rows["premium-monthly@example.com"]["current_plan_amount_cents"] == 2495
        assert rows["premium-monthly@example.com"]["current_plan_display"] == "USD $24.95 / month"
        assert rows["premium-monthly@example.com"]["total_paid_cents"] == 2170
        assert rows["premium-monthly@example.com"]["last_payment_amount_cents"] == 2170
        assert rows["premium-monthly@example.com"]["billing_price_display"] == "USD $24.95"
        assert rows["premium-monthly@example.com"]["billing_frequency_display"] == "Monthly"
        assert rows["premium-monthly@example.com"]["billing_price_source"] == "plan_default"
        assert rows["premium-annual@example.com"]["billing_price_amount"] == 24950
        assert rows["premium-annual@example.com"]["billing_frequency_display"] == "Annual"
        assert rows["pro-monthly@example.com"]["billing_price_amount"] == 3995
        assert rows["pro-monthly@example.com"]["billing_price_source"] == "plan_default"
        assert rows["pro-annual@example.com"]["billing_price_amount"] == 39995
        assert rows["pro-annual@example.com"]["last_payment_amount_cents"] == 49995
        assert rows["pro-annual@example.com"]["billing_frequency_display"] == "Annual"
        assert rows["override@example.com"]["billing_price_amount"] == 1495
        assert rows["override@example.com"]["billing_price_source"] == "override"
        for row in rows.values():
            assert "payload_json" not in row
            assert "tax_breakdown_json" not in row
            assert "payment_method" not in row
    finally:
        db.close()


def test_admin_users_current_plan_price_does_not_use_prorated_last_invoice(monkeypatch):
    monkeypatch.setenv("ADMIN_EMAILS", "admin@example.com")
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        user = _user(db, "upgrade@example.com", tier="pro")
        user.subscription_status = "active"
        user.subscription_plan = "pro"
        user.subscription_interval = "monthly"
        user.stripe_customer_id = "cus_upgrade"
        user.stripe_subscription_id = "sub_upgrade"
        db.add_all(
            [
                BillingTransaction(
                    user_id=user.id,
                    stripe_customer_id="cus_upgrade",
                    stripe_subscription_id="sub_upgrade",
                    stripe_invoice_id="in_premium_initial",
                    billing_period_type="monthly",
                    total_amount=1995,
                    currency="USD",
                    charged_at=datetime(2026, 4, 1, tzinfo=timezone.utc),
                    payment_status="paid",
                    refund_status="none",
                ),
                BillingTransaction(
                    user_id=user.id,
                    stripe_customer_id="cus_upgrade",
                    stripe_subscription_id="sub_upgrade",
                    stripe_invoice_id="in_pro_proration",
                    billing_period_type="monthly",
                    total_amount=3000,
                    currency="USD",
                    charged_at=datetime(2026, 4, 15, tzinfo=timezone.utc),
                    payment_status="paid",
                    refund_status="none",
                ),
            ]
        )
        db.commit()

        response = admin_users(_request_for_user(admin), db, search="upgrade@example.com", page=1, page_size=25)
        row = response["items"][0]

        assert row["plan"] == "pro"
        assert row["current_plan_amount_cents"] == 3995
        assert row["current_plan_display"] == "USD $39.95 / month"
        assert row["total_paid_cents"] == 4995
        assert row["total_paid_display"] == "USD $49.95"
        assert row["last_payment_amount_cents"] == 3000
        assert row["last_payment_display"] == "USD $30.00"
    finally:
        db.close()


def test_page_analytics_strips_tokens_normalizes_and_aggregates(monkeypatch):
    monkeypatch.setenv("ADMIN_EMAILS", "admin@example.com")
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        reader = _user(db, "reader@example.com", tier="premium")
        auth_request = _request_for_user(reader)
        anon_request = Request(
            {
                "type": "http",
                "method": "POST",
                "path": "/api/analytics/page-view",
                "headers": [
                    (b"user-agent", b"Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) Mobile/15E148 Safari/604.1"),
                    (b"x-walnut-analytics-session", b"anon-session"),
                ],
            }
        )

        record_page_view(
            PageViewPayload(path="/account/verify-email?token=secret-token", referrer_path="/reset-password?token=secret", title="Verify"),
            anon_request,
            db,
        )
        record_page_view(PageViewPayload(path="/ticker/AAPL?utm_source=test", session_id="anon-session-2"), anon_request, db)
        record_page_view(PageViewPayload(path="/member/NANCY_PELOSI?token=secret"), auth_request, db)

        rows = db.execute(select(PageViewEvent).order_by(PageViewEvent.id.asc())).scalars().all()
        assert rows[0].path == "/account/verify-email"
        assert rows[0].normalized_path == "/account/verify-email"
        assert rows[0].referrer_path == "/reset-password"
        assert "secret" not in rows[0].path
        assert rows[1].normalized_path == "/ticker/[symbol]"
        assert rows[1].session_id_hash
        assert rows[2].normalized_path == "/member/[id]"
        assert rows[2].user_id == reader.id
        assert rows[2].plan_at_time == "premium"

        report = admin_page_analytics(_request_for_user(admin), db, period="30d", limit=10)
        pages = {row["page"]: row for row in report["top_pages"]}
        assert pages["/ticker/[symbol]"]["views"] == 1
        assert pages["/member/[id]"]["unique_users"] == 1
        assert pages["/member/[id]"]["paid_percent"] == 100.0
        assert report["trend_by_day"]
    finally:
        db.close()


def test_product_analytics_event_endpoint_accepts_market_pressure_events_without_persisting_page_views():
    db = _session()
    try:
        request = Request(
            {
                "type": "http",
                "method": "POST",
                "path": "/api/analytics/event",
                "headers": [(b"user-agent", b"Mozilla/5.0 Chrome/126.0")],
            }
        )

        response = record_product_event(
            ProductEventPayload(
                event_name="market_pressure_page_view",
                path="/market-pressure",
                properties={"universe": "sp500", "period": "1d", "rendered": True},
            ),
            request,
            db,
        )

        assert response.status_code == 204
        assert db.execute(select(func.count(PageViewEvent.id))).scalar_one() == 0
    finally:
        db.close()


def test_admin_users_exports_xlsx_and_pdf(monkeypatch):
    monkeypatch.setenv("ADMIN_EMAILS", "admin@example.com")
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        user = _user(db, "export-user@example.com", tier="premium")
        user.name = "Export User"
        user.country = "GB"
        user.state_province = ""
        user.last_seen_at = datetime(2026, 4, 18, tzinfo=timezone.utc)
        db.commit()

        request = _request_for_user(admin)
        xlsx = admin_users_export(
            "xlsx",
            request,
            db,
            plan="premium",
            status=None,
            country=None,
            admin="non_admin",
            sort_by="email",
            sort_dir="asc",
        )
        assert xlsx.media_type == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        with zipfile.ZipFile(BytesIO(xlsx.body)) as workbook:
            worksheet = workbook.read("xl/worksheets/sheet1.xml").decode()
        assert "user name" in worksheet
        assert "user id" in worksheet
        assert f"U-{user.id:06d}" in worksheet
        assert "price" in worksheet
        assert "billing" in worksheet
        assert "USD $19.95" in worksheet
        assert "Monthly" in worksheet
        assert "Export User" in worksheet
        assert "export-user@example.com" in worksheet

        pdf = admin_users_export(
            "pdf",
            request,
            db,
            plan="premium",
            status=None,
            country=None,
            admin="non_admin",
            sort_by="email",
            sort_dir="asc",
        )
        assert pdf.media_type == "application/pdf"
        assert pdf.body.startswith(b"%PDF-1.4")
        assert b"USD $19.95" in pdf.body
        assert b"Monthly" in pdf.body
        assert f"U-{user.id:06d}".encode() in pdf.body
        assert b"Export User" in pdf.body
    finally:
        db.close()


def test_admin_can_manage_stripe_tax_readiness_settings(monkeypatch):
    monkeypatch.setenv("ADMIN_EMAILS", "admin@example.com")
    monkeypatch.setenv("STRIPE_SECRET_KEY", "sk_test_hidden")
    monkeypatch.setenv("STRIPE_PRICE_ID", "price_123")
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        request = _request_for_user(admin)

        updated = admin_update_stripe_tax_settings(
            StripeTaxSettingsPayload(
                automatic_tax_enabled=True,
                require_billing_address=True,
                product_tax_code="txcd_10000000",
                price_tax_behavior="exclusive",
            ),
            request,
            db,
        )

        assert updated["configured"] is True
        assert updated["automatic_tax_enabled"] is True
        assert updated["require_billing_address"] is True
        assert updated["product_tax_code"] == "txcd_10000000"
        assert updated["price_tax_behavior"] == "exclusive"
        assert updated["secret_key"] == "configured"
        assert "sk_test_hidden" not in str(updated)

        settings = admin_settings(request, db)
        assert "tax_rules" not in settings
        assert settings["stripe_tax"]["automatic_tax_enabled"] is True
        assert settings["stripe_tax"]["price_id"] == "price_123"
    finally:
        db.close()


def test_stripe_tax_readiness_helper_prompts_for_missing_location():
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        admin_update_stripe_tax_settings(
            StripeTaxSettingsPayload(automatic_tax_enabled=True, require_billing_address=True),
            _request_for_user(admin),
            db,
        )

        missing = stripe_tax_billing_readiness(db)
        ready = stripe_tax_billing_readiness(
            db,
            {
                "country": "US",
                "state_province": "CA",
                "postal_code": "94105",
                "city": "San Francisco",
                "address_line1": "1 Market St",
            },
        )

        assert missing["should_prompt_for_location"] is True
        assert missing["can_start_checkout"] is False
        assert missing["missing_fields"] == ["country", "postal_code", "city", "address_line1"]
        assert ready["should_prompt_for_location"] is False
        assert ready["can_start_checkout"] is True
    finally:
        db.close()


def test_taxable_checkout_syncs_customer_location_and_enables_automatic_tax(monkeypatch):
    monkeypatch.setenv("STRIPE_SECRET_KEY", "sk_test_hidden")
    monkeypatch.setenv("STRIPE_PRICE_ID_MONTHLY", "price_monthly")
    db = _session()
    calls = []

    def fake_stripe_post(path, data):
        calls.append((path, dict(data)))
        if path == "customers":
            return {"id": "cus_tax_ready"}
        if path == "checkout/sessions":
            return {"id": "cs_test", "url": "https://checkout.stripe.test/session"}
        raise AssertionError(f"Unexpected Stripe path {path}")

    monkeypatch.setattr("app.routers.accounts._stripe_post", fake_stripe_post)
    try:
        admin = _user(db, "admin@example.com", role="admin")
        admin_update_stripe_tax_settings(
            StripeTaxSettingsPayload(automatic_tax_enabled=True, require_billing_address=True),
            _request_for_user(admin),
            db,
        )
        registered = register(_register_payload("tax-ready@example.com"), db)
        user = db.get(UserAccount, registered["user"]["id"])
        assert user is not None
        user.email_verified_at = datetime.now(timezone.utc)
        db.commit()

        response = create_checkout_session(_request_for_user(user), CheckoutSessionPayload(billing_interval="monthly"), db)

        assert response["url"] == "https://checkout.stripe.test/session"
        assert calls[0][0] == "customers"
        assert calls[0][1]["name"] == "Reader One"
        assert calls[0][1]["email"] == "tax-ready@example.com"
        assert calls[0][1]["address[country]"] == "US"
        assert calls[0][1]["address[state]"] == "CA"
        assert calls[0][1]["address[postal_code]"] == "94105"
        assert calls[0][1]["tax[validate_location]"] == "immediately"
        assert calls[1][0] == "checkout/sessions"
        assert calls[1][1]["customer"] == "cus_tax_ready"
        assert calls[1][1]["line_items[0][price]"] == "price_monthly"
        assert calls[1][1]["automatic_tax[enabled]"] == "true"
        assert calls[1][1]["billing_address_collection"] == "required"
        assert calls[1][1]["customer_update[address]"] == "auto"
        assert calls[1][1]["customer_update[name]"] == "auto"
    finally:
        db.close()


def test_customer_portal_session_uses_app_return_url(monkeypatch):
    monkeypatch.setenv("STRIPE_SECRET_KEY", "sk_test_hidden")
    monkeypatch.setenv("FRONTEND_BASE_URL", "https://www.walnut-intel.com")
    monkeypatch.setenv("APP_BASE_URL", "https://www.walnut-intel.com")
    monkeypatch.setenv("FRONTEND_APP_URL", "https://app.walnutmarkets.com")
    monkeypatch.delenv("STRIPE_CUSTOMER_PORTAL_RETURN_URL", raising=False)
    db = _session()
    calls = []

    def fake_stripe_post(path, data):
        calls.append((path, dict(data)))
        if path == "billing_portal/sessions":
            return {"url": "https://billing.stripe.test/session"}
        raise AssertionError(f"Unexpected Stripe path {path}")

    monkeypatch.setattr("app.routers.accounts._stripe_post", fake_stripe_post)
    try:
        user = _user(db, "portal-return@example.com")
        user.email_verified_at = datetime.now(timezone.utc)
        user.stripe_customer_id = "cus_portal_return"
        db.commit()

        response = create_customer_portal_session(_request_for_user(user), db)

        assert response["url"] == "https://billing.stripe.test/session"
        assert calls == [
            (
                "billing_portal/sessions",
                {
                    "customer": "cus_portal_return",
                    "return_url": "https://app.walnutmarkets.com/account/billing?portal_return=1",
                },
            )
        ]
        assert "www.walnut-intel.com/account/billing" not in json.dumps(calls)
    finally:
        db.close()


def test_customer_portal_session_prefers_explicit_return_url(monkeypatch):
    monkeypatch.setenv("STRIPE_SECRET_KEY", "sk_test_hidden")
    monkeypatch.setenv("FRONTEND_BASE_URL", "https://www.walnut-intel.com")
    monkeypatch.delenv("FRONTEND_APP_URL", raising=False)
    monkeypatch.delenv("APP_BASE_URL", raising=False)
    monkeypatch.setenv(
        "STRIPE_CUSTOMER_PORTAL_RETURN_URL",
        "https://app.walnutmarkets.com/account/billing?portal_return=1",
    )
    db = _session()
    calls = []

    def fake_stripe_post(path, data):
        calls.append((path, dict(data)))
        if path == "billing_portal/sessions":
            return {"url": "https://billing.stripe.test/session"}
        raise AssertionError(f"Unexpected Stripe path {path}")

    monkeypatch.setattr("app.routers.accounts._stripe_post", fake_stripe_post)
    try:
        user = _user(db, "explicit-portal-return@example.com")
        user.email_verified_at = datetime.now(timezone.utc)
        user.stripe_customer_id = "cus_explicit_portal_return"
        db.commit()

        create_customer_portal_session(_request_for_user(user), db)

        assert calls[0][1]["return_url"] == "https://app.walnutmarkets.com/account/billing?portal_return=1"
    finally:
        db.close()


def test_checkout_session_uses_app_success_and_cancel_urls(monkeypatch):
    monkeypatch.setenv("STRIPE_SECRET_KEY", "sk_test_hidden")
    monkeypatch.setenv("STRIPE_PRICE_ID_PREMIUM_MONTHLY", "price_premium_monthly")
    monkeypatch.setenv("FRONTEND_BASE_URL", "https://www.walnut-intel.com")
    monkeypatch.setenv("APP_BASE_URL", "https://www.walnut-intel.com")
    monkeypatch.delenv("FRONTEND_APP_URL", raising=False)
    db = _session()
    calls = []

    def fake_stripe_post(path, data):
        calls.append((path, dict(data)))
        if path.startswith("customers/"):
            return {"id": "cus_checkout_urls"}
        if path == "checkout/sessions":
            return {"id": "cs_checkout_urls", "url": "https://checkout.stripe.test/session"}
        raise AssertionError(f"Unexpected Stripe path {path}")

    monkeypatch.setattr("app.routers.accounts._stripe_post", fake_stripe_post)
    try:
        user = _user(db, "checkout-urls@example.com")
        user.email_verified_at = datetime.now(timezone.utc)
        user.stripe_customer_id = "cus_checkout_urls"
        db.commit()

        create_checkout_session(_request_for_user(user), CheckoutSessionPayload(plan="premium", interval="monthly"), db)

        checkout_data = calls[-1][1]
        assert checkout_data["success_url"] == "https://app.walnutmarkets.com/account/billing?checkout=success"
        assert checkout_data["cancel_url"] == "https://app.walnutmarkets.com/pricing?checkout=cancelled"
        assert "www.walnut-intel.com/account/billing" not in json.dumps(checkout_data)
    finally:
        db.close()


def test_unverified_user_cannot_create_checkout_session(monkeypatch):
    monkeypatch.setenv("STRIPE_SECRET_KEY", "sk_test_hidden")
    monkeypatch.setenv("STRIPE_PRICE_ID_PREMIUM_MONTHLY", "price_premium_monthly")
    monkeypatch.setattr("app.routers.accounts._stripe_post", lambda path, data: (_ for _ in ()).throw(AssertionError("Stripe should not be called")))
    db = _session()
    try:
        registered = register(_register_payload("checkout-unverified@example.com"), db)
        user = db.get(UserAccount, registered["user"]["id"])
        assert user is not None
        assert user.email_verified_at is None

        try:
            create_checkout_session(_request_for_user(user), CheckoutSessionPayload(plan="premium", interval="monthly"), db)
        except HTTPException as exc:
            assert exc.status_code == 403
            assert exc.detail["code"] == "email_verification_required"
        else:
            raise AssertionError("Expected unverified checkout to be blocked")
    finally:
        db.close()


def test_paid_user_cannot_create_second_checkout_subscription(monkeypatch):
    monkeypatch.setattr("app.routers.accounts._stripe_post", lambda path, data: (_ for _ in ()).throw(AssertionError("Stripe should not be called")))
    db = _session()
    try:
        user = _user(db, "checkout-paid@example.com", tier="premium")
        user.email_verified_at = datetime.now(timezone.utc)
        user.subscription_status = "past_due"
        user.subscription_plan = "premium"
        user.stripe_subscription_id = "sub_existing"
        db.commit()

        try:
            create_checkout_session(_request_for_user(user), CheckoutSessionPayload(plan="pro", interval="monthly"), db)
        except HTTPException as exc:
            assert exc.status_code == 409
            assert exc.detail["code"] == "active_subscription_exists"
            assert exc.detail["message"] == "You already have an active subscription. Use Manage billing to change plans."
            assert exc.detail["action"] == "manage_billing"
            assert exc.detail["redirect_path"] == "/account/billing"
        else:
            raise AssertionError("Expected paid checkout to be blocked")
    finally:
        db.close()


def test_free_user_with_stale_test_customer_can_create_live_checkout(monkeypatch):
    monkeypatch.setenv("STRIPE_SECRET_KEY", "sk_live_hidden")
    monkeypatch.setenv("STRIPE_PRICE_ID_PREMIUM_MONTHLY", "price_live_premium_monthly")
    db = _session()
    calls = []

    def fake_stripe_post(path, data):
        calls.append((path, dict(data)))
        if path == "customers/cus_test_stale":
            raise HTTPException(
                status_code=400,
                detail=(
                    "Stripe request failed: No such customer: 'cus_test_stale'; "
                    "a similar object exists in test mode, but a live mode key was used to make this request."
                ),
            )
        if path == "customers":
            return {"id": "cus_live_replacement"}
        if path == "checkout/sessions":
            return {"id": "cs_live_replacement", "url": "https://checkout.stripe.com/live"}
        raise AssertionError(f"Unexpected Stripe path {path}")

    monkeypatch.setattr("app.routers.accounts._stripe_post", fake_stripe_post)
    try:
        user = _user(db, "checkout-stale-free@example.com", tier="free")
        user.email_verified_at = datetime.now(timezone.utc)
        user.entitlement_tier = "free"
        user.subscription_plan = "premium"
        user.subscription_status = "active"
        user.stripe_customer_id = "cus_test_stale"
        user.stripe_subscription_id = "sub_test_stale"
        user.stripe_price_id = "price_test_stale"
        db.commit()

        response = create_checkout_session(_request_for_user(user), CheckoutSessionPayload(plan="premium", interval="monthly"), db)

        assert response["id"] == "cs_live_replacement"
        assert response["url"] == "https://checkout.stripe.com/live"
        assert [call[0] for call in calls] == ["customers/cus_test_stale", "customers", "checkout/sessions"]
        db.refresh(user)
        assert user.entitlement_tier == "free"
        assert user.subscription_plan == "free"
        assert user.subscription_status == "free"
        assert user.stripe_customer_id == "cus_live_replacement"
        assert user.stripe_subscription_id is None
        assert user.stripe_price_id is None
    finally:
        db.close()


def test_checkout_updates_existing_customer_email_before_session(monkeypatch):
    monkeypatch.setenv("STRIPE_SECRET_KEY", "sk_live_hidden")
    monkeypatch.setenv("STRIPE_PRICE_ID_PREMIUM_MONTHLY", "price_live_premium_monthly")
    db = _session()
    calls = []

    def fake_stripe_post(path, data):
        calls.append((path, dict(data)))
        if path == "customers/cus_missing_email":
            return {"id": "cus_missing_email"}
        if path == "checkout/sessions":
            return {"id": "cs_live_email", "url": "https://checkout.stripe.com/live-email"}
        raise AssertionError(f"Unexpected Stripe path {path}")

    monkeypatch.setattr("app.routers.accounts._stripe_post", fake_stripe_post)
    try:
        user = _user(db, "receipt-ready@example.com")
        user.email_verified_at = datetime.now(timezone.utc)
        user.stripe_customer_id = "cus_missing_email"
        db.commit()

        response = create_checkout_session(_request_for_user(user), CheckoutSessionPayload(plan="premium", interval="monthly"), db)

        assert response["url"] == "https://checkout.stripe.com/live-email"
        customer_call = calls[0]
        checkout_call = calls[1]
        assert customer_call[0] == "customers/cus_missing_email"
        assert customer_call[1]["email"] == "receipt-ready@example.com"
        assert customer_call[1]["metadata[user_id]"] == str(user.id)
        assert customer_call[1]["metadata[email]"] == "receipt-ready@example.com"
        assert checkout_call[0] == "checkout/sessions"
        assert checkout_call[1]["customer"] == "cus_missing_email"
        assert checkout_call[1]["metadata[email]"] == "receipt-ready@example.com"
        assert checkout_call[1]["subscription_data[metadata][email]"] == "receipt-ready@example.com"
    finally:
        db.close()


def test_checkout_session_maps_all_plan_intervals_to_price_ids(monkeypatch):
    monkeypatch.setenv("STRIPE_SECRET_KEY", "sk_test_hidden")
    monkeypatch.setenv("STRIPE_PRICE_ID_PREMIUM_MONTHLY", "price_premium_monthly")
    monkeypatch.setenv("STRIPE_PRICE_ID_PREMIUM_ANNUAL", "price_premium_annual")
    monkeypatch.setenv("STRIPE_PRICE_ID_PRO_MONTHLY", "price_pro_monthly")
    monkeypatch.setenv("STRIPE_PRICE_ID_PRO_ANNUAL", "price_pro_annual")
    db = _session()
    calls = []

    def fake_stripe_post(path, data):
        calls.append((path, dict(data)))
        if path.startswith("customers/"):
            return {"id": "cus_four_prices"}
        if path == "checkout/sessions":
            return {"id": f"cs_{data['metadata[tier]']}_{data['metadata[billing_interval]']}", "url": "https://checkout.stripe.test/session"}
        raise AssertionError(f"Unexpected Stripe path {path}")

    monkeypatch.setattr("app.routers.accounts._stripe_post", fake_stripe_post)
    try:
        user = _user(db, "four-prices@example.com")
        user.email_verified_at = datetime.now(timezone.utc)
        user.stripe_customer_id = "cus_four_prices"
        db.commit()

        expected = {
            ("premium", "monthly"): "price_premium_monthly",
            ("premium", "annual"): "price_premium_annual",
            ("pro", "monthly"): "price_pro_monthly",
            ("pro", "annual"): "price_pro_annual",
        }
        for (plan, interval), price_id in expected.items():
            response = create_checkout_session(_request_for_user(user), CheckoutSessionPayload(plan=plan, interval=interval), db)
            assert response["url"] == "https://checkout.stripe.test/session"
            checkout_call = calls[-1]
            assert checkout_call[0] == "checkout/sessions"
            assert checkout_call[1]["line_items[0][price]"] == price_id
            assert checkout_call[1]["client_reference_id"] == str(user.id)
            assert checkout_call[1]["metadata[user_id]"] == str(user.id)
            assert checkout_call[1]["metadata[email]"] == "four-prices@example.com"
            assert checkout_call[1]["metadata[plan]"] == plan
            assert checkout_call[1]["metadata[price_id]"] == price_id
            assert checkout_call[1]["subscription_data[metadata][user_id]"] == str(user.id)
            assert checkout_call[1]["subscription_data[metadata][email]"] == "four-prices@example.com"
            assert checkout_call[1]["subscription_data[metadata][plan]"] == plan
            assert checkout_call[1]["subscription_data[metadata][price_id]"] == price_id
            assert checkout_call[1]["metadata[tier]"] == plan
            assert checkout_call[1]["metadata[billing_interval]"] == interval
    finally:
        db.close()


def test_subscription_updated_maps_configured_price_ids_to_entitlements(monkeypatch):
    monkeypatch.setenv("CT_DEFAULT_TIER", "free")
    monkeypatch.setenv("STRIPE_PRICE_ID_PREMIUM_MONTHLY", "price_premium_monthly")
    monkeypatch.setenv("STRIPE_PRICE_ID_PREMIUM_ANNUAL", "price_premium_annual")
    monkeypatch.setenv("STRIPE_PRICE_ID_PRO_MONTHLY", "price_pro_monthly")
    monkeypatch.setenv("STRIPE_PRICE_ID_PRO_ANNUAL", "price_pro_annual")
    db = _session()
    try:
        cases = [
            ("premium-monthly@example.com", "price_premium_monthly", "premium", "monthly"),
            ("premium-annual@example.com", "price_premium_annual", "premium", "annual"),
            ("pro-monthly@example.com", "price_pro_monthly", "pro", "monthly"),
            ("pro-annual@example.com", "price_pro_annual", "pro", "annual"),
        ]
        for index, (email, price_id, tier, interval) in enumerate(cases, start=1):
            user = _user(db, email)
            user.stripe_customer_id = f"cus_price_{index}"
            db.commit()

            result = process_stripe_event(
                db,
                {
                    "id": f"evt_sub_price_{index}",
                    "type": "customer.subscription.updated",
                    "data": {
                        "object": {
                            "object": "subscription",
                            "id": f"sub_price_{index}",
                            "customer": f"cus_price_{index}",
                            "status": "active",
                            "current_period_end": 1_893_456_000,
                            "items": {
                                "data": [
                                    {
                                        "price": {
                                            "id": price_id,
                                            "recurring": {"interval": "year" if interval == "annual" else "month"},
                                        }
                                    }
                                ]
                            },
                        }
                    },
                },
            )
            db.refresh(user)

            assert result["status"] == "processed"
            assert user.entitlement_tier == tier
            assert user.subscription_plan == tier
            assert user.subscription_interval == interval
            assert user.stripe_price_id == price_id
            assert current_entitlements(_request_for_user(user), db).tier == tier
    finally:
        db.close()


def test_subscription_updated_maps_admin_free_price_ids_to_entitlements(monkeypatch):
    monkeypatch.setenv("CT_DEFAULT_TIER", "free")
    monkeypatch.setenv("STRIPE_PREMIUM_ADMIN_FREE_PRICE_ID", "price_admin_premium_free")
    monkeypatch.setenv("STRIPE_PRO_ADMIN_FREE_PRICE_ID", "price_admin_pro_free")
    db = _session()
    try:
        cases = [
            ("admin-premium-free@example.com", "price_admin_premium_free", "premium"),
            ("admin-pro-free@example.com", "price_admin_pro_free", "pro"),
        ]
        for index, (email, price_id, tier) in enumerate(cases, start=1):
            user = _user(db, email)
            user.password_hash = "hashed-password"
            user.stripe_customer_id = f"cus_admin_free_{index}"
            db.commit()

            result = process_stripe_event(
                db,
                {
                    "id": f"evt_admin_free_{index}",
                    "type": "customer.subscription.updated",
                    "data": {
                        "object": {
                            "object": "subscription",
                            "id": f"sub_admin_free_{index}",
                            "customer": f"cus_admin_free_{index}",
                            "status": "active",
                            "current_period_end": 1_893_456_000,
                            "items": {
                                "data": [
                                    {
                                        "price": {
                                            "id": price_id,
                                            "unit_amount": 0,
                                            "currency": "usd",
                                            "recurring": {"interval": "month"},
                                        }
                                    }
                                ]
                            },
                        }
                    },
                },
            )
            db.refresh(user)
            entitlements = current_entitlements(_request_for_user(user), db)
            me_payload = me(_request_for_user(user), db)

            assert result["status"] == "processed"
            assert result["mapped_plan"] == tier
            assert user.entitlement_tier == tier
            assert user.subscription_plan == tier
            assert user.stripe_price_id == price_id
            assert entitlements.tier == tier
            assert me_payload["entitlements"]["tier"] == tier
            assert me_payload["entitlements"]["source"] == "admin_subscription"
    finally:
        db.close()


def test_subscription_updated_prefers_paid_stripe_state_over_stale_manual_free_override(monkeypatch):
    monkeypatch.setenv("CT_DEFAULT_TIER", "free")
    monkeypatch.setenv("STRIPE_PRICE_ID_PREMIUM_MONTHLY", "price_premium_monthly")
    db = _session()
    try:
        user = _user(db, "stale-free-override@example.com")
        user.manual_tier_override = "free"
        user.entitlement_tier = "premium"
        user.subscription_plan = "premium"
        user.subscription_status = "active"
        user.stripe_customer_id = "cus_stale_free_override"
        db.commit()

        result = process_stripe_event(
            db,
            {
                "id": "evt_stale_free_override",
                "type": "customer.subscription.updated",
                "data": {
                    "object": {
                        "object": "subscription",
                        "id": "sub_stale_free_override",
                        "customer": "cus_stale_free_override",
                        "status": "active",
                        "current_period_end": 1_893_456_000,
                        "items": {
                            "data": [
                                {
                                    "price": {
                                        "id": "price_premium_monthly",
                                        "recurring": {"interval": "month"},
                                    }
                                }
                            ]
                        },
                    }
                },
            },
        )
        db.refresh(user)

        assert result["status"] == "processed"
        assert user.manual_tier_override == "free"
        assert user.entitlement_tier == "premium"
        assert user.subscription_plan == "premium"
        assert current_entitlements(_request_for_user(user), db).tier == "premium"
    finally:
        db.close()


def test_subscription_updated_portal_change_premium_to_pro_uses_current_item_price(monkeypatch):
    monkeypatch.setenv("CT_DEFAULT_TIER", "free")
    monkeypatch.setenv("STRIPE_PRICE_ID_PREMIUM_MONTHLY", "price_portal_premium")
    monkeypatch.setenv("STRIPE_PRICE_ID_PRO_MONTHLY", "price_portal_pro")
    db = _session()
    try:
        user = _user(db, "portal-pro@example.com", tier="premium")
        user.subscription_plan = "premium"
        user.subscription_status = "active"
        user.stripe_customer_id = "cus_portal_pro"
        user.stripe_subscription_id = "sub_portal_pro"
        user.stripe_price_id = "price_portal_premium"
        db.commit()

        result = process_stripe_event(
            db,
            {
                "id": "evt_portal_premium_to_pro",
                "type": "customer.subscription.updated",
                "data": {
                    "object": {
                        "object": "subscription",
                        "id": "sub_portal_pro",
                        "customer": "cus_portal_pro",
                        "status": "active",
                        "metadata": {"tier": "premium", "price_id": "price_portal_premium"},
                        "current_period_end": 1_893_456_000,
                        "items": {"data": [{"id": "si_pro", "price": {"id": "price_portal_pro", "recurring": {"interval": "month"}}}]},
                    }
                },
            },
        )
        db.refresh(user)

        assert result["status"] == "processed"
        assert result["mapped_plan"] == "pro"
        assert user.subscription_plan == "pro"
        assert user.entitlement_tier == "pro"
        assert user.stripe_price_id == "price_portal_pro"
        entitlements = current_entitlements(_request_for_user(user), db)
        assert entitlements.tier == "pro"
        assert entitlements.has_feature("options_flow_feed") is True
        assert entitlements.has_feature("institutional_feed") is True
    finally:
        db.close()


def test_subscription_updated_portal_change_pro_to_premium_uses_current_item_price(monkeypatch):
    monkeypatch.setenv("CT_DEFAULT_TIER", "free")
    monkeypatch.setenv("STRIPE_PRICE_ID_PREMIUM_MONTHLY", "price_downgrade_premium")
    monkeypatch.setenv("STRIPE_PRICE_ID_PRO_MONTHLY", "price_downgrade_pro")
    db = _session()
    try:
        user = _user(db, "portal-premium@example.com", tier="pro")
        user.subscription_plan = "pro"
        user.subscription_status = "active"
        user.stripe_customer_id = "cus_portal_premium"
        user.stripe_subscription_id = "sub_portal_premium"
        user.stripe_price_id = "price_downgrade_pro"
        db.commit()

        result = process_stripe_event(
            db,
            {
                "id": "evt_portal_pro_to_premium",
                "type": "customer.subscription.updated",
                "data": {
                    "object": {
                        "object": "subscription",
                        "id": "sub_portal_premium",
                        "customer": "cus_portal_premium",
                        "status": "active",
                        "metadata": {"tier": "pro", "price_id": "price_downgrade_pro"},
                        "current_period_end": 1_893_456_000,
                        "items": {"data": [{"id": "si_premium", "price": {"id": "price_downgrade_premium", "recurring": {"interval": "month"}}}]},
                    }
                },
            },
        )
        db.refresh(user)

        assert result["status"] == "processed"
        assert result["mapped_plan"] == "premium"
        assert user.subscription_plan == "premium"
        assert user.entitlement_tier == "premium"
        assert user.stripe_price_id == "price_downgrade_premium"
        entitlements = current_entitlements(_request_for_user(user), db)
        assert entitlements.tier == "premium"
        assert entitlements.has_feature("signals") is True
        assert entitlements.has_feature("options_flow_feed") is False
        assert entitlements.has_feature("institutional_feed") is False
    finally:
        db.close()


def test_subscription_updated_multiple_items_selects_highest_active_plan(monkeypatch):
    monkeypatch.setenv("CT_DEFAULT_TIER", "free")
    monkeypatch.setenv("STRIPE_PRICE_ID_PREMIUM_MONTHLY", "price_multi_premium")
    monkeypatch.setenv("STRIPE_PRICE_ID_PRO_MONTHLY", "price_multi_pro")
    monkeypatch.setenv("STRIPE_PRICE_ID_PRO_ANNUAL", "price_multi_deleted_pro")
    db = _session()
    try:
        user = _user(db, "multi-item@example.com", tier="premium")
        user.stripe_customer_id = "cus_multi_item"
        user.stripe_subscription_id = "sub_multi_item"
        db.commit()

        result = process_stripe_event(
            db,
            {
                "id": "evt_multi_item",
                "type": "customer.subscription.updated",
                "data": {
                    "object": {
                        "object": "subscription",
                        "id": "sub_multi_item",
                        "customer": "cus_multi_item",
                        "status": "active",
                        "items": {
                            "data": [
                                {"id": "si_deleted_pro", "deleted": True, "price": {"id": "price_multi_deleted_pro", "recurring": {"interval": "year"}}},
                                {"id": "si_premium", "price": {"id": "price_multi_premium", "recurring": {"interval": "month"}}},
                                {"id": "si_pro", "price": {"id": "price_multi_pro", "recurring": {"interval": "month"}}},
                            ]
                        },
                    }
                },
            },
        )
        db.refresh(user)

        assert result["mapped_plan"] == "pro"
        assert user.subscription_plan == "pro"
        assert user.stripe_price_id == "price_multi_pro"
    finally:
        db.close()


def test_subscription_created_and_invoice_payment_paid_upgrade_user(monkeypatch):
    monkeypatch.setenv("CT_DEFAULT_TIER", "free")
    monkeypatch.setenv("STRIPE_PRICE_ID_PREMIUM_MONTHLY", "price_created")
    db = _session()
    try:
        created_user = _user(db, "created@example.com")
        created_user.stripe_customer_id = "cus_created"
        invoice_user = _user(db, "invoice-alias@example.com")
        invoice_user.stripe_customer_id = "cus_invoice_alias"
        invoice_user.stripe_subscription_id = "sub_invoice_alias"
        db.commit()

        created = process_stripe_event(
            db,
            {
                "id": "evt_sub_created",
                "type": "customer.subscription.created",
                "data": {
                    "object": {
                        "object": "subscription",
                        "id": "sub_created",
                        "customer": "cus_created",
                        "status": "active",
                        "current_period_end": 1_893_456_000,
                        "items": {"data": [{"price": {"id": "price_created", "recurring": {"interval": "month"}}}]},
                    }
                },
            },
        )
        paid = process_stripe_event(
            db,
            {
                "id": "evt_invoice_payment_paid",
                "type": "invoice.payment.paid",
                "data": {
                    "object": {
                        "id": "in_alias",
                        "object": "invoice",
                        "customer": "cus_invoice_alias",
                        "subscription": "sub_invoice_alias",
                        "customer_email": "invoice-alias@example.com",
                        "status": "paid",
                        "lines": {
                            "data": [
                                {
                                    "period": {"start": 1_800_000_000, "end": 1_802_592_000},
                                    "price": {"id": "price_created", "recurring": {"interval": "month"}},
                                }
                            ]
                        },
                    }
                },
            },
        )
        db.refresh(created_user)
        db.refresh(invoice_user)

        assert created["status"] == "processed"
        assert created_user.entitlement_tier == "premium"
        assert created_user.stripe_subscription_id == "sub_created"
        assert created_user.stripe_price_id == "price_created"
        assert paid["status"] == "processed"
        assert invoice_user.entitlement_tier == "premium"
        assert invoice_user.stripe_price_id == "price_created"
    finally:
        db.close()


def test_webhook_prefers_active_user_over_deleted_stripe_ghost(monkeypatch):
    monkeypatch.setenv("CT_DEFAULT_TIER", "free")
    monkeypatch.setenv("STRIPE_PRICE_ID_PREMIUM_MONTHLY", "price_ghost")
    db = _session()
    try:
        deleted = _user(db, "deleted+1+ghost@example.com")
        deleted.original_email = "ghost@example.com"
        deleted.deleted_at = datetime.now(timezone.utc)
        deleted.is_suspended = True
        deleted.stripe_customer_id = "cus_ghost"
        deleted.stripe_subscription_id = "sub_ghost"
        active = _user(db, "ghost@example.com")
        db.commit()

        result = process_stripe_event(
            db,
            {
                "id": "evt_ghost_created",
                "type": "customer.subscription.created",
                "data": {
                    "object": {
                        "object": "subscription",
                        "id": "sub_ghost",
                        "customer": "cus_ghost",
                        "customer_email": "ghost@example.com",
                        "status": "active",
                        "current_period_end": 1_893_456_000,
                        "items": {"data": [{"price": {"id": "price_ghost", "recurring": {"interval": "month"}}}]},
                    }
                },
            },
        )
        db.refresh(active)
        db.refresh(deleted)

        assert result["status"] == "processed"
        assert active.entitlement_tier == "premium"
        assert active.stripe_customer_id == "cus_ghost"
        assert active.stripe_subscription_id == "sub_ghost"
        assert deleted.stripe_customer_id is None
        assert deleted.stripe_subscription_id is None
    finally:
        db.close()


def test_customer_deleted_webhook_clears_billing_without_recreating_user(monkeypatch):
    monkeypatch.setenv("CT_DEFAULT_TIER", "free")
    db = _session()
    try:
        user = _user(db, "customer-deleted@example.com", tier="premium")
        user.stripe_customer_id = "cus_deleted_event"
        user.stripe_subscription_id = "sub_deleted_event"
        user.stripe_price_id = "price_deleted_event"
        user.subscription_status = "active"
        user.subscription_plan = "premium"
        user.subscription_interval = "monthly"
        user.current_plan_amount_cents = 1995
        user.current_plan_currency = "USD"
        db.commit()

        result = process_stripe_event(
            db,
            {
                "id": "evt_customer_deleted",
                "type": "customer.deleted",
                "data": {"object": {"object": "customer", "id": "cus_deleted_event", "deleted": True}},
            },
        )
        db.refresh(user)
        duplicate = process_stripe_event(
            db,
            {
                "id": "evt_customer_deleted_duplicate_payload",
                "type": "customer.deleted",
                "data": {"object": {"object": "customer", "id": "cus_deleted_event", "deleted": True}},
            },
        )

        assert result["status"] == "processed"
        assert duplicate["status"] == "processed"
        assert user.stripe_customer_id is None
        assert user.stripe_subscription_id is None
        assert user.stripe_price_id is None
        assert user.subscription_status == "deleted"
        assert user.subscription_plan == "free"
        assert user.entitlement_tier == "free"
        assert db.execute(select(func.count()).select_from(UserAccount)).scalar_one() == 1
    finally:
        db.close()


def test_subscription_webhook_for_deleted_user_does_not_restore_paid_or_recreate(monkeypatch):
    monkeypatch.setenv("CT_DEFAULT_TIER", "free")
    monkeypatch.setenv("STRIPE_PRICE_ID_PREMIUM_MONTHLY", "price_deleted_webhook")
    db = _session()
    try:
        deleted = _user(db, "deleted+44+late@example.com", tier="premium")
        deleted.original_email = "late@example.com"
        deleted.deleted_at = datetime.now(timezone.utc)
        deleted.stripe_customer_id = "cus_late_deleted"
        deleted.stripe_subscription_id = "sub_late_deleted"
        db.commit()

        result = process_stripe_event(
            db,
            {
                "id": "evt_late_deleted_subscription",
                "type": "customer.subscription.updated",
                "data": {
                    "object": {
                        "object": "subscription",
                        "id": "sub_late_deleted",
                        "customer": "cus_late_deleted",
                        "customer_email": "late@example.com",
                        "status": "active",
                        "current_period_end": 1_893_456_000,
                        "items": {"data": [{"price": {"id": "price_deleted_webhook", "recurring": {"interval": "month"}}}]},
                    }
                },
            },
        )
        db.refresh(deleted)

        assert result["status"] == "processed"
        assert deleted.deleted_at is not None
        assert deleted.entitlement_tier == "free"
        assert deleted.subscription_plan == "free"
        assert deleted.subscription_status == "deleted"
        assert db.execute(select(func.count()).select_from(UserAccount)).scalar_one() == 1
    finally:
        db.close()


def test_stripe_webhook_does_not_create_user_from_customer_email(monkeypatch):
    monkeypatch.setenv("CT_DEFAULT_TIER", "free")
    monkeypatch.setenv("STRIPE_PRICE_ID_PREMIUM_MONTHLY", "price_no_create")
    db = _session()
    try:
        result = process_stripe_event(
            db,
            {
                "id": "evt_no_create_from_stripe",
                "type": "customer.subscription.updated",
                "data": {
                    "object": {
                        "object": "subscription",
                        "id": "sub_no_create",
                        "customer": "cus_no_create",
                        "customer_email": "stripe-only@example.com",
                        "status": "active",
                        "current_period_end": 1_893_456_000,
                        "items": {"data": [{"price": {"id": "price_no_create", "recurring": {"interval": "month"}}}]},
                    }
                },
            },
        )

        assert result["status"] == "processed"
        assert db.execute(select(func.count()).select_from(UserAccount)).scalar_one() == 0
    finally:
        db.close()


def test_unmapped_subscription_price_does_not_grant_paid_access(monkeypatch, caplog):
    monkeypatch.setenv("CT_DEFAULT_TIER", "free")
    monkeypatch.setenv("STRIPE_PRICE_ID_PREMIUM_MONTHLY", "price_known")
    db = _session()
    try:
        user = _user(db, "unknown-price@example.com")
        user.stripe_customer_id = "cus_unknown_price"
        db.commit()

        with caplog.at_level(logging.INFO):
            result = process_stripe_event(
                db,
                {
                    "id": "evt_unknown_price",
                    "type": "customer.subscription.updated",
                    "data": {
                        "object": {
                            "object": "subscription",
                            "id": "sub_unknown_price",
                            "customer": "cus_unknown_price",
                            "status": "active",
                            "items": {"data": [{"id": "si_unknown", "price": {"id": "price_not_configured", "recurring": {"interval": "month"}}}]},
                        }
                    },
                },
            )
        db.refresh(user)

        assert result["warning"] == "unmapped_price_id"
        assert user.stripe_price_id == "price_not_configured"
        assert user.subscription_plan == "free"
        assert user.entitlement_tier == "free"
        assert current_entitlements(_request_for_user(user), db).tier == "free"
        assert "price_not_configured" in caplog.text
        assert "item_count=1" in caplog.text
    finally:
        db.close()


def test_duplicate_subscription_updated_event_remains_idempotent(monkeypatch):
    monkeypatch.setenv("CT_DEFAULT_TIER", "free")
    monkeypatch.setenv("STRIPE_PRICE_ID_PREMIUM_MONTHLY", "price_idem_premium")
    monkeypatch.setenv("STRIPE_PRICE_ID_PRO_MONTHLY", "price_idem_pro")
    db = _session()
    try:
        user = _user(db, "idempotent-subscription@example.com")
        user.stripe_customer_id = "cus_idem"
        db.commit()

        event = {
            "id": "evt_subscription_idempotent",
            "type": "customer.subscription.updated",
            "data": {
                "object": {
                    "object": "subscription",
                    "id": "sub_idem",
                    "customer": "cus_idem",
                    "status": "active",
                    "items": {"data": [{"price": {"id": "price_idem_pro", "recurring": {"interval": "month"}}}]},
                }
            },
        }
        first = process_stripe_event(db, event)
        event["data"]["object"]["items"]["data"][0]["price"]["id"] = "price_idem_premium"
        second = process_stripe_event(db, event)
        db.refresh(user)

        assert first["status"] == "processed"
        assert second == {"status": "already_processed", "event_type": "customer.subscription.updated"}
        ledger = db.get(StripeWebhookEvent, "evt_subscription_idempotent")
        assert ledger is not None
        assert ledger.status == "processed"
        assert ledger.error_message is None
        assert user.subscription_plan == "pro"
        assert user.stripe_price_id == "price_idem_pro"
    finally:
        db.close()


def test_concurrent_duplicate_stripe_event_runs_side_effects_once(monkeypatch, tmp_path):
    monkeypatch.setenv("CT_DEFAULT_TIER", "free")
    monkeypatch.setenv("STRIPE_PRICE_ID_PREMIUM_MONTHLY", "price_race")
    db_path = tmp_path / "stripe-idempotency.db"
    engine = create_engine(
        f"sqlite:///{db_path}",
        connect_args={"check_same_thread": False, "timeout": 5},
    )
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    Base.metadata.create_all(engine)
    with Session() as db:
        seed_feature_gates(db)
        seed_plan_prices(db)
        user = _user(db, "race-stripe@example.com")
        user_id = user.id

    original_sync = accounts_module._sync_user_subscription
    side_effect_calls: list[str] = []
    first_side_effect_started = threading.Event()
    release_first_side_effect = threading.Event()

    def slow_sync(db, **kwargs):
        side_effect_calls.append(str(kwargs.get("status")))
        first_side_effect_started.set()
        release_first_side_effect.wait(timeout=2)
        return original_sync(db, **kwargs)

    monkeypatch.setattr(accounts_module, "_sync_user_subscription", slow_sync)
    event = {
        "id": "evt_concurrent_duplicate",
        "type": "customer.subscription.updated",
        "data": {
            "object": {
                "object": "subscription",
                "id": "sub_race",
                "customer": "cus_race",
                "customer_email": "race-stripe@example.com",
                "status": "active",
                "current_period_end": 1_893_456_000,
                "metadata": {"user_id": str(user_id), "email": "race-stripe@example.com"},
                "items": {"data": [{"price": {"id": "price_race", "recurring": {"interval": "month"}}}]},
            }
        },
    }
    results: list[dict] = []
    errors: list[BaseException] = []

    def deliver():
        db = Session()
        try:
            results.append(process_stripe_event(db, event))
        except BaseException as exc:  # pragma: no cover - failure path asserted below
            errors.append(exc)
        finally:
            db.close()

    first = threading.Thread(target=deliver)
    second = threading.Thread(target=deliver)
    first.start()
    assert first_side_effect_started.wait(timeout=2)
    second.start()
    time.sleep(0.2)
    release_first_side_effect.set()
    first.join(timeout=5)
    second.join(timeout=5)

    assert not first.is_alive()
    assert not second.is_alive()
    assert not errors
    assert sorted(result["status"] for result in results) == ["already_processed", "processed"]
    assert side_effect_calls == ["active"]
    with Session() as db:
        user = db.get(UserAccount, user_id)
        ledger = db.get(StripeWebhookEvent, "evt_concurrent_duplicate")
        assert user is not None
        assert user.stripe_customer_id == "cus_race"
        assert user.entitlement_tier == "premium"
        assert ledger is not None
        assert ledger.status == "processed"
        assert ledger.error_message is None


def test_failed_stripe_event_records_safe_failed_status_and_can_retry(monkeypatch):
    monkeypatch.setenv("CT_DEFAULT_TIER", "free")
    monkeypatch.setenv("STRIPE_PRICE_ID_PREMIUM_MONTHLY", "price_retry")
    db = _session()
    original_sync = accounts_module._sync_user_subscription

    def failing_sync(*_args, **_kwargs):
        raise RuntimeError("sk_live_secret_should_not_be_recorded")

    try:
        user = _user(db, "retry-stripe@example.com")
        event = {
            "id": "evt_retry_after_failure",
            "type": "customer.subscription.updated",
            "data": {
                "object": {
                    "object": "subscription",
                    "id": "sub_retry",
                    "customer": "cus_retry",
                    "customer_email": user.email,
                    "status": "active",
                    "current_period_end": 1_893_456_000,
                    "metadata": {"user_id": str(user.id), "email": user.email},
                    "items": {"data": [{"price": {"id": "price_retry", "recurring": {"interval": "month"}}}]},
                }
            },
        }

        monkeypatch.setattr(accounts_module, "_sync_user_subscription", failing_sync)
        with pytest.raises(RuntimeError):
            process_stripe_event(db, event)

        failed = db.get(StripeWebhookEvent, "evt_retry_after_failure")
        assert failed is not None
        assert failed.status == "failed"
        assert failed.error_message == "RuntimeError"
        assert "sk_live" not in (failed.error_message or "")
        db.refresh(user)
        assert user.entitlement_tier == "free"
        db.expunge(failed)

        monkeypatch.setattr(accounts_module, "_sync_user_subscription", original_sync)
        retried = process_stripe_event(db, event)
        db.refresh(user)
        processed = db.get(StripeWebhookEvent, "evt_retry_after_failure")

        assert retried["status"] == "processed"
        assert processed is not None
        assert processed.status == "processed"
        assert processed.error_message is None
        assert user.entitlement_tier == "premium"
        assert user.stripe_customer_id == "cus_retry"
    finally:
        db.close()


def test_unknown_stripe_event_is_recorded_and_ignored_safely():
    db = _session()
    try:
        result = process_stripe_event(
            db,
            {
                "id": "evt_unknown_ignored",
                "type": "customer.created",
                "data": {"object": {"object": "customer", "id": "cus_unknown_ignored"}},
            },
        )
        ledger = db.get(StripeWebhookEvent, "evt_unknown_ignored")

        assert result == {"status": "ignored", "event_type": "customer.created"}
        assert ledger is not None
        assert ledger.status == "processed"
        assert ledger.error_message is None
    finally:
        db.close()


def test_admin_subscription_debug_is_admin_only_and_secret_safe(monkeypatch):
    monkeypatch.setenv("ADMIN_EMAILS", "admin@example.com")
    monkeypatch.setenv("STRIPE_PRICE_ID_PREMIUM_MONTHLY", "price_debug")
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        reader = _user(db, "debug-reader@example.com", tier="premium")
        reader.stripe_customer_id = "cus_debug"
        reader.stripe_subscription_id = "sub_debug"
        reader.stripe_price_id = "price_debug"
        reader.subscription_status = "active"
        reader.subscription_plan = "premium"
        reader.subscription_interval = "monthly"
        db.commit()

        debug = admin_subscription_debug(_request_for_user(admin), reader.email, db)
        forbidden = {"STRIPE_SECRET_KEY", "STRIPE_WEBHOOK_SECRET", "secret_key", "webhook_secret"}

        assert debug["derived_entitlement"] == "premium"
        assert debug["price_mapping"]["matched"] is True
        assert debug["stripe_customer_id"] == "cus_debug"
        assert forbidden.isdisjoint(set(json.dumps(debug).split('"')))

        try:
            admin_subscription_debug(_request_for_user(reader), reader.email, db)
        except HTTPException as exc:
            assert exc.status_code == 403
        else:
            raise AssertionError("Expected non-admin debug access to be rejected.")
    finally:
        db.close()


def test_admin_subscription_debug_reports_stripe_current_item_mismatch(monkeypatch):
    monkeypatch.setenv("ADMIN_EMAILS", "admin@example.com")
    monkeypatch.setenv("STRIPE_SECRET_KEY", "sk_test_hidden")
    monkeypatch.setenv("STRIPE_PRICE_ID_PREMIUM_MONTHLY", "price_debug_premium")
    monkeypatch.setenv("STRIPE_PRICE_ID_PRO_MONTHLY", "price_debug_pro")

    def fake_stripe_get(path, params=None):
        if path == "subscriptions/sub_debug_mismatch":
            return {
                "object": "subscription",
                "id": "sub_debug_mismatch",
                "customer": "cus_debug_mismatch",
                "status": "active",
                "items": {"data": [{"id": "si_debug_pro", "price": {"id": "price_debug_pro", "recurring": {"interval": "month"}}}]},
            }
        raise AssertionError(f"Unexpected Stripe GET path {path}")

    monkeypatch.setattr("app.routers.accounts._stripe_get", fake_stripe_get)
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        reader = _user(db, "debug-mismatch@example.com", tier="premium")
        reader.stripe_customer_id = "cus_debug_mismatch"
        reader.stripe_subscription_id = "sub_debug_mismatch"
        reader.stripe_price_id = "price_debug_premium"
        reader.subscription_status = "active"
        reader.subscription_plan = "premium"
        db.commit()

        debug = admin_subscription_debug(_request_for_user(admin), reader.email, db)

        assert debug["local_plan"] == "premium"
        assert debug["local_stripe_price_id"] == "price_debug_premium"
        assert debug["stripe_lookup"]["subscription_status_found"] == "active"
        assert debug["stripe_lookup"]["current_item_price_id"] == "price_debug_pro"
        assert debug["stripe_lookup"]["mapped_stripe_plan"] == "pro"
        assert debug["stripe_lookup"]["mismatch"] is True
    finally:
        db.close()


def test_admin_sync_stripe_subscription_repairs_missed_webhook(monkeypatch):
    monkeypatch.setenv("ADMIN_EMAILS", "admin@example.com")
    monkeypatch.setenv("STRIPE_PRICE_ID_PREMIUM_MONTHLY", "price_sync")
    db = _session()
    calls = []

    def fake_stripe_get(path, params=None):
        calls.append((path, dict(params or {})))
        if path == "customers":
            return {"data": [{"id": "cus_sync"}]}
        if path == "subscriptions":
            return {
                "data": [
                    {
                        "object": "subscription",
                        "id": "sub_sync",
                        "customer": "cus_sync",
                        "status": "active",
                        "current_period_end": 1_893_456_000,
                        "cancel_at_period_end": False,
                        "items": {"data": [{"price": {"id": "price_sync", "recurring": {"interval": "month"}}}]},
                    }
                ]
            }
        raise AssertionError(f"Unexpected Stripe GET path {path}")

    monkeypatch.setattr("app.routers.accounts._stripe_get", fake_stripe_get)
    try:
        admin = _user(db, "admin@example.com", role="admin")
        reader = _user(db, "sync-reader@example.com")

        synced = admin_sync_stripe_subscription(AdminSubscriptionSyncPayload(email=reader.email), _request_for_user(admin), db)
        db.refresh(reader)

        assert synced["status"] == "synced"
        assert reader.stripe_customer_id == "cus_sync"
        assert reader.stripe_subscription_id == "sub_sync"
        assert reader.stripe_price_id == "price_sync"
        assert reader.subscription_interval == "monthly"
        assert current_entitlements(_request_for_user(reader), db).tier == "premium"
        assert calls[0][0] == "customers"
        assert calls[1][0] == "subscriptions"
    finally:
        db.close()


def test_user_refresh_subscription_repairs_missed_webhook(monkeypatch):
    monkeypatch.setenv("CT_DEFAULT_TIER", "free")
    monkeypatch.setenv("STRIPE_SECRET_KEY", "sk_test_hidden")
    monkeypatch.setenv("STRIPE_PRICE_ID_PREMIUM_MONTHLY", "price_refresh")
    db = _session()
    calls = []

    def fake_stripe_get(path, params=None):
        calls.append((path, dict(params or {})))
        if path == "customers":
            return {"data": [{"id": "cus_refresh"}]}
        if path == "subscriptions":
            return {
                "data": [
                    {
                        "object": "subscription",
                        "id": "sub_refresh",
                        "customer": "cus_refresh",
                        "status": "active",
                        "current_period_end": 1_893_456_000,
                        "cancel_at_period_end": False,
                        "items": {"data": [{"price": {"id": "price_refresh", "recurring": {"interval": "month"}}}]},
                    }
                ]
            }
        raise AssertionError(f"Unexpected Stripe GET path {path}")

    monkeypatch.setattr("app.routers.accounts._stripe_get", fake_stripe_get)
    try:
        user = _user(db, "refresh-reader@example.com")

        refreshed = refresh_subscription_from_stripe(_request_for_user(user), db)
        db.refresh(user)

        assert refreshed["status"] == "refreshed"
        assert "sync" not in refreshed
        assert refreshed["message"] == "Subscription refreshed."
        assert refreshed["user"]["subscription_plan"] == "premium"
        assert refreshed["user"]["subscription_status"] == "active"
        assert refreshed["user"]["entitlement_tier"] == "premium"
        assert refreshed["user"]["current_plan"] == "premium"
        assert "stripe_price_id" not in refreshed["user"]
        assert "subscription_item_resolution" not in refreshed
        assert user.stripe_customer_id == "cus_refresh"
        assert user.stripe_subscription_id == "sub_refresh"
        assert user.stripe_price_id == "price_refresh"
        assert user.subscription_status == "active"
        assert current_entitlements(_request_for_user(user), db).tier == "premium"
        assert calls[0][0] == "customers"
        assert calls[1][0] == "subscriptions"
    finally:
        db.close()


def test_user_refresh_subscription_repairs_portal_change_to_pro(monkeypatch):
    monkeypatch.setenv("CT_DEFAULT_TIER", "free")
    monkeypatch.setenv("STRIPE_SECRET_KEY", "sk_test_hidden")
    monkeypatch.setenv("STRIPE_PRICE_ID_PREMIUM_MONTHLY", "price_refresh_premium")
    monkeypatch.setenv("STRIPE_PRICE_ID_PRO_MONTHLY", "price_refresh_pro")
    db = _session()

    def fake_stripe_get(path, params=None):
        if path == "subscriptions/sub_refresh_portal":
            return {
                "object": "subscription",
                "id": "sub_refresh_portal",
                "customer": "cus_refresh_portal",
                "status": "active",
                "current_period_end": 1_893_456_000,
                "cancel_at_period_end": False,
                "metadata": {"tier": "premium", "price_id": "price_refresh_premium"},
                "items": {"data": [{"id": "si_refresh_pro", "price": {"id": "price_refresh_pro", "recurring": {"interval": "month"}}}]},
            }
        raise AssertionError(f"Unexpected Stripe GET path {path}")

    monkeypatch.setattr("app.routers.accounts._stripe_get", fake_stripe_get)
    try:
        user = _user(db, "refresh-portal-pro@example.com", tier="premium")
        user.subscription_plan = "premium"
        user.subscription_status = "active"
        user.stripe_customer_id = "cus_refresh_portal"
        user.stripe_subscription_id = "sub_refresh_portal"
        user.stripe_price_id = "price_refresh_premium"
        db.commit()

        refreshed = refresh_subscription_from_stripe(_request_for_user(user), db)
        db.refresh(user)

        assert refreshed["status"] == "refreshed"
        assert "sync" not in refreshed
        assert refreshed["user"]["subscription_plan"] == "pro"
        assert refreshed["user"]["entitlement_tier"] == "pro"
        assert refreshed["user"]["current_plan"] == "pro"
        assert "stripe_price_id" not in refreshed["user"]
        assert "subscription_item_resolution" not in refreshed
        assert user.subscription_plan == "pro"
        assert user.entitlement_tier == "pro"
        assert user.stripe_price_id == "price_refresh_pro"
        assert current_entitlements(_request_for_user(user), db).tier == "pro"
    finally:
        db.close()


def test_taxable_checkout_requires_billing_location_before_stripe_call(monkeypatch):
    monkeypatch.setenv("STRIPE_SECRET_KEY", "sk_test_hidden")
    monkeypatch.setenv("STRIPE_PRICE_ID", "price_default")
    db = _session()
    monkeypatch.setattr("app.routers.accounts._stripe_post", lambda path, data: (_ for _ in ()).throw(AssertionError("Stripe should not be called")))
    try:
        admin = _user(db, "admin@example.com", role="admin")
        admin_update_stripe_tax_settings(
            StripeTaxSettingsPayload(automatic_tax_enabled=True, require_billing_address=True),
            _request_for_user(admin),
            db,
        )
        user = _user(db, "missing-location@example.com")
        user.email_verified_at = datetime.now(timezone.utc)
        db.commit()

        try:
            create_checkout_session(_request_for_user(user), CheckoutSessionPayload(), db)
        except HTTPException as exc:
            assert exc.status_code == 422
            assert exc.detail["code"] == "billing_location_required"
            assert "country" in exc.detail["missing_fields"]
            assert "postal_code" in exc.detail["missing_fields"]
        else:
            raise AssertionError("Expected missing billing location to block checkout")
    finally:
        db.close()


def test_invoice_paid_persists_stripe_derived_billing_snapshot(monkeypatch):
    monkeypatch.setenv("CT_DEFAULT_TIER", "free")
    db = _session()
    try:
        user = _user(db, "invoice-paid@example.com")
        user.stripe_customer_id = "cus_invoice"
        user.stripe_subscription_id = "sub_invoice"
        db.commit()

        result = process_stripe_event(
            db,
            {
                "id": "evt_invoice_paid",
                "type": "invoice.paid",
                "data": {
                    "object": {
                        "id": "in_123",
                        "object": "invoice",
                        "customer": "cus_invoice",
                        "subscription": "sub_invoice",
                        "customer_name": "Invoice Reader",
                        "customer_email": "invoice-paid@example.com",
                        "customer_address": {"country": "US", "state": "CA", "postal_code": "94105"},
                        "description": "Premium monthly subscription",
                        "subtotal": 2000,
                        "total_tax_amounts": [{"amount": 175, "tax_rate": "txr_123"}],
                        "total": 2175,
                        "currency": "usd",
                        "status": "paid",
                        "number": "INV-2026-0001",
                        "hosted_invoice_url": "https://invoice.stripe.com/i/acct_test/in_123",
                        "invoice_pdf": "https://pay.stripe.com/invoice/acct_test/in_123/pdf",
                        "payment_intent": "pi_123",
                        "charge": {
                            "id": "ch_123",
                            "receipt_url": "https://pay.stripe.com/receipts/acct_test/ch_123",
                        },
                        "created": 1_800_000_000,
                        "status_transitions": {"paid_at": 1_800_000_100},
                        "lines": {
                            "data": [
                                {
                                    "description": "Premium monthly subscription",
                                    "period": {"start": 1_800_000_000, "end": 1_802_592_000},
                                    "price": {"recurring": {"interval": "month"}},
                                    "tax_amounts": [{"amount": 175, "tax_rate": "txr_123"}],
                                }
                            ]
                        },
                    }
                },
            },
        )

        db.refresh(user)
        snapshot = db.execute(
            select(BillingTransaction).where(BillingTransaction.stripe_invoice_id == "in_123")
        ).scalar_one()
        assert result["status"] == "processed"
        assert user.entitlement_tier == "premium"
        assert user.access_expires_at.replace(tzinfo=timezone.utc) == datetime.fromtimestamp(1_802_592_000, tz=timezone.utc)
        assert snapshot.user_id == user.id
        assert snapshot.stripe_customer_id == "cus_invoice"
        assert snapshot.stripe_subscription_id == "sub_invoice"
        assert snapshot.stripe_payment_intent_id == "pi_123"
        assert snapshot.stripe_charge_id == "ch_123"
        assert snapshot.customer_name == "Invoice Reader"
        assert snapshot.customer_email == "invoice-paid@example.com"
        assert snapshot.billing_country == "US"
        assert snapshot.billing_state_province == "CA"
        assert snapshot.billing_postal_code == "94105"
        assert snapshot.billing_period_type == "monthly"
        assert snapshot.subtotal_amount == 2000
        assert snapshot.tax_amount == 175
        assert snapshot.total_amount == 2175
        assert snapshot.currency == "USD"
        assert snapshot.payment_status == "paid"
        assert snapshot.refund_status == "none"
        assert "total_tax_amounts" in (snapshot.tax_breakdown_json or "")

        db.add(
            BillingTransaction(
                user_id=user.id,
                stripe_invoice_id="in_without_docs",
                description="Legacy premium subscription",
                total_amount=2000,
                currency="USD",
                charged_at=datetime(2027, 1, 20, tzinfo=timezone.utc),
                payment_status="paid",
                refund_status="none",
                payload_json='{"hosted_invoice_url":"https://example.com/not-stripe"}',
            )
        )
        db.commit()

        history = account_billing_history(_request_for_user(user), db, limit=10)
        documented = next(item for item in history["items"] if item["description"] == "Premium monthly subscription")
        fallback = next(item for item in history["items"] if item["description"] == "Legacy premium subscription")
        assert "stripe_invoice_id" not in documented
        assert "stripe_payment_intent_id" not in documented
        assert "stripe_charge_id" not in documented
        assert not documented["transaction_id"].startswith("in_")
        assert documented["documents"]["invoice_number"] == "INV-2026-0001"
        assert documented["documents"]["hosted_invoice_url"] == "https://invoice.stripe.com/i/acct_test/in_123"
        assert documented["documents"]["invoice_pdf"] == "https://pay.stripe.com/invoice/acct_test/in_123/pdf"
        assert documented["documents"]["invoice_pdf_url"] == "https://pay.stripe.com/invoice/acct_test/in_123/pdf"
        assert documented["documents"]["receipt_url"] == "https://pay.stripe.com/receipts/acct_test/ch_123"
        assert documented["documents"]["has_stripe_document"] is True
        assert fallback["documents"]["invoice_number"] is None
        assert fallback["documents"]["has_stripe_document"] is False
        assert fallback["documents"]["hosted_invoice_url"] is None
    finally:
        db.close()


def test_invoice_paid_fetches_expanded_invoice_for_receipt_links(monkeypatch):
    monkeypatch.setenv("CT_DEFAULT_TIER", "free")
    monkeypatch.setenv("STRIPE_SECRET_KEY", "sk_live_hidden")
    monkeypatch.setenv("STRIPE_PRICE_ID_PREMIUM_MONTHLY", "price_live_premium_monthly")
    db = _session()
    get_calls = []

    def fake_stripe_get(path, params=None):
        get_calls.append((path, params or {}))
        if path == "invoices/in_fetch_docs":
            return {
                "id": "in_fetch_docs",
                "object": "invoice",
                "customer": "cus_fetch_docs",
                "subscription": "sub_fetch_docs",
                "customer_email": "invoice-docs@example.com",
                "description": "Premium monthly subscription",
                "subtotal": 2000,
                "total": 2000,
                "currency": "usd",
                "status": "paid",
                "number": "INV-LIVE-0001",
                "hosted_invoice_url": "https://invoice.stripe.com/i/acct_live/in_fetch_docs",
                "invoice_pdf": "https://pay.stripe.com/invoice/acct_live/in_fetch_docs/pdf",
                "payment_intent": {
                    "id": "pi_fetch_docs",
                    "latest_charge": {
                        "id": "ch_fetch_docs",
                        "receipt_url": "https://pay.stripe.com/receipts/acct_live/ch_fetch_docs",
                    },
                },
                "charge": {
                    "id": "ch_fetch_docs",
                    "receipt_url": "https://pay.stripe.com/receipts/acct_live/ch_fetch_docs",
                },
                "created": 1_800_000_000,
                "status_transitions": {"paid_at": 1_800_000_100},
                "lines": {
                    "data": [
                        {
                            "description": "Premium monthly subscription",
                            "period": {"start": 1_800_000_000, "end": 1_802_592_000},
                            "price": {
                                "id": "price_live_premium_monthly",
                                "recurring": {"interval": "month"},
                            },
                        }
                    ]
                },
            }
        if path == "subscriptions/sub_fetch_docs":
            return {
                "id": "sub_fetch_docs",
                "object": "subscription",
                "customer": "cus_fetch_docs",
                "status": "active",
                "current_period_end": 1_802_592_000,
                "items": {"data": [{"price": {"id": "price_live_premium_monthly", "recurring": {"interval": "month"}}}]},
            }
        raise AssertionError(f"Unexpected Stripe path {path}")

    monkeypatch.setattr("app.routers.accounts._stripe_get", fake_stripe_get)
    try:
        user = _user(db, "invoice-docs@example.com")
        user.stripe_customer_id = "cus_fetch_docs"
        user.stripe_subscription_id = "sub_fetch_docs"
        db.commit()

        result = process_stripe_event(
            db,
            {
                "id": "evt_invoice_fetch_docs",
                "type": "invoice.paid",
                "data": {
                    "object": {
                        "id": "in_fetch_docs",
                        "object": "invoice",
                        "customer": "cus_fetch_docs",
                        "subscription": "sub_fetch_docs",
                        "customer_email": "invoice-docs@example.com",
                        "status": "paid",
                        "payment_intent": "pi_fetch_docs",
                        "charge": "ch_fetch_docs",
                        "lines": {
                            "data": [
                                {
                                    "price": {
                                        "id": "price_live_premium_monthly",
                                        "recurring": {"interval": "month"},
                                    }
                                }
                            ]
                        },
                    }
                },
            },
        )

        assert result["status"] == "processed"
        assert [call[0] for call in get_calls] == ["invoices/in_fetch_docs", "subscriptions/sub_fetch_docs"]
        assert set(get_calls[0][1]["expand[]"]) == {
            "charge",
            "payment_intent",
            "payment_intent.latest_charge",
        }
        history = account_billing_history(_request_for_user(user), db, limit=10)
        documented = history["items"][0]
        assert documented["documents"]["invoice_number"] == "INV-LIVE-0001"
        assert documented["documents"]["hosted_invoice_url"] == "https://invoice.stripe.com/i/acct_live/in_fetch_docs"
        assert documented["documents"]["invoice_pdf"] == "https://pay.stripe.com/invoice/acct_live/in_fetch_docs/pdf"
        assert documented["documents"]["invoice_pdf_url"] == "https://pay.stripe.com/invoice/acct_live/in_fetch_docs/pdf"
        assert documented["documents"]["receipt_url"] == "https://pay.stripe.com/receipts/acct_live/ch_fetch_docs"
        assert documented["documents"]["has_stripe_document"] is True
    finally:
        db.close()


def test_admin_sales_ledger_filters_sorts_and_paginates(monkeypatch):
    monkeypatch.setenv("ADMIN_EMAILS", "admin@example.com")
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        db.add_all(
            [
                BillingTransaction(
                    stripe_invoice_id="in_us_1",
                    customer_name="Zeta Customer",
                    customer_email="zeta@example.com",
                    billing_country="US",
                    billing_state_province="CA",
                    description="Premium monthly subscription",
                    subtotal_amount=2000,
                    tax_amount=175,
                    total_amount=2175,
                    currency="USD",
                    charged_at=datetime(2026, 4, 10, tzinfo=timezone.utc),
                    payment_status="paid",
                    refund_status="none",
                    tax_breakdown_json='{"total_tax_amounts":[{"amount":175,"tax_rate":{"display_name":"CA VAT"}}]}',
                ),
                BillingTransaction(
                    stripe_invoice_id="in_ca_1",
                    customer_name="Alpha Customer",
                    customer_email="alpha@example.com",
                    billing_country="CA",
                    billing_state_province="ON",
                    description="Premium annual subscription",
                    subtotal_amount=10000,
                    tax_amount=1300,
                    total_amount=11300,
                    currency="USD",
                    charged_at=datetime(2026, 4, 11, tzinfo=timezone.utc),
                    payment_status="paid",
                    refund_status="partially_refunded",
                ),
                BillingTransaction(
                    stripe_invoice_id="in_us_2",
                    customer_name="Beta Customer",
                    customer_email="beta@example.com",
                    billing_country="US",
                    billing_state_province="NY",
                    description="Premium monthly subscription",
                    subtotal_amount=2000,
                    tax_amount=180,
                    total_amount=2180,
                    currency="USD",
                    charged_at=datetime(2026, 5, 2, tzinfo=timezone.utc),
                    payment_status="paid",
                    refund_status="none",
                ),
            ]
        )
        db.commit()

        response = admin_sales_ledger(
            _request_for_user(admin),
            db,
            period="custom",
            start_date="2026-04-01",
            end_date="2026-04-30",
            country="US",
            sort_by="customer_name",
            sort_dir="asc",
            page=1,
            page_size=1,
        )

        assert response["total"] == 1
        assert response["total_pages"] == 1
        assert response["items"][0]["transaction_id"] == "in_us_1"
        assert response["items"][0]["vat1_label"] == "CA VAT"
        assert response["items"][0]["vat1_collected"] == 175
        assert response["items"][0]["status_refund_state"] == "paid"
    finally:
        db.close()


def test_admin_sales_ledger_all_dates_has_no_date_bounds(monkeypatch):
    monkeypatch.setenv("ADMIN_EMAILS", "admin@example.com")
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        db.add_all(
            [
                BillingTransaction(
                    stripe_invoice_id="in_old",
                    customer_name="Old Customer",
                    customer_email="old@example.com",
                    billing_country="US",
                    description="Legacy premium subscription",
                    total_amount=1000,
                    currency="USD",
                    charged_at=datetime(2024, 1, 10, tzinfo=timezone.utc),
                    payment_status="paid",
                    refund_status="none",
                ),
                BillingTransaction(
                    stripe_invoice_id="in_new",
                    customer_name="New Customer",
                    customer_email="new@example.com",
                    billing_country="US",
                    description="Current premium subscription",
                    total_amount=2000,
                    currency="USD",
                    charged_at=datetime(2026, 4, 10, tzinfo=timezone.utc),
                    payment_status="paid",
                    refund_status="none",
                ),
            ]
        )
        db.commit()

        response = admin_sales_ledger(
            _request_for_user(admin),
            db,
            period="all_dates",
            sort_by="date_charged",
            sort_dir="asc",
            page=1,
            page_size=25,
        )

        assert response["total"] == 2
        assert response["filters"]["start_date"] is None
        assert response["filters"]["end_date"] is None
    finally:
        db.close()


def test_admin_sales_ledger_exports_xlsx_and_pdf(monkeypatch):
    monkeypatch.setenv("ADMIN_EMAILS", "admin@example.com")
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        db.add(
            BillingTransaction(
                stripe_invoice_id="in_export",
                customer_name="Export Customer",
                customer_email="export@example.com",
                billing_country="GB",
                billing_state_province="",
                description="Premium monthly subscription",
                subtotal_amount=2000,
                tax_amount=400,
                total_amount=2400,
                currency="USD",
                charged_at=datetime(2026, 4, 12, tzinfo=timezone.utc),
                payment_status="paid",
                refund_status="none",
            )
        )
        db.commit()
        request = _request_for_user(admin)

        xlsx = admin_sales_ledger_export(
            "xlsx",
            request,
            db,
            period="custom",
            start_date="2026-04-01",
            end_date="2026-04-30",
            sort_by="date_charged",
            sort_dir="desc",
        )
        assert xlsx.media_type == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        with zipfile.ZipFile(BytesIO(xlsx.body)) as workbook:
            worksheet = workbook.read("xl/worksheets/sheet1.xml").decode()
        assert "transaction id" in worksheet
        assert "in_export" in worksheet

        pdf = admin_sales_ledger_export(
            "pdf",
            request,
            db,
            period="custom",
            start_date="2026-04-01",
            end_date="2026-04-30",
            sort_by="date_charged",
            sort_dir="desc",
        )
        assert pdf.media_type == "application/pdf"
        assert pdf.body.startswith(b"%PDF-1.4")
        assert b"Export Customer" in pdf.body
    finally:
        db.close()


def test_cancel_keeps_paid_access_and_reactivate_clears_period_end_cancel(monkeypatch):
    monkeypatch.setenv("CT_DEFAULT_TIER", "free")
    db = _session()
    period_end = 1_893_456_000
    calls = []

    def fake_stripe_post(path, data):
        calls.append((path, dict(data)))
        return {
            "id": "sub_cancel",
            "object": "subscription",
            "customer": "cus_cancel",
            "status": "active",
            "current_period_end": period_end,
            "cancel_at_period_end": data["cancel_at_period_end"] == "true",
        }

    monkeypatch.setattr("app.routers.accounts._stripe_post", fake_stripe_post)
    try:
        user = _user(db, "cancel@example.com", tier="premium")
        user.stripe_customer_id = "cus_cancel"
        user.stripe_subscription_id = "sub_cancel"
        user.subscription_status = "active"
        user.access_expires_at = datetime.fromtimestamp(period_end, tz=timezone.utc)
        db.commit()

        canceled = cancel_subscription_at_period_end(_request_for_user(user), db)
        db.refresh(user)
        assert canceled["subscription_cancel_at_period_end"] is True
        assert user.entitlement_tier == "premium"
        assert current_entitlements(_request_for_user(user), db).tier == "premium"
        assert calls[-1] == ("subscriptions/sub_cancel", {"cancel_at_period_end": "true"})

        reactivated = reactivate_subscription_before_expiry(_request_for_user(user), db)
        db.refresh(user)
        assert reactivated["subscription_cancel_at_period_end"] is False
        assert user.subscription_cancel_at_period_end is False
        assert current_entitlements(_request_for_user(user), db).tier == "premium"
        assert calls[-1] == ("subscriptions/sub_cancel", {"cancel_at_period_end": "false"})
    finally:
        db.close()


def test_admin_can_upgrade_downgrade_suspend_and_delete_user(monkeypatch):
    monkeypatch.setenv("ADMIN_EMAILS", "admin@example.com")
    monkeypatch.setenv("STRIPE_SECRET_KEY", "sk_test_hidden")
    monkeypatch.setenv("STRIPE_PRICE_ID_PREMIUM_MONTHLY", "price_admin_premium")
    monkeypatch.setenv("STRIPE_PREMIUM_ADMIN_FREE_PRICE_ID", "price_admin_premium_free")
    db = _session()
    stripe_calls: list[tuple[str, str, dict, str | None]] = []
    subscription_status = "active"

    def fake_stripe_get(path, params=None):
        stripe_calls.append(("GET", path, dict(params or {}), None))
        if path == "customers":
            return {"data": []}
        if path == "subscriptions/sub_admin_manage":
            return {
                "id": "sub_admin_manage",
                "object": "subscription",
                "customer": "cus_admin_manage",
                "status": subscription_status,
                "items": {"data": [{"id": "si_admin_manage", "price": {"id": "price_admin_premium_free", "unit_amount": 0, "currency": "usd", "recurring": {"interval": "month"}}}]},
            }
        if path == "subscriptions":
            return {"data": []}
        raise AssertionError(f"Unexpected Stripe GET path {path}")

    def fake_stripe_post(path, data, *, idempotency_key=None):
        nonlocal subscription_status
        stripe_calls.append(("POST", path, dict(data), idempotency_key))
        if path == "customers":
            return {"id": "cus_admin_manage"}
        if path == "customers/cus_admin_manage":
            return {"id": "cus_admin_manage"}
        if path == "subscriptions":
            subscription_status = "active"
            return {
                "id": "sub_admin_manage",
                "object": "subscription",
                "customer": "cus_admin_manage",
                "status": "active",
                "current_period_end": 1_893_456_000,
                "items": {"data": [{"id": "si_admin_manage", "price": {"id": data["items[0][price]"], "unit_amount": 0, "currency": "usd", "recurring": {"interval": "month"}}}]},
            }
        if path == "subscriptions/sub_admin_manage":
            return {"id": "sub_admin_manage", "customer": "cus_admin_manage", "status": subscription_status}
        raise AssertionError(f"Unexpected Stripe POST path {path}")

    def fake_stripe_delete(path, data=None, *, idempotency_key=None):
        nonlocal subscription_status
        stripe_calls.append(("DELETE", path, dict(data or {}), idempotency_key))
        if path == "subscriptions/sub_admin_manage":
            subscription_status = "canceled"
            return {"id": "sub_admin_manage", "status": "canceled"}
        if path == "customers/cus_admin_manage":
            return {"id": "cus_admin_manage", "object": "customer", "deleted": True}
        raise AssertionError(f"Unexpected Stripe DELETE path {path}")

    monkeypatch.setattr("app.routers.accounts._stripe_get", fake_stripe_get)
    monkeypatch.setattr("app.routers.accounts._stripe_post", fake_stripe_post)
    monkeypatch.setattr("app.routers.accounts._stripe_delete", fake_stripe_delete)
    try:
        admin = _user(db, "admin@example.com", role="admin")
        reader = _user(db, "reader@example.com")
        request = _request_for_user(admin)

        upgraded = admin_set_premium(reader.id, ManualPremiumPayload(tier="premium"), request, db)
        assert upgraded["manual_tier_override"] == "premium"
        assert upgraded["plan"] == "premium"
        assert upgraded["status"] == "active"
        assert upgraded["subscription_status"] == "active"
        assert upgraded["current_plan_amount_cents"] == 0
        assert upgraded["current_plan_display"] == "USD $0.00 / month"

        downgraded = admin_set_premium(reader.id, ManualPremiumPayload(tier="free"), request, db)
        assert downgraded["manual_tier_override"] == "free"
        assert downgraded["plan"] == "free"
        assert downgraded["status"] == "active"
        assert downgraded["subscription_status"] == "canceled"
        assert any(call[0] == "POST" and call[1] == "subscriptions" for call in stripe_calls)
        assert any(call[0] == "DELETE" and call[1] == "subscriptions/sub_admin_manage" for call in stripe_calls)
        active_rows_after_set_free = admin_users(request, db, search="reader@example.com", status="active", page=1, page_size=25)["items"]
        assert len(active_rows_after_set_free) == 1
        assert active_rows_after_set_free[0]["status"] == "active"
        assert active_rows_after_set_free[0]["subscription_status"] == "canceled"

        restored_plan = admin_set_premium(reader.id, ManualPremiumPayload(tier="premium"), request, db)
        assert restored_plan["manual_tier_override"] == "premium"
        assert restored_plan["plan"] == "premium"
        assert restored_plan["status"] == "active"
        assert restored_plan["subscription_status"] == "active"
        assert restored_plan["current_plan_amount_cents"] == 0
        assert restored_plan["current_plan_display"] == "USD $0.00 / month"

        suspended = admin_suspend_user(reader.id, SuspendPayload(suspended=True), request, db)
        assert suspended["is_suspended"] is True
        assert suspended["status"] == "suspended"
        assert suspended["subscription_status"] == "active"
        restored_access = admin_suspend_user(reader.id, SuspendPayload(suspended=False), request, db)
        assert restored_access["is_suspended"] is False
        assert restored_access["status"] == "active"
        assert restored_access["subscription_status"] == "active"
        assert any(call[0] == "POST" and call[1] == "customers" for call in stripe_calls)
        assert any(call[3] and call[3].startswith("admin-billing:user-") for call in stripe_calls if call[0] == "POST")

        result = admin_delete_user(reader.id, request, db)
        assert result["status"] == "deleted"
        assert result["stripe_cleanup"]["customer_deleted"] is True
        assert db.get(UserAccount, reader.id) is None
        assert any(call[0] == "DELETE" and call[1] == "customers/cus_admin_manage" for call in stripe_calls)
    finally:
        db.close()


def test_admin_delete_user_without_stripe_reference_succeeds_and_audits(monkeypatch):
    monkeypatch.setenv("ADMIN_EMAILS", "admin@example.com")
    monkeypatch.delenv("STRIPE_SECRET_KEY", raising=False)
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        reader = _user(db, "delete-no-stripe@example.com")

        result = admin_delete_user(reader.id, _request_for_user(admin), db)
        audit = db.execute(select(AdminBillingOverrideAuditLog)).scalar_one()

        assert result["status"] == "deleted"
        assert result["stripe_cleanup"]["status"] == "skipped_no_stripe_reference"
        assert db.get(UserAccount, reader.id) is None
        assert audit.override_type == "delete"
        assert audit.stripe_sync_status == "succeeded"
        assert '"stripe_cleanup_status": "skipped_no_stripe_reference"' in audit.requested_state_json
    finally:
        db.close()


def test_admin_delete_user_deletes_stripe_customer_after_canceling_active_subscription(monkeypatch):
    monkeypatch.setenv("ADMIN_EMAILS", "admin@example.com")
    monkeypatch.setenv("STRIPE_SECRET_KEY", "sk_test_hidden")
    db = _session()
    calls: list[tuple[str, str, dict, str | None]] = []

    def fake_stripe_get(path, params=None):
        calls.append(("GET", path, dict(params or {}), None))
        if path == "subscriptions/sub_delete":
            return {"id": "sub_delete", "object": "subscription", "customer": "cus_delete", "status": "active"}
        if path == "subscriptions":
            return {"data": [{"id": "sub_delete", "object": "subscription", "customer": "cus_delete", "status": "active"}]}
        raise AssertionError(f"Unexpected Stripe GET path {path}")

    def fake_stripe_delete(path, data=None, *, idempotency_key=None):
        calls.append(("DELETE", path, dict(data or {}), idempotency_key))
        if path == "subscriptions/sub_delete":
            return {"id": "sub_delete", "status": "canceled"}
        if path == "customers/cus_delete":
            return {"id": "cus_delete", "object": "customer", "deleted": True}
        raise AssertionError(f"Unexpected Stripe DELETE path {path}")

    monkeypatch.setattr("app.routers.accounts._stripe_get", fake_stripe_get)
    monkeypatch.setattr("app.routers.accounts._stripe_delete", fake_stripe_delete)
    try:
        admin = _user(db, "admin@example.com", role="admin")
        reader = _user(db, "delete-active-sub@example.com", tier="premium")
        reader.stripe_customer_id = "cus_delete"
        reader.stripe_subscription_id = "sub_delete"
        reader.subscription_status = "active"
        reader.subscription_plan = "premium"
        db.commit()

        result = admin_delete_user(reader.id, _request_for_user(admin), db)
        audit = db.execute(select(AdminBillingOverrideAuditLog)).scalar_one()

        assert result["status"] == "deleted"
        assert result["stripe_cleanup"]["subscriptions_cancelled"] == 1
        assert result["stripe_cleanup"]["customer_deleted"] is True
        assert result["stripe_cleanup"]["customer_retained"] is False
        assert db.get(UserAccount, reader.id) is None
        assert ("DELETE", "subscriptions/sub_delete", {}, f"admin-delete-user:{reader.id}:subscription:sub_delete") in calls
        assert ("DELETE", "customers/cus_delete", {}, f"admin-delete-user:{reader.id}:customer:cus_delete") in calls
        assert audit.override_type == "delete"
        assert audit.stripe_sync_status == "succeeded"
        assert audit.stripe_customer_id == "cus_delete"
        assert '"subscriptions_cancelled_count": 1' in audit.requested_state_json
        assert '"customer_deleted": true' in audit.requested_state_json
        assert "sk_test_hidden" not in audit.previous_state_json + audit.requested_state_json
    finally:
        db.close()


def test_admin_delete_user_with_stripe_customer_and_no_subscription_deletes_customer(monkeypatch):
    monkeypatch.setenv("ADMIN_EMAILS", "admin@example.com")
    monkeypatch.setenv("STRIPE_SECRET_KEY", "sk_test_hidden")
    db = _session()
    calls: list[tuple[str, str]] = []

    def fake_stripe_get(path, params=None):
        calls.append(("GET", path))
        if path == "subscriptions":
            return {"data": []}
        raise AssertionError(f"Unexpected Stripe GET path {path}")

    def fake_stripe_delete(path, data=None, *, idempotency_key=None):
        calls.append(("DELETE", path))
        if path == "customers/cus_no_sub_delete":
            return {"id": "cus_no_sub_delete", "object": "customer", "deleted": True}
        raise AssertionError(f"Unexpected Stripe DELETE path {path}")

    monkeypatch.setattr("app.routers.accounts._stripe_get", fake_stripe_get)
    monkeypatch.setattr("app.routers.accounts._stripe_delete", fake_stripe_delete)
    try:
        admin = _user(db, "admin@example.com", role="admin")
        reader = _user(db, "delete-no-sub@example.com")
        reader.stripe_customer_id = "cus_no_sub_delete"
        db.commit()

        result = admin_delete_user(reader.id, _request_for_user(admin), db)

        assert result["status"] == "deleted"
        assert result["stripe_cleanup"]["subscriptions_cancelled"] == 0
        assert result["stripe_cleanup"]["customer_deleted"] is True
        assert db.get(UserAccount, reader.id) is None
        assert ("GET", "subscriptions") in calls
        assert ("DELETE", "customers/cus_no_sub_delete") in calls
    finally:
        db.close()


def test_reregister_after_admin_delete_starts_with_clean_account(monkeypatch):
    monkeypatch.setenv("ADMIN_EMAILS", "admin@example.com")
    monkeypatch.setenv("APP_ENV", "test")
    monkeypatch.setenv("STRIPE_SECRET_KEY", "sk_test_hidden")
    db = _session()

    def fake_stripe_get(path, params=None):
        if path == "subscriptions":
            return {"data": []}
        raise AssertionError(f"Unexpected Stripe GET path {path}")

    def fake_stripe_delete(path, data=None, *, idempotency_key=None):
        if path == "customers/cus_reregister_delete":
            return {"id": "cus_reregister_delete", "object": "customer", "deleted": True}
        raise AssertionError(f"Unexpected Stripe DELETE path {path}")

    monkeypatch.setattr("app.routers.accounts._stripe_get", fake_stripe_get)
    monkeypatch.setattr("app.routers.accounts._stripe_delete", fake_stripe_delete)
    try:
        admin = _user(db, "admin@example.com", role="admin")
        reader = _user(db, "reregister-admin-delete@example.com", tier="premium")
        reader.stripe_customer_id = "cus_reregister_delete"
        reader.subscription_status = "active"
        reader.subscription_plan = "premium"
        db.commit()
        old_id = reader.id

        result = admin_delete_user(reader.id, _request_for_user(admin), db)
        assert db.get(UserAccount, old_id) is None
        registered = register(_register_payload("reregister-admin-delete@example.com"), db)
        new_user = db.get(UserAccount, registered["user"]["id"])

        assert result["stripe_cleanup"]["customer_deleted"] is True
        assert new_user is not None
        assert new_user.email == "reregister-admin-delete@example.com"
        assert new_user.stripe_customer_id is None
        assert new_user.stripe_subscription_id is None
        assert new_user.entitlement_tier == "free"
        assert registered["dev_verification_url"].startswith("http://localhost:3000/account/verify-email?token=")
    finally:
        db.close()


def test_admin_delete_user_stripe_cleanup_failure_keeps_local_user_and_audits(monkeypatch):
    monkeypatch.setenv("ADMIN_EMAILS", "admin@example.com")
    monkeypatch.setenv("STRIPE_SECRET_KEY", "sk_test_hidden")
    db = _session()

    def fake_stripe_get(path, params=None):
        if path == "subscriptions/sub_fail_delete":
            return {"id": "sub_fail_delete", "object": "subscription", "customer": "cus_fail_delete", "status": "active"}
        if path == "subscriptions":
            return {"data": [{"id": "sub_fail_delete", "object": "subscription", "customer": "cus_fail_delete", "status": "active"}]}
        raise AssertionError(f"Unexpected Stripe GET path {path}")

    def fake_stripe_delete(path, data=None, *, idempotency_key=None):
        raise HTTPException(status_code=502, detail="raw provider detail sk_test_hidden")

    monkeypatch.setattr("app.routers.accounts._stripe_get", fake_stripe_get)
    monkeypatch.setattr("app.routers.accounts._stripe_delete", fake_stripe_delete)
    try:
        admin = _user(db, "admin@example.com", role="admin")
        reader = _user(db, "delete-fails@example.com", tier="premium")
        reader.stripe_customer_id = "cus_fail_delete"
        reader.stripe_subscription_id = "sub_fail_delete"
        reader.subscription_status = "active"
        reader.subscription_plan = "premium"
        db.commit()

        with pytest.raises(HTTPException) as exc:
            admin_delete_user(reader.id, _request_for_user(admin), db)
        audit = db.execute(select(AdminBillingOverrideAuditLog)).scalar_one()

        assert exc.value.status_code == 502
        assert exc.value.detail["code"] == "admin_delete_stripe_cleanup_failed"
        assert db.get(UserAccount, reader.id) is not None
        assert audit.override_type == "delete"
        assert audit.stripe_sync_status == "failed"
        assert audit.error_message == "stripe_sync_failed_status_502"
        assert "raw provider detail" not in audit.error_message
        assert "sk_test_hidden" not in audit.previous_state_json + audit.requested_state_json + (audit.error_message or "")
    finally:
        db.close()


def test_admin_plan_override_syncs_to_stripe_before_local_state_changes(monkeypatch):
    monkeypatch.setenv("ADMIN_EMAILS", "admin@example.com")
    monkeypatch.setenv("STRIPE_SECRET_KEY", "sk_test_hidden")
    monkeypatch.setenv("STRIPE_PRICE_ID_PREMIUM_MONTHLY", "price_admin_premium_monthly")
    monkeypatch.setenv("STRIPE_PREMIUM_ADMIN_FREE_PRICE_ID", "price_admin_premium_free")
    db = _session()
    calls: list[tuple[str, str, dict, str | None]] = []
    try:
        admin = _user(db, "admin@example.com", role="admin")
        reader = _user(db, "plan-sync@example.com")

        def fake_stripe_get(path, params=None):
            calls.append(("GET", path, dict(params or {}), None))
            if path == "customers":
                return {"data": []}
            if path == "subscriptions":
                return {"data": []}
            raise AssertionError(f"Unexpected Stripe GET path {path}")

        def fake_stripe_post(path, data, *, idempotency_key=None):
            db.refresh(reader)
            assert reader.manual_tier_override is None
            assert reader.entitlement_tier == "free"
            calls.append(("POST", path, dict(data), idempotency_key))
            if path == "customers":
                return {"id": "cus_plan_sync"}
            if path == "subscriptions":
                return {
                    "id": "sub_plan_sync",
                    "object": "subscription",
                    "customer": "cus_plan_sync",
                    "status": "active",
                    "current_period_end": 1_893_456_000,
                    "items": {"data": [{"id": "si_plan_sync", "price": {"id": data["items[0][price]"], "unit_amount": 0, "currency": "usd", "recurring": {"interval": "month"}}}]},
                }
            raise AssertionError(f"Unexpected Stripe POST path {path}")

        monkeypatch.setattr("app.routers.accounts._stripe_get", fake_stripe_get)
        monkeypatch.setattr("app.routers.accounts._stripe_post", fake_stripe_post)

        result = admin_set_premium(reader.id, ManualPremiumPayload(tier="premium"), _request_for_user(admin), db)
        db.refresh(reader)
        audit = db.execute(select(AdminBillingOverrideAuditLog)).scalar_one()

        assert result["manual_tier_override"] == "premium"
        assert reader.entitlement_tier == "premium"
        assert reader.stripe_customer_id == "cus_plan_sync"
        assert reader.stripe_subscription_id == "sub_plan_sync"
        assert reader.stripe_price_id == "price_admin_premium_free"
        assert reader.subscription_plan == "premium"
        assert reader.subscription_status == "active"
        assert reader.subscription_cancel_at_period_end is False
        assert reader.access_expires_at is None
        customer_call = next(call for call in calls if call[0] == "POST" and call[1] == "customers")
        subscription_call = next(call for call in calls if call[0] == "POST" and call[1] == "subscriptions")
        assert customer_call[2]["metadata[walnut_admin_plan_override]"] == "premium"
        assert subscription_call[2]["items[0][price]"] == "price_admin_premium_free"
        assert subscription_call[2]["metadata[admin_override]"] == "true"
        assert subscription_call[2]["metadata[admin_override_price_mode]"] == "free_admin_grant"
        assert subscription_call[2]["metadata[tier]"] == "premium"
        assert customer_call[3] is not None
        assert customer_call[3].startswith(f"admin-billing:user-{reader.id}:plan:")
        assert audit.override_type == "plan"
        assert audit.stripe_sync_status == "succeeded"
        assert audit.stripe_customer_id == "cus_plan_sync"
        assert audit.stripe_subscription_id == "sub_plan_sync"
        assert '"price_mode": "free_admin_grant"' in audit.requested_state_json
        assert '"stripe_price_id": "price_admin_premium_free"' in audit.requested_state_json
        assert audit.error_message is None
        assert "sk_test_hidden" not in audit.previous_state_json + audit.requested_state_json
    finally:
        db.close()


def test_admin_plan_override_failure_leaves_local_state_unchanged_and_audits_safely(monkeypatch):
    monkeypatch.setenv("ADMIN_EMAILS", "admin@example.com")
    monkeypatch.setenv("STRIPE_SECRET_KEY", "sk_test_hidden")
    monkeypatch.setenv("STRIPE_PRICE_ID_PREMIUM_MONTHLY", "price_admin_premium_monthly")
    monkeypatch.setenv("STRIPE_PREMIUM_ADMIN_FREE_PRICE_ID", "price_admin_premium_free")
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        reader = _user(db, "plan-fail@example.com")

        monkeypatch.setattr("app.routers.accounts._stripe_get", lambda path, params=None: {"data": []})

        def fail_stripe_post(path, data, *, idempotency_key=None):
            if path == "customers":
                return {"id": "cus_plan_fail"}
            raise HTTPException(status_code=502, detail="Stripe request failed: raw provider detail")

        monkeypatch.setattr("app.routers.accounts._stripe_post", fail_stripe_post)

        with pytest.raises(HTTPException) as exc:
            admin_set_premium(reader.id, ManualPremiumPayload(tier="premium"), _request_for_user(admin), db)

        db.refresh(reader)
        audit = db.execute(select(AdminBillingOverrideAuditLog)).scalar_one()

        assert exc.value.status_code == 502
        assert exc.value.detail["message"] == "Couldn't create/update the Stripe subscription. No local plan change was saved."
        assert reader.manual_tier_override is None
        assert reader.entitlement_tier == "free"
        assert reader.stripe_subscription_id is None
        assert audit.override_type == "plan"
        assert audit.stripe_sync_status == "failed"
        assert audit.error_message == "stripe_sync_failed_status_502"
        assert "raw provider detail" not in audit.error_message
        assert "sk_test_hidden" not in audit.previous_state_json + audit.requested_state_json + (audit.error_message or "")
    finally:
        db.close()


def test_admin_plan_override_updates_existing_subscription_item_without_duplicate(monkeypatch):
    monkeypatch.setenv("ADMIN_EMAILS", "admin@example.com")
    monkeypatch.setenv("STRIPE_SECRET_KEY", "sk_test_hidden")
    monkeypatch.setenv("STRIPE_PRICE_ID_PRO_MONTHLY", "price_admin_pro_monthly")
    db = _session()
    calls: list[tuple[str, str, dict, str | None]] = []
    try:
        admin = _user(db, "admin@example.com", role="admin")
        reader = _user(db, "existing-sub@example.com", tier="premium")
        reader.stripe_customer_id = "cus_existing_sub"
        reader.stripe_subscription_id = "sub_existing_sub"
        reader.subscription_status = "active"
        reader.subscription_plan = "premium"
        db.commit()

        def fake_stripe_get(path, params=None):
            calls.append(("GET", path, dict(params or {}), None))
            if path == "subscriptions/sub_existing_sub":
                return {
                    "id": "sub_existing_sub",
                    "object": "subscription",
                    "customer": "cus_existing_sub",
                    "status": "active",
                    "items": {"data": [{"id": "si_existing_sub", "price": {"id": "price_admin_premium_monthly", "unit_amount": 1995, "currency": "usd", "recurring": {"interval": "month"}}}]},
                }
            raise AssertionError(f"Unexpected Stripe GET path {path}")

        def fake_stripe_post(path, data, *, idempotency_key=None):
            calls.append(("POST", path, dict(data), idempotency_key))
            if path == "customers/cus_existing_sub":
                return {"id": "cus_existing_sub"}
            if path == "subscriptions/sub_existing_sub":
                assert data["items[0][id]"] == "si_existing_sub"
                assert data["items[0][price]"] == "price_admin_pro_monthly"
                assert data["metadata[admin_override_price_mode]"] == "default"
                return {
                    "id": "sub_existing_sub",
                    "object": "subscription",
                    "customer": "cus_existing_sub",
                    "status": "active",
                    "current_period_end": 1_893_456_000,
                    "items": {"data": [{"id": "si_existing_sub", "price": {"id": "price_admin_pro_monthly", "unit_amount": 4995, "currency": "usd", "recurring": {"interval": "month"}}}]},
                }
            raise AssertionError(f"Unexpected Stripe POST path {path}")

        monkeypatch.setattr("app.routers.accounts._stripe_get", fake_stripe_get)
        monkeypatch.setattr("app.routers.accounts._stripe_post", fake_stripe_post)

        result = admin_set_premium(reader.id, ManualPremiumPayload(tier="pro", price_mode="default"), _request_for_user(admin), db)
        db.refresh(reader)

        assert result["manual_tier_override"] == "pro"
        assert reader.subscription_plan == "pro"
        assert reader.stripe_price_id == "price_admin_pro_monthly"
        assert [call[1] for call in calls if call[0] == "POST"].count("subscriptions") == 0
        assert [call[1] for call in calls if call[0] == "POST"].count("subscriptions/sub_existing_sub") == 1
    finally:
        db.close()


def test_admin_free_grant_upgrades_and_downgrades_existing_subscription_item(monkeypatch):
    monkeypatch.setenv("ADMIN_EMAILS", "admin@example.com")
    monkeypatch.setenv("STRIPE_SECRET_KEY", "sk_test_hidden")
    monkeypatch.setenv("STRIPE_PREMIUM_ADMIN_FREE_PRICE_ID", "price_admin_premium_free")
    monkeypatch.setenv("STRIPE_PRO_ADMIN_FREE_PRICE_ID", "price_admin_pro_free")
    db = _session()
    calls: list[tuple[str, str, dict, str | None]] = []
    current_price = "price_admin_premium_free"
    try:
        admin = _user(db, "admin@example.com", role="admin")
        reader = _user(db, "free-grant-upgrade@example.com", tier="premium")
        reader.manual_tier_override = "premium"
        reader.stripe_customer_id = "cus_admin_free_upgrade"
        reader.stripe_subscription_id = "sub_admin_free_upgrade"
        reader.subscription_status = "active"
        reader.subscription_plan = "premium"
        reader.stripe_price_id = "price_admin_premium_free"
        db.commit()

        def subscription_payload(price_id: str) -> dict:
            return {
                "id": "sub_admin_free_upgrade",
                "object": "subscription",
                "customer": "cus_admin_free_upgrade",
                "status": "active",
                "current_period_end": 1_893_456_000,
                "items": {
                    "data": [
                        {
                            "id": "si_admin_free_upgrade",
                            "price": {
                                "id": price_id,
                                "unit_amount": 0,
                                "currency": "usd",
                                "recurring": {"interval": "month"},
                            },
                        }
                    ]
                },
            }

        def fake_stripe_get(path, params=None):
            calls.append(("GET", path, dict(params or {}), None))
            if path == "subscriptions/sub_admin_free_upgrade":
                return subscription_payload(current_price)
            raise AssertionError(f"Unexpected Stripe GET path {path}")

        def fake_stripe_post(path, data, *, idempotency_key=None):
            nonlocal current_price
            calls.append(("POST", path, dict(data), idempotency_key))
            if path == "customers/cus_admin_free_upgrade":
                return {"id": "cus_admin_free_upgrade"}
            if path == "subscriptions/sub_admin_free_upgrade":
                assert data["items[0][id]"] == "si_admin_free_upgrade"
                assert data["proration_behavior"] == "none"
                assert data["metadata[admin_override_price_mode]"] == "free_admin_grant"
                current_price = data["items[0][price]"]
                return subscription_payload(current_price)
            raise AssertionError(f"Unexpected Stripe POST path {path}")

        monkeypatch.setattr("app.routers.accounts._stripe_get", fake_stripe_get)
        monkeypatch.setattr("app.routers.accounts._stripe_post", fake_stripe_post)

        upgraded = admin_set_premium(
            reader.id,
            ManualPremiumPayload(tier="pro", price_mode="free_admin_grant"),
            _request_for_user(admin),
            db,
        )
        db.refresh(reader)

        assert upgraded["manual_tier_override"] == "pro"
        assert reader.subscription_plan == "pro"
        assert reader.stripe_price_id == "price_admin_pro_free"
        assert current_entitlements(_request_for_user(reader), db).tier == "pro"

        downgraded = admin_set_premium(
            reader.id,
            ManualPremiumPayload(tier="premium", price_mode="free_admin_grant"),
            _request_for_user(admin),
            db,
        )
        db.refresh(reader)

        assert downgraded["manual_tier_override"] == "premium"
        assert reader.subscription_plan == "premium"
        assert reader.stripe_price_id == "price_admin_premium_free"
        assert current_entitlements(_request_for_user(reader), db).tier == "premium"
        assert [call[1] for call in calls if call[0] == "POST"].count("subscriptions") == 0
        assert [
            call[2]["items[0][price]"]
            for call in calls
            if call[0] == "POST" and call[1] == "subscriptions/sub_admin_free_upgrade"
        ] == ["price_admin_pro_free", "price_admin_premium_free"]
    finally:
        db.close()


def test_admin_free_grant_missing_pro_price_fails_without_stripe_mutation(monkeypatch):
    monkeypatch.setenv("ADMIN_EMAILS", "admin@example.com")
    monkeypatch.setenv("STRIPE_SECRET_KEY", "sk_test_hidden")
    monkeypatch.setenv("STRIPE_PREMIUM_ADMIN_FREE_PRICE_ID", "price_admin_premium_free")
    monkeypatch.delenv("STRIPE_PRO_ADMIN_FREE_PRICE_ID", raising=False)
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        reader = _user(db, "missing-pro-free-grant@example.com", tier="premium")
        reader.manual_tier_override = "premium"
        reader.stripe_customer_id = "cus_missing_pro_free"
        reader.stripe_subscription_id = "sub_missing_pro_free"
        reader.subscription_status = "active"
        reader.subscription_plan = "premium"
        reader.stripe_price_id = "price_admin_premium_free"
        db.commit()

        monkeypatch.setattr("app.routers.accounts._stripe_get", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("Stripe should not be read before setup validation.")))
        monkeypatch.setattr("app.routers.accounts._stripe_post", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("Stripe should not be mutated before setup validation.")))

        with pytest.raises(HTTPException) as exc:
            admin_set_premium(
                reader.id,
                ManualPremiumPayload(tier="pro", price_mode="free_admin_grant"),
                _request_for_user(admin),
                db,
            )

        db.refresh(reader)
        audit = db.execute(select(AdminBillingOverrideAuditLog)).scalar_one()

        assert exc.value.status_code == 503
        assert exc.value.detail["code"] == "admin_free_price_not_configured"
        assert exc.value.detail["message"] == "Pro admin free Stripe price is not configured."
        assert exc.value.detail["missing_env_vars"] == ["STRIPE_PRO_ADMIN_FREE_PRICE_ID"]
        assert reader.manual_tier_override == "premium"
        assert reader.entitlement_tier == "premium"
        assert reader.subscription_plan == "premium"
        assert reader.stripe_price_id == "price_admin_premium_free"
        assert audit.override_type == "plan"
        assert audit.stripe_sync_status == "failed"
        assert audit.error_message == "admin_free_price_not_configured"
    finally:
        db.close()


def test_admin_plan_override_custom_price_creates_price_and_subscription(monkeypatch):
    monkeypatch.setenv("ADMIN_EMAILS", "admin@example.com")
    monkeypatch.setenv("STRIPE_SECRET_KEY", "sk_test_hidden")
    monkeypatch.setenv("STRIPE_PRICE_ID_PREMIUM_MONTHLY", "price_admin_premium_monthly")
    db = _session()
    calls: list[tuple[str, str, dict, str | None]] = []
    try:
        admin = _user(db, "admin@example.com", role="admin")
        reader = _user(db, "custom-price@example.com")

        def fake_stripe_get(path, params=None):
            calls.append(("GET", path, dict(params or {}), None))
            if path == "customers":
                return {"data": []}
            if path == "prices/price_admin_premium_monthly":
                return {"id": "price_admin_premium_monthly", "product": "prod_premium"}
            if path == "subscriptions":
                return {"data": []}
            raise AssertionError(f"Unexpected Stripe GET path {path}")

        def fake_stripe_post(path, data, *, idempotency_key=None):
            calls.append(("POST", path, dict(data), idempotency_key))
            if path == "customers":
                return {"id": "cus_custom_price"}
            if path == "prices":
                assert data["unit_amount"] == 500
                assert data["currency"] == "usd"
                assert data["recurring[interval]"] == "month"
                assert data["product"] == "prod_premium"
                assert data["metadata[admin_override]"] == "true"
                return {"id": "price_admin_custom_500"}
            if path == "subscriptions":
                assert data["items[0][price]"] == "price_admin_custom_500"
                assert data["metadata[admin_override_price_mode]"] == "custom"
                return {
                    "id": "sub_custom_price",
                    "object": "subscription",
                    "customer": "cus_custom_price",
                    "status": "active",
                    "current_period_end": 1_893_456_000,
                    "items": {"data": [{"id": "si_custom_price", "price": {"id": "price_admin_custom_500", "unit_amount": 500, "currency": "usd", "recurring": {"interval": "month"}}}]},
                }
            raise AssertionError(f"Unexpected Stripe POST path {path}")

        monkeypatch.setattr("app.routers.accounts._stripe_get", fake_stripe_get)
        monkeypatch.setattr("app.routers.accounts._stripe_post", fake_stripe_post)

        result = admin_set_premium(
            reader.id,
            ManualPremiumPayload(
                tier="premium",
                price_mode="custom",
                custom_price={"amount_cents": 500, "currency": "USD", "interval": "month"},
            ),
            _request_for_user(admin),
            db,
        )
        db.refresh(reader)
        audit = db.execute(select(AdminBillingOverrideAuditLog)).scalar_one()

        assert result["manual_tier_override"] == "premium"
        assert reader.stripe_subscription_id == "sub_custom_price"
        assert reader.stripe_price_id == "price_admin_custom_500"
        assert '"price_mode": "custom"' in audit.requested_state_json
        assert '"amount_cents": 500' in audit.requested_state_json
        assert any(call[1] == "prices" and call[3] and call[3].endswith(":price") for call in calls)
    finally:
        db.close()


def test_billing_refresh_preserves_admin_created_zero_dollar_subscription(monkeypatch):
    monkeypatch.setenv("STRIPE_SECRET_KEY", "sk_test_hidden")
    db = _session()
    try:
        reader = _user(db, "refresh-admin-grant@example.com")
        reader.stripe_customer_id = "cus_refresh_admin"
        reader.stripe_subscription_id = "sub_refresh_admin"
        reader.manual_tier_override = "premium"
        db.commit()

        def fake_stripe_get(path, params=None):
            if path == "subscriptions/sub_refresh_admin":
                return {
                    "id": "sub_refresh_admin",
                    "object": "subscription",
                    "customer": "cus_refresh_admin",
                    "status": "active",
                    "current_period_end": 1_893_456_000,
                    "metadata": {
                        "admin_override": "true",
                        "admin_override_plan": "premium",
                        "admin_override_price_mode": "free_admin_grant",
                        "user_id": str(reader.id),
                    },
                    "items": {"data": [{"id": "si_refresh_admin", "price": {"id": "price_unmapped_admin_free", "unit_amount": 0, "currency": "usd", "recurring": {"interval": "month"}}}]},
                }
            raise AssertionError(f"Unexpected Stripe GET path {path}")

        monkeypatch.setattr("app.routers.accounts._stripe_get", fake_stripe_get)

        refreshed = refresh_subscription_from_stripe(_request_for_user(reader), db)
        db.refresh(reader)

        assert refreshed["user"]["entitlement_tier"] == "premium"
        assert reader.subscription_plan == "premium"
        assert reader.subscription_status == "active"
        assert current_entitlements(_request_for_user(reader), db).tier == "premium"
    finally:
        db.close()


def test_billing_refresh_preserves_paid_admin_override_when_no_subscription(monkeypatch):
    monkeypatch.setenv("STRIPE_SECRET_KEY", "sk_test_hidden")
    db = _session()
    try:
        reader = _user(db, "refresh-admin-grant-no-sub@example.com")
        reader.stripe_customer_id = "cus_no_sub"
        reader.manual_tier_override = "premium"
        reader.entitlement_tier = "premium"
        reader.subscription_plan = "premium"
        reader.subscription_status = "active"
        reader.subscription_cancel_at_period_end = True
        reader.access_expires_at = datetime.now(timezone.utc) - timedelta(days=1)
        db.commit()

        def fake_stripe_get(path, params=None):
            if path == "subscriptions":
                return {"data": []}
            raise AssertionError(f"Unexpected Stripe GET path {path}")

        monkeypatch.setattr("app.routers.accounts._stripe_get", fake_stripe_get)

        refreshed = refresh_subscription_from_stripe(_request_for_user(reader), db)
        db.refresh(reader)

        assert refreshed["status"] == "refreshed"
        assert refreshed["user"]["entitlement_tier"] == "premium"
        assert refreshed["user"]["current_plan"] == "premium"
        assert reader.manual_tier_override == "premium"
        assert reader.subscription_plan == "premium"
        assert reader.subscription_status == "active"
        assert reader.subscription_cancel_at_period_end is False
        assert reader.access_expires_at is None
        assert current_entitlements(_request_for_user(reader), db).tier == "premium"
    finally:
        db.close()


def test_stale_subscription_webhook_does_not_overwrite_newer_admin_override(monkeypatch):
    monkeypatch.setenv("CT_DEFAULT_TIER", "free")
    db = _session()
    try:
        reader = _user(db, "stale-webhook@example.com")
        reader.stripe_customer_id = "cus_stale_webhook"
        reader.stripe_subscription_id = "sub_new_admin"
        reader.manual_tier_override = "premium"
        reader.entitlement_tier = "premium"
        reader.subscription_plan = "premium"
        reader.subscription_status = "active"
        db.commit()

        result = process_stripe_event(
            db,
            {
                "id": "evt_stale_subscription_update",
                "type": "customer.subscription.updated",
                "data": {
                    "object": {
                        "id": "sub_old_checkout",
                        "object": "subscription",
                        "customer": "cus_stale_webhook",
                        "status": "active",
                        "metadata": {"user_id": str(reader.id), "plan": "free"},
                        "items": {"data": []},
                    }
                },
            },
        )
        db.refresh(reader)

        assert result["status"] == "processed"
        assert reader.stripe_subscription_id == "sub_new_admin"
        assert reader.manual_tier_override == "premium"
        assert reader.entitlement_tier == "premium"
    finally:
        db.close()


def test_admin_price_override_syncs_to_stripe_metadata_without_creating_prices(monkeypatch):
    monkeypatch.setenv("ADMIN_EMAILS", "admin@example.com")
    monkeypatch.setenv("STRIPE_SECRET_KEY", "sk_test_hidden")
    db = _session()
    calls: list[tuple[str, str, dict, str | None]] = []
    try:
        admin = _user(db, "admin@example.com", role="admin")
        reader = _user(db, "price-sync@example.com", tier="premium")
        reader.stripe_customer_id = "cus_price_sync"
        reader.stripe_subscription_id = "sub_price_sync"
        db.commit()

        def fake_stripe_get(path, params=None):
            calls.append(("GET", path, dict(params or {}), None))
            if path == "subscriptions/sub_price_sync":
                return {
                    "object": "subscription",
                    "id": "sub_price_sync",
                    "customer": "cus_price_sync",
                    "status": "active",
                    "items": {"data": []},
                }
            raise AssertionError(f"Unexpected Stripe GET path {path}")

        def fake_stripe_post(path, data, *, idempotency_key=None):
            calls.append(("POST", path, dict(data), idempotency_key))
            if path == "customers/cus_price_sync":
                return {"id": "cus_price_sync"}
            if path == "subscriptions/sub_price_sync":
                return {"id": "sub_price_sync", "customer": "cus_price_sync"}
            raise AssertionError(f"Unexpected Stripe POST path {path}")

        monkeypatch.setattr("app.routers.accounts._stripe_get", fake_stripe_get)
        monkeypatch.setattr("app.routers.accounts._stripe_post", fake_stripe_post)

        result = admin_set_user_price_override(
            reader.id,
            PriceOverridePayload(monthly_price_override=1495, annual_price_override=14995, override_currency="USD", override_note="support note"),
            _request_for_user(admin),
            db,
        )
        db.refresh(reader)
        audit = db.execute(select(AdminBillingOverrideAuditLog)).scalar_one()

        assert result["monthly_price_override"] == 1495
        assert reader.monthly_price_override == 1495
        assert reader.annual_price_override == 14995
        post_paths = [call[1] for call in calls if call[0] == "POST"]
        assert post_paths == ["customers/cus_price_sync", "subscriptions/sub_price_sync"]
        assert "prices" not in post_paths
        assert "subscription_items" not in post_paths
        customer_metadata = calls[1][2]
        subscription_metadata = calls[2][2]
        assert customer_metadata["metadata[walnut_admin_price_override_monthly]"] == "1495"
        assert subscription_metadata["metadata[walnut_admin_price_override_annual]"] == "14995"
        assert audit.override_type == "price"
        assert audit.stripe_subscription_id == "sub_price_sync"
        assert audit.stripe_sync_status == "succeeded"
    finally:
        db.close()


def test_admin_price_override_failure_does_not_save_local_override(monkeypatch):
    monkeypatch.setenv("ADMIN_EMAILS", "admin@example.com")
    monkeypatch.delenv("STRIPE_SECRET_KEY", raising=False)
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        reader = _user(db, "price-fail@example.com")

        with pytest.raises(HTTPException) as exc:
            admin_set_user_price_override(
                reader.id,
                PriceOverridePayload(monthly_price_override=995, override_currency="USD"),
                _request_for_user(admin),
                db,
            )

        db.refresh(reader)
        audit = db.execute(select(AdminBillingOverrideAuditLog)).scalar_one()

        assert exc.value.status_code == 503
        assert exc.value.detail["message"] == "Couldn't create/update the Stripe subscription. No local plan change was saved."
        assert reader.monthly_price_override is None
        assert reader.override_currency is None
        assert audit.override_type == "price"
        assert audit.stripe_sync_status == "failed"
        assert audit.error_message == "stripe_sync_failed_status_503"
    finally:
        db.close()


def test_admin_suspension_and_unsuspension_sync_to_stripe_with_stable_idempotency(monkeypatch):
    monkeypatch.setenv("ADMIN_EMAILS", "admin@example.com")
    monkeypatch.setenv("STRIPE_SECRET_KEY", "sk_test_hidden")
    db = _session()
    calls: list[tuple[str, str, dict, str | None]] = []
    try:
        admin = _user(db, "admin@example.com", role="admin")
        reader = _user(db, "suspend-sync@example.com")
        reader.stripe_customer_id = "cus_suspend_sync"
        reader.stripe_subscription_id = "sub_suspend_sync"
        db.commit()

        def fake_stripe_get(path, params=None):
            calls.append(("GET", path, dict(params or {}), None))
            if path == "subscriptions/sub_suspend_sync":
                return {
                    "object": "subscription",
                    "id": "sub_suspend_sync",
                    "customer": "cus_suspend_sync",
                    "status": "active",
                    "items": {"data": []},
                }
            raise AssertionError(f"Unexpected Stripe GET path {path}")

        def fake_stripe_post(path, data, *, idempotency_key=None):
            calls.append(("POST", path, dict(data), idempotency_key))
            if path == "customers/cus_suspend_sync":
                return {"id": "cus_suspend_sync"}
            if path == "subscriptions/sub_suspend_sync":
                return {"id": "sub_suspend_sync", "customer": "cus_suspend_sync"}
            raise AssertionError(f"Unexpected Stripe POST path {path}")

        monkeypatch.setattr("app.routers.accounts._stripe_get", fake_stripe_get)
        monkeypatch.setattr("app.routers.accounts._stripe_post", fake_stripe_post)

        first = admin_suspend_user(reader.id, SuspendPayload(suspended=True), _request_for_user(admin), db)
        second = admin_suspend_user(reader.id, SuspendPayload(suspended=True), _request_for_user(admin), db)
        restored = admin_suspend_user(reader.id, SuspendPayload(suspended=False), _request_for_user(admin), db)
        db.refresh(reader)

        post_calls = [call for call in calls if call[0] == "POST"]
        assert first["is_suspended"] is True
        assert second["is_suspended"] is True
        assert restored["is_suspended"] is False
        assert reader.is_suspended is False
        assert post_calls[0][2]["metadata[walnut_admin_suspended]"] == "true"
        assert post_calls[1][2]["metadata[walnut_admin_suspended]"] == "true"
        assert post_calls[4][2]["metadata[walnut_admin_suspended]"] == "false"
        assert post_calls[0][3] == post_calls[2][3]
        assert post_calls[1][3] == post_calls[3][3]
        assert all(call[1] != "subscriptions" for call in post_calls)
        assert all("subscription_items" not in call[1] for call in post_calls)
        audits = db.execute(select(AdminBillingOverrideAuditLog).order_by(AdminBillingOverrideAuditLog.id.asc())).scalars().all()
        assert [row.stripe_sync_status for row in audits] == ["succeeded", "succeeded", "succeeded"]
        assert all(row.override_type == "suspension" for row in audits)
    finally:
        db.close()


def test_admin_can_send_user_password_reset_without_exposing_token(monkeypatch, caplog):
    monkeypatch.setenv("ADMIN_EMAILS", "admin@example.com")
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        reader = _user(db, "reset-target@example.com")
        caplog.set_level(logging.INFO)

        result = admin_send_password_reset(reader.id, _request_for_user(admin), db)

        assert result == {"status": "ok"}
        assert "token" not in result
        db.refresh(reader)
        assert reader.password_reset_token_hash
        assert reader.password_reset_expires_at
        delivery = db.execute(
            select(EmailDelivery).where(EmailDelivery.template_key == "account.password_reset")
        ).scalar_one()
        assert delivery.user_id == reader.id
        assert delivery.to_email == "reset-target@example.com"
        assert delivery.idempotency_key == f"password-reset:{reader.id}:{reader.password_reset_token_hash}"
        assert "reset_url" in (delivery.payload_json or "")
        assert any("action=password_reset_requested" in record.getMessage() for record in caplog.records)
    finally:
        db.close()


def test_non_admin_cannot_send_user_password_reset(monkeypatch):
    monkeypatch.setenv("ADMIN_EMAILS", "admin@example.com")
    db = _session()
    try:
        reader = _user(db, "non-admin-reset@example.com")
        target = _user(db, "reset-target-2@example.com")

        with pytest.raises(HTTPException) as exc:
            admin_send_password_reset(target.id, _request_for_user(reader), db)

        assert exc.value.status_code == 403
        db.refresh(target)
        assert target.password_reset_token_hash is None
    finally:
        db.close()


def test_admin_feature_gate_change_is_backend_authoritative(monkeypatch):
    monkeypatch.setenv("ADMIN_EMAILS", "admin@example.com")
    monkeypatch.setenv("CT_DEFAULT_TIER", "free")
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        reader = _user(db, "reader@example.com")
        request = _request_for_user(admin)
        reader_request = _request_for_user(reader)
        admin_update_feature_gate("watchlists", FeatureGatePayload(required_tier="premium"), request, db)

        try:
            create_watchlist(WatchlistPayload(name="Blocked"), reader_request, db)
        except HTTPException as exc:
            assert exc.status_code == 402
            assert exc.detail["feature"] == "watchlists"
        else:
            raise AssertionError("Expected premium-required response")

        admin_update_feature_gate("watchlists", FeatureGatePayload(required_tier="free"), request, db)
        response = create_watchlist(WatchlistPayload(name="Allowed"), reader_request, db)
        assert response["name"] == "Allowed"
    finally:
        db.close()


def test_admin_plan_limit_change_updates_entitlements_and_pricing_config(monkeypatch):
    monkeypatch.setenv("ADMIN_EMAILS", "admin@example.com")
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        reader = _user(db, "reader@example.com")
        request = _request_for_user(admin)

        updated = admin_update_plan_limit(
            "watchlists",
            PlanLimitPayload(tier="free", limit_value=4),
            request,
            db,
        )

        assert updated["limit_value"] == 4
        assert current_entitlements(_request_for_user(reader), db).limit("watchlists") == 4
        config = public_plan_config(db)
        watchlists = next(feature for feature in config["features"] if feature["feature_key"] == "watchlists")
        assert watchlists["limits"]["free"] == 4
    finally:
        db.close()


def test_plan_config_free_defaults_fall_back_for_saved_views_and_monitoring_sources():
    db = _session()
    try:
        config = public_plan_config(db)
        free_tier = next(tier for tier in config["tiers"] if tier["tier"] == "free")
        premium_tier = next(tier for tier in config["tiers"] if tier["tier"] == "premium")
        assert free_tier["limits"]["saved_views"] == 1
        assert free_tier["limits"]["screener_saved_screens"] == 1
        assert free_tier["limits"]["monitoring_sources"] == 3
        assert premium_tier["limits"]["screener_saved_screens"] == 5
        assert premium_tier["limits"]["saved_views"] == 10
    finally:
        db.close()


def test_legacy_free_saved_views_setting_still_feeds_saved_views_and_saved_screens():
    db = _session()
    try:
        db.add(AppSetting(key="free_saved_views_limit", value="7"))
        db.commit()

        config = public_plan_config(db)
        free_tier = next(tier for tier in config["tiers"] if tier["tier"] == "free")
        assert free_tier["limits"]["saved_views"] == 7
        assert free_tier["limits"]["screener_saved_screens"] == 7
    finally:
        db.close()


def test_admin_saved_screen_and_saved_view_limits_persist_separately(monkeypatch):
    monkeypatch.setenv("ADMIN_EMAILS", "admin@example.com")
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        free_reader = _user(db, "reader@example.com")
        premium_reader = _user(db, "premium-reader@example.com", tier="premium")

        free_screens = admin_update_plan_limit(
            "screener_saved_screens",
            PlanLimitPayload(tier="free", limit_value=6),
            _request_for_user(admin),
            db,
        )
        premium_screens = admin_update_plan_limit(
            "screener_saved_screens",
            PlanLimitPayload(tier="premium", limit_value=12),
            _request_for_user(admin),
            db,
        )
        free_views = admin_update_plan_limit(
            "saved_views",
            PlanLimitPayload(tier="free", limit_value=4),
            _request_for_user(admin),
            db,
        )
        premium_views = admin_update_plan_limit(
            "saved_views",
            PlanLimitPayload(tier="premium", limit_value=42),
            _request_for_user(admin),
            db,
        )

        assert free_screens["limit_value"] == 6
        assert premium_screens["limit_value"] == 12
        assert free_views["limit_value"] == 4
        assert premium_views["limit_value"] == 42

        free_entitlements = current_entitlements(_request_for_user(free_reader), db)
        assert free_entitlements.limit("screener_saved_screens") == 6
        assert free_entitlements.limit("saved_views") == 4

        premium_entitlements = current_entitlements(_request_for_user(premium_reader), db)
        assert premium_entitlements.limit("screener_saved_screens") == 12
        assert premium_entitlements.limit("saved_views") == 42

        assert db.get(AppSetting, "saved_screens_free_limit").value == "6"
        assert db.get(AppSetting, "saved_screens_premium_limit").value == "12"
        assert db.get(AppSetting, "saved_views_free_limit").value == "4"
        assert db.get(AppSetting, "saved_views_premium_limit").value == "42"

        config = public_plan_config(db)
        free_tier = next(tier for tier in config["tiers"] if tier["tier"] == "free")
        premium_tier = next(tier for tier in config["tiers"] if tier["tier"] == "premium")
        assert free_tier["limits"]["saved_views"] == 4
        assert free_tier["limits"]["screener_saved_screens"] == 6
        assert premium_tier["limits"]["saved_views"] == 42
        assert premium_tier["limits"]["screener_saved_screens"] == 12
    finally:
        db.close()


def test_admin_plan_price_change_updates_public_pricing_config(monkeypatch):
    monkeypatch.setenv("ADMIN_EMAILS", "admin@example.com")
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        request = _request_for_user(admin)

        monthly = admin_update_plan_price(
            "premium",
            "monthly",
            PlanPricePayload(amount_cents=2495, currency="USD"),
            request,
            db,
        )
        annual = admin_update_plan_price(
            "premium",
            "annual",
            PlanPricePayload(amount_cents=21995, currency="USD"),
            request,
            db,
        )

        assert monthly["amount_cents"] == 2495
        assert annual["amount_cents"] == 21995
        config = public_plan_config(db)
        prices = {
            (price["tier"], price["billing_interval"]): price["amount_cents"]
            for price in config["plan_prices"]
        }
        assert prices[("premium", "monthly")] == 2495
        assert prices[("premium", "annual")] == 21995
    finally:
        db.close()


def test_admin_google_client_id_setting_persists_and_drives_oauth(monkeypatch):
    monkeypatch.setenv("ADMIN_EMAILS", "admin@example.com")
    monkeypatch.delenv("GOOGLE_CLIENT_ID", raising=False)
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        request = _request_for_user(admin)

        updated = admin_update_oauth_settings(
            OAuthSettingsPayload(google_client_id="saved-google-client"),
            request,
            db,
        )
        assert updated["google_client_id"] == "saved-google-client"
        assert admin_settings(request, db)["oauth"]["google_client_id"] == "saved-google-client"

        claims = _google_claims("reader-google@example.com", sub="saved-sub", name="Google Reader")
        claims["aud"] = "saved-google-client"
        user = upsert_google_user(db, claims)
        assert user.email == "reader-google@example.com"
    finally:
        db.close()


def test_google_start_uses_env_client_id_without_db_lookup(monkeypatch):
    monkeypatch.setenv("GOOGLE_CLIENT_ID", "env-google-client")

    class BusyDb:
        def get(self, *_args, **_kwargs):
            raise AssertionError("Google OAuth start should not query settings when env client id is configured.")

    assert _google_client_id(BusyDb(), prefer_env=True) == "env-google-client"
    started = google_auth_start(return_to="/terminal", db=BusyDb())

    assert "client_id=env-google-client" in started["authorization_url"]
    assert "return_to" not in started
    assert started["state"]


def test_google_callback_sets_session_cookie_on_fastapi_response(monkeypatch):
    monkeypatch.setenv("GOOGLE_CLIENT_ID", "google-client")
    monkeypatch.setenv("GOOGLE_CLIENT_SECRET", "google-secret")
    db = _session()
    try:
        state = sign_session_payload(
            {
                "kind": "google_oauth_state",
                "return_to": "/terminal",
                "exp": int(datetime.now(timezone.utc).timestamp()) + 600,
            }
        )

        class GoogleTokenResponse:
            status_code = 200
            text = ""

            def json(self):
                return {"id_token": _google_id_token(_google_claims("callback-reader@example.com"))}

        def fake_google_token_post(url, data, timeout):
            assert url == "https://oauth2.googleapis.com/token"
            assert data["code"] == "oauth-code"
            assert data["client_id"] == "google-client"
            assert data["client_secret"] == "google-secret"
            assert data["redirect_uri"] == "https://app.walnutmarkets.com/auth/google/callback"
            assert timeout == 20
            return GoogleTokenResponse()

        monkeypatch.setattr("app.routers.accounts.requests.post", fake_google_token_post)

        response = Response()
        auth = google_auth_callback(
            GoogleCallbackPayload(
                code="oauth-code",
                state=state,
                redirect_uri="https://app.walnutmarkets.com/auth/google/callback",
            ),
            response,
            db,
        )

        assert auth["user"]["email"] == "callback-reader@example.com"
        assert auth["return_to"] == "/terminal"
        assert auth["authenticated"] is True
        assert "token" not in auth
        assert f"{SESSION_COOKIE_NAME}=" in response.headers["set-cookie"]
        user = db.execute(select(UserAccount).where(UserAccount.email == "callback-reader@example.com")).scalar_one()
        assert user.email_verified_at is not None
        delivery = db.execute(
            select(EmailDelivery).where(EmailDelivery.template_key == "account.welcome")
        ).scalar_one()
        assert delivery.user_id == user.id
        assert delivery.to_email == "callback-reader@example.com"
        assert delivery.idempotency_key == f"account.welcome:user:{user.id}"
    finally:
        db.close()


def test_google_callback_repeat_login_does_not_duplicate_welcome(monkeypatch):
    monkeypatch.setenv("GOOGLE_CLIENT_ID", "google-client")
    monkeypatch.setenv("GOOGLE_CLIENT_SECRET", "google-secret")
    db = _session()
    try:
        state = sign_session_payload(
            {
                "kind": "google_oauth_state",
                "return_to": "/terminal",
                "exp": int(datetime.now(timezone.utc).timestamp()) + 600,
            }
        )

        class GoogleTokenResponse:
            status_code = 200
            text = ""

            def json(self):
                return {"id_token": _google_id_token(_google_claims("repeat-google@example.com", sub="repeat-sub"))}

        monkeypatch.setattr("app.routers.accounts.requests.post", lambda url, data, timeout: GoogleTokenResponse())

        first = google_auth_callback(GoogleCallbackPayload(code="oauth-code", state=state), Response(), db)
        second = google_auth_callback(GoogleCallbackPayload(code="oauth-code", state=state), Response(), db)

        assert first["user"]["id"] == second["user"]["id"]
        deliveries = db.execute(
            select(EmailDelivery).where(EmailDelivery.template_key == "account.welcome")
        ).scalars().all()
        assert len(deliveries) == 1
    finally:
        db.close()


def test_google_account_linking_does_not_send_welcome(monkeypatch):
    monkeypatch.setenv("GOOGLE_CLIENT_ID", "google-client")
    monkeypatch.setenv("GOOGLE_CLIENT_SECRET", "google-secret")
    db = _session()
    try:
        existing = _user(db, "linked-existing@example.com")
        state = sign_session_payload(
            {
                "kind": "google_oauth_state",
                "return_to": "/terminal",
                "exp": int(datetime.now(timezone.utc).timestamp()) + 600,
            }
        )

        class GoogleTokenResponse:
            status_code = 200
            text = ""

            def json(self):
                return {"id_token": _google_id_token(_google_claims("linked-existing@example.com", sub="linked-existing-sub"))}

        monkeypatch.setattr("app.routers.accounts.requests.post", lambda url, data, timeout: GoogleTokenResponse())

        auth = google_auth_callback(GoogleCallbackPayload(code="oauth-code", state=state), Response(), db)

        assert auth["user"]["id"] == existing.id
        assert db.execute(select(EmailDelivery)).scalars().all() == []
    finally:
        db.close()


def test_google_callback_token_exchange_failure_is_controlled(monkeypatch):
    monkeypatch.setenv("GOOGLE_CLIENT_ID", "google-client")
    monkeypatch.setenv("GOOGLE_CLIENT_SECRET", "google-secret")
    db = _session()
    try:
        state = sign_session_payload(
            {
                "kind": "google_oauth_state",
                "return_to": "/terminal",
                "exp": int(datetime.now(timezone.utc).timestamp()) + 600,
            }
        )

        class GoogleTokenResponse:
            status_code = 400
            text = "invalid_grant"

            def json(self):
                return {}

        monkeypatch.setattr("app.routers.accounts.requests.post", lambda url, data, timeout: GoogleTokenResponse())

        try:
            google_auth_callback(
                GoogleCallbackPayload(code="oauth-code", state=state),
                Response(),
                db,
            )
        except HTTPException as exc:
            assert exc.status_code == 401
            assert "Google token exchange failed" in exc.detail
        else:
            raise AssertionError("Expected Google token exchange failure to raise HTTPException.")
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


def test_google_sign_in_preserves_existing_admin_role(monkeypatch):
    monkeypatch.setenv("GOOGLE_CLIENT_ID", "google-client")
    db = _session()
    try:
        _user(db, "admin-google@example.com", role="admin")
        user = upsert_google_user(db, _google_claims("admin-google@example.com", sub="admin-sub", name="Admin"))
        db.commit()
        db.refresh(user)

        assert user.role == "admin"
        entitlements = current_entitlements(_request_for_user(user), db)
        assert entitlements.tier == "admin"
        assert "notification_digests" in entitlements.features
        assert admin_settings(_request_for_user(user), db, include_users=True)["users"][0]["email"] == "admin-google@example.com"
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
