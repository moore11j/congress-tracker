from __future__ import annotations

import zipfile
from datetime import datetime, timezone
from io import BytesIO

from fastapi import HTTPException
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from starlette.requests import Request

from app.auth import sign_session_payload
from app.db import Base
from app.entitlements import current_entitlements, seed_feature_gates
from app.main import WatchlistPayload, create_watchlist
from app.models import BillingTransaction, UserAccount, Watchlist
from app.routers.accounts import (
    CheckoutSessionPayload,
    FeatureGatePayload,
    LoginPayload,
    ManualPremiumPayload,
    NotificationSettingsPayload,
    OAuthSettingsPayload,
    PasswordResetConfirmPayload,
    PasswordResetRequestPayload,
    PasswordChangePayload,
    PlanLimitPayload,
    PlanPricePayload,
    ProfileUpdatePayload,
    RegisterPayload,
    StripeTaxSettingsPayload,
    SuspendPayload,
    admin_set_premium,
    admin_settings,
    admin_sales_ledger,
    admin_reports_summary,
    admin_sales_ledger_export,
    admin_suspend_user,
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
    cancel_subscription_at_period_end,
    create_checkout_session,
    confirm_password_reset,
    login,
    process_stripe_event,
    public_plan_config,
    reactivate_subscription_before_expiry,
    register,
    request_password_reset,
    update_account_notifications,
    update_account_password,
    update_account_profile,
    stripe_tax_billing_readiness,
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


def _register_payload(email: str, *, password: str = "password123") -> RegisterPayload:
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


def test_email_password_register_login_and_reset_flow():
    db = _session()
    try:
        registered = register(_register_payload("reader-one@example.com"), db)
        user = db.get(UserAccount, registered["user"]["id"])
        assert user is not None
        assert user.name == "Reader One"
        assert user.country == "US"
        assert user.postal_code == "94105"
        assert registered["user"]["billing_profile_complete"] is True
        assert user.password_hash

        signed_in = login(LoginPayload(email="reader-one@example.com", password="password123"), db)
        assert signed_in["user"]["email"] == "reader-one@example.com"

        reset = request_password_reset(PasswordResetRequestPayload(email="reader-one@example.com"), db)
        assert reset["reset_path"].startswith("/reset-password?token=")
        token = reset["reset_path"].split("token=", 1)[1]

        confirmed = confirm_password_reset(PasswordResetConfirmPayload(token=token, password="newpassword123"), db)
        assert confirmed["user"]["email"] == "reader-one@example.com"
        assert login(LoginPayload(email="reader-one@example.com", password="newpassword123"), db)["user"]["id"] == user.id
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
                current_password="password123",
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
        assert admin_settings(_request_for_user(admin), db, include_users=False)["users"] == []
    finally:
        db.close()


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
        assert premium_response["items"][0]["plan"] == "premium"
        assert premium_response["items"][0]["status"] == "active"
        assert premium_response["items"][0]["admin_flag"] == "no"
        assert "password_hash" not in premium_response["items"][0]

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
        documented = next(item for item in history["items"] if item["transaction_id"] == "in_123")
        fallback = next(item for item in history["items"] if item["transaction_id"] == "in_without_docs")
        assert documented["documents"]["invoice_number"] == "INV-2026-0001"
        assert documented["documents"]["hosted_invoice_url"] == "https://invoice.stripe.com/i/acct_test/in_123"
        assert documented["documents"]["invoice_pdf_url"] == "https://pay.stripe.com/invoice/acct_test/in_123/pdf"
        assert documented["documents"]["receipt_url"] == "https://pay.stripe.com/receipts/acct_test/ch_123"
        assert documented["documents"]["has_stripe_document"] is True
        assert fallback["documents"]["has_stripe_document"] is False
        assert fallback["documents"]["hosted_invoice_url"] is None
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
        assert free_tier["limits"]["saved_views"] == 3
        assert free_tier["limits"]["screener_saved_screens"] == 3
        assert free_tier["limits"]["monitoring_sources"] == 2
    finally:
        db.close()


def test_admin_free_saved_views_limit_updates_saved_screens_too(monkeypatch):
    monkeypatch.setenv("ADMIN_EMAILS", "admin@example.com")
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        reader = _user(db, "reader@example.com")

        updated = admin_update_plan_limit(
            "saved_views",
            PlanLimitPayload(tier="free", limit_value=6),
            _request_for_user(admin),
            db,
        )

        assert updated["limit_value"] == 6
        entitlements = current_entitlements(_request_for_user(reader), db)
        assert entitlements.limit("saved_views") == 6
        assert entitlements.limit("screener_saved_screens") == 6
        config = public_plan_config(db)
        free_tier = next(tier for tier in config["tiers"] if tier["tier"] == "free")
        assert free_tier["limits"]["saved_views"] == 6
        assert free_tier["limits"]["screener_saved_screens"] == 6
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
