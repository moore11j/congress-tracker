from __future__ import annotations

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from starlette.requests import Request

from app.auth import sign_session_payload
from app.db import Base, ensure_email_notification_schema
from app.models import EmailDelivery, EmailTemplate, UserAccount
from app.routers.accounts import EmailTemplateBulkResetPayload, admin_reset_email_template_default, admin_reset_email_templates_defaults
from app.services.email_delivery import send_email
from app.services.email_templates import DEFAULT_TEMPLATES, reset_email_template_to_default, seed_default_email_templates


class FakeResponse:
    def __init__(self, status_code: int, payload: dict | None = None, text: str = ""):
        self.status_code = status_code
        self._payload = payload or {}
        self.text = text

    def json(self):
        return self._payload


def _session():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    ensure_email_notification_schema(engine)
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    db = Session()
    seed_default_email_templates(db)
    return db


def _reset_context() -> dict[str, object]:
    return {
        "first_name": "Ada",
        "reset_url": "https://app.walnutmarkets.com/reset-password?token=redacted",
        "expires_minutes": 30,
    }


def _request_for_user(user: UserAccount) -> Request:
    token = sign_session_payload({"uid": user.id, "email": user.email})
    return Request({"type": "http", "method": "POST", "path": "/", "headers": [(b"authorization", f"Bearer {token}".encode())]})


def _user(db, email: str, *, role: str = "user") -> UserAccount:
    user = UserAccount(email=email, first_name="Ada", role=role, entitlement_tier="premium")
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


def test_default_templates_seed_password_changed_without_overwriting_existing():
    db = _session()
    try:
        welcome = db.execute(
            select(EmailTemplate).where(EmailTemplate.template_key == "account.welcome")
        ).scalar_one()
        assert welcome.category == "account"
        assert welcome.from_name == "Walnut Markets"
        assert welcome.from_email == "no-reply@walnutmarkets.com"
        assert welcome.subject == "Welcome to Walnut"
        assert "app_url" in welcome.variables_json

        template = db.execute(
            select(EmailTemplate).where(EmailTemplate.template_key == "account.password_changed")
        ).scalar_one()
        assert template.category == "account"
        assert template.from_name == "Walnut Markets"
        assert template.from_email == "no-reply@walnutmarkets.com"
        assert template.reply_to == "support@walnutmarkets.com"
        assert template.subject == "Your Walnut password was changed"
        assert "login_url" in template.variables_json

        template.subject = "Admin edited subject"
        db.commit()
        assert seed_default_email_templates(db) == 0
        db.refresh(template)
        assert template.subject == "Admin edited subject"
    finally:
        db.close()


def test_seed_refreshes_legacy_template_branding_without_overwriting_subject():
    db = _session()
    try:
        template = db.execute(
            select(EmailTemplate).where(EmailTemplate.template_key == "account.password_reset")
        ).scalar_one()
        template.from_name = "Walnut Support"
        template.from_email = "support@walnut-intel.com"
        template.reply_to = "support@walnut-intel.com"
        template.subject = "Admin edited reset subject"
        template.body_text = "Walnut Support\nsupport@walnut-intel.com | walnut-intel.com | https://app.walnut-intel.com"
        template.body_html = '<a href="https://walnut-intel.com">support@walnut-intel.com</a><p>Walnut Support</p>'
        db.commit()

        assert seed_default_email_templates(db) == 0
        db.refresh(template)
        assert template.from_name == "Walnut Markets"
        assert template.from_email == "no-reply@walnutmarkets.com"
        assert template.reply_to == "support@walnutmarkets.com"
        assert template.subject == "Admin edited reset subject"
        assert "walnut-intel.com" not in template.body_text
        assert "walnut-intel.com" not in template.body_html
        assert "Walnut Support" not in template.body_text
        assert "Walnut Support" not in template.body_html
    finally:
        db.close()


def test_seed_refreshes_walnut_intel_subject_and_sender_branding():
    db = _session()
    try:
        template = db.execute(
            select(EmailTemplate).where(EmailTemplate.template_key == "account.verify_email")
        ).scalar_one()
        template.from_name = "Walnut Intelligence Support"
        template.from_email = "support@walnut-intel.com"
        template.reply_to = "support@walnut-intel.com"
        template.subject = "Verify your Walnut Intel email"
        template.body_text = "Walnut Intelligence Support\nVerify your Walnut Intel email at https://app.walnut-intel.com"
        template.body_html = "<h1>Walnut Intelligence</h1><p>Verify your Walnut Intel email.</p>"
        db.commit()

        assert seed_default_email_templates(db) == 0
        db.refresh(template)
        assert template.from_name == "Walnut Markets"
        assert template.from_email == "no-reply@walnutmarkets.com"
        assert template.reply_to == "support@walnutmarkets.com"
        assert template.subject == "Verify your Walnut Markets email"
        assert "walnut-intel.com" not in template.body_text
        assert "Walnut Intel" not in template.body_text
        assert "Walnut Intelligence" not in template.body_html
    finally:
        db.close()


def test_seed_legacy_refresh_preserves_legal_company_name():
    db = _session()
    try:
        assert seed_default_email_templates(db) == 0
        template = db.execute(select(EmailTemplate).where(EmailTemplate.template_key == "account.password_reset")).scalar_one()
        assert "Walnut Intelligence Inc. operates Walnut Market Terminal" in (template.body_html or "")
        assert "Walnut Markets Inc." not in (template.body_html or "")
    finally:
        db.close()


def test_default_templates_contain_branded_html_wrapper():
    for template in DEFAULT_TEMPLATES:
        body_html = template["body_html"]
        assert "<!doctype html>" in body_html
        assert "Walnut Intelligence Inc. operates Walnut Market Terminal" in body_html
        assert ">Walnut Markets</div>" in body_html
        assert ">Market Terminal</div>" in body_html
        assert "Walnut Market Terminal" in body_html
        assert "width:44px;height:44px" in body_html
        assert "border-left:4px solid #14d6a3" in body_html
        assert "background:#071114" in body_html
        assert "border-bottom:3px solid #14d6a3" in body_html
        assert "font-family:Arial,Helvetica,sans-serif" in body_html
        assert "<p>Hello" not in body_html


def test_named_default_templates_use_walnut_product_hierarchy():
    expected = {
        "account.password_reset": ("Walnut Markets", "Reset your Walnut Markets password"),
        "account.password_changed": ("Walnut Markets", "Your Walnut password was changed"),
        "account.verify_email": ("Walnut Markets", "Verify your Walnut Markets email"),
        "alerts.monitoring_digest": ("Walnut Markets", "Walnut monitoring digest"),
        "alerts.signal_alert": ("Walnut Markets", "Walnut signal digest"),
        "alerts.watchlist_activity": ("Walnut Markets", "Watchlist activity from Walnut"),
        "billing.monthly_statement": ("Walnut Markets", "Your Walnut monthly statement"),
        "billing.subscription_expiry_reminder": ("Walnut Markets", "Your Walnut {{plan}} access ends soon"),
    }
    templates = {str(template["template_key"]): template for template in DEFAULT_TEMPLATES}
    for template_key, (from_name, subject) in expected.items():
        template = templates[template_key]
        body_html = template["body_html"]
        assert template["from_name"] == from_name
        assert template["subject"] == subject
        assert ">Walnut Markets</div>" in body_html
        assert ">Market Terminal</div>" in body_html
        assert "Walnut Intelligence</div>" not in body_html
        assert "Launch Terminal" in body_html
        assert "Walnut Intelligence Inc. operates Walnut Market Terminal" in body_html


def test_alert_defaults_include_investment_disclaimer_but_account_defaults_do_not():
    for template in DEFAULT_TEMPLATES:
        body_html = template["body_html"]
        if template["category"] == "alerts":
            assert "does not constitute investment advice" in body_html
            assert "Manage notifications in Account Settings" in body_html
        elif template["template_key"] == "account.welcome":
            assert "not investment advice" in body_html
            assert "because you have a Walnut account" in body_html
        elif template["category"] == "account":
            assert "does not constitute investment advice" not in body_html
            assert "because you have a Walnut account" in body_html
            assert "Walnut will never ask for your password" in body_html


def test_reset_default_replaces_existing_plain_template_without_changing_seeder_behavior():
    db = _session()
    try:
        template = db.execute(select(EmailTemplate).where(EmailTemplate.template_key == "account.password_reset")).scalar_one()
        template.subject = "Plain reset"
        template.body_html = "<p>Hello {{first_name}}</p>"
        db.commit()

        assert seed_default_email_templates(db) == 0
        db.refresh(template)
        assert template.subject == "Plain reset"
        assert template.body_html == "<p>Hello {{first_name}}</p>"

        reset = reset_email_template_to_default(db, "account.password_reset")

        assert reset is not None
        assert reset.subject == "Reset your Walnut Markets password"
        assert "<!doctype html>" in (reset.body_html or "")
        assert "Walnut Market Terminal" in (reset.body_html or "")
        assert "Reset password" in (reset.body_html or "")
    finally:
        db.close()


def test_admin_reset_default_endpoint_requires_admin():
    db = _session()
    try:
        user = _user(db, "reader@example.com")
        try:
            admin_reset_email_template_default("account.password_reset", _request_for_user(user), db)
        except Exception as exc:
            assert getattr(exc, "status_code", None) == 403
        else:
            raise AssertionError("Expected non-admin reset to be rejected.")

        admin = _user(db, "admin@example.com", role="admin")
        template = admin_reset_email_template_default("account.password_reset", _request_for_user(admin), db)
        assert template["template_key"] == "account.password_reset"
        assert "Walnut Market Terminal" in template["body_html"]
    finally:
        db.close()


def test_admin_bulk_reset_defaults_replaces_all_system_templates():
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        plain = db.execute(select(EmailTemplate).where(EmailTemplate.template_key == "account.password_changed")).scalar_one()
        plain.body_html = "<p>Password changed</p>"
        db.commit()

        result = admin_reset_email_templates_defaults(EmailTemplateBulkResetPayload(), _request_for_user(admin), db)

        assert len(result["items"]) == len(DEFAULT_TEMPLATES)
        refreshed = db.execute(select(EmailTemplate).where(EmailTemplate.template_key == "account.password_changed")).scalar_one()
        assert "<!doctype html>" in (refreshed.body_html or "")
        assert "width:44px;height:44px" in (refreshed.body_html or "")
        assert "<p>Password changed</p>" not in (refreshed.body_html or "")
    finally:
        db.close()


def test_html_render_escapes_regular_variables_but_keeps_trusted_digest_snippets(monkeypatch):
    monkeypatch.setenv("EMAIL_PROVIDER", "postmark")
    monkeypatch.setenv("EMAIL_DELIVERY_ENABLED", "true")
    monkeypatch.delenv("POSTMARK_SERVER_TOKEN", raising=False)
    db = _session()
    try:
        result = send_email(
            db,
            to_email="reader@example.com",
            template_key="alerts.watchlist_activity",
            context={
                "first_name": "<Admin>",
                "watchlist_name": "AI <Infra>",
                "summary": "One <match>",
                "items_text": "- NVDA",
                "items_html": "<table><tr><td>NVDA</td></tr></table>",
                "activity_url": "https://app.walnutmarkets.com/watchlists/1",
            },
            category="alerts",
        )

        body_html = result["body_html"]
        assert "Hello &lt;Admin&gt;" in body_html
        assert "AI &lt;Infra&gt;" in body_html
        assert "One &lt;match&gt;" in body_html
        assert "<table><tr><td>NVDA</td></tr></table>" in body_html
    finally:
        db.close()


def test_postmark_disabled_delivery_creates_skipped_row(monkeypatch):
    monkeypatch.setenv("EMAIL_PROVIDER", "postmark")
    monkeypatch.setenv("EMAIL_DELIVERY_ENABLED", "false")
    monkeypatch.setenv("POSTMARK_SERVER_TOKEN", "server-token")

    def fail_post(*args, **kwargs):
        raise AssertionError("Provider should not be called when delivery is disabled.")

    monkeypatch.setattr("app.services.email_delivery.requests.post", fail_post)
    db = _session()
    try:
        result = send_email(
            db,
            to_email="reader@example.com",
            template_key="account.password_reset",
            context=_reset_context(),
            category="account",
        )

        row = db.execute(select(EmailDelivery)).scalar_one()
        assert result["status"] == "skipped"
        assert row.status == "skipped"
        assert row.provider == "postmark"
        assert row.error == "Email delivery is disabled."
    finally:
        db.close()


def test_postmark_missing_token_creates_log_only_row(monkeypatch):
    monkeypatch.setenv("EMAIL_PROVIDER", "postmark")
    monkeypatch.setenv("EMAIL_DELIVERY_ENABLED", "true")
    monkeypatch.delenv("POSTMARK_SERVER_TOKEN", raising=False)

    db = _session()
    try:
        result = send_email(
            db,
            to_email="reader@example.com",
            template_key="account.password_reset",
            context=_reset_context(),
            category="account",
        )

        row = db.execute(select(EmailDelivery)).scalar_one()
        assert result["status"] == "log_only"
        assert result["body_text"]
        assert row.status == "log_only"
        assert row.provider == "postmark"
        assert row.error == "Provider API key is not configured."
    finally:
        db.close()


def test_postmark_success_marks_delivery_sent(monkeypatch):
    monkeypatch.setenv("EMAIL_PROVIDER", "postmark")
    monkeypatch.setenv("EMAIL_DELIVERY_ENABLED", "true")
    monkeypatch.setenv("POSTMARK_SERVER_TOKEN", "server-token")
    captured = {}

    def fake_post(url, headers, json, timeout):
        captured.update({"url": url, "headers": headers, "json": json, "timeout": timeout})
        return FakeResponse(200, {"MessageID": "postmark-message-id"})

    monkeypatch.setattr("app.services.email_delivery.requests.post", fake_post)
    db = _session()
    try:
        result = send_email(
            db,
            to_email="reader@example.com",
            template_key="account.password_reset",
            context=_reset_context(),
            category="account",
        )

        row = db.execute(select(EmailDelivery)).scalar_one()
        assert result["status"] == "sent"
        assert result["provider_message_id"] == "postmark-message-id"
        assert row.status == "sent"
        assert row.provider_message_id == "postmark-message-id"
        assert captured["url"] == "https://api.postmarkapp.com/email"
        assert captured["headers"]["X-Postmark-Server-Token"] == "server-token"
        assert captured["json"]["MessageStream"] == "outbound"
        assert captured["json"]["From"] == "Walnut Markets <no-reply@walnutmarkets.com>"
        assert captured["json"]["ReplyTo"] == "support@walnutmarkets.com"
        assert captured["json"]["To"] == "reader@example.com"
        assert captured["json"]["TextBody"]
        assert captured["json"]["HtmlBody"]
    finally:
        db.close()


def test_template_sender_overrides_alerts_env_fallback(monkeypatch):
    monkeypatch.setenv("EMAIL_PROVIDER", "postmark")
    monkeypatch.setenv("EMAIL_DELIVERY_ENABLED", "true")
    monkeypatch.setenv("POSTMARK_SERVER_TOKEN", "server-token")
    monkeypatch.setenv("EMAIL_FROM_ALERTS", "Walnut Alerts <alerts@walnut-intel.com>")
    captured = {}

    def fake_post(url, headers, json, timeout):
        captured.update(json)
        return FakeResponse(200, {"MessageID": "postmark-message-id"})

    monkeypatch.setattr("app.services.email_delivery.requests.post", fake_post)
    db = _session()
    try:
        send_email(
            db,
            to_email="reader@example.com",
            template_key="alerts.watchlist_activity",
            context={
                "first_name": "Ada",
                "watchlist_name": "AI Infrastructure",
                "summary": "1 new item",
                "items_text": "- NVDA",
                "items_html": "<table><tr><td>NVDA</td></tr></table>",
                "activity_url": "https://app.walnutmarkets.com/watchlists/1",
            },
            category="alerts",
        )

        row = db.execute(select(EmailDelivery)).scalar_one()
        assert captured["From"] == "Walnut Markets <alerts@walnutmarkets.com>"
        assert row.from_email == "alerts@walnutmarkets.com"
    finally:
        db.close()


def test_blank_template_sender_uses_alerts_env_fallback(monkeypatch):
    monkeypatch.setenv("EMAIL_PROVIDER", "postmark")
    monkeypatch.setenv("EMAIL_DELIVERY_ENABLED", "true")
    monkeypatch.setenv("POSTMARK_SERVER_TOKEN", "server-token")
    monkeypatch.setenv("EMAIL_FROM_ALERTS", "Walnut Alerts <alerts@walnut-intel.com>")
    captured = {}

    def fake_post(url, headers, json, timeout):
        captured.update(json)
        return FakeResponse(200, {"MessageID": "postmark-message-id"})

    monkeypatch.setattr("app.services.email_delivery.requests.post", fake_post)
    db = _session()
    try:
        template = db.execute(select(EmailTemplate).where(EmailTemplate.template_key == "alerts.watchlist_activity")).scalar_one()
        template.from_email = ""
        db.commit()

        send_email(
            db,
            to_email="reader@example.com",
            template_key="alerts.watchlist_activity",
            context={
                "first_name": "Ada",
                "watchlist_name": "AI Infrastructure",
                "summary": "1 new item",
                "items_text": "- NVDA",
                "items_html": "<table><tr><td>NVDA</td></tr></table>",
                "activity_url": "https://app.walnutmarkets.com/watchlists/1",
            },
            category="alerts",
        )

        row = db.execute(select(EmailDelivery)).scalar_one()
        assert captured["From"] == "Walnut Markets <alerts@walnut-intel.com>"
        assert row.from_email == "alerts@walnut-intel.com"
    finally:
        db.close()


def test_account_sender_prefers_expected_global_env_names(monkeypatch):
    monkeypatch.setenv("EMAIL_PROVIDER", "postmark")
    monkeypatch.setenv("EMAIL_DELIVERY_ENABLED", "true")
    monkeypatch.setenv("POSTMARK_SERVER_TOKEN", "server-token")
    monkeypatch.setenv("EMAIL_FROM_SUPPORT", "Walnut Intelligence Support <support@walnut-intel.com>")
    monkeypatch.setenv("EMAIL_REPLY_TO_SUPPORT", "support@walnut-intel.com")
    monkeypatch.setenv("EMAIL_FROM", "Walnut Markets <no-reply@walnutmarkets.com>")
    monkeypatch.setenv("EMAIL_REPLY_TO", "support@walnutmarkets.com")
    captured = {}

    def fake_post(url, headers, json, timeout):
        captured.update(json)
        return FakeResponse(200, {"MessageID": "postmark-message-id"})

    monkeypatch.setattr("app.services.email_delivery.requests.post", fake_post)
    db = _session()
    try:
        send_email(
            db,
            to_email="reader@example.com",
            template_key="account.verify_email",
            context={
                "first_name": "Ada",
                "verification_url": "https://app.walnutmarkets.com/account/verify-email?token=redacted",
                "expires_minutes": 1440,
            },
            category="account",
        )

        assert captured["From"] == "Walnut Markets <no-reply@walnutmarkets.com>"
        assert captured["ReplyTo"] == "support@walnutmarkets.com"
        assert captured["Subject"] == "Verify your Walnut Markets email"
    finally:
        db.close()


def test_password_reset_sender_prefers_password_reset_from(monkeypatch):
    monkeypatch.setenv("EMAIL_PROVIDER", "postmark")
    monkeypatch.setenv("EMAIL_DELIVERY_ENABLED", "true")
    monkeypatch.setenv("POSTMARK_SERVER_TOKEN", "server-token")
    monkeypatch.setenv("EMAIL_FROM", "Walnut Markets Support <support@walnutmarkets.com>")
    monkeypatch.setenv("PASSWORD_RESET_FROM", "Walnut Markets <no-reply@walnutmarkets.com>")
    monkeypatch.setenv("EMAIL_REPLY_TO", "support@walnutmarkets.com")
    captured = {}

    def fake_post(url, headers, json, timeout):
        captured.update(json)
        return FakeResponse(200, {"MessageID": "postmark-message-id"})

    monkeypatch.setattr("app.services.email_delivery.requests.post", fake_post)
    db = _session()
    try:
        send_email(
            db,
            to_email="reader@example.com",
            template_key="account.password_reset",
            context=_reset_context(),
            category="account",
        )

        assert captured["From"] == "Walnut Markets <no-reply@walnutmarkets.com>"
        assert captured["ReplyTo"] == "support@walnutmarkets.com"
        assert captured["Subject"] == "Reset your Walnut Markets password"
    finally:
        db.close()


def test_postmark_non_2xx_marks_delivery_failed(monkeypatch):
    monkeypatch.setenv("EMAIL_PROVIDER", "postmark")
    monkeypatch.setenv("EMAIL_DELIVERY_ENABLED", "true")
    monkeypatch.setenv("POSTMARK_SERVER_TOKEN", "server-token")

    def fake_post(url, headers, json, timeout):
        return FakeResponse(422, {"Message": "Sender signature not found."})

    monkeypatch.setattr("app.services.email_delivery.requests.post", fake_post)
    db = _session()
    try:
        result = send_email(
            db,
            to_email="reader@example.com",
            template_key="account.password_reset",
            context=_reset_context(),
            category="account",
        )

        row = db.execute(select(EmailDelivery)).scalar_one()
        assert result["status"] == "failed"
        assert row.status == "failed"
        assert row.provider == "postmark"
        assert row.provider_message_id is None
        assert "HTTP 422" in (row.error or "")
        assert "Sender signature not found" in (row.error or "")
    finally:
        db.close()
