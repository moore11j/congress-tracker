from __future__ import annotations

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from app.db import Base, ensure_email_notification_schema
from app.models import EmailDelivery, EmailTemplate
from app.services.email_delivery import send_email
from app.services.email_templates import seed_default_email_templates


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
        "reset_url": "https://walnut-intel.com/reset-password?token=redacted",
        "expires_minutes": 30,
    }


def test_default_templates_seed_password_changed_without_overwriting_existing():
    db = _session()
    try:
        template = db.execute(
            select(EmailTemplate).where(EmailTemplate.template_key == "account.password_changed")
        ).scalar_one()
        assert template.category == "account"
        assert template.from_name == "Walnut Intelligence Support"
        assert template.from_email == "support@walnut-intel.com"
        assert template.reply_to == "support@walnut-intel.com"
        assert template.subject == "Your Walnut Intelligence password was changed"
        assert "login_url" in template.variables_json

        template.subject = "Admin edited subject"
        db.commit()
        assert seed_default_email_templates(db) == 0
        db.refresh(template)
        assert template.subject == "Admin edited subject"
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
        assert captured["json"]["To"] == "reader@example.com"
        assert captured["json"]["TextBody"]
        assert captured["json"]["HtmlBody"]
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
