from __future__ import annotations

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from starlette.requests import Request

from app.db import Base, ensure_email_notification_schema
from app.models import EmailDelivery, EmailTemplate
from app.routers.contact import CONTACT_SUCCESS_MESSAGE, ContactFormPayload, submit_contact_form
from app.services.email_templates import seed_default_email_templates


class FakeResponse:
    def __init__(self, status_code: int, payload: dict | None = None):
        self.status_code = status_code
        self._payload = payload or {}
        self.text = str(self._payload)

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


def _request() -> Request:
    return Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/api/contact",
            "scheme": "https",
            "server": ("walnutmarkets.com", 443),
            "client": ("127.0.0.1", 12345),
            "headers": [
                (b"referer", b"https://walnutmarkets.com/contact"),
                (b"user-agent", b"pytest-contact-form"),
            ],
        }
    )


def test_contact_template_is_seeded():
    db = _session()
    try:
        template = db.execute(select(EmailTemplate).where(EmailTemplate.template_key == "support.contact_form")).scalar_one()
        assert template.category == "support"
        assert template.from_name == "Walnut Support"
        assert template.from_email == "support@walnutmarkets.com"
        assert "sender_email" in template.variables_json
    finally:
        db.close()


def test_contact_form_sends_postmark_email_to_support(monkeypatch):
    monkeypatch.setenv("EMAIL_PROVIDER", "postmark")
    monkeypatch.setenv("EMAIL_DELIVERY_ENABLED", "true")
    monkeypatch.setenv("POSTMARK_SERVER_TOKEN", "server-token")
    monkeypatch.setenv("EMAIL_FROM_SUPPORT", "Walnut Support <support@walnutmarkets.com>")
    monkeypatch.setenv("EMAIL_REPLY_TO_SUPPORT", "support@walnutmarkets.com")
    captured = {}

    def fake_post(url, headers, json, timeout):
        captured["url"] = url
        captured["headers"] = headers
        captured["payload"] = json
        return FakeResponse(200, {"MessageID": "postmark-contact-id"})

    monkeypatch.setattr("app.services.email_delivery.requests.post", fake_post)
    db = _session()
    try:
        result = submit_contact_form(
            ContactFormPayload(
                request_type="Reporting a bug",
                email="Reader@Example.com",
                message="The contact page should send through Postmark.",
            ),
            _request(),
            db,
        )

        assert result == {"status": "sent", "message": CONTACT_SUCCESS_MESSAGE}
        assert captured["url"] == "https://api.postmarkapp.com/email"
        assert captured["payload"]["To"] == "support@walnutmarkets.com"
        assert captured["payload"]["From"] == "Walnut Support <support@walnutmarkets.com>"
        assert captured["payload"]["ReplyTo"] == "reader@example.com"
        assert captured["payload"]["Subject"] == "Walnut contact: Reporting a bug"
        assert "The contact page should send through Postmark." in captured["payload"]["TextBody"]

        row = db.execute(select(EmailDelivery)).scalar_one()
        assert row.status == "sent"
        assert row.provider == "postmark"
        assert row.template_key == "support.contact_form"
        assert row.to_email == "support@walnutmarkets.com"
    finally:
        db.close()
