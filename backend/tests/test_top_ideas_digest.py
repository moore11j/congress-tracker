from datetime import datetime, timezone

import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from app.db import Base
from app.entitlements import ENTITLEMENTS
from app.models import EmailDelivery, EmailTemplate, UserAccount
from app.services import top_ideas_digest as service
from app.services.email_digests import run_digest_job
from app.routers.accounts import _set_top_ideas_preference
from test_top_ideas_access import snapshot

FRIDAY = datetime(2026, 9, 25, 20, 10, tzinfo=timezone.utc)
THURSDAY = datetime(2026, 9, 24, 20, 10, tzinfo=timezone.utc)


@pytest.fixture
def db(monkeypatch):
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine, tables=[UserAccount.__table__, EmailTemplate.__table__, EmailDelivery.__table__])
    monkeypatch.setattr(service, "entitlements_for_user", lambda db, user: ENTITLEMENTS[user.entitlement_tier])
    monkeypatch.setattr("app.entitlements.entitlements_for_user", lambda db, user: ENTITLEMENTS[user.entitlement_tier])
    monkeypatch.setattr(service, "build_top_stocks_response", lambda *a, **k: snapshot())
    with Session(engine) as session:
        yield session


def user(db, frequency="off", tier="free", **kw):
    row = UserAccount(email=f"user{len(db.scalars(select(UserAccount)).all())}@example.test", entitlement_tier=tier,
                      top_stock_ideas_frequency=frequency, email_verified_at=FRIDAY, **kw)
    db.add(row)
    db.commit()
    return row


def test_default_off_and_explicit_weekly_opt_in(db):
    row = user(db)
    assert run_digest_job(db, kind="top_ideas", dry_run=True, now=FRIDAY) == []
    _set_top_ideas_preference(db, row, "weekly")
    db.commit()
    result = run_digest_job(db, kind="top_ideas", dry_run=True, now=FRIDAY)
    assert result[0]["status"] == "would_send"
    assert result[0]["items_count"] == 5
    assert not db.scalars(select(EmailDelivery)).all()
    digest = service.build_top_ideas_digest(db, row)
    assert "STOCK1" in digest.context["items_text"]
    assert "PAID_EVIDENCE" not in str(digest)
    assert "confirmation_score" not in str(digest)


def test_free_cannot_request_daily_and_downgrade_is_weekly(db):
    row = user(db, "weekly")
    with pytest.raises(HTTPException) as error:
        _set_top_ideas_preference(db, row, "daily")
    assert error.value.status_code == 403
    row.top_stock_ideas_frequency = "daily"  # Paid preference retained after downgrade.
    db.commit()
    assert service.run_top_ideas_digest(db, dry_run=True, now=THURSDAY) == []
    assert service.run_top_ideas_digest(db, dry_run=True, now=FRIDAY)[0]["items_count"] == 5


@pytest.mark.parametrize("tier,count", [("premium", 10), ("pro", 25)])
def test_paid_daily_delivery_has_more_ideas_and_entitled_evidence(db, tier, count):
    row = user(db, "daily", tier)
    assert service.run_top_ideas_digest(db, dry_run=True, now=THURSDAY)[0]["items_count"] == count
    assert "PAID_EVIDENCE" in service.build_top_ideas_digest(db, row).context["items_text"]


@pytest.mark.parametrize("field", ["is_suspended", "alerts_enabled", "email_notifications_enabled", "email_verified_at", "deleted_at"])
def test_existing_delivery_opt_outs_and_unverified_accounts_are_respected(db, field):
    row = user(db, "weekly")
    setattr(row, field, True if field == "is_suspended" else FRIDAY if field == "deleted_at" else None if field == "email_verified_at" else False)
    db.commit()
    assert service.run_top_ideas_digest(db, dry_run=True, now=FRIDAY) == []


def test_retries_skip_sent_users_and_reach_later_recipients(db, monkeypatch):
    for _ in range(3):
        user(db, "weekly")
    sent = set()
    monkeypatch.setattr("app.services.email_digests._duplicate_digest_result", lambda db, key: {"status": "skipped"} if key in sent else None)
    def send(db, *, user, digest, category, idempotency_key):
        sent.add(idempotency_key)
        return {"status": "sent"}
    monkeypatch.setattr("app.services.email_digests._send_digest", send)
    for _ in range(3):
        assert service.run_top_ideas_digest(db, limit=1, now=FRIDAY)[0]["status"] == "sent"
    assert service.run_top_ideas_digest(db, limit=1, now=FRIDAY) == []
    assert len(sent) == 3


def test_template_rendering_escapes_content_and_includes_manage_delivery(db, monkeypatch):
    from app.services.email_renderer import render_template_string
    from app.services.email_templates import DEFAULT_TEMPLATE_BY_KEY
    row = user(db, "weekly")
    data = snapshot()
    data["items"][0]["company_name"] = '<script>alert("test")</script>'
    monkeypatch.setattr(service, "build_top_stocks_response", lambda *a, **k: data)
    digest = service.build_top_ideas_digest(db, row)
    template = DEFAULT_TEMPLATE_BY_KEY[service.TEMPLATE]
    rendered = render_template_string(template["body_html"], digest.context, template["variables"], html=True)
    assert "<script>" not in rendered
    assert "&lt;script&gt;" in rendered
    assert "Turn off or manage these emails" in rendered
    assert "{{" not in rendered
