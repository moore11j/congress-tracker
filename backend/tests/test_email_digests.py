from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from starlette.requests import Request

from app.auth import sign_session_payload
from app.db import Base, ensure_email_notification_schema
from app.models import EmailDelivery, Event, NotificationSubscription, Security, UserAccount, Watchlist, WatchlistItem
from app.routers.accounts import AdminDigestSendTestPayload, admin_send_monitoring_digest_test
from app.services.email_digests import send_watchlist_activity_digest
from app.services.email_templates import seed_default_email_templates


def _session():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    ensure_email_notification_schema(engine)
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    db = Session()
    seed_default_email_templates(db)
    return db


def _request_for_user(user: UserAccount) -> Request:
    token = sign_session_payload({"uid": user.id, "email": user.email})
    return Request({"type": "http", "method": "GET", "path": "/", "headers": [(b"authorization", f"Bearer {token}".encode())]})


def _user(db, email: str, *, role: str = "user", watchlist_notifications: bool = True) -> UserAccount:
    user = UserAccount(
        email=email,
        first_name="Ada",
        role=role,
        entitlement_tier="premium",
        watchlist_activity_notifications=watchlist_notifications,
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


def _watchlist(db, user: UserAccount, *, active_subscription: bool = True, only_if_new: bool = True) -> Watchlist:
    watchlist = Watchlist(name=f"{user.id} AI", owner_user_id=user.id)
    security = Security(symbol="NVDA", name="Nvidia", asset_class="stock", sector=None)
    db.add_all([watchlist, security])
    db.flush()
    db.add(WatchlistItem(watchlist_id=watchlist.id, security_id=security.id))
    db.add(
        NotificationSubscription(
            email=user.email,
            source_type="watchlist",
            source_id=str(watchlist.id),
            source_name=watchlist.name,
            frequency="daily",
            only_if_new=only_if_new,
            active=active_subscription,
            alert_triggers_json="[]",
        )
    )
    db.commit()
    db.refresh(watchlist)
    return watchlist


def _event(db, symbol: str = "NVDA", *, ts: datetime | None = None) -> Event:
    now = ts or datetime.now(timezone.utc)
    event = Event(
        event_type="congress_trade",
        ts=now,
        event_date=now,
        symbol=symbol,
        source="test",
        impact_score=82,
        payload_json=json.dumps({"smart_score": 82}),
        member_name="Example Member",
        trade_type="purchase",
        amount_min=15_001,
        amount_max=50_000,
    )
    db.add(event)
    db.commit()
    db.refresh(event)
    return event


def test_watchlist_digest_skips_when_user_toggle_disabled():
    db = _session()
    try:
        user = _user(db, "toggle@example.com", watchlist_notifications=False)
        watchlist = _watchlist(db, user)
        _event(db)

        result = send_watchlist_activity_digest(db, user, watchlist, datetime.now(timezone.utc) - timedelta(days=1))

        assert result["status"] == "skipped"
        assert result["error"] == "user_alerts_disabled"
        row = db.execute(select(EmailDelivery)).scalar_one()
        assert row.template_key == "alerts.watchlist_activity"
    finally:
        db.close()


def test_watchlist_digest_skips_when_subscription_inactive_unless_forced():
    db = _session()
    try:
        user = _user(db, "inactive@example.com")
        watchlist = _watchlist(db, user, active_subscription=False)
        _event(db)

        result = send_watchlist_activity_digest(db, user, watchlist, datetime.now(timezone.utc) - timedelta(days=1))

        assert result["status"] == "skipped"
        assert result["error"] == "watchlist_digest_inactive"
    finally:
        db.close()


def test_watchlist_digest_only_new_logs_no_new_items():
    db = _session()
    try:
        user = _user(db, "nonew@example.com")
        watchlist = _watchlist(db, user, only_if_new=True)
        _event(db, ts=datetime.now(timezone.utc) - timedelta(days=5))

        result = send_watchlist_activity_digest(db, user, watchlist, datetime.now(timezone.utc) - timedelta(hours=1))

        assert result["status"] == "skipped"
        assert result["error"] == "no_new_items"
    finally:
        db.close()


def test_watchlist_digest_idempotency_prevents_duplicate_delivery_rows(monkeypatch):
    monkeypatch.setenv("EMAIL_PROVIDER", "postmark")
    monkeypatch.setenv("EMAIL_DELIVERY_ENABLED", "false")
    db = _session()
    try:
        user = _user(db, "idempotent@example.com")
        watchlist = _watchlist(db, user)
        _event(db)
        since = datetime.now(timezone.utc) - timedelta(days=1)

        first = send_watchlist_activity_digest(db, user, watchlist, since)
        second = send_watchlist_activity_digest(db, user, watchlist, since)

        assert first["id"] == second["id"]
        assert second["status"] == "skipped"
        assert second["error"] == "duplicate_window_already_sent"
        assert db.query(EmailDelivery).count() == 1
    finally:
        db.close()


def test_admin_monitoring_digest_endpoint_targets_one_send(monkeypatch):
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        user = _user(db, "reader@example.com")
        watchlist = _watchlist(db, user)
        calls = []

        def fake_send(db_arg, user_arg, watchlist_arg, since_arg, force=False):
            calls.append((user_arg.id, watchlist_arg.id, since_arg, force))
            return {
                "id": 123,
                "status": "log_only",
                "provider": "postmark",
                "provider_message_id": None,
                "template_key": "alerts.monitoring_digest",
                "category": "alerts",
                "to_email": user_arg.email,
                "error": None,
            }

        monkeypatch.setattr("app.routers.accounts.send_monitoring_digest", fake_send)
        result = admin_send_monitoring_digest_test(
            AdminDigestSendTestPayload(user_id=user.id, watchlist_id=watchlist.id, lookback_days=7, force=True),
            _request_for_user(admin),
            db,
        )

        assert result["template_key"] == "alerts.monitoring_digest"
        assert len(calls) == 1
        assert calls[0][0] == user.id
        assert calls[0][1] == watchlist.id
        assert calls[0][3] is True
    finally:
        db.close()
