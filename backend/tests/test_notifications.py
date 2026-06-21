from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from fastapi import HTTPException
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from starlette.requests import Request

from app.auth import SESSION_COOKIE_NAME, sign_session_payload
from app.db import Base
from app.models import Event, NotificationDelivery, NotificationSubscription, Security, UserAccount, Watchlist, WatchlistItem
from app.routers.notifications import (
    NotificationSubscriptionPayload,
    delete_notification_subscription,
    list_notification_deliveries,
    list_notification_subscriptions,
    put_notification_subscription,
    run_notification_digests,
)
from app.services.notifications import build_digest_for_subscription, create_digest_delivery, upsert_subscription


def _session():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    Base.metadata.create_all(engine)
    return Session()


def _user(db, email: str, *, role: str = "user", tier: str = "premium") -> UserAccount:
    user = UserAccount(email=email, role=role, entitlement_tier=tier)
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


def _request_for_user(user: UserAccount) -> Request:
    token = sign_session_payload({"uid": user.id, "email": user.email})
    return Request({"type": "http", "method": "GET", "path": "/", "headers": [(b"cookie", f"{SESSION_COOKIE_NAME}={token}".encode())]})


def _anonymous_request() -> Request:
    return Request({"type": "http", "method": "GET", "path": "/", "headers": []})


def _event(symbol: str, ts: datetime, amount_max: int, event_type: str = "congress_trade") -> Event:
    return Event(
        event_type=event_type,
        ts=ts,
        event_date=ts,
        symbol=symbol,
        source="test",
        payload_json=json.dumps({"symbol": symbol}),
        impact_score=0,
        member_name="Test Person",
        trade_type="purchase",
        amount_min=amount_max // 2,
        amount_max=amount_max,
    )


def test_watchlist_digest_uses_unseen_since_and_only_sends_new_items():
    db = _session()
    try:
        now = datetime.now(timezone.utc)
        watchlist = Watchlist(name="AI")
        security = Security(symbol="NVDA", name="Nvidia", asset_class="stock", sector=None)
        db.add_all([watchlist, security])
        db.flush()
        db.add(WatchlistItem(watchlist_id=watchlist.id, security_id=security.id))
        db.add_all(
            [
                _event("NVDA", now - timedelta(hours=3), 100_000),
                _event("NVDA", now - timedelta(minutes=20), 300_000),
            ]
        )
        db.commit()

        subscription = upsert_subscription(
            db,
            email="user@example.com",
            source_type="watchlist",
            source_id=str(watchlist.id),
            source_name=watchlist.name,
            source_payload={"unseen_since": (now - timedelta(hours=1)).isoformat()},
            frequency="daily",
            only_if_new=True,
            active=True,
            alert_triggers=[],
            min_smart_score=None,
            large_trade_amount=None,
        )

        items, alerts = build_digest_for_subscription(db, subscription)

        assert [item.event_id for item in items] == [2]
        assert alerts == []
    finally:
        db.close()


def test_saved_view_digest_can_fire_large_trade_alert():
    db = _session()
    try:
        now = datetime.now(timezone.utc)
        db.add_all(
            [
                _event("AAPL", now - timedelta(minutes=30), 500_000, "insider_trade"),
                _event("MSFT", now - timedelta(minutes=10), 50_000, "insider_trade"),
            ]
        )
        db.commit()

        subscription = upsert_subscription(
            db,
            email="user@example.com",
            source_type="saved_view",
            source_id="view-1",
            source_name="AAPL insiders",
            source_payload={
                "surface": "feed",
                "params": {"mode": "insider", "symbol": "AAPL"},
                "lastSeenAt": (now - timedelta(hours=1)).isoformat(),
            },
            frequency="daily",
            only_if_new=True,
            active=True,
            alert_triggers=["large_trade_threshold"],
            min_smart_score=None,
            large_trade_amount=250_000,
        )

        items, alerts = build_digest_for_subscription(db, subscription)

        assert len(items) == 1
        assert items[0].symbol == "AAPL"
        assert alerts[0][0] == "large_trade_threshold"
        assert alerts[0][1].amount_max == 500_000
    finally:
        db.close()


def test_only_if_new_creates_skipped_delivery_when_digest_is_empty():
    db = _session()
    try:
        now = datetime.now(timezone.utc)
        watchlist = Watchlist(name="Quiet")
        security = Security(symbol="MSFT", name="Microsoft", asset_class="stock", sector=None)
        db.add_all([watchlist, security])
        db.flush()
        db.add(WatchlistItem(watchlist_id=watchlist.id, security_id=security.id))
        db.add(_event("MSFT", now - timedelta(days=2), 100_000))
        db.commit()

        subscription = upsert_subscription(
            db,
            email="user@example.com",
            source_type="watchlist",
            source_id=str(watchlist.id),
            source_name=watchlist.name,
            source_payload={"unseen_since": (now - timedelta(hours=1)).isoformat()},
            frequency="daily",
            only_if_new=True,
            active=True,
            alert_triggers=[],
            min_smart_score=None,
            large_trade_amount=None,
        )

        delivery = create_digest_delivery(db, subscription)

        assert delivery.status == "skipped"
        assert delivery.items_count == 0
        assert db.query(NotificationDelivery).count() == 1
    finally:
        db.close()


def test_notification_subscription_endpoints_require_auth():
    db = _session()
    try:
        payload = NotificationSubscriptionPayload(
            email="reader@example.com",
            source_type="saved_view",
            source_id="view-1",
            source_name="View",
            only_if_new=True,
            active=True,
            alert_triggers=[],
        )
        for call in (
            lambda: list_notification_subscriptions(_anonymous_request(), db),
            lambda: put_notification_subscription(payload, _anonymous_request(), db),
            lambda: delete_notification_subscription(1, _anonymous_request(), db),
            lambda: list_notification_deliveries(_anonymous_request(), db),
            lambda: run_notification_digests(_anonymous_request(), db),
        ):
            try:
                call()
            except HTTPException as exc:
                assert exc.status_code == 401
            else:
                raise AssertionError("Expected authentication failure")
    finally:
        db.close()


def test_user_cannot_access_another_users_subscription_or_deliveries():
    db = _session()
    try:
        owner = _user(db, "owner@example.com")
        other = _user(db, "other@example.com")
        subscription = upsert_subscription(
            db,
            email=owner.email,
            source_type="saved_view",
            source_id="view-1",
            source_name="Owner view",
            source_payload={},
            frequency="daily",
            only_if_new=True,
            active=True,
            alert_triggers=[],
            min_smart_score=None,
            large_trade_amount=None,
        )
        db.add(
            NotificationDelivery(
                subscription_id=subscription.id,
                channel="email",
                status="queued",
                subject="Digest",
                body_text="Body",
                items_count=0,
                alerts_count=0,
            )
        )
        db.commit()

        try:
            delete_notification_subscription(subscription.id, _request_for_user(other), db)
        except HTTPException as exc:
            assert exc.status_code == 404
        else:
            raise AssertionError("Expected cross-user delete failure")

        own_list = list_notification_subscriptions(_request_for_user(owner), db)
        other_list = list_notification_subscriptions(_request_for_user(other), db)
        assert [item["id"] for item in own_list["items"]] == [subscription.id]
        assert other_list["items"] == []

        try:
            list_notification_deliveries(_request_for_user(owner), db)
        except HTTPException as exc:
            assert exc.status_code == 403
        else:
            raise AssertionError("Expected deliveries to require admin")

        try:
            run_notification_digests(_request_for_user(owner), db, send=False, limit=1)
        except HTTPException as exc:
            assert exc.status_code == 403
        else:
            raise AssertionError("Expected digest run to require admin")
    finally:
        db.close()


def test_admin_can_inspect_notifications_and_run_digest():
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        subscription = upsert_subscription(
            db,
            email="reader@example.com",
            source_type="saved_view",
            source_id="view-1",
            source_name="Reader view",
            source_payload={},
            frequency="daily",
            only_if_new=True,
            active=True,
            alert_triggers=[],
            min_smart_score=None,
            large_trade_amount=None,
        )
        db.add(
            NotificationDelivery(
                subscription_id=subscription.id,
                channel="email",
                status="queued",
                subject="Digest",
                body_text="Body",
                items_count=0,
                alerts_count=0,
            )
        )
        db.commit()

        listed = list_notification_subscriptions(_request_for_user(admin), db)
        deliveries = list_notification_deliveries(_request_for_user(admin), db, limit=25)
        digest = run_notification_digests(_request_for_user(admin), db, send=False, limit=1)

        assert [item["id"] for item in listed["items"]] == [subscription.id]
        assert len(deliveries["items"]) == 1
        assert "items" in digest
    finally:
        db.close()


def test_watchlist_subscription_requires_owned_watchlist():
    db = _session()
    try:
        owner = _user(db, "owner@example.com")
        other = _user(db, "other@example.com")
        watchlist = Watchlist(name="Owner list", owner_user_id=owner.id)
        db.add(watchlist)
        db.commit()
        db.refresh(watchlist)

        payload = NotificationSubscriptionPayload(
            source_type="watchlist",
            source_id=str(watchlist.id),
            source_name="Owner list",
            only_if_new=True,
            active=True,
            alert_triggers=[],
        )

        try:
            put_notification_subscription(payload, _request_for_user(other), db)
        except HTTPException as exc:
            assert exc.status_code == 404
        else:
            raise AssertionError("Expected watchlist ownership failure")

        saved = put_notification_subscription(payload, _request_for_user(owner), db)
        assert saved["email"] == owner.email
    finally:
        db.close()
