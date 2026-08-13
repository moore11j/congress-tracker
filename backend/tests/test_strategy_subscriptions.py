from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from app.db import Base, ensure_strategy_storage_schema
from app.models import StrategyDefinition, StrategyEvent, StrategyEventDelivery, StrategyVersion, UserAccount
from app.services import strategy_subscriptions
from app.services.strategy_subscriptions import (
    process_pending_strategy_event_deliveries,
    queue_strategy_event_deliveries,
    unsubscribe_strategy,
    upsert_strategy_subscription,
)


def _session():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=engine)
    ensure_strategy_storage_schema(engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False)


def _strategy_and_user(db):
    strategy = StrategyDefinition(
        slug="followable-test",
        name="Followable Test",
        category="walnut",
        status="published",
        access_tier="premium",
        methodology_version="v1",
    )
    user = UserAccount(email="subscriber@example.com", entitlement_tier="premium")
    db.add_all([strategy, user])
    db.commit()
    db.add(StrategyVersion(strategy_id=strategy.id, version=1, status="active"))
    db.commit()
    return strategy, user


def _event(db, strategy_id: int, *, key: str, created_at: datetime | None = None):
    event = StrategyEvent(
        strategy_id=strategy_id,
        strategy_version_id=1,
        event_type="trade_added",
        occurred_at=datetime.now(timezone.utc),
        dedupe_key=key,
        payload_json="{}",
        created_at=created_at or datetime.now(timezone.utc),
    )
    db.add(event)
    db.commit()
    db.refresh(event)
    return event


def test_strategy_follow_queue_is_idempotent_and_preserves_unsubscribe():
    SessionLocal = _session()
    db = SessionLocal()
    try:
        strategy, user = _strategy_and_user(db)
        subscription = upsert_strategy_subscription(
            db,
            user_id=user.id,
            slug=strategy.slug,
            email_enabled=True,
            delivery_mode="realtime",
            event_types=["trade_added", "rebalance_completed"],
        )
        assert subscription["isActive"] is True
        assert subscription["deliveryMode"] == "daily"
        event = _event(db, strategy.id, key="event-after-subscription")

        assert queue_strategy_event_deliveries(db, events=[event]) == {"queued": 1, "skipped": 0}
        assert queue_strategy_event_deliveries(db, events=[event]) == {"queued": 0, "skipped": 1}
        delivery = db.execute(select(StrategyEventDelivery)).scalar_one()
        assert delivery.status == "pending"
        assert delivery.attempts == 0

        unsubscribe_strategy(db, user_id=user.id, slug=strategy.slug)
        later = _event(db, strategy.id, key="event-after-unsubscribe")
        assert queue_strategy_event_deliveries(db, events=[later]) == {"queued": 0, "skipped": 0}
        assert len(db.execute(select(StrategyEventDelivery)).scalars().all()) == 1
    finally:
        db.close()


def test_strategy_follow_does_not_queue_events_that_predate_the_subscription():
    SessionLocal = _session()
    db = SessionLocal()
    try:
        strategy, user = _strategy_and_user(db)
        earlier = _event(
            db,
            strategy.id,
            key="event-before-subscription",
            created_at=datetime.now(timezone.utc) - timedelta(minutes=5),
        )
        upsert_strategy_subscription(
            db,
            user_id=user.id,
            slug=strategy.slug,
            email_enabled=True,
            delivery_mode="realtime",
            event_types=["trade_added"],
        )
        assert queue_strategy_event_deliveries(db, events=[earlier]) == {"queued": 0, "skipped": 0}
        assert db.execute(select(StrategyEventDelivery)).scalars().all() == []
    finally:
        db.close()


def test_strategy_delivery_queue_skips_downgraded_subscribers_without_deleting_subscription():
    SessionLocal = _session()
    db = SessionLocal()
    try:
        strategy, user = _strategy_and_user(db)
        upsert_strategy_subscription(db, user_id=user.id, slug=strategy.slug, email_enabled=True, delivery_mode="realtime", event_types=["trade_added"])
        user.entitlement_tier = "free"
        db.commit()

        event = _event(db, strategy.id, key="event-after-downgrade")
        assert queue_strategy_event_deliveries(db, events=[event]) == {"queued": 0, "skipped": 1}
        assert db.execute(select(StrategyEventDelivery)).scalars().all() == []
    finally:
        db.close()


def test_strategy_delivery_worker_rechecks_entitlement_after_queueing(monkeypatch):
    SessionLocal = _session()
    db = SessionLocal()
    calls: list[dict] = []
    try:
        strategy, user = _strategy_and_user(db)
        upsert_strategy_subscription(db, user_id=user.id, slug=strategy.slug, email_enabled=True, delivery_mode="realtime", event_types=["trade_added"])
        event = _event(db, strategy.id, key="delivery-after-downgrade")
        assert queue_strategy_event_deliveries(db, events=[event]) == {"queued": 1, "skipped": 0}

        user.entitlement_tier = "free"
        db.commit()
        monkeypatch.setenv("STRATEGY_EMAIL_DELIVERY_ENABLED", "true")
        monkeypatch.setattr(strategy_subscriptions, "email_delivery_enabled", lambda: True)
        monkeypatch.setattr(strategy_subscriptions, "send_email", lambda *_args, **kwargs: calls.append(kwargs))

        assert process_pending_strategy_event_deliveries(db, now=datetime.now(timezone.utc) + timedelta(minutes=1))["skipped"] == 1
        assert calls == []
        assert db.execute(select(StrategyEventDelivery)).scalar_one().status == "skipped"
    finally:
        db.close()


def test_strategy_delivery_worker_retries_and_uses_safe_context(monkeypatch):
    SessionLocal = _session()
    db = SessionLocal()
    calls: list[dict] = []
    try:
        strategy, user = _strategy_and_user(db)
        upsert_strategy_subscription(db, user_id=user.id, slug=strategy.slug, email_enabled=True, delivery_mode="realtime", event_types=["trade_added"])
        event = _event(db, strategy.id, key="delivery-worker-event")
        queue_strategy_event_deliveries(db, events=[event])
        monkeypatch.setenv("STRATEGY_EMAIL_DELIVERY_ENABLED", "true")
        monkeypatch.setattr(strategy_subscriptions, "email_delivery_enabled", lambda: True)

        def fake_send_email(_db, **kwargs):
            calls.append(kwargs)
            return {"status": "failed", "error": "temporary provider error"} if len(calls) == 1 else {"status": "sent", "provider_message_id": "provider-1"}

        monkeypatch.setattr(strategy_subscriptions, "send_email", fake_send_email)
        now = datetime(2026, 8, 9, 18, 0, tzinfo=timezone.utc)
        assert process_pending_strategy_event_deliveries(db, now=now)["retried"] == 1
        assert calls[0]["template_key"] == "alerts.strategy_event"
        assert calls[0]["context"]["symbol"] == "Portfolio-level update"
        assert calls[0]["context"]["event_label"] == "New position"
        assert "payload_json" not in calls[0]["context"]
        assert process_pending_strategy_event_deliveries(db, now=now + timedelta(seconds=61))["delivered"] == 1
        delivery = db.execute(select(StrategyEventDelivery)).scalar_one()
        assert delivery.status == "delivered"
        assert delivery.attempts == 2
    finally:
        db.close()
