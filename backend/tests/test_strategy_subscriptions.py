from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from app.db import Base, ensure_strategy_storage_schema
from app.models import StrategyDefinition, StrategyEvent, StrategyEventDelivery, UserAccount
from app.services.strategy_subscriptions import (
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
