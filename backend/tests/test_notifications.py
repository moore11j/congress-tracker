from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.models import Event, NotificationDelivery, NotificationSubscription, Security, Watchlist, WatchlistItem
from app.services.notifications import build_digest_for_subscription, create_digest_delivery, upsert_subscription


def _session():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    Base.metadata.create_all(engine)
    return Session()


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
