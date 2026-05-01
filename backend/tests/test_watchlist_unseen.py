from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from starlette.requests import Request

from app.auth import sign_session_payload
from app.db import Base
from app.main import _watchlist_view_summary, mark_watchlist_seen
from app.models import Event, MonitoringAlert, Security, UserAccount, Watchlist, WatchlistItem, WatchlistViewState


def _session():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    Base.metadata.create_all(
        engine,
        tables=[
            Security.__table__,
            Event.__table__,
            UserAccount.__table__,
            Watchlist.__table__,
            WatchlistItem.__table__,
            WatchlistViewState.__table__,
            MonitoringAlert.__table__,
        ],
    )
    return Session()


def _user(db) -> UserAccount:
    user = UserAccount(email="owner@example.com", role="user", entitlement_tier="free")
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


def _request_for_user(user: UserAccount) -> Request:
    token = sign_session_payload({"uid": user.id, "email": user.email})
    return Request(
        {"type": "http", "method": "POST", "path": "/", "headers": [(b"authorization", f"Bearer {token}".encode())]}
    )


def test_watchlist_unseen_count_is_per_watchlist_and_uses_last_seen_checkpoint():
    db = _session()
    try:
        now = datetime.now(timezone.utc)
        aapl = Security(symbol="AAPL", name="Apple", asset_class="stock", sector=None)
        msft = Security(symbol="MSFT", name="Microsoft", asset_class="stock", sector=None)
        db.add_all([aapl, msft])
        db.flush()
        user = _user(db)

        first = Watchlist(name="AI", owner_user_id=user.id)
        second = Watchlist(name="Cloud", owner_user_id=user.id)
        db.add_all([first, second])
        db.flush()
        db.add_all(
            [
                WatchlistItem(watchlist_id=first.id, security_id=aapl.id),
                WatchlistItem(watchlist_id=second.id, security_id=msft.id),
                WatchlistViewState(watchlist_id=first.id, last_seen_at=now - timedelta(hours=2)),
                WatchlistViewState(watchlist_id=second.id, last_seen_at=now - timedelta(hours=2)),
            ]
        )

        db.add_all(
            [
                Event(
                    event_type="congress_trade",
                    ts=now - timedelta(hours=3),
                    event_date=now - timedelta(hours=3),
                    created_at=now - timedelta(hours=3),
                    symbol="AAPL",
                    source="test",
                    payload_json=json.dumps({}),
                    impact_score=0,
                ),
                Event(
                    event_type="insider_trade",
                    ts=now - timedelta(minutes=30),
                    event_date=now - timedelta(minutes=30),
                    created_at=now - timedelta(minutes=30),
                    symbol="AAPL",
                    source="test",
                    payload_json=json.dumps({}),
                    impact_score=0,
                ),
                Event(
                    event_type="insider_trade",
                    ts=now - timedelta(minutes=10),
                    event_date=now - timedelta(minutes=10),
                    created_at=now - timedelta(minutes=10),
                    symbol="MSFT",
                    source="test",
                    payload_json=json.dumps({}),
                    impact_score=0,
                ),
            ]
        )
        db.commit()

        assert _watchlist_view_summary(db, first.id)["unseen_count"] == 1
        assert _watchlist_view_summary(db, second.id)["unseen_count"] == 1

        mark_watchlist_seen(first.id, _request_for_user(user), db)

        assert _watchlist_view_summary(db, first.id)["unseen_count"] == 0
        assert _watchlist_view_summary(db, second.id)["unseen_count"] == 1
    finally:
        db.close()


def test_watchlist_without_prior_checkpoint_has_no_unseen_count():
    db = _session()
    try:
        watchlist = Watchlist(name="Fresh")
        security = Security(symbol="NVDA", name="Nvidia", asset_class="stock", sector=None)
        db.add_all([watchlist, security])
        db.flush()
        db.add(WatchlistItem(watchlist_id=watchlist.id, security_id=security.id))
        db.add(
            Event(
                event_type="insider_trade",
                ts=datetime.now(timezone.utc),
                event_date=datetime.now(timezone.utc),
                created_at=datetime.now(timezone.utc),
                symbol="NVDA",
                source="test",
                payload_json=json.dumps({}),
                impact_score=0,
            )
        )
        db.commit()

        summary = _watchlist_view_summary(db, watchlist.id)

        assert summary["unseen_count"] == 0
        assert summary["unseen_since"] is None
    finally:
        db.close()
