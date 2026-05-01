from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from starlette.requests import Request

from app.auth import sign_session_payload
from app.db import Base
from app.main import get_monitoring_unread_count, mark_monitoring_source_read
from app.models import (
    AppSetting,
    Event,
    FeatureGate,
    MonitoringAlert,
    PlanLimit,
    PlanPrice,
    Security,
    UserAccount,
    Watchlist,
    WatchlistItem,
    WatchlistViewState,
)
from app.services.monitoring_alerts import refresh_watchlist_alerts, unread_count


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
            AppSetting.__table__,
            FeatureGate.__table__,
            PlanLimit.__table__,
            PlanPrice.__table__,
        ],
    )
    return Session()


def _request_for_user(user: UserAccount) -> Request:
    token = sign_session_payload({"uid": user.id, "email": user.email})
    return Request(
        {"type": "http", "method": "POST", "path": "/", "headers": [(b"authorization", f"Bearer {token}".encode())]}
    )


def _seed_watchlist(db):
    now = datetime.now(timezone.utc)
    user = UserAccount(email="jarod@example.com", name="Jarod Moore", role="user", entitlement_tier="free")
    aapl = Security(symbol="AAPL", name="Apple", asset_class="stock", sector=None)
    watchlist = Watchlist(name="Jarod's watchlist", owner_user_id=1)
    db.add_all([user, aapl])
    db.flush()
    watchlist.owner_user_id = user.id
    db.add(watchlist)
    db.flush()
    db.add(WatchlistItem(watchlist_id=watchlist.id, security_id=aapl.id))
    db.add(WatchlistViewState(watchlist_id=watchlist.id, last_seen_at=now - timedelta(hours=2)))
    return user, watchlist, now


def test_watchlist_alert_uses_created_at_not_old_trade_date_and_dedupes():
    db = _session()
    try:
        user, watchlist, now = _seed_watchlist(db)
        db.add(
            Event(
                event_type="insider_trade",
                ts=now - timedelta(days=4),
                event_date=now - timedelta(days=4),
                created_at=now - timedelta(minutes=10),
                symbol="AAPL",
                source="insider",
                trade_type="sale",
                payload_json=json.dumps({"insider_name": "Parekh Kevan", "trade_date": "2026-04-27"}),
                impact_score=0,
            )
        )
        db.commit()

        assert refresh_watchlist_alerts(db, user_id=user.id, watchlist=watchlist) == 1
        db.commit()
        assert unread_count(db, user_id=user.id) == 1
        assert refresh_watchlist_alerts(db, user_id=user.id, watchlist=watchlist) == 0
        db.commit()
        assert db.query(MonitoringAlert).count() == 1
    finally:
        db.close()


def test_mark_source_read_clears_unread_count_and_endpoint_reports_count():
    db = _session()
    try:
        user, watchlist, now = _seed_watchlist(db)
        db.add(
            Event(
                event_type="insider_trade",
                ts=now,
                event_date=now,
                created_at=now,
                symbol="AAPL",
                source="insider",
                trade_type="sale",
                payload_json=json.dumps({}),
                impact_score=0,
            )
        )
        db.commit()
        refresh_watchlist_alerts(db, user_id=user.id, watchlist=watchlist)
        db.commit()

        request = _request_for_user(user)
        assert get_monitoring_unread_count(request, db)["unread_count"] == 1
        response = mark_monitoring_source_read(str(watchlist.id), request, db)
        assert response["unread_count"] == 0
        assert db.query(MonitoringAlert).filter(MonitoringAlert.read_at.is_(None)).count() == 0
    finally:
        db.close()
