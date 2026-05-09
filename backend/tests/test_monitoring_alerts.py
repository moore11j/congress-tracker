from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from starlette.requests import Request

from app.auth import sign_session_payload
from app.db import Base
from app.main import (
    get_monitoring_inbox,
    get_monitoring_unread_count,
    mark_monitoring_items_read,
    mark_monitoring_items_unread,
    list_watchlists,
    mark_monitoring_alert_read,
    mark_monitoring_alert_unread,
    mark_monitoring_source_read,
    mark_monitoring_source_unread,
)
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
from app.services.monitoring_alerts import refresh_watchlist_alerts, unread_count, watchlist_unread_count


class _ItemsPayload:
    def __init__(self, item_ids):
        self.item_ids = item_ids


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
        assert get_monitoring_unread_count(request, db)["unread_sources_count"] == 1
        response = mark_monitoring_source_read(str(watchlist.id), request, db)
        assert response["unread_count"] == 0
        assert response["source_unread_count"] == 0
        assert db.query(MonitoringAlert).filter(MonitoringAlert.read_at.is_(None)).count() == 0
    finally:
        db.close()


def test_watchlist_monitoring_counts_share_checkpoint_without_existing_alerts():
    db = _session()
    try:
        user, watchlist, now = _seed_watchlist(db)
        db.add_all(
            [
                Event(
                    event_type="insider_trade",
                    ts=now - timedelta(days=4),
                    event_date=now - timedelta(days=4),
                    created_at=now - timedelta(minutes=30),
                    symbol="AAPL",
                    source="insider",
                    trade_type="sale",
                    payload_json=json.dumps({"filing_date": (now - timedelta(minutes=30)).isoformat()}),
                    impact_score=0,
                ),
                Event(
                    event_type="congress_trade",
                    ts=now - timedelta(days=3),
                    event_date=now - timedelta(days=3),
                    created_at=now - timedelta(minutes=20),
                    symbol="AAPL",
                    source="congress",
                    trade_type="purchase",
                    payload_json=json.dumps({"report_date": (now - timedelta(minutes=20)).isoformat()}),
                    impact_score=0,
                ),
                Event(
                    event_type="insider_trade",
                    ts=now - timedelta(days=5),
                    event_date=now - timedelta(days=5),
                    created_at=now - timedelta(days=5),
                    symbol="AAPL",
                    source="insider",
                    trade_type="sale",
                    payload_json=json.dumps({}),
                    impact_score=0,
                ),
            ]
        )
        db.commit()

        request = _request_for_user(user)

        assert watchlist_unread_count(db, watchlist.id) == 2
        assert get_monitoring_unread_count(request, db)["unread_watchlist_updates"] == 2
        inbox = get_monitoring_inbox(request, db)
        assert inbox["unread_total"] == 2
        assert inbox["sources"][0]["unread_count"] == 2
        assert list_watchlists(request, db)[0]["unseen_count"] == 2
    finally:
        db.close()


def test_mark_source_unread_restores_source_unread_count():
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
        mark_monitoring_source_read(str(watchlist.id), request, db)
        assert get_monitoring_unread_count(request, db)["unread_count"] == 0

        response = mark_monitoring_source_unread(str(watchlist.id), request, db)

        assert response["marked_unread"] == 1
        assert response["source_unread_count"] == 1
        assert response["unread_count"] == 1
        assert db.query(MonitoringAlert).filter(MonitoringAlert.read_at.is_(None)).count() == 1
    finally:
        db.close()


def test_mark_alert_read_and_unread_mutations_update_unread_count():
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
        alert = db.query(MonitoringAlert).one()

        request = _request_for_user(user)
        read_response = mark_monitoring_alert_read(alert.id, request, db)
        assert read_response["read"] is True
        assert read_response["unread_count"] == 0

        unread_response = mark_monitoring_alert_unread(alert.id, request, db)
        assert unread_response["read"] is False
        assert unread_response["unread_count"] == 1
    finally:
        db.close()


def test_inbox_returns_individual_items_with_stable_keys_and_states():
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
                payload_json=json.dumps({"smart_score": 82}),
                impact_score=0,
            )
        )
        db.commit()
        refresh_watchlist_alerts(db, user_id=user.id, watchlist=watchlist)
        db.commit()

        inbox = get_monitoring_inbox(_request_for_user(user), db)

        assert inbox["unread_total"] == 1
        assert len(inbox["items"]) == 1
        item = inbox["items"][0]
        assert item["id"]
        assert item["item_key"] == f"watchlist:{watchlist.id}:insider_trade:{item['event_id']}"
        assert item["source_name"] == "Jarod's watchlist"
        assert item["description"]
        assert item["timestamp"]
        assert item["is_unread"] is True
        assert item["is_read"] is False
    finally:
        db.close()


def test_bulk_mark_selected_items_read_and_unread_only_updates_selected():
    db = _session()
    try:
        user, watchlist, now = _seed_watchlist(db)
        db.add_all(
            [
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
                ),
                Event(
                    event_type="congress_trade",
                    ts=now + timedelta(seconds=1),
                    event_date=now + timedelta(seconds=1),
                    created_at=now + timedelta(seconds=1),
                    symbol="AAPL",
                    source="congress",
                    trade_type="purchase",
                    payload_json=json.dumps({}),
                    impact_score=0,
                ),
            ]
        )
        db.commit()
        refresh_watchlist_alerts(db, user_id=user.id, watchlist=watchlist)
        db.commit()
        alerts = db.query(MonitoringAlert).order_by(MonitoringAlert.id.asc()).all()

        request = _request_for_user(user)
        read_response = mark_monitoring_items_read(_ItemsPayload([alerts[0].id]), request, db)
        assert read_response["marked_read"] == 1
        assert read_response["unread_count"] == 1
        assert db.get(MonitoringAlert, alerts[0].id).read_at is not None
        assert db.get(MonitoringAlert, alerts[1].id).read_at is None
        assert list_watchlists(request, db)[0]["unread_count"] == 1

        unread_response = mark_monitoring_items_unread(_ItemsPayload([alerts[0].id]), request, db)
        assert unread_response["marked_unread"] == 1
        assert unread_response["unread_count"] == 2
        assert db.get(MonitoringAlert, alerts[0].id).read_at is None
        assert db.get(MonitoringAlert, alerts[1].id).read_at is None
    finally:
        db.close()


def test_bulk_item_mutation_does_not_cross_user_boundary():
    db = _session()
    try:
        user, watchlist, now = _seed_watchlist(db)
        other = UserAccount(email="other@example.com", name="Other User", role="user", entitlement_tier="free")
        db.add(other)
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
        alert = db.query(MonitoringAlert).one()

        response = mark_monitoring_items_read(_ItemsPayload([alert.id]), _request_for_user(other), db)

        assert response["marked_read"] == 0
        assert db.get(MonitoringAlert, alert.id).read_at is None
        assert unread_count(db, user_id=user.id) == 1
    finally:
        db.close()
