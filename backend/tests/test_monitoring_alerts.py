from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine, event as sqlalchemy_event
from sqlalchemy.orm import sessionmaker
from starlette.requests import Request

from app.auth import SESSION_COOKIE_NAME, sign_session_payload
from app.db import Base
from app.main import (
    add_to_watchlist,
    get_monitoring_inbox,
    get_monitoring_unread_count,
    get_watchlist,
    mark_monitoring_items_read,
    mark_monitoring_items_unread,
    dismiss_monitoring_items,
    list_watchlists,
    mark_monitoring_alert_read,
    mark_monitoring_alert_unread,
    mark_monitoring_source_read,
    mark_monitoring_source_unread,
    stop_monitoring_source,
)
from app.models import (
    AppSetting,
    Event,
    FeatureGate,
    MonitoringAlert,
    MonitoringSourcePreference,
    NotificationSubscription,
    PlanLimit,
    PlanPrice,
    SavedScreen,
    Security,
    StrategyDefinition,
    StrategySubscription,
    UserAccount,
    Watchlist,
    WatchlistItem,
    WatchlistViewState,
)
from app.services.monitoring_alerts import refresh_watchlist_alerts, unread_count, watchlist_unread_count
from app.services.monitoring_titles import build_monitoring_event_title


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
            MonitoringSourcePreference.__table__,
            NotificationSubscription.__table__,
            SavedScreen.__table__,
            AppSetting.__table__,
            FeatureGate.__table__,
            PlanLimit.__table__,
            PlanPrice.__table__,
            StrategyDefinition.__table__,
            StrategySubscription.__table__,
        ],
    )
    return Session()


def _request_for_user(user: UserAccount) -> Request:
    token = sign_session_payload({"uid": user.id, "email": user.email})
    return Request(
        {"type": "http", "method": "POST", "path": "/", "headers": [(b"cookie", f"{SESSION_COOKIE_NAME}={token}".encode())]}
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


def test_watchlist_member_target_and_ticker_match_same_event_once():
    db = _session()
    try:
        user, watchlist, now = _seed_watchlist(db)
        db.add(
            WatchlistItem(
                watchlist_id=watchlist.id,
                target_type="member",
                target_value="P000197",
                target_label="Nancy Pelosi",
            )
        )
        db.add(
            Event(
                event_type="congress_trade",
                ts=now - timedelta(minutes=5),
                event_date=now - timedelta(minutes=5),
                created_at=now - timedelta(minutes=5),
                symbol="AAPL",
                source="congress",
                member_name="Nancy Pelosi",
                member_bioguide_id="P000197",
                trade_type="purchase",
                payload_json=json.dumps({"member": "Nancy Pelosi", "transaction_date": "2026-06-01"}),
                impact_score=0,
            )
        )
        db.commit()

        assert refresh_watchlist_alerts(db, user_id=user.id, watchlist=watchlist) == 1
        db.commit()
        assert refresh_watchlist_alerts(db, user_id=user.id, watchlist=watchlist) == 0
        db.commit()
        assert db.query(MonitoringAlert).count() == 1
    finally:
        db.close()


def test_watchlist_insider_target_matches_reporting_cik_without_ticker_watch():
    db = _session()
    try:
        user, watchlist, now = _seed_watchlist(db)
        db.query(WatchlistItem).delete()
        db.add(
            WatchlistItem(
                watchlist_id=watchlist.id,
                target_type="insider",
                target_value="0000320193",
                target_label="Apple Insider",
            )
        )
        db.add(
            Event(
                event_type="insider_trade",
                ts=now - timedelta(minutes=5),
                event_date=now - timedelta(minutes=5),
                created_at=now - timedelta(minutes=5),
                symbol="MSFT",
                source="insider",
                trade_type="sale",
                payload_json=json.dumps({"reporting_cik": "320193", "insider_name": "Apple Insider", "trade_date": "2026-06-01"}),
                impact_score=0,
            )
        )
        db.commit()

        assert refresh_watchlist_alerts(db, user_id=user.id, watchlist=watchlist) == 1
        db.commit()
        alert = db.query(MonitoringAlert).one()
        assert alert.symbol == "MSFT"
    finally:
        db.close()


def test_non_ticker_watchlist_targets_do_not_require_security_id():
    db = _session()
    try:
        user, watchlist, _now = _seed_watchlist(db)
        request = _request_for_user(user)
        user.entitlement_tier = "premium"
        db.commit()

        add_to_watchlist(
            watchlist.id,
            request,
            target_type="member",
            target_value="P000197",
            target_label="Nancy Pelosi",
            db=db,
        )
        add_to_watchlist(
            watchlist.id,
            request,
            target_type="insider",
            target_value="0000320193",
            target_label="Apple Insider",
            db=db,
        )
        add_to_watchlist(
            watchlist.id,
            request,
            target_type="department",
            target_value="Department of Defense",
            target_label="Department of Defense",
            db=db,
        )

        user.entitlement_tier = "pro"
        db.commit()
        add_to_watchlist(
            watchlist.id,
            request,
            target_type="institution",
            target_value="0001067983",
            target_label="Berkshire Hathaway",
            db=db,
        )

        rows = (
            db.query(WatchlistItem)
            .filter(WatchlistItem.watchlist_id == watchlist.id)
            .filter(WatchlistItem.target_type.in_(["member", "insider", "department", "institution"]))
            .order_by(WatchlistItem.target_type.asc())
            .all()
        )
        assert [(row.target_type, row.security_id) for row in rows] == [
            ("department", None),
            ("insider", None),
            ("institution", None),
            ("member", None),
        ]
        assert {row.target_type: row.target_value for row in rows} == {
            "department": "Department of Defense",
            "insider": "0000320193",
            "institution": "0001067983",
            "member": "P000197",
        }
    finally:
        db.close()


def test_member_insider_department_watchlist_targets_are_premium_only_and_limited():
    db = _session()
    try:
        user, watchlist, _now = _seed_watchlist(db)
        request = _request_for_user(user)

        try:
            add_to_watchlist(
                watchlist.id,
                request,
                target_type="member",
                target_value="P000197",
                target_label="Nancy Pelosi",
                db=db,
            )
            raise AssertionError("Expected free member follow to be blocked")
        except Exception as exc:
            assert getattr(exc, "status_code", None) == 402
            assert exc.detail["feature"] == "watchlist_people_departments"

        user.entitlement_tier = "premium"
        db.commit()
        for index in range(10):
            response = add_to_watchlist(
                watchlist.id,
                request,
                target_type="member",
                target_value=f"M{index:06d}",
                target_label=f"Member {index}",
                db=db,
            )
            assert response["status"] == "added"

        try:
            add_to_watchlist(
                watchlist.id,
                request,
                target_type="department",
                target_value="Department of Defense",
                target_label="Department of Defense",
                db=db,
            )
            raise AssertionError("Expected premium people/department follow cap")
        except Exception as exc:
            assert getattr(exc, "status_code", None) == 402
            assert exc.detail["feature"] == "watchlist_people_departments"
            assert exc.detail["limit"] == 10
    finally:
        db.close()


def test_institution_watchlist_targets_are_pro_only_and_limited():
    db = _session()
    try:
        user, watchlist, _now = _seed_watchlist(db)
        user.entitlement_tier = "premium"
        db.commit()
        request = _request_for_user(user)

        try:
            add_to_watchlist(
                watchlist.id,
                request,
                target_type="institution",
                target_value="0001067983",
                target_label="Berkshire Hathaway",
                db=db,
            )
            raise AssertionError("Expected premium institution follow to be blocked")
        except Exception as exc:
            assert getattr(exc, "status_code", None) == 402
            assert exc.detail["feature"] == "watchlist_institutions"

        user.entitlement_tier = "pro"
        db.commit()
        for index in range(25):
            response = add_to_watchlist(
                watchlist.id,
                request,
                target_type="institution",
                target_value=str(1000 + index),
                target_label=f"Institution {index}",
                db=db,
            )
            assert response["status"] == "added"

        try:
            add_to_watchlist(
                watchlist.id,
                request,
                target_type="institution",
                target_value="9999",
                target_label="Institution 26",
                db=db,
            )
            raise AssertionError("Expected pro institution follow cap")
        except Exception as exc:
            assert getattr(exc, "status_code", None) == 402
            assert exc.detail["feature"] == "watchlist_institutions"
            assert exc.detail["limit"] == 25
    finally:
        db.close()


def test_get_watchlist_does_not_refresh_alerts_inline():
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

        response = get_watchlist(watchlist.id, _request_for_user(user), None, db)

        assert response["watchlist_id"] == watchlist.id
        assert response["tickers"] == [{"symbol": "AAPL", "name": "Apple"}]
        assert response["unseen_count"] == 1
        assert db.query(MonitoringAlert).count() == 0
    finally:
        db.close()


def test_refresh_watchlist_alerts_batches_existing_alert_lookup():
    db = _session()
    statements: list[str] = []

    def before_cursor_execute(_conn, _cursor, statement, _parameters, _context, _executemany):
        statements.append(statement)

    try:
        user, watchlist, now = _seed_watchlist(db)
        for index in range(6):
            db.add(
                Event(
                    event_type="insider_trade",
                    ts=now + timedelta(seconds=index),
                    event_date=now + timedelta(seconds=index),
                    created_at=now + timedelta(seconds=index),
                    symbol="AAPL",
                    source="insider",
                    trade_type="sale",
                    payload_json=json.dumps({"sequence": index}),
                    impact_score=0,
                )
            )
        db.commit()

        sqlalchemy_event.listen(db.get_bind(), "before_cursor_execute", before_cursor_execute)
        try:
            assert refresh_watchlist_alerts(db, user_id=user.id, watchlist=watchlist) == 6
        finally:
            sqlalchemy_event.remove(db.get_bind(), "before_cursor_execute", before_cursor_execute)

        duplicate_alert_selects = [
            statement
            for statement in statements
            if "FROM monitoring_alerts" in statement and "monitoring_alerts.event_id" in statement
        ]
        per_event_duplicate_selects = [
            statement
            for statement in statements
            if "SELECT monitoring_alerts.id" in statement and "FROM monitoring_alerts" in statement
        ]
        assert len(duplicate_alert_selects) == 1
        assert per_event_duplicate_selects == []
    finally:
        db.close()


def test_watchlist_insider_alert_title_uses_reporting_owner_and_normalized_side():
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
                source="fmp",
                trade_type=None,
                transaction_type="D",
                payload_json=json.dumps(
                    {
                        "reporting_owner_name": "Jane Insider",
                        "transactionType": "D",
                        "trade_date": "2026-05-14",
                    }
                ),
                impact_score=0,
            )
        )
        db.commit()

        assert refresh_watchlist_alerts(db, user_id=user.id, watchlist=watchlist) == 1
        db.commit()
        alert = db.query(MonitoringAlert).one()

        assert alert.title == "AAPL - Jane Insider - sale"
        assert alert.body == "New insider trade filed 2026-05-14."
    finally:
        db.close()


def test_congress_monitoring_title_format_remains_unchanged():
    now = datetime.now(timezone.utc)
    event = Event(
        event_type="congress_trade",
        ts=now,
        event_date=now,
        created_at=now,
        symbol="AAPL",
        source="congress",
        trade_type="purchase",
        member_name="John McGuire",
        payload_json=json.dumps({}),
        impact_score=0,
    )

    assert build_monitoring_event_title(event, {}) == "AAPL - John McGuire - purchase"


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
        assert response["counts"]["total_unread"] == 0
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
        summaries = list_watchlists(request, db)
        assert summaries[0]["unseen_count"] == 2
        assert summaries[0]["symbols"] == ["AAPL"]
    finally:
        db.close()


def test_list_watchlists_does_not_refresh_monitoring_alerts():
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

        summaries = list_watchlists(_request_for_user(user), db)

        assert summaries[0]["name"] == "Jarod's watchlist"
        assert summaries[0]["symbols"] == ["AAPL"]
        assert summaries[0]["unseen_count"] == 1
        assert db.query(MonitoringAlert).count() == 0
    finally:
        db.close()


def test_institutional_watchlist_alerts_are_pro_gated():
    db = _session()
    try:
        user, watchlist, now = _seed_watchlist(db)
        db.add(
            Event(
                event_type="institutional_accumulation",
                ts=now,
                event_date=now,
                created_at=now,
                symbol="AAPL",
                source="institutional_activity",
                member_name="Blue Ridge Capital",
                trade_type="Accumulation",
                amount_min=10_000_000,
                amount_max=10_000_000,
                payload_json=json.dumps(
                    {
                        "holder_name": "Blue Ridge Capital",
                        "filing_date": now.date().isoformat(),
                        "reported_value_usd": 10_000_000,
                    }
                ),
                impact_score=90,
            )
        )
        db.commit()

        request = _request_for_user(user)
        assert refresh_watchlist_alerts(db, user_id=user.id, watchlist=watchlist) == 0
        assert watchlist_unread_count(db, watchlist.id, user_id=user.id) == 0
        assert get_monitoring_unread_count(request, db)["unread_watchlist_updates"] == 0
        assert get_monitoring_inbox(request, db)["items"] == []
        assert list_watchlists(request, db)[0]["unseen_count"] == 0

        user.entitlement_tier = "pro"
        db.commit()
        assert refresh_watchlist_alerts(db, user_id=user.id, watchlist=watchlist, force_lookback=True) == 1
        db.commit()
        alert = db.query(MonitoringAlert).one()
        assert alert.alert_type == "institutional_accumulation"
        assert "Blue Ridge Capital" in alert.payload_json
        assert get_monitoring_inbox(request, db)["items"][0]["alert_type"] == "institutional_accumulation"
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
        assert item["score"] is None
        assert "smart_score" not in item["payload"].get("event", {})
    finally:
        db.close()


def test_free_watchlist_alerts_exclude_signal_events_and_redact_scores():
    db = _session()
    try:
        user, watchlist, now = _seed_watchlist(db)
        db.add_all(
            [
                Event(
                    event_type="signal",
                    ts=now,
                    event_date=now,
                    created_at=now,
                    symbol="AAPL",
                    source="signals",
                    trade_type="purchase",
                    payload_json=json.dumps({"smart_score": 91, "signal": {"direction": "bullish"}}),
                    impact_score=91,
                ),
                Event(
                    event_type="insider_trade",
                    ts=now + timedelta(seconds=1),
                    event_date=now + timedelta(seconds=1),
                    created_at=now + timedelta(seconds=1),
                    symbol="AAPL",
                    source="insider",
                    trade_type="purchase",
                    payload_json=json.dumps({"smart_score": 82, "confirmation_score": 77, "insider_name": "Jane Insider"}),
                    impact_score=82,
                ),
            ]
        )
        db.commit()

        assert refresh_watchlist_alerts(db, user_id=user.id, watchlist=watchlist) == 1
        db.commit()
        inbox = get_monitoring_inbox(_request_for_user(user), db)

        assert inbox["unread_total"] == 1
        item = inbox["items"][0]
        assert item["alert_type"] == "insider_trade"
        assert item["score"] is None
        event_payload = item["payload"]["event"]
        assert event_payload["insider_name"] == "Jane Insider"
        assert "smart_score" not in event_payload
        assert "confirmation_score" not in event_payload
    finally:
        db.close()


def test_inbox_includes_alert_only_saved_screen_source_in_counts():
    db = _session()
    try:
        user, _watchlist, now = _seed_watchlist(db)
        screen = SavedScreen(user_id=user.id, name="Bullish confirmation", params_json='{"confirmation_direction":"bullish"}')
        db.add(screen)
        db.flush()
        db.add(
            MonitoringAlert(
                user_id=user.id,
                source_type="saved_screen",
                source_id=str(screen.id),
                source_name=screen.name,
                event_id=101,
                alert_type="entered_screen",
                symbol="BSX",
                title="BSX entered your 'Bullish confirmation' screen",
                body="BSX moved from strong to mixed.",
                payload_json=json.dumps({"score": 87}),
                event_created_at=now,
            )
        )
        db.commit()

        inbox = get_monitoring_inbox(_request_for_user(user), db)

        assert inbox["unread_total"] == 1
        assert inbox["counts"]["saved_screen_unread"] == 1
        source = next(item for item in inbox["sources"] if item["id"] == str(screen.id) and item["type"] == "saved_screen")
        assert source["name"] == "Bullish confirmation"
        assert source["unread_count"] == 1
        assert source["new_count"] == 1
    finally:
        db.close()


def test_inbox_keeps_subscribed_saved_screen_source_with_no_alerts():
    db = _session()
    try:
        user, _watchlist, _now = _seed_watchlist(db)
        user.entitlement_tier = "premium"
        screen = SavedScreen(user_id=user.id, name="Bullish confirmation", params_json='{"confirmation_direction":"bullish"}')
        db.add_all(
            [
                screen,
                PlanLimit(tier="premium", feature_key="monitoring_sources", limit_value=1),
            ]
        )
        db.flush()
        subscription = NotificationSubscription(
            email=user.email,
            source_type="saved_view",
            source_id=f"saved-screen:{screen.id}",
            source_name=screen.name,
            source_payload_json=json.dumps({"id": f"saved-screen:{screen.id}", "surface": "screener"}),
            active=True,
            frequency="daily",
            only_if_new=True,
            alert_triggers_json=json.dumps(["saved_screen_entry"]),
        )
        db.add(subscription)
        db.commit()

        inbox = get_monitoring_inbox(_request_for_user(user), db)

        source = next(item for item in inbox["sources"] if item["id"] == str(screen.id) and item["type"] == "saved_screen")
        assert source["name"] == "Bullish confirmation"
        assert source["subscription_id"] == subscription.id
        assert source["unread_count"] == 0
        assert source["new_count"] == 0
    finally:
        db.close()


def test_inbox_includes_active_strategy_subscriptions_as_sources():
    db = _session()
    try:
        user, _watchlist, _now = _seed_watchlist(db)
        user.entitlement_tier = "premium"
        strategy = StrategyDefinition(
            slug="cross-source-confirmation",
            name="Cross-source Confirmation",
            category="walnut",
            status="published",
            access_tier="premium",
            methodology_version="v1",
        )
        db.add(strategy)
        db.flush()
        db.add(
            StrategySubscription(
                user_id=user.id,
                strategy_id=strategy.id,
                is_active=True,
                email_enabled=True,
                delivery_mode="realtime",
                event_types_json='["trade_added"]',
            )
        )
        db.commit()

        inbox = get_monitoring_inbox(_request_for_user(user), db)

        source = next(item for item in inbox["sources"] if item["id"] == "cross-source-confirmation" and item["type"] == "strategy")
        assert source["name"] == "Cross-source Confirmation"
        assert source["subscription_id"]
        assert source["unread_count"] == 0
        assert source["new_count"] == 0
    finally:
        db.close()


def test_premium_watchlist_alerts_keep_signal_scores():
    db = _session()
    try:
        user, watchlist, now = _seed_watchlist(db)
        user.entitlement_tier = "premium"
        db.add(
            Event(
                event_type="insider_trade",
                ts=now,
                event_date=now,
                created_at=now,
                symbol="AAPL",
                source="insider",
                trade_type="purchase",
                payload_json=json.dumps({"smart_score": 82, "confirmation_score": 77, "insider_name": "Jane Insider"}),
                impact_score=82,
            )
        )
        db.commit()

        assert refresh_watchlist_alerts(db, user_id=user.id, watchlist=watchlist) == 1
        db.commit()
        item = get_monitoring_inbox(_request_for_user(user), db)["items"][0]

        assert item["score"] == 82
        assert item["payload"]["event"]["smart_score"] == 82
        assert item["payload"]["event"]["confirmation_score"] == 77
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
        assert read_response["counts"]["total_unread"] == 1
        assert db.get(MonitoringAlert, alerts[0].id).read_at is not None
        assert db.get(MonitoringAlert, alerts[1].id).read_at is None
        assert list_watchlists(request, db)[0]["unread_count"] == 1

        unread_response = mark_monitoring_items_unread(_ItemsPayload([alerts[0].id]), request, db)
        assert unread_response["marked_unread"] == 1
        assert unread_response["unread_count"] == 2
        assert unread_response["counts"]["total_unread"] == 2
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


def test_bulk_dismiss_selected_items_removes_them_from_inbox_and_counts():
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
        response = dismiss_monitoring_items(_ItemsPayload([alerts[0].id]), request, db)
        inbox = get_monitoring_inbox(request, db)

        assert response["dismissed"] == 1
        assert response["unread_count"] == 1
        assert len(inbox["items"]) == 1
        assert inbox["items"][0]["id"] == alerts[1].id
        assert db.get(MonitoringAlert, alerts[0].id).dismissed_at is not None
    finally:
        db.close()


def test_stop_monitoring_keeps_watchlist_and_disables_its_subscription():
    db = _session()
    try:
        user, watchlist, _now = _seed_watchlist(db)
        db.add(
            NotificationSubscription(
                email=user.email,
                source_type="watchlist",
                source_id=str(watchlist.id),
                source_name=watchlist.name,
                frequency="daily",
                active=True,
                alert_triggers_json="[]",
            )
        )
        db.commit()

        stop_monitoring_source("watchlist", str(watchlist.id), _request_for_user(user), db)

        preference = db.query(MonitoringSourcePreference).one()
        assert db.get(Watchlist, watchlist.id) is not None
        assert preference.source_type == "watchlist"
        assert preference.source_id == str(watchlist.id)
        assert preference.is_monitored is False
        assert db.query(NotificationSubscription).count() == 0
    finally:
        db.close()
