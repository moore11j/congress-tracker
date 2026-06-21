from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from starlette.requests import Request

from app.auth import SESSION_COOKIE_NAME, sign_session_payload
from app.db import Base
from app.models import Event, GovernmentContractAction, MonitoringAlert, Security, UserAccount, Watchlist, WatchlistItem
from app.routers.events import list_events, list_watchlist_events


def _session():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    Base.metadata.create_all(
        engine,
        tables=[
            Security.__table__,
            Event.__table__,
            GovernmentContractAction.__table__,
            MonitoringAlert.__table__,
            UserAccount.__table__,
            Watchlist.__table__,
            WatchlistItem.__table__,
        ],
    )
    return Session()


def _request_for_user(user: UserAccount) -> Request:
    token = sign_session_payload({"uid": user.id, "email": user.email})
    return Request(
        {"type": "http", "method": "GET", "path": "/", "headers": [(b"cookie", f"{SESSION_COOKIE_NAME}={token}".encode())]}
    )


def _event(
    *,
    symbol: str,
    transaction_date: str,
    filing_date: str,
    event_date: str,
    event_id: int | None = None,
) -> Event:
    event_dt = datetime.fromisoformat(f"{event_date}T12:00:00+00:00")
    return Event(
        id=event_id,
        event_type="insider_trade",
        ts=event_dt,
        event_date=event_dt,
        symbol=symbol,
        source="test",
        impact_score=0,
        member_name="Parekh Kevan",
        trade_type="sale",
        amount_min=1000,
        amount_max=2000,
        created_at=event_dt,
        payload_json=json.dumps(
            {
                "symbol": symbol,
                "company_name": "APPLE INC",
                "transaction_date": transaction_date,
                "filing_date": filing_date,
                "insider_name": "Parekh Kevan",
            }
        ),
    )


def _congress_event(
    *,
    symbol: str,
    trade_date: str,
    report_date: str,
    event_id: int | None = None,
) -> Event:
    report_dt = datetime.fromisoformat(f"{report_date}T00:00:00+00:00")
    return Event(
        id=event_id,
        event_type="congress_trade",
        ts=report_dt,
        event_date=report_dt,
        symbol=symbol,
        source="house_fmp",
        impact_score=0,
        member_name="Bill Keating",
        member_bioguide_id="FMP_HOUSE_MA09",
        chamber="house",
        party="Democrat",
        trade_type="purchase",
        transaction_type="purchase",
        amount_min=1001,
        amount_max=15000,
        created_at=report_dt,
        payload_json=json.dumps(
            {
                "symbol": symbol,
                "trade_date": trade_date,
                "report_date": report_date,
                "filing_date": report_date,
                "member": {"name": "Bill Keating", "chamber": "house"},
            }
        ),
    )


def test_watchlist_recent_activity_filters_by_filing_date_not_transaction_date(monkeypatch):
    db = _session()
    try:
        monkeypatch.setattr("app.routers.events.get_current_prices_meta_db", lambda *_args, **_kwargs: {})
        monkeypatch.setattr("app.routers.events.get_eod_close", lambda *_args, **_kwargs: None)
        monkeypatch.setattr("app.routers.events.get_confirmation_metrics_for_symbols", lambda *_args, **_kwargs: {})

        user = UserAccount(email="owner@example.com", role="user", entitlement_tier="free")
        security = Security(symbol="AAPL", name="APPLE INC", asset_class="stock", sector=None)
        db.add_all([user, security])
        db.flush()
        watchlist = Watchlist(name="Core", owner_user_id=user.id)
        db.add(watchlist)
        db.flush()
        db.add(WatchlistItem(watchlist_id=watchlist.id, security_id=security.id))
        db.add_all(
            [
                _event(
                    event_id=1,
                    symbol="AAPL",
                    transaction_date="2026-04-23",
                    filing_date="2026-04-27",
                    event_date="2026-04-23",
                ),
                _event(
                    event_id=2,
                    symbol="AAPL",
                    transaction_date="2026-04-29",
                    filing_date="2026-04-20",
                    event_date="2026-04-29",
                ),
            ]
        )
        db.commit()

        page = list_watchlist_events(
            watchlist.id,
            _request_for_user(user),
            db,
            since=datetime(2026, 4, 24, tzinfo=timezone.utc).isoformat(),
            limit=10,
        )

        assert [item.id for item in page.items] == [1]
        assert page.items[0].payload["filing_date"] == "2026-04-27"
        assert page.items[0].payload["transaction_date"] == "2026-04-23"
    finally:
        db.close()


def test_feed_query_returns_newly_reported_congress_trade_with_older_trade_date(monkeypatch):
    db = _session()
    try:
        monkeypatch.setattr("app.routers.events.get_current_prices_meta_db", lambda *_args, **_kwargs: {})
        monkeypatch.setattr("app.routers.events.get_eod_close", lambda *_args, **_kwargs: None)
        monkeypatch.setattr("app.routers.events.get_confirmation_metrics_for_symbols", lambda *_args, **_kwargs: {})

        today = datetime.now(timezone.utc).date()
        old_trade_date = today - timedelta(days=90)
        db.add(
            _congress_event(
                event_id=31,
                symbol="JPM",
                trade_date=old_trade_date.isoformat(),
                report_date=today.isoformat(),
            )
        )
        db.commit()

        page = list_events(
            request=None,
            db=db,
            mode="congress",
            member="Bill Keating",
            recent_days=7,
            limit=10,
            enrich_prices=False,
        )

        assert [item.id for item in page.items] == [31]
        assert page.items[0].payload["trade_date"] == old_trade_date.isoformat()
        assert page.items[0].payload["report_date"] == today.isoformat()
    finally:
        db.close()


def test_watchlist_recent_activity_includes_same_event_in_thirty_day_window(monkeypatch):
    db = _session()
    try:
        monkeypatch.setattr("app.routers.events.get_current_prices_meta_db", lambda *_args, **_kwargs: {})
        monkeypatch.setattr("app.routers.events.get_eod_close", lambda *_args, **_kwargs: None)
        monkeypatch.setattr("app.routers.events.get_confirmation_metrics_for_symbols", lambda *_args, **_kwargs: {})

        user = UserAccount(email="owner@example.com", role="user", entitlement_tier="free")
        security = Security(symbol="AAPL", name="APPLE INC", asset_class="stock", sector=None)
        db.add_all([user, security])
        db.flush()
        watchlist = Watchlist(name="Core", owner_user_id=user.id)
        db.add(watchlist)
        db.flush()
        db.add(WatchlistItem(watchlist_id=watchlist.id, security_id=security.id))
        db.add(
            _event(
                symbol="AAPL",
                transaction_date="2026-04-23",
                filing_date="2026-04-27",
                event_date="2026-04-23",
            )
        )
        db.commit()

        page = list_watchlist_events(
            watchlist.id,
            _request_for_user(user),
            db,
            since=datetime(2026, 4, 1, tzinfo=timezone.utc).isoformat(),
            limit=10,
        )

        assert len(page.items) == 1
        assert page.items[0].id == 1
    finally:
        db.close()


def test_watchlist_recent_activity_applies_recent_window_even_when_showing_unread_only(monkeypatch):
    db = _session()
    try:
        monkeypatch.setattr("app.routers.events.get_current_prices_meta_db", lambda *_args, **_kwargs: {})
        monkeypatch.setattr("app.routers.events.get_eod_close", lambda *_args, **_kwargs: None)
        monkeypatch.setattr("app.routers.events.get_confirmation_metrics_for_symbols", lambda *_args, **_kwargs: {})

        user = UserAccount(email="owner@example.com", role="user", entitlement_tier="free")
        security = Security(symbol="AAPL", name="APPLE INC", asset_class="stock", sector=None)
        db.add_all([user, security])
        db.flush()
        watchlist = Watchlist(name="Core", owner_user_id=user.id)
        db.add(watchlist)
        db.flush()
        db.add(WatchlistItem(watchlist_id=watchlist.id, security_id=security.id))

        now = datetime.now(timezone.utc)
        recent_trade = _event(
            event_id=11,
            symbol="AAPL",
            transaction_date=(now.date()).isoformat(),
            filing_date=(now.date()).isoformat(),
            event_date=(now.date()).isoformat(),
        )
        older_day = now.date() - timedelta(days=21)
        older_trade = _event(
            event_id=12,
            symbol="AAPL",
            transaction_date=older_day.isoformat(),
            filing_date=older_day.isoformat(),
            event_date=older_day.isoformat(),
        )
        db.add_all([recent_trade, older_trade])
        db.flush()
        db.add_all(
            [
                MonitoringAlert(
                    user_id=user.id,
                    source_type="watchlist",
                    source_id=str(watchlist.id),
                    source_name=watchlist.name,
                    event_id=recent_trade.id,
                    alert_type="watchlist_activity",
                    symbol="AAPL",
                    title="Recent alert",
                    body=None,
                    payload_json="{}",
                    event_created_at=recent_trade.created_at,
                    read_at=None,
                    dismissed_at=None,
                ),
                MonitoringAlert(
                    user_id=user.id,
                    source_type="watchlist",
                    source_id=str(watchlist.id),
                    source_name=watchlist.name,
                    event_id=older_trade.id,
                    alert_type="watchlist_activity",
                    symbol="AAPL",
                    title="Older alert",
                    body=None,
                    payload_json="{}",
                    event_created_at=older_trade.created_at,
                    read_at=None,
                    dismissed_at=None,
                ),
            ]
        )
        db.commit()

        page = list_watchlist_events(
            watchlist.id,
            _request_for_user(user),
            db,
            recent_days=7,
            unread_only=True,
            limit=10,
        )

        assert [item.id for item in page.items] == [11]
    finally:
        db.close()
