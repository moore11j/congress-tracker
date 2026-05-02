from __future__ import annotations

import json
from datetime import datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from starlette.requests import Request

from app.auth import sign_session_payload
from app.db import Base
from app.models import Event, GovernmentContractAction, Security, UserAccount, Watchlist, WatchlistItem
from app.routers.events import list_watchlist_events


def _session():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    Base.metadata.create_all(
        engine,
        tables=[
            Security.__table__,
            Event.__table__,
            GovernmentContractAction.__table__,
            UserAccount.__table__,
            Watchlist.__table__,
            WatchlistItem.__table__,
        ],
    )
    return Session()


def _request_for_user(user: UserAccount) -> Request:
    token = sign_session_payload({"uid": user.id, "email": user.email})
    return Request(
        {"type": "http", "method": "GET", "path": "/", "headers": [(b"authorization", f"Bearer {token}".encode())]}
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
