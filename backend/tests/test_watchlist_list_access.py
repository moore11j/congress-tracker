from datetime import datetime, timedelta, timezone
import socket

import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session
from starlette.requests import Request

from app.auth import SESSION_COOKIE_NAME, sign_session_payload
from app.db import Base
from app.entitlements import seed_plan_config
from app.main import list_watchlists
from app.models import Event, MonitoringAlert, Security, UserAccount, Watchlist, WatchlistItem, WatchlistViewState


def request_for(user=None):
    headers = []
    if user is not None:
        token = sign_session_payload({"uid": user.id, "email": user.email})
        headers = [(b"cookie", f"{SESSION_COOKIE_NAME}={token}".encode())]
    return Request({"type": "http", "method": "GET", "path": "/api/watchlists", "headers": headers})


@pytest.mark.parametrize("tier,role,expected", [
    ("free", "user", 1), ("premium", "user", 2),
    ("pro", "user", 3), ("free", "admin", 3),
])
@pytest.mark.parametrize("include_ordinary", [True, False])
def test_owned_watchlist_counts_respect_current_plan_without_writes_or_network(monkeypatch, tier, role, expected, include_ordinary):
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as db:
        seed_plan_config(db)
        owner = UserAccount(email="owner@example.test", role=role, entitlement_tier=tier)
        other = UserAccount(email="other@example.test", role="user", entitlement_tier="pro")
        security = Security(symbol="NBIS", name="Fixture", asset_class="stock")
        db.add_all([owner, other, security])
        db.flush()
        owned = Watchlist(name="Owned", owner_user_id=owner.id)
        foreign = Watchlist(name="Private", owner_user_id=other.id)
        empty = Watchlist(name="Empty", owner_user_id=owner.id)
        legacy = Watchlist(name="Legacy", owner_user_id=owner.id)
        db.add_all([owned, foreign, empty, legacy])
        db.flush()
        for watchlist in [owned, foreign, legacy]:
            db.add(WatchlistItem(watchlist_id=watchlist.id, security_id=security.id, target_type="ticker"))
        now = datetime.now(timezone.utc)
        db.add(WatchlistViewState(watchlist_id=legacy.id, last_seen_at=now - timedelta(hours=2)))
        db.add(Event(event_type="insider_trade", ts=now, event_date=now, created_at=now,
                     symbol="NBIS", source="test", payload_json="{}", impact_score=0))
        for index, (user, watchlist, kind, read, dismissed) in enumerate([
            (owner, owned, "insider_trade", None, None),
            (owner, owned, "institutional_buy", None, None),
            (owner, owned, "signal", None, None),
            (owner, owned, "insider_trade", now, None),
            (owner, owned, "insider_trade", None, now),
            (other, owned, "insider_trade", None, None),
            (other, foreign, "insider_trade", None, None),
        ]):
            if index == 0 and not include_ordinary:
                continue
            db.add(MonitoringAlert(user_id=user.id, source_type="watchlist", source_id=str(watchlist.id),
                source_name=watchlist.name, event_id=index + 1, alert_type=kind, symbol="NBIS",
                title="Fixture", body="Fixture", payload_json="{}", event_created_at=now,
                read_at=read, dismissed_at=dismissed))
        db.commit()
        request = request_for(owner)
        owned_id, empty_id, legacy_id = owned.id, empty.id, legacy.id
        expected -= int(not include_ordinary)

        def deny_network(*args, **kwargs):
            raise AssertionError("Watchlist reads must not access the network")

        def readonly(conn, cursor, statement, parameters, context, executemany):
            assert statement.lstrip().upper().startswith(("SELECT", "PRAGMA")), statement

        monkeypatch.setattr(socket.socket, "connect", deny_network)
        event.listen(engine, "before_cursor_execute", readonly)
        rows = list_watchlists(request, db)
        assert rows == [
            {"id": empty_id, "name": "Empty", "ticker_count": 0, "symbols": [],
             "unseen_count": 0, "unread_count": 0, "new_count": 0},
            {"id": legacy_id, "name": "Legacy", "ticker_count": 1, "symbols": ["NBIS"],
             "unseen_count": 1, "unread_count": 1, "new_count": 1},
            {"id": owned_id, "name": "Owned", "ticker_count": 1, "symbols": ["NBIS"],
             "unseen_count": expected, "unread_count": expected, "new_count": expected},
        ]
        assert not db.new and not db.dirty and not db.deleted
    engine.dispose()


def test_watchlist_list_requires_a_real_session():
    engine = create_engine("sqlite:///:memory:")
    with Session(engine) as db:
        with pytest.raises(HTTPException) as error:
            list_watchlists(request_for(), db)
        assert error.value.status_code == 401
    engine.dispose()
