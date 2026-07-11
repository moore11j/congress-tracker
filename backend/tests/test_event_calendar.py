from __future__ import annotations

from datetime import date

from fastapi import HTTPException
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from starlette.requests import Request

from app.auth import SESSION_COOKIE_NAME, sign_session_payload
from app.db import Base
from app.models import AppSetting, FeatureGate, PlanLimit, PlanPrice, Security, UserAccount, Watchlist, WatchlistItem
from app.routers.event_calendar import get_monitoring_event_calendar
from app.services.event_calendar import fetch_event_calendar


def _session():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    Base.metadata.create_all(
        engine,
        tables=[
            AppSetting.__table__,
            FeatureGate.__table__,
            PlanLimit.__table__,
            PlanPrice.__table__,
            Security.__table__,
            UserAccount.__table__,
            Watchlist.__table__,
            WatchlistItem.__table__,
        ],
    )
    return Session()


def _user(db, email: str, *, tier: str = "premium") -> UserAccount:
    user = UserAccount(email=email, entitlement_tier=tier)
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


def _request_for_user(user: UserAccount) -> Request:
    token = sign_session_payload({"uid": user.id, "email": user.email})
    return Request({"type": "http", "method": "GET", "path": "/", "headers": [(b"cookie", f"{SESSION_COOKIE_NAME}={token}".encode())]})


def _watchlist(db, user: UserAccount, symbol: str = "NVDA") -> Watchlist:
    security = Security(symbol=symbol, name=symbol, asset_class="stock", sector=None)
    watchlist = Watchlist(name="Core", owner_user_id=user.id)
    db.add_all([security, watchlist])
    db.flush()
    db.add(WatchlistItem(watchlist_id=watchlist.id, security_id=security.id))
    db.commit()
    db.refresh(watchlist)
    return watchlist


def test_event_calendar_normalizes_fmp_rows_and_filters_watchlist_symbols(monkeypatch):
    db = _session()
    try:
        user = _user(db, "calendar@example.com")
        _watchlist(db, user, "NASDAQ:NVDA")

        def fake_request(endpoint, **kwargs):
            if endpoint == "economic-calendar":
                return [{"date": "2026-08-12 08:30:00", "event": "CPI", "country": "US", "estimate": "2.8%"}]
            if endpoint == "ipos-calendar":
                return [{"date": "2026-08-20", "symbol": "AAPL", "company": "Apple Inc."}]
            if endpoint == "earnings":
                assert kwargs["params"]["symbol"] == "NVDA"
                return [
                    {"date": "2026-08-14", "symbol": "NVDA", "epsEstimated": "1.02"},
                    {"date": "2026-09-14", "symbol": "NVDA", "epsEstimated": "1.04"},
                    {"date": "2026-08-14", "symbol": "AAPL", "epsEstimated": "1.12"},
                ]
            if endpoint == "dividends":
                assert kwargs["params"]["symbol"] == "NVDA"
                return [{"date": "2026-08-15", "dividend": 0.01}]
            if endpoint == "splits":
                assert kwargs["params"]["symbol"] == "NVDA"
                return []
            return []

        monkeypatch.setattr("app.services.event_calendar.request_fmp_json", fake_request)

        result = fetch_event_calendar(db, user, start=date(2026, 8, 1), end=date(2026, 8, 31), scope="watchlist")

        titles = [item["title"] for item in result.items]
        assert "CPI" in titles
        assert "NVDA earnings" in titles
        assert "NVDA dividend" in titles
        assert "AAPL IPO" in titles
        assert "AAPL earnings" not in titles
        assert result.errors == []
    finally:
        db.close()


def test_event_calendar_route_requires_premium(monkeypatch):
    db = _session()
    try:
        free_user = _user(db, "free-calendar@example.com", tier="free")

        try:
            get_monitoring_event_calendar(
                _request_for_user(free_user),
                db,
                start=date(2026, 8, 1),
                end=date(2026, 8, 31),
                scope="watchlist",
            )
        except HTTPException as exc:
            assert exc.status_code == 402
            assert exc.detail["feature"] == "event_calendar"
        else:
            raise AssertionError("Expected premium-required response")
    finally:
        db.close()
