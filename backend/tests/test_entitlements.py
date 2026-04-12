from __future__ import annotations

from fastapi import HTTPException
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from starlette.requests import Request

from app.db import Base
from app.main import WatchlistPayload, add_to_watchlist, create_watchlist
from app.models import (
    Event,
    FeatureGate,
    NotificationSubscription,
    Security,
    StripeWebhookEvent,
    UserAccount,
    Watchlist,
    WatchlistItem,
    WatchlistViewState,
)
from app.routers.notifications import NotificationSubscriptionPayload, put_notification_subscription


def _session():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    Base.metadata.create_all(
        engine,
        tables=[
            Event.__table__,
            FeatureGate.__table__,
            NotificationSubscription.__table__,
            Security.__table__,
            StripeWebhookEvent.__table__,
            UserAccount.__table__,
            Watchlist.__table__,
            WatchlistItem.__table__,
            WatchlistViewState.__table__,
        ],
    )
    return Session()


def _request(tier: str | None = None) -> Request:
    headers = []
    if tier:
        headers.append((b"x-ct-entitlement-tier", tier.encode("utf-8")))
    return Request({"type": "http", "method": "POST", "path": "/", "headers": headers})


def _seed_watchlists(db, count: int) -> None:
    db.add_all([Watchlist(name=f"List {idx}") for idx in range(count)])
    db.commit()


def _seed_watchlist_with_tickers(db, ticker_count: int) -> int:
    watchlist = Watchlist(name="Core")
    db.add(watchlist)
    db.flush()
    securities = [
        Security(symbol=f"T{idx}", name=f"Ticker {idx}", asset_class="stock", sector=None)
        for idx in range(ticker_count)
    ]
    db.add_all(securities)
    db.flush()
    db.add_all([WatchlistItem(watchlist_id=watchlist.id, security_id=security.id) for security in securities])
    db.add(Security(symbol="AAPL", name="Apple", asset_class="stock", sector=None))
    db.commit()
    return watchlist.id


def test_free_user_hitting_watchlist_limit_gets_upgrade_response(monkeypatch):
    monkeypatch.setenv("CT_DEFAULT_TIER", "free")
    monkeypatch.setenv("CT_ALLOW_ENTITLEMENT_HEADER", "1")
    db = _session()
    try:
        _seed_watchlists(db, 3)

        try:
            create_watchlist(WatchlistPayload(name="Overflow"), _request(), db)
        except HTTPException as exc:
            assert exc.status_code == 402
            assert exc.detail["code"] == "premium_required"
            assert exc.detail["feature"] == "watchlists"
        else:
            raise AssertionError("Expected premium-required response")
    finally:
        db.close()


def test_premium_user_can_create_past_free_watchlist_limit(monkeypatch):
    monkeypatch.setenv("CT_DEFAULT_TIER", "free")
    monkeypatch.setenv("CT_ALLOW_ENTITLEMENT_HEADER", "1")
    db = _session()
    try:
        _seed_watchlists(db, 3)

        response = create_watchlist(WatchlistPayload(name="Premium overflow"), _request("premium"), db)

        assert response["name"] == "Premium overflow"
    finally:
        db.close()


def test_free_user_hitting_watchlist_ticker_limit_gets_upgrade_response(monkeypatch):
    monkeypatch.setenv("CT_DEFAULT_TIER", "free")
    monkeypatch.setenv("CT_ALLOW_ENTITLEMENT_HEADER", "1")
    db = _session()
    try:
        watchlist_id = _seed_watchlist_with_tickers(db, 15)

        try:
            add_to_watchlist(watchlist_id, "AAPL", _request(), db)
        except HTTPException as exc:
            assert exc.status_code == 402
            assert exc.detail["feature"] == "watchlist_tickers"
        else:
            raise AssertionError("Expected premium-required response")
    finally:
        db.close()


def test_free_user_keeps_core_watchlist_flow_under_limits(monkeypatch):
    monkeypatch.setenv("CT_DEFAULT_TIER", "free")
    monkeypatch.setenv("CT_ALLOW_ENTITLEMENT_HEADER", "1")
    db = _session()
    try:
        watchlist_id = _seed_watchlist_with_tickers(db, 2)

        response = add_to_watchlist(watchlist_id, "AAPL", _request(), db)

        assert response == {"status": "added", "symbol": "AAPL"}
    finally:
        db.close()


def test_digest_subscriptions_are_premium_only(monkeypatch):
    monkeypatch.setenv("CT_DEFAULT_TIER", "free")
    monkeypatch.setenv("CT_ALLOW_ENTITLEMENT_HEADER", "1")
    payload = NotificationSubscriptionPayload(
        email="reader@example.com",
        source_type="saved_view",
        source_id="view-1",
        source_name="High conviction",
        source_payload={"params": {"mode": "all"}},
        only_if_new=True,
        active=True,
        alert_triggers=["cross_source_confirmation"],
    )
    db = _session()
    try:
        try:
            put_notification_subscription(payload, _request(), db)
        except HTTPException as exc:
            assert exc.status_code == 402
            assert exc.detail["feature"] == "notification_digests"
        else:
            raise AssertionError("Expected premium-required response")

        response = put_notification_subscription(payload, _request("premium"), db)

        assert response["email"] == "reader@example.com"
    finally:
        db.close()
