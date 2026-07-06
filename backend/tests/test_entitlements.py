from __future__ import annotations

from fastapi import HTTPException
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from starlette.requests import Request

from app.auth import SESSION_COOKIE_NAME, sign_session_payload
from app.db import Base
from app.entitlements import ENTITLEMENTS, DEFAULT_FEATURE_GATES, entitlements_for_user, feature_gate_payloads, plan_config_payload
from app.main import (
    WatchlistPayload,
    add_to_watchlist,
    create_watchlist,
    get_entitlements,
    refresh_watchlist_confirmation_monitoring_endpoint,
)
from app.models import (
    AppSetting,
    Event,
    FeatureGate,
    NotificationSubscription,
    PlanLimit,
    PlanPrice,
    SavedScreen,
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
            AppSetting.__table__,
            FeatureGate.__table__,
            NotificationSubscription.__table__,
            PlanLimit.__table__,
            PlanPrice.__table__,
            SavedScreen.__table__,
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


def _user(db, email: str, *, tier: str = "free", role: str = "user") -> UserAccount:
    user = UserAccount(email=email, role=role, entitlement_tier=tier)
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


def _request_for_user(user: UserAccount) -> Request:
    token = sign_session_payload({"uid": user.id, "email": user.email})
    return Request(
        {"type": "http", "method": "POST", "path": "/", "headers": [(b"cookie", f"{SESSION_COOKIE_NAME}={token}".encode())]}
    )


def _seed_watchlists(db, count: int, owner_user_id: int) -> None:
    db.add_all([Watchlist(name=f"List {idx}", owner_user_id=owner_user_id) for idx in range(count)])
    db.commit()


def _seed_watchlist_with_tickers(db, ticker_count: int, owner_user_id: int) -> int:
    watchlist = Watchlist(name="Core", owner_user_id=owner_user_id)
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


def test_options_flow_and_institutional_activity_are_pro_only_even_if_gates_drift():
    assert ENTITLEMENTS["premium"].limit("options_flow_feed") == 0
    assert ENTITLEMENTS["premium"].limit("options_flow_filters") == 0
    assert ENTITLEMENTS["premium"].limit("institutional_feed") == 0
    assert ENTITLEMENTS["premium"].limit("institutional_filters") == 0
    for feature in ("options_flow_feed", "options_flow_filters", "institutional_feed", "institutional_filters"):
        assert feature not in ENTITLEMENTS["premium"].features
        assert feature in ENTITLEMENTS["pro"].features
        assert DEFAULT_FEATURE_GATES[feature]["required_tier"] == "pro"

    db = _session()
    try:
        db.add_all(
            [
                FeatureGate(feature_key="options_flow_feed", required_tier="premium", description="stale"),
                FeatureGate(feature_key="options_flow_filters", required_tier="premium", description="stale"),
                FeatureGate(feature_key="institutional_feed", required_tier="premium", description="stale"),
                FeatureGate(feature_key="institutional_filters", required_tier="premium", description="stale"),
            ]
        )
        db.commit()
        premium_user = _user(db, "premium-matrix@example.com", tier="premium")
        pro_user = _user(db, "pro-matrix@example.com", tier="pro")

        premium = entitlements_for_user(db, premium_user)
        pro = entitlements_for_user(db, pro_user)
        for feature in ("options_flow_feed", "options_flow_filters", "institutional_feed", "institutional_filters"):
            assert premium.has_feature(feature) is False
            assert pro.has_feature(feature) is True

        gates = {row["feature_key"]: row["required_tier"] for row in feature_gate_payloads(db)}
        config = {row["feature_key"]: row["required_tier"] for row in plan_config_payload(db)["features"]}
        for feature in ("options_flow_feed", "options_flow_filters", "institutional_feed", "institutional_filters"):
            assert gates[feature] == "pro"
            assert config[feature] == "pro"
    finally:
        db.close()


def test_admin_entitlements_include_all_paid_feature_sources():
    db = _session()
    try:
        admin = _user(db, "admin-entitlements@example.com", tier="free", role="admin")

        entitlements = entitlements_for_user(db, admin)
        payload = get_entitlements(_request_for_user(admin), db)

        assert entitlements.tier == "admin"
        assert payload["tier"] == "admin"
        assert payload["effective_tier"] == "admin"
        assert payload["is_admin"] is True
        for feature in (
            "signals",
            "ticker_confirmation",
            "premium_feed_metrics",
            "backtesting",
            "screener_intelligence",
            "options_flow_feed",
            "options_flow_filters",
            "institutional_feed",
            "institutional_filters",
            "screener_monitoring",
        ):
            assert entitlements.has_feature(feature) is True
            assert feature in payload["features"]
            assert int(payload["limits"][feature]) >= 1
    finally:
        db.close()


def test_premium_feed_metrics_gate_defaults_to_premium_and_unlocks_paid_users():
    db = _session()
    try:
        free_user = _user(db, "metrics-free@example.com", tier="free")
        premium_user = _user(db, "metrics-premium@example.com", tier="premium")
        pro_user = _user(db, "metrics-pro@example.com", tier="pro")

        assert DEFAULT_FEATURE_GATES["premium_feed_metrics"]["required_tier"] == "premium"
        assert entitlements_for_user(db, free_user).has_feature("premium_feed_metrics") is False
        assert entitlements_for_user(db, premium_user).has_feature("premium_feed_metrics") is True
        assert entitlements_for_user(db, pro_user).has_feature("premium_feed_metrics") is True

        gates = {row["feature_key"]: row for row in feature_gate_payloads(db)}
        assert gates["premium_feed_metrics"]["required_tier"] == "premium"
        assert "gain/loss" in gates["premium_feed_metrics"]["description"].lower()
    finally:
        db.close()


def test_ticker_confirmation_gate_defaults_to_premium_and_unlocks_paid_users():
    db = _session()
    try:
        free_user = _user(db, "ticker-confirmation-free@example.com", tier="free")
        premium_user = _user(db, "ticker-confirmation-premium@example.com", tier="premium")
        pro_user = _user(db, "ticker-confirmation-pro@example.com", tier="pro")

        assert DEFAULT_FEATURE_GATES["ticker_confirmation"]["required_tier"] == "premium"
        assert entitlements_for_user(db, free_user).has_feature("ticker_confirmation") is False
        assert entitlements_for_user(db, premium_user).has_feature("ticker_confirmation") is True
        assert entitlements_for_user(db, pro_user).has_feature("ticker_confirmation") is True

        gates = {row["feature_key"]: row for row in feature_gate_payloads(db)}
        config = {row["feature_key"]: row for row in plan_config_payload(db)["features"]}
        assert gates["ticker_confirmation"]["required_tier"] == "premium"
        assert "confirmation score" in gates["ticker_confirmation"]["description"].lower()
        assert config["ticker_confirmation"]["required_tier"] == "premium"
    finally:
        db.close()


def test_free_user_hitting_watchlist_limit_gets_upgrade_response(monkeypatch):
    monkeypatch.setenv("CT_DEFAULT_TIER", "free")
    monkeypatch.setenv("CT_ALLOW_ENTITLEMENT_HEADER", "1")
    db = _session()
    try:
        user = _user(db, "free@example.com")
        _seed_watchlists(db, 1, user.id)

        try:
            create_watchlist(WatchlistPayload(name="Overflow"), _request_for_user(user), db)
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
        user = _user(db, "premium@example.com", tier="premium")
        _seed_watchlists(db, 1, user.id)

        response = create_watchlist(WatchlistPayload(name="Premium overflow"), _request_for_user(user), db)

        assert response["name"] == "Premium overflow"
    finally:
        db.close()


def test_free_user_hitting_watchlist_ticker_limit_gets_upgrade_response(monkeypatch):
    monkeypatch.setenv("CT_DEFAULT_TIER", "free")
    monkeypatch.setenv("CT_ALLOW_ENTITLEMENT_HEADER", "1")
    db = _session()
    try:
        user = _user(db, "ticker-limit@example.com")
        watchlist_id = _seed_watchlist_with_tickers(db, 10, user.id)

        try:
            add_to_watchlist(watchlist_id, "AAPL", _request_for_user(user), db)
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
        user = _user(db, "core@example.com")
        watchlist_id = _seed_watchlist_with_tickers(db, 2, user.id)

        response = add_to_watchlist(watchlist_id, "AAPL", _request_for_user(user), db)

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
            assert exc.status_code == 401
        else:
            raise AssertionError("Expected sign-in-required response")

        free_user = _user(db, "free-reader@example.com", tier="free")
        try:
            put_notification_subscription(payload, _request_for_user(free_user), db)
        except HTTPException as exc:
            assert exc.status_code == 402
            assert exc.detail["feature"] == "notification_digests"
        else:
            raise AssertionError("Expected premium-required response")

        premium_user = _user(db, "reader@example.com", tier="premium")
        response = put_notification_subscription(payload, _request_for_user(premium_user), db)

        assert response["email"] == "reader@example.com"
    finally:
        db.close()


def test_watchlist_digest_uses_signed_in_account_email(monkeypatch):
    monkeypatch.setenv("CT_DEFAULT_TIER", "free")
    monkeypatch.setenv("CT_ALLOW_ENTITLEMENT_HEADER", "1")
    db = _session()
    try:
        user = _user(db, "reader@example.com", tier="premium")
        watchlist_id = _seed_watchlist_with_tickers(db, 1, user.id)

        response = put_notification_subscription(
            NotificationSubscriptionPayload(
                source_type="watchlist",
                source_id=str(watchlist_id),
                source_name="Core",
                source_payload={"unseen_since": "2026-04-12T00:00:00Z"},
                only_if_new=True,
                active=True,
                alert_triggers=["cross_source_confirmation"],
            ),
            _request_for_user(user),
            db,
        )

        assert response["email"] == "reader@example.com"
        assert db.query(NotificationSubscription).count() == 1
    finally:
        db.close()


def test_free_monitoring_sources_limit_is_enforced_for_watchlist_monitoring(monkeypatch):
    monkeypatch.setenv("CT_DEFAULT_TIER", "free")
    monkeypatch.setenv("CT_ALLOW_ENTITLEMENT_HEADER", "1")
    db = _session()
    try:
        user = _user(db, "monitoring@example.com")
        db.add_all(
            [
                Watchlist(name="List 1", owner_user_id=user.id),
                Watchlist(name="List 2", owner_user_id=user.id),
                Watchlist(name="List 3", owner_user_id=user.id),
            ]
        )
        db.commit()
        blocked_watchlist = db.query(Watchlist).filter(Watchlist.name == "List 3").one()

        try:
            refresh_watchlist_confirmation_monitoring_endpoint(blocked_watchlist.id, _request_for_user(user), db)
        except HTTPException as exc:
            assert exc.status_code == 402
            assert exc.detail["feature"] == "monitoring_sources"
        else:
            raise AssertionError("Expected premium-required response")
    finally:
        db.close()
