from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from starlette.requests import Request

from app.auth import SESSION_COOKIE_NAME, sign_session_payload
from app.db import Base, ensure_email_notification_schema
from app.models import (
    ConfirmationMonitoringEvent,
    EmailDelivery,
    EmailTemplate,
    Event,
    MonitoringAlert,
    NotificationSubscription,
    Security,
    UserAccount,
    Watchlist,
    WatchlistItem,
)
from app.routers.accounts import AdminDigestRunNowPayload, AdminDigestSendTestPayload, admin_run_email_digest_now, admin_send_monitoring_digest_test
from app.services.email_digests import build_monitoring_digest, build_signal_alert_digest, build_watchlist_activity_digest, send_monitoring_digest, send_signal_alert_digest, send_watchlist_activity_digest
from app.services.email_intraday import run_intraday_alert_sweep, summarize_intraday_alert_results
from app.services.email_templates import seed_default_email_templates


class FakePostmarkResponse:
    status_code = 200

    def json(self):
        return {"MessageID": "message-id"}


def _session():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    ensure_email_notification_schema(engine)
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    db = Session()
    seed_default_email_templates(db)
    return db


def _request_for_user(user: UserAccount) -> Request:
    token = sign_session_payload({"uid": user.id, "email": user.email})
    return Request({"type": "http", "method": "GET", "path": "/", "headers": [(b"cookie", f"{SESSION_COOKIE_NAME}={token}".encode())]})


def _user(db, email: str, *, role: str = "user", watchlist_notifications: bool = True, tier: str = "premium") -> UserAccount:
    user = UserAccount(
        email=email,
        first_name="Ada",
        role=role,
        entitlement_tier=tier,
        watchlist_activity_notifications=watchlist_notifications,
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


def _watchlist(db, user: UserAccount, *, active_subscription: bool = True, only_if_new: bool = True) -> Watchlist:
    watchlist = Watchlist(name=f"{user.id} AI", owner_user_id=user.id)
    security = db.execute(select(Security).where(Security.symbol == "NVDA")).scalar_one_or_none()
    if security is None:
        security = Security(symbol="NVDA", name="Nvidia", asset_class="stock", sector=None)
        db.add(security)
    db.add(watchlist)
    db.flush()
    db.add(WatchlistItem(watchlist_id=watchlist.id, security_id=security.id))
    db.add(
        NotificationSubscription(
            email=user.email,
            source_type="watchlist",
            source_id=str(watchlist.id),
            source_name=watchlist.name,
            frequency="daily",
            only_if_new=only_if_new,
            active=active_subscription,
            alert_triggers_json="[]",
        )
    )
    db.commit()
    db.refresh(watchlist)
    return watchlist


def _event(db, symbol: str = "NVDA", *, ts: datetime | None = None) -> Event:
    now = ts or datetime.now(timezone.utc)
    event = Event(
        event_type="congress_trade",
        ts=now,
        event_date=now,
        symbol=symbol,
        source="test",
        impact_score=82,
        payload_json=json.dumps({"smart_score": 82}),
        member_name="Example Member",
        trade_type="purchase",
        amount_min=15_001,
        amount_max=50_000,
    )
    db.add(event)
    db.commit()
    db.refresh(event)
    return event


def _bare_event(
    db,
    symbol: str = "NVDA",
    *,
    event_type: str = "congress_trade",
    ts: datetime | None = None,
    payload: dict | None = None,
    member_name: str | None = None,
    impact_score: float | None = None,
) -> Event:
    now = ts or datetime.now(timezone.utc)
    event = Event(
        event_type=event_type,
        ts=now,
        event_date=now,
        symbol=symbol,
        source="test",
        impact_score=impact_score,
        payload_json=json.dumps(payload or {}),
        member_name=member_name,
        trade_type="purchase",
        amount_min=None,
        amount_max=None,
    )
    db.add(event)
    db.commit()
    db.refresh(event)
    return event


def _monitoring_alert(
    db,
    user: UserAccount,
    watchlist: Watchlist,
    *,
    source_type: str = "watchlist",
    alert_type: str = "watchlist_activity",
    event_id: int = 777,
    symbol: str = "NVDA",
    ts: datetime | None = None,
    title: str | None = None,
    body: str | None = None,
    payload: dict | None = None,
) -> MonitoringAlert:
    now = ts or datetime.now(timezone.utc)
    alert = MonitoringAlert(
        user_id=user.id,
        source_type=source_type,
        source_id=str(watchlist.id),
        source_name=watchlist.name,
        event_id=event_id,
        alert_type=alert_type,
        symbol=symbol,
        title=title or f"{symbol} has fresh monitored activity",
        body=body if body is not None else "New monitored activity.",
        payload_json=json.dumps(payload if payload is not None else {"score": 88, "direction": "bullish"}),
        event_created_at=now,
    )
    db.add(alert)
    db.commit()
    db.refresh(alert)
    return alert


def _confirmation_event(db, user: UserAccount, watchlist: Watchlist, *, ticker: str = "XOM", ts: datetime | None = None) -> ConfirmationMonitoringEvent:
    now = ts or datetime.now(timezone.utc)
    event = ConfirmationMonitoringEvent(
        user_id=user.id,
        watchlist_id=watchlist.id,
        ticker=ticker,
        event_type="confirmation_upgraded",
        title=f"{ticker} confirmation score rose",
        body="Confirmation strengthened.",
        score_before=50,
        score_after=84,
        band_before="moderate",
        band_after="strong",
        direction_before="mixed",
        direction_after="bullish",
        source_count_before=1,
        source_count_after=3,
        payload_json="{}",
        created_at=now,
    )
    db.add(event)
    db.commit()
    db.refresh(event)
    return event


def test_institutional_watchlist_activity_digest_is_pro_gated():
    db = _session()
    try:
        since = datetime.now(timezone.utc) - timedelta(days=1)
        premium_user = _user(db, "premium-institutional-digest@example.com", tier="premium")
        premium_watchlist = _watchlist(db, premium_user)
        pro_user = _user(db, "pro-institutional-digest@example.com", tier="pro")
        pro_watchlist = _watchlist(db, pro_user)
        event = _bare_event(
            db,
            event_type="institutional_accumulation",
            payload={"holder_name": "Blue Ridge Capital", "filing_date": datetime.now(timezone.utc).date().isoformat()},
            member_name="Blue Ridge Capital",
            impact_score=90,
        )
        event.amount_min = 10_000_000
        event.amount_max = 10_000_000
        db.commit()

        premium_digest = build_watchlist_activity_digest(db, premium_user, premium_watchlist, since)
        pro_digest = build_watchlist_activity_digest(db, pro_user, pro_watchlist, since)

        assert premium_digest.items_count == 0
        assert "Blue Ridge Capital" not in premium_digest.context["items_text"]
        assert pro_digest.items_count == 1
        assert pro_digest.items[0]["event_type"] == "institutional accumulation"
    finally:
        db.close()


def test_institutional_monitoring_digest_alert_details_are_pro_gated():
    db = _session()
    try:
        since = datetime.now(timezone.utc) - timedelta(days=1)
        premium_user = _user(db, "premium-monitoring-digest@example.com", tier="premium")
        premium_watchlist = _watchlist(db, premium_user)
        pro_user = _user(db, "pro-monitoring-digest@example.com", tier="pro")
        pro_watchlist = _watchlist(db, pro_user)
        _monitoring_alert(
            db,
            premium_user,
            premium_watchlist,
            alert_type="institutional_accumulation",
            title="NVDA Institutional Activity",
            body="Blue Ridge Capital reported a larger NVDA position.",
            payload={"event": {"holder_name": "Blue Ridge Capital", "direction": "bullish", "smart_score": 90}},
        )
        _monitoring_alert(
            db,
            pro_user,
            pro_watchlist,
            alert_type="institutional_accumulation",
            title="NVDA Institutional Activity",
            body="Blue Ridge Capital reported a larger NVDA position.",
            payload={"event": {"holder_name": "Blue Ridge Capital", "direction": "bullish", "smart_score": 90}},
        )

        premium_digest = build_monitoring_digest(db, premium_user, premium_watchlist, since)
        pro_digest = build_monitoring_digest(db, pro_user, pro_watchlist, since)

        assert premium_digest.items_count == 0
        assert "Blue Ridge Capital" not in premium_digest.context["items_text"]
        assert pro_digest.items_count == 1
        assert "Blue Ridge Capital" in pro_digest.context["items_text"]
    finally:
        db.close()


def test_intraday_institutional_email_candidates_are_pro_gated(monkeypatch):
    db = _session()
    try:
        now = datetime.now(timezone.utc)
        premium_user = _user(db, "premium-intraday@example.com", tier="premium")
        _watchlist(db, premium_user)
        pro_user = _user(db, "pro-intraday@example.com", tier="pro")
        _watchlist(db, pro_user)
        event = _bare_event(
            db,
            event_type="institutional_accumulation",
            payload={"holder_name": "Blue Ridge Capital", "filing_date": now.date().isoformat(), "smart_score": 95},
            member_name="Blue Ridge Capital",
            impact_score=95,
        )
        event.amount_min = 5_000_000
        event.amount_max = 5_000_000
        db.commit()
        monkeypatch.setenv("EMAIL_ALERT_INTRADAY_ENABLED", "true")

        results = run_intraday_alert_sweep(
            db,
            lookback_minutes=120,
            limit=10,
            dry_run=True,
            now=now + timedelta(minutes=1),
            market_hours_only=False,
        )

        assert [item["to_email"] for item in results] == ["pro-intraday@example.com"]
        assert results[0]["event_type"] == "institutional_accumulation"
    finally:
        db.close()


def test_watchlist_digest_skips_when_user_toggle_disabled():
    db = _session()
    try:
        user = _user(db, "toggle@example.com", watchlist_notifications=False)
        watchlist = _watchlist(db, user)
        _event(db)

        result = send_watchlist_activity_digest(db, user, watchlist, datetime.now(timezone.utc) - timedelta(days=1))

        assert result["status"] == "skipped"
        assert result["error"] == "user_alerts_disabled"
        row = db.execute(select(EmailDelivery)).scalar_one()
        assert row.template_key == "alerts.watchlist_activity"
    finally:
        db.close()


def test_watchlist_digest_skips_when_subscription_inactive_unless_forced():
    db = _session()
    try:
        user = _user(db, "inactive@example.com")
        watchlist = _watchlist(db, user, active_subscription=False)
        _event(db)

        result = send_watchlist_activity_digest(db, user, watchlist, datetime.now(timezone.utc) - timedelta(days=1))

        assert result["status"] == "skipped"
        assert result["error"] == "watchlist_digest_inactive"
    finally:
        db.close()


def test_watchlist_digest_only_new_logs_no_new_items():
    db = _session()
    try:
        user = _user(db, "nonew@example.com")
        watchlist = _watchlist(db, user, only_if_new=True)
        _event(db, ts=datetime.now(timezone.utc) - timedelta(days=5))

        result = send_watchlist_activity_digest(db, user, watchlist, datetime.now(timezone.utc) - timedelta(hours=1))

        assert result["status"] == "skipped"
        assert result["error"] == "no_new_items"
    finally:
        db.close()


def test_watchlist_digest_resolves_insider_actor_from_payload():
    db = _session()
    try:
        user = _user(db, "insider-name@example.com")
        watchlist = _watchlist(db, user)
        _bare_event(
            db,
            event_type="insider_trade",
            payload={"raw": {"reportingName": "Mercer Park Brand Acquisition Corp"}},
        )

        digest = build_watchlist_activity_digest(db, user, watchlist, datetime.now(timezone.utc) - timedelta(days=1))

        assert digest.items[0]["actor"] == "Mercer Park Brand Acquisition Corp"
        assert digest.items[0]["actor"] != "Unknown"
    finally:
        db.close()


def test_watchlist_digest_hides_score_column_when_all_scores_missing():
    db = _session()
    try:
        user = _user(db, "score-hidden@example.com")
        watchlist = _watchlist(db, user)
        _bare_event(db)

        digest = build_watchlist_activity_digest(db, user, watchlist, datetime.now(timezone.utc) - timedelta(days=1))

        assert ">Score<" not in digest.context["items_html"]
        assert "n/a" not in digest.context["items_html"].lower()
        assert "score" not in digest.context["items_text"].lower()
    finally:
        db.close()


def test_watchlist_digest_mixed_scores_use_dash_for_missing_values():
    db = _session()
    try:
        user = _user(db, "score-mixed@example.com")
        watchlist = _watchlist(db, user)
        _bare_event(db, ts=datetime.now(timezone.utc) - timedelta(minutes=2))
        _bare_event(db, ts=datetime.now(timezone.utc), payload={"smart_score": 91})

        digest = build_watchlist_activity_digest(db, user, watchlist, datetime.now(timezone.utc) - timedelta(days=1))

        assert ">Score<" in digest.context["items_html"]
        assert ">91<" in digest.context["items_html"]
        assert "&mdash;" in digest.context["items_html"]
        assert "n/a" not in digest.context["items_html"].lower()
    finally:
        db.close()


def test_watchlist_digest_caps_display_rows_and_reports_more_count():
    db = _session()
    try:
        user = _user(db, "watchlist-cap@example.com")
        watchlist = _watchlist(db, user)
        base = datetime.now(timezone.utc)
        for idx in range(12):
            _bare_event(db, ts=base - timedelta(minutes=idx), member_name=f"Member {idx}")

        digest = build_watchlist_activity_digest(db, user, watchlist, base - timedelta(days=1))

        assert digest.items_count == 12
        assert digest.context["items_html"].count("<tr>") == 11
        assert "Showing 10 of 12 items" in digest.context["items_html"]
        assert "Showing 10 of 12 items" in digest.context["items_text"]
    finally:
        db.close()


def test_watchlist_digest_idempotency_prevents_duplicate_delivery_rows(monkeypatch):
    monkeypatch.setenv("EMAIL_PROVIDER", "postmark")
    monkeypatch.setenv("EMAIL_DELIVERY_ENABLED", "true")
    monkeypatch.delenv("POSTMARK_SERVER_TOKEN", raising=False)
    db = _session()
    try:
        user = _user(db, "idempotent@example.com")
        watchlist = _watchlist(db, user)
        _event(db)
        since = datetime.now(timezone.utc) - timedelta(days=1)

        first = send_watchlist_activity_digest(db, user, watchlist, since)
        second = send_watchlist_activity_digest(db, user, watchlist, since)

        assert first["id"] == second["id"]
        assert first["status"] == "log_only"
        assert second["status"] == "skipped"
        assert second["error"] == "duplicate_window_already_sent"
        assert db.query(EmailDelivery).count() == 1
    finally:
        db.close()


def test_monitoring_digest_includes_watchlist_monitoring_alert(monkeypatch):
    monkeypatch.setenv("EMAIL_PROVIDER", "postmark")
    monkeypatch.setenv("EMAIL_DELIVERY_ENABLED", "true")
    monkeypatch.delenv("POSTMARK_SERVER_TOKEN", raising=False)
    db = _session()
    try:
        user = _user(db, "monitoring-alert@example.com")
        watchlist = _watchlist(db, user)
        _monitoring_alert(db, user, watchlist, source_type="saved_screen", alert_type="smart_score_threshold")

        result = send_monitoring_digest(db, user, watchlist, datetime.now(timezone.utc) - timedelta(days=1))

        assert result["status"] == "log_only"
        assert result["item_count"] == 1
        assert result["template_key"] == "alerts.signal_alert"
        assert result["rendered_preview"]["sample_items"][0]["why_notable"] == "NVDA has fresh monitored activity"
    finally:
        db.close()


def test_monitoring_digest_uses_template_sender_over_alerts_env(monkeypatch):
    monkeypatch.setenv("EMAIL_PROVIDER", "postmark")
    monkeypatch.setenv("EMAIL_DELIVERY_ENABLED", "true")
    monkeypatch.setenv("POSTMARK_SERVER_TOKEN", "server-token")
    monkeypatch.setenv("EMAIL_FROM_ALERTS", "Walnut Alerts <alerts@walnut-intel.com>")
    captured = {}

    def fake_post(url, headers, json, timeout):
        captured.update(json)
        return FakePostmarkResponse()

    monkeypatch.setattr("app.services.email_delivery.requests.post", fake_post)
    db = _session()
    try:
        user = _user(db, "monitoring-sender@example.com")
        watchlist = _watchlist(db, user)
        _monitoring_alert(db, user, watchlist, source_type="saved_screen", alert_type="smart_score_threshold")
        template = db.execute(select(EmailTemplate).where(EmailTemplate.template_key == "alerts.signal_alert")).scalar_one()
        template.from_email = "alerts@walnutmarkets.com"
        db.commit()

        result = send_monitoring_digest(db, user, watchlist, datetime.now(timezone.utc) - timedelta(days=1))

        row = db.execute(select(EmailDelivery)).scalar_one()
        assert result["status"] == "sent"
        assert captured["From"] == "Walnut Alerts <alerts@walnutmarkets.com>"
        assert row.from_email == "alerts@walnutmarkets.com"
    finally:
        db.close()


def test_monitoring_digest_uses_window_label_and_friendly_pt_timestamp():
    db = _session()
    try:
        user = _user(db, "monitoring-window@example.com")
        watchlist = _watchlist(db, user)
        alert_ts = datetime(2026, 6, 5, 4, 20, tzinfo=timezone.utc)
        _monitoring_alert(db, user, watchlist, ts=alert_ts)

        digest = build_monitoring_digest(
            db,
            user,
            watchlist,
            datetime(2026, 5, 30, 7, 0, tzinfo=timezone.utc),
            window_end=datetime(2026, 6, 5, 4, 30, tzinfo=timezone.utc),
        )

        assert "Jun 4, 2026" in digest.context["digest_date"]
        assert "Jun 5, 2026" not in digest.context["digest_date"]
        assert "Jun 4, 2026, 9:20 PM PT" in digest.context["items_text"]
        assert "UTC" not in digest.context["items_text"]
    finally:
        db.close()


def test_monitoring_digest_excludes_broken_unknown_rows():
    db = _session()
    try:
        user = _user(db, "monitoring-unknown@example.com")
        watchlist = _watchlist(db, user)
        _monitoring_alert(db, user, watchlist, symbol="UNKNOWN", payload={})

        digest = build_monitoring_digest(db, user, watchlist, datetime.now(timezone.utc) - timedelta(days=1))

        assert digest.items_count == 0
        assert "UNKNOWN" not in digest.context["items_text"]
    finally:
        db.close()


def test_signal_digest_includes_saved_screen_monitoring_alert(monkeypatch):
    monkeypatch.setenv("EMAIL_PROVIDER", "postmark")
    monkeypatch.setenv("EMAIL_DELIVERY_ENABLED", "true")
    monkeypatch.delenv("POSTMARK_SERVER_TOKEN", raising=False)
    db = _session()
    try:
        user = _user(db, "signal-alert@example.com")
        watchlist = _watchlist(db, user)
        _monitoring_alert(db, user, watchlist, source_type="saved_screen", alert_type="smart_score_threshold")

        result = send_signal_alert_digest(db, user, datetime.now(timezone.utc) - timedelta(days=1))

        assert result["status"] == "log_only"
        assert result["item_count"] >= 1
        assert any(item["source_stack"] == watchlist.name for item in result["rendered_preview"]["sample_items"])
    finally:
        db.close()


def test_signal_digest_uses_template_sender_over_alerts_env(monkeypatch):
    monkeypatch.setenv("EMAIL_PROVIDER", "postmark")
    monkeypatch.setenv("EMAIL_DELIVERY_ENABLED", "true")
    monkeypatch.setenv("POSTMARK_SERVER_TOKEN", "server-token")
    monkeypatch.setenv("EMAIL_FROM_ALERTS", "Walnut Alerts <alerts@walnut-intel.com>")
    captured = {}

    def fake_post(url, headers, json, timeout):
        captured.update(json)
        return FakePostmarkResponse()

    monkeypatch.setattr("app.services.email_delivery.requests.post", fake_post)
    db = _session()
    try:
        user = _user(db, "signal-sender@example.com")
        watchlist = _watchlist(db, user)
        _monitoring_alert(db, user, watchlist, source_type="saved_screen", alert_type="smart_score_threshold")
        template = db.execute(select(EmailTemplate).where(EmailTemplate.template_key == "alerts.signal_alert")).scalar_one()
        template.from_email = "alerts@walnutmarkets.com"
        db.commit()

        result = send_signal_alert_digest(db, user, datetime.now(timezone.utc) - timedelta(days=1))

        row = db.execute(select(EmailDelivery)).scalar_one()
        assert result["status"] == "sent"
        assert captured["From"] == "Walnut Alerts <alerts@walnutmarkets.com>"
        assert row.from_email == "alerts@walnutmarkets.com"
    finally:
        db.close()


def test_signal_digest_excludes_raw_watchlist_trade_events(monkeypatch):
    monkeypatch.setenv("EMAIL_PROVIDER", "postmark")
    monkeypatch.setenv("EMAIL_DELIVERY_ENABLED", "true")
    monkeypatch.delenv("POSTMARK_SERVER_TOKEN", raising=False)
    db = _session()
    try:
        user = _user(db, "signal-raw-excluded@example.com")
        _watchlist(db, user)
        _event(db, symbol="NVDA")

        result = send_signal_alert_digest(db, user, datetime.now(timezone.utc) - timedelta(days=1))

        assert result["status"] == "skipped"
        assert result["error"] == "no_qualified_signals"
        assert result["item_count"] == 0
    finally:
        db.close()


def test_signal_digest_excludes_unknown_ticker_rows():
    db = _session()
    try:
        user = _user(db, "signal-unknown@example.com")
        watchlist = _watchlist(db, user)
        _monitoring_alert(db, user, watchlist, source_type="saved_screen", alert_type="smart_score_threshold", symbol="UNKNOWN")

        digest = build_signal_alert_digest(db, user, datetime.now(timezone.utc) - timedelta(days=1))

        assert digest.items_count == 0
        assert digest.diagnostics["candidate_count"] == 1
        assert digest.diagnostics["excluded_reasons"]["missing_ticker"] == 1
        assert "UNKNOWN" not in digest.context["signals_text"]
        assert "--" not in digest.context["signals_text"]
    finally:
        db.close()


def test_signal_digest_excludes_null_score_rows():
    db = _session()
    try:
        user = _user(db, "signal-null-score@example.com")
        watchlist = _watchlist(db, user)
        _monitoring_alert(
            db,
            user,
            watchlist,
            source_type="saved_screen",
            alert_type="smart_score_threshold",
            symbol="NBIS",
            payload={"direction": "bearish"},
        )

        digest = build_signal_alert_digest(db, user, datetime.now(timezone.utc) - timedelta(days=1))

        assert digest.items_count == 0
        assert digest.diagnostics["excluded_reasons"]["missing_score"] == 1
        assert "NBIS" not in digest.context["signals_text"]
    finally:
        db.close()


def test_signal_digest_excludes_generic_saved_screen_refresh_rows():
    db = _session()
    try:
        user = _user(db, "signal-refresh@example.com")
        watchlist = _watchlist(db, user)
        _monitoring_alert(
            db,
            user,
            watchlist,
            source_type="saved_screen",
            alert_type="saved_screen_refreshed",
            symbol="NVDA",
            title="Bullish confirmation screen refreshed",
            payload={"score": 91, "direction": "bullish"},
        )

        digest = build_signal_alert_digest(db, user, datetime.now(timezone.utc) - timedelta(days=1))

        assert digest.items_count == 0
        assert digest.diagnostics["excluded_reasons"]["internal_refresh_event"] == 1
        assert "Bullish confirmation screen refreshed" not in digest.context["signals_text"]
    finally:
        db.close()


def test_signal_digest_includes_resolved_scored_signal_with_source_and_link():
    db = _session()
    try:
        user = _user(db, "signal-qualified@example.com")
        watchlist = _watchlist(db, user)
        _monitoring_alert(
            db,
            user,
            watchlist,
            source_type="saved_screen",
            alert_type="cross_source_confirmation",
            symbol="NBIS",
            title="NBIS lost multi-source confirmation",
            payload={"score": 39, "direction": "bearish", "source_stack": "Insiders + price/volume"},
        )

        digest = build_signal_alert_digest(db, user, datetime.now(timezone.utc) - timedelta(days=1))

        assert digest.items_count == 1
        item = digest.items[0]
        assert item["ticker"] == "NBIS"
        assert item["signal_score"] == 39
        assert item["direction"] == "bearish"
        assert item["why_notable"] == "NBIS lost multi-source confirmation"
        assert item["source_stack"] == "Insiders + price/volume"
        assert item["href"].endswith("/ticker/NBIS")
        assert "NBIS" in digest.context["signals_text"]
        assert "Insiders + price/volume" in digest.context["signals_text"]
    finally:
        db.close()


def test_signal_digest_scheduled_send_skips_when_no_qualified_signals(monkeypatch):
    monkeypatch.setenv("EMAIL_PROVIDER", "postmark")
    monkeypatch.setenv("EMAIL_DELIVERY_ENABLED", "true")
    monkeypatch.delenv("POSTMARK_SERVER_TOKEN", raising=False)
    db = _session()
    try:
        user = _user(db, "signal-scheduled-skip@example.com")
        watchlist = _watchlist(db, user)
        _monitoring_alert(db, user, watchlist, source_type="saved_screen", alert_type="smart_score_threshold", symbol="UNKNOWN")

        result = send_signal_alert_digest(db, user, datetime.now(timezone.utc) - timedelta(days=1))

        assert result["status"] == "skipped"
        assert result["error"] == "no_qualified_signals"
        assert result["candidate_count"] == 1
        assert result["qualified_count"] == 0
        assert result["excluded_count"] == 1
        assert result["excluded_reasons"]["missing_ticker"] == 1
    finally:
        db.close()


def test_signal_digest_force_test_does_not_change_scheduled_skip_logic(monkeypatch):
    monkeypatch.setenv("EMAIL_PROVIDER", "postmark")
    monkeypatch.setenv("EMAIL_DELIVERY_ENABLED", "true")
    monkeypatch.delenv("POSTMARK_SERVER_TOKEN", raising=False)
    db = _session()
    try:
        user = _user(db, "signal-force-empty@example.com")
        watchlist = _watchlist(db, user)
        _monitoring_alert(db, user, watchlist, source_type="saved_screen", alert_type="smart_score_threshold", symbol="UNKNOWN")
        since = datetime.now(timezone.utc) - timedelta(days=1)

        forced = send_signal_alert_digest(db, user, since, force=True)
        scheduled = send_signal_alert_digest(db, user, since)

        assert forced["status"] == "log_only"
        assert forced["item_count"] == 0
        assert scheduled["status"] == "skipped"
        assert scheduled["error"] == "no_qualified_signals"
    finally:
        db.close()


def test_admin_monitoring_digest_run_now_reports_quality_diagnostics():
    db = _session()
    try:
        admin = _user(db, "signal-admin@example.com", role="admin")
        watchlist = _watchlist(db, admin)
        _monitoring_alert(db, admin, watchlist, source_type="saved_screen", alert_type="smart_score_threshold", event_id=1, symbol="UNKNOWN")
        _monitoring_alert(db, admin, watchlist, source_type="saved_screen", alert_type="smart_score_threshold", event_id=2, symbol="NBIS")

        result = admin_run_email_digest_now(
            AdminDigestRunNowPayload(kind="monitoring", lookback_days=1, limit=10, dry_run=True),
            _request_for_user(admin),
            db,
        )

        assert result["summary"]["candidate_count"] == 2
        assert result["summary"]["qualified_count"] == 1
        assert result["summary"]["excluded_count"] == 1
        assert result["summary"]["excluded_reasons"]["missing_ticker"] == 1
        assert result["items"][0]["candidate_count"] == 2
        assert result["items"][0]["excluded_reasons"]["missing_ticker"] == 1
    finally:
        db.close()


def test_single_signal_alert_subject_targets_one_ticker(monkeypatch):
    monkeypatch.setenv("EMAIL_PROVIDER", "postmark")
    monkeypatch.setenv("EMAIL_DELIVERY_ENABLED", "true")
    monkeypatch.delenv("POSTMARK_SERVER_TOKEN", raising=False)
    db = _session()
    try:
        user = _user(db, "single-signal@example.com")
        watchlist = _watchlist(db, user)
        _monitoring_alert(db, user, watchlist, source_type="saved_screen", alert_type="smart_score_threshold", symbol="XOM")

        result = send_signal_alert_digest(db, user, datetime.now(timezone.utc) - timedelta(days=1))
        row = db.execute(select(EmailDelivery).where(EmailDelivery.id == result["id"])).scalar_one()

        assert row.subject == "Walnut monitoring digest"
        assert result["rendered_preview"]["sample_items"] == [result["rendered_preview"]["sample_items"][0]]
        assert result["rendered_preview"]["sample_items"][0]["ticker"] == "XOM"
    finally:
        db.close()


def test_multi_signal_digest_subject_matches_digest_content(monkeypatch):
    monkeypatch.setenv("EMAIL_PROVIDER", "postmark")
    monkeypatch.setenv("EMAIL_DELIVERY_ENABLED", "true")
    monkeypatch.delenv("POSTMARK_SERVER_TOKEN", raising=False)
    db = _session()
    try:
        user = _user(db, "multi-signal@example.com")
        watchlist = _watchlist(db, user)
        _monitoring_alert(db, user, watchlist, source_type="saved_screen", alert_type="smart_score_threshold", event_id=1, symbol="XOM")
        _confirmation_event(db, user, watchlist, ticker="MSFT")

        result = send_signal_alert_digest(db, user, datetime.now(timezone.utc) - timedelta(days=1))
        row = db.execute(select(EmailDelivery).where(EmailDelivery.id == result["id"])).scalar_one()
        tickers = {item["ticker"] for item in result["rendered_preview"]["sample_items"]}

        assert row.subject == "Walnut monitoring digest"
        assert result["item_count"] == 2
        assert {"XOM", "MSFT"}.issubset(tickers)
    finally:
        db.close()


def test_intraday_dry_run_keeps_low_priority_watchlist_item_in_digest():
    db = _session()
    try:
        user = _user(db, "intraday-low@example.com")
        _watchlist(db, user)
        now = datetime(2026, 6, 5, 17, 0, tzinfo=timezone.utc)
        _bare_event(db, ts=now - timedelta(minutes=5), impact_score=25, payload={"smart_score": 25})

        results = run_intraday_alert_sweep(db, lookback_minutes=60, dry_run=True, now=now)
        summary = summarize_intraday_alert_results(results)

        assert summary["candidate_count"] == 1
        assert summary["skipped_count"] == 1
        assert summary["skip_reasons"]["low_priority"] == 1
        assert results[0]["status"] == "skipped"
    finally:
        db.close()


def test_intraday_high_priority_watchlist_item_sends(monkeypatch):
    monkeypatch.setenv("EMAIL_ALERT_INTRADAY_ENABLED", "true")
    monkeypatch.setenv("EMAIL_DELIVERY_ENABLED", "true")
    monkeypatch.setenv("EMAIL_PROVIDER", "postmark")
    monkeypatch.delenv("POSTMARK_SERVER_TOKEN", raising=False)
    db = _session()
    try:
        user = _user(db, "intraday-watchlist@example.com")
        _watchlist(db, user)
        now = datetime(2026, 6, 5, 17, 0, tzinfo=timezone.utc)
        _bare_event(db, ts=now - timedelta(minutes=5), impact_score=91, payload={"smart_score": 91})

        results = run_intraday_alert_sweep(db, lookback_minutes=60, dry_run=False, now=now)
        summary = summarize_intraday_alert_results(results)

        assert summary["candidate_count"] == 1
        assert summary["sent_count"] == 1
        assert results[0]["status"] == "log_only"
        assert results[0]["template_key"] == "alerts.watchlist_intraday"
        assert results[0]["trigger"] == "smart_score_threshold"
    finally:
        db.close()


def test_intraday_high_conviction_signal_sends(monkeypatch):
    monkeypatch.setenv("EMAIL_ALERT_INTRADAY_ENABLED", "true")
    monkeypatch.setenv("EMAIL_DELIVERY_ENABLED", "true")
    monkeypatch.setenv("EMAIL_PROVIDER", "postmark")
    monkeypatch.delenv("POSTMARK_SERVER_TOKEN", raising=False)
    db = _session()
    try:
        user = _user(db, "intraday-signal@example.com")
        watchlist = _watchlist(db, user)
        now = datetime(2026, 6, 5, 17, 0, tzinfo=timezone.utc)
        _monitoring_alert(
            db,
            user,
            watchlist,
            source_type="saved_screen",
            alert_type="smart_score_threshold",
            symbol="NVDA",
            ts=now - timedelta(minutes=5),
        )

        results = run_intraday_alert_sweep(db, lookback_minutes=60, dry_run=False, now=now)
        summary = summarize_intraday_alert_results(results)

        assert summary["candidate_count"] == 1
        assert summary["sent_count"] == 1
        assert results[0]["status"] == "log_only"
        assert results[0]["template_key"] == "alerts.signal_intraday"
        assert results[0]["trigger"] == "smart_score_threshold"
    finally:
        db.close()


def test_intraday_duplicate_run_does_not_resend(monkeypatch):
    monkeypatch.setenv("EMAIL_ALERT_INTRADAY_ENABLED", "true")
    monkeypatch.setenv("EMAIL_DELIVERY_ENABLED", "true")
    monkeypatch.setenv("EMAIL_PROVIDER", "postmark")
    monkeypatch.delenv("POSTMARK_SERVER_TOKEN", raising=False)
    db = _session()
    try:
        user = _user(db, "intraday-duplicate@example.com")
        _watchlist(db, user)
        now = datetime(2026, 6, 5, 17, 0, tzinfo=timezone.utc)
        _bare_event(db, ts=now - timedelta(minutes=5), impact_score=92, payload={"smart_score": 92})

        first = run_intraday_alert_sweep(db, lookback_minutes=60, dry_run=False, now=now)
        second = run_intraday_alert_sweep(db, lookback_minutes=60, dry_run=False, now=now)
        second_summary = summarize_intraday_alert_results(second)

        assert first[0]["status"] == "log_only"
        assert second[0]["status"] == "skipped"
        assert second[0]["skip_reason"] == "duplicate_alert_already_sent"
        assert second_summary["sent_count"] == 0
        assert db.query(EmailDelivery).count() == 1
    finally:
        db.close()


def test_admin_digest_run_now_dry_run_requires_admin_and_returns_summary():
    db = _session()
    try:
        admin = _user(db, "run-admin@example.com", role="admin")
        user = _user(db, "run-reader@example.com")
        _watchlist(db, user)
        _event(db, ts=datetime.now(timezone.utc) - timedelta(hours=12))

        result = admin_run_email_digest_now(
            AdminDigestRunNowPayload(kind="watchlist_activity", lookback_days=1, limit=10, dry_run=True),
            _request_for_user(admin),
            db,
        )

        assert result["dry_run"] is True
        assert result["summary"]["total"] == 1
        assert result["summary"]["would_send"] == 1
        assert result["items"][0]["item_count"] == 1
        assert db.query(EmailDelivery).count() == 0
    finally:
        db.close()


def test_admin_monitoring_digest_endpoint_targets_ranked_digest(monkeypatch):
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        user = _user(db, "reader@example.com")
        calls = []

        def fake_send(db_arg, user_arg, since_arg, force=False):
            calls.append((user_arg.id, since_arg, force))
            return {
                "id": 123,
                "status": "log_only",
                "provider": "postmark",
                "provider_message_id": None,
                "template_key": "alerts.signal_alert",
                "category": "alerts",
                "to_email": user_arg.email,
                "error": None,
            }

        monkeypatch.setattr("app.routers.accounts.send_signal_alert_digest", fake_send)
        result = admin_send_monitoring_digest_test(
            AdminDigestSendTestPayload(user_id=user.id, lookback_days=7, force=True),
            _request_for_user(admin),
            db,
        )

        assert result["template_key"] == "alerts.signal_alert"
        assert len(calls) == 1
        assert calls[0][0] == user.id
        assert calls[0][2] is True
    finally:
        db.close()
