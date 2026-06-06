from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from starlette.requests import Request

from app.auth import sign_session_payload
from app.db import Base, ensure_email_notification_schema
from app.models import (
    ConfirmationMonitoringEvent,
    EmailDelivery,
    Event,
    MonitoringAlert,
    NotificationSubscription,
    Security,
    UserAccount,
    Watchlist,
    WatchlistItem,
)
from app.routers.accounts import AdminDigestRunNowPayload, AdminDigestSendTestPayload, admin_run_email_digest_now, admin_send_monitoring_digest_test
from app.services.email_digests import build_monitoring_digest, build_watchlist_activity_digest, send_monitoring_digest, send_signal_alert_digest, send_watchlist_activity_digest
from app.services.email_intraday import run_intraday_alert_sweep, summarize_intraday_alert_results
from app.services.email_templates import seed_default_email_templates


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
    return Request({"type": "http", "method": "GET", "path": "/", "headers": [(b"authorization", f"Bearer {token}".encode())]})


def _user(db, email: str, *, role: str = "user", watchlist_notifications: bool = True) -> UserAccount:
    user = UserAccount(
        email=email,
        first_name="Ada",
        role=role,
        entitlement_tier="premium",
        watchlist_activity_notifications=watchlist_notifications,
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


def _watchlist(db, user: UserAccount, *, active_subscription: bool = True, only_if_new: bool = True) -> Watchlist:
    watchlist = Watchlist(name=f"{user.id} AI", owner_user_id=user.id)
    security = Security(symbol="NVDA", name="Nvidia", asset_class="stock", sector=None)
    db.add_all([watchlist, security])
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
        title=f"{symbol} has fresh monitored activity",
        body="New monitored activity.",
        payload_json=json.dumps({"score": 88, "direction": "bullish"}),
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
        _monitoring_alert(db, user, watchlist)

        result = send_monitoring_digest(db, user, watchlist, datetime.now(timezone.utc) - timedelta(days=1))

        assert result["status"] == "log_only"
        assert result["item_count"] == 1
        assert result["rendered_preview"]["sample_items"][0]["title"] == "NVDA has fresh monitored activity"
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
        assert result["error"] == "no_signal_items"
        assert result["item_count"] == 0
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

        assert row.subject == "Walnut signal digest"
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

        assert row.subject == "Walnut signal digest"
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


def test_admin_monitoring_digest_endpoint_targets_one_send(monkeypatch):
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        user = _user(db, "reader@example.com")
        watchlist = _watchlist(db, user)
        calls = []

        def fake_send(db_arg, user_arg, watchlist_arg, since_arg, force=False):
            calls.append((user_arg.id, watchlist_arg.id, since_arg, force))
            return {
                "id": 123,
                "status": "log_only",
                "provider": "postmark",
                "provider_message_id": None,
                "template_key": "alerts.monitoring_digest",
                "category": "alerts",
                "to_email": user_arg.email,
                "error": None,
            }

        monkeypatch.setattr("app.routers.accounts.send_monitoring_digest", fake_send)
        result = admin_send_monitoring_digest_test(
            AdminDigestSendTestPayload(user_id=user.id, watchlist_id=watchlist.id, lookback_days=7, force=True),
            _request_for_user(admin),
            db,
        )

        assert result["template_key"] == "alerts.monitoring_digest"
        assert len(calls) == 1
        assert calls[0][0] == user.id
        assert calls[0][1] == watchlist.id
        assert calls[0][3] is True
    finally:
        db.close()
