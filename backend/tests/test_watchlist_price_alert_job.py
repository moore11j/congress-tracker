from datetime import datetime, timezone
from unittest.mock import MagicMock

import app.jobs.run_watchlist_price_alerts as job


def test_price_cycle_evaluates_and_delivers_independently_with_close_grace(monkeypatch):
    monkeypatch.setenv("EMAIL_ALERT_INTRADAY_ENABLED", "true")
    monkeypatch.setenv("EMAIL_ALERT_SCHEDULE_DRY_RUN", "false")
    monkeypatch.delenv("BACKGROUND_JOBS_PAUSED", raising=False)
    refresh = MagicMock(return_value={"failures": 0, "custom_rules_triggered": 1})
    send = MagicMock(return_value=[{"status": "sent"}])
    monkeypatch.setattr(job, "refresh_all_monitored_watchlist_confirmation_monitoring", refresh)
    monkeypatch.setattr(job, "run_intraday_alert_sweep", send)
    monkeypatch.setattr(job, "SessionLocal", MagicMock())
    reference_refresh = MagicMock(return_value={"unavailable": 0})
    monkeypatch.setattr(job, "refresh_daily_price_references", reference_refresh)
    result = job.run_price_alert_cycle(now=datetime(2026, 9, 15, 20, 5, tzinfo=timezone.utc))
    assert result["delivery"]["sent_count"] == 1
    reference_refresh.assert_called_once()
    assert refresh.call_args.kwargs == {"refresh_quotes": True, "price_rules_only": True}
    assert send.call_args.kwargs["custom_price_only"] is True
    assert send.call_args.kwargs["market_hours_only"] is False
    assert 960 <= send.call_args.kwargs["lookback_minutes"] <= 970


def test_price_cycle_does_not_run_at_night_or_when_disabled(monkeypatch):
    refresh = MagicMock()
    monkeypatch.setattr(job, "refresh_all_monitored_watchlist_confirmation_monitoring", refresh)
    assert job.run_price_alert_cycle(now=datetime(2026, 9, 16, 2, tzinfo=timezone.utc))["reason"] == "outside_price_alert_window"
    monkeypatch.setenv("EMAIL_ALERT_INTRADAY_ENABLED", "false")
    assert job.run_price_alert_cycle(now=datetime(2026, 9, 15, 18, tzinfo=timezone.utc))["reason"] == "intraday_disabled"
    refresh.assert_not_called()


def test_missing_reference_is_reported_without_blocking_other_deliveries(monkeypatch):
    monkeypatch.setenv("EMAIL_ALERT_INTRADAY_ENABLED", "true")
    monkeypatch.delenv("BACKGROUND_JOBS_PAUSED", raising=False)
    monkeypatch.setattr(job, "SessionLocal", MagicMock())
    monkeypatch.setattr(job, "refresh_daily_price_references", MagicMock(return_value={"unavailable": 1}))
    monkeypatch.setattr(job, "refresh_all_monitored_watchlist_confirmation_monitoring", MagicMock(return_value={"failures": 0}))
    send = MagicMock(return_value=[{"status": "sent"}])
    monkeypatch.setattr(job, "run_intraday_alert_sweep", send)
    result = job.run_price_alert_cycle(now=datetime(2026, 9, 16, 18, tzinfo=timezone.utc))
    assert result["status"] == "failed"
    assert result["references"]["unavailable"] == 1
    assert result["delivery"]["sent_count"] == 1
