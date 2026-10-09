"""Unavailable calendar coverage must not become a claim of no upcoming events."""
from datetime import date, datetime, timezone
import json

import pytest
from sqlalchemy import select

from app.models import NotificationSubscription, TickerContentCache
from app.services import email_digests as digests, event_calendar as calendar
from test_email_digests import _session, _user, _watchlist


@pytest.fixture
def account(monkeypatch):
    monkeypatch.setenv("FMP_PROVIDER_DISABLED", "false")
    def forbidden(*args, **kwargs):
        pytest.fail("Calendar validation must not fetch providers or send email")
    monkeypatch.setattr(calendar, "request_fmp_json", forbidden)
    monkeypatch.setattr(digests, "_send_digest", forbidden)
    db = _session()
    user = _user(db, "calendar-coverage@example.test", tier="premium")
    _watchlist(db, user)
    yield db, user
    db.close()


def digest(db, user):
    return digests.build_signal_alert_digest(db, user,
        datetime(2026, 10, 8, tzinfo=timezone.utc),
        window_end=datetime(2026, 10, 9, tzinfo=timezone.utc))


def cached_calendar(db, user, items):
    key = calendar._calendar_cache_key(user.id, "watchlist", date(2026, 10, 9), date(2026, 10, 16),
        calendar.watchlist_provider_symbols_for_user(db, user.id))
    calendar._store_calendar_cache(db, key, items)
    return db.scalar(select(TickerContentCache).where(TickerContentCache.content_type == "event_calendar"))


def test_missing_cache_is_unavailable_but_successful_empty_cache_is_empty(account):
    db, user = account
    first = digest(db, user)
    for key in ("upcoming_events_text", "upcoming_events_html"):
        assert "coverage is currently unavailable" in first.context[key]
        assert "No upcoming" not in first.context[key]
    row = cached_calendar(db, user, [])
    assert row is not None
    second = digest(db, user)
    for key in ("upcoming_events_text", "upcoming_events_html"):
        assert "No upcoming watchlist calendar dates" in second.context[key]
        assert "unavailable" not in second.context[key]


def test_disabled_provider_cannot_reuse_old_calendar_or_destroy_history(account, monkeypatch):
    db, user = account
    row = cached_calendar(db, user, [{"id": "old", "kind": "earnings", "date": "2026-10-12", "symbol": "NVDA", "title": "Old earnings date"}])
    before = (row.id, row.payload_json, row.fetched_at, row.source)
    monkeypatch.setenv("FMP_PROVIDER_DISABLED", "1")
    for live in (True, False):
        result = calendar.upcoming_event_calendar_items(db, user, start=date(2026, 10, 9), end=date(2026, 10, 16),
            allow_live_fetch=live, kinds=("earnings",))
        assert result.items == []
        assert result.errors == [{"kind": "earnings", "reason": "provider_disabled"}]
    result = calendar.fetch_event_calendar(db, user, start=date(2026, 10, 9), end=date(2026, 10, 16))
    assert result.items == [] and len(result.errors) == 5
    built = digest(db, user)
    assert "Old earnings date" not in built.context["upcoming_events_text"]
    assert "coverage is currently unavailable" in built.context["upcoming_events_text"]
    db.refresh(row)
    assert before == (row.id, row.payload_json, row.fetched_at, row.source)


@pytest.mark.parametrize("active,kinds", [(False, ["earnings"]), (True, [])])
def test_opted_out_calendar_has_no_false_empty_claim(account, monkeypatch, active, kinds):
    db, user = account
    db.add(NotificationSubscription(email=user.email, source_type="event_calendar", source_id="watchlist",
        source_name="Calendar", source_payload_json=json.dumps({"calendar_kinds": kinds}), active=active,
        frequency="daily", only_if_new=False))
    db.commit()
    monkeypatch.setattr(digests, "upcoming_event_calendar_items", lambda *a, **k: pytest.fail("Opted-out calendar fetched"))
    built = digest(db, user)
    assert built.context["upcoming_events_text"] == built.context["upcoming_events_html"] == ""


def test_partial_coverage_keeps_verified_items_and_labels_missing_kinds(account, monkeypatch):
    db, user = account
    monkeypatch.setattr(digests, "upcoming_event_calendar_items", lambda *a, **k: calendar.CalendarFetchResult(
        items=[{"kind": "split", "date": "2026-10-12", "symbol": "NVDA", "title": "Verified split"}],
        errors=[{"kind": "earnings", "reason": "provider_disabled"}]))
    built = digest(db, user)
    for key in ("upcoming_events_text", "upcoming_events_html"):
        assert "Verified split" in built.context[key]
        assert "unavailable for: Earnings" in built.context[key]
        assert "No upcoming" not in built.context[key]


def test_errors_for_unselected_kinds_do_not_report_failed_selected_coverage(account, monkeypatch):
    db, user = account
    monkeypatch.setattr(calendar, "fetch_event_calendar", lambda *a, **k: calendar.CalendarFetchResult(
        items=[], errors=[{"kind": "earnings", "reason": "provider_disabled"}]))
    result = calendar.upcoming_event_calendar_items(db, user, start=date(2026, 10, 9), end=date(2026, 10, 16), kinds=("split",))
    assert result.items == result.errors == []


def test_calendar_exception_is_coverage_failure(account, monkeypatch):
    db, user = account
    def fail(*args, **kwargs):
        raise RuntimeError("test failure")
    monkeypatch.setattr(digests, "upcoming_event_calendar_items", fail)
    built = digest(db, user)
    assert "coverage is currently unavailable" in built.context["upcoming_events_text"]
    assert "No upcoming" not in built.context["upcoming_events_html"]
