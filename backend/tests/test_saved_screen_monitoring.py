from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.db import Base
from app.models import SavedScreen, SavedScreenEvent, SavedScreenSnapshot
from app.services.saved_screen_monitoring import MAX_FETCH_ROWS, refresh_saved_screen_monitoring


def _engine():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    return engine


def _row(
    ticker: str,
    *,
    score: int,
    band: str,
    direction: str,
    source_count: int,
    why_now_state: str,
) -> dict:
    return {
        "symbol": ticker,
        "confirmation": {
            "score": score,
            "band": band,
            "direction": direction,
            "status": f"{source_count}-source {direction}",
            "source_count": source_count,
        },
        "why_now": {"state": why_now_state},
    }


def test_saved_screen_refresh_initializes_without_emitting(monkeypatch):
    engine = _engine()
    monkeypatch.setattr(
        "app.services.saved_screen_monitoring.build_screener_rows",
        lambda *_args, **_kwargs: [_row("AAPL", score=66, band="strong", direction="bullish", source_count=2, why_now_state="strengthening")],
    )

    with Session(engine) as db:
        screen = SavedScreen(user_id=1, name="Large Cap Activity", params_json="{}")
        db.add(screen)
        db.commit()

        result = refresh_saved_screen_monitoring(db, screen)
        db.commit()

        assert result["initialized"] == 1
        assert result["generated"] == 0
        assert db.query(SavedScreenSnapshot).count() == 1
        assert db.query(SavedScreenEvent).count() == 0


def test_saved_screen_refresh_emits_entry_and_exit_events(monkeypatch):
    engine = _engine()
    current_rows = {"rows": [_row("AAPL", score=64, band="strong", direction="bullish", source_count=2, why_now_state="strong")]}

    def fake_rows(*_args, **_kwargs):
        return current_rows["rows"]

    monkeypatch.setattr("app.services.saved_screen_monitoring.build_screener_rows", fake_rows)

    with Session(engine) as db:
        screen = SavedScreen(user_id=1, name="Large Cap Activity", params_json="{}")
        db.add(screen)
        db.commit()

        refresh_saved_screen_monitoring(db, screen, now=datetime.now(timezone.utc) - timedelta(hours=2))
        current_rows["rows"] = [_row("NVDA", score=71, band="strong", direction="bullish", source_count=2, why_now_state="strengthening")]
        result = refresh_saved_screen_monitoring(db, screen)
        db.commit()

        event_types = [item["event_type"] for item in result["items"]]
        assert result["generated"] == 2
        assert "entered_screen" in event_types
        assert "exited_screen" in event_types


def test_saved_screen_refresh_prioritizes_direction_change(monkeypatch):
    engine = _engine()
    current_rows = {"rows": [_row("AAPL", score=62, band="strong", direction="bullish", source_count=2, why_now_state="early")]}

    def fake_rows(*_args, **_kwargs):
        return current_rows["rows"]

    monkeypatch.setattr("app.services.saved_screen_monitoring.build_screener_rows", fake_rows)

    with Session(engine) as db:
        screen = SavedScreen(user_id=1, name="Large Cap Activity", params_json="{}")
        db.add(screen)
        db.commit()

        refresh_saved_screen_monitoring(db, screen, now=datetime.now(timezone.utc) - timedelta(hours=2))
        current_rows["rows"] = [_row("AAPL", score=82, band="exceptional", direction="bearish", source_count=2, why_now_state="strengthening")]
        result = refresh_saved_screen_monitoring(db, screen)

        assert result["generated"] == 1
        assert result["items"][0]["event_type"] == "direction_changed"


def test_saved_screen_refresh_emits_why_now_change_when_other_state_is_flat(monkeypatch):
    engine = _engine()
    current_rows = {"rows": [_row("AAPL", score=58, band="moderate", direction="bullish", source_count=1, why_now_state="early")]}

    def fake_rows(*_args, **_kwargs):
        return current_rows["rows"]

    monkeypatch.setattr("app.services.saved_screen_monitoring.build_screener_rows", fake_rows)

    with Session(engine) as db:
        screen = SavedScreen(user_id=1, name="Large Cap Activity", params_json="{}")
        db.add(screen)
        db.commit()

        refresh_saved_screen_monitoring(db, screen, now=datetime.now(timezone.utc) - timedelta(hours=2))
        current_rows["rows"] = [_row("AAPL", score=58, band="moderate", direction="bullish", source_count=1, why_now_state="strengthening")]
        result = refresh_saved_screen_monitoring(db, screen)

        assert result["generated"] == 1
        assert result["items"][0]["event_type"] == "why_now_changed"


def test_saved_screen_refresh_suppresses_membership_noise_when_bounded(monkeypatch):
    engine = _engine()
    current_rows = {"rows": [_row("AAPL", score=64, band="strong", direction="bullish", source_count=2, why_now_state="strong")]}

    def fake_rows(*_args, **_kwargs):
        return current_rows["rows"]

    monkeypatch.setattr("app.services.saved_screen_monitoring.build_screener_rows", fake_rows)

    with Session(engine) as db:
        screen = SavedScreen(user_id=1, name="Large Cap Activity", params_json="{}")
        db.add(screen)
        db.commit()

        refresh_saved_screen_monitoring(db, screen, now=datetime.now(timezone.utc) - timedelta(hours=2))
        current_rows["rows"] = [
            _row(f"T{i:03d}", score=40, band="moderate", direction="bullish", source_count=1, why_now_state="early")
            for i in range(MAX_FETCH_ROWS)
        ]
        result = refresh_saved_screen_monitoring(db, screen)

        assert result["membership_changes_allowed"] is False
        assert result["generated"] == 0
