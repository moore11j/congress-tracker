from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.db import Base
from app.models import AppSetting, SavedScreen, SavedScreenEvent, SavedScreenSnapshot
from app.services.saved_screen_monitoring import MAX_FETCH_ROWS, refresh_saved_screen_monitoring, saved_screen_payload


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
    status: str | None = None,
) -> dict:
    return {
        "symbol": ticker,
        "confirmation": {
            "score": score,
            "band": band,
            "direction": direction,
            "status": status or f"{source_count}-source {direction}",
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


def test_bullish_saved_screen_monitoring_rejects_inactive_confirmation_entries(monkeypatch):
    engine = _engine()
    current_rows = {"rows": [_row("AAPL", score=64, band="strong", direction="bullish", source_count=2, why_now_state="strong")]}

    def fake_rows(*_args, **_kwargs):
        return current_rows["rows"]

    monkeypatch.setattr("app.services.saved_screen_monitoring.build_screener_rows", fake_rows)

    with Session(engine) as db:
        screen = SavedScreen(
            user_id=1,
            name="Bullish confirmation",
            params_json='{"confirmation_direction":"bullish"}',
        )
        db.add(screen)
        db.commit()

        refresh_saved_screen_monitoring(db, screen, now=datetime.now(timezone.utc) - timedelta(hours=2))
        current_rows["rows"] = [
            _row("AAPL", score=64, band="strong", direction="bullish", source_count=2, why_now_state="strong"),
            _row(
                "VTTHX",
                score=0,
                band="inactive",
                direction="bullish",
                source_count=0,
                why_now_state="inactive",
                status="Inactive",
            ),
        ]
        result = refresh_saved_screen_monitoring(db, screen)
        db.commit()

        assert result["generated"] == 0
        assert all(item["ticker"] != "VTTHX" for item in result["items"])
        assert db.query(SavedScreenSnapshot).filter(SavedScreenSnapshot.ticker == "VTTHX").count() == 0


def test_bullish_saved_screen_legacy_params_are_normalized_for_payload_and_monitoring(monkeypatch):
    engine = _engine()
    captured = {}

    def fake_rows(_db, params, **_kwargs):
        captured["params"] = params
        return [_row("AAPL", score=64, band="strong", direction="bullish", source_count=2, why_now_state="strong")]

    monkeypatch.setattr("app.services.saved_screen_monitoring.build_screener_rows", fake_rows)

    with Session(engine) as db:
        screen = SavedScreen(
            user_id=1,
            name="Bullish confirmation",
            params_json='{"confirmation_score_min":"60"}',
        )
        db.add(screen)
        db.commit()

        payload = saved_screen_payload(screen)
        result = refresh_saved_screen_monitoring(db, screen)

        assert payload["params"]["confirmation_direction"] == "bullish"
        assert payload["params"]["confirmation_score_min"] == "60"
        assert payload["params"]["confirmation_band"] == "strong_plus"
        assert captured["params"].confirmation_direction == "bullish"
        assert captured["params"].confirmation_score_min == 60
        assert captured["params"].confirmation_band == "strong_plus"
        assert result["initialized"] == 1


def test_bullish_saved_screen_monitoring_exits_on_bearish_flip(monkeypatch):
    engine = _engine()
    current_rows = {"rows": [_row("VTTHX", score=64, band="strong", direction="bullish", source_count=2, why_now_state="strong")]}

    def fake_rows(*_args, **_kwargs):
        return current_rows["rows"]

    monkeypatch.setattr("app.services.saved_screen_monitoring.build_screener_rows", fake_rows)

    with Session(engine) as db:
        screen = SavedScreen(
            user_id=1,
            name="Bullish confirmation",
            params_json='{"confirmation_direction":"bullish","confirmation_score_min":"60"}',
        )
        db.add(screen)
        db.commit()

        refresh_saved_screen_monitoring(db, screen, now=datetime.now(timezone.utc) - timedelta(hours=2))
        current_rows["rows"] = [
            _row("VTTHX", score=68, band="strong", direction="bearish", source_count=2, why_now_state="strong")
        ]
        result = refresh_saved_screen_monitoring(db, screen)

        assert result["generated"] == 1
        assert result["items"][0]["event_type"] == "exited_screen"
        assert result["items"][0]["title"] == "VTTHX exited your 'Bullish confirmation' screen"
        assert result["items"][0]["description"] == "Direction changed from bullish to bearish."


def test_saved_screen_refresh_resets_baseline_on_version_change(monkeypatch):
    engine = _engine()
    current_rows = {"rows": [_row("AAPL", score=64, band="strong", direction="bullish", source_count=2, why_now_state="strong")]}

    def fake_rows(*_args, **_kwargs):
        return current_rows["rows"]

    monkeypatch.setattr("app.services.saved_screen_monitoring.build_screener_rows", fake_rows)

    with Session(engine) as db:
        screen = SavedScreen(user_id=1, name="Bullish confirmation", params_json='{"confirmation_direction":"bullish"}')
        db.add(screen)
        db.commit()

        refresh_saved_screen_monitoring(db, screen, now=datetime.now(timezone.utc) - timedelta(hours=2))
        version_key = f"saved_screen_monitoring_baseline_version:{screen.user_id}:{screen.id}"
        db.get(AppSetting, version_key).value = "legacy_confirmation_v1"
        db.commit()

        current_rows["rows"] = [_row("NVDA", score=72, band="strong", direction="bullish", source_count=2, why_now_state="strong")]
        result = refresh_saved_screen_monitoring(db, screen)
        db.commit()

        assert result["generated"] == 0
        assert result["baseline_reset"] is True
        assert result["baseline_reset_reason"] == "version_mismatch"
        assert db.query(SavedScreenEvent).count() == 0
        snapshot_tickers = [row.ticker for row in db.query(SavedScreenSnapshot).order_by(SavedScreenSnapshot.ticker.asc()).all()]
        assert snapshot_tickers == ["NVDA"]


def test_saved_screen_refresh_collapses_large_membership_wave(monkeypatch):
    engine = _engine()
    current_rows = {
        "rows": [
            _row(f"A{i:03d}", score=64, band="strong", direction="bullish", source_count=2, why_now_state="strong")
            for i in range(30)
        ]
    }

    def fake_rows(*_args, **_kwargs):
        return current_rows["rows"]

    monkeypatch.setattr("app.services.saved_screen_monitoring.build_screener_rows", fake_rows)

    with Session(engine) as db:
        screen = SavedScreen(user_id=1, name="Bullish confirmation", params_json='{"confirmation_direction":"bullish"}')
        db.add(screen)
        db.commit()

        refresh_saved_screen_monitoring(db, screen, now=datetime.now(timezone.utc) - timedelta(hours=2))
        current_rows["rows"] = [
            _row(f"N{i:03d}", score=68, band="strong", direction="bullish", source_count=2, why_now_state="strengthening")
            for i in range(30)
        ]
        result = refresh_saved_screen_monitoring(db, screen)
        db.commit()

        assert result["generated"] == 1
        assert result["baseline_reset"] is True
        assert result["baseline_reset_reason"] == "membership_wave"
        assert result["items"][0]["event_type"] == "screen_refreshed"
        assert db.query(SavedScreenEvent).count() == 1
        snapshot_tickers = [row.ticker for row in db.query(SavedScreenSnapshot).order_by(SavedScreenSnapshot.ticker.asc()).all()]
        assert snapshot_tickers == [f"N{i:03d}" for i in range(30)]
