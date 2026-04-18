from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.db import Base
from app.models import ConfirmationMonitoringEvent, ConfirmationMonitoringSnapshot
from app.services.confirmation_monitoring import (
    ConfirmationMonitoringState,
    decide_confirmation_monitoring_event,
    refresh_watchlist_confirmation_monitoring,
)


def _state(
    *,
    ticker: str = "AAPL",
    score: int,
    band: str,
    direction: str,
    source_count: int,
    status: str,
) -> ConfirmationMonitoringState:
    return ConfirmationMonitoringState(
        ticker=ticker,
        score=score,
        band=band,
        direction=direction,
        source_count=source_count,
        status=status,
        observed_at=datetime.now(timezone.utc),
    )


def test_confirmation_monitoring_prefers_multi_source_gain_summary():
    before = _state(score=34, band="weak", direction="neutral", source_count=1, status="Single-source neutral")
    after = _state(score=62, band="strong", direction="bearish", source_count=2, status="2-source bearish confirmation")

    decision = decide_confirmation_monitoring_event(before, after)

    assert decision is not None
    assert decision.event_type == "new_multi_source_confirmation"
    assert decision.title == "AAPL upgraded to 2-source bearish confirmation"
    assert decision.payload["score_before"] == 34
    assert decision.payload["score_after"] == 62


def test_confirmation_monitoring_ignores_tiny_score_moves():
    before = _state(score=51, band="moderate", direction="bullish", source_count=2, status="2-source bullish confirmation")
    after = _state(score=57, band="moderate", direction="bullish", source_count=2, status="2-source bullish confirmation")

    assert decide_confirmation_monitoring_event(before, after) is None


def test_confirmation_monitoring_detects_direction_flip():
    before = _state(score=64, band="strong", direction="bullish", source_count=2, status="2-source bullish confirmation")
    after = _state(score=61, band="strong", direction="bearish", source_count=2, status="2-source bearish confirmation")

    decision = decide_confirmation_monitoring_event(before, after)

    assert decision is not None
    assert decision.event_type == "direction_flipped"
    assert decision.title == "AAPL flipped from bullish to bearish confirmation"


def test_refresh_initializes_snapshot_without_emitting(monkeypatch):
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)

    def fake_bundles(_db, symbols, *, lookback_days=30):
        return {
            symbol: {
                "score": 62,
                "band": "strong",
                "direction": "bullish",
                "status": "2-source bullish confirmation",
                "sources": {
                    "congress": {"present": True, "direction": "bullish"},
                    "insiders": {"present": True, "direction": "bullish"},
                },
            }
            for symbol in symbols
        }

    monkeypatch.setattr("app.services.confirmation_monitoring.get_confirmation_score_bundles_for_tickers", fake_bundles)

    with Session(engine) as db:
        result = refresh_watchlist_confirmation_monitoring(db, user_id=1, watchlist_id=7, tickers=["AAPL"])
        db.commit()

        assert result["initialized"] == 1
        assert result["generated"] == 0
        assert db.query(ConfirmationMonitoringSnapshot).count() == 1
        assert db.query(ConfirmationMonitoringEvent).count() == 0


def test_refresh_emits_once_for_same_after_state_inside_dedupe_window(monkeypatch):
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    now = datetime.now(timezone.utc)

    before = ConfirmationMonitoringSnapshot(
        user_id=1,
        watchlist_id=7,
        ticker="AAPL",
        score=34,
        band="weak",
        direction="bullish",
        source_count=1,
        status="Single-source bullish",
        observed_at=now - timedelta(hours=2),
    )

    def fake_bundles(_db, symbols, *, lookback_days=30):
        return {
            symbol: {
                "score": 66,
                "band": "strong",
                "direction": "bullish",
                "status": "2-source bullish confirmation",
                "sources": {
                    "congress": {"present": True, "direction": "bullish"},
                    "insiders": {"present": True, "direction": "bullish"},
                },
            }
            for symbol in symbols
        }

    monkeypatch.setattr("app.services.confirmation_monitoring.get_confirmation_score_bundles_for_tickers", fake_bundles)

    with Session(engine) as db:
        db.add(before)
        db.commit()

        first = refresh_watchlist_confirmation_monitoring(db, user_id=1, watchlist_id=7, tickers=["AAPL"], now=now)
        second = refresh_watchlist_confirmation_monitoring(db, user_id=1, watchlist_id=7, tickers=["AAPL"], now=now + timedelta(minutes=5))
        db.commit()

        assert first["generated"] == 1
        assert second["generated"] == 0
        assert db.query(ConfirmationMonitoringEvent).count() == 1
