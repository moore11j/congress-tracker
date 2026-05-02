from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from app.db import Base
from app.models import ConfirmationMonitoringEvent, ConfirmationMonitoringSnapshot, Security, UserAccount, Watchlist, WatchlistItem
from app.services.confirmation_monitoring import (
    ConfirmationMonitoringState,
    decide_confirmation_monitoring_event,
    refresh_all_monitored_watchlist_confirmation_monitoring,
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


def test_scheduled_refresh_checks_monitored_watchlists_with_per_watchlist_commits(monkeypatch, caplog):
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
        future=True,
    )
    Base.metadata.create_all(bind=engine)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    now = datetime.now(timezone.utc)

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

    with SessionLocal() as db:
        user = UserAccount(email="user@example.com", entitlement_tier="premium")
        security = Security(symbol="AAPL", name="APPLE INC", asset_class="stock", sector=None)
        watchlist = Watchlist(name="Core", owner_user_id=1)
        db.add_all([user, security, watchlist])
        db.flush()
        watchlist.owner_user_id = user.id
        db.add(WatchlistItem(watchlist_id=watchlist.id, security_id=security.id))
        db.add(
            ConfirmationMonitoringSnapshot(
                user_id=user.id,
                watchlist_id=watchlist.id,
                ticker="AAPL",
                score=34,
                band="weak",
                direction="bullish",
                source_count=1,
                status="Single-source bullish",
                observed_at=now - timedelta(hours=2),
            )
        )
        db.commit()

    with caplog.at_level("INFO", logger="app.services.confirmation_monitoring"):
        first = refresh_all_monitored_watchlist_confirmation_monitoring(SessionLocal, now=now)
        second = refresh_all_monitored_watchlist_confirmation_monitoring(SessionLocal, now=now + timedelta(minutes=5))

    with SessionLocal() as db:
        assert db.query(ConfirmationMonitoringEvent).count() == 1

    assert first["watchlists_checked"] == 1
    assert first["changes_created"] == 1
    assert second["watchlists_checked"] == 1
    assert second["changes_created"] == 0
    assert "scheduled_monitor_refresh_started" in caplog.text
    assert "watchlists_checked=1" in caplog.text
    assert "changes_created=1" in caplog.text
