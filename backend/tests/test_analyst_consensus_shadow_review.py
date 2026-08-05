from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.db import Base
from app.models import AnalystConsensusSnapshot, ConfirmationMonitoringEvent, PriceCache
from app.services.analyst_consensus_shadow_review import (
    analyst_consensus_shadow_component_score,
    shadow_review_payload,
)
from app.services.confirmation_score import get_confirmation_score_bundle_for_ticker


def _snapshot(
    symbol: str,
    snapshot_date: date,
    *,
    weighted_rating_value: float,
    upside: float,
    total_rating_count: int = 12,
) -> AnalystConsensusSnapshot:
    return AnalystConsensusSnapshot(
        symbol=symbol,
        provider_symbol=symbol,
        snapshot_date=snapshot_date,
        total_rating_count=total_rating_count,
        weighted_rating_value=weighted_rating_value,
        recommendation_label="Bullish" if weighted_rating_value > 0 else "Bearish",
        price_target_consensus=120,
        current_price_at_snapshot=100,
        consensus_implied_upside_pct=upside,
        availability_status="available",
        provider_status="available",
        source="fmp",
        methodology_version="analyst_consensus_v1",
        raw_payload_json="{}",
        ingested_at=datetime.combine(snapshot_date, datetime.min.time(), tzinfo=timezone.utc),
    )


def _price(symbol: str, day: date, close: float) -> PriceCache:
    return PriceCache(symbol=symbol, date=day.isoformat(), close=close)


def test_shadow_review_backtests_cached_forward_returns_without_activation():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    start = date(2026, 1, 1)

    with Session(engine) as db:
        db.add_all(
            [
                _snapshot("AAA", start, weighted_rating_value=1.2, upside=22),
                _snapshot("BBB", start, weighted_rating_value=-1.1, upside=-18),
                _snapshot("CCC", start, weighted_rating_value=0.1, upside=1),
                _price("AAA", start, 100),
                _price("AAA", start + timedelta(days=30), 116),
                _price("BBB", start, 100),
                _price("BBB", start + timedelta(days=30), 88),
                _price("CCC", start, 100),
                _price("CCC", start + timedelta(days=30), 101),
                ConfirmationMonitoringEvent(
                    user_id=1,
                    watchlist_id=1,
                    ticker="AAA",
                    event_type="score_changed",
                    title="AAA score",
                    score_before=60,
                    score_after=61,
                    band_before="strong",
                    band_after="strong",
                    direction_before="bullish",
                    direction_after="bullish",
                    source_count_before=2,
                    source_count_after=2,
                    created_at=datetime(2026, 1, 1, 12, tzinfo=timezone.utc),
                ),
                ConfirmationMonitoringEvent(
                    user_id=1,
                    watchlist_id=1,
                    ticker="BBB",
                    event_type="score_changed",
                    title="BBB score",
                    score_before=42,
                    score_after=39,
                    band_before="moderate",
                    band_after="weak",
                    direction_before="neutral",
                    direction_after="bearish",
                    source_count_before=1,
                    source_count_after=1,
                    created_at=datetime(2026, 1, 1, 12, tzinfo=timezone.utc),
                ),
                ConfirmationMonitoringEvent(
                    user_id=1,
                    watchlist_id=1,
                    ticker="CCC",
                    event_type="score_changed",
                    title="CCC score",
                    score_before=50,
                    score_after=50,
                    band_before="moderate",
                    band_after="moderate",
                    direction_before="neutral",
                    direction_after="neutral",
                    source_count_before=1,
                    source_count_after=1,
                    created_at=datetime(2026, 1, 1, 12, tzinfo=timezone.utc),
                ),
            ]
        )
        db.commit()

        payload = shadow_review_payload(
            db,
            days=60,
            horizon_days=30,
            min_backtest_samples=3,
            min_backtest_symbols=3,
            max_confirmation_correlation=1.0,
            as_of=date(2026, 2, 15),
        )

    assert payload["activationState"] == "shadow"
    assert payload["includedInLiveScore"] is False
    assert payload["coverage"]["sampleCount"] == 3
    assert payload["backtest"]["status"] == "passed"
    assert payload["backtest"]["bullishMinusBearishReturnPct"] == 28.0
    assert payload["activationReview"]["canActivateLiveWeight"] is False
    assert payload["activationReview"]["recommendation"] == "keep_shadow_only"
    assert payload["doubleCountingReview"]["status"] == "manual_review_required"


def test_confirmation_bundle_exposes_shadow_component_without_live_source_weight():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    snapshot_date = date(2026, 1, 1)

    with Session(engine) as db:
        db.add(_snapshot("AAA", snapshot_date, weighted_rating_value=1.0, upside=20))
        db.commit()

        bundle = get_confirmation_score_bundle_for_ticker(db, "AAA", lookback_days=30)

    expected_score = analyst_consensus_shadow_component_score(
        {"weightedRatingValue": 1.0, "consensusImpliedUpsidePct": 20}
    )
    shadow = bundle["analyst_consensus_shadow"]
    assert bundle["score"] == 0
    assert shadow["activation_state"] == "shadow"
    assert shadow["included_in_score"] is False
    assert shadow["live_weight_assigned"] is False
    assert shadow["component_score"] == expected_score
    assert shadow["review_gates"]["explicit_live_flag"]["status"] == "disabled"
