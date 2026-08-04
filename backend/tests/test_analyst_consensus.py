from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base, ensure_analyst_consensus_schema
from app.models import AnalystConsensusSnapshot, AnalystGradeEvent, PriceCache
from app.entitlements import ENTITLEMENTS, require_feature
from app.services.analyst_consensus import (
    build_snapshot_payload,
    compare_consensus_payload,
    consensus_changes,
    current_consensus_payload,
    event_values,
    grade_event_stats,
    implied_upside,
    latest_cached_price,
    recommendation_label,
    target_dispersion,
    total_rating_count,
    upsert_consensus_snapshot,
    upsert_grade_event,
    weighted_sentiment,
)


def _session():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    Base.metadata.create_all(bind=engine)
    ensure_analyst_consensus_schema(engine)
    return SessionLocal, engine


def test_consensus_calculations_handle_counts_targets_and_missing_values():
    counts = {
        "strong_buy_count": 4,
        "buy_count": 6,
        "hold_count": 5,
        "sell_count": 1,
        "strong_sell_count": 0,
    }

    assert total_rating_count(counts) == 16
    assert round(weighted_sentiment(counts) or 0, 4) == 0.8125
    assert recommendation_label(weighted_sentiment(counts), 16) == "Bullish"
    assert round(implied_upside(120, 100) or 0, 2) == 20.0
    assert round(target_dispersion(140, 80, 110) or 0, 2) == 54.55

    missing = {**counts, "buy_count": None}
    assert total_rating_count(missing) is None
    assert weighted_sentiment(missing) is None
    assert implied_upside(None, 100) is None
    assert target_dispersion(120, 80, None) is None


def test_build_snapshot_payload_parses_accessible_fmp_shapes_and_keeps_nulls():
    observed_at = datetime(2026, 8, 4, 21, 0, tzinfo=timezone.utc)
    payload = build_snapshot_payload(
        "aapl",
        grades_summary_rows=[
            {"symbol": "AAPL", "strongBuy": 10, "buy": 12, "hold": 8, "sell": 1, "strongSell": None, "consensus": "Buy"}
        ],
        price_target_consensus_rows=[{"symbol": "AAPL", "targetHigh": 300, "targetLow": 180, "targetConsensus": 240, "targetMedian": 235}],
        price_target_summary_rows=[{"symbol": "AAPL", "allTimeCount": 42, "allTimeAvgPriceTarget": 230}],
        price=None,
        observed_at=observed_at,
    )

    assert payload["symbol"] == "AAPL"
    assert payload["strong_sell_count"] is None
    assert payload["total_rating_count"] is None
    assert payload["weighted_rating_value"] is None
    assert payload["price_target_consensus"] == 240.0
    assert payload["price_target_average"] == 230.0
    assert payload["current_price_at_snapshot"] is None
    assert payload["availability_status"] == "partial"

    zero_count_payload = build_snapshot_payload(
        "AAPL",
        grades_summary_rows=[{"strongBuy": 1, "buy": 1, "hold": 1, "sell": 0, "strongSell": 0}],
        price_target_consensus_rows=[],
        price_target_summary_rows=[],
        observed_at=observed_at,
    )
    assert zero_count_payload["sell_count"] == 0
    assert zero_count_payload["strong_sell_count"] == 0
    assert zero_count_payload["total_rating_count"] == 3


def test_snapshot_upsert_is_idempotent_and_change_uses_nearest_prior_snapshot():
    SessionLocal, _ = _session()
    db = SessionLocal()
    try:
        db.add(PriceCache(symbol="AAPL", date="2026-08-04", close=200.0))
        db.commit()
        price = latest_cached_price(db, "AAPL")

        first_values = build_snapshot_payload(
            "AAPL",
            grades_summary_rows=[{"strongBuy": 2, "buy": 4, "hold": 4, "sell": 0, "strongSell": 0}],
            price_target_consensus_rows=[{"targetHigh": 260, "targetLow": 180, "targetConsensus": 225, "targetMedian": 220}],
            price_target_summary_rows=[{"allTimeCount": 12, "allTimeAvgPriceTarget": 222}],
            price=price,
            observed_at=datetime(2026, 7, 1, 12, 0, tzinfo=timezone.utc),
        )
        second_values = build_snapshot_payload(
            "AAPL",
            grades_summary_rows=[{"strongBuy": 4, "buy": 5, "hold": 3, "sell": 0, "strongSell": 0}],
            price_target_consensus_rows=[{"targetHigh": 300, "targetLow": 200, "targetConsensus": 250, "targetMedian": 245}],
            price_target_summary_rows=[{"allTimeCount": 15, "allTimeAvgPriceTarget": 248}],
            price=price,
            observed_at=datetime(2026, 8, 4, 12, 0, tzinfo=timezone.utc),
        )

        upsert_consensus_snapshot(db, first_values)
        row, created = upsert_consensus_snapshot(db, first_values)
        assert created is False
        assert row.current_price_at_snapshot == 200.0
        upsert_consensus_snapshot(db, second_values)
        db.commit()

        assert db.query(AnalystConsensusSnapshot).count() == 2
        changes = consensus_changes(db, db.query(AnalystConsensusSnapshot).filter_by(snapshot_date=date(2026, 8, 4)).one())
        assert changes["days30"]["comparisonDate"] == "2026-07-01"
        assert changes["days30"]["consensusTargetChange"] == 25.0
    finally:
        db.close()


def test_grade_event_deduplication_and_trailing_stats():
    SessionLocal, _ = _session()
    db = SessionLocal()
    try:
        now = datetime(2026, 8, 4, 12, 0, tzinfo=timezone.utc)
        upgrade = event_values(
            "MSFT",
            {"symbol": "MSFT", "date": "2026-08-01", "gradingCompany": "Firm A", "previousGrade": "Hold", "newGrade": "Buy", "action": "upgrade"},
            ingested_at=now,
        )
        downgrade = event_values(
            "MSFT",
            {"symbol": "MSFT", "date": "2026-07-15", "gradingCompany": "Firm B", "previousGrade": "Buy", "newGrade": "Hold", "action": "Downgrade"},
            ingested_at=now,
        )
        first, created = upsert_grade_event(db, upgrade)
        duplicate, duplicate_created = upsert_grade_event(db, upgrade)
        upsert_grade_event(db, downgrade)
        db.commit()

        assert created is True
        assert duplicate_created is False
        assert first.id == duplicate.id
        assert db.query(AnalystGradeEvent).count() == 2
        stats = grade_event_stats(db, "MSFT", as_of=date(2026, 8, 4))
        assert stats["days30"] == {"upgrades": 1, "downgrades": 1, "netActions": 0}
        assert stats["mostRecentEvent"]["action"] == "Upgrade"
        assert stats["daysSinceMostRecentEvent"] == 3
    finally:
        db.close()


def test_current_payload_reports_unavailable_without_zero_counts():
    SessionLocal, _ = _session()
    db = SessionLocal()
    try:
        payload = current_consensus_payload(db, "NVDA")

        assert payload["symbol"] == "NVDA"
        assert payload["currentSnapshot"] is None
        assert payload["availability"]["status"] == "unavailable"
        assert payload["interpretation"]["combinedLabel"] == "Unavailable"
    finally:
        db.close()


def test_history_feature_is_premium_gated_and_current_payload_is_free_readable():
    SessionLocal, _ = _session()
    db = SessionLocal()
    db.add(
        AnalystConsensusSnapshot(
            symbol="AAPL",
            snapshot_date=date(2026, 8, 4),
            total_rating_count=10,
            weighted_rating_value=0.8,
            recommendation_label="Bullish",
            availability_status="available",
            provider_status="available",
            source="fmp",
            ingested_at=datetime(2026, 8, 4, tzinfo=timezone.utc),
            raw_payload_json="{}",
        )
    )
    db.commit()

    current = current_consensus_payload(db, "AAPL")
    assert current["currentSnapshot"]["recommendationLabel"] == "Bullish"

    with pytest.raises(Exception) as exc_info:
        require_feature(
            ENTITLEMENTS["free"],
            "analyst_consensus_history",
            message="Analyst consensus history is included with Premium.",
        )
    assert getattr(exc_info.value, "status_code", None) == 402
    require_feature(
        ENTITLEMENTS["premium"],
        "analyst_consensus_history",
        message="Analyst consensus history is included with Premium.",
    )
    db.close()


def test_free_current_payload_redacts_consensus_details():
    SessionLocal, _ = _session()
    db = SessionLocal()
    try:
        db.add(
            AnalystConsensusSnapshot(
                symbol="AAPL",
                snapshot_date=date(2026, 8, 4),
                strong_buy_count=2,
                buy_count=4,
                hold_count=3,
                sell_count=1,
                strong_sell_count=0,
                total_rating_count=10,
                weighted_rating_value=0.7,
                recommendation_label="Bullish",
                price_target_high=260,
                price_target_low=180,
                price_target_median=220,
                price_target_consensus=225,
                consensus_implied_upside_pct=12.5,
                availability_status="available",
                provider_status="available",
                source="fmp",
                ingested_at=datetime(2026, 8, 4, tzinfo=timezone.utc),
                raw_payload_json="{}",
            )
        )
        db.commit()

        free_payload = current_consensus_payload(db, "AAPL", include_details=False)
        premium_payload = current_consensus_payload(db, "AAPL", include_details=True)

        assert free_payload["access"]["detailLevel"] == "current_summary"
        assert free_payload["access"]["detailsLocked"] is True
        assert free_payload["currentSnapshot"]["recommendationLabel"] == "Bullish"
        assert free_payload["currentSnapshot"]["consensusImpliedUpsidePct"] == 12.5
        assert "recommendationDistribution" not in free_payload["currentSnapshot"]
        assert "priceTargetRange" not in free_payload["currentSnapshot"]
        assert "changes" not in free_payload
        assert "gradeEventStats" not in free_payload
        assert premium_payload["currentSnapshot"]["recommendationDistribution"]["total"] == 10
        assert premium_payload["currentSnapshot"]["priceTargetRange"]["consensus"] == 225
    finally:
        db.close()


def test_compare_consensus_redacts_details_for_free():
    SessionLocal, _ = _session()
    db = SessionLocal()
    try:
        db.add(
            AnalystConsensusSnapshot(
                symbol="MSFT",
                snapshot_date=date(2026, 8, 4),
                total_rating_count=8,
                weighted_rating_value=0.5,
                recommendation_label="Bullish",
                price_target_consensus=500,
                consensus_implied_upside_pct=10,
                availability_status="available",
                provider_status="available",
                source="fmp",
                ingested_at=datetime(2026, 8, 4, tzinfo=timezone.utc),
                raw_payload_json="{}",
            )
        )
        db.commit()

        free_payload = compare_consensus_payload(db, ["MSFT"], include_details=False)
        premium_payload = compare_consensus_payload(db, ["MSFT"], include_details=True)

        assert free_payload["access"]["detailsLocked"] is True
        assert free_payload["items"]["MSFT"]["summary"]["weightedRatingValue"] is None
        assert "priceTargetRange" not in free_payload["items"]["MSFT"]["currentSnapshot"]
        assert premium_payload["items"]["MSFT"]["summary"]["weightedRatingValue"] == 0.5
    finally:
        db.close()
