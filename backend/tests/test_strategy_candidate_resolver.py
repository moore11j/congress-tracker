from __future__ import annotations

from datetime import date, datetime, timezone

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.models import ConfirmationScoreSnapshot, Event, PriceCache, Security, StrategyDefinition, StrategyVersion
from app.services.strategy_candidate_resolver import UnsupportedStrategyCandidateSource, resolve_strategy_candidates


def _session():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False)


def _version(db, rules: str):
    strategy = StrategyDefinition(slug="confirmation-live", name="Confirmation Live", category="cross_source", status="draft", access_tier="premium", methodology_version="v1")
    db.add(strategy)
    db.flush()
    version = StrategyVersion(strategy_id=strategy.id, version=1, status="approved", rules_json=rules)
    db.add(version)
    db.commit()
    return version


def _snapshot(db, security: Security, *, snapshot_id: int, symbol: str, score: int, sources: int, market_date: date, calculated_at: datetime, price: float = 100):
    db.add(
        ConfirmationScoreSnapshot(
            id=snapshot_id,
            security_id=security.id,
            ticker_at_time=symbol,
            calculated_at=calculated_at,
            market_date=market_date,
            score=score,
            direction="bullish",
            strength="strong",
            reference_price=price,
            active_source_count=sources,
            active_sources_json="[\"congress\"]",
            source_contributions_json="{}",
            source_freshness_json="{}",
            input_hash=f"hash-{snapshot_id}",
            methodology_version_id=1,
            calculation_type="live",
        )
    )


def test_confirmation_resolver_uses_only_visible_fresh_snapshots_and_equal_weights():
    SessionLocal = _session()
    db = SessionLocal()
    try:
        version = _version(db, '{"candidate_source":"confirmation_score_snapshots","min_score":70,"min_active_sources":2,"max_positions":2,"max_snapshot_age_days":3}')
        nvda = Security(symbol="NVDA", name="NVIDIA", asset_class="stock", sector="Technology")
        msft = Security(symbol="MSFT", name="Microsoft", asset_class="stock", sector="Technology")
        stale = Security(symbol="OLD", name="Old", asset_class="stock", sector=None)
        db.add_all([nvda, msft, stale])
        db.flush()
        day = date(2026, 8, 10)
        _snapshot(db, nvda, snapshot_id=1, symbol="NVDA", score=80, sources=3, market_date=day, calculated_at=datetime(2026, 8, 10, 14, tzinfo=timezone.utc))
        _snapshot(db, nvda, snapshot_id=2, symbol="NVDA", score=95, sources=4, market_date=day, calculated_at=datetime(2026, 8, 10, 18, tzinfo=timezone.utc))
        _snapshot(db, msft, snapshot_id=3, symbol="MSFT", score=85, sources=2, market_date=day, calculated_at=datetime(2026, 8, 10, 14, tzinfo=timezone.utc))
        _snapshot(db, stale, snapshot_id=4, symbol="OLD", score=99, sources=5, market_date=date(2026, 8, 1), calculated_at=datetime(2026, 8, 1, 14, tzinfo=timezone.utc))
        db.commit()

        resolution = resolve_strategy_candidates(
            db,
            strategy_version_id=version.id,
            evaluation_date=day,
            available_at=datetime(2026, 8, 10, 15, tzinfo=timezone.utc),
        )
        assert [candidate.symbol for candidate in resolution.candidates] == ["MSFT", "NVDA"]
        assert [candidate.score for candidate in resolution.candidates] == [85.0, 80.0]
        assert [candidate.weight_pct for candidate in resolution.candidates] == [50.0, 50.0]
        assert resolution.candidates[1].qualification_snapshot["confirmationSnapshotId"] == 1
    finally:
        db.close()


def test_resolver_rejects_unapproved_or_unknown_sources():
    SessionLocal = _session()
    db = SessionLocal()
    try:
        version = _version(db, '{"candidate_source":"congress_filings"}')
        with pytest.raises(UnsupportedStrategyCandidateSource):
            resolve_strategy_candidates(db, strategy_version_id=version.id, evaluation_date=date(2026, 8, 10))
    finally:
        db.close()


def test_congress_member_resolver_replays_only_publicly_ingested_post_activation_filings():
    SessionLocal = _session()
    db = SessionLocal()
    try:
        strategy = StrategyDefinition(slug="member-live", name="Member Live", category="congress", status="published", access_tier="premium", methodology_version="v1")
        db.add(strategy)
        db.flush()
        version = StrategyVersion(
            strategy_id=strategy.id,
            version=1,
            status="active",
            effective_from=date(2026, 8, 10),
            rules_json='{"candidate_source":"congress_member_disclosures","member_bioguide_id":"F000110"}',
        )
        db.add_all(
            [
                version,
                Security(symbol="NVDA", name="NVIDIA", asset_class="stock", sector="Technology"),
                Security(symbol="MSFT", name="Microsoft", asset_class="stock", sector="Technology"),
                PriceCache(symbol="NVDA", date="2026-08-11", close=190.0, adjusted_close=190.0),
                PriceCache(symbol="MSFT", date="2026-08-11", close=480.0, adjusted_close=480.0),
                Event(
                    event_type="congress_trade",
                    ts=datetime(2026, 8, 10, 12, tzinfo=timezone.utc),
                    event_date=datetime(2026, 8, 10, 12, tzinfo=timezone.utc),
                    symbol="NVDA",
                    source="test",
                    payload_json='{"trade_date":"2026-08-01","filing_date":"2026-08-10","asset_class":"stock"}',
                    member_bioguide_id="F000110",
                    member_name="Cleo Fields",
                    trade_type="purchase",
                    amount_min=1001,
                    amount_max=15000,
                    created_at=datetime(2026, 8, 10, 18, tzinfo=timezone.utc),
                ),
                Event(
                    event_type="congress_trade",
                    ts=datetime(2026, 8, 10, 12, tzinfo=timezone.utc),
                    event_date=datetime(2026, 8, 10, 12, tzinfo=timezone.utc),
                    symbol="MSFT",
                    source="test",
                    payload_json='{"trade_date":"2026-08-01","filing_date":"2026-08-10","asset_class":"stock"}',
                    member_bioguide_id="F000110",
                    member_name="Cleo Fields",
                    trade_type="purchase",
                    amount_min=1001,
                    amount_max=15000,
                    created_at=datetime(2026, 8, 10, 23, tzinfo=timezone.utc),
                ),
            ]
        )
        db.commit()

        resolution = resolve_strategy_candidates(
            db,
            strategy_version_id=version.id,
            evaluation_date=date(2026, 8, 10),
            available_at=datetime(2026, 8, 10, 20, tzinfo=timezone.utc),
        )

        assert resolution.source == "congress_member_disclosures"
        assert resolution.universe_count == 1
        assert [candidate.symbol for candidate in resolution.candidates] == ["NVDA"]
        assert resolution.candidates[0].effective_date == date(2026, 8, 11)
        assert resolution.candidates[0].entry_price == 190.0
        assert resolution.candidates[0].qualification_snapshot["memberBioguideId"] == "F000110"
    finally:
        db.close()
