from __future__ import annotations

from datetime import date, datetime, timezone

import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.models import ConfirmationScoreSnapshot, OutcomeEntry, Security, StrategyDefinition
from app.services.strategy_versions import (
    approve_strategy_version,
    create_strategy_version,
    list_strategy_versions,
    preview_strategy_version,
)


def _session():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False)


def _strategy(db) -> StrategyDefinition:
    strategy = StrategyDefinition(
        slug="versioned-confirmation",
        name="Versioned Confirmation",
        category="cross_source",
        status="draft",
        access_tier="premium",
        methodology_version="v1",
    )
    db.add(strategy)
    db.commit()
    return strategy


def _snapshot(db, security: Security) -> None:
    db.add(
        ConfirmationScoreSnapshot(
            security_id=security.id,
            ticker_at_time="NVDA",
            calculated_at=datetime(2026, 8, 10, 14, tzinfo=timezone.utc),
            market_date=date(2026, 8, 10),
            score=84,
            direction="bullish",
            strength="strong",
            reference_price=125.0,
            active_source_count=4,
            active_sources_json="[\"congress\",\"insider\"]",
            source_contributions_json="{}",
            source_freshness_json="{}",
            input_hash="version-preview",
            methodology_version_id=1,
            calculation_type="live",
        )
    )
    db.flush()
    snapshot = db.execute(select(ConfirmationScoreSnapshot).where(ConfirmationScoreSnapshot.security_id == security.id)).scalar_one()
    at = datetime(2026, 8, 10, 14, 30, tzinfo=timezone.utc)
    db.add(OutcomeEntry(snapshot_id=snapshot.id, security_id=security.id, ticker_at_time=security.symbol, entry_key="version-preview-entry", qualifying_event_at=at, evidence_cutoff_at=at, entry_session_date=date(2026, 8, 10), entry_price=125, entry_price_at=at, entry_price_source="fixture", adjustment_type="split_adjusted_price_return", benchmark_entry_price=100, benchmark_entry_price_at=at, benchmark_price_source="fixture", methodology_version="test", audit_version="test"))
    db.commit()


def test_admin_version_lifecycle_creates_previews_and_approves_immutable_rules():
    SessionLocal = _session()
    db = SessionLocal()
    try:
        _strategy(db)
        version = create_strategy_version(
            db,
            slug="versioned-confirmation",
            rules={
                "candidate_source": "confirmation_score_snapshots",
                "direction": "bullish",
                "min_score": 70,
                "min_active_sources": 3,
                "max_positions": 10,
                "max_snapshot_age_days": 3,
            },
            methodology="Point-in-time confirmation candidate selection.",
        )
        assert version["status"] == "draft"
        assert version["version"] == 1

        security = Security(symbol="NVDA", name="NVIDIA", asset_class="stock", sector="Technology")
        db.add(security)
        db.flush()
        _snapshot(db, security)

        preview = preview_strategy_version(
            db,
            slug="versioned-confirmation",
            version_id=version["id"],
            evaluation_date=date(2026, 8, 10),
        )
        assert preview["mode"] == "dry_run"
        assert preview["qualifyingCount"] == 1
        assert preview["candidates"][0]["symbol"] == "NVDA"

        approved = approve_strategy_version(db, slug="versioned-confirmation", version_id=version["id"])
        assert approved["status"] == "approved"
        assert list_strategy_versions(db, slug="versioned-confirmation")["items"][0]["id"] == version["id"]
    finally:
        db.close()


def test_approval_rejects_non_point_in_time_candidate_sources():
    SessionLocal = _session()
    db = SessionLocal()
    try:
        _strategy(db)
        version = create_strategy_version(db, slug="versioned-confirmation", rules={"candidate_source": "current_scores"})
        with pytest.raises(HTTPException) as error:
            approve_strategy_version(db, slug="versioned-confirmation", version_id=version["id"])
        assert error.value.status_code == 422
    finally:
        db.close()
