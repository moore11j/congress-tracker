from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from app.db import Base, ensure_outcome_ledger_schema
from app.models import ConfirmationMethodologyVersion, ConfirmationMonitoringEvent, ConfirmationScoreSnapshot, PriceCache
from app.backfill_outcome_ledger_history import backfill_outcome_ledger_history
from app.services import outcome_ledger as outcome_ledger_module
from app.services.outcome_ledger import (
    capture_live_confirmation_score_snapshot,
    current_confirmation_methodology,
    input_hash_for_confirmation_bundle,
    list_outcome_snapshots,
)
from app.seed_outcome_ledger_demo import seed_hydrated_outcome_ledger_demo_snapshots, seed_outcome_ledger_demo_snapshots


def _engine():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    ensure_outcome_ledger_schema(engine)
    return engine


def _bundle(score: int = 67) -> dict:
    return {
        "ticker": "CRM",
        "score": score,
        "band": "strong" if score >= 60 else "moderate",
        "direction": "bearish",
        "status": "2-source bearish confirmation",
        "classification_version": "confirmation_direction_v3",
        "active_sources": ["insiders", "price_volume"],
        "sources": {
            "insiders": {
                "present": True,
                "direction": "bearish",
                "strength": 82,
                "quality": 80,
                "freshness_days": 4,
                "score_contribution": 0,
                "label": "Insiders",
            },
            "price_volume": {
                "present": True,
                "direction": "bearish",
                "strength": 74,
                "quality": 76,
                "freshness_days": 1,
                "score_contribution": 0,
                "label": "Price / Volume",
            },
        },
    }


def test_methodology_seed_and_single_current_version():
    engine = _engine()
    with Session(engine) as db:
        current = current_confirmation_methodology(db)
        db.add(
            ConfirmationMethodologyVersion(
                version="confirmation-v2-test",
                description="Future test version",
                configuration_json=json.dumps({"test": True}),
                code_commit_sha="test",
                deployed_at=datetime.now(timezone.utc),
                is_current=False,
            )
        )
        db.commit()

        current_rows = db.execute(
            select(ConfirmationMethodologyVersion).where(ConfirmationMethodologyVersion.is_current.is_(True))
        ).scalars().all()
        assert current.version == "confirmation-v1"
        assert [row.version for row in current_rows] == ["confirmation-v1"]


def test_live_capture_dedupes_identical_input_and_preserves_new_input():
    engine = _engine()
    with Session(engine) as db:
        db.add(PriceCache(symbol="CRM", date="2026-08-04", close=101.25, price_source="test"))
        db.commit()

        first = capture_live_confirmation_score_snapshot(db, "CRM", _bundle(67), calculated_at=datetime(2026, 8, 4, 15, tzinfo=timezone.utc))
        duplicate = capture_live_confirmation_score_snapshot(db, "CRM", _bundle(67), calculated_at=datetime(2026, 8, 4, 16, tzinfo=timezone.utc))
        changed = capture_live_confirmation_score_snapshot(db, "CRM", _bundle(68), calculated_at=datetime(2026, 8, 4, 17, tzinfo=timezone.utc))

        rows = db.execute(select(ConfirmationScoreSnapshot).order_by(ConfirmationScoreSnapshot.id)).scalars().all()
        assert first is not None
        assert duplicate is not None
        assert changed is not None
        assert duplicate.id == first.id
        assert len(rows) == 2
        assert rows[0].score == 67
        assert rows[1].score == 68
        assert rows[0].calculation_type == "live"
        assert rows[0].reference_price == 101.25
        assert json.loads(rows[0].active_sources_json) == ["insiders", "price_volume"]


def test_input_hash_is_deterministic_and_excludes_calculation_time():
    engine = _engine()
    with Session(engine) as db:
        methodology = current_confirmation_methodology(db)
        left = input_hash_for_confirmation_bundle(_bundle(67), methodology)
        right = input_hash_for_confirmation_bundle({**_bundle(67), "generated_at": "later"}, methodology)
        changed = input_hash_for_confirmation_bundle(_bundle(69), methodology)

        assert left == right
        assert left != changed


def test_snapshots_and_used_methodologies_are_immutable():
    engine = _engine()
    with Session(engine) as db:
        db.add(PriceCache(symbol="CRM", date="2026-08-04", close=101.25))
        db.commit()
        snapshot = capture_live_confirmation_score_snapshot(db, "CRM", _bundle(), calculated_at=datetime(2026, 8, 4, 15, tzinfo=timezone.utc))
        assert snapshot is not None

        snapshot.score = 12
        with pytest.raises(ValueError):
            db.commit()


def test_demo_seeder_populates_pending_snapshots_with_prices_and_skips_reruns():
    engine = _engine()
    with Session(engine) as db:
        first = seed_outcome_ledger_demo_snapshots(db, count=3)
        second = seed_outcome_ledger_demo_snapshots(db, count=3)

        rows = db.execute(select(ConfirmationScoreSnapshot).order_by(ConfirmationScoreSnapshot.id)).scalars().all()
        assert first["created"] == 3
        assert second["created"] == 0
        assert second["skipped"] == 3
        assert len(rows) == 3
        assert {row.ticker_at_time for row in rows} == {"NVDA", "MSFT", "XOM"}
        assert all(row.reference_price is not None for row in rows)
        assert all(row.calculation_type == "live" for row in rows)
        db.rollback()

        methodology = current_confirmation_methodology(db)
        methodology.description = "Changed after use"
        with pytest.raises(ValueError):
            db.commit()


def test_hydrated_demo_seeder_populates_matured_outcomes_and_skips_reruns():
    engine = _engine()
    with Session(engine) as db:
        first = seed_hydrated_outcome_ledger_demo_snapshots(db, count=3)
        second = seed_hydrated_outcome_ledger_demo_snapshots(db, count=3)

        rows = db.execute(select(ConfirmationScoreSnapshot).order_by(ConfirmationScoreSnapshot.id)).scalars().all()
        response = list_outcome_snapshots(db, limit=10, calculation_type="live")
        nvda = next(item for item in response["items"] if item["ticker"] == "NVDA")

        assert first["created"] == 3
        assert second["created"] == 0
        assert second["skipped"] == 3
        assert len(rows) == 3
        assert nvda["outcomes"]["30D"]["status"] == "matured"
        assert nvda["outcomes"]["30D"]["return_pct"] == 8.6
        assert nvda["outcomes"]["30D"]["directionally_correct"] is True
        assert nvda["outcomes"]["30D"]["spy_return_pct"] == 3.4


def test_pending_snapshot_listing_skips_price_outcome_lookups(monkeypatch):
    engine = _engine()
    with Session(engine) as db:
        today = datetime.now(timezone.utc).date()
        db.add(PriceCache(symbol="CRM", date=today.isoformat(), close=101.25, price_source="test"))
        db.commit()
        snapshot = capture_live_confirmation_score_snapshot(db, "CRM", _bundle(), calculated_at=datetime.now(timezone.utc))
        assert snapshot is not None

        def fail_price_lookup(*_args, **_kwargs):
            raise AssertionError("pending horizons should not query outcome prices")

        monkeypatch.setattr(outcome_ledger_module, "_price_on_or_after", fail_price_lookup)

        response = list_outcome_snapshots(db, limit=10, calculation_type="live")
        crm = next(item for item in response["items"] if item["ticker"] == "CRM")

        assert crm["outcomes"]["7D"]["status"] == "pending"
        assert crm["outcomes"]["30D"]["status"] == "pending"


def test_backfill_history_creates_matured_rows_from_monitoring_events():
    engine = _engine()
    with Session(engine) as db:
        observed_at = datetime.now(timezone.utc) - outcome_ledger_module.timedelta(days=60)
        entry_day = observed_at.date().isoformat()
        thirty_day = (observed_at.date() + outcome_ledger_module.timedelta(days=30)).isoformat()
        db.add_all(
            [
                PriceCache(symbol="CRM", date=entry_day, close=100.0, price_source="test"),
                PriceCache(symbol="CRM", date=thirty_day, close=112.0, price_source="test"),
                PriceCache(symbol="SPY", date=entry_day, close=500.0, price_source="test"),
                PriceCache(symbol="SPY", date=thirty_day, close=510.0, price_source="test"),
                ConfirmationMonitoringEvent(
                    user_id=1,
                    watchlist_id=1,
                    ticker="CRM",
                    event_type="confirmation_upgraded",
                    title="CRM confirmation score rose",
                    body=None,
                    score_before=52,
                    score_after=70,
                    band_before="moderate",
                    band_after="strong",
                    direction_before="bullish",
                    direction_after="bullish",
                    source_count_before=1,
                    source_count_after=3,
                    payload_json="{}",
                    created_at=observed_at,
                ),
            ]
        )
        db.commit()

        report = backfill_outcome_ledger_history(
            db,
            since_days=120,
            limit=10,
            min_score=40,
            min_source_count=1,
            hydrate_prices=False,
        )
        response = list_outcome_snapshots(db, limit=10, calculation_type="historical_reconstruction")
        crm = next(item for item in response["items"] if item["ticker"] == "CRM" and item["score"] == 70)
        live_response = list_outcome_snapshots(db, limit=10, calculation_type="live")

        assert report["created"] == 2
        assert live_response["total"] == 0
        assert crm["outcomes"]["30D"]["status"] == "matured"
        assert crm["outcomes"]["30D"]["return_pct"] == 12.0
        assert crm["outcomes"]["30D"]["spy_return_pct"] == 2.0
        assert crm["calculation_type"] == "historical_reconstruction"
