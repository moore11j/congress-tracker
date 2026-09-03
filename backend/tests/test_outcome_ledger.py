from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from app.db import Base, ensure_outcome_ledger_schema
from app.models import ConfirmationMethodologyVersion, ConfirmationMonitoringEvent, ConfirmationScoreSnapshot, MarketPressureSnapshot, PriceCache
from app.backfill_outcome_ledger_history import backfill_outcome_ledger_history
from app.services import outcome_ledger as outcome_ledger_module
from app.services.outcome_ledger import (
    cached_public_outcome_ledger_payload,
    capture_live_confirmation_score_snapshot,
    current_confirmation_methodology,
    input_hash_for_confirmation_bundle,
    list_outcome_snapshots,
    outcome_ledger_summary,
    public_outcome_ledger_cache_key,
    warm_public_outcome_ledger_cache,
)
from app.services.outcome_ledger_backtest import build_outcome_ledger_v2_backtest_report, load_clean_training_events
from app.main import (
    _PUBLIC_OUTCOME_LEDGER_RESPONSE_CACHE,
    _PUBLIC_OUTCOME_LEDGER_RESPONSE_CACHE_LOCK,
    _public_outcome_ledger_cache_control,
    _public_outcome_ledger_cache_get,
    _public_outcome_ledger_cache_set,
)
from app.seed_outcome_ledger_demo import seed_hydrated_outcome_ledger_demo_snapshots, seed_outcome_ledger_demo_snapshots


def _engine():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    ensure_outcome_ledger_schema(engine)
    return engine


def _bundle(score: int = 67, direction: str = "bearish") -> dict:
    return {
        "ticker": "CRM",
        "score": score,
        "band": "strong" if score >= 60 else "moderate",
        "direction": direction,
        "status": f"2-source {direction} confirmation",
        "classification_version": "confirmation_direction_v3",
        "active_sources": ["insiders", "price_volume"],
        "sources": {
            "insiders": {
                "present": True,
                "direction": direction,
                "strength": 82,
                "quality": 80,
                "freshness_days": 4,
                "score_contribution": 0,
                "label": "Insiders",
            },
            "price_volume": {
                "present": True,
                "direction": direction,
                "strength": 74,
                "quality": 76,
                "freshness_days": 1,
                "score_contribution": 0,
                "label": "Price / Volume",
            },
        },
    }


def test_public_outcome_ledger_cache_uses_long_public_ttl(monkeypatch):
    monkeypatch.setenv("OUTCOME_LEDGER_PUBLIC_CACHE_TTL_SECONDS", "43200")

    assert _public_outcome_ledger_cache_control() == "public, max-age=43200, s-maxage=43200, stale-while-revalidate=43200"


def test_public_outcome_ledger_cache_returns_deep_copies(monkeypatch):
    monkeypatch.setenv("OUTCOME_LEDGER_PUBLIC_CACHE_TTL_SECONDS", "43200")
    with _PUBLIC_OUTCOME_LEDGER_RESPONSE_CACHE_LOCK:
        _PUBLIC_OUTCOME_LEDGER_RESPONSE_CACHE.clear()

    original = {"items": [{"ticker": "NVDA"}], "total": 1}
    _public_outcome_ledger_cache_set("snapshots:test", original)
    original["items"][0]["ticker"] = "MUTATED"

    cached = _public_outcome_ledger_cache_get("snapshots:test")
    assert cached == {"items": [{"ticker": "NVDA"}], "total": 1}
    cached["items"][0]["ticker"] = "AAPL"

    assert _public_outcome_ledger_cache_get("snapshots:test") == {"items": [{"ticker": "NVDA"}], "total": 1}


def test_warm_public_outcome_ledger_cache_persists_overview_payload(monkeypatch):
    monkeypatch.setenv("OUTCOME_LEDGER_CACHE_WARM_HORIZONS", "30D,7D")
    engine = _engine()
    with Session(engine) as db:
        report = warm_public_outcome_ledger_cache(db, snapshot_limit=25)
        overview = cached_public_outcome_ledger_payload(
            db,
            public_outcome_ledger_cache_key("overview", {"horizons": ["30D", "7D"], "snapshot_limit": 25}),
        )

        assert report["status"] == "ok"
        assert overview is not None
        assert overview["default_horizon"] == "30D"
        assert set(overview["summaries"]) == {"7D", "30D"}
        assert overview["status"]["enabled"] is True
        assert overview["snapshots"]["limit"] == 25


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
        assert current.version == "confirmation-v2"
        assert [row.version for row in current_rows] == ["confirmation-v2"]


def test_current_methodology_promotes_deployed_version_over_existing_current():
    engine = _engine()
    with Session(engine) as db:
        legacy = db.execute(
            select(ConfirmationMethodologyVersion).where(ConfirmationMethodologyVersion.version == "confirmation-v1")
        ).scalar_one()
        legacy.is_current = True
        legacy.retired_at = None
        db.commit()

        current = current_confirmation_methodology(db)
        current_rows = db.execute(
            select(ConfirmationMethodologyVersion).where(ConfirmationMethodologyVersion.is_current.is_(True))
        ).scalars().all()
        retired_v1 = db.execute(
            select(ConfirmationMethodologyVersion).where(ConfirmationMethodologyVersion.version == "confirmation-v1")
        ).scalar_one()

        assert current.version == "confirmation-v2"
        assert [row.version for row in current_rows] == ["confirmation-v2"]
        assert retired_v1.is_current is False
        assert retired_v1.retired_at is not None


def test_live_capture_shows_latest_visible_daily_event_when_score_changes():
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
        assert changed.id != first.id
        assert changed.supersedes_snapshot_id == first.id
        assert len(rows) == 2
        assert rows[0].score == 67
        assert rows[1].score == 68

        response = list_outcome_snapshots(db, limit=10, calculation_type="live")
        assert response["total"] == 1
        assert response["items"][0]["score"] == 68
        assert response["items"][0]["calculation_type"] == "live"
        assert response["items"][0]["reference_price"] == 101.25
        assert json.loads(rows[0].active_sources_json) == ["insiders", "price_volume"]


def test_live_capture_opens_new_event_when_direction_changes():
    engine = _engine()
    with Session(engine) as db:
        db.add(PriceCache(symbol="CRM", date="2026-08-04", close=101.25, price_source="test"))
        db.commit()

        mixed = capture_live_confirmation_score_snapshot(db, "CRM", _bundle(59, "mixed"), calculated_at=datetime(2026, 8, 4, 15, tzinfo=timezone.utc))
        bullish = capture_live_confirmation_score_snapshot(db, "CRM", _bundle(64, "bullish"), calculated_at=datetime(2026, 8, 4, 17, tzinfo=timezone.utc))

        rows = db.execute(select(ConfirmationScoreSnapshot).order_by(ConfirmationScoreSnapshot.id)).scalars().all()
        response = list_outcome_snapshots(db, limit=10, calculation_type="live")

        assert mixed is not None
        assert bullish is not None
        assert bullish.id != mixed.id
        assert bullish.supersedes_snapshot_id == mixed.id
        assert len(rows) == 2
        assert {row.direction for row in rows} == {"mixed", "bullish"}
        assert response["total"] == 1
        assert response["items"][0]["direction"] == "bullish"
        assert response["items"][0]["score"] == 64


def test_live_capture_persists_v2_training_features():
    engine = _engine()
    with Session(engine) as db:
        db.add_all(
            [
                PriceCache(symbol="CRM", date="2026-07-05", close=90.0, price_source="test"),
                PriceCache(symbol="CRM", date="2026-07-28", close=96.0, price_source="test"),
                PriceCache(symbol="CRM", date="2026-08-03", close=98.0, price_source="test"),
                PriceCache(symbol="CRM", date="2026-08-04", close=101.25, price_source="test"),
                PriceCache(symbol="SPY", date="2026-07-05", close=500.0, price_source="test"),
                PriceCache(symbol="SPY", date="2026-07-28", close=510.0, price_source="test"),
                PriceCache(symbol="SPY", date="2026-08-03", close=520.0, price_source="test"),
                PriceCache(symbol="SPY", date="2026-08-04", close=525.0, price_source="test"),
            ]
        )
        db.commit()

        first = capture_live_confirmation_score_snapshot(db, "CRM", _bundle(58, "neutral"), calculated_at=datetime(2026, 8, 3, 15, tzinfo=timezone.utc))
        second = capture_live_confirmation_score_snapshot(db, "CRM", _bundle(67, "bearish"), calculated_at=datetime(2026, 8, 4, 15, tzinfo=timezone.utc))
        rows = db.execute(select(ConfirmationScoreSnapshot).order_by(ConfirmationScoreSnapshot.id)).scalars().all()
        payload = json.loads(rows[-1].source_contributions_json)
        features = payload["__v2_features"]

        assert first is not None
        assert second is not None
        assert features["source_metrics"]["active_sources"] == ["insiders", "price_volume"]
        assert features["source_metrics"]["agreement_state"] == "aligned"
        assert features["source_metrics"]["short_horizon_source_count"] == 1
        assert features["score_change"]["previous_score"] == 58
        assert features["score_change"]["score_delta"] == 9
        assert features["score_change"]["direction_changed"] is True
        assert features["regime_context"]["spy_return_30d"] == 5.0
        assert features["regime_context"]["ticker_minus_spy_30d"] == 7.5


def test_mixed_snapshot_is_not_a_scored_directional_event():
    engine = _engine()
    with Session(engine) as db:
        observed_day = (datetime.now(timezone.utc) - outcome_ledger_module.timedelta(days=60)).date()
        seven_day = observed_day + outcome_ledger_module.timedelta(days=7)
        thirty_day = observed_day + outcome_ledger_module.timedelta(days=30)
        db.add_all(
            [
                PriceCache(symbol="CRM", date=observed_day.isoformat(), close=100.0, price_source="test"),
                PriceCache(symbol="CRM", date=seven_day.isoformat(), close=110.0, price_source="test"),
                PriceCache(symbol="CRM", date=thirty_day.isoformat(), close=115.0, price_source="test"),
                PriceCache(symbol="SPY", date=observed_day.isoformat(), close=500.0, price_source="test"),
                PriceCache(symbol="SPY", date=seven_day.isoformat(), close=505.0, price_source="test"),
                PriceCache(symbol="SPY", date=thirty_day.isoformat(), close=510.0, price_source="test"),
            ]
        )
        db.commit()

        mixed = capture_live_confirmation_score_snapshot(
            db,
            "CRM",
            _bundle(59, "mixed"),
            calculated_at=datetime.combine(observed_day, datetime.min.time(), tzinfo=timezone.utc).replace(hour=15),
        )
        bullish = capture_live_confirmation_score_snapshot(
            db,
            "CRM",
            _bundle(64, "bullish"),
            calculated_at=datetime.combine(observed_day, datetime.min.time(), tzinfo=timezone.utc).replace(hour=17),
        )

        assert mixed is not None
        assert bullish is not None
        mixed_row = outcome_ledger_module._snapshot_row(db, mixed)
        response = list_outcome_snapshots(db, limit=10, calculation_type="live")

        assert mixed_row["outcomes"]["7D"]["status"] == "not_directional"
        assert response["total"] == 1
        assert response["items"][0]["ticker"] == "CRM"
        assert response["items"][0]["direction"] == "bullish"
        assert response["items"][0]["outcomes"]["7D"]["status"] == "matured"


def test_opposite_direction_closes_previous_directional_event():
    engine = _engine()
    with Session(engine) as db:
        observed_day = (datetime.now(timezone.utc) - outcome_ledger_module.timedelta(days=60)).date()
        closed_day = observed_day + outcome_ledger_module.timedelta(days=3)
        seven_day = observed_day + outcome_ledger_module.timedelta(days=7)
        db.add_all(
            [
                PriceCache(symbol="CRM", date=observed_day.isoformat(), close=100.0, price_source="test"),
                PriceCache(symbol="CRM", date=closed_day.isoformat(), close=104.0, price_source="test"),
                PriceCache(symbol="CRM", date=seven_day.isoformat(), close=110.0, price_source="test"),
                PriceCache(symbol="SPY", date=observed_day.isoformat(), close=500.0, price_source="test"),
                PriceCache(symbol="SPY", date=seven_day.isoformat(), close=505.0, price_source="test"),
            ]
        )
        db.commit()

        bullish = capture_live_confirmation_score_snapshot(
            db,
            "CRM",
            _bundle(64, "bullish"),
            calculated_at=datetime.combine(observed_day, datetime.min.time(), tzinfo=timezone.utc).replace(hour=15),
        )
        bearish = capture_live_confirmation_score_snapshot(
            db,
            "CRM",
            _bundle(67, "bearish"),
            calculated_at=datetime.combine(closed_day, datetime.min.time(), tzinfo=timezone.utc).replace(hour=15),
        )

        assert bullish is not None
        assert bearish is not None
        response = list_outcome_snapshots(db, limit=10, calculation_type="live")
        crm_bullish = next(item for item in response["items"] if item["direction"] == "bullish")

        assert response["total"] == 2
        assert crm_bullish["lifecycle_status"] == "closed"
        assert crm_bullish["closed_at"] == closed_day.isoformat()
        assert crm_bullish["outcomes"]["7D"]["status"] == "closed"
        assert "return_pct" not in crm_bullish["outcomes"]["7D"]


def test_live_capture_dedupes_same_visible_daily_event_when_hash_changes():
    engine = _engine()
    with Session(engine) as db:
        db.add(PriceCache(symbol="CRM", date="2026-08-04", close=101.25, price_source="test"))
        db.commit()

        changed_freshness = _bundle(67)
        changed_freshness["sources"]["price_volume"]["freshness_days"] = 2

        first = capture_live_confirmation_score_snapshot(db, "CRM", _bundle(67), calculated_at=datetime(2026, 8, 4, 15, tzinfo=timezone.utc))
        duplicate = capture_live_confirmation_score_snapshot(db, "CRM", changed_freshness, calculated_at=datetime(2026, 8, 4, 16, tzinfo=timezone.utc))

        rows = db.execute(select(ConfirmationScoreSnapshot).order_by(ConfirmationScoreSnapshot.id)).scalars().all()
        assert first is not None
        assert duplicate is not None
        assert duplicate.id == first.id
        assert len(rows) == 1


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
        assert nvda["outcomes"]["30D"]["raw_directionally_correct"] is True
        assert nvda["outcomes"]["30D"]["spy_return_pct"] == 3.4
        assert nvda["outcomes"]["30D"]["benchmark_directionally_correct"] is True
        assert nvda["outcomes"]["30D"]["grading_basis"] == "raw_or_vs_spy"


def test_outcome_ledger_summary_calculates_cached_headline_metrics():
    engine = _engine()
    with Session(engine) as db:
        observed_day = (datetime.now(timezone.utc) - outcome_ledger_module.timedelta(days=10)).date()
        seven_day = observed_day + outcome_ledger_module.timedelta(days=7)
        db.add_all(
            [
                PriceCache(symbol="CRM", date=observed_day.isoformat(), close=100.0, price_source="test"),
                PriceCache(symbol="CRM", date=seven_day.isoformat(), close=110.0, price_source="test"),
                PriceCache(symbol="MSFT", date=observed_day.isoformat(), close=200.0, price_source="test"),
                PriceCache(symbol="MSFT", date=seven_day.isoformat(), close=220.0, price_source="test"),
                PriceCache(symbol="SPY", date=observed_day.isoformat(), close=500.0, price_source="test"),
                PriceCache(symbol="SPY", date=seven_day.isoformat(), close=505.0, price_source="test"),
            ]
        )
        db.commit()

        assert capture_live_confirmation_score_snapshot(
            db,
            "CRM",
            _bundle(70, "bullish"),
            calculated_at=datetime.combine(observed_day, datetime.min.time(), tzinfo=timezone.utc).replace(hour=15),
        )
        assert capture_live_confirmation_score_snapshot(
            db,
            "MSFT",
            _bundle(42, "bearish"),
            calculated_at=datetime.combine(observed_day, datetime.min.time(), tzinfo=timezone.utc).replace(hour=16),
        )

        summary = outcome_ledger_summary(db, horizon="7D", calculation_type="live")
        bands = {row["band"]: row for row in summary["score_bands"]}

        assert summary["completed_events"] == 2
        assert summary["directional_sample_count"] == 2
        assert summary["accuracy"] == 50
        assert summary["average_directional_return"] == 0.0
        assert summary["average_spy_return"] == 1.0
        assert summary["average_directional_excess_return"] == 0.0
        assert summary["benchmarked_events"] == 2
        assert summary["matured_horizon_count"] == 2
        assert bands["70-74"] == {"band": "70-74", "accuracy": 100, "count": 1}
        assert bands["40-59"] == {"band": "40-59", "accuracy": 0, "count": 1}


def test_directional_correctness_allows_raw_return_or_spy_relative_win():
    engine = _engine()
    with Session(engine) as db:
        observed_day = (datetime.now(timezone.utc) - outcome_ledger_module.timedelta(days=10)).date()
        seven_day = observed_day + outcome_ledger_module.timedelta(days=7)
        db.add_all(
            [
                PriceCache(symbol="UPLAG", date=observed_day.isoformat(), close=100.0, price_source="test"),
                PriceCache(symbol="UPLAG", date=seven_day.isoformat(), close=102.0, price_source="test"),
                PriceCache(symbol="BEAT", date=observed_day.isoformat(), close=100.0, price_source="test"),
                PriceCache(symbol="BEAT", date=seven_day.isoformat(), close=99.0, price_source="test"),
                PriceCache(symbol="SPY", date=observed_day.isoformat(), close=100.0, price_source="test"),
                PriceCache(symbol="SPY", date=seven_day.isoformat(), close=104.0, price_source="test"),
            ]
        )
        db.commit()

        assert capture_live_confirmation_score_snapshot(
            db,
            "UPLAG",
            _bundle(70, "bullish"),
            calculated_at=datetime.combine(observed_day, datetime.min.time(), tzinfo=timezone.utc).replace(hour=15),
        )
        assert capture_live_confirmation_score_snapshot(
            db,
            "BEAT",
            _bundle(70, "bearish"),
            calculated_at=datetime.combine(observed_day, datetime.min.time(), tzinfo=timezone.utc).replace(hour=16),
        )

        response = list_outcome_snapshots(db, limit=10, calculation_type="live")
        by_ticker = {item["ticker"]: item for item in response["items"]}
        bullish = by_ticker["UPLAG"]["outcomes"]["7D"]
        bearish = by_ticker["BEAT"]["outcomes"]["7D"]

        assert bullish["raw_directionally_correct"] is True
        assert bullish["benchmark_directionally_correct"] is False
        assert bullish["directionally_correct"] is True
        assert bearish["raw_directionally_correct"] is True
        assert bearish["benchmark_directionally_correct"] is True
        assert bearish["directionally_correct"] is True


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


def test_live_capture_preserves_event_but_quarantines_stale_reference_price():
    engine = _engine()
    with Session(engine) as db:
        db.add(PriceCache(symbol="CRM", date="2026-01-15", close=101.25, price_source="test"))
        db.commit()

        snapshot = capture_live_confirmation_score_snapshot(
            db,
            "CRM",
            _bundle(),
            calculated_at=datetime(2026, 8, 4, 15, tzinfo=timezone.utc),
        )

        assert snapshot is not None
        assert snapshot.reference_price is None
        public_row = outcome_ledger_module._snapshot_row(db, snapshot)
        assert public_row["data_integrity_status"] == "requires_reconstruction"
        assert public_row["reference_price"] is None
        assert db.execute(select(ConfirmationScoreSnapshot)).scalars().all() == [snapshot]


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

        assert report["created"] == 1
        assert live_response["total"] == 0
        assert crm["outcomes"]["30D"]["status"] == "matured"
        assert crm["outcomes"]["30D"]["return_pct"] == 12.0
        assert crm["outcomes"]["30D"]["spy_return_pct"] == 2.0
        assert crm["calculation_type"] == "historical_reconstruction"


def test_backfill_history_dedupes_same_visible_daily_point():
    engine = _engine()
    with Session(engine) as db:
        observed_at = datetime.now(timezone.utc) - outcome_ledger_module.timedelta(days=60)
        entry_day = observed_at.date().isoformat()
        thirty_day = (observed_at.date() + outcome_ledger_module.timedelta(days=30)).isoformat()
        db.add_all(
            [
                PriceCache(symbol="DRAM", date=entry_day, close=60.0, price_source="test"),
                PriceCache(symbol="DRAM", date=thirty_day, close=63.0, price_source="test"),
                PriceCache(symbol="SPY", date=entry_day, close=500.0, price_source="test"),
                PriceCache(symbol="SPY", date=thirty_day, close=510.0, price_source="test"),
                ConfirmationMonitoringEvent(
                    user_id=1,
                    watchlist_id=1,
                    ticker="DRAM",
                    event_type="confirmation_changed",
                    title="DRAM confirmation changed",
                    body=None,
                    score_before=40,
                    score_after=40,
                    band_before="weak",
                    band_after="weak",
                    direction_before="bullish",
                    direction_after="bullish",
                    source_count_before=1,
                    source_count_after=2,
                    payload_json="{}",
                    created_at=observed_at,
                ),
            ]
        )
        db.commit()

        report = backfill_outcome_ledger_history(db, since_days=120, limit=10, min_score=0, min_source_count=1, hydrate_prices=False)
        response = list_outcome_snapshots(db, limit=10, calculation_type="historical_reconstruction")

        assert report["created"] == 1
        assert response["total"] == 1
        assert response["items"][0]["ticker"] == "DRAM"
        assert response["items"][0]["active_source_count"] == 2


def test_backfill_history_uses_market_pressure_score_snapshots():
    engine = _engine()
    with Session(engine) as db:
        observed_at = datetime.now(timezone.utc) - outcome_ledger_module.timedelta(days=60)
        entry_day = observed_at.date().isoformat()
        thirty_day = (observed_at.date() + outcome_ledger_module.timedelta(days=30)).isoformat()
        db.add_all(
            [
                PriceCache(symbol="PLTR", date=entry_day, close=50.0, price_source="test"),
                PriceCache(symbol="PLTR", date=thirty_day, close=60.0, price_source="test"),
                PriceCache(symbol="SPY", date=entry_day, close=500.0, price_source="test"),
                PriceCache(symbol="SPY", date=thirty_day, close=525.0, price_source="test"),
                MarketPressureSnapshot(
                    universe="test",
                    period="1d",
                    symbol="PLTR",
                    confirmation_score=72,
                    confirmation_direction="aligned_bullish",
                    confirmation_as_of=observed_at,
                    generated_at=observed_at,
                    tile_json=json.dumps(
                        {
                            "confirmationBand": "strong",
                            "confirmationSourceCount": 4,
                            "dataState": "complete",
                        }
                    ),
                ),
            ]
        )
        db.commit()

        report = backfill_outcome_ledger_history(db, since_days=120, limit=10, min_score=0, min_source_count=1, hydrate_prices=False)
        response = list_outcome_snapshots(db, limit=10, calculation_type="historical_reconstruction")
        pltr = next(item for item in response["items"] if item["ticker"] == "PLTR")

        assert report["created"] == 1
        assert pltr["score"] == 72
        assert pltr["direction"] == "bullish"
        assert pltr["active_source_count"] == 4
        assert pltr["outcomes"]["30D"]["return_pct"] == 20.0
        assert pltr["outcomes"]["30D"]["spy_return_pct"] == 5.0


def test_clean_training_set_keeps_latest_same_day_directional_event_only():
    engine = _engine()
    with Session(engine) as db:
        observed_day = (datetime.now(timezone.utc) - outcome_ledger_module.timedelta(days=45)).date()
        thirty_day = observed_day + outcome_ledger_module.timedelta(days=30)
        db.add_all(
            [
                PriceCache(symbol="CRM", date=observed_day.isoformat(), close=100.0, price_source="test"),
                PriceCache(symbol="CRM", date=thirty_day.isoformat(), close=120.0, price_source="test"),
                PriceCache(symbol="SPY", date=observed_day.isoformat(), close=500.0, price_source="test"),
                PriceCache(symbol="SPY", date=thirty_day.isoformat(), close=510.0, price_source="test"),
            ]
        )
        db.commit()

        older = capture_live_confirmation_score_snapshot(
            db,
            "CRM",
            _bundle(61, "bullish"),
            calculated_at=datetime.combine(observed_day, datetime.min.time(), tzinfo=timezone.utc).replace(hour=14),
        )
        latest = capture_live_confirmation_score_snapshot(
            db,
            "CRM",
            _bundle(72, "bullish"),
            calculated_at=datetime.combine(observed_day, datetime.min.time(), tzinfo=timezone.utc).replace(hour=15),
        )
        mixed = capture_live_confirmation_score_snapshot(
            db,
            "CRM",
            _bundle(55, "mixed"),
            calculated_at=datetime.combine(observed_day, datetime.min.time(), tzinfo=timezone.utc).replace(hour=16),
        )

        events, _exclusions = load_clean_training_events(db)

        assert older is not None
        assert latest is not None
        assert mixed is not None
        assert [event.snapshot_id for event in events] == [latest.id]
        assert events[0].score == 72
        assert events[0].directionally_correct is True
        assert events[0].source_payload_quality == "real_source_payload"


def test_clean_training_set_excludes_directional_event_closed_before_30d():
    engine = _engine()
    with Session(engine) as db:
        opened_day = (datetime.now(timezone.utc) - outcome_ledger_module.timedelta(days=50)).date()
        closed_day = opened_day + outcome_ledger_module.timedelta(days=5)
        bearish_target = closed_day + outcome_ledger_module.timedelta(days=30)
        db.add_all(
            [
                PriceCache(symbol="CRM", date=opened_day.isoformat(), close=100.0, price_source="test"),
                PriceCache(symbol="CRM", date=closed_day.isoformat(), close=95.0, price_source="test"),
                PriceCache(symbol="CRM", date=bearish_target.isoformat(), close=80.0, price_source="test"),
                PriceCache(symbol="SPY", date=opened_day.isoformat(), close=500.0, price_source="test"),
                PriceCache(symbol="SPY", date=closed_day.isoformat(), close=500.0, price_source="test"),
                PriceCache(symbol="SPY", date=bearish_target.isoformat(), close=505.0, price_source="test"),
            ]
        )
        db.commit()

        bullish = capture_live_confirmation_score_snapshot(
            db,
            "CRM",
            _bundle(70, "bullish"),
            calculated_at=datetime.combine(opened_day, datetime.min.time(), tzinfo=timezone.utc).replace(hour=12),
        )
        bearish = capture_live_confirmation_score_snapshot(
            db,
            "CRM",
            _bundle(76, "bearish"),
            calculated_at=datetime.combine(closed_day, datetime.min.time(), tzinfo=timezone.utc).replace(hour=12),
        )

        events, exclusions = load_clean_training_events(db)

        assert bullish is not None
        assert bearish is not None
        assert [event.snapshot_id for event in events] == [bearish.id]
        assert events[0].side == "bearish"
        assert events[0].directionally_correct is True
        assert exclusions["not_matured:closed"] == 1


def test_v2_backtest_report_measures_components_and_candidate_coverage():
    engine = _engine()
    with Session(engine) as db:
        observed_day = (datetime.now(timezone.utc) - outcome_ledger_module.timedelta(days=45)).date()
        previous_day = observed_day - outcome_ledger_module.timedelta(days=30)
        thirty_day = observed_day + outcome_ledger_module.timedelta(days=30)
        db.add_all(
            [
                PriceCache(symbol="CRM", date=previous_day.isoformat(), close=90.0, price_source="test"),
                PriceCache(symbol="CRM", date=observed_day.isoformat(), close=100.0, price_source="test"),
                PriceCache(symbol="CRM", date=thirty_day.isoformat(), close=112.0, price_source="test"),
                PriceCache(symbol="MSFT", date=previous_day.isoformat(), close=180.0, price_source="test"),
                PriceCache(symbol="MSFT", date=observed_day.isoformat(), close=200.0, price_source="test"),
                PriceCache(symbol="MSFT", date=thirty_day.isoformat(), close=190.0, price_source="test"),
                PriceCache(symbol="SPY", date=previous_day.isoformat(), close=480.0, price_source="test"),
                PriceCache(symbol="SPY", date=observed_day.isoformat(), close=500.0, price_source="test"),
                PriceCache(symbol="SPY", date=thirty_day.isoformat(), close=505.0, price_source="test"),
            ]
        )
        db.commit()

        capture_live_confirmation_score_snapshot(
            db,
            "CRM",
            _bundle(72, "bullish"),
            calculated_at=datetime.combine(observed_day, datetime.min.time(), tzinfo=timezone.utc).replace(hour=12),
        )
        capture_live_confirmation_score_snapshot(
            db,
            "MSFT",
            _bundle(76, "bearish"),
            calculated_at=datetime.combine(observed_day, datetime.min.time(), tzinfo=timezone.utc).replace(hour=12),
        )

        report = build_outcome_ledger_v2_backtest_report(db, min_sample=1)

        assert report["clean_training_set"]["events"] == 2
        assert report["baseline"]["sample_size"] == 2
        assert report["baseline"]["accuracy"] == 100.0
        assert report["component_analysis"]["component_eligible_sample"] == 2
        assert report["component_analysis"]["components"]["price_volume"]["present"]["sample_size"] == 2
        assert report["component_analysis"]["score_change_over_time"][0]["key"] == "unavailable"
        assert report["component_analysis"]["spy_regime"][0]["key"] == "spy_positive"
        assert report["component_analysis"]["relative_regime"][0]["key"] in {"relative_positive", "relative_negative"}
        score_rule = next(rule for rule in report["candidate_v2_rules"] if rule["rule"] == "score>=70")
        assert score_rule["calls_kept"] == 2
        assert score_rule["calls_rejected"] == 0
        assert score_rule["meets_min_sample"] is True
