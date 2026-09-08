from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from app.db import Base, ensure_outcome_ledger_schema
from app.models import (
    ConfirmationScoreSnapshot,
    OutcomeEvidenceProvenance,
    OutcomeHorizonObservation,
    PriceCache,
)
from app.services.outcome_integrity import (
    canonical_entry_session_row,
    canonical_price_path,
    entry_price_invariant,
    evidence_provenance_from_bundle,
    market_open_at,
    materialize_outcome_entry,
    materialize_outcome_horizons,
)
from app.services.outcome_ledger import _project_directional_outcome_events, _snapshot_row, list_outcome_snapshots, outcome_ledger_summary
from app.services.price_lookup import EodPriceBar, reconstruct_adjusted_price_bars

UTC = timezone.utc


def _engine():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    ensure_outcome_ledger_schema(engine)
    return engine


def _bar(symbol: str, day: date, opened: float, closed: float | None = None, *, low: float | None = None, high: float | None = None):
    close = opened if closed is None else closed
    return PriceCache(
        symbol=symbol,
        date=day.isoformat(),
        close=close,
        adjusted_close=close,
        raw_close=close,
        open_price=opened,
        low_price=min(opened, close) if low is None else low,
        high_price=max(opened, close) if high is None else high,
        price_source="test-authoritative",
        adjustment_status="split_adjusted_price_return",
    )


def _snapshot(
    db: Session,
    calculated_at: datetime,
    *,
    ticker: str = "CRM",
    security_id: int = 1,
    score: int = 70,
    direction: str = "bullish",
    legacy_reference_price: float | None = None,
):
    snapshot = ConfirmationScoreSnapshot(
        security_id=security_id,
        ticker_at_time=ticker,
        calculated_at=calculated_at,
        market_date=calculated_at.astimezone(timezone(timedelta(hours=-5))).date(),
        score=score,
        direction=direction,
        strength="strong",
        reference_price=legacy_reference_price,
        reference_price_at=calculated_at if legacy_reference_price is not None else None,
        reference_price_source="test" if legacy_reference_price is not None else None,
        active_source_count=1,
        active_sources_json='["congress"]',
        source_contributions_json="{}",
        source_freshness_json="{}",
        input_hash=f"{ticker}-{calculated_at.isoformat()}-{score}",
        methodology_version_id=1,
        calculation_type="live",
        created_at=calculated_at,
    )
    db.add(snapshot)
    db.flush()
    db.add(
        OutcomeEvidenceProvenance(
            snapshot_id=snapshot.id,
            source_key="congress",
            evidence_id=f"event-{snapshot.id}",
            available_at=calculated_at - timedelta(minutes=1),
            qualifying_event_at=calculated_at,
            source_timestamp=calculated_at - timedelta(minutes=1),
            source_payload_hash="abc",
        )
    )
    db.flush()
    return snapshot


def _entry_for(calculated_at: datetime, same_day: date, next_day: date):
    engine = _engine()
    with Session(engine) as db:
        snapshot = _snapshot(db, calculated_at)
        db.add_all([_bar("CRM", same_day, 100), _bar("SPY", same_day, 500), _bar("CRM", next_day, 101), _bar("SPY", next_day, 501)])
        db.flush()
        entry = materialize_outcome_entry(db, snapshot)
        return entry


def test_during_market_event_uses_next_session_open():
    entry = _entry_for(datetime(2026, 1, 5, 15, 0, tzinfo=UTC), date(2026, 1, 5), date(2026, 1, 6))
    assert entry and entry.entry_session_date == date(2026, 1, 6) and entry.entry_price == 101


def test_after_market_event_uses_next_session_open():
    entry = _entry_for(datetime(2026, 1, 5, 22, 0, tzinfo=UTC), date(2026, 1, 5), date(2026, 1, 6))
    assert entry and entry.entry_session_date == date(2026, 1, 6)


def test_premarket_event_uses_same_session_open():
    entry = _entry_for(datetime(2026, 1, 5, 13, 0, tzinfo=UTC), date(2026, 1, 5), date(2026, 1, 6))
    assert entry and entry.entry_session_date == date(2026, 1, 5) and entry.entry_price == 100


def test_weekend_event_uses_monday_open():
    entry = _entry_for(datetime(2026, 1, 3, 15, 0, tzinfo=UTC), date(2026, 1, 5), date(2026, 1, 6))
    assert entry and entry.entry_session_date == date(2026, 1, 5)


def test_market_holiday_uses_first_price_session_after_holiday():
    entry = _entry_for(datetime(2026, 7, 4, 13, 0, tzinfo=UTC), date(2026, 7, 6), date(2026, 7, 7))
    assert entry and entry.entry_session_date == date(2026, 7, 6)


def test_dst_transition_uses_new_york_offset():
    assert market_open_at(date(2026, 1, 5)).hour == 14
    assert market_open_at(date(2026, 7, 6)).hour == 13


def test_public_snapshot_exposes_canonical_entry_date_instead_of_snapshot_market_date():
    engine = _engine()
    with Session(engine) as db:
        calculated_at = datetime(2026, 1, 5, 15, 0, tzinfo=UTC)
        snapshot = _snapshot(db, calculated_at)
        db.add_all([
            _bar("CRM", date(2026, 1, 6), 101),
            _bar("SPY", date(2026, 1, 6), 501),
        ])
        db.flush()
        entry = materialize_outcome_entry(db, snapshot)
        assert entry is not None

        payload = _snapshot_row(db, snapshot)

        assert payload["market_date"] == "2026-01-05"
        assert payload["entry_session_date"] == "2026-01-06"
        assert payload["entry_timestamp"] == "2026-01-06T14:30:00+00:00"


def test_horizon_balanced_snapshot_sample_contains_matured_and_open_events():
    engine = _engine()
    with Session(engine) as db:
        today = datetime.now(UTC).date()
        matured_day = today - timedelta(days=10)
        pending_day = today - timedelta(days=2)
        matured_snapshot = _snapshot(
            db,
            datetime.combine(matured_day, datetime.min.time(), tzinfo=UTC).replace(hour=13),
            ticker="DONE",
            security_id=1,
        )
        pending_snapshot = _snapshot(
            db,
            datetime.combine(pending_day, datetime.min.time(), tzinfo=UTC).replace(hour=13),
            ticker="OPEN",
            security_id=2,
        )
        db.add_all([
            _bar("DONE", matured_day, 100),
            _bar("SPY", matured_day, 500),
            _bar("DONE", matured_day + timedelta(days=7), 110),
            _bar("SPY", matured_day + timedelta(days=7), 505),
            _bar("OPEN", pending_day, 50),
            _bar("SPY", pending_day, 510),
            _bar("OPEN", pending_day + timedelta(days=1), 55),
            _bar("SPY", pending_day + timedelta(days=1), 515),
        ])
        db.flush()
        matured_entry = materialize_outcome_entry(db, matured_snapshot)
        pending_entry = materialize_outcome_entry(db, pending_snapshot)
        assert matured_entry is not None and pending_entry is not None
        assert materialize_outcome_horizons(db, matured_entry, as_of=today)

        payload = list_outcome_snapshots(db, limit=2, balanced_horizon="7D")

        assert {item["ticker"] for item in payload["items"]} == {"DONE", "OPEN"}
        by_ticker = {item["ticker"]: item for item in payload["items"]}
        assert by_ticker["DONE"]["outcomes"]["7D"]["status"] == "matured"
        assert by_ticker["OPEN"]["outcomes"]["7D"]["status"] == "pending"
        assert by_ticker["OPEN"]["live_mark"]["status"] == "provisional"
        assert by_ticker["OPEN"]["live_mark"]["price_date"] == (pending_day + timedelta(days=1)).isoformat()
        assert by_ticker["OPEN"]["live_mark"]["return_pct"] == 10.0


def test_stock_split_does_not_create_fake_return():
    raw = {
        "2026-01-02": EodPriceBar(date="2026-01-02", close=100, raw_close=100),
        "2026-01-05": EodPriceBar(date="2026-01-05", close=50, raw_close=50),
    }
    adjusted = reconstruct_adjusted_price_bars(raw, dividends={}, split_factors={"2026-01-05": 0.5}, apply_split_factors=True)
    assert adjusted["2026-01-02"].close == adjusted["2026-01-05"].close


def test_reverse_split_does_not_create_fake_return():
    raw = {
        "2026-01-02": EodPriceBar(date="2026-01-02", close=10, raw_close=10),
        "2026-01-05": EodPriceBar(date="2026-01-05", close=100, raw_close=100),
    }
    adjusted = reconstruct_adjusted_price_bars(raw, dividends={}, split_factors={"2026-01-05": 10}, apply_split_factors=True)
    assert adjusted["2026-01-02"].close == adjusted["2026-01-05"].close


def test_dividend_is_excluded_from_price_return_adjustment():
    raw = {
        "2026-01-02": EodPriceBar(date="2026-01-02", close=100, raw_close=100),
        "2026-01-05": EodPriceBar(date="2026-01-05", close=99, raw_close=99),
    }
    adjusted = reconstruct_adjusted_price_bars(raw, dividends={}, split_factors={}, apply_split_factors=True)
    assert adjusted["2026-01-02"].close == 100 and adjusted["2026-01-05"].close == 99


def test_ticker_change_does_not_substitute_new_ticker_silently():
    engine = _engine()
    with Session(engine) as db:
        snapshot = _snapshot(db, datetime(2026, 1, 5, 13, tzinfo=UTC), ticker="OLD")
        db.add_all([_bar("OLD", date(2026, 1, 5), 10), _bar("SPY", date(2026, 1, 5), 500), _bar("NEW", date(2026, 1, 12), 20), _bar("SPY", date(2026, 1, 12), 510)])
        db.flush()
        entry = materialize_outcome_entry(db, snapshot)
        assert entry and materialize_outcome_horizons(db, entry, as_of=date(2026, 1, 12)) == []


def test_delisted_ticker_produces_missing_observation_not_survivor_substitution():
    engine = _engine()
    with Session(engine) as db:
        snapshot = _snapshot(db, datetime(2026, 1, 5, 13, tzinfo=UTC), ticker="DELIST")
        db.add_all([_bar("DELIST", date(2026, 1, 5), 10), _bar("SPY", date(2026, 1, 5), 500), _bar("SPY", date(2026, 1, 12), 510)])
        db.flush()
        entry = materialize_outcome_entry(db, snapshot)
        assert entry and not materialize_outcome_horizons(db, entry, as_of=date(2026, 1, 12))


def test_missing_market_data_prevents_entry_materialization():
    engine = _engine()
    with Session(engine) as db:
        snapshot = _snapshot(db, datetime(2026, 1, 5, 13, tzinfo=UTC))
        assert materialize_outcome_entry(db, snapshot) is None


def test_incorrect_daily_price_outside_range_fails_invariant():
    row = _bar("CRM", date(2026, 1, 5), 100, low=99, high=101)
    assert not entry_price_invariant(row, Decimal("150"), market_open_at(date(2026, 1, 5)), datetime(2026, 1, 5, 13, tzinfo=UTC))


def test_entry_before_event_timestamp_fails_invariant():
    row = _bar("CRM", date(2026, 1, 5), 100)
    assert not entry_price_invariant(row, Decimal("100"), market_open_at(date(2026, 1, 5)), datetime(2026, 1, 5, 15, tzinfo=UTC))


@pytest.mark.parametrize("source", ["congress", "insiders", "institutional_activity"])
def test_delayed_disclosure_sources_reject_evidence_after_score(source: str):
    calculated = datetime(2026, 7, 10, 15, tzinfo=UTC)
    bundle = {
        "active_sources": [source],
        "sources": {source: {"present": True}},
        "evidence_provenance": [{"source_key": source, "evidence_id": "late", "available_at": "2026-07-28T15:00:00Z"}],
    }
    assert evidence_provenance_from_bundle(bundle, calculated) == []


def test_spy_benchmark_is_aligned_to_security_session():
    engine = _engine()
    with Session(engine) as db:
        snapshot = _snapshot(db, datetime(2026, 1, 5, 13, tzinfo=UTC))
        db.add_all([_bar("CRM", date(2026, 1, 5), 100), _bar("SPY", date(2026, 1, 5), 500), _bar("CRM", date(2026, 1, 13), 110), _bar("SPY", date(2026, 1, 12), 505), _bar("SPY", date(2026, 1, 13), 506)])
        db.flush()
        entry = materialize_outcome_entry(db, snapshot)
        assert entry and materialize_outcome_horizons(db, entry, as_of=date(2026, 1, 13)) == []


@pytest.mark.parametrize("days", [7, 30, 90, 180, 365])
def test_calendar_horizon_uses_first_valid_close_on_or_after_target(days: int):
    engine = _engine()
    with Session(engine) as db:
        entry_day = date(2024, 1, 2)
        target = entry_day + timedelta(days=days)
        exit_day = target + timedelta(days=2)
        snapshot = _snapshot(db, datetime(2024, 1, 2, 13, tzinfo=UTC))
        db.add_all([_bar("CRM", entry_day, 100), _bar("SPY", entry_day, 500), _bar("CRM", exit_day, 110), _bar("SPY", exit_day, 505)])
        db.flush()
        entry = materialize_outcome_entry(db, snapshot)
        rows = materialize_outcome_horizons(db, entry, as_of=exit_day)
        row = next(item for item in rows if item.horizon_days == days)
        assert row.target_date == target and row.security_session_date == exit_day


def test_returns_use_high_precision_until_storage():
    engine = _engine()
    with Session(engine) as db:
        snapshot = _snapshot(db, datetime(2026, 1, 5, 13, tzinfo=UTC))
        db.add_all([_bar("CRM", date(2026, 1, 5), 3), _bar("SPY", date(2026, 1, 5), 7), _bar("CRM", date(2026, 1, 12), 4), _bar("SPY", date(2026, 1, 12), 8)])
        db.flush()
        entry = materialize_outcome_entry(db, snapshot)
        row = materialize_outcome_horizons(db, entry, as_of=date(2026, 1, 12))[0]
        assert row.security_return_pct == pytest.approx(33.33333333333333)
        assert row.excess_return_pct == pytest.approx(19.047619047619047)


def test_price_path_uses_exact_persisted_entry_and_aligned_sessions():
    engine = _engine()
    with Session(engine) as db:
        snapshot = _snapshot(db, datetime(2026, 1, 5, 13, tzinfo=UTC))
        db.add_all([_bar("CRM", date(2026, 1, 5), 100, 101), _bar("SPY", date(2026, 1, 5), 500, 501), _bar("CRM", date(2026, 1, 12), 110), _bar("SPY", date(2026, 1, 12), 505)])
        db.flush()
        entry = materialize_outcome_entry(db, snapshot)
        materialize_outcome_horizons(db, entry, as_of=date(2026, 1, 12))
        payload = canonical_price_path(db, snapshot.id, horizon_days=7)
        assert payload and payload["points"][0]["price_type"] == "official_open"
        assert payload["points"][-1]["security_return_pct"] == 10


def test_duplicate_same_day_snapshot_is_prevented_in_projection():
    engine = _engine()
    with Session(engine) as db:
        first = _snapshot(db, datetime(2026, 1, 5, 14, tzinfo=UTC), score=70)
        later = _snapshot(db, datetime(2026, 1, 5, 15, tzinfo=UTC), score=72)
        assert [event.snapshot.id for event in _project_directional_outcome_events([first, later])] == [later.id]


def test_overlapping_same_direction_events_require_cooldown_and_score_change():
    engine = _engine()
    with Session(engine) as db:
        first = _snapshot(db, datetime(2026, 1, 5, 14, tzinfo=UTC), score=70)
        next_day = _snapshot(db, datetime(2026, 1, 6, 14, tzinfo=UTC), score=71)
        day_31_small = _snapshot(db, datetime(2026, 2, 5, 14, tzinfo=UTC), score=72)
        day_32_material = _snapshot(db, datetime(2026, 2, 6, 14, tzinfo=UTC), score=81)
        events = _project_directional_outcome_events([first, next_day, day_31_small, day_32_material])
        assert [event.snapshot.id for event in events] == [first.id, day_32_material.id]


def test_incomplete_horizon_is_not_persisted_or_counted():
    engine = _engine()
    with Session(engine) as db:
        snapshot = _snapshot(db, datetime(2026, 1, 5, 13, tzinfo=UTC))
        db.add_all([_bar("CRM", date(2026, 1, 5), 100), _bar("SPY", date(2026, 1, 5), 500)])
        db.flush()
        entry = materialize_outcome_entry(db, snapshot)
        assert entry and materialize_outcome_horizons(db, entry, as_of=date(2026, 1, 10)) == []
        assert db.execute(select(OutcomeHorizonObservation)).scalars().all() == []


def test_bearish_directional_win_rate_uses_inverse_security_return():
    engine = _engine()
    with Session(engine) as db:
        day = datetime.now(UTC).date() - timedelta(days=10)
        db.add_all([_bar("CRM", day, 100), _bar("SPY", day, 100), _bar("CRM", day + timedelta(days=7), 90), _bar("SPY", day + timedelta(days=7), 101)])
        snapshot = _snapshot(
            db,
            datetime.combine(day, datetime.min.time(), tzinfo=UTC) + timedelta(hours=8),
            direction="bearish",
            legacy_reference_price=100,
        )
        db.commit()
        summary = outcome_ledger_summary(db, horizon="7D")
        assert summary["accuracy"] == 100


def test_average_excess_return_is_security_minus_spy():
    engine = _engine()
    with Session(engine) as db:
        day = datetime.now(UTC).date() - timedelta(days=10)
        db.add_all([_bar("CRM", day, 100), _bar("SPY", day, 100), _bar("CRM", day + timedelta(days=7), 110), _bar("SPY", day + timedelta(days=7), 104)])
        snapshot = _snapshot(
            db,
            datetime.combine(day, datetime.min.time(), tzinfo=UTC) + timedelta(hours=8),
            legacy_reference_price=100,
        )
        db.commit()
        summary = outcome_ledger_summary(db, horizon="7D")
        assert summary["average_directional_excess_return"] == 6.0
