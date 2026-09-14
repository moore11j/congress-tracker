from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import create_engine, event as sqlalchemy_event, select
from sqlalchemy.orm import Session

from app.db import Base, ensure_outcome_ledger_schema
from app.models import (
    ConfirmationScoreSnapshot,
    LeaderboardSnapshot,
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
    materialize_cached_outcome_horizons,
)
from app.services.outcome_ledger import _date_spread_sample, _project_directional_outcome_events, _snapshot_row, list_outcome_snapshots, outcome_ledger_summary
from app.services.price_lookup import EodPriceBar, is_market_trading_day, reconstruct_adjusted_price_bars

UTC = timezone.utc


def test_public_horizon_repair_batches_over_100_anchors_and_ignores_internal_versions(monkeypatch):
    from app.services import outcome_horizon_repair as repair
    from app.models import OutcomeEntry

    engine = _engine()
    with Session(engine) as db:
        day = date(2026, 1, 5)
        db.add_all([_bar("CRM", day, 100), _bar("SPY", day, 500)])
        for i in range(105):
            snapshot = _snapshot(db, datetime(2026, 1, 5, 13, tzinfo=UTC), security_id=i + 1)
            assert materialize_outcome_entry(db, snapshot)
        # An internal scoring upgrade has its own entry but is not a new public event.
        db.add_all([_bar("CRM", date(2026, 1, 6), 102), _bar("SPY", date(2026, 1, 6), 501)])
        internal = _snapshot(db, datetime(2026, 1, 6, 13, tzinfo=UTC), security_id=1, methodology_version_id=2)
        assert materialize_outcome_entry(db, internal)
        db.commit()
        calls = []

        def hydrate(session, symbol, start, end):
            calls.append((symbol, start, end))
            session.merge(_bar(symbol, date(2026, 1, 12), 110 if symbol == "CRM" else 505))
            session.commit()
            return 1

        monkeypatch.setattr(repair, "hydrate_split_adjusted_ohlc", hydrate)
        result = repair.repair_public_outcome_horizons(db, as_of=date(2026, 1, 13))
        assert result["due_observations"] == 105
        assert result["observations_created"] == 105
        assert calls == [("SPY", "2026-01-12", "2026-01-12"), ("CRM", "2026-01-12", "2026-01-12")]
        assert db.scalar(select(OutcomeHorizonObservation).where(OutcomeHorizonObservation.snapshot_id == internal.id)) is None
        calls.clear()
        assert repair.repair_public_outcome_horizons(db, as_of=date(2026, 1, 13))["observations_created"] == 0
        assert calls == []
        assert len(db.scalars(select(OutcomeEntry)).all()) == 106


def test_public_horizon_repair_rotates_past_failed_symbols_between_runs(monkeypatch):
    from app.services import outcome_horizon_repair as repair

    engine = _engine()
    with Session(engine) as db:
        for i, symbol in enumerate(["AAA", "BBB"]):
            db.merge(_bar("SPY", date(2026, 1, 5), 500))
            db.add(_bar(symbol, date(2026, 1, 5), 100))
            snapshot = _snapshot(db, datetime(2026, 1, 5, 13, tzinfo=UTC), ticker=symbol, security_id=i + 1)
            assert materialize_outcome_entry(db, snapshot)
        db.commit()
        clock = [0]
        calls = []
        monkeypatch.setattr(repair.time, "monotonic", lambda: clock[0])

        def hydrate(session, symbol, start, end):
            calls.append(symbol)
            clock[0] += 1 if symbol == "SPY" else 10
            if symbol == "AAA":
                raise RuntimeError("provider has no usable price")
            session.merge(_bar(symbol, date(2026, 1, 12), 110 if symbol == "BBB" else 505))
            session.commit()
            return 1

        monkeypatch.setattr(repair, "hydrate_split_adjusted_ohlc", hydrate)
        first = repair.repair_public_outcome_horizons(db, as_of=date(2026, 1, 13), max_seconds=10)
        assert first["attempted_symbols"] == 1 and first["observations_created"] == 0
        assert calls == ["SPY", "AAA"]
        calls.clear()
        second = repair.repair_public_outcome_horizons(db, as_of=date(2026, 1, 13), max_seconds=10)
        assert calls == ["SPY", "BBB"]
        assert second["observations_created"] == 1


def test_public_horizon_repair_refreshes_mutable_prices_without_expiring_all_snapshots(monkeypatch):
    from sqlalchemy import update
    from app.services import outcome_horizon_repair as repair

    with Session(_engine()) as db:
        db.add_all([_bar("CRM", date(2026, 1, 5), 100), _bar("SPY", date(2026, 1, 5), 500)])
        snapshot = _snapshot(db, datetime(2026, 1, 5, 13, tzinfo=UTC))
        entry = materialize_outcome_entry(db, snapshot)
        stale_price = _bar("CRM", date(2026, 1, 12), 90)
        stale_price.adjustment_status = None
        db.add_all([stale_price, _bar("SPY", date(2026, 1, 12), 500)])
        db.commit()
        assert stale_price.close == 90

        def hydrate(session, symbol, start, end):
            assert session.expire_on_commit is False
            price = 110 if symbol == "CRM" else 505
            session.execute(update(PriceCache).where(
                PriceCache.symbol == symbol, PriceCache.date == "2026-01-12",
            ).values(close=price, adjusted_close=price, raw_close=price, adjustment_status="split_adjusted_price_return")
                .execution_options(synchronize_session=False))
            session.commit()
            return 1

        monkeypatch.setattr(repair, "hydrate_split_adjusted_ohlc", hydrate)
        assert repair.repair_public_outcome_horizons(db, as_of=date(2026, 1, 12))["observations_created"] == 1
        assert db.expire_on_commit is True
        observation = db.scalar(select(OutcomeHorizonObservation).where(OutcomeHorizonObservation.entry_id == entry.id))
        assert observation.security_return_pct == pytest.approx(10)
        assert observation.benchmark_return_pct == pytest.approx(1)


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
    methodology_version_id: int = 1,
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
        methodology_version_id=methodology_version_id,
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

        query_count = 0

        def count_query(*_args):
            nonlocal query_count
            query_count += 1

        sqlalchemy_event.listen(engine, "before_cursor_execute", count_query)
        try:
            payload = list_outcome_snapshots(db, limit=2, balanced_horizon="7D")
        finally:
            sqlalchemy_event.remove(engine, "before_cursor_execute", count_query)

        assert {item["ticker"] for item in payload["items"]} == {"DONE", "OPEN"}
        by_ticker = {item["ticker"]: item for item in payload["items"]}
        assert by_ticker["DONE"]["outcomes"]["7D"]["status"] == "matured"
        assert by_ticker["OPEN"]["outcomes"]["7D"]["status"] == "pending"
        assert by_ticker["OPEN"]["live_mark"]["status"] == "provisional"
        assert by_ticker["OPEN"]["live_mark"]["price_date"] == (pending_day + timedelta(days=1)).isoformat()
        assert by_ticker["OPEN"]["live_mark"]["return_pct"] == 10.0
        assert query_count <= 10


def test_date_spread_sample_represents_neighboring_entry_sessions_before_repeating_busy_days():
    entry_dates = {
        "new-a": date(2026, 9, 2),
        "new-b": date(2026, 9, 2),
        "new-c": date(2026, 9, 2),
        "middle": date(2026, 8, 25),
        "old": date(2026, 8, 5),
    }

    sample = _date_spread_sample(list(entry_dates), 4, entry_dates.__getitem__)

    assert sample == ["new-a", "middle", "old", "new-b"]


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
        exit_day = target
        while not is_market_trading_day(exit_day):
            exit_day += timedelta(days=1)
        snapshot = _snapshot(db, datetime(2024, 1, 2, 13, tzinfo=UTC))
        db.add_all([_bar("CRM", entry_day, 100), _bar("SPY", entry_day, 500), _bar("CRM", exit_day, 110), _bar("SPY", exit_day, 505)])
        db.flush()
        entry = materialize_outcome_entry(db, snapshot)
        rows = materialize_outcome_horizons(db, entry, as_of=exit_day)
        row = next(item for item in rows if item.horizon_days == days)
        assert row.target_date == target and row.security_session_date == exit_day


def test_tsm_holiday_cache_row_does_not_block_thirty_day_measurement():
    engine = _engine()
    with Session(engine) as db:
        snapshot = _snapshot(db, datetime(2026, 8, 5, 17, tzinfo=UTC), ticker="TSM")
        db.add_all([
            _bar("TSM", date(2026, 8, 6), 409.54), _bar("SPY", date(2026, 8, 6), 760),
            PriceCache(symbol="SPY", date="2026-09-07", close=770.24),
            PriceCache(symbol="TSM", date="2026-09-07", close=428.91),
            _bar("TSM", date(2026, 9, 8), 437, 439), _bar("SPY", date(2026, 9, 8), 767, 765.96),
        ])
        db.flush()
        entry = materialize_outcome_entry(db, snapshot)
        assert entry is not None
        assert materialize_outcome_horizons(db, entry, as_of=date(2026, 9, 7)) == []
        first = materialize_cached_outcome_horizons(db, as_of=date(2026, 9, 11))
        assert first == {"entries_checked": 1, "observations_created": 1}
        observation = db.execute(select(OutcomeHorizonObservation)).scalar_one()
        assert observation.horizon_days == 30
        assert observation.security_session_date == observation.benchmark_session_date == date(2026, 9, 8)
        assert observation.security_return_pct == pytest.approx((439 / 409.54 - 1) * 100)
        assert materialize_cached_outcome_horizons(db, as_of=date(2026, 9, 11))["observations_created"] == 0


def test_missing_target_session_is_not_replaced_by_later_cached_prices():
    engine = _engine()
    with Session(engine) as db:
        snapshot = _snapshot(db, datetime(2026, 1, 5, 13, tzinfo=UTC))
        db.add_all([
            _bar("CRM", date(2026, 1, 5), 100), _bar("SPY", date(2026, 1, 5), 500),
            _bar("CRM", date(2026, 1, 13), 110), _bar("SPY", date(2026, 1, 13), 510),
        ])
        db.flush()
        entry = materialize_outcome_entry(db, snapshot)
        assert entry is not None
        assert materialize_outcome_horizons(db, entry, as_of=date(2026, 1, 13)) == []


def test_cached_repair_is_not_limited_to_one_hundred_entries():
    engine = _engine()
    with Session(engine) as db:
        db.add_all([_bar("SPY", date(2026, 1, 5), 500), _bar("SPY", date(2026, 1, 12), 510)])
        for index in range(105):
            symbol = f"T{index}"
            snapshot = _snapshot(db, datetime(2026, 1, 5, 13, tzinfo=UTC), ticker=symbol, security_id=index + 1)
            db.add_all([_bar(symbol, date(2026, 1, 5), 100), _bar(symbol, date(2026, 1, 12), 110)])
            db.flush()
            assert materialize_outcome_entry(db, snapshot) is not None
        report = materialize_cached_outcome_horizons(db, as_of=date(2026, 1, 12))
        assert report == {"entries_checked": 105, "observations_created": 105}
        assert materialize_cached_outcome_horizons(db, as_of=date(2026, 1, 12))["observations_created"] == 0


def test_published_leader_is_in_preview_and_search_finds_older_events():
    engine = _engine()
    with Session(engine) as db:
        for index, (symbol, day) in enumerate([("TSM", date(2026, 8, 6)), ("NEW", date(2026, 9, 1))]):
            snapshot = _snapshot(db, datetime.combine(day, time=datetime.min.time(), tzinfo=UTC).replace(hour=12), ticker=symbol, security_id=index + 1)
            db.add_all([_bar(symbol, day, 100), _bar("SPY", day, 500)])
            db.flush()
            assert materialize_outcome_entry(db, snapshot) is not None
        db.add(LeaderboardSnapshot(leaderboard_key="top_stocks", generated_at=datetime.now(UTC), payload_json='{"items":[{"symbol":"TSM"}]}'))
        db.flush()
        assert list_outcome_snapshots(db, limit=1, balanced_horizon="30D")["items"][0]["ticker"] == "TSM"
        found = list_outcome_snapshots(db, ticker="tsm", limit=500, balanced_horizon="30D")
        assert found["total"] == 1 and found["items"][0]["ticker"] == "TSM"


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
        assert [event.snapshot.id for event in _project_directional_outcome_events([first, later])] == [first.id]


def test_same_day_upgrade_preserves_original_opening():
    engine = _engine()
    with Session(engine) as db:
        first = _snapshot(db, datetime(2026, 8, 5, 14, tzinfo=UTC), score=71)
        upgraded = _snapshot(db, datetime(2026, 8, 5, 15, tzinfo=UTC), score=100, methodology_version_id=2)
        events = _project_directional_outcome_events([upgraded, first])
        assert [event.snapshot.id for event in events] == [first.id]
        assert events[0].closed_at is None


def test_same_direction_score_updates_keep_original_thesis_open():
    engine = _engine()
    with Session(engine) as db:
        first = _snapshot(db, datetime(2026, 1, 5, 14, tzinfo=UTC), score=70)
        next_day = _snapshot(db, datetime(2026, 1, 6, 14, tzinfo=UTC), score=71)
        day_31_small = _snapshot(db, datetime(2026, 2, 5, 14, tzinfo=UTC), score=72)
        day_32_material = _snapshot(db, datetime(2026, 2, 6, 14, tzinfo=UTC), score=81)
        events = _project_directional_outcome_events([first, next_day, day_31_small, day_32_material])
        assert [event.snapshot.id for event in events] == [first.id]
        assert events[0].closed_at is None


def test_methodology_upgrade_and_watch_states_do_not_restart_thesis():
    engine = _engine()
    with Session(engine) as db:
        first = _snapshot(db, datetime(2026, 8, 5, 17, tzinfo=UTC), ticker="TSM", score=71)
        upgraded = _snapshot(db, datetime(2026, 8, 21, 14, tzinfo=UTC), ticker="TSM", score=100, methodology_version_id=2)
        mixed = _snapshot(db, datetime(2026, 8, 25, 14, tzinfo=UTC), ticker="TSM", direction="mixed")
        neutral = _snapshot(db, datetime(2026, 8, 26, 14, tzinfo=UTC), ticker="TSM", direction="neutral")
        later = _snapshot(db, datetime(2026, 9, 13, 14, tzinfo=UTC), ticker="TSM", score=86, methodology_version_id=2)
        events = _project_directional_outcome_events([first, upgraded, mixed, neutral, later])
        assert [event.snapshot.id for event in events] == [first.id]
        assert events[0].closed_at is None


def test_bearish_reversal_across_methodologies_closes_original_thesis():
    engine = _engine()
    with Session(engine) as db:
        first = _snapshot(db, datetime(2026, 8, 5, 17, tzinfo=UTC), ticker="TSM", score=71)
        reversal = _snapshot(db, datetime(2026, 9, 14, 14, tzinfo=UTC), ticker="TSM", direction="bearish", methodology_version_id=2)
        events = _project_directional_outcome_events([first, reversal])
        assert [event.snapshot.id for event in events] == [first.id, reversal.id]
        assert events[0].closed_at == reversal.market_date
        assert events[1].closed_at is None


def test_public_continuity_keeps_current_score_and_hides_versions():
    from app.models import ConfirmationMethodologyVersion
    engine = _engine()
    with Session(engine) as db:
        first = _snapshot(db, datetime(2026, 8, 5, 17, tzinfo=UTC), ticker="TSM", score=71)
        upgraded = _snapshot(db, datetime(2026, 8, 21, 14, tzinfo=UTC), ticker="TSM", score=100, methodology_version_id=2)
        latest = _snapshot(db, datetime(2026, 9, 13, 14, tzinfo=UTC), ticker="TSM", score=86, methodology_version_id=2)
        db.add_all([_bar("TSM", date(2026, 8, 6), 409.54), _bar("SPY", date(2026, 8, 6), 760)])
        db.flush()
        assert materialize_outcome_entry(db, first) is not None
        payload = list_outcome_snapshots(db, ticker="TSM")
        assert payload["total"] == 1
        item = payload["items"][0]
        assert item["id"] == first.id and item["score"] == 71
        assert item["entry_session_date"] == "2026-08-06"
        assert item["current_confirmation"]["score"] == 86
        assert item["current_confirmation"]["calculated_at"] == latest.calculated_at.isoformat()
        assert "methodology" not in item
        assert "methodology" not in item["current_confirmation"]
        # Date filters must not turn the later scoring update into an event.
        assert list_outcome_snapshots(db, ticker="TSM", start_date=date(2026, 8, 20))["total"] == 0
        assert outcome_ledger_summary(db, horizon="30D", start_date=date(2026, 8, 20))["verified_events"] == 0


def test_unverified_older_snapshot_does_not_hide_verified_continuous_entry():
    engine = _engine()
    with Session(engine) as db:
        older = _snapshot(db, datetime(2026, 8, 5, 14, tzinfo=UTC), ticker="TSM", score=70)
        verified = _snapshot(db, datetime(2026, 8, 5, 17, tzinfo=UTC), ticker="TSM", score=71)
        updated = _snapshot(db, datetime(2026, 8, 21, 14, tzinfo=UTC), ticker="TSM", score=100, methodology_version_id=2)
        db.add_all([_bar("TSM", date(2026, 8, 6), 409.54), _bar("SPY", date(2026, 8, 6), 760)])
        db.flush()
        assert materialize_outcome_entry(db, verified) is not None
        result = list_outcome_snapshots(db, ticker="TSM")
        assert result["total"] == 1
        assert result["items"][0]["id"] == verified.id
        assert result["items"][0]["reference_price"] == 409.54
        assert result["items"][0]["current_confirmation"]["score"] == 100
        assert outcome_ledger_summary(db, horizon="30D")["verified_events"] == 1


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
