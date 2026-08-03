from __future__ import annotations

import json
from datetime import date, datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.db import Base
from app.models import (
    Event,
    InstitutionalActivityEvent,
    InstitutionalFiling,
    InstitutionalHolder,
    InstitutionalPosition,
    InstitutionalPositionChange,
    InstitutionalSymbolSummary,
)
from app.tools.repair_sparse_historical_13f_dates import run_repair


def _session() -> Session:
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    return Session(engine, autoflush=False)


def _seed_bad_sparse_13f(db: Session) -> int:
    old_date = date(2026, 3, 31)
    db.add(InstitutionalHolder(cik="0000093751", holder_name="STATE STREET CORP", latest_filing_date=old_date))
    filing = InstitutionalFiling(
        cik="0000093751",
        filing_date=old_date,
        report_year=2024,
        report_quarter=1,
        report_period_end=old_date,
        raw_metadata_json=json.dumps({"cik": "0000093751", "date": "2026-03-31", "year": 2024, "quarter": 1}),
    )
    db.add(filing)
    db.flush()
    db.add(
        InstitutionalPosition(
            filing_id=filing.id,
            cik=filing.cik,
            symbol="AAPL",
            normalized_symbol="AAPL",
            shares=10,
            value_usd=1000,
            report_year=2024,
            report_quarter=1,
            filing_date=old_date,
        )
    )
    db.add(
        InstitutionalPositionChange(
            cik=filing.cik,
            holder_name="STATE STREET CORP",
            symbol="AAPL",
            normalized_symbol="AAPL",
            report_year=2024,
            report_quarter=1,
            filing_date=old_date,
            curr_shares=10,
            curr_value_usd=1000,
            change_type="new_position",
            direction="bullish",
            materiality_score=50,
            passive_adjusted_score=30,
            is_material=True,
        )
    )
    activity = InstitutionalActivityEvent(
        symbol="AAPL",
        normalized_symbol="AAPL",
        cik=filing.cik,
        holder_name="STATE STREET CORP",
        event_type="new_institutional_position",
        direction="bullish",
        title="State Street opened AAPL",
        summary="Bad old date",
        filing_date=old_date,
        report_year=2024,
        report_quarter=1,
        materiality_score=50,
        feed_visible=True,
    )
    db.add(activity)
    db.flush()
    event_dt = datetime(2026, 3, 31, tzinfo=timezone.utc)
    db.add(
        Event(
            event_type="new_institutional_position",
            ts=event_dt,
            event_date=event_dt,
            symbol="AAPL",
            source="13F filing",
            impact_score=50,
            payload_json="{}",
            source_provider="institutional_13f",
            source_filing_id=f"institutional:{activity.id}:new_institutional_position:2024q1",
        )
    )
    db.commit()
    return int(filing.id)


def test_repair_sparse_historical_13f_dates_dry_run_keeps_rows_unchanged():
    db = _session()
    try:
        filing_id = _seed_bad_sparse_13f(db)

        result = run_repair(db, apply=False)
        db.rollback()

        filing = db.get(InstitutionalFiling, filing_id)
        assert result["mode"] == "dry_run"
        assert result["target_count"] == 1
        assert result["totals"]["positions"] == 1
        assert result["totals"]["position_changes"] == 1
        assert result["totals"]["activity_events"] == 1
        assert result["totals"]["feed_events"] == 1
        assert filing.filing_date == date(2026, 3, 31)
        assert filing.report_period_end == date(2026, 3, 31)
    finally:
        db.close()


def test_repair_sparse_historical_13f_dates_apply_updates_downstream_dates():
    db = _session()
    try:
        filing_id = _seed_bad_sparse_13f(db)

        result = run_repair(db, apply=True)
        db.commit()

        filing = db.get(InstitutionalFiling, filing_id)
        position = db.query(InstitutionalPosition).one()
        change = db.query(InstitutionalPositionChange).one()
        activity = db.query(InstitutionalActivityEvent).one()
        feed_event = db.query(Event).one()
        raw = json.loads(filing.raw_metadata_json or "{}")

        assert result["mode"] == "apply"
        assert filing.filing_date == date(2024, 5, 15)
        assert filing.report_period_end == date(2024, 3, 31)
        assert raw["_walnut_filing_date_source"] == "estimated_13f_deadline"
        assert raw["_walnut_original_filing_date"] == "2026-03-31"
        assert position.filing_date == date(2024, 5, 15)
        assert change.filing_date == date(2024, 5, 15)
        assert activity.filing_date == date(2024, 5, 15)
        assert feed_event.ts.date() == date(2024, 5, 15)
        assert feed_event.event_date.date() == date(2024, 5, 15)
    finally:
        db.close()


def test_repair_sparse_historical_13f_dates_repairs_aggregate_rows():
    db = _session()
    try:
        old_date = date(2026, 3, 31)
        db.add(
            InstitutionalSymbolSummary(
                symbol="AAPL",
                normalized_symbol="AAPL",
                report_year=2024,
                report_quarter=1,
                latest_filing_date=old_date,
            )
        )
        activity = InstitutionalActivityEvent(
            symbol="AAPL",
            normalized_symbol="AAPL",
            cik=None,
            event_type="cluster_accumulation",
            direction="bullish",
            title="Institutions report net accumulation in AAPL",
            summary="Bad aggregate date",
            filing_date=old_date,
            report_year=2024,
            report_quarter=1,
            materiality_score=50,
            feed_visible=True,
        )
        db.add(activity)
        db.flush()
        event_dt = datetime(2026, 3, 31, tzinfo=timezone.utc)
        db.add(
            Event(
                event_type="cluster_accumulation",
                ts=event_dt,
                event_date=event_dt,
                symbol="AAPL",
                source="13F filing",
                impact_score=50,
                payload_json="{}",
                source_provider="institutional_13f",
                source_filing_id=f"institutional:{activity.id}:cluster_accumulation:2024q1",
            )
        )
        db.commit()

        dry_run = run_repair(db, apply=False, repair_aggregate_rows=True)
        db.rollback()
        assert dry_run["totals"]["aggregate_activity_events"] == 1
        assert dry_run["totals"]["aggregate_symbol_summaries"] == 1
        assert dry_run["totals"]["aggregate_feed_events"] == 1

        result = run_repair(db, apply=True, repair_aggregate_rows=True)
        db.commit()

        summary = db.query(InstitutionalSymbolSummary).one()
        activity = db.query(InstitutionalActivityEvent).one()
        feed_event = db.query(Event).one()
        assert result["totals"]["aggregate_activity_events"] == 1
        assert summary.latest_filing_date == date(2024, 5, 15)
        assert activity.filing_date == date(2024, 5, 15)
        assert feed_event.ts.date() == date(2024, 5, 15)
        assert feed_event.event_date.date() == date(2024, 5, 15)
    finally:
        db.close()
