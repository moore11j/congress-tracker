from __future__ import annotations

from datetime import date, timedelta

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from app.db import Base
from app.models import Event, InstitutionalActivityEvent, InstitutionalPositionChange, InstitutionalSymbolSummary
from app.services.institutional_activity import (
    institutional_confirmation_contribution,
    get_institutional_activity_summaries_for_symbols,
    parse_latest_filing,
    process_filing_changes_and_events,
    upsert_institutional_filing,
    upsert_institutional_holder,
    upsert_positions_for_filing,
)


def _engine():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    return engine


def _filing_row(*, cik: str, filing_date: date, year: int, quarter: int, holder: str = "Blue Ridge Capital") -> dict:
    return {
        "cik": cik,
        "holderName": holder,
        "filingDate": filing_date.isoformat(),
        "year": year,
        "quarter": quarter,
        "formType": "13F-HR",
        "accessionNumber": f"{cik}-{year}-{quarter}",
    }


def test_parse_latest_filing_normalizes_13f_metadata():
    candidate = parse_latest_filing(
        {
            "cik": "1234567",
            "institutionName": "Blue Ridge Capital",
            "filingDate": "2026-02-14",
            "periodOfReport": "2025-12-31",
            "formType": "13F-HR/A",
        }
    )

    assert candidate is not None
    assert candidate.cik == "0001234567"
    assert candidate.report_year == 2025
    assert candidate.report_quarter == 4
    assert candidate.is_amendment is True


def test_institutional_contribution_is_freshness_bounded_and_capped():
    today = date.today()

    fresh = institutional_confirmation_contribution(
        filing_date=today - timedelta(days=1),
        materiality_score=100,
        direction="bullish",
        holder_quality_weight=3,
    )
    stale = institutional_confirmation_contribution(
        filing_date=today - timedelta(days=45),
        materiality_score=100,
        direction="bullish",
    )
    bearish = institutional_confirmation_contribution(
        filing_date=today - timedelta(days=1),
        materiality_score=100,
        direction="bearish",
        holder_quality_weight=3,
    )

    assert fresh == 15
    assert stale == 0
    assert bearish == -15


def test_process_filing_changes_creates_summary_and_activity_event():
    engine = _engine()
    today = date.today()
    cik = "0001234567"

    with Session(engine) as db:
        prior_candidate = parse_latest_filing(_filing_row(cik=cik, filing_date=today - timedelta(days=95), year=2025, quarter=4))
        current_candidate = parse_latest_filing(_filing_row(cik=cik, filing_date=today - timedelta(days=5), year=2026, quarter=1))
        assert prior_candidate is not None
        assert current_candidate is not None

        upsert_institutional_holder(db, prior_candidate)
        prior_filing, _ = upsert_institutional_filing(db, prior_candidate)
        db.flush()
        upsert_positions_for_filing(
            db,
            filing=prior_filing,
            rows=[{"symbol": "NVDA", "shares": 100_000, "marketValue": 5_000_000, "cusip": "67066G104"}],
        )

        upsert_institutional_holder(db, current_candidate)
        current_filing, _ = upsert_institutional_filing(db, current_candidate)
        db.flush()
        upsert_positions_for_filing(
            db,
            filing=current_filing,
            rows=[{"symbol": "NVDA", "shares": 400_000, "marketValue": 65_000_000, "cusip": "67066G104"}],
        )

        counts = process_filing_changes_and_events(db, current_filing)
        db.commit()

        assert counts["changes"] == 1
        change = db.execute(select(InstitutionalPositionChange)).scalar_one()
        assert change.change_type == "increase"
        assert change.direction == "bullish"
        assert change.value_delta_usd == 60_000_000
        assert change.is_material is True

        summary = db.execute(select(InstitutionalSymbolSummary).where(InstitutionalSymbolSummary.normalized_symbol == "NVDA")).scalar_one()
        assert summary.holders_increased == 1
        assert summary.direction == "bullish"
        assert summary.net_value_delta_usd == 60_000_000

        activity = db.execute(
            select(InstitutionalActivityEvent).where(
                InstitutionalActivityEvent.normalized_symbol == "NVDA",
                InstitutionalActivityEvent.event_type == "institutional_accumulation",
            )
        ).scalar_one()
        assert activity.event_type == "institutional_accumulation"
        assert activity.source_label == "Institutional Activity"
        assert "13F" in activity.summary

        summaries, availability = get_institutional_activity_summaries_for_symbols(db, ["NVDA"], lookback_days=30)
        assert availability["status"] == "ok"
        assert summaries["NVDA"]["active"] is True
        assert summaries["NVDA"]["confirmation_contribution"] > 0
        assert summaries["NVDA"]["confirmation_contribution"] <= 15
        assert summaries["NVDA"]["source_label"] == "Institutional Activity"


def test_amended_filing_stores_but_suppresses_user_facing_events():
    engine = _engine()
    today = date.today()
    cik = "0001234567"

    with Session(engine) as db:
        prior_candidate = parse_latest_filing(_filing_row(cik=cik, filing_date=today - timedelta(days=95), year=2025, quarter=4))
        current_candidate = parse_latest_filing(_filing_row(cik=cik, filing_date=today - timedelta(days=5), year=2026, quarter=1))
        assert prior_candidate is not None
        assert current_candidate is not None

        upsert_institutional_holder(db, prior_candidate)
        prior_filing, _ = upsert_institutional_filing(db, prior_candidate)
        db.flush()
        upsert_positions_for_filing(
            db,
            filing=prior_filing,
            rows=[{"symbol": "NVDA", "shares": 100_000, "marketValue": 5_000_000, "cusip": "67066G104"}],
        )

        upsert_institutional_holder(db, current_candidate)
        current_filing, _ = upsert_institutional_filing(db, current_candidate)
        db.flush()
        upsert_positions_for_filing(
            db,
            filing=current_filing,
            rows=[{"symbol": "NVDA", "shares": 400_000, "marketValue": 65_000_000, "cusip": "67066G104"}],
        )
        process_filing_changes_and_events(db, current_filing)
        db.commit()
        feed_events_before = db.query(Event).filter(Event.event_type.in_(("institutional_accumulation", "institutional_distribution"))).count()
        activity_events_before = db.query(InstitutionalActivityEvent).count()

        amendment_row = _filing_row(cik=cik, filing_date=today - timedelta(days=1), year=2026, quarter=1)
        amendment_row["formType"] = "13F-HR/A"
        amendment_row["accessionNumber"] = f"{cik}-2026-1-A"
        amendment_candidate = parse_latest_filing(amendment_row)
        assert amendment_candidate is not None
        assert amendment_candidate.is_amendment is True

        amended_filing, _ = upsert_institutional_filing(db, amendment_candidate)
        db.flush()
        upsert_positions_for_filing(
            db,
            filing=amended_filing,
            rows=[{"symbol": "NVDA", "shares": 800_000, "marketValue": 120_000_000, "cusip": "67066G104"}],
        )
        counts = process_filing_changes_and_events(db, amended_filing)
        db.commit()

        assert counts["amendment_suppressed"] == 1
        assert counts["activity_events"] == 0
        assert counts["feed_events"] == 0
        assert db.query(Event).filter(Event.event_type.in_(("institutional_accumulation", "institutional_distribution"))).count() == feed_events_before
        assert db.query(InstitutionalActivityEvent).count() == activity_events_before
