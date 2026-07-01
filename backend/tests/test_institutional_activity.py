from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session
from starlette.requests import Request

from app.db import Base
from app import ingest_institutional_activity as ingest_module
from app.models import Event, InstitutionalActivityEvent, InstitutionalFiling, InstitutionalPositionChange, InstitutionalSymbolSummary
from app.routers.institutional import ticker_institutional_activity
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


def _session(engine):
    return Session(engine, autoflush=False)


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


def _request(tier: str | None = None) -> Request:
    headers = []
    if tier:
        headers.append((b"x-ct-entitlement-tier", tier.encode("utf-8")))
    return Request({"type": "http", "method": "GET", "path": "/", "headers": headers})


def _process_single_change(
    db: Session,
    *,
    symbol: str,
    prior_row: dict | None,
    current_row: dict | None,
    filing_days_ago: int = 5,
    holder: str = "Blue Ridge Capital",
) -> dict[str, int | str]:
    today = date.today()
    cik = "0001234567"
    prior_candidate = parse_latest_filing(_filing_row(cik=cik, filing_date=today - timedelta(days=95), year=2025, quarter=4, holder=holder))
    current_candidate = parse_latest_filing(
        _filing_row(cik=cik, filing_date=today - timedelta(days=filing_days_ago), year=2026, quarter=1, holder=holder)
    )
    assert prior_candidate is not None
    assert current_candidate is not None

    upsert_institutional_holder(db, prior_candidate)
    prior_filing, _ = upsert_institutional_filing(db, prior_candidate)
    db.flush()
    if prior_row is not None:
        upsert_positions_for_filing(db, filing=prior_filing, rows=[{**prior_row, "symbol": symbol}])

    upsert_institutional_holder(db, current_candidate)
    current_filing, _ = upsert_institutional_filing(db, current_candidate)
    db.flush()
    if current_row is not None:
        upsert_positions_for_filing(db, filing=current_filing, rows=[{**current_row, "symbol": symbol}])

    return process_filing_changes_and_events(db, current_filing)


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


def test_parse_latest_filing_handles_latest_endpoint_payload_shape():
    candidate = parse_latest_filing(
        {
            "acceptedDate": "2026-07-01 12:49:57",
            "cik": "0001452208",
            "date": "2026-06-30",
            "filingDate": "2026-07-01 00:00:00",
            "finalLink": "https://www.sec.gov/Archives/edgar/data/1452208/000110465926079776/xslForm13F_X02/primary_doc.xml",
            "formType": "13F-HR",
            "link": "https://www.sec.gov/Archives/edgar/data/1452208/000110465926079776/0001104659-26-079776-index.htm",
            "name": "CACTI ASSET MANAGEMENT LLC",
        }
    )

    assert candidate is not None
    assert candidate.cik == "0001452208"
    assert candidate.holder_name == "CACTI ASSET MANAGEMENT LLC"
    assert candidate.filing_date == date(2026, 7, 1)
    assert candidate.report_period_end == date(2026, 6, 30)
    assert candidate.report_year == 2026
    assert candidate.report_quarter == 2
    assert candidate.accession_number == "0001104659-26-079776"
    assert candidate.form_type == "13F-HR"
    assert candidate.filing_url == "https://www.sec.gov/Archives/edgar/data/1452208/000110465926079776/0001104659-26-079776-index.htm"


def test_latest_ingest_metrics_split_parse_failures(monkeypatch):
    class DummySession:
        def close(self):
            pass

    monkeypatch.setattr(ingest_module, "ensure_institutional_activity_schema", lambda _engine: None)
    monkeypatch.setattr(ingest_module, "SessionLocal", lambda: DummySession())
    monkeypatch.setattr(ingest_module, "fetch_latest_institutional_filings", lambda **_kwargs: [{"filingDate": "2026-07-01"}])

    result = ingest_module.ingest_latest_institutional_filings(pages=1, limit=5, max_filings=1)

    assert result["scanned"] == 1
    assert result["parsed"] == 0
    assert result["parse_failed"] == 1
    assert result["already_processed_skipped"] == 0
    assert result["skipped"] == 1
    assert result["processed_filings"] == 0


def test_latest_ingest_metrics_split_already_processed_skips(monkeypatch):
    engine = _engine()
    latest_row = {
        "cik": "0001452208",
        "date": "2026-06-30",
        "filingDate": "2026-07-01 00:00:00",
        "formType": "13F-HR",
        "link": "https://www.sec.gov/Archives/edgar/data/1452208/000110465926079776/0001104659-26-079776-index.htm",
        "name": "CACTI ASSET MANAGEMENT LLC",
    }
    candidate = parse_latest_filing(latest_row)
    assert candidate is not None

    with _session(engine) as db:
        upsert_institutional_holder(db, candidate)
        filing, _ = upsert_institutional_filing(db, candidate)
        filing.processed_at = datetime.now(timezone.utc)
        db.commit()

    monkeypatch.setattr(ingest_module, "ensure_institutional_activity_schema", lambda _engine: None)
    monkeypatch.setattr(ingest_module, "SessionLocal", lambda: _session(engine))
    monkeypatch.setattr(ingest_module, "fetch_latest_institutional_filings", lambda **_kwargs: [latest_row])
    monkeypatch.setattr(
        ingest_module,
        "fetch_institutional_filing_extract",
        lambda **_kwargs: pytest.fail("already-processed latest filing should not fetch positions"),
    )

    result = ingest_module.ingest_latest_institutional_filings(pages=1, limit=5, max_filings=1)

    assert result["scanned"] == 1
    assert result["parsed"] == 1
    assert result["parse_failed"] == 0
    assert result["already_processed_skipped"] == 1
    assert result["skipped"] == 1
    assert result["processed_filings"] == 0


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


@pytest.mark.parametrize(
    ("symbol", "prior_row", "current_row", "change_type", "event_type", "direction"),
    [
        (
            "EXIT",
            {"shares": 400_000, "marketValue": 65_000_000, "cusip": "000EXIT01"},
            None,
            "exit",
            "major_holder_exit",
            "bearish",
        ),
        (
            "REDU",
            {"shares": 400_000, "marketValue": 65_000_000, "cusip": "000REDU01"},
            {"shares": 100_000, "marketValue": 5_000_000, "cusip": "000REDU01"},
            "decrease",
            "major_holder_reduction",
            "bearish",
        ),
        (
            "INCR",
            {"shares": 100_000, "marketValue": 5_000_000, "cusip": "000INCR01"},
            {"shares": 400_000, "marketValue": 65_000_000, "cusip": "000INCR01"},
            "increase",
            "institutional_accumulation",
            "bullish",
        ),
        (
            "NEWP",
            None,
            {"shares": 200_000, "marketValue": 25_000_000, "cusip": "000NEWP01"},
            "new_position",
            "new_institutional_position",
            "bullish",
        ),
    ],
)
def test_material_position_changes_create_activity_events_with_autoflush_disabled(
    symbol: str,
    prior_row: dict | None,
    current_row: dict | None,
    change_type: str,
    event_type: str,
    direction: str,
):
    engine = _engine()

    with _session(engine) as db:
        counts = _process_single_change(db, symbol=symbol, prior_row=prior_row, current_row=current_row)
        db.commit()

        assert counts["changes"] == 1
        assert counts["activity_events"] >= 1
        change = db.execute(select(InstitutionalPositionChange).where(InstitutionalPositionChange.normalized_symbol == symbol)).scalar_one()
        assert change.change_type == change_type
        assert change.direction == direction
        assert change.is_material is True

        activity = db.execute(
            select(InstitutionalActivityEvent).where(
                InstitutionalActivityEvent.normalized_symbol == symbol,
                InstitutionalActivityEvent.event_type == event_type,
            )
        ).scalar_one()
        assert activity.direction == direction
        assert activity.source_label == "Institutional Activity"
        assert activity.reported_value_usd is not None
        assert "13F" in activity.summary
        assert "Buy" not in activity.title
        assert "Sell" not in activity.title


def test_stale_material_filing_still_creates_activity_event_without_30d_confirmation():
    engine = _engine()

    with _session(engine) as db:
        counts = _process_single_change(
            db,
            symbol="STALE",
            prior_row={"shares": 100_000, "marketValue": 5_000_000, "cusip": "00STALE01"},
            current_row={"shares": 400_000, "marketValue": 65_000_000, "cusip": "00STALE01"},
            filing_days_ago=45,
        )
        db.commit()

        assert counts["activity_events"] >= 1
        activity = db.execute(
            select(InstitutionalActivityEvent).where(
                InstitutionalActivityEvent.normalized_symbol == "STALE",
                InstitutionalActivityEvent.event_type == "institutional_accumulation",
            )
        ).scalar_one()
        assert activity.freshness_status == "stale"
        assert activity.confirmation_score == 0

        summaries, availability = get_institutional_activity_summaries_for_symbols(db, ["STALE"], lookback_days=30)
        assert availability["status"] == "ok"
        assert summaries["STALE"]["active"] is False
        assert summaries["STALE"]["confirmation_contribution"] == 0


def test_institutional_activity_endpoint_redacts_details_until_pro(monkeypatch):
    monkeypatch.setenv("CT_DEFAULT_TIER", "free")
    monkeypatch.setenv("CT_ALLOW_ENTITLEMENT_HEADER", "1")
    engine = _engine()

    with _session(engine) as db:
        _process_single_change(
            db,
            symbol="LOCK",
            prior_row={"shares": 100_000, "marketValue": 5_000_000, "cusip": "000LOCK01"},
            current_row={"shares": 400_000, "marketValue": 65_000_000, "cusip": "000LOCK01"},
        )
        db.commit()

        for tier in (None, "free", "premium"):
            payload = ticker_institutional_activity("LOCK", _request(tier), lookback_days=365, limit=5, db=db)
            assert payload["locked"] is True
            assert payload["items"] == []
            assert payload["summary"]["total_value"] is None
            assert payload["summary"]["institution_count"] is None

        for tier in ("pro", "admin"):
            payload = ticker_institutional_activity("LOCK", _request(tier), lookback_days=365, limit=5, db=db)
            assert payload.get("locked") is not True
            assert payload["summary"]["locked"] is False
            holder_event = next(item for item in payload["items"] if item["event_type"] == "institutional_accumulation")
            assert holder_event["cik"] == "0001234567"
            assert holder_event["reported_value_usd"] == 65_000_000


def test_rerunning_processing_does_not_duplicate_activity_or_feed_events():
    engine = _engine()

    with _session(engine) as db:
        _process_single_change(
            db,
            symbol="RERUN",
            prior_row={"shares": 400_000, "marketValue": 65_000_000, "cusip": "00RERUN01"},
            current_row=None,
        )
        db.commit()
        activity_count = db.query(InstitutionalActivityEvent).count()
        feed_count = db.query(Event).count()
        filing = db.execute(
            select(InstitutionalFiling).where(
                InstitutionalFiling.cik == "0001234567",
                InstitutionalFiling.report_year == 2026,
                InstitutionalFiling.report_quarter == 1,
            )
        ).scalar_one()

        counts = process_filing_changes_and_events(db, filing)
        db.commit()

        assert counts["changes"] == 1
        assert db.query(InstitutionalActivityEvent).count() == activity_count
        assert db.query(Event).count() == feed_count


def test_process_filing_changes_creates_summary_and_activity_event():
    engine = _engine()
    today = date.today()
    cik = "0001234567"

    with _session(engine) as db:
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

    with _session(engine) as db:
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
