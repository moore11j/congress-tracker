from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session
from starlette.requests import Request

from app.db import Base
from app import ingest_institutional_activity as ingest_module
from app.services import institutional_activity as institutional_service
from app.models import CikMeta, Event, InstitutionalActivityEvent, InstitutionalFiling, InstitutionalHolder, InstitutionalPosition, InstitutionalPositionChange, InstitutionalSymbolSummary
from app.routers.institutional import institution_activity, institution_filings, institution_holdings, institution_profile, ticker_institutional_activity, ticker_ownership
from app.services.institutional_activity import (
    INSTITUTIONAL_EVENT_SOURCE,
    cleanup_overbroad_institutional_feed_events,
    get_canonical_filing_for_holder_period,
    institutional_confirmation_contribution,
    get_institutional_activity_summaries_for_symbols,
    institutional_filing_duplicate_report,
    is_canonical_institutional_filing,
    filings_for_holder,
    holder_profile,
    materialize_feed_events_for_symbol,
    parse_latest_filing,
    parse_position,
    positions_for_holder,
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


def test_latest_ingest_default_start_page_zero_preserves_fetch_window(monkeypatch):
    class DummySession:
        def close(self):
            pass

    calls = []

    def fake_fetch_latest(*, page: int, limit: int):
        calls.append((page, limit))
        return [{"filingDate": "2026-07-01"}]

    monkeypatch.setattr(ingest_module, "ensure_institutional_activity_schema", lambda _engine: None)
    monkeypatch.setattr(ingest_module, "SessionLocal", lambda: DummySession())
    monkeypatch.setattr(ingest_module, "fetch_latest_institutional_filings", fake_fetch_latest)

    result = ingest_module.ingest_latest_institutional_filings(pages=2, limit=25, max_filings=1)

    assert calls == [(0, 25), (1, 25)]
    assert result["start_page"] == 0
    assert result["pages"] == 2
    assert result["scanned"] == 2
    assert result["parse_failed"] == 2


def test_latest_ingest_start_page_fetches_only_requested_page(monkeypatch):
    class DummySession:
        def close(self):
            pass

    calls = []

    def fake_fetch_latest(*, page: int, limit: int):
        calls.append((page, limit))
        return [{"filingDate": "2026-07-01"}]

    monkeypatch.setattr(ingest_module, "ensure_institutional_activity_schema", lambda _engine: None)
    monkeypatch.setattr(ingest_module, "SessionLocal", lambda: DummySession())
    monkeypatch.setattr(ingest_module, "fetch_latest_institutional_filings", fake_fetch_latest)

    result = ingest_module.ingest_latest_institutional_filings(start_page=3, pages=1, limit=25, max_filings=1)

    assert calls == [(3, 25)]
    assert result["start_page"] == 3
    assert result["pages"] == 1
    assert result["scanned"] == 1


def test_latest_ingest_start_page_and_pages_fetches_contiguous_window(monkeypatch):
    class DummySession:
        def close(self):
            pass

    calls = []

    def fake_fetch_latest(*, page: int, limit: int):
        calls.append((page, limit))
        return [{"filingDate": "2026-07-01"}]

    monkeypatch.setattr(ingest_module, "ensure_institutional_activity_schema", lambda _engine: None)
    monkeypatch.setattr(ingest_module, "SessionLocal", lambda: DummySession())
    monkeypatch.setattr(ingest_module, "fetch_latest_institutional_filings", fake_fetch_latest)

    result = ingest_module.ingest_latest_institutional_filings(start_page=3, pages=2, limit=25, max_filings=1)

    assert calls == [(3, 25), (4, 25)]
    assert result["start_page"] == 3
    assert result["pages"] == 2
    assert result["scanned"] == 2


def test_latest_ingest_max_filings_counts_rows_that_reach_extraction(monkeypatch):
    engine = _engine()
    latest_rows = [
        _filing_row(cik="0000000001", filing_date=date(2026, 6, 1), year=2026, quarter=1, holder="One Capital"),
        _filing_row(cik="0000000002", filing_date=date(2026, 6, 1), year=2026, quarter=1, holder="Two Capital"),
        _filing_row(cik="0000000003", filing_date=date(2026, 6, 1), year=2026, quarter=1, holder="Three Capital"),
    ]
    extract_calls = []

    def fake_extract(*, cik: str, year: int, quarter: int):
        extract_calls.append((cik, year, quarter))
        return []

    monkeypatch.setattr(ingest_module, "ensure_institutional_activity_schema", lambda _engine: None)
    monkeypatch.setattr(ingest_module, "SessionLocal", lambda: _session(engine))
    monkeypatch.setattr(ingest_module, "fetch_latest_institutional_filings", lambda **_kwargs: latest_rows)
    monkeypatch.setattr(ingest_module, "fetch_institutional_filing_extract", fake_extract)

    result = ingest_module.ingest_latest_institutional_filings(pages=1, limit=25, max_filings=2)

    assert extract_calls == [("0000000001", 2026, 1), ("0000000002", 2026, 1)]
    assert result["scanned"] == 2
    assert result["parsed"] == 2
    assert result["empty_extract_retryable"] == 2
    assert result["processed_filings"] == 0


def test_latest_ingest_already_processed_skips_do_not_count_toward_max_filings(monkeypatch):
    engine = _engine()
    processed_rows = [
        {**_filing_row(cik="0000000001", filing_date=date(2026, 6, 1), year=2026, quarter=1, holder="One Capital"), "formType": "13F-NT"},
        {**_filing_row(cik="0000000002", filing_date=date(2026, 6, 1), year=2026, quarter=1, holder="Two Capital"), "formType": "13F-NT"},
    ]
    unprocessed_row = _filing_row(cik="0000000003", filing_date=date(2026, 6, 1), year=2026, quarter=1, holder="Three Capital")
    latest_rows = [*processed_rows, unprocessed_row]

    with _session(engine) as db:
        for row in processed_rows:
            candidate = parse_latest_filing(row)
            assert candidate is not None
            upsert_institutional_holder(db, candidate)
            filing, _ = upsert_institutional_filing(db, candidate)
            filing.processed_at = datetime.now(timezone.utc)
        db.commit()

    extract_calls = []

    def fake_extract(*, cik: str, year: int, quarter: int):
        extract_calls.append((cik, year, quarter))
        return []

    monkeypatch.setattr(ingest_module, "ensure_institutional_activity_schema", lambda _engine: None)
    monkeypatch.setattr(ingest_module, "SessionLocal", lambda: _session(engine))
    monkeypatch.setattr(ingest_module, "fetch_latest_institutional_filings", lambda **_kwargs: latest_rows)
    monkeypatch.setattr(ingest_module, "fetch_institutional_filing_extract", fake_extract)

    result = ingest_module.ingest_latest_institutional_filings(pages=1, limit=25, max_filings=1)

    assert extract_calls == [("0000000003", 2026, 1)]
    assert result["scanned"] == 3
    assert result["already_processed_skipped"] == 2
    assert result["empty_extract_retryable"] == 1


def test_latest_ingest_parse_failures_do_not_count_toward_max_filings(monkeypatch):
    engine = _engine()
    valid_row = _filing_row(cik="0000000003", filing_date=date(2026, 6, 1), year=2026, quarter=1, holder="Three Capital")
    latest_rows = [{"filingDate": "2026-07-01"}, {"filingDate": "2026-07-02"}, valid_row]
    extract_calls = []

    def fake_extract(*, cik: str, year: int, quarter: int):
        extract_calls.append((cik, year, quarter))
        return []

    monkeypatch.setattr(ingest_module, "ensure_institutional_activity_schema", lambda _engine: None)
    monkeypatch.setattr(ingest_module, "SessionLocal", lambda: _session(engine))
    monkeypatch.setattr(ingest_module, "fetch_latest_institutional_filings", lambda **_kwargs: latest_rows)
    monkeypatch.setattr(ingest_module, "fetch_institutional_filing_extract", fake_extract)

    result = ingest_module.ingest_latest_institutional_filings(pages=1, limit=25, max_filings=1)

    assert extract_calls == [("0000000003", 2026, 1)]
    assert result["scanned"] == 3
    assert result["parse_failed"] == 2
    assert result["empty_extract_retryable"] == 1


def test_latest_ingest_stops_window_on_database_error(monkeypatch):
    class DummySession:
        def rollback(self):
            pass

        def close(self):
            pass

    calls = []
    valid_row = _filing_row(cik="0000000003", filing_date=date(2026, 6, 1), year=2026, quarter=1, holder="Three Capital")

    def fake_fetch_latest(*, page: int, limit: int):
        calls.append((page, limit))
        return [valid_row]

    def fail_holder(*_args, **_kwargs):
        raise OperationalError("select 1", {}, Exception("db closed"))

    monkeypatch.setattr(ingest_module, "ensure_institutional_activity_schema", lambda _engine: None)
    monkeypatch.setattr(ingest_module, "SessionLocal", lambda: DummySession())
    monkeypatch.setattr(ingest_module, "fetch_latest_institutional_filings", fake_fetch_latest)
    monkeypatch.setattr(ingest_module, "upsert_institutional_holder", fail_holder)

    result = ingest_module.ingest_latest_institutional_filings(pages=2, limit=25, max_filings=25)

    assert calls == [(0, 25)]
    assert result["errors"] == 1
    assert result["scanned"] == 1
    assert result["pages_scanned"] == 1


def test_start_page_arg_does_not_change_specific_cik_ingest_path(monkeypatch, capsys):
    calls = []

    def fake_specific_ingest(*, cik: str, year: int, quarter: int, force: bool = False):
        calls.append({"cik": cik, "year": year, "quarter": quarter, "force": force})
        return {"status": "ok", "processed_filings": 1}

    monkeypatch.setattr(ingest_module.sys, "argv", ["prog", "--start-page", "3", "--cik", "0001067983", "--year", "2026", "--quarter", "1"])
    monkeypatch.setattr(ingest_module, "ingest_institutional_filing", fake_specific_ingest)
    monkeypatch.setattr(
        ingest_module,
        "ingest_latest_institutional_filings",
        lambda **_kwargs: pytest.fail("specific CIK ingest should not use latest-filings path"),
    )

    ingest_module.main()

    assert calls == [{"cik": "0001067983", "year": 2026, "quarter": 1, "force": False}]
    assert "processed_filings" in capsys.readouterr().out


def test_upsert_filing_preserves_existing_metadata_when_candidate_is_sparse():
    engine = _engine()
    rich_candidate = parse_latest_filing(
        {
            "cik": "0001452208",
            "date": "2026-06-30",
            "filingDate": "2026-07-01 00:00:00",
            "formType": "13F-HR",
            "link": "https://www.sec.gov/Archives/edgar/data/1452208/000110465926079776/0001104659-26-079776-index.htm",
            "name": "CACTI ASSET MANAGEMENT LLC",
        }
    )
    sparse_candidate = parse_latest_filing(
        {
            "cik": "0001452208",
            "date": "2026-03-31",
            "filingDate": "2026-03-31 00:00:00",
            "year": 2026,
            "quarter": 2,
        }
    )
    assert rich_candidate is not None
    assert sparse_candidate is not None
    assert sparse_candidate.form_type is None

    with _session(engine) as db:
        filing, created = upsert_institutional_filing(db, rich_candidate)
        assert created is True
        db.flush()

        same_filing, created_again = upsert_institutional_filing(db, sparse_candidate)
        db.flush()

        assert created_again is False
        assert same_filing.id == filing.id
        assert db.query(InstitutionalFiling).count() == 1
        assert same_filing.form_type == "13F-HR"
        assert same_filing.accession_number == "0001104659-26-079776"
        assert same_filing.filing_url == rich_candidate.filing_url
        assert json.loads(same_filing.raw_metadata_json or "{}").get("formType") == "13F-HR"


def test_sparse_period_metadata_reuses_canonical_amendment_without_creating_third_filing():
    engine = _engine()
    today = date.today()
    cik = "0001009012"
    original_row = _filing_row(cik=cik, filing_date=today - timedelta(days=5), year=2026, quarter=1, holder="Zazove Associates LLC")
    amendment_row = _filing_row(cik=cik, filing_date=today - timedelta(days=1), year=2026, quarter=1, holder="Zazove Associates LLC")
    amendment_row["formType"] = "13F-HR/A"
    amendment_row["accessionNumber"] = f"{cik}-2026-1-A"
    sparse_row = {
        "cik": cik,
        "date": "2026-03-31",
        "filingDate": "2026-03-31",
        "year": 2026,
        "quarter": 1,
    }
    original_candidate = parse_latest_filing(original_row)
    amendment_candidate = parse_latest_filing(amendment_row)
    sparse_candidate = parse_latest_filing(sparse_row)
    assert original_candidate is not None
    assert amendment_candidate is not None
    assert sparse_candidate is not None
    assert sparse_candidate.accession_number is None
    assert sparse_candidate.form_type is None

    with _session(engine) as db:
        original, _ = upsert_institutional_filing(db, original_candidate)
        amendment, _ = upsert_institutional_filing(db, amendment_candidate)
        same, created = upsert_institutional_filing(db, sparse_candidate)
        db.flush()

        assert created is False
        assert same.id == amendment.id
        assert original.superseded_by == amendment.id
        assert amendment.superseded_by is None
        assert db.query(InstitutionalFiling).count() == 2
        assert same.accession_number == amendment_candidate.accession_number
        assert same.form_type == "13F-HR/A"


def test_latest_ingest_metrics_split_already_processed_skips(monkeypatch):
    engine = _engine()
    latest_row = {
        "cik": "0001452208",
        "date": "2026-06-30",
        "filingDate": "2026-07-01 00:00:00",
        "formType": "13F-NT",
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


def test_specific_ingest_chooses_amended_candidate_when_original_is_listed_first(monkeypatch):
    engine = _engine()
    today = date.today()
    cik = "0001009012"
    original_row = _filing_row(cik=cik, filing_date=today - timedelta(days=5), year=2026, quarter=1, holder="Zazove Associates LLC")
    amendment_row = _filing_row(cik=cik, filing_date=today - timedelta(days=1), year=2026, quarter=1, holder="Zazove Associates LLC")
    amendment_row["formType"] = "13F-HR/A"
    amendment_row["accessionNumber"] = f"{cik}-2026-1-A"
    original_candidate = parse_latest_filing(original_row)
    assert original_candidate is not None

    with _session(engine) as db:
        upsert_institutional_holder(db, original_candidate)
        original_filing, _ = upsert_institutional_filing(db, original_candidate)
        db.flush()
        upsert_positions_for_filing(
            db,
            filing=original_filing,
            rows=[{"symbol": "UONE", "shares": 400_000, "marketValue": 60_000_000, "cusip": "91705J105"}],
        )
        db.commit()

    monkeypatch.setattr(ingest_module, "ensure_institutional_activity_schema", lambda _engine: None)
    monkeypatch.setattr(ingest_module, "SessionLocal", lambda: _session(engine))
    monkeypatch.setattr(ingest_module, "fetch_institutional_filing_dates", lambda **_kwargs: [original_row, amendment_row])
    monkeypatch.setattr(
        ingest_module,
        "fetch_institutional_filing_extract",
        lambda **_kwargs: [{"symbol": "UONE", "shares": 900_000, "marketValue": 120_000_000, "cusip": "91705J105"}],
    )

    result = ingest_module.ingest_institutional_filing(cik=cik, year=2026, quarter=1, force=True)

    assert result["processed_filings"] == 1
    assert result["position_rows"] == 1
    assert result["position_changes"] == 1
    with _session(engine) as db:
        original = db.execute(select(InstitutionalFiling).where(InstitutionalFiling.accession_number == original_row["accessionNumber"])).scalar_one()
        amendment = db.execute(select(InstitutionalFiling).where(InstitutionalFiling.accession_number == amendment_row["accessionNumber"])).scalar_one()
        assert original.superseded_by == amendment.id
        assert amendment.superseded_by is None
        change = db.execute(select(InstitutionalPositionChange).where(InstitutionalPositionChange.cik == cik)).scalar_one()
        assert change.curr_value_usd == 120_000_000
        assert change.filing_date == amendment.filing_date


def test_specific_ingest_uses_existing_canonical_amendment_when_provider_lists_original_only(monkeypatch):
    engine = _engine()
    today = date.today()
    cik = "0001009012"
    original_row = _filing_row(cik=cik, filing_date=today - timedelta(days=5), year=2026, quarter=1, holder="Zazove Associates LLC")
    amendment_row = _filing_row(cik=cik, filing_date=today - timedelta(days=1), year=2026, quarter=1, holder="Zazove Associates LLC")
    amendment_row["formType"] = "13F-HR/A"
    amendment_row["accessionNumber"] = f"{cik}-2026-1-A"
    original_candidate = parse_latest_filing(original_row)
    amendment_candidate = parse_latest_filing(amendment_row)
    assert original_candidate is not None
    assert amendment_candidate is not None

    with _session(engine) as db:
        upsert_institutional_holder(db, original_candidate)
        original_filing, _ = upsert_institutional_filing(db, original_candidate)
        db.flush()
        upsert_positions_for_filing(
            db,
            filing=original_filing,
            rows=[{"symbol": "UONE", "shares": 400_000, "marketValue": 60_000_000, "cusip": "91705J105"}],
        )
        upsert_institutional_holder(db, amendment_candidate)
        amendment_filing, _ = upsert_institutional_filing(db, amendment_candidate)
        db.flush()
        upsert_positions_for_filing(
            db,
            filing=amendment_filing,
            rows=[{"symbol": "UONE", "shares": 500_000, "marketValue": 70_000_000, "cusip": "91705J105"}],
        )
        db.commit()

    monkeypatch.setattr(ingest_module, "ensure_institutional_activity_schema", lambda _engine: None)
    monkeypatch.setattr(ingest_module, "SessionLocal", lambda: _session(engine))
    monkeypatch.setattr(ingest_module, "fetch_institutional_filing_dates", lambda **_kwargs: [original_row])
    monkeypatch.setattr(
        ingest_module,
        "fetch_institutional_filing_extract",
        lambda **_kwargs: [{"symbol": "UONE", "shares": 900_000, "marketValue": 120_000_000, "cusip": "91705J105"}],
    )

    result = ingest_module.ingest_institutional_filing(cik=cik, year=2026, quarter=1, force=True)

    assert result["processed_filings"] == 1
    assert result["position_rows"] == 1
    assert result["position_changes"] == 1
    with _session(engine) as db:
        original = db.execute(select(InstitutionalFiling).where(InstitutionalFiling.accession_number == original_row["accessionNumber"])).scalar_one()
        amendment = db.execute(select(InstitutionalFiling).where(InstitutionalFiling.accession_number == amendment_row["accessionNumber"])).scalar_one()
        assert original.superseded_by == amendment.id
        assert amendment.superseded_by is None
        amendment_positions = db.execute(select(InstitutionalPosition).where(InstitutionalPosition.filing_id == amendment.id)).scalars().all()
        assert len(amendment_positions) == 1
        assert amendment_positions[0].value_usd == 120_000_000
        change = db.execute(select(InstitutionalPositionChange).where(InstitutionalPositionChange.cik == cik)).scalar_one()
        assert change.curr_value_usd == 120_000_000
        assert change.filing_date == amendment.filing_date


def test_latest_ingest_keeps_zero_extract_13f_hr_retryable(monkeypatch):
    engine = _engine()
    latest_row = {
        "cik": "0001452208",
        "date": "2026-06-30",
        "filingDate": "2026-07-01 00:00:00",
        "formType": "13F-HR",
        "link": "https://www.sec.gov/Archives/edgar/data/1452208/000110465926079776/0001104659-26-079776-index.htm",
        "name": "CACTI ASSET MANAGEMENT LLC",
    }

    monkeypatch.setattr(ingest_module, "ensure_institutional_activity_schema", lambda _engine: None)
    monkeypatch.setattr(ingest_module, "SessionLocal", lambda: _session(engine))
    monkeypatch.setattr(ingest_module, "fetch_latest_institutional_filings", lambda **_kwargs: [latest_row])
    monkeypatch.setattr(ingest_module, "fetch_institutional_filing_extract", lambda **_kwargs: [])

    result = ingest_module.ingest_latest_institutional_filings(pages=1, limit=5, max_filings=1)

    assert result["processed_filings"] == 0
    assert result["empty_extract_retryable"] == 1
    assert result["empty_extract_processed_no_holdings"] == 0
    assert result["position_rows"] == 0

    with _session(engine) as db:
        filing = db.execute(select(InstitutionalFiling)).scalar_one()
        assert filing.form_type == "13F-HR"
        assert filing.processed_at is None


def test_latest_ingest_processes_zero_extract_13f_nt_as_no_holdings(monkeypatch):
    engine = _engine()
    latest_row = {
        "cik": "0000796370",
        "date": "2026-06-30",
        "filingDate": "2026-07-01 00:00:00",
        "formType": "13F-NT",
        "link": "https://www.sec.gov/Archives/edgar/data/796370/000079637026000004/0000796370-26-000004-index.htm",
        "name": "BRIGHTSPHERE INC.",
    }

    monkeypatch.setattr(ingest_module, "ensure_institutional_activity_schema", lambda _engine: None)
    monkeypatch.setattr(ingest_module, "SessionLocal", lambda: _session(engine))
    monkeypatch.setattr(ingest_module, "fetch_latest_institutional_filings", lambda **_kwargs: [latest_row])
    monkeypatch.setattr(ingest_module, "fetch_institutional_filing_extract", lambda **_kwargs: [])

    result = ingest_module.ingest_latest_institutional_filings(pages=1, limit=5, max_filings=1)

    assert result["processed_filings"] == 1
    assert result["empty_extract_retryable"] == 0
    assert result["empty_extract_processed_no_holdings"] == 1
    assert result["position_rows"] == 0

    with _session(engine) as db:
        filing = db.execute(select(InstitutionalFiling)).scalar_one()
        assert filing.form_type == "13F-NT"
        assert filing.processed_at is not None


def test_latest_ingest_reruns_retryable_13f_hr_without_duplicate_filing(monkeypatch):
    engine = _engine()
    latest_row = {
        "cik": "0001452208",
        "date": "2026-06-30",
        "filingDate": "2026-07-01 00:00:00",
        "formType": "13F-HR",
        "link": "https://www.sec.gov/Archives/edgar/data/1452208/000110465926079776/0001104659-26-079776-index.htm",
        "name": "CACTI ASSET MANAGEMENT LLC",
    }

    monkeypatch.setattr(ingest_module, "ensure_institutional_activity_schema", lambda _engine: None)
    monkeypatch.setattr(ingest_module, "SessionLocal", lambda: _session(engine))
    monkeypatch.setattr(ingest_module, "fetch_latest_institutional_filings", lambda **_kwargs: [latest_row])
    monkeypatch.setattr(ingest_module, "fetch_institutional_filing_extract", lambda **_kwargs: [])

    first = ingest_module.ingest_latest_institutional_filings(pages=1, limit=5, max_filings=1)
    second = ingest_module.ingest_latest_institutional_filings(pages=1, limit=5, max_filings=1)

    assert first["empty_extract_retryable"] == 1
    assert second["empty_extract_retryable"] == 1
    with _session(engine) as db:
        assert db.query(InstitutionalFiling).count() == 1
        filing = db.execute(select(InstitutionalFiling)).scalar_one()
        assert filing.processed_at is None


def test_retryable_13f_hr_sets_processed_when_positions_later_appear(monkeypatch):
    engine = _engine()
    latest_row = {
        "cik": "0001452208",
        "date": "2026-06-30",
        "filingDate": "2026-07-01 00:00:00",
        "formType": "13F-HR",
        "link": "https://www.sec.gov/Archives/edgar/data/1452208/000110465926079776/0001104659-26-079776-index.htm",
        "name": "CACTI ASSET MANAGEMENT LLC",
    }
    extract_batches = [
        [],
        [{"symbol": "LATE", "cusip": "000LATE01", "shares": 10_000, "marketValue": 2_000_000}],
    ]

    monkeypatch.setattr(ingest_module, "ensure_institutional_activity_schema", lambda _engine: None)
    monkeypatch.setattr(ingest_module, "SessionLocal", lambda: _session(engine))
    monkeypatch.setattr(ingest_module, "fetch_latest_institutional_filings", lambda **_kwargs: [latest_row])
    monkeypatch.setattr(ingest_module, "fetch_institutional_filing_extract", lambda **_kwargs: extract_batches.pop(0))

    first = ingest_module.ingest_latest_institutional_filings(pages=1, limit=5, max_filings=1)
    second = ingest_module.ingest_latest_institutional_filings(pages=1, limit=5, max_filings=1)

    assert first["empty_extract_retryable"] == 1
    assert first["processed_filings"] == 0
    assert second["empty_extract_retryable"] == 0
    assert second["processed_filings"] == 1
    assert second["position_rows"] == 1

    with _session(engine) as db:
        assert db.query(InstitutionalFiling).count() == 1
        filing = db.execute(select(InstitutionalFiling)).scalar_one()
        assert filing.processed_at is not None
        position = db.execute(select(InstitutionalPosition).where(InstitutionalPosition.filing_id == filing.id)).scalar_one()
        assert position.normalized_symbol == "LATE"


def _seed_institutional_filing(db: Session, *, cik: str = "0002055065") -> InstitutionalFiling:
    candidate = parse_latest_filing(
        _filing_row(
            cik=cik,
            filing_date=date(2026, 7, 1),
            year=2026,
            quarter=2,
            holder="NOBLE WEALTH MANAGEMENT PBC",
        )
    )
    assert candidate is not None
    upsert_institutional_holder(db, candidate)
    filing, _ = upsert_institutional_filing(db, candidate)
    db.flush()
    return filing


def test_parse_position_reads_put_call_share_from_extract_payload():
    payload = parse_position(
        {
            "symbol": None,
            "securityCusip": "67066G104",
            "nameOfIssuer": "NVIDIA CORPORATION COM",
            "shares": 1000,
            "value": 200_090_000,
            "putCallShare": "PUT",
        }
    )

    assert payload is not None
    assert payload.cusip == "67066G104"
    assert payload.normalized_symbol is None
    assert payload.put_call == "put"


def test_exact_duplicate_extract_rows_create_one_position_with_autoflush_disabled():
    engine = _engine()
    with _session(engine) as db:
        filing = _seed_institutional_filing(db)
        counts = upsert_positions_for_filing(
            db,
            filing=filing,
            rows=[
                {"symbol": None, "securityCusip": "67066G104", "nameOfIssuer": "NVIDIA CORPORATION COM", "shares": 1000, "value": 200_090_000},
                {"symbol": None, "securityCusip": "67066G104", "nameOfIssuer": "NVIDIA CORPORATION COM", "shares": 1000, "value": 200_090_000},
            ],
        )
        db.flush()

        positions = db.execute(select(InstitutionalPosition).where(InstitutionalPosition.filing_id == filing.id)).scalars().all()
        assert counts["inserted_positions"] == 1
        assert len(positions) == 1
        assert positions[0].cusip == "67066G104"
        assert positions[0].put_call is None
        assert positions[0].shares == 1000
        assert positions[0].value_usd == 200_090_000


def test_same_cusip_blank_symbol_and_null_put_call_aggregates_split_rows():
    engine = _engine()
    with _session(engine) as db:
        filing = _seed_institutional_filing(db)
        upsert_positions_for_filing(
            db,
            filing=filing,
            rows=[
                {"symbol": None, "securityCusip": "111111111", "nameOfIssuer": "SPLIT COMMON", "shares": 40, "value": 4000},
                {"symbol": "", "cusip": "111111111", "issuerName": "SPLIT COMMON", "shares": 60, "marketValue": 6000, "putCallShare": ""},
            ],
        )
        db.flush()

        position = db.execute(select(InstitutionalPosition).where(InstitutionalPosition.filing_id == filing.id)).scalar_one()
        assert position.cusip == "111111111"
        assert position.normalized_symbol is None
        assert position.put_call is None
        assert position.shares == 100
        assert position.value_usd == 10_000


def test_same_cusip_common_put_and_call_rows_remain_distinct_positions():
    engine = _engine()
    with _session(engine) as db:
        filing = _seed_institutional_filing(db)
        upsert_positions_for_filing(
            db,
            filing=filing,
            rows=[
                {"symbol": None, "securityCusip": "770700102", "nameOfIssuer": "ROBINHOOD MKTS INC COM CL A", "shares": 58, "value": 5_816_000, "putCallShare": ""},
                {"symbol": None, "securityCusip": "770700102", "nameOfIssuer": "ROBINHOOD MKTS INC COM CL A", "shares": 100, "value": 10_028_000, "putCallShare": "CALL"},
                {"symbol": None, "securityCusip": "770700102", "nameOfIssuer": "ROBINHOOD MKTS INC COM CL A", "shares": 25, "value": 2_500_000, "putCallShare": "PUT"},
            ],
        )
        db.flush()

        positions = db.execute(select(InstitutionalPosition).where(InstitutionalPosition.filing_id == filing.id)).scalars().all()
        by_put_call = {position.put_call or "common": position for position in positions}
        assert sorted(by_put_call) == ["call", "common", "put"]
        assert by_put_call["common"].shares == 58
        assert by_put_call["call"].shares == 100
        assert by_put_call["put"].shares == 25


def test_cusip_present_identity_ignores_missing_or_conflicting_symbol():
    engine = _engine()
    with _session(engine) as db:
        filing = _seed_institutional_filing(db)
        upsert_positions_for_filing(
            db,
            filing=filing,
            rows=[
                {"symbol": "", "securityCusip": "123456789", "nameOfIssuer": "CUSIP FIRST INC", "shares": 10, "value": 1000},
                {"symbol": "WRONG", "securityCusip": "123456789", "nameOfIssuer": "CUSIP FIRST INC", "shares": 15, "value": 1500},
            ],
        )
        db.flush()

        position = db.execute(select(InstitutionalPosition).where(InstitutionalPosition.filing_id == filing.id)).scalar_one()
        assert position.cusip == "123456789"
        assert position.shares == 25
        assert position.value_usd == 2500
        assert position.normalized_symbol == "WRONG"


def test_upsert_positions_for_filing_is_idempotent_for_cusip_put_call_identity():
    engine = _engine()
    rows = [
        {"symbol": None, "securityCusip": "02079K107", "nameOfIssuer": "ALPHABET INC CAP STK CL C", "shares": 365, "value": 128_904_000, "putCallShare": ""},
        {"symbol": None, "securityCusip": "02079K107", "nameOfIssuer": "ALPHABET INC CAP STK CL C", "shares": 1000, "value": 353_330_000, "putCallShare": "PUT"},
    ]
    with _session(engine) as db:
        filing = _seed_institutional_filing(db)
        first = upsert_positions_for_filing(db, filing=filing, rows=rows)
        db.flush()
        second = upsert_positions_for_filing(db, filing=filing, rows=rows)
        db.flush()

        positions = db.execute(select(InstitutionalPosition).where(InstitutionalPosition.filing_id == filing.id)).scalars().all()
        assert first["inserted_positions"] == 2
        assert second["updated_positions"] == 2
        assert len(positions) == 2
        assert {position.put_call or "common" for position in positions} == {"common", "put"}


def test_position_changes_do_not_double_count_aggregated_rows():
    engine = _engine()
    with _session(engine) as db:
        prior_candidate = parse_latest_filing(
            _filing_row(cik="0002055065", filing_date=date(2026, 5, 15), year=2026, quarter=1, holder="NOBLE WEALTH MANAGEMENT PBC")
        )
        current_candidate = parse_latest_filing(
            _filing_row(cik="0002055065", filing_date=date(2026, 7, 1), year=2026, quarter=2, holder="NOBLE WEALTH MANAGEMENT PBC")
        )
        assert prior_candidate is not None
        assert current_candidate is not None
        upsert_institutional_holder(db, prior_candidate)
        prior_filing, _ = upsert_institutional_filing(db, prior_candidate)
        db.flush()
        upsert_positions_for_filing(
            db,
            filing=prior_filing,
            rows=[{"symbol": "NVDA", "cusip": "67066G104", "shares": 100, "marketValue": 10_000}],
        )
        upsert_institutional_holder(db, current_candidate)
        current_filing, _ = upsert_institutional_filing(db, current_candidate)
        db.flush()
        upsert_positions_for_filing(
            db,
            filing=current_filing,
            rows=[
                {"symbol": "NVDA", "cusip": "67066G104", "shares": 150, "marketValue": 15_000},
                {"symbol": "NVDA", "cusip": "67066G104", "shares": 50, "marketValue": 5_000},
            ],
        )

        process_filing_changes_and_events(db, current_filing)
        change = db.execute(select(InstitutionalPositionChange).where(InstitutionalPositionChange.normalized_symbol == "NVDA")).scalar_one()
        summary = db.execute(select(InstitutionalSymbolSummary).where(InstitutionalSymbolSummary.normalized_symbol == "NVDA")).scalar_one()

        assert change.curr_shares == 200
        assert change.curr_value_usd == 20_000
        assert change.shares_delta == 100
        assert summary.total_value_usd == 20_000


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


def test_collapsed_same_key_changes_create_one_activity_event_with_autoflush_disabled():
    engine = _engine()
    today = date.today()
    cik = "0001009012"

    with _session(engine) as db:
        candidate = parse_latest_filing(_filing_row(cik=cik, filing_date=today - timedelta(days=5), year=2026, quarter=1, holder="Zazove Associates LLC"))
        assert candidate is not None
        upsert_institutional_holder(db, candidate)
        filing, _ = upsert_institutional_filing(db, candidate)
        db.flush()
        upsert_positions_for_filing(
            db,
            filing=filing,
            rows=[
                {"symbol": "UONE", "nameOfIssuer": "Urban One Inc.", "shares": 1_000_000, "marketValue": 150_000_000, "cusip": "91705J105"},
                {"symbol": "UONE", "nameOfIssuer": "Urban One Inc.", "shares": 250_000, "marketValue": 25_000_000, "cusip": "91705J204"},
            ],
        )

        counts = process_filing_changes_and_events(db, filing)
        db.commit()

        assert counts["changes"] == 2
        holder_events = db.execute(
            select(InstitutionalActivityEvent).where(
                InstitutionalActivityEvent.normalized_symbol == "UONE",
                InstitutionalActivityEvent.cik == cik,
                InstitutionalActivityEvent.event_type == "new_institutional_position",
                InstitutionalActivityEvent.report_year == 2026,
                InstitutionalActivityEvent.report_quarter == 1,
            )
        ).scalars().all()
        assert len(holder_events) == 1
        assert holder_events[0].reported_value_usd == 150_000_000
        assert db.query(Event).count() == 0


def test_strict_feed_materialization_keeps_institutional_mode_broader_than_all():
    engine = _engine()
    today = date.today()

    with _session(engine) as db:
        summary = InstitutionalSymbolSummary(
            symbol="WIDE",
            normalized_symbol="WIDE",
            report_year=2026,
            report_quarter=1,
            latest_filing_date=today,
            materiality_score=100,
            net_value_delta_usd=200_000_000,
        )
        ordinary = InstitutionalActivityEvent(
            symbol="WIDE",
            normalized_symbol="WIDE",
            cik="0000000001",
            holder_name="Ordinary Capital",
            event_type="new_institutional_position",
            direction="bullish",
            title="Ordinary Capital reports new WIDE position",
            summary="Reported 13F filing activity.",
            filing_date=today,
            report_year=2026,
            report_quarter=1,
            reported_value_usd=25_000_000,
            value_delta_usd=25_000_000,
            holder_breadth=1,
            materiality_score=100,
            feed_visible=True,
        )
        high_signal = InstitutionalActivityEvent(
            symbol="WIDE",
            normalized_symbol="WIDE",
            cik="0000000002",
            holder_name="High Signal Capital",
            event_type="new_institutional_position",
            direction="bullish",
            title="High Signal Capital reports new WIDE position",
            summary="Reported 13F filing activity.",
            filing_date=today,
            report_year=2026,
            report_quarter=1,
            reported_value_usd=150_000_000,
            value_delta_usd=150_000_000,
            holder_breadth=1,
            materiality_score=95,
            feed_visible=True,
        )
        db.add_all([summary, ordinary, high_signal])
        db.flush()

        created = materialize_feed_events_for_symbol(db, summary)
        db.commit()

        assert created == 1
        feed_event = db.execute(select(Event)).scalar_one()
        assert feed_event.source_provider == INSTITUTIONAL_EVENT_SOURCE
        assert feed_event.source_filing_id == f"institutional:{high_signal.id}:new_institutional_position:2026q1"
        assert "Buy" not in feed_event.payload_json
        assert "Sell" not in feed_event.payload_json


def test_cleanup_overbroad_institutional_feed_events_keeps_other_sources():
    engine = _engine()
    today = date.today()

    with _session(engine) as db:
        high_signal = InstitutionalActivityEvent(
            symbol="KEEP",
            normalized_symbol="KEEP",
            cik="0000000002",
            holder_name="High Signal Capital",
            event_type="new_institutional_position",
            direction="bullish",
            title="High Signal Capital reports new KEEP position",
            summary="Reported 13F filing activity.",
            filing_date=today,
            report_year=2026,
            report_quarter=1,
            reported_value_usd=150_000_000,
            value_delta_usd=150_000_000,
            holder_breadth=1,
            materiality_score=95,
            feed_visible=True,
        )
        db.add(high_signal)
        db.flush()
        db.add_all(
            [
                Event(
                    event_type="new_institutional_position",
                    ts=datetime.now(timezone.utc),
                    event_date=datetime.now(timezone.utc),
                    symbol="DROP",
                    source="13F filing",
                    source_provider=INSTITUTIONAL_EVENT_SOURCE,
                    source_filing_id="institutional:999999:new_institutional_position:2026q1",
                    payload_json=json.dumps({"source_label": "Institutional Activity"}),
                ),
                Event(
                    event_type="new_institutional_position",
                    ts=datetime.now(timezone.utc),
                    event_date=datetime.now(timezone.utc),
                    symbol="KEEP",
                    source="13F filing",
                    source_provider=INSTITUTIONAL_EVENT_SOURCE,
                    source_filing_id=f"institutional:{high_signal.id}:new_institutional_position:2026q1",
                    amount_min=150_000_000,
                    amount_max=150_000_000,
                    impact_score=95,
                    payload_json=json.dumps({"source_label": "Institutional Activity"}),
                ),
                Event(
                    event_type="government_contract",
                    ts=datetime.now(timezone.utc),
                    event_date=datetime.now(timezone.utc),
                    symbol="LMT",
                    source="usaspending",
                    payload_json=json.dumps({"event_subtype": "funding_action"}),
                ),
                Event(
                    event_type="smart_money_confirmation",
                    ts=datetime.now(timezone.utc),
                    event_date=datetime.now(timezone.utc),
                    symbol="OTHER",
                    source="test",
                    source_provider="other",
                    payload_json=json.dumps({"source_label": "Other"}),
                ),
            ]
        )
        db.commit()

        result = cleanup_overbroad_institutional_feed_events(db, dry_run=False)
        db.commit()

        assert result["before"] == 2
        assert result["removed"] == 1
        assert result["remaining"] == 1
        assert db.query(Event).filter(Event.symbol == "DROP").count() == 0
        assert db.query(Event).filter(Event.symbol == "KEEP").count() == 1
        assert db.query(Event).filter(Event.event_type == "government_contract").count() == 1
        assert db.query(Event).filter(Event.symbol == "OTHER").count() == 1


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


def test_ticker_ownership_endpoint_is_pro_only_and_returns_breakdown(monkeypatch):
    monkeypatch.setenv("CT_DEFAULT_TIER", "free")
    monkeypatch.setenv("CT_ALLOW_ENTITLEMENT_HEADER", "1")
    monkeypatch.setattr(institutional_service, "fetch_shares_float", lambda **_kwargs: [])
    engine = _engine()

    with _session(engine) as db:
        _process_single_change(
            db,
            symbol="OWN",
            prior_row={"shares": 100_000, "marketValue": 5_000_000, "ownershipPct": 1.25, "cusip": "000OWN001"},
            current_row={"shares": 400_000, "marketValue": 65_000_000, "ownershipPct": 4.5, "cusip": "000OWN001"},
        )
        db.commit()

        for tier in (None, "free", "premium"):
            payload = ticker_ownership("OWN", _request(tier), history_limit=8, holder_limit=10, db=db)
            assert payload["locked"] is True
            assert payload["status"] == "pro_locked"
            assert payload["holders"] == []
            assert payload["history"] == []

        payload = ticker_ownership("OWN", _request("pro"), history_limit=8, holder_limit=10, db=db)
        assert payload["locked"] is False
        assert payload["latest"]["institutional_ownership_pct"] == 4.5
        assert payload["latest"]["retail_ownership_pct"] == 95.5
        assert payload["holders"][0]["holder_name"] == "Blue Ridge Capital"
        assert payload["holders"][0]["ownership_pct"] == 4.5
        assert payload["history"][-1]["period"] == "Q1 2026"
        assert payload["history"][-1]["institutional_ownership_pct"] == 4.5


def test_ticker_ownership_computes_split_from_institutional_shares_and_float(monkeypatch):
    monkeypatch.setenv("CT_DEFAULT_TIER", "free")
    monkeypatch.setenv("CT_ALLOW_ENTITLEMENT_HEADER", "1")
    engine = _engine()

    def fake_shares_float(*, symbol: str):
        assert symbol == "TSM"
        return [{"symbol": "TSM", "floatShares": 2_000_000, "outstandingShares": 2_500_000}]

    monkeypatch.setattr(institutional_service, "fetch_shares_float", fake_shares_float)
    monkeypatch.setattr(institutional_service, "fetch_symbol_positions_summary", lambda **_kwargs: [])

    with _session(engine) as db:
        _process_single_change(
            db,
            symbol="TSM",
            prior_row={"shares": 100_000, "marketValue": 5_000_000, "ownershipPct": 0, "cusip": "000TSM001"},
            current_row={"shares": 400_000, "marketValue": 65_000_000, "ownershipPct": 0, "cusip": "000TSM001"},
        )
        db.commit()

        summary = db.execute(select(InstitutionalSymbolSummary).where(InstitutionalSymbolSummary.normalized_symbol == "TSM")).scalar_one()
        assert summary.institutional_ownership_pct == 0

        payload = ticker_ownership("TSM", _request("pro"), history_limit=8, holder_limit=10, db=db)
        assert payload["latest"]["institutional_ownership_pct"] == 20.0
        assert payload["latest"]["retail_ownership_pct"] == 80.0
        assert payload["latest"]["total_institutional_shares"] == 400_000
        assert payload["latest"]["float_shares"] == 2_000_000
        assert payload["latest"]["float_shares_source"] == "floatShares"
        assert payload["latest"]["ownership_source"] == "institutional_shares_over_float"
        assert payload["holders"][0]["ownership_pct"] == 20.0
        assert payload["holders"][0]["ownership_pct_source"] == "shares_over_float"
        assert payload["history"][-1]["institutional_ownership_pct"] == 20.0


def test_ticker_ownership_returns_reported_holdings_when_percent_is_pending(monkeypatch):
    monkeypatch.setenv("CT_DEFAULT_TIER", "free")
    monkeypatch.setenv("CT_ALLOW_ENTITLEMENT_HEADER", "1")
    monkeypatch.setattr(institutional_service, "fetch_shares_float", lambda **_kwargs: [])
    monkeypatch.setattr(institutional_service, "fetch_symbol_positions_summary", lambda **_kwargs: [])
    engine = _engine()

    with _session(engine) as db:
        _process_single_change(
            db,
            symbol="TSM",
            prior_row={"shares": 100_000, "marketValue": 5_000_000, "ownershipPct": 0, "cusip": "000TSM001"},
            current_row={"shares": 400_000, "marketValue": 65_000_000, "ownershipPct": 0, "cusip": "000TSM001"},
        )
        db.commit()

        payload = ticker_ownership("TSM", _request("pro"), history_limit=8, holder_limit=10, db=db)
        assert payload["status"] == "ok"
        assert payload["message"] == "Reported institutional holdings are available; float share data is not available for this ticker."
        assert payload["latest"]["institutional_ownership_pct"] is None
        assert payload["latest"]["total_value_usd"] == 65_000_000
        assert payload["holders"][0]["holder_name"] == "Blue Ridge Capital"
        assert payload["holders"][0]["value_usd"] == 65_000_000
        assert payload["holders"][0]["shares"] == 400_000


def test_institution_profile_endpoints_are_locked_until_pro(monkeypatch):
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
            profile = institution_profile("0001234567", _request(tier), db=db)
            assert profile["locked"] is True
            assert profile["availability_status"] == "pro_locked"
            assert profile["holder_name"] is None
            assert profile["total_reported_value_usd"] is None
            assert profile["holdings_count"] is None
            assert not profile.get("top_holdings")
            assert institution_holdings("0001234567", _request(tier), year=None, quarter=None, page=0, limit=5, db=db)["items"] == []
            assert institution_activity("0001234567", _request(tier), page=0, limit=5, db=db)["items"] == []
            assert institution_filings("0001234567", _request(tier), page=0, limit=5, db=db)["items"] == []

        for tier in ("pro", "admin"):
            profile = institution_profile("0001234567", _request(tier), db=db)
            assert profile["locked"] is False
            assert profile["holder_name"] == "Blue Ridge Capital"
            assert profile["total_reported_value_usd"] == 65_000_000
            assert profile["holdings_count"] == 1
            assert profile["top_holdings"][0]["value_usd"] == 65_000_000
            holdings = institution_holdings("0001234567", _request(tier), year=None, quarter=None, page=0, limit=5, db=db)
            assert holdings["items"][0]["symbol"] == "LOCK"
            assert holdings["items"][0]["value_usd"] == 65_000_000
            activity = institution_activity("0001234567", _request(tier), page=0, limit=5, db=db)
            assert activity["items"][0]["action"] == "Reported Increase"
            assert "buy" not in activity["items"][0]["action"].lower()
            assert "sell" not in activity["items"][0]["action"].lower()
            filings = institution_filings("0001234567", _request(tier), page=0, limit=5, db=db)
            assert filings["items"][0]["holdings_count"] == 1
            assert filings["items"][0]["status"] == "processed"


def test_institution_profile_uses_cik_metadata_name_fallback(monkeypatch):
    monkeypatch.setenv("CT_DEFAULT_TIER", "free")
    monkeypatch.setenv("CT_ALLOW_ENTITLEMENT_HEADER", "1")
    engine = _engine()

    with _session(engine) as db:
        db.add_all(
            [
                InstitutionalHolder(cik="0001067983", holder_name=None, latest_report_year=2026, latest_report_quarter=1, latest_filing_date=date(2026, 3, 31)),
                CikMeta(cik="0001067983", company_name="Berkshire Hathaway Inc."),
                InstitutionalFiling(
                    id=1,
                    cik="0001067983",
                    accession_number="0001067983-26-000001",
                    filing_date=date(2026, 3, 31),
                    report_year=2026,
                    report_quarter=1,
                    form_type="13F-HR",
                    is_amendment=False,
                ),
                InstitutionalPosition(
                    filing_id=1,
                    cik="0001067983",
                    symbol="AAPL",
                    normalized_symbol="AAPL",
                    issuer_name="Apple Inc.",
                    shares=100,
                    value_usd=10_000_000,
                    report_year=2026,
                    report_quarter=1,
                    filing_date=date(2026, 3, 31),
                ),
            ]
        )
        db.commit()

        profile = institution_profile("1067983", _request("pro"), db=db)

        assert profile["cik"] == "0001067983"
        assert profile["holder_name"] == "Berkshire Hathaway Inc."
        assert profile["locked"] is False


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


def test_amended_filing_supersedes_original_and_replaces_user_facing_activity():
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
        original_activity = db.execute(
            select(InstitutionalActivityEvent).where(
                InstitutionalActivityEvent.cik == cik,
                InstitutionalActivityEvent.normalized_symbol == "NVDA",
                InstitutionalActivityEvent.report_year == 2026,
                InstitutionalActivityEvent.report_quarter == 1,
            )
        ).scalar_one()
        assert original_activity.reported_value_usd == 65_000_000

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

        assert counts["changes"] == 1
        assert counts["activity_events"] >= 1
        db.refresh(current_filing)
        db.refresh(amended_filing)
        assert current_filing.superseded_by == amended_filing.id
        assert amended_filing.superseded_by is None
        assert is_canonical_institutional_filing(db, amended_filing) is True
        assert is_canonical_institutional_filing(db, current_filing) is False

        change = db.execute(
            select(InstitutionalPositionChange).where(
                InstitutionalPositionChange.cik == cik,
                InstitutionalPositionChange.normalized_symbol == "NVDA",
                InstitutionalPositionChange.report_year == 2026,
                InstitutionalPositionChange.report_quarter == 1,
            )
        ).scalar_one()
        assert change.curr_value_usd == 120_000_000
        assert change.filing_date == amendment_candidate.filing_date

        activity = db.execute(
            select(InstitutionalActivityEvent).where(
                InstitutionalActivityEvent.cik == cik,
                InstitutionalActivityEvent.normalized_symbol == "NVDA",
                InstitutionalActivityEvent.report_year == 2026,
                InstitutionalActivityEvent.report_quarter == 1,
            )
        ).scalar_one()
        assert activity.reported_value_usd == 120_000_000
        assert activity.filing_date == amendment_candidate.filing_date

        holdings = positions_for_holder(db, cik, year=2026, quarter=1, page=0, limit=10)
        assert [item["value_usd"] for item in holdings["items"]] == [120_000_000]
        profile = holder_profile(db, cik)
        assert profile is not None
        assert profile["total_reported_value_usd"] == 120_000_000
        filings = filings_for_holder(db, cik)
        q1_filings = [item for item in filings["items"] if item["report_year"] == 2026 and item["report_quarter"] == 1]
        assert len(q1_filings) == 2
        assert [item["canonical"] for item in q1_filings] == [True, False]
        assert q1_filings[1]["superseded_by"] == amended_filing.id
        assert institutional_filing_duplicate_report(db) == {
            "accession_duplicates": 0,
            "total_period_duplicates": 1,
            "active_period_duplicates": 0,
        }


def test_multiple_amendments_choose_latest_amendment_as_canonical():
    engine = _engine()
    today = date.today()
    cik = "0002222222"

    with _session(engine) as db:
        rows = []
        original = _filing_row(cik=cik, filing_date=today - timedelta(days=10), year=2026, quarter=1, holder="Zazove Associates LLC")
        rows.append(original)
        first_amendment = _filing_row(cik=cik, filing_date=today - timedelta(days=5), year=2026, quarter=1, holder="Zazove Associates LLC")
        first_amendment["formType"] = "13F-HR/A"
        first_amendment["accessionNumber"] = f"{cik}-2026-1-A1"
        rows.append(first_amendment)
        second_amendment = _filing_row(cik=cik, filing_date=today - timedelta(days=1), year=2026, quarter=1, holder="Zazove Associates LLC")
        second_amendment["formType"] = "13F-HR/A"
        second_amendment["accessionNumber"] = f"{cik}-2026-1-A2"
        rows.append(second_amendment)

        filings = []
        for row in rows:
            candidate = parse_latest_filing(row)
            assert candidate is not None
            upsert_institutional_holder(db, candidate)
            filing, _ = upsert_institutional_filing(db, candidate)
            filings.append(filing)
        db.commit()

        canonical = get_canonical_filing_for_holder_period(db, cik, 2026, 1)
        assert canonical is not None
        assert canonical.accession_number == f"{cik}-2026-1-A2"
        for filing in filings:
            db.refresh(filing)
        assert filings[0].superseded_by == canonical.id
        assert filings[1].superseded_by == canonical.id
        assert filings[2].superseded_by is None
        assert institutional_filing_duplicate_report(db)["active_period_duplicates"] == 0


def test_superseded_original_does_not_generate_activity_or_feed_events():
    engine = _engine()
    today = date.today()
    cik = "0003333333"

    with _session(engine) as db:
        original_row = _filing_row(cik=cik, filing_date=today - timedelta(days=5), year=2026, quarter=1)
        amendment_row = _filing_row(cik=cik, filing_date=today - timedelta(days=1), year=2026, quarter=1)
        amendment_row["formType"] = "13F-HR/A"
        amendment_row["accessionNumber"] = f"{cik}-2026-1-A"
        original_candidate = parse_latest_filing(original_row)
        amendment_candidate = parse_latest_filing(amendment_row)
        assert original_candidate is not None
        assert amendment_candidate is not None

        upsert_institutional_holder(db, amendment_candidate)
        amended_filing, _ = upsert_institutional_filing(db, amendment_candidate)
        db.flush()
        upsert_positions_for_filing(
            db,
            filing=amended_filing,
            rows=[{"symbol": "MSFT", "shares": 900_000, "marketValue": 120_000_000, "cusip": "594918104"}],
        )

        upsert_institutional_holder(db, original_candidate)
        original_filing, _ = upsert_institutional_filing(db, original_candidate)
        db.flush()
        upsert_positions_for_filing(
            db,
            filing=original_filing,
            rows=[{"symbol": "MSFT", "shares": 400_000, "marketValue": 60_000_000, "cusip": "594918104"}],
        )
        counts = process_filing_changes_and_events(db, original_filing)
        db.commit()

        assert counts["superseded_suppressed"] == 1
        assert counts["activity_events"] == 0
        assert counts["feed_events"] == 0
        assert db.query(InstitutionalPositionChange).count() == 0
        assert db.query(InstitutionalActivityEvent).count() == 0
        assert db.query(Event).count() == 0
