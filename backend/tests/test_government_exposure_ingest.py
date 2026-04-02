from __future__ import annotations

import json
from datetime import date

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.db import Base
from app.clients.usaspending import USAspendingClientError
from app.ingest_government_exposure import ingest_usaspending_government_exposure
from app.models import Security, TickerGovernmentExposure
from app.services.government_exposure import get_ticker_government_exposure


def _session() -> Session:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine)
    maker = sessionmaker(bind=engine)
    return maker()


def _empty_detail_fetcher(*, start_date: date, end_date: date, recipient_name: str, page: int, limit: int):
    return {"results": [], "has_next": False}


def test_ingest_populates_ticker_government_exposure_from_mapped_recipients() -> None:
    db = _session()
    try:
        db.add(Security(symbol="LMT", name="Lockheed Martin Corporation", asset_class="Equity", sector="Industrials"))
        db.commit()

        def fetcher(*, start_date: date, end_date: date, page: int, limit: int):
            rows = [
                {"recipient_name": "Lockheed Martin Corporation", "amount": 6_400_000_000, "award_count": 25},
            ]
            return {"results": rows, "has_next": False}

        result = ingest_usaspending_government_exposure(
            db=db,
            lookback_days=365,
            recent_days=90,
            max_pages=1,
            per_page=50,
            fetcher=fetcher,
            detail_fetcher=_empty_detail_fetcher,
            as_of=date(2026, 3, 31),
        )

        assert result["symbols_mapped"] == 1

        row = db.get(TickerGovernmentExposure, "LMT")
        assert row is not None
        assert row.has_government_exposure is True
        assert row.contract_exposure_level == "high"
        assert row.summary_label.startswith("Government contract exposure present")
    finally:
        db.close()


def test_service_returns_safe_default_when_no_mapping_exists() -> None:
    db = _session()
    try:
        summary = get_ticker_government_exposure(db, "NVDA")
        assert summary.has_government_exposure is False
        assert summary.contract_exposure_level is None
        assert summary.summary_label == "No known contract exposure in current data"
    finally:
        db.close()


def test_recent_award_activity_flag_only_when_recent_window_has_awards() -> None:
    db = _session()
    try:
        db.add(Security(symbol="BA", name="Boeing Company", asset_class="Equity", sector="Industrials"))
        db.commit()

        def fetcher(*, start_date: date, end_date: date, page: int, limit: int):
            if start_date >= date(2025, 12, 31):
                return {"results": [], "has_next": False}
            return {
                "results": [{"recipient_name": "Boeing Company", "amount": 800_000_000, "award_count": 5}],
                "has_next": False,
            }

        ingest_usaspending_government_exposure(
            db=db,
            lookback_days=365,
            recent_days=90,
            max_pages=1,
            per_page=20,
            fetcher=fetcher,
            detail_fetcher=_empty_detail_fetcher,
            as_of=date(2026, 3, 31),
        )

        row = db.get(TickerGovernmentExposure, "BA")
        assert row is not None
        assert row.recent_award_activity is False
    finally:
        db.close()


def test_exposure_level_normalization_hides_unsafe_level_values() -> None:
    db = _session()
    try:
        db.add(
            TickerGovernmentExposure(
                symbol="ABC",
                has_government_exposure=True,
                contract_exposure_level="very_high",
                recent_award_activity=True,
                summary_label="Government contract exposure present",
                source_context="test",
            )
        )
        db.commit()

        summary = get_ticker_government_exposure(db, "ABC")
        assert summary.contract_exposure_level is None
    finally:
        db.close()


def test_recent_award_activity_implies_has_exposure_and_consistent_summary() -> None:
    db = _session()
    try:
        db.add(Security(symbol="PLTR", name="Palantir Technologies Inc", asset_class="Equity", sector="Technology"))
        db.commit()

        def fetcher(*, start_date: date, end_date: date, page: int, limit: int):
            # Simulate pagination/window skew where lookback pages miss a row that
            # is still present in the recent window.
            if start_date == date(2025, 1, 1):
                return {"results": [], "has_next": False}
            if start_date == date(2025, 12, 31):
                return {
                    "results": [{"recipient_name": "Palantir Technologies Inc", "amount": 10_000_000, "award_count": 2}],
                    "has_next": False,
                }
            return {"results": [], "has_next": False}

        ingest_usaspending_government_exposure(
            db=db,
            lookback_days=455,
            recent_days=90,
            max_pages=1,
            per_page=20,
            fetcher=fetcher,
            detail_fetcher=_empty_detail_fetcher,
            as_of=date(2026, 3, 31),
        )

        row = db.get(TickerGovernmentExposure, "PLTR")
        assert row is not None
        assert row.recent_award_activity is True
        assert row.has_government_exposure is True
        assert row.summary_label == "Government contract exposure present · Recent award activity detected"
        details = json.loads(row.source_details_json or "{}")
        assert details["totals"]["obligated_amount"] == 0.0
        assert details["totals"]["award_count"] == 0
        assert details["recent_window"]["obligated_amount"] == 10_000_000.0
        assert details["recent_window"]["award_count"] == 2

        summary = get_ticker_government_exposure(db, "PLTR")
        assert summary.has_government_exposure is True
        assert summary.recent_award_activity is True
    finally:
        db.close()


def test_ingest_persists_latest_notable_award_snapshot_from_award_details_path() -> None:
    db = _session()
    try:
        db.add(Security(symbol="PLTR", name="Palantir Technologies Inc", asset_class="Equity", sector="Technology"))
        db.commit()

        def aggregate_fetcher(*, start_date: date, end_date: date, page: int, limit: int):
            return {
                "results": [
                    {
                        "recipient_name": "Palantir Technologies Inc",
                        "amount": 350_000_000,
                        "award_count": 9,
                    },
                ],
                "has_next": False,
            }

        def detail_fetcher(*, start_date: date, end_date: date, recipient_name: str, page: int, limit: int):
            return {
                "results": [
                    {
                        "recipient_name": recipient_name,
                        "award_date": "2026-03-28",
                        "award_amount": 750_000,
                        "awarding_department": "Department of Homeland Security",
                        "awarding_agency": "CBP",
                        "award_description": "Sub-$1M row should not win",
                        "award_id": "AWD-LOW",
                    },
                    {
                        "recipient_name": recipient_name,
                        "award_date": "2026-03-20",
                        "award_amount": 50_000_000,
                        "awarding_department": "Department of Defense",
                        "awarding_agency": "U.S. Air Force",
                        "award_description": "AI and mission planning software integration support for operational units.",
                        "award_id": "AWD-HIGH",
                        "contract_id": "PIID-123",
                    },
                ],
                "has_next": False,
            }

        ingest_usaspending_government_exposure(
            db=db,
            lookback_days=365,
            recent_days=90,
            max_pages=1,
            per_page=20,
            fetcher=aggregate_fetcher,
            detail_fetcher=detail_fetcher,
            as_of=date(2026, 3, 31),
        )

        summary = get_ticker_government_exposure(db, "PLTR")
        assert summary.latest_notable_award is not None
        assert summary.latest_notable_award["awarding_department"] == "Department of Defense"
        assert summary.latest_notable_award["award_amount"] == 50_000_000.0
        assert summary.latest_notable_award["award_date"] == "2026-03-20"
        assert summary.latest_notable_award["award_description"] is not None
        assert summary.latest_notable_award["contract_id"] == "PIID-123"
        assert summary.latest_notable_award["is_notable"] is False
    finally:
        db.close()


def test_ingest_keeps_aggregate_exposure_even_without_qualifying_award_snapshot() -> None:
    db = _session()
    try:
        db.add(Security(symbol="LMT", name="Lockheed Martin Corporation", asset_class="Equity", sector="Industrials"))
        db.commit()

        def aggregate_fetcher(*, start_date: date, end_date: date, page: int, limit: int):
            return {
                "results": [{"recipient_name": "Lockheed Martin Corporation", "amount": 7_000_000_000, "award_count": 30}],
                "has_next": False,
            }

        def detail_fetcher(*, start_date: date, end_date: date, recipient_name: str, page: int, limit: int):
            return {
                "results": [{"recipient_name": recipient_name, "award_date": "2026-03-01", "award_amount": 250_000}],
                "has_next": False,
            }

        ingest_usaspending_government_exposure(
            db=db,
            lookback_days=365,
            recent_days=90,
            max_pages=1,
            per_page=20,
            fetcher=aggregate_fetcher,
            detail_fetcher=detail_fetcher,
            as_of=date(2026, 3, 31),
        )

        summary = get_ticker_government_exposure(db, "LMT")
        assert summary.has_government_exposure is True
        assert summary.contract_exposure_level == "high"
        assert summary.latest_notable_award is None
    finally:
        db.close()


def test_ingest_prefers_rows_with_valid_award_dates_for_notable_snapshot() -> None:
    db = _session()
    try:
        db.add(Security(symbol="PLTR", name="Palantir Technologies Inc", asset_class="Equity", sector="Technology"))
        db.commit()

        def aggregate_fetcher(*, start_date: date, end_date: date, page: int, limit: int):
            return {
                "results": [{"recipient_name": "Palantir Technologies Inc", "amount": 400_000_000, "award_count": 12}],
                "has_next": False,
            }

        def detail_fetcher(*, start_date: date, end_date: date, recipient_name: str, page: int, limit: int):
            return {
                "results": [
                    {
                        "recipient_name": recipient_name,
                        "award_date": "not-a-real-date",
                        "award_amount": 200_000_000,
                        "awarding_department": "Department of Defense",
                        "awarding_agency": "U.S. Space Force",
                        "award_description": "Invalid date should not outrank valid dates.",
                        "award_id": "AWD-INVALID-DATE",
                    },
                    {
                        "recipient_name": recipient_name,
                        "award_date": "2026-03-25",
                        "award_amount": 60_000_000,
                        "awarding_department": "Department of Defense",
                        "awarding_agency": "U.S. Air Force",
                        "award_description": "Latest valid date should win among >= $1M rows.",
                        "award_id": "AWD-VALID-DATE",
                    },
                ],
                "has_next": False,
            }

        ingest_usaspending_government_exposure(
            db=db,
            lookback_days=365,
            recent_days=90,
            max_pages=1,
            per_page=20,
            fetcher=aggregate_fetcher,
            detail_fetcher=detail_fetcher,
            as_of=date(2026, 3, 31),
        )

        summary = get_ticker_government_exposure(db, "PLTR")
        assert summary.latest_notable_award is not None
        assert summary.latest_notable_award["award_id"] == "AWD-VALID-DATE"
        assert summary.latest_notable_award["award_date"] == "2026-03-25"
    finally:
        db.close()


def test_ingest_continues_when_detail_fetch_fails_for_one_recipient() -> None:
    db = _session()
    try:
        db.add(Security(symbol="PLTR", name="Palantir Technologies Inc", asset_class="Equity", sector="Technology"))
        db.add(Security(symbol="LMT", name="Lockheed Martin Corporation", asset_class="Equity", sector="Industrials"))
        db.commit()

        def aggregate_fetcher(*, start_date: date, end_date: date, page: int, limit: int):
            return {
                "results": [
                    {"recipient_name": "Palantir Technologies Inc", "amount": 350_000_000, "award_count": 9},
                    {"recipient_name": "Lockheed Martin Corporation", "amount": 7_000_000_000, "award_count": 30},
                ],
                "has_next": False,
            }

        def detail_fetcher(*, start_date: date, end_date: date, recipient_name: str, page: int, limit: int):
            if recipient_name == "Palantir Technologies Inc":
                raise USAspendingClientError("USAspending request failed: transient disconnect")
            return {
                "results": [
                    {
                        "recipient_name": recipient_name,
                        "award_date": "2026-03-20",
                        "award_amount": 75_000_000,
                        "award_id": "AWD-LMT-1",
                    }
                ],
                "has_next": False,
            }

        result = ingest_usaspending_government_exposure(
            db=db,
            lookback_days=365,
            recent_days=90,
            max_pages=1,
            per_page=20,
            fetcher=aggregate_fetcher,
            detail_fetcher=detail_fetcher,
            detail_request_pause_s=0.0,
            as_of=date(2026, 3, 31),
        )

        assert result["symbols_mapped"] == 2
        assert result["detail_failures"] == 1
        assert result["detail_symbols_skipped"] == 1

        pltr = get_ticker_government_exposure(db, "PLTR")
        lmt = get_ticker_government_exposure(db, "LMT")
        assert pltr.has_government_exposure is True
        assert pltr.latest_notable_award is None
        assert lmt.latest_notable_award is not None
        assert lmt.latest_notable_award["award_id"] == "AWD-LMT-1"
    finally:
        db.close()
