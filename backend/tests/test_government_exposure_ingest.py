from __future__ import annotations

import json
from datetime import date

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.db import Base
from app.ingest_government_exposure import ingest_usaspending_government_exposure
from app.models import Security, TickerGovernmentExposure
from app.services.government_exposure import get_ticker_government_exposure


def _session() -> Session:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine)
    maker = sessionmaker(bind=engine)
    return maker()


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
