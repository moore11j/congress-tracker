from __future__ import annotations

from datetime import date

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from starlette.requests import Request

from app.db import Base
from app.main import member_reported_holdings
from app.models import HouseAnnualDisclosureDocument, HouseAnnualDisclosureHolding, Member


def _session():
    engine = create_engine("sqlite:///:memory:", future=True)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    Base.metadata.create_all(bind=engine)
    return SessionLocal()


def _request() -> Request:
    return Request(
        {
            "type": "http",
            "method": "GET",
            "path": "/api/members/M000001/reported-holdings",
            "headers": [
                (b"user-agent", b"Mozilla/5.0"),
                (b"x-walnut-request-source", b"client"),
            ],
        }
    )


def test_member_reported_holdings_uses_latest_annual_document_only():
    db = _session()
    try:
        db.add(
            Member(
                bioguide_id="M000001",
                first_name="Test",
                last_name="Member",
                chamber="house",
                party="D",
                state="CA",
            )
        )
        older = HouseAnnualDisclosureDocument(
            member_name="Test Member",
            member_bioguide_id="M000001",
            filing_year=2024,
            document_id="old-document",
            filing_date=date(2025, 5, 1),
        )
        older_amendment = HouseAnnualDisclosureDocument(
            member_name="Test Member",
            member_bioguide_id="M000001",
            filing_year=2024,
            filing_type="A",
            document_id="old-amendment",
            filing_date=date(2026, 5, 15),
        )
        latest = HouseAnnualDisclosureDocument(
            member_name="Test Member",
            member_bioguide_id="M000001",
            filing_year=2025,
            document_id="latest-document",
            filing_date=date(2026, 5, 1),
            report_url="https://example.test/latest.pdf",
        )
        db.add_all([older, older_amendment, latest])
        db.flush()
        db.add_all(
            [
                HouseAnnualDisclosureHolding(
                    document_row_id=older.id,
                    member_name="Test Member",
                    member_bioguide_id="M000001",
                    filing_year=2024,
                    document_id="old-document",
                    asset_name="Old Holding",
                    symbol="OLD",
                    value_min=10_000,
                    value_max=15_000,
                ),
                HouseAnnualDisclosureHolding(
                    document_row_id=older_amendment.id,
                    member_name="Test Member",
                    member_bioguide_id="M000001",
                    filing_year=2024,
                    document_id="old-amendment",
                    asset_name="Late amendment that must not replace a newer reporting year",
                    symbol="LATE",
                    value_min=500_000,
                    value_max=1_000_000,
                ),
                HouseAnnualDisclosureHolding(
                    document_row_id=latest.id,
                    member_name="Test Member",
                    member_bioguide_id="M000001",
                    filing_year=2025,
                    document_id="latest-document",
                    asset_name="Apple",
                    symbol="aapl",
                    value_min=15_001,
                    value_max=50_000,
                ),
                HouseAnnualDisclosureHolding(
                    document_row_id=latest.id,
                    member_name="Test Member",
                    member_bioguide_id="M000001",
                    filing_year=2025,
                    document_id="latest-document",
                    asset_name="Open ended holding",
                    value_min=1_000_001,
                    value_max=None,
                ),
            ]
        )
        db.commit()

        payload = member_reported_holdings("M000001", request=_request(), db=db)

        assert payload["status"] == "ok"
        assert payload["report"]["document_id"] == "latest-document"
        assert payload["positions_count"] == 2
        assert payload["visible_symbols"] == ["AAPL"]
        assert payload["value_lower_bound"] == 1_015_002
        assert payload["value_upper_bound"] is None
        assert payload["estimated_net_worth"] == 1_032_501.5
        assert payload["estimated_net_worth_is_conservative"] is True
        assert payload["estimated_net_worth_method"] == "midpoint_of_reported_asset_ranges"
    finally:
        db.close()


def test_member_reported_holdings_does_not_fallback_to_simulated_positions():
    db = _session()
    try:
        db.add(
            Member(
                bioguide_id="M000002",
                first_name="No",
                last_name="Disclosure",
                chamber="house",
                party="D",
                state="CA",
            )
        )
        db.commit()

        payload = member_reported_holdings("M000002", request=_request(), db=db)

        assert payload["status"] == "unavailable"
        assert payload["report"] is None
        assert payload["items"] == []
    finally:
        db.close()
