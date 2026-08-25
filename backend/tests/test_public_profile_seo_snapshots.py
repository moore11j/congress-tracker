from __future__ import annotations

from datetime import date

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.models import GovernmentContract, InsiderTransactionNormalized
from app.services.seo_snapshots import refresh_department_seo_snapshot, refresh_insider_seo_snapshot


def _session():
    engine = create_engine("sqlite:///:memory:", future=True)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    Base.metadata.create_all(bind=engine)
    return SessionLocal()


def test_insider_snapshot_contains_ssr_safe_form_4_baseline():
    db = _session()
    try:
        db.add(
            InsiderTransactionNormalized(
                accession_number="0001214156-26-000001",
                normalized_hash="tim-cook-aapl-2026-08-01",
                issuer_name="Apple Inc.",
                ticker_normalized="AAPL",
                reporting_owner_cik="0001214156",
                reporting_owner_name="Tim Cook",
                officer_title="Chief Executive Officer",
                transaction_date=date(2026, 8, 1),
                filing_date=date(2026, 8, 2),
                transaction_type_normalized="sale",
                shares=100,
                price=200,
                value=20_000,
                shares_owned_following=1_000,
                direct_or_indirect="D",
                is_duplicate=False,
            )
        )
        db.flush()

        snapshot = refresh_insider_seo_snapshot(db, "0001214156")

        assert snapshot["canonical_path"] == "/insider/tim-cook-0001214156"
        assert snapshot["indexable"] is True
        assert snapshot["title"] == "Tim Cook Insider Trades & Form 4 Activity | Walnut Markets"
        assert snapshot["payload"]["primary_company_name"] == "Apple Inc."
        assert snapshot["payload"]["recent_activity"][0]["symbol"] == "AAPL"
        assert snapshot["payload"]["recent_activity"][0]["trade_value"] == 20_000
    finally:
        db.close()


def test_department_snapshot_is_a_complete_stored_public_profile():
    db = _session()
    try:
        db.add(
            GovernmentContract(
                source="usaspending",
                award_id="dod-1",
                dedupe_key="dod-1",
                symbol="LMT",
                recipient_name="Lockheed Martin",
                award_date=date(2026, 8, 3),
                award_amount=1_000_000,
                awarding_agency="Department of Defense",
                description="Aircraft support",
            )
        )
        db.commit()

        snapshot = refresh_department_seo_snapshot(db, "department-of-defense")

        assert snapshot["canonical_path"] == "/departments/department-of-defense"
        assert snapshot["indexable"] is True
        assert snapshot["title"] == "Department of Defense Government Contracts | Walnut Markets"
        assert snapshot["payload"]["summary"]["contractCount"] == 1
        assert snapshot["payload"]["tickers"][0]["symbol"] == "LMT"
        assert snapshot["payload"]["links"] == [{"label": "LMT stock research", "href": "/ticker/LMT"}]
    finally:
        db.close()
