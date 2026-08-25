from __future__ import annotations

from datetime import date

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.models import Member, Security, Transaction
from app.services.seo_snapshots import SEO_BATCH_MAX_LIMIT, refresh_member_seo_snapshot


def _session():
    engine = create_engine("sqlite:///:memory:", future=True)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    Base.metadata.create_all(bind=engine)
    return SessionLocal()


def test_member_snapshot_uses_public_slug_and_only_indexes_disclosed_activity():
    db = _session()
    try:
        member = Member(
            bioguide_id="J000001",
            first_name="Jane",
            last_name="Doe",
            chamber="House",
            party="D",
            state="CA",
        )
        security = Security(symbol="NVDA", name="NVIDIA", asset_class="equity", sector="Technology")
        db.add_all([member, security])
        db.flush()
        db.add(
            Transaction(
                filing_id=1,
                member_id=member.id,
                security_id=security.id,
                owner_type="self",
                transaction_type="Purchase",
                trade_date=date(2026, 8, 1),
                report_date=date(2026, 8, 15),
                amount_range_min=15_001,
                amount_range_max=50_000,
                description="NVIDIA Corporation",
            )
        )
        db.flush()

        snapshot = refresh_member_seo_snapshot(db, "J000001")

        assert snapshot["canonical_path"] == "/member/JANE_DOE"
        assert snapshot["indexable"] is True
        assert snapshot["title"] == "Jane Doe Stock Trades & Portfolio | Walnut Markets"
        assert snapshot["payload"]["recent_activity"][0]["symbol"] == "NVDA"
        assert snapshot["payload"]["links"] == [{"label": "NVDA stock research", "href": "/ticker/NVDA"}]

        sparse = Member(
            bioguide_id="S000001",
            first_name="Sparse",
            last_name="Member",
            chamber="Senate",
            party="I",
            state="VT",
        )
        db.add(sparse)
        db.flush()
        sparse_snapshot = refresh_member_seo_snapshot(db, "S000001")
        assert sparse_snapshot["indexable"] is False
    finally:
        db.close()


def test_member_snapshot_batch_limit_covers_a_full_congress_roster():
    assert SEO_BATCH_MAX_LIMIT >= 535
