from datetime import date

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.main import _member_recent_trades
from app.models import Member, Security, Transaction
from app.services.policy_relevance import resolve_policy_relevance


def _session():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    Base.metadata.create_all(
        engine,
        tables=[Member.__table__, Security.__table__, Transaction.__table__],
    )
    return Session()


def test_resolve_policy_relevance_flags_overlap_for_curated_member_and_sector():
    member = Member(bioguide_id="P000197", first_name="Nancy", last_name="Pelosi", chamber="house", party="D", state="CA")

    result = resolve_policy_relevance(
        member=member,
        symbol="NVDA",
        sector="Technology",
        security_name="NVIDIA Corp",
    )

    assert result.committee_relevant is True
    assert result.relevance_domain == "technology"
    assert result.relevance_label == "Policy-domain relevant: Technology"


def test_member_recent_trades_includes_policy_relevance_fields():
    db = _session()
    try:
        member = Member(bioguide_id="P000197", first_name="Nancy", last_name="Pelosi", chamber="house", party="D", state="CA")
        security = Security(symbol="MSFT", name="Microsoft", asset_class="equity", sector="Technology")
        db.add_all([member, security])
        db.flush()

        tx = Transaction(
            filing_id=1,
            member_id=member.id,
            security_id=security.id,
            owner_type="self",
            transaction_type="P-PURCHASE",
            trade_date=date.today(),
            report_date=date.today(),
            amount_range_min=1000,
            amount_range_max=15000,
            description="policy relevance",
        )
        db.add(tx)
        db.commit()

        items = _member_recent_trades(db=db, member_pk=member.id, lookback_days=365, limit=10)

        assert len(items) == 1
        assert items[0]["committee_relevant"] is True
        assert items[0]["relevance_domain"] == "technology"
        assert items[0]["relevance_label"] == "Policy-domain relevant: Technology"
    finally:
        db.close()
