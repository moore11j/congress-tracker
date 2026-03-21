from datetime import date, datetime, timedelta, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.main import _member_recent_trades
from app.models import Member, Security, Transaction


def test_member_recent_trades_filters_by_trade_date_not_report_date():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    Base.metadata.create_all(
        engine,
        tables=[Member.__table__, Security.__table__, Transaction.__table__],
    )

    db = Session()
    try:
        member = Member(
            bioguide_id="M000355",
            first_name="Mitch",
            last_name="McConnell",
            chamber="senate",
            party="R",
            state="KY",
        )
        security = Security(
            symbol="WFC",
            name="Wells Fargo & Company",
            asset_class="equity",
            sector="Financial Services",
        )
        db.add_all([member, security])
        db.flush()

        today = datetime.now(timezone.utc).date()
        stale_trade_date = today - timedelta(days=220)
        in_window_trade_date = today - timedelta(days=20)

        # Old trade date but recent report date should be excluded for 180D lookback.
        stale_trade = Transaction(
            filing_id=1,
            member_id=member.id,
            security_id=security.id,
            owner_type="self",
            transaction_type="P-PURCHASE",
            trade_date=stale_trade_date,
            report_date=today - timedelta(days=2),
            amount_range_min=1000,
            amount_range_max=15000,
            description="stale trade date",
        )
        in_window_trade = Transaction(
            filing_id=2,
            member_id=member.id,
            security_id=security.id,
            owner_type="self",
            transaction_type="P-PURCHASE",
            trade_date=in_window_trade_date,
            report_date=today - timedelta(days=1),
            amount_range_min=1000,
            amount_range_max=15000,
            description="in-window trade date",
        )
        db.add_all([stale_trade, in_window_trade])
        db.commit()

        items = _member_recent_trades(db=db, member_pk=member.id, lookback_days=180, limit=100)

        assert [item["trade_date"] for item in items] == [in_window_trade_date.isoformat()]
        assert all(item["trade_date"] != stale_trade_date.isoformat() for item in items)
        assert all(item["report_date"] is not None for item in items)
    finally:
        db.close()


def test_member_recent_trades_orders_by_newest_trade_date_first():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    Base.metadata.create_all(
        engine,
        tables=[Member.__table__, Security.__table__, Transaction.__table__],
    )

    db = Session()
    try:
        member = Member(
            bioguide_id="M000355",
            first_name="Mitch",
            last_name="McConnell",
            chamber="senate",
            party="R",
            state="KY",
        )
        security = Security(
            symbol="ABC",
            name="ABC Corp",
            asset_class="equity",
            sector="Industrials",
        )
        db.add_all([member, security])
        db.flush()

        today = date.today()
        older = Transaction(
            filing_id=10,
            member_id=member.id,
            security_id=security.id,
            owner_type="self",
            transaction_type="P-PURCHASE",
            trade_date=today - timedelta(days=30),
            report_date=today - timedelta(days=1),
            amount_range_min=1000,
            amount_range_max=15000,
            description="older trade date",
        )
        newer = Transaction(
            filing_id=11,
            member_id=member.id,
            security_id=security.id,
            owner_type="self",
            transaction_type="P-PURCHASE",
            trade_date=today - timedelta(days=5),
            report_date=today - timedelta(days=10),
            amount_range_min=1000,
            amount_range_max=15000,
            description="newer trade date",
        )
        db.add_all([older, newer])
        db.commit()

        items = _member_recent_trades(db=db, member_pk=member.id, lookback_days=365, limit=100)
        assert [item["trade_date"] for item in items] == [
            (today - timedelta(days=5)).isoformat(),
            (today - timedelta(days=30)).isoformat(),
        ]
    finally:
        db.close()
