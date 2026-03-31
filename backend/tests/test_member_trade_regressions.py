from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.main import _member_recent_trades, congress_trader_leaderboard, member_performance
from app.models import Event, Member, Security, TradeOutcome, Transaction
from app.services.signal_score import calculate_smart_score


def _session():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    Base.metadata.create_all(
        engine,
        tables=[
            Member.__table__,
            Security.__table__,
            Transaction.__table__,
            Event.__table__,
            TradeOutcome.__table__,
        ],
    )
    return Session()


def test_member_recent_trades_enriches_with_outcome_pnl_and_signal_fields():
    db = _session()
    try:
        member = Member(
            bioguide_id="W000797",
            first_name="Debbie",
            last_name="Wasserman Schultz",
            chamber="house",
            party="D",
            state="FL",
        )
        security = Security(symbol="AAPL", name="Apple", asset_class="equity", sector="Tech")
        db.add_all([member, security])
        db.flush()

        trade_day = date.today() - timedelta(days=20)
        tx = Transaction(
            filing_id=1,
            member_id=member.id,
            security_id=security.id,
            owner_type="self",
            transaction_type="P-PURCHASE",
            trade_date=trade_day,
            report_date=trade_day + timedelta(days=25),
            amount_range_min=1000,
            amount_range_max=15000,
            description="test tx",
        )
        db.add(tx)
        db.flush()

        event = Event(
            event_type="congress_trade",
            ts=datetime.now(timezone.utc),
            event_date=datetime.now(timezone.utc),
            symbol="AAPL",
            source="congress_disclosure",
            payload_json=json.dumps({"smart_score": 84, "smart_band": "strong"}),
            member_name="Debbie Wasserman Schultz",
            member_bioguide_id="FMP_HOUSE_FL23_DEBBIE_WASSERMAN_SCHULTZ",
            chamber="house",
            party="D",
            trade_type="purchase",
            transaction_type="P-PURCHASE",
            amount_min=1000,
            amount_max=15000,
        )
        db.add(event)
        db.flush()

        outcome = TradeOutcome(
            event_id=event.id,
            member_id="FMP_HOUSE_FL23_DEBBIE_WASSERMAN_SCHULTZ",
            member_name="Debbie Wasserman Schultz",
            symbol="AAPL",
            trade_type="purchase",
            source="congress",
            trade_date=trade_day,
            benchmark_symbol="^GSPC",
            return_pct=12.5,
            alpha_pct=5.0,
            amount_min=1000,
            amount_max=15000,
            scoring_status="ok",
            methodology_version="congress_v1",
            computed_at=datetime.now(timezone.utc),
        )
        db.add(outcome)
        db.commit()

        items = _member_recent_trades(db=db, member_pk=member.id, lookback_days=365, limit=100)

        assert len(items) == 1
        assert items[0]["event_id"] == event.id
        assert items[0]["pnl_pct"] == 12.5
        assert items[0]["smart_score"] == 84
        assert items[0]["smart_band"] == "strong"
    finally:
        db.close()


def test_member_recent_trades_reads_camel_case_signal_fields_from_event_payload():
    db = _session()
    try:
        member = Member(
            bioguide_id="W000797",
            first_name="Debbie",
            last_name="Wasserman Schultz",
            chamber="house",
            party="D",
            state="FL",
        )
        security = Security(symbol="MSFT", name="Microsoft", asset_class="equity", sector="Tech")
        db.add_all([member, security])
        db.flush()

        trade_day = date.today() - timedelta(days=14)
        tx = Transaction(
            filing_id=2,
            member_id=member.id,
            security_id=security.id,
            owner_type="self",
            transaction_type="P-PURCHASE",
            trade_date=trade_day,
            report_date=trade_day + timedelta(days=21),
            amount_range_min=1000,
            amount_range_max=15000,
            description="test tx camel case payload",
        )
        db.add(tx)
        db.flush()

        event = Event(
            event_type="congress_trade",
            ts=datetime.now(timezone.utc),
            event_date=datetime.now(timezone.utc),
            symbol="MSFT",
            source="congress_disclosure",
            payload_json=json.dumps({"smartScore": 91, "smartBand": "strong"}),
            member_name="Debbie Wasserman Schultz",
            member_bioguide_id="FMP_HOUSE_FL23_DEBBIE_WASSERMAN_SCHULTZ",
            chamber="house",
            party="D",
            trade_type="purchase",
            transaction_type="P-PURCHASE",
            amount_min=1000,
            amount_max=15000,
        )
        db.add(event)
        db.flush()

        outcome = TradeOutcome(
            event_id=event.id,
            member_id="FMP_HOUSE_FL23_DEBBIE_WASSERMAN_SCHULTZ",
            member_name="Debbie Wasserman Schultz",
            symbol="MSFT",
            trade_type="purchase",
            source="congress",
            trade_date=trade_day,
            benchmark_symbol="^GSPC",
            return_pct=7.5,
            alpha_pct=2.5,
            amount_min=1000,
            amount_max=15000,
            scoring_status="ok",
            methodology_version="congress_v1",
            computed_at=datetime.now(timezone.utc),
        )
        db.add(outcome)
        db.commit()

        items = _member_recent_trades(db=db, member_pk=member.id, lookback_days=365, limit=100)

        assert len(items) == 1
        assert items[0]["event_id"] == event.id
        assert items[0]["smart_score"] == 91
        assert items[0]["smart_band"] == "strong"
    finally:
        db.close()


def test_member_recent_trades_computes_signal_when_event_payload_has_no_smart_fields():
    db = _session()
    try:
        member = Member(
            bioguide_id="C001",
            first_name="Cleo",
            last_name="Fields",
            chamber="house",
            party="D",
            state="LA",
        )
        security = Security(symbol="NVDA", name="NVIDIA", asset_class="equity", sector="Tech")
        db.add_all([member, security])
        db.flush()

        trade_day = date.today() - timedelta(days=2)
        tx = Transaction(
            filing_id=3,
            member_id=member.id,
            security_id=security.id,
            owner_type="self",
            transaction_type="P-PURCHASE",
            trade_date=trade_day,
            report_date=trade_day + timedelta(days=10),
            amount_range_min=1000,
            amount_range_max=10000,
            description="missing smart fields test",
        )
        db.add(tx)
        db.flush()

        # Baseline history for unusual_multiple inference (AVG amount_max = 1000).
        for idx in range(3):
            ts = datetime.now(timezone.utc) - timedelta(days=120 + idx)
            db.add(
                Event(
                    event_type="congress_trade",
                    ts=ts,
                    event_date=ts,
                    symbol="NVDA",
                    source="congress_disclosure",
                    payload_json=json.dumps({}),
                    member_name=f"Baseline Member {idx}",
                    member_bioguide_id=f"B{idx}",
                    chamber="house",
                    party="D",
                    trade_type="purchase",
                    transaction_type="P-PURCHASE",
                    amount_min=1000,
                    amount_max=1000,
                )
            )

        event_ts = datetime.now(timezone.utc) - timedelta(days=1)
        event = Event(
            event_type="congress_trade",
            ts=event_ts,
            event_date=event_ts,
            symbol="NVDA",
            source="congress_disclosure",
            payload_json=json.dumps({"note": "no smart fields here"}),
            member_name="Cleo Fields",
            member_bioguide_id="FMP_HOUSE_LA04_CLEO_FIELDS",
            chamber="house",
            party="D",
            trade_type="purchase",
            transaction_type="P-PURCHASE",
            amount_min=1000,
            amount_max=10000,
        )
        db.add(event)
        db.flush()

        outcome = TradeOutcome(
            event_id=event.id,
            member_id="FMP_HOUSE_LA04_CLEO_FIELDS",
            member_name="Cleo Fields",
            symbol="NVDA",
            trade_type="purchase",
            source="congress",
            trade_date=trade_day,
            benchmark_symbol="^GSPC",
            return_pct=4.0,
            alpha_pct=2.0,
            amount_min=1000,
            amount_max=10000,
            scoring_status="ok",
            methodology_version="congress_v1",
            computed_at=datetime.now(timezone.utc),
        )
        db.add(outcome)
        db.commit()

        items = _member_recent_trades(db=db, member_pk=member.id, lookback_days=365, limit=100)

        assert len(items) == 1
        expected_unusual_multiple = 10000 / 3250
        expected_score, expected_band = calculate_smart_score(
            unusual_multiple=expected_unusual_multiple,
            amount_max=10000,
            ts=event_ts,
        )
        assert items[0]["smart_score"] == expected_score
        assert items[0]["smart_band"] == expected_band
    finally:
        db.close()


def test_congress_leaderboard_matches_member_alpha_summary_cohort():
    db = _session()
    try:
        member = Member(
            bioguide_id="W000797",
            first_name="Debbie",
            last_name="Wasserman Schultz",
            chamber="house",
            party="D",
            state="FL",
        )
        db.add(member)
        db.flush()

        today = date.today()
        trade_date_a = today - timedelta(days=30)
        trade_date_b = today - timedelta(days=60)

        rows = [
            TradeOutcome(
                event_id=101,
                member_id="W000797",
                member_name="Debbie Wasserman Schultz",
                symbol="MSFT",
                trade_type="purchase",
                trade_date=trade_date_a,
                benchmark_symbol="^GSPC",
                return_pct=10.0,
                alpha_pct=8.0,
                amount_min=1000,
                amount_max=15000,
                scoring_status="ok",
                methodology_version="congress_v1",
                computed_at=datetime.now(timezone.utc),
            ),
            TradeOutcome(
                event_id=102,
                member_id="FMP_HOUSE_FL23_DEBBIE_WASSERMAN_SCHULTZ",
                member_name="Debbie Wasserman Schultz",
                symbol="MSFT",
                trade_type="purchase",
                trade_date=trade_date_a,
                benchmark_symbol="^GSPC",
                return_pct=11.0,
                alpha_pct=9.0,
                amount_min=1000,
                amount_max=15000,
                scoring_status="ok",
                methodology_version="congress_v1",
                computed_at=datetime.now(timezone.utc),
            ),
            TradeOutcome(
                event_id=103,
                member_id="FMP_HOUSE_FL23_DEBBIE_WASSERMAN_SCHULTZ",
                member_name="Debbie Wasserman Schultz",
                symbol="NVDA",
                trade_type="sale",
                trade_date=trade_date_b,
                benchmark_symbol="^GSPC",
                return_pct=-2.0,
                alpha_pct=-1.0,
                amount_min=50000,
                amount_max=100000,
                scoring_status="ok",
                methodology_version="congress_v1",
                computed_at=datetime.now(timezone.utc),
            ),
        ]
        db.add_all(rows)
        db.commit()

        perf = member_performance(member_id="W000797", lookback_days=365, db=db)
        leaderboard = congress_trader_leaderboard(
            lookback_days=365,
            chamber="all",
            source_mode="congress",
            sort="avg_alpha",
            min_trades=1,
            limit=100,
            db=db,
        )

        debbie = next(row for row in leaderboard["rows"] if row["member_id"] == "W000797")
        assert debbie["trade_count_scored"] == perf["trade_count_scored"] == 2
        assert round(float(debbie["avg_alpha"]), 6) == round(float(perf["avg_alpha"]), 6)
    finally:
        db.close()
