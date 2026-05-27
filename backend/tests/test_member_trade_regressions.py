from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone

from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker
from starlette.requests import Request

from app.auth import sign_session_payload
from app.db import Base
from app.entitlements import seed_plan_config
from app.main import _member_recent_trades, _member_top_tickers, congress_trader_leaderboard, member_performance
from app.models import CongressMemberAlias, Event, FeatureGate, GovernmentContractAction, Member, PlanLimit, PlanPrice, ReplicatedPortfolioRun, Security, TradeOutcome, Transaction, UserAccount
from app.routers.events import list_events
from app.services.signal_score import calculate_smart_score


def _session():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    Base.metadata.create_all(
        engine,
        tables=[
            Member.__table__,
            CongressMemberAlias.__table__,
            Security.__table__,
            Transaction.__table__,
            Event.__table__,
            GovernmentContractAction.__table__,
            TradeOutcome.__table__,
            ReplicatedPortfolioRun.__table__,
            UserAccount.__table__,
            FeatureGate.__table__,
            PlanLimit.__table__,
            PlanPrice.__table__,
        ],
    )
    db = Session()
    seed_plan_config(db)
    return db


def _premium_request(db) -> Request:
    user = UserAccount(email="premium-reader@example.com", role="user", entitlement_tier="premium")
    db.add(user)
    db.commit()
    db.refresh(user)
    token = sign_session_payload({"uid": user.id, "email": user.email})
    return Request({"type": "http", "method": "GET", "path": "/", "headers": [(b"authorization", f"Bearer {token}".encode())]})


def _stub_event_route_enrichment(monkeypatch):
    monkeypatch.setattr("app.routers.events.get_current_prices_meta_db", lambda *_args, **_kwargs: {})
    monkeypatch.setattr("app.routers.events.get_eod_close", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("app.routers.events.get_confirmation_metrics_for_symbols", lambda *_args, **_kwargs: {})
    monkeypatch.setattr("app.routers.events._ticker_meta_with_security_names", lambda *_args, **_kwargs: {})


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
            entry_price=125.0,
            current_price=140.0,
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
        assert items[0]["estimated_price"] == 125.0
        assert items[0]["current_price"] == 140.0
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
            request=_premium_request(db),
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


def test_member_recent_trades_uses_canonical_events_sorted_by_report_date_and_safe_labels():
    db = _session()
    try:
        member = Member(
            bioguide_id="K000375",
            first_name="William",
            last_name="Keating",
            chamber="house",
            party="D",
            state="MA",
        )
        db.add(member)
        db.flush()

        older_trade_newer_report = Event(
            event_type="congress_trade",
            ts=datetime(2026, 5, 19, tzinfo=timezone.utc),
            event_date=datetime(2026, 5, 19, tzinfo=timezone.utc),
            symbol="JPM",
            source="house_fmp",
            payload_json=json.dumps({
                "symbol": "JPM",
                "company_name": "JPMorgan Chase & Co",
                "security_name": "JPMorgan Chase & Co",
                "trade_date": "2026-04-01",
                "report_date": "2026-05-19",
                "transaction_type": "sale",
            }),
            member_name="William Keating",
            member_bioguide_id="K000375",
            chamber="house",
            party="D",
            trade_type="sale",
            transaction_type="sale",
            amount_min=1001,
            amount_max=15000,
            impact_score=0,
        )
        newer_trade_older_report = Event(
            event_type="congress_trade",
            ts=datetime(2026, 5, 15, tzinfo=timezone.utc),
            event_date=datetime(2026, 5, 15, tzinfo=timezone.utc),
            symbol=None,
            source="house_fmp",
            payload_json=json.dumps({
                "symbol": "congress_trade",
                "security_name": "congress_trade",
                "description": "First Citizens BancShares Inc",
                "trade_date": "2026-05-10",
                "report_date": "2026-05-15",
                "transaction_type": "purchase",
            }),
            member_name="William Keating",
            member_bioguide_id="K000375",
            chamber="house",
            party="D",
            trade_type="purchase",
            transaction_type="purchase",
            amount_min=1001,
            amount_max=15000,
            impact_score=0,
        )
        db.add_all([older_trade_newer_report, newer_trade_older_report])
        db.flush()
        db.add(
            TradeOutcome(
                event_id=older_trade_newer_report.id,
                member_id="K000375",
                member_name="William Keating",
                symbol="JPM",
                trade_type="sale",
                source="congress",
                trade_date=date(2026, 4, 1),
                benchmark_symbol="^GSPC",
                return_pct=3.25,
                alpha_pct=1.0,
                amount_min=1001,
                amount_max=15000,
                scoring_status="ok",
                methodology_version="congress_v1",
                computed_at=datetime.now(timezone.utc),
            )
        )
        db.commit()

        items = _member_recent_trades(db=db, member_pk=member.id, lookback_days=365, limit=100)

        assert [item["event_id"] for item in items] == [older_trade_newer_report.id, newer_trade_older_report.id]
        assert items[0]["symbol"] == "JPM"
        assert items[0]["security_name"] == "JPMorgan Chase & Co"
        assert items[0]["pnl_pct"] == 3.25
        assert items[1]["symbol"] is None
        assert items[1]["security_name"] == "First Citizens BancShares Inc"
        assert all(item["security_name"] != "congress_trade" for item in items)
    finally:
        db.close()


def test_events_member_filter_resolves_bill_keating_alias(monkeypatch):
    db = _session()
    try:
        _stub_event_route_enrichment(monkeypatch)
        event_ts = datetime(2026, 5, 19, tzinfo=timezone.utc)
        db.add_all(
            [
                Event(
                    id=701,
                    event_type="congress_trade",
                    ts=event_ts,
                    event_date=event_ts,
                    symbol="OTIS",
                    source="congress_disclosure",
                    payload_json=json.dumps({
                        "symbol": "OTIS",
                        "company_name": "Otis Worldwide Corp",
                        "trade_date": "2026-05-01",
                        "report_date": "2026-05-19",
                    }),
                    member_name="William R. Keating",
                    member_bioguide_id="K000375",
                    chamber="house",
                    party="D",
                    trade_type="sale",
                    transaction_type="sale",
                    amount_min=1001,
                    amount_max=15000,
                ),
                Event(
                    id=702,
                    event_type="congress_trade",
                    ts=event_ts - timedelta(days=1),
                    event_date=event_ts - timedelta(days=1),
                    symbol="CPB",
                    source="congress_disclosure",
                    payload_json=json.dumps({"symbol": "CPB"}),
                    member_name="Bill Keating",
                    member_bioguide_id="FMP_HOUSE_MA09",
                    chamber="house",
                    party="D",
                    trade_type="sale",
                    transaction_type="sale",
                    amount_min=1001,
                    amount_max=15000,
                ),
            ]
        )
        db.commit()

        canonical = list_events(db=db, member="William R. Keating", mode="congress", limit=50, enrich_prices=False)
        alias = list_events(db=db, member="Bill Keating", mode="congress", limit=50, enrich_prices=False)

        assert [item.id for item in canonical.items] == [701]
        assert [item.id for item in alias.items] == [701]
        assert alias.items[0].member_name == "William R. Keating"
        assert alias.items[0].symbol == "OTIS"
    finally:
        db.close()


def test_events_ticker_and_member_filters_are_combined(monkeypatch):
    db = _session()
    try:
        _stub_event_route_enrichment(monkeypatch)
        event_ts = datetime(2026, 5, 15, tzinfo=timezone.utc)
        db.add_all(
            [
                Event(
                    id=711,
                    event_type="congress_trade",
                    ts=event_ts,
                    event_date=event_ts,
                    symbol="FCBN",
                    source="congress_disclosure",
                    payload_json=json.dumps({
                        "symbol": "FCBN",
                        "company_name": "First Citizens BancShares Inc",
                        "trade_date": "2026-05-01",
                        "report_date": "2026-05-15",
                    }),
                    member_name="John Fetterman",
                    member_bioguide_id="F000479",
                    chamber="senate",
                    party="D",
                    trade_type="purchase",
                    transaction_type="purchase",
                    amount_min=1001,
                    amount_max=15000,
                ),
                Event(
                    id=712,
                    event_type="congress_trade",
                    ts=event_ts - timedelta(minutes=1),
                    event_date=event_ts - timedelta(minutes=1),
                    symbol="JPM",
                    source="congress_disclosure",
                    payload_json=json.dumps({
                        "symbol": "JPM",
                        "company_name": "JPMorgan Chase & Co",
                        "trade_date": "2026-05-01",
                        "report_date": "2026-05-15",
                    }),
                    member_name="John Fetterman",
                    member_bioguide_id="F000479",
                    chamber="senate",
                    party="D",
                    trade_type="purchase",
                    transaction_type="purchase",
                    amount_min=1001,
                    amount_max=15000,
                ),
                Event(
                    id=713,
                    event_type="congress_trade",
                    ts=event_ts - timedelta(minutes=2),
                    event_date=event_ts - timedelta(minutes=2),
                    symbol=None,
                    source="congress_disclosure",
                    payload_json=json.dumps({
                        "security_description": "Unresolved security",
                        "trade_date": "2026-05-01",
                        "report_date": "2026-05-15",
                    }),
                    member_name="John Fetterman",
                    member_bioguide_id="F000479",
                    chamber="senate",
                    party="D",
                    trade_type="purchase",
                    transaction_type="purchase",
                    amount_min=1001,
                    amount_max=15000,
                ),
            ]
        )
        db.commit()

        fcbn = list_events(db=db, ticker="FCBN", member="John Fetterman", limit=20, enrich_prices=False)
        jpm = list_events(db=db, ticker="JPM", member="John Fetterman", limit=20, enrich_prices=False)

        assert [item.symbol for item in fcbn.items] == ["FCBN"]
        assert [item.id for item in fcbn.items] == [711]
        assert [item.symbol for item in jpm.items] == ["JPM"]
        assert [item.id for item in jpm.items] == [712]
    finally:
        db.close()


def test_events_member_nickname_filter_avoids_ambiguous_matches(monkeypatch):
    db = _session()
    try:
        _stub_event_route_enrichment(monkeypatch)
        event_ts = datetime(2026, 5, 19, tzinfo=timezone.utc)
        db.add_all(
            [
                Event(
                    id=721,
                    event_type="congress_trade",
                    ts=event_ts,
                    event_date=event_ts,
                    symbol="AAA",
                    source="congress_disclosure",
                    payload_json=json.dumps({"symbol": "AAA"}),
                    member_name="William Smith",
                    member_bioguide_id="S000001",
                    chamber="house",
                    party="D",
                    trade_type="purchase",
                    transaction_type="purchase",
                    amount_min=1001,
                    amount_max=15000,
                ),
                Event(
                    id=722,
                    event_type="congress_trade",
                    ts=event_ts - timedelta(minutes=1),
                    event_date=event_ts - timedelta(minutes=1),
                    symbol="BBB",
                    source="congress_disclosure",
                    payload_json=json.dumps({"symbol": "BBB"}),
                    member_name="William Smith",
                    member_bioguide_id="S000002",
                    chamber="house",
                    party="R",
                    trade_type="purchase",
                    transaction_type="purchase",
                    amount_min=1001,
                    amount_max=15000,
                ),
            ]
        )
        db.commit()

        page = list_events(db=db, member="Bill Smith", mode="congress", limit=20, enrich_prices=False)

        assert page.items == []
    finally:
        db.close()


def test_member_top_tickers_uses_deduped_outcomes_for_obvious_concentration():
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
        event_specs = [
            (301, "MSFT", "purchase", today - timedelta(days=30), 1000, 15000),
            (302, "MSFT", "sale", today - timedelta(days=20), 15000, 50000),
            (303, "AAPL", "purchase", today - timedelta(days=10), 1000, 15000),
        ]
        for event_id, symbol, side, trade_date, amount_min, amount_max in event_specs:
            event_ts = datetime.combine(trade_date, datetime.min.time(), tzinfo=timezone.utc)
            db.add(
                Event(
                    id=event_id,
                    event_type="congress_trade",
                    ts=event_ts,
                    event_date=event_ts,
                    symbol=symbol,
                    source="congress_disclosure",
                    payload_json=json.dumps({}),
                    member_name="Debbie Wasserman Schultz",
                    member_bioguide_id="W000797",
                    chamber="house",
                    party="D",
                    trade_type=side,
                    transaction_type=side,
                    amount_min=amount_min,
                    amount_max=amount_max,
                )
            )
            db.add(
                TradeOutcome(
                    event_id=event_id,
                    member_id="W000797",
                    member_name="Debbie Wasserman Schultz",
                    symbol=symbol,
                    trade_type=side,
                    source="congress",
                    trade_date=trade_date,
                    benchmark_symbol="^GSPC",
                    return_pct=1.0,
                    alpha_pct=0.5,
                    amount_min=amount_min,
                    amount_max=amount_max,
                    scoring_status="ok",
                    methodology_version="congress_v1",
                    computed_at=datetime.now(timezone.utc),
                )
            )
        db.commit()

        top_tickers = _member_top_tickers(db, member, limit=3)

        assert top_tickers[0] == {"symbol": "MSFT", "trades": 2}
        assert top_tickers[1] == {"symbol": "AAPL", "trades": 1}
    finally:
        db.close()


def test_insider_leaderboard_uses_persisted_market_trade_outcomes_only():
    db = _session()
    try:
        today = date.today()
        insider_id = "0001234567"

        market_event_ok = Event(
            event_type="insider_trade",
            ts=datetime.now(timezone.utc),
            event_date=datetime.now(timezone.utc),
            symbol="NVDA",
            source="sec_form4",
            payload_json=json.dumps({"reporting_cik": insider_id}),
            member_name="Casey Insider",
            member_bioguide_id=None,
            chamber=None,
            party=None,
            trade_type="purchase",
            transaction_type="purchase",
            amount_min=1000,
            amount_max=5000,
        )
        market_event_unscored = Event(
            event_type="insider_trade",
            ts=datetime.now(timezone.utc),
            event_date=datetime.now(timezone.utc),
            symbol="AAPL",
            source="sec_form4",
            payload_json=json.dumps({"reporting_cik": insider_id}),
            member_name="Casey Insider",
            member_bioguide_id=None,
            chamber=None,
            party=None,
            trade_type="sale",
            transaction_type="sale",
            amount_min=1000,
            amount_max=5000,
        )
        non_market_event = Event(
            event_type="insider_trade",
            ts=datetime.now(timezone.utc),
            event_date=datetime.now(timezone.utc),
            symbol="MSFT",
            source="sec_form4",
            payload_json=json.dumps({"reporting_cik": insider_id}),
            member_name="Casey Insider",
            member_bioguide_id=None,
            chamber=None,
            party=None,
            trade_type="received",
            transaction_type="received",
            amount_min=1000,
            amount_max=5000,
        )
        db.add_all([market_event_ok, market_event_unscored, non_market_event])
        db.flush()

        db.add_all(
            [
                TradeOutcome(
                    event_id=market_event_ok.id,
                    member_id=insider_id,
                    member_name="Casey Insider",
                    symbol="NVDA",
                    trade_type="purchase",
                    source="insider",
                    trade_date=today - timedelta(days=20),
                    benchmark_symbol="^GSPC",
                    return_pct=15.0,
                    alpha_pct=9.0,
                    amount_min=1000,
                    amount_max=5000,
                    scoring_status="ok",
                    methodology_version="insider_v1",
                    computed_at=datetime.now(timezone.utc),
                ),
                TradeOutcome(
                    event_id=market_event_unscored.id,
                    member_id=insider_id,
                    member_name="Casey Insider",
                    symbol="AAPL",
                    trade_type="sale",
                    source="insider",
                    trade_date=today - timedelta(days=10),
                    benchmark_symbol="^GSPC",
                    return_pct=None,
                    alpha_pct=None,
                    amount_min=1000,
                    amount_max=5000,
                    scoring_status="no_current_price",
                    methodology_version="insider_v1",
                    computed_at=datetime.now(timezone.utc),
                ),
                TradeOutcome(
                    event_id=non_market_event.id,
                    member_id=insider_id,
                    member_name="Casey Insider",
                    symbol="MSFT",
                    trade_type="received",
                    source="insider",
                    trade_date=today - timedelta(days=5),
                    benchmark_symbol="^GSPC",
                    return_pct=None,
                    alpha_pct=None,
                    amount_min=1000,
                    amount_max=5000,
                    scoring_status="insider_non_market",
                    methodology_version="insider_v1",
                    computed_at=datetime.now(timezone.utc),
                ),
            ]
        )
        db.commit()

        leaderboard = congress_trader_leaderboard(
            request=_premium_request(db),
            lookback_days=365,
            chamber="all",
            source_mode="insiders",
            sort="avg_alpha",
            min_trades=1,
            limit=50,
            db=db,
        )

        assert len(leaderboard["rows"]) == 1
        row = leaderboard["rows"][0]
        assert row["member_id"] == insider_id
        assert row["member_name"] == "Casey Insider"
        assert row["trade_count_total"] == 2
        assert row["trade_count_scored"] == 1
        assert row["avg_alpha"] == 9.0
    finally:
        db.close()


def test_congress_leaderboard_reads_persisted_alias_snapshot_when_present():
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

        db.add_all(
            [
                CongressMemberAlias(
                    alias_member_id="W000797",
                    group_key="W000797",
                    authoritative_member_id="W000797",
                    member_name="Debbie Wasserman Schultz",
                    member_slug="W000797",
                    chamber="house",
                    party="DEMOCRAT",
                    state="FL",
                    updated_at=datetime.now(timezone.utc),
                ),
                CongressMemberAlias(
                    alias_member_id="FMP_HOUSE_FL23_DEBBIE_WASSERMAN_SCHULTZ",
                    group_key="W000797",
                    authoritative_member_id="W000797",
                    member_name="Debbie Wasserman Schultz",
                    member_slug="W000797",
                    chamber="house",
                    party="DEMOCRAT",
                    state="FL",
                    updated_at=datetime.now(timezone.utc),
                ),
            ]
        )
        db.add_all(
            [
                TradeOutcome(
                    event_id=201,
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
                    event_id=202,
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
                    event_id=203,
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
        )
        db.commit()

        leaderboard = congress_trader_leaderboard(
            request=_premium_request(db),
            lookback_days=365,
            chamber="all",
            source_mode="congress",
            sort="avg_alpha",
            min_trades=1,
            limit=100,
            db=db,
        )

        debbie = next(row for row in leaderboard["rows"] if row["member_id"] == "W000797")
        assert debbie["trade_count_scored"] == 2
        assert debbie["trade_count_total"] == 2
        assert round(float(debbie["avg_alpha"]), 6) == 4.0
        assert debbie["party"] == "DEMOCRAT"
        assert "quality_filter_applied" not in leaderboard
        assert "metadata" not in leaderboard
    finally:
        db.close()


def _add_portfolio_run(
    db,
    *,
    entity_id: str,
    total_return_pct: float,
    alpha_pct: float,
    sharpe_ratio: float,
    curve_quality_status: str = "good",
    created_at: datetime | None = None,
) -> ReplicatedPortfolioRun:
    curve_quality_status = curve_quality_status.strip().lower()
    run = ReplicatedPortfolioRun(
        entity_type="congress_member",
        entity_id=entity_id,
        mode="realistic_disclosure_lag",
        lookback_days=1095,
        benchmark_symbol="^GSPC",
        start_date=date(2023, 1, 1),
        end_date=date(2026, 1, 1),
        ending_value=100000.0 * (1.0 + (total_return_pct / 100.0)),
        benchmark_ending_value=177286.333,
        total_return_pct=total_return_pct,
        benchmark_return_pct=77.286333,
        alpha_pct=alpha_pct,
        cagr_pct=9.0,
        max_drawdown_pct=5.0,
        volatility_pct=12.0,
        sharpe_ratio=sharpe_ratio,
        win_rate_pct=66.7,
        average_exposure_pct=80.0,
        ending_cash_pct=20.0,
        points_count=10,
        positions_count=3,
        skipped_events_count=1,
        status="ok",
        status_message=json.dumps(
            {
                "curve_diagnostics": {
                    "curve_quality_status": curve_quality_status,
                    "curve_quality_notes": [f"{curve_quality_status} fixture"],
                    "data_coverage_notes": [f"{curve_quality_status} fixture"],
                }
            }
        ),
        created_at=created_at or datetime.now(timezone.utc),
        computed_at=created_at or datetime.now(timezone.utc),
    )
    db.add(run)
    db.flush()
    return run


def test_congress_portfolio_leaderboard_reads_persisted_runs_only_and_sorts(monkeypatch):
    db = _session()
    try:
        db.add_all(
            [
                Member(bioguide_id="J000310", first_name="Julie", last_name="Johnson", chamber="house", party="D", state="TX"),
                Member(bioguide_id="H001094", first_name="Val", last_name="Hoyle", chamber="house", party="D", state="OR"),
                Member(bioguide_id="MISSING1", first_name="No", last_name="Run", chamber="house", party="R", state="CA"),
            ]
        )
        db.add_all(
            [
                CongressMemberAlias(alias_member_id="J000310", group_key="J000310", authoritative_member_id="J000310", member_name="Julie Johnson", member_slug="J000310", chamber="house", party="DEMOCRAT", state="TX"),
                CongressMemberAlias(alias_member_id="H001094", group_key="H001094", authoritative_member_id="H001094", member_name="Val Hoyle", member_slug="H001094", chamber="house", party="DEMOCRAT", state="OR"),
                CongressMemberAlias(alias_member_id="MISSING1", group_key="MISSING1", authoritative_member_id="MISSING1", member_name="No Run", member_slug="MISSING1", chamber="house", party="REPUBLICAN", state="CA"),
            ]
        )
        julie_run = _add_portfolio_run(db, entity_id="J000310", total_return_pct=31.356529, alpha_pct=-45.929804, sharpe_ratio=1.16994)
        val_run = _add_portfolio_run(db, entity_id="H001094", total_return_pct=4.413867, alpha_pct=-72.872466, sharpe_ratio=0.440276)
        db.commit()

        def fail_if_called(*_args, **_kwargs):
            raise AssertionError("leaderboard portfolio path must not compute or fetch prices")

        monkeypatch.setattr("app.main.latest_replicated_portfolio_payload", fail_if_called)
        monkeypatch.setattr("app.main.get_eod_close", fail_if_called)
        monkeypatch.setattr("app.main.get_daily_close_series_with_fallback", fail_if_called)
        monkeypatch.setattr("app.main.get_current_prices", fail_if_called)

        request = _premium_request(db)
        writes: list[str] = []

        def track_writes(_conn, _cursor, statement, _parameters, _context, _executemany):
            if statement.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE")):
                writes.append(statement)

        bind = db.get_bind()
        event.listen(bind, "before_cursor_execute", track_writes)
        try:
            for sort in ["total_return_pct", "alpha_pct", "sharpe_ratio"]:
                leaderboard = congress_trader_leaderboard(
                    request=request,
                    lookback_days=1095,
                    chamber="all",
                    source_mode="congress",
                    performance_model="portfolio",
                    mode="realistic_disclosure_lag",
                    sort=sort,
                    min_trades=1,
                    limit=50,
                    db=db,
                )
                assert leaderboard["performance_model"] == "portfolio"
                assert leaderboard["persisted_only"] is True
                assert leaderboard["metadata"]["persisted_only"] is True
                assert leaderboard["metadata"]["missing_portfolio_runs_count"] == 1
                assert leaderboard["metadata"]["quality_filter_applied"] is True
                assert leaderboard["metadata"]["excluded_poor_quality_count"] == 0
                assert leaderboard["metadata"]["included_quality_statuses"] == ["good", "warning"]
                assert [row["member_id"] for row in leaderboard["rows"]] == ["J000310", "H001094"]
                assert leaderboard["rows"][0]["portfolio_run_id"] == julie_run.id
                assert leaderboard["rows"][1]["portfolio_run_id"] == val_run.id
                assert [row["curve_quality_status"] for row in leaderboard["rows"]] == ["good", "good"]

            by_alias_sort = congress_trader_leaderboard(
                request=request,
                lookback_days=1095,
                chamber="all",
                source_mode="congress",
                performance_model="portfolio",
                sort="alpha",
                min_trades=1,
                limit=50,
                db=db,
            )
            assert by_alias_sort["sort"] == "alpha_pct"
            assert by_alias_sort["rows"][0]["total_return_pct"] == 31.356529
            assert by_alias_sort["rows"][0]["alpha_pct"] == -45.929804
            assert by_alias_sort["rows"][0]["sharpe_ratio"] == 1.16994
            assert by_alias_sort["rows"][0]["benchmark_return_pct"] == 77.286333
            assert by_alias_sort["rows"][0]["average_exposure_pct"] == 80.0
        finally:
            event.remove(bind, "before_cursor_execute", track_writes)

        assert writes == []
    finally:
        db.close()


def test_congress_portfolio_leaderboard_filters_poor_quality_by_default():
    db = _session()
    try:
        db.add_all(
            [
                CongressMemberAlias(alias_member_id="GOOD1", group_key="GOOD1", authoritative_member_id="GOOD1", member_name="Good Member", member_slug="GOOD1", chamber="house", party="DEMOCRAT", state="CA"),
                CongressMemberAlias(alias_member_id="WARN1", group_key="WARN1", authoritative_member_id="WARN1", member_name="Warning Member", member_slug="WARN1", chamber="house", party="DEMOCRAT", state="CA"),
                CongressMemberAlias(alias_member_id="POOR1", group_key="POOR1", authoritative_member_id="POOR1", member_name="Poor Member", member_slug="POOR1", chamber="house", party="REPUBLICAN", state="TX"),
            ]
        )
        good_run = _add_portfolio_run(db, entity_id="GOOD1", total_return_pct=30.0, alpha_pct=20.0, sharpe_ratio=1.2, curve_quality_status="good")
        warning_run = _add_portfolio_run(db, entity_id="WARN1", total_return_pct=20.0, alpha_pct=10.0, sharpe_ratio=0.9, curve_quality_status="warning")
        poor_run = _add_portfolio_run(db, entity_id="POOR1", total_return_pct=100.0, alpha_pct=90.0, sharpe_ratio=3.0, curve_quality_status="poor")
        db.commit()
        request = _premium_request(db)

        default_leaderboard = congress_trader_leaderboard(
            request=request,
            lookback_days=1095,
            chamber="all",
            source_mode="congress",
            performance_model="portfolio",
            sort="alpha_pct",
            min_trades=1,
            limit=50,
            db=db,
        )

        assert [row["member_id"] for row in default_leaderboard["rows"]] == ["GOOD1", "WARN1"]
        assert [row["portfolio_run_id"] for row in default_leaderboard["rows"]] == [good_run.id, warning_run.id]
        assert [row["curve_quality_status"] for row in default_leaderboard["rows"]] == ["good", "warning"]
        assert default_leaderboard["quality_filter_applied"] is True
        assert default_leaderboard["excluded_poor_quality_count"] == 1
        assert default_leaderboard["included_quality_statuses"] == ["good", "warning"]
        assert default_leaderboard["metadata"]["excluded_poor_quality_count"] == 1
        assert default_leaderboard["metadata"]["rows_returned"] == 2

        debug_leaderboard = congress_trader_leaderboard(
            request=request,
            lookback_days=1095,
            chamber="all",
            source_mode="congress",
            performance_model="portfolio",
            sort="alpha_pct",
            min_trades=1,
            limit=50,
            include_poor_quality=True,
            db=db,
        )

        assert [row["member_id"] for row in debug_leaderboard["rows"]] == ["POOR1", "GOOD1", "WARN1"]
        assert debug_leaderboard["rows"][0]["portfolio_run_id"] == poor_run.id
        assert [row["curve_quality_status"] for row in debug_leaderboard["rows"]] == ["poor", "good", "warning"]
        assert debug_leaderboard["quality_filter_applied"] is False
        assert debug_leaderboard["excluded_poor_quality_count"] == 0
        assert debug_leaderboard["included_quality_statuses"] == ["good", "warning", "poor"]
        assert debug_leaderboard["metadata"]["quality_filter_applied"] is False
    finally:
        db.close()


def test_congress_portfolio_leaderboard_allows_capped_estimated_opening_runs():
    db = _session()
    try:
        db.add_all(
            [
                CongressMemberAlias(alias_member_id="GOOD1", group_key="GOOD1", authoritative_member_id="GOOD1", member_name="Good Member", member_slug="GOOD1", chamber="house", party="DEMOCRAT", state="CA"),
                CongressMemberAlias(alias_member_id="SUSPECT1", group_key="SUSPECT1", authoritative_member_id="SUSPECT1", member_name="Suspect Member", member_slug="SUSPECT1", chamber="house", party="DEMOCRAT", state="CA"),
            ]
        )
        good_run = _add_portfolio_run(db, entity_id="GOOD1", total_return_pct=30.0, alpha_pct=20.0, sharpe_ratio=1.2)
        capped_run = _add_portfolio_run(db, entity_id="SUSPECT1", total_return_pct=35.0, alpha_pct=25.0, sharpe_ratio=1.4)
        capped_run.positions_count = 600
        capped_run.average_exposure_pct = 95.0
        capped_run.status_message = json.dumps(
            {
                "curve_diagnostics": {
                    "curve_quality_status": "warning",
                    "max_exposure_pct": 100.0,
                    "max_single_day_return_jump_pct": 18.0,
                },
                "effective_window": {"no_active_holdings": False},
                "warmup_diagnostics": {
                    "estimated_opening_positions_count": 120,
                    "estimated_opening_positions_value": 100_000.0,
                    "raw_estimated_opening_value": 2_000_000.0,
                    "scaled_estimated_opening_value": 100_000.0,
                    "estimated_opening_scale_factor": 0.05,
                    "estimated_opening_exposure_pct": 100.0,
                    "estimated_opening_method": "capped_pro_rata",
                    "estimated_opening_cap": 100_000.0,
                    "sale_without_position_after_estimation": 0,
                    "sale_without_position_after_warmup": 0,
                },
            }
        )
        db.commit()
        request = _premium_request(db)

        leaderboard = congress_trader_leaderboard(
            request=request,
            lookback_days=1095,
            chamber="all",
            source_mode="congress",
            performance_model="portfolio",
            sort="alpha_pct",
            min_trades=1,
            limit=50,
            db=db,
        )

        assert [row["member_id"] for row in leaderboard["rows"]] == ["SUSPECT1", "GOOD1"]
        assert leaderboard["rows"][0]["portfolio_run_id"] == capped_run.id
        assert leaderboard["rows"][1]["portfolio_run_id"] == good_run.id
        assert leaderboard["excluded_poor_quality_count"] == 0

        debug = congress_trader_leaderboard(
            request=request,
            lookback_days=1095,
            chamber="all",
            source_mode="congress",
            performance_model="portfolio",
            sort="alpha_pct",
            min_trades=1,
            limit=50,
            include_poor_quality=True,
            db=db,
        )

        assert debug["rows"][0]["portfolio_run_id"] == capped_run.id
        assert debug["rows"][0]["public_safety_flags"] == []
    finally:
        db.close()


def test_congress_portfolio_leaderboard_rejects_uncapped_estimated_opening_exposure():
    db = _session()
    try:
        db.add_all(
            [
                CongressMemberAlias(alias_member_id="GOOD1", group_key="GOOD1", authoritative_member_id="GOOD1", member_name="Good Member", member_slug="GOOD1", chamber="house", party="DEMOCRAT", state="CA"),
                CongressMemberAlias(alias_member_id="SUSPECT1", group_key="SUSPECT1", authoritative_member_id="SUSPECT1", member_name="Suspect Member", member_slug="SUSPECT1", chamber="house", party="DEMOCRAT", state="CA"),
            ]
        )
        good_run = _add_portfolio_run(db, entity_id="GOOD1", total_return_pct=30.0, alpha_pct=20.0, sharpe_ratio=1.2)
        suspect_run = _add_portfolio_run(db, entity_id="SUSPECT1", total_return_pct=3000.0, alpha_pct=2900.0, sharpe_ratio=1.4)
        suspect_run.average_exposure_pct = 400.0
        suspect_run.status_message = json.dumps(
            {
                "curve_diagnostics": {
                    "curve_quality_status": "warning",
                    "max_exposure_pct": 400.0,
                    "max_single_day_return_jump_pct": 300.0,
                },
                "effective_window": {"no_active_holdings": False},
                "warmup_diagnostics": {
                    "estimated_opening_positions_count": 12,
                    "estimated_opening_positions_value": 2_000_000.0,
                    "raw_estimated_opening_value": 2_000_000.0,
                    "scaled_estimated_opening_value": 2_000_000.0,
                    "estimated_opening_scale_factor": 1.0,
                    "estimated_opening_exposure_pct": 2000.0,
                    "estimated_opening_method": "legacy_uncapped",
                    "estimated_opening_cap": 100_000.0,
                },
            }
        )
        db.commit()
        request = _premium_request(db)

        leaderboard = congress_trader_leaderboard(
            request=request,
            lookback_days=1095,
            chamber="all",
            source_mode="congress",
            performance_model="portfolio",
            sort="alpha_pct",
            min_trades=1,
            limit=50,
            db=db,
        )

        assert [row["member_id"] for row in leaderboard["rows"]] == ["GOOD1"]
        assert leaderboard["rows"][0]["portfolio_run_id"] == good_run.id
        assert leaderboard["excluded_poor_quality_count"] == 1

        debug = congress_trader_leaderboard(
            request=request,
            lookback_days=1095,
            chamber="all",
            source_mode="congress",
            performance_model="portfolio",
            sort="alpha_pct",
            min_trades=1,
            limit=50,
            include_poor_quality=True,
            db=db,
        )

        assert debug["rows"][0]["portfolio_run_id"] == suspect_run.id
        assert "exposure_exceeds_cap" in debug["rows"][0]["public_safety_flags"]
        assert "single_day_return_jump_outlier" in debug["rows"][0]["public_safety_flags"]
    finally:
        db.close()


def test_congress_portfolio_leaderboard_filters_sale_without_position_runs():
    db = _session()
    try:
        db.add_all(
            [
                CongressMemberAlias(alias_member_id="GOOD1", group_key="GOOD1", authoritative_member_id="GOOD1", member_name="Good Member", member_slug="GOOD1", chamber="house", party="DEMOCRAT", state="CA"),
                CongressMemberAlias(alias_member_id="SUSPECT1", group_key="SUSPECT1", authoritative_member_id="SUSPECT1", member_name="Suspect Member", member_slug="SUSPECT1", chamber="house", party="DEMOCRAT", state="CA"),
            ]
        )
        good_run = _add_portfolio_run(db, entity_id="GOOD1", total_return_pct=12.0, alpha_pct=2.0, sharpe_ratio=1.0)
        suspect_run = _add_portfolio_run(db, entity_id="SUSPECT1", total_return_pct=20.0, alpha_pct=10.0, sharpe_ratio=1.5)
        suspect_run.status_message = json.dumps(
            {
                "curve_diagnostics": {
                    "curve_quality_status": "warning",
                    "curve_quality_notes": ["warning fixture"],
                    "data_coverage_notes": ["warning fixture"],
                },
                "warmup_diagnostics": {
                    "estimated_opening_positions_count": 3,
                    "estimated_opening_positions_value": 150000.0,
                    "sale_without_position_after_estimation": 2,
                    "sale_without_position_after_warmup": 2,
                },
            }
        )
        db.commit()

        request = _premium_request(db)
        leaderboard = congress_trader_leaderboard(
            request=request,
            lookback_days=1095,
            chamber="all",
            source_mode="congress",
            performance_model="portfolio",
            sort="alpha_pct",
            min_trades=1,
            limit=50,
            db=db,
        )

        assert [row["member_id"] for row in leaderboard["rows"]] == ["GOOD1"]
        assert leaderboard["rows"][0]["portfolio_run_id"] == good_run.id
        assert leaderboard["excluded_poor_quality_count"] == 1

        debug = congress_trader_leaderboard(
            request=request,
            lookback_days=1095,
            chamber="all",
            source_mode="congress",
            performance_model="portfolio",
            sort="alpha_pct",
            min_trades=1,
            limit=50,
            include_poor_quality=True,
            db=db,
        )

        assert debug["rows"][0]["portfolio_run_id"] == suspect_run.id
        assert "sale_without_position_after_estimation" in debug["rows"][0]["public_safety_flags"]
    finally:
        db.close()


def test_congress_portfolio_leaderboard_returns_empty_when_all_runs_are_poor_quality():
    db = _session()
    try:
        db.add(
            CongressMemberAlias(alias_member_id="POOR_ONLY", group_key="POOR_ONLY", authoritative_member_id="POOR_ONLY", member_name="Poor Only", member_slug="POOR_ONLY", chamber="house", party="REPUBLICAN", state="TX")
        )
        _add_portfolio_run(db, entity_id="POOR_ONLY", total_return_pct=100.0, alpha_pct=90.0, sharpe_ratio=3.0, curve_quality_status="poor")
        db.commit()

        leaderboard = congress_trader_leaderboard(
            request=_premium_request(db),
            lookback_days=1095,
            chamber="all",
            source_mode="congress",
            performance_model="portfolio",
            sort="alpha_pct",
            min_trades=1,
            limit=50,
            db=db,
        )

        assert leaderboard["rows"] == []
        assert leaderboard["status"] == "portfolio_runs_not_populated"
        assert leaderboard["quality_filter_applied"] is True
        assert leaderboard["excluded_poor_quality_count"] == 1
        assert leaderboard["metadata"]["rows_returned"] == 0
        assert leaderboard["metadata"]["excluded_poor_quality_count"] == 1
    finally:
        db.close()


def test_congress_portfolio_leaderboard_dedupes_canonical_and_fmp_alias_runs():
    db = _session()
    try:
        db.add(Member(bioguide_id="G000599", first_name="Daniel", last_name="Goldman", chamber="house", party="D", state="NY"))
        db.add_all(
            [
                CongressMemberAlias(alias_member_id="G000599", group_key="G000599", authoritative_member_id="G000599", member_name="Daniel Goldman", member_slug="G000599", chamber="house", party="DEMOCRAT", state="NY"),
                CongressMemberAlias(alias_member_id="FMP_HOUSE_NY10", group_key="G000599", authoritative_member_id="G000599", member_name="Daniel Goldman", member_slug="G000599", chamber="house", party="DEMOCRAT", state="NY"),
            ]
        )
        older = datetime(2026, 1, 1, tzinfo=timezone.utc)
        newer = datetime(2026, 1, 2, tzinfo=timezone.utc)
        canonical_run = _add_portfolio_run(db, entity_id="G000599", total_return_pct=10.0, alpha_pct=5.0, sharpe_ratio=0.8, created_at=older)
        fmp_run = _add_portfolio_run(db, entity_id="FMP_HOUSE_NY10", total_return_pct=99.0, alpha_pct=88.0, sharpe_ratio=3.0, created_at=newer)
        db.commit()

        leaderboard = congress_trader_leaderboard(
            request=_premium_request(db),
            lookback_days=1095,
            chamber="all",
            source_mode="congress",
            performance_model="portfolio",
            sort="total_return_pct",
            min_trades=1,
            limit=50,
            db=db,
        )

        assert len(leaderboard["rows"]) == 1
        row = leaderboard["rows"][0]
        assert row["member_id"] == "G000599"
        assert row["bioguide_id"] == "G000599"
        assert row["portfolio_entity_id"] == "G000599"
        assert row["portfolio_run_id"] == canonical_run.id
        assert row["portfolio_run_id"] != fmp_run.id
        assert row["member_name"] == "Daniel Goldman"
    finally:
        db.close()


def test_congress_portfolio_leaderboard_ignores_orphaned_fmp_comma_fragments():
    db = _session()
    try:
        db.add_all(
            [
                Member(bioguide_id="J000312", first_name="James", last_name="Justice II", chamber="senate", party="R", state="WV"),
                Member(bioguide_id="M001242", first_name="Bernie", last_name="Moreno", chamber="senate", party="R", state="OH"),
            ]
        )
        db.add_all(
            [
                CongressMemberAlias(alias_member_id="J000312", group_key="J000312", authoritative_member_id="J000312", member_name="James Justice II", member_slug="J000312", chamber="senate", party="REPUBLICAN", state="WV"),
                CongressMemberAlias(alias_member_id="M001242", group_key="M001242", authoritative_member_id="M001242", member_name="Bernie Moreno", member_slug="M001242", chamber="senate", party="REPUBLICAN", state="OH"),
            ]
        )
        _add_portfolio_run(db, entity_id="FMP_SENATE_XX_JUSTICE_II", total_return_pct=900.0, alpha_pct=900.0, sharpe_ratio=9.0)
        _add_portfolio_run(db, entity_id="__JAMES_CONLEY_(SENATOR)", total_return_pct=800.0, alpha_pct=800.0, sharpe_ratio=8.0)
        _add_portfolio_run(db, entity_id="FMP_SENATE_XX_MORENO", total_return_pct=700.0, alpha_pct=700.0, sharpe_ratio=7.0)
        _add_portfolio_run(db, entity_id="_BERNARDO_(SENATOR)", total_return_pct=600.0, alpha_pct=600.0, sharpe_ratio=6.0)
        justice_run = _add_portfolio_run(db, entity_id="J000312", total_return_pct=10.0, alpha_pct=10.0, sharpe_ratio=1.0)
        moreno_run = _add_portfolio_run(db, entity_id="M001242", total_return_pct=5.0, alpha_pct=5.0, sharpe_ratio=0.8)
        db.commit()

        request = _premium_request(db)
        public = congress_trader_leaderboard(
            request=request,
            lookback_days=1095,
            chamber="all",
            source_mode="congress",
            performance_model="portfolio",
            sort="alpha_pct",
            min_trades=1,
            limit=50,
            db=db,
        )
        debug = congress_trader_leaderboard(
            request=request,
            lookback_days=1095,
            chamber="all",
            source_mode="congress",
            performance_model="portfolio",
            sort="alpha_pct",
            min_trades=1,
            limit=50,
            include_poor_quality=True,
            db=db,
        )

        fragment_ids = {
            "FMP_SENATE_XX_JUSTICE_II",
            "__JAMES_CONLEY_(SENATOR)",
            "FMP_SENATE_XX_MORENO",
            "_BERNARDO_(SENATOR)",
        }
        for leaderboard in (public, debug):
            assert not fragment_ids.intersection(row["portfolio_entity_id"] for row in leaderboard["rows"])
            assert [row["member_id"] for row in leaderboard["rows"]] == ["J000312", "M001242"]
            assert [row["portfolio_run_id"] for row in leaderboard["rows"]] == [justice_run.id, moreno_run.id]
    finally:
        db.close()
