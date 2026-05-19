from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.db import Base
from app.models import Event, GovernmentContractAction, TradeOutcome
from app.routers.events import list_events


def _db() -> Session:
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    return Session(engine)


def _stub_enrichment(monkeypatch) -> None:
    monkeypatch.setattr("app.routers.events.get_current_prices_meta_db", lambda *_args, **_kwargs: {})
    monkeypatch.setattr("app.routers.events.get_eod_close", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("app.routers.events.get_confirmation_metrics_for_symbols", lambda *_args, **_kwargs: {})
    monkeypatch.setattr("app.routers.events._ticker_meta_with_security_names", lambda *_args, **_kwargs: {})
    monkeypatch.setattr("app.routers.events.get_cik_meta", lambda *_args, **_kwargs: {})


def _event(event_id: int, event_type: str, **kwargs) -> Event:
    now = kwargs.pop("ts", datetime(2026, 5, 19, tzinfo=timezone.utc))
    return Event(
        id=event_id,
        event_type=event_type,
        ts=now,
        event_date=kwargs.pop("event_date", now),
        source=kwargs.pop("source", "test"),
        payload_json=json.dumps(kwargs.pop("payload", {})),
        **kwargs,
    )


def test_department_filter_in_all_mode_returns_only_matching_contract_events(monkeypatch):
    db = _db()
    try:
        _stub_enrichment(monkeypatch)
        contract = _event(
            1,
            "government_contract",
            symbol="LMT",
            member_name="Department of Defense",
            payload={
                "awarding_agency": "Department of Defense",
                "recipient_name": "Lockheed Martin",
                "event_subtype": "funding_action",
            },
            amount_min=1_000_000,
            amount_max=1_000_000,
        )
        db.add_all(
            [
                contract,
                GovernmentContractAction(
                    parent_award_id="A-1",
                    dedupe_key="A-1-P1",
                    event_id=1,
                    symbol="LMT",
                    awarding_agency="Department of Defense",
                    recipient_name="Lockheed Martin",
                    action_date=datetime(2026, 5, 19, tzinfo=timezone.utc).date(),
                    obligated_amount=1_000_000,
                ),
                _event(2, "insider_trade", symbol="AAPL", member_name="Tim Cook", payload={"reporting_cik": "0001214156"}),
                _event(3, "congress_trade", symbol="MSFT", member_name="Nancy Pelosi", member_bioguide_id="P000197"),
            ]
        )
        db.commit()

        page = list_events(db=db, mode="all", department="Department of Defense", limit=10, enrich_prices=False)

        assert [item.id for item in page.items] == [1]
        assert {item.event_type for item in page.items} == {"government_contract"}
    finally:
        db.close()


def test_insider_name_and_role_filters_work_in_all_and_insider_modes(monkeypatch):
    db = _db()
    try:
        _stub_enrichment(monkeypatch)
        db.add_all(
            [
                _event(
                    10,
                    "insider_trade",
                    symbol="AAPL",
                    member_name=None,
                    trade_type="purchase",
                    amount_max=5_000_000,
                    payload={
                        "insider_name": "Tim Cook",
                        "reporting_cik": "0001214156",
                        "company_name": "Apple Inc.",
                        "officerTitle": "Chief Executive Officer",
                    },
                ),
                _event(
                    11,
                    "insider_trade",
                    symbol="AAPL",
                    member_name="Arthur Levinson",
                    trade_type="sale",
                    amount_max=1_000_000,
                    payload={"reporting_cik": "0001111111", "role": "Director"},
                ),
            ]
        )
        db.commit()

        insider_page = list_events(db=db, mode="insider", member="Tim Cook", limit=10, enrich_prices=False)
        all_page = list_events(db=db, mode="all", member="Tim Cook", limit=10, enrich_prices=False)
        role_page = list_events(db=db, mode="all", role="CEO", limit=10, enrich_prices=False)

        assert [item.id for item in insider_page.items] == [10]
        assert [item.id for item in all_page.items] == [10]
        assert [item.id for item in role_page.items] == [10]
    finally:
        db.close()


def test_asset_class_filters_cover_public_equity_treasury_crypto_and_other(monkeypatch):
    db = _db()
    try:
        _stub_enrichment(monkeypatch)
        db.add_all(
            [
                _event(20, "congress_trade", symbol="MSFT", member_name="Member", member_bioguide_id="M1", trade_type="purchase", payload={"asset_class": "equity"}),
                _event(21, "insider_trade", symbol="AAPL", member_name="Tim Cook", trade_type="purchase", payload={"reporting_cik": "0001214156"}),
                _event(22, "congress_treasury_trade", symbol=None, member_name="Member", member_bioguide_id="M1", payload={"asset_class": "treasury"}),
                _event(23, "congress_crypto_trade", symbol=None, member_name="Member", member_bioguide_id="M1", payload={"asset_class": "crypto"}),
                _event(24, "congress_trade", symbol=None, member_name="Member", member_bioguide_id="M1", payload={"asset_class": "other"}),
                _event(25, "congress_trade", symbol="IBIT", member_name="Member", member_bioguide_id="M1", payload={"asset_class": "etf", "security_name": "iShares Bitcoin Trust ETF"}),
            ]
        )
        db.commit()

        equities = list_events(db=db, mode="all", asset_class="equity", limit=10, enrich_prices=False)
        treasuries = list_events(db=db, mode="congress", asset_class="treasury", limit=10, enrich_prices=False)
        crypto = list_events(db=db, mode="congress", asset_class="crypto", limit=10, enrich_prices=False)
        other = list_events(db=db, mode="congress", asset_class="other", limit=10, enrich_prices=False)
        etf_fund = list_events(db=db, mode="congress", asset_class="etf_fund", limit=10, enrich_prices=False)

        assert [item.id for item in equities.items] == [21, 20]
        assert [item.id for item in treasuries.items] == [22]
        assert [item.id for item in crypto.items] == [23]
        assert [item.id for item in other.items] == [24]
        assert [item.id for item in etf_fund.items] == [25]
    finally:
        db.close()


def test_filed_after_pnl_and_signal_filters(monkeypatch):
    db = _db()
    try:
        _stub_enrichment(monkeypatch)
        db.add_all(
            [
                _event(
                    30,
                    "congress_trade",
                    symbol="MSFT",
                    member_name="Member",
                    member_bioguide_id="M1",
                    trade_type="purchase",
                    payload={
                        "trade_date": "2026-05-01",
                        "report_date": "2026-05-10",
                        "smart_score": 82,
                    },
                ),
                _event(
                    31,
                    "congress_trade",
                    symbol="TSLA",
                    member_name="Member",
                    member_bioguide_id="M1",
                    trade_type="purchase",
                    payload={
                        "trade_date": "2026-01-01",
                        "report_date": "2026-05-10",
                        "smart_score": 45,
                    },
                ),
                TradeOutcome(
                    event_id=30,
                    member_id="M1",
                    member_name="Member",
                    symbol="MSFT",
                    trade_type="purchase",
                    trade_date=datetime(2026, 5, 1, tzinfo=timezone.utc).date(),
                    return_pct=12.5,
                    benchmark_symbol="^GSPC",
                    scoring_status="ok",
                    methodology_version="congress_v1",
                ),
                TradeOutcome(
                    event_id=31,
                    member_id="M1",
                    member_name="Member",
                    symbol="TSLA",
                    trade_type="purchase",
                    trade_date=datetime(2026, 1, 1, tzinfo=timezone.utc).date(),
                    return_pct=-3.0,
                    benchmark_symbol="^GSPC",
                    scoring_status="ok",
                    methodology_version="congress_v1",
                ),
            ]
        )
        db.commit()

        filed = list_events(db=db, mode="congress", filed_after_max=30, limit=10, enrich_prices=False)
        pnl = list_events(db=db, mode="congress", pnl_min=10, limit=10, enrich_prices=True)
        pnl_max = list_events(db=db, mode="congress", pnl_max=0, limit=10, enrich_prices=True)
        signal = list_events(db=db, mode="congress", signal_min=70, limit=10, enrich_prices=False)

        assert [item.id for item in filed.items] == [30]
        assert [item.id for item in pnl.items] == [30]
        assert [item.id for item in pnl_max.items] == [31]
        assert [item.id for item in signal.items] == [30]
    finally:
        db.close()


def test_feed_events_expose_actor_and_ticker_net_30d(monkeypatch):
    db = _db()
    try:
        _stub_enrichment(monkeypatch)
        now = datetime.now(timezone.utc)
        db.add_all(
            [
                _event(40, "congress_trade", ts=now, symbol="MSFT", member_name="Member", member_bioguide_id="M1", trade_type="purchase", amount_max=20_000),
                _event(41, "congress_trade", ts=now - timedelta(days=1), symbol="MSFT", member_name="Other", member_bioguide_id="M2", trade_type="sale", amount_max=5_000),
                _event(42, "insider_trade", ts=now, symbol="AAPL", member_name="Tim Cook", trade_type="purchase", amount_max=10_000, payload={"reporting_cik": "0001214156"}),
                _event(43, "insider_trade", ts=now - timedelta(days=1), symbol="AAPL", member_name="Other Insider", trade_type="sale", amount_max=3_000, payload={"reporting_cik": "0009999999"}),
            ]
        )
        db.commit()

        page = list_events(db=db, mode="all", limit=10, enrich_prices=False)
        by_id = {item.id: item for item in page.items}

        assert by_id[40].member_net_30d == 20_000
        assert by_id[40].symbol_net_30d == 15_000
        assert by_id[42].member_net_30d == 10_000
        assert by_id[42].symbol_net_30d == 7_000
    finally:
        db.close()
