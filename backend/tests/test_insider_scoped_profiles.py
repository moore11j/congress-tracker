from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.db import Base
from app.models import Event, TradeOutcome
from app.routers import events as events_router


def _db() -> Session:
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    return Session(engine)


def test_tim_cook_nke_scoped_profile_uses_one_outcome_set(monkeypatch):
    db = _db()
    try:
        monkeypatch.setattr(events_router, "get_current_prices_meta_db", lambda *_args, **_kwargs: {})
        monkeypatch.setattr(events_router, "get_eod_close", lambda *_args, **_kwargs: None)
        monkeypatch.setattr(events_router, "get_eod_close_series", lambda *_args, **_kwargs: {})
        monkeypatch.setattr(events_router, "load_profile_price_close_maps", lambda *_args, **_kwargs: {})
        monkeypatch.setattr(events_router, "_ticker_meta_with_security_names", lambda *_args, **_kwargs: {})
        monkeypatch.setattr(events_router, "get_cik_meta", lambda *_args, **_kwargs: {})

        cik = "0001214156"
        now = datetime.now(timezone.utc)
        trade_day = (now - timedelta(days=20)).date()
        nike_event = Event(
            id=101,
            event_type="insider_trade",
            ts=now,
            event_date=now,
            symbol="NKE",
            source="sec_form4",
            member_name="Timothy D. Cook",
            trade_type="purchase",
            amount_min=1000,
            amount_max=5000,
            payload_json=json.dumps(
                {
                    "reporting_cik": cik,
                    "insider_name": "Timothy D. Cook",
                    "symbol": "NKE",
                    "company_name": "Nike Inc.",
                    "role": "Director",
                    "transaction_date": trade_day.isoformat(),
                    "price": 75.0,
                }
            ),
        )
        apple_event = Event(
            id=102,
            event_type="insider_trade",
            ts=now - timedelta(days=1),
            event_date=now - timedelta(days=1),
            symbol="AAPL",
            source="sec_form4",
            member_name="Timothy D. Cook",
            trade_type="sale",
            amount_min=1000,
            amount_max=5000,
            payload_json=json.dumps(
                {
                    "reporting_cik": cik,
                    "insider_name": "Timothy D. Cook",
                    "symbol": "AAPL",
                    "company_name": "Apple Inc.",
                    "role": "CEO",
                    "transaction_date": trade_day.isoformat(),
                    "price": 100.0,
                }
            ),
        )
        db.add_all([nike_event, apple_event])
        db.add_all(
            [
                TradeOutcome(
                    event_id=101,
                    member_id=cik,
                    member_name="Timothy D. Cook",
                    symbol="NKE",
                    trade_type="purchase",
                    source="insider",
                    trade_date=trade_day,
                    entry_price=75.0,
                    current_price=99.9,
                    benchmark_symbol="^GSPC",
                    benchmark_return_pct=8.5,
                    return_pct=33.2,
                    alpha_pct=24.7,
                    holding_days=30,
                    amount_min=1000,
                    amount_max=5000,
                    scoring_status="ok",
                    methodology_version="insider_v1",
                    computed_at=now,
                ),
                TradeOutcome(
                    event_id=102,
                    member_id=cik,
                    member_name="Timothy D. Cook",
                    symbol="AAPL",
                    trade_type="sale",
                    source="insider",
                    trade_date=trade_day,
                    entry_price=100.0,
                    current_price=90.0,
                    benchmark_symbol="^GSPC",
                    return_pct=10.0,
                    alpha_pct=2.0,
                    holding_days=30,
                    amount_min=1000,
                    amount_max=5000,
                    scoring_status="ok",
                    methodology_version="insider_v1",
                    computed_at=now,
                ),
            ]
        )
        db.commit()

        trades = events_router.insider_trades(cik, db=db, lookback_days=90, limit=50, issuer="NKE")
        alpha = events_router.insider_alpha_summary(cik, db=db, lookback_days=90, issuer="NKE")
        summary = events_router.insider_summary(cik, db=db, lookback_days=90, issuer="NKE")

        assert [item["symbol"] for item in trades["items"]] == ["NKE"]
        assert trades["items"][0]["pnl_pct"] == 33.2
        assert trades["items"][0]["alpha_pct"] == 24.7
        assert trades["items"][0]["outcome_horizon"] == "30D Return"
        assert alpha["trades_analyzed"] == 1
        assert alpha["best_trades"][0]["event_id"] == 101
        assert alpha["best_trades"][0]["return_pct"] == trades["items"][0]["pnl_pct"]
        assert alpha["worst_trades"] == []
        assert {point["symbol"] for point in alpha["member_series"] if point["symbol"]} <= {"NKE"}
        assert summary["primary_symbol"] == "NKE"
        assert summary["primary_company_name"] == "Nike Inc."
        assert summary["primary_role"] == "Director"
    finally:
        db.close()
