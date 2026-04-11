from datetime import date, datetime, timezone
import json

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.db import Base
from app.models import Event
from app.routers.events import _insider_trade_row
from app.services.foreign_trade_normalization import normalize_insider_price
from app.services.member_performance import compute_insider_trade_outcomes


def _asx_payload(price: float = 387.0) -> dict:
    return {
        "symbol": "ASX",
        "reporting_cik": "0002111679",
        "insider_name": "Chen Jeffrey",
        "transaction_date": "2026-04-10",
        "trade_type_canonical": "sale",
        "is_market_trade": True,
        "shares": 9000,
        "price": price,
        "raw": {
            "symbol": "ASX",
            "securityName": "Ordinary Shares",
            "transactionDate": "2026-04-10",
            "transactionType": "S-Sale",
            "price": price,
            "securitiesTransacted": 9000,
            "reportingCik": "0002111679",
            "reportingName": "Chen Jeffrey",
        },
    }


def test_asx_filing_price_normalizes_to_usd_adr_basis():
    normalized = normalize_insider_price(symbol="ASX", payload=_asx_payload(), trade_date="2026-04-10")

    assert normalized.status == "normalized"
    assert normalized.raw_price == 387.0
    assert normalized.raw_currency == "TWD"
    assert normalized.display_currency == "USD"
    assert normalized.ordinary_shares_per_adr == 2.0
    assert round(normalized.display_price or 0, 2) == 23.76


def test_standard_us_insider_trade_stays_on_same_basis():
    payload = {"symbol": "AAPL", "transaction_date": "2026-04-10", "price": 190.25}
    normalized = normalize_insider_price(symbol="AAPL", payload=payload, trade_date="2026-04-10")

    assert normalized.status == "same_basis"
    assert normalized.raw_price == 190.25
    assert normalized.display_price == 190.25
    assert normalized.raw_currency == "USD"


def test_insider_trade_row_preserves_reported_price_under_normalized_display():
    event = Event(
        id=122139,
        event_type="insider_trade",
        ts=datetime(2026, 4, 10, tzinfo=timezone.utc),
        event_date=datetime(2026, 4, 10, tzinfo=timezone.utc),
        symbol="ASX",
        source="fmp",
        trade_type="sale",
        amount_min=3483000,
        amount_max=3483000,
        payload_json=json.dumps(_asx_payload()),
    )

    row = _insider_trade_row(event, _asx_payload(), outcome=None)

    assert round(row["price"], 2) == 23.76
    assert row["reported_price"] == 387.0
    assert row["reported_price_currency"] == "TWD"
    assert round(row["trade_value"], 0) == 213886
    assert row["pnl_pct"] is None


def test_insider_trade_row_can_prefer_live_normalized_pnl_over_stale_outcome():
    event = Event(
        id=122139,
        event_type="insider_trade",
        ts=datetime(2026, 4, 10, tzinfo=timezone.utc),
        event_date=datetime(2026, 4, 10, tzinfo=timezone.utc),
        symbol="ASX",
        source="fmp",
        trade_type="sale",
        amount_min=3483000,
        amount_max=3483000,
        payload_json=json.dumps(_asx_payload()),
    )
    stale_outcome = TradeOutcome(
        event_id=122139,
        symbol="ASX",
        trade_type="sale",
        trade_date=date(2026, 4, 10),
        benchmark_symbol="^GSPC",
        return_pct=93.5,
        alpha_pct=90.0,
        scoring_status="ok",
        methodology_version="insider_v1",
    )

    row = _insider_trade_row(
        event,
        _asx_payload(),
        outcome=stale_outcome,
        fallback_pnl_pct=-4.54,
        prefer_fallback_pnl=True,
    )

    assert row["pnl_pct"] == -4.54
    assert row["pnl_source"] == "normalized_filing"


def test_normalized_foreign_trade_outcome_uses_corrected_entry_price(monkeypatch):
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)

    event = Event(
        id=122139,
        event_type="insider_trade",
        ts=datetime(2026, 4, 10, tzinfo=timezone.utc),
        event_date=datetime(2026, 4, 10, tzinfo=timezone.utc),
        symbol="ASX",
        source="fmp",
        trade_type="sale",
        transaction_type="S-Sale",
        amount_min=3483000,
        amount_max=3483000,
        payload_json=json.dumps(_asx_payload()),
    )

    def fake_entry(db, symbol, trade_date, price_memo):
        return {"close": 24.0, "status": "ok", "error": None, "symbol": symbol}

    def fake_quotes(db, symbols, **kwargs):
        return {symbol: {"price": 24.84, "asof_ts": datetime(2026, 4, 11, tzinfo=timezone.utc)} for symbol in symbols}

    def fake_benchmark(*args, **kwargs):
        return 5000.0

    monkeypatch.setattr("app.services.member_performance._entry_price_for_congress_event", fake_entry)
    monkeypatch.setattr("app.services.member_performance.get_current_prices_meta_db", fake_quotes)
    monkeypatch.setattr("app.services.member_performance._benchmark_entry_close_for_trade_date", fake_benchmark)

    with Session(engine) as db:
        rows = compute_insider_trade_outcomes(db, [event], "^GSPC")

    row = rows[0]
    assert row["scoring_status"] == "ok"
    assert round(row["entry_price"], 2) == 23.76
    assert round(row["return_pct"], 2) == -4.54
