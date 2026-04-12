from datetime import date, datetime, timezone
import json

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.db import Base
from app.main import feed
from app.models import Event, Security, TradeOutcome
from app.routers.events import _insider_trade_row, insider_trades, list_events, list_ticker_events
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


def _model_dict(value):
    return value.model_dump() if hasattr(value, "model_dump") else value.dict()


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
    assert round(row["trade_value"], 0) == round(row["display_price"] * 9000, 0)
    assert row["pnl_pct"] is None


def test_feed_insider_trade_uses_same_normalized_display_basis_as_insider_row(monkeypatch):
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

    def fake_quotes(db, symbols, **kwargs):
        return {symbol: 24.84 for symbol in symbols}

    monkeypatch.setattr("app.main.get_current_prices_db", fake_quotes)

    with Session(engine) as db:
        db.add(Security(symbol="ASX", name="ASE Technology Holding Co.", asset_class="equity", sector="Technology"))
        db.add(event)
        db.commit()

        response = feed(db=db, tape="insider", limit=10)

    item = response["items"][0]
    canonical = _insider_trade_row(event, _asx_payload(), outcome=None, fallback_pnl_pct=-4.54, prefer_fallback_pnl=True)

    assert item["security"]["name"] == "ASE Technology Holding Co."
    assert round(item["estimated_price"], 2) == round(canonical["display_price"], 2) == 23.76
    assert item["payload"]["reported_price"] == canonical["reported_price"] == 387.0
    assert item["payload"]["reported_price_currency"] == "TWD"
    assert round(item["amount_range_max"], 0) == round(canonical["trade_value"], 0)
    assert round(item["pnl_pct"], 2) == -4.54
    assert item["pnl_source"] == "normalized_filing"


def test_same_foreign_insider_trade_matches_feed_ticker_and_insider_surfaces(monkeypatch):
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

    def fake_feed_quotes(db, symbols, **kwargs):
        return {symbol: 24.84 for symbol in symbols}

    def fake_quote_meta(db, symbols, **kwargs):
        return {
            symbol: {
                "price": 24.84,
                "asof_ts": datetime(2026, 4, 11, tzinfo=timezone.utc),
                "is_stale": False,
            }
            for symbol in symbols
        }

    monkeypatch.setattr("app.main.get_current_prices_db", fake_feed_quotes)
    monkeypatch.setattr("app.routers.events.get_current_prices_meta_db", fake_quote_meta)

    with Session(engine) as db:
        db.add(Security(symbol="ASX", name="ASE Technology Holding Co.", asset_class="equity", sector="Technology"))
        db.add(event)
        db.commit()

        feed_item = feed(db=db, tape="insider", limit=10)["items"][0]
        ticker_item = _model_dict(list_ticker_events(symbol="ASX", db=db, limit=10).items[0])
        events_item = _model_dict(
            list_events(
                db=db,
                symbol="ASX",
                event_type="insider_trade",
                recent_days=None,
                min_amount=None,
                max_amount=None,
                limit=10,
                offset=0,
                include_total=False,
                enrich_prices=True,
            ).items[0]
        )
        insider_item = insider_trades("0002111679", db=db, lookback_days=90, limit=10)["items"][0]

    assert feed_item["security"]["name"] == "ASE Technology Holding Co."
    assert events_item["payload"]["company_name"] == "ASE Technology Holding Co."
    assert ticker_item["payload"]["company_name"] == "ASE Technology Holding Co."
    assert insider_item["company_name"] == "ASE Technology Holding Co."

    assert round(feed_item["estimated_price"], 2) == round(insider_item["display_price"], 2) == 23.76
    assert round(events_item["estimated_price"], 2) == round(ticker_item["estimated_price"], 2) == 23.76
    assert round(feed_item["amount_range_max"], 0) == round(insider_item["trade_value"], 0) == round(insider_item["display_price"] * 9000, 0)
    assert round(events_item["amount_max"], 0) == round(ticker_item["amount_max"], 0) == round(insider_item["display_price"] * 9000, 0)
    assert feed_item["payload"]["reported_price"] == events_item["payload"]["reported_price"] == insider_item["reported_price"] == 387.0
    assert feed_item["payload"]["reported_price_currency"] == events_item["payload"]["reported_price_currency"] == insider_item["reported_price_currency"] == "TWD"
    assert round(feed_item["pnl_pct"], 2) == round(events_item["pnl_pct"], 2) == round(ticker_item["pnl_pct"], 2) == round(insider_item["pnl_pct"], 2) == -4.54
    assert feed_item["pnl_source"] == events_item["pnl_source"] == ticker_item["pnl_source"] == insider_item["pnl_source"] == "normalized_filing"


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
