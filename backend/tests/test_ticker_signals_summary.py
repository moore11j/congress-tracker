from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

import app.main as main_module
from app.db import Base
from app.main import _ticker_profiles_response, ticker_signals_summary
from app.models import Event, GovernmentContract, TickerMeta


def _engine():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    return engine


def _mock_signal_auth(monkeypatch):
    monkeypatch.setattr(main_module, "current_user", lambda *args, **kwargs: object())
    monkeypatch.setattr(main_module, "current_entitlements", lambda *args, **kwargs: {})
    monkeypatch.setattr(main_module, "require_feature", lambda *args, **kwargs: None)
    main_module._TICKER_SIGNALS_SUMMARY_CACHE.clear()


def _event(
    event_id: int,
    *,
    symbol: str,
    event_type: str,
    trade_type: str,
    days_ago: int = 1,
    amount: int = 25_000,
    member_name: str = "Example Actor",
) -> Event:
    ts = datetime.now(timezone.utc) - timedelta(days=days_ago)
    payload = {"symbol": symbol, "reporting_cik": f"{event_id:010d}", "insider_name": member_name}
    return Event(
        id=event_id,
        event_type=event_type,
        ts=ts,
        event_date=ts,
        symbol=symbol,
        source="test",
        trade_type=trade_type,
        amount_min=amount,
        amount_max=amount,
        member_name=member_name,
        member_bioguide_id=f"M{event_id}" if event_type == "congress_trade" else None,
        payload_json=json.dumps(payload),
    )


def test_ticker_signals_summary_uses_bounded_symbol_query(monkeypatch):
    captured: dict[str, object] = {}

    def fake_query(**kwargs):
        captured.update(kwargs)
        return [
            SimpleNamespace(
                model_dump=lambda mode="json": {
                    "symbol": kwargs["symbol"],
                    "ts": "2026-06-12T12:00:00Z",
                    "smart_score": 82,
                }
            )
        ]

    _mock_signal_auth(monkeypatch)
    monkeypatch.setattr(main_module, "_query_unified_signals", fake_query)
    monkeypatch.setattr(
        main_module,
        "_ticker_price_volume_summary",
        lambda db, symbol: {"status": "limited", "summary": "Limited price history", "score": None, "lines": ["Limited price history"]},
    )
    monkeypatch.setattr(
        main_module,
        "_ticker_trade_activity_summary",
        lambda *args, **kwargs: {
            "status": "inactive",
            "direction": "neutral",
            "title": "No recent activity",
            "subtitle": "No matching trades.",
            "buy_count": 0,
            "sell_count": 0,
            "net_flow": None,
        },
    )
    monkeypatch.setattr(
        main_module,
        "get_government_contracts_summary",
        lambda *args, **kwargs: {
            "status": "ok",
            "active": False,
            "contract_count": 0,
            "total_award_amount": 0,
            "detail": "No contracts above threshold in selected window.",
        },
    )

    response = ticker_signals_summary(object(), "nbis", side="buy", limit=3, lookback_days=30, db=object())

    assert captured["symbol"] == "NBIS"
    assert captured["limit"] == 3
    assert captured["side"] == "buy"
    assert captured["congress_recent_days"] == 30
    assert captured["insider_recent_days"] == 30
    assert response["symbol"] == "NBIS"
    assert response["latest_signal_score"] == 82
    assert response["recent_signal_count"] == 1
    assert response["items"][0]["symbol"] == "NBIS"
    assert response["price_volume"]["status"] == "limited"
    assert response["price_volume"]["title"] == "Limited price history"


def test_ticker_signals_summary_returns_congress_activity_when_events_exist(monkeypatch):
    _mock_signal_auth(monkeypatch)
    monkeypatch.setattr(main_module, "_query_unified_signals", lambda **kwargs: [])
    monkeypatch.setattr(
        main_module,
        "_ticker_price_volume_summary",
        lambda db, symbol: {"status": "inactive", "summary": "No active tape confirmation", "score": 0, "lines": ["RSI near neutral"], "direction": "neutral"},
    )

    engine = _engine()
    with Session(engine) as db:
        db.add(_event(1, symbol="AAPL", event_type="congress_trade", trade_type="purchase", amount=50_000, member_name="Rep One"))
        db.add(_event(2, symbol="AAPL", event_type="congress_trade", trade_type="sale", amount=10_000, member_name="Rep Two"))
        db.add(GovernmentContract(symbol="MSFT", award_date=date.today(), award_amount=2_000_000, source="test"))
        db.commit()

        response = ticker_signals_summary(object(), "AAPL", side="all", limit=3, lookback_days=30, db=db)

    assert response["congress"]["status"] == "active"
    assert response["congress"]["buy_count"] == 1
    assert response["congress"]["sell_count"] == 1
    assert response["congress"]["title"] != "No recent Congress trades"
    assert response["insiders"]["status"] == "inactive"


def test_ticker_signals_summary_returns_nbis_insider_sell_activity(monkeypatch):
    _mock_signal_auth(monkeypatch)
    monkeypatch.setattr(main_module, "_query_unified_signals", lambda **kwargs: [])
    monkeypatch.setattr(
        main_module,
        "_ticker_price_volume_summary",
        lambda db, symbol: {"status": "inactive", "summary": "No active tape confirmation", "score": 0, "lines": ["RSI near neutral"], "direction": "neutral"},
    )

    engine = _engine()
    with Session(engine) as db:
        db.add(_event(10, symbol="NBIS", event_type="insider_trade", trade_type="sale", amount=125_000, member_name="NBIS Insider"))
        db.add(GovernmentContract(symbol="MSFT", award_date=date.today(), award_amount=2_000_000, source="test"))
        db.commit()

        response = ticker_signals_summary(object(), "NBIS", side="all", limit=3, lookback_days=30, db=db)

    assert response["insiders"]["status"] == "active"
    assert response["insiders"]["direction"] == "bearish"
    assert response["insiders"]["sell_count"] == 1
    assert response["insiders"]["buy_count"] == 0
    assert response["insiders"]["net_flow"] == -125_000


def test_ticker_signals_summary_inactive_government_contracts_when_dataset_has_no_symbol_rows(monkeypatch):
    _mock_signal_auth(monkeypatch)
    monkeypatch.setattr(main_module, "_query_unified_signals", lambda **kwargs: [])
    monkeypatch.setattr(
        main_module,
        "_ticker_price_volume_summary",
        lambda db, symbol: {"status": "inactive", "summary": "No active tape confirmation", "score": 0, "lines": ["RSI near neutral"], "direction": "neutral"},
    )

    engine = _engine()
    with Session(engine) as db:
        db.add(GovernmentContract(symbol="MSFT", award_date=date.today(), award_amount=2_000_000, source="test"))
        db.commit()

        response = ticker_signals_summary(object(), "AAPL", side="all", limit=3, lookback_days=30, db=db)

    assert response["government_contracts"]["status"] == "inactive"
    assert response["government_contracts"]["title"] == "No major government contracts"
    assert response["government_contracts"]["contract_count"] == 0


def test_ticker_profiles_batch_returns_lightweight_shells(monkeypatch):
    def fail_full_profile(*args, **kwargs):
        raise AssertionError("batch ticker profiles should not build full ticker pages")

    monkeypatch.setattr(main_module, "_build_ticker_profile", fail_full_profile)
    monkeypatch.setattr(main_module, "enqueue_data_enrichment_job", lambda *args, **kwargs: True)

    engine = _engine()
    with Session(engine) as db:
        db.add(TickerMeta(symbol="SAIC", company_name="Science Applications International", exchange="NYSE", sector="Technology", industry="Information Technology Services", country="US"))
        db.add(TickerMeta(symbol="BAH", company_name="Booz Allen Hamilton", exchange="NYSE", sector="Industrials", industry="Consulting Services", country="US"))
        db.commit()

        response = _ticker_profiles_response("SAIC,BAH", db)

    assert response["tickers"]["SAIC"]["ticker"]["name"] == "Science Applications International"
    assert response["tickers"]["BAH"]["ticker"]["name"] == "Booz Allen Hamilton"
    assert response["tickers"]["SAIC"]["ticker"]["sector"] == "Technology"


def test_ticker_price_volume_summary_distinguishes_missing_and_inactive(monkeypatch):
    monkeypatch.setattr(
        main_module,
        "build_ticker_technical_indicators",
        lambda *args, **kwargs: {
            "price_points": 0,
            "rsi": {"status": "unavailable", "signal": "unavailable", "message": "RSI temporarily unavailable"},
            "macd": {"status": "unavailable", "signal": "unavailable", "message": "MACD temporarily unavailable"},
            "ema_trend": {"status": "unavailable", "signal": "unavailable", "message": "EMA trend temporarily unavailable"},
        },
    )
    missing = main_module._ticker_price_volume_summary(object(), "NBIS")
    assert missing["status"] == "loading"
    assert missing["summary"] == "Loading price and volume data"

    monkeypatch.setattr(
        main_module,
        "build_ticker_technical_indicators",
        lambda *args, **kwargs: {
            "price_points": 60,
            "rsi": {"status": "ok", "signal": "neutral", "message": "RSI near neutral"},
            "macd": {"status": "ok", "signal": "neutral", "message": "MACD mixed"},
            "ema_trend": {"status": "ok", "signal": "neutral", "message": "EMA trend mixed"},
        },
    )
    inactive = main_module._ticker_price_volume_summary(object(), "NBIS")
    assert inactive["status"] == "inactive"
    assert inactive["summary"] == "No active tape confirmation"
