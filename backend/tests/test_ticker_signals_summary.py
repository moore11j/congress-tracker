from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

import app.main as main_module
from app.db import Base
from app.main import _ticker_profiles_response, ticker_signals_summary
from app.models import DataEnrichmentJob, Event, GovernmentContract, PriceCache, TickerMeta
from app.services.confirmation_score import (
    get_slim_confirmation_score_bundles_for_tickers,
    slim_confirmation_score_bundle,
)


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


def _seed_score_contract_fixture(db: Session) -> None:
    today = date.today()
    price_points = {
        "AAPL": (100, 108),
        "MSTR": (100, 96),
        "NBIS": (100, 99),
        "^GSPC": (100, 102),
    }
    for symbol, (start_close, end_close) in price_points.items():
        db.add(PriceCache(symbol=symbol, date=(today - timedelta(days=29)).isoformat(), close=start_close))
        db.add(PriceCache(symbol=symbol, date=today.isoformat(), close=end_close))

    db.add_all(
        [
            _event(101, symbol="AAPL", event_type="congress_trade", trade_type="purchase", days_ago=2, amount=220_000, member_name="Rep Apple"),
            _event(102, symbol="AAPL", event_type="congress_trade", trade_type="purchase", days_ago=95, amount=20_000, member_name="Rep Apple"),
            _event(103, symbol="AAPL", event_type="congress_trade", trade_type="purchase", days_ago=125, amount=20_000, member_name="Rep Apple"),
            _event(104, symbol="AAPL", event_type="congress_trade", trade_type="purchase", days_ago=155, amount=20_000, member_name="Rep Apple"),
            _event(201, symbol="MSTR", event_type="insider_trade", trade_type="sale", days_ago=1, amount=240_000, member_name="MSTR Insider"),
            _event(202, symbol="MSTR", event_type="insider_trade", trade_type="sale", days_ago=90, amount=20_000, member_name="MSTR Insider"),
            _event(203, symbol="MSTR", event_type="insider_trade", trade_type="sale", days_ago=120, amount=20_000, member_name="MSTR Insider"),
            _event(204, symbol="MSTR", event_type="insider_trade", trade_type="sale", days_ago=150, amount=20_000, member_name="MSTR Insider"),
            _event(205, symbol="MSTR", event_type="congress_trade", trade_type="purchase", days_ago=3, amount=180_000, member_name="Rep Buyer"),
            _event(301, symbol="NBIS", event_type="insider_trade", trade_type="sale", days_ago=2, amount=180_000, member_name="NBIS Insider"),
            _event(302, symbol="NBIS", event_type="insider_trade", trade_type="sale", days_ago=95, amount=15_000, member_name="NBIS Insider"),
            _event(303, symbol="NBIS", event_type="insider_trade", trade_type="sale", days_ago=125, amount=15_000, member_name="NBIS Insider"),
            _event(304, symbol="NBIS", event_type="insider_trade", trade_type="sale", days_ago=155, amount=15_000, member_name="NBIS Insider"),
        ]
    )


def test_ticker_signals_summary_uses_fixed_30d_signal_window(monkeypatch):
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
    monkeypatch.setattr(
        main_module,
        "get_confirmation_score_bundles_for_tickers",
        lambda db, tickers, **kwargs: captured.update(
            {
                "confirmation_tickers": list(tickers),
                "confirmation_lookback_days": kwargs.get("lookback_days"),
            }
        )
        or {
            "NBIS": main_module.inactive_confirmation_score_bundle(
                "NBIS",
                lookback_days=int(kwargs.get("lookback_days") or 30),
            )
        },
    )

    response = ticker_signals_summary(object(), "nbis", side="buy", limit=3, lookback_days=365, db=object())

    assert captured["symbol"] == "NBIS"
    assert captured["limit"] == 3
    assert captured["side"] == "buy"
    assert captured["congress_recent_days"] == 30
    assert captured["insider_recent_days"] == 30
    assert captured["confirmation_tickers"] == ["NBIS"]
    assert captured["confirmation_lookback_days"] == 30
    assert response["symbol"] == "NBIS"
    assert response["latest_signal_score"] == 82
    assert response["recent_signal_count"] == 1
    assert response["lookback_days"] == 30
    assert response["effective_window_days"] == 30
    assert response["items"][0]["symbol"] == "NBIS"
    assert response["price_volume"]["status"] == "limited"
    assert response["price_volume"]["title"] == "Limited price history"
    assert response["confirmation_score_bundle"]["lookback_days"] == 30


def test_ticker_signals_summary_matches_signals_score_contract_for_30d(monkeypatch):
    _mock_signal_auth(monkeypatch)
    monkeypatch.setattr(main_module, "_query_unified_signals", lambda **kwargs: [])

    engine = _engine()
    symbols = ["AAPL", "MSTR", "NBIS"]
    with Session(engine) as db:
        _seed_score_contract_fixture(db)
        db.commit()

        expected_by_symbol = get_slim_confirmation_score_bundles_for_tickers(db, symbols, lookback_days=30)
        for symbol in symbols:
            response = ticker_signals_summary(object(), symbol, side="all", limit=3, lookback_days=365, db=db)
            actual_slim = slim_confirmation_score_bundle(response["confirmation_score_bundle"])
            expected = expected_by_symbol[symbol]

            assert response["lookback_days"] == 30
            assert response["effective_window_days"] == 30
            assert response["confirmation_score_bundle"]["lookback_days"] == 30
            assert actual_slim["confirmation_score"] == expected["confirmation_score"]
            assert actual_slim["confirmation_direction"] == expected["confirmation_direction"]
            assert actual_slim["confirmation_status"] == expected["confirmation_status"]
            assert actual_slim["confirmation_source_count"] == expected["confirmation_source_count"]
            assert response["signal_freshness"]["freshness_score"] == expected["signal_freshness"]["freshness_score"]
            assert response["signal_freshness"]["timing"]["active_source_count"] == expected["signal_freshness"]["timing"]["active_source_count"]


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
    assert response["confirmation_score_bundle"]["score"] > 0
    assert response["confirmation_score_bundle"]["status"] != "Inactive"


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
    assert response["confirmation_score_bundle"]["direction"] == "bearish"
    assert response["confirmation_score_bundle"]["score"] > 0
    assert response["signal_freshness"]["timing"]["active_source_count"] == 1


def test_ticker_signals_summary_conflicting_sources_produce_mixed_confirmation(monkeypatch):
    _mock_signal_auth(monkeypatch)
    monkeypatch.setattr(main_module, "_query_unified_signals", lambda **kwargs: [])
    monkeypatch.setattr(
        main_module,
        "_ticker_price_volume_summary",
        lambda db, symbol: {"status": "unavailable", "summary": "Price and volume unavailable", "score": None, "lines": ["Price and volume unavailable"], "direction": "neutral"},
    )

    engine = _engine()
    with Session(engine) as db:
        db.add(_event(20, symbol="MSTR", event_type="insider_trade", trade_type="sale", amount=125_000, member_name="MSTR Insider"))
        db.add(_event(21, symbol="MSTR", event_type="congress_trade", trade_type="purchase", amount=125_000, member_name="Rep Buyer"))
        db.commit()

        response = ticker_signals_summary(object(), "MSTR", side="all", limit=3, lookback_days=365, db=db)

    assert response["lookback_days"] == 30
    assert response["effective_window_days"] == 30
    assert response["confirmation_score_bundle"]["lookback_days"] == 30
    assert response["confirmation_score_bundle"]["direction"] == "mixed"
    assert response["confirmation_score_bundle"]["status"] != "Inactive"
    assert response["confirmation_score_bundle"]["score"] > 0
    assert response["signal_freshness"]["timing"]["active_source_count"] == 2


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
    assert missing["status"] == "unavailable"
    assert missing["summary"] == "Price and volume unavailable"

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
    engine = _engine()
    today = date.today()
    with Session(engine) as db:
        for offset in range(40):
            day = today - timedelta(days=39 - offset)
            db.add(PriceCache(symbol="NBIS", date=day.isoformat(), close=100 + offset, volume=1_000_000 + offset))
        db.commit()
        inactive = main_module._ticker_price_volume_summary(db, "NBIS")

    assert inactive["status"] == "active"
    assert inactive["direction"] == "neutral"
    assert inactive["summary"] == "Price and volume available"
    assert inactive["latest_close"] is not None
    assert inactive["latest_volume"] is not None
    assert inactive["avg_volume_20d"] is not None


def test_ticker_price_volume_summary_uses_cached_price_rows_without_provider(monkeypatch):
    def fail_provider_loader(*_args, **_kwargs):
        raise AssertionError("signals-summary price volume must not hydrate providers")

    monkeypatch.setattr("app.services.technical_indicators.get_daily_close_series_with_fallback", fail_provider_loader)
    engine = _engine()
    today = date.today()
    with Session(engine) as db:
        for symbol in ("AAPL", "MSTR", "NBIS"):
            for offset in range(70):
                day = today - timedelta(days=69 - offset)
                db.add(
                    PriceCache(
                        symbol=symbol,
                        date=day.isoformat(),
                        close=100 + offset,
                        volume=1_000_000 + offset * 10_000,
                        day_volume=1_000_000 + offset * 10_000,
                    )
                )
        db.commit()

        for symbol in ("AAPL", "MSTR", "NBIS"):
            summary = main_module._ticker_price_volume_summary(db, symbol)
            assert summary["status"] == "active"
            assert summary["status"] != "unavailable"
            assert summary["inputs"]["has_price_series"] is True
            assert summary["inputs"]["has_volume"] is True
            assert summary["inputs"]["point_count"] >= 35
            assert summary["latest_close"] is not None
            assert summary["latest_volume"] is not None
            assert summary["avg_volume_20d"] is not None
            assert summary["volume_vs_avg"] is not None


def test_ticker_price_volume_summary_loading_only_when_hydration_pending(monkeypatch):
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
    engine = _engine()
    with Session(engine) as db:
        db.add(
            DataEnrichmentJob(
                job_type="technical_indicators",
                symbol="NBIS",
                dedupe_key="technical_indicators|NBIS|technical:90d|",
                status="queued",
            )
        )
        db.commit()

        summary = main_module._ticker_price_volume_summary(db, "NBIS")

    assert summary["status"] == "unavailable"
    assert summary["summary"] == "Updating price and volume data"
