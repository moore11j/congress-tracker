from __future__ import annotations

from types import SimpleNamespace

import app.main as main_module
from app.main import ticker_signals_summary


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

    monkeypatch.setattr(main_module, "current_user", lambda *args, **kwargs: object())
    monkeypatch.setattr(main_module, "current_entitlements", lambda *args, **kwargs: {})
    monkeypatch.setattr(main_module, "require_feature", lambda *args, **kwargs: None)
    monkeypatch.setattr(main_module, "_query_unified_signals", fake_query)
    monkeypatch.setattr(
        main_module,
        "_ticker_price_volume_summary",
        lambda db, symbol: {"status": "limited", "summary": "Limited price history", "score": None, "lines": ["Limited price history"]},
    )

    response = ticker_signals_summary(object(), "nbis", side="buy", limit=3, db=object())

    assert captured["symbol"] == "NBIS"
    assert captured["limit"] == 3
    assert captured["side"] == "buy"
    assert response["symbol"] == "NBIS"
    assert response["latest_signal_score"] == 82
    assert response["recent_signal_count"] == 1
    assert response["items"][0]["symbol"] == "NBIS"
    assert response["price_volume"]["status"] == "limited"


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
