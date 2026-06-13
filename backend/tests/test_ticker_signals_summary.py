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

    response = ticker_signals_summary(object(), "nbis", side="buy", limit=3, db=object())

    assert captured["symbol"] == "NBIS"
    assert captured["limit"] == 3
    assert captured["side"] == "buy"
    assert response["symbol"] == "NBIS"
    assert response["latest_signal_score"] == 82
    assert response["recent_signal_count"] == 1
    assert response["items"][0]["symbol"] == "NBIS"
