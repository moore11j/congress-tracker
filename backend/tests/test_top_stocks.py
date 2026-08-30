from __future__ import annotations

import json
from datetime import datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.models import LeaderboardSnapshot
from app.services import top_stocks


def _session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine, tables=[LeaderboardSnapshot.__table__])
    return sessionmaker(bind=engine, autoflush=False, autocommit=False)()


def test_top_stocks_get_reads_the_daily_snapshot_without_invoking_the_screener(monkeypatch):
    db = _session()
    try:
        payload = {
            "items": [{"rank": 1, "symbol": "NVDA", "confirmation_score": 86}],
            "returned": 1,
            "generated_at": "2026-08-29T18:00:00Z",
            "source": "bullish_confirmation_screener_daily_cache",
            "qualification": {"confirmation_score_min": 60},
        }
        db.add(
            LeaderboardSnapshot(
                leaderboard_key=top_stocks.TOP_STOCKS_LEADERBOARD_KEY,
                generated_at=datetime(2026, 8, 29, 18, tzinfo=timezone.utc),
                payload_json=json.dumps(payload),
            )
        )
        db.commit()
        monkeypatch.setattr(top_stocks, "build_screener_rows", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("GET must not build")))

        assert top_stocks.build_top_stocks_response(db) == payload
    finally:
        db.close()


def test_daily_refresh_uses_the_canonical_bullish_confirmation_screener_and_persists_top_ten(monkeypatch):
    db = _session()
    calls = []
    rows = [
        {
            "symbol": "NVDA",
            "company_name": "NVIDIA Corporation",
            "price": 123.45,
            "ticker_url": "/ticker/NVDA",
            "confirmation": {"score": 86, "band": "exceptional", "direction": "bullish"},
            "market_cap": 4_000_000_000_000,
            "sector": "Information Technology",
            "country": "United States",
        },
        {
            "symbol": "MSFT",
            "company_name": "Microsoft Corporation",
            "price": 456.78,
            "ticker_url": "/ticker/MSFT",
            "confirmation": {"score": 80, "band": "strong", "direction": "bullish"},
            "market_cap": 3_000_000_000_000,
            "sector": "Information Technology",
            "country": "United States",
        },
    ]
    try:
        def fake_screener(db_arg, params, *, requested_rows):
            calls.append((db_arg, params, requested_rows))
            return rows

        monkeypatch.setattr(top_stocks, "build_screener_rows", fake_screener)
        refreshed = top_stocks.refresh_top_stocks_leaderboard(db, now=datetime(2026, 8, 29, 18, tzinfo=timezone.utc))

        assert calls[0][1] == top_stocks.TOP_STOCKS_PARAMS
        assert calls[0][2] == top_stocks.MAX_FETCH_ROWS
        assert [item["symbol"] for item in refreshed["items"]] == ["NVDA", "MSFT"]
        assert [item["symbol"] for item in refreshed["filter_items"]["tech"]] == ["NVDA", "MSFT"]
        assert [item["symbol"] for item in refreshed["filter_items"]["large_cap"]] == ["NVDA", "MSFT"]
        assert [item["symbol"] for item in refreshed["filter_items"]["us"]] == ["NVDA", "MSFT"]
        assert refreshed["items"][0]["key_drivers"] == ["Confirmation Score"]
        assert refreshed["qualification"] == {
            "confirmation_score_min": 60,
            "confirmation_direction": "bullish",
            "confirmation_band": "strong_plus",
            "lookback_days": 30,
        }
        assert top_stocks.build_top_stocks_response(db) == refreshed
    finally:
        db.close()
