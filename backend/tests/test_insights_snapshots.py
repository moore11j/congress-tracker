from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.models import FredObservation, FredSeriesRefresh, InsightsSnapshot, PriceCache
from app.services.insights_snapshots import (
    get_insights_headlines,
    get_insights_snapshot,
    refresh_insights_headlines,
    refresh_insights_snapshot,
)


def _db():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    return Session()


def _payload(status: str = "ok") -> dict:
    return {
        "world_indexes": [],
        "indexes": [{"label": "S&P 500", "symbol": "SPY", "value": 5000, "change_pct": 1.2}],
        "treasury": [],
        "economics": [],
        "commodities": [],
        "currencies": [],
        "crypto": [],
        "sector_performance": [],
        "status": status,
        "generated_at": "2026-06-05T12:00:00+00:00",
    }


def _seed_fred(db, series_id: str, rows: list[tuple[str, float]]) -> None:
    now = datetime.now(timezone.utc)
    for day, value in rows:
        db.add(
            FredObservation(
                series_id=series_id,
                observation_date=datetime.fromisoformat(day).date(),
                value=value,
                source="fred",
                payload_json="{}",
                fetched_at=now,
            )
        )
    db.add(
        FredSeriesRefresh(
            series_id=series_id,
            source="fred",
            status="ok",
            observation_count=len(rows),
            latest_observation_date=datetime.fromisoformat(rows[-1][0]).date(),
            last_refreshed_at=now,
        )
    )


def _seed_price(db, symbol: str, rows: list[tuple[str, float]]) -> None:
    for day, close in rows:
        db.add(PriceCache(symbol=symbol, date=day, close=close))


def test_insights_snapshot_returns_cached_data_without_provider_call(monkeypatch):
    db = _db()
    try:
        row = InsightsSnapshot(
            kind="macro-snapshot",
            payload_json=json.dumps(_payload()),
            source="test",
            fetched_at=datetime.now(timezone.utc),
        )
        db.add(row)
        db.commit()

        def fail_provider():
            raise AssertionError("provider should not be called on cache hit")

        monkeypatch.setattr("app.services.insights_snapshots.get_macro_snapshot", fail_provider)

        payload = get_insights_snapshot(db)

        assert payload["status"] == "ok"
        assert payload["cache_hit"] is True
        assert payload["stale"] is False
        assert payload["source"] == "test"
        assert payload["as_of"]
    finally:
        db.close()


def test_insights_snapshot_marks_old_cache_stale():
    db = _db()
    try:
        db.add(
            InsightsSnapshot(
                kind="macro-snapshot",
                payload_json=json.dumps(_payload()),
                source="test",
                fetched_at=datetime.now(timezone.utc) - timedelta(minutes=30),
            )
        )
        db.commit()

        payload = get_insights_snapshot(db)

        assert payload["status"] == "ok"
        assert payload["stale"] is True
        assert payload["cache_hit"] is True
    finally:
        db.close()


def test_insights_snapshot_cache_miss_returns_warming_without_provider_call(monkeypatch):
    db = _db()
    try:
        def fail_provider():
            raise AssertionError("provider should not be called on public cache miss")

        monkeypatch.setattr("app.services.insights_snapshots.get_macro_snapshot", fail_provider)

        payload = get_insights_snapshot(db)

        assert payload["status"] == "warming"
        assert payload["cache_hit"] is False
        assert payload["stale"] is True
    finally:
        db.close()


def test_insights_headlines_cache_miss_returns_warming_without_provider_call(monkeypatch):
    db = _db()
    try:
        def fail_provider(**_kwargs):
            raise AssertionError("provider should not be called on public headlines cache miss")

        monkeypatch.setattr("app.services.insights_snapshots.get_general_news", fail_provider)

        payload = get_insights_headlines(db, page=0, limit=20)

        assert payload["status"] == "warming"
        assert payload["cache_status"] == "warming"
        assert "message" not in payload
        assert payload["cache_hit"] is False
        assert payload["items"] == []
    finally:
        db.close()


def test_insights_headlines_refresh_generates_and_saves_walnut_takes(monkeypatch):
    db = _db()
    try:
        monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
        monkeypatch.setattr(
            "app.services.insights_snapshots.get_general_news",
            lambda **_kwargs: {
                "items": [
                    {
                        "title": "Chip Stocks Rebound Lifting Indexes Ahead of Big Tech Earnings",
                        "site": "TestWire",
                        "published_at": "2026-07-21T12:00:00+00:00",
                        "url": "https://example.com/chips",
                        "summary": "Chip shares rose as investors positioned for earnings.",
                        "symbol": None,
                        "market_read": "neutral",
                        "source": "fmp_general_news",
                    }
                ],
                "has_next": False,
            },
        )

        class FakeResponse:
            status_code = 200

            def json(self):
                return {
                    "output_text": json.dumps(
                        {
                            "items": [
                                {
                                    "id": "url:https://example.com/chips",
                                    "summary": "Chip shares rallied ahead of major tech earnings.",
                                    "bias": "bullish",
                                    "take": "Positive read for chip exposure, but follow-through depends on earnings guidance.",
                                }
                            ]
                        }
                    ),
                    "usage": {"input_tokens": 100, "output_tokens": 30},
                }

        captured = {}

        def fake_post(url, **kwargs):
            captured["url"] = url
            captured["json"] = kwargs.get("json")
            return FakeResponse()

        monkeypatch.setattr("app.services.walnut_takes.requests.post", fake_post)

        payload = refresh_insights_headlines(db, limit=10)

        item = payload["items"][0]
        assert captured["url"] == "https://api.openai.com/v1/responses"
        assert captured["json"]["model"] == "gpt-5.6"
        assert item["walnut_summary"] == "Chip shares rallied ahead of major tech earnings."
        assert item["walnut_take_bias"] == "bullish"
        assert item["walnut_take_source"] == "openai"

        cached = get_insights_headlines(db, page=0, limit=10)
        assert cached["items"][0]["walnut_take"] == "Positive read for chip exposure, but follow-through depends on earnings guidance."
    finally:
        db.close()


def test_insights_headlines_refresh_reuses_cached_walnut_takes_without_openai(monkeypatch):
    db = _db()
    try:
        monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
        db.add(
            InsightsSnapshot(
                kind="market-headlines",
                payload_json=json.dumps(
                    {
                        "items": [
                            {
                                "title": "Energy stocks rally",
                                "url": "https://example.com/energy",
                                "summary": "Old provider summary.",
                                "market_read": "neutral",
                                "walnut_summary": "Energy stocks rallied on oil supply concerns.",
                                "walnut_take_bias": "bullish",
                                "walnut_take": "Supportive for energy exposure while oil supply risk remains elevated.",
                                "walnut_take_source": "openai",
                                "walnut_take_model": "gpt-5.6-sol",
                                "walnut_take_generated_at": "2026-07-21T12:00:00+00:00",
                            }
                        ],
                        "status": "ok",
                        "page": 0,
                        "limit": 1,
                        "has_next": False,
                    }
                ),
                source="fmp",
                fetched_at=datetime.now(timezone.utc),
            )
        )
        db.commit()
        monkeypatch.setattr(
            "app.services.insights_snapshots.get_general_news",
            lambda **_kwargs: {
                "items": [
                    {
                        "title": "Energy stocks rally",
                        "url": "https://example.com/energy",
                        "summary": "New provider summary should not trigger a second OpenAI call.",
                        "market_read": "neutral",
                        "source": "fmp_general_news",
                    }
                ],
                "has_next": False,
            },
        )

        def fail_post(*_args, **_kwargs):
            raise AssertionError("OpenAI should not be called for an article with a cached Walnut Take")

        monkeypatch.setattr("app.services.walnut_takes.requests.post", fail_post)

        payload = refresh_insights_headlines(db, limit=10)

        item = payload["items"][0]
        assert item["summary"] == "New provider summary should not trigger a second OpenAI call."
        assert item["walnut_take"] == "Supportive for energy exposure while oil supply risk remains elevated."
        assert item["walnut_take_bias"] == "bullish"
        assert item["walnut_take_source"] == "openai"
    finally:
        db.close()


def test_insights_refresh_returns_stale_cache_when_provider_fails(monkeypatch):
    db = _db()
    try:
        db.add(
            InsightsSnapshot(
                kind="macro-snapshot",
                payload_json=json.dumps(_payload()),
                source="test",
                fetched_at=datetime.now(timezone.utc) - timedelta(hours=1),
            )
        )
        db.commit()

        def fail_provider():
            raise RuntimeError("provider down")

        monkeypatch.setattr("app.services.insights_snapshots.get_macro_snapshot", fail_provider)

        payload = refresh_insights_snapshot(db)

        assert payload["status"] == "ok"
        assert payload["stale"] is True
        assert payload["cache_hit"] is True
    finally:
        db.close()


def test_insights_refresh_builder_safe_uses_fred_cache_and_eod_proxies(monkeypatch):
    db = _db()
    try:
        _seed_fred(db, "FEDFUNDS", [("2026-03-01", 4.25), ("2026-04-01", 4.5)])
        _seed_fred(db, "CPILFESL", [("2025-04-01", 300.0), ("2026-03-01", 306.0), ("2026-04-01", 309.0)])
        _seed_fred(db, "UNRATE", [("2026-03-01", 4.0), ("2026-04-01", 4.1)])
        _seed_fred(db, "GFDEGDQ188S", [("2026-01-01", 119.8), ("2026-04-01", 120.4)])
        _seed_fred(db, "RSAFS", [("2026-03-01", 650000.0), ("2026-04-01", 656500.0)])
        _seed_fred(db, "GDPC1", [("2025-10-01", 23100.0), ("2026-01-01", 23200.0), ("2026-04-01", 23300.0)])
        _seed_fred(db, "DGS10", [("2026-04-01", 4.2), ("2026-04-02", 4.25)])
        _seed_price(db, "SPY", [("2026-04-01", 510.0), ("2026-04-02", 515.1)])
        db.commit()

        def fail_provider():
            raise AssertionError("FMP macro snapshot should not be called in builder_safe mode")

        monkeypatch.delenv("INSIGHTS_DATA_MODE", raising=False)
        monkeypatch.setattr("app.services.insights_snapshots.get_macro_snapshot", fail_provider)
        monkeypatch.setattr("app.services.insights_builder_safe.get_treasury_rates_snapshot", lambda: [])

        payload = refresh_insights_snapshot(db)

        assert payload["source"] == "builder_safe_cache"
        assert payload["cache_hit"] is False
        assert payload["indexes"][0]["label"] == "S&P 500 ETF Proxy"
        assert payload["indexes"][0]["symbol"] == "SPY"
        assert payload["economics"][0]["source"] == "fred"
        assert payload["economics"][0]["value"] == 4.5
        assert payload["treasury"][3]["series_id"] == "DGS10"
        assert payload["fred_macro_cache"]["last_refresh_at"]
        assert payload["currencies"][0]["status"] == "disabled"
        assert payload["crypto"][0]["status"] == "disabled"
    finally:
        db.close()


def test_insights_refresh_builder_safe_uses_treasury_rates_snapshot(monkeypatch):
    db = _db()
    try:
        _seed_fred(db, "DGS10", [("2026-07-08", 4.56)])
        db.commit()

        def fail_provider():
            raise AssertionError("FMP macro snapshot should not be called in builder_safe mode")

        monkeypatch.delenv("INSIGHTS_DATA_MODE", raising=False)
        monkeypatch.setattr("app.services.insights_snapshots.get_macro_snapshot", fail_provider)
        monkeypatch.setattr(
            "app.services.insights_builder_safe.get_treasury_rates_snapshot",
            lambda: [
                {
                    "label": "10Y Treasury",
                    "value": 4.54,
                    "date": "2026-07-09",
                    "change": -2.0,
                    "change_unit": "bps",
                    "timeframe_label": "1D change",
                    "unit_label": "yield",
                }
            ],
        )

        payload = refresh_insights_snapshot(db)

        assert payload["source"] == "builder_safe_cache"
        assert payload["treasury"] == [
            {
                "label": "10Y Treasury",
                "value": 4.54,
                "date": "2026-07-09",
                "change": -2.0,
                "change_unit": "bps",
                "timeframe_label": "1D change",
                "unit_label": "yield",
            }
        ]
        assert payload["block_status"]["us_treasury"]["source"] == "treasury_rates"
    finally:
        db.close()


def test_insights_refresh_builder_safe_uses_sector_performance_snapshot(monkeypatch):
    db = _db()
    try:
        def fail_provider():
            raise AssertionError("FMP macro snapshot should not be called in builder_safe mode")

        monkeypatch.delenv("INSIGHTS_DATA_MODE", raising=False)
        monkeypatch.setattr("app.services.insights_snapshots.get_macro_snapshot", fail_provider)
        monkeypatch.setattr(
            "app.services.insights_builder_safe.get_sector_performance_snapshot",
            lambda: [
                {
                    "sector": "Basic Materials",
                    "change_pct": 0.73,
                    "date": "2026-07-10",
                    "unit_label": "%",
                    "source": "sector_performance_snapshot",
                }
            ],
        )

        payload = refresh_insights_snapshot(db)

        assert payload["source"] == "builder_safe_cache"
        assert payload["sector_performance"] == [
            {
                "sector": "Basic Materials",
                "change_pct": 0.73,
                "date": "2026-07-10",
                "unit_label": "%",
                "source": "sector_performance_snapshot",
            }
        ]
        assert payload["block_status"]["us_sectors"]["source"] == "sector_performance_snapshot"
    finally:
        db.close()
