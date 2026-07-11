from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from starlette.requests import Request

from app.db import Base
from app.entitlements import ENTITLEMENTS
from app.main import insights_macro_positioning
from app.models import MacroPositioningAsset


def _db():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    return Session()


def _request() -> Request:
    return Request({"type": "http", "method": "GET", "path": "/api/insights/macro-positioning", "headers": []})


def _asset(
    asset_key: str,
    name: str,
    bias: str,
    *,
    rating: int = 4,
    positioning_date: date | None = None,
    payload: dict | None = None,
) -> MacroPositioningAsset:
    now = datetime(2026, 7, 10, tzinfo=timezone.utc)
    return MacroPositioningAsset(
        asset_key=asset_key,
        display_name=name,
        bias=bias,
        rating=rating,
        positioning_date=positioning_date or date(2026, 7, 10),
        payload_json=json.dumps(payload or {"percentile": 72, "trend": "increasing", "trend_weeks": 2}),
        fetched_at=now,
    )


def _set_tier(monkeypatch, tier: str) -> None:
    import app.main as main_module

    monkeypatch.setattr(main_module, "current_entitlements", lambda *_args, **_kwargs: ENTITLEMENTS[tier])


def test_insights_macro_positioning_pro_receives_full_payload(monkeypatch):
    db = _db()
    try:
        _set_tier(monkeypatch, "pro")
        db.add(_asset("gold_futures", "Gold Futures", "bullish", payload={"percentile": 89, "trend": "increasing", "trend_weeks": 3}))
        db.commit()

        payload = insights_macro_positioning(_request(), db)

        assert payload["status"] == "available"
        assert payload["entitlement"] == {"required_plan": "pro", "unlocked": True}
        assert payload["markets"][0]["name"] == "Gold"
        assert payload["markets"][0]["bias"] == "bullish"
        assert payload["markets"][0]["percentile"] == 89
        assert payload["summary"]
        serialized = json.dumps(payload).lower()
        for forbidden in ("cot", "commitment of traders", "cftc", "fmp"):
            assert forbidden not in serialized
    finally:
        db.close()


def test_insights_macro_positioning_admin_receives_full_payload(monkeypatch):
    db = _db()
    try:
        _set_tier(monkeypatch, "admin")
        db.add(_asset("nasdaq_futures", "Nasdaq Futures", "bullish"))
        db.commit()

        payload = insights_macro_positioning(_request(), db)

        assert payload["entitlement"]["unlocked"] is True
        assert payload["markets"][0]["name"] == "Nasdaq 100"
    finally:
        db.close()


def test_insights_macro_positioning_non_pro_tiers_are_redacted(monkeypatch):
    for tier in ("free", "premium"):
        db = _db()
        try:
            _set_tier(monkeypatch, tier)
            db.add(_asset("gold_futures", "Gold Futures", "bullish"))
            db.commit()

            payload = insights_macro_positioning(_request(), db)

            assert payload["status"] == "locked"
            assert payload["entitlement"] == {"required_plan": "pro", "unlocked": False}
            assert payload["markets"] == []
            assert payload["summary"] is None
        finally:
            db.close()


def test_insights_macro_positioning_guest_is_redacted(monkeypatch):
    db = _db()
    try:
        _set_tier(monkeypatch, "free")
        db.add(_asset("gold_futures", "Gold Futures", "bullish"))
        db.commit()

        payload = insights_macro_positioning(_request(), db)

        assert payload["status"] == "locked"
        assert payload["markets"] == []
    finally:
        db.close()


def test_insights_macro_positioning_missing_data_is_awaiting_refresh_not_neutral(monkeypatch):
    db = _db()
    try:
        _set_tier(monkeypatch, "pro")

        payload = insights_macro_positioning(_request(), db)

        assert payload["status"] == "awaiting_first_refresh"
        assert payload["markets"] == []
        assert "neutral" not in json.dumps(payload).lower()
    finally:
        db.close()


def test_insights_macro_positioning_marks_stale_but_serves_latest(monkeypatch):
    db = _db()
    try:
        _set_tier(monkeypatch, "pro")
        old_date = datetime.now(timezone.utc).date() - timedelta(days=30)
        db.add(_asset("sp_futures", "S&P Futures", "bullish", positioning_date=old_date))
        db.commit()

        payload = insights_macro_positioning(_request(), db)

        assert payload["status"] == "stale"
        assert payload["stale"] is True
        assert payload["markets"][0]["name"] == "S&P 500"
        assert payload["message"] == "Latest weekly positioning data is delayed."
    finally:
        db.close()


def test_insights_macro_positioning_summary_is_derived_from_cached_assets(monkeypatch):
    db = _db()
    try:
        _set_tier(monkeypatch, "pro")
        db.add_all(
            [
                _asset("sp_futures", "S&P Futures", "bullish", payload={"percentile": 70, "trend": "increasing"}),
                _asset("nasdaq_futures", "Nasdaq Futures", "bullish", payload={"percentile": 80, "trend": "increasing"}),
                _asset("us_dollar", "US Dollar", "bearish", payload={"percentile": 20, "trend": "decreasing"}),
            ]
        )
        db.commit()

        payload = insights_macro_positioning(_request(), db)

        assert "positioning strengthened in S&P 500 and Nasdaq 100" in payload["summary"]
        assert "US Dollar" in payload["summary"]
    finally:
        db.close()


def test_insights_macro_positioning_endpoint_does_not_refresh_cache(monkeypatch):
    db = _db()
    try:
        _set_tier(monkeypatch, "pro")
        db.add(_asset("gold_futures", "Gold Futures", "bullish"))
        db.commit()

        def fail_refresh(*_args, **_kwargs):
            raise AssertionError("Insights endpoint must read precomputed data only")

        monkeypatch.setattr("app.services.macro_positioning.refresh_macro_positioning_cache", fail_refresh)

        payload = insights_macro_positioning(_request(), db)

        assert payload["status"] == "available"
    finally:
        db.close()
