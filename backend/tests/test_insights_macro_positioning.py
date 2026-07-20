from __future__ import annotations

import csv
import io
import json
from datetime import date, datetime, timedelta, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from starlette.requests import Request

from app.db import Base
from app.entitlements import ENTITLEMENTS
from app.main import insights_macro_positioning, macro_positioning_feed, ticker_macro_positioning
from app.models import MacroPositioningAsset, MacroPositioningCache, MacroPositioningFeedEvent
from app.services.macro_positioning import (
    _CFTC_MARKET_SPECS,
    get_insights_macro_positioning,
    get_macro_positioning_feed,
    ingest_macro_positioning_assets,
    macro_positioning_cache_payload,
    refresh_macro_positioning_feed_events,
)

FAKE_CFTC_REPORT_DATE = datetime.now(timezone.utc).date() - timedelta(days=3)


def _db():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    return Session()


def _request(path: str = "/api/insights/macro-positioning") -> Request:
    return Request({"type": "http", "method": "GET", "path": path, "headers": []})


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


class _FakeResponse:
    def __init__(self, text: str) -> None:
        self.text = text

    def raise_for_status(self) -> None:
        return None


def _cftc_csv_row(market: str, *, source: str, long_contracts: int, short_contracts: int) -> str:
    size = 191 if source == "disaggregated" else 87
    row = [""] * size
    row[0] = market
    row[2] = FAKE_CFTC_REPORT_DATE.isoformat()
    if source == "disaggregated":
        row[14] = str(long_contracts)
        row[15] = str(short_contracts)
    else:
        row[11] = str(long_contracts)
        row[12] = str(short_contracts)
        row[28] = "7"
        row[29] = "2"
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="")
    writer.writerow(row)
    return buffer.getvalue()


def _fake_cftc_texts() -> dict[str, str]:
    rows = {"financial": [], "disaggregated": []}
    for index, spec in enumerate(_CFTC_MARKET_SPECS):
        long_contracts = 240 + index
        short_contracts = 100 if index % 2 == 0 else 260
        rows[spec.source].append(
            _cftc_csv_row(
                spec.match_terms[0],
                source=spec.source,
                long_contracts=long_contracts,
                short_contracts=short_contracts,
            )
        )
    return {source: "\n".join(source_rows) for source, source_rows in rows.items()}


def _install_fake_cftc(monkeypatch) -> None:
    texts = _fake_cftc_texts()

    def fake_get(url: str, **_kwargs):
        return _FakeResponse(texts["financial"] if "FinFutWk" in url else texts["disaggregated"])

    monkeypatch.setattr("app.services.macro_positioning.requests.get", fake_get)


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


def test_macro_positioning_ingest_populates_supported_assets(monkeypatch):
    db = _db()
    try:
        _install_fake_cftc(monkeypatch)
        result = ingest_macro_positioning_assets(db)

        assert result["status"] == "ok"
        assert result["refreshed"] == len(_CFTC_MARKET_SPECS)
        assert result["missing"] == []

        assets = db.query(MacroPositioningAsset).all()
        assert len(assets) == len(_CFTC_MARKET_SPECS)
        by_key = {asset.asset_key: asset for asset in assets}
        assert by_key["sp_futures"].bias == "bullish"
        assert by_key["gold_futures"].positioning_date == FAKE_CFTC_REPORT_DATE

        payload = get_insights_macro_positioning(db)
        assert payload["status"] == "available"
        assert len(payload["markets"]) == len(_CFTC_MARKET_SPECS)
        gold = next(market for market in payload["markets"] if market["id"] == "gold")
        assert gold["percentile"] is None
        assert gold["trend"] is None
        assert "neutral" not in json.dumps(gold).lower()
    finally:
        db.close()


def test_macro_positioning_ingest_keeps_last_good_rows_when_source_fails(monkeypatch):
    db = _db()
    try:
        db.add(_asset("sp_futures", "S&P 500", "bullish", positioning_date=date(2026, 7, 1)))
        db.commit()

        def fail_get(*_args, **_kwargs):
            import app.services.macro_positioning as macro_module

            raise macro_module.requests.RequestException("network unavailable")

        monkeypatch.setattr("app.services.macro_positioning.requests.get", fail_get)

        result = ingest_macro_positioning_assets(db)
        row = db.get(MacroPositioningAsset, "sp_futures")

        assert result["status"] == "unavailable"
        assert result["refreshed"] == 0
        assert row is not None
        assert row.positioning_date == date(2026, 7, 1)
        assert row.bias == "bullish"
    finally:
        db.close()


def test_macro_positioning_feed_generation_is_idempotent(monkeypatch):
    db = _db()
    try:
        _install_fake_cftc(monkeypatch)
        ingest_macro_positioning_assets(db)

        first = refresh_macro_positioning_feed_events(db)
        second = refresh_macro_positioning_feed_events(db)

        assert first["status"] == "ok"
        assert second["status"] == "ok"
        assert first["significant"] > 0
        assert db.query(MacroPositioningFeedEvent).filter(MacroPositioningFeedEvent.is_summary.is_(True)).count() == 1
        event_ids = [row.event_id for row in db.query(MacroPositioningFeedEvent).all()]
        assert len(event_ids) == len(set(event_ids))
    finally:
        db.close()


def test_macro_positioning_feed_pro_receives_dedicated_rows(monkeypatch):
    db = _db()
    try:
        _set_tier(monkeypatch, "pro")
        _install_fake_cftc(monkeypatch)
        ingest_macro_positioning_assets(db)
        refresh_macro_positioning_feed_events(db)

        payload = macro_positioning_feed(
            _request("/api/feed/macro-positioning"),
            page=1,
            page_size=25,
            view="significant",
            market=None,
            positioning=None,
            event=None,
            sort=None,
            db=db,
        )

        assert payload["status"] == "available"
        assert payload["entitlement"] == {"required_plan": "pro", "unlocked": True}
        assert payload["cadence"] == "weekly"
        assert payload["page_size_options"] == [25, 50, 100]
        assert payload["summary"]
        assert payload["items"]
        assert payload["items"][0]["report_date"] == FAKE_CFTC_REPORT_DATE.isoformat()
        serialized = json.dumps(payload).lower()
        for forbidden in ("cot", "commitment of traders", "cftc", "fmp", "endpoint"):
            assert forbidden not in serialized
    finally:
        db.close()


def test_macro_positioning_feed_locked_payload_redacts_market_data(monkeypatch):
    for tier in ("free", "premium"):
        db = _db()
        try:
            _set_tier(monkeypatch, tier)
            db.add(_asset("gold_futures", "Gold", "bullish"))
            db.commit()

            payload = macro_positioning_feed(
                _request("/api/feed/macro-positioning"),
                page=1,
                page_size=25,
                view="significant",
                market=None,
                positioning=None,
                event=None,
                sort=None,
                db=db,
            )

            assert payload["status"] == "locked"
            assert payload["items"] == []
            serialized = json.dumps(payload).lower()
            assert "gold" not in serialized
            assert "bullish" not in serialized
            assert "percentile" not in serialized
        finally:
            db.close()


def test_macro_positioning_feed_all_markets_pagination_and_filters(monkeypatch):
    db = _db()
    try:
        _install_fake_cftc(monkeypatch)
        ingest_macro_positioning_assets(db)
        refresh_macro_positioning_feed_events(db)

        all_payload = get_macro_positioning_feed(db, view="all", page_size=25)
        significant_payload = get_macro_positioning_feed(db, view="significant", page_size=25)
        commodities = get_macro_positioning_feed(db, view="all", market="commodities", page_size=50)

        assert all_payload["pagination"]["total"] == len(_CFTC_MARKET_SPECS)
        assert significant_payload["pagination"]["total"] < all_payload["pagination"]["total"]
        assert all(item["event_kind"] == "current_state" for item in all_payload["items"])
        assert all(item["market_group"] == "commodities" for item in commodities["items"])
        assert commodities["pagination"]["page_size"] == 50
    finally:
        db.close()


def test_macro_positioning_feed_request_makes_no_provider_call(monkeypatch):
    db = _db()
    try:
        _install_fake_cftc(monkeypatch)
        ingest_macro_positioning_assets(db)
        refresh_macro_positioning_feed_events(db)

        def fail_get(*_args, **_kwargs):
            raise AssertionError("Feed request must not call upstream providers")

        monkeypatch.setattr("app.services.macro_positioning.requests.get", fail_get)

        payload = get_macro_positioning_feed(db, view="all")

        assert payload["items"]
    finally:
        db.close()


def test_ticker_macro_positioning_guest_locked_payload_redacts_values():
    db = _db()
    try:
        db.add(
            MacroPositioningCache(
                symbol="NVDA",
                status="ok",
                overall="bullish",
                rating=5,
                summary="Institutional positioning currently supports growth equities and semiconductor stocks.",
                drivers_json=json.dumps([{"name": "Nasdaq Futures", "bias": "bullish"}]),
                mapped_sector="Technology",
                updated=date(2026, 7, 10),
                generated_at=datetime(2026, 7, 10, tzinfo=timezone.utc),
            )
        )
        db.commit()

        payload = ticker_macro_positioning(_request("/api/ticker/NVDA/macro-positioning"), "NVDA", db)

        assert payload["status"] == "pro_locked"
        assert payload["locked"] is True
        assert payload["required_plan"] == "pro"
        assert "overall" not in payload
        assert "rating" not in payload
        assert payload["drivers"] == []
    finally:
        db.close()


def test_ticker_macro_positioning_guest_irrelevant_ticker_does_not_show_locked_filler():
    db = _db()
    try:
        payload = ticker_macro_positioning(_request("/api/ticker/CASH/macro-positioning"), "CASH", db)

        assert payload["status"] == "unavailable"
        assert payload.get("locked") is None
        assert "overall" not in payload
        assert "rating" not in payload
    finally:
        db.close()


def test_ticker_macro_positioning_payload_does_not_show_neutral_when_all_visible_drivers_are_bullish():
    row = MacroPositioningCache(
        symbol="MU",
        status="ok",
        overall="neutral",
        rating=3,
        summary="Institutional positioning is currently neutral for this investment thesis.",
        drivers_json=json.dumps(
            [
                {"name": "Nasdaq Futures", "bias": "bullish"},
                {"name": "US Dollar", "bias": "bullish"},
                {"name": "10-Year Treasury", "bias": "bullish"},
            ]
        ),
        mapped_sector="Technology",
        updated=date(2026, 7, 10),
        generated_at=datetime(2026, 7, 10, tzinfo=timezone.utc),
    )

    payload = macro_positioning_cache_payload(row)

    assert payload["overall"] == "bullish"
    assert payload["summary"] == "Institutional positioning currently supports this investment thesis."
