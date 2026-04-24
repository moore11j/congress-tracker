from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from starlette.requests import Request

from app.db import Base
from app.models import Event
from app.routers.screener import stock_screener_export
from app.services.screener import MAX_EXPORT_ROWS, ScreenerParams, build_screener_csv_export, build_screener_response


def _engine():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    return engine


def _request(tier: str | None = None) -> Request:
    headers = []
    if tier:
        headers.append((b"x-ct-entitlement-tier", tier.encode("utf-8")))
    return Request({"type": "http", "method": "GET", "path": "/", "headers": headers})


def _event(
    *,
    event_id: int,
    symbol: str,
    event_type: str,
    trade_type: str,
    days_ago: int,
) -> Event:
    ts = datetime.now(timezone.utc) - timedelta(days=days_ago)
    return Event(
        id=event_id,
        event_type=event_type,
        ts=ts,
        event_date=ts,
        symbol=symbol,
        source="test",
        member_name="Test Actor",
        member_bioguide_id="T001" if event_type == "congress_trade" else None,
        trade_type=trade_type,
        amount_min=10_000,
        amount_max=250_000,
        payload_json=json.dumps({"symbol": symbol, "reporting_cik": "0001234567"}),
    )


def test_screener_maps_v1_filters_to_fmp_and_paginates(monkeypatch):
    captured: dict = {}

    def fake_fetch_company_screener(*, filters, limit):
        captured["filters"] = filters
        captured["limit"] = limit
        return [
            {
                "symbol": "AAA",
                "companyName": "Aaa Corp",
                "sector": "Technology",
                "marketCap": 50_000_000_000,
                "price": 50,
                "volume": 5_000_000,
                "beta": 1.2,
                "country": "US",
                "exchangeShortName": "NASDAQ",
            },
            {
                "symbol": "BBB",
                "companyName": "Bbb Corp",
                "sector": "Technology",
                "marketCap": 25_000_000_000,
                "price": 20,
                "volume": 2_000_000,
                "beta": 0.8,
                "country": "US",
                "exchangeShortName": "NYSE",
            },
        ]

    monkeypatch.setattr("app.services.screener.fetch_company_screener", fake_fetch_company_screener)
    engine = _engine()

    with Session(engine) as db:
        response = build_screener_response(
            db,
            ScreenerParams(
                page=1,
                page_size=10,
                sort="market_cap",
                market_cap_min=10_000_000_000,
                price_min=10,
                volume_min=1_000_000,
                beta_max=1.5,
                sector="Technology",
                country="US",
                exchange="NASDAQ,NYSE",
            ),
        )

    assert captured["filters"] == {
        "marketCapMoreThan": 10_000_000_000,
        "priceMoreThan": 10,
        "volumeMoreThan": 1_000_000,
        "betaLowerThan": 1.5,
        "sector": "Technology",
        "country": "US",
        "exchange": "NASDAQ,NYSE",
    }
    assert captured["limit"] == 11
    assert response["items"][0]["symbol"] == "AAA"
    assert response["items"][0]["ticker_url"] == "/ticker/AAA"
    assert response["filters"]["market_cap_min"] == 10_000_000_000
    assert response["has_next"] is False


def test_screener_enriches_rows_with_canonical_confirmation_sources(monkeypatch):
    monkeypatch.setattr(
        "app.services.screener.fetch_company_screener",
        lambda *, filters, limit: [
            {
                "symbol": "ALIGN",
                "companyName": "Alignment Inc",
                "sector": "Healthcare",
                "marketCap": 5_000_000_000,
                "price": 30,
                "volume": 1_500_000,
                "beta": 1.1,
                "country": "US",
                "exchangeShortName": "NASDAQ",
            }
        ],
    )
    engine = _engine()

    with Session(engine) as db:
        db.add(_event(event_id=1, symbol="ALIGN", event_type="congress_trade", trade_type="purchase", days_ago=2))
        db.add(_event(event_id=2, symbol="ALIGN", event_type="insider_trade", trade_type="purchase", days_ago=1))
        db.commit()

        response = build_screener_response(db, ScreenerParams(sort="confirmation_score"))

    row = response["items"][0]
    assert row["symbol"] == "ALIGN"
    assert row["congress_activity"]["present"] is True
    assert row["insider_activity"]["present"] is True
    assert row["confirmation"]["score"] > 0
    assert row["confirmation"]["band"] in {"weak", "moderate", "strong", "exceptional"}
    assert row["confirmation"]["direction"] == "bullish"
    assert row["confirmation"]["status"] == "2-source bullish confirmation"
    assert row["confirmation"]["source_count"] == 2
    assert row["why_now"]["state"] in {"strengthening", "strong"}
    assert "ALIGN" in row["why_now"]["headline"]


def test_screener_filters_intelligence_dimensions_with_canonical_overlays(monkeypatch):
    monkeypatch.setattr(
        "app.services.screener.fetch_company_screener",
        lambda *, filters, limit: [
            {
                "symbol": "ALIGN",
                "companyName": "Alignment Inc",
                "sector": "Healthcare",
                "marketCap": 5_000_000_000,
                "price": 30,
                "volume": 1_500_000,
                "beta": 1.1,
                "country": "US",
                "exchangeShortName": "NASDAQ",
            },
            {
                "symbol": "MIXD",
                "companyName": "Mixed Corp",
                "sector": "Technology",
                "marketCap": 12_000_000_000,
                "price": 45,
                "volume": 3_200_000,
                "beta": 1.0,
                "country": "US",
                "exchangeShortName": "NYSE",
            },
            {
                "symbol": "OLD",
                "companyName": "Old Signal Co",
                "sector": "Industrials",
                "marketCap": 9_000_000_000,
                "price": 18,
                "volume": 900_000,
                "beta": 0.9,
                "country": "US",
                "exchangeShortName": "NASDAQ",
            },
        ],
    )
    engine = _engine()

    with Session(engine) as db:
        db.add(_event(event_id=1, symbol="ALIGN", event_type="congress_trade", trade_type="purchase", days_ago=2))
        db.add(_event(event_id=2, symbol="ALIGN", event_type="insider_trade", trade_type="purchase", days_ago=1))
        db.add(_event(event_id=3, symbol="MIXD", event_type="congress_trade", trade_type="purchase", days_ago=2))
        db.add(_event(event_id=4, symbol="MIXD", event_type="insider_trade", trade_type="sale", days_ago=1))
        db.add(_event(event_id=5, symbol="OLD", event_type="congress_trade", trade_type="purchase", days_ago=35))
        db.commit()

        filtered = build_screener_response(
            db,
            ScreenerParams(
                sort="confirmation_score",
                lookback_days=90,
                congress_activity="buy_leaning",
                insider_activity="has_activity",
                confirmation_score_min=40,
                confirmation_direction="bullish",
                confirmation_band="moderate_plus",
                why_now_state="strengthening",
                freshness="fresh",
            ),
        )
        limited = build_screener_response(
            db,
            ScreenerParams(
                sort="confirmation_score",
                lookback_days=90,
                why_now_state="limited",
            ),
        )
        stale = build_screener_response(
            db,
            ScreenerParams(
                sort="freshness",
                sort_dir="asc",
                lookback_days=90,
                freshness="stale",
            ),
        )

    assert [row["symbol"] for row in filtered["items"]] == ["ALIGN"]
    assert filtered["items"][0]["signal_freshness"]["freshness_state"] == "fresh"
    assert filtered["items"][0]["why_now"]["state"] == "strengthening"

    assert [row["symbol"] for row in limited["items"]] == ["MIXD"]
    assert limited["items"][0]["why_now"]["state"] == "mixed"

    assert [row["symbol"] for row in stale["items"]] == ["OLD"]
    assert stale["items"][0]["signal_freshness"]["freshness_state"] == "stale"


def test_screener_csv_export_uses_shared_rows_and_human_headers(monkeypatch):
    captured: dict[str, int] = {}

    def fake_fetch_company_screener(*, filters, limit):
        captured["limit"] = limit
        return [
            {
                "symbol": "ALIGN",
                "companyName": "Alignment Inc",
                "sector": "Healthcare",
                "industry": "Biotechnology",
                "marketCap": 5_000_000_000,
                "price": 30,
                "volume": 1_500_000,
                "beta": 1.1,
                "country": "US",
                "exchangeShortName": "NASDAQ",
            }
        ]

    monkeypatch.setattr("app.services.screener.fetch_company_screener", fake_fetch_company_screener)
    engine = _engine()

    with Session(engine) as db:
        db.add(_event(event_id=1, symbol="ALIGN", event_type="congress_trade", trade_type="purchase", days_ago=2))
        db.add(_event(event_id=2, symbol="ALIGN", event_type="insider_trade", trade_type="purchase", days_ago=1))
        db.commit()

        csv_text, exported_rows = build_screener_csv_export(db, ScreenerParams(sort="confirmation_score"))

    lines = csv_text.strip().splitlines()
    assert captured["limit"] == MAX_EXPORT_ROWS
    assert exported_rows == 1
    assert (
        lines[0]
        == "Symbol,Company,Sector,Industry,Country,Exchange,Market Cap,Price,Volume,Beta,Congress Activity,Insider Activity,Confirmation Score,Confirmation Direction,Confirmation Band,Confirmation Status,Why Now State,Why Now Headline,Freshness State"
    )
    assert "ALIGN,Alignment Inc,Healthcare,Biotechnology,US,NASDAQ,5000000000,30,1500000,1.1" in lines[1]
    assert ",Bullish," in lines[1]


def test_screener_export_route_returns_csv_attachment(monkeypatch):
    monkeypatch.setenv("CT_DEFAULT_TIER", "free")
    monkeypatch.setenv("CT_ALLOW_ENTITLEMENT_HEADER", "1")
    monkeypatch.setattr(
        "app.services.screener.fetch_company_screener",
        lambda *, filters, limit: [
            {
                "symbol": "AAA",
                "companyName": "Aaa Corp",
                "sector": "Technology",
                "industry": "Software - Infrastructure",
                "marketCap": 50_000_000_000,
                "price": 50,
                "volume": 5_000_000,
                "beta": 1.2,
                "country": "US",
                "exchangeShortName": "NASDAQ",
            }
        ],
    )
    engine = _engine()

    with Session(engine) as db:
        response = stock_screener_export(request=_request("premium"), db=db, sort="symbol", filename_prefix="Growth Leaders")

    assert response.headers["content-disposition"].startswith('attachment; filename="growth-leaders-')
    assert response.headers["x-screener-export-row-cap"] == "250"
    assert response.headers["x-screener-exported-rows"] == "1"
    assert "Symbol,Company,Sector,Industry,Country,Exchange" in response.body.decode("utf-8")
