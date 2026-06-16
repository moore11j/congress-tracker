from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session
from starlette.requests import Request

from app.db import Base
from app.main import ticker_government_contracts
from app.models import Event, GovernmentContract, PriceCache
from app.routers.screener import stock_screener_export
from app.services.confirmation_score import get_confirmation_score_bundle_for_ticker
from app.services.government_contracts import get_government_contracts_summaries_for_symbols
from app.services.screener import MAX_EXPORT_ROWS, ScreenerParams, build_screener_csv_export, build_screener_response, screener_params_from_mapping


def _engine():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    return engine


@pytest.fixture(autouse=True)
def _allow_provider_screener_fallback(monkeypatch):
    monkeypatch.setenv("SCREENER_PROVIDER_FALLBACK", "1")


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


def _confirmation_bundle(symbol: str, *, score: int, band: str, direction: str, present: bool = True) -> dict:
    return {
        "ticker": symbol,
        "lookback_days": 30,
        "score": score,
        "band": band,
        "direction": direction,
        "status": f"2-source {direction} confirmation" if present else "Inactive",
        "explanation": "Test confirmation",
        "drivers": ["Test confirmation"] if present else [],
        "active_sources": ["congress", "insiders"] if present else [],
        "source_details": {},
        "sources": {
            "congress": {
                "present": present,
                "direction": direction if present else "neutral",
                "strength": score,
                "quality": 80,
                "freshness_days": 1,
                "label": "Congress",
            },
            "insiders": {
                "present": present,
                "direction": direction if present else "neutral",
                "strength": score,
                "quality": 80,
                "freshness_days": 1,
                "label": "Insiders",
            },
        },
    }


def _government_contract(
    *,
    contract_id: int,
    symbol: str,
    days_ago: int,
    award_amount: float,
    awarding_agency: str,
    description: str | None = None,
    award_id: str | None = None,
) -> GovernmentContract:
    award_day = (datetime.now(timezone.utc) - timedelta(days=days_ago)).date()
    return GovernmentContract(
        id=contract_id,
        award_id=award_id or f"AWD-{contract_id}",
        dedupe_key=f"dedupe-{contract_id}",
        symbol=symbol,
        recipient_name=f"{symbol} Recipient",
        raw_recipient_name=f"{symbol} Recipient",
        award_date=award_day,
        award_amount=award_amount,
        awarding_agency=awarding_agency,
        description=description,
        contract_type="DEFINITIVE CONTRACT",
        source="usaspending",
        mapping_method="alias_exact",
        mapping_confidence=1.0,
        payload_json=json.dumps(
            {
                "symbol": symbol,
                "award_id": award_id or f"AWD-{contract_id}",
                "award_date": award_day.isoformat(),
                "award_amount": award_amount,
                "awarding_agency": awarding_agency,
                "description": description,
            }
        ),
    )


def _add_price_history(db: Session, symbol: str, closes: list[float]) -> None:
    start = (datetime.now(timezone.utc) - timedelta(days=len(closes))).date()
    for index, close in enumerate(closes):
        db.add(PriceCache(symbol=symbol, date=(start + timedelta(days=index)).isoformat(), close=float(close)))


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


def test_screener_core_filters_are_parsed_emitted_and_locally_enforced(monkeypatch):
    captured_filters: list[dict] = []

    def fake_fetch_company_screener(*, filters, limit):
        captured_filters.append(filters)
        return [
            {
                "symbol": "PASS",
                "companyName": "Pass Corp",
                "sector": "Technology",
                "industry": "Semiconductors",
                "country": "US",
                "exchangeShortName": "NASDAQ",
                "marketCap": 50_000_000_000,
                "price": 150,
                "volume": 3_000_000,
                "beta": 1.1,
                "dividendYield": 0.025,
            },
            {
                "symbol": "LOWPRICE",
                "companyName": "Low Price Corp",
                "sector": "Technology",
                "industry": "Semiconductors",
                "country": "US",
                "exchangeShortName": "NASDAQ",
                "marketCap": 50_000_000_000,
                "price": 5,
                "volume": 3_000_000,
                "beta": 1.1,
                "dividendYield": 0.025,
            },
            {
                "symbol": "WRONGSECTOR",
                "companyName": "Wrong Sector Corp",
                "sector": "Energy",
                "industry": "Oil & Gas Integrated",
                "country": "US",
                "exchangeShortName": "NYSE",
                "marketCap": 50_000_000_000,
                "price": 150,
                "volume": 3_000_000,
                "beta": 1.1,
                "dividendYield": 0.025,
            },
        ]

    monkeypatch.setattr("app.services.screener.fetch_company_screener", fake_fetch_company_screener)
    params = screener_params_from_mapping(
        {
            "market_cap_min": "10,000,000,000",
            "market_cap_max": "100000000000",
            "price_min": "10",
            "price_max": "200",
            "volume_min": "1000000",
            "beta_min": "0.5",
            "beta_max": "1.5",
            "dividend_yield_min": "2",
            "sector": "Technology",
            "industry": "Semiconductors",
            "country": "US",
            "exchange": "NASDAQ",
            "page_size": "10",
        }
    )
    engine = _engine()

    with Session(engine) as db:
        response = build_screener_response(db, params)
        reset_response = build_screener_response(db, screener_params_from_mapping({"market_cap_min": "", "sector": "Any"}))

    assert captured_filters[0] == {
        "marketCapMoreThan": 10_000_000_000,
        "marketCapLowerThan": 100_000_000_000,
        "priceMoreThan": 10,
        "priceLowerThan": 200,
        "volumeMoreThan": 1_000_000,
        "betaMoreThan": 0.5,
        "betaLowerThan": 1.5,
        "dividendMoreThan": 2,
        "sector": "Technology",
        "industry": "Semiconductors",
        "country": "US",
        "exchange": "NASDAQ",
    }
    assert [row["symbol"] for row in response["items"]] == ["PASS"]
    assert response["items"][0]["dividend_yield"] == 2.5
    assert "market_cap_min" not in reset_response["filters"]
    assert "sector" not in reset_response["filters"]


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


def test_screener_any_intelligence_values_are_inactive(monkeypatch):
    monkeypatch.setattr(
        "app.services.screener.fetch_company_screener",
        lambda *, filters, limit: [
            {"symbol": "AAA", "companyName": "Aaa Corp", "marketCap": 1, "price": 10, "volume": 1},
            {"symbol": "BBB", "companyName": "Bbb Corp", "marketCap": 1, "price": 11, "volume": 1},
        ],
    )
    params = screener_params_from_mapping(
        {
            "congress_activity": "Any",
            "insider_activity": "any",
            "confirmation_direction": "",
            "confirmation_band": " ",
            "why_now_state": "Any",
            "freshness": "any",
            "options_flow_active": "",
            "institutional_activity_active": "Any",
        }
    )
    engine = _engine()

    with Session(engine) as db:
        response = build_screener_response(db, params)

    inactive_keys = {
        "congress_activity",
        "insider_activity",
        "confirmation_direction",
        "confirmation_band",
        "why_now_state",
        "freshness",
        "options_flow_active",
        "institutional_activity_active",
    }
    assert not inactive_keys.intersection(response["filters"])
    assert {row["symbol"] for row in response["items"]} == {"AAA", "BBB"}


def test_screener_filters_technical_dimensions_and_excludes_missing_values(monkeypatch):
    provider_called = {"count": 0}

    def fake_fetch_company_screener(*, filters, limit):
        provider_called["count"] += 1
        assert "rel_volume_min" not in filters
        return [
            {
                "symbol": "PASS",
                "companyName": "Pass Corp",
                "sector": "Technology",
                "marketCap": 10_000_000_000,
                "price": 20,
                "volume": 1_600_000,
                "avgVolume": 1_000_000,
                "beta": 1.0,
                "changesPercentage": 4.2,
                "rsi": 62,
                "macdState": "Bullish crossover",
                "trendState": "SMA above LMA",
            },
            {
                "symbol": "MISS",
                "companyName": "Miss Corp",
                "sector": "Technology",
                "marketCap": 9_000_000_000,
                "price": 18,
                "volume": 500_000,
                "avgVolume": 1_000_000,
                "beta": 1.0,
                "changesPercentage": -3,
                "rsi": 72,
                "macdState": "Bearish",
                "trendState": "SMA below LMA",
            },
            {
                "symbol": "NODATA",
                "companyName": "No Data Corp",
                "sector": "Technology",
                "marketCap": 8_000_000_000,
                "price": 16,
                "volume": 900_000,
                "beta": 1.0,
            },
        ]

    monkeypatch.setattr("app.services.screener.fetch_company_screener", fake_fetch_company_screener)
    engine = _engine()

    with Session(engine) as db:
        response = build_screener_response(
            db,
            ScreenerParams(
                rel_volume_min=1,
                rel_volume_max=2,
                price_move_min=0,
                price_move_max=10,
                rsi_min=30,
                rsi_max=70,
                macd_state="crossover_bullish",
                trend_state="sma_above_lma",
            ),
        )
        default_response = build_screener_response(db, ScreenerParams())

    assert provider_called["count"] == 2
    assert [row["symbol"] for row in response["items"]] == ["PASS"]
    assert response["items"][0]["rel_volume"] == 1.6
    assert response["items"][0]["price_move_pct"] == 4.2
    assert response["items"][0]["macd_state"] == "crossover_bullish"
    assert response["items"][0]["trend_state"] == "sma_above_lma"
    assert {row["symbol"] for row in default_response["items"]} == {"PASS", "MISS", "NODATA"}


def test_screener_empty_string_technical_params_are_inactive(monkeypatch):
    monkeypatch.setattr(
        "app.services.screener.fetch_company_screener",
        lambda *, filters, limit: [
            {
                "symbol": "NODATA",
                "companyName": "No Data Corp",
                "marketCap": 8_000_000_000,
                "price": 16,
                "volume": 900_000,
                "beta": 1.0,
            }
        ],
    )
    params = screener_params_from_mapping(
        {
            "rel_volume_min": "",
            "rel_volume_max": " ",
            "price_move_min": "",
            "price_move_max": "",
            "rsi_min": "",
            "rsi_max": "",
            "macd_state": "",
            "trend_state": "",
        }
    )
    engine = _engine()

    with Session(engine) as db:
        response = build_screener_response(db, params)

    assert not any(key in response["filters"] for key in ("rel_volume_min", "price_move_min", "rsi_min", "macd_state", "trend_state"))
    assert [row["symbol"] for row in response["items"]] == ["NODATA"]


def test_screener_broad_relative_volume_filter_includes_expected_rows(monkeypatch):
    monkeypatch.setattr(
        "app.services.screener.fetch_company_screener",
        lambda *, filters, limit: [
            {"symbol": "LOW", "companyName": "Low Corp", "marketCap": 1, "price": 10, "volume": 500_000, "avgVolume": 1_000_000},
            {"symbol": "MID", "companyName": "Mid Corp", "marketCap": 1, "price": 10, "volume": 1_600_000, "avgVolume": 1_000_000},
            {"symbol": "HIGH", "companyName": "High Corp", "marketCap": 1, "price": 10, "volume": 2_500_000, "avgVolume": 1_000_000},
            {"symbol": "MISS", "companyName": "Missing Corp", "marketCap": 1, "price": 10, "volume": 1_000_000},
        ],
    )
    engine = _engine()

    with Session(engine) as db:
        response = build_screener_response(db, ScreenerParams(rel_volume_min=0, rel_volume_max=2))

    assert [row["symbol"] for row in response["items"]] == ["MID", "LOW"]
    assert {row["rel_volume"] for row in response["items"]} == {0.5, 1.6}


def test_screener_price_move_filter_normalizes_decimal_and_percent_units(monkeypatch):
    monkeypatch.setattr(
        "app.services.screener.fetch_company_screener",
        lambda *, filters, limit: [
            {"symbol": "DEC", "companyName": "Decimal Move", "marketCap": 1, "price": 10, "volume": 1, "changesPercentage": 0.05},
            {"symbol": "PCT", "companyName": "Percent Move", "marketCap": 1, "price": 10, "volume": 1, "changesPercentage": 5},
            {"symbol": "OUT", "companyName": "Outside Move", "marketCap": 1, "price": 10, "volume": 1, "changesPercentage": 12},
        ],
    )
    engine = _engine()

    with Session(engine) as db:
        response = build_screener_response(db, ScreenerParams(price_move_min=-10, price_move_max=10))

    assert [row["symbol"] for row in response["items"]] == ["PCT", "DEC"]
    assert {row["price_move_pct"] for row in response["items"]} == {5.0}


def test_screener_technical_filters_use_cached_price_history_without_provider_calls(monkeypatch):
    monkeypatch.setattr(
        "app.services.screener.fetch_company_screener",
        lambda *, filters, limit: [
            {"symbol": "CACHE", "companyName": "Cache Corp", "marketCap": 1, "price": 101, "volume": 1_000_000, "avgVolume": 1_000_000},
            {"symbol": "MISS", "companyName": "Missing Corp", "marketCap": 1, "price": 50, "volume": 1_000_000, "avgVolume": 1_000_000},
        ],
    )
    engine = _engine()

    with Session(engine) as db:
        closes = [100 + (index % 2) for index in range(60)]
        _add_price_history(db, "CACHE", closes)
        db.commit()

        response = build_screener_response(db, ScreenerParams(price_move_min=-10, price_move_max=10, rsi_min=30, rsi_max=70))

    assert [row["symbol"] for row in response["items"]] == ["CACHE"]
    assert response["items"][0]["price_move_pct"] is not None
    assert 30 <= response["items"][0]["rsi"] <= 70


def test_screener_relative_volume_uses_local_price_cache_volume_when_available(monkeypatch):
    monkeypatch.setattr(
        "app.services.screener.fetch_company_screener",
        lambda *, filters, limit: [
            {"symbol": "CACHEVOL", "companyName": "Cache Volume Corp", "marketCap": 1, "price": 10, "volume": 200},
            {"symbol": "NOCACHE", "companyName": "No Cache Corp", "marketCap": 1, "price": 10, "volume": 200},
        ],
    )
    engine = _engine()

    with Session(engine) as db:
        start = (datetime.now(timezone.utc) - timedelta(days=5)).date()
        for index in range(5):
            db.execute(
                text("insert into price_cache (symbol, date, close, volume) values (:symbol, :date, :close, :volume)"),
                {
                    "symbol": "CACHEVOL",
                    "date": (start + timedelta(days=index)).isoformat(),
                    "close": 10 + index,
                    "volume": 100,
                },
            )
        db.commit()

        response = build_screener_response(db, ScreenerParams(rel_volume_min=1.5, rel_volume_max=2.5))

    assert [row["symbol"] for row in response["items"]] == ["CACHEVOL"]
    assert response["items"][0]["avg_volume"] == 100
    assert response["items"][0]["rel_volume"] == 2


def test_screener_any_technical_dropdown_values_are_inactive(monkeypatch):
    monkeypatch.setattr(
        "app.services.screener.fetch_company_screener",
        lambda *, filters, limit: [
            {"symbol": "AAA", "companyName": "Aaa Corp", "marketCap": 1, "price": 10, "volume": 1},
            {"symbol": "BBB", "companyName": "Bbb Corp", "marketCap": 1, "price": 11, "volume": 1},
        ],
    )
    params = screener_params_from_mapping({"macd_state": "Any", "trend_state": "any"})
    engine = _engine()

    with Session(engine) as db:
        response = build_screener_response(db, params)

    assert not any(key in response["filters"] for key in ("macd_state", "trend_state"))
    assert {row["symbol"] for row in response["items"]} == {"AAA", "BBB"}


def test_screener_filters_fundamental_dimensions_and_excludes_missing_values(monkeypatch):
    monkeypatch.setattr(
        "app.services.screener.fetch_company_screener",
        lambda *, filters, limit: [
            {
                "symbol": "QUALITY",
                "companyName": "Quality Corp",
                "sector": "Healthcare",
                "marketCap": 20_000_000_000,
                "price": 40,
                "volume": 2_000_000,
                "beta": 0.9,
                "pe": 18,
                "forwardPE": 16,
                "priceToSalesRatio": 4.5,
                "enterpriseValueOverEBITDA": 12,
                "revenueGrowth": 0.12,
                "epsGrowth": 15,
                "grossMargin": 0.7,
                "operatingMargin": 26,
                "netMargin": 18,
                "returnOnEquity": 0.22,
                "returnOnInvestedCapital": 14,
                "debtToEquity": 0.5,
                "epsTTM": 3.2,
                "freeCashFlow": 1_500_000_000,
                "freeCashFlowMargin": 0.18,
            },
            {
                "symbol": "RICH",
                "companyName": "Rich Corp",
                "sector": "Healthcare",
                "marketCap": 18_000_000_000,
                "price": 38,
                "volume": 1_700_000,
                "beta": 1.1,
                "pe": 45,
                "forwardPE": 35,
                "priceToSalesRatio": 14,
                "enterpriseValueOverEBITDA": 30,
                "revenueGrowth": 0.03,
                "epsGrowth": 2,
                "grossMargin": 0.4,
                "operatingMargin": 9,
                "netMargin": 6,
                "returnOnEquity": 0.08,
                "returnOnInvestedCapital": 5,
                "debtToEquity": 2.5,
                "epsTTM": 0.8,
                "freeCashFlow": 100_000_000,
                "freeCashFlowMargin": 0.03,
            },
            {
                "symbol": "MISSING",
                "companyName": "Missing Corp",
                "sector": "Healthcare",
                "marketCap": 16_000_000_000,
                "price": 30,
                "volume": 1_200_000,
                "beta": 1.0,
            },
        ],
    )
    engine = _engine()

    with Session(engine) as db:
        response = build_screener_response(
            db,
            ScreenerParams(
                trailing_pe_min=10,
                trailing_pe_max=25,
                forward_pe_max=20,
                price_sales_max=6,
                ev_ebitda_max=15,
                revenue_growth_min=10,
                eps_growth_min=10,
                gross_margin_min=60,
                operating_margin_min=20,
                net_margin_min=10,
                roe_min=15,
                roic_min=10,
                debt_equity_max=1,
                eps_ttm_min=1,
                fcf_min=1_000_000_000,
                fcf_margin_min=10,
            ),
        )
        default_response = build_screener_response(db, ScreenerParams())

    assert [row["symbol"] for row in response["items"]] == ["QUALITY"]
    assert response["items"][0]["price_sales"] == 4.5
    assert response["items"][0]["ev_ebitda"] == 12
    assert response["items"][0]["revenue_growth"] == 12
    assert response["items"][0]["gross_margin"] == 70
    assert {row["symbol"] for row in default_response["items"]} == {"QUALITY", "RICH", "MISSING"}


FUNDAMENTAL_RANGE_CASES = [
    ("trailing_pe", "trailing_pe", 18, 8, 35),
    ("forward_pe", "forward_pe", 16, 6, 28),
    ("price_to_sales", "price_sales", 4.5, 1.2, 9),
    ("ev_to_ebitda", "ev_ebitda", 12, 4, 24),
    ("gross_margin", "gross_margin", 55, 30, 80),
    ("operating_margin", "operating_margin", 22, 8, 40),
    ("net_margin", "net_margin", 14, 3, 26),
    ("roe", "roe", 18, 4, 34),
    ("roic", "roic", 12, 2, 24),
    ("revenue_growth", "revenue_growth", 20, 4, 45),
    ("eps_growth", "eps_growth", 18, 3, 42),
    ("debt_to_equity", "debt_equity", 0.8, 0.2, 2.5),
    ("current_ratio", "current_ratio", 1.8, 0.8, 3.2),
    ("free_cash_flow", "fcf", 500_000_000, 100_000_000, 2_000_000_000),
    ("fcf_margin", "fcf_margin", 16, 3, 32),
]


def _fundamental_fixture_rows(row_field: str, *, passing: float, below: float, above: float) -> list[dict]:
    base = {
        "companyName": "Fundamental Fixture",
        "marketCap": 10_000_000_000,
        "price": 50,
        "volume": 2_000_000,
    }
    return [
        {**base, "symbol": "PASS", row_field: passing},
        {**base, "symbol": "LOW", row_field: below},
        {**base, "symbol": "HIGH", row_field: above},
        {**base, "symbol": "NULL", row_field: None},
    ]


@pytest.mark.parametrize("param_base,row_field,passing,below,above", FUNDAMENTAL_RANGE_CASES)
def test_screener_fundamental_min_max_filters_exclude_out_of_range_and_nulls(
    monkeypatch,
    param_base,
    row_field,
    passing,
    below,
    above,
):
    monkeypatch.setattr(
        "app.services.screener.fetch_company_screener",
        lambda *, filters, limit: _fundamental_fixture_rows(row_field, passing=passing, below=below, above=above),
    )
    engine = _engine()
    min_value = (below + passing) / 2
    max_value = (passing + above) / 2

    with Session(engine) as db:
        default_response = build_screener_response(db, ScreenerParams())
        min_response = build_screener_response(db, screener_params_from_mapping({f"{param_base}_min": str(min_value)}))
        max_response = build_screener_response(db, screener_params_from_mapping({f"{param_base}_max": str(max_value)}))

    assert {row["symbol"] for row in default_response["items"]} == {"PASS", "LOW", "HIGH", "NULL"}
    assert {row["symbol"] for row in min_response["items"]} == {"PASS", "HIGH"}
    assert {row["symbol"] for row in max_response["items"]} == {"PASS", "LOW"}
    assert f"{param_base}_min" in min_response["filters"]
    assert f"{param_base}_max" in max_response["filters"]


def test_forward_pe_filter_is_independent_of_trailing_pe(monkeypatch):
    monkeypatch.setattr(
        "app.services.screener.fetch_company_screener",
        lambda *, filters, limit: [
            {
                "symbol": "PASS",
                "companyName": "Forward Pass",
                "marketCap": 10_000_000_000,
                "price": 50,
                "volume": 2_000_000,
                "trailing_pe": 80,
                "forward_pe": 16,
            },
            {
                "symbol": "FAIL",
                "companyName": "Forward Fail",
                "marketCap": 10_000_000_000,
                "price": 50,
                "volume": 2_000_000,
                "trailing_pe": 8,
                "forward_pe": 30,
            },
        ],
    )
    engine = _engine()

    with Session(engine) as db:
        response = build_screener_response(db, screener_params_from_mapping({"forward_pe_max": "20"}))

    assert [row["symbol"] for row in response["items"]] == ["PASS"]


def test_screener_fundamental_aliases_parse_to_canonical_response_filters(monkeypatch):
    monkeypatch.setattr(
        "app.services.screener.fetch_company_screener",
        lambda *, filters, limit: [
            {
                "symbol": "PASS",
                "companyName": "Alias Pass",
                "marketCap": 10_000_000_000,
                "price": 50,
                "volume": 2_000_000,
                "price_sales": 1.5,
                "ev_ebitda": 9,
                "debt_equity": 0.7,
                "net_debt_ebitda": 1.2,
                "fcf": 750_000_000,
            }
        ],
    )
    params = screener_params_from_mapping(
        {
            "price_sales_max": "2",
            "ev_ebitda_max": "10",
            "debt_equity_max": "1",
            "net_debt_ebitda_max": "2",
            "fcf_min": "500,000,000",
        }
    )
    engine = _engine()

    with Session(engine) as db:
        response = build_screener_response(db, params)

    assert [row["symbol"] for row in response["items"]] == ["PASS"]
    assert "price_to_sales_max" in response["filters"]
    assert "ev_to_ebitda_max" in response["filters"]
    assert "debt_to_equity_max" in response["filters"]
    assert "net_debt_to_ebitda_max" in response["filters"]
    assert "free_cash_flow_min" in response["filters"]


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
        == "Symbol,Company,Sector,Industry,Country,Exchange,Market Cap,Price,Volume,Beta,Congress Activity,Insider Activity,Confirmation Score,Confirmation Direction,Confirmation Band,Confirmation Status,Why Now State,Why Now Headline,Freshness State,Government Contracts Active,Government Contracts Score Contribution,Government Contracts Count,Government Contracts Total Amount,Government Contracts Largest Amount,Government Contracts Latest Date,Government Contracts Top Agency,Options Flow Active,Options Flow Score,Options Flow Direction,Options Flow Intensity,Options Flow Total Premium,Options Flow Latest Date,Institutional Activity Active,Institutional Activity Direction,Institutional Activity Net Activity,Institutional Activity Total Value,Institutional Activity Latest Date,Institutional Activity Status"
    )
    assert "ALIGN,Alignment Inc,Healthcare,Biotechnology,US,NASDAQ,5000000000,30,1500000,1.1" in lines[1]
    assert ",Bullish," in lines[1]


def test_directional_confirmation_filters_reject_opposite_and_inactive_states(monkeypatch):
    monkeypatch.setattr(
        "app.services.screener.fetch_company_screener",
        lambda *, filters, limit: [
            {"symbol": "BULL", "companyName": "Bull Co", "marketCap": 1_000_000_000, "price": 10, "volume": 1_000_000},
            {"symbol": "BEAR", "companyName": "Bear Co", "marketCap": 1_000_000_000, "price": 10, "volume": 1_000_000},
            {"symbol": "IDLE", "companyName": "Idle Co", "marketCap": 1_000_000_000, "price": 10, "volume": 1_000_000},
        ],
    )
    monkeypatch.setattr(
        "app.services.confirmation_context.get_confirmation_score_bundles_for_tickers",
        lambda *_args, **_kwargs: {
            "BULL": _confirmation_bundle("BULL", score=72, band="strong", direction="bullish"),
            "BEAR": _confirmation_bundle("BEAR", score=72, band="strong", direction="bearish"),
            "IDLE": _confirmation_bundle("IDLE", score=0, band="inactive", direction="neutral", present=False),
        },
    )
    engine = _engine()

    with Session(engine) as db:
        bullish = build_screener_response(
            db,
            ScreenerParams(confirmation_direction="bullish", confirmation_score_min=60, sort="confirmation_score"),
        )
        bearish = build_screener_response(
            db,
            ScreenerParams(confirmation_direction="bearish", confirmation_score_min=60, sort="confirmation_score"),
        )

    assert [row["symbol"] for row in bullish["items"]] == ["BULL"]
    assert [row["symbol"] for row in bearish["items"]] == ["BEAR"]


def test_screener_government_contract_filters_and_row_fields(monkeypatch):
    monkeypatch.setattr(
        "app.services.screener.fetch_company_screener",
        lambda *, filters, limit: [
            {
                "symbol": "GOVT",
                "companyName": "Govt Corp",
                "sector": "Industrials",
                "marketCap": 8_000_000_000,
                "price": 28,
                "volume": 2_400_000,
                "beta": 0.9,
                "country": "US",
                "exchangeShortName": "NYSE",
            },
            {
                "symbol": "SMOL",
                "companyName": "Smol Corp",
                "sector": "Industrials",
                "marketCap": 4_000_000_000,
                "price": 19,
                "volume": 900_000,
                "beta": 1.0,
                "country": "US",
                "exchangeShortName": "NASDAQ",
            },
        ],
    )
    engine = _engine()

    with Session(engine) as db:
        db.add(
            _government_contract(
                contract_id=10,
                symbol="GOVT",
                days_ago=10,
                award_amount=12_000_000,
                awarding_agency="Department of Defense",
            )
        )
        db.add(
            _government_contract(
                contract_id=11,
                symbol="SMOL",
                days_ago=400,
                award_amount=800_000,
                awarding_agency="NASA",
            )
        )
        db.commit()

        response = build_screener_response(
            db,
            ScreenerParams(
                government_contracts_active=True,
                government_contracts_min_amount=10_000_000,
                government_contracts_lookback_days=365,
            ),
        )

    assert [row["symbol"] for row in response["items"]] == ["GOVT"]
    row = response["items"][0]
    assert row["government_contracts_active"] is True
    assert row["government_contracts_score_contribution"] == 13
    assert row["government_contracts_count"] == 1
    assert row["government_contracts_total_amount"] == 12_000_000
    assert row["government_contracts_top_agency"] == "Department of Defense"
    assert row["government_contracts_status"] == "ok"


def test_government_contract_aggregate_returns_known_symbols_from_local_index():
    engine = _engine()
    with Session(engine) as db:
        db.add_all(
            [
                _government_contract(
                    contract_id=21,
                    symbol="LMT",
                    days_ago=5,
                    award_amount=14_000_000,
                    awarding_agency="Department of Defense",
                ),
                _government_contract(
                    contract_id=22,
                    symbol="RTX",
                    days_ago=8,
                    award_amount=9_500_000,
                    awarding_agency="Air Force",
                ),
            ]
        )
        db.commit()

        summaries = get_government_contracts_summaries_for_symbols(
            db,
            ["lmt", "rtx", "ba"],
            lookback_days=365,
            min_amount=1_000_000,
        )

    assert summaries["LMT"]["active"] is True
    assert summaries["LMT"]["contract_count"] == 1
    assert summaries["LMT"]["total_award_amount"] == 14_000_000
    assert summaries["RTX"]["active"] is True
    assert summaries["RTX"]["contract_count"] == 1
    assert summaries["BA"]["active"] is False


def test_screener_and_bundle_share_government_contract_score_contribution(monkeypatch):
    monkeypatch.setattr(
        "app.services.screener.fetch_company_screener",
        lambda *, filters, limit: [
            {
                "symbol": "SYNC",
                "companyName": "Sync Corp",
                "sector": "Industrials",
                "marketCap": 9_000_000_000,
                "price": 44,
                "volume": 1_800_000,
                "beta": 1.0,
                "country": "US",
                "exchangeShortName": "NYSE",
            }
        ],
    )
    engine = _engine()

    with Session(engine) as db:
        db.add(
            _government_contract(
                contract_id=24,
                symbol="SYNC",
                days_ago=4,
                award_amount=55_000_000,
                awarding_agency="Department of Defense",
            )
        )
        db.commit()

        response = build_screener_response(
            db,
            ScreenerParams(
                government_contracts_min_amount=1_000_000,
                government_contracts_lookback_days=365,
            ),
        )
        summary = get_government_contracts_summaries_for_symbols(
            db,
            ["SYNC"],
            lookback_days=365,
            min_amount=1_000_000,
        )["SYNC"]
        bundle = get_confirmation_score_bundle_for_ticker(db, "SYNC", lookback_days=30)

    row = response["items"][0]
    assert row["government_contracts_score_contribution"] == summary["score_contribution"] == 20
    assert bundle["sources"]["government_contracts"]["score_contribution"] == row["government_contracts_score_contribution"]
    assert row["confirmation"]["score"] is not None


def test_screener_government_contract_filter_applies_before_pagination(monkeypatch):
    captured: dict[str, int] = {}

    def fake_fetch_company_screener(*, filters, limit):
        captured["limit"] = limit
        return [
            {
                "symbol": f"T{index:03d}",
                "companyName": f"Ticker {index}",
                "sector": "Industrials",
                "marketCap": 1_000_000_000 + index,
                "price": 20 + index,
                "volume": 1_500_000 + index,
                "beta": 1.0,
                "country": "US",
                "exchangeShortName": "NYSE",
            }
            for index in range(60)
        ]

    monkeypatch.setattr("app.services.screener.fetch_company_screener", fake_fetch_company_screener)
    engine = _engine()
    with Session(engine) as db:
        db.add(
            _government_contract(
                contract_id=30,
                symbol="T055",
                days_ago=4,
                award_amount=6_000_000,
                awarding_agency="Department of Defense",
            )
        )
        db.commit()

        response = build_screener_response(
            db,
            ScreenerParams(
                page=1,
                page_size=50,
                government_contracts_active=True,
                government_contracts_min_amount=1_000_000,
                government_contracts_lookback_days=365,
            ),
        )

    assert captured["limit"] == 500
    assert response["total_available"] == 1
    assert [row["symbol"] for row in response["items"]] == ["T055"]


def test_empty_government_contract_index_returns_unavailable_metadata(monkeypatch):
    monkeypatch.setattr(
        "app.services.screener.fetch_company_screener",
        lambda *, filters, limit: [
            {
                "symbol": "MSFT",
                "companyName": "Microsoft Corporation",
                "sector": "Technology",
                "marketCap": 3_000_000_000_000,
                "price": 420,
                "volume": 30_000_000,
                "beta": 1.1,
                "country": "US",
                "exchangeShortName": "NASDAQ",
            }
        ],
    )
    engine = _engine()

    with Session(engine) as db:
        response = build_screener_response(
            db,
            ScreenerParams(
                government_contracts_active=True,
                government_contracts_min_amount=1_000_000,
                government_contracts_lookback_days=365,
            ),
        )

    row = response["items"][0]
    assert response["overlay_availability"]["government_contracts"]["status"] == "unavailable"
    assert response["overlay_availability"]["government_contracts"]["reason"] == "empty_dataset"
    assert response["ignored_filters"] == ["government_contracts_active"]
    assert row["government_contracts_status"] == "unavailable"
    assert row["government_contracts_active"] is None
    assert row["government_contracts_total_amount"] is None


def test_ticker_government_contracts_endpoint_returns_local_summary():
    engine = _engine()
    with Session(engine) as db:
        db.add_all(
            [
                _government_contract(
                    contract_id=41,
                    symbol="LMT",
                    days_ago=6,
                    award_amount=11_000_000,
                    awarding_agency="Department of Defense",
                    description="Missile systems support",
                ),
                _government_contract(
                    contract_id=42,
                    symbol="LMT",
                    days_ago=18,
                    award_amount=4_000_000,
                    awarding_agency="Navy",
                    description="Radar modernization",
                ),
            ]
        )
        db.commit()

        payload = ticker_government_contracts(
            symbol="LMT",
            lookback_days=365,
            min_amount=1_000_000,
            limit=10,
            db=db,
        )

    assert payload["status"] == "ok"
    assert payload["contract_count"] == 2
    assert payload["total_award_amount"] == 15_000_000
    assert payload["top_agency"] == "Department of Defense"
    assert len(payload["items"]) == 2


def test_screener_options_flow_filters_degrade_gracefully_when_local_data_is_unavailable(monkeypatch):
    monkeypatch.setattr(
        "app.services.screener.fetch_company_screener",
        lambda *, filters, limit: [
            {
                "symbol": "FLOW",
                "companyName": "Flow Corp",
                "sector": "Technology",
                "marketCap": 15_000_000_000,
                "price": 55,
                "volume": 3_000_000,
                "beta": 1.3,
                "country": "US",
                "exchangeShortName": "NASDAQ",
            }
        ],
    )
    monkeypatch.setattr("app.services.confirmation_score.get_options_flow_summary", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("live options flow should not run in screener batches")))
    engine = _engine()

    with Session(engine) as db:
        response = build_screener_response(
            db,
            ScreenerParams(
                options_flow_active=True,
                options_flow_direction="bullish",
                options_flow_min_score=65,
                options_flow_min_premium=500_000,
            ),
        )

    assert response["overlay_availability"]["options_flow"]["status"] == "unavailable"
    assert response["ignored_filters"] == [
        "options_flow_active",
        "options_flow_direction",
        "options_flow_min_score",
        "options_flow_min_premium",
    ]
    assert response["items"][0]["options_flow_status"] == "unavailable"


def test_screener_institutional_overlay_defaults_to_not_configured(monkeypatch):
    monkeypatch.setattr(
        "app.services.screener.fetch_company_screener",
        lambda *, filters, limit: [
            {
                "symbol": "INST",
                "companyName": "Institution Corp",
                "sector": "Technology",
                "marketCap": 11_000_000_000,
                "price": 44,
                "volume": 2_200_000,
                "beta": 1.0,
                "country": "US",
                "exchangeShortName": "NASDAQ",
            }
        ],
    )
    engine = _engine()

    with Session(engine) as db:
        response = build_screener_response(
            db,
            ScreenerParams(
                institutional_activity_active=True,
                institutional_activity_direction="bullish",
                institutional_activity_min_value=1_000_000,
            ),
        )

    assert response["overlay_availability"]["institutional_activity"]["status"] == "not_configured"
    assert response["items"][0]["institutional_activity_status"] == "not_configured"


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
