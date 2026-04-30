from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from starlette.requests import Request

from app.db import Base
from app.main import ticker_government_contracts
from app.models import Event
from app.routers.screener import stock_screener_export
from app.services.government_contracts import get_government_contracts_summaries_for_symbols
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
        == "Symbol,Company,Sector,Industry,Country,Exchange,Market Cap,Price,Volume,Beta,Congress Activity,Insider Activity,Confirmation Score,Confirmation Direction,Confirmation Band,Confirmation Status,Why Now State,Why Now Headline,Freshness State,Government Contracts Active,Government Contracts Count,Government Contracts Total Amount,Government Contracts Largest Amount,Government Contracts Latest Date,Government Contracts Top Agency,Options Flow Active,Options Flow Score,Options Flow Direction,Options Flow Intensity,Options Flow Total Premium,Options Flow Latest Date,Institutional Activity Active,Institutional Activity Direction,Institutional Activity Net Activity,Institutional Activity Total Value,Institutional Activity Latest Date,Institutional Activity Status"
    )
    assert "ALIGN,Alignment Inc,Healthcare,Biotechnology,US,NASDAQ,5000000000,30,1500000,1.1" in lines[1]
    assert ",Bullish," in lines[1]


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

    now = datetime.now(timezone.utc)
    with Session(engine) as db:
        db.add(
            Event(
                id=10,
                event_type="government_contract",
                ts=now - timedelta(days=10),
                event_date=None,
                symbol="GOVT",
                source="usaspending",
                amount_min=12_000_000,
                amount_max=12_000_000,
                payload_json=json.dumps(
                    {
                        "symbol": "GOVT",
                        "award_date": (now - timedelta(days=10)).date().isoformat(),
                        "award_amount": 12_000_000,
                        "awarding_agency": "Department of Defense",
                    }
                ),
            )
        )
        db.add(
            Event(
                id=11,
                event_type="government_contract",
                ts=now - timedelta(days=400),
                event_date=None,
                symbol="SMOL",
                source="usaspending",
                amount_min=800_000,
                amount_max=800_000,
                payload_json=json.dumps(
                    {
                        "symbol": "SMOL",
                        "award_date": (now - timedelta(days=400)).date().isoformat(),
                        "award_amount": 800_000,
                        "awarding_agency": "NASA",
                    }
                ),
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
    assert row["government_contracts_count"] == 1
    assert row["government_contracts_total_amount"] == 12_000_000
    assert row["government_contracts_top_agency"] == "Department of Defense"
    assert row["government_contracts_status"] == "ok"


def test_government_contract_aggregate_returns_known_symbols_from_local_index():
    engine = _engine()
    now = datetime.now(timezone.utc)

    with Session(engine) as db:
        db.add_all(
            [
                Event(
                    id=21,
                    event_type="government_contract",
                    ts=now - timedelta(days=5),
                    event_date=None,
                    symbol="LMT",
                    source="usaspending",
                    amount_min=14_000_000,
                    amount_max=14_000_000,
                    payload_json=json.dumps(
                        {
                            "symbol": "LMT",
                            "award_date": (now - timedelta(days=5)).date().isoformat(),
                            "award_amount": 14_000_000,
                            "awarding_agency": "Department of Defense",
                        }
                    ),
                ),
                Event(
                    id=22,
                    event_type="government_contract",
                    ts=now - timedelta(days=8),
                    event_date=None,
                    symbol="RTX",
                    source="usaspending",
                    amount_min=9_500_000,
                    amount_max=9_500_000,
                    payload_json=json.dumps(
                        {
                            "symbol": "RTX",
                            "award_date": (now - timedelta(days=8)).date().isoformat(),
                            "award_amount": 9_500_000,
                            "awarding_agency": "Air Force",
                        }
                    ),
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
    now = datetime.now(timezone.utc)

    with Session(engine) as db:
        db.add(
            Event(
                id=30,
                event_type="government_contract",
                ts=now - timedelta(days=4),
                event_date=None,
                symbol="T055",
                source="usaspending",
                amount_min=6_000_000,
                amount_max=6_000_000,
                payload_json=json.dumps(
                    {
                        "symbol": "T055",
                        "award_date": (now - timedelta(days=4)).date().isoformat(),
                        "award_amount": 6_000_000,
                        "awarding_agency": "Department of Defense",
                    }
                ),
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
    now = datetime.now(timezone.utc)

    with Session(engine) as db:
        db.add_all(
            [
                Event(
                    id=41,
                    event_type="government_contract",
                    ts=now - timedelta(days=6),
                    event_date=None,
                    symbol="LMT",
                    source="usaspending",
                    amount_min=11_000_000,
                    amount_max=11_000_000,
                    payload_json=json.dumps(
                        {
                            "symbol": "LMT",
                            "award_date": (now - timedelta(days=6)).date().isoformat(),
                            "award_amount": 11_000_000,
                            "awarding_agency": "Department of Defense",
                            "description": "Missile systems support",
                        }
                    ),
                ),
                Event(
                    id=42,
                    event_type="government_contract",
                    ts=now - timedelta(days=18),
                    event_date=None,
                    symbol="LMT",
                    source="usaspending",
                    amount_min=4_000_000,
                    amount_max=4_000_000,
                    payload_json=json.dumps(
                        {
                            "symbol": "LMT",
                            "award_date": (now - timedelta(days=18)).date().isoformat(),
                            "award_amount": 4_000_000,
                            "awarding_agency": "Navy",
                            "description": "Radar modernization",
                        }
                    ),
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
