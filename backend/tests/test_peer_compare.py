from __future__ import annotations

from datetime import datetime, timezone

import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

import app.main as main_module
from app.db import Base
from app.entitlements import ENTITLEMENTS
from app.models import FundamentalsCache, TickerFinancialsCache


def _engine():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    return engine


def _fundamentals(symbol: str, *, revenue_growth: float, roe: float, forward_pe: float) -> FundamentalsCache:
    return FundamentalsCache(
        symbol=symbol,
        provider="fmp",
        fetched_at=datetime.now(timezone.utc),
        status="ok",
        company_name=f"{symbol} Inc.",
        sector="Technology",
        revenue_growth=revenue_growth,
        eps_growth=revenue_growth,
        gross_margin=55,
        operating_margin=28,
        roe=roe,
        net_debt_to_ebitda=0.5,
        forward_pe=forward_pe,
        trailing_pe=forward_pe + 2,
        ev_to_ebitda=forward_pe / 2,
        price_to_sales=8,
        fcf_yield=4,
    )


def test_peer_compare_rejects_same_symbol():
    engine = _engine()
    with Session(engine) as db:
        with pytest.raises(HTTPException) as exc:
            main_module._build_peer_compare_payload(db, "MU", "MU", entitlements=ENTITLEMENTS["free"], authenticated=False)

    assert exc.value.status_code == 422
    assert "different" in str(exc.value.detail).lower()


def test_peer_compare_free_tier_returns_teaser_only(monkeypatch):
    engine = _engine()
    with Session(engine) as db:
        db.add_all(
            [
                _fundamentals("AAA", revenue_growth=24, roe=30, forward_pe=18),
                _fundamentals("BBB", revenue_growth=8, roe=12, forward_pe=32),
            ]
        )
        db.commit()

        monkeypatch.setattr(
            main_module,
            "_ticker_price_volume_summary",
            lambda *_args, **_kwargs: pytest.fail("locked compare should not load price/volume"),
        )
        monkeypatch.setattr(
            main_module,
            "get_government_contracts_summary",
            lambda *_args, **_kwargs: pytest.fail("locked compare should not load government data"),
        )
        monkeypatch.setattr(
            main_module,
            "_ticker_confirmation_context",
            lambda *_args, **_kwargs: pytest.fail("locked compare should not load confirmation context"),
        )

        payload = main_module._build_peer_compare_payload(
            db,
            "AAA",
            "BBB",
            entitlements=ENTITLEMENTS["free"],
            authenticated=False,
        )

    assert payload["status"] == "locked"
    assert payload["access"]["required_plan"] == "premium"
    assert payload["call"]["winner"] == "even"
    assert payload["call"]["symbol"] is None
    assert payload["call"]["score"] is None
    assert payload["tradeoffs"] == []
    by_key = {category["key"]: category for category in payload["categories"]}
    for key in ("business_quality", "valuation", "price_volume", "confirmation_score"):
        assert by_key[key]["locked"] is True
        assert by_key[key]["required_plan"] == "premium"
        assert by_key[key]["metrics"] == []
        assert by_key[key]["edge"] == "even"
        assert by_key[key]["score"] is None
    assert by_key["institutional_activity"]["required_plan"] == "pro"
    assert by_key["options_flow"]["required_plan"] == "pro"


def test_peer_compare_includes_analyst_consensus_category(monkeypatch):
    engine = _engine()
    with Session(engine) as db:
        db.add_all(
            [
                _fundamentals("AAA", revenue_growth=20, roe=25, forward_pe=18),
                _fundamentals("BBB", revenue_growth=12, roe=16, forward_pe=28),
            ]
        )
        db.commit()

        monkeypatch.setattr(
            main_module,
            "_ticker_price_volume_summary",
            lambda _db, _symbol: {"direction": "neutral", "change_pct_1d": 0.0, "volume_vs_avg": 1.0},
        )
        monkeypatch.setattr(main_module, "get_government_contracts_summary", lambda *_args, **_kwargs: {"status": "ok", "contract_count": 0, "total_award_amount": 0})
        monkeypatch.setattr(
            main_module,
            "_ticker_confirmation_context",
            lambda _db, symbol: {
                "confirmation_score_bundle": {
                    "score": 80 if symbol == "AAA" else 50,
                    "direction": "bullish" if symbol == "AAA" else "neutral",
                }
            },
        )
        monkeypatch.setattr(main_module, "_redact_locked_ticker_confirmation_sources", lambda bundle, _source_entitlements: bundle)
        monkeypatch.setattr(
            main_module,
            "compare_consensus_payload",
            lambda *_args, **_kwargs: {
                "items": {
                    "AAA": {
                        "summary": {
                            "recommendationLabel": "Bullish",
                            "weightedRatingValue": 1.2,
                            "consensusImpliedUpsidePct": 32.0,
                            "targetDispersionPct": 20.0,
                            "availabilityStatus": "available",
                        },
                        "currentSnapshot": {"recommendationDistribution": {"total": 24}},
                    },
                    "BBB": {
                        "summary": {
                            "recommendationLabel": "Neutral",
                            "weightedRatingValue": 0.1,
                            "consensusImpliedUpsidePct": 8.0,
                            "targetDispersionPct": 45.0,
                            "availabilityStatus": "available",
                        },
                        "currentSnapshot": {"recommendationDistribution": {"total": 9}},
                    },
                }
            },
        )

        payload = main_module._build_peer_compare_payload(
            db,
            "AAA",
            "BBB",
            entitlements=ENTITLEMENTS["premium"],
            authenticated=True,
        )

    by_key = {category["key"]: category for category in payload["categories"]}
    analyst = by_key["analyst_consensus"]
    assert analyst["label"] == "Analysts"
    assert analyst["edge"] == "left"
    metrics = {metric["key"]: metric for metric in analyst["metrics"]}
    assert metrics["consensus_upside"]["left"] == 32.0
    assert metrics["rating_count"]["right"] == 9
    assert any("agrees with Walnut confirmation" in note for note in payload["notes"])


def test_peer_compare_uses_financials_cache_for_forward_metrics(monkeypatch):
    engine = _engine()
    with Session(engine) as db:
        db.add_all(
            [
                _fundamentals("AAA", revenue_growth=24, roe=30, forward_pe=18),
                _fundamentals("BBB", revenue_growth=8, roe=12, forward_pe=32),
                TickerFinancialsCache(
                    symbol="AAA",
                    status="ok",
                    fetched_at=datetime.now(timezone.utc),
                    payload_json='{"summary":{"revenueTtm":1000000000,"epsTtm":4.25,"priceToBook":3.2,"navPerShare":18.5,"forwardPE":15,"forwardPESource":"price_over_estimated_eps","expectedEpsGrowthRatePercent":21},"valuation_metrics":{"forward_pe":15,"forward_pe_source":"price_over_estimated_eps","expected_eps_growth_rate_percent":21,"status":"ok"}}',
                ),
                TickerFinancialsCache(
                    symbol="BBB",
                    status="ok",
                    fetched_at=datetime.now(timezone.utc),
                    payload_json='{"summary":{"revenueTtm":500000000,"epsTtm":1.1,"priceToBook":7.4,"navPerShare":12.25,"forwardPE":28,"forwardPESource":"price_over_estimated_eps","expectedEpsGrowthRatePercent":9},"valuation_metrics":{"forward_pe":28,"forward_pe_source":"price_over_estimated_eps","expected_eps_growth_rate_percent":9,"status":"ok"}}',
                ),
            ]
        )
        db.flush()
        db.query(FundamentalsCache).filter(FundamentalsCache.symbol == "AAA").update({"forward_pe": None, "eps_growth": None})
        db.query(FundamentalsCache).filter(FundamentalsCache.symbol == "BBB").update({"forward_pe": None, "eps_growth": None})
        db.commit()

        monkeypatch.setattr(
            main_module,
            "_ticker_price_volume_summary",
            lambda _db, _symbol: {"direction": "neutral", "change_pct_1d": 0.0, "volume_vs_avg": 1.0},
        )
        monkeypatch.setattr(main_module, "get_government_contracts_summary", lambda *_args, **_kwargs: {"status": "ok", "contract_count": 0, "total_award_amount": 0})

        payload = main_module._build_peer_compare_payload(
            db,
            "AAA",
            "BBB",
            entitlements=ENTITLEMENTS["premium"],
            authenticated=True,
        )

    by_key = {category["key"]: category for category in payload["categories"]}
    business_metrics = {metric["key"]: metric for metric in by_key["business_quality"]["metrics"]}
    valuation_metrics = {metric["key"]: metric for metric in by_key["valuation"]["metrics"]}
    assert business_metrics["revenue_ttm"]["left"] == 1_000_000_000
    assert business_metrics["eps_ttm"]["left"] == 4.25
    assert business_metrics["eps_growth"]["left"] == 21
    assert business_metrics["eps_growth"]["right"] == 9
    assert valuation_metrics["forward_pe"]["left"] == 15
    assert valuation_metrics["forward_pe"]["right"] == 28
    assert valuation_metrics["price_to_book"]["left"] == 3.2
    assert valuation_metrics["price_to_book"]["right"] == 7.4
    assert valuation_metrics["price_to_book"]["edge"] == "left"
    assert valuation_metrics["nav_per_share"]["left"] == 18.5
    assert valuation_metrics["nav_per_share"]["right"] == 12.25


def test_peer_compare_business_quality_weights_profitability_and_scale_over_growth():
    nvda = _fundamentals("NVDA", revenue_growth=65.5, roe=111.7, forward_pe=22)
    nvda.eps_growth = 66
    nvda.gross_margin = 74.1
    nvda.operating_margin = 64
    nvda.eps_ttm = 3.25
    nvda.net_debt_to_ebitda = 0
    bmnr = _fundamentals("BMNR", revenue_growth=84.1, roe=-84.6, forward_pe=34)
    bmnr.eps_growth = 104
    bmnr.gross_margin = 83.5
    bmnr.operating_margin = -145.1
    bmnr.eps_ttm = -0.55
    bmnr.net_debt_to_ebitda = 0.04

    category = main_module._peer_compare_business_category(
        nvda,
        bmnr,
        left_fallbacks={"revenue_ttm": 130_000_000_000},
        right_fallbacks={"revenue_ttm": 328_000_000},
    )

    metrics = {metric["key"]: metric for metric in category["metrics"]}
    assert category["edge"] == "left"
    assert metrics["revenue_ttm"]["edge"] == "left"
    assert metrics["eps_ttm"]["edge"] == "left"
    assert metrics["operating_margin"]["edge"] == "left"
    assert metrics["roe"]["edge"] == "left"
    assert metrics["revenue_growth"]["edge"] == "right"
    assert metrics["eps_growth"]["edge"] == "right"


def test_peer_compare_price_volume_includes_rsi_and_macd():
    category = main_module._peer_compare_price_volume_category(
        {
            "direction": "bullish",
            "change_pct_1d": 2.0,
            "volume_vs_avg": 1.4,
            "rsi": {"status": "ok", "signal": "bullish", "value": 61.2},
            "macd": {"status": "ok", "signal": "bullish"},
        },
        {
            "direction": "bearish",
            "change_pct_1d": -1.0,
            "volume_vs_avg": 0.8,
            "rsi": {"status": "ok", "signal": "bearish", "value": 38.4},
            "macd": {"status": "ok", "signal": "bearish"},
        },
    )

    metrics = {metric["key"]: metric for metric in category["metrics"]}
    assert metrics["rsi"]["left"] == 61.2
    assert metrics["rsi"]["edge"] == "left"
    assert metrics["macd"]["left"] == "bullish"
    assert metrics["macd"]["right"] == "bearish"
    assert metrics["macd"]["edge"] == "left"


def test_peer_compare_premium_unlocks_core_but_not_pro_sources(monkeypatch):
    engine = _engine()
    with Session(engine) as db:
        db.add_all(
            [
                _fundamentals("AAA", revenue_growth=18, roe=24, forward_pe=20),
                _fundamentals("BBB", revenue_growth=12, roe=18, forward_pe=24),
            ]
        )
        db.commit()

        monkeypatch.setattr(
            main_module,
            "_ticker_price_volume_summary",
            lambda _db, symbol: {"direction": "bullish" if symbol == "AAA" else "neutral", "change_pct_1d": 1.0, "volume_vs_avg": 1.2},
        )
        monkeypatch.setattr(main_module, "get_government_contracts_summary", lambda *_args, **_kwargs: {"status": "ok", "contract_count": 0, "total_award_amount": 0})
        monkeypatch.setattr(
            main_module,
            "_ticker_confirmation_context",
            lambda _db, symbol: {
                "confirmation_score_bundle": {"score": 72 if symbol == "AAA" else 54, "direction": "bullish" if symbol == "AAA" else "neutral", "sources": {}},
                "institutional_activity_summary": {"status": "ok", "direction": "bullish", "net_activity": 10_000_000},
                "options_flow_summary": {"status": "ok", "direction": "bullish", "score": 80, "total_premium": 5_000_000},
            },
        )

        payload = main_module._build_peer_compare_payload(
            db,
            "AAA",
            "BBB",
            entitlements=ENTITLEMENTS["premium"],
            authenticated=True,
        )

    assert payload["status"] == "ok"
    by_key = {category["key"]: category for category in payload["categories"]}
    assert by_key["business_quality"].get("locked") is not True
    assert by_key["valuation"].get("locked") is not True
    assert by_key["price_volume"].get("locked") is not True
    assert by_key["confirmation_score"].get("locked") is not True
    assert by_key["institutional_activity"]["locked"] is True
    assert by_key["institutional_activity"]["metrics"] == []
    assert by_key["options_flow"]["locked"] is True
    assert by_key["options_flow"]["metrics"] == []
    assert "10000000" not in str(payload)
    assert "5000000" not in str(payload)


def test_peer_compare_pro_tier_unlocks_pro_sources(monkeypatch):
    engine = _engine()
    with Session(engine) as db:
        db.add_all(
            [
                _fundamentals("AAA", revenue_growth=18, roe=24, forward_pe=20),
                _fundamentals("BBB", revenue_growth=12, roe=18, forward_pe=24),
            ]
        )
        db.commit()

        monkeypatch.setattr(
            main_module,
            "_ticker_price_volume_summary",
            lambda _db, symbol: {"direction": "neutral", "change_pct_1d": 0.0, "volume_vs_avg": 1.0},
        )
        monkeypatch.setattr(main_module, "get_government_contracts_summary", lambda *_args, **_kwargs: {"status": "ok", "contract_count": 0, "total_award_amount": 0})
        monkeypatch.setattr(
            main_module,
            "_ticker_confirmation_context",
            lambda _db, symbol: {
                "confirmation_score_bundle": {"score": 72 if symbol == "AAA" else 54, "direction": "bullish" if symbol == "AAA" else "neutral", "sources": {}},
                "institutional_activity_summary": {"status": "ok", "direction": "bullish" if symbol == "AAA" else "neutral", "net_activity": 10 if symbol == "AAA" else 1, "holder_breadth": 5 if symbol == "AAA" else 1},
                "options_flow_summary": {"status": "ok", "direction": "bullish" if symbol == "AAA" else "neutral", "score": 70 if symbol == "AAA" else 40, "total_premium": 2_000_000 if symbol == "AAA" else 300_000},
            },
        )

        payload = main_module._build_peer_compare_payload(
            db,
            "AAA",
            "BBB",
            entitlements=ENTITLEMENTS["pro"],
            authenticated=True,
        )

    by_key = {category["key"]: category for category in payload["categories"]}
    assert by_key["confirmation_score"].get("locked") is not True
    assert by_key["institutional_activity"].get("locked") is not True
    assert by_key["options_flow"].get("locked") is not True
    assert by_key["confirmation_score"]["edge"] == "left"
    assert by_key["institutional_activity"]["edge"] == "left"
    assert by_key["options_flow"]["edge"] == "left"


def test_peer_compare_hides_removed_activity_metrics_and_weights_confirmation(monkeypatch):
    engine = _engine()
    with Session(engine) as db:
        db.add_all(
            [
                _fundamentals("MU", revenue_growth=49, roe=24, forward_pe=20),
                _fundamentals("TSM", revenue_growth=33, roe=18, forward_pe=20),
            ]
        )
        db.commit()

        monkeypatch.setattr(
            main_module,
            "_ticker_price_volume_summary",
            lambda _db, _symbol: {"direction": "neutral", "change_pct_1d": 0.0, "volume_vs_avg": 1.0},
        )
        monkeypatch.setattr(
            main_module,
            "get_government_contracts_summary",
            lambda *_args, **_kwargs: {"status": "ok", "contract_count": 0, "total_award_amount": 0},
        )
        monkeypatch.setattr(
            main_module,
            "_ticker_confirmation_context",
            lambda _db, symbol: {
                "confirmation_score_bundle": {
                    "score": 59 if symbol == "MU" else 89,
                    "direction": "mixed" if symbol == "MU" else "bullish",
                    "sources": {},
                },
                "institutional_activity_summary": {
                    "status": "ok",
                    "direction": "bullish",
                    "net_activity": 14_389_039_618 if symbol == "MU" else 24_399_276_572,
                    "holder_breadth": 100 if symbol == "MU" else 1,
                },
                "options_flow_summary": {"status": "ok", "direction": "neutral", "score": None, "total_premium": None},
            },
        )

        payload = main_module._build_peer_compare_payload(
            db,
            "MU",
            "TSM",
            entitlements=ENTITLEMENTS["pro"],
            authenticated=True,
        )

    by_key = {category["key"]: category for category in payload["categories"]}
    congress_metric_keys = {metric["key"] for metric in by_key["congress_activity"]["metrics"]}
    insider_metric_keys = {metric["key"] for metric in by_key["insider_activity"]["metrics"]}
    institutional_metric_keys = {metric["key"] for metric in by_key["institutional_activity"]["metrics"]}

    assert "unique_traders" not in congress_metric_keys
    assert "unique_traders" not in insider_metric_keys
    assert "holder_breadth" not in institutional_metric_keys
    assert by_key["institutional_activity"]["edge"] == "right"
    assert by_key["confirmation_score"]["edge"] == "right"
    assert payload["call"]["winner"] == "right"
    assert payload["call"]["symbol"] == "TSM"
