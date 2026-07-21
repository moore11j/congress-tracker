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


def test_peer_compare_free_tier_excludes_locked_sources(monkeypatch):
    engine = _engine()
    with Session(engine) as db:
        db.add_all(
            [
                _fundamentals("AAA", revenue_growth=24, roe=30, forward_pe=18),
                _fundamentals("BBB", revenue_growth=8, roe=12, forward_pe=32),
            ]
        )
        db.commit()

        def price_volume(_db, symbol):
            return {
                "direction": "bullish" if symbol == "AAA" else "bearish",
                "change_pct_1d": 2.0 if symbol == "AAA" else -1.0,
                "volume_vs_avg": 1.4 if symbol == "AAA" else 0.8,
            }

        monkeypatch.setattr(main_module, "_ticker_price_volume_summary", price_volume)
        monkeypatch.setattr(main_module, "get_government_contracts_summary", lambda *_args, **_kwargs: {"status": "ok", "contract_count": 0, "total_award_amount": 0})
        monkeypatch.setattr(
            main_module,
            "_ticker_confirmation_context",
            lambda *_args, **_kwargs: pytest.fail("locked confirmation context should not be loaded for free tier"),
        )

        payload = main_module._build_peer_compare_payload(
            db,
            "AAA",
            "BBB",
            entitlements=ENTITLEMENTS["free"],
            authenticated=False,
        )

    assert payload["call"]["winner"] == "left"
    by_key = {category["key"]: category for category in payload["categories"]}
    assert by_key["business_quality"]["edge"] == "left"
    assert by_key["valuation"]["edge"] == "left"
    assert by_key["price_volume"]["edge"] == "left"
    assert by_key["confirmation_score"]["locked"] is True
    assert by_key["institutional_activity"]["locked"] is True
    assert by_key["options_flow"]["locked"] is True
    assert any("excluded from the call" in note for note in payload["notes"])
    gov_metrics = {metric["key"]: metric for metric in by_key["government_contracts"]["metrics"]}
    assert gov_metrics["total_award_amount"]["left"] == "N/A"
    assert gov_metrics["total_award_amount"]["right"] == "N/A"


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
                    payload_json='{"summary":{"forwardPE":15,"forwardPESource":"price_over_estimated_eps","expectedEpsGrowthRatePercent":21},"valuation_metrics":{"forward_pe":15,"forward_pe_source":"price_over_estimated_eps","expected_eps_growth_rate_percent":21,"status":"ok"}}',
                ),
                TickerFinancialsCache(
                    symbol="BBB",
                    status="ok",
                    fetched_at=datetime.now(timezone.utc),
                    payload_json='{"summary":{"forwardPE":28,"forwardPESource":"price_over_estimated_eps","expectedEpsGrowthRatePercent":9},"valuation_metrics":{"forward_pe":28,"forward_pe_source":"price_over_estimated_eps","expected_eps_growth_rate_percent":9,"status":"ok"}}',
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
            entitlements=ENTITLEMENTS["free"],
            authenticated=False,
        )

    by_key = {category["key"]: category for category in payload["categories"]}
    business_metrics = {metric["key"]: metric for metric in by_key["business_quality"]["metrics"]}
    valuation_metrics = {metric["key"]: metric for metric in by_key["valuation"]["metrics"]}
    assert business_metrics["eps_growth"]["left"] == 21
    assert business_metrics["eps_growth"]["right"] == 9
    assert valuation_metrics["forward_pe"]["left"] == 15
    assert valuation_metrics["forward_pe"]["right"] == 28


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
