from __future__ import annotations

from datetime import datetime, timezone

import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

import app.main as main_module
from app.db import Base
from app.entitlements import ENTITLEMENTS
from app.models import FundamentalsCache


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
