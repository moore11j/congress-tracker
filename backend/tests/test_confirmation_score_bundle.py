from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.db import Base
from app.models import Event, FundamentalsCache, GovernmentContract, PriceCache
from app.services.confirmation_score import (
    CONFIRMATION_CLASSIFICATION_VERSION,
    confirmation_score_bundle_from_source_contexts,
    confirmation_score_bundle_from_source_payloads,
    confirmation_band_for_score,
    get_slim_confirmation_score_bundles_for_tickers,
    slim_confirmation_score_bundle,
    get_confirmation_score_bundle_for_ticker,
)


def _event(
    *,
    event_id: int,
    symbol: str,
    event_type: str,
    trade_type: str,
    event_date: datetime,
    amount_max: int = 250_000,
):
    return Event(
        id=event_id,
        event_type=event_type,
        ts=event_date,
        event_date=event_date,
        symbol=symbol,
        source="test",
        payload_json=json.dumps({"symbol": symbol, "reporting_cik": "000123"}),
        trade_type=trade_type,
        amount_min=10_000,
        amount_max=amount_max,
    )


def _price(symbol: str, day: datetime, close: float) -> PriceCache:
    return PriceCache(symbol=symbol, date=day.date().isoformat(), close=close)


def test_confirmation_band_thresholds_match_product_contract():
    assert confirmation_band_for_score(0) == "inactive"
    assert confirmation_band_for_score(19) == "inactive"
    assert confirmation_band_for_score(20) == "weak"
    assert confirmation_band_for_score(40) == "moderate"
    assert confirmation_band_for_score(60) == "strong"
    assert confirmation_band_for_score(80) == "exceptional"


def test_confirmation_score_bundle_combines_insider_and_price_confirmation():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)

    now = datetime.now(timezone.utc)
    start = now - timedelta(days=29)
    recent = now - timedelta(days=5)

    with Session(engine) as db:
        db.add(
            _event(
                event_id=1,
                symbol="CRM",
                event_type="insider_trade",
                trade_type="sale",
                event_date=recent,
            )
        )
        db.add_all(
            [
                _price("CRM", start, 100),
                _price("CRM", recent, 90),
                _price("SPY", start, 100),
                _price("SPY", recent, 98),
            ]
        )
        db.commit()

        bundle = get_confirmation_score_bundle_for_ticker(db, "CRM", lookback_days=30)

        assert bundle["ticker"] == "CRM"
        assert bundle["band"] in {"weak", "moderate", "strong"}
        assert bundle["direction"] == "bearish"
        assert bundle["status"] == "2-source bearish confirmation"
        assert bundle["sources"]["insiders"]["present"] is True
        assert bundle["sources"]["price_volume"]["present"] is True
        assert bundle["sources"]["congress"]["present"] is False
        assert 0 <= bundle["score"] <= 100
        assert 2 <= len(bundle["drivers"]) <= 4

        slim_by_symbol = get_slim_confirmation_score_bundles_for_tickers(db, ["CRM"], lookback_days=30)
        assert slim_by_symbol["CRM"] == slim_confirmation_score_bundle(bundle)


def test_confirmation_score_bundle_degrades_to_inactive_without_sources():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)

    with Session(engine) as db:
        bundle = get_confirmation_score_bundle_for_ticker(db, "ZZZ", lookback_days=30)

        assert bundle["score"] == 0
        assert bundle["band"] == "inactive"
        assert bundle["direction"] == "neutral"
        assert bundle["status"] == "Inactive"
        assert bundle["sources"]["congress"]["present"] is False
        assert bundle["sources"]["insiders"]["present"] is False
        assert bundle["sources"]["signals"]["present"] is False
        assert bundle["sources"]["price_volume"]["present"] is False
        assert bundle["sources"]["fundamentals"]["present"] is False


def test_confirmation_score_bundle_receives_fundamentals_without_unavailable_penalty():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)

    now = datetime.now(timezone.utc)
    with Session(engine) as db:
        db.add(
            FundamentalsCache(
                symbol="QUAL",
                provider="fmp",
                fetched_at=now,
                status="ok",
                revenue_growth=12,
                roe=24,
                fcf_yield=4,
                operating_margin_expansion=2,
                net_debt_to_ebitda=0.8,
            )
        )
        db.commit()

        bundle = get_confirmation_score_bundle_for_ticker(db, "QUAL", lookback_days=30)

    assert bundle["sources"]["fundamentals"]["present"] is True
    assert bundle["sources"]["fundamentals"]["direction"] == "bullish"
    assert bundle["direction"] == "bullish"


def test_unavailable_fundamentals_do_not_count_as_bearish():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)

    with Session(engine) as db:
        db.add(
            FundamentalsCache(
                symbol="EMPTY",
                provider="fmp",
                fetched_at=datetime.now(timezone.utc),
                status="ok",
            )
        )
        db.commit()

        bundle = get_confirmation_score_bundle_for_ticker(db, "EMPTY", lookback_days=30)

    assert bundle["sources"]["fundamentals"]["present"] is False
    assert bundle["direction"] == "neutral"


def test_slim_confirmation_score_bundle_derives_active_source_count():
    bundle = {
        "score": 61,
        "band": "strong",
        "direction": "bullish",
        "status": "2-source bullish confirmation",
        "explanation": "Congress buy-skewed aligns with bullish smart signal.",
        "drivers": ["Congress buy-skewed", "Bullish smart signal"],
        "sources": {
            "congress": {"present": True, "direction": "bullish"},
            "signals": {"present": True, "direction": "bullish"},
            "insiders": {"present": True, "direction": "neutral"},
            "price_volume": {"present": False, "direction": "neutral"},
        },
    }

    slim = slim_confirmation_score_bundle(bundle)

    assert slim["confirmation_score"] == 61
    assert slim["confirmation_band"] == "strong"
    assert slim["confirmation_direction"] == "bullish"
    assert slim["confirmation_source_count"] == 2
    assert slim["is_multi_source"] is True
    assert slim["confirmation_explanation"] == "Congress buy-skewed"


def test_confirmation_bundle_can_include_government_contracts_without_breaking_why_now():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)

    now = datetime.now(timezone.utc)

    with Session(engine) as db:
        db.add(
            GovernmentContract(
                id=90,
                award_id="AWD-90",
                dedupe_key="dedupe-90",
                symbol="GOVT",
                recipient_name="Govt Recipient",
                raw_recipient_name="Govt Recipient",
                award_date=(now - timedelta(days=3)).date(),
                award_amount=25_000_000,
                awarding_agency="Department of Defense",
                source="usaspending",
                mapping_method="alias_exact",
                mapping_confidence=1.0,
                payload_json=json.dumps(
                    {
                        "symbol": "GOVT",
                        "award_id": "AWD-90",
                        "award_date": (now - timedelta(days=3)).date().isoformat(),
                        "award_amount": 25_000_000,
                        "awarding_agency": "Department of Defense",
                    }
                ),
            )
        )
        db.commit()

        bundle = get_confirmation_score_bundle_for_ticker(db, "GOVT", lookback_days=30)
        slim = slim_confirmation_score_bundle(bundle)

    assert bundle["sources"]["government_contracts"]["present"] is True
    assert bundle["sources"]["government_contracts"]["direction"] == "bullish"
    assert slim["confirmation_source_count"] >= 1
    assert slim["why_now"]["state"] != "inactive"


def test_government_contracts_do_not_flip_bearish_bundle_direction():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)

    now = datetime.now(timezone.utc)
    recent = now - timedelta(days=2)

    with Session(engine) as db:
        db.add(
            _event(
                event_id=201,
                symbol="CLSH",
                event_type="insider_trade",
                trade_type="sale",
                event_date=recent,
                amount_max=500_000,
            )
        )
        db.add(
            GovernmentContract(
                id=202,
                award_id="AWD-202",
                dedupe_key="dedupe-202",
                symbol="CLSH",
                recipient_name="Conflict Recipient",
                raw_recipient_name="Conflict Recipient",
                award_date=recent.date(),
                award_amount=12_000_000,
                awarding_agency="Department of Defense",
                source="usaspending",
                mapping_method="alias_exact",
                mapping_confidence=1.0,
                payload_json=json.dumps({"symbol": "CLSH"}),
            )
        )
        db.commit()

        bundle = get_confirmation_score_bundle_for_ticker(db, "CLSH", lookback_days=30)

    assert bundle["direction"] == "bearish"
    assert bundle["sources"]["insiders"]["direction"] == "bearish"
    assert bundle["sources"]["government_contracts"]["direction"] == "bullish"
    assert bundle["sources"]["government_contracts"]["score_contribution"] == 15


def test_conflicting_government_contract_support_caps_bearish_bundle_below_exceptional():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)

    now = datetime.now(timezone.utc)
    start = now - timedelta(days=29)
    recent = now - timedelta(days=2)

    with Session(engine) as db:
        db.add(
            _event(
                event_id=301,
                symbol="PLTR",
                event_type="insider_trade",
                trade_type="sale",
                event_date=recent,
                amount_max=750_000,
            )
        )
        db.add_all(
            [
                _price("PLTR", start, 100),
                _price("PLTR", recent, 84),
                _price("SPY", start, 100),
                _price("SPY", recent, 99),
            ]
        )
        db.add(
            GovernmentContract(
                id=302,
                award_id="AWD-302",
                dedupe_key="dedupe-302",
                symbol="PLTR",
                recipient_name="Palantir",
                raw_recipient_name="Palantir",
                award_date=recent.date(),
                award_amount=103_000_000,
                awarding_agency="Department of Agriculture",
                source="usaspending",
                mapping_method="alias_exact",
                mapping_confidence=1.0,
                payload_json=json.dumps({"symbol": "PLTR"}),
            )
        )
        db.commit()

        bundle = get_confirmation_score_bundle_for_ticker(db, "PLTR", lookback_days=30)

    assert bundle["direction"] == "bearish"
    assert bundle["sources"]["government_contracts"]["direction"] == "bullish"
    assert bundle["sources"]["government_contracts"]["present"] is True
    assert bundle["score"] <= 79
    assert bundle["band"] != "exceptional"


def test_mixed_price_volume_discounts_but_does_not_override_broad_bullish_stack():
    bundle = confirmation_score_bundle_from_source_contexts(
        "TSM",
        source_contexts={
            "insiders": {"status": "active", "direction": "bullish", "buy_count": 36, "sell_count": 0},
            "congress": {"status": "active", "direction": "bullish", "buy_count": 1, "sell_count": 0},
            "price_volume": {
                "status": "active",
                "direction": "mixed",
                "title": "Mixed tape confirmation",
                "summary": "Mixed tape confirmation",
                "score": 25,
                "price_points": 120,
            },
            "fundamentals": {
                "status": "bullish",
                "headline": "Fundamental strength",
                "data_quality": {"scored_metric_count": 5},
                "metrics": {
                    "revenue_growth": {"state": "bullish"},
                    "return_on_equity": {"state": "bullish"},
                    "ev_to_ebitda": {"state": "neutral"},
                    "operating_margin_expansion": {"state": "bullish"},
                    "net_debt_to_ebitda": {"state": "bullish"},
                },
            },
            "institutional_activity": {
                "status": "active",
                "direction": "bullish",
                "score": 45,
                "title": "Institutional Activity",
                "subtitle": "Net reported accumulation",
            },
        },
    )

    assert bundle["sources"]["price_volume"]["direction"] == "mixed"
    assert bundle["direction"] == "bullish"
    assert bundle["score"] == 58
    assert bundle["band"] == "moderate"


def _payload_source(direction: str, *, strength: int = 92, quality: int = 92, freshness_days: int | None = 3) -> dict:
    return {
        "present": True,
        "direction": direction,
        "strength": strength,
        "quality": quality,
        "freshness_days": freshness_days,
        "label": f"{direction} fixture",
        "score_contribution": 10,
    }


def test_canonical_classifier_returns_bullish_with_clear_edge():
    bundle = confirmation_score_bundle_from_source_payloads(
        "BULL",
        sources_payload={
            "signals": _payload_source("bullish"),
            "price_volume": _payload_source("bullish"),
            "institutional_activity": _payload_source("bullish"),
            "options_flow": _payload_source("bearish", strength=35, quality=35),
        },
    )

    assert bundle["direction"] == "bullish"
    assert bundle["classification_version"] == CONFIRMATION_CLASSIFICATION_VERSION


def test_canonical_classifier_returns_bearish_with_clear_edge():
    bundle = confirmation_score_bundle_from_source_payloads(
        "BEAR",
        sources_payload={
            "signals": _payload_source("bearish"),
            "price_volume": _payload_source("bearish"),
            "institutional_activity": _payload_source("bearish"),
            "options_flow": _payload_source("bullish", strength=35, quality=35),
        },
    )

    assert bundle["direction"] == "bearish"


def test_one_opposing_layer_does_not_force_conflicted():
    bundle = confirmation_score_bundle_from_source_payloads(
        "EDGE",
        sources_payload={
            "signals": _payload_source("bullish"),
            "price_volume": _payload_source("bullish"),
            "options_flow": _payload_source("bearish"),
        },
    )

    assert bundle["direction"] == "bullish"
    assert bundle["score"] > 39


def test_material_near_balanced_opposition_returns_conflicted():
    bundle = confirmation_score_bundle_from_source_payloads(
        "TIE",
        sources_payload={
            "signals": _payload_source("bullish"),
            "price_volume": _payload_source("bearish"),
        },
    )

    assert bundle["direction"] == "mixed"
    assert bundle["score"] <= 59
    assert "no clear directional edge" in bundle["explanation"]


def test_weak_and_stale_evidence_do_not_create_conflict():
    weak = confirmation_score_bundle_from_source_payloads(
        "WEAK",
        sources_payload={
            "signals": _payload_source("bullish", strength=20, quality=20),
            "options_flow": _payload_source("bearish", strength=20, quality=20),
        },
    )
    stale = confirmation_score_bundle_from_source_payloads(
        "STALE",
        sources_payload={
            "signals": _payload_source("bullish", freshness_days=3),
            "price_volume": _payload_source("bearish", freshness_days=160),
        },
    )
    missing = confirmation_score_bundle_from_source_payloads("MISS", sources_payload={})

    assert weak["direction"] == "neutral"
    assert stale["direction"] == "bullish"
    assert missing["direction"] == "neutral"
    assert missing["score"] == 0


def test_slim_confirmation_bundle_exposes_classification_version():
    bundle = confirmation_score_bundle_from_source_payloads(
        "VERS",
        sources_payload={"signals": _payload_source("bullish")},
    )
    slim = slim_confirmation_score_bundle(bundle)

    assert slim["confirmation_direction"] == bundle["direction"]
    assert slim["confirmation_classification_version"] == CONFIRMATION_CLASSIFICATION_VERSION


def test_bearish_source_conflict_no_longer_caps_clear_bullish_edge():
    bundle = confirmation_score_bundle_from_source_contexts(
        "CLSH",
        source_contexts={
            "insiders": {"status": "active", "direction": "bullish", "buy_count": 3, "sell_count": 0},
            "congress": {"status": "active", "direction": "bearish", "buy_count": 0, "sell_count": 2},
            "price_volume": {"status": "active", "direction": "mixed", "score": 25, "price_points": 120},
            "fundamentals": {
                "status": "bullish",
                "headline": "Fundamental strength",
                "data_quality": {"scored_metric_count": 5},
                "metrics": {
                    "revenue_growth": {"state": "bullish"},
                    "return_on_equity": {"state": "bullish"},
                    "ev_to_ebitda": {"state": "neutral"},
                    "operating_margin_expansion": {"state": "bullish"},
                    "net_debt_to_ebitda": {"state": "bullish"},
                },
            },
        },
    )

    assert bundle["direction"] == "bullish"
    assert bundle["score"] > 39
