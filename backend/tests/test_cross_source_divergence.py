import json
from datetime import datetime, timezone

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from app.db import Base, ensure_outcome_ledger_schema
from app.models import ConfirmationScoreSnapshot, PriceCache
from app.services.cross_source_divergence import (
    CROSS_SOURCE_DIVERGENCE_METHODOLOGY_VERSION,
    build_cross_source_divergence,
    public_cross_source_divergence,
)
from app.services.outcome_ledger import capture_live_confirmation_score_snapshot


def _source(direction: str, contribution: int, *, freshness_days: int | None = 4, present: bool = True) -> dict:
    return {
        "present": present,
        "direction": direction,
        "strength": 70,
        "quality": 70,
        "freshness_days": freshness_days,
        "score_contribution": contribution,
    }


def _bundle(**sources: dict) -> dict:
    return {"sources": sources}


def test_aligned_sources_are_not_reported_as_divergent():
    result = build_cross_source_divergence(
        _bundle(fundamentals=_source("bullish", 14), analysts=_source("bullish", 7), price_volume=_source("bullish", 11))
    )
    assert result["state"] == "aligned"
    assert result["bullish_source_count"] == 3
    assert result["bearish_source_count"] == 0
    assert result["methodology_version"] == CROSS_SOURCE_DIVERGENCE_METHODOLOGY_VERSION


def test_capped_directional_analyst_evidence_is_included():
    result = build_cross_source_divergence(
        _bundle(fundamentals=_source("bullish", 14), analysts=_source("bullish", 2), price_volume=_source("bullish", 11))
    )
    assert result["state"] == "aligned"
    assert result["bullish_source_count"] == 3
    assert {source["key"] for source in result["bullish_sources"]} == {"fundamentals", "analysts", "price_volume"}


def test_fast_slow_conflict_is_classified_deterministically():
    result = build_cross_source_divergence(
        _bundle(
            fundamentals=_source("bullish", 14),
            analysts=_source("bullish", 7),
            institutional_activity=_source("bullish", 8),
            price_volume=_source("bearish", 12),
            insiders=_source("bearish", 7),
        )
    )
    assert result["state"] == "moderate_divergence"
    assert result["directional_context"] == "near_term_bearish_longer_term_bullish"
    assert result["fast_group_state"] == "bearish"
    assert result["slow_group_state"] == "bullish"


def test_balanced_multi_source_conflict_is_strong():
    result = build_cross_source_divergence(
        _bundle(
            price_volume=_source("bullish", 14),
            options_flow=_source("bullish", 10),
            fundamentals=_source("bearish", 11),
            analysts=_source("bearish", 7),
            institutional_activity=_source("bearish", 8),
        )
    )
    assert result["state"] == "strong_divergence"
    assert result["directional_context"] == "near_term_bullish_longer_term_bearish"


def test_stale_neutral_and_inactive_sources_do_not_create_conflict():
    result = build_cross_source_divergence(
        _bundle(
            fundamentals=_source("bullish", 14),
            price_volume=_source("bearish", 14, freshness_days=91),
            insiders=_source("neutral", 14),
            analysts=_source("bearish", 14, present=False),
        )
    )
    assert result["state"] == "unavailable"
    assert result["bullish_source_count"] == 1
    assert result["bearish_source_count"] == 0
    assert result["excluded_sources"]["stale"] == 1


def test_public_payload_withholds_named_source_breakdown():
    result = build_cross_source_divergence(
        _bundle(fundamentals=_source("bullish", 14), price_volume=_source("bearish", 12))
    )
    guest = public_cross_source_divergence(result, allowed_source_keys=set())
    premium = public_cross_source_divergence(result, allowed_source_keys={"fundamentals", "price_volume"})
    assert guest["source_breakdown_available"] is False
    assert guest["bullish_sources"] == []
    assert "Fundamentals" not in guest["explanation"]
    assert premium["source_breakdown_available"] is True
    assert premium["bullish_sources"][0]["key"] == "fundamentals"


def test_new_confirmation_snapshot_keeps_divergence_at_point_in_time():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    ensure_outcome_ledger_schema(engine)
    now = datetime.now(timezone.utc)
    bundle = {
        "ticker": "CRM",
        "score": 68,
        "band": "strong",
        "direction": "bullish",
        "status": "test",
        "classification_version": "confirmation-test",
        "active_sources": ["fundamentals", "price_volume"],
        "sources": {
            "fundamentals": _source("bullish", 14),
            "price_volume": _source("bearish", 12),
        },
    }
    with Session(engine) as db:
        db.add(PriceCache(symbol="CRM", date=now.date().isoformat(), close=100.0))
        db.commit()
        assert capture_live_confirmation_score_snapshot(db, "CRM", bundle, calculated_at=now) is not None
        row = db.execute(select(ConfirmationScoreSnapshot)).scalar_one()
        stored = json.loads(row.source_contributions_json)
        assert stored["__cross_source_divergence"]["methodology_version"] == CROSS_SOURCE_DIVERGENCE_METHODOLOGY_VERSION
        assert stored["__cross_source_divergence"]["state"] == "moderate_divergence"
