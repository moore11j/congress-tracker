from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.services.confirmation_score import get_confirmation_score_bundle_for_ticker
from app.services.options_flow import OptionsFlowObservation, summarize_options_flow


def test_options_flow_summary_classifies_recent_put_premium_skew_as_bearish():
    now = datetime.now(timezone.utc)
    summary = summarize_options_flow(
        "PLTR",
        [
            OptionsFlowObservation("put", premium=1_250_000, contract_volume=700, observed_at=now - timedelta(days=2)),
            OptionsFlowObservation("call", premium=500_000, contract_volume=260, observed_at=now - timedelta(days=2)),
        ],
        lookback_days=30,
        provider="massive",
        now=now,
    )

    assert summary["state"] == "bearish"
    assert summary["confidence"] == "moderate"
    assert summary["can_confirm"] is True
    assert summary["freshness_days"] == 2
    assert summary["metrics"]["put_call_premium_ratio"] == 2.5
    assert "Put premium outweighs calls in recent flow" in summary["signals"]


def test_options_flow_summary_keeps_conflicted_activity_mixed_and_non_confirming():
    now = datetime.now(timezone.utc)
    summary = summarize_options_flow(
        "NVDA",
        [
            OptionsFlowObservation("put", premium=1_200_000, contract_volume=120, observed_at=now - timedelta(days=1)),
            OptionsFlowObservation("call", premium=300_000, contract_volume=700, observed_at=now - timedelta(days=1)),
        ],
        lookback_days=30,
        provider="massive",
        now=now,
    )

    assert summary["state"] == "mixed"
    assert summary["confidence"] == "low"
    assert summary["can_confirm"] is False


def test_options_flow_summary_degrades_to_inactive_without_meaningful_activity():
    now = datetime.now(timezone.utc)
    summary = summarize_options_flow(
        "ZZZ",
        [
            OptionsFlowObservation("call", premium=3_000, contract_volume=1, observed_at=now - timedelta(days=1)),
            OptionsFlowObservation("put", premium=2_000, contract_volume=1, observed_at=now - timedelta(days=1)),
        ],
        lookback_days=30,
        provider="massive",
        now=now,
    )

    assert summary["state"] == "inactive"
    assert summary["can_confirm"] is False
    assert summary["summary"] == "No notable recent options flow."


def test_options_flow_can_join_confirmation_when_summary_is_confirmable():
    summary = {
        "ticker": "PLTR",
        "lookback_days": 30,
        "state": "bullish",
        "label": "Bullish flow skew",
        "is_active": True,
        "confidence": "moderate",
        "freshness_days": 1,
        "summary": "Call premium outweighs puts in recent flow.",
        "signals": ["Call premium outweighs puts in recent flow"],
        "metrics": {
            "put_call_premium_ratio": 0.4,
            "net_premium_skew": 1_500_000,
            "recent_contract_volume": 700,
            "observed_contracts": 22,
            "freshness_days": 1,
        },
        "can_confirm": True,
        "provider": "massive",
    }

    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session

    from app.db import Base

    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)

    with Session(engine) as db:
        bundle = get_confirmation_score_bundle_for_ticker(db, "PLTR", options_flow_summary=summary)

    assert bundle["sources"]["options_flow"]["present"] is True
    assert bundle["sources"]["options_flow"]["direction"] == "bullish"
    assert bundle["sources"]["options_flow"]["freshness_days"] == 1
    assert bundle["direction"] == "bullish"
