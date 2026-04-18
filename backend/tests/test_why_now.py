from __future__ import annotations

from app.services.why_now import build_why_now_bundle, inactive_why_now_bundle


def test_why_now_classifies_aligned_multi_source_as_strengthening():
    bundle = {
        "ticker": "CRM",
        "lookback_days": 30,
        "score": 68,
        "band": "strong",
        "direction": "bearish",
        "status": "2-source bearish confirmation",
        "sources": {
            "congress": {"present": False, "direction": "neutral", "label": "Inactive"},
            "insiders": {"present": True, "direction": "bearish", "strength": 74, "quality": 68, "freshness_days": 3, "label": "Active / sell-skewed"},
            "signals": {"present": False, "direction": "neutral", "label": "No current smart signal"},
            "price_volume": {"present": True, "direction": "bearish", "strength": 51, "quality": 80, "freshness_days": 1, "label": "Moderate bearish price confirmation"},
        },
    }

    why_now = build_why_now_bundle("CRM", bundle)

    assert why_now["state"] == "strengthening"
    assert why_now["headline"] == "Recent insider selling and moderate bearish price confirmation are strengthening the bearish setup for CRM."
    assert why_now["evidence"][:3] == [
        "2-source bearish confirmation",
        "Recent insider selling",
        "Moderate bearish price confirmation",
    ]
    assert why_now["caveat"] == "No current smart signal is reinforcing the move."


def test_why_now_labels_conflicting_sources_as_mixed():
    bundle = {
        "ticker": "NVDA",
        "score": 55,
        "band": "moderate",
        "direction": "mixed",
        "status": "Mixed multi-source setup",
        "sources": {
            "congress": {"present": True, "direction": "bullish", "strength": 70, "quality": 60, "freshness_days": 1},
            "insiders": {"present": True, "direction": "bearish", "strength": 65, "quality": 55, "freshness_days": 2},
            "signals": {"present": False, "direction": "neutral"},
            "price_volume": {"present": False, "direction": "neutral"},
        },
    }

    why_now = build_why_now_bundle("NVDA", bundle)

    assert why_now["state"] == "mixed"
    assert why_now["headline"] == "Congress buy-skewed is active, but mixed confirmation is limiting conviction on NVDA."
    assert "Mixed multi-source setup" in why_now["evidence"]
    assert why_now["caveat"] == "Evidence is conflicting across active sources."


def test_why_now_degrades_to_inactive_without_sources():
    why_now = inactive_why_now_bundle("ZZZ", lookback_days=30)

    assert why_now["state"] == "inactive"
    assert why_now["headline"] == "No active confirmation sources are currently putting ZZZ on the radar."
    assert why_now["evidence"][:2] == [
        "No active confirmation sources",
        "Congress activity remains inactive",
    ]
    assert why_now["caveat"] is None
