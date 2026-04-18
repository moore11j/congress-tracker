from __future__ import annotations

from app.services.signal_freshness import build_signal_freshness_bundle


def _bundle(*, direction: str = "bullish", sources: dict) -> dict:
    defaults = {
        "congress": {"present": False, "direction": "neutral", "strength": 0, "quality": 0, "freshness_days": None, "label": "Inactive"},
        "insiders": {"present": False, "direction": "neutral", "strength": 0, "quality": 0, "freshness_days": None, "label": "Inactive"},
        "signals": {"present": False, "direction": "neutral", "strength": 0, "quality": 0, "freshness_days": None, "label": "No current smart signal"},
        "price_volume": {"present": False, "direction": "neutral", "strength": 0, "quality": 0, "freshness_days": None, "label": "No price confirmation"},
    }
    defaults.update(sources)
    return {
        "ticker": "CRM",
        "lookback_days": 30,
        "direction": direction,
        "sources": defaults,
    }


def test_signal_freshness_classifies_clustered_multi_source_as_fresh():
    freshness = build_signal_freshness_bundle(
        "CRM",
        _bundle(
            sources={
                "insiders": {"present": True, "direction": "bullish", "strength": 74, "quality": 80, "freshness_days": 3, "label": "Active / buy-skewed"},
                "price_volume": {"present": True, "direction": "bullish", "strength": 55, "quality": 70, "freshness_days": 8, "label": "Moderate bullish price confirmation"},
            },
        ),
    )

    assert freshness["freshness_state"] == "fresh"
    assert freshness["freshness_label"] == "Fresh multi-source setup"
    assert 75 <= freshness["freshness_score"] <= 100
    assert freshness["timing"] == {
        "freshest_source_days": 3,
        "stalest_active_source_days": 8,
        "active_source_count": 2,
        "overlap_window_days": 5,
    }


def test_signal_freshness_classifies_recent_single_source_as_early():
    freshness = build_signal_freshness_bundle(
        "CRM",
        _bundle(
            sources={
                "congress": {"present": True, "direction": "bullish", "strength": 61, "quality": 55, "freshness_days": 2, "label": "Active / buy-skewed"},
            },
        ),
    )

    assert freshness["freshness_state"] == "early"
    assert 60 <= freshness["freshness_score"] <= 79
    assert freshness["explanation"] == "A single recent source is active, but broader confirmation is still limited."


def test_signal_freshness_ages_broad_overlap_to_maturing():
    freshness = build_signal_freshness_bundle(
        "CRM",
        _bundle(
            sources={
                "congress": {"present": True, "direction": "bullish", "strength": 61, "quality": 55, "freshness_days": 4, "label": "Active / buy-skewed"},
                "insiders": {"present": True, "direction": "bullish", "strength": 61, "quality": 55, "freshness_days": 29, "label": "Active / buy-skewed"},
            },
        ),
    )

    assert freshness["freshness_state"] == "maturing"
    assert 30 <= freshness["freshness_score"] <= 49
    assert freshness["timing"]["overlap_window_days"] == 25


def test_signal_freshness_treats_mixed_direction_as_active_not_fresh():
    freshness = build_signal_freshness_bundle(
        "CRM",
        _bundle(
            direction="mixed",
            sources={
                "congress": {"present": True, "direction": "bullish", "strength": 70, "quality": 60, "freshness_days": 1, "label": "Active / buy-skewed"},
                "insiders": {"present": True, "direction": "bearish", "strength": 65, "quality": 55, "freshness_days": 2, "label": "Active / sell-skewed"},
            },
        ),
    )

    assert freshness["freshness_state"] == "active"
    assert freshness["freshness_score"] <= 68
    assert "mixed direction" in freshness["explanation"]


def test_signal_freshness_degrades_missing_timing_without_breaking():
    freshness = build_signal_freshness_bundle(
        "CRM",
        _bundle(
            sources={
                "congress": {"present": True, "direction": "bullish", "strength": 70, "quality": 60, "freshness_days": None, "label": "Active / buy-skewed"},
                "signals": {"present": True, "direction": "bullish", "strength": 65, "quality": 55, "freshness_days": None, "label": "Strong smart signal"},
            },
        ),
    )

    assert freshness["freshness_state"] == "maturing"
    assert freshness["timing"]["freshest_source_days"] is None
    assert freshness["explanation"] == "Active sources are present, but source timing is incomplete."


def test_signal_freshness_inactive_without_directional_sources():
    freshness = build_signal_freshness_bundle("ZZZ", _bundle(direction="neutral", sources={}))

    assert freshness["freshness_state"] == "inactive"
    assert freshness["freshness_score"] == 0
    assert freshness["timing"]["active_source_count"] == 0
