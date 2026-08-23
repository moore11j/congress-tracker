"""Deterministic interpretation of disagreement inside a confirmation bundle.

This deliberately does not participate in the confirmation-score calculation.  It
uses the already-normalized source payload and produces a separately versioned,
qualitative interpretation that can be persisted with a score snapshot.
"""
from __future__ import annotations

import os
from typing import Any

from app.services.confirmation_score import MATERIAL_EVIDENCE_MAX_FRESHNESS_DAYS, SOURCE_LABELS, SOURCE_ORDER


CROSS_SOURCE_DIVERGENCE_METHODOLOGY_VERSION = "divergence-v1"
MIN_MATERIAL_CONTRIBUTION = 5.0
MILD_CONFLICT_RATIO = 0.12
MODERATE_CONFLICT_RATIO = 0.25
STRONG_CONFLICT_RATIO = 0.42

# These horizons are an interpretation layer, not a change to confirmation
# methodology. Keep the mapping explicit and version it with the result.
FAST_SOURCE_KEYS = frozenset({"price_volume", "options_flow", "signals"})
SLOW_SOURCE_KEYS = frozenset(set(SOURCE_ORDER) - FAST_SOURCE_KEYS)


def cross_source_divergence_enabled() -> bool:
    return os.getenv("CROSS_SOURCE_DIVERGENCE_ENABLED", "true").strip().lower() not in {"0", "false", "no", "off"}


def cross_source_divergence_methodology() -> dict[str, Any]:
    return {
        "version": CROSS_SOURCE_DIVERGENCE_METHODOLOGY_VERSION,
        "minimum_material_contribution": MIN_MATERIAL_CONTRIBUTION,
        "maximum_freshness_days": MATERIAL_EVIDENCE_MAX_FRESHNESS_DAYS,
        "conflict_ratios": {
            "mild": MILD_CONFLICT_RATIO,
            "moderate": MODERATE_CONFLICT_RATIO,
            "strong": STRONG_CONFLICT_RATIO,
        },
        "fast_sources": sorted(FAST_SOURCE_KEYS),
        "slow_sources": sorted(SLOW_SOURCE_KEYS),
    }


def _number(value: Any, default: float = 0.0) -> float:
    try:
        value = float(value)
    except (TypeError, ValueError):
        return default
    return value if value == value else default


def _freshness(source: dict[str, Any]) -> int | None:
    value = source.get("freshness_days")
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _source_magnitude(source: dict[str, Any]) -> float:
    """Use native contribution when available, otherwise a bounded evidence proxy."""
    contribution = abs(_number(source.get("score_contribution")))
    if contribution > 0:
        return contribution
    strength = max(0.0, _number(source.get("strength")))
    quality = max(0.0, _number(source.get("quality")))
    # Confirmation's own evidence formula is deliberately not imported here;
    # this proxy only makes sources without an explicit contribution comparable.
    return round((strength * 0.50 + quality * 0.35) / 10.0, 2)


def _state_for_strengths(bullish: float, bearish: float) -> str:
    if bullish <= 0 and bearish <= 0:
        return "neutral"
    if bullish > 0 and bearish <= 0:
        return "bullish"
    if bearish > 0 and bullish <= 0:
        return "bearish"
    # A group can contain a small opposing source while still expressing a
    # useful horizon-level lead. Reserve "split" for genuinely balanced groups.
    conflict_ratio = min(bullish, bearish) / max(bullish + bearish, 1.0)
    if conflict_ratio < MODERATE_CONFLICT_RATIO:
        return "bullish" if bullish > bearish else "bearish"
    return "split"


def _display_label(key: str, source: dict[str, Any]) -> str:
    label = source.get("label")
    # Source labels can be contextual (for example "Active / buy-skewed").
    # The canonical source name is clearer in a cross-source comparison.
    return SOURCE_LABELS.get(key, label if isinstance(label, str) else key.replace("_", " ").title())


def _short_explanation(state: str, directional_context: str, active_count: int) -> str:
    if state == "unavailable":
        return "Not enough fresh directional evidence is active to compare sources."
    if state == "aligned":
        return "Most active evidence points in the same direction."
    messages = {
        "near_term_bearish_longer_term_bullish": "Near-term evidence conflicts with supportive longer-term evidence.",
        "near_term_bullish_longer_term_bearish": "Near-term evidence is supportive while longer-term evidence is weaker.",
        "bullish_evidence_leads": "Meaningful evidence is split, with bullish sources carrying more weight.",
        "bearish_evidence_leads": "Meaningful evidence is split, with bearish sources carrying more weight.",
        "split": "Meaningful evidence sources are pointing in opposite directions.",
    }
    return messages.get(directional_context, f"{active_count} active sources include meaningful disagreement.")


def build_cross_source_divergence(bundle: dict[str, Any] | None) -> dict[str, Any]:
    """Return a stable, render-ready divergence result from a confirmation bundle."""
    raw_sources = bundle.get("sources") if isinstance(bundle, dict) and isinstance(bundle.get("sources"), dict) else {}
    eligible: list[dict[str, Any]] = []
    excluded = {"inactive": 0, "neutral_or_mixed": 0, "stale": 0, "immaterial": 0}
    for key in SOURCE_ORDER:
        source = raw_sources.get(key)
        if not isinstance(source, dict) or source.get("present") is not True:
            excluded["inactive"] += 1
            continue
        direction = str(source.get("direction") or "neutral").lower()
        if direction not in {"bullish", "bearish"}:
            excluded["neutral_or_mixed"] += 1
            continue
        freshness_days = _freshness(source)
        if freshness_days is not None and freshness_days > MATERIAL_EVIDENCE_MAX_FRESHNESS_DAYS:
            excluded["stale"] += 1
            continue
        magnitude = _source_magnitude(source)
        if magnitude < MIN_MATERIAL_CONTRIBUTION:
            excluded["immaterial"] += 1
            continue
        eligible.append(
            {
                "key": key,
                "label": _display_label(key, source),
                "direction": direction,
                "strength": round(magnitude, 2),
                "freshness_days": freshness_days,
                "horizon": "fast" if key in FAST_SOURCE_KEYS else "slow",
            }
        )

    bullish = [item for item in eligible if item["direction"] == "bullish"]
    bearish = [item for item in eligible if item["direction"] == "bearish"]
    bullish_strength = round(sum(item["strength"] for item in bullish), 2)
    bearish_strength = round(sum(item["strength"] for item in bearish), 2)
    total_strength = bullish_strength + bearish_strength
    fast_bullish = round(sum(item["strength"] for item in bullish if item["horizon"] == "fast"), 2)
    fast_bearish = round(sum(item["strength"] for item in bearish if item["horizon"] == "fast"), 2)
    slow_bullish = round(sum(item["strength"] for item in bullish if item["horizon"] == "slow"), 2)
    slow_bearish = round(sum(item["strength"] for item in bearish if item["horizon"] == "slow"), 2)
    fast_state = _state_for_strengths(fast_bullish, fast_bearish)
    slow_state = _state_for_strengths(slow_bullish, slow_bearish)

    if len(eligible) < 2 or total_strength <= 0:
        state = "unavailable"
        conflict_ratio = 0.0
    elif not bullish or not bearish:
        state = "aligned"
        conflict_ratio = 0.0
    else:
        conflict_ratio = round(min(bullish_strength, bearish_strength) / total_strength, 3)
        if conflict_ratio >= STRONG_CONFLICT_RATIO and len(bullish) >= 2 and len(bearish) >= 2:
            state = "strong_divergence"
        elif conflict_ratio >= MODERATE_CONFLICT_RATIO:
            state = "moderate_divergence"
        elif conflict_ratio >= MILD_CONFLICT_RATIO:
            state = "mild_divergence"
        else:
            state = "aligned"

    if fast_state == "bearish" and slow_state == "bullish":
        directional_context = "near_term_bearish_longer_term_bullish"
    elif fast_state == "bullish" and slow_state == "bearish":
        directional_context = "near_term_bullish_longer_term_bearish"
    elif bullish_strength > bearish_strength:
        directional_context = "bullish_evidence_leads"
    elif bearish_strength > bullish_strength:
        directional_context = "bearish_evidence_leads"
    else:
        directional_context = "split"

    label = {
        "unavailable": "Limited evidence",
        "aligned": "Aligned",
        "mild_divergence": "Mild Divergence",
        "moderate_divergence": "Moderate Divergence",
        "strong_divergence": "Strong Divergence",
    }[state]
    details = _short_explanation(state, directional_context, len(eligible))
    if state not in {"unavailable", "aligned"}:
        leader = bullish if bullish_strength >= bearish_strength else bearish
        opposing = bearish if bullish_strength >= bearish_strength else bullish
        lead_names = ", ".join(item["label"] for item in leader[:3])
        opposing_names = ", ".join(item["label"] for item in opposing[:3])
        if lead_names and opposing_names:
            details = f"{lead_names} lean {leader[0]['direction']}, while {opposing_names} lean {opposing[0]['direction']}."

    return {
        "methodology_version": CROSS_SOURCE_DIVERGENCE_METHODOLOGY_VERSION,
        "state": state,
        "label": label,
        "directional_context": directional_context,
        "public_explanation": _short_explanation(state, directional_context, len(eligible)),
        "explanation": details,
        "active_source_count": len(eligible),
        "bullish_source_count": len(bullish),
        "bearish_source_count": len(bearish),
        "bullish_strength": bullish_strength,
        "bearish_strength": bearish_strength,
        "conflict_ratio": conflict_ratio,
        "fast_group_state": fast_state,
        "slow_group_state": slow_state,
        "bullish_sources": bullish,
        "bearish_sources": bearish,
        "neutral_sources": [],
        "excluded_sources": excluded,
        "methodology": cross_source_divergence_methodology(),
    }


def public_cross_source_divergence(payload: dict[str, Any], *, allowed_source_keys: set[str] | None) -> dict[str, Any]:
    """Withhold named source evidence unless the viewer is entitled to it."""
    result = dict(payload)
    visible = allowed_source_keys or set()
    for key in ("bullish_sources", "bearish_sources", "neutral_sources"):
        items = payload.get(key) if isinstance(payload.get(key), list) else []
        result[key] = [item for item in items if isinstance(item, dict) and item.get("key") in visible]
    result["source_breakdown_available"] = bool(visible)
    if not visible:
        result["explanation"] = result.get("public_explanation")
    result.pop("methodology", None)
    result.pop("excluded_sources", None)
    result.pop("conflict_ratio", None)
    result.pop("bullish_strength", None)
    result.pop("bearish_strength", None)
    return result
