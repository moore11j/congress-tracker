"""Rejected score-ceiling candidate; offline research only.

Weights reproduce divergence-v2. Retain this implementation for reproducibility,
not for use by the application: the candidate did not improve primary 30D accuracy.
"""
from __future__ import annotations

from math import isfinite
from typing import Any

MAX_EVIDENCE_AGE_DAYS = 90
MIN_MATERIAL_CONTRIBUTION = 2.0


def _number(value: Any) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return 0.0
    return parsed if isfinite(parsed) else 0.0


def evidence_freshness(source: dict[str, Any]) -> int | None:
    try:
        value = source.get("freshness_days")
        return int(value) if value is not None else None
    except (TypeError, ValueError, OverflowError):
        return None


def evidence_magnitude(source: dict[str, Any]) -> float:
    contribution = abs(_number(source.get("score_contribution")))
    if contribution > 0:
        return contribution
    strength = max(0.0, min(100.0, _number(source.get("strength"))))
    quality = max(0.0, min(100.0, _number(source.get("quality"))))
    return round((strength * 0.50 + quality * 0.35) / 10.0, 2)


def evidence_exclusion(source: Any) -> str | None:
    if not isinstance(source, dict) or source.get("present") is not True:
        return "inactive"
    if str(source.get("direction") or "neutral").lower() not in {"bullish", "bearish"}:
        return "neutral_or_mixed"
    age = evidence_freshness(source)
    if age is not None and (age < 0 or age > MAX_EVIDENCE_AGE_DAYS):
        return "stale"
    if evidence_magnitude(source) < MIN_MATERIAL_CONTRIBUTION:
        return "immaterial"
    return None


def directional_score_ceiling(sources: dict[str, dict[str, Any]], direction: str) -> int:
    """A directional score cannot exceed the share of evidence supporting it.

    Apply after saturation so additive bonuses cannot erase disagreement. This
    is a consistency bound, not a probability estimate or a new return model.
    """
    if direction not in {"bullish", "bearish"}:
        return 100
    aligned = opposing = 0.0
    for source in sources.values():
        if evidence_exclusion(source) is not None:
            continue
        weight = evidence_magnitude(source)
        if source["direction"] == direction:
            aligned += weight
        else:
            opposing += weight
    if opposing <= 0:
        return 100
    return min(99, int(100 * aligned / (aligned + opposing)))
