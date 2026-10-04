"""Shared material-evidence weights for confirmation and its divergence display."""
from __future__ import annotations

from math import isfinite, sqrt
from typing import Any

MATERIAL_EVIDENCE_MAX_FRESHNESS_DAYS = 90
MACRO_EVIDENCE_MAX_FRESHNESS_DAYS = 10
MIN_MATERIAL_CONTRIBUTION = 2.0

# Full-source bullish capacity totals 100. Missing evidence never shrinks it.
# Transfer ten points to fundamentals, taking exactly 10/9 from each other source.
# Keep full precision here; rounding nine allocations would change the total.
OTHER_SOURCE_REDUCTION = 10.0 / 9.0
SOURCE_MAX_POINTS = {
    "fundamentals": 30.0,
    "institutional_activity": 20.0 - OTHER_SOURCE_REDUCTION,
    "price_volume": 15.0 - OTHER_SOURCE_REDUCTION,
    "congress": 10.0 - OTHER_SOURCE_REDUCTION,
    "insiders": 10.0 - OTHER_SOURCE_REDUCTION,
    "analysts": 8.0 - OTHER_SOURCE_REDUCTION,
    "signals": 5.0 - OTHER_SOURCE_REDUCTION,
    "options_flow": 5.0 - OTHER_SOURCE_REDUCTION,
    "macro_positioning": 5.0 - OTHER_SOURCE_REDUCTION,
    "government_contracts": 2.0 - OTHER_SOURCE_REDUCTION,
}
INSIDER_MAX_POINTS = {"bullish": SOURCE_MAX_POINTS["insiders"], "bearish": 1.0, "mixed": 3.0}


def source_max_points(key: str, direction: str) -> float:
    if key == "insiders":
        return INSIDER_MAX_POINTS.get(direction, 0.0)
    return SOURCE_MAX_POINTS.get(key, 0.0)


def freshness_score(days: int | None) -> int:
    if days is None:
        return 0
    if days <= 3:
        return 100
    if days <= 7:
        return 85
    if days <= 14:
        return 65
    if days <= 30:
        return 40
    return 15


def _number(value: Any) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        return 0.0
    return result if isfinite(result) else 0.0


def evidence_freshness(source: dict[str, Any]) -> int | None:
    try:
        value = source.get("freshness_days")
        return int(value) if value is not None else None
    except (TypeError, ValueError, OverflowError):
        return None


def _materiality_magnitude(source: dict[str, Any]) -> float:
    contribution = abs(_number(source.get("score_contribution")))
    if contribution > 0:
        return contribution
    strength = max(0.0, min(100.0, _number(source.get("strength"))))
    quality = max(0.0, min(100.0, _number(source.get("quality"))))
    return round((strength * 0.50 + quality * 0.35) / 10.0, 2)


def evidence_magnitude(source: dict[str, Any], key: str) -> float:
    """Apply approved priority once, rather than letting native bonuses bypass it."""
    strength = max(0.0, min(100.0, _number(source.get("strength"))))
    quality = max(0.0, min(100.0, _number(source.get("quality"))))
    freshness = freshness_score(evidence_freshness(source))
    strength_fraction = (strength * .50 + quality * .35 + freshness * .15) / 100
    return source_max_points(key, str(source.get("direction") or "neutral").lower()) * strength_fraction


def evidence_exclusion(source: Any, key: str | None = None) -> str | None:
    if not isinstance(source, dict) or source.get("present") is not True:
        return "inactive"
    if str(source.get("direction") or "neutral").lower() not in {"bullish", "bearish"}:
        return "neutral_or_mixed"
    age = evidence_freshness(source)
    if key == "macro_positioning" and (age is None or age > MACRO_EVIDENCE_MAX_FRESHNESS_DAYS):
        return "stale"
    if age is not None and (age < 0 or age > MATERIAL_EVIDENCE_MAX_FRESHNESS_DAYS):
        return "stale"
    # Test input materiality before applying priority: credible insider selling
    # remains weak opposing evidence instead of disappearing under a 2-point floor.
    if _materiality_magnitude(source) < MIN_MATERIAL_CONTRIBUTION:
        return "immaterial"
    return None


def confirmation_conflict_ceiling(sources: dict[str, dict[str, Any]], direction: str) -> dict[str, Any]:
    """Bound confidence by aligned evidence, not a forecast of positive returns."""
    aligned = opposing = 0.0
    if direction in {"bullish", "bearish"}:
        for key, source in sources.items():
            if evidence_exclusion(source, key) is not None:
                continue
            if str(source["direction"]).lower() == direction:
                aligned += round(evidence_magnitude(source, key), 2)
            else:
                opposing += round(evidence_magnitude(source, key), 2)
    # Integer hundredths avoid floating-point floor errors at exact boundaries.
    aligned_units, opposing_units = round(aligned * 100), round(opposing * 100)
    ceiling = min(99, 100 * aligned_units // (aligned_units + opposing_units)) if opposing_units else 100
    return {"ceiling": ceiling, "aligned_weight": aligned_units / 100,
            "opposing_weight": opposing_units / 100}


def net_confirmation(sources: dict[str, dict[str, Any]], direction: str) -> dict[str, Any]:
    """Agreement and evidence quality, discounted for limited weighted coverage.

    Source contributions remain signed evidence weights, not additive pieces of
    the nonlinear final score. Strength, quality and freshness enter once through
    the evidence weights. A lone source cannot reach moderate confirmation.
    """
    sources = {key: sources.get(key, {}) for key in SOURCE_MAX_POINTS}
    weights = {key: evidence_magnitude(source, key)
               if evidence_exclusion(source, key) is None else 0.0
               for key, source in sources.items()}
    signed = {key: (weight if sources[key].get("direction") == direction else -weight)
              if direction in {"bullish", "bearish"} else 0.0
              for key, weight in weights.items()}
    aligned = sum(value for value in signed.values() if value > 0)
    opposing = -sum(value for value in signed.values() if value < 0)
    total = aligned + opposing
    net = aligned - opposing
    capacity = 100.0
    aligned_keys = [key for key, value in signed.items() if value > 0]
    aligned_capacity = sum(SOURCE_MAX_POINTS[key] for key in aligned_keys)
    coverage = min(1.0, aligned_capacity / capacity)
    agreement = max(0.0, net / total) if total else 0.0
    quality = min(1.0, aligned / aligned_capacity) if aligned_capacity else 0.0
    raw_score = (80.0 * agreement + 20.0 * quality) * sqrt(coverage) if total else 0.0
    score = max(0, min(100, int(round(raw_score))))
    single_source_cap_applied = len(aligned_keys) < 2 and score > 39
    if single_source_cap_applied:
        score = 39
    # Neither missing coverage nor near-perfect inputs may round up to 100.
    if not all(signed[key] >= maximum for key, maximum in SOURCE_MAX_POINTS.items()):
        score = min(score, 99)
    aligned_count = len(aligned_keys)
    return {"method": "agreement_quality_weighted_coverage", "aligned_weight": round(aligned, 4),
            "opposing_weight": round(opposing, 4), "net_weight": round(net, 4), "total_weight": round(total, 4),
            "capacity_weight": capacity, "aligned_source_count": aligned_count,
            "aligned_capacity": round(aligned_capacity, 4),
            "agreement": round(agreement, 6), "evidence_quality": round(quality, 6),
            "weighted_coverage": round(coverage, 6), "coverage_multiplier": round(sqrt(coverage), 6),
            "source_count": len(SOURCE_MAX_POINTS),
            "raw_score": round(raw_score, 4), "score": score,
            "single_source_cap_applied": single_source_cap_applied,
            "source_weights": {key: round(value, 4) for key, value in weights.items()},
            "source_contributions": {key: round(100 * value / capacity, 4)
                                     for key, value in signed.items()}}
