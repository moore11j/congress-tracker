"""Shared material-evidence weights for confirmation and its divergence display."""
from __future__ import annotations

from math import isfinite
from typing import Any

MATERIAL_EVIDENCE_MAX_FRESHNESS_DAYS = 90
MIN_MATERIAL_CONTRIBUTION = 2.0

# Full-source bullish capacity totals 100. Missing evidence never shrinks it.
SOURCE_MAX_POINTS = {
    "fundamentals": 20.0,
    "institutional_activity": 20.0,
    "price_volume": 15.0,
    "congress": 10.0,
    "insiders": 10.0,
    "analysts": 8.0,
    "signals": 5.0,
    "options_flow": 5.0,
    "macro_positioning": 5.0,
    "government_contracts": 2.0,
}
INSIDER_MAX_POINTS = {"bullish": 10.0, "bearish": 1.0, "mixed": 3.0}


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
    return round(source_max_points(key, str(source.get("direction") or "neutral").lower()) * strength_fraction, 4)


def evidence_exclusion(source: Any) -> str | None:
    if not isinstance(source, dict) or source.get("present") is not True:
        return "inactive"
    if str(source.get("direction") or "neutral").lower() not in {"bullish", "bearish"}:
        return "neutral_or_mixed"
    age = evidence_freshness(source)
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
            if evidence_exclusion(source) is not None:
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
    """Weighted full-source confirmation on a fixed 100-point capacity.

    A fixed direction is monotonic in each eligible source's evidence weight.
    No activity, breadth, quality or freshness bonuses are added separately.
    Quality and freshness already affect each source's evidence magnitude.
    """
    sources = {key: sources.get(key, {}) for key in SOURCE_MAX_POINTS}
    weights = {key: round(evidence_magnitude(source, key), 2)
               if evidence_exclusion(source) is None else 0.0
               for key, source in sources.items()}
    signed = {key: (weight if sources[key].get("direction") == direction else -weight)
              if direction in {"bullish", "bearish"} else 0.0
              for key, weight in weights.items()}
    aligned = round(sum(value for value in signed.values() if value > 0), 2)
    opposing = round(-sum(value for value in signed.values() if value < 0), 2)
    total = round(aligned + opposing, 2)
    net = round(aligned - opposing, 2)
    capacity = sum(SOURCE_MAX_POINTS.values())
    raw_score = max(0.0, 100 * net / capacity)
    score = max(0, min(100, int(round(raw_score))))
    # Neither missing coverage nor near-perfect inputs may round up to 100.
    if net < capacity:
        score = min(score, 99)
    aligned_count = sum(value > 0 for value in signed.values())
    return {"method": "weighted_full_source_confirmation", "aligned_weight": aligned,
            "opposing_weight": opposing, "net_weight": net, "total_weight": total,
            "capacity_weight": capacity, "aligned_source_count": aligned_count,
            "source_count": len(SOURCE_MAX_POINTS),
            "raw_score": round(raw_score, 4), "score": score,
            "single_source_cap_applied": False,
            "source_weights": weights,
            "source_contributions": {key: round(100 * value / capacity, 4)
                                     for key, value in signed.items()}}
