"""Point-in-time historical setup matching for ticker Context.

Only live confirmation snapshots are eligible. Historical reconstructions store
placeholder source states, so including them would create look-ahead-like,
non-comparable matches even though their subsequent returns are real.
"""
from __future__ import annotations

import json
import os
from datetime import date, datetime, timezone
from statistics import median
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import ConfirmationScoreSnapshot
from app.services.confirmation_score import SOURCE_LABELS, SOURCE_ORDER
from app.services.cross_source_divergence import CROSS_SOURCE_DIVERGENCE_METHODOLOGY_VERSION, build_cross_source_divergence
from app.services.outcome_ledger import (
    _directional_side,
    _prefetch_outcome_price_rows,
    _snapshot_outcomes,
    current_confirmation_methodology,
)


HISTORICAL_SIMILARITY_METHODOLOGY_VERSION = "similarity-v2"
DEFAULT_HORIZONS = ("7D", "30D")
MIN_PREVIEW_SAMPLE = 5
STANDARD_SAMPLE = 20
MAX_CANDIDATES = 5000
MAX_MATCHES = 250
MIN_SIMILARITY = 55.0
SIMILARITY_WEIGHTS = {
    "score": 0.22,
    "source_vector": 0.38,
    "active_source_count": 0.10,
    "divergence": 0.14,
    "horizon_state": 0.06,
    "sector": 0.04,
    "freshness": 0.06,
}


def similar_historical_setups_enabled() -> bool:
    return os.getenv("SIMILAR_HISTORICAL_SETUPS_ENABLED", "true").strip().lower() not in {"0", "false", "no", "off"}


def historical_similarity_methodology() -> dict[str, Any]:
    return {
        "version": HISTORICAL_SIMILARITY_METHODOLOGY_VERSION,
        "cohort_rules": {
            "calculation_type": "live",
            "same_confirmation_methodology": True,
            "direction_match": True,
            "exclude_historical_reconstruction": True,
            "minimum_directional_sources": 2,
            "same_ticker_episode_cooldown_days": 30,
        },
        "weights": SIMILARITY_WEIGHTS,
        "minimum_similarity": MIN_SIMILARITY,
        "minimum_preview_sample": MIN_PREVIEW_SAMPLE,
        "standard_sample": STANDARD_SAMPLE,
        "horizons": list(DEFAULT_HORIZONS),
    }


def _loads(raw: str | None, fallback: Any) -> Any:
    try:
        value = json.loads(raw or "")
    except (TypeError, ValueError):
        return fallback
    return value


def _number(value: Any) -> float:
    try:
        value = float(value)
    except (TypeError, ValueError):
        return 0.0
    return value if value == value else 0.0


def _source_magnitude(source: dict[str, Any]) -> float:
    contribution = abs(_number(source.get("score_contribution")))
    if contribution:
        return min(contribution, 25.0)
    return min(max((_number(source.get("strength")) * 0.50 + _number(source.get("quality")) * 0.35) / 10.0, 0.0), 25.0)


def _merge_snapshot_sources(snapshot: ConfirmationScoreSnapshot) -> dict[str, dict[str, Any]]:
    contributions = _loads(snapshot.source_contributions_json, {})
    freshness = _loads(snapshot.source_freshness_json, {})
    if not isinstance(contributions, dict):
        return {}
    result: dict[str, dict[str, Any]] = {}
    for key in SOURCE_ORDER:
        source = contributions.get(key)
        if not isinstance(source, dict):
            continue
        merged = dict(source)
        fresh = freshness.get(key) if isinstance(freshness, dict) else None
        if isinstance(fresh, dict) and "freshness_days" not in merged:
            merged["freshness_days"] = fresh.get("freshness_days")
        result[key] = merged
    return result


def _freshness_bucket_value(source: dict[str, Any]) -> float:
    value = source.get("freshness_days")
    try:
        days = int(value) if value is not None else None
    except (TypeError, ValueError):
        days = None
    if days is None:
        return 0.5
    if days <= 7:
        return 0.0
    if days <= 30:
        return 0.33
    if days <= 90:
        return 0.66
    return 1.0


def _source_vector(sources: dict[str, dict[str, Any]]) -> tuple[dict[str, float], int, dict[str, float]]:
    vector: dict[str, float] = {}
    freshness: dict[str, float] = {}
    active = 0
    for key in SOURCE_ORDER:
        source = sources.get(key)
        if not isinstance(source, dict) or source.get("present") is not True:
            vector[key] = 0.0
            freshness[key] = 0.0
            continue
        direction = str(source.get("direction") or "neutral").lower()
        magnitude = _source_magnitude(source) / 25.0
        freshness[key] = _freshness_bucket_value(source)
        if direction == "bullish":
            vector[key] = magnitude
            active += 1
        elif direction == "bearish":
            vector[key] = -magnitude
            active += 1
        else:
            vector[key] = 0.0
    return vector, active, freshness


def _snapshot_divergence(snapshot: ConfirmationScoreSnapshot, sources: dict[str, dict[str, Any]]) -> dict[str, Any]:
    contributions = _loads(snapshot.source_contributions_json, {})
    persisted = contributions.get("__cross_source_divergence") if isinstance(contributions, dict) else None
    if isinstance(persisted, dict) and persisted.get("methodology_version") == CROSS_SOURCE_DIVERGENCE_METHODOLOGY_VERSION:
        return persisted
    # Safe fallback for older *live* source snapshots: it only reads fields
    # captured in that snapshot, never today’s ticker state.
    return build_cross_source_divergence({"sources": sources})


def _snapshot_sector(snapshot: ConfirmationScoreSnapshot) -> str | None:
    contributions = _loads(snapshot.source_contributions_json, {})
    features = contributions.get("__v2_features") if isinstance(contributions, dict) else None
    context = features.get("regime_context") if isinstance(features, dict) else None
    sector = context.get("sector") if isinstance(context, dict) else None
    return str(sector).strip() if isinstance(sector, str) and sector.strip() else None


def _profile_from_bundle(bundle: dict[str, Any], *, sector: str | None) -> dict[str, Any] | None:
    direction = _directional_side(str(bundle.get("direction") or ""))
    sources = bundle.get("sources") if isinstance(bundle.get("sources"), dict) else {}
    sources = {key: value for key, value in sources.items() if isinstance(value, dict)}
    vector, active_sources, freshness = _source_vector(sources)
    if direction is None or active_sources < 2:
        return None
    divergence = build_cross_source_divergence({"sources": sources})
    return {
        "score": int(_number(bundle.get("score"))),
        "direction": direction,
        "sources": sources,
        "vector": vector,
        "active_source_count": active_sources,
        "freshness": freshness,
        "divergence": divergence,
        "sector": sector,
    }


def _profile_from_snapshot(snapshot: ConfirmationScoreSnapshot) -> dict[str, Any] | None:
    direction = _directional_side(snapshot.direction)
    sources = _merge_snapshot_sources(snapshot)
    vector, active_sources, freshness = _source_vector(sources)
    if direction is None or active_sources < 2:
        return None
    return {
        "score": snapshot.score,
        "direction": direction,
        "sources": sources,
        "vector": vector,
        "active_source_count": active_sources,
        "freshness": freshness,
        "divergence": _snapshot_divergence(snapshot, sources),
        "sector": _snapshot_sector(snapshot),
    }


def _distance(current: dict[str, Any], candidate: dict[str, Any]) -> float:
    weights = SIMILARITY_WEIGHTS
    score_loss = min(abs(current["score"] - candidate["score"]) / 40.0, 1.0)
    vector_loss = sum(abs(current["vector"][key] - candidate["vector"][key]) / 2.0 for key in SOURCE_ORDER) / len(SOURCE_ORDER)
    source_count_loss = min(abs(current["active_source_count"] - candidate["active_source_count"]) / 5.0, 1.0)
    freshness_loss = sum(abs(current["freshness"][key] - candidate["freshness"][key]) for key in SOURCE_ORDER) / len(SOURCE_ORDER)
    current_divergence = current["divergence"]
    candidate_divergence = candidate["divergence"]
    divergence_loss = 0.0 if current_divergence.get("state") == candidate_divergence.get("state") else 1.0
    horizon_loss = 0.0 if (
        current_divergence.get("fast_group_state") == candidate_divergence.get("fast_group_state")
        and current_divergence.get("slow_group_state") == candidate_divergence.get("slow_group_state")
    ) else 1.0
    current_sector = str(current.get("sector") or "").strip().lower()
    candidate_sector = str(candidate.get("sector") or "").strip().lower()
    sector_loss = 0.0 if current_sector and candidate_sector and current_sector == candidate_sector else 0.5
    return (
        score_loss * weights["score"]
        + vector_loss * weights["source_vector"]
        + source_count_loss * weights["active_source_count"]
        + divergence_loss * weights["divergence"]
        + horizon_loss * weights["horizon_state"]
        + sector_loss * weights["sector"]
        + freshness_loss * weights["freshness"]
    )


def _similarity_reasons(current: dict[str, Any], candidate: dict[str, Any]) -> list[str]:
    reasons = [f"{candidate['score']} score vs current {current['score']}", f"{candidate['direction'].title()} direction"]
    if candidate["divergence"].get("state") == current["divergence"].get("state"):
        reasons.append(f"{str(current['divergence'].get('label') or 'matching divergence').lower()}")
    matching_sources = [
        SOURCE_LABELS[key]
        for key in SOURCE_ORDER
        if current["vector"][key] and candidate["vector"][key] and (current["vector"][key] > 0) == (candidate["vector"][key] > 0)
    ]
    if matching_sources:
        reasons.append(f"{', '.join(matching_sources[:3])} aligned")
    return reasons[:4]


def _median(values: list[float]) -> float | None:
    return round(float(median(values)), 2) if values else None


def _horizon_metrics(matches: list[dict[str, Any]], horizon: str) -> dict[str, Any]:
    matured = [match for match in matches if isinstance(match.get("outcomes", {}).get(horizon), dict) and match["outcomes"][horizon].get("status") == "matured"]
    outcomes = [match["outcomes"][horizon] for match in matured]
    directional = [float(item["directional_return_pct"]) for item in outcomes if isinstance(item.get("directional_return_pct"), (int, float))]
    excess = [float(item["directional_excess_return_pct"]) for item in outcomes if isinstance(item.get("directional_excess_return_pct"), (int, float))]
    correct = [item["directionally_correct"] for item in outcomes if isinstance(item.get("directionally_correct"), bool)]
    sample_size = len(matured)
    status = "ready" if sample_size >= STANDARD_SAMPLE else "limited" if sample_size >= MIN_PREVIEW_SAMPLE else "building"
    return {
        "status": status,
        "sample_size": sample_size,
        "directional_accuracy_pct": round(sum(1 for item in correct if item) / len(correct) * 100, 1) if correct else None,
        "median_directional_return_pct": _median(directional),
        "median_directional_excess_vs_spy_pct": _median(excess),
        "sample_warning": (
            "Historical coverage is still building; treat this small cohort cautiously."
            if status == "limited"
            else None
        ),
    }


def _episode_deduped(rows: list[ConfirmationScoreSnapshot]) -> list[ConfirmationScoreSnapshot]:
    """Keep one live event per ticker/direction within a 30-day episode."""
    kept: list[ConfirmationScoreSnapshot] = []
    last_by_ticker_direction: dict[tuple[str, str], date] = {}
    for row in sorted(rows, key=lambda item: (item.market_date, item.calculated_at, item.id)):
        side = _directional_side(row.direction)
        if side is None:
            continue
        key = (row.ticker_at_time.upper(), side)
        previous = last_by_ticker_direction.get(key)
        if previous is not None and (row.market_date - previous).days < 30:
            continue
        last_by_ticker_direction[key] = row.market_date
        kept.append(row)
    return kept


def _empty_payload(current: dict[str, Any] | None, *, status: str = "building") -> dict[str, Any]:
    return {
        "status": status,
        "methodology_version": HISTORICAL_SIMILARITY_METHODOLOGY_VERSION,
        "current_setup": current,
        "match_count": 0,
        "horizons": {horizon: _horizon_metrics([], horizon) for horizon in DEFAULT_HORIZONS},
        "top_matches": [],
        "sample_warning": "Historical coverage is building from live point-in-time confirmation snapshots.",
        "cohort_type": "live_prospective_only",
    }


def build_similar_historical_setups(
    db: Session,
    *,
    symbol: str,
    confirmation_bundle: dict[str, Any],
    sector: str | None = None,
) -> dict[str, Any]:
    """Match a current evidence profile against comparable mature live events."""
    current = _profile_from_bundle(confirmation_bundle, sector=sector)
    if current is None:
        return _empty_payload(None, status="unavailable")
    methodology = current_confirmation_methodology(db)
    today = datetime.now(timezone.utc).date()
    rows = db.execute(
        select(ConfirmationScoreSnapshot)
        .where(
            ConfirmationScoreSnapshot.calculation_type == "live",
            ConfirmationScoreSnapshot.methodology_version_id == methodology.id,
            ConfirmationScoreSnapshot.market_date < today,
        )
        .order_by(ConfirmationScoreSnapshot.market_date.desc(), ConfirmationScoreSnapshot.id.desc())
        .limit(MAX_CANDIDATES)
    ).scalars().all()
    candidates: list[dict[str, Any]] = []
    for row in _episode_deduped(rows):
        candidate = _profile_from_snapshot(row)
        if candidate is None or candidate["direction"] != current["direction"]:
            continue
        distance = _distance(current, candidate)
        similarity = round(max(0.0, (1.0 - distance) * 100.0), 1)
        if similarity < MIN_SIMILARITY:
            continue
        candidates.append(
            {
                "snapshot": row,
                "profile": candidate,
                "similarity": similarity,
                "reasons": _similarity_reasons(current, candidate),
            }
        )
    candidates.sort(key=lambda item: (-item["similarity"], item["snapshot"].market_date, item["snapshot"].id))
    matches = candidates[:MAX_MATCHES]
    price_rows = _prefetch_outcome_price_rows(db, [item["snapshot"] for item in matches])
    for match in matches:
        match["outcomes"] = _snapshot_outcomes(db, match["snapshot"], price_rows_by_symbol=price_rows)
    horizons = {horizon: _horizon_metrics(matches, horizon) for horizon in DEFAULT_HORIZONS}
    thirty_day_state = horizons["30D"]["status"]
    top_matches = []
    for match in matches[:5]:
        snapshot = match["snapshot"]
        top_matches.append(
            {
                "ticker": snapshot.ticker_at_time,
                "market_date": snapshot.market_date.isoformat(),
                "score": snapshot.score,
                "direction": _directional_side(snapshot.direction),
                "similarity": match["similarity"],
                "reasons": match["reasons"],
                "outcomes": {key: match["outcomes"].get(key) for key in DEFAULT_HORIZONS},
            }
        )
    return {
        "status": thirty_day_state,
        "methodology_version": HISTORICAL_SIMILARITY_METHODOLOGY_VERSION,
        "current_setup": {
            "score": current["score"],
            "direction": current["direction"],
            "divergence": current["divergence"].get("label"),
            "active_source_count": current["active_source_count"],
        },
        "match_count": len(matches),
        "horizons": horizons,
        "top_matches": top_matches,
        "sample_warning": horizons["30D"].get("sample_warning"),
        "cohort_type": "live_prospective_only",
    }


def public_similar_historical_setups(payload: dict[str, Any], *, include_details: bool) -> dict[str, Any]:
    """Apply server-side entitlement gating without fabricating hidden results."""
    result = dict(payload)
    if include_details:
        return result
    horizons = result.get("horizons") if isinstance(result.get("horizons"), dict) else {}
    result["horizons"] = {
        horizon: {
            "status": (preview := horizons.get(horizon) if isinstance(horizons.get(horizon), dict) else {}).get("status", "building"),
            "sample_size": preview.get("sample_size", 0),
            "directional_accuracy_pct": preview.get("directional_accuracy_pct"),
        }
        for horizon in DEFAULT_HORIZONS
    }
    result["top_matches"] = []
    result["access"] = {"locked": True, "required_plan": "premium"}
    return result
