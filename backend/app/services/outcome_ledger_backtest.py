from __future__ import annotations

import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date
from statistics import mean
from typing import Any, Callable

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import ConfirmationScoreSnapshot
from app.services.confirmation_score import SHORT_HORIZON_SOURCES, SOURCE_ORDER, THIRTY_DAY_DURABLE_SOURCES
from app.services.outcome_ledger import (
    OUTCOME_SCORE_BANDS,
    _apply_snapshot_filters,
    _directional_side,
    _prefetch_outcome_price_rows,
    _project_directional_outcome_events,
    _score_band_for_score,
    _snapshot_row,
)


HORIZON = "30D"
MIN_PUBLIC_CLAIM_SAMPLE = 100


@dataclass(frozen=True)
class CleanTrainingEvent:
    snapshot_id: int
    ticker: str
    market_date: date
    score: int
    score_band: str
    direction: str
    side: str
    calculation_type: str
    lifecycle_status: str
    active_source_count: int
    active_sources: tuple[str, ...]
    source_contributions: dict[str, Any]
    source_freshness: dict[str, Any]
    return_pct: float
    directional_return_pct: float
    spy_return_pct: float | None
    excess_return_pct: float | None
    directional_excess_return_pct: float | None
    directionally_correct: bool
    raw_directionally_correct: bool | None
    benchmark_directionally_correct: bool | None
    source_payload_quality: str


def _json_dict(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _is_placeholder_source_payload(contributions: dict[str, Any]) -> bool:
    if not contributions:
        return True
    present = [value for value in contributions.values() if isinstance(value, dict) and value.get("present") is True]
    if not present:
        return True
    return all(
        value.get("direction") is None
        and value.get("strength") is None
        and value.get("quality") is None
        and value.get("score_contribution") is None
        for value in present
    )


def _source_payload_quality(row: dict[str, Any]) -> str:
    contributions = _json_dict(row.get("source_contributions"))
    if _is_placeholder_source_payload(contributions):
        if row.get("calculation_type") == "historical_reconstruction":
            return "placeholder_backfill"
        return "missing_or_placeholder"
    return "real_source_payload"


def _event_from_row(row: dict[str, Any]) -> CleanTrainingEvent | None:
    outcome = (row.get("outcomes") or {}).get(HORIZON)
    if not isinstance(outcome, dict) or outcome.get("status") != "matured":
        return None
    if not isinstance(outcome.get("return_pct"), (int, float)):
        return None
    if not isinstance(outcome.get("directionally_correct"), bool):
        return None
    side = _directional_side(str(row.get("direction") or ""))
    if side not in {"bullish", "bearish"}:
        return None
    market_date_raw = row.get("market_date")
    try:
        market_date = date.fromisoformat(str(market_date_raw)[:10])
    except (TypeError, ValueError):
        return None
    active_sources = tuple(str(source) for source in row.get("active_sources") or [] if str(source) in SOURCE_ORDER)
    score = int(row.get("score") or 0)
    source_contributions = _json_dict(row.get("source_contributions"))
    return CleanTrainingEvent(
        snapshot_id=int(row.get("id") or 0),
        ticker=str(row.get("ticker") or "").upper(),
        market_date=market_date,
        score=score,
        score_band=_score_band_for_score(score),
        direction=str(row.get("direction") or "").lower(),
        side=side,
        calculation_type=str(row.get("calculation_type") or ""),
        lifecycle_status=str(row.get("lifecycle_status") or ""),
        active_source_count=int(row.get("active_source_count") or len(active_sources)),
        active_sources=active_sources,
        source_contributions=source_contributions,
        source_freshness=_json_dict(row.get("source_freshness")),
        return_pct=float(outcome["return_pct"]),
        directional_return_pct=float(outcome.get("directional_return_pct") or 0.0),
        spy_return_pct=float(outcome["spy_return_pct"]) if isinstance(outcome.get("spy_return_pct"), (int, float)) else None,
        excess_return_pct=float(outcome["excess_return_pct"]) if isinstance(outcome.get("excess_return_pct"), (int, float)) else None,
        directional_excess_return_pct=(
            float(outcome["directional_excess_return_pct"])
            if isinstance(outcome.get("directional_excess_return_pct"), (int, float))
            else None
        ),
        directionally_correct=bool(outcome["directionally_correct"]),
        raw_directionally_correct=(
            bool(outcome["raw_directionally_correct"]) if isinstance(outcome.get("raw_directionally_correct"), bool) else None
        ),
        benchmark_directionally_correct=(
            bool(outcome["benchmark_directionally_correct"])
            if isinstance(outcome.get("benchmark_directionally_correct"), bool)
            else None
        ),
        source_payload_quality=_source_payload_quality(row),
    )


def load_clean_training_events(
    db: Session,
    *,
    start_date: date | None = None,
    end_date: date | None = None,
    calculation_type: str | None = None,
) -> tuple[list[CleanTrainingEvent], dict[str, int]]:
    query = _apply_snapshot_filters(
        select(ConfirmationScoreSnapshot),
        start_date=start_date,
        end_date=end_date,
        calculation_type=calculation_type,
    )
    rows = db.execute(
        query.order_by(
            ConfirmationScoreSnapshot.security_id.asc(),
            ConfirmationScoreSnapshot.market_date.asc(),
            ConfirmationScoreSnapshot.calculated_at.asc(),
            ConfirmationScoreSnapshot.id.asc(),
        )
    ).scalars().all()
    events = _project_directional_outcome_events(rows)
    projected_snapshots = [event.snapshot for event in events]
    price_rows = _prefetch_outcome_price_rows(db, projected_snapshots, horizons=(30,))
    exclusions: Counter[str] = Counter()
    cleaned: list[CleanTrainingEvent] = []
    for event in events:
        row = _snapshot_row(
            db,
            event.snapshot,
            include_internal=True,
            price_rows_by_symbol=price_rows,
            closed_at=event.closed_at,
        )
        outcome = (row.get("outcomes") or {}).get(HORIZON)
        if not isinstance(outcome, dict):
            exclusions["missing_outcome_payload"] += 1
            continue
        status = str(outcome.get("status") or "")
        if status != "matured":
            exclusions[f"not_matured:{status or 'unknown'}"] += 1
            continue
        item = _event_from_row(row)
        if item is None:
            exclusions["invalid_matured_row"] += 1
            continue
        cleaned.append(item)
    return cleaned, dict(sorted(exclusions.items()))


def _pct(numerator: int, denominator: int) -> float | None:
    if denominator <= 0:
        return None
    return round((numerator / denominator) * 100, 1)


def _avg(values: list[float | None]) -> float | None:
    real = [value for value in values if isinstance(value, (int, float))]
    if not real:
        return None
    return round(mean(real), 2)


def metric_summary(events: list[CleanTrainingEvent]) -> dict[str, Any]:
    benchmarked = [event for event in events if event.benchmark_directionally_correct is not None]
    bullish = [event for event in events if event.side == "bullish"]
    bearish = [event for event in events if event.side == "bearish"]
    return {
        "sample_size": len(events),
        "accuracy": _pct(sum(1 for event in events if event.directionally_correct), len(events)),
        "raw_accuracy": _pct(
            sum(1 for event in events if event.raw_directionally_correct is True),
            sum(1 for event in events if event.raw_directionally_correct is not None),
        ),
        "benchmark_accuracy": _pct(
            sum(1 for event in benchmarked if event.benchmark_directionally_correct is True),
            len(benchmarked),
        ),
        "average_return": _avg([event.directional_return_pct for event in events]),
        "average_excess_vs_spy": _avg([event.directional_excess_return_pct for event in events]),
        "bullish_accuracy": _pct(sum(1 for event in bullish if event.directionally_correct), len(bullish)),
        "bearish_accuracy": _pct(sum(1 for event in bearish if event.directionally_correct), len(bearish)),
        "bullish_sample": len(bullish),
        "bearish_sample": len(bearish),
    }


def _score_band_metrics(events: list[CleanTrainingEvent]) -> list[dict[str, Any]]:
    by_band: dict[str, list[CleanTrainingEvent]] = {band: [] for band in OUTCOME_SCORE_BANDS}
    for event in events:
        by_band.setdefault(event.score_band, []).append(event)
    return [
        {
            "band": band,
            **metric_summary(items),
        }
        for band, items in by_band.items()
    ]


def _source_is_present(event: CleanTrainingEvent, source: str) -> bool:
    if source in event.active_sources:
        return True
    payload = event.source_contributions.get(source)
    return isinstance(payload, dict) and payload.get("present") is True


def _source_direction(event: CleanTrainingEvent, source: str) -> str | None:
    payload = event.source_contributions.get(source)
    if isinstance(payload, dict):
        direction = str(payload.get("direction") or "").strip().lower()
        if direction in {"bullish", "bearish", "mixed", "neutral"}:
            return direction
    return None


def _source_strength(event: CleanTrainingEvent, source: str) -> float | None:
    payload = event.source_contributions.get(source)
    if not isinstance(payload, dict):
        return None
    try:
        return float(payload.get("strength"))
    except (TypeError, ValueError):
        return None


def _source_freshness_days(event: CleanTrainingEvent, source: str) -> int | None:
    payload = event.source_contributions.get(source)
    if isinstance(payload, dict):
        raw = payload.get("freshness_days")
        if isinstance(raw, int):
            return raw
    freshness = event.source_freshness.get(source)
    if isinstance(freshness, int):
        return freshness
    return None


def _agreement_bucket(event: CleanTrainingEvent) -> str:
    directions = [
        direction
        for source in SOURCE_ORDER
        if (direction := _source_direction(event, source)) in {"bullish", "bearish", "mixed"}
    ]
    if not directions:
        return "unknown"
    if "mixed" in directions:
        return "conflicted"
    unique = set(directions)
    if len(unique) == 1:
        return "aligned"
    return "conflicted"


def _freshness_bucket(event: CleanTrainingEvent) -> str:
    values = [
        days
        for source in SOURCE_ORDER
        if (days := _source_freshness_days(event, source)) is not None
    ]
    if not values:
        return "unknown"
    freshest = min(values)
    if freshest <= 7:
        return "fresh_0_7d"
    if freshest <= 30:
        return "fresh_8_30d"
    if freshest <= 90:
        return "aging_31_90d"
    return "stale_90d_plus"


def _score_change_bucket(_event: CleanTrainingEvent) -> str:
    # The current immutable snapshot table does not yet persist prior score deltas.
    return "unavailable"


def _sector_regime_bucket(_event: CleanTrainingEvent) -> str:
    # Sector-relative regime data is not currently captured point-in-time on the outcome row.
    return "unavailable"


def group_metrics(
    events: list[CleanTrainingEvent],
    key_fn: Callable[[CleanTrainingEvent], str | None],
    *,
    min_sample: int = 1,
) -> list[dict[str, Any]]:
    groups: dict[str, list[CleanTrainingEvent]] = defaultdict(list)
    for event in events:
        key = key_fn(event)
        if key:
            groups[key].append(event)
    rows = []
    for key, items in groups.items():
        if len(items) < min_sample:
            continue
        rows.append(
            {
                "key": key,
                **metric_summary(items),
                "score_bands": _score_band_metrics(items),
            }
        )
    rows.sort(key=lambda item: (-int(item["sample_size"]), str(item["key"])))
    return rows


def component_metrics(events: list[CleanTrainingEvent]) -> dict[str, Any]:
    component_eligible = [event for event in events if event.source_payload_quality == "real_source_payload"]
    metrics: dict[str, Any] = {
        "component_eligible_sample": len(component_eligible),
        "source_payload_quality": dict(Counter(event.source_payload_quality for event in events)),
        "components": {},
        "source_combinations": group_metrics(
            component_eligible,
            lambda event: "+".join(event.active_sources) if event.active_sources else None,
        )[:25],
        "source_agreement": group_metrics(component_eligible, _agreement_bucket),
        "source_freshness": group_metrics(component_eligible, _freshness_bucket),
        "score_change_over_time": group_metrics(component_eligible, _score_change_bucket),
        "spy_sector_regime": group_metrics(component_eligible, _sector_regime_bucket),
    }
    for source in SOURCE_ORDER:
        present = [event for event in component_eligible if _source_is_present(event, source)]
        absent = [event for event in component_eligible if not _source_is_present(event, source)]
        directional = group_metrics(present, lambda event, source=source: _source_direction(event, source) or "unknown")
        strength = group_metrics(
            present,
            lambda event, source=source: (
                "strength_80_plus"
                if ((value := _source_strength(event, source)) is not None and value >= 80)
                else "strength_60_79"
                if value is not None and value >= 60
                else "strength_1_59"
                if value is not None and value > 0
                else "strength_unknown"
            ),
        )
        metrics["components"][source] = {
            "present": metric_summary(present),
            "absent": metric_summary(absent),
            "direction": directional,
            "strength": strength,
            "score_bands_when_present": _score_band_metrics(present),
        }
    return metrics


def _candidate_rules() -> list[tuple[str, Callable[[CleanTrainingEvent], bool]]]:
    rules: list[tuple[str, Callable[[CleanTrainingEvent], bool]]] = []
    for min_score in (60, 65, 70, 75, 80):
        rules.append((f"score>={min_score}", lambda event, min_score=min_score: event.score >= min_score))
    for min_sources in (2, 3, 4):
        rules.append((f"sources>={min_sources}", lambda event, min_sources=min_sources: event.active_source_count >= min_sources))
    for min_score in (60, 65, 70, 75):
        for min_sources in (2, 3):
            rules.append(
                (
                    f"score>={min_score} and sources>={min_sources}",
                    lambda event, min_score=min_score, min_sources=min_sources: event.score >= min_score
                    and event.active_source_count >= min_sources,
                )
            )
    rules.extend(
        [
            ("durable_source_present", lambda event: any(source in event.active_sources for source in THIRTY_DAY_DURABLE_SOURCES)),
            ("short_horizon_source_absent", lambda event: not any(source in event.active_sources for source in SHORT_HORIZON_SOURCES)),
            (
                "aligned_real_sources",
                lambda event: event.source_payload_quality == "real_source_payload" and _agreement_bucket(event) == "aligned",
            ),
            (
                "score>=65 aligned_real_sources",
                lambda event: event.score >= 65
                and event.source_payload_quality == "real_source_payload"
                and _agreement_bucket(event) == "aligned",
            ),
            (
                "bullish>=65 bearish>=75",
                lambda event: (event.side == "bullish" and event.score >= 65) or (event.side == "bearish" and event.score >= 75),
            ),
            (
                "bullish>=70 bearish>=80",
                lambda event: (event.side == "bullish" and event.score >= 70) or (event.side == "bearish" and event.score >= 80),
            ),
        ]
    )
    return rules


def candidate_v2_backtests(events: list[CleanTrainingEvent], *, min_sample: int = MIN_PUBLIC_CLAIM_SAMPLE) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for name, predicate in _candidate_rules():
        kept = [event for event in events if predicate(event)]
        rejected = [event for event in events if not predicate(event)]
        summary = metric_summary(kept)
        failures = Counter(f"{event.side}:{event.score_band}" for event in kept if not event.directionally_correct)
        rows.append(
            {
                "rule": name,
                **summary,
                "calls_kept": len(kept),
                "calls_rejected": len(rejected),
                "coverage_pct": _pct(len(kept), len(events)),
                "meets_min_sample": len(kept) >= min_sample,
                "top_failure_modes": [
                    {"bucket": bucket, "count": count}
                    for bucket, count in failures.most_common(8)
                ],
            }
        )
    rows.sort(
        key=lambda item: (
            not bool(item["meets_min_sample"]),
            -(item.get("accuracy") or -1),
            -(item.get("average_excess_vs_spy") or -999),
            -int(item["calls_kept"]),
        )
    )
    return rows


def recommended_weight_hints(component_report: dict[str, Any], baseline: dict[str, Any]) -> list[dict[str, Any]]:
    baseline_accuracy = baseline.get("accuracy")
    baseline_excess = baseline.get("average_excess_vs_spy")
    hints: list[dict[str, Any]] = []
    for source, report in component_report.get("components", {}).items():
        present = report.get("present") or {}
        sample = int(present.get("sample_size") or 0)
        accuracy = present.get("accuracy")
        excess = present.get("average_excess_vs_spy")
        if sample <= 0 or baseline_accuracy is None:
            action = "insufficient_data"
        elif sample < MIN_PUBLIC_CLAIM_SAMPLE:
            action = "observe_more"
        elif accuracy >= baseline_accuracy + 5 and (baseline_excess is None or (excess or -999) >= baseline_excess):
            action = "consider_upweight"
        elif accuracy <= baseline_accuracy - 5 or (baseline_excess is not None and excess is not None and excess < baseline_excess - 1):
            action = "consider_downweight"
        else:
            action = "keep_neutral"
        hints.append(
            {
                "component": source,
                "sample_size": sample,
                "accuracy": accuracy,
                "average_excess_vs_spy": excess,
                "baseline_accuracy": baseline_accuracy,
                "recommended_action": action,
            }
        )
    hints.sort(key=lambda item: (-int(item["sample_size"]), str(item["component"])))
    return hints


def build_outcome_ledger_v2_backtest_report(
    db: Session,
    *,
    start_date: date | None = None,
    end_date: date | None = None,
    calculation_type: str | None = None,
    min_sample: int = MIN_PUBLIC_CLAIM_SAMPLE,
) -> dict[str, Any]:
    events, exclusions = load_clean_training_events(
        db,
        start_date=start_date,
        end_date=end_date,
        calculation_type=calculation_type,
    )
    baseline = metric_summary(events)
    components = component_metrics(events)
    candidates = candidate_v2_backtests(events, min_sample=min_sample)
    return {
        "horizon": HORIZON,
        "clean_training_set": {
            "events": len(events),
            "exclusions": exclusions,
            "calculation_types": dict(Counter(event.calculation_type for event in events)),
            "source_payload_quality": dict(Counter(event.source_payload_quality for event in events)),
        },
        "baseline": {
            **baseline,
            "score_bands": _score_band_metrics(events),
        },
        "component_analysis": components,
        "candidate_v2_rules": candidates,
        "weight_hints": recommended_weight_hints(components, baseline),
        "decision": _decision(candidates, events, min_sample=min_sample),
    }


def _decision(candidates: list[dict[str, Any]], events: list[CleanTrainingEvent], *, min_sample: int) -> dict[str, Any]:
    viable = [
        candidate
        for candidate in candidates
        if candidate.get("meets_min_sample") and (candidate.get("accuracy") or 0) >= 70
    ]
    if not viable:
        return {
            "status": "do_not_ship_calibrated_v2",
            "reason": "No candidate rule reached 70%+ 30D accuracy at the required sample size.",
            "minimum_sample": min_sample,
            "available_clean_events": len(events),
        }
    best = viable[0]
    return {
        "status": "candidate_ready_for_review",
        "rule": best["rule"],
        "accuracy": best["accuracy"],
        "calls_kept": best["calls_kept"],
        "coverage_pct": best["coverage_pct"],
    }

