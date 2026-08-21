from __future__ import annotations

import argparse
import json
from collections import defaultdict
from typing import Any, Iterable

from app.db import SessionLocal
from app.services.outcome_ledger import OUTCOME_SCORE_BANDS, list_outcome_snapshots

COMPONENTS = (
    "price_volume",
    "fundamentals",
    "analysts",
    "congress",
    "insiders",
    "institutional_activity",
    "macro_positioning",
    "signals",
    "options_flow",
    "government_contracts",
)


def _score_band(score: int) -> str:
    if score >= 80:
        return "80+"
    if score >= 75:
        return "75-79"
    if score >= 70:
        return "70-74"
    if score >= 65:
        return "65-69"
    if score >= 60:
        return "60-64"
    if score >= 40:
        return "40-59"
    return "0-39"


def _avg(values: Iterable[float]) -> float | None:
    items = list(values)
    return round(sum(items) / len(items), 2) if items else None


def _source_keys(row: dict[str, Any]) -> tuple[str, ...]:
    contributions = row.get("source_contributions")
    if isinstance(contributions, dict) and contributions:
        return tuple(
            sorted(
                key
                for key, value in contributions.items()
                if isinstance(value, dict) and value.get("present") is True
            )
        )
    sources = row.get("active_sources")
    if isinstance(sources, list):
        return tuple(sorted(str(source) for source in sources if source))
    return ()


def _source_payload(row: dict[str, Any], key: str) -> dict[str, Any]:
    contributions = row.get("source_contributions")
    if isinstance(contributions, dict):
        payload = contributions.get(key)
        if isinstance(payload, dict):
            return payload
    return {}


def _source_present(row: dict[str, Any], key: str) -> bool:
    payload = _source_payload(row, key)
    if payload:
        return payload.get("present") is True
    sources = row.get("active_sources")
    return isinstance(sources, list) and key in {str(source) for source in sources}


def _source_direction(row: dict[str, Any], key: str) -> str:
    payload = _source_payload(row, key)
    value = str(payload.get("direction") or payload.get("status") or "inactive").strip().lower()
    if "bull" in value:
        return "bullish"
    if "bear" in value:
        return "bearish"
    if "mixed" in value:
        return "mixed"
    if "neutral" in value:
        return "neutral"
    return "active" if _source_present(row, key) else "inactive"


def _source_freshness_bucket(row: dict[str, Any], key: str) -> str:
    payload = _source_payload(row, key)
    freshness = payload.get("freshness_days")
    if not isinstance(freshness, (int, float)):
        freshness_payload = row.get("source_freshness")
        if isinstance(freshness_payload, dict):
            freshness = freshness_payload.get(key)
    if not isinstance(freshness, (int, float)):
        return "unknown"
    if freshness <= 7:
        return "0-7d"
    if freshness <= 30:
        return "8-30d"
    if freshness <= 90:
        return "31-90d"
    return "90d+"


def _agreement_bucket(row: dict[str, Any]) -> str:
    directions = [
        _source_direction(row, key)
        for key in COMPONENTS
        if _source_present(row, key)
    ]
    bullish = sum(1 for direction in directions if direction == "bullish")
    bearish = sum(1 for direction in directions if direction == "bearish")
    if bullish and bearish:
        return "conflicted"
    if bullish >= 2:
        return "bullish_agreement"
    if bearish >= 2:
        return "bearish_agreement"
    if bullish == 1 or bearish == 1:
        return "single_directional_source"
    return "no_directional_sources"


def _spy_regime(row: dict[str, Any], horizon: str) -> str:
    outcome = row["outcomes"][horizon]
    spy_return = outcome.get("spy_return_pct")
    if not isinstance(spy_return, (int, float)):
        return "unknown"
    if spy_return >= 2:
        return "spy_up_2pct_plus"
    if spy_return <= -2:
        return "spy_down_2pct_plus"
    return "spy_flat"


def _bullish_accuracy(rows: list[dict[str, Any]], horizon: str) -> dict[str, Any]:
    return _stats([row for row in rows if row.get("direction") == "bullish"], horizon)


def _bearish_accuracy(rows: list[dict[str, Any]], horizon: str) -> dict[str, Any]:
    return _stats([row for row in rows if row.get("direction") == "bearish"], horizon)


def _stats(rows: list[dict[str, Any]], horizon: str) -> dict[str, Any]:
    outcomes = [row["outcomes"][horizon] for row in rows]
    directional = [outcome for outcome in outcomes if isinstance(outcome.get("directionally_correct"), bool)]
    benchmarked = [
        outcome
        for outcome in outcomes
        if isinstance(outcome.get("directional_return_pct"), (int, float))
        and isinstance(outcome.get("spy_return_pct"), (int, float))
    ]
    return {
        "n": len(directional),
        "accuracy": round(100 * sum(1 for outcome in directional if outcome["directionally_correct"]) / len(directional), 1)
        if directional
        else None,
        "avg_directional_return": _avg(float(outcome["directional_return_pct"]) for outcome in directional if isinstance(outcome.get("directional_return_pct"), (int, float))),
        "avg_spy_return": _avg(float(outcome["spy_return_pct"]) for outcome in benchmarked),
        "avg_excess_vs_spy": _avg(float(outcome["directional_excess_return_pct"]) for outcome in benchmarked if isinstance(outcome.get("directional_excess_return_pct"), (int, float))),
    }


def _cohort_table(rows: list[dict[str, Any]], horizon: str, key_fn, *, min_sample: int) -> list[dict[str, Any]]:
    cohorts: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        cohorts[str(key_fn(row))].append(row)
    table = []
    for name, cohort_rows in cohorts.items():
        stats = _stats(cohort_rows, horizon)
        if stats["n"] >= min_sample:
            table.append({"cohort": name, **stats})
    return sorted(table, key=lambda item: (item["accuracy"] or 0, item["n"]), reverse=True)


def _load_rows(horizon: str, limit: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with SessionLocal() as db:
        page = 0
        while True:
            payload = list_outcome_snapshots(db, page=page, limit=limit, include_internal=True)
            rows.extend(payload["items"])
            if not payload["has_next"]:
                break
            page += 1
    return [
        row
        for row in rows
        if row.get("direction") in {"bullish", "bearish"}
        and isinstance(row.get("outcomes"), dict)
        and isinstance(row["outcomes"].get(horizon), dict)
        and row["outcomes"][horizon].get("status") == "matured"
        and isinstance(row["outcomes"][horizon].get("directionally_correct"), bool)
    ]


def _component_tables(rows: list[dict[str, Any]], horizon: str, *, min_sample: int) -> dict[str, Any]:
    by_present = []
    by_direction = []
    by_freshness = []
    for key in COMPONENTS:
        present_rows = [row for row in rows if _source_present(row, key)]
        if len(present_rows) >= min_sample:
            by_present.append(
                {
                    "component": key,
                    **_stats(present_rows, horizon),
                    "bullish": _bullish_accuracy(present_rows, horizon),
                    "bearish": _bearish_accuracy(present_rows, horizon),
                }
            )
        direction_rows = _cohort_table(present_rows, horizon, lambda row, component=key: _source_direction(row, component), min_sample=min_sample)
        for item in direction_rows:
            by_direction.append({"component": key, **item})
        freshness_rows = _cohort_table(present_rows, horizon, lambda row, component=key: _source_freshness_bucket(row, component), min_sample=min_sample)
        for item in freshness_rows:
            by_freshness.append({"component": key, **item})
    return {
        "present": sorted(by_present, key=lambda item: (item["accuracy"] or 0, item["n"]), reverse=True),
        "direction": sorted(by_direction, key=lambda item: (item["accuracy"] or 0, item["n"]), reverse=True),
        "freshness": sorted(by_freshness, key=lambda item: (item["accuracy"] or 0, item["n"]), reverse=True),
    }


def _high_confidence_rules(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    def has_any(row: dict[str, Any], keys: set[str]) -> bool:
        return any(_source_present(row, key) for key in keys)

    def has_direction(row: dict[str, Any], key: str, direction: str) -> bool:
        return _source_direction(row, key) == direction

    durable = {"fundamentals", "analysts", "institutional_activity", "macro_positioning"}
    short_horizon = {"price_volume", "options_flow", "signals"}
    return {
        "score_70_plus_with_durable_source": [
            row for row in rows if int(row.get("score") or 0) >= 70 and has_any(row, durable)
        ],
        "score_75_plus_with_durable_source": [
            row for row in rows if int(row.get("score") or 0) >= 75 and has_any(row, durable)
        ],
        "bullish_75_plus_with_durable_source": [
            row for row in rows if row.get("direction") == "bullish" and int(row.get("score") or 0) >= 75 and has_any(row, durable)
        ],
        "bearish_70_plus_requires_non_tape_bearish": [
            row
            for row in rows
            if row.get("direction") == "bearish"
            and int(row.get("score") or 0) >= 70
            and any(has_direction(row, key, "bearish") for key in durable | {"congress", "insiders"})
        ],
        "exclude_short_horizon_only": [
            row
            for row in rows
            if not has_any(row, set(COMPONENTS) - short_horizon)
        ],
        "agreement_no_conflict": [
            row
            for row in rows
            if _agreement_bucket(row) in {"bullish_agreement", "bearish_agreement"}
        ],
    }


def _rule_stats(rows: list[dict[str, Any]], horizon: str, *, min_sample: int) -> dict[str, Any]:
    result = {}
    for name, kept_rows in _high_confidence_rules(rows).items():
        kept = _stats(kept_rows, horizon)
        if kept["n"] < min_sample:
            continue
        rejected_rows = [row for row in rows if row not in kept_rows]
        result[name] = {
            "kept": kept,
            "rejected": _stats(rejected_rows, horizon),
            "coverage_pct": round(100 * len(kept_rows) / len(rows), 1) if rows else None,
            "kept_count": len(kept_rows),
            "rejected_count": len(rejected_rows),
        }
    return result


def analyze(*, horizon: str, min_sample: int, limit: int) -> dict[str, Any]:
    rows = _load_rows(horizon, limit)
    source_combo_rows = _cohort_table(rows, horizon, lambda row: "+".join(_source_keys(row)) or "no_sources", min_sample=min_sample)
    return {
        "horizon": horizon,
        "grading_basis": "directional excess vs SPY; mixed/neutral excluded",
        "baseline": _stats(rows, horizon),
        "by_direction": _cohort_table(rows, horizon, lambda row: row.get("direction"), min_sample=min_sample),
        "by_methodology": _cohort_table(rows, horizon, lambda row: row.get("methodology") or "unknown", min_sample=min_sample),
        "by_score_band": [
            {"cohort": band, **_stats([row for row in rows if _score_band(int(row.get("score") or 0)) == band], horizon)}
            for band in OUTCOME_SCORE_BANDS
        ],
        "by_source_count": _cohort_table(rows, horizon, lambda row: row.get("active_source_count"), min_sample=min_sample),
        "by_agreement": _cohort_table(rows, horizon, _agreement_bucket, min_sample=min_sample),
        "by_spy_regime": _cohort_table(rows, horizon, lambda row: _spy_regime(row, horizon), min_sample=min_sample),
        "components": _component_tables(rows, horizon, min_sample=min_sample),
        "top_source_combinations": source_combo_rows[:20],
        "candidate_rules": _rule_stats(rows, horizon, min_sample=min_sample),
        "high_accuracy_warning": "Treat any 75%+ cohort as provisional unless it has enough live samples and survives walk-forward validation.",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze Outcome Ledger cohorts before proposing confirmation-v2.")
    parser.add_argument("--horizon", default="30D", choices=["7D", "30D", "90D", "180D", "365D"])
    parser.add_argument("--min-sample", type=int, default=30)
    parser.add_argument("--limit", type=int, default=5000)
    args = parser.parse_args()
    print(json.dumps(analyze(horizon=args.horizon, min_sample=args.min_sample, limit=args.limit), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
