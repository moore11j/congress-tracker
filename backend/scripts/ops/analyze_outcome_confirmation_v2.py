from __future__ import annotations

import argparse
import json
from collections import defaultdict
from typing import Any, Iterable

from app.db import SessionLocal
from app.services.outcome_ledger import OUTCOME_SCORE_BANDS, list_outcome_snapshots


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


def analyze(*, horizon: str, min_sample: int, limit: int) -> dict[str, Any]:
    rows = _load_rows(horizon, limit)
    source_combo_rows = _cohort_table(rows, horizon, lambda row: "+".join(_source_keys(row)) or "no_sources", min_sample=min_sample)
    candidate_rules = {
        "bullish_only": [row for row in rows if row.get("direction") == "bullish"],
        "bearish_only": [row for row in rows if row.get("direction") == "bearish"],
        "score_80_plus": [row for row in rows if int(row.get("score") or 0) >= 80],
        "score_75_plus_bullish": [row for row in rows if int(row.get("score") or 0) >= 75 and row.get("direction") == "bullish"],
        "source_count_6_plus": [row for row in rows if int(row.get("active_source_count") or 0) >= 6],
        "source_count_8_plus": [row for row in rows if int(row.get("active_source_count") or 0) >= 8],
    }
    return {
        "horizon": horizon,
        "baseline": _stats(rows, horizon),
        "by_direction": _cohort_table(rows, horizon, lambda row: row.get("direction"), min_sample=min_sample),
        "by_score_band": [
            {"cohort": band, **_stats([row for row in rows if _score_band(int(row.get("score") or 0)) == band], horizon)}
            for band in OUTCOME_SCORE_BANDS
        ],
        "by_source_count": _cohort_table(rows, horizon, lambda row: row.get("active_source_count"), min_sample=min_sample),
        "top_source_combinations": source_combo_rows[:20],
        "candidate_rules": {
            name: _stats(rule_rows, horizon)
            for name, rule_rows in candidate_rules.items()
            if len(rule_rows) >= min_sample
        },
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
