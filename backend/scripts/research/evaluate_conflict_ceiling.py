"""Offline audit of one fixed score ceiling on frozen opening evidence.

No fitting, threshold selection, direction changes, database access, or history
rewrites. The prior research cohort is reused, so this is exploratory evidence.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

from conflict_ceiling import directional_score_ceiling


def revised_score(row: dict) -> int:
    sources = {}
    for key, value in row["sources"].items():
        source = dict(value)
        freshness = row.get("freshness", {}).get(key)
        if isinstance(freshness, dict):
            source["freshness_days"] = freshness.get("freshness_days")
        sources[key] = source
    return min(int(row["score"]), directional_score_ceiling(sources, row["direction"]))


def metrics(rows: list[dict], horizon: str) -> dict:
    n = len(rows)
    wins = sum(row["outcomes"][horizon]["correct"] is True for row in rows)
    raw_wins = sum(row["outcomes"][horizon]["raw_correct"] is True for row in rows)
    return {
        "n": n, "wins": wins,
        "accuracy_pct": round(100 * wins / n, 2) if n else None,
        "raw_accuracy_pct": round(100 * raw_wins / n, 2) if n else None,
        "unique_securities": len({row["security_id"] for row in rows}),
    }


def compare(rows: list[dict], horizon: str, threshold: int) -> dict:
    baseline = [r for r in rows if r["score"] >= threshold]
    retained = [r for r in baseline if revised_score(r) >= threshold]
    removed = [r for r in baseline if revised_score(r) < threshold]
    return {
        "baseline": metrics(baseline, horizon),
        "revised": metrics(retained, horizon),
        "removed": metrics(removed, horizon),
        "retained_pct": round(100 * len(retained) / len(baseline), 2) if baseline else None,
    }


def evaluate(events: list[dict]) -> dict:
    results = {}
    for horizon in ("30", "7"):
        mature = [r for r in events if horizon in r.get("outcomes", {}) and r["direction"] in {"bullish", "bearish"}]
        partitions = {
            "all": mature,
            "prior_research_test_securities": [r for r in mature if int(hashlib.sha256(f"confirmation-30d-research-v1|{r['security_id']}".encode()).hexdigest()[:8], 16) % 100 >= 80],
        }
        first = {}
        for row in sorted(mature, key=lambda r: (r["entry_date"], r["id"])):
            first.setdefault(row["security_id"], row)
        partitions["first_per_security"] = list(first.values())
        results[horizon] = {
            "mature_events": len(mature),
            "entry_date_range": [min(r["entry_date"] for r in mature), max(r["entry_date"] for r in mature)] if mature else [],
            "methodology_counts": {str(key): sum(r["methodology_id"] == key for r in mature) for key in sorted({r["methodology_id"] for r in mature})},
            "partitions": {
                partition: {
                    direction: {
                        str(threshold): compare([r for r in subset if direction == "all" or r["direction"] == direction], horizon, threshold)
                        for threshold in (0, 40, 60, 80)
                    }
                    for direction in ("all", "bullish", "bearish")
                }
                for partition, subset in partitions.items()
            },
        }
    return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("cohort", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    raw = args.cohort.read_bytes()
    events = json.loads(raw)["events"]
    report = {
        "cohort_sha256": hashlib.sha256(raw).hexdigest(),
        "rule": "min(original frozen score, floor(100 * aligned material weight / total material directional weight))",
        "primary_comparison": "30D bullish, existing strong-or-better threshold 60",
        "limitations": [
            "Original directions, entries and measured outcome flags are unchanged.",
            "This only evaluates the score ceiling; source-date repairs cannot be reconstructed from these aggregated inputs.",
            "This cohort has been used in prior research and is not an untouched test or prospective validation.",
            "Thresholds 40, 60 and 80 are existing product bands, reported without tuning or selecting a winner.",
            "Retained-cohort accuracy is not overall prediction improvement; removed signals are abstentions, not corrected calls.",
        ],
        "results": evaluate(events),
    }
    assert args.cohort.read_bytes() == raw
    args.output.write_text(json.dumps(report, indent=2), encoding="utf-8")
    for h in ("30", "7"):
        print(h + "D bullish >=60: " + json.dumps(report["results"][h]["partitions"]["all"]["bullish"]["60"]))


if __name__ == "__main__":
    main()
