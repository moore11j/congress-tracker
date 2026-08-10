from __future__ import annotations

import argparse
import json
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db import SessionLocal
from app.models import StrategyBacktestRun, StrategyDefinition, StrategyPerformanceSnapshot
from app.strategy_research.candidate_strategy_validation import walnut_strategy_score_from_validation_result


def _loads(value: str | None) -> dict[str, Any]:
    try:
        parsed = json.loads(value or "{}")
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def recalibrate_persisted_strategy_scores(db: Session, *, apply: bool = False) -> dict[str, Any]:
    """Re-score candidate runs from their recorded validation periods, without a backtest rerun."""
    rows: list[dict[str, Any]] = []
    runs = db.execute(
        select(StrategyBacktestRun, StrategyDefinition.slug)
        .join(StrategyDefinition, StrategyDefinition.id == StrategyBacktestRun.strategy_id)
        .order_by(StrategyDefinition.slug, StrategyBacktestRun.id)
    ).all()
    for run, slug in runs:
        diagnostics = _loads(run.diagnostics_json)
        validation_result = diagnostics.get("validation")
        if not isinstance(validation_result, dict):
            continue
        score = walnut_strategy_score_from_validation_result(validation_result)
        if score is None:
            continue

        previous_score = run.walnut_strategy_score
        rows.append(
            {
                "slug": slug,
                "run_id": int(run.id),
                "before": round(float(previous_score), 2) if previous_score is not None else None,
                "after": score["score"],
                "score_version": score["score_version"],
            }
        )
        if not apply:
            continue

        validation_result["walnut_strategy_score"] = score
        diagnostics["validation"] = validation_result
        run.diagnostics_json = json.dumps(diagnostics, separators=(",", ":"), sort_keys=True)
        run.walnut_strategy_score = float(score["score"])
        snapshots = db.execute(
            select(StrategyPerformanceSnapshot).where(StrategyPerformanceSnapshot.run_id == run.id)
        ).scalars()
        for snapshot in snapshots:
            snapshot.walnut_strategy_score = float(score["score"])

    if apply:
        db.commit()
    return {"mode": "apply" if apply else "dry_run", "updated_runs": len(rows), "rows": rows}


def main() -> None:
    parser = argparse.ArgumentParser(description="Recalibrate persisted candidate Strategy Scores from recorded validation data.")
    parser.add_argument("--apply", action="store_true", help="Persist recalibrated scores. Defaults to a read-only preview.")
    args = parser.parse_args()
    db = SessionLocal()
    try:
        print(json.dumps(recalibrate_persisted_strategy_scores(db, apply=args.apply), sort_keys=True))
    finally:
        db.close()


if __name__ == "__main__":
    main()
