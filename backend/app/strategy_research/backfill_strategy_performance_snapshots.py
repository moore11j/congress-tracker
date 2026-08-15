"""Repair stored strategy period metrics from their persisted equity curves."""

from __future__ import annotations

import argparse
import json
from typing import Any

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from app.db import SessionLocal
from app.models import StrategyBacktestRun, StrategyDefinition, StrategyEquityCurvePoint, StrategyPerformanceSnapshot
from app.services.strategy_performance_metrics import trailing_snapshot_values
from app.services.strategy_refresh import json_dumps


def _loads(value: str | None) -> dict[str, Any]:
    try:
        parsed = json.loads(value or "{}")
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def backfill_strategy_performance_snapshots(
    db: Session,
    *,
    slug: str | None = None,
    apply: bool = False,
) -> dict[str, Any]:
    statement = (
        select(StrategyBacktestRun, StrategyDefinition)
        .join(StrategyDefinition, StrategyDefinition.id == StrategyBacktestRun.strategy_id)
        .where(StrategyBacktestRun.status == "ok")
        .order_by(StrategyDefinition.slug.asc(), StrategyBacktestRun.completed_at.desc().nullslast(), StrategyBacktestRun.id.desc())
    )
    if slug:
        statement = statement.where(StrategyDefinition.slug == slug)

    rows = db.execute(statement).all()
    result_rows: list[dict[str, Any]] = []
    for run, strategy in rows:
        points = db.execute(
            select(StrategyEquityCurvePoint)
            .where(StrategyEquityCurvePoint.run_id == int(run.id))
            .order_by(StrategyEquityCurvePoint.date.asc())
        ).scalars().all()
        if not points:
            result_rows.append({"slug": strategy.slug, "run_id": int(run.id), "status": "skipped", "reason": "missing_equity_curve"})
            continue
        as_of_date = run.backtest_end_date or points[-1].date
        snapshot_values = trailing_snapshot_values(
            strategy_id=int(strategy.id),
            run_id=int(run.id),
            as_of_date=as_of_date,
            points=points,
            baseline_metrics=_loads(run.metrics_json),
            walnut_score=run.walnut_strategy_score,
        )
        if apply:
            db.execute(delete(StrategyPerformanceSnapshot).where(StrategyPerformanceSnapshot.run_id == int(run.id)))
            db.add_all(
                StrategyPerformanceSnapshot(**{
                    **values,
                    "metrics_json": json_dumps(values["metrics_json"]),
                })
                for values in snapshot_values
            )
        result_rows.append(
            {
                "slug": strategy.slug,
                "run_id": int(run.id),
                "status": "updated" if apply else "would_update",
                "equity_points": len(points),
                "periods": [values["period"] for values in snapshot_values],
            }
        )
    if apply:
        db.commit()
    return {"mode": "apply" if apply else "dry_run", "rows": result_rows}


def main() -> None:
    parser = argparse.ArgumentParser(description="Recalculate stored trailing strategy metrics from persisted equity curves.")
    parser.add_argument("--slug", help="Repair only one strategy slug.")
    parser.add_argument("--apply", action="store_true", help="Write repaired snapshots. Dry-run by default.")
    args = parser.parse_args()
    with SessionLocal() as db:
        print(json.dumps(backfill_strategy_performance_snapshots(db, slug=args.slug, apply=args.apply), sort_keys=True))


if __name__ == "__main__":
    main()
