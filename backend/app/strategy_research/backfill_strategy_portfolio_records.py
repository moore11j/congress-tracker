"""Backfill durable transaction history for replicated strategy backtests.

Candidate strategies write their ledger as part of their normal artifact refresh.
Replicated Congress portfolios retain their source positions separately, so this
command copies those auditable positions into the common strategy transaction
ledger without recomputing a strategy or changing portfolio results.
"""

from __future__ import annotations

import argparse
import json

from sqlalchemy import delete, select

from app.db import SessionLocal
from app.models import ReplicatedPortfolioPosition, StrategyBacktestRun, StrategyDefinition, StrategyHistoricalTransaction
from app.services.replicated_portfolio_strategy_refresh import _historical_transactions


def _source_run_id(run: StrategyBacktestRun) -> int | None:
    try:
        dataset = json.loads(run.dataset_versions_json or "{}")
    except (TypeError, ValueError):
        return None
    value = dataset.get("source_run_id") if isinstance(dataset, dict) else None
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill persisted transaction records for replicated strategy portfolios.")
    parser.add_argument("--apply", action="store_true", help="Write records. Defaults to a read-only report.")
    args = parser.parse_args()

    rows: list[dict[str, object]] = []
    with SessionLocal() as db:
        strategies = db.execute(select(StrategyDefinition).where(StrategyDefinition.status == "published")).scalars().all()
        for strategy in strategies:
            run = db.execute(
                select(StrategyBacktestRun)
                .where(StrategyBacktestRun.strategy_id == int(strategy.id), StrategyBacktestRun.status == "ok")
                .order_by(StrategyBacktestRun.completed_at.desc().nullslast(), StrategyBacktestRun.id.desc())
                .limit(1)
            ).scalars().first()
            if run is None:
                rows.append({"slug": strategy.slug, "status": "missing_run"})
                continue
            source_run_id = _source_run_id(run)
            if source_run_id is None:
                rows.append({"slug": strategy.slug, "status": "candidate_refresh_required", "run_id": int(run.id)})
                continue
            positions = db.execute(
                select(ReplicatedPortfolioPosition)
                .where(ReplicatedPortfolioPosition.run_id == source_run_id)
                .order_by(ReplicatedPortfolioPosition.id.asc())
            ).scalars().all()
            transactions = _historical_transactions(
                strategy_id=int(strategy.id),
                strategy_run_id=int(run.id),
                positions=positions,
            )
            rows.append({
                "slug": strategy.slug,
                "status": "ready",
                "run_id": int(run.id),
                "source_run_id": source_run_id,
                "positions": len(positions),
                "transactions": len(transactions),
            })
            if args.apply:
                db.execute(delete(StrategyHistoricalTransaction).where(StrategyHistoricalTransaction.strategy_run_id == int(run.id)))
                db.add_all(transactions)
        if args.apply:
            db.commit()

    print(json.dumps({"mode": "apply" if args.apply else "dry_run", "rows": rows}, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
