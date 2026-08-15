from __future__ import annotations

import argparse
import json
import subprocess
from typing import Any

from app.db import SessionLocal
from app.services.replicated_portfolio_strategy_refresh import persist_top_congress_portfolio_strategies
from app.utils.symbols import normalize_symbol


def _git_sha() -> str | None:
    try:
        completed = subprocess.run(
            ["git", "rev-parse", "--short=12", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        )
    except Exception:
        return None
    value = completed.stdout.strip()
    return value or None


def _print_text(result: dict[str, Any]) -> None:
    metadata = result.get("metadata") or {}
    print(
        "REPLICATED_PORTFOLIO_STRATEGY_REFRESH "
        f"mode={metadata.get('mode')} publish={metadata.get('publish')} "
        f"lookback_days={metadata.get('lookback_days')} eligible={metadata.get('eligible_runs')} "
        f"rows={metadata.get('rows')}"
    )
    for row in result.get("rows") or []:
        print(
            "ROW "
            f"rank={row.get('rank')} slug={row.get('slug')} source_run_id={row.get('source_run_id')} "
            f"score={row.get('walnut_strategy_score')} cagr={row.get('cagr_pct')} "
            f"alpha={row.get('alpha_pct')} sharpe={row.get('sharpe')} "
            f"mdd={row.get('max_drawdown_pct')} positions={row.get('positions_count')} "
            f"holdings={row.get('current_holdings')} as_of={row.get('as_of_date')}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Persist top individual Congress replicated portfolios into strategy storage. "
            "Dry-run by default; pass --apply to write draft rows."
        )
    )
    parser.add_argument("--lookback-days", type=int, default=1095)
    parser.add_argument("--top", type=int, default=10)
    parser.add_argument("--benchmark", default="SPY")
    parser.add_argument(
        "--entity-ids",
        help="Optional comma-separated Congress member IDs. Refreshes only these existing portfolio strategies.",
    )
    parser.add_argument("--min-positions", type=int, default=1)
    parser.add_argument("--min-points", type=int, default=2)
    parser.add_argument("--ranking", choices=("alpha", "cagr", "walnut_score"), default="alpha", help="Match the Congress leaderboard rank order.")
    parser.add_argument("--code-version", help="Code version to record. Defaults to current git short SHA when available.")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--publish", action="store_true", help="Publish strategies immediately. Requires --apply.")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    if args.publish and not args.apply:
        parser.error("--publish requires --apply")
    benchmark = normalize_symbol(args.benchmark) or "SPY"
    code_version = args.code_version or _git_sha()
    with SessionLocal() as db:
        result = persist_top_congress_portfolio_strategies(
            db,
            lookback_days=args.lookback_days,
            top=args.top,
            benchmark=benchmark,
            min_positions=args.min_positions,
            min_points=args.min_points,
            ranking=args.ranking,
            entity_ids=[value.strip() for value in (args.entity_ids or "").split(",") if value.strip()],
            code_version=code_version,
            publish=args.publish,
            apply=args.apply,
        )
    result["metadata"]["code_version"] = code_version
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        _print_text(result)


if __name__ == "__main__":
    main()
