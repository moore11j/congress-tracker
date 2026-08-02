from __future__ import annotations

import argparse
import json
import subprocess
from datetime import date
from typing import Any

from app.db import SessionLocal
from app.services.strategy_refresh import persist_candidate_strategy_artifact
from app.strategy_research.candidate_strategy_artifacts import build_candidate_strategy_artifact
from app.strategy_research.candidate_strategy_validation import (
    _candidate_lookup,
    _parse_candidate_slugs,
    validate_candidate,
)
from app.strategy_research.congress_buys import parse_iso_date
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


def _print_text(rows: list[dict[str, Any]], *, apply: bool) -> None:
    mode = "APPLY" if apply else "DRY_RUN"
    print(f"STRATEGY_REFRESH {mode} rows={len(rows)}")
    for row in rows:
        print(
            "ROW "
            f"slug={row.get('slug')} mode={row.get('mode')} status={row.get('status')} "
            f"run_key={row.get('run_key')} as_of={row.get('as_of_date')} "
            f"lots={row.get('lots')} equity_points={row.get('equity_points')} "
            f"holdings={row.get('current_holdings')} confidence={row.get('data_quality_confidence')} "
            f"score={row.get('walnut_strategy_score')}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Build candidate strategy artifacts and persist strategy storage rows. "
            "Dry-run by default; pass --apply to write."
        )
    )
    parser.add_argument("--candidates", help="Comma-separated candidate slugs. Defaults to all built-in candidates.")
    parser.add_argument("--start-date", help="Optional full-history start date override.")
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--benchmark", default="SPY")
    parser.add_argument("--slippage-bps", type=float, default=5.0)
    parser.add_argument("--fee-bps", type=float, default=0.0)
    parser.add_argument("--allow-raw-prices", action="store_true")
    parser.add_argument("--min-lots", type=int, default=50)
    parser.add_argument("--snapshot-source-kind", default="ticker_financials_cache_statement_proxy")
    parser.add_argument("--min-snapshots-per-symbol", type=int, default=1)
    parser.add_argument("--top", type=int, default=5)
    parser.add_argument("--apply", action="store_true", help="Write rows to strategy storage tables.")
    parser.add_argument("--publish", action="store_true", help="Mark definitions published. Defaults to draft/preserve existing.")
    parser.add_argument("--skip-validation", action="store_true", help="Skip train/validation/test score calculation.")
    parser.add_argument("--train-start", default="2014-01-01")
    parser.add_argument("--train-end", default="2019-12-31")
    parser.add_argument("--validation-start", default="2020-01-01")
    parser.add_argument("--validation-end", default="2024-07-30")
    parser.add_argument("--test-start", default="2024-07-31")
    parser.add_argument("--test-end", help="Validation test end. Defaults to --end-date.")
    parser.add_argument("--code-version", help="Code version to record. Defaults to current git short SHA when available.")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    start_date = parse_iso_date(args.start_date)
    end_date = parse_iso_date(args.end_date) or date.today()
    benchmark = normalize_symbol(args.benchmark) or "SPY"
    code_version = args.code_version or _git_sha()
    lookup = _candidate_lookup()
    rows: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []

    with SessionLocal() as db:
        for slug in _parse_candidate_slugs(args.candidates):
            candidate = lookup.get(slug)
            if not candidate:
                errors.append({"slug": slug, "error": "unknown_candidate"})
                continue
            try:
                artifact = build_candidate_strategy_artifact(
                    db,
                    candidate,
                    start_date=start_date,
                    end_date=end_date,
                    benchmark=benchmark,
                    slippage_bps=args.slippage_bps,
                    fee_bps=args.fee_bps,
                    require_adjusted=not args.allow_raw_prices,
                    min_lots=args.min_lots,
                    snapshot_source_kind=args.snapshot_source_kind,
                    min_snapshots_per_symbol=args.min_snapshots_per_symbol,
                    top=args.top,
                )
                validation = None
                if not args.skip_validation:
                    validation = validate_candidate(
                        db,
                        candidate,
                        train_start=parse_iso_date(args.train_start) or date(2014, 1, 1),
                        train_end=parse_iso_date(args.train_end) or date(2019, 12, 31),
                        validation_start=parse_iso_date(args.validation_start) or date(2020, 1, 1),
                        validation_end=parse_iso_date(args.validation_end) or date(2024, 7, 30),
                        test_start=parse_iso_date(args.test_start) or date(2024, 7, 31),
                        test_end=parse_iso_date(args.test_end) or end_date,
                        benchmark=benchmark,
                        slippage_bps=args.slippage_bps,
                        fee_bps=args.fee_bps,
                        require_adjusted=not args.allow_raw_prices,
                        min_lots=args.min_lots,
                        snapshot_source_kind=args.snapshot_source_kind,
                        min_snapshots_per_symbol=args.min_snapshots_per_symbol,
                        top=args.top,
                    )
                rows.append(
                    persist_candidate_strategy_artifact(
                        db,
                        artifact,
                        validation_result=validation,
                        code_version=code_version,
                        publish=args.publish,
                        apply=args.apply,
                    )
                )
            except Exception as exc:
                if args.apply:
                    db.rollback()
                errors.append({"slug": slug, "error": str(exc)})

    result = {
        "metadata": {
            "mode": "apply" if args.apply else "dry_run",
            "publish": bool(args.publish),
            "code_version": code_version,
            "rows": len(rows),
            "errors": len(errors),
        },
        "rows": rows,
        "errors": errors,
    }
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        _print_text(rows, apply=args.apply)
        for error in errors:
            print(f"ERROR slug={error.get('slug')} error={error.get('error')}")


if __name__ == "__main__":
    main()
