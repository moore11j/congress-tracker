from __future__ import annotations

import argparse
import json
from datetime import date

from app.db import SessionLocal
from app.services.outcome_ledger_backtest import build_outcome_ledger_v2_backtest_report


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    return date.fromisoformat(value)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a read-only Outcome Ledger 30D component/v2 backtest report.")
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--calculation-type", default=None)
    parser.add_argument("--min-sample", type=int, default=100)
    parser.add_argument("--indent", type=int, default=2)
    args = parser.parse_args()

    with SessionLocal() as db:
        report = build_outcome_ledger_v2_backtest_report(
            db,
            start_date=_parse_date(args.start_date),
            end_date=_parse_date(args.end_date),
            calculation_type=args.calculation_type,
            min_sample=max(1, int(args.min_sample or 100)),
        )
    print(json.dumps(report, indent=args.indent, sort_keys=True, default=str))


if __name__ == "__main__":
    main()

