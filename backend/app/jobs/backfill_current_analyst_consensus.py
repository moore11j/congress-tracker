from __future__ import annotations

import argparse
import json

from app.jobs.refresh_analyst_consensus import refresh_analyst_consensus


def _parse_symbols(value: str | None) -> list[str] | None:
    if value is None:
        return None
    return [part.strip() for part in value.split(",") if part.strip()]


def main() -> None:
    parser = argparse.ArgumentParser(description="Safely backfill current Analyst Consensus snapshots.")
    parser.add_argument("--symbols", help="Optional comma-separated symbols.")
    parser.add_argument("--limit", type=int, default=25, help="Maximum symbols to process. Defaults to 25 for safety.")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--sleep-seconds", type=float, default=0.0)
    args = parser.parse_args()
    print(
        json.dumps(
            refresh_analyst_consensus(
                symbols=_parse_symbols(args.symbols),
                limit=args.limit,
                dry_run=args.dry_run,
                sleep_seconds=args.sleep_seconds,
            ),
            sort_keys=True,
            default=str,
        )
    )


if __name__ == "__main__":
    main()
