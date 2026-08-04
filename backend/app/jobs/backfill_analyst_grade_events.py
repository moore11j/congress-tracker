from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone

from app.db import SessionLocal, engine, ensure_analyst_consensus_schema
from app.services.analyst_consensus import (
    eligible_equity_symbols,
    finish_ingestion_run,
    ingest_symbol_grade_events,
    start_ingestion_run,
)


def _parse_symbols(value: str | None) -> list[str] | None:
    if value is None:
        return None
    return [part.strip() for part in value.split(",") if part.strip()]


def backfill_analyst_grade_events(
    *,
    symbols: list[str] | None = None,
    limit: int | None = None,
    dry_run: bool = False,
    sleep_seconds: float = 0.0,
) -> dict[str, object]:
    ensure_analyst_consensus_schema(engine)
    observed_at = datetime.now(timezone.utc)
    with SessionLocal() as db:
        planned = eligible_equity_symbols(db, symbols, limit=limit)
        run = start_ingestion_run(
            db,
            "analyst_grade_events_backfill",
            metadata={"dry_run": dry_run, "limit": limit, "requested_symbols": symbols or []},
        )
        attempted = succeeded = failed = inserted = updated = 0
        provider_errors: list[dict[str, object]] = []
        results: list[dict[str, object]] = []
        for symbol in planned:
            attempted += 1
            result = ingest_symbol_grade_events(db, symbol, observed_at=observed_at)
            results.append(result)
            if result.get("status") in {"unsupported", "provider_error"}:
                failed += 1
                provider_errors.append({"symbol": symbol, "error": result.get("error")})
            else:
                succeeded += 1
                inserted += int(result.get("inserted") or 0)
                updated += int(result.get("updated") or 0)
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)
        status = "success" if failed == 0 else "partial"
        finish_ingestion_run(
            run,
            status=status,
            attempted=attempted,
            succeeded=succeeded,
            failed=failed,
            inserted=inserted,
            updated=updated,
            provider_errors=provider_errors,
        )
        payload = {
            "status": status,
            "dry_run": dry_run,
            "committed": not dry_run,
            "symbols_attempted": attempted,
            "symbols_succeeded": succeeded,
            "symbols_failed": failed,
            "records_inserted": inserted,
            "records_updated": updated,
            "provider_errors": provider_errors[:25],
            "results_sample": results[:25],
            "observed_at": observed_at.isoformat(),
        }
        if dry_run:
            db.rollback()
        else:
            db.commit()
        return payload


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill Analyst Consensus grade-event history.")
    parser.add_argument("--symbols", help="Optional comma-separated symbols. Defaults to the eligible Walnut equity universe.")
    parser.add_argument("--limit", type=int, default=25, help="Maximum symbols to process. Defaults to 25 for safety.")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--sleep-seconds", type=float, default=0.0, help="Optional delay between symbols for rate-limit control.")
    args = parser.parse_args()
    print(
        json.dumps(
            backfill_analyst_grade_events(
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
