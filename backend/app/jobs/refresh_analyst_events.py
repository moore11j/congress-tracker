from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from typing import Any

from app.db import SessionLocal, engine, ensure_analyst_consensus_schema
from app.services.analyst_consensus import (
    GRADE_DAILY_REFRESH_JOB,
    PRICE_TARGET_DAILY_REFRESH_JOB,
    eligible_historical_grade_symbols,
    eligible_price_target_event_symbols,
    finish_ingestion_run,
    ingest_symbol_grade_events,
    ingest_symbol_price_target_events,
    record_symbol_backfill_attempt,
    start_ingestion_run,
)

DAILY_EVENTS_JOB = "analyst_events_daily_refresh"


def _parse_symbols(value: str | None) -> list[str] | None:
    if value is None:
        return None
    return [part.strip() for part in value.split(",") if part.strip()]


def _is_provider_failure(result: dict[str, Any]) -> bool:
    return result.get("status") in {"unsupported", "provider_error"}


def refresh_analyst_events(
    *,
    symbols: list[str] | None = None,
    limit: int | None = 250,
    pages: int = 1,
    page_size: int = 100,
    dry_run: bool = False,
    sleep_seconds: float = 0.3,
    timeout_seconds: int = 30,
    include_grades: bool = True,
    include_price_targets: bool = True,
) -> dict[str, object]:
    ensure_analyst_consensus_schema(engine)
    observed_at = datetime.now(timezone.utc)
    with SessionLocal() as db:
        grade_symbols = (
            eligible_historical_grade_symbols(
                db,
                symbols,
                limit=limit,
                job_name=GRADE_DAILY_REFRESH_JOB,
            )
            if include_grades
            else []
        )
        target_symbols = (
            eligible_price_target_event_symbols(
                db,
                symbols,
                limit=limit,
                job_name=PRICE_TARGET_DAILY_REFRESH_JOB,
            )
            if include_price_targets
            else []
        )
        run = start_ingestion_run(
            db,
            DAILY_EVENTS_JOB,
            metadata={
                "dry_run": dry_run,
                "limit": limit,
                "pages": pages,
                "page_size": page_size,
                "requested_symbols": symbols or [],
                "include_grades": include_grades,
                "include_price_targets": include_price_targets,
            },
        )
        attempted = succeeded = failed = inserted = updated = 0
        provider_errors: list[dict[str, object]] = []
        samples: dict[str, list[dict[str, Any]]] = {"grades": [], "price_targets": []}

        def record_result(kind: str, symbol: str, result: dict[str, Any], job_name: str) -> None:
            nonlocal attempted, succeeded, failed, inserted, updated
            attempted += 1
            record_symbol_backfill_attempt(db, job_name=job_name, symbol=symbol, result=result, attempted_at=observed_at)
            samples[kind].append(result)
            if _is_provider_failure(result):
                failed += 1
                provider_errors.append({"kind": kind, "symbol": symbol, "error": result.get("error")})
            else:
                succeeded += 1
                inserted += int(result.get("inserted") or 0)
                updated += int(result.get("updated") or 0)
            if not dry_run:
                run.symbols_attempted = attempted
                run.symbols_succeeded = succeeded
                run.symbols_failed = failed
                run.records_inserted = inserted
                run.records_updated = updated
                db.commit()
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)

        for symbol in grade_symbols:
            result = ingest_symbol_grade_events(db, symbol, observed_at=observed_at)
            record_result("grades", symbol, result, GRADE_DAILY_REFRESH_JOB)

        for symbol in target_symbols:
            result = ingest_symbol_price_target_events(
                db,
                symbol,
                observed_at=observed_at,
                pages=pages,
                page_size=page_size,
                timeout_s=timeout_seconds,
            )
            record_result("price_targets", symbol, result, PRICE_TARGET_DAILY_REFRESH_JOB)

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
            "grades": {
                "symbols_attempted": len(grade_symbols),
                "results_sample": samples["grades"][:25],
            },
            "price_targets": {
                "symbols_attempted": len(target_symbols),
                "results_sample": samples["price_targets"][:25],
            },
            "observed_at": observed_at.isoformat(),
        }
        if dry_run:
            db.rollback()
        else:
            db.commit()
        return payload


def main() -> None:
    parser = argparse.ArgumentParser(description="Refresh recent analyst grade and price-target event history.")
    parser.add_argument("--symbols", help="Optional comma-separated symbols. Defaults to rotating covered equity symbols.")
    parser.add_argument("--limit", type=int, default=250, help="Maximum symbols per event type. Defaults to 250.")
    parser.add_argument("--pages", type=int, default=1, help="Price-target-news pages per symbol. Defaults to 1.")
    parser.add_argument("--page-size", type=int, default=100, help="Rows per price-target page, capped by provider client.")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--sleep-seconds", type=float, default=0.3, help="Delay between symbol/event calls for rate-limit control.")
    parser.add_argument("--timeout-seconds", type=int, default=30, help="Provider timeout per symbol. Defaults to 30.")
    parser.add_argument("--skip-grades", action="store_true")
    parser.add_argument("--skip-price-targets", action="store_true")
    args = parser.parse_args()
    print(
        json.dumps(
            refresh_analyst_events(
                symbols=_parse_symbols(args.symbols),
                limit=args.limit,
                pages=args.pages,
                page_size=args.page_size,
                dry_run=args.dry_run,
                sleep_seconds=args.sleep_seconds,
                timeout_seconds=args.timeout_seconds,
                include_grades=not args.skip_grades,
                include_price_targets=not args.skip_price_targets,
            ),
            sort_keys=True,
            default=str,
        )
    )


if __name__ == "__main__":
    main()
