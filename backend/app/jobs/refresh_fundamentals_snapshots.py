from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone

from app.db import SessionLocal, engine, ensure_fundamentals_snapshot_schema
from app.services.fundamentals_snapshots import snapshot_current_fundamentals


def _parse_symbols(value: str | None) -> list[str] | None:
    if value is None:
        return None
    return [part.strip() for part in value.split(",") if part.strip()]


def main() -> None:
    parser = argparse.ArgumentParser(description="Persist point-in-time snapshots from the current fundamentals cache.")
    parser.add_argument("--symbols", help="Optional comma-separated symbols. Defaults to all ok fundamentals cache rows.")
    parser.add_argument("--provider", default="fmp")
    parser.add_argument("--observed-at", help="Optional ISO timestamp for reproducible backfill/testing.")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verbose-symbols", action="store_true")
    args = parser.parse_args()

    observed_at = datetime.fromisoformat(args.observed_at) if args.observed_at else datetime.now(timezone.utc)
    if observed_at.tzinfo is None:
        observed_at = observed_at.replace(tzinfo=timezone.utc)

    ensure_fundamentals_snapshot_schema(engine)
    with SessionLocal() as db:
        result = snapshot_current_fundamentals(
            db,
            symbols=_parse_symbols(args.symbols),
            provider=args.provider,
            observed_at=observed_at,
        )
        if args.dry_run:
            db.rollback()
            result = {**result, "dry_run": True, "committed": False}
        else:
            db.commit()
            result = {**result, "dry_run": False, "committed": True}
    if not args.verbose_symbols:
        result = {
            **result,
            "symbols_sample": result.get("symbols", [])[:25],
            "symbols": f"{len(result.get('symbols', []))} symbols omitted; pass --verbose-symbols to print all",
        }
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
