from __future__ import annotations

import argparse
import json
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import func, select

from app.backfill_price_cache import backfill_price_cache
from app.db import SessionLocal
from app.models import InstitutionalPosition, PriceCache
from app.utils.symbols import normalize_symbol


class SymbolsPerMinuteThrottle:
    def __init__(self, symbols_per_minute: int):
        self.symbols_per_minute = max(1, int(symbols_per_minute))
        self.calls: deque[float] = deque()

    def wait(self) -> None:
        now = time.monotonic()
        while self.calls and now - self.calls[0] >= 60:
            self.calls.popleft()
        if len(self.calls) >= self.symbols_per_minute:
            sleep_for = max(0.0, 60 - (now - self.calls[0])) + 0.05
            time.sleep(sleep_for)
            self.wait()
            return
        self.calls.append(time.monotonic())


def _default_start_date() -> str:
    return (datetime.now(timezone.utc).date() - timedelta(days=365 * 3 + 10)).isoformat()


def _default_end_date() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _candidate_symbols(
    *,
    start_date: str,
    end_date: str,
    min_adjusted_rows: int,
    limit_symbols: int | None,
) -> list[str]:
    position_totals = (
        select(
            InstitutionalPosition.normalized_symbol.label("symbol"),
            func.coalesce(func.sum(InstitutionalPosition.value_usd), 0.0).label("reported_value"),
        )
        .where(InstitutionalPosition.normalized_symbol.is_not(None))
        .where(InstitutionalPosition.value_usd.is_not(None))
        .where(InstitutionalPosition.value_usd > 0)
        .group_by(InstitutionalPosition.normalized_symbol)
        .subquery()
    )
    adjusted_counts = (
        select(
            PriceCache.symbol.label("symbol"),
            func.count(PriceCache.date).label("adjusted_rows"),
        )
        .where(PriceCache.date >= start_date)
        .where(PriceCache.date <= end_date)
        .where(PriceCache.adjusted_close.is_not(None))
        .where(PriceCache.adjusted_close > 0)
        .group_by(PriceCache.symbol)
        .subquery()
    )
    query = (
        select(position_totals.c.symbol)
        .outerjoin(adjusted_counts, adjusted_counts.c.symbol == position_totals.c.symbol)
        .where(func.coalesce(adjusted_counts.c.adjusted_rows, 0) < int(min_adjusted_rows))
        .order_by(position_totals.c.reported_value.desc(), position_totals.c.symbol.asc())
    )
    if limit_symbols is not None:
        query = query.limit(max(0, int(limit_symbols)))

    with SessionLocal() as db:
        rows = db.execute(query).all()
    return [symbol for (symbol,) in rows if normalize_symbol(symbol)]


def run_backfill(
    *,
    start_date: str,
    end_date: str,
    min_adjusted_rows: int,
    limit_symbols: int | None,
    symbols_per_minute: int,
    dry_run: bool,
    progress_every: int,
) -> dict[str, Any]:
    symbols = _candidate_symbols(
        start_date=start_date,
        end_date=end_date,
        min_adjusted_rows=min_adjusted_rows,
        limit_symbols=limit_symbols,
    )
    stats: dict[str, Any] = {
        "status": "ok",
        "dry_run": dry_run,
        "start_date": start_date,
        "end_date": end_date,
        "min_adjusted_rows": min_adjusted_rows,
        "symbols_per_minute": symbols_per_minute,
        "symbols_total": len(symbols),
        "symbols_seen": 0,
        "symbols_updated": 0,
        "symbols_failed": 0,
        "rows_inserted_or_updated": 0,
    }
    throttle = SymbolsPerMinuteThrottle(symbols_per_minute)
    progress_interval = max(1, int(progress_every or 25))
    for index, symbol in enumerate(symbols, start=1):
        throttle.wait()
        stats["symbols_seen"] += 1
        try:
            report = backfill_price_cache(
                symbols=[symbol],
                start_date=start_date,
                end_date=end_date,
                dry_run=dry_run,
                reconstruct_adjusted=True,
            )
        except Exception as exc:
            stats["symbols_failed"] += 1
            print({"symbol": symbol, "status": "failed", "error": exc.__class__.__name__, "progress": f"{index}/{len(symbols)}", **stats})
            continue

        row = (report.get("rows") or [{}])[0]
        updated = int(row.get("rows_inserted_or_updated") or 0)
        stats["rows_inserted_or_updated"] += updated
        if updated:
            stats["symbols_updated"] += 1
        if index == 1 or index % progress_interval == 0 or index == len(symbols):
            print(
                {
                    "symbol": symbol,
                    "status": "updated" if updated else "skipped",
                    "progress": f"{index}/{len(symbols)}",
                    "last_rows_inserted_or_updated": updated,
                    "rows_provider_adjusted": int(row.get("rows_provider_adjusted") or 0),
                    "failure": row.get("failure"),
                    **stats,
                }
            )
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill adjusted price_cache rows for institutional holding symbols.")
    parser.add_argument("--start-date", default=_default_start_date())
    parser.add_argument("--end-date", default=_default_end_date())
    parser.add_argument("--min-adjusted-rows", type=int, default=500)
    parser.add_argument("--limit-symbols", type=int, default=None)
    parser.add_argument("--symbols-per-minute", type=int, default=100)
    parser.add_argument("--progress-every", type=int, default=25)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    if not args.dry_run and not args.apply:
        raise SystemExit("Pass --dry-run to preview or --apply to write adjusted price_cache rows.")
    if args.dry_run and args.apply:
        raise SystemExit("Choose only one of --dry-run or --apply.")

    print(
        json.dumps(
            run_backfill(
                start_date=args.start_date,
                end_date=args.end_date,
                min_adjusted_rows=args.min_adjusted_rows,
                limit_symbols=args.limit_symbols,
                symbols_per_minute=args.symbols_per_minute,
                dry_run=args.dry_run,
                progress_every=args.progress_every,
            ),
            sort_keys=True,
            default=str,
        )
    )


if __name__ == "__main__":
    main()
