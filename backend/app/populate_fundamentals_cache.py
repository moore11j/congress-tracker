from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select

from app.db import Base, SessionLocal, engine, ensure_fundamentals_cache_schema
from app.models import FundamentalsCache
from app.services.fundamentals_cache import (
    FUNDAMENTAL_FIELD_NAMES,
    PROVIDER,
    fetch_fundamentals_for_symbol,
    fetch_screener_universe_fundamentals,
    sleep_between_provider_calls,
    stale_or_missing_symbols,
    upsert_fundamentals_cache,
)
from app.utils.symbols import normalize_symbol

logger = logging.getLogger(__name__)


def _parse_symbols(value: str | None) -> list[str]:
    symbols: list[str] = []
    for raw_symbol in (value or "").split(","):
        symbol = normalize_symbol(raw_symbol)
        if symbol:
            symbols.append(symbol)
    return list(dict.fromkeys(symbols))


def _existing_cache_symbols(db, *, provider: str, limit: int) -> list[str]:
    return [
        symbol
        for symbol in db.execute(
            select(FundamentalsCache.symbol)
            .where(FundamentalsCache.provider == provider)
            .order_by(FundamentalsCache.fetched_at.asc())
            .limit(max(1, int(limit)))
        ).scalars().all()
        if symbol
    ]


def _coverage(success_rows: list[dict[str, Any]]) -> dict[str, float]:
    denominator = max(1, len(success_rows))
    return {
        field: round(
            100.0 * sum(1 for row in success_rows if row.get(field) is not None) / denominator,
            1,
        )
        for field in FUNDAMENTAL_FIELD_NAMES
    }


def populate_fundamentals_cache(
    *,
    symbols: list[str] | None = None,
    screener_universe: bool = False,
    stale_days: int | None = None,
    limit: int = 500,
    dry_run: bool = True,
    sleep_s: float = 0.0,
    provider: str = PROVIDER,
) -> dict[str, Any]:
    Base.metadata.create_all(bind=engine)
    ensure_fundamentals_cache_schema(engine)
    bounded_limit = max(1, int(limit))
    explicit_symbols = list(dict.fromkeys(symbol for symbol in (symbols or []) if symbol))

    with SessionLocal() as db:
        already_fresh = 0
        if screener_universe:
            provider_results = fetch_screener_universe_fundamentals(limit=bounded_limit)
            considered_symbols = [result.symbol for result in provider_results]
            if stale_days is not None:
                stale_symbols, already_fresh = stale_or_missing_symbols(
                    db,
                    considered_symbols,
                    stale_days=stale_days,
                    provider=provider,
                )
                stale_set = set(stale_symbols)
                provider_results = [result for result in provider_results if result.symbol in stale_set]
        else:
            considered_symbols = explicit_symbols or _existing_cache_symbols(db, provider=provider, limit=bounded_limit)
            if stale_days is not None:
                considered_symbols, already_fresh = stale_or_missing_symbols(
                    db,
                    considered_symbols,
                    stale_days=stale_days,
                    provider=provider,
                )
            considered_symbols = considered_symbols[:bounded_limit]
            provider_results = []
            for index, symbol in enumerate(considered_symbols):
                provider_results.append(fetch_fundamentals_for_symbol(symbol))
                if index < len(considered_symbols) - 1:
                    sleep_between_provider_calls(sleep_s)

        fetched = 0
        updated = 0
        failed = 0
        success_rows: list[dict[str, Any]] = []
        rows: list[dict[str, Any]] = []
        for result in provider_results[:bounded_limit]:
            fetched += 1
            values = dict(result.values)
            if result.status != "ok":
                failed += 1
                values = {
                    "symbol": result.symbol,
                    "provider": provider,
                    "fetched_at": datetime.now(timezone.utc),
                    "status": "failed",
                    "error": result.error,
                }
            else:
                values["provider"] = provider
                values["status"] = "ok"
                values["error"] = None
                success_rows.append(values)
            if not dry_run:
                if upsert_fundamentals_cache(db, values) and result.status == "ok":
                    updated += 1
            rows.append(
                {
                    "symbol": result.symbol,
                    "status": result.status,
                    "updated": False if dry_run or result.status != "ok" else True,
                    "error": result.error,
                    "non_null_fields": sum(1 for field in FUNDAMENTAL_FIELD_NAMES if values.get(field) is not None),
                }
            )
        if not dry_run:
            db.commit()

    report = {
        "dry_run": dry_run,
        "provider": provider,
        "symbols_considered": len(considered_symbols),
        "already_fresh": already_fresh,
        "fetched": fetched,
        "updated": updated,
        "failed": failed,
        "field_coverage_pct": _coverage(success_rows) if success_rows else {},
        "rows": rows,
    }
    logger.info("fundamentals cache populate summary=%s", report)
    return report


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Populate cached screener fundamentals.")
    parser.add_argument("--symbols", help="Comma-separated symbols, for example AAPL,NVDA,MSFT")
    parser.add_argument("--screener-universe", action="store_true", help="Fetch a provider screener universe and cache its normalized rows.")
    parser.add_argument("--stale-days", type=int, help="Only refresh rows missing or older than N days.")
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--sleep-s", type=float, default=0.0)
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing. This is the default unless --apply is passed.")
    parser.add_argument("--apply", action="store_true", help="Write cache rows.")
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    if args.apply and args.dry_run:
        raise SystemExit("Choose only one of --dry-run or --apply.")
    dry_run = not args.apply
    symbols = _parse_symbols(args.symbols)
    if not symbols and not args.screener_universe and args.stale_days is None:
        raise SystemExit("Pass --symbols, --screener-universe, or --stale-days.")

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    report = populate_fundamentals_cache(
        symbols=symbols,
        screener_universe=args.screener_universe,
        stale_days=args.stale_days,
        limit=args.limit,
        dry_run=dry_run,
        sleep_s=args.sleep_s,
    )
    print(json.dumps(report, indent=2, sort_keys=True, default=str))


if __name__ == "__main__":
    main()
