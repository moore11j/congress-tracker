from __future__ import annotations

import argparse
import logging
import sys

from app.db import Base, SessionLocal, engine, ensure_market_pressure_snapshot_schema, ensure_price_cache_volume_columns, ensure_quote_cache_market_cap_schema
from app.models import FundamentalsCache, IndexMembership, MarketPressureSnapshot, PriceCache, QuoteCache, Security, TickerMeta
from app.services.market_pressure_ingest import refresh_market_pressure_snapshots

logger = logging.getLogger(__name__)


def run(
    *,
    universes: list[str] | None = None,
    all_universes: bool = False,
    force: bool = False,
    market_hours_only: bool = True,
    calls_per_minute: int | None = None,
    fresh_minutes: int | None = None,
    max_symbols: int | None = None,
) -> list[dict]:
    Base.metadata.create_all(
        bind=engine,
        tables=[
            Security.__table__,
            IndexMembership.__table__,
            MarketPressureSnapshot.__table__,
            PriceCache.__table__,
            QuoteCache.__table__,
            TickerMeta.__table__,
            FundamentalsCache.__table__,
        ],
    )
    ensure_market_pressure_snapshot_schema(engine)
    ensure_quote_cache_market_cap_schema(engine)
    ensure_price_cache_volume_columns(engine)
    selected = None if all_universes else universes
    db = SessionLocal()
    try:
        results = refresh_market_pressure_snapshots(
            db,
            universes=selected,
            force=force,
            market_hours_only=market_hours_only,
            calls_per_minute=calls_per_minute,
            fresh_minutes=fresh_minutes,
            max_symbols=max_symbols,
        )
        payload = [result.__dict__ for result in results]
        logger.info("market_pressure_snapshot_refresh_complete results=%s", payload)
        return payload
    finally:
        db.close()


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refresh Market Pressure price/score snapshots.")
    parser.add_argument("--universe", action="append", choices=["sp500", "nasdaq100", "etf"], help="Universe to refresh. Repeat for multiple.")
    parser.add_argument("--all", action="store_true", help="Refresh S&P 500, Nasdaq 100, and ETF universes.")
    parser.add_argument("--force", action="store_true", help="Run even outside market hours.")
    parser.add_argument("--no-market-hours-only", action="store_true", help="Disable the market-hours guard.")
    parser.add_argument("--calls-per-minute", type=int, default=None, help="FMP call budget. Default comes from MARKET_PRESSURE_INGEST_CALLS_PER_MINUTE.")
    parser.add_argument("--fresh-minutes", type=int, default=None, help="Reuse quote cache rows newer than this many minutes.")
    parser.add_argument("--max-symbols", type=int, default=None, help="Safety cap per universe. 0 means no cap.")
    args = parser.parse_args(argv)
    if args.all and args.universe:
        parser.error("Pass --all or --universe, not both.")
    if not args.all and not args.universe:
        args.all = True
    return args


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    results = run(
        universes=args.universe,
        all_universes=args.all,
        force=args.force,
        market_hours_only=not args.no_market_hours_only,
        calls_per_minute=args.calls_per_minute,
        fresh_minutes=args.fresh_minutes,
        max_symbols=args.max_symbols,
    )
    for result in results:
        print(result)
    return 1 if any(result.get("status") not in {"ok", "skipped_market_closed"} for result in results) else 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    raise SystemExit(main())
