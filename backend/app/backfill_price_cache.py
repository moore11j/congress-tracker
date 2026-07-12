from __future__ import annotations

import argparse
import json
import logging
from datetime import date

from sqlalchemy import select

from app.db import Base, SessionLocal, engine, ensure_price_cache_volume_columns
from app.models import PriceCache
from app.services.price_lookup import _fetch_provider_eod_price_volume_series, _safe_cache_upsert
from app.utils.symbols import normalize_symbol

logger = logging.getLogger(__name__)


def _parse_date(value: str) -> date:
    return date.fromisoformat((value or "").strip()[:10])


def _parse_symbols(value: str) -> list[str]:
    symbols = []
    for raw_symbol in (value or "").split(","):
        symbol = normalize_symbol(raw_symbol)
        if symbol:
            symbols.append(symbol)
    return list(dict.fromkeys(symbols))


def _existing_dates(db, *, symbol: str, start_date: str, end_date: str) -> set[str]:
    rows = db.execute(
        select(PriceCache.date)
        .where(PriceCache.symbol == symbol)
        .where(PriceCache.date >= start_date)
        .where(PriceCache.date <= end_date)
    ).all()
    return {str(row[0]) for row in rows}


def backfill_price_cache(
    *,
    symbols: list[str],
    start_date: str,
    end_date: str,
    dry_run: bool,
) -> dict:
    Base.metadata.create_all(bind=engine)
    ensure_price_cache_volume_columns(engine)
    start = _parse_date(start_date).isoformat()
    end = _parse_date(end_date).isoformat()
    if start > end:
        start, end = end, start

    report_rows: list[dict] = []
    with SessionLocal() as db:
        for symbol in symbols:
            existing = _existing_dates(db, symbol=symbol, start_date=start, end_date=end)
            provider_map: dict[str, float] = {}
            provider_symbol = None
            failure = None
            try:
                provider_map, volume_map, provider_symbol = _fetch_provider_eod_price_volume_series(symbol, start, end)
            except Exception as exc:
                volume_map = {}
                failure = exc.__class__.__name__
                logger.warning("price cache backfill provider failure symbol=%s error=%s", symbol, failure)

            provider_dates = set(provider_map.keys())
            missing_provider_dates = sorted(provider_dates - existing)
            inserted_or_updated = 0
            if not dry_run and provider_map:
                for day, close in sorted(provider_map.items()):
                    if _safe_cache_upsert(db, provider_symbol or symbol, day, close, volume_map.get(day)):
                        inserted_or_updated += 1
                db.commit()

            report_rows.append(
                {
                    "symbol": symbol,
                    "provider_symbol": provider_symbol,
                    "start_date": start,
                    "end_date": end,
                    "dry_run": dry_run,
                    "rows_existing": len(existing),
                    "rows_provider": len(provider_map),
                    "rows_provider_volume": len(volume_map),
                    "rows_missing": len(missing_provider_dates),
                    "rows_inserted_or_updated": inserted_or_updated,
                    "first_provider_date": min(provider_dates) if provider_dates else None,
                    "last_provider_date": max(provider_dates) if provider_dates else None,
                    "failure": failure,
                }
            )

    return {
        "dry_run": dry_run,
        "symbols": symbols,
        "start_date": start,
        "end_date": end,
        "rows": report_rows,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill cached EOD prices for targeted symbols.")
    parser.add_argument("--symbols", required=True, help="Comma-separated symbols, for example SPY or AAPL,MSFT")
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    if not args.dry_run and not args.apply:
        raise SystemExit("Pass --dry-run to preview or --apply to write price_cache rows.")
    if args.dry_run and args.apply:
        raise SystemExit("Choose only one of --dry-run or --apply.")

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    report = backfill_price_cache(
        symbols=_parse_symbols(args.symbols),
        start_date=args.start_date,
        end_date=args.end_date,
        dry_run=args.dry_run,
    )
    print(json.dumps(report, indent=2, sort_keys=True, default=str))


if __name__ == "__main__":
    main()
