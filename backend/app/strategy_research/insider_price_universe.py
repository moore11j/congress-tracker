from __future__ import annotations

import argparse
import json
import logging
import time
from dataclasses import asdict, dataclass
from datetime import date
from typing import Iterable

from sqlalchemy import bindparam, text
from sqlalchemy.orm import Session

from app.backfill_price_cache import backfill_price_cache
from app.db import SessionLocal
from app.services.backtesting.queries import parse_iso_date
from app.utils.symbols import normalize_symbol

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class InsiderPriceCoverageRow:
    symbol: str
    purchase_count: int
    first_filing_date: str | None
    last_filing_date: str | None
    price_rows: int
    adjusted_rows: int
    first_price_date: str | None
    last_price_date: str | None


def _normalize_symbols(symbols: Iterable[str]) -> tuple[str, ...]:
    normalized: list[str] = []
    for symbol in symbols:
        cleaned = normalize_symbol(symbol)
        if cleaned:
            normalized.append(cleaned)
    return tuple(dict.fromkeys(normalized))


def load_insider_purchase_price_coverage(
    db: Session,
    *,
    start_date: date,
    end_date: date,
    min_purchase_count: int = 1,
    symbols: Iterable[str] | None = None,
) -> list[InsiderPriceCoverageRow]:
    symbol_filter = _normalize_symbols(symbols or ())
    params: dict[str, object] = {
        "price_start_date": start_date.isoformat(),
        "price_end_date": end_date.isoformat(),
        "filing_end_date": end_date,
        "min_purchase_count": int(min_purchase_count),
    }
    symbol_clause = ""
    if symbol_filter:
        symbol_clause = "and upper(ticker_normalized) in :symbols"
        params["symbols"] = symbol_filter

    statement = text(
        f"""
            with purchase_symbols as (
                select
                    upper(ticker_normalized) as symbol,
                    count(*) as purchase_count,
                    min(filing_date) as first_filing_date,
                    max(filing_date) as last_filing_date
                from insider_transactions_normalized
                where is_duplicate = false
                  and transaction_type_normalized = 'open_market_purchase'
                  and ticker_normalized is not null
                  and filing_date is not null
                  and filing_date <= :filing_end_date
                  and (transaction_date is null or transaction_date <= :filing_end_date)
                  {symbol_clause}
                group by upper(ticker_normalized)
                having count(*) >= :min_purchase_count
            ),
            coverage as (
                select
                    upper(symbol) as symbol,
                    count(*) as price_rows,
                    count(adjusted_close) as adjusted_rows,
                    min(date) as first_price_date,
                    max(date) as last_price_date
                from price_cache
                where date >= :price_start_date
                  and date <= :price_end_date
                group by upper(symbol)
            )
            select
                p.symbol,
                p.purchase_count,
                p.first_filing_date,
                p.last_filing_date,
                coalesce(c.price_rows, 0) as price_rows,
                coalesce(c.adjusted_rows, 0) as adjusted_rows,
                c.first_price_date,
                c.last_price_date
            from purchase_symbols p
            left join coverage c on c.symbol = p.symbol
            order by p.purchase_count desc, p.symbol asc
            """
    )
    if symbol_filter:
        statement = statement.bindparams(bindparam("symbols", expanding=True))
    rows = db.execute(statement, params).all()
    return [
        InsiderPriceCoverageRow(
            symbol=str(row.symbol),
            purchase_count=int(row.purchase_count or 0),
            first_filing_date=str(row.first_filing_date) if row.first_filing_date is not None else None,
            last_filing_date=str(row.last_filing_date) if row.last_filing_date is not None else None,
            price_rows=int(row.price_rows or 0),
            adjusted_rows=int(row.adjusted_rows or 0),
            first_price_date=str(row.first_price_date) if row.first_price_date is not None else None,
            last_price_date=str(row.last_price_date) if row.last_price_date is not None else None,
        )
        for row in rows
    ]


def rows_needing_adjusted_backfill(
    rows: Iterable[InsiderPriceCoverageRow],
    *,
    min_adjusted_rows: int,
) -> list[InsiderPriceCoverageRow]:
    return [row for row in rows if row.adjusted_rows < min_adjusted_rows]


def _chunks(values: list[str], size: int) -> Iterable[list[str]]:
    size = max(int(size), 1)
    for index in range(0, len(values), size):
        yield values[index : index + size]


def run(
    *,
    start_date: date,
    end_date: date,
    apply: bool,
    min_purchase_count: int,
    min_adjusted_rows: int,
    max_symbols: int | None,
    symbols_per_batch: int,
    sleep_seconds: float,
    symbols: Iterable[str] | None = None,
) -> dict[str, object]:
    with SessionLocal() as db:
        coverage = load_insider_purchase_price_coverage(
            db,
            start_date=start_date,
            end_date=end_date,
            min_purchase_count=min_purchase_count,
            symbols=symbols,
        )
    all_needing_backfill = rows_needing_adjusted_backfill(coverage, min_adjusted_rows=min_adjusted_rows)
    needs_backfill = all_needing_backfill
    if max_symbols is not None:
        needs_backfill = needs_backfill[: max(int(max_symbols), 0)]

    symbols_to_backfill = [row.symbol for row in needs_backfill]
    report: dict[str, object] = {
        "apply": apply,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "min_purchase_count": min_purchase_count,
        "min_adjusted_rows": min_adjusted_rows,
        "total_purchase_symbols": len(coverage),
        "symbols_needing_backfill": len(all_needing_backfill),
        "symbols_selected": len(symbols_to_backfill),
        "selected_preview": [asdict(row) for row in needs_backfill[:50]],
        "batches": [],
    }
    if not apply:
        return report

    batches: list[dict[str, object]] = []
    for batch_index, batch_symbols in enumerate(_chunks(symbols_to_backfill, symbols_per_batch), start=1):
        logger.info("backfilling insider price universe batch=%s symbols=%s", batch_index, ",".join(batch_symbols))
        batch_report = backfill_price_cache(
            symbols=batch_symbols,
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            dry_run=False,
            reconstruct_adjusted=True,
            apply_split_factors=False,
        )
        rows = batch_report.get("rows") if isinstance(batch_report, dict) else None
        batches.append(
            {
                "batch": batch_index,
                "symbols": batch_symbols,
                "rows": rows,
            }
        )
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)
    report["batches"] = batches
    return report


def _parse_symbols(value: str | None) -> tuple[str, ...] | None:
    if not value:
        return None
    return _normalize_symbols(part.strip() for part in value.split(","))


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit/backfill adjusted prices for normalized insider purchase tickers.")
    parser.add_argument("--start-date", default="2023-04-01")
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--min-purchase-count", type=int, default=1)
    parser.add_argument("--min-adjusted-rows", type=int, default=250)
    parser.add_argument("--max-symbols", type=int)
    parser.add_argument("--symbols-per-batch", type=int, default=25)
    parser.add_argument("--sleep-seconds", type=float, default=0.0)
    parser.add_argument("--symbols")
    args = parser.parse_args()

    if args.apply == args.dry_run:
        raise SystemExit("Pass exactly one of --dry-run or --apply.")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    result = run(
        start_date=parse_iso_date(args.start_date) or date.fromisoformat(args.start_date),
        end_date=parse_iso_date(args.end_date) or date.fromisoformat(args.end_date),
        apply=args.apply,
        min_purchase_count=int(args.min_purchase_count),
        min_adjusted_rows=int(args.min_adjusted_rows),
        max_symbols=args.max_symbols,
        symbols_per_batch=int(args.symbols_per_batch),
        sleep_seconds=float(args.sleep_seconds),
        symbols=_parse_symbols(args.symbols),
    )
    print(json.dumps(result, indent=2, sort_keys=True, default=str))


if __name__ == "__main__":
    main()
