from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Iterable

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.db import SessionLocal
from app.models import Event, InsiderTransaction, InsiderTransactionNormalized, PriceCache
from app.services.backtesting.queries import first_text, parse_iso_date, parse_payload
from app.services.ticker_meta import normalize_cik
from app.services.trade_outcome_display import normalize_trade_side
from app.strategy_research.congress_buys import DEFAULT_UNIVERSE, _normalize_universe
from app.utils.symbols import normalize_symbol

METHODOLOGY_VERSION = "insider_data_audit_v1"


@dataclass(frozen=True)
class LegacyInsiderAuditRow:
    source_table: str
    row_id: int
    symbol: str | None
    side: str | None
    filing_date: date | None
    transaction_date: date | None
    reporting_cik: str | None
    insider_name: str | None
    role: str | None
    shares: float | None
    price: float | None
    value: float | None
    raw_type: str | None
    is_market_purchase: bool
    future_dated: bool


def _amount_value(shares: float | None, price: float | None, payload: dict[str, Any]) -> float | None:
    for key in ("value", "transaction_value", "transactionValue", "amount", "estimated_value"):
        value = payload.get(key)
        if isinstance(value, (int, float)) and value > 0:
            return float(value)
        if isinstance(value, str):
            try:
                parsed = float(value.replace(",", "").replace("$", ""))
            except ValueError:
                continue
            if parsed > 0:
                return parsed
    if shares is not None and price is not None and shares > 0 and price > 0:
        return float(shares) * float(price)
    return None


def _event_date(value: datetime | None) -> date | None:
    return value.date() if value is not None else None


def _payload_date(payload: dict[str, Any], *keys: str) -> date | None:
    return parse_iso_date(first_text(payload, *keys))


def _legacy_row_from_insider(row: InsiderTransaction, *, as_of: date) -> LegacyInsiderAuditRow:
    payload = parse_payload(row.payload_json)
    raw_type = row.transaction_type or first_text(
        payload,
        "trade_type",
        "tradeType",
        "transaction_type",
        "transactionType",
        "transaction_code",
        "transactionCode",
    )
    side = normalize_trade_side(raw_type)
    symbol = normalize_symbol(row.symbol or first_text(payload, "symbol", "ticker", "ticker_normalized", "tickerNormalized"))
    filing_date = row.filing_date or _payload_date(payload, "filing_date", "filingDate", "report_date", "reportDate")
    transaction_date = row.transaction_date or _payload_date(payload, "transaction_date", "transactionDate", "trade_date", "tradeDate")
    reporting_cik = normalize_cik(row.reporting_cik) or normalize_cik(
        first_text(payload, "reporting_cik", "reportingCik", "reportingCIK", "rptOwnerCik")
    )
    value = _amount_value(row.shares, row.price, payload)
    future_dated = any(day is not None and day > as_of for day in (filing_date, transaction_date))
    return LegacyInsiderAuditRow(
        source_table="insider_transactions",
        row_id=int(row.id),
        symbol=symbol,
        side=side,
        filing_date=filing_date,
        transaction_date=transaction_date,
        reporting_cik=reporting_cik,
        insider_name=row.insider_name or first_text(payload, "insider_name", "insiderName", "reporting_owner_name", "reportingOwnerName"),
        role=row.role or first_text(payload, "role", "officer_title", "officerTitle"),
        shares=float(row.shares) if row.shares is not None else None,
        price=float(row.price) if row.price is not None else None,
        value=value,
        raw_type=raw_type,
        is_market_purchase=side == "purchase",
        future_dated=future_dated,
    )


def _legacy_row_from_event(row: Event, *, as_of: date) -> LegacyInsiderAuditRow:
    payload = parse_payload(row.payload_json)
    raw_type = row.trade_type or row.transaction_type or first_text(
        payload,
        "trade_type",
        "tradeType",
        "transaction_type",
        "transactionType",
        "transaction_code",
        "transactionCode",
    )
    side = normalize_trade_side(raw_type)
    symbol = normalize_symbol(row.symbol or first_text(payload, "symbol", "ticker", "ticker_normalized", "tickerNormalized"))
    filing_date = _payload_date(payload, "filing_date", "filingDate", "report_date", "reportDate") or _event_date(row.event_date)
    transaction_date = _payload_date(payload, "transaction_date", "transactionDate", "trade_date", "tradeDate")
    reporting_cik = normalize_cik(
        first_text(payload, "reporting_cik", "reportingCik", "reportingCIK", "rptOwnerCik", "reporting_owner_cik")
    )
    shares = payload.get("shares")
    price = payload.get("price")
    shares_value = float(shares) if isinstance(shares, (int, float)) else None
    price_value = float(price) if isinstance(price, (int, float)) else None
    value = _amount_value(shares_value, price_value, payload)
    future_dated = any(day is not None and day > as_of for day in (filing_date, transaction_date, _event_date(row.ts)))
    return LegacyInsiderAuditRow(
        source_table="events",
        row_id=int(row.id),
        symbol=symbol,
        side=side,
        filing_date=filing_date,
        transaction_date=transaction_date,
        reporting_cik=reporting_cik,
        insider_name=row.member_name or first_text(payload, "insider_name", "insiderName", "reporting_owner_name", "reportingOwnerName"),
        role=first_text(payload, "role", "officer_title", "officerTitle"),
        shares=shares_value,
        price=price_value,
        value=value,
        raw_type=raw_type,
        is_market_purchase=side == "purchase",
        future_dated=future_dated,
    )


def _date_range(days: Iterable[date | None]) -> dict[str, str | None]:
    values = sorted(day for day in days if day is not None)
    return {
        "earliest": values[0].isoformat() if values else None,
        "latest": values[-1].isoformat() if values else None,
    }


def _year_key(day: date | None) -> str:
    return str(day.year) if day is not None else "missing"


def _row_is_usable(row: LegacyInsiderAuditRow, *, as_of: date) -> bool:
    return (
        row.is_market_purchase
        and row.symbol is not None
        and row.filing_date is not None
        and row.filing_date <= as_of
        and not row.future_dated
    )


def _price_coverage(db: Session, symbols: Iterable[str], *, start_date: date, end_date: date) -> dict[str, dict[str, object]]:
    normalized = _normalize_universe(symbols)
    if not normalized:
        return {}
    rows = (
        db.execute(
            select(
                PriceCache.symbol,
                PriceCache.date,
                PriceCache.adjusted_close,
            )
            .where(PriceCache.symbol.in_(normalized))
            .where(PriceCache.date >= start_date.isoformat())
            .where(PriceCache.date <= end_date.isoformat())
            .order_by(PriceCache.symbol.asc(), PriceCache.date.asc())
        )
        .all()
    )
    grouped: dict[str, list[tuple[str, object]]] = defaultdict(list)
    for symbol, day, adjusted_close in rows:
        grouped[str(symbol)].append((str(day), adjusted_close))
    coverage = {
        symbol: {
            "rows": len(items),
            "adjusted_rows": sum(1 for _, adjusted_close in items if adjusted_close is not None),
            "earliest": items[0][0] if items else None,
            "latest": items[-1][0] if items else None,
        }
        for symbol, items in sorted(grouped.items())
    }
    return coverage


def _load_legacy_rows(db: Session, *, as_of: date, max_rows: int | None) -> list[LegacyInsiderAuditRow]:
    query = select(InsiderTransaction).order_by(InsiderTransaction.id.asc())
    if max_rows is not None:
        query = query.limit(max_rows)
    rows = db.execute(query).scalars().all()
    return [_legacy_row_from_insider(row, as_of=as_of) for row in rows]


def _load_event_rows(db: Session, *, as_of: date, max_rows: int | None) -> list[LegacyInsiderAuditRow]:
    query = select(Event).where(Event.event_type == "insider_trade").order_by(Event.id.asc())
    if max_rows is not None:
        query = query.limit(max_rows)
    rows = db.execute(query).scalars().all()
    return [_legacy_row_from_event(row, as_of=as_of) for row in rows]


def run_audit(
    db: Session,
    *,
    as_of: date,
    focus_universe: tuple[str, ...],
    top_n: int,
    max_rows: int | None = None,
) -> dict[str, object]:
    legacy_rows = _load_legacy_rows(db, as_of=as_of, max_rows=max_rows)
    event_rows = _load_event_rows(db, as_of=as_of, max_rows=max_rows)
    normalized_total = int(db.execute(select(func.count()).select_from(InsiderTransactionNormalized)).scalar() or 0)
    normalized_open_market_purchase = int(
        db.execute(
            select(func.count())
            .select_from(InsiderTransactionNormalized)
            .where(InsiderTransactionNormalized.is_duplicate.is_(False))
            .where(InsiderTransactionNormalized.transaction_type_normalized == "open_market_purchase")
        ).scalar()
        or 0
    )

    focus_set = set(focus_universe)
    usable_legacy = [row for row in legacy_rows if _row_is_usable(row, as_of=as_of)]
    usable_events = [row for row in event_rows if _row_is_usable(row, as_of=as_of)]
    preferred_rows = usable_legacy if usable_legacy else usable_events
    purchase_symbols = Counter(row.symbol for row in preferred_rows if row.symbol)
    sale_symbols = Counter(row.symbol for row in legacy_rows if row.side == "sale" and row.symbol)
    all_symbols = Counter(row.symbol for row in legacy_rows if row.symbol)
    by_year: dict[str, Counter[str]] = defaultdict(Counter)
    for row in legacy_rows:
        key = _year_key(row.filing_date or row.transaction_date)
        by_year[key]["rows"] += 1
        if row.side == "purchase":
            by_year[key]["purchases"] += 1
        if row.side == "sale":
            by_year[key]["sales"] += 1
        if row.filing_date is not None:
            by_year[key]["with_filing_date"] += 1
        if row.future_dated:
            by_year[key]["future_dated"] += 1

    top_purchase_symbols = [
        {
            "symbol": symbol,
            "usable_purchase_rows": int(count),
            "sale_rows": int(sale_symbols.get(symbol, 0)),
            "all_rows": int(all_symbols.get(symbol, 0)),
        }
        for symbol, count in purchase_symbols.most_common(top_n)
    ]
    focus_counts = [
        {
            "symbol": symbol,
            "usable_purchase_rows": int(purchase_symbols.get(symbol, 0)),
            "sale_rows": int(sale_symbols.get(symbol, 0)),
            "all_rows": int(all_symbols.get(symbol, 0)),
        }
        for symbol in focus_universe
    ]
    top_symbols = [str(item["symbol"]) for item in top_purchase_symbols if item.get("symbol")]
    price_symbols = tuple(dict.fromkeys([*top_symbols, *focus_universe, "SPY"]))

    return {
        "metadata": {
            "methodology_version": METHODOLOGY_VERSION,
            "run_timestamp": datetime.now(timezone.utc).isoformat(),
            "as_of": as_of.isoformat(),
            "data_state": "production PostgreSQL read-only research query",
            "confidence_note": "Legacy insider tables are lower confidence until normalized Form 4 rows are backfilled.",
        },
        "table_counts": {
            "insider_transactions": len(legacy_rows),
            "events_insider_trade": len(event_rows),
            "insider_transactions_normalized": normalized_total,
            "normalized_open_market_purchase": normalized_open_market_purchase,
        },
        "legacy_date_ranges": {
            "filing_date": _date_range(row.filing_date for row in legacy_rows),
            "transaction_date": _date_range(row.transaction_date for row in legacy_rows),
        },
        "event_date_ranges": {
            "filing_date": _date_range(row.filing_date for row in event_rows),
            "transaction_date": _date_range(row.transaction_date for row in event_rows),
        },
        "legacy_quality": {
            "purchase_rows": sum(1 for row in legacy_rows if row.side == "purchase"),
            "sale_rows": sum(1 for row in legacy_rows if row.side == "sale"),
            "missing_symbol_rows": sum(1 for row in legacy_rows if row.symbol is None),
            "missing_filing_date_rows": sum(1 for row in legacy_rows if row.filing_date is None),
            "future_dated_rows": sum(1 for row in legacy_rows if row.future_dated),
            "usable_purchase_rows": len(usable_legacy),
            "unique_usable_purchase_symbols": len({row.symbol for row in usable_legacy if row.symbol}),
            "unique_usable_purchase_insiders": len({row.reporting_cik or row.insider_name for row in usable_legacy if row.reporting_cik or row.insider_name}),
        },
        "event_quality": {
            "purchase_rows": sum(1 for row in event_rows if row.side == "purchase"),
            "sale_rows": sum(1 for row in event_rows if row.side == "sale"),
            "missing_symbol_rows": sum(1 for row in event_rows if row.symbol is None),
            "missing_filing_date_rows": sum(1 for row in event_rows if row.filing_date is None),
            "future_dated_rows": sum(1 for row in event_rows if row.future_dated),
            "usable_purchase_rows": len(usable_events),
            "unique_usable_purchase_symbols": len({row.symbol for row in usable_events if row.symbol}),
            "unique_usable_purchase_insiders": len({row.reporting_cik or row.insider_name for row in usable_events if row.reporting_cik or row.insider_name}),
        },
        "legacy_by_year": {
            year: dict(counter)
            for year, counter in sorted(by_year.items(), key=lambda item: item[0])
        },
        "top_usable_purchase_symbols": top_purchase_symbols,
        "focus_universe_counts": focus_counts,
        "price_coverage_for_candidate_symbols": _price_coverage(
            db,
            price_symbols,
            start_date=date(2013, 1, 1),
            end_date=as_of,
        ),
        "future_dated_examples": [
            {
                "source_table": row.source_table,
                "row_id": row.row_id,
                "symbol": row.symbol,
                "side": row.side,
                "filing_date": row.filing_date.isoformat() if row.filing_date else None,
                "transaction_date": row.transaction_date.isoformat() if row.transaction_date else None,
                "raw_type": row.raw_type,
            }
            for row in [item for item in legacy_rows if item.future_dated][:10]
        ],
    }


def _print_text_report(result: dict[str, object]) -> None:
    print(f"RUN methodology={result['metadata']['methodology_version']} as_of={result['metadata']['as_of']}")
    print(f"TABLE_COUNTS {json.dumps(result['table_counts'], sort_keys=True)}")
    print(f"LEGACY_QUALITY {json.dumps(result['legacy_quality'], sort_keys=True)}")
    print(f"EVENT_QUALITY {json.dumps(result['event_quality'], sort_keys=True)}")
    print("TOP_USABLE_PURCHASE_SYMBOLS")
    for item in result["top_usable_purchase_symbols"]:
        print(
            f"{item['symbol']} purchases={item['usable_purchase_rows']} "
            f"sales={item['sale_rows']} all_rows={item['all_rows']}"
        )
    print("FOCUS_UNIVERSE")
    for item in result["focus_universe_counts"]:
        print(
            f"{item['symbol']} purchases={item['usable_purchase_rows']} "
            f"sales={item['sale_rows']} all_rows={item['all_rows']}"
        )
    print("FUTURE_DATED_EXAMPLES")
    for item in result["future_dated_examples"]:
        print(json.dumps(item, sort_keys=True))


def _parse_symbols(value: str | None) -> tuple[str, ...]:
    if not value:
        return DEFAULT_UNIVERSE
    return _normalize_universe(part.strip() for part in value.split(","))


def main() -> None:
    parser = argparse.ArgumentParser(description="Read-only insider data coverage and quality audit.")
    parser.add_argument("--as-of", default=date.today().isoformat())
    parser.add_argument("--symbols", help="Comma-separated focus universe. Defaults to approved 24-symbol research universe.")
    parser.add_argument("--top-n", type=int, default=30)
    parser.add_argument("--max-rows", type=int, default=None, help="Optional local/test row limit.")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    as_of = parse_iso_date(args.as_of) or date.today()
    with SessionLocal() as db:
        result = run_audit(
            db,
            as_of=as_of,
            focus_universe=_parse_symbols(args.symbols),
            top_n=max(int(args.top_n), 1),
            max_rows=args.max_rows,
        )
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        _print_text_report(result)


if __name__ == "__main__":
    main()
