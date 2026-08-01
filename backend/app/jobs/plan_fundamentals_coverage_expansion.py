from __future__ import annotations

import argparse
import json
import math
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timezone
from typing import Any

from sqlalchemy import select

from app.db import SessionLocal
from app.models import DataEnrichmentJob, Event, InsiderTransactionNormalized, PriceCache, TickerFinancialsCache
from app.services.backtesting.queries import first_text, parse_iso_date, parse_payload
from app.services.data_enrichment_queue import ACTIVE_STATUSES, enqueue_data_enrichment_job
from app.services.trade_outcome_display import normalize_trade_side
from app.utils.symbols import normalize_symbol


@dataclass
class SymbolActivity:
    symbol: str
    congress_purchases: int = 0
    congress_amount_max: float = 0.0
    insider_purchases: int = 0
    insider_value: float = 0.0
    insider_director_purchases: int = 0
    insider_officer_purchases: int = 0
    price_rows: int = 0
    adjusted_price_rows: int = 0
    dollar_volume_rows: int = 0
    avg_dollar_volume: float | None = None
    price_start: str | None = None
    price_end: str | None = None
    financial_cache_status: str | None = None
    financial_cache_fetched_at: str | None = None
    active_financial_jobs: int = 0
    priority_score: float = 0.0

    @property
    def total_signals(self) -> int:
        return self.congress_purchases + self.insider_purchases

    @property
    def has_financial_cache(self) -> bool:
        return self.financial_cache_status in {"ok", "partial"}

    @property
    def has_active_financial_job(self) -> bool:
        return self.active_financial_jobs > 0


def _utc_bounds(start_date: date | None, end_date: date) -> tuple[datetime, datetime]:
    start = datetime.combine(start_date or date(1990, 1, 1), time.min, tzinfo=timezone.utc)
    end = datetime.combine(end_date, time.max, tzinfo=timezone.utc)
    return start, end


def _activity_for_symbol(activities: dict[str, SymbolActivity], raw_symbol: str | None) -> SymbolActivity | None:
    symbol = normalize_symbol(raw_symbol)
    if not symbol:
        return None
    activity = activities.get(symbol)
    if activity is None:
        activity = SymbolActivity(symbol=symbol)
        activities[symbol] = activity
    return activity


def _load_congress_activity(db, activities: dict[str, SymbolActivity], *, start_date: date | None, end_date: date) -> None:
    query_start, query_end = _utc_bounds(start_date, end_date)
    rows = (
        db.execute(
            select(Event)
            .where(Event.event_type == "congress_trade")
            .where(Event.ts >= query_start)
            .where(Event.ts <= query_end)
            .order_by(Event.ts.asc(), Event.id.asc())
        )
        .scalars()
        .all()
    )
    for row in rows:
        payload = parse_payload(row.payload_json)
        side = normalize_trade_side(
            row.trade_type
            or row.transaction_type
            or first_text(payload, "trade_type", "tradeType", "transaction_type", "transactionType")
        )
        if side != "purchase":
            continue
        activity = _activity_for_symbol(activities, row.symbol or first_text(payload, "symbol", "ticker"))
        if activity is None:
            continue
        activity.congress_purchases += 1
        if row.amount_max is not None:
            activity.congress_amount_max += float(row.amount_max)


def _is_ceo_or_cfo(title: str | None) -> bool:
    text = str(title or "").strip().lower()
    return any(term in text for term in ("chief executive", "chief financial", "ceo", "cfo"))


def _load_insider_activity(db, activities: dict[str, SymbolActivity], *, start_date: date | None, end_date: date) -> None:
    statement = (
        select(InsiderTransactionNormalized)
        .where(InsiderTransactionNormalized.is_duplicate.is_(False))
        .where(InsiderTransactionNormalized.transaction_type_normalized == "open_market_purchase")
        .where(InsiderTransactionNormalized.ticker_normalized.is_not(None))
        .order_by(InsiderTransactionNormalized.filing_date.asc(), InsiderTransactionNormalized.id.asc())
    )
    if start_date is not None:
        statement = statement.where(InsiderTransactionNormalized.filing_date >= start_date)
    statement = statement.where(InsiderTransactionNormalized.filing_date <= end_date)
    rows = db.execute(statement).scalars().all()
    for row in rows:
        activity = _activity_for_symbol(activities, row.ticker_normalized)
        if activity is None:
            continue
        activity.insider_purchases += 1
        if row.value is not None:
            activity.insider_value += float(row.value)
        if row.is_director:
            activity.insider_director_purchases += 1
        if row.is_officer or _is_ceo_or_cfo(row.officer_title):
            activity.insider_officer_purchases += 1


def _load_price_coverage(db, activities: dict[str, SymbolActivity], *, start_date: date | None, end_date: date) -> None:
    symbols = sorted(activities)
    if not symbols:
        return
    statement = (
        select(PriceCache.symbol, PriceCache.date, PriceCache.adjusted_close, PriceCache.dollar_volume)
        .where(PriceCache.symbol.in_(symbols))
        .where(PriceCache.date <= end_date.isoformat())
        .order_by(PriceCache.symbol.asc(), PriceCache.date.asc())
    )
    if start_date is not None:
        statement = statement.where(PriceCache.date >= start_date.isoformat())
    rows = db.execute(statement).all()
    dollar_volume_sums: dict[str, float] = {}
    for raw_symbol, raw_day, adjusted_close, dollar_volume in rows:
        symbol = normalize_symbol(raw_symbol)
        activity = activities.get(symbol or "")
        if activity is None:
            continue
        activity.price_rows += 1
        if adjusted_close is not None and float(adjusted_close) > 0:
            activity.adjusted_price_rows += 1
        if dollar_volume is not None and float(dollar_volume) > 0:
            activity.dollar_volume_rows += 1
            dollar_volume_sums[activity.symbol] = dollar_volume_sums.get(activity.symbol, 0.0) + float(dollar_volume)
        day = str(raw_day)
        if activity.price_start is None or day < activity.price_start:
            activity.price_start = day
        if activity.price_end is None or day > activity.price_end:
            activity.price_end = day
    for symbol, total in dollar_volume_sums.items():
        activity = activities.get(symbol)
        if activity is not None and activity.dollar_volume_rows > 0:
            activity.avg_dollar_volume = round(total / activity.dollar_volume_rows, 2)


def _load_financial_cache_state(db, activities: dict[str, SymbolActivity]) -> None:
    symbols = sorted(activities)
    if not symbols:
        return
    rows = db.execute(select(TickerFinancialsCache).where(TickerFinancialsCache.symbol.in_(symbols))).scalars().all()
    for row in rows:
        symbol = normalize_symbol(row.symbol)
        activity = activities.get(symbol or "")
        if activity is None:
            continue
        activity.financial_cache_status = row.status
        activity.financial_cache_fetched_at = row.fetched_at.isoformat() if row.fetched_at else None


def _load_active_financial_jobs(db, activities: dict[str, SymbolActivity]) -> None:
    symbols = sorted(activities)
    if not symbols:
        return
    rows = (
        db.execute(
            select(DataEnrichmentJob.symbol, DataEnrichmentJob.status)
            .where(DataEnrichmentJob.job_type == "ticker_financials")
            .where(DataEnrichmentJob.status.in_(ACTIVE_STATUSES))
            .where(DataEnrichmentJob.symbol.in_(symbols))
        )
        .all()
    )
    for raw_symbol, _status in rows:
        symbol = normalize_symbol(raw_symbol)
        activity = activities.get(symbol or "")
        if activity is not None:
            activity.active_financial_jobs += 1


def _priority_score(activity: SymbolActivity) -> float:
    source_bonus = 20.0 if activity.congress_purchases and activity.insider_purchases else 0.0
    price_bonus = min(activity.adjusted_price_rows / 252.0, 2.0) * 10.0
    liquidity_bonus = min(math.log10(max(activity.avg_dollar_volume or 0.0, 1.0)), 8.0)
    congress_score = activity.congress_purchases * 5.0
    insider_score = activity.insider_purchases * 3.0
    role_score = activity.insider_director_purchases * 1.0 + activity.insider_officer_purchases * 1.0
    value_score = math.log10(max(activity.congress_amount_max + activity.insider_value, 0.0) + 1.0)
    queued_penalty = 15.0 if activity.has_active_financial_job else 0.0
    return round(source_bonus + price_bonus + liquidity_bonus + congress_score + insider_score + role_score + value_score - queued_penalty, 4)


def plan_fundamentals_coverage_expansion(
    *,
    start_date: date | None = None,
    end_date: date | None = None,
    min_adjusted_price_rows: int = 60,
    min_avg_dollar_volume: float = 1_000_000.0,
    include_cached: bool = False,
    include_queued: bool = False,
    limit: int = 100,
    batch_size: int = 25,
) -> dict[str, Any]:
    end = end_date or datetime.now(timezone.utc).date()
    activities: dict[str, SymbolActivity] = {}
    with SessionLocal() as db:
        _load_congress_activity(db, activities, start_date=start_date, end_date=end)
        _load_insider_activity(db, activities, start_date=start_date, end_date=end)
        _load_price_coverage(db, activities, start_date=start_date, end_date=end)
        _load_financial_cache_state(db, activities)
        _load_active_financial_jobs(db, activities)

    for activity in activities.values():
        activity.priority_score = _priority_score(activity)

    rows = sorted(activities.values(), key=lambda item: (item.priority_score, item.total_signals, item.adjusted_price_rows), reverse=True)
    missing_cache = [row for row in rows if not row.has_financial_cache]
    eligible = [
        row
        for row in rows
        if (include_cached or not row.has_financial_cache)
        and (include_queued or not row.has_active_financial_job)
        and row.adjusted_price_rows >= min_adjusted_price_rows
        and (row.avg_dollar_volume or 0.0) >= min_avg_dollar_volume
    ]
    selected = eligible[: max(0, limit)]
    batches = [
        [item.symbol for item in selected[index : index + max(1, batch_size)]]
        for index in range(0, len(selected), max(1, batch_size))
    ]
    return {
        "status": "ok",
        "mode": "read_only_plan",
        "run_timestamp": datetime.now(timezone.utc).isoformat(),
        "parameters": {
            "start_date": start_date.isoformat() if start_date else None,
            "end_date": end.isoformat(),
            "min_adjusted_price_rows": min_adjusted_price_rows,
            "min_avg_dollar_volume": min_avg_dollar_volume,
            "include_cached": include_cached,
            "include_queued": include_queued,
            "limit": limit,
            "batch_size": batch_size,
        },
        "coverage": {
            "strategy_signal_symbols": len(rows),
            "symbols_missing_financial_cache": len(missing_cache),
            "symbols_with_financial_cache": len(rows) - len(missing_cache),
            "missing_cache_with_min_adjusted_prices": sum(1 for row in missing_cache if row.adjusted_price_rows >= min_adjusted_price_rows),
            "missing_cache_without_min_adjusted_prices": sum(1 for row in missing_cache if row.adjusted_price_rows < min_adjusted_price_rows),
            "missing_cache_with_min_liquidity": sum(1 for row in missing_cache if (row.avg_dollar_volume or 0.0) >= min_avg_dollar_volume),
            "missing_cache_without_min_liquidity": sum(1 for row in missing_cache if (row.avg_dollar_volume or 0.0) < min_avg_dollar_volume),
            "symbols_with_active_financial_jobs": sum(1 for row in rows if row.has_active_financial_job),
            "eligible_selected": len(selected),
            "batch_count": len(batches),
        },
        "batches": batches,
        "symbols": [asdict(row) for row in selected],
    }


def enqueue_fundamentals_coverage_batch(
    *,
    start_date: date | None = None,
    end_date: date | None = None,
    min_adjusted_price_rows: int = 60,
    min_avg_dollar_volume: float = 1_000_000.0,
    limit: int = 25,
    batch_size: int = 25,
    priority: int = 70,
    reason: str = "strategies_fundamentals_coverage_expansion",
) -> dict[str, Any]:
    plan = plan_fundamentals_coverage_expansion(
        start_date=start_date,
        end_date=end_date,
        min_adjusted_price_rows=min_adjusted_price_rows,
        min_avg_dollar_volume=min_avg_dollar_volume,
        include_cached=False,
        include_queued=False,
        limit=limit,
        batch_size=batch_size,
    )
    selected_symbols = [row["symbol"] for row in plan["symbols"]]
    enqueued: list[str] = []
    skipped: list[str] = []
    for index, symbol in enumerate(selected_symbols):
        did_enqueue = enqueue_data_enrichment_job(
            job_type="ticker_financials",
            symbol=symbol,
            source="strategy_research",
            reason=reason,
            priority=priority + index,
            payload={
                "planner": "plan_fundamentals_coverage_expansion",
                "run_timestamp": plan["run_timestamp"],
                "min_adjusted_price_rows": min_adjusted_price_rows,
                "min_avg_dollar_volume": min_avg_dollar_volume,
            },
            max_attempts=3,
        )
        if did_enqueue:
            enqueued.append(symbol)
        else:
            skipped.append(symbol)

    return {
        "status": "ok",
        "mode": "apply_enqueue",
        "run_timestamp": datetime.now(timezone.utc).isoformat(),
        "parameters": {
            **plan["parameters"],
            "priority_start": priority,
            "reason": reason,
        },
        "coverage": plan["coverage"],
        "selected_symbols": selected_symbols,
        "enqueued_symbols": enqueued,
        "skipped_symbols": skipped,
        "enqueued_count": len(enqueued),
        "skipped_count": len(skipped),
        "planner_run_timestamp": plan["run_timestamp"],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Planner for expanding historical fundamentals coverage.")
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--min-adjusted-price-rows", type=int, default=60)
    parser.add_argument("--min-avg-dollar-volume", type=float, default=1_000_000.0)
    parser.add_argument("--include-cached", action="store_true")
    parser.add_argument("--include-queued", action="store_true")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--batch-size", type=int, default=25)
    parser.add_argument("--apply", action="store_true", help="Enqueue ticker_financials jobs for the selected uncached symbols.")
    parser.add_argument("--priority", type=int, default=70)
    parser.add_argument("--reason", default="strategies_fundamentals_coverage_expansion")
    args = parser.parse_args()

    parsed_start = parse_iso_date(args.start_date) if args.start_date else None
    parsed_end = parse_iso_date(args.end_date) if args.end_date else None
    if args.apply:
        result = enqueue_fundamentals_coverage_batch(
            start_date=parsed_start,
            end_date=parsed_end,
            min_adjusted_price_rows=max(0, int(args.min_adjusted_price_rows)),
            min_avg_dollar_volume=max(0.0, float(args.min_avg_dollar_volume)),
            limit=max(0, int(args.limit)),
            batch_size=max(1, int(args.batch_size)),
            priority=int(args.priority),
            reason=str(args.reason or "strategies_fundamentals_coverage_expansion"),
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return

    result = plan_fundamentals_coverage_expansion(
        start_date=parsed_start,
        end_date=parsed_end,
        min_adjusted_price_rows=max(0, int(args.min_adjusted_price_rows)),
        min_avg_dollar_volume=max(0.0, float(args.min_avg_dollar_volume)),
        include_cached=bool(args.include_cached),
        include_queued=bool(args.include_queued),
        limit=max(0, int(args.limit)),
        batch_size=max(1, int(args.batch_size)),
    )
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
