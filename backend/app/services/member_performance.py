from __future__ import annotations

import json
from statistics import mean, median
from datetime import datetime, timedelta

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models import Event
from app.services.price_lookup import get_eod_close
from app.services.quote_lookup import get_current_prices_db


def _parse_payload(payload_json) -> dict:
    try:
        if isinstance(payload_json, dict):
            return payload_json
        if isinstance(payload_json, str) and payload_json:
            parsed = json.loads(payload_json)
            if isinstance(parsed, dict):
                return parsed
    except Exception:
        pass
    return {}


def _entry_price_for_congress_event(
    db: Session,
    event: Event,
    payload: dict,
    price_memo: dict[tuple[str, str], float | None],
) -> tuple[str, float | None, str | None]:
    symbol = (event.symbol or payload.get("symbol") or "").strip().upper()
    trade_date = payload.get("trade_date") or payload.get("transaction_date")
    trade_date_str = str(trade_date or "")[:10]

    if not (symbol and trade_date_str):
        return symbol, None, trade_date_str or None

    key = (symbol, trade_date_str)
    if key not in price_memo:
        price_memo[key] = get_eod_close(db, symbol, trade_date_str)
    return symbol, price_memo[key], trade_date_str


def score_member_congress_trade_outcomes(
    db: Session,
    member_id: str,
    lookback_days: int,
    benchmark_symbol: str,
    max_score_trades: int,
    max_symbols_per_request: int | None = None,
):
    sort_ts = func.coalesce(Event.event_date, Event.ts)
    cutoff_dt = datetime.utcnow() - timedelta(days=lookback_days)
    events = db.execute(
        select(Event)
        .where(Event.member_bioguide_id == member_id)
        .where(Event.event_type == "congress_trade")
        .where(sort_ts >= cutoff_dt)
        .order_by(sort_ts.desc(), Event.id.desc())
    ).scalars().all()

    total_count = len(events)
    events_to_score = events[:max_score_trades]

    scored_rows = score_congress_events(
        db=db,
        events=events_to_score,
        benchmark_symbol=benchmark_symbol,
        max_symbols_per_request=max_symbols_per_request,
    )

    return {
        "events": events,
        "total_count": total_count,
        "scored_rows": scored_rows,
    }


def score_congress_events(
    db: Session,
    events: list[Event],
    benchmark_symbol: str,
    max_symbols_per_request: int | None = None,
) -> list[dict]:

    price_memo: dict[tuple[str, str], float | None] = {}
    parsed_events: list[tuple[Event, str, float | None, str | None]] = []
    quote_symbols: set[str] = set()
    for event in events:
        payload = _parse_payload(event.payload_json)
        symbol, entry_price, trade_date = _entry_price_for_congress_event(db, event, payload, price_memo)
        if symbol and entry_price is not None and entry_price > 0:
            quote_symbols.add(symbol)
        parsed_events.append((event, symbol, entry_price, trade_date))

    symbols_to_quote = sorted(quote_symbols)
    if max_symbols_per_request is not None and max_symbols_per_request > 0:
        symbols_to_quote = symbols_to_quote[:max_symbols_per_request]
    current_price_memo = get_current_prices_db(db, symbols_to_quote) if symbols_to_quote else {}
    benchmark_current_memo = get_current_prices_db(db, [benchmark_symbol])
    benchmark_current = benchmark_current_memo.get(benchmark_symbol)
    benchmark_entry_memo: dict[str, float | None] = {}

    scored_rows: list[dict] = []
    for event, symbol, entry_price, trade_date in parsed_events:
        current_price = current_price_memo.get(symbol) if symbol else None
        if current_price is None or entry_price is None or entry_price <= 0:
            continue

        return_pct = float(((current_price - entry_price) / entry_price) * 100)
        alpha_pct = None
        if benchmark_current is not None and benchmark_current > 0 and trade_date:
            if trade_date not in benchmark_entry_memo:
                benchmark_entry_memo[trade_date] = get_eod_close(db, benchmark_symbol, trade_date)
            bench_entry = benchmark_entry_memo[trade_date]
            if bench_entry is not None and bench_entry > 0:
                bench_ret = ((benchmark_current - bench_entry) / bench_entry) * 100
                alpha_pct = float(return_pct - bench_ret)

        sort_ts_value = event.event_date or event.ts
        holding_days = None
        if sort_ts_value is not None:
            holding_days = (datetime.utcnow().date() - sort_ts_value.date()).days

        scored_rows.append(
            {
                "event_id": event.id,
                "symbol": symbol,
                "trade_type": event.trade_type,
                "asof_date": sort_ts_value.date().isoformat() if sort_ts_value else None,
                "entry_price": float(entry_price),
                "current_price": float(current_price),
                "return_pct": return_pct,
                "alpha_pct": alpha_pct,
                "holding_days": holding_days,
            }
        )

    return scored_rows


def score_congress_trade_outcomes_by_member(
    db: Session,
    events: list[Event],
    benchmark_symbol: str,
    max_score_trades: int,
    max_symbols_per_request: int | None = None,
) -> dict[str, dict]:
    member_total_counts: dict[str, int] = {}
    member_scored_events: dict[str, list[Event]] = {}

    for event in events:
        member_id = (event.member_bioguide_id or "").strip()
        if not member_id:
            continue

        member_total_counts[member_id] = member_total_counts.get(member_id, 0) + 1
        selected = member_scored_events.setdefault(member_id, [])
        if len(selected) < max_score_trades:
            selected.append(event)

    out: dict[str, dict] = {}
    for member_id, selected_events in member_scored_events.items():
        out[member_id] = {
            "total_count": member_total_counts.get(member_id, 0),
            "scored_rows": score_congress_events(
                db=db,
                events=selected_events,
                benchmark_symbol=benchmark_symbol,
                max_symbols_per_request=max_symbols_per_request,
            ),
        }

    for member_id, total_count in member_total_counts.items():
        out.setdefault(member_id, {"total_count": total_count, "scored_rows": []})

    return out


def aggregate_member_performance(scored_rows: list[dict], total_count: int, max_score_trades: int) -> dict:
    scored_returns = [row["return_pct"] for row in scored_rows]
    scored_count = len(scored_returns)
    alpha_values = [row["alpha_pct"] for row in scored_rows if row.get("alpha_pct") is not None]

    if scored_count > 0:
        avg_return = mean(scored_returns)
        median_return = median(scored_returns)
        win_rate = sum(1 for value in scored_returns if value > 0) / scored_count
    else:
        avg_return = None
        median_return = None
        win_rate = None

    avg_alpha = mean(alpha_values) if alpha_values else None
    median_alpha = median(alpha_values) if alpha_values else None

    if total_count > max_score_trades:
        pnl_status = "partial"
    elif scored_count > 0 or total_count == 0:
        pnl_status = "ok"
    else:
        pnl_status = "unavailable"

    return {
        "trade_count_total": total_count,
        "trade_count_scored": scored_count,
        "avg_return": avg_return,
        "median_return": median_return,
        "win_rate": win_rate,
        "avg_alpha": avg_alpha,
        "median_alpha": median_alpha,
        "pnl_status": pnl_status,
    }
