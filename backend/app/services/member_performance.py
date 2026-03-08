from __future__ import annotations

import json
from statistics import mean, median
from datetime import datetime, timedelta, timezone

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models import Event
from app.services.price_lookup import get_eod_close, get_eod_close_with_meta
from app.services.quote_lookup import get_current_prices_meta_db
from app.utils.symbols import classify_symbol

METHODOLOGY_VERSION = "congress_v1"


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
    symbol: str,
    trade_date: str,
    price_memo: dict[tuple[str, str], dict],
) -> dict:
    key = (symbol, trade_date)
    if key not in price_memo:
        price_memo[key] = get_eod_close_with_meta(db, symbol, trade_date)
    return price_memo[key]




def _latest_eod_close_with_meta(db: Session, symbol: str, max_days_back: int = 7) -> dict:
    today = datetime.now(timezone.utc).date()
    saw_402 = False
    saw_429 = False

    for offset in range(max_days_back + 1):
        target_date = (today - timedelta(days=offset)).isoformat()
        result = get_eod_close_with_meta(db, symbol, target_date)
        close = result.get("close")
        if close is not None and close > 0:
            return {
                "close": close,
                "date": target_date,
                "status": "ok",
                "error": None,
            }
        status = result.get("status")
        if status == "provider_402":
            saw_402 = True
        elif status == "provider_429":
            saw_429 = True

    if saw_402:
        return {"close": None, "date": None, "status": "provider_402", "error": "Provider plan does not cover symbol"}
    if saw_429:
        return {"close": None, "date": None, "status": "provider_429", "error": "Provider rate-limited request"}
    return {"close": None, "date": None, "status": "no_data", "error": f"No recent EOD close for symbol={symbol}"}

def score_member_congress_trade_outcomes(
    db: Session,
    member_id: str,
    lookback_days: int,
    benchmark_symbol: str,
    max_score_trades: int,
    max_symbols_per_request: int | None = None,
):
    sort_ts = func.coalesce(Event.event_date, Event.ts)
    cutoff_dt = datetime.now(timezone.utc) - timedelta(days=lookback_days)
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

    outcomes = compute_congress_trade_outcomes(
        db=db,
        events=events,
        benchmark_symbol=benchmark_symbol,
        max_symbols_per_request=max_symbols_per_request,
    )

    scored_rows: list[dict] = []
    for outcome in outcomes:
        if outcome.get("scoring_status") != "ok":
            continue
        scored_rows.append(
            {
                "event_id": outcome["event_id"],
                "symbol": outcome["symbol"],
                "trade_type": outcome["trade_type"],
                "asof_date": outcome["asof_date"],
                "entry_price": outcome["entry_price"],
                "current_price": outcome["current_price"],
                "return_pct": outcome["return_pct"],
                "alpha_pct": outcome["alpha_pct"],
                "holding_days": outcome["holding_days"],
            }
        )

    return scored_rows


def compute_congress_trade_outcomes(
    db: Session,
    events: list[Event],
    benchmark_symbol: str,
    max_symbols_per_request: int | None = None,
) -> list[dict]:
    """Canonical congress scoring methodology shared by APIs and persistence jobs."""

    price_memo: dict[tuple[str, str], dict] = {}
    parsed_events: list[tuple[Event, str, str, dict, str | None, str | None]] = []
    quote_symbols: set[str] = set()
    for event in events:
        payload = _parse_payload(event.payload_json)
        raw_symbol = (event.symbol or payload.get("symbol") or "").strip().upper()
        eligibility_status, normalized_symbol, eligibility_error = classify_symbol(raw_symbol)
        entry_price_meta = {"close": None, "status": eligibility_status, "error": eligibility_error}
        trade_date = str(payload.get("trade_date") or payload.get("transaction_date") or "")[:10] or None

        effective_symbol = normalized_symbol or ""
        if eligibility_status == "eligible" and normalized_symbol and trade_date:
            entry_price_meta = _entry_price_for_congress_event(db, normalized_symbol, trade_date, price_memo)
            resolved_symbol = entry_price_meta.get("symbol")
            if isinstance(resolved_symbol, str) and resolved_symbol:
                effective_symbol = resolved_symbol
            if effective_symbol:
                quote_symbols.add(effective_symbol)
        parsed_events.append((event, raw_symbol, effective_symbol, entry_price_meta, trade_date, eligibility_error))

    symbols_to_quote = sorted(quote_symbols)
    chunk_size = max_symbols_per_request if max_symbols_per_request is not None and max_symbols_per_request > 0 else 200
    current_price_meta: dict[str, dict] = {}
    if symbols_to_quote:
        for idx in range(0, len(symbols_to_quote), chunk_size):
            chunk = symbols_to_quote[idx: idx + chunk_size]
            current_price_meta.update(get_current_prices_meta_db(db, chunk))
    benchmark_current_meta = get_current_prices_meta_db(db, [benchmark_symbol])
    benchmark_current_payload = benchmark_current_meta.get(benchmark_symbol, {})
    benchmark_current = benchmark_current_payload.get("price") if isinstance(benchmark_current_payload, dict) else None
    benchmark_current_date = None
    benchmark_asof = benchmark_current_payload.get("asof_ts") if isinstance(benchmark_current_payload, dict) else None
    if benchmark_asof is not None:
        benchmark_current_date = benchmark_asof.date()
    elif benchmark_current is not None:
        benchmark_current_date = datetime.now(timezone.utc).date()
    benchmark_entry_memo: dict[str, float | None] = {}

    scored_rows: list[dict] = []
    for event, raw_symbol, normalized_symbol, entry_price_meta, trade_date, eligibility_error in parsed_events:
        symbol = normalized_symbol or raw_symbol
        current_payload = current_price_meta.get(symbol, {}) if symbol else {}
        current_price = current_payload.get("price") if isinstance(current_payload, dict) else None
        entry_price = entry_price_meta.get("close")
        current_price_date = None
        current_asof = current_payload.get("asof_ts") if isinstance(current_payload, dict) else None
        quote_status = current_payload.get("status") if isinstance(current_payload, dict) else None
        if current_asof is not None:
            current_price_date = current_asof.date()
        elif current_price is not None:
            current_price_date = datetime.now(timezone.utc).date()

        if (current_price is None or current_price <= 0) and symbol:
            eod_fallback = _latest_eod_close_with_meta(db, symbol)
            fallback_close = eod_fallback.get("close")
            if fallback_close is not None and fallback_close > 0:
                current_price = float(fallback_close)
                fallback_date = eod_fallback.get("date")
                if isinstance(fallback_date, str):
                    try:
                        current_price_date = datetime.strptime(fallback_date, "%Y-%m-%d").date()
                    except ValueError:
                        current_price_date = datetime.now(timezone.utc).date()
                else:
                    current_price_date = datetime.now(timezone.utc).date()
            elif quote_status in {"provider_429", "provider_402"}:
                quote_status = quote_status
            else:
                quote_status = eod_fallback.get("status")

        status = "ok"
        error = None
        if not symbol:
            status = "no_symbol"
            error = "Missing symbol on event/payload"
        elif entry_price_meta.get("status") in {"unsupported_symbol", "non_equity_or_unpriced_asset", "provider_429", "provider_402", "provider_unavailable"}:
            status = str(entry_price_meta.get("status"))
            error = entry_price_meta.get("error") or eligibility_error
        elif entry_price_meta.get("status") == "no_data":
            status = "no_data"
            error = entry_price_meta.get("error") or f"No entry close for symbol={symbol} trade_date={trade_date}"
        elif entry_price is None or entry_price <= 0:
            status = "no_entry_price"
            error = f"No entry close for symbol={symbol} trade_date={trade_date}"
        elif current_price is None or current_price <= 0:
            if quote_status in {"provider_429", "provider_402"}:
                status = quote_status
                error = f"Provider quote lookup failed with status={quote_status} symbol={symbol}"
            else:
                status = "no_current_price"
                error = f"No current quote or recent EOD close for symbol={symbol}"

        return_pct = None
        alpha_pct = None
        benchmark_entry = None
        benchmark_return_pct = None

        if status == "ok" and current_price is not None and entry_price is not None:
            return_pct = float(((current_price - entry_price) / entry_price) * 100)

        if status == "ok" and benchmark_current is not None and benchmark_current > 0 and trade_date:
            if trade_date not in benchmark_entry_memo:
                benchmark_entry_memo[trade_date] = get_eod_close(db, benchmark_symbol, trade_date)
            benchmark_entry = benchmark_entry_memo[trade_date]
            if benchmark_entry is None or benchmark_entry <= 0:
                status = "no_benchmark_entry"
                error = f"No benchmark entry for symbol={benchmark_symbol} trade_date={trade_date}"
            else:
                benchmark_return_pct = float(((benchmark_current - benchmark_entry) / benchmark_entry) * 100)
                alpha_pct = float(return_pct - benchmark_return_pct) if return_pct is not None else None
        elif status == "ok" and benchmark_current in (None, 0):
            status = "no_benchmark_current"
            error = f"No benchmark current quote for symbol={benchmark_symbol}"

        sort_ts_value = event.event_date or event.ts
        holding_days = None
        if sort_ts_value is not None:
            holding_days = (datetime.now(timezone.utc).date() - sort_ts_value.date()).days

        scored_rows.append(
            {
                "event_id": event.id,
                "symbol": symbol,
                "trade_type": event.trade_type,
                "asof_date": sort_ts_value.date().isoformat() if sort_ts_value else None,
                "member_id": event.member_bioguide_id,
                "member_name": event.member_name,
                "source": event.source,
                "trade_date": trade_date,
                "entry_price": float(entry_price) if entry_price is not None else None,
                "entry_price_date": trade_date,
                "current_price": float(current_price) if current_price is not None else None,
                "current_price_date": current_price_date.isoformat() if current_price_date else None,
                "benchmark_symbol": benchmark_symbol,
                "benchmark_entry_price": float(benchmark_entry) if benchmark_entry is not None else None,
                "benchmark_current_price": float(benchmark_current) if benchmark_current is not None else None,
                "benchmark_current_price_date": benchmark_current_date.isoformat() if benchmark_current_date else None,
                "return_pct": return_pct,
                "benchmark_return_pct": benchmark_return_pct,
                "alpha_pct": alpha_pct,
                "holding_days": holding_days,
                "amount_min": event.amount_min,
                "amount_max": event.amount_max,
                "scoring_status": status,
                "scoring_error": error,
                "methodology_version": METHODOLOGY_VERSION,
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
