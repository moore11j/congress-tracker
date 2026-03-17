from __future__ import annotations

import json
import logging
import os
from statistics import mean, median
from datetime import datetime, timedelta, timezone

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.insider_market_trade import canonicalize_market_trade_type
from app.models import Event
from app.services.price_lookup import (
    get_close_for_date_or_prior,
    get_eod_close,
    get_eod_close_series,
    get_eod_close_with_meta,
)
from app.services.quote_lookup import get_current_prices_meta_db
from app.services.returns import signed_return_pct
from app.services.ticker_meta import normalize_cik
from app.utils.symbols import classify_symbol

METHODOLOGY_VERSION = "congress_v1"
INSIDER_METHODOLOGY_VERSION = "insider_v1"

logger = logging.getLogger(__name__)

_INSIDER_DEBUG_EVENT_ID_RAW = os.getenv("INSIDER_OUTCOME_DEBUG_EVENT_ID", "112349").strip().lower()
if _INSIDER_DEBUG_EVENT_ID_RAW in {"", "none", "off", "false", "0"}:
    INSIDER_DEBUG_EVENT_ID: int | None = None
elif _INSIDER_DEBUG_EVENT_ID_RAW == "all":
    INSIDER_DEBUG_EVENT_ID = -1
else:
    try:
        INSIDER_DEBUG_EVENT_ID = int(_INSIDER_DEBUG_EVENT_ID_RAW)
    except ValueError:
        INSIDER_DEBUG_EVENT_ID = 112349


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


def _parse_positive_float(value) -> float | None:
    if value is None:
        return None
    if isinstance(value, str):
        cleaned = value.strip().replace(",", "")
        if not cleaned:
            return None
        value = cleaned
    try:
        parsed = float(value)
    except Exception:
        return None
    return parsed if parsed > 0 else None


def _insider_transaction_price(payload: dict) -> float | None:
    raw = payload.get("raw") if isinstance(payload.get("raw"), dict) else {}
    direct_candidates = (
        payload.get("price"),
        payload.get("transaction_price"),
        payload.get("transactionPrice"),
        payload.get("price_per_share"),
        payload.get("pricePerShare"),
        payload.get("insider_transaction_price"),
    )
    raw_candidates = (
        raw.get("price"),
        raw.get("transactionPrice"),
        raw.get("transaction_price"),
        raw.get("pricePerShare"),
        raw.get("price_per_share"),
    )

    for candidate in (*direct_candidates, *raw_candidates):
        parsed = _parse_positive_float(candidate)
        if parsed is not None:
            return parsed
    return None


def _coerce_optional_bool(value: object) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes"}:
            return True
        if normalized in {"false", "0", "no"}:
            return False
    return None


def _is_market_eligible_insider_trade(is_market_trade: object, trade_type: str | None) -> bool:
    explicit_market_flag = _coerce_optional_bool(is_market_trade)
    if explicit_market_flag is not None:
        return explicit_market_flag
    return canonicalize_market_trade_type(trade_type) is not None


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


def _event_member_identity(event: Event, payload: dict, event_type: str) -> tuple[str | None, str | None]:
    if event_type == "insider_trade":
        raw = payload.get("raw") if isinstance(payload.get("raw"), dict) else {}
        insider_id = normalize_cik(
            payload.get("reporting_cik")
            or payload.get("reportingCik")
            or raw.get("reportingCik")
            or raw.get("reportingCIK")
            or raw.get("rptOwnerCik")
        )
        insider_name = (
            payload.get("insider_name")
            or payload.get("reporting_name")
            or payload.get("reportingName")
            or raw.get("reportingName")
            or raw.get("reportingOwnerName")
            or event.member_name
        )
        return insider_id, insider_name

    return event.member_bioguide_id, event.member_name


def _event_trade_date(payload: dict) -> str | None:
    raw = payload.get("raw") if isinstance(payload.get("raw"), dict) else {}
    return str(
        payload.get("trade_date")
        or payload.get("transaction_date")
        or payload.get("transactionDate")
        or raw.get("transactionDate")
        or ""
    )[:10] or None


def _should_log_insider_event(event_id: int | None, event_type: str) -> bool:
    if event_type != "insider_trade" or INSIDER_DEBUG_EVENT_ID is None:
        return False
    if INSIDER_DEBUG_EVENT_ID == -1:
        return True
    return event_id == INSIDER_DEBUG_EVENT_ID


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


def _benchmark_entry_close_for_trade_date(
    db: Session,
    benchmark_symbol: str,
    trade_date: str,
    benchmark_entry_memo: dict[tuple[str, str], float | None],
    benchmark_series_memo: dict[tuple[str, str], tuple[dict[str, float], list[str]]],
) -> float | None:
    memo_key = (benchmark_symbol, trade_date)
    if memo_key in benchmark_entry_memo:
        return benchmark_entry_memo[memo_key]

    # Use the same safe EOD lookup path as stock entry-price resolution,
    # and explicitly walk backward to the nearest valid prior trading day.
    try:
        end_date = datetime.strptime(trade_date, "%Y-%m-%d").date()
    except ValueError:
        benchmark_entry_memo[memo_key] = None
        return None

    for offset in range(0, 8):
        candidate_date = (end_date - timedelta(days=offset)).isoformat()
        candidate_meta = get_eod_close_with_meta(db, benchmark_symbol, candidate_date)
        candidate_close = candidate_meta.get("close")
        if candidate_close is not None and candidate_close > 0:
            benchmark_entry_memo[memo_key] = float(candidate_close)
            return float(candidate_close)

    direct_entry = get_eod_close(db, benchmark_symbol, trade_date)
    if direct_entry is not None and direct_entry > 0:
        benchmark_entry_memo[memo_key] = direct_entry
        return direct_entry

    if memo_key not in benchmark_series_memo:
        start_date = (end_date - timedelta(days=14)).isoformat()
        price_map = get_eod_close_series(
            db,
            symbol=benchmark_symbol,
            start_date=start_date,
            end_date=end_date.isoformat(),
        )
        benchmark_series_memo[memo_key] = (price_map, sorted(price_map.keys()))

    price_map, sorted_dates = benchmark_series_memo[memo_key]
    prior_entry = get_close_for_date_or_prior(trade_date, price_map, sorted_dates)
    benchmark_entry_memo[memo_key] = prior_entry
    return prior_entry

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
    return _compute_trade_outcomes(
        db=db,
        events=events,
        benchmark_symbol=benchmark_symbol,
        methodology_version=METHODOLOGY_VERSION,
        event_type="congress_trade",
        max_symbols_per_request=max_symbols_per_request,
    )


def compute_insider_trade_outcomes(
    db: Session,
    events: list[Event],
    benchmark_symbol: str,
    max_symbols_per_request: int | None = None,
) -> list[dict]:
    return _compute_trade_outcomes(
        db=db,
        events=events,
        benchmark_symbol=benchmark_symbol,
        methodology_version=INSIDER_METHODOLOGY_VERSION,
        event_type="insider_trade",
        max_symbols_per_request=max_symbols_per_request,
    )


def _compute_trade_outcomes(
    db: Session,
    events: list[Event],
    benchmark_symbol: str,
    methodology_version: str,
    event_type: str,
    max_symbols_per_request: int | None = None,
) -> list[dict]:
    """Canonical scoring methodology shared by APIs and persistence jobs."""

    insider_debug_enabled = event_type == "insider_trade" and INSIDER_DEBUG_EVENT_ID is not None
    if insider_debug_enabled:
        logger.debug(
            "[insider_outcomes] start events=%s debug_event_id=%s benchmark=%s",
            len(events),
            INSIDER_DEBUG_EVENT_ID,
            benchmark_symbol,
        )

    price_memo: dict[tuple[str, str], dict] = {}
    parsed_events = []
    quote_symbols: set[str] = set()
    for event in events:
        payload = _parse_payload(event.payload_json)
        parsed_trade_type = (
            event.trade_type
            or payload.get("trade_type")
            or payload.get("tradeType")
            or payload.get("transaction_type")
            or payload.get("transactionType")
        )
        is_market_trade = payload.get("is_market_trade") if isinstance(payload, dict) else None
        raw_symbol = (event.symbol or payload.get("symbol") or "").strip().upper()
        eligibility_status = "eligible"
        normalized_symbol = raw_symbol
        eligibility_error = None
        trade_date = _event_trade_date(payload)
        member_id, member_name = _event_member_identity(event, payload, event_type)
        market_eligible = True
        if event_type == "insider_trade":
            market_eligible = _is_market_eligible_insider_trade(is_market_trade, parsed_trade_type)

        if market_eligible:
            eligibility_status, normalized_symbol, eligibility_error = classify_symbol(raw_symbol)
            entry_price_meta = {"close": None, "status": eligibility_status, "error": eligibility_error}
        else:
            entry_price_meta = {
                "close": None,
                "status": "insider_non_market",
                "error": (
                    f"Insider transaction is non-market activity and excluded from insider analytics "
                    f"trade_type={parsed_trade_type or 'unknown'}"
                ),
            }

        effective_symbol = normalized_symbol or ""
        insider_transaction_price = _insider_transaction_price(payload) if event_type == "insider_trade" else None
        if event_type == "insider_trade" and market_eligible and insider_transaction_price is not None:
            entry_price_meta = {
                "close": insider_transaction_price,
                "status": "ok",
                "error": None,
                "source": "insider_transaction",
            }
        elif market_eligible and eligibility_status == "eligible" and normalized_symbol and trade_date:
            entry_price_meta = _entry_price_for_congress_event(db, normalized_symbol, trade_date, price_memo)
            if _should_log_insider_event(event.id, event_type):
                logger.debug(
                    "[insider_outcomes] event_id=%s entry_lookup symbol=%s trade_date=%s status=%s close=%s error=%s",
                    event.id,
                    normalized_symbol,
                    trade_date,
                    entry_price_meta.get("status"),
                    entry_price_meta.get("close"),
                    entry_price_meta.get("error"),
                )
            resolved_symbol = entry_price_meta.get("symbol")
            if isinstance(resolved_symbol, str) and resolved_symbol:
                effective_symbol = resolved_symbol

        if market_eligible and eligibility_status == "eligible" and effective_symbol:
            quote_symbols.add(effective_symbol)
        if _should_log_insider_event(event.id, event_type):
            logger.debug(
                "[insider_outcomes] event_id=%s symbol=%s parsed_trade_date=%s parsed_trade_type=%s entry_price_input=%s is_market_trade=%s eligibility_status=%s eligibility_error=%s",
                event.id,
                effective_symbol or raw_symbol,
                trade_date,
                parsed_trade_type,
                entry_price_meta,
                is_market_trade,
                eligibility_status,
                eligibility_error,
            )

        parsed_events.append((event, raw_symbol, effective_symbol, entry_price_meta, trade_date, eligibility_error, member_id, member_name, parsed_trade_type, is_market_trade, market_eligible))

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
    benchmark_entry_memo: dict[tuple[str, str], float | None] = {}
    benchmark_series_memo: dict[tuple[str, str], tuple[dict[str, float], list[str]]] = {}

    scored_rows: list[dict] = []
    for event, raw_symbol, normalized_symbol, entry_price_meta, trade_date, eligibility_error, member_id, member_name, parsed_trade_type, is_market_trade, market_eligible in parsed_events:
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

        if market_eligible and (current_price is None or current_price <= 0) and symbol:
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

        if _should_log_insider_event(event.id, event_type):
            logger.debug(
                "[insider_outcomes] event_id=%s current_lookup symbol=%s current_payload=%s resolved_current_price=%s resolved_current_date=%s quote_status=%s",
                event.id,
                symbol,
                current_payload,
                current_price,
                current_price_date,
                quote_status,
            )

        status = "ok"
        error = None
        if entry_price_meta.get("status") == "insider_non_market":
            status = "insider_non_market"
            error = entry_price_meta.get("error")
        elif not symbol:
            status = "no_symbol"
            error = "Missing symbol on event/payload"
        elif entry_price_meta.get("status") in {"unsupported_symbol", "non_equity_or_unpriced_asset", "provider_429", "provider_402", "provider_unavailable"}:
            status = str(entry_price_meta.get("status"))
            if status == "provider_429":
                error = entry_price_meta.get("error") or f"Provider rate-limited entry lookup symbol={symbol} trade_date={trade_date}"
            else:
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

        if _should_log_insider_event(event.id, event_type) and status != "ok":
            logger.debug(
                "[insider_outcomes] event_id=%s skip_reason status=%s error=%s symbol=%s trade_date=%s parsed_trade_type=%s is_market_trade=%s",
                event.id,
                status,
                error,
                symbol,
                trade_date,
                parsed_trade_type,
                is_market_trade,
            )

        return_pct = None
        alpha_pct = None
        benchmark_entry = None
        benchmark_return_pct = None

        if status == "ok" and current_price is not None and entry_price is not None:
            return_pct = signed_return_pct(current_price, entry_price, parsed_trade_type or event.trade_type)

        if status == "ok" and benchmark_current is not None and benchmark_current > 0 and trade_date:
            benchmark_entry = _benchmark_entry_close_for_trade_date(
                db,
                benchmark_symbol,
                trade_date,
                benchmark_entry_memo,
                benchmark_series_memo,
            )
            if benchmark_entry is None or benchmark_entry <= 0:
                status = "no_benchmark_entry"
                error = f"No benchmark entry for symbol={benchmark_symbol} trade_date={trade_date}"
                if _should_log_insider_event(event.id, event_type):
                    logger.debug(
                        "[insider_outcomes] event_id=%s benchmark_lookup success=false benchmark_current=%s benchmark_entry=%s trade_date=%s",
                        event.id,
                        benchmark_current,
                        benchmark_entry,
                        trade_date,
                    )
            else:
                benchmark_return_pct = float(((benchmark_current - benchmark_entry) / benchmark_entry) * 100)
                alpha_pct = float(return_pct - benchmark_return_pct) if return_pct is not None else None
                if _should_log_insider_event(event.id, event_type):
                    logger.debug(
                        "[insider_outcomes] event_id=%s benchmark_lookup success=true benchmark_current=%s benchmark_entry=%s trade_date=%s",
                        event.id,
                        benchmark_current,
                        benchmark_entry,
                        trade_date,
                    )
        elif status == "ok" and benchmark_current in (None, 0):
            status = "no_benchmark_current"
            error = f"No benchmark current quote for symbol={benchmark_symbol}"
            if _should_log_insider_event(event.id, event_type):
                logger.debug(
                    "[insider_outcomes] event_id=%s benchmark_lookup success=false benchmark_current=%s trade_date=%s",
                    event.id,
                    benchmark_current,
                    trade_date,
                )

        if _should_log_insider_event(event.id, event_type):
            logger.debug(
                "[insider_outcomes] event_id=%s price_lookup_success=%s entry_price=%s current_price=%s",
                event.id,
                bool(entry_price is not None and entry_price > 0 and current_price is not None and current_price > 0),
                entry_price,
                current_price,
            )

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
                "member_id": member_id,
                "member_name": member_name,
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
                "methodology_version": methodology_version,
            }
        )

    if insider_debug_enabled:
        ok_count = sum(1 for row in scored_rows if row.get("scoring_status") == "ok")
        logger.debug("[insider_outcomes] finished scored_rows=%s ok_rows=%s", len(scored_rows), ok_count)

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
