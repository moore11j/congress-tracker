from __future__ import annotations

import json
from bisect import bisect_left, bisect_right
from collections import defaultdict
from datetime import date, datetime, time, timedelta, timezone
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models import Event, PriceCache, SavedScreen, SavedScreenEvent, SavedScreenSnapshot, Security, Watchlist, WatchlistItem
from app.services.backtesting.models import BacktestSignal, BacktestStrategyConfig
from app.services.screener import build_screener_rows, screener_params_from_mapping
from app.services.ticker_meta import normalize_cik
from app.services.trade_outcome_display import normalize_trade_side
from app.utils.symbols import normalize_symbol


VISIBLE_SIGNAL_TRADE_SIDES = {"purchase", "p-purchase", "buy", "p"}


def parse_payload(raw_payload: str | None) -> dict[str, Any]:
    try:
        parsed = json.loads(raw_payload or "{}")
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def first_text(payload: dict[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    nested_raw = payload.get("raw")
    if isinstance(nested_raw, dict):
        for key in keys:
            value = nested_raw.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return None


def parse_iso_date(value: str | None) -> date | None:
    cleaned = (value or "").strip()
    if not cleaned:
        return None
    try:
        return date.fromisoformat(cleaned[:10])
    except ValueError:
        return None


def event_entry_date(event: Event, payload: dict[str, Any]) -> date | None:
    return (
        parse_iso_date(first_text(payload, "filing_date", "filingDate", "report_date", "reportDate"))
        or (event.event_date.date() if event.event_date is not None else None)
        or event.ts.date()
        or parse_iso_date(first_text(payload, "transaction_date", "transactionDate", "trade_date", "tradeDate"))
    )


def event_reporting_cik(payload: dict[str, Any]) -> str | None:
    return normalize_cik(
        first_text(payload, "reporting_cik", "reportingCik", "reportingCIK", "rptOwnerCik")
    )


def load_price_histories(
    db: Session,
    symbols: list[str],
    start_date: date,
    end_date: date,
) -> dict[str, dict[str, float]]:
    normalized_symbols = sorted({normalize_symbol(symbol) for symbol in symbols if normalize_symbol(symbol)})
    if not normalized_symbols:
        return {}

    rows = db.execute(
        select(PriceCache.symbol, PriceCache.date, PriceCache.close)
        .where(PriceCache.symbol.in_(normalized_symbols))
        .where(PriceCache.date >= start_date.isoformat())
        .where(PriceCache.date <= end_date.isoformat())
        .order_by(PriceCache.symbol.asc(), PriceCache.date.asc())
    ).all()

    price_maps: dict[str, dict[str, float]] = defaultdict(dict)
    for symbol, day, close in rows:
        if close is None:
            continue
        price_maps[str(symbol)][str(day)] = float(close)
    return dict(price_maps)


def sorted_price_dates(price_map: dict[str, float]) -> list[str]:
    return sorted(price_map.keys())


def first_price_on_or_after(target_date: date, price_map: dict[str, float]) -> tuple[date, float] | None:
    dates = sorted_price_dates(price_map)
    if not dates:
        return None
    target_key = target_date.isoformat()
    index = bisect_left(dates, target_key)
    if index >= len(dates):
        return None
    resolved = dates[index]
    return date.fromisoformat(resolved), float(price_map[resolved])


def last_price_on_or_before(target_date: date, price_map: dict[str, float]) -> tuple[date, float] | None:
    dates = sorted_price_dates(price_map)
    if not dates:
        return None
    target_key = target_date.isoformat()
    index = bisect_right(dates, target_key) - 1
    if index < 0:
        return None
    resolved = dates[index]
    return date.fromisoformat(resolved), float(price_map[resolved])


def price_on_or_before(target_date: str, price_map: dict[str, float], dates: list[str]) -> float | None:
    if not dates:
        return None
    index = bisect_right(dates, target_date[:10]) - 1
    if index < 0:
        return None
    return float(price_map[dates[index]])


def load_owned_watchlist(db: Session, *, watchlist_id: int, user_id: int) -> Watchlist | None:
    return (
        db.execute(
            select(Watchlist)
            .where(Watchlist.id == watchlist_id)
            .where(Watchlist.owner_user_id == user_id)
        )
        .scalars()
        .first()
    )


def load_watchlist_symbols(db: Session, *, watchlist_id: int) -> list[str]:
    rows = db.execute(
        select(Security.symbol)
        .join(WatchlistItem, WatchlistItem.security_id == Security.id)
        .where(WatchlistItem.watchlist_id == watchlist_id)
        .where(Security.symbol.is_not(None))
        .order_by(Security.symbol.asc())
    ).all()
    return [str(symbol).upper() for (symbol,) in rows if symbol]


def load_owned_saved_screen(db: Session, *, saved_screen_id: int, user_id: int) -> SavedScreen | None:
    return (
        db.execute(
            select(SavedScreen)
            .where(SavedScreen.id == saved_screen_id)
            .where(SavedScreen.user_id == user_id)
        )
        .scalars()
        .first()
    )


def load_saved_screen_entry_signals(
    db: Session,
    *,
    screen: SavedScreen,
    start_date: date,
    end_date: date,
    hold_days: int,
) -> list[BacktestSignal]:
    window_start = datetime.combine(start_date - timedelta(days=hold_days + 14), time.min, tzinfo=timezone.utc)
    window_end = datetime.combine(end_date + timedelta(days=1), time.min, tzinfo=timezone.utc)
    rows = (
        db.execute(
            select(SavedScreenEvent)
            .where(SavedScreenEvent.saved_screen_id == screen.id)
            .where(SavedScreenEvent.event_type == "entered_screen")
            .where(SavedScreenEvent.created_at >= window_start)
            .where(SavedScreenEvent.created_at < window_end)
            .order_by(SavedScreenEvent.created_at.asc(), SavedScreenEvent.id.asc())
        )
        .scalars()
        .all()
    )
    signals: list[BacktestSignal] = []
    for row in rows:
        ticker = normalize_symbol(row.ticker)
        if not ticker:
            continue
        signals.append(
            BacktestSignal(
                symbol=ticker,
                signal_date=row.created_at.date(),
                source_event_id=row.id,
                source_label=screen.name,
            )
        )
    return signals


def load_saved_screen_current_symbols(db: Session, *, screen: SavedScreen) -> tuple[list[str], str]:
    snapshot_rows = (
        db.execute(
            select(SavedScreenSnapshot.ticker)
            .where(SavedScreenSnapshot.saved_screen_id == screen.id)
            .order_by(func.upper(SavedScreenSnapshot.ticker).asc())
        )
        .all()
    )
    snapshot_symbols = [normalize_symbol(ticker) for (ticker,) in snapshot_rows]
    snapshot_symbols = [symbol for symbol in snapshot_symbols if symbol]
    if snapshot_symbols:
        return snapshot_symbols, "snapshot"

    params = screener_params_from_mapping(parse_payload(screen.params_json), page=1, page_size=100)
    rows = build_screener_rows(db, params, requested_rows=100)
    screen_symbols = [normalize_symbol(str(row.get("symbol") or "")) for row in rows]
    return [symbol for symbol in screen_symbols if symbol], "current_universe"


def _event_window(config: BacktestStrategyConfig) -> tuple[datetime, datetime]:
    window_start = datetime.combine(
        config.start_date - timedelta(days=config.hold_days + 14),
        time.min,
        tzinfo=timezone.utc,
    )
    window_end = datetime.combine(config.end_date + timedelta(days=1), time.min, tzinfo=timezone.utc)
    return window_start, window_end


def load_congress_signals(db: Session, config: BacktestStrategyConfig) -> list[BacktestSignal]:
    window_start, window_end = _event_window(config)
    query = (
        select(Event)
        .where(Event.event_type == "congress_trade")
        .where(Event.ts >= window_start)
        .where(Event.ts < window_end)
    )
    if config.source_scope == "house":
        query = query.where(func.lower(func.coalesce(Event.chamber, "")) == "house")
    elif config.source_scope == "senate":
        query = query.where(func.lower(func.coalesce(Event.chamber, "")) == "senate")
    elif config.source_scope == "member":
        query = query.where(func.lower(func.coalesce(Event.member_bioguide_id, "")) == (config.member_id or "").strip().lower())

    rows = db.execute(query.order_by(Event.ts.asc(), Event.id.asc())).scalars().all()
    signals: list[BacktestSignal] = []
    for row in rows:
        payload = parse_payload(row.payload_json)
        side = normalize_trade_side(row.trade_type or first_text(payload, "trade_type", "transaction_type", "transactionType"))
        if side not in VISIBLE_SIGNAL_TRADE_SIDES:
            continue
        symbol = normalize_symbol(row.symbol or first_text(payload, "symbol", "ticker"))
        entry_date = event_entry_date(row, payload)
        if not symbol or entry_date is None:
            continue
        signals.append(
            BacktestSignal(
                symbol=symbol,
                signal_date=entry_date,
                source_event_id=row.id,
                source_label=(row.member_name or row.member_bioguide_id or "Congress"),
            )
        )
    return signals


def load_insider_signals(db: Session, config: BacktestStrategyConfig) -> list[BacktestSignal]:
    window_start, window_end = _event_window(config)
    query = (
        select(Event)
        .where(Event.event_type == "insider_trade")
        .where(Event.ts >= window_start)
        .where(Event.ts < window_end)
    )
    rows = db.execute(query.order_by(Event.ts.asc(), Event.id.asc())).scalars().all()
    target_cik = normalize_cik(config.insider_cik) if config.source_scope == "insider" else None

    signals: list[BacktestSignal] = []
    for row in rows:
        payload = parse_payload(row.payload_json)
        side = normalize_trade_side(row.trade_type or first_text(payload, "trade_type", "transaction_type", "transactionType"))
        if side not in VISIBLE_SIGNAL_TRADE_SIDES:
            continue
        reporting_cik = event_reporting_cik(payload)
        if target_cik and reporting_cik != target_cik:
            continue
        symbol = normalize_symbol(row.symbol or first_text(payload, "symbol", "ticker"))
        entry_date = event_entry_date(row, payload)
        if not symbol or entry_date is None:
            continue
        signals.append(
            BacktestSignal(
                symbol=symbol,
                signal_date=entry_date,
                source_event_id=row.id,
                source_label=(first_text(payload, "insider_name", "insiderName") or reporting_cik or "Insider"),
            )
        )
    return signals
