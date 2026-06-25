from __future__ import annotations

import json
from bisect import bisect_left, bisect_right
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models import Event, PriceCache, SavedScreen, SavedScreenEvent, SavedScreenSnapshot, Security, Watchlist, WatchlistItem
from app.services.backtesting.models import BacktestSignal, BacktestStrategyConfig
from app.services.saved_screen_params import load_saved_screen_params
from app.services.screener import build_screener_rows, screener_params_from_mapping
from app.services.ticker_meta import normalize_cik
from app.services.trade_outcome_display import normalize_trade_side
from app.utils.symbols import normalize_symbol, symbol_variants


VISIBLE_SIGNAL_TRADE_SIDES = {"purchase", "p-purchase", "buy", "p"}
MAX_PRICE_FALLBACK_TRADING_DAYS = 7
EXEMPT_ACQUISITION_TRANSACTION_CODES = {"A", "M"}
EXEMPT_ACQUISITION_NORMALIZED_TYPES = {"grant_award", "option_exercise_conversion"}


@dataclass(frozen=True)
class ResolvedPrice:
    date: date
    close: float
    used_fallback: bool = False


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


def _flatten_payload_text(payload: dict[str, Any]) -> list[tuple[str, str]]:
    values: list[tuple[str, str]] = []

    def walk(value: Any, key: str = "") -> None:
        if isinstance(value, dict):
            for child_key, child_value in value.items():
                child_path = f"{key}.{child_key}" if key else str(child_key)
                walk(child_value, child_path)
        elif isinstance(value, list):
            for index, child_value in enumerate(value):
                child_path = f"{key}.{index}" if key else str(index)
                walk(child_value, child_path)
        elif value is not None:
            text = str(value).strip()
            if text:
                values.append((key, text))

    walk(payload)
    return values


def _key_token(value: str) -> str:
    return "".join(ch for ch in value.lower() if ch.isalnum())


def _first_nested_text(payload: dict[str, Any], *keys: str) -> str | None:
    wanted = [_key_token(key) for key in keys]
    for key, value in _flatten_payload_text(payload):
        token = _key_token(key)
        if any(token == item or token.endswith(item) or item in token for item in wanted):
            return value
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


def _insider_transaction_code(event: Event, payload: dict[str, Any]) -> str | None:
    value = (
        event.transaction_type
        or first_text(payload, "transaction_code", "transactionCode", "transaction_type", "transactionType")
        or _first_nested_text(
            payload,
            "transactionCode",
            "transaction_code",
            "transactionTypeCode",
            "transaction_type_code",
            "transactionCodingCode",
            "transactionCoding.code",
        )
    )
    normalized = (value or "").strip().upper()
    if "-" in normalized:
        normalized = normalized.split("-", 1)[0].strip()
    return normalized or None


def _insider_acquisition_disposition_code(payload: dict[str, Any]) -> str | None:
    return _first_nested_text(
        payload,
        "transactionAcquiredDisposedCode",
        "transaction_acquired_disposed_code",
        "acquiredDisposedCode",
        "acquisitionDispositionCode",
        "acquisition_or_disposition",
        "acquiredDisposed",
        "acquired_disposed",
    )


def _insider_raw_side(event: Event, payload: dict[str, Any]) -> str | None:
    return (
        event.trade_type
        or first_text(payload, "trade_type", "tradeType", "transaction_type", "transactionType")
        or _first_nested_text(
            payload,
            "trade_type",
            "tradeType",
            "transaction_type",
            "transactionType",
            "transactionTypeCode",
            "transaction_type_code",
            "transactionCode",
            "transaction_code",
            "transactionCodingCode",
            "transactionCoding.code",
        )
    )


def _insider_side(event: Event, payload: dict[str, Any]) -> str | None:
    side = normalize_trade_side(_insider_raw_side(event, payload))
    if side in {"purchase", "sale"}:
        return side
    acquired_disposed = (_insider_acquisition_disposition_code(payload) or "").strip().lower()
    if acquired_disposed in {"a", "acquired", "acquisition"}:
        return "purchase"
    if acquired_disposed in {"d", "disposed", "disposition"}:
        return "sale"
    return side


def _insider_description_text(event: Event, payload: dict[str, Any]) -> str:
    values = [
        event.trade_type,
        event.transaction_type,
        first_text(payload, "trade_type", "tradeType", "transaction_type", "transactionType"),
        first_text(payload, "transaction_code_description", "transactionCodeDescription"),
        first_text(payload, "transaction_type_normalized", "transactionTypeNormalized"),
        first_text(payload, "description", "transaction_description", "transactionDescription"),
    ]
    values.extend(value for _, value in _flatten_payload_text(payload))
    return " ".join(str(value).strip().lower() for value in values if value is not None and str(value).strip())


def is_exempt_acquisition(event: Event, payload: dict[str, Any]) -> bool:
    side = _insider_side(event, payload)
    description = _insider_description_text(event, payload)
    acquired_disposed = (_insider_acquisition_disposition_code(payload) or "").strip().lower()
    transaction_code = _insider_transaction_code(event, payload)
    normalized_type = (
        first_text(payload, "transaction_type_normalized", "transactionTypeNormalized")
        or _first_nested_text(payload, "transaction_type_normalized", "transactionTypeNormalized")
        or ""
    ).strip().lower()
    explicit_market = _coerce_optional_bool(payload.get("is_market_trade"))

    if side == "sale" or acquired_disposed in {"d", "disposed", "disposition"}:
        return False
    if any(term in description for term in ("sale", "sell", "sold", "disposition", "disposed")):
        return False
    if transaction_code == "P" or explicit_market is True:
        return False
    if transaction_code in EXEMPT_ACQUISITION_TRANSACTION_CODES:
        return True
    if normalized_type in EXEMPT_ACQUISITION_NORMALIZED_TYPES:
        return True
    if acquired_disposed in {"a", "acquired", "acquisition"} and any(
        term in description for term in ("exempt", "award", "grant", "exercise", "conversion", "acquisition")
    ):
        return True
    return any(term in description for term in ("a-award", "grant", "award", "exempt acquisition"))


def is_buy_like_entry(event: Event, payload: dict[str, Any], *, include_exempt_acquisitions: bool = False) -> bool:
    side = normalize_trade_side(_insider_raw_side(event, payload))
    if side in VISIBLE_SIGNAL_TRADE_SIDES:
        return True
    return include_exempt_acquisitions and is_exempt_acquisition(event, payload)


def insider_source_label(payload: dict[str, Any], *, reporting_cik: str | None, exempt_acquisition: bool) -> str:
    base_label = first_text(payload, "insider_name", "insiderName", "reporting_owner_name", "reportingOwnerName") or reporting_cik or "Insider"
    if not exempt_acquisition:
        return base_label
    raw_label = (
        first_text(payload, "transaction_code_description", "transactionCodeDescription")
        or first_text(payload, "transaction_type", "transactionType")
        or first_text(payload, "trade_type", "tradeType")
        or _first_nested_text(payload, "transactionCode", "transaction_code")
        or "Exempt acquisition"
    )
    return f"{base_label} - Exempt acquisition ({raw_label})"


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
    lookup_symbols = sorted(
        {
            variant
            for symbol in normalized_symbols
            for variant in (symbol_variants(symbol) or [symbol])
            if variant
        }
    )

    rows = db.execute(
        select(PriceCache.symbol, PriceCache.date, PriceCache.close)
        .where(PriceCache.symbol.in_(lookup_symbols))
        .where(PriceCache.date >= start_date.isoformat())
        .where(PriceCache.date <= end_date.isoformat())
        .order_by(PriceCache.symbol.asc(), PriceCache.date.asc())
    ).all()

    variant_price_maps: dict[str, dict[str, float]] = defaultdict(dict)
    for symbol, day, close in rows:
        if close is None:
            continue
        variant_price_maps[str(symbol)][str(day)] = float(close)

    price_maps: dict[str, dict[str, float]] = {}
    for requested_symbol in normalized_symbols:
        candidates = symbol_variants(requested_symbol) or [requested_symbol]
        best_symbol = max(
            candidates,
            key=lambda candidate: (
                len(variant_price_maps.get(candidate, {})),
                1 if candidate == requested_symbol else 0,
            ),
        )
        if variant_price_maps.get(best_symbol):
            price_maps[requested_symbol] = dict(variant_price_maps[best_symbol])
    return price_maps


def sorted_price_dates(price_map: dict[str, float]) -> list[str]:
    return sorted(price_map.keys())


def _resolved_price_at_index(price_map: dict[str, float], dates: list[str], index: int, *, exact_index: int) -> ResolvedPrice | None:
    if index < 0 or index >= len(dates):
        return None
    resolved = dates[index]
    close = price_map.get(resolved)
    if close is None or close <= 0:
        return None
    return ResolvedPrice(
        date=date.fromisoformat(resolved),
        close=float(close),
        used_fallback=index != exact_index,
    )


def first_price_on_or_after(
    target_date: date,
    price_map: dict[str, float],
    *,
    max_trading_days: int | None = None,
) -> ResolvedPrice | None:
    dates = sorted_price_dates(price_map)
    if not dates:
        return None
    target_key = target_date.isoformat()
    index = bisect_left(dates, target_key)
    if index >= len(dates):
        return None
    max_index = min(index + max(max_trading_days or 0, 0), len(dates) - 1) if max_trading_days is not None else len(dates) - 1
    for resolved_index in range(index, max_index + 1):
        resolved = _resolved_price_at_index(price_map, dates, resolved_index, exact_index=index)
        if resolved is not None:
            return resolved
    return None


def last_price_on_or_before(
    target_date: date,
    price_map: dict[str, float],
    *,
    max_trading_days: int | None = None,
) -> ResolvedPrice | None:
    dates = sorted_price_dates(price_map)
    if not dates:
        return None
    target_key = target_date.isoformat()
    index = bisect_right(dates, target_key) - 1
    if index < 0:
        return None
    min_index = max(index - max(max_trading_days or 0, 0), 0) if max_trading_days is not None else 0
    for resolved_index in range(index, min_index - 1, -1):
        resolved = _resolved_price_at_index(price_map, dates, resolved_index, exact_index=index)
        if resolved is not None:
            return resolved
    return None


def nearest_price_on_date(
    target_date: date,
    price_map: dict[str, float],
    *,
    prefer_previous: bool,
    max_backward_trading_days: int = MAX_PRICE_FALLBACK_TRADING_DAYS,
    max_forward_trading_days: int = MAX_PRICE_FALLBACK_TRADING_DAYS,
) -> ResolvedPrice | None:
    dates = sorted_price_dates(price_map)
    if not dates:
        return None

    target_key = target_date.isoformat()
    next_index = bisect_left(dates, target_key)
    previous_index = bisect_right(dates, target_key) - 1
    if next_index < len(dates) and dates[next_index] == target_key:
        return _resolved_price_at_index(price_map, dates, next_index, exact_index=next_index)

    search_order: list[tuple[int, int, int]] = []
    if prefer_previous:
        search_order.append((previous_index, -1, max_backward_trading_days))
        search_order.append((next_index, 1, max_forward_trading_days))
    else:
        search_order.append((next_index, 1, max_forward_trading_days))
        search_order.append((previous_index, -1, max_backward_trading_days))

    for start_index, step, steps_allowed in search_order:
        if start_index < 0 or start_index >= len(dates):
            continue
        for offset in range(max(steps_allowed, 0) + 1):
            candidate_index = start_index + (offset * step)
            if candidate_index < 0 or candidate_index >= len(dates):
                break
            resolved = _resolved_price_at_index(price_map, dates, candidate_index, exact_index=start_index)
            if resolved is not None:
                resolved_date = resolved.date if isinstance(resolved.date, date) else date.fromisoformat(resolved.date)
                calendar_delta = abs((resolved_date - target_date).days)
                max_calendar_days = max(steps_allowed, 0) * 3 + 7
                if calendar_delta > max_calendar_days:
                    continue
                return ResolvedPrice(date=resolved.date, close=resolved.close, used_fallback=True)
    return None


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

    params = screener_params_from_mapping(load_saved_screen_params(screen.params_json, screen_name=screen.name), page=1, page_size=100)
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
    elif config.source_scope == "member_list":
        member_ids = [(member_id or "").strip().lower() for member_id in config.member_ids if member_id]
        if member_ids:
            query = query.where(func.lower(func.coalesce(Event.member_bioguide_id, "")).in_(member_ids))

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
        exempt_acquisition = is_exempt_acquisition(row, payload)
        if not is_buy_like_entry(
            row,
            payload,
            include_exempt_acquisitions=config.include_exempt_acquisitions,
        ):
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
                source_label=insider_source_label(
                    payload,
                    reporting_cik=reporting_cik,
                    exempt_acquisition=exempt_acquisition,
                ),
                is_exempt_acquisition=exempt_acquisition,
            )
        )
    return signals
