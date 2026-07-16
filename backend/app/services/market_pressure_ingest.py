from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable
from zoneinfo import ZoneInfo

import requests
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as postgres_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from app.clients.fmp import FMP_BASE_URL
from app.models import MarketPressureSnapshot, PriceCache, QuoteCache
from app.services import market_pressure
from app.services.confirmation_score import get_confirmation_score_bundles_for_tickers
from app.services.provider_usage import ensure_fmp_live_allowed, record_provider_response
from app.services.quote_lookup import quote_cache_upsert_many

logger = logging.getLogger(__name__)

MarketMinuteFetcher = Callable[[str], dict[str, Any] | None]
Sleeper = Callable[[float], None]


@dataclass(frozen=True)
class MarketPressureIngestResult:
    universe: str
    period: str
    status: str
    symbol_count: int
    snapshot_count: int
    fetched_count: int
    cache_hit_count: int
    skipped_count: int
    error_count: int
    generated_at: str


def is_market_hours(now: datetime | None = None) -> bool:
    local_now = (now or datetime.now(timezone.utc)).astimezone(ZoneInfo("America/New_York"))
    if local_now.weekday() >= 5:
        return False
    market_open = local_now.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close = local_now.replace(hour=16, minute=15, second=0, microsecond=0)
    return market_open <= local_now <= market_close


def refresh_market_pressure_snapshots(
    db: Session,
    *,
    universes: list[str] | None = None,
    period: str = "1d",
    force: bool = False,
    market_hours_only: bool = True,
    calls_per_minute: int | None = None,
    fresh_minutes: int | None = None,
    max_symbols: int | None = None,
    fetcher: MarketMinuteFetcher | None = None,
    sleep_fn: Sleeper = time.sleep,
    now: datetime | None = None,
) -> list[MarketPressureIngestResult]:
    generated_at = now or datetime.now(timezone.utc)
    if market_hours_only and not force and not is_market_hours(generated_at):
        return [
            MarketPressureIngestResult(
                universe=universe,
                period=period,
                status="skipped_market_closed",
                symbol_count=0,
                snapshot_count=0,
                fetched_count=0,
                cache_hit_count=0,
                skipped_count=0,
                error_count=0,
                generated_at=_dt_iso(generated_at),
            )
            for universe in (universes or ["sp500", "nasdaq100", "etf"])
        ]

    selected_universes = universes or ["sp500", "nasdaq100", "etf"]
    results = []
    for universe in selected_universes:
        results.append(
            refresh_market_pressure_universe_snapshot(
                db,
                universe=universe,
                period=period,
                generated_at=generated_at,
                calls_per_minute=calls_per_minute,
                fresh_minutes=fresh_minutes,
                max_symbols=max_symbols,
                fetcher=fetcher,
                sleep_fn=sleep_fn,
            )
        )
    return results


def refresh_market_pressure_universe_snapshot(
    db: Session,
    *,
    universe: str,
    period: str = "1d",
    generated_at: datetime | None = None,
    calls_per_minute: int | None = None,
    fresh_minutes: int | None = None,
    max_symbols: int | None = None,
    fetcher: MarketMinuteFetcher | None = None,
    sleep_fn: Sleeper = time.sleep,
) -> MarketPressureIngestResult:
    generated_at = generated_at or datetime.now(timezone.utc)
    resolved = market_pressure.resolve_market_pressure_params(universe=universe, period=period, view="market_pressure")
    symbols = market_pressure._resolve_universe_symbols(db, resolved.universe, user=None)
    if max_symbols is None:
        max_symbols = _max_symbols()
    if max_symbols > 0:
        symbols = symbols[:max_symbols]
    if not symbols:
        return MarketPressureIngestResult(resolved.universe, resolved.period, "empty", 0, 0, 0, 0, 0, 0, _dt_iso(generated_at))

    identities = market_pressure._load_identities(db, symbols)
    symbols = market_pressure._prioritize_live_quote_symbols(market_pressure._prioritize_symbols_for_market_data(symbols, identities))
    previous_closes = _previous_closes(db, symbols, generated_at.date())
    fresh_quotes = _fresh_quote_rows(db, symbols, generated_at, fresh_minutes=fresh_minutes)
    quote_prices: dict[str, float] = {}
    price_as_of: dict[str, datetime] = {}
    fetched_count = 0
    cache_hit_count = 0
    skipped_count = 0
    error_count = 0
    fetch = fetcher or _fetch_latest_intraday_minute
    rate_delay = _rate_delay(calls_per_minute)

    for symbol in symbols:
        cached = fresh_quotes.get(symbol)
        if cached is not None:
            quote_prices[symbol] = cached[0]
            price_as_of[symbol] = cached[1]
            cache_hit_count += 1
            continue
        try:
            row = fetch(symbol)
        except Exception:
            logger.exception("market_pressure_intraday_fetch_failed symbol=%s", symbol)
            error_count += 1
            row = None
        if not row:
            skipped_count += 1
            continue
        price = _safe_float(row.get("close") or row.get("price"))
        as_of = _parse_provider_datetime(row.get("date") or row.get("datetime") or row.get("timestamp")) or generated_at
        if price is None:
            skipped_count += 1
            continue
        quote_prices[symbol] = price
        price_as_of[symbol] = as_of
        fetched_count += 1
        if rate_delay > 0:
            sleep_fn(rate_delay)

    _write_price_caches(db, quote_prices, price_as_of, generated_at.date())
    bundles = get_confirmation_score_bundles_for_tickers(
        db,
        symbols,
        lookback_days=market_pressure.CONFIRMATION_FRESHNESS_WINDOW_DAYS,
    )

    snapshot_rows: list[dict[str, Any]] = []
    for symbol in symbols:
        identity = identities.get(symbol, market_pressure.Identity(symbol=symbol))
        current_price = quote_prices.get(symbol)
        previous = previous_closes.get(symbol)
        change_pct = _change_pct(previous[1], current_price) if previous and current_price is not None else None
        price = market_pressure.PricePerformance(
            change_pct=change_pct,
            start_at=market_pressure._date_iso(previous[0]) if previous else None,
            end_at=market_pressure._dt_iso(price_as_of[symbol]) if symbol in price_as_of else None,
            as_of=market_pressure._dt_iso(price_as_of[symbol]) if symbol in price_as_of else None,
            complete=change_pct is not None,
            market_cap=identity.market_cap,
        )
        tile = market_pressure._build_tile(symbol, identity, price, bundles.get(symbol, {}), generated_at)
        snapshot_rows.append(_snapshot_row(resolved.universe, resolved.period, symbol, tile, generated_at, price=current_price))

    _upsert_snapshots(db, snapshot_rows)
    status = "ok" if snapshot_rows else "empty"
    return MarketPressureIngestResult(
        universe=resolved.universe,
        period=resolved.period,
        status=status,
        symbol_count=len(symbols),
        snapshot_count=len(snapshot_rows),
        fetched_count=fetched_count,
        cache_hit_count=cache_hit_count,
        skipped_count=skipped_count,
        error_count=error_count,
        generated_at=_dt_iso(generated_at),
    )


def _fetch_latest_intraday_minute(symbol: str) -> dict[str, Any] | None:
    api_key = os.getenv("FMP_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("missing_fmp_api_key")
    ensure_fmp_live_allowed(category="market_pressure:intraday", symbol=symbol)
    response = requests.get(
        f"{FMP_BASE_URL}/historical-chart/1min",
        params={"symbol": symbol, "apikey": api_key},
        timeout=20,
    )
    record_provider_response(category="market_pressure:intraday", symbol=symbol, status_code=response.status_code)
    if response.status_code in {400, 404}:
        return None
    if response.status_code == 429:
        raise RuntimeError("fmp_rate_limited")
    response.raise_for_status()
    payload = response.json()
    rows = payload if isinstance(payload, list) else payload.get("data") if isinstance(payload, dict) else []
    if not isinstance(rows, list):
        return None
    valid_rows = [row for row in rows if isinstance(row, dict) and _safe_float(row.get("close") or row.get("price")) is not None]
    if not valid_rows:
        return None
    return max(valid_rows, key=lambda row: str(row.get("date") or row.get("datetime") or row.get("timestamp") or ""))


def _fresh_quote_rows(
    db: Session,
    symbols: list[str],
    generated_at: datetime,
    *,
    fresh_minutes: int | None = None,
) -> dict[str, tuple[float, datetime]]:
    cutoff = generated_at - timedelta(minutes=fresh_minutes or _fresh_minutes())
    rows = db.execute(
        select(QuoteCache.symbol, QuoteCache.price, QuoteCache.asof_ts)
        .where(QuoteCache.symbol.in_(symbols), QuoteCache.asof_ts >= cutoff.replace(tzinfo=None))
    ).all()
    result: dict[str, tuple[float, datetime]] = {}
    for symbol, price, asof in rows:
        if price is None or asof is None:
            continue
        result[str(symbol)] = (float(price), _ensure_aware(asof))
    return result


def _previous_closes(db: Session, symbols: list[str], today: date) -> dict[str, tuple[str, float]]:
    rows = db.execute(
        select(PriceCache.symbol, PriceCache.date, PriceCache.close)
        .where(PriceCache.symbol.in_(symbols), PriceCache.date < today.isoformat())
        .order_by(PriceCache.symbol.asc(), PriceCache.date.desc())
    ).all()
    result: dict[str, tuple[str, float]] = {}
    for symbol, day, close in rows:
        if symbol in result or close is None:
            continue
        result[str(symbol)] = (str(day), float(close))
    return result


def _write_price_caches(db: Session, prices: dict[str, float], as_of: dict[str, datetime], today: date) -> None:
    if not prices:
        return
    quote_cache_upsert_many(db, prices)
    for symbol, price in prices.items():
        db.merge(PriceCache(symbol=symbol, date=today.isoformat(), close=float(price)))
    db.commit()


def _upsert_snapshots(db: Session, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    insert_fn = postgres_insert if db.get_bind().dialect.name == "postgresql" else sqlite_insert
    stmt = insert_fn(MarketPressureSnapshot.__table__).values(rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=["universe", "period", "symbol"],
        set_={
            "company_name": stmt.excluded.company_name,
            "sector": stmt.excluded.sector,
            "exchange": stmt.excluded.exchange,
            "price": stmt.excluded.price,
            "price_change_pct": stmt.excluded.price_change_pct,
            "market_cap": stmt.excluded.market_cap,
            "confirmation_score": stmt.excluded.confirmation_score,
            "confirmation_direction": stmt.excluded.confirmation_direction,
            "data_state": stmt.excluded.data_state,
            "price_as_of": stmt.excluded.price_as_of,
            "confirmation_as_of": stmt.excluded.confirmation_as_of,
            "generated_at": stmt.excluded.generated_at,
            "source": stmt.excluded.source,
            "tile_json": stmt.excluded.tile_json,
        },
    )
    db.execute(stmt)
    db.commit()


def _snapshot_row(universe: str, period: str, symbol: str, tile: dict[str, Any], generated_at: datetime, *, price: float | None) -> dict[str, Any]:
    return {
        "universe": universe,
        "period": period,
        "symbol": symbol,
        "company_name": tile.get("companyName"),
        "sector": tile.get("sector"),
        "exchange": tile.get("exchange"),
        "price": price,
        "price_change_pct": tile.get("priceChangePct"),
        "market_cap": tile.get("marketCap"),
        "confirmation_score": tile.get("confirmationScore"),
        "confirmation_direction": tile.get("confirmationDirection"),
        "data_state": tile.get("dataState"),
        "price_as_of": _parse_provider_datetime(tile.get("priceEndAt")),
        "confirmation_as_of": _parse_provider_datetime(tile.get("confirmationAsOf")),
        "generated_at": generated_at,
        "source": "market_pressure_ingest",
        "tile_json": json.dumps(tile, separators=(",", ":"), default=str),
    }


def _change_pct(previous_close: float | None, current_price: float | None) -> float | None:
    if previous_close is None or current_price is None or previous_close == 0:
        return None
    return round(((current_price - previous_close) / previous_close) * 100, 4)


def _rate_delay(calls_per_minute: int | None) -> float:
    limit = calls_per_minute or int(os.getenv("MARKET_PRESSURE_INGEST_CALLS_PER_MINUTE", "450") or 450)
    if limit <= 0:
        return 0.0
    return 60.0 / float(limit)


def _fresh_minutes() -> int:
    try:
        return max(1, int(os.getenv("MARKET_PRESSURE_INGEST_FRESH_MINUTES", "55") or 55))
    except ValueError:
        return 55


def _max_symbols() -> int:
    try:
        return max(0, int(os.getenv("MARKET_PRESSURE_INGEST_MAX_SYMBOLS", "800") or 800))
    except ValueError:
        return 800


def _safe_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if parsed != parsed:
        return None
    return parsed


def _parse_provider_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return _ensure_aware(value)
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return _ensure_aware(parsed)


def _ensure_aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _dt_iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
