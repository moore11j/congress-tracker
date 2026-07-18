from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from time import perf_counter
from typing import Any, Callable, Literal

from fastapi import HTTPException
from sqlalchemy import event, func, or_, select
from sqlalchemy.orm import Session

from app.entitlements import TierEntitlements
from app.models import FundamentalsCache, MarketPressureSnapshot, PriceCache, QuoteCache, Security, TickerMeta, UserAccount, Watchlist, WatchlistItem
from app.services.confirmation_score import (
    SOURCE_ORDER,
    confirmation_active_source_count,
    confirmation_band_for_score,
    get_confirmation_score_bundles_for_tickers,
)
from app.services.index_memberships import active_index_membership_snapshot, index_universe_capabilities
from app.services.quote_lookup import get_current_prices_meta_db
from app.utils.symbols import normalize_symbol

logger = logging.getLogger(__name__)

MarketPressurePeriod = Literal["1d", "5d", "1m", "3m", "ytd", "1y"]
MarketPressureUniverse = Literal["sp500", "nasdaq100", "etf", "all_us", "watchlist"]
MarketPressureView = Literal["market_pressure", "hidden_accumulation", "fragile_winners", "crowded_trades", "rotation"]
MarketPressureDirection = Literal["bullish", "bearish", "neutral", "conflicted", "unavailable"]
Divergence = Literal[
    "hidden_accumulation",
    "fragile_winner",
    "aligned_bullish",
    "aligned_bearish",
    "conflicted",
    "none",
    "unavailable",
]

CONFIRMATION_FRESHNESS_WINDOW_DAYS = 30
SCORING_VERSION = "confirmation_score_v1"
MARKET_PRESSURE_LIVE_PRICE_DEFAULT_LIMIT = 260
MARKET_PRESSURE_UNIVERSE_SYMBOL_EXCLUSIONS: dict[MarketPressureUniverse, set[str]] = {
    "sp500": {"GOOG"},
}
MARKET_PRESSURE_LIVE_QUOTE_PRIORITY: tuple[str, ...] = (
    "NVDA",
    "AAPL",
    "MSFT",
    "GOOGL",
    "AMZN",
    "META",
    "AVGO",
    "ORCL",
    "TSLA",
    "PLTR",
    "INTC",
    "MU",
    "JPM",
    "XOM",
)
MARKET_PRESSURE_IMPORTANT_MARKET_CAP_FLOORS: dict[str, float] = {
    "NVDA": 500_000_000_000,
    "AAPL": 500_000_000_000,
    "MSFT": 500_000_000_000,
    "GOOGL": 300_000_000_000,
    "AMZN": 300_000_000_000,
    "META": 300_000_000_000,
    "AVGO": 150_000_000_000,
    "ORCL": 100_000_000_000,
    "TSLA": 100_000_000_000,
    "JPM": 100_000_000_000,
    "XOM": 100_000_000_000,
    "MU": 25_000_000_000,
    "INTC": 20_000_000_000,
}
MARKET_PRESSURE_SNAPSHOT_FRESH_MINUTES = 75
MARKET_PRESSURE_SNAPSHOT_MIN_COVERAGE = 0.8
SUPPORTED_PERIODS: tuple[MarketPressurePeriod, ...] = ("1d", "5d", "1m", "3m", "ytd", "1y")
SUPPORTED_UNIVERSES: tuple[MarketPressureUniverse, ...] = ("sp500", "nasdaq100", "etf", "all_us", "watchlist")
SUPPORTED_VIEWS: tuple[MarketPressureView, ...] = (
    "market_pressure",
    "hidden_accumulation",
    "fragile_winners",
    "crowded_trades",
    "rotation",
)

SOURCE_LAYER_KEYS = {
    "price_volume": "priceVolume",
    "fundamentals": "fundamentals",
    "congress": "congress",
    "insiders": "insiders",
    "government_contracts": "governmentContracts",
    "signals": "signals",
    "institutional_activity": "institutions",
    "options_flow": "optionsFlow",
    "macro_positioning": "macroPositioning",
}

ETF_ASSET_CLASS_VALUES = {
    "etf",
    "etfs",
    "etf_fund",
    "etf fund",
    "fund",
    "mutual fund",
    "exchange traded fund",
    "exchange-traded fund",
}

@dataclass(frozen=True)
class MarketPressureParams:
    universe: MarketPressureUniverse
    period: MarketPressurePeriod
    view: MarketPressureView
    warnings: list[str]


@dataclass(frozen=True)
class Identity:
    symbol: str
    company_name: str | None = None
    sector: str | None = None
    exchange: str | None = None
    market_cap: float | None = None


@dataclass(frozen=True)
class PricePerformance:
    change_pct: float | None
    start_at: str | None
    end_at: str | None
    as_of: str | None
    complete: bool
    market_cap: float | None = None


ConfirmationLoader = Callable[[Session, list[str]], dict[str, dict]]


def resolve_market_pressure_params(
    *,
    universe: str | None,
    period: str | None,
    view: str | None,
) -> MarketPressureParams:
    warnings: list[str] = []
    normalized_universe = (universe or "sp500").strip().lower().replace("-", "_")
    normalized_period = (period or "1d").strip().lower()
    normalized_view = (view or "market_pressure").strip().lower().replace("-", "_")
    if normalized_universe in {"etfs", "etf_fund", "funds"}:
        normalized_universe = "etf"

    if normalized_universe not in SUPPORTED_UNIVERSES:
        warnings.append(f"invalid_universe:{normalized_universe or 'blank'}")
        normalized_universe = "sp500"
    if normalized_period not in SUPPORTED_PERIODS:
        warnings.append(f"invalid_period:{normalized_period or 'blank'}")
        normalized_period = "1d"
    if normalized_view not in SUPPORTED_VIEWS:
        warnings.append(f"invalid_view:{normalized_view or 'blank'}")
        normalized_view = "market_pressure"

    return MarketPressureParams(
        universe=normalized_universe,  # type: ignore[arg-type]
        period=normalized_period,  # type: ignore[arg-type]
        view=normalized_view,  # type: ignore[arg-type]
        warnings=warnings,
    )


def market_pressure_capabilities(db: Session | None = None) -> dict[str, Any]:
    index_capabilities = index_universe_capabilities(db) if db is not None else {}
    sp500 = index_capabilities.get("sp500", {"supported": False, "status": "unavailable", "reason": "membership_not_loaded"})
    nasdaq100 = index_capabilities.get("nasdaq100", {"supported": False, "status": "unavailable", "reason": "membership_not_loaded"})
    etf_symbols = _resolve_etf_universe_symbols(db) if db is not None else []
    etf_capability = {
        "supported": bool(etf_symbols),
        "membershipCount": len(etf_symbols),
        "source": "security_master",
        "sourceKind": "security_asset_class",
        "sourceAsOf": None,
        "refreshedAt": None,
        "status": "available" if etf_symbols else "unavailable",
        "reason": None if etf_symbols else "etf_universe_not_loaded",
        "sourceLabel": "Walnut ETF securities",
    }
    return {
        "universes": {
            "sp500": bool(sp500.get("supported")),
            "nasdaq100": bool(nasdaq100.get("supported")),
            "etf": bool(etf_capability["supported"]),
            "all_us": False,
            "watchlist": True,
        },
        "universeDetails": {
            "sp500": sp500,
            "nasdaq100": nasdaq100,
            "etf": etf_capability,
            "all_us": {
                "supported": False,
                "membershipCount": 0,
                "source": None,
                "sourceAsOf": None,
                "refreshedAt": None,
                "status": "unavailable",
                "reason": "complete_us_equity_universe_not_available",
            },
            "watchlist": {
                "supported": True,
                "membershipCount": None,
                "source": "user_watchlist",
                "sourceAsOf": None,
                "refreshedAt": None,
                "status": "available",
                "reason": None,
            },
        },
        "views": {
            "market_pressure": True,
            "hidden_accumulation": True,
            "fragile_winners": True,
            "crowded_trades": False,
            "rotation": False,
        },
        "pressureTrendAvailable": False,
    }


def build_market_pressure_capabilities_response(
    db: Session,
    *,
    entitlements: dict[str, Any],
    user: UserAccount | None,
) -> dict[str, Any]:
    require_market_pressure_access(entitlements, user)
    return market_pressure_capabilities(db)


def build_market_pressure_response(
    db: Session,
    *,
    universe: str | None,
    period: str | None,
    view: str | None,
    entitlements: TierEntitlements,
    user: UserAccount | None = None,
    confirmation_loader: ConfirmationLoader | None = None,
) -> dict[str, Any]:
    require_market_pressure_access(entitlements, user)
    started = perf_counter()
    generated_at = datetime.now(timezone.utc)
    timings: dict[str, float] = {}
    sql_query_count = 0
    bind = db.get_bind()

    def count_sql(*_args: Any, **_kwargs: Any) -> None:
        nonlocal sql_query_count
        sql_query_count += 1

    event.listen(bind, "before_cursor_execute", count_sql)
    try:
        params = resolve_market_pressure_params(universe=universe, period=period, view=view)
        warnings = list(params.warnings)

        mark = perf_counter()
        capabilities = market_pressure_capabilities(db)
        timings["capabilitiesDurationMs"] = _elapsed_ms(mark)

        if not capabilities["views"][params.view]:
            warnings.append(f"unsupported_view:{params.view}")
            response = _empty_response(params, generated_at, entitlements, capabilities, warnings)
            return _attach_metadata(response, started=started, sql_query_count=sql_query_count, timings=timings)

        if not capabilities["universes"][params.universe]:
            warnings.append(f"unsupported_universe:{params.universe}")
            response = _empty_response(params, generated_at, entitlements, capabilities, warnings)
            return _attach_metadata(response, started=started, sql_query_count=sql_query_count, timings=timings)

        mark = perf_counter()
        symbols = _resolve_universe_symbols(db, params.universe, user)
        timings["membershipDurationMs"] = _elapsed_ms(mark)
        if not symbols:
            warnings.append("empty_universe")
            response = _empty_response(params, generated_at, entitlements, capabilities, warnings)
            return _attach_metadata(response, started=started, sql_query_count=sql_query_count, timings=timings)

        mark = perf_counter()
        snapshot_response = _snapshot_response(
            db,
            params=params,
            symbols=symbols,
            generated_at=generated_at,
            entitlements=entitlements,
            capabilities=capabilities,
            warnings=warnings,
        )
        timings["snapshotDurationMs"] = _elapsed_ms(mark)
        if snapshot_response is not None:
            logger.info(
                "market_pressure_request universe=%s period=%s view=%s tier=%s symbol_count=%s cache_hit=%s warning_count=%s",
                params.universe,
                params.period,
                params.view,
                entitlements.tier,
                len(symbols),
                True,
                len(warnings),
            )
            return _attach_metadata(snapshot_response, started=started, sql_query_count=sql_query_count, timings=timings)

        mark = perf_counter()
        identities = _load_identities(db, symbols)
        timings["identityDurationMs"] = _elapsed_ms(mark)
        mark = perf_counter()
        prioritized_symbols = _prioritize_symbols_for_market_data(symbols, identities)
        price_by_symbol = _load_price_performance(db, prioritized_symbols, params.period, generated_at.date(), identities)
        timings["priceDurationMs"] = _elapsed_ms(mark)
        loader = confirmation_loader or _default_confirmation_loader
        mark = perf_counter()
        canonical_bundles = loader(db, symbols)
        timings["confirmationDurationMs"] = _elapsed_ms(mark)

        mark = perf_counter()
        tiles: list[dict[str, Any]] = []
        for symbol in symbols:
            raw_bundle = canonical_bundles.get(symbol, {})
            price = price_by_symbol.get(symbol, PricePerformance(None, None, None, None, False))
            identity = identities.get(symbol, Identity(symbol=symbol))
            tile = _build_tile(symbol, identity, price, raw_bundle, generated_at)
            if _tile_matches_view(tile, params.view):
                tiles.append(tile)

        sectors = _group_tiles_by_sector(tiles)
        summary = _summary_for_tiles(tiles, len(symbols))
        audit = _audit_market_pressure_map(params, symbols, tiles, warnings)
        price_as_of = _latest_iso([tile.get("priceEndAt") for tile in tiles])
        confirmation_as_of = _latest_iso([tile.get("confirmationAsOf") for tile in tiles])
        timings["serializationDurationMs"] = _elapsed_ms(mark)

        duration_ms = round((perf_counter() - started) * 1000, 1)
        logger.info(
            "market_pressure_request universe=%s period=%s view=%s tier=%s symbol_count=%s classified_count=%s partial_count=%s unavailable_count=%s sql_query_count=%s duration_ms=%.1f cache_hit=%s warning_count=%s",
            params.universe,
            params.period,
            params.view,
            entitlements.tier,
            len(symbols),
            summary["classifiedCount"],
            summary["partialCount"],
            summary["unavailableCount"],
            sql_query_count,
            duration_ms,
            False,
            len(warnings),
        )
        response = {
            "universe": params.universe,
            "period": params.period,
            "view": params.view,
            "generatedAt": _dt_iso(generated_at),
            "priceAsOf": price_as_of,
            "confirmationAsOf": confirmation_as_of,
            "confirmationFreshnessWindowDays": CONFIRMATION_FRESHNESS_WINDOW_DAYS,
            "scoringVersion": SCORING_VERSION,
            "capabilities": capabilities,
            "entitlement": _entitlement_payload(entitlements),
            "summary": summary,
            "audit": audit,
            "sectors": sectors,
            "warnings": warnings,
            "metadata": {
                "cacheHit": False,
                "cacheScope": "request",
                "responseTimeMs": duration_ms,
                "priceCloseBasis": "price_cache.close",
            },
        }
        return _attach_metadata(response, started=started, sql_query_count=sql_query_count, timings=timings)
    finally:
        event.remove(bind, "before_cursor_execute", count_sql)


def _default_confirmation_loader(db: Session, symbols: list[str]) -> dict[str, dict]:
    return get_confirmation_score_bundles_for_tickers(
        db,
        symbols,
        lookback_days=CONFIRMATION_FRESHNESS_WINDOW_DAYS,
    )


def _snapshot_fresh_minutes() -> int:
    raw = os.getenv("MARKET_PRESSURE_SNAPSHOT_FRESH_MINUTES", "").strip()
    if not raw:
        return MARKET_PRESSURE_SNAPSHOT_FRESH_MINUTES
    try:
        return max(1, int(raw))
    except ValueError:
        return MARKET_PRESSURE_SNAPSHOT_FRESH_MINUTES


def _snapshot_min_coverage() -> float:
    raw = os.getenv("MARKET_PRESSURE_SNAPSHOT_MIN_COVERAGE", "").strip()
    if not raw:
        return MARKET_PRESSURE_SNAPSHOT_MIN_COVERAGE
    try:
        return min(1.0, max(0.0, float(raw)))
    except ValueError:
        return MARKET_PRESSURE_SNAPSHOT_MIN_COVERAGE


def _snapshot_response(
    db: Session,
    *,
    params: MarketPressureParams,
    symbols: list[str],
    generated_at: datetime,
    entitlements: TierEntitlements,
    capabilities: dict[str, Any],
    warnings: list[str],
) -> dict[str, Any] | None:
    if params.universe == "watchlist" or params.period != "1d":
        return None
    cutoff = generated_at - timedelta(minutes=_snapshot_fresh_minutes())
    rows = db.execute(
        select(MarketPressureSnapshot)
        .where(
            MarketPressureSnapshot.universe == params.universe,
            MarketPressureSnapshot.period == params.period,
            MarketPressureSnapshot.generated_at >= cutoff,
            MarketPressureSnapshot.symbol.in_(symbols),
        )
        .order_by(MarketPressureSnapshot.symbol.asc())
    ).scalars().all()
    required = max(1, int(len(symbols) * _snapshot_min_coverage()))
    if len(rows) < required:
        return None
    tiles: list[dict[str, Any]] = []
    for row in rows:
        try:
            tile = json.loads(row.tile_json)
        except (TypeError, ValueError):
            continue
        if isinstance(tile, dict) and _tile_matches_view(tile, params.view):
            tiles.append(tile)
    sectors = _group_tiles_by_sector(tiles)
    summary = _summary_for_tiles(tiles, len(symbols))
    audit = _audit_market_pressure_map(params, symbols, tiles, warnings)
    return {
        "universe": params.universe,
        "period": params.period,
        "view": params.view,
        "generatedAt": _dt_iso(max((row.generated_at for row in rows), default=generated_at)),
        "priceAsOf": _latest_iso([tile.get("priceEndAt") for tile in tiles]),
        "confirmationAsOf": _latest_iso([tile.get("confirmationAsOf") for tile in tiles]),
        "confirmationFreshnessWindowDays": CONFIRMATION_FRESHNESS_WINDOW_DAYS,
        "scoringVersion": SCORING_VERSION,
        "capabilities": capabilities,
        "entitlement": _entitlement_payload(entitlements),
        "summary": summary,
        "audit": audit,
        "sectors": sectors,
        "warnings": warnings,
        "metadata": {
            "cacheHit": True,
            "cacheScope": "market_pressure_snapshots",
            "responseTimeMs": 0,
            "priceCloseBasis": "market_pressure_snapshots",
        },
    }


def _audit_market_pressure_map(
    params: MarketPressureParams,
    symbols: list[str],
    tiles: list[dict[str, Any]],
    warnings: list[str],
) -> dict[str, Any]:
    expected_symbols = set(symbols)
    rendered_symbols = {normalize_symbol(str(tile.get("symbol") or "")) for tile in tiles}
    rendered_symbols.discard("")
    full_universe_view = params.view == "market_pressure"
    missing_symbols = sorted(expected_symbols - rendered_symbols) if full_universe_view else []
    important_symbols = [symbol for symbol in MARKET_PRESSURE_LIVE_QUOTE_PRIORITY if symbol in expected_symbols] if full_universe_view else []
    tiles_by_symbol = {normalize_symbol(str(tile.get("symbol") or "")): tile for tile in tiles}
    missing_important = [symbol for symbol in important_symbols if symbol not in rendered_symbols]
    missing_market_cap: list[str] = []
    low_market_cap: list[str] = []
    for symbol in important_symbols:
        tile = tiles_by_symbol.get(symbol)
        if not tile:
            continue
        market_cap = _safe_float(tile.get("marketCap"))
        if market_cap is None:
            missing_market_cap.append(symbol)
            continue
        floor = MARKET_PRESSURE_IMPORTANT_MARKET_CAP_FLOORS.get(symbol)
        if floor is not None and market_cap < floor:
            low_market_cap.append(symbol)

    status = "ok"
    if missing_important or missing_market_cap or low_market_cap:
        status = "fail"
    elif missing_symbols:
        status = "warn"

    if missing_symbols:
        warnings.append(f"market_pressure_audit:missing_symbols:{len(missing_symbols)}")
    if missing_important:
        warnings.append(f"market_pressure_audit:important_missing:{','.join(missing_important)}")
    if missing_market_cap:
        warnings.append(f"market_pressure_audit:important_market_cap_missing:{','.join(missing_market_cap)}")
    if low_market_cap:
        warnings.append(f"market_pressure_audit:important_market_cap_low:{','.join(low_market_cap)}")

    if status != "ok":
        logger.warning(
            "market_pressure_audit_failed universe=%s period=%s view=%s status=%s missing_symbols=%s important_missing=%s important_market_cap_missing=%s important_market_cap_low=%s",
            params.universe,
            params.period,
            params.view,
            status,
            len(missing_symbols),
            missing_important,
            missing_market_cap,
            low_market_cap,
        )

    return {
        "status": status,
        "expectedSymbolCount": len(expected_symbols),
        "renderedSymbolCount": len(rendered_symbols),
        "missingSymbolCount": len(missing_symbols),
        "missingSymbols": missing_symbols[:50],
        "importantSymbols": important_symbols,
        "importantMissingSymbols": missing_important,
        "importantMissingMarketCapSymbols": missing_market_cap,
        "importantLowMarketCapSymbols": low_market_cap,
    }


def require_market_pressure_access(entitlements: TierEntitlements, user: UserAccount | None) -> None:
    if user is None:
        raise HTTPException(status_code=401, detail="Authentication required")
    if bool(getattr(user, "is_suspended", False)):
        raise HTTPException(
            status_code=403,
            detail={"code": "pro_required", "message": "Market Pressure is available on the Pro plan."},
        )
    if entitlements.tier not in {"pro", "admin"}:
        raise HTTPException(
            status_code=403,
            detail={"code": "pro_required", "message": "Market Pressure is available on the Pro plan."},
        )


def _resolve_universe_symbols(db: Session, universe: MarketPressureUniverse, user: UserAccount | None) -> list[str]:
    if universe in {"sp500", "nasdaq100"}:
        snapshot = active_index_membership_snapshot(db, universe)
        return _apply_universe_symbol_policy(universe, snapshot.symbols) if snapshot.supported else []
    if universe == "etf":
        return _resolve_etf_universe_symbols(db)
    if universe != "watchlist":
        return []
    if user is None:
        raise HTTPException(status_code=401, detail="Authentication required")

    watchlist = db.execute(
        select(Watchlist)
        .where(Watchlist.owner_user_id == user.id)
        .order_by(Watchlist.id.asc())
        .limit(1)
    ).scalar_one_or_none()
    if watchlist is None:
        return []

    rows = db.execute(
        select(Security.symbol)
        .join(WatchlistItem, WatchlistItem.security_id == Security.id)
        .where(WatchlistItem.watchlist_id == watchlist.id)
        .order_by(Security.symbol.asc())
    ).scalars()
    return sorted({normalized for symbol in rows if (normalized := normalize_symbol(symbol))})


def _apply_universe_symbol_policy(universe: MarketPressureUniverse, symbols: list[str]) -> list[str]:
    excluded = MARKET_PRESSURE_UNIVERSE_SYMBOL_EXCLUSIONS.get(universe, set())
    if not excluded:
        return symbols
    return [symbol for symbol in symbols if symbol not in excluded]


def _resolve_etf_universe_symbols(db: Session) -> list[str]:
    asset_class = func.lower(func.coalesce(Security.asset_class, ""))
    security_name = func.lower(func.coalesce(Security.name, ""))
    rows = db.execute(
        select(Security.symbol)
        .where(
            Security.symbol.is_not(None),
            or_(
                asset_class.in_(ETF_ASSET_CLASS_VALUES),
                security_name.like("% etf%"),
                security_name.like("% exchange traded fund%"),
                security_name.like("% exchange-traded fund%"),
            ),
        )
        .order_by(Security.symbol.asc())
    ).scalars()
    return sorted({normalized for symbol in rows if (normalized := normalize_symbol(symbol))})


def _period_start_date(period: MarketPressurePeriod, today: date) -> date:
    if period == "5d":
        return today - timedelta(days=14)
    if period == "1m":
        return today - timedelta(days=45)
    if period == "3m":
        return today - timedelta(days=120)
    if period == "ytd":
        return date(today.year, 1, 1) - timedelta(days=7)
    if period == "1y":
        return today - timedelta(days=400)
    return today - timedelta(days=10)


def _period_target_date(period: MarketPressurePeriod, today: date) -> date:
    if period == "5d":
        return today - timedelta(days=7)
    if period == "1m":
        return today - timedelta(days=30)
    if period == "3m":
        return today - timedelta(days=90)
    if period == "ytd":
        return date(today.year, 1, 1)
    if period == "1y":
        return today - timedelta(days=365)
    return today - timedelta(days=1)


def _load_price_performance(
    db: Session,
    symbols: list[str],
    period: MarketPressurePeriod,
    today: date,
    identities: dict[str, Identity] | None = None,
) -> dict[str, PricePerformance]:
    if not symbols:
        return {}
    start_date = _period_start_date(period, today).isoformat()
    end_date = today.isoformat()
    rows = db.execute(
        select(PriceCache.symbol, PriceCache.date, PriceCache.close)
        .where(PriceCache.symbol.in_(symbols), PriceCache.date >= start_date, PriceCache.date <= end_date)
        .order_by(PriceCache.symbol.asc(), PriceCache.date.asc())
    ).all()
    grouped: dict[str, list[tuple[str, float]]] = {symbol: [] for symbol in symbols}
    for symbol, row_date, close in rows:
        normalized = normalize_symbol(symbol)
        if not normalized or close is None:
            continue
        grouped.setdefault(normalized, []).append((str(row_date), float(close)))

    target = _period_target_date(period, today).isoformat()
    results: dict[str, PricePerformance] = {}
    for symbol, values in grouped.items():
        results[symbol] = _price_performance_from_rows(values, period, target)
    if period == "1d":
        missing = [symbol for symbol in symbols if not results.get(symbol, PricePerformance(None, None, None, None, False)).complete]
        missing_cap = [
            symbol
            for symbol in symbols
            if identities is not None and identities.get(symbol, Identity(symbol=symbol)).market_cap is None
        ]
        live_symbols = _prioritize_live_quote_symbols(list(dict.fromkeys([*missing_cap, *missing])))
        live_results = _load_live_one_day_price_fallback(db, live_symbols)
        for symbol, live_price in live_results.items():
            current = results.get(symbol)
            if current is not None and current.complete:
                results[symbol] = PricePerformance(
                    change_pct=current.change_pct,
                    start_at=current.start_at,
                    end_at=current.end_at,
                    as_of=current.as_of,
                    complete=current.complete,
                    market_cap=current.market_cap or live_price.market_cap,
                )
                continue
            results[symbol] = live_price
    return results


def _market_pressure_live_price_limit() -> int:
    raw = os.getenv("MARKET_PRESSURE_LIVE_PRICE_LIMIT", "").strip()
    if not raw:
        return MARKET_PRESSURE_LIVE_PRICE_DEFAULT_LIMIT
    try:
        parsed = int(raw)
    except ValueError:
        return MARKET_PRESSURE_LIVE_PRICE_DEFAULT_LIMIT
    return max(0, min(parsed, 503))


def _prioritize_live_quote_symbols(symbols: list[str]) -> list[str]:
    priority = {symbol: index for index, symbol in enumerate(MARKET_PRESSURE_LIVE_QUOTE_PRIORITY)}
    return sorted(symbols, key=lambda symbol: (priority.get(symbol, len(priority)), symbol))


def _load_live_one_day_price_fallback(db: Session, symbols: list[str]) -> dict[str, PricePerformance]:
    limit = _market_pressure_live_price_limit()
    if not symbols or limit <= 0:
        return {}
    requested = symbols[:limit]
    try:
        quote_meta = get_current_prices_meta_db(
            db,
            requested,
            lane="market_pressure_quote",
            allow_live_user_fetch=True,
            stale_while_revalidate=True,
            force_quote_endpoint=True,
            skip_db_sanity=True,
            max_network_fetch=limit,
        )
    except Exception:
        logger.exception("market_pressure_live_price_fallback_failed symbols=%s", len(requested))
        return {}

    fallback: dict[str, PricePerformance] = {}
    for symbol, meta in quote_meta.items():
        change_pct = _safe_float(meta.get("change_percent"))
        market_cap = _safe_float(meta.get("market_cap"))
        if change_pct is None and market_cap is None:
            continue
        as_of = _quote_as_of_iso(meta.get("asof_ts")) or _dt_iso(datetime.now(timezone.utc))
        fallback[symbol] = PricePerformance(
            change_pct=round(change_pct, 4) if change_pct is not None else None,
            start_at=None,
            end_at=as_of,
            as_of=as_of,
            complete=change_pct is not None,
            market_cap=market_cap,
        )
    return fallback


def _price_performance_from_rows(
    rows: list[tuple[str, float]],
    period: MarketPressurePeriod,
    target_date: str,
) -> PricePerformance:
    if not rows:
        return PricePerformance(None, None, None, None, False)
    end_date, end_close = rows[-1]
    start: tuple[str, float] | None = None
    if period == "1d":
        start = rows[-2] if len(rows) >= 2 else None
    else:
        prior_rows = [row for row in rows if row[0] <= target_date]
        start = prior_rows[-1] if prior_rows else None
    if start is None or start[1] == 0:
        return PricePerformance(None, None, _date_iso(end_date), _date_iso(end_date), False)
    change = ((end_close - start[1]) / start[1]) * 100
    return PricePerformance(round(change, 4), _date_iso(start[0]), _date_iso(end_date), _date_iso(end_date), True)


def _load_identities(db: Session, symbols: list[str]) -> dict[str, Identity]:
    identities: dict[str, Identity] = {symbol: Identity(symbol=symbol) for symbol in symbols}
    for row in db.execute(select(Security.symbol, Security.name, Security.sector).where(Security.symbol.in_(symbols))).all():
        symbol = normalize_symbol(row.symbol)
        if not symbol:
            continue
        current = identities.get(symbol, Identity(symbol=symbol))
        identities[symbol] = Identity(symbol=symbol, company_name=row.name, sector=row.sector, exchange=current.exchange, market_cap=current.market_cap)

    for row in db.execute(
        select(TickerMeta.symbol, TickerMeta.company_name, TickerMeta.exchange, TickerMeta.sector)
        .where(TickerMeta.symbol.in_(symbols))
    ).all():
        symbol = normalize_symbol(row.symbol)
        if not symbol:
            continue
        current = identities.get(symbol, Identity(symbol=symbol))
        identities[symbol] = Identity(
            symbol=symbol,
            company_name=row.company_name or current.company_name,
            sector=row.sector or current.sector,
            exchange=row.exchange or current.exchange,
            market_cap=current.market_cap,
        )

    for row in db.execute(
        select(
            FundamentalsCache.symbol,
            FundamentalsCache.company_name,
            FundamentalsCache.exchange,
            FundamentalsCache.sector,
            FundamentalsCache.market_cap,
        ).where(FundamentalsCache.symbol.in_(symbols), FundamentalsCache.status == "ok")
    ).all():
        symbol = normalize_symbol(row.symbol)
        if not symbol:
            continue
        current = identities.get(symbol, Identity(symbol=symbol))
        identities[symbol] = Identity(
            symbol=symbol,
            company_name=current.company_name or row.company_name,
            sector=current.sector or row.sector,
            exchange=current.exchange or row.exchange,
            market_cap=_safe_float(row.market_cap) or current.market_cap,
        )
    for row in db.execute(
        select(QuoteCache.symbol, QuoteCache.market_cap)
        .where(QuoteCache.symbol.in_(symbols), QuoteCache.market_cap.is_not(None))
    ).all():
        symbol = normalize_symbol(row.symbol)
        market_cap = _safe_float(row.market_cap)
        if not symbol or market_cap is None:
            continue
        current = identities.get(symbol, Identity(symbol=symbol))
        identities[symbol] = Identity(
            symbol=symbol,
            company_name=current.company_name,
            sector=current.sector,
            exchange=current.exchange,
            market_cap=current.market_cap or market_cap,
        )
    return identities


def _prioritize_symbols_for_market_data(symbols: list[str], identities: dict[str, Identity]) -> list[str]:
    return sorted(
        symbols,
        key=lambda symbol: (
            -float(identities.get(symbol, Identity(symbol=symbol)).market_cap or 0),
            symbol,
        ),
    )


def _entitlement_payload(entitlements: TierEntitlements) -> dict[str, Any]:
    return {
        "tier": entitlements.tier,
        "visibleLayers": [SOURCE_LAYER_KEYS[key] for key in SOURCE_ORDER],
        "lockedLayers": [],
    }


def _build_tile(
    symbol: str,
    identity: Identity,
    price: PricePerformance,
    bundle: dict[str, Any],
    generated_at: datetime,
) -> dict[str, Any]:
    score = _safe_int(bundle.get("score"))
    band = bundle.get("band") if bundle.get("band") in {"inactive", "weak", "moderate", "strong", "exceptional"} else confirmation_band_for_score(score or 0)
    present_sources = _present_source_count(bundle)
    directional_sources = confirmation_active_source_count(bundle)
    direction = _market_direction(bundle, score, present_sources)
    strength = _confirmation_strength(band, direction)
    layers = _layers_payload(bundle, generated_at)
    stale = any(layer.get("status") == "stale" for layer in layers.values() if isinstance(layer, dict))
    divergence = _divergence(price.change_pct, direction, strength, directional_sources, stale)
    data_state = _data_state(price, direction, stale, layers)
    confirmation_as_of = _latest_iso([layer.get("asOf") for layer in layers.values() if isinstance(layer, dict)])
    market_cap = identity.market_cap or price.market_cap
    return {
        "symbol": symbol,
        "companyName": identity.company_name,
        "sector": identity.sector or "Unclassified",
        "exchange": identity.exchange,
        "marketCap": market_cap,
        "priceChangePct": price.change_pct,
        "priceStartAt": price.start_at,
        "priceEndAt": price.end_at,
        "confirmationScore": score if direction != "unavailable" else None,
        "confirmationDirection": direction,
        "confirmationStrength": strength,
        "confirmationTrend": None,
        "divergence": divergence,
        "confirmationAsOf": confirmation_as_of,
        "latestEvidenceAt": confirmation_as_of,
        "dataState": data_state,
        "availableLayerCount": sum(1 for layer in layers.values() if isinstance(layer, dict) and layer.get("status") in {"available", "stale"}),
        "eligibleLayerCount": len(SOURCE_ORDER),
        "layers": layers,
    }


def _present_source_count(bundle: dict[str, Any]) -> int:
    sources = bundle.get("sources") if isinstance(bundle, dict) else None
    if not isinstance(sources, dict):
        return 0
    return sum(1 for source in sources.values() if isinstance(source, dict) and source.get("present") is True)


def _market_direction(bundle: dict[str, Any], score: int | None, present_sources: int) -> MarketPressureDirection:
    if not bundle or present_sources <= 0 or score is None or score <= 19:
        return "unavailable"
    raw_direction = bundle.get("direction")
    if raw_direction == "mixed":
        return "conflicted"
    if raw_direction in {"bullish", "bearish"} and score >= 40:
        return raw_direction
    if raw_direction == "neutral" or score < 40:
        return "neutral"
    return "unavailable"


def _confirmation_strength(band: str | None, direction: MarketPressureDirection) -> str | None:
    if direction == "unavailable" or band in {None, "inactive"}:
        return None
    if band == "weak":
        return "weak"
    if band == "moderate":
        return "moderate"
    if band in {"strong", "exceptional"}:
        return "strong"
    return None


def _divergence(
    price_change_pct: float | None,
    direction: MarketPressureDirection,
    strength: str | None,
    active_sources: int,
    stale: bool,
) -> Divergence:
    if price_change_pct is None or direction == "unavailable" or stale:
        return "unavailable"
    if direction == "conflicted":
        return "conflicted"
    sufficient = active_sources >= 2 and strength in {"moderate", "strong"}
    if price_change_pct <= 0 and direction == "bullish" and sufficient:
        return "hidden_accumulation"
    if price_change_pct > 0 and direction in {"bearish", "conflicted"} and sufficient:
        return "fragile_winner"
    if price_change_pct > 0 and direction == "bullish":
        return "aligned_bullish"
    if price_change_pct < 0 and direction == "bearish":
        return "aligned_bearish"
    return "none"


def _layers_payload(bundle: dict[str, Any], generated_at: datetime) -> dict[str, dict[str, Any]]:
    raw_sources = bundle.get("sources") if isinstance(bundle.get("sources"), dict) else {}
    layers: dict[str, dict[str, Any]] = {}
    for source_key in SOURCE_ORDER:
        layer_key = SOURCE_LAYER_KEYS[source_key]
        source = raw_sources.get(source_key) if isinstance(raw_sources.get(source_key), dict) else {}
        if source.get("present") is not True:
            layers[layer_key] = {
                "status": "unavailable",
                "direction": None,
                "contribution": None,
                "asOf": None,
            }
            continue
        freshness_days = source.get("freshness_days")
        status = "available"
        as_of = None
        if isinstance(freshness_days, int):
            as_of = _dt_iso(generated_at - timedelta(days=max(0, freshness_days)))
            if freshness_days > CONFIRMATION_FRESHNESS_WINDOW_DAYS:
                status = "stale"
        direction = source.get("direction")
        layers[layer_key] = {
            "status": status,
            "direction": _layer_direction(direction),
            "contribution": None,
            "asOf": as_of,
        }
    return layers


def _layer_direction(value: Any) -> str | None:
    if value == "mixed":
        return "conflicted"
    if value in {"bullish", "bearish", "neutral"}:
        return value
    return None


def _data_state(
    price: PricePerformance,
    direction: MarketPressureDirection,
    stale: bool,
    layers: dict[str, dict[str, Any]],
) -> str:
    if stale:
        return "stale"
    if not price.complete and direction == "unavailable":
        return "unavailable"
    if not price.complete or any(layer.get("status") == "unavailable" for layer in layers.values()):
        return "partial"
    return "complete"


def _tile_matches_view(tile: dict[str, Any], view: MarketPressureView) -> bool:
    if view == "hidden_accumulation":
        return tile.get("divergence") == "hidden_accumulation"
    if view == "fragile_winners":
        return tile.get("divergence") == "fragile_winner"
    return True


def _group_tiles_by_sector(tiles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for tile in tiles:
        sector = str(tile.get("sector") or "Unclassified")
        grouped.setdefault(sector, []).append(tile)
    sectors = []
    for sector, sector_tiles in grouped.items():
        sorted_tiles = sorted(sector_tiles, key=_tile_sort_key)
        sectors.append(
            {
                "sector": sector,
                "summary": _sector_summary(sorted_tiles),
                "tiles": sorted_tiles,
            }
        )
    return sorted(sectors, key=lambda item: (item["sector"] == "Unclassified", item["sector"]))


def _tile_sort_key(tile: dict[str, Any]) -> tuple[int, int, float, str]:
    divergence_rank = {
        "hidden_accumulation": 0,
        "fragile_winner": 1,
        "conflicted": 2,
        "aligned_bullish": 3,
        "aligned_bearish": 4,
        "none": 5,
        "unavailable": 6,
    }.get(str(tile.get("divergence")), 9)
    strength_rank = {"strong": 0, "moderate": 1, "weak": 2, None: 3}.get(tile.get("confirmationStrength"), 3)
    price_move = tile.get("priceChangePct")
    abs_move = -abs(float(price_move)) if isinstance(price_move, (int, float)) else 0.0
    return (divergence_rank, strength_rank, abs_move, str(tile.get("symbol") or ""))


def _summary_for_tiles(tiles: list[dict[str, Any]], symbol_count: int) -> dict[str, int]:
    classified = [tile for tile in tiles if tile.get("confirmationDirection") != "unavailable"]
    return {
        "symbolCount": symbol_count,
        "classifiedCount": len(classified),
        "partialCount": sum(1 for tile in tiles if tile.get("dataState") == "partial"),
        "unavailableCount": sum(1 for tile in tiles if tile.get("confirmationDirection") == "unavailable"),
        "bullishCount": sum(1 for tile in tiles if tile.get("confirmationDirection") == "bullish"),
        "bearishCount": sum(1 for tile in tiles if tile.get("confirmationDirection") == "bearish"),
        "neutralCount": sum(1 for tile in tiles if tile.get("confirmationDirection") == "neutral"),
        "conflictedCount": sum(1 for tile in tiles if tile.get("confirmationDirection") == "conflicted"),
        "hiddenAccumulationCount": sum(1 for tile in tiles if tile.get("divergence") == "hidden_accumulation"),
        "fragileWinnerCount": sum(1 for tile in tiles if tile.get("divergence") == "fragile_winner"),
    }


def _sector_summary(tiles: list[dict[str, Any]]) -> dict[str, Any]:
    price_values = [
        float(tile["priceChangePct"])
        for tile in tiles
        if isinstance(tile.get("priceChangePct"), (int, float))
    ]
    return {
        "symbolCount": len(tiles),
        "averagePriceChangePct": round(sum(price_values) / len(price_values), 4) if price_values else None,
        "bullishCount": sum(1 for tile in tiles if tile.get("confirmationDirection") == "bullish"),
        "bearishCount": sum(1 for tile in tiles if tile.get("confirmationDirection") == "bearish"),
        "conflictedCount": sum(1 for tile in tiles if tile.get("confirmationDirection") == "conflicted"),
        "divergenceCount": sum(
            1
            for tile in tiles
            if tile.get("divergence") in {"hidden_accumulation", "fragile_winner", "conflicted"}
        ),
    }


def _empty_response(
    params: MarketPressureParams,
    generated_at: datetime,
    entitlements: TierEntitlements,
    capabilities: dict[str, Any],
    warnings: list[str],
) -> dict[str, Any]:
    return {
        "universe": params.universe,
        "period": params.period,
        "view": params.view,
        "generatedAt": _dt_iso(generated_at),
        "priceAsOf": None,
        "confirmationAsOf": None,
        "confirmationFreshnessWindowDays": CONFIRMATION_FRESHNESS_WINDOW_DAYS,
        "scoringVersion": SCORING_VERSION,
        "capabilities": capabilities,
        "entitlement": _entitlement_payload(entitlements),
        "summary": _summary_for_tiles([], 0),
        "sectors": [],
        "warnings": warnings,
        "metadata": {"cacheHit": False, "cacheScope": "request", "responseTimeMs": 0, "priceCloseBasis": "price_cache.close"},
    }


def _attach_metadata(
    response: dict[str, Any],
    *,
    started: float,
    sql_query_count: int,
    timings: dict[str, float],
) -> dict[str, Any]:
    duration_ms = round((perf_counter() - started) * 1000, 1)
    response.pop("metadata", None)
    payload_bytes = len(json.dumps(response, separators=(",", ":"), default=str).encode("utf-8"))
    logger.info(
        "market_pressure_internal_metrics sql_query_count=%s response_time_ms=%.1f payload_bytes=%s timings=%s",
        sql_query_count,
        duration_ms,
        payload_bytes,
        timings,
    )
    return response


def _elapsed_ms(started: float) -> float:
    return round((perf_counter() - started) * 1000, 1)


def _safe_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _safe_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if parsed != parsed:
        return None
    return parsed


def _quote_as_of_iso(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value).strip()
        if not text:
            return None
        try:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return text
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return _dt_iso(dt)


def _date_iso(value: str) -> str:
    return f"{value}T00:00:00Z"


def _dt_iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _latest_iso(values: list[Any]) -> str | None:
    strings = [str(value) for value in values if isinstance(value, str) and value]
    return max(strings) if strings else None
