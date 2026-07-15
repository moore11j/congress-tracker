from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from time import perf_counter
from typing import Any, Callable, Literal

from fastapi import HTTPException
from sqlalchemy import event, select
from sqlalchemy.orm import Session

from app.entitlements import TierEntitlements
from app.models import FundamentalsCache, PriceCache, Security, TickerMeta, UserAccount, Watchlist, WatchlistItem
from app.services.confirmation_score import (
    SOURCE_ORDER,
    confirmation_active_source_count,
    confirmation_band_for_score,
    get_confirmation_score_bundles_for_tickers,
)
from app.services.index_memberships import active_index_membership_snapshot, index_universe_capabilities
from app.utils.symbols import normalize_symbol

logger = logging.getLogger(__name__)

MarketPressurePeriod = Literal["1d", "5d", "1m", "3m", "ytd", "1y"]
MarketPressureUniverse = Literal["sp500", "nasdaq100", "all_us", "watchlist"]
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
SUPPORTED_PERIODS: tuple[MarketPressurePeriod, ...] = ("1d", "5d", "1m", "3m", "ytd", "1y")
SUPPORTED_UNIVERSES: tuple[MarketPressureUniverse, ...] = ("sp500", "nasdaq100", "all_us", "watchlist")
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


@dataclass(frozen=True)
class PricePerformance:
    change_pct: float | None
    start_at: str | None
    end_at: str | None
    as_of: str | None
    complete: bool


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
    return {
        "universes": {
            "sp500": bool(sp500.get("supported")),
            "nasdaq100": bool(nasdaq100.get("supported")),
            "all_us": False,
            "watchlist": True,
        },
        "universeDetails": {
            "sp500": sp500,
            "nasdaq100": nasdaq100,
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
        price_by_symbol = _load_price_performance(db, symbols, params.period, generated_at.date())
        timings["priceDurationMs"] = _elapsed_ms(mark)
        mark = perf_counter()
        identities = _load_identities(db, symbols)
        timings["identityDurationMs"] = _elapsed_ms(mark)
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
        return snapshot.symbols if snapshot.supported else []
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
    return results


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
        identities[symbol] = Identity(symbol=symbol, company_name=row.name, sector=row.sector)

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
        )

    for row in db.execute(
        select(
            FundamentalsCache.symbol,
            FundamentalsCache.company_name,
            FundamentalsCache.exchange,
            FundamentalsCache.sector,
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
        )
    return identities


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
    return {
        "symbol": symbol,
        "companyName": identity.company_name,
        "sector": identity.sector or "Unclassified",
        "exchange": identity.exchange,
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
    metadata = response.setdefault("metadata", {})
    metadata.update(timings)
    metadata["sqlQueryCount"] = sql_query_count
    metadata["responseTimeMs"] = round((perf_counter() - started) * 1000, 1)
    metadata["payloadBytes"] = len(json.dumps(response, separators=(",", ":"), default=str).encode("utf-8"))
    return response


def _elapsed_ms(started: float) -> float:
    return round((perf_counter() - started) * 1000, 1)


def _safe_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _date_iso(value: str) -> str:
    return f"{value}T00:00:00Z"


def _dt_iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _latest_iso(values: list[Any]) -> str | None:
    strings = [str(value) for value in values if isinstance(value, str) and value]
    return max(strings) if strings else None
