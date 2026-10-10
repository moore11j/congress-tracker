from __future__ import annotations

import json
import logging
from copy import deepcopy
from datetime import datetime, timezone
from math import isfinite
from typing import Any

from sqlalchemy import JSON, cast, select, type_coerce
from sqlalchemy.orm import Session

from app.models import LeaderboardSnapshot, TickerContextBundleCache
from app.services.confirmation_context import TICKER_CONFIRMATION_CONTEXT_VERSION, build_ticker_confirmation_context
from app.services.confirmation_score import CONFIRMATION_SCORING_VERSION, SOURCE_LABELS
from app.services.fundamentals_cache import cached_screener_rows
from app.services.leaderboard_market_data import enrich_leaderboard_market_data
from app.services.screener import ScreenerParams, matches_confirmation_filters
from app.services.top_ideas_context import load_idea_context
from app.utils.symbols import classify_symbol

logger = logging.getLogger(__name__)
TOP_STOCKS_LEADERBOARD_KEY = "top_stocks"
TOP_STOCKS_SCORE_BATCH_SIZE = 100
TOP_STOCKS_PARAMS = ScreenerParams(
    page=1,
    page_size=10,
    sort="confirmation_score",
    sort_dir="desc",
    lookback_days=30,
    confirmation_score_min=20,
    confirmation_direction="bullish",
    confirmation_band=None,
)

TOP_STOCKS_FILTERS = {
    "all": "All Stocks",
    "us": "US",
    "large_cap": "Large Cap",
    "mid_cap": "Mid Cap",
    "small_cap": "Small Cap",
    "tech": "Tech",
    "healthcare": "Healthcare",
    "financials": "Financials",
}


def build_top_stocks_response(db: Session, *, entitlements=None) -> dict[str, Any]:
    """Read prepared scores, using newer canonical ticker caches when available.

    No scoring, provider hydration, or database writes occur on page loads.
    Keep the entire candidate universe so a refreshed score can enter or leave
    the top ten and every filter is ranked from the same evidence.
    """
    snapshot = db.execute(
        select(LeaderboardSnapshot).where(LeaderboardSnapshot.leaderboard_key == TOP_STOCKS_LEADERBOARD_KEY)
    ).scalar_one_or_none()
    if snapshot is None:
        return _empty_response()
    payload = _payload(snapshot.payload_json)
    if payload is None:
        return _empty_response()
    if payload.get("score_context_version") != TICKER_CONFIRMATION_CONTEXT_VERSION:
        return _empty_response()
    candidates = payload.pop("candidate_rows", None)
    if not isinstance(candidates, list):
        # Old snapshots have neither the common score inputs nor a complete
        # candidate universe. Do not present their incompatible scores.
        return _empty_response()
    symbols = [row["symbol"] for row in candidates]
    from app.main import _TICKER_CONTEXT_BUNDLE_VERSION, _redact_locked_ticker_confirmation_sources, _ticker_context_source_entitlements

    json_payload = (
        cast(TickerContextBundleCache.payload_json, JSON)
        if db.get_bind().dialect.name == "postgresql"
        else type_coerce(TickerContextBundleCache.payload_json, JSON)
    )
    caches = db.execute(
        select(
            TickerContextBundleCache.symbol,
            TickerContextBundleCache.generated_at,
            json_payload["confirmation_score_bundle"].label("bundle"),
        )
        .where(TickerContextBundleCache.symbol.in_(symbols))
        .where(TickerContextBundleCache.user_segment == "canonical")
        .where(TickerContextBundleCache.cache_key.like(f"ticker-context-bundle:v{_TICKER_CONTEXT_BUNDLE_VERSION}:%:30:all:3:canonical"))
        .where(TickerContextBundleCache.expires_at > datetime.now(timezone.utc))
        .order_by(TickerContextBundleCache.generated_at.desc())
    ).all() if symbols else []
    latest = {}
    for cache in caches:
        bundle = cache.bundle
        if (cache.symbol not in latest and isinstance(bundle, dict) and bundle.get("lookback_days") == 30
                and bundle.get("scoring_version") == CONFIRMATION_SCORING_VERSION):
            latest[cache.symbol] = (bundle, _iso(cache.generated_at))
    source_entitlements = _ticker_context_source_entitlements(entitlements) if entitlements is not None else None
    for row in candidates:
        bundle = row.pop("confirmation_bundle", {})
        update = latest.get(row["symbol"])
        # An unexpired ticker cache is exactly what the ticker page displays,
        # even when the daily discovery job ran after that cache was built.
        if update:
            bundle, row["updated_at"] = update
        if not isinstance(bundle, dict) or bundle.get("scoring_version") != CONFIRMATION_SCORING_VERSION:
            # Do not rank an incomplete universe or mix provider methodologies.
            return {**_empty_response(), "empty_message": "Stock rankings are being refreshed. Check back shortly."}
        row["confirmation"] = bundle
        if source_entitlements is not None and entitlements.has_feature("ticker_confirmation"):
            row["visible_confirmation"] = _redact_locked_ticker_confirmation_sources(bundle, source_entitlements)
    # Rank the canonical evidence once. Tier projection must never change a
    # stock's true position; only the inspectable evidence changes by plan.
    return _ranked_payload(candidates, generated_at=payload.get("generated_at"))


def refresh_top_stocks_leaderboard(db: Session, *, now: datetime | None = None) -> dict[str, Any]:
    """Score the entire cached stock universe with the exact ticker calculation.

    The public API/page never invokes this builder: score assembly, cached source
    reads, and any enrichment work remain confined to the scheduled job. The
    interactive screener's result limit must not truncate candidates before
    scoring: missing market caps would select mostly early-alphabet tickers.
    """
    generated_at = _utc(now or datetime.now(timezone.utc))
    rows = []
    for original in cached_screener_rows(db):
        status, symbol, _ = classify_symbol(original["symbol"])
        if status == "eligible":
            rows.append({**original, "symbol": symbol})
    candidates = []
    for offset in range(0, len(rows), TOP_STOCKS_SCORE_BATCH_SIZE):
        batch = rows[offset:offset + TOP_STOCKS_SCORE_BATCH_SIZE]
        symbols = [row["symbol"] for row in batch]
        bundles = build_ticker_confirmation_context(db, symbols)["bundles"]
        idea_context = load_idea_context(db, symbols, generated_at)
        for original in batch:
            row = deepcopy(original)
            # A failed batch must leave the last complete snapshot intact.
            bundle = bundles.get(row["symbol"])
            if not isinstance(bundle, dict) or bundle.get("inputs_incomplete"):
                raise ValueError(f"Missing ticker confirmation for {row['symbol']}")
            row["confirmation"] = bundle
            row["confirmation_bundle"] = bundle
            row["ranking_context"] = idea_context.get(row["symbol"], {})
            row["updated_at"] = _iso(generated_at)
            candidates.append(row)
        logger.info("top_stocks_scoring_progress scored=%s total=%s", len(candidates), len(rows))
    enrich_leaderboard_market_data(db, candidates, now=generated_at)
    payload = _ranked_payload(candidates, generated_at=_iso(generated_at))
    stored_payload = {**payload, "candidate_rows": candidates, "score_context_version": TICKER_CONFIRMATION_CONTEXT_VERSION}
    snapshot = db.execute(
        select(LeaderboardSnapshot).where(LeaderboardSnapshot.leaderboard_key == TOP_STOCKS_LEADERBOARD_KEY)
    ).scalar_one_or_none()
    serialized = json.dumps(stored_payload, separators=(",", ":"), sort_keys=True)
    if snapshot is None:
        db.add(LeaderboardSnapshot(leaderboard_key=TOP_STOCKS_LEADERBOARD_KEY, generated_at=generated_at, payload_json=serialized))
    else:
        snapshot.generated_at = generated_at
        snapshot.payload_json = serialized
    db.commit()
    return payload


def _ranked_payload(candidates: list[dict[str, Any]], *, generated_at: str | None) -> dict[str, Any]:
    rows = [row for row in candidates if matches_confirmation_filters(row, TOP_STOCKS_PARAMS)
            and (row["confirmation"].get("score_calculation") or {}).get("aligned_source_count", row["confirmation"].get("source_count", 0)) >= 2]
    rows.sort(key=_ranking_key)
    filter_rows = {
        key: [
            _item_from_screener_row(row, rank=index, updated_at=row.get("updated_at") or generated_at)
            for index, row in enumerate(_rows_for_filter(rows, key)[:25], start=1)
        ]
        for key in TOP_STOCKS_FILTERS
    }
    top_rows = filter_rows["all"]
    return {
        "items": top_rows,
        "filter_items": filter_rows,
        "filters": TOP_STOCKS_FILTERS,
        "returned": len(top_rows),
        "generated_at": max((row.get("updated_at") or generated_at or "" for row in candidates), default=generated_at),
        "universe_generated_at": generated_at,
        "source": "canonical_ticker_confirmation_cache",
        "qualification": _qualification(),
    }


def _item_from_screener_row(
    row: dict[str, Any],
    *,
    rank: int,
    updated_at: str,
) -> dict[str, Any]:
    canonical = row.get("confirmation") if isinstance(row.get("confirmation"), dict) else {}
    confirmation = row.get("visible_confirmation", canonical)
    drivers = _drivers_from_screener_row({**row, "confirmation": canonical})
    context = row.get("ranking_context") or {}
    strong = canonical.get("score", 0) >= 60
    reason = ("Strong multi-source confirmation" if strong else "Developing multi-source bullish confirmation") if len(drivers) >= 3 else ("Strong confirmation in available evidence" if strong else "Developing bullish confirmation")
    if context.get("insider_cluster_count", 0) >= 2:
        drivers.append("Insider clusters")
        reason = "Insider buying cluster with strong confirmation" if strong else "Insider buying cluster with bullish confirmation"
        if _bullish(canonical, "congress"):
            drivers.append("Congress + insider clusters")
            reason = "Insider cluster with Congress confirmation"
    if context.get("strategy_entries", 0):
        drivers.append("Strategy entries")
    sources = confirmation.get("sources") or {}
    symbol = str(row.get("symbol") or "").strip().upper()
    return {
        "rank": rank,
        "symbol": symbol,
        "company_name": str(row.get("company_name") or symbol),
        "confirmation_score": confirmation.get("score"),
        "confirmation_band": confirmation.get("band") or "inactive",
        "confirmation_direction": confirmation.get("direction") or "neutral",
        "confirmation_coverage": {key: (canonical.get("score_calculation") or {}).get(key) for key in ("aligned_source_count", "source_count")},
        "price": row.get("price"),
        "market_cap": row.get("market_cap"),
        "avg_volume": row.get("avg_volume"),
        "market_cap_as_of": row.get("market_cap_as_of"),
        "avg_volume_as_of": row.get("avg_volume_as_of"),
        "sector": row.get("sector"),
        "country": row.get("country"),
        "key_drivers": drivers,
        "why_ranked": reason,
        "why_this_ranked": [
            {"source": SOURCE_LABELS[key], "direction": source.get("direction"),
             "summary": source.get("summary") or source.get("detail") or source.get("label"), "freshness_days": source.get("freshness_days")}
            for key, source in sources.items() if key in SOURCE_LABELS and isinstance(source, dict) and source.get("present") is True
        ],
        "updated_at": updated_at,
        "ticker_url": str(row.get("ticker_url") or f"/ticker/{symbol}"),
    }


def _bullish(bundle, source):
    data = (bundle.get("sources") or {}).get(source) or {}
    return data.get("present") is True and data.get("direction") == "bullish"


def _ranking_key(row):
    """Score, market cap, and average volume descending; exact ties use A–Z.

    Unavailable market data sorts after positive values within the same score.
    """
    return (-row["confirmation"].get("score", 0), -_market_cap(row),
            -_positive_number(row.get("avg_volume")), row["symbol"])


def _rows_for_filter(rows: list[dict[str, Any]], filter_key: str) -> list[dict[str, Any]]:
    """Filter the already-built daily screener universe without any request work."""
    if filter_key == "all":
        return rows
    if filter_key == "us":
        return [row for row in rows if _is_us_stock(row)]
    if filter_key == "large_cap":
        return [row for row in rows if _market_cap(row) >= 10_000_000_000]
    if filter_key == "mid_cap":
        return [row for row in rows if 2_000_000_000 <= _market_cap(row) < 10_000_000_000]
    if filter_key == "small_cap":
        return [row for row in rows if 300_000_000 <= _market_cap(row) < 2_000_000_000]
    if filter_key == "tech":
        return [row for row in rows if "technology" in _sector(row) or "tech" in _sector(row)]
    if filter_key == "healthcare":
        return [row for row in rows if "health" in _sector(row)]
    if filter_key == "financials":
        return [row for row in rows if "financial" in _sector(row)]
    return []


def _market_cap(row: dict[str, Any]) -> float:
    return _positive_number(row.get("market_cap"))


def _positive_number(value: Any) -> float:
    return float(value) if type(value) in (int, float) and isfinite(value) and value > 0 else 0.0


def _sector(row: dict[str, Any]) -> str:
    return str(row.get("sector") or "").strip().lower()


def _is_us_stock(row: dict[str, Any]) -> bool:
    country = str(row.get("country") or "").strip().lower().replace(".", "")
    return country in {"united states", "united states of america", "us", "usa", "u s", "u s a"}


def _empty_response() -> dict[str, Any]:
    return {
        "items": [],
        "filter_items": {key: [] for key in TOP_STOCKS_FILTERS},
        "filters": TOP_STOCKS_FILTERS,
        "returned": 0,
        "generated_at": None,
        "source": "canonical_ticker_confirmation_cache",
        "qualification": _qualification(),
    }


def _qualification() -> dict[str, Any]:
    return {
        "confirmation_score_min": TOP_STOCKS_PARAMS.confirmation_score_min,
        "confirmation_direction": TOP_STOCKS_PARAMS.confirmation_direction,
        "confirmation_band": TOP_STOCKS_PARAMS.confirmation_band,
        "aligned_sources_min": 2,
        "lookback_days": TOP_STOCKS_PARAMS.lookback_days,
    }


def _drivers_from_screener_row(row: dict[str, Any]) -> list[str]:
    """Drivers must describe the same tier-visible evidence as the score."""
    confirmation = row.get("confirmation") or {}
    sources = confirmation.get("sources") or {}
    drivers = [
        label for key, label in SOURCE_LABELS.items()
        if isinstance(sources.get(key), dict)
        and sources[key].get("present") is True
        and sources[key].get("direction") == confirmation.get("direction")
    ]
    return drivers[:4] or ["Confirmation Score"]


def _payload(raw: str | None) -> dict[str, Any] | None:
    try:
        parsed = json.loads(raw or "{}")
    except (TypeError, ValueError):
        return None
    return parsed if isinstance(parsed, dict) else None


def _utc(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


def _iso(value: datetime) -> str:
    return _utc(value).astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
