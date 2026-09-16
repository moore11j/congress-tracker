from __future__ import annotations

import json
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import JSON, cast, select, type_coerce
from sqlalchemy.orm import Session

from app.models import LeaderboardSnapshot, TickerContextBundleCache
from app.services.confirmation_context import TICKER_CONFIRMATION_CONTEXT_VERSION, build_ticker_confirmation_context
from app.services.confirmation_score import SOURCE_LABELS
from app.services.screener import MAX_FETCH_ROWS, ScreenerParams, build_screener_rows, matches_confirmation_filters

TOP_STOCKS_LEADERBOARD_KEY = "top_stocks"
TOP_STOCKS_PARAMS = ScreenerParams(
    page=1,
    page_size=10,
    sort="confirmation_score",
    sort_dir="desc",
    lookback_days=30,
    confirmation_score_min=60,
    confirmation_direction="bullish",
    confirmation_band="strong_plus",
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
        if cache.symbol not in latest and isinstance(bundle, dict) and bundle.get("lookback_days") == 30:
            latest[cache.symbol] = (bundle, _iso(cache.generated_at))
    source_entitlements = _ticker_context_source_entitlements(entitlements) if entitlements is not None else None
    for row in candidates:
        bundle = row.pop("confirmation_bundle", {})
        update = latest.get(row["symbol"])
        # An unexpired ticker cache is exactly what the ticker page displays,
        # even when the daily discovery job ran after that cache was built.
        if update:
            bundle, row["updated_at"] = update
        if source_entitlements is not None:
            bundle = _redact_locked_ticker_confirmation_sources(bundle, source_entitlements)
        row["confirmation"] = bundle
    return _ranked_payload(candidates, generated_at=payload.get("generated_at"))


def refresh_top_stocks_leaderboard(db: Session, *, now: datetime | None = None) -> dict[str, Any]:
    """Score the cached screener universe with the exact ticker calculation.

    The public API/page never invokes this builder: score assembly, cached source
    reads, and any enrichment work remain confined to the scheduled job.
    """
    generated_at = _utc(now or datetime.now(timezone.utc))
    rows = build_screener_rows(db, TOP_STOCKS_PARAMS, requested_rows=MAX_FETCH_ROWS, apply_confirmation_filters=False)
    bundles = build_ticker_confirmation_context(db, [row["symbol"] for row in rows])["bundles"]
    candidates = []
    for original in rows:
        row = deepcopy(original)
        # Never fall back to the old screener score if canonical scoring failed.
        bundle = bundles.get(row["symbol"])
        if not isinstance(bundle, dict) or bundle.get("inputs_incomplete"):
            raise ValueError(f"Missing ticker confirmation for {row['symbol']}")
        row["confirmation"] = bundle
        row["confirmation_bundle"] = bundle
        row["updated_at"] = _iso(generated_at)
        candidates.append(row)
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
    rows = [row for row in candidates if matches_confirmation_filters(row, TOP_STOCKS_PARAMS)]
    rows.sort(key=lambda row: (row["confirmation"].get("score", 0), _market_cap(row), row["symbol"]), reverse=True)
    filter_rows = {
        key: [
            _item_from_screener_row(row, rank=index, updated_at=row.get("updated_at") or generated_at)
            for index, row in enumerate(_rows_for_filter(rows, key)[:10], start=1)
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
    confirmation = row.get("confirmation") if isinstance(row.get("confirmation"), dict) else {}
    symbol = str(row.get("symbol") or "").strip().upper()
    return {
        "rank": rank,
        "symbol": symbol,
        "company_name": str(row.get("company_name") or symbol),
        "confirmation_score": confirmation.get("score"),
        "confirmation_band": confirmation.get("band") or "inactive",
        "confirmation_direction": confirmation.get("direction") or "neutral",
        "price": row.get("price"),
        "market_cap": row.get("market_cap"),
        "sector": row.get("sector"),
        "country": row.get("country"),
        "key_drivers": _drivers_from_screener_row(row),
        "updated_at": updated_at,
        "ticker_url": str(row.get("ticker_url") or f"/ticker/{symbol}"),
    }


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
    value = row.get("market_cap")
    return float(value) if isinstance(value, (int, float)) else 0.0


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
