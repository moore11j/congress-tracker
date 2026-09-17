"""Refresh disposable current score views without rewriting recorded history.

Uses cached evidence only; does not fetch quotes or send monitoring alerts.
Run with: python -m app.jobs.refresh_current_confirmation
"""
from __future__ import annotations

import copy
import json
from datetime import datetime, timezone

from sqlalchemy import select

from app.db import SessionLocal
from app.models import MarketPressureSnapshot, TickerContextBundleCache
from app.services.confirmation_context import build_ticker_confirmation_context
from app.services.confirmation_score import CONFIRMATION_SCORING_VERSION, slim_confirmation_score_bundle
from app.services import market_pressure
from app.services.ticker_decision_layer import build_ticker_decision_layer
from app.services.top_stocks import refresh_top_stocks_leaderboard


def refresh_ticker_payload(payload: dict, bundle: dict, symbol: str, now: datetime) -> dict:
    payload = copy.deepcopy(payload)
    bundle = copy.deepcopy(bundle)
    # These are recorded observations, not a series to recalculate.
    bundle["history"] = (payload.get("confirmation_score_bundle") or {}).get("history", [])
    payload["confirmation_score_bundle"] = bundle
    summary = payload.setdefault("signals_summary", {})
    summary["confirmation_score_bundle"] = bundle
    freshness = slim_confirmation_score_bundle(bundle)["signal_freshness"]
    payload["signal_freshness"] = summary["signal_freshness"] = freshness
    payload["decision_layer"] = build_ticker_decision_layer(
        symbol, confirmation_bundle=bundle, source_contexts=payload.get("source_cards") or {},
        generated_at=str(payload.get("generated_at") or now.isoformat()), freshness_window="30d",
    )
    from app.services.cross_source_divergence import build_cross_source_divergence
    payload["cross_source_divergence"] = build_cross_source_divergence(bundle)
    payload["confirmation_refreshed_at"] = now.isoformat()
    return payload


def refresh_current_confirmation(db, *, batch_size: int = 50) -> dict:
    from app.main import _TICKER_CONTEXT_BUNDLE_VERSION

    now = datetime.now(timezone.utc)
    caches = db.scalars(select(TickerContextBundleCache).where(
        TickerContextBundleCache.user_segment == "canonical",
        TickerContextBundleCache.cache_key.like(f"ticker-context-bundle:v{_TICKER_CONTEXT_BUNDLE_VERSION}:%"),
    )).all()
    snapshots = db.scalars(select(MarketPressureSnapshot)).all()
    symbols = sorted({row.symbol for row in [*caches, *snapshots]})
    result = {"scoring_version": CONFIRMATION_SCORING_VERSION, "symbols": len(symbols),
              "ticker_caches": 0, "market_tiles": 0, "examples": {}}
    for offset in range(0, len(symbols), batch_size):
        batch = symbols[offset:offset + batch_size]
        bundles = build_ticker_confirmation_context(db, batch)["bundles"]
        if any(bundle.get("inputs_incomplete") for bundle in bundles.values()) or set(bundles) != set(batch):
            raise RuntimeError(f"Incomplete confirmation inputs in batch {offset}")
        for row in caches:
            if row.symbol not in bundles:
                continue
            row.payload_json = json.dumps(refresh_ticker_payload(json.loads(row.payload_json), bundles[row.symbol], row.symbol, now), default=str)
            result["ticker_caches"] += 1
        for row in snapshots:
            if row.symbol not in bundles:
                continue
            old = json.loads(row.tile_json)
            tile = market_pressure._build_tile(
                row.symbol,
                market_pressure.Identity(row.symbol, row.company_name, row.sector, row.exchange, row.market_cap),
                market_pressure.PricePerformance(row.price_change_pct, old.get("priceStartAt"), old.get("priceEndAt"),
                    old.get("priceEndAt"), row.price_change_pct is not None, row.market_cap),
                bundles[row.symbol], now,
            )
            # Keep price timestamps and snapshot freshness truthful after a score-only refresh.
            tile["confirmationRefreshedAt"] = now.isoformat()
            row.tile_json = json.dumps(tile, default=str)
            row.confirmation_score = tile["confirmationScore"]
            row.confirmation_direction = tile["confirmationDirection"]
            row.confirmation_as_of = datetime.fromisoformat(tile["confirmationAsOf"].replace("Z", "+00:00")) if tile["confirmationAsOf"] else None
            row.data_state = tile["dataState"]
            result["market_tiles"] += 1
        for symbol in ("BA", "NVDA"):
            if symbol in bundles:
                bundle = bundles[symbol]
                from app.services.ticker_decision_layer import _confirmation_label
                result["examples"][symbol] = {"score": bundle["score"], "label": _confirmation_label(bundle["band"], bundle["direction"], bundle["score"])}
        db.commit()
        print(f"REFRESH_PROGRESS={min(offset + batch_size, len(symbols))}/{len(symbols)}", flush=True)
    ranking = refresh_top_stocks_leaderboard(db)
    result["top_stocks"] = {"returned": ranking["returned"], "generated_at": ranking["generated_at"]}
    return result


if __name__ == "__main__":
    with SessionLocal() as session:
        print("RELEASE_JSON=" + json.dumps(refresh_current_confirmation(session), default=str), flush=True)
