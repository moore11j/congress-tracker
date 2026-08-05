from __future__ import annotations

import argparse
import json
from datetime import datetime, time, timedelta, timezone
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.db import Base, SessionLocal, engine, ensure_outcome_ledger_schema, ensure_price_cache_volume_columns
from app.models import ConfirmationScoreSnapshot, PriceCache, Security
from app.services.outcome_ledger import (
    capture_live_confirmation_score_snapshot,
    current_code_commit_sha,
    current_confirmation_methodology,
    input_hash_for_confirmation_bundle,
    source_contributions_from_bundle,
    source_freshness_from_bundle,
)
from app.services.confirmation_score import confirmation_active_source_count

DEMO_SEED_VERSION = "phase1_demo_pending_snapshot_v2"
DEMO_MATURED_SEED_VERSION = "phase1_demo_hydrated_outcomes_v1"
DEMO_HORIZONS = (7, 30, 90, 180, 365)
DEMO_SPY_PRICE = 620.0
DEMO_SPY_RETURNS = {7: 1.1, 30: 3.4, 90: 7.1, 180: 10.4, 365: 15.2}

DEMO_EVENTS = [
    {
        "symbol": "NVDA",
        "name": "NVIDIA Corporation",
        "sector": "Technology",
        "price": 179.24,
        "score": 82,
        "direction": "bullish",
        "horizon_returns": {7: 2.4, 30: 8.6, 90: 16.3, 180: 22.4, 365: 37.2},
        "sources": [("price_volume", "bullish", 88), ("fundamentals", "bullish", 76), ("institutional_activity", "bullish", 71)],
    },
    {
        "symbol": "MSFT",
        "name": "Microsoft Corporation",
        "sector": "Technology",
        "price": 512.66,
        "score": 74,
        "direction": "bullish",
        "horizon_returns": {7: -1.2, 30: 4.9, 90: 9.8, 180: 16.1, 365: 24.2},
        "sources": [("fundamentals", "bullish", 84), ("institutional_activity", "bullish", 69)],
    },
    {
        "symbol": "XOM",
        "name": "Exxon Mobil Corporation",
        "sector": "Energy",
        "price": 111.48,
        "score": 68,
        "direction": "bearish",
        "horizon_returns": {7: 1.5, 30: -3.8, 90: -7.9, 180: -4.4, 365: -10.7},
        "sources": [("price_volume", "bearish", 72), ("options_flow", "bearish", 64)],
    },
    {
        "symbol": "JPM",
        "name": "JPMorgan Chase & Co.",
        "sector": "Financial Services",
        "price": 284.19,
        "score": 63,
        "direction": "bullish",
        "horizon_returns": {7: 0.7, 30: 3.3, 90: 8.2, 180: 12.0, 365: 18.4},
        "sources": [("fundamentals", "bullish", 73), ("price_volume", "bullish", 61)],
    },
    {
        "symbol": "CRWD",
        "name": "CrowdStrike Holdings, Inc.",
        "sector": "Technology",
        "price": 421.07,
        "score": 59,
        "direction": "neutral",
        "horizon_returns": {7: -2.8, 30: -6.1, 90: 3.0, 180: 9.5, 365: 14.0},
        "sources": [("price_volume", "bearish", 58), ("fundamentals", "bullish", 56)],
    },
    {
        "symbol": "LLY",
        "name": "Eli Lilly and Company",
        "sector": "Healthcare",
        "price": 806.11,
        "score": 71,
        "direction": "bullish",
        "horizon_returns": {7: -1.1, 30: -4.8, 90: 2.6, 180: 6.9, 365: 12.7},
        "sources": [("fundamentals", "bullish", 78), ("institutional_activity", "bullish", 66)],
    },
]


def _band(score: int) -> str:
    if score >= 80:
        return "exceptional"
    if score >= 60:
        return "strong"
    if score >= 40:
        return "moderate"
    if score >= 20:
        return "weak"
    return "inactive"


def _bundle(event: dict[str, Any], *, seed_version: str = DEMO_SEED_VERSION) -> dict[str, Any]:
    sources = {}
    active_sources = []
    for index, (source_key, direction, strength) in enumerate(event["sources"]):
        active_sources.append(source_key)
        sources[source_key] = {
            "present": True,
            "direction": direction,
            "strength": strength,
            "quality": max(50, strength - 4),
            "freshness_days": index + 1,
            "score_contribution": max(1, round((strength - 50) / 3)),
            "label": source_key.replace("_", " ").title(),
            "summary": "Phase 1 demo snapshot.",
            "detail": "Demo data only for local Outcome Ledger validation.",
        }
    return {
        "ticker": event["symbol"],
        "score": event["score"],
        "band": _band(event["score"]),
        "direction": event["direction"],
        "status": seed_version,
        "classification_version": "confirmation_direction_v3",
        "active_sources": active_sources,
        "sources": sources,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


def _ensure_security(db: Session, event: dict[str, Any]) -> Security:
    symbol = str(event["symbol"]).upper()
    security = db.execute(select(Security).where(func.upper(Security.symbol) == symbol)).scalar_one_or_none()
    if security is None:
        security = Security(symbol=symbol, name=event["name"], asset_class="equity", sector=event["sector"])
        db.add(security)
        db.flush()
    return security


def _ensure_price(db: Session, event: dict[str, Any], market_date: str) -> None:
    symbol = str(event["symbol"]).upper()
    existing = db.get(PriceCache, {"symbol": symbol, "date": market_date})
    if existing is not None:
        return
    db.add(
        PriceCache(
            symbol=symbol,
            date=market_date,
            close=float(event["price"]),
            adjusted_close=float(event["price"]),
            price_source="outcome_ledger_demo_seed",
        )
    )
    db.flush()


def _ensure_price_point(db: Session, symbol: str, market_date: str, price: float, *, source: str) -> None:
    normalized_symbol = symbol.upper()
    existing = db.get(PriceCache, {"symbol": normalized_symbol, "date": market_date})
    if existing is not None:
        return
    db.add(
        PriceCache(
            symbol=normalized_symbol,
            date=market_date,
            close=round(float(price), 2),
            adjusted_close=round(float(price), 2),
            price_source=source,
        )
    )
    db.flush()


def _price_after_return(entry_price: float, return_pct: float) -> float:
    return round(entry_price * (1 + (return_pct / 100)), 2)


def seed_hydrated_outcome_ledger_demo_snapshots(db: Session, *, count: int = 6, dry_run: bool = False) -> dict[str, Any]:
    bounded_count = max(1, min(int(count or 6), len(DEMO_EVENTS)))
    entry_date = datetime.now(timezone.utc).date() - timedelta(days=400)
    methodology = current_confirmation_methodology(db)
    spy_security = _ensure_security(
        db,
        {"symbol": "SPY", "name": "SPDR S&P 500 ETF Trust", "sector": "Benchmark"},
    )
    report = {"requested": bounded_count, "created": 0, "skipped": 0, "items": []}

    for days in (0, *DEMO_HORIZONS):
        market_date = (entry_date + timedelta(days=days)).isoformat()
        spy_price = DEMO_SPY_PRICE if days == 0 else _price_after_return(DEMO_SPY_PRICE, DEMO_SPY_RETURNS[days])
        if not dry_run:
            _ensure_price_point(db, spy_security.symbol, market_date, spy_price, source="outcome_ledger_demo_hydrated_seed")

    for index, event in enumerate(DEMO_EVENTS[:bounded_count]):
        symbol = str(event["symbol"]).upper()
        bundle = _bundle(event, seed_version=DEMO_MATURED_SEED_VERSION)
        security = _ensure_security(db, event)
        input_hash = input_hash_for_confirmation_bundle(bundle, methodology)
        existing = db.execute(
            select(ConfirmationScoreSnapshot).where(
                ConfirmationScoreSnapshot.security_id == security.id,
                ConfirmationScoreSnapshot.methodology_version_id == methodology.id,
                ConfirmationScoreSnapshot.market_date == entry_date,
                ConfirmationScoreSnapshot.input_hash == input_hash,
                ConfirmationScoreSnapshot.calculation_type == "live",
            )
        ).scalar_one_or_none()
        if existing is not None:
            report["skipped"] += 1
            report["items"].append({"symbol": symbol, "status": "already_seeded", "snapshot_id": existing.id})
            continue
        if dry_run:
            report["items"].append({"symbol": symbol, "status": "would_create"})
            continue

        entry_price = float(event["price"])
        _ensure_price_point(db, symbol, entry_date.isoformat(), entry_price, source="outcome_ledger_demo_hydrated_seed")
        for days in DEMO_HORIZONS:
            horizon_date = (entry_date + timedelta(days=days)).isoformat()
            _ensure_price_point(
                db,
                symbol,
                horizon_date,
                _price_after_return(entry_price, float(event["horizon_returns"][days])),
                source="outcome_ledger_demo_hydrated_seed",
            )

        calculated_at = datetime.now(timezone.utc) - timedelta(minutes=(bounded_count - index) * 5)
        snapshot = ConfirmationScoreSnapshot(
            security_id=security.id,
            ticker_at_time=symbol,
            calculated_at=calculated_at,
            market_date=entry_date,
            score=int(event["score"]),
            direction=str(event["direction"]),
            strength=_band(int(event["score"])),
            reference_price=entry_price,
            reference_price_at=datetime.combine(entry_date, time(21, 0), tzinfo=timezone.utc),
            reference_price_source="outcome_ledger_demo_hydrated_seed",
            active_source_count=confirmation_active_source_count(bundle),
            active_sources_json=json.dumps(bundle["active_sources"], sort_keys=True, separators=(",", ":")),
            source_contributions_json=json.dumps(source_contributions_from_bundle(bundle), sort_keys=True, separators=(",", ":")),
            source_freshness_json=json.dumps(source_freshness_from_bundle(bundle), sort_keys=True, separators=(",", ":")),
            input_hash=input_hash,
            methodology_version_id=methodology.id,
            calculation_type="live",
            code_commit_sha=current_code_commit_sha(),
        )
        db.add(snapshot)
        db.commit()
        report["created"] += 1
        report["items"].append({"symbol": symbol, "status": "created", "snapshot_id": snapshot.id})

    if dry_run:
        db.rollback()
    return report


def seed_outcome_ledger_demo_snapshots(db: Session, *, count: int = 6, dry_run: bool = False) -> dict[str, Any]:
    bounded_count = max(1, min(int(count or 6), len(DEMO_EVENTS)))
    market_date = datetime.now(timezone.utc).date().isoformat()
    methodology = current_confirmation_methodology(db)
    report = {"requested": bounded_count, "created": 0, "skipped": 0, "items": []}
    for index, event in enumerate(DEMO_EVENTS[:bounded_count]):
        symbol = str(event["symbol"]).upper()
        bundle = _bundle(event)
        security = _ensure_security(db, event)
        _ensure_price(db, event, market_date)
        input_hash = input_hash_for_confirmation_bundle(bundle, methodology)
        existing = db.execute(
            select(ConfirmationScoreSnapshot).where(
                ConfirmationScoreSnapshot.security_id == security.id,
                ConfirmationScoreSnapshot.methodology_version_id == methodology.id,
                ConfirmationScoreSnapshot.market_date == datetime.now(timezone.utc).date(),
                ConfirmationScoreSnapshot.input_hash == input_hash,
                ConfirmationScoreSnapshot.calculation_type == "live",
            )
        ).scalar_one_or_none()
        if existing is not None:
            report["skipped"] += 1
            report["items"].append({"symbol": symbol, "status": "already_seeded", "snapshot_id": existing.id})
            continue
        if dry_run:
            report["items"].append({"symbol": symbol, "status": "would_create"})
            continue
        calculated_at = datetime.now(timezone.utc) - timedelta(minutes=(bounded_count - index) * 7)
        snapshot = capture_live_confirmation_score_snapshot(db, symbol, bundle, calculated_at=calculated_at)
        if snapshot is None:
            report["items"].append({"symbol": symbol, "status": "failed"})
        else:
            report["created"] += 1
            report["items"].append({"symbol": symbol, "status": "created", "snapshot_id": snapshot.id})
    if dry_run:
        db.rollback()
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed local/demo Outcome Ledger snapshots.")
    parser.add_argument("--count", type=int, default=6, help="Number of pending demo snapshots to seed.")
    parser.add_argument("--hydrated", action="store_true", help="Seed historical demo snapshots with matured horizon prices.")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be created without writing.")
    parser.add_argument(
        "--confirm-demo-data",
        action="store_true",
        help="Required for writes so demo snapshots are not inserted accidentally.",
    )
    args = parser.parse_args()
    if not args.dry_run and not args.confirm_demo_data:
        raise SystemExit("Refusing to write demo data without --confirm-demo-data.")
    Base.metadata.create_all(bind=engine)
    ensure_price_cache_volume_columns(engine)
    ensure_outcome_ledger_schema(engine)
    with SessionLocal() as db:
        if args.hydrated:
            report = seed_hydrated_outcome_ledger_demo_snapshots(db, count=args.count, dry_run=args.dry_run)
        else:
            report = seed_outcome_ledger_demo_snapshots(db, count=args.count, dry_run=args.dry_run)
        print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
