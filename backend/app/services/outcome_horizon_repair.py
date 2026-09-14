"""Fair, resumable provider repair for the continuous public Outcome ledger."""
from __future__ import annotations

import json
import logging
import time
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import ConfirmationScoreSnapshot, OutcomeEntry, OutcomeHorizonObservation, TickerContextBundleCache
from app.services.outcome_integrity import OUTCOME_HORIZONS, materialize_cached_outcome_horizons, materialize_outcome_horizons
from app.services.outcome_ledger import _project_directional_outcome_events
from app.services.price_lookup import hydrate_split_adjusted_ohlc, is_market_trading_day

logger = logging.getLogger(__name__)
REPAIR_STATE_KEY = "outcome-horizon-repair:attempts:v1"


def repair_public_outcome_horizons(db: Session, *, as_of: date, max_seconds: float = 240) -> dict:
    """Repair due public anchors, batching by symbol rather than scoring snapshot.

    Attempt times survive runs so a missing provider symbol cannot monopolize
    the bounded job. Existing entry prices and observations are never replaced.
    """
    started = time.monotonic()
    cached = materialize_cached_outcome_horizons(db, as_of=as_of)
    db.commit()
    entries = {row.snapshot_id: row for row in db.scalars(select(OutcomeEntry)).all()}
    snapshots = db.scalars(select(ConfirmationScoreSnapshot).where(ConfirmationScoreSnapshot.calculation_type == "live")).all()
    events = _project_directional_outcome_events(snapshots, verified_snapshot_ids=set(entries))
    observed = defaultdict(list)
    for row in db.scalars(select(OutcomeHorizonObservation)).all():
        observed[row.entry_id].append(row)
    work = defaultdict(list)
    targets = defaultdict(list)
    benchmarks = defaultdict(list)
    due_count = 0
    for event in events:
        entry = entries.get(event.snapshot.id)
        if entry is None:
            continue
        existing = {row.horizon_days for row in observed[entry.id]}
        due = []
        for days in OUTCOME_HORIZONS:
            if days in existing:
                continue
            target = entry.entry_session_date + timedelta(days=days)
            while not is_market_trading_day(target):
                target += timedelta(days=1)
            if target <= as_of:
                due.append(target)
        if not due:
            continue
        due_count += len(due)
        work[entry.ticker_at_time].append(entry)
        targets[entry.ticker_at_time].extend(due)
        benchmarks[entry.benchmark_symbol].extend(due)

    result = {"due_observations": due_count, "due_symbols": len(work), "attempted_symbols": 0,
              "observations_created": cached["observations_created"], "price_points": 0, "failures": []}
    if not work:
        return result
    state = db.get(TickerContextBundleCache, REPAIR_STATE_KEY)
    try:
        attempts = json.loads(state.payload_json) if state is not None else {}
    except (ValueError, TypeError):
        attempts = {}
    # Retain only outstanding work. New targets on previously completed symbols
    # immediately regain priority, while failed symbols rotate to the back.
    attempts = {symbol: value for symbol, value in attempts.items() if symbol in work}
    if state is None:
        state = TickerContextBundleCache(cache_key=REPAIR_STATE_KEY, symbol="__OUTCOME_REPAIR__", user_segment="internal")
    ordered = sorted(work, key=lambda symbol: (attempts.get(symbol, ""), min(targets[symbol]), symbol))

    # Shared benchmarks must be ready before any individual ticker is graded.
    for symbol, days in benchmarks.items():
        if time.monotonic() - started >= max_seconds:
            break
        try:
            result["price_points"] += hydrate_split_adjusted_ohlc(db, symbol, min(days).isoformat(), max(days).isoformat())
        except Exception as exc:
            db.rollback()
            result["failures"].append({"symbol": symbol, "error": type(exc).__name__})

    for symbol in ordered:
        if time.monotonic() - started >= max_seconds:
            break
        try:
            days = targets[symbol]
            result["price_points"] += hydrate_split_adjusted_ohlc(db, symbol, min(days).isoformat(), max(days).isoformat())
            # Grade immediately after hydration, before another cache writer can
            # change the price basis. Keep the immutable observation as evidence.
            created = 0
            for entry in work[symbol]:
                before = observed[entry.id]
                rows = materialize_outcome_horizons(db, entry, as_of=as_of, existing_observations=before)
                created += len(rows) - len(before)
            db.commit()
            result["observations_created"] += created
        except Exception as exc:
            db.rollback()
            logger.warning("outcome_public_horizon_repair_failed symbol=%s error=%s", symbol, type(exc).__name__)
            result["failures"].append({"symbol": symbol, "error": type(exc).__name__})
        now = datetime.now(timezone.utc)
        attempts[symbol] = now.isoformat()
        state.payload_json = json.dumps(attempts, sort_keys=True)
        state.generated_at = now
        state.stale_after = now + timedelta(days=30)
        state.expires_at = now + timedelta(days=90)
        db.merge(state)
        db.commit()
        result["attempted_symbols"] += 1
        if result["attempted_symbols"] % 25 == 0:
            logger.info("outcome_public_horizon_repair_progress attempted=%s total=%s created=%s", result["attempted_symbols"], len(work), result["observations_created"])
    result["remaining_symbols_unattempted"] = len(work) - result["attempted_symbols"]
    result["duration_seconds"] = round(time.monotonic() - started, 3)
    return result
