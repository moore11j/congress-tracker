from __future__ import annotations

import argparse
import json
import logging
import time as time_module
from datetime import datetime, time, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db import SessionLocal
from app.ingest.government_contracts import (
    USA_SPENDING_SOURCE,
    _government_contract_action_external_id,
    _sync_government_contract_action_event,
    ensure_government_contracts_schema,
)
from app.models import Event, GovernmentContractAction

logger = logging.getLogger(__name__)


def project_government_contract_actions_to_events(
    db: Session,
    *,
    limit: int | None = None,
    batch_size: int = 100,
    sleep_ms: int = 100,
    commit_batches: bool = False,
) -> dict[str, Any]:
    ensure_government_contracts_schema(db.get_bind())
    query = select(GovernmentContractAction).order_by(
        GovernmentContractAction.action_date.desc(),
        GovernmentContractAction.id.desc(),
    )
    if limit is not None:
        query = query.limit(max(1, int(limit)))

    rows = db.execute(query).scalars().all()
    summary = {
        "status": "ok",
        "actions_seen": len(rows),
        "events_inserted": 0,
        "events_updated": 0,
        "events_relinked": 0,
        "actions_skipped": 0,
        "batch_size": max(1, int(batch_size or 100)),
        "sleep_ms": max(0, int(sleep_ms or 0)),
    }
    rows_since_commit = 0
    batch_started_at = time_module.perf_counter()

    def commit_batch(*, final: bool = False) -> None:
        nonlocal rows_since_commit, batch_started_at
        if not commit_batches or rows_since_commit <= 0:
            return
        db.commit()
        duration = time_module.perf_counter() - batch_started_at
        logger.info(
            "project_government_contract_actions_to_events committed batch rows=%s duration_s=%.3f final=%s",
            rows_since_commit,
            duration,
            final,
        )
        rows_since_commit = 0
        batch_started_at = time_module.perf_counter()
        if summary["sleep_ms"] > 0:
            time_module.sleep(summary["sleep_ms"] / 1000)

    for action in rows:
        if not action.parent_award_id or not action.symbol or action.action_date is None:
            summary["actions_skipped"] += 1
            continue

        existing_event = db.get(Event, action.event_id) if action.event_id else None
        if existing_event is None:
            existing_event = _find_existing_action_event(db, action)
            if existing_event is not None:
                action.event_id = existing_event.id
                summary["events_relinked"] += 1

        had_event = existing_event is not None
        had_event_id = action.event_id is not None
        _sync_government_contract_action_event(db, action)
        db.flush()

        if had_event or had_event_id:
            summary["events_updated"] += 1
        else:
            summary["events_inserted"] += 1
        rows_since_commit += 1
        if rows_since_commit >= summary["batch_size"]:
            commit_batch()

    commit_batch(final=True)
    return summary


def _find_existing_action_event(db: Session, action: GovernmentContractAction) -> Event | None:
    external_id = _government_contract_action_external_id(action)
    event_ts = datetime.combine(action.action_date, time.min, tzinfo=timezone.utc)
    amount = int(round(action.obligated_amount)) if action.obligated_amount is not None else None

    candidates = db.execute(
        select(Event)
        .where(Event.event_type == "government_contract")
        .where(Event.source == USA_SPENDING_SOURCE)
        .where(Event.symbol == action.symbol)
        .where(Event.trade_type == "funding_action")
        .where(Event.event_date == event_ts)
        .where(Event.amount_max == amount)
        .order_by(Event.id.desc())
    ).scalars().all()

    for event in candidates:
        payload = _loads_dict(event.payload_json)
        if payload.get("external_id") == external_id:
            return event
        if (
            payload.get("parent_award_id") == action.parent_award_id
            and payload.get("modification_number") == action.modification_number
        ):
            return event
    return None


def _loads_dict(raw: str | None) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Project government contract funding actions into canonical events.")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--sleep-ms", type=int, default=100)
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    db = SessionLocal()
    try:
        summary = project_government_contract_actions_to_events(
            db,
            limit=args.limit,
            batch_size=args.batch_size,
            sleep_ms=args.sleep_ms,
            commit_batches=True,
        )
        print(json.dumps(summary, sort_keys=True))
    finally:
        db.close()


if __name__ == "__main__":
    main()
