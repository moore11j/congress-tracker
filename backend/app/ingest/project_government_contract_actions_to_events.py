from __future__ import annotations

import argparse
import json
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


def project_government_contract_actions_to_events(
    db: Session,
    *,
    limit: int | None = None,
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
    }

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
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    db = SessionLocal()
    try:
        summary = project_government_contract_actions_to_events(db, limit=args.limit)
        db.commit()
        print(json.dumps(summary, sort_keys=True))
    finally:
        db.close()


if __name__ == "__main__":
    main()
