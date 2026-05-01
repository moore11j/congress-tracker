from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models import Event, MonitoringAlert, Security, Watchlist, WatchlistItem, WatchlistViewState

logger = logging.getLogger(__name__)

ALERTABLE_EVENT_TYPES = ("congress_trade", "insider_trade", "signal", "government_contract")


def event_freshness_at(event: Event) -> datetime:
    return event.created_at or event.ts


def watchlist_symbols(db: Session, watchlist_id: int) -> list[str]:
    rows = (
        db.execute(
            select(Security.symbol)
            .join(WatchlistItem, WatchlistItem.security_id == Security.id)
            .where(WatchlistItem.watchlist_id == watchlist_id)
            .order_by(Security.symbol.asc())
        )
        .scalars()
        .all()
    )
    return sorted({row.strip().upper() for row in rows if row and row.strip()})


def watchlist_checkpoint(db: Session, watchlist_id: int) -> datetime | None:
    state = db.execute(
        select(WatchlistViewState).where(WatchlistViewState.watchlist_id == watchlist_id)
    ).scalar_one_or_none()
    return state.last_seen_at if state else None


def refresh_watchlist_alerts(
    db: Session,
    *,
    user_id: int,
    watchlist: Watchlist,
    lookback_days: int = 7,
    force_lookback: bool = False,
) -> int:
    symbols = watchlist_symbols(db, watchlist.id)
    checkpoint = watchlist_checkpoint(db, watchlist.id)
    since = datetime.now(timezone.utc) - timedelta(days=max(int(lookback_days or 1), 1))
    if checkpoint is not None and not force_lookback:
        since = checkpoint

    if not symbols:
        logger.info(
            "monitoring_watchlist_check user_id=%s watchlist_id=%s symbols_count=0 checkpoint=%s matched_events=0 unread_created=0",
            user_id,
            watchlist.id,
            checkpoint,
        )
        return 0

    freshness_ts = func.coalesce(Event.created_at, Event.ts)
    events = (
        db.execute(
            select(Event)
            .where(Event.symbol.is_not(None))
            .where(func.upper(Event.symbol).in_(symbols))
            .where(Event.event_type.in_(ALERTABLE_EVENT_TYPES))
            .where(freshness_ts > since)
            .order_by(freshness_ts.asc(), Event.id.asc())
        )
        .scalars()
        .all()
    )

    created = 0
    for event in events:
        if _ensure_alert_for_event(db, user_id=user_id, watchlist=watchlist, event=event):
            created += 1

    logger.info(
        "monitoring_watchlist_check user_id=%s watchlist_id=%s symbols_count=%s checkpoint=%s matched_events=%s unread_created=%s",
        user_id,
        watchlist.id,
        len(symbols),
        checkpoint,
        len(events),
        created,
    )
    return created


def unread_count(db: Session, *, user_id: int) -> int:
    return int(
        db.execute(
            select(func.count())
            .select_from(MonitoringAlert)
            .where(MonitoringAlert.user_id == user_id, MonitoringAlert.read_at.is_(None))
        ).scalar_one()
        or 0
    )


def unread_count_by_source(db: Session, *, user_id: int) -> dict[tuple[str, str], int]:
    rows = db.execute(
        select(MonitoringAlert.source_type, MonitoringAlert.source_id, func.count())
        .where(MonitoringAlert.user_id == user_id, MonitoringAlert.read_at.is_(None))
        .group_by(MonitoringAlert.source_type, MonitoringAlert.source_id)
    ).all()
    return {(str(source_type), str(source_id)): int(count or 0) for source_type, source_id, count in rows}


def recent_alerts(db: Session, *, user_id: int, unread_only: bool = False, limit: int = 8) -> list[MonitoringAlert]:
    q = select(MonitoringAlert).where(MonitoringAlert.user_id == user_id)
    if unread_only:
        q = q.where(MonitoringAlert.read_at.is_(None))
    return (
        db.execute(q.order_by(MonitoringAlert.event_created_at.desc(), MonitoringAlert.id.desc()).limit(limit))
        .scalars()
        .all()
    )


def mark_alert_read(db: Session, *, user_id: int, alert_id: int, now: datetime | None = None) -> bool:
    alert = db.execute(
        select(MonitoringAlert).where(MonitoringAlert.id == alert_id, MonitoringAlert.user_id == user_id)
    ).scalar_one_or_none()
    if alert is None:
        return False
    alert.read_at = now or datetime.now(timezone.utc)
    return True


def mark_source_read(db: Session, *, user_id: int, source_id: str, source_type: str = "watchlist", now: datetime | None = None) -> int:
    read_at = now or datetime.now(timezone.utc)
    alerts = (
        db.execute(
            select(MonitoringAlert).where(
                MonitoringAlert.user_id == user_id,
                MonitoringAlert.source_type == source_type,
                MonitoringAlert.source_id == str(source_id),
                MonitoringAlert.read_at.is_(None),
            )
        )
        .scalars()
        .all()
    )
    for alert in alerts:
        alert.read_at = read_at
    return len(alerts)


def alert_to_dict(alert: MonitoringAlert) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    try:
        parsed = json.loads(alert.payload_json or "{}")
        if isinstance(parsed, dict):
            payload = parsed
    except Exception:
        payload = {}
    return {
        "id": alert.id,
        "source_type": alert.source_type,
        "source_id": alert.source_id,
        "source_name": alert.source_name,
        "event_id": alert.event_id,
        "alert_type": alert.alert_type,
        "symbol": alert.symbol,
        "title": alert.title,
        "body": alert.body,
        "payload": payload,
        "event_created_at": alert.event_created_at,
        "created_at": alert.created_at,
        "read_at": alert.read_at,
    }


def _ensure_alert_for_event(db: Session, *, user_id: int, watchlist: Watchlist, event: Event) -> bool:
    existing = db.execute(
        select(MonitoringAlert.id).where(
            MonitoringAlert.user_id == user_id,
            MonitoringAlert.source_type == "watchlist",
            MonitoringAlert.source_id == str(watchlist.id),
            MonitoringAlert.event_id == event.id,
        )
    ).scalar_one_or_none()
    if existing is not None:
        return False

    payload = _event_payload(event)
    alert = MonitoringAlert(
        user_id=user_id,
        source_type="watchlist",
        source_id=str(watchlist.id),
        source_name=watchlist.name,
        event_id=event.id,
        alert_type=event.event_type,
        symbol=(event.symbol or "").upper() or None,
        title=_event_title(event, payload),
        body=_event_body(event, payload),
        payload_json=json.dumps({"event": payload}, default=str),
        event_created_at=event_freshness_at(event),
    )
    db.add(alert)
    db.flush()
    return True


def _event_payload(event: Event) -> dict[str, Any]:
    try:
        parsed = json.loads(event.payload_json or "{}")
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _event_title(event: Event, payload: dict[str, Any]) -> str:
    symbol = event.symbol or payload.get("symbol") or payload.get("ticker")
    actor = (
        event.member_name
        or payload.get("member_name")
        or payload.get("insider_name")
        or payload.get("insiderName")
        or payload.get("reporting_owner_name")
        or payload.get("reportingOwnerName")
        or event.source
    )
    action = event.trade_type or event.transaction_type or payload.get("transaction_type") or payload.get("transactionType")
    return " - ".join(str(part) for part in (symbol, actor, action) if part) or event.event_type.replace("_", " ").title()


def _event_body(event: Event, payload: dict[str, Any]) -> str | None:
    date_value = (
        payload.get("filing_date")
        or payload.get("filingDate")
        or payload.get("report_date")
        or payload.get("reportDate")
        or payload.get("trade_date")
        or payload.get("transaction_date")
    )
    if date_value:
        return f"New {event.event_type.replace('_', ' ')} filed {date_value}."
    return f"New {event.event_type.replace('_', ' ')} activity."
