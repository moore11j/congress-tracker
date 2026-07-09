from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.entitlements import entitlements_for_user
from app.models import Event, MonitoringAlert, SavedScreen, SavedScreenEvent, Security, UserAccount, Watchlist, WatchlistItem, WatchlistViewState
from app.routers.events import _event_effective_activity_ts, _event_effective_activity_ts_expr
from app.services.institutional_activity import INSTITUTIONAL_EVENT_TYPES
from app.services.monitoring_titles import build_monitoring_event_title

logger = logging.getLogger(__name__)

ALERTABLE_EVENT_TYPES = (
    "congress_trade",
    "congress_trade_new",
    "insider_trade",
    "insider_trade_new",
    "signal",
    "government_contract",
    "government_contract_new",
    "institutional_activity_change",
    *INSTITUTIONAL_EVENT_TYPES,
)
INSTITUTIONAL_ALERT_TYPES = (*INSTITUTIONAL_EVENT_TYPES, "institutional_activity")
SIGNAL_ALERT_TYPES = ("signal",)
PREMIUM_SIGNAL_PAYLOAD_KEYS = {
    "confirmation",
    "confirmation_score",
    "confirmationScore",
    "score",
    "signal",
    "signals",
    "signal_score",
    "signalScore",
    "signal_freshness",
    "signalFreshness",
    "smart_band",
    "smartBand",
    "smart_score",
    "smartScore",
}


def event_freshness_at(event: Event) -> datetime:
    return _event_effective_activity_ts(event)


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


def set_watchlist_checkpoint(db: Session, watchlist_id: int, checkpoint: datetime | None) -> None:
    state = db.execute(
        select(WatchlistViewState).where(WatchlistViewState.watchlist_id == watchlist_id)
    ).scalar_one_or_none()
    now = datetime.now(timezone.utc)
    if state:
        state.last_seen_at = checkpoint
        state.updated_at = now
        return
    db.add(WatchlistViewState(watchlist_id=watchlist_id, last_seen_at=checkpoint))


def _watchlist_alerts_exist(db: Session, watchlist_id: int) -> bool:
    return bool(
        db.execute(
            select(MonitoringAlert.id)
            .where(
                MonitoringAlert.source_type == "watchlist",
                MonitoringAlert.source_id == str(watchlist_id),
            )
            .limit(1)
        ).scalar_one_or_none()
    )


def _user_can_view_institutional_activity(db: Session, user_id: int | None) -> bool:
    if user_id is None:
        return True
    user = db.get(UserAccount, user_id)
    return bool(user and entitlements_for_user(db, user).has_feature("institutional_feed"))


def _user_can_view_signal_context(db: Session, user_id: int | None) -> bool:
    if user_id is None:
        return False
    user = db.get(UserAccount, user_id)
    return bool(user and entitlements_for_user(db, user).has_feature("signals"))


def _event_types_for_user(db: Session, user_id: int | None) -> tuple[str, ...]:
    event_types = ALERTABLE_EVENT_TYPES
    if not _user_can_view_institutional_activity(db, user_id):
        event_types = tuple(event_type for event_type in event_types if event_type not in INSTITUTIONAL_EVENT_TYPES)
    if not _user_can_view_signal_context(db, user_id):
        event_types = tuple(event_type for event_type in event_types if event_type not in SIGNAL_ALERT_TYPES)
    return event_types


def _is_institutional_alert_type(value: str | None) -> bool:
    return (value or "").strip().lower() in INSTITUTIONAL_ALERT_TYPES


def _exclude_institutional_alerts(query, db: Session, user_id: int | None):
    if not _user_can_view_institutional_activity(db, user_id):
        query = query.where(MonitoringAlert.alert_type.notin_(INSTITUTIONAL_ALERT_TYPES))
    if not _user_can_view_signal_context(db, user_id):
        query = query.where(MonitoringAlert.alert_type.notin_(SIGNAL_ALERT_TYPES))
    return query


def _redact_premium_signal_payload(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: _redact_premium_signal_payload(item)
            for key, item in value.items()
            if key not in PREMIUM_SIGNAL_PAYLOAD_KEYS
        }
    if isinstance(value, list):
        return [_redact_premium_signal_payload(item) for item in value]
    return value


def watchlist_unread_count(db: Session, watchlist_id: int, checkpoint: datetime | None = None, user_id: int | None = None) -> int:
    if _watchlist_alerts_exist(db, watchlist_id):
        query = (
            select(func.count())
            .select_from(MonitoringAlert)
            .where(
                MonitoringAlert.source_type == "watchlist",
                MonitoringAlert.source_id == str(watchlist_id),
                MonitoringAlert.read_at.is_(None),
                MonitoringAlert.dismissed_at.is_(None),
            )
        )
        return int(
            db.execute(_exclude_institutional_alerts(query, db, user_id)).scalar_one()
            or 0
        )

    if checkpoint is None:
        checkpoint = watchlist_checkpoint(db, watchlist_id)
    if checkpoint is None:
        return 0

    symbols = watchlist_symbols(db, watchlist_id)
    if not symbols:
        return 0

    activity_ts = _event_effective_activity_ts_expr(db)
    return int(
        db.execute(
            select(func.count())
            .select_from(Event)
            .where(Event.symbol.is_not(None))
            .where(func.upper(Event.symbol).in_(symbols))
            .where(Event.event_type.in_(_event_types_for_user(db, user_id)))
            .where(activity_ts >= checkpoint)
        ).scalar_one()
        or 0
    )


def watchlist_unread_counts(db: Session, watchlist_ids: list[int], user_id: int | None = None) -> dict[int, int]:
    return {watchlist_id: watchlist_unread_count(db, watchlist_id, user_id=user_id) for watchlist_id in watchlist_ids}


def watchlist_unread_summary(db: Session, watchlist_id: int, user_id: int | None = None) -> dict[str, Any]:
    checkpoint = watchlist_checkpoint(db, watchlist_id)
    count = watchlist_unread_count(db, watchlist_id, checkpoint, user_id=user_id)
    alert_since = None
    if _watchlist_alerts_exist(db, watchlist_id):
        query = select(func.min(MonitoringAlert.event_created_at)).where(
                MonitoringAlert.source_type == "watchlist",
                MonitoringAlert.source_id == str(watchlist_id),
                MonitoringAlert.read_at.is_(None),
                MonitoringAlert.dismissed_at.is_(None),
        )
        alert_since = db.execute(_exclude_institutional_alerts(query, db, user_id)).scalar_one_or_none()
    return {
        "last_seen_at": checkpoint,
        "unseen_since": alert_since or (checkpoint if count > 0 else None),
        "unseen_count": count,
        "unread_count": count,
        "new_count": count,
    }


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

    freshness_ts = _event_effective_activity_ts_expr(db)
    events = (
        db.execute(
            select(Event)
            .where(Event.symbol.is_not(None))
            .where(func.upper(Event.symbol).in_(symbols))
            .where(Event.event_type.in_(_event_types_for_user(db, user_id)))
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
            _exclude_institutional_alerts(
                select(func.count())
                .select_from(MonitoringAlert)
                .where(MonitoringAlert.user_id == user_id, MonitoringAlert.read_at.is_(None))
                .where(MonitoringAlert.dismissed_at.is_(None)),
                db,
                user_id,
            )
        ).scalar_one()
        or 0
    )


def source_unread_count(db: Session, *, user_id: int, source_id: str, source_type: str = "watchlist") -> int:
    query = (
        select(func.count())
        .select_from(MonitoringAlert)
        .where(
            MonitoringAlert.user_id == user_id,
            MonitoringAlert.source_type == source_type,
            MonitoringAlert.source_id == str(source_id),
            MonitoringAlert.read_at.is_(None),
            MonitoringAlert.dismissed_at.is_(None),
        )
    )
    return int(
        db.execute(_exclude_institutional_alerts(query, db, user_id)).scalar_one()
        or 0
    )


def unread_count_by_source(db: Session, *, user_id: int) -> dict[tuple[str, str], int]:
    query = (
        select(MonitoringAlert.source_type, MonitoringAlert.source_id, func.count())
        .where(MonitoringAlert.user_id == user_id, MonitoringAlert.read_at.is_(None))
        .where(MonitoringAlert.dismissed_at.is_(None))
        .group_by(MonitoringAlert.source_type, MonitoringAlert.source_id)
    )
    rows = db.execute(_exclude_institutional_alerts(query, db, user_id)).all()
    return {(str(source_type), str(source_id)): int(count or 0) for source_type, source_id, count in rows}


def recent_alerts(db: Session, *, user_id: int, unread_only: bool = False, limit: int = 8) -> list[MonitoringAlert]:
    q = _exclude_institutional_alerts(
        select(MonitoringAlert).where(MonitoringAlert.user_id == user_id, MonitoringAlert.dismissed_at.is_(None)),
        db,
        user_id,
    )
    if unread_only:
        q = q.where(MonitoringAlert.read_at.is_(None))
    return (
        db.execute(q.order_by(MonitoringAlert.event_created_at.desc(), MonitoringAlert.id.desc()).limit(limit))
        .scalars()
        .all()
    )


def mark_alerts_read(db: Session, *, user_id: int, alert_ids: list[int], now: datetime | None = None) -> int:
    if not alert_ids:
        return 0
    read_at = now or datetime.now(timezone.utc)
    alerts = (
        db.execute(
            select(MonitoringAlert).where(
                MonitoringAlert.user_id == user_id,
                MonitoringAlert.id.in_(sorted({int(alert_id) for alert_id in alert_ids})),
                MonitoringAlert.dismissed_at.is_(None),
            )
        )
        .scalars()
        .all()
    )
    marked = 0
    for alert in alerts:
        if alert.read_at is None:
            marked += 1
        alert.read_at = read_at
    return marked


def mark_alerts_unread(db: Session, *, user_id: int, alert_ids: list[int]) -> int:
    if not alert_ids:
        return 0
    alerts = (
        db.execute(
            select(MonitoringAlert).where(
                MonitoringAlert.user_id == user_id,
                MonitoringAlert.id.in_(sorted({int(alert_id) for alert_id in alert_ids})),
                MonitoringAlert.dismissed_at.is_(None),
            )
        )
        .scalars()
        .all()
    )
    marked = 0
    for alert in alerts:
        if alert.read_at is not None:
            marked += 1
        alert.read_at = None
    return marked


def dismiss_alerts(db: Session, *, user_id: int, alert_ids: list[int], now: datetime | None = None) -> int:
    if not alert_ids:
        return 0
    dismissed_at = now or datetime.now(timezone.utc)
    alerts = (
        db.execute(
            select(MonitoringAlert).where(
                MonitoringAlert.user_id == user_id,
                MonitoringAlert.id.in_(sorted({int(alert_id) for alert_id in alert_ids})),
                MonitoringAlert.dismissed_at.is_(None),
            )
        )
        .scalars()
        .all()
    )
    for alert in alerts:
        alert.dismissed_at = dismissed_at
        if alert.read_at is None:
            alert.read_at = dismissed_at
    return len(alerts)


def mark_alert_read(db: Session, *, user_id: int, alert_id: int, now: datetime | None = None) -> bool:
    alert = db.execute(
        select(MonitoringAlert).where(
            MonitoringAlert.id == alert_id,
            MonitoringAlert.user_id == user_id,
            MonitoringAlert.dismissed_at.is_(None),
        )
    ).scalar_one_or_none()
    if alert is None:
        return False
    alert.read_at = now or datetime.now(timezone.utc)
    return True


def mark_alert_unread(db: Session, *, user_id: int, alert_id: int) -> bool:
    alert = db.execute(
        select(MonitoringAlert).where(
            MonitoringAlert.id == alert_id,
            MonitoringAlert.user_id == user_id,
            MonitoringAlert.dismissed_at.is_(None),
        )
    ).scalar_one_or_none()
    if alert is None:
        return False
    alert.read_at = None
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
                MonitoringAlert.dismissed_at.is_(None),
            )
        )
        .scalars()
        .all()
    )
    for alert in alerts:
        alert.read_at = read_at
    return len(alerts)


def mark_source_unread(db: Session, *, user_id: int, source_id: str, source_type: str = "watchlist") -> int:
    alerts = (
        db.execute(
            select(MonitoringAlert).where(
                MonitoringAlert.user_id == user_id,
                MonitoringAlert.source_type == source_type,
                MonitoringAlert.source_id == str(source_id),
                MonitoringAlert.read_at.is_not(None),
                MonitoringAlert.dismissed_at.is_(None),
            )
        )
        .scalars()
        .all()
    )
    for alert in alerts:
        alert.read_at = None
    return len(alerts)


def _read_alert_event_ids(db: Session, *, user_id: int, source_id: str, source_type: str = "watchlist") -> list[int]:
    return [
        int(event_id)
        for event_id in db.execute(
            select(MonitoringAlert.event_id).where(
                MonitoringAlert.user_id == user_id,
                MonitoringAlert.source_type == source_type,
                MonitoringAlert.source_id == str(source_id),
                MonitoringAlert.read_at.is_not(None),
                MonitoringAlert.dismissed_at.is_(None),
            )
        )
        .scalars()
        .all()
        if event_id is not None
    ]


def _minimum_event_activity_at(db: Session, event_ids: list[int]) -> datetime | None:
    if not event_ids:
        return None
    events = db.execute(select(Event).where(Event.id.in_(event_ids))).scalars().all()
    activity_values = [_event_effective_activity_ts(event) for event in events]
    return min(activity_values) if activity_values else None


def mark_watchlist_source_read(
    db: Session,
    *,
    user_id: int,
    watchlist: Watchlist,
    now: datetime | None = None,
) -> int:
    current_unread = watchlist_unread_count(db, watchlist.id, user_id=user_id)
    refresh_watchlist_alerts(db, user_id=user_id, watchlist=watchlist)
    marked = mark_source_read(db, user_id=user_id, source_type="watchlist", source_id=str(watchlist.id), now=now)
    set_watchlist_checkpoint(db, watchlist.id, now or datetime.now(timezone.utc))
    return max(marked, current_unread)


def mark_watchlist_source_unread(db: Session, *, user_id: int, watchlist: Watchlist) -> int:
    event_ids = _read_alert_event_ids(db, user_id=user_id, source_type="watchlist", source_id=str(watchlist.id))
    marked = mark_source_unread(db, user_id=user_id, source_type="watchlist", source_id=str(watchlist.id))
    earliest = _minimum_event_activity_at(db, event_ids)
    if earliest is not None:
        set_watchlist_checkpoint(db, watchlist.id, earliest - timedelta(microseconds=1))
    return marked


def alert_to_dict(alert: MonitoringAlert, *, can_view_signal_context: bool = True) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    try:
        parsed = json.loads(alert.payload_json or "{}")
        if isinstance(parsed, dict):
            payload = parsed
    except Exception:
        payload = {}
    if not can_view_signal_context:
        payload = _redact_premium_signal_payload(payload)
    score = payload.get("smart_score") or payload.get("score")
    event_payload = payload.get("event") if isinstance(payload.get("event"), dict) else {}
    if score is None and isinstance(event_payload, dict):
        score = event_payload.get("smart_score") or event_payload.get("confirmation_score")
    return {
        "id": alert.id,
        "item_key": f"{alert.source_type}:{alert.source_id}:{alert.alert_type}:{alert.event_id}",
        "source_type": alert.source_type,
        "source_id": alert.source_id,
        "source_name": alert.source_name,
        "event_id": alert.event_id,
        "alert_type": alert.alert_type,
        "symbol": alert.symbol,
        "title": alert.title,
        "description": alert.body,
        "body": alert.body,
        "payload": payload,
        "timestamp": alert.event_created_at,
        "event_created_at": alert.event_created_at,
        "created_at": alert.created_at,
        "read_at": alert.read_at,
        "dismissed_at": alert.dismissed_at,
        "is_read": alert.read_at is not None,
        "is_unread": alert.read_at is None,
        "is_dismissed": alert.dismissed_at is not None,
        "score": score if isinstance(score, (int, float)) else None,
    }


def _ensure_alert_for_event(db: Session, *, user_id: int, watchlist: Watchlist, event: Event) -> bool:
    if event.event_type in INSTITUTIONAL_EVENT_TYPES and not _user_can_view_institutional_activity(db, user_id):
        return False
    can_view_signal_context = _user_can_view_signal_context(db, user_id)
    if event.event_type in SIGNAL_ALERT_TYPES and not can_view_signal_context:
        return False
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
    if not can_view_signal_context:
        payload = _redact_premium_signal_payload(payload)
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


def ensure_alert_for_saved_screen_event(
    db: Session,
    *,
    event: SavedScreenEvent,
    screen: SavedScreen | None = None,
    screen_name: str | None = None,
) -> bool:
    if _is_institutional_alert_type(event.event_type) and not _user_can_view_institutional_activity(db, event.user_id):
        return False
    can_view_signal_context = _user_can_view_signal_context(db, event.user_id)
    source_id = str(event.saved_screen_id)
    existing = db.execute(
        select(MonitoringAlert.id).where(
            MonitoringAlert.user_id == event.user_id,
            MonitoringAlert.source_type == "saved_screen",
            MonitoringAlert.source_id == source_id,
            MonitoringAlert.event_id == event.id,
        )
    ).scalar_one_or_none()
    if existing is not None:
        return False

    resolved_name = screen_name or (screen.name if screen is not None else None) or "Saved screen"
    before_snapshot = _loads_dict_or_none(event.before_json)
    after_snapshot = _loads_dict_or_none(event.after_json)
    if not can_view_signal_context:
        before_snapshot = _redact_premium_signal_payload(before_snapshot)
        after_snapshot = _redact_premium_signal_payload(after_snapshot)
    payload = {
        "saved_screen_event": {
            "id": event.id,
            "saved_screen_id": event.saved_screen_id,
            "ticker": event.ticker,
            "event_type": event.event_type,
            "before": before_snapshot,
            "after": after_snapshot,
        }
    }
    after = payload["saved_screen_event"].get("after") or {}
    alert = MonitoringAlert(
        user_id=event.user_id,
        source_type="saved_screen",
        source_id=source_id,
        source_name=resolved_name,
        event_id=event.id,
        alert_type=event.event_type,
        symbol=(event.ticker or "").upper() or None,
        title=event.title,
        body=event.description,
        payload_json=json.dumps(
            {
                **payload,
                "score": after.get("confirmation_score") if can_view_signal_context and isinstance(after, dict) else None,
            },
            default=str,
        ),
        event_created_at=event.created_at,
    )
    db.add(alert)
    db.flush()
    return True


def ensure_alerts_for_saved_screen_events(
    db: Session,
    *,
    user_id: int,
    screens: list[SavedScreen],
    limit: int = 100,
) -> int:
    if not screens:
        return 0
    screen_names = {screen.id: screen.name for screen in screens}
    rows = (
        db.execute(
            select(SavedScreenEvent)
            .where(SavedScreenEvent.user_id == user_id)
            .where(SavedScreenEvent.saved_screen_id.in_(list(screen_names.keys())))
            .order_by(SavedScreenEvent.created_at.desc(), SavedScreenEvent.id.desc())
            .limit(limit)
        )
        .scalars()
        .all()
    )
    created = 0
    for event in rows:
        if ensure_alert_for_saved_screen_event(db, event=event, screen_name=screen_names.get(event.saved_screen_id)):
            created += 1
    return created


def _event_payload(event: Event) -> dict[str, Any]:
    try:
        parsed = json.loads(event.payload_json or "{}")
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _event_title(event: Event, payload: dict[str, Any]) -> str:
    return build_monitoring_event_title(event, payload)


def _event_body(event: Event, payload: dict[str, Any]) -> str | None:
    date_value = (
        payload.get("filing_date")
        or payload.get("filingDate")
        or payload.get("report_date")
        or payload.get("reportDate")
        or payload.get("trade_date")
        or payload.get("transaction_date")
    )
    if event.event_type in INSTITUTIONAL_EVENT_TYPES:
        if date_value:
            return f"New Institutional Activity 13F filing reported {date_value}."
        return "New Institutional Activity 13F filing."
    if date_value:
        return f"New {event.event_type.replace('_', ' ')} filed {date_value}."
    return f"New {event.event_type.replace('_', ' ')} activity."


def _loads_dict_or_none(raw: str | None) -> dict[str, Any] | None:
    if not raw:
        return None
    try:
        parsed = json.loads(raw)
    except Exception:
        return None
    return parsed if isinstance(parsed, dict) else None
