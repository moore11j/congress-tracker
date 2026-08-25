from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from app.entitlements import entitlements_for_user
from app.models import CongressMemberAlias, Event, MonitoringAlert, SavedScreen, SavedScreenEvent, Security, UserAccount, Watchlist, WatchlistItem, WatchlistViewState
from app.routers.events import _event_effective_activity_ts, _event_effective_activity_ts_expr
from app.services.government_departments import canonical_department_name
from app.services.institutional_activity import INSTITUTIONAL_EVENT_TYPES
from app.services.monitoring_titles import build_monitoring_event_title
from app.services.ticker_meta import normalize_cik
from app.services.watchlist_content_events import sync_watchlist_content_events

logger = logging.getLogger(__name__)

ALERTABLE_EVENT_TYPES = (
    "congress_trade",
    "congress_trade_new",
    "insider_trade",
    "insider_trade_new",
    "signal",
    "government_contract",
    "government_contract_new",
    "analyst_consensus_change",
    "news_article",
    "press_release",
    "institutional_buy",
    "institutional_activity_change",
    *INSTITUTIONAL_EVENT_TYPES,
)
INSTITUTIONAL_ALERT_TYPES = (
    *INSTITUTIONAL_EVENT_TYPES,
    "institutional_buy",
    "institutional_activity",
    "institutional_activity_change",
)
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
            .where(WatchlistItem.target_type == "ticker")
            .order_by(Security.symbol.asc())
        )
        .scalars()
        .all()
    )
    return sorted({row.strip().upper() for row in rows if row and row.strip()})


def watchlist_targets(db: Session, watchlist_id: int) -> dict[str, set[str]]:
    rows = (
        db.execute(
            select(WatchlistItem.target_type, WatchlistItem.target_value, WatchlistItem.target_label)
            .where(WatchlistItem.watchlist_id == watchlist_id)
            .where(WatchlistItem.target_type != "ticker")
        )
        .all()
    )
    targets: dict[str, set[str]] = {
        "member": set(),
        "member_name": set(),
        "insider": set(),
        "department": set(),
        "institution": set(),
    }
    for target_type, target_value, target_label in rows:
        key = (target_type or "").strip().lower()
        value = (target_value or "").strip()
        if key not in targets or not value:
            continue
        if key in {"insider", "institution"}:
            normalized = normalize_cik(value)
            if normalized:
                targets[key].add(normalized)
        elif key == "department":
            targets[key].add(_department_key(value))
        else:
            targets[key].add(value.upper())
            member_name = _member_name_key(target_label or value)
            if member_name:
                targets["member_name"].add(member_name)
    targets["member"] = _member_aliases(db, targets["member"])
    return targets


def _member_aliases(db: Session, member_ids: set[str]) -> set[str]:
    """Expand canonical and provider member IDs through the identity registry.

    Historical House data can carry an FMP-generated member identifier while a
    watchlist stores the canonical Bioguide ID.  Aliases are authoritative,
    unlike the name fallback retained below for unmapped legacy records.
    """
    resolved = {value.strip().upper() for value in member_ids if value and value.strip()}
    if not resolved:
        return resolved
    frontier = set(resolved)
    for _ in range(4):
        if not frontier:
            break
        rows = db.execute(
            select(CongressMemberAlias.alias_member_id, CongressMemberAlias.authoritative_member_id)
            .where(
                or_(
                    CongressMemberAlias.alias_member_id.in_(frontier),
                    CongressMemberAlias.authoritative_member_id.in_(frontier),
                )
            )
        ).all()
        additions = {
            str(value).strip().upper()
            for row in rows
            for value in row
            if value and str(value).strip()
        } - resolved
        resolved.update(additions)
        frontier = additions
    return resolved


def _department_key(value: str | None) -> str:
    return (canonical_department_name(value) or value or "").strip().lower()


def _member_name_key(value: str | None) -> str:
    """Stable fallback for legacy provider member IDs.

    Congress provider rows have historically used synthetic FMP IDs while
    watchlists store canonical Bioguide IDs.  A normalized display-name match
    is intentionally limited to Congress events and supplements, never
    replaces, the canonical identifier check.
    """
    return re.sub(r"[^a-z0-9]+", " ", (value or "").casefold()).strip()


def _payload_values(payload: Any, keys: set[str]) -> list[str]:
    values: list[str] = []
    if isinstance(payload, dict):
        for key, value in payload.items():
            if key in keys and value is not None:
                values.append(str(value))
            if isinstance(value, (dict, list)):
                values.extend(_payload_values(value, keys))
    elif isinstance(payload, list):
        for item in payload:
            values.extend(_payload_values(item, keys))
    return values


def _event_ciks(event: Event, payload: dict[str, Any]) -> set[str]:
    raw_values = [
        event.member_bioguide_id,
        *_payload_values(
            payload,
            {
                "reporting_cik",
                "reportingCik",
                "reporting_owner_cik",
                "reportingOwnerCik",
                "holder_cik",
                "holderCik",
                "institution_cik",
                "institutionCik",
                "cik",
            },
        ),
    ]
    return {normalized for raw in raw_values for normalized in [normalize_cik(raw)] if normalized}


def _event_department_keys(event: Event, payload: dict[str, Any]) -> set[str]:
    raw_values = [
        event.member_name,
        *_payload_values(payload, {"awarding_agency", "awardingAgency", "funding_agency", "fundingAgency"}),
    ]
    return {key for raw in raw_values for key in [_department_key(raw)] if key}


def _event_matches_watchlist(
    event: Event,
    *,
    symbols: set[str],
    targets: dict[str, set[str]],
) -> bool:
    if event.symbol and event.symbol.strip().upper() in symbols:
        return True
    payload = _event_payload(event)
    member_id = (event.member_bioguide_id or "").strip().upper()
    if member_id and member_id in targets.get("member", set()):
        return True
    if (
        event.event_type.startswith("congress_trade")
        and _member_name_key(event.member_name) in targets.get("member_name", set())
    ):
        return True
    event_ciks = _event_ciks(event, payload)
    if event.event_type.startswith("insider") and event_ciks.intersection(targets.get("insider", set())):
        return True
    if event.event_type in INSTITUTIONAL_ALERT_TYPES and event_ciks.intersection(targets.get("institution", set())):
        return True
    if event.event_type.startswith("government_contract") and _event_department_keys(event, payload).intersection(targets.get("department", set())):
        return True
    return False


def event_matches_watchlist(db: Session, watchlist_id: int, event: Event) -> bool:
    return _event_matches_watchlist(event, symbols=set(watchlist_symbols(db, watchlist_id)), targets=watchlist_targets(db, watchlist_id))


def watchlist_candidate_events(
    db: Session,
    *,
    watchlist_id: int,
    event_types: tuple[str, ...],
    since: datetime,
    strict_since: bool,
    descending: bool = False,
    limit: int | None = None,
    use_effective_activity: bool = True,
) -> list[Event]:
    symbols = set(watchlist_symbols(db, watchlist_id))
    targets = watchlist_targets(db, watchlist_id)
    target_values = set().union(*targets.values()) if targets else set()
    if not symbols and not target_values:
        return []

    freshness_ts = (
        _event_effective_activity_ts_expr(db)
        if use_effective_activity
        else func.coalesce(Event.event_date, Event.ts)
    )
    predicates = []
    if symbols:
        predicates.append(Event.symbol.is_not(None) & func.upper(Event.symbol).in_(symbols))
    member_ids = targets.get("member", set())
    ciks = targets.get("insider", set()).union(targets.get("institution", set()))
    if member_ids:
        predicates.append(Event.member_bioguide_id.in_(member_ids))
    member_names = targets.get("member_name", set())
    if member_names:
        predicates.append(func.lower(Event.member_name).in_(member_names))
    if ciks:
        predicates.append(Event.member_bioguide_id.in_(ciks))
        cik_needles = sorted(ciks.union({cik.lstrip("0") for cik in ciks if cik.lstrip("0")}))
        predicates.extend(Event.payload_json.like(f"%{cik}%") for cik in cik_needles)
    department_values = targets.get("department", set())
    if department_values:
        predicates.extend(Event.payload_json.like(f"%{department}%") for department in department_values)

    if not predicates:
        return []
    since_clause = freshness_ts > since if strict_since else freshness_ts >= since
    query = (
        select(Event)
        .where(Event.event_type.in_(event_types))
        .where(or_(*predicates))
        .where(since_clause)
        .order_by(
            freshness_ts.desc() if descending else freshness_ts.asc(),
            Event.id.desc() if descending else Event.id.asc(),
        )
    )
    if limit is not None:
        query = query.limit(max(int(limit), 1))
    rows = db.execute(query).scalars().all()
    seen: set[int] = set()
    matched: list[Event] = []
    for event in rows:
        if event.id in seen:
            continue
        if _event_matches_watchlist(event, symbols=symbols, targets=targets):
            matched.append(event)
            if event.id is not None:
                seen.add(event.id)
    return matched


# Private compatibility alias for internal callers while delivery services use
# the public target-aware matcher above.
def _watchlist_candidate_events(
    db: Session,
    *,
    watchlist_id: int,
    event_types: tuple[str, ...],
    since: datetime,
    strict_since: bool,
) -> list[Event]:
    return watchlist_candidate_events(
        db,
        watchlist_id=watchlist_id,
        event_types=event_types,
        since=since,
        strict_since=strict_since,
    )


def watchlist_matching_event_ids_for_target(db: Session, target_type: str, target_value: str) -> list[int]:
    normalized_type = (target_type or "").strip().lower()
    value = (target_value or "").strip()
    if not normalized_type or not value:
        return []
    targets = {"member": set(), "insider": set(), "department": set(), "institution": set()}
    if normalized_type == "member":
        targets["member"].add(value.upper())
    elif normalized_type in {"insider", "institution"}:
        normalized_cik = normalize_cik(value)
        if normalized_cik:
            targets[normalized_type].add(normalized_cik)
    elif normalized_type == "department":
        targets["department"].add(_department_key(value))
    else:
        return []
    rows = db.execute(select(Event).where(Event.event_type.in_(ALERTABLE_EVENT_TYPES))).scalars().all()
    return [int(event.id) for event in rows if event.id is not None and _event_matches_watchlist(event, symbols=set(), targets=targets)]


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


def _watchlist_alerts_exist(db: Session, watchlist_id: int, *, user_id: int | None = None) -> bool:
    query = select(MonitoringAlert.id).where(
        MonitoringAlert.source_type == "watchlist",
        MonitoringAlert.source_id == str(watchlist_id),
    )
    if user_id is not None:
        query = query.where(MonitoringAlert.user_id == user_id)
    return bool(db.execute(query.limit(1)).scalar_one_or_none())


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


def _event_types_from_visibility(*, can_view_institutional: bool, can_view_signal_context: bool) -> tuple[str, ...]:
    event_types = ALERTABLE_EVENT_TYPES
    if not can_view_institutional:
        event_types = tuple(event_type for event_type in event_types if event_type not in INSTITUTIONAL_ALERT_TYPES)
    if not can_view_signal_context:
        event_types = tuple(event_type for event_type in event_types if event_type not in SIGNAL_ALERT_TYPES)
    return event_types


def _event_types_for_user(db: Session, user_id: int | None) -> tuple[str, ...]:
    return _event_types_from_visibility(
        can_view_institutional=_user_can_view_institutional_activity(db, user_id),
        can_view_signal_context=_user_can_view_signal_context(db, user_id),
    )


def _visibility_for_user(db: Session, user_id: int | None) -> tuple[bool, bool]:
    if user_id is None:
        return True, False
    user = db.get(UserAccount, user_id)
    if user is None:
        return False, False
    entitlements = entitlements_for_user(db, user)
    return entitlements.has_feature("institutional_feed"), entitlements.has_feature("signals")


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
    if _watchlist_alerts_exist(db, watchlist_id, user_id=user_id):
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
        if user_id is not None:
            query = query.where(MonitoringAlert.user_id == user_id)
        return int(
            db.execute(_exclude_institutional_alerts(query, db, user_id)).scalar_one()
            or 0
        )

    if checkpoint is None:
        checkpoint = watchlist_checkpoint(db, watchlist_id)
    if checkpoint is None:
        return 0

    events = _watchlist_candidate_events(
        db,
        watchlist_id=watchlist_id,
        event_types=_event_types_for_user(db, user_id),
        since=checkpoint,
        strict_since=False,
    )
    return len(events)


def watchlist_unread_counts(db: Session, watchlist_ids: list[int], user_id: int | None = None) -> dict[int, int]:
    normalized_ids = sorted({int(watchlist_id) for watchlist_id in watchlist_ids})
    if not normalized_ids:
        return {}

    source_ids = [str(watchlist_id) for watchlist_id in normalized_ids]
    can_view_institutional, can_view_signal_context = _visibility_for_user(db, user_id)
    query = (
        select(MonitoringAlert.source_id, func.count())
        .where(MonitoringAlert.source_type == "watchlist")
        .where(MonitoringAlert.source_id.in_(source_ids))
        .where(MonitoringAlert.read_at.is_(None), MonitoringAlert.dismissed_at.is_(None))
        .group_by(MonitoringAlert.source_id)
    )
    if user_id is not None:
        query = query.where(MonitoringAlert.user_id == user_id)
    if not can_view_institutional:
        query = query.where(MonitoringAlert.alert_type.notin_(INSTITUTIONAL_ALERT_TYPES))
    if not can_view_signal_context:
        query = query.where(MonitoringAlert.alert_type.notin_(SIGNAL_ALERT_TYPES))

    counts = {int(source_id): int(count or 0) for source_id, count in db.execute(query).all() if str(source_id).isdigit()}
    # Legacy watchlists can predate materialized alerts. Retain their checkpoint
    # fallback, but only for those exceptional sources rather than every source.
    for watchlist_id in normalized_ids:
        if watchlist_id not in counts:
            counts[watchlist_id] = watchlist_unread_count(db, watchlist_id, user_id=user_id)
    return counts


def watchlist_unread_summary(db: Session, watchlist_id: int, user_id: int | None = None) -> dict[str, Any]:
    checkpoint = watchlist_checkpoint(db, watchlist_id)
    count = watchlist_unread_count(db, watchlist_id, checkpoint, user_id=user_id)
    alert_since = None
    if _watchlist_alerts_exist(db, watchlist_id, user_id=user_id):
        query = select(func.min(MonitoringAlert.event_created_at)).where(
                MonitoringAlert.source_type == "watchlist",
                MonitoringAlert.source_id == str(watchlist_id),
                MonitoringAlert.read_at.is_(None),
                MonitoringAlert.dismissed_at.is_(None),
        )
        if user_id is not None:
            query = query.where(MonitoringAlert.user_id == user_id)
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
    # News and releases are fetched by the existing ticker-content ingestion
    # pipeline. Materialize that durable cache before matching Events so the
    # inbox and both email jobs share one provenance-bearing event record.
    sync_watchlist_content_events(db, watchlist.id)
    can_view_institutional, can_view_signal_context = _visibility_for_user(db, user_id)
    symbols = watchlist_symbols(db, watchlist.id)
    targets = watchlist_targets(db, watchlist.id)
    checkpoint = watchlist_checkpoint(db, watchlist.id)
    since = datetime.now(timezone.utc) - timedelta(days=max(int(lookback_days or 1), 1))
    if checkpoint is not None and not force_lookback:
        since = checkpoint

    target_count = sum(len(values) for values in targets.values())
    if not symbols and target_count == 0:
        logger.info(
            "monitoring_watchlist_check user_id=%s watchlist_id=%s symbols_count=0 targets_count=0 checkpoint=%s matched_events=0 unread_created=0",
            user_id,
            watchlist.id,
            checkpoint,
        )
        return 0

    events = _watchlist_candidate_events(
        db,
        watchlist_id=watchlist.id,
        event_types=_event_types_from_visibility(
            can_view_institutional=can_view_institutional,
            can_view_signal_context=can_view_signal_context,
        ),
        since=since,
        strict_since=True,
    )

    created = _ensure_alerts_for_events(
        db,
        user_id=user_id,
        watchlist=watchlist,
        events=events,
        can_view_institutional=can_view_institutional,
        can_view_signal_context=can_view_signal_context,
    )

    logger.info(
        "monitoring_watchlist_check user_id=%s watchlist_id=%s symbols_count=%s targets_count=%s checkpoint=%s matched_events=%s unread_created=%s",
        user_id,
        watchlist.id,
        len(symbols),
        target_count,
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
        "monitoring_source_type": alert.source_type,
        "monitoring_source_id": alert.source_id,
        "monitoring_source_name": alert.source_name,
        "event_id": alert.event_id,
        "alert_type": alert.alert_type,
        "trigger_type": alert.alert_type,
        "data_category": alert.alert_type,
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


def _ensure_alerts_for_events(
    db: Session,
    *,
    user_id: int,
    watchlist: Watchlist,
    events: list[Event],
    can_view_institutional: bool,
    can_view_signal_context: bool,
) -> int:
    event_ids = [event.id for event in events if event.id is not None]
    if not event_ids:
        return 0

    existing_event_ids = {
        int(event_id)
        for event_id in db.execute(
            select(MonitoringAlert.event_id).where(
                MonitoringAlert.user_id == user_id,
                MonitoringAlert.source_type == "watchlist",
                MonitoringAlert.source_id == str(watchlist.id),
                MonitoringAlert.event_id.in_(event_ids),
            )
        )
        .scalars()
        .all()
        if event_id is not None
    }

    alerts: list[MonitoringAlert] = []
    for event in events:
        if event.id in existing_event_ids:
            continue
        if event.event_type in INSTITUTIONAL_EVENT_TYPES and not can_view_institutional:
            continue
        if event.event_type in SIGNAL_ALERT_TYPES and not can_view_signal_context:
            continue

        payload = _event_payload(event)
        if not can_view_signal_context:
            payload = _redact_premium_signal_payload(payload)
        alerts.append(
            MonitoringAlert(
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
        )

    if not alerts:
        return 0

    db.add_all(alerts)
    db.flush()
    return len(alerts)


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
    if event.event_type in {"news_article", "press_release"}:
        return payload.get("summary") or payload.get("publisher") or "New watchlist market content."
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
