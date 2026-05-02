from __future__ import annotations

import json
import os
import smtplib
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from typing import Any

from sqlalchemy import and_, func, or_, select
from sqlalchemy.orm import Session

from app.models import Event, NotificationDelivery, NotificationSubscription, Security, Watchlist, WatchlistItem
from app.routers.events import _build_events_query, _fetch_events_page, _parse_since
from app.routers.signals import CONGRESS_SIGNAL_DEFAULTS, INSIDER_DEFAULTS, _query_unified_signals

SUPPORTED_ALERT_TRIGGERS = {
    "cross_source_confirmation",
    "smart_score_threshold",
    "large_trade_threshold",
    "congress_activity",
    "insider_activity",
}


@dataclass
class DigestCandidate:
    event_id: int
    ts: datetime
    symbol: str | None
    event_type: str
    who: str | None
    trade_type: str | None
    amount_max: float | None
    smart_score: int | None
    confirmation_30d: dict[str, Any] | None


def normalize_alert_triggers(values: list[str] | None) -> list[str]:
    if not values:
        return []
    seen: set[str] = set()
    normalized: list[str] = []
    for value in values:
        key = (value or "").strip().lower()
        if key in SUPPORTED_ALERT_TRIGGERS and key not in seen:
            seen.add(key)
            normalized.append(key)
    return normalized


def notification_subscription_payload(subscription: NotificationSubscription) -> dict[str, Any]:
    return {
        "id": subscription.id,
        "email": subscription.email,
        "source_type": subscription.source_type,
        "source_id": subscription.source_id,
        "source_name": subscription.source_name,
        "source_payload": _loads_dict(subscription.source_payload_json),
        "frequency": subscription.frequency,
        "only_if_new": bool(subscription.only_if_new),
        "active": bool(subscription.active),
        "alert_triggers": normalize_alert_triggers(_loads_list(subscription.alert_triggers_json)),
        "min_smart_score": subscription.min_smart_score,
        "large_trade_amount": subscription.large_trade_amount,
        "last_delivered_at": subscription.last_delivered_at,
        "created_at": subscription.created_at,
        "updated_at": subscription.updated_at,
    }


def notification_delivery_payload(delivery: NotificationDelivery) -> dict[str, Any]:
    return {
        "id": delivery.id,
        "subscription_id": delivery.subscription_id,
        "channel": delivery.channel,
        "status": delivery.status,
        "subject": delivery.subject,
        "body_text": delivery.body_text,
        "items_count": delivery.items_count,
        "alerts_count": delivery.alerts_count,
        "error": delivery.error,
        "created_at": delivery.created_at,
        "delivered_at": delivery.delivered_at,
    }


def upsert_subscription(
    db: Session,
    *,
    email: str,
    source_type: str,
    source_id: str,
    source_name: str,
    source_payload: dict[str, Any] | None,
    frequency: str,
    only_if_new: bool,
    active: bool,
    alert_triggers: list[str],
    min_smart_score: int | None,
    large_trade_amount: int | None,
    match_email: bool = True,
) -> NotificationSubscription:
    now = datetime.now(timezone.utc)
    normalized_source_type = source_type.strip().lower()
    normalized_source_id = source_id.strip()
    normalized_email = email.strip()
    existing_query = (
        select(NotificationSubscription)
        .where(NotificationSubscription.source_type == normalized_source_type)
        .where(NotificationSubscription.source_id == normalized_source_id)
    )
    if match_email:
        existing_query = existing_query.where(func.lower(NotificationSubscription.email) == normalized_email.lower())
    existing_query = existing_query.order_by(NotificationSubscription.updated_at.desc(), NotificationSubscription.id.desc()).limit(1)
    existing = db.execute(existing_query).scalar_one_or_none()

    subscription = existing or NotificationSubscription(
        email=normalized_email,
        source_type=normalized_source_type,
        source_id=normalized_source_id,
        source_name=source_name.strip(),
    )
    subscription.email = normalized_email
    subscription.source_name = source_name.strip()
    subscription.source_payload_json = json.dumps(source_payload or {}, sort_keys=True)
    subscription.frequency = frequency
    subscription.only_if_new = only_if_new
    subscription.active = active
    subscription.alert_triggers_json = json.dumps(normalize_alert_triggers(alert_triggers))
    subscription.min_smart_score = min_smart_score
    subscription.large_trade_amount = large_trade_amount
    subscription.updated_at = now
    if existing is None:
        db.add(subscription)

    db.commit()
    db.refresh(subscription)
    return subscription


def build_digest_for_subscription(
    db: Session,
    subscription: NotificationSubscription,
    *,
    limit: int = 10,
) -> tuple[list[DigestCandidate], list[tuple[str, DigestCandidate]]]:
    items = _collect_digest_candidates(db, subscription, limit=limit)
    alerts = _matching_alerts(subscription, items)
    return items, alerts


def create_digest_delivery(
    db: Session,
    subscription: NotificationSubscription,
    *,
    send: bool = False,
    limit: int = 10,
) -> NotificationDelivery:
    items, alerts = build_digest_for_subscription(db, subscription, limit=limit)
    should_skip = bool(subscription.only_if_new) and not items and not alerts
    subject = _digest_subject(subscription, items, alerts)
    body_text = _digest_body(subscription, items, alerts)
    now = datetime.now(timezone.utc)
    status = "skipped" if should_skip else "queued"
    error = None
    delivered_at = None

    if send and not should_skip:
        try:
            if _send_email(subscription.email, subject, body_text):
                status = "sent"
                delivered_at = now
            else:
                status = "queued"
                error = "SMTP is not configured."
        except Exception as exc:
            status = "failed"
            error = str(exc)[:500]

    delivery = NotificationDelivery(
        subscription_id=subscription.id,
        channel="email",
        status=status,
        subject=subject,
        body_text=body_text,
        items_count=len(items),
        alerts_count=len(alerts),
        error=error,
        delivered_at=delivered_at,
    )
    db.add(delivery)

    if not should_skip:
        subscription.last_delivered_at = now
        subscription.updated_at = now

    db.commit()
    db.refresh(delivery)
    return delivery


def run_due_digests(db: Session, *, send: bool = False, limit: int = 10) -> list[NotificationDelivery]:
    now = datetime.now(timezone.utc)
    due_before = now - timedelta(hours=20)
    subscriptions = (
        db.execute(
            select(NotificationSubscription)
            .where(NotificationSubscription.active == True)  # noqa: E712
            .where(NotificationSubscription.frequency == "daily")
            .where(or_(NotificationSubscription.last_delivered_at.is_(None), NotificationSubscription.last_delivered_at <= due_before))
            .order_by(NotificationSubscription.id.asc())
        )
        .scalars()
        .all()
    )
    return [create_digest_delivery(db, subscription, send=send, limit=limit) for subscription in subscriptions]


def _collect_digest_candidates(db: Session, subscription: NotificationSubscription, *, limit: int) -> list[DigestCandidate]:
    if subscription.source_type == "watchlist":
        return _collect_watchlist_candidates(db, subscription, limit=limit)
    if subscription.source_type == "saved_view":
        return _collect_saved_view_candidates(db, subscription, limit=limit)
    return []


def _collect_watchlist_candidates(db: Session, subscription: NotificationSubscription, *, limit: int) -> list[DigestCandidate]:
    try:
        watchlist_id = int(subscription.source_id)
    except ValueError:
        return []
    symbols = (
        db.execute(
            select(Security.symbol)
            .join(WatchlistItem, WatchlistItem.security_id == Security.id)
            .where(WatchlistItem.watchlist_id == watchlist_id)
        )
        .scalars()
        .all()
    )
    symbol_values = [symbol.strip().upper() for symbol in symbols if symbol and symbol.strip()]
    if not symbol_values:
        return []

    source_payload = _loads_dict(subscription.source_payload_json)
    since = _parse_since(source_payload.get("unseen_since") if subscription.only_if_new else None)
    if subscription.only_if_new and since is None:
        since = subscription.last_delivered_at

    q = _build_events_query(
        db=db,
        symbols=symbol_values,
        types=[],
        since=since,
        cursor=None,
        limit=limit,
        extra_filters=[],
        congress_filters=[],
    )
    return [_event_out_to_candidate(item) for item in _fetch_events_page(db, q, limit, enrich_prices=False).items]


def _collect_saved_view_candidates(db: Session, subscription: NotificationSubscription, *, limit: int) -> list[DigestCandidate]:
    payload = _loads_dict(subscription.source_payload_json)
    surface = str(payload.get("surface") or "feed")
    params = payload.get("params") if isinstance(payload.get("params"), dict) else {}
    since_value = payload.get("lastSeenAt") if subscription.only_if_new else None
    since_dt = _parse_since(since_value) if isinstance(since_value, str) and since_value else None
    if subscription.only_if_new and since_dt is None:
        since_dt = subscription.last_delivered_at

    if surface == "signals":
        mode = str(params.get("mode") or "all")
        side = str(params.get("side") or "all")
        min_smart_score = _int_param(params.get("min_smart_score"))
        items = _query_unified_signals(
            db=db,
            mode=mode if mode in {"all", "congress", "insider"} else "all",
            sort="smart",
            limit=limit,
            offset=0,
            baseline_days=365,
            congress_recent_days=CONGRESS_SIGNAL_DEFAULTS["recent_days"],
            insider_recent_days=INSIDER_DEFAULTS["recent_days"],
            congress_min_baseline_count=CONGRESS_SIGNAL_DEFAULTS["min_baseline_count"],
            insider_min_baseline_count=INSIDER_DEFAULTS["min_baseline_count"],
            congress_multiple=CONGRESS_SIGNAL_DEFAULTS["multiple"],
            insider_multiple=INSIDER_DEFAULTS["multiple"],
            congress_min_amount=CONGRESS_SIGNAL_DEFAULTS["min_amount"],
            insider_min_amount=INSIDER_DEFAULTS["min_amount"],
            min_smart_score=min_smart_score,
            side=side if side in {"all", "buy", "sell", "buy_or_sell", "award", "inkind", "exempt"} else "all",
            symbol=_string_param(params.get("symbol")),
        )
        candidates = [_signal_out_to_candidate(item) for item in items]
        if since_dt is not None:
            candidates = [item for item in candidates if item.ts >= since_dt]
        return candidates[:limit]

    return _collect_saved_event_view_candidates(db, params, since_dt=since_dt, limit=limit)


def _collect_saved_event_view_candidates(
    db: Session,
    params: dict[str, Any],
    *,
    since_dt: datetime | None,
    limit: int,
) -> list[DigestCandidate]:
    symbols = []
    symbol = _string_param(params.get("symbol"))
    if symbol:
        symbols.append(symbol.upper())
    mode = str(params.get("mode") or params.get("tape") or "all").lower()
    types: list[str] = []
    if mode == "congress":
        types = ["congress_trade"]
    elif mode == "insider":
        types = ["insider_trade"]
    elif mode == "institutional":
        types = ["institutional_buy"]

    extra_filters = []
    min_amount = _float_param(params.get("min_amount"))
    if min_amount is not None:
        extra_filters.append(Event.amount_max >= min_amount)
    trade_type = _string_param(params.get("trade_type"))
    if trade_type:
        extra_filters.append(func.lower(Event.trade_type) == trade_type.lower())

    recent_days = _int_param(params.get("recent_days"))
    if since_dt is None and recent_days:
        since_dt = datetime.now(timezone.utc) - timedelta(days=recent_days)

    q = _build_events_query(
        db=db,
        symbols=symbols,
        types=types,
        since=since_dt,
        cursor=None,
        limit=limit,
        extra_filters=extra_filters,
        congress_filters=[],
    )
    return [_event_out_to_candidate(item) for item in _fetch_events_page(db, q, limit, enrich_prices=False).items]


def _matching_alerts(
    subscription: NotificationSubscription,
    items: list[DigestCandidate],
) -> list[tuple[str, DigestCandidate]]:
    triggers = normalize_alert_triggers(_loads_list(subscription.alert_triggers_json))
    matches: list[tuple[str, DigestCandidate]] = []
    for item in items:
        for trigger in triggers:
            if trigger == "cross_source_confirmation" and item.confirmation_30d and item.confirmation_30d.get("cross_source_confirmed_30d"):
                matches.append((trigger, item))
            elif trigger == "smart_score_threshold" and item.smart_score is not None and subscription.min_smart_score is not None and item.smart_score >= subscription.min_smart_score:
                matches.append((trigger, item))
            elif trigger == "large_trade_threshold" and item.amount_max is not None and subscription.large_trade_amount is not None and item.amount_max >= subscription.large_trade_amount:
                matches.append((trigger, item))
            elif trigger == "congress_activity" and item.event_type == "congress_trade":
                matches.append((trigger, item))
            elif trigger == "insider_activity" and item.event_type == "insider_trade":
                matches.append((trigger, item))
    return matches


def _digest_subject(
    subscription: NotificationSubscription,
    items: list[DigestCandidate],
    alerts: list[tuple[str, DigestCandidate]],
) -> str:
    if alerts:
        return f"{len(alerts)} alert{'s' if len(alerts) != 1 else ''} in {subscription.source_name}"
    return f"{len(items)} new item{'s' if len(items) != 1 else ''} in {subscription.source_name}"


def _digest_body(
    subscription: NotificationSubscription,
    items: list[DigestCandidate],
    alerts: list[tuple[str, DigestCandidate]],
) -> str:
    lines = [subscription.source_name, ""]
    if alerts:
        lines.append("Alerts")
        for trigger, item in alerts[:5]:
            lines.append(f"- {_trigger_label(trigger)}: {_candidate_line(item)}")
        lines.append("")
    if items:
        lines.append("Latest")
        for item in items[:10]:
            lines.append(f"- {_candidate_line(item)}")
    else:
        lines.append("No new matching items.")
    return "\n".join(lines)


def _candidate_line(item: DigestCandidate) -> str:
    symbol = item.symbol or "UNKNOWN"
    who = item.who or "Unknown"
    amount = f" ${item.amount_max:,.0f}" if item.amount_max is not None else ""
    score = f" score {item.smart_score}" if item.smart_score is not None else ""
    return f"{symbol} {item.event_type.replace('_', ' ')} by {who}{amount}{score}"


def _send_email(to_email: str, subject: str, body_text: str) -> bool:
    host = os.getenv("SMTP_HOST")
    if not host:
        return False

    port = int(os.getenv("SMTP_PORT", "587"))
    username = os.getenv("SMTP_USERNAME")
    password = os.getenv("SMTP_PASSWORD")
    from_email = os.getenv("NOTIFICATION_FROM_EMAIL") or username or "alerts@congress-tracker.local"
    use_tls = os.getenv("SMTP_USE_TLS", "1") != "0"

    message = EmailMessage()
    message["From"] = from_email
    message["To"] = to_email
    message["Subject"] = subject
    message.set_content(body_text)

    with smtplib.SMTP(host, port, timeout=20) as smtp:
        if use_tls:
            smtp.starttls()
        if username and password:
            smtp.login(username, password)
        smtp.send_message(message)
    return True


def _trigger_label(trigger: str) -> str:
    return trigger.replace("_", " ")


def _event_out_to_candidate(item: Any) -> DigestCandidate:
    return DigestCandidate(
        event_id=int(item.id),
        ts=item.ts,
        symbol=item.symbol,
        event_type=item.event_type,
        who=item.member_name,
        trade_type=item.trade_type,
        amount_max=item.amount_max,
        smart_score=item.smart_score,
        confirmation_30d=item.confirmation_30d.model_dump() if hasattr(item.confirmation_30d, "model_dump") else item.confirmation_30d,
    )


def _signal_out_to_candidate(item: Any) -> DigestCandidate:
    event_type = "insider_trade" if item.kind == "insider" else "congress_trade"
    return DigestCandidate(
        event_id=int(item.event_id),
        ts=item.ts,
        symbol=item.symbol,
        event_type=event_type,
        who=item.who,
        trade_type=item.trade_type,
        amount_max=item.amount_max,
        smart_score=item.smart_score,
        confirmation_30d=item.confirmation_30d.model_dump() if hasattr(item.confirmation_30d, "model_dump") else item.confirmation_30d,
    )


def _loads_dict(value: str | None) -> dict[str, Any]:
    if not value:
        return {}
    try:
        parsed = json.loads(value)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _loads_list(value: str | None) -> list[str]:
    if not value:
        return []
    try:
        parsed = json.loads(value)
    except Exception:
        return []
    return [str(item) for item in parsed] if isinstance(parsed, list) else []


def _string_param(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned or None


def _int_param(value: Any) -> int | None:
    try:
        parsed = int(str(value))
    except Exception:
        return None
    return parsed


def _float_param(value: Any) -> float | None:
    try:
        parsed = float(str(value))
    except Exception:
        return None
    return parsed
