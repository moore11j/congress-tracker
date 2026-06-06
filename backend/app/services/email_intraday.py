from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from app.auth import normalize_email
from app.models import (
    ConfirmationMonitoringEvent,
    EmailDelivery,
    Event,
    MonitoringAlert,
    NotificationSubscription,
    Security,
    UserAccount,
    Watchlist,
    WatchlistItem,
)
from app.services.email_delivery import send_email
from app.services.email_digests import (
    DEFAULT_DIGEST_TIMEZONE,
    DUPLICATE_BLOCKING_STATUSES,
    _alert_skip_reason,
    _amount,
    _delivery_result,
    _event_actor,
    _format_date,
    _format_datetime,
    _frontend_base_url,
    _loads_dict,
    _score_display,
)

INTRADAY_WATCHLIST_TEMPLATE = "alerts.watchlist_intraday"
INTRADAY_SIGNAL_TEMPLATE = "alerts.signal_intraday"
INTRADAY_EVENT_TYPES = ("congress_trade", "insider_trade", "institutional_buy", "government_contract", "signal")
SIGNAL_ALERT_TYPES = (
    "signal",
    "score_change",
    "new_multi_source_confirmation",
    "confirmation_upgraded",
    "direction_flipped",
    "smart_score_threshold",
    "cross_source_confirmation",
)
SEND_LIKE_STATUSES = {"sent", "log_only", "queued"}


@dataclass(frozen=True)
class IntradayAlertCandidate:
    source: str
    user: UserAccount
    template_key: str
    event_key: str
    ticker: str
    event_type: str
    score: int | None
    amount: int | float | None
    trigger: str | None
    skip_reason: str | None
    context: dict[str, Any]
    watchlist_id: int | None = None


def intraday_alerts_enabled() -> bool:
    return os.getenv("EMAIL_ALERT_INTRADAY_ENABLED", "false").strip().lower() in {"1", "true", "yes", "on"}


def intraday_schedule_dry_run_default() -> bool:
    return os.getenv("EMAIL_ALERT_SCHEDULE_DRY_RUN", "true").strip().lower() not in {"0", "false", "no", "off"}


def email_alert_min_score() -> int:
    return _env_int("EMAIL_ALERT_MIN_SCORE", 80, minimum=0, maximum=100)


def email_alert_min_flow_usd() -> int:
    return _env_int("EMAIL_ALERT_MIN_FLOW_USD", 250_000, minimum=0)


def email_alert_sweep_lookback_minutes() -> int:
    return _env_int("EMAIL_ALERT_SWEEP_LOOKBACK_MINUTES", 60, minimum=1, maximum=1440)


def run_intraday_alert_sweep(
    db: Session,
    *,
    lookback_minutes: int | None = None,
    limit: int = 100,
    dry_run: bool | None = None,
    now: datetime | None = None,
    market_hours_only: bool = True,
) -> list[dict[str, Any]]:
    current = _coerce_aware(now or datetime.now(timezone.utc))
    window_start = current - timedelta(minutes=lookback_minutes or email_alert_sweep_lookback_minutes())
    requested_limit = max(1, min(int(limit or 100), 500))
    should_dry_run = intraday_schedule_dry_run_default() if dry_run is None else bool(dry_run)
    outside_market_hours = market_hours_only and not is_market_hours(current)
    enabled = intraday_alerts_enabled()
    candidates = _collect_intraday_candidates(db, since=window_start, limit=requested_limit)
    results: list[dict[str, Any]] = []
    for candidate in candidates[:requested_limit]:
        skip_reason = candidate.skip_reason or _alert_skip_reason(candidate.user, "signals" if candidate.source == "signal" else "watchlist_activity")
        if skip_reason is None and outside_market_hours:
            skip_reason = "outside_market_hours"
        if skip_reason is None and not enabled and not should_dry_run:
            skip_reason = "intraday_disabled"
        results.append(
            _preview_intraday_candidate(candidate, window_start, current, skip_reason)
            if should_dry_run
            else _send_intraday_candidate(db, candidate, window_start, current, skip_reason)
        )
    return results


def summarize_intraday_alert_results(results: list[dict[str, Any]]) -> dict[str, Any]:
    skip_reasons: dict[str, int] = {}
    for item in results:
        reason = item.get("skip_reason") or item.get("error")
        if item.get("status") == "skipped" and reason:
            skip_reasons[str(reason)] = skip_reasons.get(str(reason), 0) + 1
    sent_count = sum(1 for item in results if item.get("status") in SEND_LIKE_STATUSES)
    return {
        "candidate_count": len(results),
        "sent_count": sent_count,
        "skipped_count": sum(1 for item in results if item.get("status") == "skipped"),
        "would_send_count": sum(1 for item in results if item.get("status") == "would_send"),
        "failed_count": sum(1 for item in results if item.get("status") == "failed"),
        "skip_reasons": skip_reasons,
    }


def is_market_hours(value: datetime | None = None, *, timezone_name: str = DEFAULT_DIGEST_TIMEZONE) -> bool:
    current = _coerce_aware(value or datetime.now(timezone.utc)).astimezone(ZoneInfo(timezone_name))
    if current.weekday() >= 5:
        return False
    local_time = current.time()
    return time(6, 30) <= local_time <= time(13, 0)


def _collect_intraday_candidates(db: Session, *, since: datetime, limit: int) -> list[IntradayAlertCandidate]:
    candidates = _watchlist_intraday_candidates(db, since=since, limit=limit)
    remaining = max(limit - len(candidates), 0)
    if remaining:
        candidates.extend(_signal_intraday_candidates(db, since=since, limit=remaining))
    return sorted(candidates, key=lambda item: str(item.context.get("sort_timestamp") or ""), reverse=True)[:limit]


def _watchlist_intraday_candidates(db: Session, *, since: datetime, limit: int) -> list[IntradayAlertCandidate]:
    subscriptions = (
        db.execute(
            select(NotificationSubscription)
            .where(NotificationSubscription.source_type == "watchlist")
            .where(NotificationSubscription.active == True)  # noqa: E712
            .where(NotificationSubscription.frequency == "daily")
            .order_by(NotificationSubscription.id.asc())
            .limit(limit)
        )
        .scalars()
        .all()
    )
    candidates: list[IntradayAlertCandidate] = []
    for subscription in subscriptions:
        user = db.execute(select(UserAccount).where(func.lower(UserAccount.email) == normalize_email(subscription.email))).scalar_one_or_none()
        watchlist_id = _int_value(subscription.source_id)
        watchlist = db.get(Watchlist, watchlist_id) if watchlist_id is not None else None
        if not user or not watchlist:
            continue
        symbols = _watchlist_symbols(db, watchlist.id)
        if not symbols:
            continue
        activity_ts = func.coalesce(Event.event_date, Event.ts)
        rows = (
            db.execute(
                select(Event)
                .where(Event.symbol.is_not(None))
                .where(func.upper(Event.symbol).in_(symbols))
                .where(Event.event_type.in_(INTRADAY_EVENT_TYPES))
                .where(activity_ts >= since)
                .order_by(activity_ts.desc(), Event.id.desc())
                .limit(limit)
            )
            .scalars()
            .all()
        )
        for event in rows:
            candidates.append(_watchlist_candidate(user, watchlist, event))
    return candidates[:limit]


def _signal_intraday_candidates(db: Session, *, since: datetime, limit: int) -> list[IntradayAlertCandidate]:
    users = (
        db.execute(
            select(UserAccount)
            .where(UserAccount.is_suspended == False)  # noqa: E712
            .order_by(UserAccount.id.asc())
            .limit(limit)
        )
        .scalars()
        .all()
    )
    candidates: list[IntradayAlertCandidate] = []
    for user in users:
        alert_rows = (
            db.execute(
                select(MonitoringAlert)
                .where(MonitoringAlert.user_id == user.id)
                .where(MonitoringAlert.dismissed_at.is_(None))
                .where(MonitoringAlert.event_created_at >= since)
                .where(or_(MonitoringAlert.source_type == "saved_screen", MonitoringAlert.alert_type.in_(SIGNAL_ALERT_TYPES)))
                .order_by(MonitoringAlert.event_created_at.desc(), MonitoringAlert.id.desc())
                .limit(limit)
            )
            .scalars()
            .all()
        )
        watchlist_symbols = set(_user_watchlist_symbols(db, user.id))
        for alert in alert_rows:
            candidates.append(_signal_alert_candidate(user, alert, watchlist_symbols))
        confirmation_rows = (
            db.execute(
                select(ConfirmationMonitoringEvent)
                .where(ConfirmationMonitoringEvent.user_id == user.id)
                .where(ConfirmationMonitoringEvent.created_at >= since)
                .order_by(ConfirmationMonitoringEvent.created_at.desc(), ConfirmationMonitoringEvent.id.desc())
                .limit(limit)
            )
            .scalars()
            .all()
        )
        for event in confirmation_rows:
            candidates.append(_confirmation_candidate(user, event, watchlist_symbols))
    return candidates[:limit]


def _watchlist_candidate(user: UserAccount, watchlist: Watchlist, event: Event) -> IntradayAlertCandidate:
    payload = _loads_dict(event.payload_json)
    score = _event_score(event, payload)
    amount = event.amount_max if event.amount_max is not None else event.amount_min
    trigger = _watchlist_trigger(event, payload, score, amount)
    ticker = (event.symbol or "UNKNOWN").upper()
    actor = _event_actor(event, payload)
    context = {
        "first_name": _first_name(user),
        "ticker": ticker,
        "watchlist_name": watchlist.name,
        "alert_title": f"High-priority watchlist activity: {ticker}",
        "alert_intro": f"Walnut found high-priority activity for {ticker} on {watchlist.name}.",
        "event_type": event.event_type.replace("_", " "),
        "actor": actor,
        "amount": _amount(event.amount_min, event.amount_max),
        "signal_score": _score_display(score),
        "direction": _direction_from_payload(payload),
        "trigger": _trigger_label(trigger),
        "why_notable": _watchlist_reason(event, trigger),
        "source_stack": _source_stack(payload, event.source),
        "event_date": _format_date(event.event_date or event.ts),
        "alert_url": f"{_frontend_base_url()}/watchlists/{watchlist.id}",
        "sort_timestamp": _coerce_aware(event.event_date or event.ts).isoformat(),
    }
    return IntradayAlertCandidate(
        source="watchlist_activity",
        user=user,
        template_key=INTRADAY_WATCHLIST_TEMPLATE,
        event_key=f"event:{event.id}",
        ticker=ticker,
        event_type=event.event_type,
        score=score,
        amount=amount,
        trigger=trigger,
        skip_reason=None if trigger else "low_priority",
        context=context,
        watchlist_id=watchlist.id,
    )


def _signal_alert_candidate(user: UserAccount, alert: MonitoringAlert, watchlist_symbols: set[str]) -> IntradayAlertCandidate:
    payload = _loads_dict(alert.payload_json)
    saved_screen_event = payload.get("saved_screen_event") if isinstance(payload.get("saved_screen_event"), dict) else {}
    after = saved_screen_event.get("after") if isinstance(saved_screen_event.get("after"), dict) else {}
    score = _numeric_score(payload.get("score") or after.get("confirmation_score") or after.get("smart_score"))
    ticker = (alert.symbol or saved_screen_event.get("ticker") or "UNKNOWN").upper()
    trigger = _signal_trigger(alert.alert_type, payload, score, ticker in watchlist_symbols)
    context = {
        "first_name": _first_name(user),
        "ticker": ticker,
        "alert_title": f"High-conviction signal: {ticker}",
        "alert_intro": f"A saved Walnut signal matched high-conviction intraday criteria for {ticker}.",
        "event_type": alert.alert_type.replace("_", " "),
        "signal_score": _score_display(score),
        "direction": str(payload.get("direction") or after.get("direction") or "mixed"),
        "trigger": _trigger_label(trigger),
        "why_notable": alert.title or alert.alert_type.replace("_", " "),
        "source_stack": alert.source_name or alert.source_type.replace("_", " "),
        "event_date": _format_datetime(alert.event_created_at),
        "alert_url": f"{_frontend_base_url()}/ticker/{ticker}" if ticker != "UNKNOWN" else f"{_frontend_base_url()}/signals",
        "sort_timestamp": _coerce_aware(alert.event_created_at).isoformat(),
    }
    return IntradayAlertCandidate(
        source="signal",
        user=user,
        template_key=INTRADAY_SIGNAL_TEMPLATE,
        event_key=f"monitoring-alert:{alert.id}",
        ticker=ticker,
        event_type=alert.alert_type,
        score=score,
        amount=None,
        trigger=trigger,
        skip_reason=None if trigger else "low_conviction",
        context=context,
    )


def _confirmation_candidate(user: UserAccount, event: ConfirmationMonitoringEvent, watchlist_symbols: set[str]) -> IntradayAlertCandidate:
    ticker = (event.ticker or "UNKNOWN").upper()
    score = _numeric_score(event.score_after)
    trigger = _confirmation_trigger(event, ticker in watchlist_symbols)
    context = {
        "first_name": _first_name(user),
        "ticker": ticker,
        "alert_title": f"Major monitoring change: {ticker}",
        "alert_intro": f"Walnut monitoring detected a major confirmation change for {ticker}.",
        "event_type": event.event_type.replace("_", " "),
        "signal_score": _score_display(score),
        "direction": event.direction_after or "mixed",
        "trigger": _trigger_label(trigger),
        "why_notable": event.title or event.event_type.replace("_", " "),
        "source_stack": "Confirmation monitoring",
        "event_date": _format_datetime(event.created_at),
        "alert_url": f"{_frontend_base_url()}/ticker/{ticker}" if ticker != "UNKNOWN" else f"{_frontend_base_url()}/signals",
        "sort_timestamp": _coerce_aware(event.created_at).isoformat(),
    }
    return IntradayAlertCandidate(
        source="signal",
        user=user,
        template_key=INTRADAY_SIGNAL_TEMPLATE,
        event_key=f"confirmation-event:{event.id}",
        ticker=ticker,
        event_type=event.event_type,
        score=score,
        amount=None,
        trigger=trigger,
        skip_reason=None if trigger else "monitoring_digest_only",
        context=context,
        watchlist_id=event.watchlist_id,
    )


def _send_intraday_candidate(
    db: Session,
    candidate: IntradayAlertCandidate,
    window_start: datetime,
    window_end: datetime,
    skip_reason: str | None,
) -> dict[str, Any]:
    idempotency_key = _intraday_key(candidate)
    duplicate = _duplicate_intraday_result(db, idempotency_key)
    if duplicate:
        return _with_intraday_meta(duplicate, candidate, window_start, window_end, idempotency_key)
    if skip_reason:
        return _with_intraday_meta(_skip_result(candidate, skip_reason), candidate, window_start, window_end, idempotency_key)
    result = send_email(
        db,
        to_email=candidate.user.email,
        template_key=candidate.template_key,
        context=candidate.context,
        user_id=candidate.user.id,
        category="alerts",
        idempotency_key=idempotency_key,
    )
    return _with_intraday_meta(result, candidate, window_start, window_end, idempotency_key)


def _preview_intraday_candidate(
    candidate: IntradayAlertCandidate,
    window_start: datetime,
    window_end: datetime,
    skip_reason: str | None,
) -> dict[str, Any]:
    idempotency_key = _intraday_key(candidate)
    status = "skipped" if skip_reason else "would_send"
    result = {
        "id": None,
        "status": status,
        "provider": os.getenv("EMAIL_PROVIDER", "resend").strip().lower() or "resend",
        "provider_message_id": None,
        "template_key": candidate.template_key,
        "category": "alerts",
        "to_email": normalize_email(candidate.user.email),
        "idempotency_key": idempotency_key,
        "error": skip_reason,
    }
    return _with_intraday_meta(result, candidate, window_start, window_end, idempotency_key)


def _with_intraday_meta(
    result: dict[str, Any],
    candidate: IntradayAlertCandidate,
    window_start: datetime,
    window_end: datetime,
    idempotency_key: str,
) -> dict[str, Any]:
    return {
        **result,
        "source": candidate.source,
        "ticker": candidate.ticker,
        "event_type": candidate.event_type,
        "event_key": candidate.event_key,
        "trigger": candidate.trigger,
        "skip_reason": result.get("error") if result.get("status") == "skipped" else None,
        "item_count": 1,
        "items_count": 1,
        "window_start": window_start,
        "window_end": window_end,
        "idempotency_key": idempotency_key,
        "rendered_preview": {
            "summary": candidate.context.get("why_notable"),
            "items_count": 1,
            "sample_items": [
                {
                    "ticker": candidate.ticker,
                    "event_type": candidate.event_type,
                    "trigger": candidate.trigger,
                    "score": candidate.score,
                    "amount": candidate.amount,
                }
            ],
        },
    }


def _duplicate_intraday_result(db: Session, idempotency_key: str) -> dict[str, Any] | None:
    existing = db.execute(select(EmailDelivery).where(EmailDelivery.idempotency_key == idempotency_key)).scalar_one_or_none()
    if existing is None or existing.status not in DUPLICATE_BLOCKING_STATUSES:
        return None
    return _delivery_result(existing) | {"status": "skipped", "error": "duplicate_alert_already_sent"}


def _skip_result(candidate: IntradayAlertCandidate, reason: str) -> dict[str, Any]:
    return {
        "id": None,
        "status": "skipped",
        "provider": os.getenv("EMAIL_PROVIDER", "resend").strip().lower() or "resend",
        "provider_message_id": None,
        "template_key": candidate.template_key,
        "category": "alerts",
        "to_email": normalize_email(candidate.user.email),
        "error": reason,
    }


def _intraday_key(candidate: IntradayAlertCandidate) -> str:
    watchlist_part = f":watchlist:{candidate.watchlist_id}" if candidate.watchlist_id is not None else ""
    return f"intraday:{candidate.template_key}:user:{candidate.user.id}{watchlist_part}:{candidate.event_key}"


def _watchlist_trigger(event: Event, payload: dict[str, Any], score: int | None, amount: int | float | None) -> str | None:
    if score is not None and score >= email_alert_min_score():
        return "smart_score_threshold"
    if amount is not None and float(amount) >= email_alert_min_flow_usd():
        return "large_trade_threshold"
    if _has_cross_source_confirmation(payload):
        return "cross_source_confirmation"
    if _major_direction_change(payload):
        return "major_direction_change"
    if event.event_type == "government_contract" and amount is not None and float(amount) >= email_alert_min_flow_usd():
        return "government_contract"
    return None


def _signal_trigger(alert_type: str, payload: dict[str, Any], score: int | None, on_watchlist: bool) -> str | None:
    if score is not None and score >= email_alert_min_score():
        return "smart_score_threshold"
    if _strong_conviction(payload):
        return "strong_saved_screen_match"
    if _has_cross_source_confirmation(payload) or alert_type in {"cross_source_confirmation", "new_multi_source_confirmation"}:
        return "cross_source_confirmation"
    if on_watchlist and _strong_signal(payload, score):
        return "watchlist_strong_signal"
    return None


def _confirmation_trigger(event: ConfirmationMonitoringEvent, on_watchlist: bool) -> str | None:
    if event.score_after >= email_alert_min_score():
        return "smart_score_threshold"
    if event.source_count_after >= 2 and (event.source_count_before or 0) < event.source_count_after:
        return "cross_source_confirmation"
    direction_changed = bool(event.direction_before and event.direction_after and event.direction_before != event.direction_after)
    score_delta = event.score_after - (event.score_before or 0)
    if direction_changed and score_delta >= 20:
        return "major_direction_change"
    if on_watchlist and event.score_after >= max(email_alert_min_score() - 10, 0):
        return "watchlist_strong_signal"
    return None


def _watchlist_reason(event: Event, trigger: str | None) -> str:
    if trigger == "smart_score_threshold":
        return "Signal score cleared the intraday alert threshold."
    if trigger == "large_trade_threshold":
        return "Dollar flow cleared the intraday materiality threshold."
    if trigger == "cross_source_confirmation":
        return "Multiple source types confirmed the activity."
    if trigger == "major_direction_change":
        return "Confirmation direction changed materially."
    return f"{event.event_type.replace('_', ' ').title()} is available in the daily digest."


def _trigger_label(trigger: str | None) -> str:
    if not trigger:
        return "Daily digest"
    return trigger.replace("_", " ").title()


def _event_score(event: Event, payload: dict[str, Any]) -> int | None:
    return _numeric_score(
        payload.get("smart_score")
        or payload.get("signal_score")
        or payload.get("confirmation_score")
        or (round(event.impact_score) if event.impact_score is not None else None)
    )


def _numeric_score(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return int(round(float(value)))
    if isinstance(value, str) and value.strip():
        try:
            return int(round(float(value.strip())))
        except ValueError:
            return None
    return None


def _has_cross_source_confirmation(payload: dict[str, Any]) -> bool:
    confirmation = payload.get("confirmation_30d") if isinstance(payload.get("confirmation_30d"), dict) else {}
    if confirmation.get("cross_source_confirmed_30d"):
        return True
    if payload.get("cross_source_confirmation") or payload.get("cross_source_confirmed"):
        return True
    source_count = _numeric_score(payload.get("source_count") or payload.get("confirming_source_count"))
    return bool(source_count is not None and source_count >= 2)


def _strong_conviction(payload: dict[str, Any]) -> bool:
    conviction = str(payload.get("conviction") or payload.get("strength") or payload.get("match_strength") or "").strip().lower()
    return conviction in {"strong", "high", "high_conviction", "strong_match"}


def _strong_signal(payload: dict[str, Any], score: int | None) -> bool:
    if score is not None and score >= max(email_alert_min_score() - 10, 0):
        return True
    return _strong_conviction(payload)


def _major_direction_change(payload: dict[str, Any]) -> bool:
    if payload.get("major_confirmation_direction_change") or payload.get("major_direction_change"):
        return True
    before = str(payload.get("direction_before") or "").strip().lower()
    after = str(payload.get("direction_after") or payload.get("direction") or "").strip().lower()
    return bool(before and after and before != after and after not in {"neutral", "mixed"})


def _direction_from_payload(payload: dict[str, Any]) -> str:
    return str(payload.get("direction_after") or payload.get("direction") or payload.get("side") or "mixed")


def _source_stack(payload: dict[str, Any], fallback: str | None) -> str:
    value = payload.get("source_stack") or payload.get("sources") or fallback
    if isinstance(value, list):
        return ", ".join(str(item) for item in value if item)
    return str(value or "Walnut activity feed")


def _watchlist_symbols(db: Session, watchlist_id: int) -> list[str]:
    return [
        symbol.upper()
        for symbol in db.execute(
            select(Security.symbol)
            .join(WatchlistItem, WatchlistItem.security_id == Security.id)
            .where(WatchlistItem.watchlist_id == watchlist_id)
            .where(Security.symbol.is_not(None))
        ).scalars()
        if symbol and symbol.strip()
    ]


def _user_watchlist_symbols(db: Session, user_id: int) -> list[str]:
    return [
        symbol.upper()
        for symbol in db.execute(
            select(Security.symbol)
            .join(WatchlistItem, WatchlistItem.security_id == Security.id)
            .join(Watchlist, Watchlist.id == WatchlistItem.watchlist_id)
            .where(Watchlist.owner_user_id == user_id)
            .where(Security.symbol.is_not(None))
        ).scalars()
        if symbol and symbol.strip()
    ]


def _int_value(value: Any) -> int | None:
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return None


def _env_int(name: str, default: int, *, minimum: int | None = None, maximum: int | None = None) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except ValueError:
        value = default
    if minimum is not None:
        value = max(value, minimum)
    if maximum is not None:
        value = min(value, maximum)
    return value


def _first_name(user: UserAccount) -> str:
    return (user.first_name or user.name or "there").strip().split(" ", 1)[0] or "there"


def _coerce_aware(value: datetime) -> datetime:
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
