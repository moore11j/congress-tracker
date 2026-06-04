from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from html import escape as html_escape
from typing import Any

from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from app.auth import normalize_email
from app.models import (
    BillingTransaction,
    ConfirmationMonitoringEvent,
    EmailDelivery,
    EmailTemplate,
    Event,
    NotificationSubscription,
    Security,
    UserAccount,
    Watchlist,
    WatchlistItem,
)
from app.services.email_delivery import send_email
from app.services.email_renderer import render_template_string
from app.services.email_templates import seed_default_email_templates

ALERT_EVENT_TYPES = ("congress_trade", "insider_trade", "institutional_buy", "government_contract", "signal")
SUPPORT_EMAIL = "support@walnut-intel.com"


@dataclass(frozen=True)
class DigestBuild:
    template_key: str
    context: dict[str, Any]
    items_count: int
    summary: str
    items: list[dict[str, Any]]


def build_watchlist_activity_digest(db: Session, user: UserAccount, watchlist: Watchlist, since: datetime) -> DigestBuild:
    rows = _watchlist_events(db, watchlist.id, since=since, limit=12)
    items = [_event_item(row) for row in rows]
    summary = _count_summary(len(items), "new filing or event", "new filings or events")
    return DigestBuild(
        template_key="alerts.watchlist_activity",
        items_count=len(items),
        summary=summary,
        items=items,
        context={
            "first_name": _first_name(user),
            "watchlist_name": watchlist.name,
            "summary": summary,
            "items_text": _watchlist_items_text(items),
            "items_html": _watchlist_items_html(items),
            "activity_url": f"{_frontend_base_url()}/watchlists/{watchlist.id}",
        },
    )


def send_watchlist_activity_digest(
    db: Session,
    user: UserAccount,
    watchlist: Watchlist,
    since: datetime,
    force: bool = False,
) -> dict[str, Any]:
    template_key = "alerts.watchlist_activity"
    window_end = datetime.now(timezone.utc)
    subscription = _watchlist_subscription(db, user, watchlist)
    skip = _alert_skip_reason(user, "watchlist_activity")
    only_if_new = True if subscription is None else bool(subscription.only_if_new)
    if skip is None and subscription is None and not force:
        skip = "watchlist_digest_inactive"
    if skip is None and subscription is not None and not subscription.active and not force:
        skip = "watchlist_digest_inactive"

    digest = build_watchlist_activity_digest(db, user, watchlist, since)
    idempotency_key = None if force else _digest_key(template_key, user.id, watchlist.id, since, window_end)
    if skip:
        return _log_skip(db, user=user, template_key=template_key, category="alerts", context=digest.context, idempotency_key=idempotency_key, reason=skip)
    if only_if_new and digest.items_count == 0 and not force:
        return _log_skip(db, user=user, template_key=template_key, category="alerts", context=digest.context, idempotency_key=idempotency_key, reason="no_new_items")
    result = _send_digest(db, user=user, digest=digest, category="alerts", idempotency_key=idempotency_key)
    _mark_subscription_delivered(db, subscription, result)
    return _with_preview(result, digest)


def build_monitoring_digest(db: Session, user: UserAccount, watchlist: Watchlist, since: datetime) -> DigestBuild:
    rows = (
        db.execute(
            select(ConfirmationMonitoringEvent)
            .where(ConfirmationMonitoringEvent.user_id == user.id)
            .where(ConfirmationMonitoringEvent.watchlist_id == watchlist.id)
            .where(ConfirmationMonitoringEvent.created_at >= since)
            .order_by(ConfirmationMonitoringEvent.created_at.desc(), ConfirmationMonitoringEvent.id.desc())
            .limit(12)
        )
        .scalars()
        .all()
    )
    items = [_monitoring_item(row) for row in rows]
    summary = _count_summary(len(items), "monitoring change", "monitoring changes")
    return DigestBuild(
        template_key="alerts.monitoring_digest",
        items_count=len(items),
        summary=summary,
        items=items,
        context={
            "first_name": _first_name(user),
            "watchlist_name": watchlist.name,
            "digest_date": _format_date(datetime.now(timezone.utc)),
            "summary": summary,
            "items_text": _monitoring_items_text(items),
            "items_html": _monitoring_items_html(items),
            "digest_url": f"{_frontend_base_url()}/watchlists/{watchlist.id}",
        },
    )


def send_monitoring_digest(
    db: Session,
    user: UserAccount,
    watchlist: Watchlist,
    since: datetime,
    force: bool = False,
) -> dict[str, Any]:
    template_key = "alerts.monitoring_digest"
    window_end = datetime.now(timezone.utc)
    subscription = _watchlist_subscription(db, user, watchlist)
    skip = _alert_skip_reason(user, "watchlist_activity")
    only_if_new = True if subscription is None else bool(subscription.only_if_new)
    if skip is None and subscription is None and not force:
        skip = "watchlist_digest_inactive"
    if skip is None and subscription is not None and not subscription.active and not force:
        skip = "watchlist_digest_inactive"

    digest = build_monitoring_digest(db, user, watchlist, since)
    idempotency_key = None if force else _digest_key(template_key, user.id, watchlist.id, since, window_end)
    if skip:
        return _log_skip(db, user=user, template_key=template_key, category="alerts", context=digest.context, idempotency_key=idempotency_key, reason=skip)
    if only_if_new and digest.items_count == 0 and not force:
        return _log_skip(db, user=user, template_key=template_key, category="alerts", context=digest.context, idempotency_key=idempotency_key, reason="no_new_items")
    result = _send_digest(db, user=user, digest=digest, category="alerts", idempotency_key=idempotency_key)
    _mark_subscription_delivered(db, subscription, result)
    return _with_preview(result, digest)


def build_signal_alert_digest(
    db: Session,
    user: UserAccount,
    since: datetime,
    watchlist: Watchlist | None = None,
) -> DigestBuild:
    rows = _signal_events(db, user, since=since, watchlist=watchlist, limit=10)
    items = [_signal_item(row) for row in rows]
    lead = items[0] if items else {}
    ticker = str(lead.get("ticker") or "Daily signal alerts")
    summary = _count_summary(len(items), "notable signal", "notable signals")
    return DigestBuild(
        template_key="alerts.signal_alert",
        items_count=len(items),
        summary=summary,
        items=items,
        context={
            "first_name": _first_name(user),
            "ticker": ticker,
            "signal_score": str(lead.get("signal_score") or "n/a"),
            "direction": str(lead.get("direction") or "mixed"),
            "why_notable": str(lead.get("why_notable") or summary),
            "source_stack": str(lead.get("source_stack") or "Walnut event and confirmation signals"),
            "cautions": "Review source context before acting.",
            "signals_text": _signal_items_text(items),
            "signals_html": _signal_items_html(items),
            "signal_url": _signal_url(ticker),
        },
    )


def send_signal_alert_digest(db: Session, user: UserAccount, since: datetime, force: bool = False) -> dict[str, Any]:
    template_key = "alerts.signal_alert"
    window_end = datetime.now(timezone.utc)
    digest = build_signal_alert_digest(db, user, since)
    idempotency_key = None if force else _digest_key(template_key, user.id, None, since, window_end)
    skip = _alert_skip_reason(user, "signals")
    if skip:
        return _log_skip(db, user=user, template_key=template_key, category="alerts", context=digest.context, idempotency_key=idempotency_key, reason=skip)
    if digest.items_count == 0 and not force:
        return _log_skip(db, user=user, template_key=template_key, category="alerts", context=digest.context, idempotency_key=idempotency_key, reason="no_new_items")
    return _with_preview(_send_digest(db, user=user, digest=digest, category="alerts", idempotency_key=idempotency_key), digest)


def send_monthly_billing_statement(
    db: Session,
    user: UserAccount,
    period_start: datetime | date,
    period_end: datetime | date,
    force: bool = False,
) -> dict[str, Any]:
    start = _coerce_window_start(period_start)
    end = _coerce_window_end(period_end)
    template_key = "billing.monthly_statement"
    digest = _build_billing_statement(db, user, start, end)
    idempotency_key = None if force else _digest_key(template_key, user.id, None, start, end)
    if not _valid_user_email(user):
        return _log_skip(db, user=user, template_key=template_key, category="billing", context=digest.context, idempotency_key=idempotency_key, reason="invalid_email")
    return _with_preview(_send_digest(db, user=user, digest=digest, category="billing", idempotency_key=idempotency_key), digest)


def run_digest_job(db: Session, *, kind: str, lookback_days: int = 1, limit: int = 100, force: bool = False) -> list[dict[str, Any]]:
    since = datetime.now(timezone.utc) - timedelta(days=max(int(lookback_days or 1), 1))
    results: list[dict[str, Any]] = []
    if kind == "signals":
        users = _eligible_users(db, limit=limit)
        return [send_signal_alert_digest(db, user, since, force=force) for user in users]

    subscriptions = (
        db.execute(
            select(NotificationSubscription)
            .where(NotificationSubscription.source_type == "watchlist")
            .where(NotificationSubscription.active == True)  # noqa: E712
            .order_by(NotificationSubscription.id.asc())
            .limit(limit)
        )
        .scalars()
        .all()
    )
    for subscription in subscriptions:
        user = db.execute(select(UserAccount).where(func.lower(UserAccount.email) == normalize_email(subscription.email))).scalar_one_or_none()
        watchlist_id = _int_value(subscription.source_id)
        watchlist = db.get(Watchlist, watchlist_id) if watchlist_id is not None else None
        if not user or not watchlist:
            continue
        if kind == "monitoring":
            results.append(send_monitoring_digest(db, user, watchlist, since, force=force))
        elif kind == "watchlist_activity":
            results.append(send_watchlist_activity_digest(db, user, watchlist, since, force=force))
    return results


def _build_billing_statement(db: Session, user: UserAccount, start: datetime, end: datetime) -> DigestBuild:
    rows = (
        db.execute(
            select(BillingTransaction)
            .where(
                or_(
                    BillingTransaction.user_id == user.id,
                    func.lower(BillingTransaction.customer_email) == normalize_email(user.email),
                )
            )
            .where(or_(BillingTransaction.charged_at.is_(None), BillingTransaction.charged_at >= start))
            .where(or_(BillingTransaction.charged_at.is_(None), BillingTransaction.charged_at < end))
            .order_by(BillingTransaction.charged_at.desc().nullslast(), BillingTransaction.id.desc())
            .limit(5)
        )
        .scalars()
        .all()
    )
    latest = rows[0] if rows else None
    amount_cents = latest.total_amount if latest and latest.total_amount is not None else user.monthly_price_override
    currency = (latest.currency if latest and latest.currency else user.override_currency or "USD").upper()
    amount = _money(amount_cents, currency)
    status = latest.payment_status if latest and latest.payment_status else user.subscription_status or "recorded"
    plan = user.subscription_plan or user.entitlement_tier or "Walnut Market Terminal"
    period = f"{_format_date(start)} - {_format_date(end - timedelta(seconds=1))}"
    return DigestBuild(
        template_key="billing.monthly_statement",
        items_count=len(rows),
        summary=f"Statement for {period}",
        items=[],
        context={
            "first_name": _first_name(user),
            "billing_period": period,
            "plan": plan,
            "amount_due": amount,
            "currency": currency,
            "payment_status": status,
            "statement_url": f"{_frontend_base_url()}/account/billing",
        },
    )


def _send_digest(db: Session, *, user: UserAccount, digest: DigestBuild, category: str, idempotency_key: str | None) -> dict[str, Any]:
    return send_email(
        db,
        to_email=user.email,
        template_key=digest.template_key,
        context=digest.context,
        user_id=user.id,
        category=category,
        idempotency_key=idempotency_key,
    )


def _log_skip(
    db: Session,
    *,
    user: UserAccount,
    template_key: str,
    category: str,
    context: dict[str, Any],
    idempotency_key: str | None,
    reason: str,
) -> dict[str, Any]:
    if idempotency_key:
        existing = db.execute(select(EmailDelivery).where(EmailDelivery.idempotency_key == idempotency_key)).scalar_one_or_none()
        if existing:
            return _delivery_result(existing)
    template = _template(db, template_key)
    delivery = EmailDelivery(
        user_id=user.id,
        to_email=normalize_email(user.email),
        from_email=template.from_email,
        template_key=template.template_key,
        category=category,
        subject=_render_subject(template, context),
        provider=_provider_name(),
        status="skipped",
        idempotency_key=idempotency_key,
        error=reason,
        payload_json=json.dumps({"context_keys": sorted(context.keys()), "skip_reason": reason}, sort_keys=True),
    )
    db.add(delivery)
    db.commit()
    db.refresh(delivery)
    return _delivery_result(delivery)


def _template(db: Session, template_key: str) -> EmailTemplate:
    template = db.execute(select(EmailTemplate).where(EmailTemplate.template_key == template_key)).scalar_one_or_none()
    if template:
        return template
    seed_default_email_templates(db)
    return db.execute(select(EmailTemplate).where(EmailTemplate.template_key == template_key)).scalar_one()


def _render_subject(template: EmailTemplate, context: dict[str, Any]) -> str:
    try:
        variables = json.loads(template.variables_json or "[]")
    except Exception:
        variables = []
    allowed = [str(item) for item in variables] if isinstance(variables, list) else []
    return render_template_string(template.subject, context, allowed)


def _delivery_result(delivery: EmailDelivery) -> dict[str, Any]:
    return {
        "id": delivery.id,
        "status": delivery.status,
        "provider": delivery.provider,
        "provider_message_id": delivery.provider_message_id,
        "template_key": delivery.template_key,
        "category": delivery.category,
        "to_email": delivery.to_email,
        "error": delivery.error,
    }


def _with_preview(result: dict[str, Any], digest: DigestBuild) -> dict[str, Any]:
    return {
        **result,
        "items_count": digest.items_count,
        "rendered_preview": {
            "summary": digest.summary,
            "items_count": digest.items_count,
            "sample_items": digest.items[:3],
        },
    }


def _alert_skip_reason(user: UserAccount, kind: str) -> str | None:
    if not _valid_user_email(user):
        return "invalid_email"
    if user.is_suspended:
        return "user_suspended"
    if not user.alerts_enabled:
        return "alerts_disabled"
    if not user.email_notifications_enabled:
        return "email_notifications_disabled"
    if kind == "watchlist_activity" and not user.watchlist_activity_notifications:
        return "watchlist_activity_notifications_disabled"
    if kind == "signals" and not user.signals_notifications:
        return "signals_notifications_disabled"
    return None


def _valid_user_email(user: UserAccount) -> bool:
    email = normalize_email(user.email)
    return bool(email and "@" in email)


def _watchlist_subscription(db: Session, user: UserAccount, watchlist: Watchlist) -> NotificationSubscription | None:
    return (
        db.execute(
            select(NotificationSubscription)
            .where(NotificationSubscription.source_type == "watchlist")
            .where(NotificationSubscription.source_id == str(watchlist.id))
            .where(func.lower(NotificationSubscription.email) == normalize_email(user.email))
            .order_by(NotificationSubscription.updated_at.desc(), NotificationSubscription.id.desc())
            .limit(1)
        )
        .scalars()
        .first()
    )


def _watchlist_events(db: Session, watchlist_id: int, *, since: datetime, limit: int) -> list[Event]:
    symbols = _watchlist_symbols(db, watchlist_id)
    if not symbols:
        return []
    activity_ts = func.coalesce(Event.event_date, Event.ts)
    return (
        db.execute(
            select(Event)
            .where(Event.symbol.is_not(None))
            .where(func.upper(Event.symbol).in_(symbols))
            .where(Event.event_type.in_(ALERT_EVENT_TYPES))
            .where(activity_ts >= since)
            .order_by(activity_ts.desc(), Event.id.desc())
            .limit(limit)
        )
        .scalars()
        .all()
    )


def _signal_events(db: Session, user: UserAccount, *, since: datetime, watchlist: Watchlist | None, limit: int) -> list[Event]:
    symbols = _watchlist_symbols(db, watchlist.id) if watchlist else _user_watchlist_symbols(db, user.id)
    if not symbols:
        return []
    activity_ts = func.coalesce(Event.event_date, Event.ts)
    return (
        db.execute(
            select(Event)
            .where(Event.symbol.is_not(None))
            .where(func.upper(Event.symbol).in_(symbols))
            .where(Event.event_type.in_(ALERT_EVENT_TYPES))
            .where(activity_ts >= since)
            .order_by(Event.impact_score.desc(), activity_ts.desc(), Event.id.desc())
            .limit(limit)
        )
        .scalars()
        .all()
    )


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
    rows = (
        db.execute(
            select(Security.symbol)
            .join(WatchlistItem, WatchlistItem.security_id == Security.id)
            .join(Watchlist, Watchlist.id == WatchlistItem.watchlist_id)
            .where(Watchlist.owner_user_id == user_id)
            .where(Security.symbol.is_not(None))
        )
        .scalars()
        .all()
    )
    return sorted({row.strip().upper() for row in rows if row and row.strip()})


def _eligible_users(db: Session, *, limit: int) -> list[UserAccount]:
    return (
        db.execute(
            select(UserAccount)
            .where(UserAccount.is_suspended == False)  # noqa: E712
            .order_by(UserAccount.id.asc())
            .limit(limit)
        )
        .scalars()
        .all()
    )


def _event_item(event: Event) -> dict[str, Any]:
    payload = _loads_dict(event.payload_json)
    return {
        "ticker": (event.symbol or "").upper() or "UNKNOWN",
        "event_type": event.event_type.replace("_", " "),
        "actor": event.member_name or payload.get("actor") or payload.get("agency") or payload.get("insider_name") or "Unknown",
        "trade": event.trade_type or event.transaction_type or payload.get("action") or "activity",
        "amount": _amount(event.amount_min, event.amount_max),
        "date": _format_date(event.event_date or event.ts),
        "signal_score": payload.get("smart_score") or payload.get("signal_score") or (round(event.impact_score) if event.impact_score else None),
    }


def _monitoring_item(event: ConfirmationMonitoringEvent) -> dict[str, Any]:
    payload = _loads_dict(event.payload_json)
    return {
        "ticker": event.ticker,
        "title": event.title,
        "score_change": _score_change(event.score_before, event.score_after),
        "direction_change": _direction_change(event.direction_before, event.direction_after),
        "timestamp": _format_datetime(event.created_at),
        "reason": event.body or payload.get("event_type") or event.event_type,
    }


def _signal_item(event: Event) -> dict[str, Any]:
    payload = _loads_dict(event.payload_json)
    score = payload.get("smart_score") or payload.get("signal_score") or (round(event.impact_score) if event.impact_score else None)
    direction = payload.get("direction") or _direction_from_trade(event.trade_type or event.transaction_type)
    ticker = (event.symbol or "").upper() or "UNKNOWN"
    return {
        "ticker": ticker,
        "signal_score": score or "n/a",
        "direction": direction,
        "why_notable": event.event_type.replace("_", " "),
        "source_stack": event.source or "Walnut event stream",
        "cautions": "Confirm recency, liquidity, and filing context.",
        "date": _format_date(event.event_date or event.ts),
    }


def _watchlist_items_text(items: list[dict[str, Any]]) -> str:
    if not items:
        return "No new matching items."
    return "\n".join(
        f"- {item['ticker']} {item['event_type']} | {item['actor']} | {item['trade']} | {item['amount']} | {item['date']} | score {item.get('signal_score') or 'n/a'}"
        for item in items
    )


def _watchlist_items_html(items: list[dict[str, Any]]) -> str:
    if not items:
        return "<p>No new matching items.</p>"
    rows = "".join(
        "<tr>"
        f"<td style=\"padding:10px;border-bottom:1px solid #e2e8f0;font-weight:700;color:#0f172a;\">{html_escape(str(item['ticker']))}</td>"
        f"<td style=\"padding:10px;border-bottom:1px solid #e2e8f0;color:#334155;\">{html_escape(str(item['event_type']))}</td>"
        f"<td style=\"padding:10px;border-bottom:1px solid #e2e8f0;color:#334155;\">{html_escape(str(item['actor']))}</td>"
        f"<td style=\"padding:10px;border-bottom:1px solid #e2e8f0;color:#334155;\">{html_escape(str(item['amount']))}</td>"
        f"<td style=\"padding:10px;border-bottom:1px solid #e2e8f0;color:#334155;\">{html_escape(str(item.get('signal_score') or 'n/a'))}</td>"
        "</tr>"
        for item in items
    )
    return _table(["Ticker", "Event", "Actor", "Amount", "Score"], rows)


def _monitoring_items_text(items: list[dict[str, Any]]) -> str:
    if not items:
        return "No monitoring changes."
    return "\n".join(
        f"- {item['ticker']}: {item['title']} | score {item['score_change']} | direction {item['direction_change']} | {item['timestamp']} | {item['reason']}"
        for item in items
    )


def _monitoring_items_html(items: list[dict[str, Any]]) -> str:
    if not items:
        return "<p>No monitoring changes.</p>"
    rows = "".join(
        "<tr>"
        f"<td style=\"padding:10px;border-bottom:1px solid #e2e8f0;font-weight:700;color:#0f172a;\">{html_escape(str(item['ticker']))}</td>"
        f"<td style=\"padding:10px;border-bottom:1px solid #e2e8f0;color:#334155;\">{html_escape(str(item['title']))}</td>"
        f"<td style=\"padding:10px;border-bottom:1px solid #e2e8f0;color:#334155;\">{html_escape(str(item['score_change']))}</td>"
        f"<td style=\"padding:10px;border-bottom:1px solid #e2e8f0;color:#334155;\">{html_escape(str(item['direction_change']))}</td>"
        f"<td style=\"padding:10px;border-bottom:1px solid #e2e8f0;color:#334155;\">{html_escape(str(item['timestamp']))}</td>"
        "</tr>"
        for item in items
    )
    return _table(["Ticker", "Title", "Score", "Direction", "Time"], rows)


def _signal_items_text(items: list[dict[str, Any]]) -> str:
    if not items:
        return "No notable signals."
    return "\n".join(
        f"- {item['ticker']}: score {item['signal_score']} | {item['direction']} | {item['why_notable']} | {item['source_stack']} | {item['date']}"
        for item in items
    )


def _signal_items_html(items: list[dict[str, Any]]) -> str:
    if not items:
        return "<p>No notable signals.</p>"
    rows = "".join(
        "<tr>"
        f"<td style=\"padding:10px;border-bottom:1px solid #e2e8f0;font-weight:700;color:#0f172a;\">{html_escape(str(item['ticker']))}</td>"
        f"<td style=\"padding:10px;border-bottom:1px solid #e2e8f0;color:#334155;\">{html_escape(str(item['signal_score']))}</td>"
        f"<td style=\"padding:10px;border-bottom:1px solid #e2e8f0;color:#334155;\">{html_escape(str(item['direction']))}</td>"
        f"<td style=\"padding:10px;border-bottom:1px solid #e2e8f0;color:#334155;\">{html_escape(str(item['why_notable']))}</td>"
        "</tr>"
        for item in items
    )
    return _table(["Ticker", "Score", "Direction", "Why"], rows)


def _table(headers: list[str], rows: str) -> str:
    head = "".join(f"<th align=\"left\" style=\"padding:10px;background:#ecfeff;color:#0f766e;font-size:12px;\">{html_escape(label)}</th>" for label in headers)
    return f"<table role=\"presentation\" width=\"100%\" cellspacing=\"0\" cellpadding=\"0\" style=\"border-collapse:collapse;border:1px solid #dbe6ea;border-radius:6px;overflow:hidden;\"><thead><tr>{head}</tr></thead><tbody>{rows}</tbody></table>"


def _mark_subscription_delivered(db: Session, subscription: NotificationSubscription | None, result: dict[str, Any]) -> None:
    if subscription is None or result.get("status") not in {"sent", "log_only"}:
        return
    subscription.last_delivered_at = datetime.now(timezone.utc)
    subscription.updated_at = subscription.last_delivered_at
    db.commit()


def _provider_name() -> str:
    return os.getenv("EMAIL_PROVIDER", "resend").strip().lower() or "resend"


def _digest_key(template_key: str, user_id: int, watchlist_id: int | None, start: datetime, end: datetime) -> str:
    watchlist_part = f":watchlist:{watchlist_id}" if watchlist_id is not None else ""
    start_key, end_key = _window_key(start, end)
    return f"digest:{template_key}:user:{user_id}{watchlist_part}:window:{start_key}:{end_key}"


def _window_key(start: datetime, end: datetime) -> tuple[str, str]:
    start_utc = start.astimezone(timezone.utc) if start.tzinfo else start.replace(tzinfo=timezone.utc)
    end_utc = end.astimezone(timezone.utc) if end.tzinfo else end.replace(tzinfo=timezone.utc)
    if end_utc - start_utc >= timedelta(hours=12):
        return start_utc.date().isoformat(), end_utc.date().isoformat()
    return start_utc.strftime("%Y-%m-%dT%H:%MZ"), end_utc.strftime("%Y-%m-%dT%H:%MZ")


def _frontend_base_url() -> str:
    return (
        os.getenv("FRONTEND_BASE_URL")
        or os.getenv("APP_BASE_URL")
        or os.getenv("NEXT_PUBLIC_APP_BASE_URL")
        or "https://app.walnut-intel.com"
    ).rstrip("/")


def _signal_url(ticker: str) -> str:
    if ticker and ticker != "Daily signal alerts":
        return f"{_frontend_base_url()}/ticker/{ticker}"
    return f"{_frontend_base_url()}/signals"


def _first_name(user: UserAccount) -> str:
    return (user.first_name or user.name or "there").strip().split(" ", 1)[0] or "there"


def _loads_dict(value: str | None) -> dict[str, Any]:
    if not value:
        return {}
    try:
        parsed = json.loads(value)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _amount(min_value: int | None, max_value: int | None) -> str:
    if min_value is None and max_value is None:
        return "n/a"
    if min_value is not None and max_value is not None:
        return f"${min_value:,.0f} - ${max_value:,.0f}"
    value = max_value if max_value is not None else min_value
    return f"${value:,.0f}"


def _money(cents: int | None, currency: str) -> str:
    if cents is None:
        return "0.00"
    return f"{(int(cents) / 100):,.2f}"


def _format_date(value: datetime | date | None) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, datetime):
        return value.date().isoformat()
    return value.isoformat()


def _format_datetime(value: datetime | None) -> str:
    if value is None:
        return "n/a"
    return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def _score_change(before: int | None, after: int | None) -> str:
    if before is None:
        return f"new to {after or 0}"
    return f"{before} -> {after or 0}"


def _direction_change(before: str | None, after: str | None) -> str:
    if not before:
        return after or "neutral"
    if before == after:
        return after or "neutral"
    return f"{before} -> {after or 'neutral'}"


def _direction_from_trade(value: str | None) -> str:
    normalized = (value or "").lower()
    if "purchase" in normalized or "buy" in normalized or normalized == "p":
        return "bullish"
    if "sale" in normalized or "sell" in normalized or normalized == "s":
        return "bearish"
    return "mixed"


def _count_summary(count: int, singular: str, plural: str) -> str:
    if count == 0:
        return f"No {plural} in this window."
    if count == 1:
        return f"1 {singular} in this window."
    return f"{count} {plural} in this window."


def _coerce_window_start(value: datetime | date) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return datetime.combine(value, time.min, tzinfo=timezone.utc)


def _coerce_window_end(value: datetime | date) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return datetime.combine(value, time.min, tzinfo=timezone.utc)


def _int_value(value: str | None) -> int | None:
    try:
        return int(str(value))
    except Exception:
        return None
