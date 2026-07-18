from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from html import escape as html_escape
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from app.auth import normalize_email
from app.entitlements import entitlements_for_user
from app.models import (
    BillingTransaction,
    ConfirmationMonitoringEvent,
    EmailDelivery,
    EmailTemplate,
    Event,
    MonitoringAlert,
    NotificationSubscription,
    Security,
    UserAccount,
    Watchlist,
    WatchlistItem,
)
from app.services.email_delivery import (
    email_delivery_enabled,
    log_sender_resolution,
    resolve_sender_for_template,
    send_email,
)
from app.services.email_renderer import render_template_string
from app.services.email_templates import reset_email_template_to_default, seed_default_email_templates
from app.services.event_calendar import upcoming_event_calendar_items
from app.services.institutional_activity import INSTITUTIONAL_EVENT_TYPES
from app.services.monitoring_titles import resolve_insider_name

ALERT_EVENT_TYPES = (
    "congress_trade",
    "congress_trade_new",
    "insider_trade",
    "insider_trade_new",
    "institutional_buy",
    *INSTITUTIONAL_EVENT_TYPES,
    "institutional_activity_change",
    "government_contract",
    "government_contract_new",
    "government_contract_award",
    "contract_award",
    "government_exposure",
    "price_volume_change",
    "price_volume_signal",
    "unusual_price_volume",
    "volume_surge",
    "technical_breakout",
    "technical_breakdown",
    "price_volume_flip",
    "fundamental_change",
    "fundamentals_change",
    "fundamentals_flip",
    "signal",
)
INSTITUTIONAL_ALERT_TYPES = (*INSTITUTIONAL_EVENT_TYPES, "institutional_activity")
GOVERNMENT_CONTRACT_ALERT_TYPES = (
    "government_contract",
    "government_contract_new",
    "government_contract_award",
    "contract_award",
    "government_exposure",
)
PRICE_VOLUME_ALERT_TYPES = (
    "price_volume_change",
    "price_volume_signal",
    "unusual_price_volume",
    "volume_surge",
    "technical_breakout",
    "technical_breakdown",
    "price_volume_flip",
)
FUNDAMENTAL_ALERT_TYPES = (
    "fundamental_change",
    "fundamentals_change",
    "fundamentals_flip",
)
SOURCE_MONITORING_ALERT_TYPES = (
    "congress_trade",
    "congress_trade_new",
    "insider_trade",
    "insider_trade_new",
    *GOVERNMENT_CONTRACT_ALERT_TYPES,
    "institutional_buy",
    *INSTITUTIONAL_EVENT_TYPES,
    "institutional_activity",
    "institutional_activity_change",
    *PRICE_VOLUME_ALERT_TYPES,
    *FUNDAMENTAL_ALERT_TYPES,
)
SUPPORT_EMAIL = "support@walnutmarkets.com"
DEFAULT_DIGEST_TIMEZONE = "America/Los_Angeles"
SEND_LIKE_STATUSES = {"sent", "log_only", "queued"}
DUPLICATE_BLOCKING_STATUSES = SEND_LIKE_STATUSES | {"skipped"}
WATCHLIST_DISPLAY_LIMIT = 10
WATCHLIST_FETCH_LIMIT = 25
SIGNAL_DISPLAY_LIMIT = 10
SIGNAL_ALLOWED_DIRECTIONS = {"bullish", "bearish", "mixed", "neutral"}
SIGNAL_REFRESH_TOKENS = ("refresh", "refreshed", "status", "sync", "screen refreshed")
CALENDAR_EVENT_KINDS = ("economic", "earnings", "dividend", "ipo", "split")
CALENDAR_EVENT_LABELS = {
    "economic": "Economic",
    "earnings": "Earnings",
    "dividend": "Dividends",
    "ipo": "IPOs",
    "split": "Splits",
}


@dataclass(frozen=True)
class DigestBuild:
    template_key: str
    context: dict[str, Any]
    items_count: int
    summary: str
    items: list[dict[str, Any]]
    diagnostics: dict[str, Any] = field(default_factory=dict)


def build_watchlist_activity_digest(db: Session, user: UserAccount, watchlist: Watchlist, since: datetime) -> DigestBuild:
    rows = _watchlist_events(db, watchlist.id, since=since, limit=WATCHLIST_FETCH_LIMIT, user=user)
    items = [_event_item(row) for row in rows]
    total_count = _watchlist_events_count(db, watchlist.id, since=since, user=user)
    summary = _count_summary(total_count, "new filing or event", "new filings or events")
    return DigestBuild(
        template_key="alerts.watchlist_activity",
        items_count=total_count,
        summary=summary,
        items=items,
        context={
            "first_name": _first_name(user),
            "watchlist_name": watchlist.name,
            "summary": summary,
            "items_text": _watchlist_items_text(items, total_count=total_count),
            "items_html": _watchlist_items_html(items, total_count=total_count),
            "activity_url": f"{_frontend_base_url()}/watchlists/{watchlist.id}",
        },
    )


def send_watchlist_activity_digest(
    db: Session,
    user: UserAccount,
    watchlist: Watchlist,
    since: datetime,
    window_end: datetime | None = None,
    force: bool = False,
) -> dict[str, Any]:
    template_key = "alerts.watchlist_activity"
    window_end = _coerce_aware(window_end or datetime.now(timezone.utc))
    since = _coerce_aware(since)
    subscription = _watchlist_subscription(db, user, watchlist)
    skip = _alert_skip_reason(user, "watchlist_activity")
    only_if_new = True if subscription is None else bool(subscription.only_if_new)
    if skip is None and subscription is None and not force:
        skip = "watchlist_digest_inactive"
    if skip is None and subscription is not None and not subscription.active and not force:
        skip = "watchlist_digest_inactive"
    if skip is None and subscription is not None and not _subscription_daily_digest_enabled(subscription) and not force:
        skip = "watchlist_daily_digest_disabled"

    digest = build_watchlist_activity_digest(db, user, watchlist, since)
    idempotency_key = None if force else _digest_key(template_key, user.id, watchlist.id, since, window_end)
    duplicate = _duplicate_digest_result(db, idempotency_key)
    if duplicate:
        return _with_digest_meta(duplicate, digest, since, window_end, idempotency_key)
    if skip:
        return _with_digest_meta(
            _log_skip(db, user=user, template_key=template_key, category="alerts", context=digest.context, idempotency_key=idempotency_key, reason=skip),
            digest,
            since,
            window_end,
            idempotency_key,
        )
    if only_if_new and digest.items_count == 0 and not force:
        return _with_digest_meta(
            _log_skip(db, user=user, template_key=template_key, category="alerts", context=digest.context, idempotency_key=idempotency_key, reason="no_new_items"),
            digest,
            since,
            window_end,
            idempotency_key,
        )
    result = _send_digest(db, user=user, digest=digest, category="alerts", idempotency_key=idempotency_key)
    _mark_subscription_delivered(db, subscription, result)
    return _with_digest_meta(result, digest, since, window_end, idempotency_key)


def build_monitoring_digest(
    db: Session,
    user: UserAccount,
    watchlist: Watchlist,
    since: datetime,
    window_end: datetime | None = None,
) -> DigestBuild:
    confirmation_query = (
        select(ConfirmationMonitoringEvent)
        .where(ConfirmationMonitoringEvent.user_id == user.id)
        .where(ConfirmationMonitoringEvent.watchlist_id == watchlist.id)
        .where(ConfirmationMonitoringEvent.created_at >= since)
        .order_by(ConfirmationMonitoringEvent.created_at.desc(), ConfirmationMonitoringEvent.id.desc())
        .limit(12)
    )
    if not _user_can_view_institutional_activity(db, user):
        confirmation_query = confirmation_query.where(ConfirmationMonitoringEvent.event_type.notin_(INSTITUTIONAL_ALERT_TYPES))
    confirmation_rows = (
        db.execute(confirmation_query)
        .scalars()
        .all()
    )
    alert_query = (
        select(MonitoringAlert)
        .where(MonitoringAlert.user_id == user.id)
        .where(MonitoringAlert.source_type == "watchlist")
        .where(MonitoringAlert.source_id == str(watchlist.id))
        .where(MonitoringAlert.dismissed_at.is_(None))
        .where(MonitoringAlert.event_created_at >= since)
        .order_by(MonitoringAlert.event_created_at.desc(), MonitoringAlert.id.desc())
        .limit(12)
    )
    alert_rows = (
        db.execute(_exclude_institutional_alerts_for_user(db, user, alert_query))
        .scalars()
        .all()
    )
    items = _qualify_monitoring_items(
        sorted(
            [_monitoring_item(row) for row in confirmation_rows] + [_monitoring_alert_item(row) for row in alert_rows],
            key=lambda item: str(item.get("sort_timestamp") or ""),
            reverse=True,
        )[:12]
    )
    items = sorted(
        items,
        key=lambda item: str(item.get("sort_timestamp") or ""),
        reverse=True,
    )[:12]
    summary = _count_summary(len(items), "monitoring change", "monitoring changes")
    digest_date = _format_window_label(since, window_end or datetime.now(timezone.utc))
    return DigestBuild(
        template_key="alerts.monitoring_digest",
        items_count=len(items),
        summary=summary,
        items=items,
        context={
            "first_name": _first_name(user),
            "watchlist_name": watchlist.name,
            "digest_date": digest_date,
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
    window_end: datetime | None = None,
    force: bool = False,
) -> dict[str, Any]:
    return send_signal_alert_digest(db, user, since, window_end=window_end, force=force)


def build_signal_alert_digest(
    db: Session,
    user: UserAccount,
    since: datetime,
    watchlist: Watchlist | None = None,
    window_end: datetime | None = None,
) -> DigestBuild:
    alert_rows = _signal_monitoring_alerts(db, user, since=since, limit=SIGNAL_DISPLAY_LIMIT)
    confirmation_rows = _signal_confirmation_events(db, user, since=since, watchlist=watchlist, limit=SIGNAL_DISPLAY_LIMIT)
    # The public Monitoring digest is a qualified ranked board. Candidates may
    # originate from MonitoringAlert rows, but broken or internal
    # monitoring changes are gated out before they reach public email content.
    raw_items = [_signal_alert_item(row) for row in alert_rows] + [_confirmation_signal_item(row) for row in confirmation_rows]
    items, diagnostics = _qualify_signal_items(raw_items)
    _attach_company_names(db, items)
    items = sorted(items, key=_signal_rank_key, reverse=True)[:SIGNAL_DISPLAY_LIMIT]
    lead = items[0] if items else {}
    is_single = len(items) == 1
    ticker = str(lead.get("ticker") or "Monitoring digest")
    signal_title = "Monitoring digest"
    signal_subject = "Walnut monitoring digest"
    signal_intro = f"Your ranked monitoring candidates for {_format_window_label(since, window_end or datetime.now(timezone.utc))}."
    summary = _count_summary(len(items), "monitoring candidate", "monitoring candidates")
    upcoming_events, calendar_filters_text = _upcoming_calendar_events_for_digest(db, user, window_end=window_end)
    _attach_calendar_company_names(db, upcoming_events)
    return DigestBuild(
        template_key="alerts.signal_alert",
        items_count=len(items),
        summary=summary,
        items=items,
        context={
            "first_name": _first_name(user),
            "signal_subject": signal_subject,
            "signal_title": signal_title,
            "signal_intro": signal_intro,
            "signal_cta_label": f"View {ticker} monitoring" if is_single else "Review monitoring",
            "ticker": ticker,
            "signal_score": _score_display(lead.get("signal_score")),
            "direction": str(lead.get("direction") or "No qualified monitoring candidates"),
            "why_notable": str(lead.get("why_notable") or summary),
            "source_stack": str(lead.get("source_stack") or "Qualified Walnut monitoring candidates"),
            "cautions": "Review source context before acting.",
            "signals_text": _signal_items_text(items),
            "signals_html": _signal_items_html(items),
            "upcoming_events_text": _calendar_items_text(upcoming_events),
            "upcoming_events_html": _calendar_items_html(upcoming_events),
            "calendar_alert_filters_text": calendar_filters_text,
            "signal_url": str(lead.get("href") or _signal_url(ticker)) if is_single else f"{_frontend_base_url()}/signals",
        },
        diagnostics=diagnostics,
    )


def send_signal_alert_digest(
    db: Session,
    user: UserAccount,
    since: datetime,
    window_end: datetime | None = None,
    force: bool = False,
) -> dict[str, Any]:
    template_key = "alerts.signal_alert"
    window_end = _coerce_aware(window_end or datetime.now(timezone.utc))
    since = _coerce_aware(since)
    digest = build_signal_alert_digest(db, user, since, window_end=window_end)
    idempotency_key = None if force else _digest_key(template_key, user.id, None, since, window_end)
    duplicate = _duplicate_digest_result(db, idempotency_key)
    if duplicate:
        return _with_digest_meta(duplicate, digest, since, window_end, idempotency_key)
    skip = _alert_skip_reason(user, "signals")
    if skip:
        return _with_digest_meta(
            _log_skip(db, user=user, template_key=template_key, category="alerts", context=digest.context, idempotency_key=idempotency_key, reason=skip),
            digest,
            since,
            window_end,
            idempotency_key,
        )
    if digest.items_count == 0 and not force:
        return _with_digest_meta(
            _log_skip(db, user=user, template_key=template_key, category="alerts", context=digest.context, idempotency_key=idempotency_key, reason="no_qualified_signals"),
            digest,
            since,
            window_end,
            idempotency_key,
        )
    return _with_digest_meta(
        _send_digest(db, user=user, digest=digest, category="alerts", idempotency_key=idempotency_key),
        digest,
        since,
        window_end,
        idempotency_key,
    )


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
    duplicate = _duplicate_digest_result(db, idempotency_key)
    if duplicate:
        return duplicate
    if not _valid_user_email(user):
        return _log_skip(db, user=user, template_key=template_key, category="billing", context=digest.context, idempotency_key=idempotency_key, reason="invalid_email")
    return _with_preview(_send_digest(db, user=user, digest=digest, category="billing", idempotency_key=idempotency_key), digest)


def run_digest_job(
    db: Session,
    *,
    kind: str,
    lookback_days: int = 1,
    limit: int = 100,
    force: bool = False,
    dry_run: bool = False,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    since, window_end = daily_digest_window(lookback_days=lookback_days, now=now)
    results: list[dict[str, Any]] = []
    if kind in {"monitoring", "signals"}:
        users = _eligible_monitoring_digest_users(db, limit=limit)
        if dry_run:
            return [_preview_signal_alert_digest(db, user, since, window_end, force=force) for user in users]
        return [send_signal_alert_digest(db, user, since, window_end=window_end, force=force) for user in users]

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
    for subscription in subscriptions:
        user = db.execute(select(UserAccount).where(func.lower(UserAccount.email) == normalize_email(subscription.email))).scalar_one_or_none()
        watchlist_id = _int_value(subscription.source_id)
        watchlist = db.get(Watchlist, watchlist_id) if watchlist_id is not None else None
        if not user or not watchlist:
            continue
        if kind == "watchlist_activity":
            results.append(
                _preview_watchlist_activity_digest(db, user, watchlist, since, window_end, force=force)
                if dry_run
                else send_watchlist_activity_digest(db, user, watchlist, since, window_end=window_end, force=force)
            )
    return results


def summarize_digest_results(results: list[dict[str, Any]]) -> dict[str, Any]:
    sent = sum(1 for item in results if item.get("status") == "sent")
    log_only = sum(1 for item in results if item.get("status") == "log_only")
    queued = sum(1 for item in results if item.get("status") == "queued")
    failed = sum(1 for item in results if item.get("status") == "failed")
    skipped = sum(1 for item in results if item.get("status") == "skipped")
    would_send = sum(1 for item in results if item.get("status") == "would_send")
    item_count = sum(int(item.get("item_count") or item.get("items_count") or 0) for item in results)
    candidate_count = sum(int(item.get("candidate_count") or 0) for item in results)
    qualified_count = sum(int(item.get("qualified_count") or item.get("item_count") or item.get("items_count") or 0) for item in results)
    excluded_count = sum(int(item.get("excluded_count") or 0) for item in results)
    excluded_reasons: dict[str, int] = {}
    for item in results:
        reasons = item.get("excluded_reasons") or {}
        if not isinstance(reasons, dict):
            continue
        for reason, count in reasons.items():
            excluded_reasons[str(reason)] = excluded_reasons.get(str(reason), 0) + int(count or 0)
    return {
        "total": len(results),
        "sent": sent,
        "log_only": log_only,
        "queued": queued,
        "failed": failed,
        "skipped": skipped,
        "would_send": would_send,
        "item_count": item_count,
        "candidate_count": candidate_count,
        "qualified_count": qualified_count,
        "excluded_count": excluded_count,
        "excluded_reasons": excluded_reasons,
    }


def daily_digest_window(
    *,
    lookback_days: int = 1,
    now: datetime | None = None,
    timezone_name: str = DEFAULT_DIGEST_TIMEZONE,
) -> tuple[datetime, datetime]:
    tz = ZoneInfo(timezone_name)
    current = now or datetime.now(timezone.utc)
    current = current if current.tzinfo else current.replace(tzinfo=timezone.utc)
    local_now = current.astimezone(tz)
    local_end = datetime.combine(local_now.date(), time.min, tzinfo=tz)
    if local_now.time() == time.min:
        local_end = local_now
    days = max(int(lookback_days or 1), 1)
    local_start = local_end - timedelta(days=days)
    return local_start.astimezone(timezone.utc), local_end.astimezone(timezone.utc)


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
    template = _template(db, digest.template_key)
    if not template.enabled:
        return _log_skip(
            db,
            user=user,
            template_key=digest.template_key,
            category=category,
            context=digest.context,
            idempotency_key=idempotency_key,
            reason="template_disabled",
        )
    if not email_delivery_enabled():
        return _log_skip(
            db,
            user=user,
            template_key=digest.template_key,
            category=category,
            context=digest.context,
            idempotency_key=idempotency_key,
            reason="delivery_disabled",
        )
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
    delivery_idempotency_key = f"skip:{reason}:{idempotency_key}" if idempotency_key else None
    if delivery_idempotency_key:
        existing = db.execute(select(EmailDelivery).where(EmailDelivery.idempotency_key == delivery_idempotency_key)).scalar_one_or_none()
        if existing:
            return _delivery_result(existing) | {"status": "skipped", "error": reason}
    template = _template(db, template_key)
    sender = resolve_sender_for_template(template)
    log_sender_resolution(template.template_key, sender)
    delivery = EmailDelivery(
        user_id=user.id,
        to_email=normalize_email(user.email),
        from_email=sender.from_email,
        template_key=template.template_key,
        category=category,
        subject=_render_subject(template, context),
        provider=_provider_name(),
        status="skipped",
        idempotency_key=delivery_idempotency_key,
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
        if template_key == "alerts.monitoring_digest" and "Your Walnut monitoring digest for {{digest_date}} is ready." in (template.body_text or ""):
            template = reset_email_template_to_default(db, template_key) or template
        if template_key == "alerts.signal_alert" and template.subject == "Walnut signal alert: {{ticker}}":
            template = reset_email_template_to_default(db, template_key) or template
        if template_key == "alerts.signal_alert" and template.subject == "Walnut signal digest":
            template = reset_email_template_to_default(db, template_key) or template
        if template_key == "alerts.signal_alert" and template.name in {"Signal alert", "Signal digest"}:
            template = reset_email_template_to_default(db, template_key) or template
        if template_key == "alerts.signal_alert" and template.preheader == "Daily summary of Walnut Market Terminal signal activity.":
            template = reset_email_template_to_default(db, template_key) or template
        if template_key == "alerts.signal_alert" and template.preheader == "Your ranked signal candidates.":
            template = reset_email_template_to_default(db, template_key) or template
        if template_key == "alerts.signal_alert" and "calendar_alert_filters_text" not in (template.variables_json or ""):
            template = reset_email_template_to_default(db, template_key) or template
        if template_key == "alerts.signal_intraday" and template.name == "Intraday signal alert":
            template = reset_email_template_to_default(db, template_key) or template
        if template_key == "alerts.signal_intraday" and template.subject == "Walnut high-conviction signal: {{ticker}}":
            template = reset_email_template_to_default(db, template_key) or template
        if template_key == "alerts.watchlist_activity" and template.name == "Watchlist activity alert":
            template = reset_email_template_to_default(db, template_key) or template
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
        "idempotency_key": delivery.idempotency_key,
        "error": delivery.error,
    }


def _duplicate_digest_result(db: Session, idempotency_key: str | None) -> dict[str, Any] | None:
    if not idempotency_key:
        return None
    existing = db.execute(select(EmailDelivery).where(EmailDelivery.idempotency_key == idempotency_key)).scalar_one_or_none()
    if existing is None:
        return None
    if existing.status not in DUPLICATE_BLOCKING_STATUSES:
        return None
    return _delivery_result(existing) | {"status": "skipped", "error": "duplicate_window_already_sent"}


def _with_digest_meta(
    result: dict[str, Any],
    digest: DigestBuild,
    window_start: datetime,
    window_end: datetime,
    idempotency_key: str | None,
) -> dict[str, Any]:
    diagnostics = digest.diagnostics or {}
    return {
        **result,
        "skip_reason": result.get("error") if result.get("status") == "skipped" else None,
        "item_count": digest.items_count,
        "items_count": digest.items_count,
        "candidate_count": diagnostics.get("candidate_count", digest.items_count),
        "qualified_count": diagnostics.get("qualified_count", digest.items_count),
        "excluded_count": diagnostics.get("excluded_count", 0),
        "excluded_reasons": diagnostics.get("excluded_reasons", {}),
        "window_start": _coerce_aware(window_start),
        "window_end": _coerce_aware(window_end),
        "idempotency_key": idempotency_key,
        "rendered_preview": {
            "summary": digest.summary,
            "items_count": digest.items_count,
            "window_label": digest.context.get("digest_date"),
            "sample_items": digest.items[:3],
            "diagnostics": diagnostics,
        },
    }


def _with_preview(result: dict[str, Any], digest: DigestBuild) -> dict[str, Any]:
    diagnostics = digest.diagnostics or {}
    return {
        **result,
        "item_count": digest.items_count,
        "items_count": digest.items_count,
        "candidate_count": diagnostics.get("candidate_count", digest.items_count),
        "qualified_count": diagnostics.get("qualified_count", digest.items_count),
        "excluded_count": diagnostics.get("excluded_count", 0),
        "excluded_reasons": diagnostics.get("excluded_reasons", {}),
        "skip_reason": result.get("error") if result.get("status") == "skipped" else None,
        "rendered_preview": {
            "summary": digest.summary,
            "items_count": digest.items_count,
            "sample_items": digest.items[:3],
            "diagnostics": diagnostics,
        },
    }


def _preview_watchlist_activity_digest(
    db: Session,
    user: UserAccount,
    watchlist: Watchlist,
    since: datetime,
    window_end: datetime,
    *,
    force: bool = False,
) -> dict[str, Any]:
    template_key = "alerts.watchlist_activity"
    digest = build_watchlist_activity_digest(db, user, watchlist, since)
    subscription = _watchlist_subscription(db, user, watchlist)
    skip = _alert_skip_reason(user, "watchlist_activity")
    only_if_new = True if subscription is None else bool(subscription.only_if_new)
    if skip is None and subscription is None and not force:
        skip = "watchlist_digest_inactive"
    if skip is None and subscription is not None and not subscription.active and not force:
        skip = "watchlist_digest_inactive"
    if skip is None and only_if_new and digest.items_count == 0 and not force:
        skip = "no_new_items"
    idempotency_key = None if force else _digest_key(template_key, user.id, watchlist.id, since, window_end)
    duplicate = _duplicate_digest_result(db, idempotency_key)
    if duplicate:
        skip = duplicate.get("error") or "duplicate_window_already_sent"
    return _with_digest_meta(
        _preview_result(user, template_key=template_key, skip_reason=skip),
        digest,
        since,
        window_end,
        idempotency_key,
    )


def _preview_monitoring_digest(
    db: Session,
    user: UserAccount,
    watchlist: Watchlist,
    since: datetime,
    window_end: datetime,
    *,
    force: bool = False,
) -> dict[str, Any]:
    template_key = "alerts.monitoring_digest"
    digest = build_monitoring_digest(db, user, watchlist, since, window_end=window_end)
    subscription = _watchlist_subscription(db, user, watchlist)
    skip = _alert_skip_reason(user, "watchlist_activity")
    only_if_new = True if subscription is None else bool(subscription.only_if_new)
    if skip is None and subscription is None and not force:
        skip = "watchlist_digest_inactive"
    if skip is None and subscription is not None and not subscription.active and not force:
        skip = "watchlist_digest_inactive"
    if skip is None and only_if_new and digest.items_count == 0 and not force:
        skip = "no_new_items"
    idempotency_key = None if force else _digest_key(template_key, user.id, watchlist.id, since, window_end)
    duplicate = _duplicate_digest_result(db, idempotency_key)
    if duplicate:
        skip = duplicate.get("error") or "duplicate_window_already_sent"
    return _with_digest_meta(
        _preview_result(user, template_key=template_key, skip_reason=skip),
        digest,
        since,
        window_end,
        idempotency_key,
    )


def _preview_signal_alert_digest(
    db: Session,
    user: UserAccount,
    since: datetime,
    window_end: datetime,
    *,
    force: bool = False,
) -> dict[str, Any]:
    template_key = "alerts.signal_alert"
    digest = build_signal_alert_digest(db, user, since, window_end=window_end)
    skip = _alert_skip_reason(user, "signals")
    if skip is None and digest.items_count == 0 and not force:
        skip = "no_qualified_signals"
    idempotency_key = None if force else _digest_key(template_key, user.id, None, since, window_end)
    duplicate = _duplicate_digest_result(db, idempotency_key)
    if duplicate:
        skip = duplicate.get("error") or "duplicate_window_already_sent"
    return _with_digest_meta(
        _preview_result(user, template_key=template_key, skip_reason=skip),
        digest,
        since,
        window_end,
        idempotency_key,
    )


def _preview_result(user: UserAccount, *, template_key: str, skip_reason: str | None) -> dict[str, Any]:
    return {
        "id": None,
        "status": "skipped" if skip_reason else "would_send",
        "provider": _provider_name(),
        "provider_message_id": None,
        "template_key": template_key,
        "category": "alerts",
        "to_email": normalize_email(user.email),
        "error": skip_reason,
    }


def _alert_skip_reason(user: UserAccount, kind: str) -> str | None:
    if not _valid_user_email(user):
        return "invalid_email"
    if user.is_suspended:
        return "user_suspended"
    if not user.alerts_enabled:
        return "user_alerts_disabled"
    if not user.email_notifications_enabled:
        return "user_email_notifications_disabled"
    if kind == "watchlist_activity" and not user.watchlist_activity_notifications:
        return "user_alerts_disabled"
    if kind == "signals" and not user.watchlist_activity_notifications:
        return "user_alerts_disabled"
    if kind == "intraday_alerts" and not user.signals_notifications:
        return "user_alerts_disabled"
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


def _subscription_payload(subscription: NotificationSubscription) -> dict[str, Any]:
    return _loads_dict(subscription.source_payload_json)


def _subscription_daily_digest_enabled(subscription: NotificationSubscription) -> bool:
    payload = _subscription_payload(subscription)
    value = payload.get("daily_digest_enabled")
    return bool(subscription.active) if value is None else bool(value)


def _eligible_monitoring_digest_users(db: Session, *, limit: int) -> list[UserAccount]:
    users = _eligible_users(db, limit=limit)
    eligible: list[UserAccount] = []
    for user in users:
        subscriptions = (
            db.execute(
                select(NotificationSubscription)
                .where(func.lower(NotificationSubscription.email) == normalize_email(user.email))
                .where(NotificationSubscription.source_type == "watchlist")
                .order_by(NotificationSubscription.updated_at.desc(), NotificationSubscription.id.desc())
            )
            .scalars()
            .all()
        )
        if not subscriptions or any(_subscription_daily_digest_enabled(subscription) for subscription in subscriptions):
            eligible.append(user)
    return eligible


def _event_calendar_subscription(db: Session, user: UserAccount) -> NotificationSubscription | None:
    return (
        db.execute(
            select(NotificationSubscription)
            .where(NotificationSubscription.source_type == "event_calendar")
            .where(func.lower(NotificationSubscription.email) == normalize_email(user.email))
            .order_by(NotificationSubscription.updated_at.desc(), NotificationSubscription.id.desc())
            .limit(1)
        )
        .scalars()
        .first()
    )


def _calendar_kinds_for_subscription(subscription: NotificationSubscription | None) -> tuple[str, ...]:
    if subscription is None:
        return CALENDAR_EVENT_KINDS
    try:
        payload = json.loads(subscription.source_payload_json or "{}")
    except Exception:
        payload = {}
    raw_kinds = payload.get("calendar_kinds") if isinstance(payload, dict) else None
    if raw_kinds is None:
        return CALENDAR_EVENT_KINDS
    if not isinstance(raw_kinds, list):
        return CALENDAR_EVENT_KINDS
    selected: list[str] = []
    for kind in raw_kinds:
        key = str(kind)
        if key in CALENDAR_EVENT_KINDS and key not in selected:
            selected.append(key)
    return tuple(selected)


def _calendar_filter_label(kinds: tuple[str, ...]) -> str:
    if not kinds:
        return "None"
    if set(kinds) == set(CALENDAR_EVENT_KINDS):
        return "Economic, Earnings, Dividends, IPOs, Splits"
    return ", ".join(CALENDAR_EVENT_LABELS.get(kind, kind.title()) for kind in kinds)


def _upcoming_calendar_events_for_digest(
    db: Session,
    user: UserAccount,
    *,
    window_end: datetime | None,
) -> tuple[list[dict[str, Any]], str]:
    if not entitlements_for_user(db, user).has_feature("event_calendar"):
        return [], _calendar_filter_label(CALENDAR_EVENT_KINDS)
    subscription = _event_calendar_subscription(db, user)
    if subscription is not None and (not subscription.active or subscription.source_id == "none"):
        return [], "None"
    enabled_kinds = _calendar_kinds_for_subscription(subscription)
    if not enabled_kinds:
        return [], "None"
    anchor = _coerce_aware(window_end or datetime.now(timezone.utc)).date()
    try:
        result = upcoming_event_calendar_items(
            db,
            user,
            start=anchor,
            end=anchor + timedelta(days=7),
            scope="watchlist",
            limit=12,
            kinds=enabled_kinds,
        )
    except Exception:
        return [], _calendar_filter_label(enabled_kinds)
    return result.items, _calendar_filter_label(enabled_kinds)


def _watchlist_events(db: Session, watchlist_id: int, *, since: datetime, limit: int, user: UserAccount) -> list[Event]:
    symbols = _watchlist_symbols(db, watchlist_id)
    if not symbols:
        return []
    activity_ts = func.coalesce(Event.event_date, Event.ts)
    event_types = _alert_event_types_for_user(db, user)
    return (
        db.execute(
            select(Event)
            .where(Event.symbol.is_not(None))
            .where(func.upper(Event.symbol).in_(symbols))
            .where(Event.event_type.in_(event_types))
            .where(activity_ts >= since)
            .order_by(activity_ts.desc(), Event.id.desc())
            .limit(limit)
        )
        .scalars()
        .all()
    )


def _watchlist_events_count(db: Session, watchlist_id: int, *, since: datetime, user: UserAccount) -> int:
    symbols = _watchlist_symbols(db, watchlist_id)
    if not symbols:
        return 0
    activity_ts = func.coalesce(Event.event_date, Event.ts)
    event_types = _alert_event_types_for_user(db, user)
    return int(
        db.execute(
            select(func.count())
            .select_from(Event)
            .where(Event.symbol.is_not(None))
            .where(func.upper(Event.symbol).in_(symbols))
            .where(Event.event_type.in_(event_types))
            .where(activity_ts >= since)
        ).scalar_one()
        or 0
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
    actor = _event_actor(event, payload)
    return {
        "ticker": (event.symbol or "").upper() or "UNKNOWN",
        "event_type": event.event_type.replace("_", " "),
        "actor": actor,
        "trade": event.trade_type or event.transaction_type or payload.get("action") or "activity",
        "amount": _amount(event.amount_min, event.amount_max),
        "date": _format_date(event.event_date or event.ts),
        "signal_score": _numeric_score(payload.get("smart_score") or payload.get("signal_score") or (round(event.impact_score) if event.impact_score else None)),
    }


def _event_actor(event: Event, payload: dict[str, Any]) -> str:
    raw = payload.get("raw") if isinstance(payload.get("raw"), dict) else {}
    if event.event_type == "insider_trade":
        return (
            resolve_insider_name(payload, event_member_name=event.member_name)
            or _clean_text(raw.get("reportingName"))
            or _clean_text(raw.get("insiderName"))
            or "Unresolved insider"
        )
    if event.event_type == "congress_trade":
        return _clean_text(event.member_name) or _clean_text(payload.get("member_name")) or "Unavailable"
    if event.event_type == "government_contract":
        return (
            _clean_text(payload.get("agency"))
            or _clean_text(payload.get("recipient"))
            or _clean_text(payload.get("recipient_name"))
            or _clean_text(payload.get("company"))
            or _clean_text(event.source)
            or "Unavailable"
        )
    return _clean_text(event.member_name) or _clean_text(payload.get("actor")) or _clean_text(event.source) or "Unavailable"


def _monitoring_item(event: ConfirmationMonitoringEvent) -> dict[str, Any]:
    payload = _loads_dict(event.payload_json)
    return {
        "ticker": _normalize_ticker(event.ticker),
        "title": event.title,
        "score_change": _score_change(event.score_before, event.score_after),
        "direction_change": _direction_change(event.direction_before, event.direction_after),
        "timestamp": _format_datetime(event.created_at),
        "sort_timestamp": _coerce_aware(event.created_at).isoformat() if event.created_at else "",
        "reason": event.body or payload.get("event_type") or event.event_type,
    }


def _monitoring_alert_item(alert: MonitoringAlert) -> dict[str, Any]:
    payload = _loads_dict(alert.payload_json)
    score = payload.get("score")
    event_payload = payload.get("event") if isinstance(payload.get("event"), dict) else {}
    if score is None and isinstance(event_payload, dict):
        score = event_payload.get("smart_score") or event_payload.get("confirmation_score")
    return {
        "ticker": _normalize_ticker(alert.symbol) if alert.symbol else "Unresolved security",
        "title": alert.title,
        "score_change": f"score {score}" if isinstance(score, (int, float)) else "--",
        "direction_change": str(payload.get("direction") or event_payload.get("direction") or "--"),
        "timestamp": _format_datetime(alert.event_created_at),
        "sort_timestamp": _coerce_aware(alert.event_created_at).isoformat() if alert.event_created_at else "",
        "reason": alert.body or alert.alert_type.replace("_", " "),
    }


def _signal_alert_item(alert: MonitoringAlert) -> dict[str, Any]:
    payload = _loads_dict(alert.payload_json)
    if not _is_signal_monitoring_alert(alert, payload):
        return {}
    score = payload.get("score")
    event_payload = payload.get("event") if isinstance(payload.get("event"), dict) else {}
    saved_screen_event = payload.get("saved_screen_event") if isinstance(payload.get("saved_screen_event"), dict) else {}
    after = saved_screen_event.get("after") if isinstance(saved_screen_event.get("after"), dict) else {}
    if score is None and isinstance(after, dict):
        score = after.get("confirmation_score") or after.get("smart_score")
    if score is None and isinstance(event_payload, dict):
        score = event_payload.get("smart_score") or event_payload.get("confirmation_score") or event_payload.get("signal_score")
    ticker = _normalize_ticker(alert.symbol or saved_screen_event.get("ticker") or event_payload.get("symbol"))
    direction = _normalize_direction(
        (after.get("direction") if isinstance(after, dict) else None)
        or payload.get("direction")
        or event_payload.get("direction")
    )
    why_notable = _clean_text(alert.title) or _clean_text(alert.body) or alert.alert_type.replace("_", " ")
    source_stack = _clean_text(payload.get("source_stack")) or _clean_text(alert.source_name) or alert.source_type.replace("_", " ")
    return {
        "ticker": ticker,
        "company_name": _clean_text(payload.get("company_name")) or _clean_text(event_payload.get("company_name")) or _clean_text(after.get("company_name")),
        "signal_score": _numeric_score(score),
        "direction": direction,
        "why_notable": why_notable,
        "source_stack": source_stack,
        "cautions": "Review source context before acting.",
        "date": _format_date(alert.event_created_at),
        "latest_event_date": _format_date(alert.event_created_at),
        "sort_timestamp": _coerce_aware(alert.event_created_at).isoformat() if alert.event_created_at else "",
        "href": _signal_url(ticker),
        "source_type": alert.source_type,
        "alert_type": alert.alert_type,
        "watchlist_boost": alert.source_type == "watchlist",
    }


def _confirmation_signal_item(event: ConfirmationMonitoringEvent) -> dict[str, Any]:
    score = event.score_after
    ticker = _normalize_ticker(event.ticker)
    return {
        "ticker": ticker,
        "company_name": None,
        "signal_score": _numeric_score(score),
        "direction": _normalize_direction(event.direction_after),
        "why_notable": event.title or event.event_type.replace("_", " "),
        "source_stack": "Confirmation monitoring",
        "cautions": "Review source context before acting.",
        "date": _format_date(event.created_at),
        "latest_event_date": _format_date(event.created_at),
        "sort_timestamp": _coerce_aware(event.created_at).isoformat() if event.created_at else "",
        "href": _signal_url(ticker),
        "source_type": "confirmation_monitoring",
        "alert_type": event.event_type,
        "watchlist_boost": event.watchlist_id is not None,
    }


def _signal_monitoring_alerts(db: Session, user: UserAccount, *, since: datetime, limit: int) -> list[MonitoringAlert]:
    signal_types = (
        "signal",
        "score_change",
        "new_multi_source_confirmation",
        "confirmation_upgraded",
        "direction_flipped",
        *SOURCE_MONITORING_ALERT_TYPES,
        "smart_score_threshold",
        "cross_source_confirmation",
    )
    query = (
        select(MonitoringAlert)
        .where(MonitoringAlert.user_id == user.id)
        .where(MonitoringAlert.dismissed_at.is_(None))
        .where(MonitoringAlert.event_created_at >= since)
        .where(or_(MonitoringAlert.source_type == "saved_screen", MonitoringAlert.alert_type.in_(signal_types)))
        .order_by(MonitoringAlert.event_created_at.desc(), MonitoringAlert.id.desc())
        .limit(limit)
    )
    return (
        db.execute(_exclude_institutional_alerts_for_user(db, user, query))
        .scalars()
        .all()
    )


def _user_can_view_institutional_activity(db: Session, user: UserAccount) -> bool:
    return entitlements_for_user(db, user).has_feature("institutional_feed")


def _alert_event_types_for_user(db: Session, user: UserAccount) -> tuple[str, ...]:
    if _user_can_view_institutional_activity(db, user):
        return ALERT_EVENT_TYPES
    return tuple(event_type for event_type in ALERT_EVENT_TYPES if event_type not in INSTITUTIONAL_EVENT_TYPES)


def _exclude_institutional_alerts_for_user(db: Session, user: UserAccount, query):
    if _user_can_view_institutional_activity(db, user):
        return query
    return query.where(MonitoringAlert.alert_type.notin_(INSTITUTIONAL_ALERT_TYPES))


def _signal_confirmation_events(
    db: Session,
    user: UserAccount,
    *,
    since: datetime,
    watchlist: Watchlist | None,
    limit: int,
) -> list[ConfirmationMonitoringEvent]:
    query = (
        select(ConfirmationMonitoringEvent)
        .where(ConfirmationMonitoringEvent.user_id == user.id)
        .where(ConfirmationMonitoringEvent.created_at >= since)
        .order_by(ConfirmationMonitoringEvent.created_at.desc(), ConfirmationMonitoringEvent.id.desc())
        .limit(limit)
    )
    if watchlist is not None:
        query = query.where(ConfirmationMonitoringEvent.watchlist_id == watchlist.id)
    if not _user_can_view_institutional_activity(db, user):
        query = query.where(ConfirmationMonitoringEvent.event_type.notin_(INSTITUTIONAL_ALERT_TYPES))
    return db.execute(query).scalars().all()


def _is_signal_monitoring_alert(alert: MonitoringAlert, payload: dict[str, Any]) -> bool:
    if alert.source_type == "saved_screen":
        return True
    if alert.alert_type in {
        "signal",
        "score_change",
        "new_multi_source_confirmation",
        "confirmation_upgraded",
        "direction_flipped",
        *SOURCE_MONITORING_ALERT_TYPES,
        "smart_score_threshold",
        "cross_source_confirmation",
    }:
        return True
    return bool(payload.get("saved_screen_event"))


def _qualify_signal_items(raw_items: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    candidates = [item for item in raw_items if item]
    qualified: list[dict[str, Any]] = []
    excluded_reasons: dict[str, int] = {}
    seen_tickers: set[str] = set()

    for item in sorted(candidates, key=_signal_rank_key, reverse=True):
        reason = _signal_exclusion_reason(item)
        ticker = str(item.get("ticker") or "")
        if reason is None and ticker in seen_tickers:
            reason = "duplicate"
        if reason is not None:
            excluded_reasons[reason] = excluded_reasons.get(reason, 0) + 1
            continue
        seen_tickers.add(ticker)
        qualified.append(item)

    return qualified, {
        "candidate_count": len(candidates),
        "qualified_count": len(qualified),
        "excluded_count": len(candidates) - len(qualified),
        "excluded_reasons": excluded_reasons,
    }


def _signal_exclusion_reason(item: dict[str, Any]) -> str | None:
    ticker = str(item.get("ticker") or "").strip().upper()
    if not ticker or ticker == "UNKNOWN":
        return "missing_ticker"
    if _is_internal_refresh_signal(item):
        return "internal_refresh_event"
    if _numeric_score(item.get("signal_score")) is None and not _is_source_monitoring_item(item):
        return "missing_score"
    if str(item.get("direction") or "").lower() not in SIGNAL_ALLOWED_DIRECTIONS:
        return "missing_direction"
    if not _clean_text(item.get("why_notable")):
        return "missing_reason"
    if not _clean_text(item.get("source_stack")):
        return "missing_source"
    if not _clean_text(item.get("href")):
        return "unresolved_security"
    return None


def _is_source_monitoring_item(item: dict[str, Any]) -> bool:
    return str(item.get("alert_type") or "").strip().lower() in SOURCE_MONITORING_ALERT_TYPES


def _is_internal_refresh_signal(item: dict[str, Any]) -> bool:
    text = " ".join(
        str(item.get(key) or "").lower()
        for key in ("why_notable", "alert_type", "source_type")
    )
    if "saved_screen" not in text and "screen" not in text:
        return False
    return any(token in text for token in SIGNAL_REFRESH_TOKENS)


def _signal_rank_key(item: dict[str, Any]) -> tuple[float, int, int, str]:
    score = float(_numeric_score(item.get("signal_score")) or -1)
    source = str(item.get("source_stack") or "").lower()
    cross_source_boost = 1 if any(token in source for token in ("multi", "cross", "+", "insiders", "volume")) else 0
    watchlist_boost = 1 if item.get("watchlist_boost") else 0
    return (score, watchlist_boost, cross_source_boost, str(item.get("sort_timestamp") or ""))


def _attach_company_names(db: Session, items: list[dict[str, Any]]) -> None:
    missing = sorted({str(item.get("ticker") or "") for item in items if item.get("ticker") and not item.get("company_name")})
    if not missing:
        return
    securities = db.execute(select(Security).where(func.upper(Security.symbol).in_(missing))).scalars().all()
    names = {security.symbol.upper(): security.name for security in securities if security.symbol and security.name}
    for item in items:
        ticker = str(item.get("ticker") or "").upper()
        item["company_name"] = item.get("company_name") or names.get(ticker)


def _attach_calendar_company_names(db: Session, items: list[dict[str, Any]]) -> None:
    missing = sorted(
        {
            str(item.get("symbol") or "").upper()
            for item in items
            if str(item.get("kind") or "").lower() == "ipo" and item.get("symbol") and not _clean_text(item.get("company"))
        }
    )
    if not missing:
        return
    securities = db.execute(select(Security).where(func.upper(Security.symbol).in_(missing))).scalars().all()
    names = {security.symbol.upper(): security.name for security in securities if security.symbol and security.name}
    for item in items:
        ticker = str(item.get("symbol") or "").upper()
        if str(item.get("kind") or "").lower() == "ipo" and ticker:
            item["company"] = item.get("company") or names.get(ticker)


def _qualify_monitoring_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    qualified: list[dict[str, Any]] = []
    for item in items:
        ticker = str(item.get("ticker") or "").strip().upper()
        title = _clean_text(item.get("title"))
        if not ticker or ticker == "UNKNOWN" or not title:
            continue
        if ticker == "UNRESOLVED SECURITY" and not _clean_text(item.get("reason")):
            continue
        qualified.append(item)
    return qualified


def _watchlist_items_text(items: list[dict[str, Any]], *, total_count: int | None = None) -> str:
    if not items:
        return "No new matching items."
    display_items = items[:WATCHLIST_DISPLAY_LIMIT]
    has_score = _has_any_score(display_items)
    suffix = _showing_summary_text(len(display_items), total_count if total_count is not None else len(items))
    lines = []
    for item in display_items:
        score = f" | score {_score_display(item.get('signal_score'), missing='--')}" if has_score else ""
        lines.append(
            f"- {item['ticker']} {item['event_type']} | {item['actor']} | {item['trade']} | {item['amount']} | {item['date']}{score}"
        )
    if suffix:
        lines.append(suffix)
    return "\n".join(lines)


def _watchlist_items_html(items: list[dict[str, Any]], *, total_count: int | None = None) -> str:
    if not items:
        return _empty_state_card("No watchlist activity in this window.")
    display_items = items[:WATCHLIST_DISPLAY_LIMIT]
    has_score = _has_any_score(display_items)
    headers = ["Ticker", "Event", "Actor", "Amount"]
    if has_score:
        headers.append("Score")
    rows = "".join(
        "<tr>"
        f"<td style=\"padding:10px;border-bottom:1px solid #e2e8f0;font-weight:700;color:#0f172a;\">{html_escape(str(item['ticker']))}</td>"
        f"<td style=\"padding:10px;border-bottom:1px solid #e2e8f0;color:#334155;\">{html_escape(str(item['event_type']))}</td>"
        f"<td style=\"padding:10px;border-bottom:1px solid #e2e8f0;color:#334155;\">{html_escape(str(item['actor']))}</td>"
        f"<td style=\"padding:10px;border-bottom:1px solid #e2e8f0;color:#334155;\">{html_escape(str(item['amount']))}</td>"
        + (
            f"<td style=\"padding:10px;border-bottom:1px solid #e2e8f0;color:#334155;\">{_score_display(item.get('signal_score'), missing='&mdash;')}</td>"
            if has_score
            else ""
        )
        + "</tr>"
        for item in display_items
    )
    summary = _showing_summary_html(len(display_items), total_count if total_count is not None else len(items))
    return _table(headers, rows) + summary


def _monitoring_items_text(items: list[dict[str, Any]]) -> str:
    if not items:
        return "No monitoring changes."
    return "\n".join(
        f"- {item['ticker']}: {item['title']} | score {item['score_change']} | direction {item['direction_change']} | {item['timestamp']} | {item['reason']}"
        for item in items
    )


def _monitoring_items_html(items: list[dict[str, Any]]) -> str:
    if not items:
        return _empty_state_card("No monitoring changes in this window.")
    rows = "".join(
        "<tr>"
        f"<td style=\"padding:10px;border-bottom:1px solid #e2e8f0;font-weight:700;color:#0f172a;\">{html_escape(str(item['ticker']))}</td>"
        f"<td style=\"padding:10px;border-bottom:1px solid #e2e8f0;color:#334155;\">{html_escape(str(item['title']))}</td>"
        f"<td style=\"padding:10px;border-bottom:1px solid #e2e8f0;color:#334155;\">{html_escape(str(item['score_change']))}</td>"
        f"<td style=\"padding:10px;border-bottom:1px solid #e2e8f0;color:#334155;\">{html_escape(str(item['direction_change']))}</td>"
        f"<td style=\"padding:10px;border-bottom:1px solid #e2e8f0;color:#334155;white-space:nowrap;\">{html_escape(str(item['timestamp']))}</td>"
        "</tr>"
        for item in items
    )
    return _table(["Ticker", "Title", "Score", "Direction", "Time"], rows)


def _signal_items_text(items: list[dict[str, Any]]) -> str:
    if not items:
        return "No qualified monitoring candidates in this window."
    return "\n".join(
        f"- {item['ticker']}: score {_score_display(item.get('signal_score'))} | {item['direction']} | {item['why_notable']} | {item['source_stack']} | {item['date']} | {item['href']}"
        for item in items
        if item
    )


def _signal_items_html(items: list[dict[str, Any]]) -> str:
    if not items:
        return _empty_state_card("No qualified monitoring candidates in this window.")
    rows = "".join(
        "<tr>"
        f"<td style=\"padding:10px;border-bottom:1px solid #e2e8f0;font-weight:700;color:#0f172a;\">{html_escape(str(item['ticker']))}</td>"
        f"<td style=\"padding:10px;border-bottom:1px solid #e2e8f0;color:#334155;\">{_score_display(item.get('signal_score'))}</td>"
        f"<td style=\"padding:10px;border-bottom:1px solid #e2e8f0;color:#334155;\">{html_escape(str(item['direction']))}</td>"
        f"<td style=\"padding:10px;border-bottom:1px solid #e2e8f0;color:#334155;\">{html_escape(str(item['why_notable']))}</td>"
        f"<td style=\"padding:10px;border-bottom:1px solid #e2e8f0;color:#334155;\">{html_escape(str(item['source_stack']))}</td>"
        f"<td style=\"padding:10px;border-bottom:1px solid #e2e8f0;color:#334155;\"><a href=\"{html_escape(str(item['href']))}\" style=\"color:#0f766e;font-weight:700;text-decoration:none;\">View</a></td>"
        "</tr>"
        for item in items
        if item
    )
    return _table(["Ticker", "Score", "Direction", "Why", "Source", "Link"], rows)


def _calendar_items_text(items: list[dict[str, Any]]) -> str:
    if not items:
        return "No upcoming watchlist calendar dates in the next week."
    lines = ["Upcoming calendar dates"]
    for item in items[:12]:
        symbol = str(item.get("symbol") or item.get("country") or "Market")
        title = str(item.get("title") or item.get("kind") or "Calendar event")
        subtitle = _calendar_detail(item)
        suffix = f" | {subtitle}" if subtitle else ""
        lines.append(f"- {item.get('date')}: {symbol} | {title}{suffix}")
    return "\n".join(lines)


def _calendar_items_html(items: list[dict[str, Any]]) -> str:
    if not items:
        return _empty_state_card("No upcoming watchlist calendar dates in the next week.")
    rows = "".join(
        "<tr>"
        f"<td style=\"padding:10px;border-bottom:1px solid #e2e8f0;color:#334155;white-space:nowrap;\">{html_escape(str(item.get('date') or ''))}</td>"
        f"<td style=\"padding:10px;border-bottom:1px solid #e2e8f0;font-weight:700;color:#0f172a;\">{html_escape(str(item.get('symbol') or item.get('country') or 'Market'))}</td>"
        f"<td style=\"padding:10px;border-bottom:1px solid #e2e8f0;color:#334155;\">{html_escape(str(item.get('title') or item.get('kind') or 'Calendar event'))}</td>"
        f"<td style=\"padding:10px;border-bottom:1px solid #e2e8f0;color:#334155;\">{html_escape(_calendar_detail(item))}</td>"
        "</tr>"
        for item in items[:12]
    )
    return _table(["Date", "Ticker", "Event", "Detail"], rows)


def _calendar_detail(item: dict[str, Any]) -> str:
    if str(item.get("kind") or "").lower() == "ipo":
        return _clean_text(item.get("company")) or _clean_text((item.get("payload") or {}).get("companyName") if isinstance(item.get("payload"), dict) else None) or _clean_text(item.get("subtitle")) or ""
    return _clean_text(item.get("subtitle")) or ""


def _table(headers: list[str], rows: str) -> str:
    head = "".join(
        f"<th align=\"left\" style=\"padding:10px;background:#ecfeff;color:#0f766e;font-family:Arial,Helvetica,sans-serif;font-size:12px;line-height:16px;\">{html_escape(label)}</th>"
        for label in headers
    )
    return f"<table role=\"presentation\" width=\"100%\" cellspacing=\"0\" cellpadding=\"0\" style=\"margin:18px 0 0 0;border-collapse:collapse;border:1px solid #dbe6ea;border-radius:6px;overflow:hidden;\"><thead><tr>{head}</tr></thead><tbody>{rows}</tbody></table>"


def _empty_state_card(message: str) -> str:
    return (
        "<table role=\"presentation\" width=\"100%\" cellspacing=\"0\" cellpadding=\"0\" "
        "style=\"margin:18px 0 0 0;border-collapse:separate;background:#f8fafc;border:1px solid #d8e6ea;border-radius:7px;\">"
        "<tr><td style=\"padding:16px;border-left:4px solid #14d6a3;font-family:Arial,Helvetica,sans-serif;font-size:14px;line-height:22px;color:#334155;\">"
        f"{html_escape(message)}"
        "</td></tr></table>"
    )


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
        or "https://app.walnutmarkets.com"
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
        return "Unavailable"
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
        return "Unavailable"
    if isinstance(value, datetime):
        value = _coerce_aware(value).astimezone(ZoneInfo(DEFAULT_DIGEST_TIMEZONE)).date()
    return _friendly_date(value)


def _format_datetime(value: datetime | None) -> str:
    if value is None:
        return "Unavailable"
    local = _coerce_aware(value).astimezone(ZoneInfo(DEFAULT_DIGEST_TIMEZONE))
    hour = local.hour % 12 or 12
    minute = f"{local.minute:02d}"
    am_pm = "AM" if local.hour < 12 else "PM"
    return f"{_friendly_date(local.date())}, {hour}:{minute} {am_pm} PT"


def _format_window_label(start: datetime, end: datetime) -> str:
    tz = ZoneInfo(DEFAULT_DIGEST_TIMEZONE)
    local_start = _coerce_aware(start).astimezone(tz)
    local_end = _coerce_aware(end).astimezone(tz)
    display_end_date = local_end.date()
    if local_end.time().replace(tzinfo=None) == time.min and local_end.date() > local_start.date():
        display_end_date = local_end.date() - timedelta(days=1)
    if local_start.date() == display_end_date:
        return f"{_friendly_date(display_end_date)} window"
    return f"{_friendly_date(local_start.date())} - {_friendly_date(display_end_date)} window"


def _friendly_date(value: date) -> str:
    return f"{value.strftime('%b')} {value.day}, {value.year}"


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


def _normalize_ticker(value: Any) -> str:
    text = str(value or "").strip().upper()
    return text or "UNKNOWN"


def _normalize_direction(value: Any) -> str | None:
    text = str(value or "").strip().lower()
    return text if text in SIGNAL_ALLOWED_DIRECTIONS else None


def _clean_text(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned or None


def _numeric_score(value: Any) -> int | float | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        return value if value == value else None
    try:
        number = float(str(value).strip())
    except (TypeError, ValueError):
        return None
    if number != number:
        return None
    return int(number) if number.is_integer() else round(number, 1)


def _has_any_score(items: list[dict[str, Any]]) -> bool:
    return any(_numeric_score(item.get("signal_score")) is not None for item in items)


def _score_display(value: Any, *, missing: str = "--") -> str:
    score = _numeric_score(value)
    if score is None:
        return missing
    return str(score)


def _showing_summary_text(display_count: int, total_count: int) -> str:
    if total_count <= display_count:
        return ""
    return f"Showing {display_count} of {total_count} items. Review the full activity in Walnut Market Terminal."


def _showing_summary_html(display_count: int, total_count: int) -> str:
    text = _showing_summary_text(display_count, total_count)
    if not text:
        return ""
    return (
        "<div style=\"margin-top:10px;font-family:Arial,Helvetica,sans-serif;font-size:13px;line-height:20px;color:#475569;\">"
        f"{html_escape(text)}"
        "</div>"
    )


def _count_summary(count: int, singular: str, plural: str) -> str:
    if count == 0:
        return f"No {plural} in this window."
    if count == 1:
        return f"1 {singular} in this window."
    return f"{count} {plural} in this window."


def _coerce_window_start(value: datetime | date) -> datetime:
    if isinstance(value, datetime):
        return _coerce_aware(value)
    return datetime.combine(value, time.min, tzinfo=timezone.utc)


def _coerce_window_end(value: datetime | date) -> datetime:
    if isinstance(value, datetime):
        return _coerce_aware(value)
    return datetime.combine(value, time.min, tzinfo=timezone.utc)


def _coerce_aware(value: datetime) -> datetime:
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


def _int_value(value: str | None) -> int | None:
    try:
        return int(str(value))
    except Exception:
        return None
