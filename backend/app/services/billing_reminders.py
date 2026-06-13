from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Literal

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.auth import normalize_email
from app.entitlements import PAID_SUBSCRIPTION_STATUSES, normalize_tier
from app.models import EmailDelivery, UserAccount
from app.services.email_delivery import send_email

ReminderWindow = Literal["7d", "24h"]

REMINDER_WINDOWS: dict[ReminderWindow, tuple[timedelta, timedelta, str]] = {
    "7d": (timedelta(hours=24), timedelta(days=7), "7 days"),
    "24h": (timedelta(0), timedelta(hours=24), "24 hours"),
}


def billing_expiry_reminder_key(user: UserAccount, window: ReminderWindow) -> str:
    subscription_id = user.stripe_subscription_id or "none"
    return f"billing_expiry_reminder:user:{user.id}:subscription:{subscription_id}:window:{window}"


def run_billing_expiry_reminders(
    db: Session,
    *,
    window: ReminderWindow | None = None,
    now: datetime | None = None,
    dry_run: bool = False,
    limit: int = 100,
) -> list[dict[str, Any]]:
    now = now or datetime.now(timezone.utc)
    windows = [window] if window else list(REMINDER_WINDOWS.keys())
    results: list[dict[str, Any]] = []
    for reminder_window in windows:
        lower, upper, label = REMINDER_WINDOWS[reminder_window]
        candidates = _candidate_users(db, now=now, lower=lower, upper=upper, limit=max(0, limit - len(results)))
        for user in candidates:
            result = _send_one_reminder(db, user=user, window=reminder_window, label=label, dry_run=dry_run, now=now)
            results.append(result)
            if len(results) >= limit:
                return results
    return results


def summarize_billing_reminders(results: list[dict[str, Any]]) -> dict[str, int]:
    summary: dict[str, int] = {}
    for result in results:
        status = str(result.get("status") or "unknown")
        summary[status] = summary.get(status, 0) + 1
    return summary


def _candidate_users(db: Session, *, now: datetime, lower: timedelta, upper: timedelta, limit: int) -> list[UserAccount]:
    if limit <= 0:
        return []
    start = now + lower
    end = now + upper
    return list(
        db.execute(
            select(UserAccount)
            .where(UserAccount.deleted_at.is_(None))
            .where(UserAccount.subscription_cancel_at_period_end.is_(True))
            .where(UserAccount.stripe_subscription_id.is_not(None))
            .where(UserAccount.access_expires_at.is_not(None))
            .where(UserAccount.access_expires_at > start)
            .where(UserAccount.access_expires_at <= end)
            .where(UserAccount.subscription_status.in_(list(PAID_SUBSCRIPTION_STATUSES)))
            .order_by(UserAccount.access_expires_at.asc(), UserAccount.id.asc())
            .limit(limit)
        ).scalars()
    )


def _send_one_reminder(
    db: Session,
    *,
    user: UserAccount,
    window: ReminderWindow,
    label: str,
    dry_run: bool,
    now: datetime,
) -> dict[str, Any]:
    email = normalize_email(user.original_email or user.email)
    key = billing_expiry_reminder_key(user, window)
    base = {
        "user_id": user.id,
        "email": email,
        "window": window,
        "idempotency_key": key,
        "access_expires_at": user.access_expires_at,
    }
    if not email or "@" not in email:
        return {**base, "status": "skipped", "reason": "invalid_email"}
    existing = db.execute(select(EmailDelivery).where(EmailDelivery.idempotency_key == key)).scalar_one_or_none()
    if existing:
        return {**base, "status": "duplicate", "delivery_id": existing.id}
    if dry_run:
        return {**base, "status": "dry_run"}
    plan = normalize_tier(user.subscription_plan or user.entitlement_tier)
    if plan not in {"premium", "pro"}:
        plan = "premium"
    result = send_email(
        db,
        to_email=email,
        template_key="billing.subscription_expiry_reminder",
        context={
            "first_name": (user.first_name or user.name or "there").strip().split(" ", 1)[0] or "there",
            "plan": plan.title(),
            "current_period_end": _format_date(user.access_expires_at),
            "manage_billing_url": f"{_frontend_base_url()}/account/billing",
            "support_email": "support@walnutmarkets.com",
            "reminder_window": label,
        },
        user_id=user.id,
        category="billing",
        idempotency_key=key,
    )
    return {**base, "status": result.get("status") or "queued", "delivery_id": result.get("id"), "sent_at": now}


def _format_date(value: datetime | None) -> str:
    if not value:
        return "the end of your billing period"
    aware = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return f"{aware:%B} {aware.day}, {aware:%Y}"


def _frontend_base_url() -> str:
    import os

    return os.getenv("FRONTEND_BASE_URL", "http://localhost:3000").rstrip("/")
