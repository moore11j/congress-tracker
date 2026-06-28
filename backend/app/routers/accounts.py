from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import secrets
import time
import zipfile
from datetime import date, datetime, timedelta, timezone
from html import escape as html_escape
from io import BytesIO
from typing import Annotated, Any, Literal
from urllib.parse import urlencode, urlparse, urlunparse
from zoneinfo import ZoneInfo

import requests
from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from pydantic import BaseModel, Field
from sqlalchemy import String, case, cast, func, or_, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.auth import (
    SESSION_COOKIE_NAME,
    attach_legacy_watchlists_to_user,
    clear_session_cookie,
    current_user,
    get_or_create_user,
    hash_password,
    is_admin_user,
    normalize_email,
    require_admin_user,
    reset_token_hash,
    set_session_cookie,
    session_cookie_domain,
    session_cookie_samesite,
    session_cookie_secure,
    sign_session_payload,
    verify_session_token,
    verify_password,
)
from app.db import get_db
from app.entitlements import (
    DEFAULT_FEATURE_GATES,
    PAID_SUBSCRIPTION_STATUSES,
    PAYMENT_GRACE_SUBSCRIPTION_STATUSES,
    REVOKED_SUBSCRIPTION_STATUSES,
    effective_user_tier,
    subscription_policy_tier,
    stripe_payment_failure_grace_days,
    plan_config_payload,
    current_entitlements,
    entitlements_for_user,
    entitlement_payload,
    feature_gate_payloads,
    normalize_tier,
    seed_plan_prices,
    set_plan_limit,
    set_plan_price,
    set_feature_gate,
)
from app.models import AdminBillingOverrideAuditLog, AppSetting, BillingTransaction, EmailDelivery, EmailTemplate, PageViewEvent, PlanPrice, StripeWebhookEvent, UserAccount, Watchlist
from app.rate_limit import (
    rate_limit_admin_export,
    rate_limit_admin_mutation,
    rate_limit_auth_login,
    rate_limit_notification_mutation,
    rate_limit_password_reset_confirm,
    rate_limit_password_reset_request,
    rate_limit_register,
)
from app.services.email_delivery import email_delivery_enabled, send_email
from app.services.email_digests import (
    DEFAULT_DIGEST_TIMEZONE,
    run_digest_job,
    send_monthly_billing_statement,
    send_monitoring_digest,
    send_signal_alert_digest,
    send_watchlist_activity_digest,
    summarize_digest_results,
)
from app.services.email_intraday import run_intraday_alert_sweep, summarize_intraday_alert_results
from app.services.billing_readiness import billing_readiness, log_billing_readiness, stripe_price_env_name, stripe_price_id, stripe_price_label
from app.services.email_renderer import render_template_string
from app.services.email_templates import reset_email_template_to_default, reset_email_templates_to_defaults
from app.services.provider_usage import provider_usage_summary

router = APIRouter(tags=["accounts"])
logger = logging.getLogger(__name__)
ADMIN_BILLING_SYNC_FAILURE_MESSAGE = "Couldn't create/update the Stripe subscription. No local plan change was saved."
ADMIN_PLAN_PRICE_MODES = {"default", "custom", "free_admin_grant"}
ADMIN_BILLING_PAID_TIERS = {"premium", "pro"}
ADMIN_DELETE_CANCELABLE_SUBSCRIPTION_STATUSES = {*PAID_SUBSCRIPTION_STATUSES, *PAYMENT_GRACE_SUBSCRIPTION_STATUSES, "incomplete"}


class LoginPayload(BaseModel):
    email: str = Field(min_length=3, max_length=320)
    password: str | None = Field(default=None, min_length=8, max_length=240)
    name: str | None = Field(default=None, max_length=160)
    admin_token: str | None = Field(default=None, max_length=240)


class RegisterPayload(BaseModel):
    name: str | None = Field(default=None, max_length=160)
    first_name: str = Field(min_length=1, max_length=80)
    last_name: str = Field(min_length=1, max_length=80)
    email: str = Field(min_length=3, max_length=320)
    password: str = Field(min_length=8, max_length=240)
    country: str = Field(min_length=1, max_length=2)
    state_province: str = Field(default="", max_length=100)
    postal_code: str = Field(min_length=1, max_length=32)
    city: str = Field(min_length=1, max_length=120)
    address_line1: str = Field(min_length=1, max_length=240)
    address_line2: str = Field(default="", max_length=240)


class PasswordResetRequestPayload(BaseModel):
    email: str = Field(min_length=3, max_length=320)


class PasswordResetConfirmPayload(BaseModel):
    token: str = Field(min_length=16, max_length=240)
    password: str | None = Field(default=None, min_length=8, max_length=240)
    new_password: str | None = Field(default=None, min_length=8, max_length=240)
    confirm_password: str = Field(min_length=8, max_length=240)


class ResendVerificationPayload(BaseModel):
    email: str | None = Field(default=None, min_length=3, max_length=320)


class GoogleCallbackPayload(BaseModel):
    code: str = Field(min_length=1)
    state: str = Field(min_length=1)
    redirect_uri: str | None = None


class ProfileUpdatePayload(BaseModel):
    first_name: str | None = Field(default=None, max_length=80)
    last_name: str | None = Field(default=None, max_length=80)
    country: str | None = Field(default=None, max_length=2)
    state_province: str | None = Field(default=None, max_length=100)
    postal_code: str | None = Field(default=None, max_length=32)
    city: str | None = Field(default=None, max_length=120)
    address_line1: str | None = Field(default=None, max_length=240)
    address_line2: str | None = Field(default=None, max_length=240)


class PasswordChangePayload(BaseModel):
    current_password: str = Field(min_length=1, max_length=240)
    new_password: str = Field(min_length=8, max_length=240)
    confirm_password: str = Field(min_length=8, max_length=240)


class DeleteAccountPayload(BaseModel):
    confirmation: str = Field(min_length=1, max_length=32)


class ReactivateAccountPayload(BaseModel):
    token: str = Field(min_length=16, max_length=240)


class NotificationSettingsPayload(BaseModel):
    alerts_enabled: bool
    email_notifications_enabled: bool
    watchlist_activity_notifications: bool
    signals_notifications: bool


class AdminCustomPricePayload(BaseModel):
    amount_cents: int = Field(ge=0, le=10000000)
    currency: str = Field(default="USD", min_length=3, max_length=8)
    interval: str = Field(default="month", max_length=16)


class ManualPremiumPayload(BaseModel):
    tier: Literal["free", "premium", "pro"] | None = None
    price_mode: Literal["default", "custom", "free_admin_grant"] | None = None
    custom_price: AdminCustomPricePayload | None = None


class SuspendPayload(BaseModel):
    suspended: bool


class PriceOverridePayload(BaseModel):
    monthly_price_override: int | None = Field(default=None, ge=0, le=10000000)
    annual_price_override: int | None = Field(default=None, ge=0, le=10000000)
    override_currency: str | None = Field(default=None, max_length=8)
    override_note: str | None = Field(default=None, max_length=500)


class AdminBatchUsersPayload(BaseModel):
    user_ids: list[int] = Field(min_length=1, max_length=500)
    tier: Literal["free", "premium", "pro"] | None = None
    suspended: bool | None = None
    price_override: PriceOverridePayload | None = None
    clear_price_override: bool = False
    price_mode: Literal["default", "custom", "free_admin_grant"] | None = None
    custom_price: AdminCustomPricePayload | None = None


class FeatureGatePayload(BaseModel):
    required_tier: Literal["free", "premium", "pro"]


class PlanLimitPayload(BaseModel):
    tier: Literal["free", "premium", "pro"]
    limit_value: int = Field(ge=0, le=100000)


class PlanPricePayload(BaseModel):
    amount_cents: int = Field(ge=0, le=10000000)
    currency: str = Field(default="USD", min_length=3, max_length=8)


class OAuthSettingsPayload(BaseModel):
    google_client_id: str = Field(default="", max_length=512)


class EmailTemplateUpdatePayload(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=200)
    category: str | None = Field(default=None, min_length=1, max_length=80)
    from_name: str | None = Field(default=None, min_length=1, max_length=200)
    from_email: str | None = Field(default=None, min_length=3, max_length=320)
    reply_to: str | None = Field(default=None, max_length=320)
    subject: str | None = Field(default=None, min_length=1, max_length=500)
    preheader: str | None = Field(default=None, max_length=500)
    body_text: str | None = Field(default=None, min_length=1)
    body_html: str | None = None
    variables_json: str | None = None
    enabled: bool | None = None


class EmailTemplatePreviewPayload(BaseModel):
    context: dict[str, Any] = Field(default_factory=dict)


class EmailTemplateSendTestPayload(BaseModel):
    to_email: str | None = Field(default=None, min_length=3, max_length=320)
    context: dict[str, Any] = Field(default_factory=dict)


class EmailTemplateBulkResetPayload(BaseModel):
    template_keys: list[str] | None = Field(default=None, max_length=50)


class AdminDigestSendTestPayload(BaseModel):
    user_id: int | None = Field(default=None, ge=1)
    email: str | None = Field(default=None, min_length=3, max_length=320)
    watchlist_id: int | None = Field(default=None, ge=1)
    since: datetime | None = None
    lookback_days: int = Field(default=1, ge=1, le=365)
    force: bool = False


class AdminBillingStatementSendTestPayload(BaseModel):
    user_id: int | None = Field(default=None, ge=1)
    email: str | None = Field(default=None, min_length=3, max_length=320)
    period_start: date | None = None
    period_end: date | None = None
    force: bool = False


class AdminDigestRunNowPayload(BaseModel):
    kind: Literal["watchlist_activity", "monitoring", "signals"]
    lookback_days: int = Field(default=1, ge=1, le=30)
    limit: int = Field(default=100, ge=1, le=500)
    force: bool = False
    dry_run: bool = False


class AdminIntradayRunNowPayload(BaseModel):
    lookback_minutes: int = Field(default=60, ge=1, le=1440)
    limit: int = Field(default=100, ge=1, le=500)
    dry_run: bool = True
    market_hours_only: bool = True


class StripeTaxSettingsPayload(BaseModel):
    automatic_tax_enabled: bool = False
    require_billing_address: bool = True
    product_tax_code: str | None = Field(default=None, max_length=80)
    price_tax_behavior: Literal["unspecified", "exclusive", "inclusive"] = "unspecified"


class CheckoutSessionPayload(BaseModel):
    billing_interval: Literal["monthly", "annual"] | None = None
    interval: Literal["monthly", "annual"] | None = None
    tier: Literal["premium", "pro"] | None = None
    plan: Literal["premium", "pro"] | None = None


CHECKOUT_BLOCKING_SUBSCRIPTION_STATUSES = {"active", "trialing", "past_due"}


class AdminSubscriptionSyncPayload(BaseModel):
    email: str = Field(min_length=3, max_length=320)


class PageViewPayload(BaseModel):
    path: str = Field(min_length=1, max_length=500)
    referrer_path: str | None = Field(default=None, max_length=500)
    title: str | None = Field(default=None, max_length=180)
    session_id: str | None = Field(default=None, max_length=160)


SalesLedgerPeriod = Literal[
    "last_7_days",
    "last_30_days",
    "month_to_date",
    "year_to_date",
    "all_dates",
    "current_month",
    "current_quarter",
    "current_year",
    "last_month",
    "last_quarter",
    "last_year",
    "custom",
]
SalesLedgerSortBy = Literal["date_charged", "customer_name", "gross_amount", "country"]
SalesLedgerSortDir = Literal["asc", "desc"]
AdminUserPlanFilter = Literal["all", "free", "premium", "pro", "admin"]
AdminUserAdminFilter = Literal["all", "admin", "non_admin"]
AdminUserSortBy = Literal["created_at", "last_seen_at", "email", "name", "country", "plan", "status"]
AdminUserSortDir = Literal["asc", "desc"]
SubscriptionInterval = Literal["monthly", "annual"]
EmailDeliveryDateWindow = Literal["today", "last_7", "last_14", "last_30", "last_month", "all_time"]


def _admin_token_matches(value: str | None) -> bool:
    configured = os.getenv("ADMIN_TOKEN", "").strip()
    return bool(configured and value and hmac.compare_digest(configured, value))


def _app_environment() -> str:
    return (os.getenv("APP_ENV") or os.getenv("ENV") or os.getenv("NODE_ENV") or "").strip().lower()


def _is_production_env() -> bool:
    return _app_environment() in {"prod", "production"}


def _allow_insecure_reset_link_response() -> bool:
    if _is_production_env():
        return False
    if os.getenv("CT_ALLOW_INSECURE_RESET_LINK_RESPONSE", "").strip().lower() not in {"1", "true", "yes"}:
        return False
    return _app_environment() in {"local", "dev", "development", "test", "testing"}


def _verification_url(token: str) -> str:
    return f"{_authenticated_app_frontend_base_url()}/account/verify-email?{urlencode({'token': token})}"


def _reset_url(token: str) -> str:
    return f"{_authenticated_app_frontend_base_url()}/reset-password?{urlencode({'token': token})}"


def _user_first_name(user: UserAccount) -> str:
    return (user.first_name or user.name or "there").strip().split(" ", 1)[0] or "there"


def _support_email() -> str:
    return os.getenv("SUPPORT_EMAIL", "support@walnutmarkets.com").strip() or "support@walnutmarkets.com"


def _allow_insecure_verification_link_response() -> bool:
    return _app_environment() in {"local", "dev", "development", "test", "testing"}


def _issue_email_verification(db: Session, user: UserAccount) -> str:
    token = secrets.token_urlsafe(32)
    user.email_verification_token_hash = reset_token_hash(token)
    user.email_verification_expires_at = datetime.now(timezone.utc) + timedelta(hours=24)
    return token


def format_expiry_duration(minutes: int) -> str:
    try:
        value = max(1, int(minutes))
    except (TypeError, ValueError):
        value = 1
    if value >= 60 and value % 60 == 0:
        hours = value // 60
        return f"{hours} hour" if hours == 1 else f"{hours} hours"
    return f"{value} minute" if value == 1 else f"{value} minutes"


def _ensure_verification_email_expiry_copy(db: Session) -> None:
    template = db.execute(
        select(EmailTemplate).where(EmailTemplate.template_key == "account.verify_email")
    ).scalar_one_or_none()
    if not template:
        return
    changed = False
    replacements = {
        "This link expires in {{expires_minutes}} minutes.": "This link expires in {{expires_label}}.",
        "This verification link expires in {{expires_minutes}} minutes.": "This verification link expires in {{expires_label}}.",
    }
    for field in ("body_text", "body_html"):
        value = getattr(template, field, None)
        if not value:
            continue
        updated = value
        for old, new in replacements.items():
            updated = updated.replace(old, new)
        if updated != value:
            setattr(template, field, updated)
            changed = True
    variables = _loads_list(template.variables_json)
    if "expires_label" not in variables:
        variables.append("expires_label")
        template.variables_json = json.dumps(variables)
        changed = True
    if changed:
        template.updated_at = datetime.now(timezone.utc)
        db.flush()


def _loads_list(value: str | None) -> list[str]:
    if not value:
        return []
    try:
        parsed = json.loads(value)
    except Exception:
        return []
    return [str(item) for item in parsed] if isinstance(parsed, list) else []


def _send_verification_email(db: Session, user: UserAccount, verification_url: str) -> dict[str, Any] | None:
    expires_minutes = 24 * 60
    try:
        _ensure_verification_email_expiry_copy(db)
        return send_email(
            db,
            to_email=user.email,
            template_key="account.verify_email",
            context={
                "first_name": _user_first_name(user),
                "verification_url": verification_url,
                "expires_minutes": expires_minutes,
                "expires_label": format_expiry_duration(expires_minutes),
            },
            user_id=user.id,
            category="account",
            idempotency_key=f"verify-email:{user.id}:{user.email_verification_token_hash}",
        )
    except Exception:
        logger.warning("verification_email_failed email_domain=%s", _email_domain(user.email), exc_info=True)
        return None


def _terminal_url() -> str:
    return f"{_authenticated_app_frontend_base_url()}/feed"


def _send_welcome_email(db: Session, user: UserAccount) -> dict[str, Any] | None:
    try:
        return send_email(
            db,
            to_email=user.email,
            template_key="account.welcome",
            context={
                "first_name": _user_first_name(user),
                "app_url": _terminal_url(),
                "support_email": _support_email(),
            },
            user_id=user.id,
            category="account",
            idempotency_key=f"account.welcome:user:{user.id}",
        )
    except Exception:
        logger.warning("welcome_email_failed email_domain=%s", _email_domain(user.email), exc_info=True)
        return None


def _send_password_reset_instructions(db: Session, user: UserAccount, reset_url: str) -> dict[str, Any] | None:
    try:
        return send_email(
            db,
            to_email=user.email,
            template_key="account.password_reset",
            context={
                "first_name": _user_first_name(user),
                "reset_url": reset_url,
                "expires_minutes": 30,
            },
            user_id=user.id,
            category="account",
            idempotency_key=f"password-reset:{user.id}:{user.password_reset_token_hash}",
        )
    except Exception:
        logger.warning("password_reset_email_failed email_domain=%s", _email_domain(user.email), exc_info=True)
        return None


def _issue_password_reset_for_user(db: Session, user: UserAccount) -> tuple[str, dict[str, Any] | None]:
    token = secrets.token_urlsafe(32)
    user.password_reset_token_hash = reset_token_hash(token)
    user.password_reset_expires_at = datetime.now(timezone.utc) + timedelta(minutes=30)
    db.commit()
    return token, _send_password_reset_instructions(db, user, _reset_url(token))


def _format_password_changed_at(value: datetime) -> str:
    changed_at = value.astimezone(timezone.utc)
    return f"{changed_at:%B} {changed_at.day}, {changed_at:%Y at %I:%M %p UTC}"


def _login_url() -> str:
    return f"{_authenticated_app_frontend_base_url()}/login"


def _send_password_changed_confirmation(db: Session, user: UserAccount, changed_at: datetime) -> dict[str, Any] | None:
    try:
        return send_email(
            db,
            to_email=user.email,
            template_key="account.password_changed",
            context={
                "first_name": _user_first_name(user),
                "changed_at": _format_password_changed_at(changed_at),
                "support_email": _support_email(),
                "login_url": _login_url(),
            },
            user_id=user.id,
            category="account",
            idempotency_key=f"password-changed:{user.id}:{changed_at.isoformat()}",
        )
    except Exception:
        db.rollback()
        logger.warning("password_changed_email_failed email_domain=%s", _email_domain(user.email), exc_info=True)
        return None


def _is_deleted_user(user: UserAccount | None) -> bool:
    return bool(user and user.deleted_at is not None)


def _account_lookup_by_active_email(db: Session, email: str) -> UserAccount | None:
    return db.execute(
        select(UserAccount)
        .where(func.lower(UserAccount.email) == email)
        .where(UserAccount.deleted_at.is_(None))
    ).scalar_one_or_none()


def _deleted_account_lookup_by_original_email(db: Session, email: str) -> UserAccount | None:
    return db.execute(
        select(UserAccount)
        .where(func.lower(func.coalesce(UserAccount.original_email, UserAccount.email)) == email)
        .where(UserAccount.deleted_at.is_not(None))
        .order_by(UserAccount.deleted_at.desc())
    ).scalars().first()


def _deleted_email_namespace(user: UserAccount, email: str) -> str:
    local, sep, domain = email.partition("@")
    if not sep:
        return f"deleted+{user.id}@deleted.local"
    compact_local = "".join(ch for ch in local if ch.isalnum() or ch in {".", "_", "-", "+"})[:80] or "account"
    return f"deleted+{user.id}+{compact_local}@{domain}"


def _paid_access_deadline(user: UserAccount, *, now: datetime | None = None) -> datetime | None:
    now = now or datetime.now(timezone.utc)
    if subscription_policy_tier(user, now=now) == "free":
        return None
    access_expires_at = _aware_utc(user.access_expires_at)
    if access_expires_at and access_expires_at > now:
        return access_expires_at
    return None


def _format_account_date(value: datetime | None) -> str:
    aware = _aware_utc(value)
    if not aware:
        return "the reactivation deadline"
    return f"{aware:%B} {aware.day}, {aware:%Y}"


def _frontend_reactivation_url(token: str) -> str:
    return f"{_authenticated_app_frontend_base_url()}/account/reactivate?{urlencode({'token': token})}"


def _send_account_deleted_reactivation_email(
    db: Session,
    user: UserAccount,
    *,
    to_email: str,
    first_name: str,
    token: str,
    deadline: datetime,
    current_period_end: datetime | None,
    is_paid: bool,
) -> dict[str, Any] | None:
    try:
        return send_email(
            db,
            to_email=to_email,
            template_key="account.account_deleted_reactivation",
            context={
                "first_name": first_name or "there",
                "reactivate_url": _frontend_reactivation_url(token),
                "reactivation_deadline": _format_account_date(deadline),
                "current_period_end": _format_account_date(current_period_end),
                "is_paid": "true" if is_paid else "",
                "support_email": _support_email(),
            },
            user_id=user.id,
            category="account",
            idempotency_key=f"account-deleted-reactivation:{user.id}:{user.reactivation_token_hash}",
        )
    except Exception:
        logger.warning("account_deleted_reactivation_email_failed user_id=%s", user.id, exc_info=True)
        return None


def _reactivation_deadline(user: UserAccount, *, now: datetime | None = None) -> tuple[datetime, datetime | None, bool]:
    now = now or datetime.now(timezone.utc)
    paid_deadline = _paid_access_deadline(user, now=now)
    if paid_deadline:
        return paid_deadline, paid_deadline, True
    return now + timedelta(days=7), None, False


def _restore_entitlement_after_reactivation(user: UserAccount, *, now: datetime | None = None) -> None:
    now = now or datetime.now(timezone.utc)
    user.entitlement_tier = subscription_policy_tier(user, now=now)


def _schedule_subscription_cancellation_for_deleted_account(db: Session, user: UserAccount) -> dict[str, Any] | None:
    status = (user.subscription_status or "").strip().lower()
    if not user.stripe_subscription_id or status not in {*PAID_SUBSCRIPTION_STATUSES, "past_due"}:
        return None
    try:
        subscription = _stripe_post(
            f"subscriptions/{user.stripe_subscription_id}",
            {"cancel_at_period_end": "true"},
        )
    except HTTPException as exc:
        logger.warning("stripe_delete_account_cancel_schedule_failed user_id=%s subscription_id=%s", user.id, user.stripe_subscription_id, exc_info=True)
        raise HTTPException(
            status_code=502,
            detail="We could not schedule your subscription cancellation. Please try again or contact support.",
        ) from exc
    updated_status = str(subscription.get("status") or user.subscription_status or "active").strip().lower() or "active"
    _sync_user_subscription(db, obj=subscription, status=updated_status)
    return subscription


def _deleted_reactivation_window_active(user: UserAccount, *, now: datetime | None = None) -> bool:
    if user.deleted_at is None:
        return False
    expires_at = _aware_utc(user.reactivation_expires_at)
    return bool(expires_at and expires_at > (now or datetime.now(timezone.utc)))


def _clear_paid_entitlement(user: UserAccount, *, status: str = "free", clear_access_expiry: bool = True) -> None:
    user.subscription_status = status
    user.subscription_plan = "free"
    if normalize_tier(user.manual_tier_override) in ADMIN_BILLING_PAID_TIERS:
        user.manual_tier_override = None
    user.entitlement_tier = "free"
    user.subscription_cancel_at_period_end = False
    if clear_access_expiry:
        user.access_expires_at = None
    user.updated_at = datetime.now(timezone.utc)


def _email_domain(email: str | None) -> str:
    value = normalize_email(email)
    if "@" not in value:
        return "invalid"
    return value.rsplit("@", 1)[1] or "unknown"


def _email_fingerprint(email: str | None) -> str:
    normalized = normalize_email(email)
    if not normalized:
        return "missing"
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16]


def _host_from_url(value: str | None) -> str | None:
    if not value:
        return None
    try:
        parsed = urlparse(value)
    except Exception:
        return None
    return (parsed.netloc or parsed.path.split("/", 1)[0]).lower() or None


def _auth_user_agent_metadata(value: str | None) -> tuple[str, bool]:
    ua = (value or "").lower()
    is_mobile = any(marker in ua for marker in ("mobile", "iphone", "ipad", "android"))
    if "crios" in ua or "chrome" in ua:
        family = "chrome"
    elif "fxios" in ua or "firefox" in ua:
        family = "firefox"
    elif "safari" in ua:
        family = "safari"
    elif ua:
        family = "other"
    else:
        family = "unknown"
    return family, is_mobile


def _auth_request_id(request: Request | None) -> str:
    if request is None:
        return "none"
    for header in ("x-request-id", "fly-request-id", "x-vercel-id"):
        value = request.headers.get(header)
        if value:
            return value[:120]
    return "none"


def _log_auth_diagnostic(
    flow: str,
    result: str,
    reason: str,
    request: Request | None = None,
    *,
    user: UserAccount | None = None,
    email: str | None = None,
    set_cookie_attempted: bool = False,
) -> None:
    ua_family, mobile = _auth_user_agent_metadata(request.headers.get("user-agent") if request else None)
    origin_host = _host_from_url(request.headers.get("origin") if request else None)
    referer_host = _host_from_url(request.headers.get("referer") if request else None)
    logger.info(
        "auth_flow flow=%s result=%s reason=%s request_id=%s user_id=%s email_hash=%s "
        "set_cookie_attempted=%s cookie_domain=%s cookie_path=/ cookie_samesite=%s cookie_secure=%s "
        "ua_family=%s mobile=%s origin_host=%s referer_host=%s",
        flow,
        result,
        reason,
        _auth_request_id(request),
        user.id if user else None,
        _email_fingerprint(email or (user.email if user else None)),
        set_cookie_attempted,
        session_cookie_domain() or "host-only",
        session_cookie_samesite(),
        session_cookie_secure(),
        ua_family,
        mobile,
        origin_host or "none",
        referer_host or "none",
    )


def _split_name(value: str | None) -> tuple[str | None, str | None]:
    cleaned = (value or "").strip()
    if not cleaned:
        return None, None
    first, _, last = cleaned.partition(" ")
    return first.strip() or None, last.strip() or None


def _display_name(first_name: str | None, last_name: str | None) -> str | None:
    full = " ".join(part for part in [(first_name or "").strip(), (last_name or "").strip()] if part)
    return full or None


BILLING_REQUIRED_FIELDS: tuple[tuple[str, str], ...] = (
    ("first_name", "First name"),
    ("last_name", "Last name"),
    ("country", "Country"),
    ("postal_code", "Postal code"),
    ("city", "City"),
    ("address_line1", "Address line 1"),
)
COUNTRIES_REQUIRING_BILLING_REGION = {"AU", "CA", "US"}

BILLING_LOCATION_FIELDS: tuple[str, ...] = (
    "country",
    "state_province",
    "postal_code",
    "city",
    "address_line1",
    "address_line2",
)


def _clean_profile_value(value: str | None) -> str | None:
    cleaned = (value or "").strip()
    return cleaned or None


def _clean_country(value: str | None) -> str | None:
    cleaned = _clean_profile_value(value)
    return cleaned.upper() if cleaned else None


def _validate_country_code(country: str | None) -> None:
    if country and len(country) != 2:
        raise HTTPException(status_code=422, detail="Country must use a two-letter ISO country code.")


def _billing_profile_missing_fields(user: UserAccount) -> list[str]:
    missing: list[str] = []
    for field, _label in BILLING_REQUIRED_FIELDS:
        if not _clean_profile_value(str(getattr(user, field) or "")):
            missing.append(field)
    if (user.country or "").strip().upper() in COUNTRIES_REQUIRING_BILLING_REGION and not _clean_profile_value(user.state_province):
        missing.append("state_province")
    return missing


def _billing_missing_fields_for_location(location: dict[str, Any]) -> list[str]:
    missing: list[str] = []
    for field in ("country", "postal_code", "city", "address_line1"):
        if not str(location.get(field) or "").strip():
            missing.append(field)
    country = str(location.get("country") or "").strip().upper()
    if country in COUNTRIES_REQUIRING_BILLING_REGION and not str(location.get("state_province") or "").strip():
        missing.append("state_province")
    return missing


def _billing_location_payload(user: UserAccount) -> dict[str, Any]:
    return {
        "first_name": user.first_name,
        "last_name": user.last_name,
        "country": user.country,
        "state_province": user.state_province,
        "postal_code": user.postal_code,
        "city": user.city,
        "address_line1": user.address_line1,
        "address_line2": user.address_line2,
    }


def _set_billing_profile(
    user: UserAccount,
    *,
    first_name: str | None,
    last_name: str | None,
    country: str | None,
    state_province: str | None,
    postal_code: str | None,
    city: str | None,
    address_line1: str | None,
    address_line2: str | None,
) -> None:
    cleaned_country = _clean_country(country)
    _validate_country_code(cleaned_country)
    user.first_name = _clean_profile_value(first_name)
    user.last_name = _clean_profile_value(last_name)
    user.name = _display_name(user.first_name, user.last_name)
    user.country = cleaned_country
    user.state_province = _clean_profile_value(state_province)
    user.postal_code = _clean_profile_value(postal_code)
    user.city = _clean_profile_value(city)
    user.address_line1 = _clean_profile_value(address_line1)
    user.address_line2 = _clean_profile_value(address_line2)


def _payload_fields_set(payload: BaseModel) -> set[str]:
    fields = getattr(payload, "model_fields_set", None)
    if fields is None:
        fields = getattr(payload, "__fields_set__", set())
    return set(fields)


def _password_meets_account_rules(value: str) -> bool:
    checks = (
        len(value) >= 8,
        any(char.isalpha() for char in value),
        any(char.isdigit() for char in value),
        any(not char.isalnum() for char in value),
    )
    return sum(1 for passed in checks if passed) >= 3


PASSWORD_RULES_DETAIL = (
    "Password must satisfy at least 3 of 4 requirements: 8 or more characters, one letter, one number, and one special character."
)


def _require_password_meets_account_rules(value: str, *, label: str = "Password") -> None:
    if _password_meets_account_rules(value):
        return
    raise HTTPException(status_code=422, detail=PASSWORD_RULES_DETAIL.replace("Password", label, 1))


def _notification_settings(user: UserAccount) -> dict[str, bool]:
    return {
        "alerts_enabled": bool(user.alerts_enabled),
        "email_notifications_enabled": bool(user.email_notifications_enabled),
        "watchlist_activity_notifications": bool(user.watchlist_activity_notifications),
        "signals_notifications": bool(user.signals_notifications),
    }


def _setting_value(db: Session, key: str) -> str | None:
    row = db.get(AppSetting, key)
    value = row.value if row else None
    return value.strip() if value and value.strip() else None


def _set_setting(db: Session, key: str, value: str | None) -> AppSetting:
    row = db.get(AppSetting, key)
    if not row:
        row = AppSetting(key=key)
        db.add(row)
    row.value = value.strip() if value and value.strip() else None
    row.updated_at = datetime.now(timezone.utc)
    db.flush()
    return row


def _loads_dict(value: str | None) -> dict[str, Any]:
    if not value:
        return {}
    try:
        parsed = json.loads(value)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def serialize_user_basic(user: UserAccount) -> dict[str, Any]:
    return {
        "id": user.id,
        "user_display_id": _user_display_id(user.id),
        "user_id_display": _user_display_id(user.id),
        "email": user.email,
        "original_email": user.original_email,
        "name": user.name,
        "first_name": user.first_name,
        "last_name": user.last_name,
        "country": user.country,
        "state_province": user.state_province,
        "postal_code": user.postal_code,
        "city": user.city,
        "address_line1": user.address_line1,
        "address_line2": user.address_line2,
        "auth_provider": user.auth_provider,
        "avatar_url": user.avatar_url,
        "role": user.role,
        "is_admin": is_admin_user(user),
        "entitlement_tier": user.entitlement_tier,
        "subscription_status": user.subscription_status,
        "subscription_plan": user.subscription_plan,
        "subscription_cancel_at_period_end": bool(user.subscription_cancel_at_period_end),
        "access_expires_at": user.access_expires_at,
        "is_suspended": user.is_suspended,
        "deleted_at": user.deleted_at,
        "deleted_by_user": bool(user.deleted_by_user),
        "deletion_reason": user.deletion_reason,
        "deletion_plan": user.deletion_plan,
        "reactivation_expires_at": user.reactivation_expires_at,
        "is_deleted": user.deleted_at is not None,
        "email_verified_at": user.email_verified_at,
        "email_verified": user.email_verified_at is not None,
        "email_verification_required": user.email_verified_at is None,
    }


def serialize_user_self_profile(user: UserAccount) -> dict[str, Any]:
    return {
        "id": user.id,
        "user_display_id": _user_display_id(user.id),
        "user_id_display": _user_display_id(user.id),
        "email": user.email,
        "name": user.name,
        "first_name": user.first_name,
        "last_name": user.last_name,
        "auth_provider": user.auth_provider,
        "avatar_url": user.avatar_url,
        "role": user.role,
        "is_admin": is_admin_user(user),
        "entitlement_tier": user.entitlement_tier,
        "current_plan": _effective_user_plan(user),
        "subscription_status": user.subscription_status,
        "subscription_plan": user.subscription_plan,
        "subscription_cancel_at_period_end": bool(user.subscription_cancel_at_period_end),
        "current_period_end": user.access_expires_at,
        "access_expires_at": user.access_expires_at,
        "is_suspended": user.is_suspended,
        "email_verified": user.email_verified_at is not None,
        "email_verification_required": user.email_verified_at is None,
    }


def _user_display_id(user_id: int | None) -> str:
    if user_id is None:
        return "U-000000"
    return f"U-{int(user_id):06d}"


def serialize_user_account(user: UserAccount) -> dict[str, Any]:
    billing_missing = _billing_profile_missing_fields(user)
    return {
        **serialize_user_basic(user),
        "first_name": user.first_name,
        "last_name": user.last_name,
        "country": user.country,
        "state_province": user.state_province,
        "postal_code": user.postal_code,
        "city": user.city,
        "address_line1": user.address_line1,
        "address_line2": user.address_line2,
        "billing_location": _billing_location_payload(user),
        "billing_profile_complete": not billing_missing,
        "billing_profile_missing_fields": billing_missing,
        "notifications": _notification_settings(user),
    }


def serialize_user_billing(user: UserAccount) -> dict[str, Any]:
    billing_missing = _billing_profile_missing_fields(user)
    return {
        **serialize_user_basic(user),
        "billing_location": _billing_location_payload(user),
        "billing_profile_complete": not billing_missing,
        "billing_profile_missing_fields": billing_missing,
    }


def serialize_user_billing_access(user: UserAccount) -> dict[str, Any]:
    billing_missing = _billing_profile_missing_fields(user)
    return {
        "id": user.id,
        "user_display_id": _user_display_id(user.id),
        "email": user.email,
        "name": user.name,
        "role": user.role,
        "is_admin": is_admin_user(user),
        "entitlement_tier": user.entitlement_tier,
        "current_plan": _effective_user_plan(user),
        "subscription_status": user.subscription_status,
        "subscription_plan": user.subscription_plan,
        "subscription_cancel_at_period_end": bool(user.subscription_cancel_at_period_end),
        "current_period_end": user.access_expires_at,
        "access_expires_at": user.access_expires_at,
        "billing_profile_complete": not billing_missing,
        "billing_profile_missing_fields": billing_missing,
        "email_verified": user.email_verified_at is not None,
        "email_verification_required": user.email_verified_at is None,
    }


def serialize_admin_user_row(user: UserAccount) -> dict[str, Any]:
    return {
        "id": user.id,
        "user_display_id": _user_display_id(user.id),
        "user_id_display": _user_display_id(user.id),
        "email": user.email,
        "original_email": user.original_email,
        "name": user.name,
        "first_name": user.first_name,
        "last_name": user.last_name,
        "country": user.country,
        "state_province": user.state_province,
        "role": user.role,
        "is_admin": is_admin_user(user),
        "entitlement_tier": user.entitlement_tier,
        "manual_tier_override": user.manual_tier_override,
        "monthly_price_override": user.monthly_price_override,
        "annual_price_override": user.annual_price_override,
        "override_currency": user.override_currency,
        "override_note": user.override_note,
        "subscription_status": user.subscription_status,
        "subscription_plan": user.subscription_plan,
        "subscription_cancel_at_period_end": bool(user.subscription_cancel_at_period_end),
        "access_expires_at": user.access_expires_at,
        "is_suspended": user.is_suspended,
        "deleted_at": user.deleted_at,
        "deleted_by_user": bool(user.deleted_by_user),
        "deletion_reason": user.deletion_reason,
        "deletion_plan": user.deletion_plan,
        "reactivation_expires_at": user.reactivation_expires_at,
        "reactivation_expired": bool(user.deleted_at is not None and not _deleted_reactivation_window_active(user)),
        "is_deleted": user.deleted_at is not None,
        "created_at": user.created_at,
        "last_seen_at": user.last_seen_at,
    }


SALES_LEDGER_COLUMNS: tuple[tuple[str, str], ...] = (
    ("transaction id", "transaction_id"),
    ("customer name", "customer_name"),
    ("date charged", "date_charged"),
    ("description", "description"),
    ("country", "country"),
    ("state/province", "state_province"),
    ("net revenue amount", "net_revenue_display"),
    ("VAT1 label", "vat1_label"),
    ("VAT1 collected", "vat1_collected_display"),
    ("VAT2 label", "vat2_label"),
    ("VAT2 collected", "vat2_collected_display"),
    ("gross amount", "gross_amount_display"),
    ("status / refund state if available", "status_refund_state"),
)

ADMIN_USER_COLUMNS: tuple[tuple[str, str], ...] = (
    ("user id", "user_display_id"),
    ("user name", "name"),
    ("email", "email"),
    ("country", "country"),
    ("state/province", "state_province"),
    ("plan", "plan"),
    ("billing interval", "billing_interval_display"),
    ("current plan price", "current_plan_display"),
    ("total paid", "total_paid_display"),
    ("last payment", "last_payment_display"),
    ("status", "status"),
    ("deleted at", "deleted_at"),
    ("reactivation deadline", "reactivation_expires_at"),
    ("reactivation expired", "reactivation_expired"),
    ("plan at deletion", "deletion_plan"),
    ("stripe status", "subscription_status"),
    ("cancel at period end", "subscription_cancel_at_period_end"),
    ("current period end", "current_period_end"),
    ("registered date", "created_at"),
    ("last active", "last_seen_at"),
    ("admin flag", "admin_flag"),
    ("access/subscription expiration", "access_expires_at"),
)


def _parse_date(value: str | None, field_name: str) -> date | None:
    cleaned = (value or "").strip()
    if not cleaned:
        return None
    try:
        return date.fromisoformat(cleaned)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=f"{field_name} must be YYYY-MM-DD.") from exc


def _quarter_start(value: date) -> date:
    month = ((value.month - 1) // 3) * 3 + 1
    return date(value.year, month, 1)


def _add_months(value: date, months: int) -> date:
    zero_based = value.month - 1 + months
    year = value.year + zero_based // 12
    month = zero_based % 12 + 1
    return date(year, month, 1)


def _sales_ledger_period_bounds(
    period: SalesLedgerPeriod,
    start_date: str | None,
    end_date: str | None,
    *,
    today: date | None = None,
) -> tuple[datetime | None, datetime | None, str | None, str | None]:
    current = today or datetime.now(timezone.utc).date()
    start: date | None = None
    end_exclusive: date | None = None

    if period == "last_7_days":
        start = current - timedelta(days=6)
        end_exclusive = current + timedelta(days=1)
    elif period == "last_30_days":
        start = current - timedelta(days=29)
        end_exclusive = current + timedelta(days=1)
    elif period in {"month_to_date", "current_month"}:
        start = date(current.year, current.month, 1)
        end_exclusive = current + timedelta(days=1)
    elif period in {"year_to_date", "current_year"}:
        start = date(current.year, 1, 1)
        end_exclusive = current + timedelta(days=1)
    elif period == "all_dates":
        start = None
        end_exclusive = None
    elif period == "current_quarter":
        start = _quarter_start(current)
        end_exclusive = _add_months(start, 3)
    elif period == "last_month":
        start = _add_months(date(current.year, current.month, 1), -1)
        end_exclusive = _add_months(start, 1)
    elif period == "last_quarter":
        start = _add_months(_quarter_start(current), -3)
        end_exclusive = _add_months(start, 3)
    elif period == "last_year":
        start = date(current.year - 1, 1, 1)
        end_exclusive = date(current.year, 1, 1)
    elif period == "custom":
        start = _parse_date(start_date, "start_date")
        end_inclusive = _parse_date(end_date, "end_date")
        if start and end_inclusive and start > end_inclusive:
            raise HTTPException(status_code=422, detail="start_date must be on or before end_date.")
        end_exclusive = end_inclusive + timedelta(days=1) if end_inclusive else None

    start_dt = datetime.combine(start, datetime.min.time(), tzinfo=timezone.utc) if start else None
    end_dt = datetime.combine(end_exclusive, datetime.min.time(), tzinfo=timezone.utc) if end_exclusive else None
    display_end = (end_exclusive - timedelta(days=1)).isoformat() if end_exclusive else None
    return start_dt, end_dt, start.isoformat() if start else None, display_end


def _amount_cents(value: int | None) -> int:
    return int(value or 0)


def _billing_net_amount(row: BillingTransaction) -> int:
    if row.subtotal_amount is not None:
        return _amount_cents(row.subtotal_amount)
    if row.total_amount is not None:
        return _amount_cents(row.total_amount) - _amount_cents(row.tax_amount)
    return 0


def _billing_gross_amount(row: BillingTransaction) -> int:
    if row.total_amount is not None:
        return _amount_cents(row.total_amount)
    return _amount_cents(row.subtotal_amount) + _amount_cents(row.tax_amount)


def _money_display(cents: int | None, currency: str | None) -> str:
    code = (currency or "USD").upper()
    return f"{code} {_amount_cents(cents) / 100:.2f}"


def _subscription_price_display(cents: int | None, currency: str | None) -> str | None:
    if cents is None:
        return None
    code = (currency or "USD").upper()
    amount = max(int(cents), 0) / 100
    if code == "USD":
        return f"USD ${amount:.2f}"
    return f"{code} {amount:.2f}"


_DYNAMIC_ROUTE_PREFIXES: tuple[tuple[str, str], ...] = (
    ("/ticker/", "/ticker/[symbol]"),
    ("/member/", "/member/[id]"),
    ("/insider/", "/insider/[id]"),
    ("/departments/", "/departments/[id]"),
    ("/watchlists/", "/watchlists/[id]"),
    ("/saved-screens/", "/saved-screens/[id]"),
)


def _safe_analytics_path(value: str | None) -> str | None:
    raw = (value or "").strip()
    if not raw:
        return None
    parsed = urlparse(raw)
    path = parsed.path or raw.split("?", 1)[0]
    if not path.startswith("/"):
        path = f"/{path}"
    while "//" in path:
        path = path.replace("//", "/")
    return path[:300] or "/"


def _normalize_analytics_path(path: str | None) -> str:
    safe = _safe_analytics_path(path) or "/"
    if safe in {"/account/verify-email", "/reset-password", "/account/reactivate"}:
        return safe
    for prefix, normalized in _DYNAMIC_ROUTE_PREFIXES:
        if safe.startswith(prefix) and len(safe) > len(prefix):
            return normalized
    return safe


def _analytics_route_group(normalized_path: str) -> str:
    parts = [part for part in normalized_path.split("/") if part]
    if not parts:
        return "home"
    if parts[0] == "leaderboards" and len(parts) > 1:
        return "/".join(parts[:2])
    return parts[0]


def _analytics_session_hash(request: Request, raw_session_id: str | None = None) -> str | None:
    raw = (
        raw_session_id
        or request.headers.get("x-walnut-analytics-session")
        or request.cookies.get("ct_analytics_sid")
        or ""
    ).strip()
    if not raw:
        return None
    pepper = os.getenv("APP_SESSION_SECRET", "dev-session-secret")
    return hashlib.sha256(f"{pepper}:{raw[:160]}".encode("utf-8")).hexdigest()


def _user_agent_family(user_agent: str | None) -> str:
    value = (user_agent or "").lower()
    if "edg/" in value:
        return "edge"
    if "chrome/" in value and "chromium" not in value:
        return "chrome"
    if "firefox/" in value:
        return "firefox"
    if "safari/" in value and "chrome/" not in value:
        return "safari"
    if "bot" in value or "crawler" in value or "spider" in value:
        return "bot"
    return "unknown"


def _device_type(user_agent: str | None) -> str:
    value = (user_agent or "").lower()
    if "ipad" in value or "tablet" in value:
        return "tablet"
    if "mobile" in value or "iphone" in value or "android" in value:
        return "mobile"
    if not value:
        return "unknown"
    return "desktop"


def _page_analytics_period_start(period: str) -> tuple[datetime, str]:
    normalized = (period or "7d").strip().lower()
    now = datetime.now(timezone.utc)
    if normalized in {"24h", "last_24h", "day"}:
        return now - timedelta(hours=24), "24h"
    if normalized in {"30d", "last_30d", "month"}:
        return now - timedelta(days=30), "30d"
    return now - timedelta(days=7), "7d"


def _tax_component_label(item: dict[str, Any], fallback: str) -> str:
    for key in ("display_name", "label", "jurisdiction", "taxability_reason"):
        value = str(item.get(key) or "").strip()
        if value:
            return value
    for container_key in ("tax_rate", "tax", "rate"):
        nested = item.get(container_key)
        if isinstance(nested, dict):
            for key in ("display_name", "label", "jurisdiction", "country", "state", "id"):
                value = str(nested.get(key) or "").strip()
                if value:
                    return value
        elif nested:
            value = str(nested).strip()
            if value:
                return value
    return fallback


def _tax_component_amount(item: dict[str, Any]) -> int:
    for key in ("amount", "tax_amount", "tax"):
        try:
            return int(item.get(key) or 0)
        except (TypeError, ValueError):
            continue
    return 0


def _billing_tax_components(row: BillingTransaction) -> list[dict[str, Any]]:
    raw_components: list[dict[str, Any]] = []
    if row.tax_breakdown_json:
        try:
            parsed = json.loads(row.tax_breakdown_json)
        except json.JSONDecodeError:
            parsed = {}
        if isinstance(parsed, dict):
            for key in ("total_tax_amounts", "total_taxes", "line_taxes"):
                values = parsed.get(key)
                if isinstance(values, list) and values:
                    raw_components = [item for item in values if isinstance(item, dict)]
                    break

    components: list[dict[str, Any]] = []
    for index, item in enumerate(raw_components):
        amount = _tax_component_amount(item)
        if amount == 0:
            continue
        components.append({"label": _tax_component_label(item, f"VAT {index + 1}"), "amount": amount})

    if not components and row.tax_amount:
        components.append({"label": "Tax", "amount": _amount_cents(row.tax_amount)})

    if len(components) <= 2:
        return components

    remainder = sum(_amount_cents(item.get("amount")) for item in components[1:])
    return [components[0], {"label": "Multiple taxes", "amount": remainder}]


def _status_refund_state(row: BillingTransaction) -> str:
    payment = (row.payment_status or "").strip() or "unknown"
    refund = (row.refund_status or "").strip()
    if refund and refund.lower() != "none":
        return f"{payment} / {refund.replace('_', ' ')}"
    return payment


def _billing_payload(row: BillingTransaction) -> dict[str, Any]:
    if not row.payload_json:
        return {}
    try:
        parsed = json.loads(row.payload_json)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _stripe_artifact_url(value: Any) -> str | None:
    cleaned = str(value or "").strip()
    if not cleaned:
        return None
    parsed = urlparse(cleaned)
    host = (parsed.hostname or "").lower()
    stripe_host = host in {"stripe.com", "stripe.network"} or host.endswith(".stripe.com") or host.endswith(".stripe.network")
    if parsed.scheme != "https" or not stripe_host:
        return None
    return cleaned


def _charge_receipt_url(invoice: dict[str, Any]) -> str | None:
    charge = invoice.get("charge")
    if isinstance(charge, dict):
        receipt_url = _stripe_artifact_url(charge.get("receipt_url"))
        if receipt_url:
            return receipt_url

    payment_intent = invoice.get("payment_intent")
    if isinstance(payment_intent, dict):
        latest_charge = payment_intent.get("latest_charge")
        if isinstance(latest_charge, dict):
            receipt_url = _stripe_artifact_url(latest_charge.get("receipt_url"))
            if receipt_url:
                return receipt_url
        charges = payment_intent.get("charges") if isinstance(payment_intent.get("charges"), dict) else {}
        for item in charges.get("data") or []:
            if isinstance(item, dict):
                receipt_url = _stripe_artifact_url(item.get("receipt_url"))
                if receipt_url:
                    return receipt_url

    return _stripe_artifact_url(invoice.get("receipt_url"))


def _invoice_has_stripe_documents(invoice: dict[str, Any]) -> bool:
    return bool(
        _stripe_artifact_url(invoice.get("hosted_invoice_url"))
        or _stripe_artifact_url(invoice.get("invoice_pdf"))
        or _charge_receipt_url(invoice)
    )


def _stripe_billing_documents(row: BillingTransaction) -> dict[str, Any]:
    invoice = _billing_payload(row)
    hosted_invoice_url = _stripe_artifact_url(invoice.get("hosted_invoice_url"))
    invoice_pdf_url = _stripe_artifact_url(invoice.get("invoice_pdf"))
    receipt_url = _charge_receipt_url(invoice)
    has_document = bool(hosted_invoice_url or invoice_pdf_url or receipt_url)
    return {
        "invoice_number": str(invoice.get("number") or "").strip() or None,
        "hosted_invoice_url": hosted_invoice_url,
        "invoice_pdf": invoice_pdf_url,
        "invoice_pdf_url": invoice_pdf_url,
        "receipt_url": receipt_url,
        "has_stripe_document": has_document,
        "fallback_message": None if has_document else "Stripe has not provided a hosted invoice or receipt for this transaction yet.",
    }


def _customer_billing_history_row(row: BillingTransaction) -> dict[str, Any]:
    gross_amount = _billing_gross_amount(row)
    currency = (row.currency or "USD").upper()
    return {
        "id": row.id,
        "transaction_id": f"billing-{row.id}",
        "date_charged": row.charged_at.isoformat() if row.charged_at else None,
        "description": row.description or row.billing_period_type or "Billing transaction",
        "billing_period_type": row.billing_period_type,
        "service_period_start": row.service_period_start.isoformat() if row.service_period_start else None,
        "service_period_end": row.service_period_end.isoformat() if row.service_period_end else None,
        "subtotal_amount": row.subtotal_amount,
        "tax_amount": row.tax_amount,
        "total_amount": gross_amount,
        "total_display": _money_display(gross_amount, currency),
        "currency": currency,
        "status": row.payment_status or "unknown",
        "refund_state": row.refund_status or "none",
        "status_refund_state": _status_refund_state(row),
        "documents": _stripe_billing_documents(row),
    }


def _sales_ledger_row(row: BillingTransaction) -> dict[str, Any]:
    taxes = _billing_tax_components(row)
    vat1 = taxes[0] if len(taxes) > 0 else {"label": "", "amount": 0}
    vat2 = taxes[1] if len(taxes) > 1 else {"label": "", "amount": 0}
    net_amount = _billing_net_amount(row)
    gross_amount = _billing_gross_amount(row)
    currency = (row.currency or "USD").upper()
    return {
        "id": row.id,
        "transaction_id": row.stripe_invoice_id or row.stripe_payment_intent_id or row.stripe_charge_id or str(row.id),
        "customer_name": row.customer_name or row.customer_email or "Unknown customer",
        "date_charged": row.charged_at.isoformat() if row.charged_at else None,
        "description": row.description or row.billing_period_type or "",
        "country": (row.billing_country or "").upper(),
        "state_province": row.billing_state_province or "",
        "net_revenue_amount": net_amount,
        "net_revenue_display": _money_display(net_amount, currency),
        "vat1_label": vat1["label"],
        "vat1_collected": _amount_cents(vat1["amount"]),
        "vat1_collected_display": _money_display(vat1["amount"], currency) if vat1["amount"] else "",
        "vat2_label": vat2["label"],
        "vat2_collected": _amount_cents(vat2["amount"]),
        "vat2_collected_display": _money_display(vat2["amount"], currency) if vat2["amount"] else "",
        "gross_amount": gross_amount,
        "gross_amount_display": _money_display(gross_amount, currency),
        "currency": currency,
        "status": row.payment_status or "unknown",
        "refund_state": row.refund_status or "none",
        "status_refund_state": _status_refund_state(row),
    }


def _sales_ledger_filtered_query(
    *,
    period: SalesLedgerPeriod,
    start_date: str | None,
    end_date: str | None,
    country: str | None,
) -> tuple[Any, dict[str, Any]]:
    start_dt, end_dt, effective_start, effective_end = _sales_ledger_period_bounds(period, start_date, end_date)
    conditions = []
    if start_dt:
        conditions.append(BillingTransaction.charged_at >= start_dt)
    if end_dt:
        conditions.append(BillingTransaction.charged_at < end_dt)
    country_code = (country or "").strip().upper()
    if country_code:
        if len(country_code) != 2:
            raise HTTPException(status_code=422, detail="country must use a two-letter ISO country code.")
        conditions.append(func.upper(BillingTransaction.billing_country) == country_code)
    query = select(BillingTransaction)
    if conditions:
        query = query.where(*conditions)
    return query, {
        "period": period,
        "start_date": effective_start,
        "end_date": effective_end,
        "country": country_code or None,
    }


def _sales_ledger_rows(
    db: Session,
    *,
    period: SalesLedgerPeriod,
    start_date: str | None,
    end_date: str | None,
    country: str | None,
    sort_by: SalesLedgerSortBy,
    sort_dir: SalesLedgerSortDir,
    page: int | None = None,
    page_size: int | None = None,
) -> tuple[list[BillingTransaction], int, dict[str, Any]]:
    query, filters = _sales_ledger_filtered_query(
        period=period,
        start_date=start_date,
        end_date=end_date,
        country=country,
    )
    count_query = select(func.count()).select_from(query.subquery())
    total = int(db.execute(count_query).scalar_one() or 0)
    sort_columns = {
        "date_charged": BillingTransaction.charged_at,
        "customer_name": BillingTransaction.customer_name,
        "gross_amount": BillingTransaction.total_amount,
        "country": BillingTransaction.billing_country,
    }
    sort_column = sort_columns[sort_by]
    ordered = query.order_by(sort_column.asc() if sort_dir == "asc" else sort_column.desc(), BillingTransaction.id.desc())
    if page is not None and page_size is not None:
        ordered = ordered.offset((page - 1) * page_size).limit(page_size)
    rows = db.execute(ordered).scalars().all()
    return rows, total, filters


def _effective_user_plan(user: UserAccount) -> str:
    return effective_user_tier(user)


def _admin_user_status(user: UserAccount) -> str:
    if user.deleted_at is not None:
        return "deleted"
    if user.is_suspended:
        return "suspended"
    return (user.subscription_status or "active").strip().lower() or "active"


def _plan_price_lookup(db: Session) -> dict[tuple[str, SubscriptionInterval], tuple[int, str]]:
    seed_plan_prices(db)
    rows = db.execute(select(PlanPrice)).scalars().all()
    prices: dict[tuple[str, SubscriptionInterval], tuple[int, str]] = {}
    for row in rows:
        tier = normalize_tier(row.tier)
        if tier not in {"free", "premium", "pro"}:
            continue
        interval = _normalize_subscription_interval(row.billing_interval)
        if interval is None:
            continue
        prices[(tier, interval)] = (int(row.amount_cents or 0), (row.currency or "USD").upper())
    return prices


def _latest_billing_rows_by_user(db: Session, users: list[UserAccount]) -> dict[int, BillingTransaction]:
    user_ids = {user.id for user in users if user.id is not None}
    customer_to_user = {str(user.stripe_customer_id): user.id for user in users if user.stripe_customer_id}
    subscription_to_user = {str(user.stripe_subscription_id): user.id for user in users if user.stripe_subscription_id}
    if not user_ids and not customer_to_user and not subscription_to_user:
        return {}

    conditions = []
    if user_ids:
        conditions.append(BillingTransaction.user_id.in_(user_ids))
    if customer_to_user:
        conditions.append(BillingTransaction.stripe_customer_id.in_(list(customer_to_user.keys())))
    if subscription_to_user:
        conditions.append(BillingTransaction.stripe_subscription_id.in_(list(subscription_to_user.keys())))

    rows = db.execute(
        select(BillingTransaction)
        .where(or_(*conditions))
        .where(
            or_(
                BillingTransaction.payment_status.is_(None),
                func.lower(func.coalesce(BillingTransaction.payment_status, "")).in_(["paid", "succeeded"]),
            )
        )
        .order_by(
            BillingTransaction.charged_at.desc().nullslast(),
            BillingTransaction.created_at.desc(),
            BillingTransaction.id.desc(),
        )
    ).scalars().all()

    latest: dict[int, BillingTransaction] = {}
    for row in rows:
        candidate_ids: list[int] = []
        if row.user_id in user_ids:
            candidate_ids.append(int(row.user_id))
        if row.stripe_customer_id and row.stripe_customer_id in customer_to_user:
            candidate_ids.append(customer_to_user[row.stripe_customer_id])
        if row.stripe_subscription_id and row.stripe_subscription_id in subscription_to_user:
            candidate_ids.append(subscription_to_user[row.stripe_subscription_id])
        for user_id in candidate_ids:
            latest.setdefault(user_id, row)
    return latest


def _billing_transaction_paid_amount(row: BillingTransaction) -> int:
    if row.total_amount is not None:
        return max(int(row.total_amount), 0)
    if row.subtotal_amount is not None:
        return max(int(row.subtotal_amount), 0)
    return 0


def _successful_billing_rows_by_user(db: Session, users: list[UserAccount]) -> dict[int, list[BillingTransaction]]:
    user_ids = {user.id for user in users if user.id is not None}
    customer_to_user = {str(user.stripe_customer_id): user.id for user in users if user.stripe_customer_id}
    subscription_to_user = {str(user.stripe_subscription_id): user.id for user in users if user.stripe_subscription_id}
    if not user_ids and not customer_to_user and not subscription_to_user:
        return {}

    conditions = []
    if user_ids:
        conditions.append(BillingTransaction.user_id.in_(user_ids))
    if customer_to_user:
        conditions.append(BillingTransaction.stripe_customer_id.in_(list(customer_to_user.keys())))
    if subscription_to_user:
        conditions.append(BillingTransaction.stripe_subscription_id.in_(list(subscription_to_user.keys())))

    rows = db.execute(
        select(BillingTransaction)
        .where(or_(*conditions))
        .where(func.lower(func.coalesce(BillingTransaction.payment_status, "")).in_(["paid", "succeeded"]))
        .where(func.lower(func.coalesce(BillingTransaction.refund_status, "none")) != "refunded")
        .order_by(
            BillingTransaction.charged_at.desc().nullslast(),
            BillingTransaction.created_at.desc(),
            BillingTransaction.id.desc(),
        )
    ).scalars().all()

    by_user: dict[int, list[BillingTransaction]] = {int(user_id): [] for user_id in user_ids}
    seen: set[tuple[int, int]] = set()
    for row in rows:
        candidate_ids: list[int] = []
        if row.user_id in user_ids:
            candidate_ids.append(int(row.user_id))
        if row.stripe_customer_id and row.stripe_customer_id in customer_to_user:
            candidate_ids.append(customer_to_user[row.stripe_customer_id])
        if row.stripe_subscription_id and row.stripe_subscription_id in subscription_to_user:
            candidate_ids.append(subscription_to_user[row.stripe_subscription_id])
        for user_id in candidate_ids:
            key = (int(user_id), int(row.id))
            if key in seen:
                continue
            by_user.setdefault(int(user_id), []).append(row)
            seen.add(key)
    return by_user


def _billing_payment_summary(rows: list[BillingTransaction] | None) -> dict[str, Any]:
    successful_rows = rows or []
    total = sum(_billing_transaction_paid_amount(row) for row in successful_rows)
    last = successful_rows[0] if successful_rows else None
    currency = (
        (last.currency or "USD").upper()
        if last
        else next(((row.currency or "").upper() for row in successful_rows if row.currency), "USD")
    )
    last_amount = _billing_transaction_paid_amount(last) if last else None
    return {
        "total_paid_cents": int(total),
        "total_paid_currency": currency,
        "total_paid_display": _subscription_price_display(int(total), currency),
        "last_payment_amount_cents": last_amount,
        "last_payment_currency": (last.currency or currency).upper() if last else None,
        "last_payment_display": _subscription_price_display(last_amount, (last.currency if last else currency)) if last else None,
    }


def _override_interval(user: UserAccount, fallback: SubscriptionInterval | None) -> SubscriptionInterval | None:
    if fallback and (
        (fallback == "monthly" and user.monthly_price_override is not None)
        or (fallback == "annual" and user.annual_price_override is not None)
    ):
        return fallback
    if user.monthly_price_override is not None and user.annual_price_override is None:
        return "monthly"
    if user.annual_price_override is not None and user.monthly_price_override is None:
        return "annual"
    return fallback


def _admin_user_billing_summary(
    user: UserAccount,
    *,
    latest_billing_row: BillingTransaction | None = None,
    billing_rows: list[BillingTransaction] | None = None,
    plan_prices: dict[tuple[str, SubscriptionInterval], tuple[int, str]] | None = None,
) -> dict[str, Any]:
    tier = normalize_tier(_effective_user_plan(user))
    has_paid_access = tier in {"premium", "pro"} or _has_actual_paid_access(user, datetime.now(timezone.utc))
    payments = _billing_payment_summary(billing_rows)
    if not has_paid_access:
        return {
            "current_plan": "free",
            "billing_interval": None,
            "current_plan_amount_cents": None,
            "current_plan_currency": None,
            "current_plan_display": None,
            "subscription_price_amount": None,
            "billing_price_amount": None,
            "subscription_currency": None,
            "subscription_interval": None,
            "billing_frequency": None,
            "billing_price_source": None,
            "billing_price_display": None,
            "billing_interval_display": None,
            "billing_frequency_display": None,
            **payments,
        }

    row_interval = _normalize_subscription_interval(latest_billing_row.billing_period_type if latest_billing_row else None)
    interval = _normalize_subscription_interval(user.subscription_interval) or row_interval or "monthly"
    override_interval = _override_interval(user, interval)
    if override_interval:
        override_amount = user.monthly_price_override if override_interval == "monthly" else user.annual_price_override
        if override_amount is not None:
            currency = (user.override_currency or "USD").upper()
            display = _subscription_price_display(int(override_amount), currency)
            interval_display = "Annual" if override_interval == "annual" else "Monthly"
            return {
                "current_plan": tier,
                "billing_interval": override_interval,
                "current_plan_amount_cents": int(override_amount),
                "current_plan_currency": currency,
                "current_plan_display": f"{display} / {'year' if override_interval == 'annual' else 'month'}" if display else None,
                "subscription_price_amount": int(override_amount),
                "billing_price_amount": int(override_amount),
                "subscription_currency": currency,
                "subscription_interval": override_interval,
                "billing_frequency": override_interval,
                "billing_price_source": "override",
                "billing_price_display": display,
                "billing_interval_display": interval_display,
                "billing_frequency_display": interval_display,
                **payments,
            }

    if user.current_plan_amount_cents is not None and int(user.current_plan_amount_cents) > 0:
        amount = int(user.current_plan_amount_cents)
        currency = (user.current_plan_currency or "USD").upper()
        display = _subscription_price_display(amount, currency)
        interval_display = "Annual" if interval == "annual" else "Monthly"
        return {
            "current_plan": tier,
            "billing_interval": interval,
            "current_plan_amount_cents": amount,
            "current_plan_currency": currency,
            "current_plan_display": f"{display} / {'year' if interval == 'annual' else 'month'}" if display else None,
            "subscription_price_amount": amount,
            "billing_price_amount": amount,
            "subscription_currency": currency,
            "subscription_interval": interval,
            "billing_frequency": interval,
            "billing_price_source": "stripe_subscription",
            "billing_price_display": display,
            "billing_interval_display": interval_display,
            "billing_frequency_display": interval_display,
            **payments,
        }

    if tier not in {"premium", "pro"}:
        return {
            "current_plan": tier,
            "billing_interval": None,
            "current_plan_amount_cents": None,
            "current_plan_currency": None,
            "current_plan_display": None,
            "subscription_price_amount": None,
            "billing_price_amount": None,
            "subscription_currency": None,
            "subscription_interval": None,
            "billing_frequency": None,
            "billing_price_source": None,
            "billing_price_display": None,
            "billing_interval_display": None,
            "billing_frequency_display": None,
            **payments,
        }

    default_amount, default_currency = (plan_prices or {}).get((tier, interval), (0, "USD"))
    if default_amount <= 0:
        return {
            "current_plan": tier,
            "billing_interval": interval,
            "current_plan_amount_cents": None,
            "current_plan_currency": None,
            "current_plan_display": None,
            "subscription_price_amount": None,
            "billing_price_amount": None,
            "subscription_currency": None,
            "subscription_interval": None,
            "billing_frequency": None,
            "billing_price_source": None,
            "billing_price_display": None,
            "billing_interval_display": "Annual" if interval == "annual" else "Monthly",
            "billing_frequency_display": None,
            **payments,
        }
    display = _subscription_price_display(int(default_amount), default_currency)
    interval_display = "Annual" if interval == "annual" else "Monthly"
    return {
        "current_plan": tier,
        "billing_interval": interval,
        "current_plan_amount_cents": int(default_amount),
        "current_plan_currency": default_currency,
        "current_plan_display": f"{display} / {'year' if interval == 'annual' else 'month'}" if display else None,
        "subscription_price_amount": int(default_amount),
        "billing_price_amount": int(default_amount),
        "subscription_currency": default_currency,
        "subscription_interval": interval,
        "billing_frequency": interval,
        "billing_price_source": "plan_default",
        "billing_price_display": display,
        "billing_interval_display": interval_display,
        "billing_frequency_display": interval_display,
        **payments,
    }


def _iso_or_blank(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value or "")


def _admin_subscription_state_label(user: UserAccount, status: str) -> str:
    paid_through = _aware_utc(user.access_expires_at)
    if bool(user.subscription_cancel_at_period_end) and paid_through and paid_through > datetime.now(timezone.utc):
        return f"Paid until {_format_account_date(paid_through)}, not renewing"
    return status


def _admin_user_row(
    user: UserAccount,
    *,
    latest_billing_row: BillingTransaction | None = None,
    billing_rows: list[BillingTransaction] | None = None,
    plan_prices: dict[tuple[str, SubscriptionInterval], tuple[int, str]] | None = None,
) -> dict[str, Any]:
    payload = serialize_admin_user_row(user)
    plan = _effective_user_plan(user)
    status = _admin_user_status(user)
    payload.update(
        {
            "plan": plan,
            "status": status,
            "subscription_state_label": _admin_subscription_state_label(user, status),
            "current_period_end": user.access_expires_at,
            "admin_flag": "yes" if payload["is_admin"] else "no",
            **_admin_user_billing_summary(user, latest_billing_row=latest_billing_row, billing_rows=billing_rows, plan_prices=plan_prices),
        }
    )
    return payload


def _apply_price_override(user: UserAccount, payload: PriceOverridePayload | None, *, clear: bool = False) -> None:
    if clear:
        user.monthly_price_override = None
        user.annual_price_override = None
        user.override_currency = None
        user.override_note = None
        return
    if payload is None:
        return
    user.monthly_price_override = payload.monthly_price_override
    user.annual_price_override = payload.annual_price_override
    user.override_currency = (payload.override_currency or "USD").strip().upper()[:8] or "USD"
    user.override_note = (payload.override_note or "").strip() or None


def _price_override_requested_state(payload: PriceOverridePayload | None = None, *, clear: bool = False) -> dict[str, Any]:
    if clear:
        return {
            "clear_price_override": True,
            "monthly_price_override": None,
            "annual_price_override": None,
            "override_currency": None,
        }
    if payload is None:
        return {}
    return {
        "monthly_price_override": payload.monthly_price_override,
        "annual_price_override": payload.annual_price_override,
        "override_currency": (payload.override_currency or "USD").strip().upper()[:8] or "USD",
        "override_note": (payload.override_note or "").strip() or None,
    }


def _admin_billing_previous_state(user: UserAccount) -> dict[str, Any]:
    return {
        "manual_tier_override": user.manual_tier_override,
        "entitlement_tier": user.entitlement_tier,
        "subscription_plan": user.subscription_plan,
        "subscription_status": user.subscription_status,
        "subscription_interval": user.subscription_interval,
        "is_suspended": bool(user.is_suspended),
        "monthly_price_override": user.monthly_price_override,
        "annual_price_override": user.annual_price_override,
        "override_currency": user.override_currency,
        "stripe_customer_id": user.stripe_customer_id,
        "stripe_subscription_id": user.stripe_subscription_id,
    }


def _admin_billing_override_type(requested_override: dict[str, Any]) -> str:
    kinds: list[str] = []
    if "plan" in requested_override:
        kinds.append("plan")
    if "price_override" in requested_override or requested_override.get("clear_price_override"):
        kinds.append("price")
    if "suspended" in requested_override:
        kinds.append("suspension")
    if not kinds:
        return "none"
    return kinds[0] if len(kinds) == 1 else "combined"


def _admin_billing_requested_state(user: UserAccount, requested_override: dict[str, Any]) -> dict[str, Any]:
    state: dict[str, Any] = {"target_user_id": user.id}
    if "plan" in requested_override:
        state["manual_tier_override"] = requested_override.get("plan")
        target_plan = normalize_tier(requested_override.get("plan"))
        if target_plan in ADMIN_BILLING_PAID_TIERS:
            state["price_mode"] = _admin_plan_price_mode(requested_override.get("price_mode"))
            custom_price = _admin_custom_price_state(requested_override.get("custom_price"))
            if custom_price:
                state["custom_price"] = custom_price
    if "price_override" in requested_override:
        state.update(requested_override.get("price_override") or {})
    if requested_override.get("clear_price_override"):
        state.update(_price_override_requested_state(clear=True))
    if "suspended" in requested_override:
        state["is_suspended"] = bool(requested_override.get("suspended"))
    return state


def _admin_billing_state_hash(state: dict[str, Any]) -> str:
    payload = json.dumps(state, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]


def _admin_billing_request_action_id(request: Request | None) -> str | None:
    if request is None:
        return None
    value = (
        request.headers.get("x-request-id")
        or request.headers.get("x-correlation-id")
        or request.headers.get("idempotency-key")
        or ""
    ).strip()
    return value[:80] or None


def _admin_billing_idempotency_key(
    user: UserAccount,
    override_type: str,
    requested_state: dict[str, Any],
    *,
    action_id: str | None = None,
) -> str:
    state_hash = _admin_billing_state_hash(requested_state)
    parts = ["admin-billing", f"user-{user.id}", override_type, state_hash]
    if action_id:
        parts.append(hashlib.sha256(action_id.encode("utf-8")).hexdigest()[:16])
    return ":".join(parts)[:240]


def _admin_billing_safe_error(exc: BaseException) -> str:
    if isinstance(exc, HTTPException):
        return f"stripe_sync_failed_status_{exc.status_code}"
    return f"stripe_sync_failed_{exc.__class__.__name__}"


def _admin_plan_price_mode(value: Any) -> str:
    mode = str(value or "").strip().lower()
    return mode if mode in ADMIN_PLAN_PRICE_MODES else "free_admin_grant"


def _admin_custom_price_state(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    if isinstance(value, BaseModel):
        raw = value.model_dump()
    elif isinstance(value, dict):
        raw = value
    else:
        return None
    try:
        amount_cents = int(raw.get("amount_cents"))
    except (TypeError, ValueError):
        amount_cents = -1
    if amount_cents < 0:
        return None
    currency = str(raw.get("currency") or "USD").strip().upper()[:8] or "USD"
    interval = _normalize_subscription_interval(str(raw.get("interval") or "")) or "monthly"
    return {"amount_cents": amount_cents, "currency": currency, "interval": interval}


def _admin_free_grant_price_env_name(tier: str) -> str:
    normalized = "pro" if normalize_tier(tier) == "pro" else "premium"
    return f"STRIPE_{normalized.upper()}_ADMIN_FREE_PRICE_ID"


def _admin_free_grant_price_id(tier: str) -> str | None:
    value = os.getenv(_admin_free_grant_price_env_name(tier), "").strip()
    return value if value.startswith("price_") else None


def _stripe_recurring_interval(interval: SubscriptionInterval) -> str:
    return "year" if interval == "annual" else "month"


def _admin_billing_sync_exception(exc: BaseException) -> HTTPException:
    status_code = exc.status_code if isinstance(exc, HTTPException) and exc.status_code in {400, 401, 403, 404, 409, 422, 503} else 502
    return HTTPException(
        status_code=status_code,
        detail={
            "code": "admin_billing_stripe_sync_failed",
            "message": ADMIN_BILLING_SYNC_FAILURE_MESSAGE,
        },
    )


def _record_admin_billing_override_audit(
    db: Session,
    *,
    admin_actor: UserAccount,
    user: UserAccount,
    override_type: str,
    previous_state: dict[str, Any],
    requested_state: dict[str, Any],
    stripe_customer_id: str | None,
    stripe_subscription_id: str | None,
    stripe_sync_status: str,
    error_message: str | None = None,
) -> AdminBillingOverrideAuditLog:
    row = AdminBillingOverrideAuditLog(
        admin_user_id=admin_actor.id,
        admin_email=normalize_email(admin_actor.email),
        target_user_id=user.id,
        target_email=normalize_email(user.email),
        override_type=override_type,
        previous_state_json=json.dumps(previous_state, sort_keys=True, default=str),
        requested_state_json=json.dumps(requested_state, sort_keys=True, default=str),
        stripe_customer_id=stripe_customer_id,
        stripe_subscription_id=stripe_subscription_id,
        stripe_sync_status=stripe_sync_status,
        error_message=error_message,
    )
    db.add(row)
    db.flush()
    return row


def _admin_delete_stripe_customer_enabled(delete_stripe_customer: bool | None = None) -> bool:
    if delete_stripe_customer is not None:
        return bool(delete_stripe_customer)
    configured = os.getenv("CT_ADMIN_DELETE_STRIPE_CUSTOMER_ON_ADMIN_DELETE", "").strip().lower()
    if configured:
        return configured in {"1", "true", "yes", "on"}
    return True


def _admin_delete_stripe_cleanup(user: UserAccount, *, delete_stripe_customer: bool | None = None) -> dict[str, Any]:
    has_stripe_reference = bool(user.stripe_customer_id or user.stripe_subscription_id)
    if not _stripe_secret_key():
        if has_stripe_reference:
            raise HTTPException(status_code=503, detail="Stripe cleanup is not configured.")
        return {
            "cleanup_status": "skipped_no_stripe_reference",
            "stripe_customer_id": None,
            "subscriptions_cancelled": [],
            "customer_deleted": False,
            "customer_retained": False,
        }

    customer_ids: list[str] = []
    if user.stripe_customer_id:
        customer_ids.append(str(user.stripe_customer_id))
    else:
        customers = _stripe_get("customers", {"email": normalize_email(user.email), "limit": 3})
        matches = [
            str(customer_id)
            for customer in (customers.get("data") or [])
            if isinstance(customer, dict)
            for customer_id in [_stripe_object_id(customer.get("id"))]
            if customer_id
        ]
        if len(matches) > 1:
            raise HTTPException(status_code=409, detail="Multiple Stripe customers match this email.")
        customer_ids.extend(matches)

    subscriptions_by_id: dict[str, dict[str, Any]] = {}
    if user.stripe_subscription_id:
        try:
            subscription = _stripe_get(
                f"subscriptions/{user.stripe_subscription_id}",
                {"expand[]": "items.data.price"},
            )
            subscription_id = _stripe_object_id(subscription.get("id")) or str(user.stripe_subscription_id)
            subscriptions_by_id[subscription_id] = subscription
            customer_id = _stripe_object_id(subscription.get("customer"))
            if customer_id and customer_id not in customer_ids:
                customer_ids.append(customer_id)
        except HTTPException as exc:
            if exc.status_code != 404:
                raise

    for customer_id in customer_ids:
        subscriptions = _stripe_get(
            "subscriptions",
            {"customer": customer_id, "status": "all", "limit": 100, "expand[]": "data.items.data.price"},
        )
        for subscription in subscriptions.get("data") or []:
            if not isinstance(subscription, dict):
                continue
            subscription_id = _stripe_object_id(subscription.get("id"))
            if subscription_id:
                subscriptions_by_id[subscription_id] = subscription

    cancelled: list[str] = []
    for subscription_id, subscription in subscriptions_by_id.items():
        status = str(subscription.get("status") or "").strip().lower()
        if status not in ADMIN_DELETE_CANCELABLE_SUBSCRIPTION_STATUSES:
            continue
        _stripe_delete(
            f"subscriptions/{subscription_id}",
            idempotency_key=f"admin-delete-user:{user.id}:subscription:{subscription_id}",
        )
        cancelled.append(subscription_id)

    customer_deleted = False
    customer_retained = False
    if customer_ids and _admin_delete_stripe_customer_enabled(delete_stripe_customer):
        for customer_id in customer_ids:
            _stripe_delete(
                f"customers/{customer_id}",
                idempotency_key=f"admin-delete-user:{user.id}:customer:{customer_id}",
            )
        customer_deleted = True
    elif customer_ids:
        customer_retained = True

    return {
        "cleanup_status": "customer_deleted" if customer_deleted else ("customer_retained" if customer_retained else "succeeded"),
        "stripe_customer_id": customer_ids[0] if customer_ids else None,
        "subscriptions_cancelled": cancelled,
        "customer_deleted": customer_deleted,
        "customer_retained": customer_retained,
    }


def _metadata_value(value: Any) -> str:
    if value is None:
        return "none"
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)[:500]


def _admin_plan_override_price_metadata(user: UserAccount, target_plan: str | None) -> dict[str, str | None]:
    tier = normalize_tier(target_plan)
    if tier not in {"premium", "pro"}:
        return {"tier": tier if target_plan is not None else None, "billing_interval": None, "price_id": None}
    interval = _normalize_subscription_interval(user.subscription_interval) or "monthly"
    price_id = _stripe_price_id(interval, tier)
    if not price_id:
        raise HTTPException(status_code=503, detail="Stripe price ID is not configured for this admin plan override.")
    return {"tier": tier, "billing_interval": interval, "price_id": price_id}


def _admin_billing_metadata(
    user: UserAccount,
    *,
    admin_actor: UserAccount,
    override_type: str,
    requested_state: dict[str, Any],
    plan_price_metadata: dict[str, str | None],
) -> dict[str, Any]:
    state_hash = _admin_billing_state_hash(requested_state)
    metadata: dict[str, Any] = {
        "metadata[walnut_admin_override]": "true",
        "metadata[walnut_admin_override_type]": override_type,
        "metadata[walnut_admin_override_state_hash]": state_hash,
        "metadata[walnut_admin_override_user_id]": str(user.id),
        "metadata[walnut_admin_override_actor_id]": str(admin_actor.id),
    }
    if "manual_tier_override" in requested_state:
        metadata["metadata[walnut_admin_plan_override]"] = _metadata_value(requested_state.get("manual_tier_override"))
        metadata["metadata[walnut_admin_plan_price_id]"] = _metadata_value(plan_price_metadata.get("price_id"))
        metadata["metadata[walnut_admin_plan_interval]"] = _metadata_value(plan_price_metadata.get("billing_interval"))
        metadata["metadata[admin_override]"] = "true"
        metadata["metadata[created_by_admin]"] = "true"
        metadata["metadata[admin_override_plan]"] = _metadata_value(requested_state.get("manual_tier_override"))
        metadata["metadata[target_plan]"] = _metadata_value(requested_state.get("manual_tier_override"))
        metadata["metadata[admin_override_price_mode]"] = _metadata_value(requested_state.get("price_mode"))
        metadata["metadata[app_user_id]"] = str(user.id)
        metadata["metadata[user_id]"] = str(user.id)
    if "monthly_price_override" in requested_state or "annual_price_override" in requested_state:
        metadata["metadata[walnut_admin_price_override_monthly]"] = _metadata_value(requested_state.get("monthly_price_override"))
        metadata["metadata[walnut_admin_price_override_annual]"] = _metadata_value(requested_state.get("annual_price_override"))
        metadata["metadata[walnut_admin_price_override_currency]"] = _metadata_value(requested_state.get("override_currency"))
    if "is_suspended" in requested_state:
        metadata["metadata[walnut_admin_suspended]"] = _metadata_value(bool(requested_state.get("is_suspended")))
    return metadata


def _stripe_current_subscription_for_admin_override(user: UserAccount, customer_id: str | None) -> dict[str, Any] | None:
    if user.stripe_subscription_id:
        return _stripe_get(
            f"subscriptions/{user.stripe_subscription_id}",
            {"expand[]": "items.data.price"},
        )
    if not customer_id:
        return None
    subscriptions = _stripe_get(
        "subscriptions",
        {"customer": customer_id, "status": "all", "limit": 10, "expand[]": "data.items.data.price"},
    )
    data = [item for item in subscriptions.get("data") or [] if isinstance(item, dict)]
    if not data:
        return None
    return sorted(data, key=_stripe_subscription_sort_key, reverse=True)[0]


def _ensure_stripe_customer_for_admin_override(
    user: UserAccount,
    *,
    metadata: dict[str, Any],
    idempotency_key: str,
    preferred_customer_id: str | None = None,
) -> str:
    if not _stripe_secret_key():
        raise HTTPException(status_code=503, detail="Stripe secret key is not configured.")
    customer_id = preferred_customer_id or _stripe_customer_for_user(user)
    payload = _stripe_customer_sync_payload(user, validate_location=False)
    payload.update(metadata)
    if customer_id:
        customer = _stripe_post(
            f"customers/{customer_id}",
            payload,
            idempotency_key=f"{idempotency_key}:customer",
        )
        return _stripe_object_id(customer.get("id")) or customer_id
    customer = _stripe_post(
        "customers",
        payload,
        idempotency_key=f"{idempotency_key}:customer",
    )
    created_customer_id = _stripe_object_id(customer.get("id"))
    if not created_customer_id:
        raise HTTPException(status_code=502, detail="Stripe did not return a customer id.")
    return created_customer_id


def _stripe_subscription_is_admin_update_candidate(subscription: dict[str, Any] | None) -> bool:
    if not subscription:
        return False
    status = str(subscription.get("status") or "").strip().lower()
    return status in {"active", "trialing", "past_due", "incomplete"}


def _stripe_subscription_is_paid_active(subscription: dict[str, Any] | None) -> bool:
    if not subscription:
        return False
    status = str(subscription.get("status") or "").strip().lower()
    return status in PAID_SUBSCRIPTION_STATUSES


def _stripe_live_subscription_for_admin_override(user: UserAccount, customer_id: str | None) -> dict[str, Any] | None:
    if user.stripe_subscription_id:
        subscription = _stripe_get(
            f"subscriptions/{user.stripe_subscription_id}",
            {"expand[]": "items.data.price"},
        )
        if _stripe_subscription_is_admin_update_candidate(subscription):
            return subscription
    if not customer_id:
        return None
    subscriptions = _stripe_get(
        "subscriptions",
        {"customer": customer_id, "status": "all", "limit": 10, "expand[]": "data.items.data.price"},
    )
    data = [item for item in subscriptions.get("data") or [] if isinstance(item, dict) and _stripe_subscription_is_admin_update_candidate(item)]
    if not data:
        return None
    return sorted(data, key=_stripe_subscription_sort_key, reverse=True)[0]


def _admin_plan_subscription_metadata(
    user: UserAccount,
    *,
    admin_actor: UserAccount,
    target_plan: str,
    price_mode: str,
    billing_interval: SubscriptionInterval,
    stripe_price_id: str,
) -> dict[str, Any]:
    return {
        "metadata[user_id]": str(user.id),
        "metadata[email]": user.email,
        "metadata[plan]": target_plan,
        "metadata[tier]": target_plan,
        "metadata[interval]": billing_interval,
        "metadata[billing_interval]": billing_interval,
        "metadata[price_id]": stripe_price_id,
        "metadata[created_by_admin]": "true",
        "metadata[admin_override]": "true",
        "metadata[target_plan]": target_plan,
        "metadata[admin_override_plan]": target_plan,
        "metadata[admin_override_price_mode]": price_mode,
        "metadata[admin_override_actor]": str(admin_actor.id),
        "metadata[app_user_id]": str(user.id),
    }


def _admin_default_plan_price_id(target_plan: str, billing_interval: SubscriptionInterval) -> str:
    price_id = _stripe_price_id(billing_interval, target_plan)
    if not price_id:
        raise HTTPException(status_code=503, detail=f"Stripe price ID is not configured for {target_plan} {billing_interval}.")
    return price_id


def _stripe_product_id_for_plan_price(target_plan: str, billing_interval: SubscriptionInterval) -> str:
    default_price_id = _admin_default_plan_price_id(target_plan, billing_interval)
    price = _stripe_get(f"prices/{default_price_id}", {"expand[]": "product"})
    product_id = _stripe_object_id(price.get("product"))
    if not product_id:
        raise HTTPException(status_code=503, detail="Stripe product is not configured for this admin custom price.")
    return product_id


def _create_admin_custom_price(
    user: UserAccount,
    *,
    admin_actor: UserAccount,
    target_plan: str,
    custom_price: dict[str, Any],
    idempotency_key: str,
) -> dict[str, Any]:
    billing_interval = custom_price.get("interval") if custom_price.get("interval") in {"monthly", "annual"} else "monthly"
    product_id = _stripe_product_id_for_plan_price(target_plan, billing_interval)
    amount_cents = int(custom_price["amount_cents"])
    currency = str(custom_price.get("currency") or "USD").strip().lower()[:8] or "usd"
    metadata = _admin_plan_subscription_metadata(
        user,
        admin_actor=admin_actor,
        target_plan=target_plan,
        price_mode="custom",
        billing_interval=billing_interval,
        stripe_price_id="pending",
    )
    payload: dict[str, Any] = {
        "unit_amount": amount_cents,
        "currency": currency,
        "recurring[interval]": _stripe_recurring_interval(billing_interval),
        "product": product_id,
        "nickname": f"Admin {target_plan} override {currency.upper()} {amount_cents / 100:.2f} / {billing_interval}",
        **metadata,
    }
    price = _stripe_post("prices", payload, idempotency_key=f"{idempotency_key}:price")
    price_id = _stripe_object_id(price.get("id"))
    if not price_id:
        raise HTTPException(status_code=502, detail="Stripe did not return a custom price id.")
    return {
        "price_id": price_id,
        "billing_interval": billing_interval,
        "amount_cents": amount_cents,
        "currency": currency.upper(),
    }


def _resolve_admin_subscription_price(
    user: UserAccount,
    *,
    admin_actor: UserAccount,
    target_plan: str,
    price_mode: str,
    custom_price: dict[str, Any] | None,
    idempotency_key: str,
) -> dict[str, Any]:
    if price_mode == "custom":
        if not custom_price:
            raise HTTPException(status_code=422, detail="Custom admin subscription price is required.")
        return _create_admin_custom_price(
            user,
            admin_actor=admin_actor,
            target_plan=target_plan,
            custom_price=custom_price,
            idempotency_key=idempotency_key,
        )
    billing_interval = _normalize_subscription_interval(user.subscription_interval) or "monthly"
    if price_mode == "default":
        return {
            "price_id": _admin_default_plan_price_id(target_plan, billing_interval),
            "billing_interval": billing_interval,
            "amount_cents": None,
            "currency": None,
        }
    free_price_id = _admin_free_grant_price_id(target_plan)
    if not free_price_id:
        raise HTTPException(
            status_code=503,
            detail=f"{_admin_free_grant_price_env_name(target_plan)} is required for free admin grants.",
        )
    return {
        "price_id": free_price_id,
        "billing_interval": billing_interval,
        "amount_cents": 0,
        "currency": None,
    }


def _admin_subscription_response_state(subscription: dict[str, Any], *, target_plan: str, billing_interval: SubscriptionInterval, stripe_price_id: str) -> dict[str, Any]:
    status = str(subscription.get("status") or "unknown").strip().lower() or "unknown"
    period_end = _datetime_from_epoch(subscription.get("current_period_end"))
    current_amount, current_currency = _subscription_item_amount_currency(subscription)
    return {
        "stripe_subscription_id": _stripe_object_id(subscription.get("id")),
        "subscription_status": status,
        "subscription_plan": target_plan,
        "subscription_interval": billing_interval,
        "stripe_price_id": stripe_price_id,
        "access_expires_at": period_end,
        "subscription_cancel_at_period_end": bool(subscription.get("cancel_at_period_end")),
        "current_plan_amount_cents": current_amount,
        "current_plan_currency": current_currency,
    }


def _create_or_update_admin_subscription(
    user: UserAccount,
    *,
    admin_actor: UserAccount,
    customer_id: str,
    target_plan: str,
    price_mode: str,
    price_resolution: dict[str, Any],
    metadata: dict[str, Any],
    idempotency_key: str,
) -> dict[str, Any]:
    price_id = str(price_resolution["price_id"])
    billing_interval = price_resolution["billing_interval"]
    subscription_metadata = {
        **metadata,
        **_admin_plan_subscription_metadata(
            user,
            admin_actor=admin_actor,
            target_plan=target_plan,
            price_mode=price_mode,
            billing_interval=billing_interval,
            stripe_price_id=price_id,
        ),
    }
    subscription = _stripe_live_subscription_for_admin_override(user, customer_id)
    if subscription:
        subscription_id = _stripe_object_id(subscription.get("id"))
        if not subscription_id:
            raise HTTPException(status_code=502, detail="Stripe did not return a subscription id.")
        selected_item = _select_subscription_item_price(subscription)
        payload: dict[str, Any] = {
            **subscription_metadata,
            "cancel_at_period_end": "false",
        }
        item_id = selected_item.get("selected_item_id")
        if item_id:
            payload["items[0][id]"] = item_id
        payload["items[0][price]"] = price_id
        if price_mode == "free_admin_grant":
            payload["proration_behavior"] = "none"
        else:
            payload["payment_behavior"] = "error_if_incomplete"
        updated = _stripe_post(
            f"subscriptions/{subscription_id}",
            payload,
            idempotency_key=f"{idempotency_key}:subscription-update",
        )
        if not _stripe_object_id(updated.get("id")):
            updated["id"] = subscription_id
        subscription = updated
    else:
        payload = {
            "customer": customer_id,
            "items[0][price]": price_id,
            "cancel_at_period_end": "false",
            **subscription_metadata,
        }
        if price_mode != "free_admin_grant":
            payload["payment_behavior"] = "error_if_incomplete"
        subscription = _stripe_post(
            "subscriptions",
            payload,
            idempotency_key=f"{idempotency_key}:subscription-create",
        )
    if not _stripe_subscription_is_paid_active(subscription):
        raise HTTPException(status_code=502, detail="Stripe subscription is not active after admin override.")
    return _admin_subscription_response_state(
        subscription,
        target_plan=target_plan,
        billing_interval=billing_interval,
        stripe_price_id=price_id,
    )


def _cancel_admin_subscription_for_free(
    user: UserAccount,
    *,
    customer_id: str | None,
    idempotency_key: str,
) -> dict[str, Any]:
    subscription = _stripe_live_subscription_for_admin_override(user, customer_id)
    if not subscription:
        return {
            "stripe_subscription_id": None,
            "subscription_status": "free",
            "subscription_plan": "free",
            "subscription_interval": None,
            "stripe_price_id": None,
            "access_expires_at": None,
            "subscription_cancel_at_period_end": False,
            "current_plan_amount_cents": None,
            "current_plan_currency": None,
        }
    subscription_id = _stripe_object_id(subscription.get("id"))
    if not subscription_id:
        raise HTTPException(status_code=502, detail="Stripe did not return a subscription id.")
    canceled = _stripe_delete(
        f"subscriptions/{subscription_id}",
        idempotency_key=f"{idempotency_key}:subscription-cancel",
    )
    return {
        "stripe_subscription_id": subscription_id,
        "subscription_status": str(canceled.get("status") or "canceled").strip().lower() or "canceled",
        "subscription_plan": "free",
        "subscription_interval": None,
        "stripe_price_id": None,
        "access_expires_at": None,
        "subscription_cancel_at_period_end": False,
        "current_plan_amount_cents": None,
        "current_plan_currency": None,
    }


def _apply_admin_billing_stripe_links(user: UserAccount, sync_result: dict[str, Any] | None) -> None:
    if not sync_result:
        return
    customer_id = sync_result.get("stripe_customer_id")
    subscription_id = sync_result.get("stripe_subscription_id")
    if customer_id and not user.stripe_customer_id:
        user.stripe_customer_id = str(customer_id)
    if subscription_id and not user.stripe_subscription_id:
        user.stripe_subscription_id = str(subscription_id)
    if "stripe_price_id" in sync_result:
        user.stripe_price_id = sync_result.get("stripe_price_id")
    if sync_result.get("subscription_status"):
        user.subscription_status = str(sync_result["subscription_status"])
    if sync_result.get("subscription_plan"):
        user.subscription_plan = normalize_tier(sync_result["subscription_plan"])
    if sync_result.get("subscription_interval") in {"monthly", "annual"}:
        user.subscription_interval = sync_result.get("subscription_interval")
    if "access_expires_at" in sync_result:
        user.access_expires_at = sync_result.get("access_expires_at")
    if "subscription_cancel_at_period_end" in sync_result:
        user.subscription_cancel_at_period_end = bool(sync_result.get("subscription_cancel_at_period_end"))
    if "current_plan_amount_cents" in sync_result and sync_result.get("current_plan_amount_cents") is not None:
        user.current_plan_amount_cents = int(sync_result["current_plan_amount_cents"])
    if sync_result.get("current_plan_currency"):
        user.current_plan_currency = str(sync_result["current_plan_currency"]).upper()


def sync_admin_billing_override_to_stripe(
    db: Session,
    *,
    user: UserAccount,
    requested_override: dict[str, Any],
    admin_actor: UserAccount,
    request: Request | None = None,
) -> dict[str, Any]:
    override_type = _admin_billing_override_type(requested_override)
    if override_type == "none":
        return {}
    previous_state = _admin_billing_previous_state(user)
    requested_state = _admin_billing_requested_state(user, requested_override)
    plan_price_metadata: dict[str, str | None] = {}
    customer_id: str | None = None
    subscription_id: str | None = None
    subscription_state: dict[str, Any] = {}
    try:
        raw_target_plan = requested_state.get("manual_tier_override") if "manual_tier_override" in requested_state else None
        target_plan = normalize_tier(raw_target_plan) if raw_target_plan is not None else None
        price_mode = _admin_plan_price_mode(requested_state.get("price_mode"))
        custom_price = _admin_custom_price_state(requested_state.get("custom_price"))
        if "manual_tier_override" in requested_state:
            if target_plan in ADMIN_BILLING_PAID_TIERS:
                plan_price_metadata = {"tier": target_plan, "billing_interval": _normalize_subscription_interval(user.subscription_interval) or "monthly", "price_id": None}
            else:
                plan_price_metadata = _admin_plan_override_price_metadata(user, requested_state.get("manual_tier_override"))
                if plan_price_metadata.get("price_id"):
                    requested_state["stripe_price_id"] = plan_price_metadata["price_id"]
                    requested_state["stripe_price_interval"] = plan_price_metadata["billing_interval"]
        idempotency_key = _admin_billing_idempotency_key(
            user,
            override_type,
            requested_state,
            action_id=_admin_billing_request_action_id(request),
        )
        metadata = _admin_billing_metadata(
            user,
            admin_actor=admin_actor,
            override_type=override_type,
            requested_state=requested_state,
            plan_price_metadata=plan_price_metadata,
        )
        if target_plan in ADMIN_BILLING_PAID_TIERS:
            customer_id = _ensure_stripe_customer_for_admin_override(
                user,
                metadata=metadata,
                idempotency_key=idempotency_key,
                preferred_customer_id=None,
            )
            price_resolution = _resolve_admin_subscription_price(
                user,
                admin_actor=admin_actor,
                target_plan=target_plan,
                price_mode=price_mode,
                custom_price=custom_price,
                idempotency_key=idempotency_key,
            )
            requested_state["price_mode"] = price_mode
            requested_state["stripe_price_id"] = price_resolution["price_id"]
            requested_state["stripe_price_interval"] = price_resolution["billing_interval"]
            if price_resolution.get("amount_cents") is not None:
                requested_state["stripe_price_amount_cents"] = price_resolution.get("amount_cents")
            if price_resolution.get("currency"):
                requested_state["stripe_price_currency"] = price_resolution.get("currency")
            subscription_state = _create_or_update_admin_subscription(
                user,
                admin_actor=admin_actor,
                customer_id=customer_id,
                target_plan=target_plan,
                price_mode=price_mode,
                price_resolution=price_resolution,
                metadata=metadata,
                idempotency_key=idempotency_key,
            )
            subscription_id = subscription_state.get("stripe_subscription_id")
        elif target_plan == "free":
            subscription: dict[str, Any] | None = None
            if user.stripe_subscription_id:
                subscription = _stripe_current_subscription_for_admin_override(user, None)
            subscription_customer_id = _stripe_object_id(subscription.get("customer")) if subscription else None
            if user.stripe_customer_id or user.stripe_subscription_id or _stripe_secret_key():
                customer_id = _ensure_stripe_customer_for_admin_override(
                    user,
                    metadata=metadata,
                    idempotency_key=idempotency_key,
                    preferred_customer_id=subscription_customer_id,
                )
            subscription_state = _cancel_admin_subscription_for_free(
                user,
                customer_id=customer_id,
                idempotency_key=idempotency_key,
            )
            subscription_id = subscription_state.get("stripe_subscription_id")
        else:
            subscription = _stripe_current_subscription_for_admin_override(user, None) if user.stripe_subscription_id else None
            subscription_customer_id = _stripe_object_id(subscription.get("customer")) if subscription else None
            customer_id = _ensure_stripe_customer_for_admin_override(
                user,
                metadata=metadata,
                idempotency_key=idempotency_key,
                preferred_customer_id=subscription_customer_id,
            )
            if subscription is None:
                subscription = _stripe_current_subscription_for_admin_override(user, customer_id)
            if subscription:
                subscription_id = _stripe_object_id(subscription.get("id"))
                if subscription_id:
                    _stripe_post(
                        f"subscriptions/{subscription_id}",
                        metadata,
                        idempotency_key=f"{idempotency_key}:subscription",
                    )
        _record_admin_billing_override_audit(
            db,
            admin_actor=admin_actor,
            user=user,
            override_type=override_type,
            previous_state=previous_state,
            requested_state=requested_state,
            stripe_customer_id=customer_id,
            stripe_subscription_id=subscription_id,
            stripe_sync_status="succeeded",
        )
        return {
            "override_type": override_type,
            "stripe_customer_id": customer_id,
            "stripe_subscription_id": subscription_id,
            "stripe_sync_status": "succeeded",
            **subscription_state,
        }
    except Exception as exc:
        safe_error = _admin_billing_safe_error(exc)
        logger.warning(
            "admin_billing_override_stripe_sync_failed admin_user_id=%s target_user_id=%s override_type=%s error=%s",
            admin_actor.id,
            user.id,
            override_type,
            safe_error,
        )
        db.rollback()
        _record_admin_billing_override_audit(
            db,
            admin_actor=admin_actor,
            user=user,
            override_type=override_type,
            previous_state=previous_state,
            requested_state=requested_state,
            stripe_customer_id=customer_id,
            stripe_subscription_id=subscription_id,
            stripe_sync_status="failed",
            error_message=safe_error,
        )
        db.commit()
        raise _admin_billing_sync_exception(exc) from exc


def _admin_user_search_id(value: str) -> int | None:
    cleaned = value.strip().upper()
    if cleaned.startswith("U-"):
        cleaned = cleaned[2:]
    if not cleaned.isdigit():
        return None
    try:
        return int(cleaned)
    except ValueError:
        return None


def _admin_user_filtered_query(
    *,
    search: str | None,
    plan: AdminUserPlanFilter,
    status: str | None,
    country: str | None,
    admin: AdminUserAdminFilter,
) -> tuple[Any, dict[str, Any]]:
    conditions = []
    normalized_search = (search or "").strip()[:160]
    if normalized_search:
        pattern = f"%{normalized_search.lower()}%"
        search_conditions = [
            func.lower(func.coalesce(UserAccount.email, "")).like(pattern),
            func.lower(func.coalesce(UserAccount.original_email, "")).like(pattern),
            func.lower(func.coalesce(UserAccount.name, "")).like(pattern),
            func.lower(func.coalesce(UserAccount.first_name, "")).like(pattern),
            func.lower(func.coalesce(UserAccount.last_name, "")).like(pattern),
            func.lower(cast(UserAccount.id, String)).like(pattern),
        ]
        search_id = _admin_user_search_id(normalized_search)
        if search_id is not None:
            search_conditions.append(UserAccount.id == search_id)
        conditions.append(or_(*search_conditions))

    normalized_plan = (plan or "all").strip().lower()
    if normalized_plan != "all":
        if normalized_plan == "admin":
            conditions.append(UserAccount.role == "admin")
        else:
            conditions.append(func.lower(func.coalesce(UserAccount.manual_tier_override, UserAccount.entitlement_tier, "free")) == normalized_plan)

    normalized_status = (status or "").strip().lower()
    if normalized_status:
        if normalized_status in {"all_with_deleted", "include_deleted"}:
            pass
        elif normalized_status == "deleted":
            conditions.append(UserAccount.deleted_at.is_not(None))
        elif normalized_status == "suspended":
            conditions.append(UserAccount.deleted_at.is_(None))
            conditions.append(UserAccount.is_suspended.is_(True))
        elif normalized_status == "active":
            conditions.append(UserAccount.deleted_at.is_(None))
            conditions.append(UserAccount.is_suspended.is_(False))
            conditions.append(or_(UserAccount.subscription_status.is_(None), func.lower(UserAccount.subscription_status) == "active"))
        else:
            conditions.append(UserAccount.deleted_at.is_(None))
            conditions.append(UserAccount.is_suspended.is_(False))
            conditions.append(func.lower(UserAccount.subscription_status) == normalized_status)
    else:
        conditions.append(UserAccount.deleted_at.is_(None))

    country_code = (country or "").strip().upper()
    if country_code:
        if len(country_code) != 2:
            raise HTTPException(status_code=422, detail="country must use a two-letter ISO country code.")
        conditions.append(func.upper(UserAccount.country) == country_code)

    if admin == "admin":
        conditions.append(UserAccount.role == "admin")
    elif admin == "non_admin":
        conditions.append(UserAccount.role != "admin")

    query = select(UserAccount)
    if conditions:
        query = query.where(*conditions)
    return query, {
        "search": normalized_search or None,
        "plan": normalized_plan,
        "status": normalized_status or None,
        "country": country_code or None,
        "admin": admin,
    }


def _admin_user_rows(
    db: Session,
    *,
    plan: AdminUserPlanFilter,
    status: str | None,
    country: str | None,
    admin: AdminUserAdminFilter,
    sort_by: AdminUserSortBy,
    sort_dir: AdminUserSortDir,
    page: int | None = None,
    page_size: int | None = None,
    search: str | None = None,
) -> tuple[list[UserAccount], int, dict[str, Any]]:
    query, filters = _admin_user_filtered_query(search=search, plan=plan, status=status, country=country, admin=admin)
    total = int(db.execute(select(func.count()).select_from(query.subquery())).scalar_one() or 0)
    sort_columns = {
        "created_at": UserAccount.created_at,
        "last_seen_at": UserAccount.last_seen_at,
        "email": UserAccount.email,
        "name": UserAccount.name,
        "country": UserAccount.country,
        "plan": func.coalesce(UserAccount.manual_tier_override, UserAccount.entitlement_tier, "free"),
        "status": func.coalesce(UserAccount.subscription_status, "active"),
    }
    sort_column = sort_columns[sort_by]
    ordered = query.order_by(sort_column.asc() if sort_dir == "asc" else sort_column.desc(), UserAccount.id.desc())
    if page is not None and page_size is not None:
        ordered = ordered.offset((page - 1) * page_size).limit(page_size)
    rows = db.execute(ordered).scalars().all()
    return rows, total, filters


def _export_filename(prefix: str, extension: str) -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d")
    return f"{prefix}-{stamp}.{extension}"


def _xlsx_col_name(index: int) -> str:
    value = index + 1
    letters = ""
    while value:
        value, remainder = divmod(value - 1, 26)
        letters = chr(65 + remainder) + letters
    return letters


def _xlsx_inline_cell(reference: str, value: Any) -> str:
    return f'<c r="{reference}" t="inlineStr"><is><t>{html_escape(str(value or ""))}</t></is></c>'


def _table_xlsx(rows: list[dict[str, Any]], columns: tuple[tuple[str, str], ...], *, sheet_name: str, title: str) -> bytes:
    sheet_rows: list[str] = []
    header_cells = [
        _xlsx_inline_cell(f"{_xlsx_col_name(index)}1", header)
        for index, (header, _key) in enumerate(columns)
    ]
    sheet_rows.append(f'<row r="1">{"".join(header_cells)}</row>')
    for row_index, row in enumerate(rows, start=2):
        cells = [
            _xlsx_inline_cell(f"{_xlsx_col_name(col_index)}{row_index}", row[key])
            for col_index, (_header, key) in enumerate(columns)
        ]
        sheet_rows.append(f'<row r="{row_index}">{"".join(cells)}</row>')

    worksheet = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        f'<sheetData>{"".join(sheet_rows)}</sheetData>'
        "</worksheet>"
    )
    output = BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as workbook:
        workbook.writestr(
            "[Content_Types].xml",
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
            '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
            '<Default Extension="xml" ContentType="application/xml"/>'
            '<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
            '<Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
            '<Override PartName="/docProps/core.xml" ContentType="application/vnd.openxmlformats-package.core-properties+xml"/>'
            '<Override PartName="/docProps/app.xml" ContentType="application/vnd.openxmlformats-officedocument.extended-properties+xml"/>'
            "</Types>",
        )
        workbook.writestr(
            "_rels/.rels",
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
            '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>'
            '<Relationship Id="rId2" Type="http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties" Target="docProps/core.xml"/>'
            '<Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties" Target="docProps/app.xml"/>'
            "</Relationships>",
        )
        workbook.writestr(
            "xl/workbook.xml",
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
            'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
            f'<sheets><sheet name="{html_escape(sheet_name)}" sheetId="1" r:id="rId1"/></sheets></workbook>',
        )
        workbook.writestr(
            "xl/_rels/workbook.xml.rels",
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
            '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>'
            "</Relationships>",
        )
        workbook.writestr("xl/worksheets/sheet1.xml", worksheet)
        workbook.writestr(
            "docProps/core.xml",
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties" '
            f'xmlns:dc="http://purl.org/dc/elements/1.1/"><dc:title>{html_escape(title)}</dc:title></cp:coreProperties>',
        )
        workbook.writestr(
            "docProps/app.xml",
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<Properties xmlns="http://schemas.openxmlformats.org/officeDocument/2006/extended-properties">'
            "<Application>Walnut Market Terminal</Application></Properties>",
        )
    return output.getvalue()


def _sales_ledger_xlsx(rows: list[dict[str, Any]]) -> bytes:
    return _table_xlsx(rows, SALES_LEDGER_COLUMNS, sheet_name="Sales Ledger", title="Sales Ledger")


def _pdf_escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def _pdf_text_line(x: int, y: int, text: str, size: int = 8) -> str:
    return f"BT /F1 {size} Tf {x} {y} Td ({_pdf_escape(text)}) Tj ET\n"


def _sales_ledger_pdf(rows: list[dict[str, Any]], filters: dict[str, Any]) -> bytes:
    pages: list[str] = []
    title = "Sales Ledger"
    filter_line = "Filters: " + ", ".join(f"{key}={value}" for key, value in filters.items() if value) if filters else "Filters: none"
    current_lines = [_pdf_text_line(36, 570, title, 14), _pdf_text_line(36, 552, filter_line[:120], 8)]
    y = 530
    for row in rows:
        if y < 70:
            pages.append("".join(current_lines))
            current_lines = [_pdf_text_line(36, 570, title, 14), _pdf_text_line(36, 552, "continued", 8)]
            y = 530
        line_one = (
            f"{row['transaction_id']} | {row['date_charged'] or ''} | {row['customer_name']} | "
            f"{row['gross_amount_display']} | {row['status_refund_state']}"
        )
        line_two = (
            f"{row['description']} | {row['country']} {row['state_province']} | net {row['net_revenue_display']} | "
            f"{row['vat1_label']} {row['vat1_collected_display']} | {row['vat2_label']} {row['vat2_collected_display']}"
        )
        current_lines.append(_pdf_text_line(36, y, line_one[:145], 8))
        current_lines.append(_pdf_text_line(36, y - 11, line_two[:145], 8))
        y -= 30
    pages.append("".join(current_lines))

    objects: list[bytes] = []
    page_object_ids: list[int] = []
    content_object_ids: list[int] = []
    for content in pages:
        content_object_ids.append(4 + len(objects))
        objects.append(f"<< /Length {len(content.encode('latin-1', 'replace'))} >>\nstream\n{content}endstream".encode("latin-1", "replace"))
        page_object_ids.append(4 + len(objects))
        objects.append(b"")

    kids = " ".join(f"{object_id} 0 R" for object_id in page_object_ids)
    base_objects = [
        f"<< /Type /Catalog /Pages 2 0 R >>".encode(),
        f"<< /Type /Pages /Kids [{kids}] /Count {len(page_object_ids)} >>".encode(),
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
    ]

    full_objects = base_objects + objects
    for index, page_object_id in enumerate(page_object_ids):
        content_id = content_object_ids[index]
        full_objects[page_object_id - 1] = (
            f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 792 612] "
            f"/Resources << /Font << /F1 3 0 R >> >> /Contents {content_id} 0 R >>"
        ).encode()

    pdf = bytearray(b"%PDF-1.4\n")
    offsets = [0]
    for object_id, body in enumerate(full_objects, start=1):
        offsets.append(len(pdf))
        pdf.extend(f"{object_id} 0 obj\n".encode())
        pdf.extend(body)
        pdf.extend(b"\nendobj\n")
    xref_at = len(pdf)
    pdf.extend(f"xref\n0 {len(full_objects) + 1}\n".encode())
    pdf.extend(b"0000000000 65535 f \n")
    for offset in offsets[1:]:
        pdf.extend(f"{offset:010d} 00000 n \n".encode())
    pdf.extend(
        f"trailer\n<< /Size {len(full_objects) + 1} /Root 1 0 R >>\nstartxref\n{xref_at}\n%%EOF\n".encode()
    )
    return bytes(pdf)


def _admin_users_export_rows(users: list[UserAccount], *, db: Session) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    billing_rows = _latest_billing_rows_by_user(db, users)
    paid_rows = _successful_billing_rows_by_user(db, users)
    plan_prices = _plan_price_lookup(db)
    for user in users:
        row = _admin_user_row(user, latest_billing_row=billing_rows.get(user.id), billing_rows=paid_rows.get(user.id), plan_prices=plan_prices)
        rows.append(
            {
                **row,
                "name": row.get("name") or "",
                "country": row.get("country") or "",
                "state_province": row.get("state_province") or "",
                "current_plan_display": row.get("current_plan_display") or "",
                "total_paid_display": row.get("total_paid_display") or "",
                "last_payment_display": row.get("last_payment_display") or "",
                "billing_interval_display": row.get("billing_interval_display") or "",
                "billing_price_display": row.get("billing_price_display") or "",
                "billing_frequency_display": row.get("billing_frequency_display") or "",
                "created_at": _iso_or_blank(row.get("created_at")),
                "last_seen_at": _iso_or_blank(row.get("last_seen_at")),
                "access_expires_at": _iso_or_blank(row.get("access_expires_at")),
                "current_period_end": _iso_or_blank(row.get("current_period_end")),
                "reactivation_expires_at": _iso_or_blank(row.get("reactivation_expires_at")),
                "reactivation_expired": "yes" if row.get("reactivation_expired") else "no",
                "subscription_cancel_at_period_end": "yes" if row.get("subscription_cancel_at_period_end") else "no",
            }
        )
    return rows


def _admin_users_xlsx(rows: list[dict[str, Any]]) -> bytes:
    return _table_xlsx(rows, ADMIN_USER_COLUMNS, sheet_name="Users", title="Admin Users")


def _admin_users_pdf(rows: list[dict[str, Any]], filters: dict[str, Any]) -> bytes:
    pages: list[str] = []
    title = "Admin Users"
    filter_line = "Filters: " + ", ".join(f"{key}={value}" for key, value in filters.items() if value and value != "all")
    if filter_line == "Filters: ":
        filter_line = "Filters: none"
    current_lines = [_pdf_text_line(36, 570, title, 14), _pdf_text_line(36, 552, filter_line[:120], 8)]
    y = 530
    for row in rows:
        if y < 70:
            pages.append("".join(current_lines))
            current_lines = [_pdf_text_line(36, 570, title, 14), _pdf_text_line(36, 552, "continued", 8)]
            y = 530
        line_one = (
            f"{row['user_display_id']} | {row['name'] or '-'} | {row['email']} | {row['country'] or '-'} {row['state_province'] or '-'} | "
            f"{row['plan']} | {row['billing_interval_display'] or '-'} | {row['current_plan_display'] or '-'} | paid {row['total_paid_display'] or '-'} | "
            f"{row.get('subscription_state_label') or row['status']} | admin {row['admin_flag']}"
        )
        line_two = (
            f"registered {row['created_at'] or '-'} | last active {row['last_seen_at'] or '-'} | "
            f"expires {row['access_expires_at'] or '-'} | reactivation expired {row.get('reactivation_expired') or '-'}"
        )
        current_lines.append(_pdf_text_line(36, y, line_one[:145], 8))
        current_lines.append(_pdf_text_line(36, y - 11, line_two[:145], 8))
        y -= 30
    pages.append("".join(current_lines))

    objects: list[bytes] = []
    page_object_ids: list[int] = []
    content_object_ids: list[int] = []
    for content in pages:
        content_object_ids.append(4 + len(objects))
        objects.append(f"<< /Length {len(content.encode('latin-1', 'replace'))} >>\nstream\n{content}endstream".encode("latin-1", "replace"))
        page_object_ids.append(4 + len(objects))
        objects.append(b"")

    kids = " ".join(f"{object_id} 0 R" for object_id in page_object_ids)
    base_objects = [
        f"<< /Type /Catalog /Pages 2 0 R >>".encode(),
        f"<< /Type /Pages /Kids [{kids}] /Count {len(page_object_ids)} >>".encode(),
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
    ]

    full_objects = base_objects + objects
    for index, page_object_id in enumerate(page_object_ids):
        content_id = content_object_ids[index]
        full_objects[page_object_id - 1] = (
            f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 792 612] "
            f"/Resources << /Font << /F1 3 0 R >> >> /Contents {content_id} 0 R >>"
        ).encode()

    pdf = bytearray(b"%PDF-1.4\n")
    offsets = [0]
    for object_id, body in enumerate(full_objects, start=1):
        offsets.append(len(pdf))
        pdf.extend(f"{object_id} 0 obj\n".encode())
        pdf.extend(body)
        pdf.extend(b"\nendobj\n")
    xref_at = len(pdf)
    pdf.extend(f"xref\n0 {len(full_objects) + 1}\n".encode())
    pdf.extend(b"0000000000 65535 f \n")
    for offset in offsets[1:]:
        pdf.extend(f"{offset:010d} 00000 n \n".encode())
    pdf.extend(
        f"trailer\n<< /Size {len(full_objects) + 1} /Root 1 0 R >>\nstartxref\n{xref_at}\n%%EOF\n".encode()
    )
    return bytes(pdf)


def _stripe_secret_key() -> str | None:
    return os.getenv("STRIPE_SECRET_KEY", "").strip() or None


def _stripe_price_env_name(billing_interval: str | None = None, tier: str | None = None) -> str:
    return stripe_price_env_name(billing_interval, tier)


def _stripe_price_label(billing_interval: str | None = None, tier: str | None = None) -> str:
    return stripe_price_label(billing_interval, tier)


def _stripe_price_id(billing_interval: str | None = None, tier: str | None = None) -> str | None:
    return stripe_price_id(billing_interval, tier)


def _stripe_price_mapping() -> dict[str, dict[str, str]]:
    mapping: dict[str, dict[str, str]] = {}
    for tier in ("premium", "pro"):
        for interval in ("monthly", "annual"):
            price_id = _stripe_price_id(interval, tier)
            if price_id:
                mapping[price_id] = {
                    "tier": tier,
                    "billing_interval": interval,
                    "env_name": _stripe_price_env_name(interval, tier),
                }
        admin_free_price_id = _admin_free_grant_price_id(tier)
        if admin_free_price_id:
            mapping[admin_free_price_id] = {
                "tier": tier,
                "billing_interval": "monthly",
                "env_name": _admin_free_grant_price_env_name(tier),
                "price_mode": "free_admin_grant",
            }
    return mapping


def _stripe_price_mapping_result(price_id: str | None) -> dict[str, Any]:
    cleaned = str(price_id or "").strip()
    if not cleaned:
        return {"matched": False, "reason": "missing_price_id"}
    mapping = _stripe_price_mapping()
    match = mapping.get(cleaned)
    if not match:
        return {"matched": False, "price_id": cleaned, "reason": "unmapped_price_id"}
    return {"matched": True, "price_id": cleaned, **match}


def _stripe_webhook_secret() -> str | None:
    return os.getenv("STRIPE_WEBHOOK_SECRET", "").strip() or None


def _log_billing_readiness(*, context: str, readiness: dict[str, Any]) -> None:
    log_billing_readiness(logger, context=context, readiness=readiness)


def _require_checkout_readiness(readiness: dict[str, Any]) -> None:
    checkout = readiness["checkout"]
    if checkout["ready"]:
        return
    raise HTTPException(
        status_code=503,
        detail={
            "code": "stripe_checkout_not_ready",
            "message": "Stripe checkout is not configured for this plan.",
            "missing_env_vars": checkout["missing_env_vars"],
        },
    )


def _require_stripe_api_readiness(readiness: dict[str, Any], *, code: str, message: str) -> None:
    stripe_api = readiness["stripe_api"]
    if stripe_api["ready"]:
        return
    raise HTTPException(
        status_code=503,
        detail={
            "code": code,
            "message": message,
            "missing_env_vars": stripe_api["missing_env_vars"],
        },
    )


def _require_webhook_readiness(readiness: dict[str, Any]) -> None:
    webhooks = readiness["webhooks"]
    if webhooks["ready"]:
        return
    logger.warning(
        "stripe_webhook_disabled billing_enabled=%s missing_env_vars=%s",
        readiness["billing_enabled"],
        webhooks["missing_env_vars"],
    )
    raise HTTPException(status_code=503, detail="Stripe billing is disabled.")


def _frontend_base_url() -> str:
    return os.getenv("FRONTEND_BASE_URL", "http://localhost:3000").rstrip("/")


AUTH_APP_FRONTEND_HOST = "app.walnutmarkets.com"
AUTH_APP_FRONTEND_DEFAULT_URL = f"https://{AUTH_APP_FRONTEND_HOST}"
DEFAULT_POST_LOGIN_PATH = "/?mode=all"


def _env_url(name: str) -> str | None:
    value = os.getenv(name, "").strip().rstrip("/")
    return value or None


def _url_host(value: str | None) -> str:
    if not value:
        return ""
    return (urlparse(value).hostname or "").lower()


def _safe_app_return_path(return_to: str | None, fallback: str = DEFAULT_POST_LOGIN_PATH) -> str:
    raw = (return_to or "").strip()
    if not raw or not raw.startswith("/") or raw.startswith("//") or "\\" in raw:
        return fallback
    parsed = urlparse(raw)
    if parsed.scheme or parsed.netloc or not parsed.path.startswith("/") or parsed.path.startswith("//"):
        return fallback
    return urlunparse(("", "", parsed.path, parsed.params, parsed.query, parsed.fragment))


def _authenticated_app_frontend_base_url() -> str:
    for name in ("FRONTEND_APP_URL", "APP_BASE_URL", "NEXT_PUBLIC_APP_BASE_URL", "NEXT_PUBLIC_APP_URL", "FRONTEND_BASE_URL"):
        value = _env_url(name)
        if _url_host(value) == AUTH_APP_FRONTEND_HOST:
            return value
    if _app_environment() in {"local", "dev", "development", "test", "testing"}:
        return _frontend_base_url()
    return AUTH_APP_FRONTEND_DEFAULT_URL


def _checkout_success_url() -> str:
    return f"{_authenticated_app_frontend_base_url()}/account/billing?checkout=success"


def _checkout_cancel_url() -> str:
    return f"{_authenticated_app_frontend_base_url()}/pricing?checkout=cancelled"


def _customer_portal_return_url() -> str:
    configured = _env_url("STRIPE_CUSTOMER_PORTAL_RETURN_URL")
    if _url_host(configured) == AUTH_APP_FRONTEND_HOST:
        return configured
    return f"{_authenticated_app_frontend_base_url()}/account/billing?portal_return=1"


def _api_base_url() -> str:
    return os.getenv("PUBLIC_API_BASE_URL", os.getenv("API_BASE", "https://congress-tracker-api.fly.dev")).rstrip("/")


def _app_base_url() -> str:
    return os.getenv("APP_BASE_URL", os.getenv("PUBLIC_API_BASE_URL", os.getenv("API_BASE", "http://localhost:8000"))).rstrip("/")


def _google_client_id(db: Session | None = None, *, prefer_env: bool = False) -> str | None:
    env_client_id = os.getenv("GOOGLE_CLIENT_ID", "").strip() or None
    if prefer_env and env_client_id:
        return env_client_id
    if db is not None:
        saved = _setting_value(db, "google_client_id")
        if saved:
            return saved
    return env_client_id


def _google_client_secret() -> str | None:
    return os.getenv("GOOGLE_CLIENT_SECRET", "").strip() or None


def _google_redirect_uri() -> str:
    configured = _env_url("GOOGLE_REDIRECT_URI")
    if _url_host(configured) == AUTH_APP_FRONTEND_HOST:
        return f"{configured}/auth/google/callback" if not configured.endswith("/auth/google/callback") else configured
    return f"{_authenticated_app_frontend_base_url()}/auth/google/callback"


def _stripe_post(path: str, data: dict[str, Any], *, idempotency_key: str | None = None) -> dict[str, Any]:
    secret = _stripe_secret_key()
    if not secret:
        raise HTTPException(status_code=503, detail="Stripe secret key is not configured.")
    headers = {"Idempotency-Key": idempotency_key} if idempotency_key else None
    response = requests.post(
        f"https://api.stripe.com/v1/{path.lstrip('/')}",
        auth=(secret, ""),
        data=data,
        headers=headers,
        timeout=20,
    )
    if response.status_code >= 400:
        message = "Stripe request failed."
        try:
            parsed = response.json()
            stripe_error = parsed.get("error") if isinstance(parsed, dict) else None
            if isinstance(stripe_error, dict) and isinstance(stripe_error.get("message"), str):
                message = f"Stripe request failed: {stripe_error['message']}"
        except ValueError:
            pass
        raise HTTPException(status_code=502, detail=message)
    parsed = response.json()
    return parsed if isinstance(parsed, dict) else {}


def _stripe_delete(path: str, data: dict[str, Any] | None = None, *, idempotency_key: str | None = None) -> dict[str, Any]:
    secret = _stripe_secret_key()
    if not secret:
        raise HTTPException(status_code=503, detail="Stripe secret key is not configured.")
    headers = {"Idempotency-Key": idempotency_key} if idempotency_key else None
    response = requests.delete(
        f"https://api.stripe.com/v1/{path.lstrip('/')}",
        auth=(secret, ""),
        data=data or {},
        headers=headers,
        timeout=20,
    )
    if response.status_code >= 400:
        message = "Stripe request failed."
        try:
            parsed = response.json()
            stripe_error = parsed.get("error") if isinstance(parsed, dict) else None
            if isinstance(stripe_error, dict) and isinstance(stripe_error.get("message"), str):
                message = f"Stripe request failed: {stripe_error['message']}"
        except ValueError:
            pass
        raise HTTPException(status_code=502, detail=message)
    parsed = response.json()
    return parsed if isinstance(parsed, dict) else {}


def _stripe_get(path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    secret = _stripe_secret_key()
    if not secret:
        raise HTTPException(status_code=503, detail="Stripe secret key is not configured.")
    response = requests.get(
        f"https://api.stripe.com/v1/{path.lstrip('/')}",
        auth=(secret, ""),
        params=params or {},
        timeout=20,
    )
    if response.status_code >= 400:
        message = "Stripe request failed."
        try:
            parsed = response.json()
            stripe_error = parsed.get("error") if isinstance(parsed, dict) else None
            if isinstance(stripe_error, dict) and isinstance(stripe_error.get("message"), str):
                message = f"Stripe request failed: {stripe_error['message']}"
        except ValueError:
            pass
        raise HTTPException(status_code=502, detail=message)
    parsed = response.json()
    return parsed if isinstance(parsed, dict) else {}


def _coerce_response_and_db(response: Response | Any | None, db: Session | Any) -> tuple[Response | None, Session]:
    if response is not None and not hasattr(response, "set_cookie") and hasattr(response, "execute"):
        return None, response
    return response, db


def _stripe_address_payload(user: UserAccount) -> dict[str, Any]:
    values = {
        "address[country]": user.country,
        "address[state]": user.state_province,
        "address[postal_code]": user.postal_code,
        "address[city]": user.city,
        "address[line1]": user.address_line1,
        "address[line2]": user.address_line2,
    }
    return {key: value for key, value in values.items() if value}


def _stripe_customer_sync_payload(user: UserAccount, *, validate_location: bool) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "email": user.email,
        "name": user.name or _display_name(user.first_name, user.last_name) or user.email,
        "metadata[user_id]": str(user.id),
        "metadata[email]": user.email,
    }
    payload.update(_stripe_address_payload(user))
    if validate_location:
        payload["tax[validate_location]"] = "immediately"
    return payload


def _stripe_http_error_message(exc: HTTPException) -> str:
    detail = exc.detail
    if isinstance(detail, dict):
        message = detail.get("message") or detail.get("detail")
        return str(message or detail)
    return str(detail or "")


def _recoverable_stale_stripe_customer_error(exc: HTTPException) -> bool:
    message = _stripe_http_error_message(exc).lower()
    return "no such customer" in message or "similar object exists in test mode" in message


def _clear_stale_free_stripe_state(user: UserAccount) -> None:
    user.stripe_customer_id = None
    user.stripe_subscription_id = None
    user.stripe_price_id = None
    user.subscription_interval = None
    _clear_paid_entitlement(user, status="free")


def _sync_stripe_customer_for_billing(db: Session, user: UserAccount) -> str:
    tax_settings = _stripe_tax_settings(db)
    readiness = stripe_tax_billing_readiness(db, _billing_location_payload(user))
    if tax_settings["automatic_tax_enabled"] and not readiness["can_start_checkout"]:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "billing_location_required",
                "message": "Complete billing location before starting taxable checkout.",
                "missing_fields": readiness["missing_fields"],
            },
        )

    payload = _stripe_customer_sync_payload(user, validate_location=bool(tax_settings["automatic_tax_enabled"]))
    if user.stripe_customer_id:
        try:
            customer = _stripe_post(f"customers/{user.stripe_customer_id}", payload)
        except HTTPException as exc:
            if _has_paid_entitlement_marker(user) or not _recoverable_stale_stripe_customer_error(exc):
                raise
            logger.warning("stripe_customer_stale_free_state_recovered user_id=%s", user.id)
            _clear_stale_free_stripe_state(user)
            db.flush()
            customer = _stripe_post("customers", payload)
            customer_id = str(customer.get("id") or "").strip()
            if not customer_id:
                raise HTTPException(status_code=502, detail="Stripe did not return a customer id.")
            user.stripe_customer_id = customer_id
            db.commit()
            db.refresh(user)
    else:
        customer = _stripe_post("customers", payload)
        customer_id = str(customer.get("id") or "").strip()
        if not customer_id:
            raise HTTPException(status_code=502, detail="Stripe did not return a customer id.")
        user.stripe_customer_id = customer_id
        db.commit()
        db.refresh(user)
    return user.stripe_customer_id or str(customer.get("id") or "")


def _stripe_config_status() -> dict[str, Any]:
    readiness = billing_readiness()
    secret = _stripe_secret_key()
    webhook = _stripe_webhook_secret()
    return {
        "configured": readiness["overall"]["ready"],
        "billing_enabled": readiness["billing_enabled"],
        "overall": readiness["overall"],
        "checkout": readiness["checkout"],
        "webhooks": readiness["webhooks"],
        "readiness": readiness,
        "missing_env_vars": readiness["missing_env_vars"],
        "missing_price_env_vars": readiness["missing_price_env_vars"],
        "missing_price_ids": readiness["missing_price_ids"],
        "secret_key": "configured" if secret else "missing",
        "price_id": readiness["price_ids"]["premium_monthly"],
        "monthly_price_id": readiness["price_ids"]["premium_monthly"],
        "annual_price_id": readiness["price_ids"]["premium_annual"],
        "premium_monthly_price_id": readiness["price_ids"]["premium_monthly"],
        "premium_annual_price_id": readiness["price_ids"]["premium_annual"],
        "pro_monthly_price_id": readiness["price_ids"]["pro_monthly"],
        "pro_annual_price_id": readiness["price_ids"]["pro_annual"],
        "price_ids": readiness["price_ids"],
        "prices": readiness["prices"],
        "price_env_vars": {
            "premium_monthly": "STRIPE_PRICE_ID_PREMIUM_MONTHLY",
            "premium_annual": "STRIPE_PRICE_ID_PREMIUM_ANNUAL",
            "pro_monthly": "STRIPE_PRICE_ID_PRO_MONTHLY",
            "pro_annual": "STRIPE_PRICE_ID_PRO_ANNUAL",
        },
        "webhook_secret": "configured" if webhook else "missing",
        "webhook_events": readiness["webhooks"].get("recommended_events", []),
        "portal_return_url": _customer_portal_return_url(),
        "success_url": _checkout_success_url(),
        "cancel_url": _checkout_cancel_url(),
        "webhook_url": f"{_api_base_url()}/api/billing/stripe/webhook",
        "notes": "Secrets are read from environment variables. Price IDs use STRIPE_PRICE_ID_PREMIUM_MONTHLY, STRIPE_PRICE_ID_PREMIUM_ANNUAL, STRIPE_PRICE_ID_PRO_MONTHLY, and STRIPE_PRICE_ID_PRO_ANNUAL.",
    }


def _setting_bool(db: Session, key: str, default: bool = False) -> bool:
    value = _setting_value(db, key)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _stripe_tax_settings(db: Session) -> dict[str, Any]:
    behavior = _setting_value(db, "stripe_tax_price_tax_behavior") or "unspecified"
    if behavior not in {"unspecified", "exclusive", "inclusive"}:
        behavior = "unspecified"
    return {
        "automatic_tax_enabled": _setting_bool(db, "stripe_tax_automatic_tax_enabled", False),
        "require_billing_address": _setting_bool(db, "stripe_tax_require_billing_address", True),
        "product_tax_code": _setting_value(db, "stripe_tax_product_tax_code"),
        "price_tax_behavior": behavior,
    }


def _stripe_business_support_info() -> dict[str, Any]:
    values = {
        "business_name": os.getenv("PUBLIC_BUSINESS_NAME", "").strip() or os.getenv("STRIPE_BUSINESS_NAME", "").strip(),
        "support_email": os.getenv("SUPPORT_EMAIL", "").strip() or os.getenv("STRIPE_SUPPORT_EMAIL", "").strip(),
        "support_url": os.getenv("SUPPORT_URL", "").strip() or os.getenv("STRIPE_SUPPORT_URL", "").strip(),
        "support_phone": os.getenv("SUPPORT_PHONE", "").strip() or os.getenv("STRIPE_SUPPORT_PHONE", "").strip(),
    }
    present = {key: bool(value) for key, value in values.items()}
    return {
        "configured": bool(values["business_name"] and (values["support_email"] or values["support_url"] or values["support_phone"])),
        "fields": present,
    }


def _readiness_check(key: str, label: str, ok: bool, detail: str, *, required: bool = True) -> dict[str, Any]:
    status = "ready" if ok else ("missing" if required else "optional")
    return {"key": key, "label": label, "status": status, "detail": detail, "required": required}


def stripe_tax_billing_readiness(db: Session, customer_location: dict[str, Any] | None = None) -> dict[str, Any]:
    settings = _stripe_tax_settings(db)
    location = customer_location or {}
    missing_fields: list[str] = []
    if settings["automatic_tax_enabled"] and settings["require_billing_address"]:
        missing_fields = _billing_missing_fields_for_location(location)
    should_prompt = bool(settings["automatic_tax_enabled"] and missing_fields)
    return {
        "automatic_tax_enabled": settings["automatic_tax_enabled"],
        "requires_customer_location": bool(settings["automatic_tax_enabled"] and settings["require_billing_address"]),
        "has_required_customer_location": not missing_fields,
        "missing_fields": missing_fields,
        "should_prompt_for_location": should_prompt,
        "can_start_checkout": not should_prompt,
        "note": "Future billing flows can use this helper to prompt for location before enabling Stripe automatic tax.",
    }


def _stripe_tax_config(db: Session) -> dict[str, Any]:
    settings = _stripe_tax_settings(db)
    secret = _stripe_secret_key()
    price = _stripe_price_id() or _stripe_price_id("monthly")
    webhook = _stripe_webhook_secret()
    business_support = _stripe_business_support_info()
    checks = [
        _readiness_check(
            "stripe_secret_key",
            "Stripe secret key",
            bool(secret),
            "Present in environment." if secret else "Set STRIPE_SECRET_KEY in the deployment environment.",
        ),
        _readiness_check(
            "stripe_price_id",
            "Stripe price",
            bool(price),
            f"Using {price}." if price else "Set STRIPE_PRICE_ID_PREMIUM_MONTHLY for the Premium monthly subscription price.",
        ),
        _readiness_check(
            "automatic_tax",
            "Automatic tax app flag",
            bool(settings["automatic_tax_enabled"]),
            "Future billing flows will request Stripe automatic tax." if settings["automatic_tax_enabled"] else "Turn on when ready to request Stripe automatic tax in billing flows.",
        ),
        _readiness_check(
            "customer_location",
            "Customer location collection",
            bool(settings["require_billing_address"]),
            "Billing flows should collect address/location before checkout." if settings["require_billing_address"] else "Billing flows are not marked to require address/location collection.",
        ),
        _readiness_check(
            "business_support_info",
            "Business/support invoice info",
            bool(business_support["configured"]),
            "Public business/support fields are present." if business_support["configured"] else "Configure business/support info in Stripe or environment before invoices go live.",
            required=False,
        ),
        _readiness_check(
            "webhook_secret",
            "Stripe webhook secret",
            bool(webhook),
            "Present in environment." if webhook else "Set STRIPE_WEBHOOK_SECRET for subscription sync.",
        ),
    ]
    return {
        **settings,
        "configured": bool(secret and price and settings["automatic_tax_enabled"]),
        "stripe_tax_status": "ready" if secret and price and settings["automatic_tax_enabled"] else "not_ready",
        "stripe_dashboard_status": "managed_in_stripe",
        "price_id": price or "missing",
        "price_configured": bool(price),
        "secret_key": "configured" if secret else "missing",
        "webhook_secret": "configured" if webhook else "missing",
        "business_support": business_support,
        "readiness": stripe_tax_billing_readiness(db),
        "checks": checks,
        "notes": "Stripe Tax will calculate tax from customer location and your Stripe registrations/settings. App settings here only prepare integration readiness; they do not calculate manual tax rates.",
    }


def _normalize_subscription_interval(value: str | None) -> SubscriptionInterval | None:
    normalized = (value or "").strip().lower()
    if normalized in {"annual", "annually", "year", "yearly"}:
        return "annual"
    if normalized in {"monthly", "month"}:
        return "monthly"
    return None


def _premium_price_lookup(db: Session) -> tuple[dict[SubscriptionInterval, int], list[str]]:
    notes: list[str] = []
    seed_plan_prices(db)
    rows = db.execute(
        select(PlanPrice).where(PlanPrice.tier == "premium").order_by(PlanPrice.billing_interval.asc())
    ).scalars().all()
    prices: dict[SubscriptionInterval, int] = {}
    for row in rows:
        interval = _normalize_subscription_interval(row.billing_interval)
        if interval is None:
            continue
        prices[interval] = int(row.amount_cents or 0)
    if "monthly" not in prices or "annual" not in prices:
        prices.setdefault("monthly", 1995)
        prices.setdefault("annual", 19995)
        notes.append("Premium pricing settings were incomplete. Default premium pricing fallback was used.")
    return prices, notes


def _latest_billing_intervals_by_user(db: Session) -> dict[int, SubscriptionInterval]:
    rows = db.execute(
        select(
            BillingTransaction.user_id,
            BillingTransaction.billing_period_type,
            BillingTransaction.charged_at,
            BillingTransaction.created_at,
            BillingTransaction.id,
        )
        .where(BillingTransaction.user_id.is_not(None))
        .where(BillingTransaction.billing_period_type.is_not(None))
        .order_by(
            BillingTransaction.user_id.asc(),
            BillingTransaction.charged_at.desc().nullslast(),
            BillingTransaction.created_at.desc(),
            BillingTransaction.id.desc(),
        )
    ).all()
    intervals: dict[int, SubscriptionInterval] = {}
    for row in rows:
        user_id = int(row.user_id)
        if user_id in intervals:
            continue
        interval = _normalize_subscription_interval(row.billing_period_type)
        if interval is not None:
            intervals[user_id] = interval
    return intervals


def _has_recent_activity(user: UserAccount, cutoff: datetime, *, use_created_fallback: bool) -> bool:
    last_seen = _aware_utc(user.last_seen_at)
    if last_seen is not None:
        return last_seen >= cutoff
    created_at = _aware_utc(user.created_at)
    return bool(use_created_fallback and created_at is not None and created_at >= cutoff)


def _has_actual_paid_access(user: UserAccount, now: datetime) -> bool:
    return subscription_policy_tier(user, now=now) != "free"


def _has_paid_entitlement_marker(user: UserAccount) -> bool:
    paid_tiers = {"premium", "pro"}
    return normalize_tier(user.manual_tier_override) in paid_tiers or normalize_tier(user.entitlement_tier) in paid_tiers


def _has_checkout_blocking_subscription(user: UserAccount) -> bool:
    status = (user.subscription_status or "").strip().lower()
    if status in CHECKOUT_BLOCKING_SUBSCRIPTION_STATUSES:
        return _has_paid_entitlement_marker(user)
    return bool(
        user.stripe_subscription_id
        and status
        and _has_paid_entitlement_marker(user)
        and status not in {"canceled", "cancelled", "free", "unpaid", "incomplete_expired"}
    )


def _fallback_paid_tier(user: UserAccount) -> str | None:
    fallback_values = [user.manual_tier_override, user.entitlement_tier, user.subscription_plan]
    for value in fallback_values:
        tier = normalize_tier(value)
        if tier in {"premium", "pro"}:
            return tier
    return None


def _reports_summary(db: Session) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=30)
    current_date = now.date()
    ytd_start = datetime(current_date.year, 1, 1, tzinfo=timezone.utc)
    ytd_end = datetime.combine(current_date + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc)
    notes: list[str] = []

    activity_data_available = (
        db.execute(select(func.count()).select_from(UserAccount).where(UserAccount.last_seen_at.is_not(None))).scalar_one() > 0
    )
    if not activity_data_available:
        notes.append("Active user metric uses created_at fallback because last_seen is unavailable.")

    prices_cents, price_notes = _premium_price_lookup(db)
    notes.extend(price_notes)
    latest_intervals_by_user = _latest_billing_intervals_by_user(db)
    users = db.execute(select(UserAccount).order_by(UserAccount.id.asc())).scalars().all()

    active_free_users = 0
    active_premium_users = 0
    active_pro_users = 0
    monthly_recurring_revenue_cents = 0.0
    used_premium_fallback = False

    for user in users:
        actual_paid = _has_actual_paid_access(user, now)
        fallback_tier = None if actual_paid else _fallback_paid_tier(user)
        recent_activity = _has_recent_activity(user, cutoff, use_created_fallback=not activity_data_available)
        paid_tier = normalize_tier(user.subscription_plan) if actual_paid else fallback_tier
        premium_for_counts = bool(actual_paid or fallback_tier)

        if is_admin_user(user) and not actual_paid:
            premium_for_counts = False

        if premium_for_counts:
            if actual_paid or recent_activity:
                if paid_tier == "pro":
                    active_pro_users += 1
                else:
                    active_premium_users += 1

            if actual_paid or (fallback_tier and recent_activity and not is_admin_user(user)):
                interval = latest_intervals_by_user.get(user.id) or _normalize_subscription_interval(user.subscription_plan) or "monthly"
                if user.id not in latest_intervals_by_user:
                    used_premium_fallback = True
                if interval == "annual":
                    monthly_recurring_revenue_cents += prices_cents["annual"] / 12
                else:
                    monthly_recurring_revenue_cents += prices_cents["monthly"]
            continue

        if recent_activity and not is_admin_user(user):
            active_free_users += 1

    if used_premium_fallback:
        notes.append("Premium user and MRR metrics use entitlement/subscription-plan fallback where full subscription state is unavailable.")

    billing_transactions_exist = db.execute(select(func.count()).select_from(BillingTransaction)).scalar_one() > 0
    revenue_ytd_cents = db.execute(
        select(func.coalesce(func.sum(BillingTransaction.total_amount), 0))
        .select_from(BillingTransaction)
        .where(BillingTransaction.charged_at.is_not(None))
        .where(BillingTransaction.charged_at >= ytd_start)
        .where(BillingTransaction.charged_at < ytd_end)
        .where(func.lower(func.coalesce(BillingTransaction.payment_status, "")).in_(["paid", "succeeded"]))
    ).scalar_one()
    if not billing_transactions_exist:
        notes.append("Revenue collection data not connected yet.")

    new_users_last_30_days = db.execute(
        select(func.count()).select_from(UserAccount).where(UserAccount.created_at >= cutoff)
    ).scalar_one()
    total_users = db.execute(select(func.count()).select_from(UserAccount)).scalar_one()

    payload: dict[str, Any] = {
        "active_free_users": int(active_free_users),
        "active_premium_users": int(active_premium_users),
        "active_pro_users": int(active_pro_users),
        "monthly_recurring_revenue": round(float(monthly_recurring_revenue_cents) / 100, 2),
        "revenue_ytd": round(float(revenue_ytd_cents or 0) / 100, 2),
        "new_users_last_30_days": int(new_users_last_30_days),
        "total_users": int(total_users or 0),
        "currency": "USD",
        "generated_at": now.isoformat(),
    }
    if notes:
        payload["notes"] = notes
    return payload


def _auth_response_for_user(db: Session, user: UserAccount, response: Response | None = None) -> dict[str, Any]:
    token = sign_session_payload({"uid": user.id, "email": user.email})
    set_session_cookie(response, token)
    return {
        "authenticated": True,
        "user": serialize_user_self_profile(user),
        "entitlements": entitlement_payload(current_entitlements(_request_from_token(token), db), user=user),
    }


@router.post("/auth/login", dependencies=[Depends(rate_limit_auth_login)])
def login(payload: LoginPayload, response: Response = None, db: Session = Depends(get_db), request: Request = None):
    response, db = _coerce_response_and_db(response, db)
    email = normalize_email(payload.email)
    existing = _account_lookup_by_active_email(db, email)
    deleted = _deleted_account_lookup_by_original_email(db, email) if not existing else None
    if deleted and _deleted_reactivation_window_active(deleted):
        _log_auth_diagnostic("password_login", "rejected", "deleted_account", request, email=email)
        raise HTTPException(status_code=403, detail="This account was recently deleted. Check your email for the reactivation link or contact support.")
    existing_is_admin = is_admin_user(existing)
    admin_token_valid = _admin_token_matches(payload.admin_token)
    if not existing:
        _log_auth_diagnostic("password_login", "rejected", "invalid_credentials", request, email=email)
        raise HTTPException(status_code=401, detail="Incorrect email or password.")

    password_ok = bool(existing.password_hash and verify_password(payload.password, existing.password_hash))
    admin_token_ok = bool(existing_is_admin and admin_token_valid)
    if existing.password_hash and not (password_ok or admin_token_ok):
        _log_auth_diagnostic("password_login", "rejected", "invalid_credentials", request, user=existing, email=email)
        raise HTTPException(status_code=401, detail="Incorrect email or password.")
    if not existing.password_hash and not admin_token_ok:
        _log_auth_diagnostic("password_login", "rejected", "invalid_credentials", request, user=existing, email=email)
        raise HTTPException(status_code=401, detail="Incorrect email or password.")

    user = get_or_create_user(db, email=email, name=payload.name)
    if payload.name and not (user.first_name or user.last_name):
        user.first_name, user.last_name = _split_name(payload.name)
    user.last_seen_at = datetime.now(timezone.utc)
    attach_legacy_watchlists_to_user(db, user)
    db.commit()
    db.refresh(user)

    _log_auth_diagnostic("password_login", "authenticated", "session_created", request, user=user, set_cookie_attempted=True)
    return _auth_response_for_user(db, user, response)


@router.post("/auth/register", dependencies=[Depends(rate_limit_register)])
def register(payload: RegisterPayload, response: Response = None, db: Session = Depends(get_db)):
    response, db = _coerce_response_and_db(response, db)
    email = normalize_email(payload.email)
    existing = _account_lookup_by_active_email(db, email)
    if existing and existing.password_hash:
        raise HTTPException(status_code=409, detail="An account already exists for this email.")
    deleted = _deleted_account_lookup_by_original_email(db, email) if not existing else None
    if deleted and _deleted_reactivation_window_active(deleted):
        raise HTTPException(
            status_code=403,
            detail="This account was recently deleted. Check your email for the reactivation link or contact support.",
        )
    _require_password_meets_account_rules(payload.password)

    cleaned_registration = {
        "first_name": _clean_profile_value(payload.first_name),
        "last_name": _clean_profile_value(payload.last_name),
        "country": _clean_country(payload.country),
        "state_province": _clean_profile_value(payload.state_province),
        "postal_code": _clean_profile_value(payload.postal_code),
        "city": _clean_profile_value(payload.city),
        "address_line1": _clean_profile_value(payload.address_line1),
        "address_line2": _clean_profile_value(payload.address_line2),
    }
    missing = [label for field, label in BILLING_REQUIRED_FIELDS if not cleaned_registration.get(field)]
    if cleaned_registration.get("country") in COUNTRIES_REQUIRING_BILLING_REGION and not cleaned_registration.get("state_province"):
        missing.append("State/province")
    if missing:
        raise HTTPException(status_code=422, detail=f"{', '.join(missing)} required.")

    is_new_user = existing is None
    user = existing or get_or_create_user(db, email=email, name=payload.name or _display_name(payload.first_name, payload.last_name))
    _set_billing_profile(user, **cleaned_registration)
    user.password_hash = hash_password(payload.password)
    user.auth_provider = user.auth_provider or "email"
    if existing is None:
        user.role = "user"
    user.password_reset_token_hash = None
    user.password_reset_expires_at = None
    verification_token = _issue_email_verification(db, user)
    user.last_seen_at = datetime.now(timezone.utc)
    attach_legacy_watchlists_to_user(db, user)
    db.commit()
    db.refresh(user)
    verification_url = _verification_url(verification_token)
    _send_verification_email(db, user, verification_url)
    if is_new_user:
        _send_welcome_email(db, user)
    auth_response = _auth_response_for_user(db, user, response)
    auth_response["email_verification_required"] = user.email_verified_at is None
    if _allow_insecure_verification_link_response():
        auth_response["dev_verification_url"] = verification_url
    return auth_response


@router.post("/auth/password-reset/request", dependencies=[Depends(rate_limit_password_reset_request)])
def request_password_reset(payload: PasswordResetRequestPayload, db: Session = Depends(get_db)):
    email = normalize_email(payload.email)
    user = _account_lookup_by_active_email(db, email)
    response: dict[str, Any] = {
        "status": "ok",
        "message": "If an account exists for that email, reset instructions have been sent.",
    }
    if not user:
        return response

    token, _delivery = _issue_password_reset_for_user(db, user)
    reset_path = f"/reset-password?token={token}"
    if _allow_insecure_reset_link_response():
        response["reset_path"] = reset_path
    return response


@router.post("/auth/password-reset/confirm", dependencies=[Depends(rate_limit_password_reset_confirm)])
def confirm_password_reset(payload: PasswordResetConfirmPayload, response: Response = None, db: Session = Depends(get_db)):
    response, db = _coerce_response_and_db(response, db)
    token_hash = reset_token_hash(payload.token)
    user = db.execute(
        select(UserAccount).where(UserAccount.password_reset_token_hash == token_hash)
    ).scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=400, detail="Invalid or expired reset link.")
    if _is_deleted_user(user):
        raise HTTPException(status_code=400, detail="Invalid or expired reset link.")
    expires_at = user.password_reset_expires_at
    if expires_at and expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)
    if not expires_at or expires_at < datetime.now(timezone.utc):
        raise HTTPException(status_code=400, detail="Invalid or expired reset link.")
    new_password = payload.new_password or payload.password
    if not new_password:
        raise HTTPException(status_code=422, detail="Password is required.")
    if new_password != payload.confirm_password:
        raise HTTPException(status_code=422, detail="Passwords do not match.")
    _require_password_meets_account_rules(new_password)

    changed_at = datetime.now(timezone.utc)
    user.password_hash = hash_password(new_password)
    user.password_reset_token_hash = None
    user.password_reset_expires_at = None
    user.email_verified_at = user.email_verified_at or changed_at
    user.email_verification_token_hash = None
    user.email_verification_expires_at = None
    user.last_seen_at = changed_at
    db.commit()
    db.refresh(user)
    _send_password_changed_confirmation(db, user, changed_at)
    clear_session_cookie(response)
    return {
        "ok": True,
        "authenticated": False,
        "redirect_to": "/login?reset=success",
    }


@router.post("/account/resend-verification")
def resend_email_verification(
    request: Request,
    payload: ResendVerificationPayload | None = None,
    db: Session = Depends(get_db),
):
    requester = current_user(db, request)
    target_email = normalize_email(payload.email if payload and payload.email else requester.email if requester else None)
    response: dict[str, Any] = {
        "status": "ok",
        "message": "If verification is required for that email, verification instructions have been sent.",
    }
    if not target_email:
        raise HTTPException(status_code=401, detail="Sign in required.")
    if requester and normalize_email(requester.email) != target_email and not is_admin_user(requester):
        raise HTTPException(status_code=403, detail="Cannot resend verification for another account.")

    user = _account_lookup_by_active_email(db, target_email)
    if not user or user.email_verified_at is not None:
        return response

    token = _issue_email_verification(db, user)
    db.commit()
    verification_url = _verification_url(token)
    _send_verification_email(db, user, verification_url)
    if requester and user.id == requester.id:
        response["email_verification_required"] = True
    if _allow_insecure_verification_link_response():
        response["dev_verification_url"] = verification_url
    return response


@router.get("/account/verify-email")
@router.post("/account/verify-email")
def verify_email(token: str = Query(default="", max_length=240), db: Session = Depends(get_db)):
    token_hash = reset_token_hash(token)
    user = db.execute(
        select(UserAccount).where(UserAccount.email_verification_token_hash == token_hash)
    ).scalar_one_or_none()
    if not user:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "invalid_verification_link",
                "message": "This verification link is invalid. Please request a new one.",
            },
        )
    if _is_deleted_user(user):
        raise HTTPException(
            status_code=400,
            detail={
                "code": "invalid_verification_link",
                "message": "This verification link is invalid. Please request a new one.",
            },
        )
    if user.email_verified_at is not None:
        return {"status": "already_verified", "email": user.email, "email_verified_at": user.email_verified_at}
    expires_at = _aware_utc(user.email_verification_expires_at)
    if not expires_at or expires_at < datetime.now(timezone.utc):
        raise HTTPException(
            status_code=400,
            detail={
                "code": "expired_verification_link",
                "message": "This verification link has expired. Please request a new one.",
            },
        )

    now = datetime.now(timezone.utc)
    user.email_verified_at = now
    user.updated_at = now
    db.commit()
    return {"status": "verified", "email": user.email, "email_verified_at": user.email_verified_at}


def _request_from_token(token: str) -> Request:
    return Request({"type": "http", "method": "GET", "path": "/", "headers": [(b"cookie", f"{SESSION_COOKIE_NAME}={token}".encode("utf-8"))]})


@router.get("/auth/google/start")
def google_auth_start(return_to: str | None = None, db: Session = Depends(get_db), request: Request = None):
    client_id = _google_client_id(db, prefer_env=True)
    if not client_id:
        _log_auth_diagnostic("google_start", "rejected", "oauth_not_configured", request)
        raise HTTPException(status_code=503, detail="Google OAuth is not configured.")
    safe_return_to = _safe_app_return_path(return_to)
    state = sign_session_payload(
        {
            "kind": "google_oauth_state",
            "return_to": safe_return_to,
            "exp": int(time.time()) + 600,
        }
    )
    params = {
        "client_id": client_id,
        "redirect_uri": _google_redirect_uri(),
        "response_type": "code",
        "scope": "openid email profile",
        "state": state,
        "access_type": "offline",
        "prompt": "select_account",
    }
    _log_auth_diagnostic("google_start", "started", "authorization_url_created", request)
    return {"authorization_url": f"https://accounts.google.com/o/oauth2/v2/auth?{urlencode(params)}", "state": state}


def _decode_jwt_payload(token: str) -> dict[str, Any]:
    parts = token.split(".")
    if len(parts) < 2:
        raise HTTPException(status_code=401, detail="Invalid Google identity token.")
    padded = parts[1] + "=" * (-len(parts[1]) % 4)
    try:
        import base64

        parsed = json.loads(base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8"))
    except Exception as exc:
        raise HTTPException(status_code=401, detail="Invalid Google identity token.") from exc
    if not isinstance(parsed, dict):
        raise HTTPException(status_code=401, detail="Invalid Google identity token.")
    return parsed


def _verify_google_claims(db: Session, claims: dict[str, Any], *, expected_client_id: str | None = None) -> dict[str, Any]:
    client_id = expected_client_id or _google_client_id(db)
    if not client_id:
        raise HTTPException(status_code=503, detail="Google OAuth is not configured.")
    if claims.get("aud") != client_id:
        raise HTTPException(status_code=401, detail="Google token audience mismatch.")
    if claims.get("iss") not in {"accounts.google.com", "https://accounts.google.com"}:
        raise HTTPException(status_code=401, detail="Google token issuer mismatch.")
    try:
        exp = int(claims.get("exp") or 0)
    except (TypeError, ValueError):
        exp = 0
    if exp < int(time.time()):
        raise HTTPException(status_code=401, detail="Google token expired.")
    if claims.get("email_verified") not in {True, "true", "True", "1", 1}:
        raise HTTPException(status_code=401, detail="Google email is not verified.")
    email = normalize_email(str(claims.get("email") or ""))
    sub = str(claims.get("sub") or "").strip()
    if not email or not sub:
        raise HTTPException(status_code=401, detail="Google token missing account identity.")
    return claims


def _google_user_exists_for_claims(db: Session, claims: dict[str, Any]) -> bool:
    email = normalize_email(str(claims.get("email") or ""))
    sub = str(claims.get("sub") or "").strip()
    if sub:
        existing_id = db.execute(
            select(UserAccount.id).where(UserAccount.google_sub == sub, UserAccount.deleted_at.is_(None))
        ).scalar_one_or_none()
        if existing_id is not None:
            return True
    if email:
        existing_id = db.execute(
            select(UserAccount.id).where(func.lower(UserAccount.email) == email, UserAccount.deleted_at.is_(None))
        ).scalar_one_or_none()
        if existing_id is not None:
            return True
    return False


def upsert_google_user(db: Session, claims: dict[str, Any], *, expected_client_id: str | None = None) -> UserAccount:
    claims = _verify_google_claims(db, claims, expected_client_id=expected_client_id)
    email = normalize_email(str(claims.get("email")))
    sub = str(claims.get("sub"))
    name = str(claims.get("name") or "").strip() or None
    picture = str(claims.get("picture") or "").strip() or None

    user = db.execute(
        select(UserAccount).where(UserAccount.google_sub == sub, UserAccount.deleted_at.is_(None))
    ).scalar_one_or_none()
    if not user:
        user = _account_lookup_by_active_email(db, email)
    if not user:
        user = get_or_create_user(db, email=email, name=name)

    user.auth_provider = "google"
    user.google_sub = sub
    if name:
        user.name = name
        if not (user.first_name or user.last_name):
            user.first_name, user.last_name = _split_name(name)
    if picture:
        user.avatar_url = picture
    user.email_verified_at = user.email_verified_at or datetime.now(timezone.utc)
    user.email_verification_token_hash = None
    user.email_verification_expires_at = None
    user.last_seen_at = datetime.now(timezone.utc)
    db.flush()
    return user


@router.post("/auth/google/callback")
def google_auth_callback(payload: GoogleCallbackPayload, response: Response = None, db: Session = Depends(get_db), request: Request = None):
    auth_response, db = _coerce_response_and_db(response, db)
    parsed_state = verify_session_token(payload.state)
    if (
        not parsed_state
        or parsed_state.get("kind") != "google_oauth_state"
        or int(parsed_state.get("exp") or 0) < int(time.time())
    ):
        _log_auth_diagnostic("google_callback", "rejected", "invalid_state", request)
        raise HTTPException(status_code=401, detail="Invalid Google sign-in state.")
    client_id = _google_client_id(db, prefer_env=True)
    client_secret = _google_client_secret()
    if not client_id or not client_secret:
        _log_auth_diagnostic("google_callback", "rejected", "oauth_not_configured", request)
        raise HTTPException(status_code=503, detail="Google OAuth is not configured.")

    try:
        google_token_response = requests.post(
            "https://oauth2.googleapis.com/token",
            data={
                "code": payload.code,
                "client_id": client_id,
                "client_secret": client_secret,
                "redirect_uri": payload.redirect_uri or _google_redirect_uri(),
                "grant_type": "authorization_code",
            },
            timeout=20,
        )
    except requests.RequestException as exc:
        _log_auth_diagnostic("google_callback", "rejected", "token_exchange_request_failed", request)
        raise HTTPException(status_code=502, detail="Google token exchange failed.") from exc
    if google_token_response.status_code >= 400:
        _log_auth_diagnostic("google_callback", "rejected", "token_exchange_rejected", request)
        raise HTTPException(status_code=401, detail="Google token exchange failed.")
    try:
        token_payload = google_token_response.json()
    except ValueError as exc:
        _log_auth_diagnostic("google_callback", "rejected", "token_exchange_invalid_response", request)
        raise HTTPException(status_code=502, detail="Google token exchange returned an invalid response.") from exc
    id_token = token_payload.get("id_token") if isinstance(token_payload, dict) else None
    if not isinstance(id_token, str):
        _log_auth_diagnostic("google_callback", "rejected", "missing_id_token", request)
        raise HTTPException(status_code=401, detail="Google did not return an identity token.")
    claims = _verify_google_claims(db, _decode_jwt_payload(id_token), expected_client_id=client_id)
    is_new_user = not _google_user_exists_for_claims(db, claims)
    user = upsert_google_user(db, claims, expected_client_id=client_id)
    db.commit()
    db.refresh(user)
    if is_new_user:
        _send_welcome_email(db, user)
    auth = _auth_response_for_user(db, user, auth_response)
    auth["return_to"] = _safe_app_return_path(str(parsed_state.get("return_to") or ""))
    _log_auth_diagnostic("google_callback", "authenticated", "session_created", request, user=user, set_cookie_attempted=True)
    return auth


@router.get("/auth/me")
def me(request: Request, db: Session = Depends(get_db)):
    user = current_user(db, request, required=False)
    entitlements = entitlements_for_user(db, user) if user else current_entitlements(request, None)
    if user:
        _log_auth_diagnostic("auth_me", "authenticated", "session_valid", request, user=user)
    else:
        reason = "missing_cookie" if SESSION_COOKIE_NAME not in request.cookies else "invalid_session"
        _log_auth_diagnostic("auth_me", "unauthenticated", reason, request)
    return {
        "user": serialize_user_self_profile(user) if user else None,
        "entitlements": entitlement_payload(entitlements, user=user),
    }


@router.get("/account/settings")
def account_settings(request: Request, db: Session = Depends(get_db)):
    user = current_user(db, request, required=True)
    return {
        "user": serialize_user_account(user),
        "notifications": _notification_settings(user),
    }


@router.get("/account/billing/history")
def account_billing_history(
    request: Request,
    db: Session = Depends(get_db),
    limit: int = Query(25, ge=1, le=100),
):
    user = current_user(db, request, required=True)
    conditions = [BillingTransaction.user_id == user.id]
    if user.stripe_customer_id:
        conditions.append(BillingTransaction.stripe_customer_id == user.stripe_customer_id)
    if user.stripe_subscription_id:
        conditions.append(BillingTransaction.stripe_subscription_id == user.stripe_subscription_id)
    email = normalize_email(user.email)
    if email:
        conditions.append(func.lower(BillingTransaction.customer_email) == email)

    rows = db.execute(
        select(BillingTransaction)
        .where(or_(*conditions))
        .order_by(BillingTransaction.charged_at.desc(), BillingTransaction.id.desc())
        .limit(limit)
    ).scalars().all()
    return {"items": [_customer_billing_history_row(row) for row in rows]}


@router.patch("/account/profile")
def update_account_profile(payload: ProfileUpdatePayload, request: Request, db: Session = Depends(get_db)):
    user = current_user(db, request, required=True)
    provided_fields = _payload_fields_set(payload)
    next_values = _billing_location_payload(user)
    for field in ("first_name", "last_name", *BILLING_LOCATION_FIELDS):
        if field in provided_fields:
            value = getattr(payload, field)
            next_values[field] = _clean_country(value) if field == "country" else _clean_profile_value(value)

    if provided_fields.intersection(BILLING_LOCATION_FIELDS):
        missing = [label for field, label in BILLING_REQUIRED_FIELDS if not next_values.get(field)]
        if next_values.get("country") in COUNTRIES_REQUIRING_BILLING_REGION and not next_values.get("state_province"):
            missing.append("State/province")
        if missing:
            raise HTTPException(status_code=422, detail=f"{', '.join(missing)} required.")

    _set_billing_profile(user, **next_values)
    user.updated_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(user)
    return serialize_user_account(user)


@router.patch("/account/password")
def update_account_password(payload: PasswordChangePayload, request: Request, db: Session = Depends(get_db)):
    user = current_user(db, request, required=True)
    if not verify_password(payload.current_password, user.password_hash):
        raise HTTPException(status_code=401, detail="Current password is incorrect.")
    if payload.new_password != payload.confirm_password:
        raise HTTPException(status_code=422, detail="Confirm password must match the new password.")
    _require_password_meets_account_rules(payload.new_password, label="New password")
    changed_at = datetime.now(timezone.utc)
    user.password_hash = hash_password(payload.new_password)
    user.password_reset_token_hash = None
    user.password_reset_expires_at = None
    user.updated_at = changed_at
    db.commit()
    db.refresh(user)
    _send_password_changed_confirmation(db, user, changed_at)
    return {"status": "ok"}


@router.patch("/account/notifications", dependencies=[Depends(rate_limit_notification_mutation)])
def update_account_notifications(
    payload: NotificationSettingsPayload,
    request: Request,
    db: Session = Depends(get_db),
):
    user = current_user(db, request, required=True)
    user.alerts_enabled = payload.alerts_enabled
    user.email_notifications_enabled = payload.email_notifications_enabled
    user.watchlist_activity_notifications = payload.watchlist_activity_notifications
    user.signals_notifications = payload.signals_notifications
    user.updated_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(user)
    return _notification_settings(user)


@router.post("/account/delete")
def delete_account(payload: DeleteAccountPayload, request: Request, response: Response = None, db: Session = Depends(get_db)):
    response, db = _coerce_response_and_db(response, db)
    if payload.confirmation != "DELETE":
        raise HTTPException(status_code=422, detail="Type DELETE to confirm account deletion.")
    user = current_user(db, request, required=True)
    if is_admin_user(user):
        raise HTTPException(status_code=400, detail="Admin accounts must be removed from the admin panel.")

    now = datetime.now(timezone.utc)
    original_email = normalize_email(user.original_email or user.email)
    first_name = (user.first_name or user.name or "there").strip().split(" ", 1)[0] or "there"
    _schedule_subscription_cancellation_for_deleted_account(db, user)
    deadline, current_period_end, is_paid = _reactivation_deadline(user, now=now)
    token = secrets.token_urlsafe(32)
    token_hash = reset_token_hash(token)

    user.original_email = original_email
    user.email = _deleted_email_namespace(user, original_email)
    user.deleted_at = now
    user.deleted_by_user = True
    user.deletion_reason = "user_requested"
    user.deletion_plan = _effective_user_plan(user)
    user.is_suspended = True
    user.reactivation_token_hash = token_hash
    user.reactivation_expires_at = deadline
    user.password_reset_token_hash = None
    user.password_reset_expires_at = None
    user.email_verification_token_hash = None
    user.email_verification_expires_at = None
    user.updated_at = now
    db.commit()
    db.refresh(user)

    _send_account_deleted_reactivation_email(
        db,
        user,
        to_email=original_email,
        first_name=first_name,
        token=token,
        deadline=deadline,
        current_period_end=current_period_end,
        is_paid=is_paid,
    )
    clear_session_cookie(response)
    return {
        "status": "deleted",
        "deleted_at": user.deleted_at,
        "reactivation_expires_at": user.reactivation_expires_at,
        "current_period_end": current_period_end,
        "is_paid": is_paid,
        "clear_cookie": SESSION_COOKIE_NAME,
    }


@router.post("/account/reactivate")
def reactivate_deleted_account(payload: ReactivateAccountPayload, db: Session = Depends(get_db)):
    token_hash = reset_token_hash(payload.token)
    user = db.execute(
        select(UserAccount).where(UserAccount.reactivation_token_hash == token_hash)
    ).scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=400, detail="Invalid or expired reactivation link.")
    expires_at = _aware_utc(user.reactivation_expires_at)
    now = datetime.now(timezone.utc)
    if not expires_at or expires_at < now:
        raise HTTPException(status_code=400, detail="Invalid or expired reactivation link.")
    if user.deleted_at is None:
        user.reactivation_token_hash = None
        user.reactivation_expires_at = None
        db.commit()
        return {
            "status": "already_active",
            "email": user.email,
            "subscription_plan": user.subscription_plan,
            "subscription_cancel_at_period_end": bool(user.subscription_cancel_at_period_end),
            "current_period_end": user.access_expires_at,
        }

    original_email = normalize_email(user.original_email or user.email)
    if not original_email or "@" not in original_email:
        raise HTTPException(status_code=400, detail="This account cannot be reactivated automatically. Contact support.")
    active_conflict = _account_lookup_by_active_email(db, original_email)
    if active_conflict and active_conflict.id != user.id:
        raise HTTPException(status_code=409, detail="An active account already exists for this email. Contact support to restore the deleted account.")

    user.email = original_email
    user.deleted_at = None
    user.deleted_by_user = False
    user.deletion_reason = None
    user.deletion_plan = None
    user.is_suspended = False
    user.reactivation_token_hash = None
    user.reactivation_expires_at = None
    reconciled = _reconcile_user_subscription_from_stripe(db, user)
    if not reconciled.get("synced"):
        _restore_entitlement_after_reactivation(user, now=now)
    user.updated_at = now
    db.commit()
    db.refresh(user)
    return {
        "status": "reactivated",
        "email": user.email,
        "subscription_plan": user.subscription_plan,
        "subscription_cancel_at_period_end": bool(user.subscription_cancel_at_period_end),
        "current_period_end": user.access_expires_at,
        "entitlement_tier": user.entitlement_tier,
    }


@router.post("/auth/logout")
def logout(response: Response = None):
    clear_session_cookie(response)
    return {"status": "ok", "clear_cookie": SESSION_COOKIE_NAME}


@router.post("/billing/checkout-session")
def create_checkout_session(
    request: Request,
    payload: CheckoutSessionPayload | None = None,
    db: Session = Depends(get_db),
):
    user = current_user(db, request, required=True)
    if user.email_verified_at is None:
        raise HTTPException(
            status_code=403,
            detail={
                "code": "email_verification_required",
                "message": "Please verify your email before upgrading with Stripe.",
            },
        )
    billing_interval = (payload.interval or payload.billing_interval) if payload else "monthly"
    tier = (payload.plan or payload.tier) if payload else "premium"
    billing_interval = billing_interval or "monthly"
    tier = tier or "premium"
    if _has_checkout_blocking_subscription(user):
        raise HTTPException(
            status_code=409,
            detail={
                "code": "active_subscription_exists",
                "message": "You already have an active subscription. Use Manage billing to change plans.",
                "action": "manage_billing",
                "redirect_path": "/account/billing",
            },
        )
    readiness = billing_readiness(checkout_tier=tier, checkout_interval=billing_interval)
    _log_billing_readiness(context="checkout", readiness=readiness)
    _require_checkout_readiness(readiness)
    price_id = readiness["checkout"]["selected_price_id"]

    customer_id = _sync_stripe_customer_for_billing(db, user)
    tax_settings = _stripe_tax_settings(db)
    user_id = str(user.id)
    data: dict[str, Any] = {
        "mode": "subscription",
        "line_items[0][price]": price_id,
        "line_items[0][quantity]": 1,
        "success_url": _checkout_success_url(),
        "cancel_url": _checkout_cancel_url(),
        "customer": customer_id,
        "client_reference_id": user_id,
        "metadata[user_id]": user_id,
        "metadata[email]": user.email,
        "metadata[plan]": tier,
        "metadata[interval]": billing_interval,
        "metadata[billing_interval]": billing_interval,
        "metadata[tier]": tier,
        "metadata[price_id]": price_id,
        "subscription_data[metadata][user_id]": user_id,
        "subscription_data[metadata][email]": user.email,
        "subscription_data[metadata][plan]": tier,
        "subscription_data[metadata][interval]": billing_interval,
        "subscription_data[metadata][billing_interval]": billing_interval,
        "subscription_data[metadata][tier]": tier,
        "subscription_data[metadata][price_id]": price_id,
    }
    if tax_settings["automatic_tax_enabled"]:
        data["automatic_tax[enabled]"] = "true"
        data["billing_address_collection"] = "required"
        data["customer_update[address]"] = "auto"
        data["customer_update[name]"] = "auto"

    session = _stripe_post("checkout/sessions", data)
    return {"id": session.get("id"), "url": session.get("url")}


@router.post("/billing/customer-portal")
def create_customer_portal_session(request: Request, db: Session = Depends(get_db)):
    user = current_user(db, request, required=True)
    if user.email_verified_at is None:
        raise HTTPException(
            status_code=403,
            detail={
                "code": "email_verification_required",
                "message": "Please verify your email before managing billing.",
            },
        )
    if not user.stripe_customer_id:
        raise HTTPException(status_code=404, detail="No Stripe customer is linked to this account.")
    readiness = billing_readiness()
    _log_billing_readiness(context="customer_portal", readiness=readiness)
    _require_stripe_api_readiness(
        readiness,
        code="stripe_customer_portal_not_ready",
        message="Stripe customer portal is not configured.",
    )
    session = _stripe_post(
        "billing_portal/sessions",
        {"customer": user.stripe_customer_id, "return_url": _customer_portal_return_url()},
    )
    return {"url": session.get("url")}


@router.post("/billing/subscription/cancel")
def cancel_subscription_at_period_end(request: Request, db: Session = Depends(get_db)):
    user = current_user(db, request, required=True)
    if not user.stripe_subscription_id:
        raise HTTPException(status_code=404, detail="No Stripe subscription is linked to this account.")
    subscription = _stripe_post(
        f"subscriptions/{user.stripe_subscription_id}",
        {"cancel_at_period_end": "true"},
    )
    status = str(subscription.get("status") or user.subscription_status or "active")
    _sync_user_subscription(db, obj=subscription, status=status)
    db.commit()
    db.refresh(user)
    return serialize_user_billing(user)


@router.post("/billing/subscription/reactivate")
def reactivate_subscription_before_expiry(request: Request, db: Session = Depends(get_db)):
    user = current_user(db, request, required=True)
    if not user.stripe_subscription_id:
        raise HTTPException(status_code=404, detail="No Stripe subscription is linked to this account.")
    access_expires_at = _aware_utc(user.access_expires_at)
    if access_expires_at is not None and access_expires_at <= datetime.now(timezone.utc):
        raise HTTPException(status_code=400, detail="Subscription access has already expired.")
    subscription = _stripe_post(
        f"subscriptions/{user.stripe_subscription_id}",
        {"cancel_at_period_end": "false"},
    )
    status = str(subscription.get("status") or user.subscription_status or "active")
    _sync_user_subscription(db, obj=subscription, status=status)
    db.commit()
    db.refresh(user)
    return serialize_user_billing(user)


def _verify_stripe_signature(payload: bytes, signature_header: str | None) -> None:
    secret = _stripe_webhook_secret()
    if not secret:
        raise HTTPException(status_code=503, detail="Stripe webhook secret is not configured.")
    if not signature_header:
        raise HTTPException(status_code=400, detail="Missing Stripe signature.")

    parts = dict(part.split("=", 1) for part in signature_header.split(",") if "=" in part)
    timestamp = parts.get("t")
    signature = parts.get("v1")
    if not timestamp or not signature:
        raise HTTPException(status_code=400, detail="Invalid Stripe signature.")
    try:
        signed_at = int(timestamp)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid Stripe signature timestamp.") from exc
    tolerance_seconds = int(os.getenv("STRIPE_WEBHOOK_TOLERANCE_SECONDS", "300") or 300)
    if tolerance_seconds > 0 and abs(int(time.time()) - signed_at) > tolerance_seconds:
        raise HTTPException(status_code=400, detail="Stripe signature timestamp is outside tolerance.")

    signed_payload = f"{timestamp}.{payload.decode('utf-8')}".encode("utf-8")
    expected = hmac.new(secret.encode("utf-8"), signed_payload, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, signature):
        raise HTTPException(status_code=400, detail="Invalid Stripe signature.")


def _stripe_object_id(value: Any) -> str | None:
    if isinstance(value, dict):
        value = value.get("id")
    cleaned = str(value or "").strip()
    return cleaned or None


def _datetime_from_epoch(value: Any) -> datetime | None:
    try:
        timestamp = int(value)
    except (TypeError, ValueError):
        return None
    if timestamp <= 0:
        return None
    return datetime.fromtimestamp(timestamp, tz=timezone.utc)


def _aware_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _extract_metadata(obj: dict[str, Any]) -> dict[str, Any]:
    metadata: dict[str, Any] = {}
    candidates = [
        obj.get("metadata"),
        (obj.get("subscription_details") or {}).get("metadata") if isinstance(obj.get("subscription_details"), dict) else None,
    ]
    lines = obj.get("lines") if isinstance(obj.get("lines"), dict) else {}
    for line in lines.get("data") or []:
        if isinstance(line, dict):
            candidates.append(line.get("metadata"))
            price = line.get("price") if isinstance(line.get("price"), dict) else {}
            candidates.append(price.get("metadata"))
    for candidate in candidates:
        if isinstance(candidate, dict):
            metadata.update({str(key): value for key, value in candidate.items()})
    return metadata


def _extract_customer_email(obj: dict[str, Any], metadata: dict[str, Any] | None = None) -> str:
    metadata = metadata or _extract_metadata(obj)
    customer_details = obj.get("customer_details") if isinstance(obj.get("customer_details"), dict) else {}
    customer = obj.get("customer") if isinstance(obj.get("customer"), dict) else {}
    return normalize_email(
        metadata.get("email")
        or obj.get("customer_email")
        or obj.get("email")
        or customer.get("email")
        or customer_details.get("email")
    )


def _extract_subscription_id(obj: dict[str, Any]) -> str | None:
    subscription = _stripe_object_id(obj.get("subscription"))
    if subscription:
        return subscription
    parent = obj.get("parent") if isinstance(obj.get("parent"), dict) else {}
    details = parent.get("subscription_details") if isinstance(parent.get("subscription_details"), dict) else {}
    return _stripe_object_id(details.get("subscription"))


def _extract_subscription_price_id(obj: dict[str, Any]) -> str | None:
    if obj.get("object") == "subscription":
        selected = _select_subscription_item_price(obj)
        if selected.get("price_id"):
            return str(selected["price_id"])
    items = obj.get("items") if isinstance(obj.get("items"), dict) else {}
    for item in items.get("data") or []:
        if isinstance(item, dict):
            price = item.get("price") if isinstance(item.get("price"), dict) else {}
            price_id = _stripe_object_id(price)
            if price_id:
                return price_id
    metadata = _extract_metadata(obj)
    metadata_price = _stripe_object_id(metadata.get("price_id"))
    if metadata_price:
        return metadata_price
    for line in _invoice_line_items(obj):
        price = line.get("price") if isinstance(line.get("price"), dict) else {}
        price_id = _stripe_object_id(price)
        if price_id:
            return price_id
    return None


def _extract_subscription_interval(obj: dict[str, Any]) -> SubscriptionInterval | None:
    if obj.get("object") == "subscription":
        selected = _select_subscription_item_price(obj)
        interval = selected.get("billing_interval")
        if interval in {"monthly", "annual"}:
            return interval
    metadata = _extract_metadata(obj)
    metadata_interval = _normalize_subscription_interval(str(metadata.get("billing_interval") or metadata.get("interval") or ""))
    if metadata_interval:
        return metadata_interval
    items = obj.get("items") if isinstance(obj.get("items"), dict) else {}
    for item in items.get("data") or []:
        if isinstance(item, dict):
            price = item.get("price") if isinstance(item.get("price"), dict) else {}
            recurring = price.get("recurring") if isinstance(price.get("recurring"), dict) else {}
            interval = _normalize_subscription_interval(str(recurring.get("interval") or ""))
            if interval:
                return interval
    return _invoice_billing_period_type(obj)


_STRIPE_PLAN_RANK: dict[str, int] = {"free": 0, "premium": 10, "pro": 20}


def _subscription_item_is_active(item: dict[str, Any]) -> bool:
    if item.get("deleted") is True:
        return False
    for key in ("canceled_at", "ended_at", "deleted_at"):
        if item.get(key):
            return False
    status = str(item.get("status") or "").strip().lower()
    return not status or status in {"active", "trialing", "past_due"}


def _subscription_item_interval(item: dict[str, Any]) -> SubscriptionInterval | None:
    price = item.get("price") if isinstance(item.get("price"), dict) else {}
    recurring = price.get("recurring") if isinstance(price.get("recurring"), dict) else {}
    return _normalize_subscription_interval(str(recurring.get("interval") or ""))


def _subscription_item_amount_currency(subscription: dict[str, Any]) -> tuple[int | None, str | None]:
    selected = _select_subscription_item_price(subscription)
    selected_item_id = selected.get("selected_item_id")
    selected_index = selected.get("selected_item_index")
    items = subscription.get("items") if isinstance(subscription.get("items"), dict) else {}
    raw_items = [item for item in items.get("data") or [] if isinstance(item, dict)]
    chosen: dict[str, Any] | None = None
    for index, item in enumerate(raw_items):
        if selected_item_id and _stripe_object_id(item.get("id")) == selected_item_id:
            chosen = item
            break
        if selected_index is not None and index == selected_index:
            chosen = item
            break
    price = chosen.get("price") if chosen and isinstance(chosen.get("price"), dict) else {}
    amount = price.get("unit_amount")
    if amount is None:
        amount = price.get("unit_amount_decimal")
    try:
        amount_cents = int(float(amount)) if amount is not None else None
    except (TypeError, ValueError):
        amount_cents = None
    currency = str(price.get("currency") or "").upper() or None
    return amount_cents, currency


def _select_subscription_item_price(subscription: dict[str, Any]) -> dict[str, Any]:
    items = subscription.get("items") if isinstance(subscription.get("items"), dict) else {}
    raw_items = [item for item in items.get("data") or [] if isinstance(item, dict)]
    candidates: list[dict[str, Any]] = []
    for index, item in enumerate(raw_items):
        price = item.get("price") if isinstance(item.get("price"), dict) else {}
        price_id = _stripe_object_id(price)
        mapping = _stripe_price_mapping_result(price_id)
        tier = str(mapping.get("tier") or "free") if mapping.get("matched") else "free"
        active = _subscription_item_is_active(item)
        candidates.append(
            {
                "index": index,
                "item_id": _stripe_object_id(item.get("id")),
                "price_id": price_id,
                "billing_interval": _subscription_item_interval(item),
                "active": active,
                "mapping": mapping,
                "plan_rank": _STRIPE_PLAN_RANK.get(tier, 0) if mapping.get("matched") else -1,
            }
        )
    selected = None
    if candidates:
        active_candidates = [candidate for candidate in candidates if candidate["active"]]
        pool = active_candidates or candidates
        selected = sorted(pool, key=lambda candidate: (candidate["plan_rank"], -int(candidate["index"])), reverse=True)[0]
    price_id = selected.get("price_id") if selected else None
    return {
        "price_id": price_id,
        "billing_interval": selected.get("billing_interval") if selected else None,
        "mapping": selected.get("mapping") if selected else _stripe_price_mapping_result(price_id),
        "item_count": len(raw_items),
        "active_item_count": len([candidate for candidate in candidates if candidate["active"]]),
        "selected_item_id": selected.get("item_id") if selected else None,
        "selected_item_index": selected.get("index") if selected else None,
        "selected_item_active": selected.get("active") if selected else None,
        "candidate_price_ids": [candidate.get("price_id") for candidate in candidates if candidate.get("price_id")],
    }


def _resolve_tier_interval_from_stripe_object(obj: dict[str, Any]) -> tuple[Literal["premium", "pro"] | None, SubscriptionInterval | None, str | None, dict[str, Any]]:
    item_resolution: dict[str, Any] | None = None
    if obj.get("object") == "subscription":
        item_resolution = _select_subscription_item_price(obj)
        price_id = item_resolution.get("price_id")
        mapping_result = item_resolution.get("mapping") if isinstance(item_resolution.get("mapping"), dict) else _stripe_price_mapping_result(price_id)
        mapping_result = {**mapping_result, "subscription_item_resolution": {key: value for key, value in item_resolution.items() if key != "mapping"}}
    else:
        price_id = _extract_subscription_price_id(obj)
        mapping_result = _stripe_price_mapping_result(price_id)
    if mapping_result.get("matched"):
        resolved_interval = (
            item_resolution.get("billing_interval")
            if item_resolution and item_resolution.get("billing_interval") in {"monthly", "annual"}
            else mapping_result["billing_interval"]
        )
        return (
            mapping_result["tier"],  # type: ignore[return-value]
            resolved_interval,  # type: ignore[return-value]
            str(mapping_result["price_id"]),
            mapping_result,
        )
    metadata = _extract_metadata(obj)
    metadata_tier = normalize_tier(metadata.get("admin_override_plan") or metadata.get("target_plan") or metadata.get("tier") or metadata.get("plan"))
    metadata_interval = _normalize_subscription_interval(str(metadata.get("billing_interval") or metadata.get("interval") or ""))
    admin_override = str(metadata.get("admin_override") or metadata.get("walnut_admin_override") or "").strip().lower() in {"1", "true", "yes", "on"}
    if obj.get("object") == "subscription" and admin_override and metadata_tier in ADMIN_BILLING_PAID_TIERS:
        return (
            metadata_tier,  # type: ignore[return-value]
            _extract_subscription_interval(obj) or metadata_interval,
            price_id,
            {**mapping_result, "tier": metadata_tier, "billing_interval": _extract_subscription_interval(obj) or metadata_interval, "matched": False, "reason": "admin_override_metadata"},
        )
    if obj.get("object") == "subscription" and price_id:
        return (
            None,
            _extract_subscription_interval(obj) or metadata_interval,
            price_id,
            mapping_result,
        )
    return (
        metadata_tier if metadata_tier in {"premium", "pro"} else None,
        _extract_subscription_interval(obj) or metadata_interval,
        price_id,
        mapping_result,
    )


def _invoice_line_items(invoice: dict[str, Any]) -> list[dict[str, Any]]:
    lines = invoice.get("lines") if isinstance(invoice.get("lines"), dict) else {}
    return [line for line in lines.get("data") or [] if isinstance(line, dict)]


def _invoice_service_period(invoice: dict[str, Any]) -> tuple[datetime | None, datetime | None]:
    starts: list[datetime] = []
    ends: list[datetime] = []
    for line in _invoice_line_items(invoice):
        period = line.get("period") if isinstance(line.get("period"), dict) else {}
        start = _datetime_from_epoch(period.get("start"))
        end = _datetime_from_epoch(period.get("end"))
        if start:
            starts.append(start)
        if end:
            ends.append(end)
    return (min(starts) if starts else None, max(ends) if ends else None)


def _invoice_billing_period_type(invoice: dict[str, Any]) -> str | None:
    metadata = _extract_metadata(invoice)
    interval = str(metadata.get("billing_interval") or metadata.get("interval") or "").strip().lower()
    if interval in {"monthly", "annual"}:
        return interval
    for line in _invoice_line_items(invoice):
        price = line.get("price") if isinstance(line.get("price"), dict) else {}
        recurring = price.get("recurring") if isinstance(price.get("recurring"), dict) else {}
        stripe_interval = str(recurring.get("interval") or "").strip().lower()
        if stripe_interval == "month":
            return "monthly"
        if stripe_interval == "year":
            return "annual"
    return None


def _invoice_tax_breakdown(invoice: dict[str, Any]) -> tuple[int | None, str | None]:
    total = 0
    found = False
    breakdown: dict[str, Any] = {}
    for key in ("total_tax_amounts", "total_taxes"):
        values = invoice.get(key)
        if isinstance(values, list) and values:
            breakdown[key] = values
            for item in values:
                if isinstance(item, dict):
                    try:
                        total += int(item.get("amount") or 0)
                        found = True
                    except (TypeError, ValueError):
                        pass
    line_taxes: list[Any] = []
    for line in _invoice_line_items(invoice):
        for key in ("tax_amounts", "taxes"):
            values = line.get(key)
            if isinstance(values, list) and values:
                line_taxes.extend(values)
                if not found:
                    for item in values:
                        if isinstance(item, dict):
                            try:
                                total += int(item.get("amount") or 0)
                                found = True
                            except (TypeError, ValueError):
                                pass
    if line_taxes:
        breakdown["line_taxes"] = line_taxes
    if not found:
        try:
            total = int(invoice.get("tax") or 0)
            found = "tax" in invoice
        except (TypeError, ValueError):
            total = 0
    return (total if found else None, json.dumps(breakdown, sort_keys=True) if breakdown else None)


def _invoice_description(invoice: dict[str, Any]) -> str | None:
    description = str(invoice.get("description") or "").strip()
    if description:
        return description
    for line in _invoice_line_items(invoice):
        description = str(line.get("description") or "").strip()
        if description:
            return description
    return None


def _refund_status(invoice: dict[str, Any]) -> str | None:
    charge = invoice.get("charge") if isinstance(invoice.get("charge"), dict) else {}
    try:
        amount_refunded = int(invoice.get("amount_refunded") or charge.get("amount_refunded") or 0)
        total = int(invoice.get("total") or charge.get("amount") or 0)
    except (TypeError, ValueError):
        amount_refunded = 0
        total = 0
    if amount_refunded > 0 and total > 0 and amount_refunded >= total:
        return "refunded"
    if amount_refunded > 0:
        return "partially_refunded"
    if charge.get("refunded") is True:
        return "refunded"
    return "none"


def _stripe_invoice_with_documents(invoice: dict[str, Any]) -> dict[str, Any]:
    invoice_id = _stripe_object_id(invoice.get("id"))
    if not invoice_id or _invoice_has_stripe_documents(invoice) or not _stripe_secret_key():
        return invoice
    try:
        expanded = _stripe_get(
            f"invoices/{invoice_id}",
            {
                "expand[]": [
                    "charge",
                    "payment_intent",
                    "payment_intent.latest_charge",
                ]
            },
        )
    except HTTPException:
        logger.warning("stripe_invoice_document_fetch_failed invoice_id=%s", invoice_id, exc_info=True)
        return invoice
    if not isinstance(expanded, dict) or _stripe_object_id(expanded.get("id")) != invoice_id:
        return invoice
    return {**invoice, **expanded}


def _persist_billing_snapshot(db: Session, invoice: dict[str, Any]) -> BillingTransaction | None:
    invoice_id = _stripe_object_id(invoice.get("id"))
    if not invoice_id:
        return None
    invoice = _stripe_invoice_with_documents(invoice)
    user = _find_user_for_stripe_object(db, invoice)
    service_start, service_end = _invoice_service_period(invoice)
    tax_amount, tax_breakdown_json = _invoice_tax_breakdown(invoice)
    customer_address = invoice.get("customer_address") if isinstance(invoice.get("customer_address"), dict) else {}
    status_transitions = invoice.get("status_transitions") if isinstance(invoice.get("status_transitions"), dict) else {}
    charged_at = (
        _datetime_from_epoch(status_transitions.get("paid_at"))
        or _datetime_from_epoch(invoice.get("created"))
    )
    row = db.execute(
        select(BillingTransaction).where(BillingTransaction.stripe_invoice_id == invoice_id)
    ).scalar_one_or_none()
    if not row:
        row = BillingTransaction(stripe_invoice_id=invoice_id)
        db.add(row)

    row.stripe_customer_id = _stripe_object_id(invoice.get("customer"))
    row.stripe_subscription_id = _extract_subscription_id(invoice)
    row.stripe_payment_intent_id = _stripe_object_id(invoice.get("payment_intent"))
    row.stripe_charge_id = _stripe_object_id(invoice.get("charge"))
    row.user_id = user.id if user else None
    row.customer_name = invoice.get("customer_name") or (user.name if user else None)
    row.customer_email = normalize_email(invoice.get("customer_email") or (user.email if user else ""))
    row.billing_country = customer_address.get("country") or (user.country if user else None)
    row.billing_state_province = customer_address.get("state") or (user.state_province if user else None)
    row.billing_postal_code = customer_address.get("postal_code") or (user.postal_code if user else None)
    row.description = _invoice_description(invoice)
    row.billing_period_type = _invoice_billing_period_type(invoice)
    row.service_period_start = service_start
    row.service_period_end = service_end
    row.subtotal_amount = int(invoice.get("subtotal") or 0) if invoice.get("subtotal") is not None else None
    row.tax_amount = tax_amount
    row.total_amount = int(invoice.get("total") or 0) if invoice.get("total") is not None else None
    row.currency = str(invoice.get("currency") or "").upper() or None
    row.charged_at = charged_at
    row.payment_status = str(invoice.get("status") or "").strip() or None
    row.access_expires_at = service_end
    row.refund_status = _refund_status(invoice)
    row.tax_breakdown_json = tax_breakdown_json
    row.payload_json = json.dumps(invoice, sort_keys=True)
    row.updated_at = datetime.now(timezone.utc)
    db.flush()
    return row


def _int_amount(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _billing_transaction_for_stripe_money_object(db: Session, obj: dict[str, Any]) -> BillingTransaction | None:
    invoice_id = _stripe_object_id(obj.get("invoice"))
    charge_id = _stripe_object_id(obj.get("charge"))
    payment_intent_id = _stripe_object_id(obj.get("payment_intent"))
    charge = obj.get("charge") if isinstance(obj.get("charge"), dict) else {}
    if not charge_id:
        charge_id = _stripe_object_id(charge)
    if not payment_intent_id:
        payment_intent_id = _stripe_object_id(charge.get("payment_intent"))
    conditions = []
    if invoice_id:
        conditions.append(BillingTransaction.stripe_invoice_id == invoice_id)
    if charge_id:
        conditions.append(BillingTransaction.stripe_charge_id == charge_id)
    if payment_intent_id:
        conditions.append(BillingTransaction.stripe_payment_intent_id == payment_intent_id)
    if not conditions:
        return None
    return db.execute(select(BillingTransaction).where(or_(*conditions)).order_by(BillingTransaction.id.desc())).scalars().first()


def _refund_state_from_stripe_object(obj: dict[str, Any]) -> str:
    charge = obj.get("charge") if isinstance(obj.get("charge"), dict) else {}
    source = charge if charge else obj
    amount_refunded = _int_amount(source.get("amount_refunded")) or 0
    amount = _int_amount(source.get("amount")) or _int_amount(source.get("total")) or 0
    if source.get("refunded") is True or (amount_refunded > 0 and amount > 0 and amount_refunded >= amount):
        return "refunded"
    refund_amount = _int_amount(obj.get("amount")) or 0
    if refund_amount > 0 and amount > 0 and refund_amount >= amount:
        return "refunded"
    metadata = _extract_metadata(obj)
    if str(metadata.get("refund_type") or metadata.get("refund_status") or "").strip().lower() == "full":
        return "refunded"
    if amount_refunded > 0 or refund_amount > 0:
        return "partially_refunded"
    return "none"


def _sync_refund_event(db: Session, obj: dict[str, Any]) -> UserAccount | None:
    refund_state = _refund_state_from_stripe_object(obj)
    transaction = _billing_transaction_for_stripe_money_object(db, obj)
    if transaction and refund_state != "none":
        transaction.refund_status = refund_state
        transaction.updated_at = datetime.now(timezone.utc)
    user = _find_user_for_stripe_object(db, obj)
    if not user and transaction and transaction.user_id:
        user = db.get(UserAccount, int(transaction.user_id))
    if not user or refund_state != "refunded":
        db.flush()
        return user
    event_subscription_id = _extract_subscription_id(obj) or (transaction.stripe_subscription_id if transaction else None)
    current_subscription_id = user.stripe_subscription_id
    if (
        event_subscription_id
        and current_subscription_id
        and event_subscription_id != current_subscription_id
        and subscription_policy_tier(user) != "free"
    ):
        db.flush()
        return user
    _clear_paid_entitlement(user, status="refunded")
    db.flush()
    return user


def _find_user_for_stripe_object(db: Session, obj: dict[str, Any]) -> UserAccount | None:
    metadata = _extract_metadata(obj)
    user_id = metadata.get("user_id") or obj.get("client_reference_id")
    email = _extract_customer_email(obj, metadata)

    def prefer_active_user(user: UserAccount | None) -> UserAccount | None:
        if not user or user.deleted_at is None:
            return user
        candidate_email = email or normalize_email(user.original_email or "")
        if candidate_email:
            active = _account_lookup_by_active_email(db, candidate_email)
            if active and active.id != user.id:
                return active
        return user

    if user_id:
        try:
            user = db.get(UserAccount, int(user_id))
            if user:
                return prefer_active_user(user)
        except (TypeError, ValueError):
            pass

    customer = _stripe_object_id(obj.get("customer"))
    subscription = _extract_subscription_id(obj) or obj.get("subscription") or obj.get("id")
    if customer:
        user = db.execute(select(UserAccount).where(UserAccount.stripe_customer_id == str(customer))).scalar_one_or_none()
        if user:
            return prefer_active_user(user)
    if subscription:
        user = db.execute(select(UserAccount).where(UserAccount.stripe_subscription_id == str(subscription))).scalar_one_or_none()
        if user:
            return prefer_active_user(user)
    if email:
        active = _account_lookup_by_active_email(db, email)
        if active:
            return active
        deleted = _deleted_account_lookup_by_original_email(db, email)
        if deleted:
            return deleted
        users = db.execute(select(UserAccount).where(func.lower(UserAccount.email) == email).limit(2)).scalars().all()
        if len(users) == 1:
            return prefer_active_user(users[0])
    return None


def _release_stripe_identifiers_from_deleted_users(
    db: Session,
    user: UserAccount,
    *,
    customer_id: str | None,
    subscription_id: str | None,
) -> None:
    if not customer_id and not subscription_id:
        return
    conditions = []
    if customer_id:
        conditions.append(UserAccount.stripe_customer_id == customer_id)
    if subscription_id:
        conditions.append(UserAccount.stripe_subscription_id == subscription_id)
    rows = db.execute(
        select(UserAccount)
        .where(UserAccount.id != user.id)
        .where(UserAccount.deleted_at.is_not(None))
        .where(or_(*conditions))
    ).scalars().all()
    for row in rows:
        if customer_id and row.stripe_customer_id == customer_id:
            row.stripe_customer_id = None
        if subscription_id and row.stripe_subscription_id == subscription_id:
            row.stripe_subscription_id = None
        row.updated_at = datetime.now(timezone.utc)
    if rows:
        db.flush()


def _sync_user_subscription(
    db: Session,
    *,
    obj: dict[str, Any],
    status: str,
    tier: Literal["free", "premium", "pro"] | None = None,
    billing_interval: SubscriptionInterval | None = None,
    stripe_price_id: str | None = None,
    access_expires_at: datetime | None = None,
) -> UserAccount | None:
    status = (status or "unknown").strip().lower() or "unknown"
    user = _find_user_for_stripe_object(db, obj)
    if not user:
        return None
    if _is_deleted_user(user):
        _clear_paid_entitlement(user, status="deleted")
        db.flush()
        return user

    customer = _stripe_object_id(obj.get("customer"))
    subscription = _extract_subscription_id(obj) or (obj.get("id") if str(obj.get("object")) == "subscription" else None)
    if (
        subscription
        and user.stripe_subscription_id
        and str(subscription) != str(user.stripe_subscription_id)
        and normalize_tier(user.manual_tier_override) in ADMIN_BILLING_PAID_TIERS
    ):
        db.flush()
        return user
    period_end = access_expires_at or _datetime_from_epoch(obj.get("current_period_end"))
    resolved_from_price_tier, resolved_from_price_interval, resolved_price_id, _mapping_result = _resolve_tier_interval_from_stripe_object(obj)
    _release_stripe_identifiers_from_deleted_users(db, user, customer_id=customer, subscription_id=str(subscription) if subscription else None)
    if customer:
        user.stripe_customer_id = customer
    if subscription:
        user.stripe_subscription_id = str(subscription)
    final_price_id = stripe_price_id or resolved_price_id
    if final_price_id:
        user.stripe_price_id = final_price_id
    user.subscription_status = status
    event_metadata = _extract_metadata(obj)
    metadata_tier = normalize_tier(event_metadata.get("tier") or event_metadata.get("plan"))
    existing_subscription_tier = normalize_tier(user.subscription_plan)
    if tier:
        resolved_tier = tier
    elif resolved_from_price_tier:
        resolved_tier = resolved_from_price_tier
    elif metadata_tier in {"premium", "pro"}:
        resolved_tier = metadata_tier
    elif existing_subscription_tier in {"premium", "pro"}:
        resolved_tier = existing_subscription_tier
    elif final_price_id:
        resolved_tier = "free"
    else:
        resolved_tier = "premium"
    user.subscription_plan = resolved_tier
    final_interval = billing_interval or resolved_from_price_interval
    if final_interval:
        user.subscription_interval = final_interval
    if obj.get("object") == "subscription":
        current_amount, current_currency = _subscription_item_amount_currency(obj)
        if current_amount is not None and current_amount > 0:
            user.current_plan_amount_cents = int(current_amount)
        if current_currency:
            user.current_plan_currency = current_currency
    if "cancel_at_period_end" in obj:
        user.subscription_cancel_at_period_end = bool(obj.get("cancel_at_period_end"))
    now = datetime.now(timezone.utc)
    if status in REVOKED_SUBSCRIPTION_STATUSES:
        _clear_paid_entitlement(user, status=status)
        db.flush()
        return user
    if status in PAYMENT_GRACE_SUBSCRIPTION_STATUSES:
        grace_days = stripe_payment_failure_grace_days()
        if grace_days > 0:
            grace_deadline = now + timedelta(days=grace_days)
            if period_end and period_end > now:
                period_end = min(period_end, grace_deadline)
            else:
                period_end = grace_deadline
        else:
            period_end = now
    if period_end:
        user.access_expires_at = period_end
    stripe_policy_tier = subscription_policy_tier(user, now=now)
    if stripe_policy_tier in ADMIN_BILLING_PAID_TIERS:
        user.entitlement_tier = stripe_policy_tier
    elif user.manual_tier_override is not None:
        user.entitlement_tier = normalize_tier(user.manual_tier_override)
    else:
        user.entitlement_tier = stripe_policy_tier
    user.updated_at = now
    db.flush()
    return user


def _link_checkout_session_for_pending_subscription(db: Session, obj: dict[str, Any], *, stripe_price_id: str | None = None) -> UserAccount | None:
    user = _find_user_for_stripe_object(db, obj)
    if not user:
        return None
    if _is_deleted_user(user):
        _clear_paid_entitlement(user, status="deleted")
        db.flush()
        return user

    customer = _stripe_object_id(obj.get("customer"))
    subscription = _extract_subscription_id(obj)
    _release_stripe_identifiers_from_deleted_users(db, user, customer_id=customer, subscription_id=subscription)
    if customer:
        user.stripe_customer_id = customer
    if subscription:
        user.stripe_subscription_id = subscription
    if stripe_price_id:
        user.stripe_price_id = stripe_price_id
    status = (user.subscription_status or "").strip().lower()
    if status not in PAID_SUBSCRIPTION_STATUSES and status not in PAYMENT_GRACE_SUBSCRIPTION_STATUSES:
        user.subscription_status = "checkout_completed"
    if normalize_tier(user.subscription_plan) not in {"premium", "pro"}:
        user.subscription_plan = "free"
    user.updated_at = datetime.now(timezone.utc)
    db.flush()
    return user


def _sync_stripe_customer_deleted(db: Session, obj: dict[str, Any]) -> UserAccount | None:
    customer_id = _stripe_object_id(obj.get("id") or obj.get("customer"))
    if not customer_id:
        return None
    user = db.execute(select(UserAccount).where(UserAccount.stripe_customer_id == customer_id)).scalar_one_or_none()
    if not user:
        return None
    _clear_paid_entitlement(user, status="deleted")
    user.stripe_customer_id = None
    user.stripe_subscription_id = None
    user.stripe_price_id = None
    user.subscription_interval = None
    user.current_plan_amount_cents = None
    user.current_plan_currency = None
    user.updated_at = datetime.now(timezone.utc)
    db.flush()
    return user


def _stripe_event_log_context(obj: dict[str, Any], user: UserAccount | None, *, price_id: str | None = None) -> dict[str, Any]:
    metadata = _extract_metadata(obj)
    item_resolution = _select_subscription_item_price(obj) if obj.get("object") == "subscription" else {}
    return {
        "customer_id": _stripe_object_id(obj.get("customer")) or (_stripe_object_id(obj.get("id")) if obj.get("object") == "customer" else None),
        "subscription_id": _extract_subscription_id(obj) or (obj.get("id") if obj.get("object") == "subscription" else None),
        "checkout_session_id": obj.get("id") if obj.get("object") == "checkout.session" else None,
        "client_reference_id": obj.get("client_reference_id"),
        "metadata_user_id": metadata.get("user_id"),
        "resolved_user_id": user.id if user else None,
        "resolved_email": user.email if user else _extract_customer_email(obj, metadata) or None,
        "price_id": price_id or _extract_subscription_price_id(obj),
        "subscription_item_count": item_resolution.get("item_count"),
        "active_subscription_item_count": item_resolution.get("active_item_count"),
        "selected_subscription_item_id": item_resolution.get("selected_item_id"),
        "mapped_plan": user.subscription_plan if user else None,
        "final_status": user.subscription_status if user else None,
        "final_access": user.entitlement_tier if user else None,
    }


def _stripe_webhook_payload_json(event: dict[str, Any]) -> str:
    return json.dumps(event, sort_keys=True)


def _stripe_webhook_safe_error(exc: Exception) -> str:
    if isinstance(exc, HTTPException):
        return f"HTTPException:{exc.status_code}"
    return exc.__class__.__name__


def _claim_stripe_webhook_event(
    db: Session,
    *,
    event_id: str,
    event_type: str,
    event: dict[str, Any],
) -> tuple[StripeWebhookEvent | None, dict[str, Any] | None]:
    if not event_id:
        return None, None

    now = datetime.now(timezone.utc)
    payload_json = _stripe_webhook_payload_json(event)
    row = StripeWebhookEvent(
        event_id=event_id,
        event_type=event_type,
        payload_json=payload_json,
        status="processing",
        error_message=None,
        processed_at=now,
    )
    db.add(row)
    try:
        db.flush()
        return row, None
    except IntegrityError:
        db.rollback()

    existing = db.get(StripeWebhookEvent, event_id)
    existing_status = str(getattr(existing, "status", "") or "processed").strip().lower()
    if existing_status == "failed":
        updated = db.execute(
            update(StripeWebhookEvent)
            .where(StripeWebhookEvent.event_id == event_id)
            .where(StripeWebhookEvent.status == "failed")
            .values(
                event_type=event_type,
                payload_json=payload_json,
                status="processing",
                error_message=None,
                processed_at=now,
            )
        ).rowcount
        if updated:
            db.flush()
            return db.get(StripeWebhookEvent, event_id), None
        db.rollback()
        existing_status = "processing"

    logger.info(
        "stripe_webhook_duplicate event_id=%s event_type=%s status=%s",
        event_id,
        event_type,
        existing_status or "processed",
    )
    return None, {"status": "already_processed", "event_type": event_type}


def _mark_stripe_webhook_processed(row: StripeWebhookEvent | None) -> None:
    if not row:
        return
    row.status = "processed"
    row.error_message = None
    row.processed_at = datetime.now(timezone.utc)


def _record_stripe_webhook_failure(
    db: Session,
    *,
    event_id: str,
    event_type: str,
    event: dict[str, Any],
    exc: Exception,
) -> None:
    if not event_id:
        return
    payload_json = _stripe_webhook_payload_json(event)
    error_message = _stripe_webhook_safe_error(exc)
    now = datetime.now(timezone.utc)
    recorded_row: StripeWebhookEvent | None = None
    try:
        row = db.get(StripeWebhookEvent, event_id)
        if row and str(row.status or "").strip().lower() == "processed":
            return
        if row:
            row.event_type = event_type
            row.payload_json = payload_json
            row.status = "failed"
            row.error_message = error_message
            row.processed_at = now
            recorded_row = row
        else:
            recorded_row = StripeWebhookEvent(
                event_id=event_id,
                event_type=event_type,
                payload_json=payload_json,
                status="failed",
                error_message=error_message,
                processed_at=now,
            )
            db.add(recorded_row)
        db.commit()
        if recorded_row is not None:
            db.expunge(recorded_row)
    except IntegrityError:
        db.rollback()
        db.execute(
            update(StripeWebhookEvent)
            .where(StripeWebhookEvent.event_id == event_id)
            .where(StripeWebhookEvent.status != "processed")
            .values(
                event_type=event_type,
                payload_json=payload_json,
                status="failed",
                error_message=error_message,
                processed_at=now,
            )
        )
        db.commit()


def process_stripe_event(db: Session, event: dict[str, Any]) -> dict[str, Any]:
    event_id = str(event.get("id") or "")
    event_type = str(event.get("type") or "")
    claimed_event, duplicate_response = _claim_stripe_webhook_event(
        db,
        event_id=event_id,
        event_type=event_type,
        event=event,
    )
    if duplicate_response is not None:
        return duplicate_response

    obj = (event.get("data") or {}).get("object") if isinstance(event.get("data"), dict) else {}
    if not isinstance(obj, dict):
        obj = {}

    try:
        handled = True
        synced_user: UserAccount | None = None
        logged_price_id: str | None = None
        if event_type == "checkout.session.completed":
            _resolved_tier, _billing_interval, price_id, _mapping = _resolve_tier_interval_from_stripe_object(obj)
            logged_price_id = price_id
            synced_user = _link_checkout_session_for_pending_subscription(db, obj, stripe_price_id=price_id)
        elif event_type in {"invoice.paid", "invoice.payment_succeeded", "invoice.payment.paid"}:
            snapshot = _persist_billing_snapshot(db, obj)
            subscription = _stripe_subscription_from_event_object(obj)
            sync_obj = subscription or obj
            resolved_tier, billing_interval, price_id, _mapping = _resolve_tier_interval_from_stripe_object(sync_obj)
            logged_price_id = price_id
            sync_status = str(sync_obj.get("status") or "active")
            if sync_obj is obj and sync_status.strip().lower() in {"paid", "succeeded"}:
                sync_status = "active"
            synced_user = _sync_user_subscription(
                db,
                obj=sync_obj,
                status=sync_status,
                tier=resolved_tier,
                billing_interval=billing_interval,
                stripe_price_id=price_id,
                access_expires_at=snapshot.access_expires_at if snapshot else None,
            )
        elif event_type in {"invoice.payment_failed", "invoice.payment_action_required"}:
            snapshot = _persist_billing_snapshot(db, obj)
            status = "payment_failed" if event_type == "invoice.payment_failed" else "payment_action_required"
            synced_user = _sync_user_subscription(
                db,
                obj=obj,
                status=status,
                access_expires_at=snapshot.access_expires_at if snapshot else None,
            )
        elif event_type == "invoice.voided":
            _persist_billing_snapshot(db, obj)
            synced_user = _sync_user_subscription(db, obj=obj, status="voided")
        elif event_type == "invoice.marked_uncollectible":
            _persist_billing_snapshot(db, obj)
            synced_user = _sync_user_subscription(db, obj=obj, status="uncollectible")
        elif event_type in {"charge.refunded", "refund.created", "refund.updated"}:
            synced_user = _sync_refund_event(db, obj)
        elif event_type in {"customer.subscription.created", "customer.subscription.updated"}:
            status = str(obj.get("status") or "unknown")
            resolved_tier, billing_interval, price_id, _mapping = _resolve_tier_interval_from_stripe_object(obj)
            logged_price_id = price_id
            synced_user = _sync_user_subscription(db, obj=obj, status=status, tier=resolved_tier, billing_interval=billing_interval, stripe_price_id=price_id)
        elif event_type == "customer.deleted":
            synced_user = _sync_stripe_customer_deleted(db, obj)
        elif event_type == "customer.subscription.deleted":
            synced_user = _sync_user_subscription(db, obj=obj, status="deleted")
        elif event_type == "customer.subscription.paused":
            synced_user = _sync_user_subscription(db, obj=obj, status="paused")
        else:
            handled = False

        _mark_stripe_webhook_processed(claimed_event)
        db.commit()
    except Exception as exc:
        db.rollback()
        _record_stripe_webhook_failure(
            db,
            event_id=event_id,
            event_type=event_type,
            event=event,
            exc=exc,
        )
        logger.warning(
            "stripe_webhook_failed event_id=%s event_type=%s error=%s",
            event_id,
            event_type,
            _stripe_webhook_safe_error(exc),
            exc_info=True,
        )
        raise

    context = _stripe_event_log_context(obj, synced_user, price_id=logged_price_id)
    logger.info(
        "stripe_webhook_processed event_id=%s event_type=%s handled=%s customer_id=%s subscription_id=%s checkout_session_id=%s client_reference_id=%s metadata_user_id=%s resolved_user_id=%s resolved_email=%s price_id=%s item_count=%s active_item_count=%s selected_item_id=%s mapped_plan=%s final_status=%s final_access=%s",
        event_id,
        event_type,
        handled,
        context["customer_id"],
        context["subscription_id"],
        context["checkout_session_id"],
        context["client_reference_id"],
        context["metadata_user_id"],
        context["resolved_user_id"],
        context["resolved_email"],
        context["price_id"],
        context["subscription_item_count"],
        context["active_subscription_item_count"],
        context["selected_subscription_item_id"],
        context["mapped_plan"],
        context["final_status"],
        context["final_access"],
    )
    response: dict[str, Any] = {"status": "processed" if handled else "ignored", "event_type": event_type}
    if logged_price_id:
        response["stripe_price_id"] = logged_price_id
        price_mapping = _stripe_price_mapping_result(logged_price_id)
        if price_mapping.get("matched"):
            response["mapped_plan"] = price_mapping.get("tier")
        else:
            response["warning"] = price_mapping.get("reason") or "unmapped_price_id"
    return response


def _last_relevant_stripe_event(db: Session, user: UserAccount) -> dict[str, Any] | None:
    rows = db.execute(
        select(StripeWebhookEvent).order_by(StripeWebhookEvent.processed_at.desc()).limit(100)
    ).scalars().all()
    identifiers = {
        str(value)
        for value in (user.stripe_customer_id, user.stripe_subscription_id, user.email, user.id)
        if value is not None and str(value)
    }
    for row in rows:
        try:
            payload = json.loads(row.payload_json or "{}")
        except Exception:
            payload = {}
        obj = (payload.get("data") or {}).get("object") if isinstance(payload.get("data"), dict) else {}
        metadata = _extract_metadata(obj) if isinstance(obj, dict) else {}
        values = {
            _stripe_object_id(obj.get("customer")) if isinstance(obj, dict) else None,
            _extract_subscription_id(obj) if isinstance(obj, dict) else None,
            _extract_customer_email(obj, metadata) if isinstance(obj, dict) else None,
            str(metadata.get("user_id") or ""),
        }
        if identifiers.intersection(str(value) for value in values if value):
            return {
                "event_id": row.event_id,
                "event_type": row.event_type,
                "processed_at": row.processed_at,
            }
    return None


def _subscription_debug_payload(db: Session, user: UserAccount) -> dict[str, Any]:
    entitlements = entitlements_for_user(db, user)
    price_mapping = _stripe_price_mapping_result(user.stripe_price_id)
    stripe_subscription: dict[str, Any] | None = None
    stripe_lookup_error: str | None = None
    if _stripe_secret_key():
        try:
            stripe_subscription = _stripe_current_subscription_for_user(user)
        except HTTPException as exc:
            stripe_lookup_error = str(exc.detail)
    stripe_item_resolution = _select_subscription_item_price(stripe_subscription) if stripe_subscription else {}
    stripe_price_id = str(stripe_item_resolution.get("price_id")) if stripe_item_resolution.get("price_id") else None
    stripe_mapping = _stripe_price_mapping_result(stripe_price_id)
    stripe_status = str(stripe_subscription.get("status") or "") if stripe_subscription else None
    local_plan = normalize_tier(user.subscription_plan)
    mapped_stripe_plan = normalize_tier(stripe_mapping.get("tier")) if stripe_mapping.get("matched") else "free"
    mismatch = bool(
        stripe_subscription
        and (
            (stripe_status or "").lower() != (user.subscription_status or "").lower()
            or mapped_stripe_plan != local_plan
            or (stripe_price_id or "") != (user.stripe_price_id or "")
        )
    )
    return {
        "user_id": user.id,
        "email": user.email,
        "deleted_at": user.deleted_at,
        "email_verified": user.email_verified_at is not None,
        "access": user.entitlement_tier,
        "local_plan": user.subscription_plan,
        "plan": user.subscription_plan,
        "subscription_status": user.subscription_status,
        "billing_interval": user.subscription_interval,
        "stripe_customer_id": user.stripe_customer_id,
        "stripe_subscription_id": user.stripe_subscription_id,
        "local_stripe_price_id": user.stripe_price_id,
        "stripe_price_id": user.stripe_price_id,
        "current_period_end": user.access_expires_at,
        "cancel_at_period_end": bool(user.subscription_cancel_at_period_end),
        "derived_entitlement": entitlements.tier,
        "price_mapping": price_mapping,
        "last_relevant_stripe_event": _last_relevant_stripe_event(db, user),
        "stripe_lookup": {
            "customer_id_found": _stripe_object_id(stripe_subscription.get("customer")) if stripe_subscription else None,
            "subscription_id_found": _stripe_object_id(stripe_subscription.get("id")) if stripe_subscription else None,
            "subscription_status_found": stripe_status,
            "current_item_price_id": stripe_price_id,
            "price_id_found": stripe_price_id,
            "mapped_plan": stripe_mapping.get("tier") if stripe_mapping.get("matched") else None,
            "mapped_stripe_plan": stripe_mapping.get("tier") if stripe_mapping.get("matched") else None,
            "mismatch": mismatch,
            "subscription_item_resolution": stripe_item_resolution,
            "configured_price_ids": sorted(_stripe_price_mapping().keys()),
            "suggested_repair_action": "sync_stripe_subscription" if mismatch else None,
            "error": stripe_lookup_error,
        },
    }


def _stripe_subscription_sort_key(subscription: dict[str, Any]) -> tuple[int, int, int]:
    status = str(subscription.get("status") or "").lower()
    status_rank = 3 if status in {"active", "trialing"} else 2 if status in {"past_due", "incomplete"} else 1
    period_end = int(subscription.get("current_period_end") or 0)
    created = int(subscription.get("created") or 0)
    return (status_rank, period_end, created)


def _stripe_customer_for_user(user: UserAccount) -> str | None:
    if user.stripe_customer_id:
        return user.stripe_customer_id
    customers = _stripe_get("customers", {"email": user.email, "limit": 2})
    data = customers.get("data") if isinstance(customers.get("data"), list) else []
    if len(data) == 1 and isinstance(data[0], dict):
        return _stripe_object_id(data[0].get("id"))
    return None


def _stripe_current_subscription_for_user(user: UserAccount) -> dict[str, Any] | None:
    if user.stripe_subscription_id:
        subscription = _stripe_get(
            f"subscriptions/{user.stripe_subscription_id}",
            {"expand[]": "items.data.price"},
        )
        if subscription.get("id"):
            return subscription
    customer_id = _stripe_customer_for_user(user)
    if not customer_id:
        return None
    subscriptions = _stripe_get(
        "subscriptions",
        {"customer": customer_id, "status": "all", "limit": 10, "expand[]": "data.items.data.price"},
    )
    data = [item for item in subscriptions.get("data") or [] if isinstance(item, dict)]
    if not data:
        return None
    return sorted(data, key=_stripe_subscription_sort_key, reverse=True)[0]


def _stripe_subscription_from_event_object(obj: dict[str, Any]) -> dict[str, Any] | None:
    if obj.get("object") == "subscription" and obj.get("id"):
        return obj
    subscription_id = _extract_subscription_id(obj)
    if not subscription_id or not _stripe_secret_key():
        return None
    try:
        subscription = _stripe_get(
            f"subscriptions/{subscription_id}",
            {"expand[]": "items.data.price"},
        )
    except HTTPException:
        logger.warning(
            "stripe_subscription_fetch_failed subscription_id=%s customer_id=%s",
            subscription_id,
            _stripe_object_id(obj.get("customer")),
            exc_info=True,
        )
        return None
    return subscription if subscription.get("id") else None


def _reconcile_user_subscription_from_stripe(db: Session, user: UserAccount) -> dict[str, Any]:
    has_stripe_link = bool(user.stripe_customer_id or user.stripe_subscription_id)
    if not has_stripe_link and not _stripe_secret_key():
        return {"synced": False, "reason": "no_stripe_link"}
    try:
        subscription = _stripe_current_subscription_for_user(user)
    except HTTPException:
        logger.warning("stripe_subscription_reconcile_failed user_id=%s", user.id, exc_info=True)
        return {"synced": False, "reason": "stripe_lookup_failed"}
    if not subscription:
        if has_stripe_link:
            _clear_paid_entitlement(user, status="free")
            db.flush()
            return {"synced": True, "status": "free", "reason": "no_subscription"}
        return {"synced": False, "reason": "no_subscription"}
    resolved_tier, billing_interval, price_id, mapping = _resolve_tier_interval_from_stripe_object(subscription)
    item_resolution = mapping.get("subscription_item_resolution") if isinstance(mapping.get("subscription_item_resolution"), dict) else {}
    if price_id and not mapping.get("matched"):
        logger.warning(
            "stripe_subscription_unmapped_price user_id=%s customer_id=%s subscription_id=%s stripe_price_id=%s item_count=%s active_item_count=%s selected_item_id=%s",
            user.id,
            _stripe_object_id(subscription.get("customer")),
            _stripe_object_id(subscription.get("id")),
            price_id,
            item_resolution.get("item_count"),
            item_resolution.get("active_item_count"),
            item_resolution.get("selected_item_id"),
        )
    customer_id = _stripe_object_id(subscription.get("customer"))
    subscription_id = _stripe_object_id(subscription.get("id"))
    _release_stripe_identifiers_from_deleted_users(db, user, customer_id=customer_id, subscription_id=subscription_id)
    if customer_id and not user.stripe_customer_id:
        user.stripe_customer_id = customer_id
    if subscription_id and not user.stripe_subscription_id:
        user.stripe_subscription_id = subscription_id
    db.flush()
    status = str(subscription.get("status") or "unknown").strip().lower() or "unknown"
    updated = _sync_user_subscription(
        db,
        obj=subscription,
        status=status,
        tier=resolved_tier,
        billing_interval=billing_interval,
        stripe_price_id=price_id,
    )
    if not updated:
        return {"synced": False, "reason": "user_not_resolved"}
    return {
        "synced": True,
        "status": updated.subscription_status,
        "plan": updated.subscription_plan,
        "stripe_price_id": updated.stripe_price_id,
        "mapped_plan": mapping.get("tier") if mapping.get("matched") else None,
        "mismatch": bool(price_id and not mapping.get("matched")),
        "reason": mapping.get("reason") if price_id and not mapping.get("matched") else None,
        "subscription_item_resolution": item_resolution,
    }


@router.get("/admin/billing/subscription-debug")
def admin_subscription_debug(
    request: Request,
    email: str = Query(min_length=3, max_length=320),
    db: Session = Depends(get_db),
):
    require_admin_user(db, request)
    normalized_email = normalize_email(email)
    user = db.execute(select(UserAccount).where(func.lower(UserAccount.email) == normalized_email)).scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="User not found.")
    return _subscription_debug_payload(db, user)


@router.post("/admin/billing/sync-stripe-subscription")
def admin_sync_stripe_subscription(
    payload: AdminSubscriptionSyncPayload,
    request: Request,
    db: Session = Depends(get_db),
):
    require_admin_user(db, request)
    normalized_email = normalize_email(payload.email)
    user = db.execute(select(UserAccount).where(func.lower(UserAccount.email) == normalized_email)).scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="User not found.")
    subscription = _stripe_current_subscription_for_user(user)
    if not subscription:
        raise HTTPException(status_code=404, detail="No Stripe subscription found for this user.")
    resolved_tier, billing_interval, price_id, mapping = _resolve_tier_interval_from_stripe_object(subscription)
    if price_id and not mapping.get("matched"):
        raise HTTPException(
            status_code=422,
            detail={
                "code": "unmapped_stripe_price_id",
                "message": "Stripe subscription price is not mapped to a Walnut plan.",
                "stripe_price_id": price_id,
                "configured_price_ids": sorted(_stripe_price_mapping().keys()),
            },
        )
    customer_id = _stripe_object_id(subscription.get("customer"))
    subscription_id = _stripe_object_id(subscription.get("id"))
    if customer_id and not user.stripe_customer_id:
        user.stripe_customer_id = customer_id
    if subscription_id and not user.stripe_subscription_id:
        user.stripe_subscription_id = subscription_id
    db.flush()
    status = str(subscription.get("status") or "unknown")
    updated = _sync_user_subscription(
        db,
        obj=subscription,
        status=status,
        tier=resolved_tier,
        billing_interval=billing_interval,
        stripe_price_id=price_id,
    )
    if not updated:
        raise HTTPException(status_code=404, detail="Unable to map Stripe subscription to user.")
    db.commit()
    db.refresh(updated)
    return {"status": "synced", "user": _subscription_debug_payload(db, updated)}


@router.post("/billing/refresh-subscription")
def refresh_subscription_from_stripe(request: Request, db: Session = Depends(get_db)):
    user = current_user(db, request, required=True)
    readiness = billing_readiness()
    _log_billing_readiness(context="refresh_subscription", readiness=readiness)
    _require_stripe_api_readiness(
        readiness,
        code="stripe_refresh_not_ready",
        message="Stripe subscription refresh is not configured.",
    )
    result = _reconcile_user_subscription_from_stripe(db, user)
    if result.get("reason") == "stripe_lookup_failed":
        raise HTTPException(status_code=502, detail="Unable to refresh subscription from Stripe.")
    db.commit()
    db.refresh(user)
    return {
        "status": "refreshed" if result.get("synced") else "not_found",
        "user": serialize_user_billing_access(user),
        "message": "Subscription refreshed." if result.get("synced") else "No active subscription was found.",
    }


@router.post("/billing/stripe/webhook")
async def stripe_webhook(request: Request, db: Session = Depends(get_db)):
    readiness = billing_readiness()
    _log_billing_readiness(context="webhook", readiness=readiness)
    _require_webhook_readiness(readiness)
    payload = await request.body()
    _verify_stripe_signature(payload, request.headers.get("stripe-signature"))
    try:
        event = json.loads(payload.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail="Invalid webhook payload.") from exc
    return process_stripe_event(db, event)


@router.get("/admin/reports/sales-ledger")
def admin_sales_ledger(
    request: Request,
    db: Session = Depends(get_db),
    period: SalesLedgerPeriod = "month_to_date",
    start_date: str | None = None,
    end_date: str | None = None,
    country: str | None = None,
    sort_by: SalesLedgerSortBy = "date_charged",
    sort_dir: SalesLedgerSortDir = "desc",
    page: int = Query(1, ge=1),
    page_size: int = Query(25, ge=1, le=100),
):
    require_admin_user(db, request)
    rows, total, filters = _sales_ledger_rows(
        db,
        period=period,
        start_date=start_date,
        end_date=end_date,
        country=country,
        sort_by=sort_by,
        sort_dir=sort_dir,
        page=page,
        page_size=page_size,
    )
    page_count = max(1, (total + page_size - 1) // page_size)
    payload_rows = [_sales_ledger_row(row) for row in rows]
    return {
        "items": payload_rows,
        "page": page,
        "page_size": page_size,
        "total": total,
        "total_pages": page_count,
        "has_previous": page > 1,
        "has_next": page < page_count,
        "filters": filters,
        "sort": {"sort_by": sort_by, "sort_dir": sort_dir},
        "summary": {
            "net_revenue_amount": sum(row["net_revenue_amount"] for row in payload_rows),
            "vat_collected": sum(row["vat1_collected"] + row["vat2_collected"] for row in payload_rows),
            "gross_amount": sum(row["gross_amount"] for row in payload_rows),
        },
    }


@router.post("/analytics/page-view", status_code=204)
def record_page_view(payload: PageViewPayload, request: Request, db: Session = Depends(get_db)):
    path = _safe_analytics_path(payload.path)
    if not path:
        return Response(status_code=204)
    normalized_path = _normalize_analytics_path(path)
    if normalized_path.startswith("/_next/") or normalized_path.startswith("/api/"):
        return Response(status_code=204)

    try:
        user = current_user(db, request, required=False)
    except HTTPException:
        user = None
    user_agent = request.headers.get("user-agent", "")
    now = datetime.now(timezone.utc)

    if user:
        duplicate_query = (
            select(PageViewEvent)
            .where(PageViewEvent.user_id == user.id)
            .where(PageViewEvent.normalized_path == normalized_path)
            .where(PageViewEvent.created_at >= now - timedelta(seconds=20))
            .limit(1)
        )
    else:
        session_hash = _analytics_session_hash(request, payload.session_id)
        duplicate_query = (
            select(PageViewEvent)
            .where(PageViewEvent.session_id_hash == session_hash)
            .where(PageViewEvent.normalized_path == normalized_path)
            .where(PageViewEvent.created_at >= now - timedelta(seconds=20))
            .limit(1)
        ) if session_hash else None
    if duplicate_query is not None and db.execute(duplicate_query).scalar_one_or_none():
        return Response(status_code=204)

    row = PageViewEvent(
        user_id=user.id if user else None,
        session_id_hash=None if user else _analytics_session_hash(request, payload.session_id),
        path=path,
        normalized_path=normalized_path,
        route_group=_analytics_route_group(normalized_path),
        referrer_path=_safe_analytics_path(payload.referrer_path),
        user_agent_family=_user_agent_family(user_agent),
        device_type=_device_type(user_agent),
        is_authenticated=bool(user),
        plan_at_time=normalize_tier(user.entitlement_tier if user else None) if user else "anonymous",
        metadata_json=json.dumps({"title": payload.title[:120]}, sort_keys=True) if payload.title else None,
        created_at=now,
    )
    db.add(row)
    db.commit()
    return Response(status_code=204)


@router.get("/admin/reports/page-analytics")
def admin_page_analytics(
    request: Request,
    db: Session = Depends(get_db),
    period: Literal["24h", "7d", "30d"] = "7d",
    limit: int = Query(20, ge=1, le=100),
):
    require_admin_user(db, request)
    start, normalized_period = _page_analytics_period_start(period)
    visitor_key = func.coalesce(cast(PageViewEvent.user_id, String), PageViewEvent.session_id_hash, cast(PageViewEvent.id, String))
    rows = db.execute(
        select(
            PageViewEvent.normalized_path.label("page"),
            PageViewEvent.route_group.label("route_group"),
            func.count(PageViewEvent.id).label("views"),
            func.count(func.distinct(visitor_key)).label("unique_visitors"),
            func.sum(case((PageViewEvent.is_authenticated.is_(True), 1), else_=0)).label("authenticated_views"),
            func.sum(case((PageViewEvent.plan_at_time.in_(["premium", "pro", "admin"]), 1), else_=0)).label("paid_views"),
            func.sum(case((PageViewEvent.plan_at_time.in_(["pro", "admin"]), 1), else_=0)).label("pro_views"),
            func.sum(case((PageViewEvent.device_type == "mobile", 1), else_=0)).label("mobile_views"),
            func.max(PageViewEvent.created_at).label("last_viewed_at"),
        )
        .where(PageViewEvent.created_at >= start)
        .group_by(PageViewEvent.normalized_path, PageViewEvent.route_group)
        .order_by(func.count(PageViewEvent.id).desc(), PageViewEvent.normalized_path.asc())
        .limit(limit)
    ).all()

    items = []
    for row in rows:
        views = int(row.views or 0)
        items.append(
            {
                "page": row.page,
                "route_group": row.route_group,
                "views": views,
                "unique_users": int(row.unique_visitors or 0),
                "authenticated_views": int(row.authenticated_views or 0),
                "anonymous_views": max(views - int(row.authenticated_views or 0), 0),
                "auth_percent": round((int(row.authenticated_views or 0) / views) * 100, 1) if views else 0,
                "paid_percent": round((int(row.paid_views or 0) / views) * 100, 1) if views else 0,
                "pro_percent": round((int(row.pro_views or 0) / views) * 100, 1) if views else 0,
                "mobile_percent": round((int(row.mobile_views or 0) / views) * 100, 1) if views else 0,
                "last_viewed_at": row.last_viewed_at,
            }
        )

    trend_rows = db.execute(
        select(func.date(PageViewEvent.created_at).label("day"), func.count(PageViewEvent.id).label("views"))
        .where(PageViewEvent.created_at >= start)
        .group_by(func.date(PageViewEvent.created_at))
        .order_by(func.date(PageViewEvent.created_at).asc())
    ).all()
    low_usage = sorted(items, key=lambda item: (item["views"], item["page"]))[: min(10, len(items))]
    return {
        "period": normalized_period,
        "generated_at": datetime.now(timezone.utc),
        "top_pages": items,
        "low_usage_pages": low_usage,
        "trend_by_day": [{"day": str(row.day), "views": int(row.views or 0)} for row in trend_rows],
    }


@router.get("/admin/provider-usage/fmp")
def admin_provider_usage_fmp(request: Request, db: Session = Depends(get_db)):
    require_admin_user(db, request)
    return provider_usage_summary(limit=30, db=db)


@router.get("/admin/reports/summary")
def admin_reports_summary(request: Request, db: Session = Depends(get_db)):
    require_admin_user(db, request)
    return _reports_summary(db)


@router.get("/admin/reports/sales-ledger/export.{export_format}", dependencies=[Depends(rate_limit_admin_export)])
def admin_sales_ledger_export(
    export_format: Literal["xlsx", "pdf"],
    request: Request,
    db: Session = Depends(get_db),
    period: SalesLedgerPeriod = "month_to_date",
    start_date: str | None = None,
    end_date: str | None = None,
    country: str | None = None,
    sort_by: SalesLedgerSortBy = "date_charged",
    sort_dir: SalesLedgerSortDir = "desc",
):
    require_admin_user(db, request)
    rows, _total, filters = _sales_ledger_rows(
        db,
        period=period,
        start_date=start_date,
        end_date=end_date,
        country=country,
        sort_by=sort_by,
        sort_dir=sort_dir,
    )
    payload_rows = [_sales_ledger_row(row) for row in rows]
    if export_format == "xlsx":
        content = _sales_ledger_xlsx(payload_rows)
        media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    else:
        content = _sales_ledger_pdf(payload_rows, filters)
        media_type = "application/pdf"
    filename = _export_filename("sales-ledger", export_format)
    return Response(
        content=content,
        media_type=media_type,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.get("/admin/users")
def admin_users(
    request: Request,
    db: Session = Depends(get_db),
    search: Annotated[str | None, Query(max_length=160)] = None,
    q: Annotated[str | None, Query(max_length=160)] = None,
    plan: AdminUserPlanFilter = "all",
    status: str | None = None,
    country: str | None = None,
    admin: AdminUserAdminFilter = "all",
    sort_by: AdminUserSortBy = "created_at",
    sort_dir: AdminUserSortDir = "desc",
    page: int = Query(1, ge=1),
    page_size: int = Query(25, ge=1, le=100),
):
    require_admin_user(db, request)
    rows, total, filters = _admin_user_rows(
        db,
        plan=plan,
        status=status,
        country=country,
        admin=admin,
        sort_by=sort_by,
        sort_dir=sort_dir,
        page=page,
        page_size=page_size,
        search=(search or "").strip() or (q or "").strip() or None,
    )
    page_count = max(1, (total + page_size - 1) // page_size)
    billing_rows = _latest_billing_rows_by_user(db, rows)
    paid_rows = _successful_billing_rows_by_user(db, rows)
    plan_prices = _plan_price_lookup(db)
    return {
        "items": [
            _admin_user_row(user, latest_billing_row=billing_rows.get(user.id), billing_rows=paid_rows.get(user.id), plan_prices=plan_prices)
            for user in rows
        ],
        "page": page,
        "page_size": page_size,
        "total": total,
        "total_pages": page_count,
        "has_previous": page > 1,
        "has_next": page < page_count,
        "filters": filters,
        "sort": {"sort_by": sort_by, "sort_dir": sort_dir},
    }


@router.get("/admin/users/export.{export_format}", dependencies=[Depends(rate_limit_admin_export)])
def admin_users_export(
    export_format: Literal["xlsx", "pdf"],
    request: Request,
    db: Session = Depends(get_db),
    search: Annotated[str | None, Query(max_length=160)] = None,
    q: Annotated[str | None, Query(max_length=160)] = None,
    plan: AdminUserPlanFilter = "all",
    status: str | None = None,
    country: str | None = None,
    admin: AdminUserAdminFilter = "all",
    sort_by: AdminUserSortBy = "created_at",
    sort_dir: AdminUserSortDir = "desc",
):
    require_admin_user(db, request)
    users, _total, filters = _admin_user_rows(
        db,
        plan=plan,
        status=status,
        country=country,
        admin=admin,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=(search or "").strip() or (q or "").strip() or None,
    )
    rows = _admin_users_export_rows(users, db=db)
    if export_format == "xlsx":
        content = _admin_users_xlsx(rows)
        media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    else:
        content = _admin_users_pdf(rows, filters)
        media_type = "application/pdf"
    filename = _export_filename("admin-users", export_format)
    return Response(
        content=content,
        media_type=media_type,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.get("/admin/settings")
def admin_settings(request: Request, db: Session = Depends(get_db), include_users: bool = False):
    require_admin_user(db, request)
    user_limit = 100
    users = (
        db.execute(
            select(UserAccount)
            .where(UserAccount.deleted_at.is_(None))
            .order_by(UserAccount.created_at.desc(), UserAccount.id.desc())
            .limit(user_limit + 1)
        ).scalars().all()
        if include_users
        else []
    )
    users_truncated = len(users) > user_limit
    if users_truncated:
        users = users[:user_limit]
    return {
        "stripe": _stripe_config_status(),
        "stripe_tax": _stripe_tax_config(db),
        "oauth": {"google_client_id": _google_client_id(db) or ""},
        "users": [_admin_user_row(user) for user in users],
        "users_limit": user_limit if include_users else 0,
        "users_truncated": users_truncated,
        "feature_gates": feature_gate_payloads(db),
        "features": DEFAULT_FEATURE_GATES,
        "plan_config": plan_config_payload(db),
    }


def _email_template_variables(template: EmailTemplate) -> list[str]:
    try:
        parsed = json.loads(template.variables_json or "[]")
    except Exception:
        return []
    return [str(item) for item in parsed] if isinstance(parsed, list) else []


def _email_template_payload(template: EmailTemplate) -> dict[str, Any]:
    return {
        "id": template.id,
        "template_key": template.template_key,
        "name": template.name,
        "category": template.category,
        "from_name": template.from_name,
        "from_email": template.from_email,
        "reply_to": template.reply_to,
        "subject": template.subject,
        "preheader": template.preheader,
        "body_text": template.body_text,
        "body_html": template.body_html,
        "variables": _email_template_variables(template),
        "variables_json": template.variables_json,
        "enabled": bool(template.enabled),
        "created_at": template.created_at,
        "updated_at": template.updated_at,
    }


def _email_delivery_payload(delivery: EmailDelivery) -> dict[str, Any]:
    return {
        "id": delivery.id,
        "user_id": delivery.user_id,
        "to_email": delivery.to_email,
        "from_email": delivery.from_email,
        "template_key": delivery.template_key,
        "category": delivery.category,
        "subject": delivery.subject,
        "provider": delivery.provider,
        "provider_message_id": delivery.provider_message_id,
        "status": delivery.status,
        "idempotency_key": delivery.idempotency_key,
        "error": delivery.error,
        "payload": _loads_dict(delivery.payload_json),
        "created_at": delivery.created_at,
        "sent_at": delivery.sent_at,
    }


def _email_delivery_date_bounds(date_window: EmailDeliveryDateWindow | None) -> tuple[datetime | None, datetime | None]:
    window = date_window or "last_30"
    if window == "all_time":
        return None, None

    try:
        app_tz = ZoneInfo(DEFAULT_DIGEST_TIMEZONE)
    except Exception:
        app_tz = timezone.utc
    now_local = datetime.now(timezone.utc).astimezone(app_tz)

    if window == "today":
        start_local = datetime.combine(now_local.date(), datetime.min.time(), tzinfo=app_tz)
        return start_local.astimezone(timezone.utc), (start_local + timedelta(days=1)).astimezone(timezone.utc)

    if window == "last_month":
        first_this_month = now_local.date().replace(day=1)
        if first_this_month.month == 1:
            first_last_month = date(first_this_month.year - 1, 12, 1)
        else:
            first_last_month = date(first_this_month.year, first_this_month.month - 1, 1)
        start_local = datetime.combine(first_last_month, datetime.min.time(), tzinfo=app_tz)
        end_local = datetime.combine(first_this_month, datetime.min.time(), tzinfo=app_tz)
        return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc)

    days_by_window = {
        "last_7": 7,
        "last_14": 14,
        "last_30": 30,
    }
    days = days_by_window.get(window, 30)
    return (now_local - timedelta(days=days)).astimezone(timezone.utc), None


def _require_email_template(db: Session, template_key: str) -> EmailTemplate:
    template = db.execute(select(EmailTemplate).where(EmailTemplate.template_key == template_key)).scalar_one_or_none()
    if not template:
        raise HTTPException(status_code=404, detail="Email template not found.")
    return template


def _render_email_template_for_admin(template: EmailTemplate, context: dict[str, Any]) -> dict[str, str | None]:
    variables = _email_template_variables(template)
    try:
        return {
            "subject": render_template_string(template.subject, context, variables),
            "body_text": render_template_string(template.body_text, context, variables),
            "body_html": render_template_string(template.body_html, context, variables, html=True) if template.body_html else None,
        }
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


def _admin_digest_user(db: Session, payload: AdminDigestSendTestPayload | AdminBillingStatementSendTestPayload, admin: UserAccount) -> UserAccount:
    user: UserAccount | None = None
    if payload.user_id is not None:
        user = db.get(UserAccount, payload.user_id)
    elif payload.email:
        user = db.execute(select(UserAccount).where(func.lower(UserAccount.email) == normalize_email(payload.email))).scalar_one_or_none()
    else:
        user = admin
    if not user:
        raise HTTPException(status_code=404, detail="missing_user")
    return user


def _admin_digest_watchlist(db: Session, user: UserAccount, watchlist_id: int | None) -> Watchlist:
    if watchlist_id is None:
        raise HTTPException(status_code=422, detail="missing_watchlist")
    watchlist = db.execute(
        select(Watchlist).where(Watchlist.id == watchlist_id, Watchlist.owner_user_id == user.id)
    ).scalar_one_or_none()
    if not watchlist:
        raise HTTPException(status_code=404, detail="missing_watchlist")
    return watchlist


def _admin_digest_since(payload: AdminDigestSendTestPayload) -> datetime:
    if payload.since is not None:
        return payload.since if payload.since.tzinfo else payload.since.replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc) - timedelta(days=payload.lookback_days)


def _default_billing_period() -> tuple[date, date]:
    today = datetime.now(timezone.utc).date()
    start = today.replace(day=1)
    if start.month == 12:
        end = date(start.year + 1, 1, 1)
    else:
        end = date(start.year, start.month + 1, 1)
    return start, end


@router.get("/admin/email/templates")
def admin_email_templates(request: Request, db: Session = Depends(get_db)):
    require_admin_user(db, request)
    templates = db.execute(select(EmailTemplate).order_by(EmailTemplate.category.asc(), EmailTemplate.template_key.asc())).scalars().all()
    return {"items": [_email_template_payload(template) for template in templates]}


@router.get("/admin/email/templates/{template_key}")
def admin_email_template(template_key: str, request: Request, db: Session = Depends(get_db)):
    require_admin_user(db, request)
    return _email_template_payload(_require_email_template(db, template_key))


@router.put("/admin/email/templates/{template_key}", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_update_email_template(
    template_key: str,
    payload: EmailTemplateUpdatePayload,
    request: Request,
    db: Session = Depends(get_db),
):
    require_admin_user(db, request)
    template = _require_email_template(db, template_key)
    fields = _payload_fields_set(payload)
    for field in (
        "name",
        "category",
        "from_name",
        "from_email",
        "reply_to",
        "subject",
        "preheader",
        "body_text",
        "body_html",
        "enabled",
    ):
        if field in fields:
            value = getattr(payload, field)
            if field == "from_email" and (not value or "@" not in normalize_email(value)):
                raise HTTPException(status_code=422, detail="from_email must be a valid email address.")
            if field == "reply_to" and value and "@" not in normalize_email(value):
                raise HTTPException(status_code=422, detail="reply_to must be a valid email address.")
            setattr(template, field, value.strip() if isinstance(value, str) else value)
    if "variables_json" in fields:
        try:
            parsed_variables = json.loads(payload.variables_json or "[]")
        except json.JSONDecodeError as exc:
            raise HTTPException(status_code=422, detail="variables_json must be a JSON array.") from exc
        if not isinstance(parsed_variables, list) or not all(isinstance(item, str) for item in parsed_variables):
            raise HTTPException(status_code=422, detail="variables_json must be a JSON array of strings.")
        template.variables_json = json.dumps(parsed_variables)
    template.updated_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(template)
    return _email_template_payload(template)


@router.post("/admin/email/templates/{template_key}/reset-default", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_reset_email_template_default(
    template_key: str,
    request: Request,
    db: Session = Depends(get_db),
):
    require_admin_user(db, request)
    template = reset_email_template_to_default(db, template_key)
    if template is None:
        raise HTTPException(status_code=404, detail="Default email template not found.")
    return _email_template_payload(template)


@router.post("/admin/email/templates/reset-defaults", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_reset_email_templates_defaults(
    payload: EmailTemplateBulkResetPayload,
    request: Request,
    db: Session = Depends(get_db),
):
    require_admin_user(db, request)
    templates = reset_email_templates_to_defaults(db, payload.template_keys)
    if payload.template_keys and len(templates) != len(set(payload.template_keys)):
        raise HTTPException(status_code=404, detail="One or more default email templates were not found.")
    return {"items": [_email_template_payload(template) for template in templates]}


@router.post("/admin/email/templates/{template_key}/preview", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_preview_email_template(
    template_key: str,
    payload: EmailTemplatePreviewPayload,
    request: Request,
    db: Session = Depends(get_db),
):
    require_admin_user(db, request)
    template = _require_email_template(db, template_key)
    return {
        "template": _email_template_payload(template),
        "rendered": _render_email_template_for_admin(template, payload.context),
    }


@router.post("/admin/email/templates/{template_key}/send-test", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_send_test_email_template(
    template_key: str,
    payload: EmailTemplateSendTestPayload,
    request: Request,
    db: Session = Depends(get_db),
):
    admin = require_admin_user(db, request)
    template = _require_email_template(db, template_key)
    to_email = normalize_email(payload.to_email or admin.email)
    if not to_email or "@" not in to_email:
        raise HTTPException(status_code=422, detail="A valid test recipient email is required.")
    result = send_email(
        db,
        to_email=to_email,
        template_key=template.template_key,
        context=payload.context,
        user_id=admin.id,
        category=template.category,
        idempotency_key=f"admin-test:{admin.id}:{template.template_key}:{int(time.time())}",
        force_log_only=not email_delivery_enabled(),
    )
    delivery_id = result.get("id") if isinstance(result, dict) else None
    delivery = db.get(EmailDelivery, delivery_id) if delivery_id is not None else None
    return _email_delivery_payload(delivery) if delivery else result


@router.post("/admin/email/digests/watchlist-activity/send-test", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_send_watchlist_activity_digest_test(
    payload: AdminDigestSendTestPayload,
    request: Request,
    db: Session = Depends(get_db),
):
    admin = require_admin_user(db, request)
    user = _admin_digest_user(db, payload, admin)
    watchlist = _admin_digest_watchlist(db, user, payload.watchlist_id)
    return send_watchlist_activity_digest(db, user, watchlist, _admin_digest_since(payload), force=payload.force)


@router.post("/admin/email/digests/monitoring/send-test", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_send_monitoring_digest_test(
    payload: AdminDigestSendTestPayload,
    request: Request,
    db: Session = Depends(get_db),
):
    admin = require_admin_user(db, request)
    user = _admin_digest_user(db, payload, admin)
    watchlist = _admin_digest_watchlist(db, user, payload.watchlist_id)
    return send_monitoring_digest(db, user, watchlist, _admin_digest_since(payload), force=payload.force)


@router.post("/admin/email/digests/signals/send-test", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_send_signal_digest_test(
    payload: AdminDigestSendTestPayload,
    request: Request,
    db: Session = Depends(get_db),
):
    admin = require_admin_user(db, request)
    user = _admin_digest_user(db, payload, admin)
    return send_signal_alert_digest(db, user, _admin_digest_since(payload), force=payload.force)


@router.post("/admin/email/digests/run-now", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_run_email_digest_now(
    payload: AdminDigestRunNowPayload,
    request: Request,
    db: Session = Depends(get_db),
):
    admin = require_admin_user(db, request)
    results = run_digest_job(
        db,
        kind=payload.kind,
        lookback_days=payload.lookback_days,
        limit=payload.limit,
        force=payload.force,
        dry_run=payload.dry_run,
    )
    summary = summarize_digest_results(results)
    logger.info(
        "admin_email_digest_run_now admin_id=%s kind=%s dry_run=%s force=%s total=%s sent=%s skipped=%s failed=%s",
        admin.id,
        payload.kind,
        payload.dry_run,
        payload.force,
        summary["total"],
        summary["sent"] + summary["log_only"] + summary["queued"],
        summary["skipped"],
        summary["failed"],
    )
    return {
        "kind": payload.kind,
        "dry_run": payload.dry_run,
        "force": payload.force,
        "lookback_days": payload.lookback_days,
        "limit": payload.limit,
        "summary": summary,
        "items": results,
    }


@router.post("/admin/email/intraday/run-now", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_run_intraday_email_alerts_now(
    payload: AdminIntradayRunNowPayload,
    request: Request,
    db: Session = Depends(get_db),
):
    admin = require_admin_user(db, request)
    results = run_intraday_alert_sweep(
        db,
        lookback_minutes=payload.lookback_minutes,
        limit=payload.limit,
        dry_run=payload.dry_run,
        market_hours_only=payload.market_hours_only,
    )
    summary = summarize_intraday_alert_results(results)
    logger.info(
        "admin_email_intraday_run_now admin_id=%s dry_run=%s limit=%s candidate_count=%s sent_count=%s skipped_count=%s failed_count=%s",
        admin.id,
        payload.dry_run,
        payload.limit,
        summary["candidate_count"],
        summary["sent_count"],
        summary["skipped_count"],
        summary["failed_count"],
    )
    return {
        "dry_run": payload.dry_run,
        "lookback_minutes": payload.lookback_minutes,
        "limit": payload.limit,
        "market_hours_only": payload.market_hours_only,
        "summary": summary,
        "items": results,
    }


@router.post("/admin/email/billing/monthly-statement/send-test", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_send_monthly_billing_statement_test(
    payload: AdminBillingStatementSendTestPayload,
    request: Request,
    db: Session = Depends(get_db),
):
    admin = require_admin_user(db, request)
    user = _admin_digest_user(db, payload, admin)
    default_start, default_end = _default_billing_period()
    return send_monthly_billing_statement(
        db,
        user,
        payload.period_start or default_start,
        payload.period_end or default_end,
        force=payload.force,
    )


@router.get("/admin/email/deliveries")
def admin_email_deliveries(
    request: Request,
    db: Session = Depends(get_db),
    status: str | None = None,
    template_key: str | None = None,
    recipient: str | None = None,
    date_window: EmailDeliveryDateWindow = "last_30",
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1),
):
    require_admin_user(db, request)
    page_size = min(max(int(page_size), 5), 100)
    status_filter = (status or "").strip()
    template_filter = (template_key or "").strip()
    recipient_filter = (recipient or "").strip()
    date_start, date_end = _email_delivery_date_bounds(date_window)

    filters = []
    if status_filter and status_filter != "all":
        filters.append(EmailDelivery.status == status_filter)
    if template_filter and template_filter != "all":
        filters.append(EmailDelivery.template_key == template_filter)
    if recipient_filter:
        filters.append(EmailDelivery.to_email.ilike(f"%{recipient_filter}%"))
    if date_start is not None:
        filters.append(EmailDelivery.created_at >= date_start)
    if date_end is not None:
        filters.append(EmailDelivery.created_at < date_end)

    query = select(EmailDelivery)
    count_query = select(func.count()).select_from(EmailDelivery)
    if filters:
        query = query.where(*filters)
        count_query = count_query.where(*filters)
    total = int(db.execute(count_query).scalar() or 0)
    total_pages = max(1, (total + page_size - 1) // page_size)
    effective_page = min(page, total_pages)
    rows = db.execute(
        query.order_by(EmailDelivery.created_at.desc(), EmailDelivery.id.desc())
        .offset((effective_page - 1) * page_size)
        .limit(page_size)
    ).scalars().all()
    return {
        "items": [_email_delivery_payload(row) for row in rows],
        "page": effective_page,
        "page_size": page_size,
        "total": total,
        "total_pages": total_pages,
        "filters": {
            "recipient": recipient_filter,
            "status": status_filter,
            "template_key": template_filter,
            "date_window": date_window,
        },
    }


@router.get("/plan-config")
def public_plan_config(response: Response = None, db: Session = Depends(get_db)):
    if isinstance(response, Session):
        db = response
        response = None
    if response is not None:
        response.headers["Cache-Control"] = "public, max-age=300, s-maxage=3600, stale-while-revalidate=86400"
    return plan_config_payload(db)


@router.patch("/admin/settings/oauth", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_update_oauth_settings(
    payload: OAuthSettingsPayload,
    request: Request,
    db: Session = Depends(get_db),
):
    require_admin_user(db, request)
    row = _set_setting(db, "google_client_id", payload.google_client_id)
    db.commit()
    return {"google_client_id": row.value or ""}


@router.patch("/admin/settings/stripe-tax", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_update_stripe_tax_settings(
    payload: StripeTaxSettingsPayload,
    request: Request,
    db: Session = Depends(get_db),
):
    require_admin_user(db, request)
    _set_setting(db, "stripe_tax_automatic_tax_enabled", "true" if payload.automatic_tax_enabled else "false")
    _set_setting(db, "stripe_tax_require_billing_address", "true" if payload.require_billing_address else "false")
    _set_setting(db, "stripe_tax_product_tax_code", payload.product_tax_code)
    _set_setting(db, "stripe_tax_price_tax_behavior", payload.price_tax_behavior)
    db.commit()
    return _stripe_tax_config(db)


@router.post("/admin/users/{user_id}/premium", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_set_premium(user_id: int, payload: ManualPremiumPayload, request: Request, db: Session = Depends(get_db)):
    admin = require_admin_user(db, request)
    user = db.get(UserAccount, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found.")
    sync_result = sync_admin_billing_override_to_stripe(
        db,
        user=user,
        requested_override={
            "plan": payload.tier,
            "price_mode": payload.price_mode,
            "custom_price": payload.custom_price.model_dump() if payload.custom_price else None,
        },
        admin_actor=admin,
        request=request,
    )
    _apply_admin_billing_stripe_links(user, sync_result)
    user.manual_tier_override = payload.tier
    if payload.tier:
        user.entitlement_tier = payload.tier
    else:
        user.entitlement_tier = subscription_policy_tier(user)
    user.updated_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(user)
    return _admin_user_row(user)


@router.patch("/admin/users/{user_id}/price-override", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_set_user_price_override(
    user_id: int,
    payload: PriceOverridePayload,
    request: Request,
    db: Session = Depends(get_db),
):
    admin = require_admin_user(db, request)
    user = db.get(UserAccount, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found.")
    sync_result = sync_admin_billing_override_to_stripe(
        db,
        user=user,
        requested_override={"price_override": _price_override_requested_state(payload)},
        admin_actor=admin,
        request=request,
    )
    _apply_admin_billing_stripe_links(user, sync_result)
    _apply_price_override(user, payload)
    user.updated_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(user)
    return _admin_user_row(user)


@router.delete("/admin/users/{user_id}/price-override", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_clear_user_price_override(user_id: int, request: Request, db: Session = Depends(get_db)):
    admin = require_admin_user(db, request)
    user = db.get(UserAccount, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found.")
    sync_result = sync_admin_billing_override_to_stripe(
        db,
        user=user,
        requested_override={"clear_price_override": True},
        admin_actor=admin,
        request=request,
    )
    _apply_admin_billing_stripe_links(user, sync_result)
    _apply_price_override(user, None, clear=True)
    user.updated_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(user)
    return _admin_user_row(user)


@router.post("/admin/users/batch", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_batch_update_users(payload: AdminBatchUsersPayload, request: Request, db: Session = Depends(get_db)):
    admin = require_admin_user(db, request)
    ids = sorted({int(user_id) for user_id in payload.user_ids})
    users = db.execute(select(UserAccount).where(UserAccount.id.in_(ids))).scalars().all()
    if len(users) != len(ids):
        raise HTTPException(status_code=404, detail="One or more users were not found.")
    changed = 0
    for user in users:
        if user.id == admin.id and payload.suspended is True:
            raise HTTPException(status_code=400, detail="Admin cannot suspend the current admin session.")
    updated_users: list[UserAccount] = []
    for user in users:
        requested_override: dict[str, Any] = {}
        if payload.tier is not None:
            requested_override["plan"] = payload.tier
            requested_override["price_mode"] = payload.price_mode
            requested_override["custom_price"] = payload.custom_price.model_dump() if payload.custom_price else None
        if payload.suspended is not None:
            requested_override["suspended"] = payload.suspended
        if payload.clear_price_override:
            requested_override["clear_price_override"] = True
        elif payload.price_override is not None:
            requested_override["price_override"] = _price_override_requested_state(payload.price_override)

        sync_result = sync_admin_billing_override_to_stripe(
            db,
            user=user,
            requested_override=requested_override,
            admin_actor=admin,
            request=request,
        )
        _apply_admin_billing_stripe_links(user, sync_result)
        if payload.tier is not None:
            user.manual_tier_override = payload.tier
            user.entitlement_tier = payload.tier
        if payload.suspended is not None:
            user.is_suspended = payload.suspended
        if payload.clear_price_override:
            _apply_price_override(user, None, clear=True)
        elif payload.price_override is not None:
            _apply_price_override(user, payload.price_override)
        user.updated_at = datetime.now(timezone.utc)
        db.commit()
        db.refresh(user)
        updated_users.append(user)
        changed += 1
    billing_rows = _latest_billing_rows_by_user(db, updated_users)
    paid_rows = _successful_billing_rows_by_user(db, updated_users)
    plan_prices = _plan_price_lookup(db)
    return {
        "status": "ok",
        "updated": changed,
        "items": [
            _admin_user_row(user, latest_billing_row=billing_rows.get(user.id), billing_rows=paid_rows.get(user.id), plan_prices=plan_prices)
            for user in updated_users
        ],
    }


@router.post("/admin/users/{user_id}/suspend", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_suspend_user(user_id: int, payload: SuspendPayload, request: Request, db: Session = Depends(get_db)):
    admin = require_admin_user(db, request)
    if admin.id == user_id and payload.suspended:
        raise HTTPException(status_code=400, detail="Admin cannot suspend the current admin session.")
    user = db.get(UserAccount, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found.")
    sync_result = sync_admin_billing_override_to_stripe(
        db,
        user=user,
        requested_override={"suspended": payload.suspended},
        admin_actor=admin,
        request=request,
    )
    _apply_admin_billing_stripe_links(user, sync_result)
    user.is_suspended = payload.suspended
    user.updated_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(user)
    return _admin_user_row(user)


@router.post("/admin/users/{user_id}/send-password-reset", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_send_password_reset(user_id: int, request: Request, db: Session = Depends(get_db)):
    admin = require_admin_user(db, request)
    user = db.get(UserAccount, user_id)
    if not user or _is_deleted_user(user):
        raise HTTPException(status_code=404, detail="User not found.")

    _token, delivery = _issue_password_reset_for_user(db, user)
    delivery_status = str((delivery or {}).get("status") or "failed").lower()
    logger.info(
        "admin_action action=password_reset_requested admin_user_id=%s target_user_id=%s timestamp=%s delivery_status=%s",
        admin.id,
        user.id,
        datetime.now(timezone.utc).isoformat(),
        delivery_status,
    )
    if delivery_status == "failed":
        raise HTTPException(status_code=502, detail="Could not send password reset email.")
    return {"status": "ok"}


@router.delete("/admin/users/{user_id}", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_delete_user(
    user_id: int,
    request: Request,
    db: Session = Depends(get_db),
    delete_stripe_customer: bool = Query(True),
):
    admin = require_admin_user(db, request)
    if admin.id == user_id:
        raise HTTPException(status_code=400, detail="Admin cannot delete the current admin session.")
    user = db.get(UserAccount, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found.")
    previous_state = _admin_billing_previous_state(user)
    cleanup_result: dict[str, Any] = {}
    try:
        cleanup_result = _admin_delete_stripe_cleanup(user, delete_stripe_customer=delete_stripe_customer)
    except HTTPException as exc:
        db.rollback()
        requested_state = {
            "action": "admin_delete_user",
            "stripe_cleanup_status": "failed",
            "safe_error": _admin_billing_safe_error(exc),
        }
        _record_admin_billing_override_audit(
            db,
            admin_actor=admin,
            user=user,
            override_type="delete",
            previous_state=previous_state,
            requested_state=requested_state,
            stripe_customer_id=user.stripe_customer_id,
            stripe_subscription_id=user.stripe_subscription_id,
            stripe_sync_status="failed",
            error_message=_admin_billing_safe_error(exc),
        )
        db.commit()
        raise HTTPException(
            status_code=exc.status_code if exc.status_code in {409, 422, 503} else 502,
            detail={
                "code": "admin_delete_stripe_cleanup_failed",
                "message": "Couldn't clean up Stripe billing for this user. No local deletion was saved.",
            },
        ) from exc

    _record_admin_billing_override_audit(
        db,
        admin_actor=admin,
        user=user,
        override_type="delete",
        previous_state=previous_state,
        requested_state={
            "action": "admin_delete_user",
            "stripe_cleanup_status": cleanup_result.get("cleanup_status"),
            "subscriptions_cancelled_count": len(cleanup_result.get("subscriptions_cancelled") or []),
            "customer_deleted": bool(cleanup_result.get("customer_deleted")),
            "customer_retained": bool(cleanup_result.get("customer_retained")),
        },
        stripe_customer_id=cleanup_result.get("stripe_customer_id") or user.stripe_customer_id,
        stripe_subscription_id=user.stripe_subscription_id,
        stripe_sync_status="succeeded",
    )
    db.delete(user)
    db.commit()
    return {
        "status": "deleted",
        "user_id": user_id,
        "stripe_cleanup": {
            "status": cleanup_result.get("cleanup_status"),
            "subscriptions_cancelled": len(cleanup_result.get("subscriptions_cancelled") or []),
            "customer_deleted": bool(cleanup_result.get("customer_deleted")),
            "customer_retained": bool(cleanup_result.get("customer_retained")),
        },
    }


@router.patch("/admin/feature-gates/{feature_key}", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_update_feature_gate(
    feature_key: str,
    payload: FeatureGatePayload,
    request: Request,
    db: Session = Depends(get_db),
):
    require_admin_user(db, request)
    row = set_feature_gate(db, feature_key=feature_key, required_tier=normalize_tier(payload.required_tier))  # type: ignore[arg-type]
    return {
        "feature_key": row.feature_key,
        "required_tier": row.required_tier,
        "description": row.description,
    }


@router.patch("/admin/plan-limits/{feature_key}", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_update_plan_limit(
    feature_key: str,
    payload: PlanLimitPayload,
    request: Request,
    db: Session = Depends(get_db),
):
    require_admin_user(db, request)
    row = set_plan_limit(
        db,
        feature_key=feature_key,  # type: ignore[arg-type]
        tier=normalize_tier(payload.tier),
        limit_value=payload.limit_value,
    )
    return {
        "feature_key": row.feature_key,
        "tier": row.tier,
        "limit_value": row.limit_value,
    }


@router.patch("/admin/plan-prices/{tier}/{billing_interval}", dependencies=[Depends(rate_limit_admin_mutation)])
def admin_update_plan_price(
    tier: str,
    billing_interval: str,
    payload: PlanPricePayload,
    request: Request,
    db: Session = Depends(get_db),
):
    require_admin_user(db, request)
    normalized_interval = "annual" if billing_interval == "annual" else "monthly"
    row = set_plan_price(
        db,
        tier=normalize_tier(tier),
        billing_interval=normalized_interval,  # type: ignore[arg-type]
        amount_cents=payload.amount_cents,
        currency=payload.currency,
    )
    return {
        "tier": row.tier,
        "billing_interval": row.billing_interval,
        "amount_cents": row.amount_cents,
        "currency": row.currency,
    }
