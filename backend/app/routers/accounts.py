from __future__ import annotations

import hashlib
import hmac
import json
import os
import secrets
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Literal
from urllib.parse import urlencode

import requests
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field
from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from app.auth import (
    SESSION_COOKIE_NAME,
    admin_emails,
    attach_legacy_watchlists_to_user,
    current_user,
    get_or_create_user,
    hash_password,
    is_admin_user,
    normalize_email,
    require_admin_user,
    reset_token_hash,
    sign_session_payload,
    verify_session_token,
    verify_password,
)
from app.db import get_db
from app.entitlements import (
    DEFAULT_FEATURE_GATES,
    plan_config_payload,
    current_entitlements,
    entitlement_payload,
    feature_gate_payloads,
    normalize_tier,
    set_plan_limit,
    set_plan_price,
    set_feature_gate,
)
from app.models import AppSetting, StripeWebhookEvent, UserAccount

router = APIRouter(tags=["accounts"])


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
    password: str = Field(min_length=8, max_length=240)


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


class NotificationSettingsPayload(BaseModel):
    alerts_enabled: bool
    email_notifications_enabled: bool
    watchlist_activity_notifications: bool
    signals_notifications: bool


class ManualPremiumPayload(BaseModel):
    tier: Literal["free", "premium"] | None = None


class SuspendPayload(BaseModel):
    suspended: bool


class FeatureGatePayload(BaseModel):
    required_tier: Literal["free", "premium"]


class PlanLimitPayload(BaseModel):
    tier: Literal["free", "premium"]
    limit_value: int = Field(ge=0, le=100000)


class PlanPricePayload(BaseModel):
    amount_cents: int = Field(ge=0, le=10000000)
    currency: str = Field(default="USD", min_length=3, max_length=8)


class OAuthSettingsPayload(BaseModel):
    google_client_id: str = Field(default="", max_length=512)


class StripeTaxSettingsPayload(BaseModel):
    automatic_tax_enabled: bool = False
    require_billing_address: bool = True
    product_tax_code: str | None = Field(default=None, max_length=80)
    price_tax_behavior: Literal["unspecified", "exclusive", "inclusive"] = "unspecified"


def _admin_token_matches(value: str | None) -> bool:
    configured = os.getenv("ADMIN_TOKEN", "").strip()
    return bool(configured and value and hmac.compare_digest(configured, value))


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
    return (
        len(value) >= 8
        and any(char.isalpha() for char in value)
        and any(char.isdigit() for char in value)
        and any(not char.isalnum() for char in value)
    )


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


def _public_user(user: UserAccount) -> dict[str, Any]:
    billing_missing = _billing_profile_missing_fields(user)
    return {
        "id": user.id,
        "email": user.email,
        "name": user.name,
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
        "auth_provider": user.auth_provider,
        "avatar_url": user.avatar_url,
        "role": user.role,
        "is_admin": is_admin_user(user),
        "entitlement_tier": user.entitlement_tier,
        "manual_tier_override": user.manual_tier_override,
        "subscription_status": user.subscription_status,
        "subscription_plan": user.subscription_plan,
        "stripe_customer_id": user.stripe_customer_id,
        "stripe_subscription_id": user.stripe_subscription_id,
        "is_suspended": user.is_suspended,
        "created_at": user.created_at,
        "last_seen_at": user.last_seen_at,
        "notifications": _notification_settings(user),
    }


def _stripe_secret_key() -> str | None:
    return os.getenv("STRIPE_SECRET_KEY", "").strip() or None


def _stripe_price_id() -> str | None:
    return os.getenv("STRIPE_PRICE_ID", "").strip() or None


def _stripe_webhook_secret() -> str | None:
    return os.getenv("STRIPE_WEBHOOK_SECRET", "").strip() or None


def _frontend_base_url() -> str:
    return os.getenv("FRONTEND_BASE_URL", "http://localhost:3000").rstrip("/")


def _api_base_url() -> str:
    return os.getenv("PUBLIC_API_BASE_URL", os.getenv("API_BASE", "http://localhost:8000")).rstrip("/")


def _google_client_id(db: Session | None = None) -> str | None:
    if db is not None:
        saved = _setting_value(db, "google_client_id")
        if saved:
            return saved
    return os.getenv("GOOGLE_CLIENT_ID", "").strip() or None


def _google_client_secret() -> str | None:
    return os.getenv("GOOGLE_CLIENT_SECRET", "").strip() or None


def _google_redirect_uri() -> str:
    return os.getenv("GOOGLE_REDIRECT_URI", f"{_frontend_base_url()}/auth/google/callback").strip()


def _stripe_post(path: str, data: dict[str, Any]) -> dict[str, Any]:
    secret = _stripe_secret_key()
    if not secret:
        raise HTTPException(status_code=503, detail="Stripe secret key is not configured.")
    response = requests.post(
        f"https://api.stripe.com/v1/{path.lstrip('/')}",
        auth=(secret, ""),
        data=data,
        timeout=20,
    )
    if response.status_code >= 400:
        raise HTTPException(status_code=502, detail=f"Stripe request failed: {response.text[:500]}")
    parsed = response.json()
    return parsed if isinstance(parsed, dict) else {}


def _stripe_config_status() -> dict[str, Any]:
    secret = _stripe_secret_key()
    price = _stripe_price_id()
    webhook = _stripe_webhook_secret()
    return {
        "configured": bool(secret and price and webhook),
        "secret_key": "configured" if secret else "missing",
        "price_id": price or "missing",
        "webhook_secret": "configured" if webhook else "missing",
        "success_url": f"{_frontend_base_url()}/account/billing?checkout=success",
        "cancel_url": f"{_frontend_base_url()}/account/billing?checkout=cancelled",
        "webhook_url": f"{_api_base_url()}/api/billing/stripe/webhook",
        "notes": "Secrets are read from environment variables: STRIPE_SECRET_KEY, STRIPE_PRICE_ID, STRIPE_WEBHOOK_SECRET.",
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
        if not str(location.get("country") or "").strip():
            missing_fields.append("country")
        if not str(location.get("postal_code") or "").strip():
            missing_fields.append("postal_code")
        if not str(location.get("city") or "").strip():
            missing_fields.append("city")
        if not str(location.get("address_line1") or "").strip():
            missing_fields.append("address_line1")
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
    price = _stripe_price_id()
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
            f"Using {price}." if price else "Set STRIPE_PRICE_ID for the subscription price.",
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
        "stripe_tax_status": "ready_in_app" if secret and price and settings["automatic_tax_enabled"] else "not_ready",
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


def _auth_response_for_user(db: Session, user: UserAccount) -> dict[str, Any]:
    token = sign_session_payload({"uid": user.id, "email": user.email})
    return {
        "token": token,
        "user": _public_user(user),
        "entitlements": entitlement_payload(current_entitlements(_request_from_token(token), db), user=user),
    }


@router.post("/auth/login")
def login(payload: LoginPayload, db: Session = Depends(get_db)):
    email = normalize_email(payload.email)
    wants_admin = email in admin_emails()
    existing = db.execute(select(UserAccount).where(func.lower(UserAccount.email) == email)).scalar_one_or_none()
    existing_is_admin = is_admin_user(existing)
    admin_token_valid = _admin_token_matches(payload.admin_token)
    if (wants_admin or existing_is_admin) and not (admin_token_valid or verify_password(payload.password, existing.password_hash if existing else None)):
        raise HTTPException(status_code=401, detail="Admin token required for this account.")

    if existing and existing.password_hash and not (admin_token_valid or verify_password(payload.password, existing.password_hash)):
        raise HTTPException(status_code=401, detail="Invalid email or password.")
    if existing and not existing.password_hash and not (admin_token_valid or wants_admin or existing_is_admin):
        raise HTTPException(status_code=401, detail="Set a password with the reset flow before signing in.")
    if not existing and not (admin_token_valid or wants_admin):
        raise HTTPException(status_code=401, detail="No account exists for this email. Register first.")

    user = get_or_create_user(db, email=email, name=payload.name)
    if payload.name and not (user.first_name or user.last_name):
        user.first_name, user.last_name = _split_name(payload.name)
    if wants_admin:
        user.role = "admin"
    user.last_seen_at = datetime.now(timezone.utc)
    attach_legacy_watchlists_to_user(db, user)
    db.commit()
    db.refresh(user)

    return _auth_response_for_user(db, user)


@router.post("/auth/register")
def register(payload: RegisterPayload, db: Session = Depends(get_db)):
    email = normalize_email(payload.email)
    existing = db.execute(select(UserAccount).where(func.lower(UserAccount.email) == email)).scalar_one_or_none()
    if existing and existing.password_hash:
        raise HTTPException(status_code=409, detail="An account already exists for this email.")

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
    if missing:
        raise HTTPException(status_code=422, detail=f"{', '.join(missing)} required.")

    user = existing or get_or_create_user(db, email=email, name=payload.name or _display_name(payload.first_name, payload.last_name))
    _set_billing_profile(user, **cleaned_registration)
    user.password_hash = hash_password(payload.password)
    user.auth_provider = user.auth_provider or "email"
    if email in admin_emails():
        user.role = "admin"
    user.password_reset_token_hash = None
    user.password_reset_expires_at = None
    user.last_seen_at = datetime.now(timezone.utc)
    attach_legacy_watchlists_to_user(db, user)
    db.commit()
    db.refresh(user)
    return _auth_response_for_user(db, user)


@router.post("/auth/password-reset/request")
def request_password_reset(payload: PasswordResetRequestPayload, db: Session = Depends(get_db)):
    email = normalize_email(payload.email)
    user = db.execute(select(UserAccount).where(func.lower(UserAccount.email) == email)).scalar_one_or_none()
    response: dict[str, Any] = {
        "status": "ok",
        "message": "If an account exists, a reset link is ready.",
    }
    if not user:
        return response

    token = secrets.token_urlsafe(32)
    user.password_reset_token_hash = reset_token_hash(token)
    user.password_reset_expires_at = datetime.now(timezone.utc) + timedelta(minutes=30)
    db.commit()
    reset_path = f"/reset-password?token={token}"
    response["reset_path"] = reset_path
    return response


@router.post("/auth/password-reset/confirm")
def confirm_password_reset(payload: PasswordResetConfirmPayload, db: Session = Depends(get_db)):
    token_hash = reset_token_hash(payload.token)
    user = db.execute(
        select(UserAccount).where(UserAccount.password_reset_token_hash == token_hash)
    ).scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=400, detail="Invalid or expired reset link.")
    expires_at = user.password_reset_expires_at
    if expires_at and expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)
    if not expires_at or expires_at < datetime.now(timezone.utc):
        raise HTTPException(status_code=400, detail="Invalid or expired reset link.")

    user.password_hash = hash_password(payload.password)
    user.password_reset_token_hash = None
    user.password_reset_expires_at = None
    user.last_seen_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(user)
    return _auth_response_for_user(db, user)


def _request_from_token(token: str) -> Request:
    return Request({"type": "http", "method": "GET", "path": "/", "headers": [(b"authorization", f"Bearer {token}".encode("utf-8"))]})


@router.get("/auth/google/start")
def google_auth_start(return_to: str | None = None, db: Session = Depends(get_db)):
    client_id = _google_client_id(db)
    if not client_id:
        raise HTTPException(status_code=503, detail="Google OAuth is not configured.")
    state = sign_session_payload(
        {
            "kind": "google_oauth_state",
            "return_to": return_to or "/account/billing",
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


def _verify_google_claims(db: Session, claims: dict[str, Any]) -> dict[str, Any]:
    client_id = _google_client_id(db)
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


def upsert_google_user(db: Session, claims: dict[str, Any]) -> UserAccount:
    claims = _verify_google_claims(db, claims)
    email = normalize_email(str(claims.get("email")))
    sub = str(claims.get("sub"))
    name = str(claims.get("name") or "").strip() or None
    picture = str(claims.get("picture") or "").strip() or None

    user = db.execute(select(UserAccount).where(UserAccount.google_sub == sub)).scalar_one_or_none()
    if not user:
        user = db.execute(select(UserAccount).where(func.lower(UserAccount.email) == email)).scalar_one_or_none()
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
    if email in admin_emails():
        user.role = "admin"
    user.last_seen_at = datetime.now(timezone.utc)
    db.flush()
    return user


@router.post("/auth/google/callback")
def google_auth_callback(payload: GoogleCallbackPayload, db: Session = Depends(get_db)):
    parsed_state = verify_session_token(payload.state)
    if (
        not parsed_state
        or parsed_state.get("kind") != "google_oauth_state"
        or int(parsed_state.get("exp") or 0) < int(time.time())
    ):
        raise HTTPException(status_code=401, detail="Invalid Google sign-in state.")
    client_id = _google_client_id(db)
    client_secret = _google_client_secret()
    if not client_id or not client_secret:
        raise HTTPException(status_code=503, detail="Google OAuth is not configured.")

    response = requests.post(
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
    if response.status_code >= 400:
        raise HTTPException(status_code=401, detail=f"Google token exchange failed: {response.text[:300]}")
    token_payload = response.json()
    id_token = token_payload.get("id_token") if isinstance(token_payload, dict) else None
    if not isinstance(id_token, str):
        raise HTTPException(status_code=401, detail="Google did not return an identity token.")
    user = upsert_google_user(db, _decode_jwt_payload(id_token))
    db.commit()
    db.refresh(user)
    auth = _auth_response_for_user(db, user)
    auth["return_to"] = parsed_state.get("return_to") or "/account/billing"
    return auth


@router.get("/auth/me")
def me(request: Request, db: Session = Depends(get_db)):
    user = current_user(db, request, required=False)
    return {
        "user": _public_user(user) if user else None,
        "entitlements": entitlement_payload(current_entitlements(request, db), user=user),
    }


@router.get("/account/settings")
def account_settings(request: Request, db: Session = Depends(get_db)):
    user = current_user(db, request, required=True)
    return {
        "user": _public_user(user),
        "notifications": _notification_settings(user),
    }


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
        if missing:
            raise HTTPException(status_code=422, detail=f"{', '.join(missing)} required.")

    _set_billing_profile(user, **next_values)
    user.updated_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(user)
    return _public_user(user)


@router.patch("/account/password")
def update_account_password(payload: PasswordChangePayload, request: Request, db: Session = Depends(get_db)):
    user = current_user(db, request, required=True)
    if not verify_password(payload.current_password, user.password_hash):
        raise HTTPException(status_code=401, detail="Current password is incorrect.")
    if payload.new_password != payload.confirm_password:
        raise HTTPException(status_code=422, detail="Confirm password must match the new password.")
    if not _password_meets_account_rules(payload.new_password):
        raise HTTPException(
            status_code=422,
            detail="New password must include at least one letter, one number, and one special character.",
        )
    user.password_hash = hash_password(payload.new_password)
    user.password_reset_token_hash = None
    user.password_reset_expires_at = None
    user.updated_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(user)
    return {"status": "ok"}


@router.patch("/account/notifications")
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


@router.post("/auth/logout")
def logout():
    return {"status": "ok", "clear_cookie": SESSION_COOKIE_NAME}


@router.post("/billing/checkout-session")
def create_checkout_session(request: Request, db: Session = Depends(get_db)):
    user = current_user(db, request, required=True)
    if not _stripe_price_id():
        raise HTTPException(status_code=503, detail="Stripe price id is not configured.")

    data: dict[str, Any] = {
        "mode": "subscription",
        "line_items[0][price]": _stripe_price_id(),
        "line_items[0][quantity]": 1,
        "success_url": f"{_frontend_base_url()}/account/billing?checkout=success",
        "cancel_url": f"{_frontend_base_url()}/account/billing?checkout=cancelled",
        "metadata[user_id]": user.id,
        "metadata[email]": user.email,
        "subscription_data[metadata][user_id]": user.id,
        "subscription_data[metadata][email]": user.email,
    }
    if user.stripe_customer_id:
        data["customer"] = user.stripe_customer_id
    else:
        data["customer_email"] = user.email

    session = _stripe_post("checkout/sessions", data)
    return {"id": session.get("id"), "url": session.get("url")}


@router.post("/billing/customer-portal")
def create_customer_portal_session(request: Request, db: Session = Depends(get_db)):
    user = current_user(db, request, required=True)
    if not user.stripe_customer_id:
        raise HTTPException(status_code=404, detail="No Stripe customer is linked to this account.")
    session = _stripe_post(
        "billing_portal/sessions",
        {"customer": user.stripe_customer_id, "return_url": f"{_frontend_base_url()}/account/billing"},
    )
    return {"url": session.get("url")}


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

    signed_payload = f"{timestamp}.{payload.decode('utf-8')}".encode("utf-8")
    expected = hmac.new(secret.encode("utf-8"), signed_payload, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, signature):
        raise HTTPException(status_code=400, detail="Invalid Stripe signature.")


def _find_user_for_stripe_object(db: Session, obj: dict[str, Any]) -> UserAccount | None:
    metadata = obj.get("metadata") if isinstance(obj.get("metadata"), dict) else {}
    user_id = metadata.get("user_id")
    if user_id:
        try:
            user = db.get(UserAccount, int(user_id))
            if user:
                return user
        except (TypeError, ValueError):
            pass

    customer = obj.get("customer")
    subscription = obj.get("subscription") or obj.get("id")
    email = normalize_email(metadata.get("email") or obj.get("customer_email"))
    query = select(UserAccount)
    conditions = []
    if customer:
        conditions.append(UserAccount.stripe_customer_id == str(customer))
    if subscription:
        conditions.append(UserAccount.stripe_subscription_id == str(subscription))
    if email:
        conditions.append(func.lower(UserAccount.email) == email)
    if not conditions:
        return None
    return db.execute(query.where(or_(*conditions))).scalar_one_or_none()


def _sync_user_subscription(
    db: Session,
    *,
    obj: dict[str, Any],
    status: str,
    tier: Literal["free", "premium"],
) -> UserAccount | None:
    user = _find_user_for_stripe_object(db, obj)
    if not user:
        metadata = obj.get("metadata") if isinstance(obj.get("metadata"), dict) else {}
        email = normalize_email(metadata.get("email") or obj.get("customer_email"))
        if email:
            user = get_or_create_user(db, email=email)
    if not user:
        return None

    customer = obj.get("customer")
    subscription = obj.get("subscription") or (obj.get("id") if str(obj.get("object")) == "subscription" else None)
    if customer:
        user.stripe_customer_id = str(customer)
    if subscription:
        user.stripe_subscription_id = str(subscription)
    user.subscription_status = status
    user.subscription_plan = "premium"
    user.entitlement_tier = tier
    user.updated_at = datetime.now(timezone.utc)
    db.flush()
    return user


def process_stripe_event(db: Session, event: dict[str, Any]) -> dict[str, Any]:
    event_id = str(event.get("id") or "")
    event_type = str(event.get("type") or "")
    if event_id and db.get(StripeWebhookEvent, event_id):
        return {"status": "already_processed", "event_type": event_type}

    obj = (event.get("data") or {}).get("object") if isinstance(event.get("data"), dict) else {}
    if not isinstance(obj, dict):
        obj = {}

    handled = True
    if event_type == "checkout.session.completed":
        _sync_user_subscription(db, obj=obj, status="active", tier="premium")
    elif event_type == "invoice.paid":
        _sync_user_subscription(db, obj=obj, status="active", tier="premium")
    elif event_type == "invoice.payment_failed":
        _sync_user_subscription(db, obj=obj, status="payment_failed", tier="free")
    elif event_type == "customer.subscription.updated":
        status = str(obj.get("status") or "unknown")
        tier = "premium" if status in {"active", "trialing"} else "free"
        _sync_user_subscription(db, obj=obj, status=status, tier=tier)
    elif event_type == "customer.subscription.deleted":
        _sync_user_subscription(db, obj=obj, status="canceled", tier="free")
    else:
        handled = False

    if event_id:
        db.add(
            StripeWebhookEvent(
                event_id=event_id,
                event_type=event_type,
                payload_json=json.dumps(event, sort_keys=True),
            )
        )
    db.commit()
    return {"status": "processed" if handled else "ignored", "event_type": event_type}


@router.post("/billing/stripe/webhook")
async def stripe_webhook(request: Request, db: Session = Depends(get_db)):
    payload = await request.body()
    _verify_stripe_signature(payload, request.headers.get("stripe-signature"))
    try:
        event = json.loads(payload.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail="Invalid webhook payload.") from exc
    return process_stripe_event(db, event)


@router.get("/admin/settings")
def admin_settings(request: Request, db: Session = Depends(get_db)):
    require_admin_user(db, request)
    users = db.execute(select(UserAccount).order_by(UserAccount.created_at.desc(), UserAccount.id.desc())).scalars().all()
    return {
        "stripe": _stripe_config_status(),
        "stripe_tax": _stripe_tax_config(db),
        "oauth": {"google_client_id": _google_client_id(db) or ""},
        "users": [_public_user(user) for user in users],
        "feature_gates": feature_gate_payloads(db),
        "features": DEFAULT_FEATURE_GATES,
        "plan_config": plan_config_payload(db),
    }


@router.get("/plan-config")
def public_plan_config(db: Session = Depends(get_db)):
    return plan_config_payload(db)


@router.patch("/admin/settings/oauth")
def admin_update_oauth_settings(
    payload: OAuthSettingsPayload,
    request: Request,
    db: Session = Depends(get_db),
):
    require_admin_user(db, request)
    row = _set_setting(db, "google_client_id", payload.google_client_id)
    db.commit()
    return {"google_client_id": row.value or ""}


@router.patch("/admin/settings/stripe-tax")
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


@router.post("/admin/users/{user_id}/premium")
def admin_set_premium(user_id: int, payload: ManualPremiumPayload, request: Request, db: Session = Depends(get_db)):
    require_admin_user(db, request)
    user = db.get(UserAccount, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found.")
    user.manual_tier_override = payload.tier
    if payload.tier:
        user.entitlement_tier = payload.tier
    db.commit()
    db.refresh(user)
    return _public_user(user)


@router.post("/admin/users/{user_id}/suspend")
def admin_suspend_user(user_id: int, payload: SuspendPayload, request: Request, db: Session = Depends(get_db)):
    admin = require_admin_user(db, request)
    if admin.id == user_id and payload.suspended:
        raise HTTPException(status_code=400, detail="Admin cannot suspend the current admin session.")
    user = db.get(UserAccount, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found.")
    user.is_suspended = payload.suspended
    db.commit()
    db.refresh(user)
    return _public_user(user)


@router.delete("/admin/users/{user_id}")
def admin_delete_user(user_id: int, request: Request, db: Session = Depends(get_db)):
    admin = require_admin_user(db, request)
    if admin.id == user_id:
        raise HTTPException(status_code=400, detail="Admin cannot delete the current admin session.")
    user = db.get(UserAccount, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found.")
    db.delete(user)
    db.commit()
    return {"status": "deleted", "user_id": user_id}


@router.patch("/admin/feature-gates/{feature_key}")
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


@router.patch("/admin/plan-limits/{feature_key}")
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


@router.patch("/admin/plan-prices/{tier}/{billing_interval}")
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
