from __future__ import annotations

import hashlib
import hmac
import json
import os
import time
from datetime import datetime, timezone
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
    current_user,
    get_or_create_user,
    is_admin_user,
    normalize_email,
    require_admin_user,
    sign_session_payload,
    verify_session_token,
)
from app.db import get_db
from app.entitlements import (
    DEFAULT_FEATURE_GATES,
    current_entitlements,
    entitlement_payload,
    feature_gate_payloads,
    normalize_tier,
    set_feature_gate,
)
from app.models import StripeWebhookEvent, UserAccount

router = APIRouter(tags=["accounts"])


class LoginPayload(BaseModel):
    email: str = Field(min_length=3, max_length=320)
    name: str | None = Field(default=None, max_length=160)
    admin_token: str | None = Field(default=None, max_length=240)


class GoogleCallbackPayload(BaseModel):
    code: str = Field(min_length=1)
    state: str = Field(min_length=1)
    redirect_uri: str | None = None


class ManualPremiumPayload(BaseModel):
    tier: Literal["free", "premium"] | None = None


class SuspendPayload(BaseModel):
    suspended: bool


class FeatureGatePayload(BaseModel):
    required_tier: Literal["free", "premium"]


def _admin_token_matches(value: str | None) -> bool:
    configured = os.getenv("ADMIN_TOKEN", "").strip()
    return bool(configured and value and hmac.compare_digest(configured, value))


def _public_user(user: UserAccount) -> dict[str, Any]:
    return {
        "id": user.id,
        "email": user.email,
        "name": user.name,
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


def _google_client_id() -> str | None:
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
    if (wants_admin or existing_is_admin) and not _admin_token_matches(payload.admin_token):
        raise HTTPException(status_code=401, detail="Admin token required for this account.")

    user = get_or_create_user(db, email=email, name=payload.name)
    if wants_admin:
        user.role = "admin"
    user.last_seen_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(user)

    return _auth_response_for_user(db, user)


def _request_from_token(token: str) -> Request:
    return Request({"type": "http", "method": "GET", "path": "/", "headers": [(b"authorization", f"Bearer {token}".encode("utf-8"))]})


@router.get("/auth/google/start")
def google_auth_start(return_to: str | None = None):
    client_id = _google_client_id()
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


def _verify_google_claims(claims: dict[str, Any]) -> dict[str, Any]:
    client_id = _google_client_id()
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
    claims = _verify_google_claims(claims)
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
    client_id = _google_client_id()
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
        "users": [_public_user(user) for user in users],
        "feature_gates": feature_gate_payloads(db),
        "features": DEFAULT_FEATURE_GATES,
    }


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
