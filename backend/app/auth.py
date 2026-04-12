from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
from datetime import datetime, timezone
from typing import Any

from fastapi import HTTPException, Request
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models import UserAccount

SESSION_COOKIE_NAME = "ct_session"


def normalize_email(value: str | None) -> str:
    return (value or "").strip().lower()


def admin_emails() -> set[str]:
    raw = os.getenv("ADMIN_EMAILS", "")
    configured = {normalize_email(item) for item in raw.split(",") if normalize_email(item)}
    return configured | {"moore11j@gmail.com"}


def session_secret() -> str:
    return os.getenv("APP_SESSION_SECRET") or os.getenv("ADMIN_TOKEN") or "dev-session-secret"


def _b64_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def _b64_decode(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode((value + padding).encode("ascii"))


def sign_session_payload(payload: dict[str, Any]) -> str:
    body = _b64_encode(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8"))
    signature = hmac.new(session_secret().encode("utf-8"), body.encode("ascii"), hashlib.sha256).digest()
    return f"{body}.{_b64_encode(signature)}"


def verify_session_token(token: str | None) -> dict[str, Any] | None:
    if not token or "." not in token:
        return None
    body, signature = token.rsplit(".", 1)
    expected = hmac.new(session_secret().encode("utf-8"), body.encode("ascii"), hashlib.sha256).digest()
    try:
        supplied = _b64_decode(signature)
    except Exception:
        return None
    if not hmac.compare_digest(expected, supplied):
        return None
    try:
        parsed = json.loads(_b64_decode(body).decode("utf-8"))
    except Exception:
        return None
    return parsed if isinstance(parsed, dict) else None


def request_session_token(request: Request) -> str | None:
    auth = request.headers.get("authorization", "")
    if auth.lower().startswith("bearer "):
        return auth[7:].strip()
    return request.cookies.get(SESSION_COOKIE_NAME)


def get_or_create_user(db: Session, *, email: str, name: str | None = None) -> UserAccount:
    normalized = normalize_email(email)
    if not normalized or "@" not in normalized:
        raise HTTPException(status_code=422, detail="A valid email is required.")

    user = db.execute(select(UserAccount).where(func.lower(UserAccount.email) == normalized)).scalar_one_or_none()
    now = datetime.now(timezone.utc)
    if user:
        if name and name.strip():
            user.name = name.strip()
        user.last_seen_at = now
        return user

    user = UserAccount(
        email=normalized,
        name=name.strip() if name and name.strip() else None,
        role="user",
        entitlement_tier="free",
        last_seen_at=now,
    )
    db.add(user)
    db.flush()
    return user


def current_user(db: Session, request: Request, *, required: bool = False) -> UserAccount | None:
    parsed = verify_session_token(request_session_token(request))
    if not parsed:
        if required:
            raise HTTPException(status_code=401, detail="Sign in required.")
        return None

    user_id = parsed.get("uid")
    user: UserAccount | None = None
    if isinstance(user_id, int):
        user = db.get(UserAccount, user_id)
    if not user and parsed.get("email"):
        email = normalize_email(str(parsed.get("email")))
        user = db.execute(select(UserAccount).where(func.lower(UserAccount.email) == email)).scalar_one_or_none()

    if not user:
        if required:
            raise HTTPException(status_code=401, detail="Account not found.")
        return None
    if user.is_suspended:
        raise HTTPException(status_code=403, detail="Account suspended.")

    user.last_seen_at = datetime.now(timezone.utc)
    return user


def is_admin_user(user: UserAccount | None) -> bool:
    if not user:
        return False
    return user.role == "admin" or normalize_email(user.email) in admin_emails()


def require_admin_user(db: Session, request: Request) -> UserAccount:
    user = current_user(db, request, required=True)
    if not is_admin_user(user):
        raise HTTPException(status_code=403, detail="Admin access required.")
    return user
