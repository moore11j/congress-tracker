from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import secrets
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import HTTPException, Request, Response
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models import UserAccount, Watchlist

SESSION_COOKIE_NAME = "ct_session"
SESSION_TTL_SECONDS = 60 * 60 * 24 * 30
MIN_PRODUCTION_SESSION_SECRET_LENGTH = 32
_TRUE_VALUES = {"1", "true", "yes", "on", "enabled"}
_NONPRODUCTION_ENVS = {"local", "dev", "development", "test", "testing", "ci"}
_CURRENT_USER_CACHE_ATTR = "_walnut_current_user"


def normalize_email(value: str | None) -> str:
    return (value or "").strip().lower()


def admin_emails() -> set[str]:
    raw = os.getenv("ADMIN_EMAILS", "")
    return {normalize_email(item) for item in raw.split(",") if normalize_email(item)}


def legacy_watchlist_owner_email() -> str:
    return normalize_email(os.getenv("LEGACY_WATCHLIST_OWNER_EMAIL", "moore11j@gmail.com"))


def _runtime_environment() -> str:
    return (os.getenv("APP_ENV") or os.getenv("ENV") or os.getenv("NODE_ENV") or "").strip().lower()


def _is_production_runtime() -> bool:
    runtime = _runtime_environment()
    if runtime in {"prod", "production"}:
        return True
    if runtime in _NONPRODUCTION_ENVS:
        return False
    return bool(os.getenv("FLY_APP_NAME", "").strip())


def _is_truthy_env(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in _TRUE_VALUES


def _configured_session_secret() -> str:
    return os.getenv("APP_SESSION_SECRET", "").strip()


def validate_session_secret_config() -> None:
    secret = _configured_session_secret()
    if not _is_production_runtime():
        return
    if not secret:
        raise RuntimeError("APP_SESSION_SECRET is required in production.")
    if len(secret) < MIN_PRODUCTION_SESSION_SECRET_LENGTH:
        raise RuntimeError(
            f"APP_SESSION_SECRET must be at least {MIN_PRODUCTION_SESSION_SECRET_LENGTH} characters in production."
        )
    if secret == "dev-session-secret":
        raise RuntimeError("APP_SESSION_SECRET must not use the development default in production.")
    admin_token = os.getenv("ADMIN_TOKEN", "").strip()
    if admin_token and hmac.compare_digest(secret, admin_token):
        raise RuntimeError("APP_SESSION_SECRET must not reuse ADMIN_TOKEN in production.")


def session_secret() -> str:
    validate_session_secret_config()
    return _configured_session_secret() or "dev-session-secret"


def session_ttl_seconds() -> int:
    raw = os.getenv("APP_SESSION_TTL_SECONDS", "").strip()
    if raw:
        try:
            ttl = int(raw)
        except ValueError:
            ttl = SESSION_TTL_SECONDS
        if ttl > 0:
            return ttl
    return SESSION_TTL_SECONDS


def session_cookie_secure() -> bool:
    return _is_production_runtime()


def session_cookie_samesite() -> str:
    default = "none" if _is_production_runtime() else "lax"
    configured = os.getenv("APP_SESSION_COOKIE_SAMESITE", default).strip().lower()
    return configured if configured in {"lax", "strict", "none"} else "lax"


def session_cookie_domain() -> str | None:
    configured = os.getenv("APP_SESSION_COOKIE_DOMAIN", "").strip().lower()
    return configured or None


def allow_bearer_session_auth() -> bool:
    """Legacy session-token bearer auth is disabled by default and never normal production auth."""
    if _is_production_runtime():
        return False
    return _is_truthy_env("APP_ALLOW_BEARER_SESSION_AUTH")


def _now_ts() -> int:
    return int(datetime.now(timezone.utc).timestamp())


def _b64_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def _b64_decode(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode((value + padding).encode("ascii"))


def sign_session_payload(payload: dict[str, Any]) -> str:
    payload = dict(payload)
    now = _now_ts()
    payload.setdefault("iat", now)
    payload.setdefault("exp", now + session_ttl_seconds())
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
    if not isinstance(parsed, dict):
        return None
    try:
        expires_at = int(parsed.get("exp") or 0)
    except (TypeError, ValueError):
        return None
    if expires_at <= _now_ts():
        return None
    return parsed


def set_session_cookie(response: Response | None, token: str) -> None:
    if response is None:
        return
    max_age = session_ttl_seconds()
    response.set_cookie(
        key=SESSION_COOKIE_NAME,
        value=token,
        max_age=max_age,
        expires=datetime.now(timezone.utc) + timedelta(seconds=max_age),
        path="/",
        domain=session_cookie_domain(),
        secure=session_cookie_secure(),
        httponly=True,
        samesite=session_cookie_samesite(),
    )


def clear_session_cookie(response: Response | None) -> None:
    if response is None:
        return
    response.delete_cookie(
        key=SESSION_COOKIE_NAME,
        path="/",
        domain=session_cookie_domain(),
        secure=session_cookie_secure(),
        httponly=True,
        samesite=session_cookie_samesite(),
    )


def hash_password(password: str) -> str:
    value = password or ""
    if len(value) < 8:
        raise HTTPException(status_code=422, detail="Password must be at least 8 characters.")
    salt = secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", value.encode("utf-8"), salt, 210_000)
    return f"pbkdf2_sha256$210000${_b64_encode(salt)}${_b64_encode(digest)}"


def verify_password(password: str | None, encoded: str | None) -> bool:
    if not password or not encoded:
        return False
    try:
        scheme, iterations_raw, salt_raw, digest_raw = encoded.split("$", 3)
        if scheme != "pbkdf2_sha256":
            return False
        iterations = int(iterations_raw)
        salt = _b64_decode(salt_raw)
        expected = _b64_decode(digest_raw)
    except Exception:
        return False
    supplied = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    return hmac.compare_digest(expected, supplied)


def reset_token_hash(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def attach_legacy_watchlists_to_user(db: Session, user: UserAccount) -> int:
    """Attach pre-auth watchlists to the configured owner account once it exists."""
    if normalize_email(user.email) != legacy_watchlist_owner_email():
        return 0
    rows = db.execute(select(Watchlist).where(Watchlist.owner_user_id.is_(None))).scalars().all()
    for watchlist in rows:
        watchlist.owner_user_id = user.id
    if rows:
        db.flush()
    return len(rows)


def request_session_token(request: Request) -> str | None:
    cookie_token = request.cookies.get(SESSION_COOKIE_NAME)
    if cookie_token:
        return cookie_token
    auth = request.headers.get("authorization", "")
    if allow_bearer_session_auth() and auth.lower().startswith("bearer "):
        return auth[7:].strip()
    return None


def _request_cached_user(request: Request) -> tuple[bool, UserAccount | None]:
    state = getattr(request, "state", None)
    if state is None or not hasattr(state, _CURRENT_USER_CACHE_ATTR):
        return False, None
    return True, getattr(state, _CURRENT_USER_CACHE_ATTR)


def _set_request_cached_user(request: Request, user: UserAccount | None) -> None:
    state = getattr(request, "state", None)
    if state is not None:
        setattr(state, _CURRENT_USER_CACHE_ATTR, user)


def get_or_create_user(db: Session, *, email: str, name: str | None = None) -> UserAccount:
    normalized = normalize_email(email)
    if not normalized or "@" not in normalized:
        raise HTTPException(status_code=422, detail="A valid email is required.")

    user = db.execute(select(UserAccount).where(func.lower(UserAccount.email) == normalized)).scalar_one_or_none()
    now = datetime.now(timezone.utc)
    if user:
        if user.deleted_at is not None:
            raise HTTPException(status_code=403, detail="This account has been deleted. Please create a new account or contact support.")
        if name and name.strip():
            user.name = name.strip()
        user.last_seen_at = now
        attach_legacy_watchlists_to_user(db, user)
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
    attach_legacy_watchlists_to_user(db, user)
    return user


def current_user(db: Session, request: Request, *, required: bool = False) -> UserAccount | None:
    cached, cached_user = _request_cached_user(request)
    if cached:
        if cached_user is None and required:
            raise HTTPException(status_code=401, detail="Sign in required.")
        return cached_user

    parsed = verify_session_token(request_session_token(request))
    if not parsed:
        _set_request_cached_user(request, None)
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
        _set_request_cached_user(request, None)
        if required:
            raise HTTPException(status_code=401, detail="Account not found.")
        return None
    if user.deleted_at is not None:
        raise HTTPException(status_code=403, detail="This account has been deleted. Please reactivate it or create a new account.")
    if user.is_suspended:
        raise HTTPException(status_code=403, detail="Account suspended.")

    _set_request_cached_user(request, user)
    return user


def is_admin_user(user: UserAccount | None) -> bool:
    if not user:
        return False
    return user.role == "admin"


def require_admin_user(db: Session, request: Request) -> UserAccount:
    user = current_user(db, request, required=True)
    if not is_admin_user(user):
        raise HTTPException(status_code=403, detail="Admin access required.")
    return user
