from __future__ import annotations

import hmac
import logging
import os
from urllib.parse import urlparse

from app.services.billing_readiness import (
    STRIPE_BILLING_ENABLE_FLAGS,
    billing_readiness,
    log_billing_readiness,
)

logger = logging.getLogger(__name__)

MIN_PRODUCTION_SESSION_SECRET_LENGTH = 32

DEFAULT_PRODUCTION_FRONTEND_ORIGINS = (
    "https://app.walnutmarkets.com",
    "https://walnutmarkets.com",
    "https://www.walnutmarkets.com",
    "https://app.walnut-intel.com",
    "https://walnut-intel.com",
    "https://www.walnut-intel.com",
    "https://congress-tracker-two.vercel.app",
)
DEFAULT_LOCAL_FRONTEND_ORIGINS = (
    "http://localhost:3000",
    "http://localhost:3001",
    "http://127.0.0.1:3000",
    "http://127.0.0.1:3001",
)

_TRUE_VALUES = {"1", "true", "yes", "on", "enabled"}
_PRODUCTION_ENVS = {"prod", "production"}
_NONPRODUCTION_ENVS = {"local", "dev", "development", "test", "testing", "ci"}
_KNOWN_COOKIE_SITE_SUFFIXES = (
    ".walnutmarkets.com",
    ".walnut-intel.com",
)


class StartupSecurityError(RuntimeError):
    """Raised when production security configuration is unsafe."""


def _env(name: str) -> str:
    return os.getenv(name, "").strip()


def _env_lower(name: str) -> str:
    return _env(name).lower()


def _is_truthy(name: str) -> bool:
    return _env_lower(name) in _TRUE_VALUES


def _origin_host(origin: str) -> str:
    parsed = urlparse(origin)
    return (parsed.hostname or "").strip().lower()


def _cookie_site_key(origin: str) -> str:
    parsed = urlparse(origin)
    scheme = (parsed.scheme or "https").lower()
    host = (parsed.hostname or "").strip().lower()
    for suffix in _KNOWN_COOKIE_SITE_SUFFIXES:
        bare = suffix.lstrip(".")
        if host == bare or host.endswith(suffix):
            return f"{scheme}://{bare}"
    return f"{scheme}://{host}"


def backend_public_origin() -> str:
    for name in ("PUBLIC_API_URL", "API_BASE_URL", "BACKEND_PUBLIC_URL", "APP_API_BASE_URL"):
        raw = _env(name)
        if raw:
            parsed = urlparse(raw)
            if parsed.scheme and parsed.netloc:
                return f"{parsed.scheme.lower()}://{parsed.netloc.lower()}".rstrip("/")
    fly_app = _env("FLY_APP_NAME")
    if fly_app:
        return f"https://{fly_app}.fly.dev"
    return "https://congress-tracker-api.fly.dev"


def frontend_backend_are_cross_site() -> bool:
    backend_site = _cookie_site_key(backend_public_origin())
    origins = cors_allowed_origins()
    return any(_origin_host(origin) and _cookie_site_key(origin) != backend_site for origin in origins)


def stripe_billing_readiness() -> dict[str, object]:
    readiness = billing_readiness()
    explicit_enabled = any(_is_truthy(name) for name in STRIPE_BILLING_ENABLE_FLAGS)
    return {
        **readiness,
        "configured": readiness["overall"]["ready"],
        "explicit_enabled": explicit_enabled,
        "billing_enabled": bool(explicit_enabled or readiness["billing_enabled"]),
    }


def stripe_webhook_enabled() -> bool:
    return bool(billing_readiness()["webhooks"]["ready"])


def log_stripe_billing_readiness(*, context: str) -> dict[str, object]:
    readiness = stripe_billing_readiness()
    log_billing_readiness(logger, context=context, readiness=readiness)
    return readiness


def runtime_environment() -> str:
    return (_env("APP_ENV") or _env("ENV") or _env("NODE_ENV")).lower()


def is_production() -> bool:
    runtime = runtime_environment()
    if runtime in _PRODUCTION_ENVS:
        return True
    if runtime in _NONPRODUCTION_ENVS:
        return False
    if _env("FLY_APP_NAME"):
        return True
    return False


def split_origins(raw: str | None) -> list[str]:
    origins: list[str] = []
    for item in (raw or "").split(","):
        origin = item.strip().rstrip("/")
        if origin and origin not in origins:
            origins.append(origin)
    return origins


def configured_cors_origins() -> list[str]:
    return [
        *split_origins(os.getenv("FRONTEND_ORIGINS")),
        *split_origins(os.getenv("CORS_ALLOW_ORIGINS")),
        *split_origins(os.getenv("FRONTEND_URL")),
    ]


def cors_allowed_origins() -> list[str]:
    configured = configured_cors_origins()
    origins: list[str] = []
    for origin in configured:
        if origin == "*":
            logger.warning(
                "cors_wildcard_origin_ignored environment=%s",
                "production" if is_production() else "nonproduction",
            )
            continue
        if origin not in origins:
            origins.append(origin)

    for origin in DEFAULT_PRODUCTION_FRONTEND_ORIGINS:
        if origin not in origins:
            origins.append(origin)

    if not is_production():
        for origin in DEFAULT_LOCAL_FRONTEND_ORIGINS:
            if origin not in origins:
                origins.append(origin)

    if is_production() and not configured:
        logger.warning("cors_origins_using_default_production_allowlist")
    return origins


def billing_enabled() -> bool:
    readiness = stripe_billing_readiness()
    return bool(readiness["billing_enabled"])


def validate_session_secret_config() -> None:
    if not is_production():
        return

    secret = _env("APP_SESSION_SECRET")
    if not secret:
        raise StartupSecurityError("APP_SESSION_SECRET is required in production.")
    if len(secret) < MIN_PRODUCTION_SESSION_SECRET_LENGTH:
        raise StartupSecurityError(
            f"APP_SESSION_SECRET must be at least {MIN_PRODUCTION_SESSION_SECRET_LENGTH} characters in production."
        )
    if secret == "dev-session-secret":
        raise StartupSecurityError("APP_SESSION_SECRET must not use the development default in production.")

    admin_token = _env("ADMIN_TOKEN")
    if admin_token and hmac.compare_digest(secret, admin_token):
        raise StartupSecurityError("APP_SESSION_SECRET must not reuse ADMIN_TOKEN in production.")


def validate_session_cookie_config() -> None:
    if not is_production():
        return

    samesite = _env_lower("APP_SESSION_COOKIE_SAMESITE") or "lax"
    if frontend_backend_are_cross_site() and samesite != "none":
        raise StartupSecurityError(
            "APP_SESSION_COOKIE_SAMESITE=none is required in production when the frontend and backend are cross-site."
        )


def validate_bearer_session_auth_config() -> None:
    if is_production() and _is_truthy("APP_ALLOW_BEARER_SESSION_AUTH"):
        raise StartupSecurityError("APP_ALLOW_BEARER_SESSION_AUTH must be disabled in production.")


def validate_cors_config() -> None:
    if not is_production():
        return

    configured = configured_cors_origins()
    if "*" in configured:
        raise StartupSecurityError("CORS wildcard origins are not allowed in production.")

    origins = cors_allowed_origins()
    if "*" in origins:
        raise StartupSecurityError("Credentialed CORS must use explicit origins in production.")

    missing_defaults = [origin for origin in DEFAULT_PRODUCTION_FRONTEND_ORIGINS if origin not in origins]
    if missing_defaults:
        raise StartupSecurityError("Expected production frontend origin is missing from CORS allowlist.")


def validate_entitlement_header_config() -> None:
    if is_production() and _is_truthy("CT_ALLOW_ENTITLEMENT_HEADER"):
        raise StartupSecurityError("CT_ALLOW_ENTITLEMENT_HEADER must be disabled in production.")


def validate_password_reset_config() -> None:
    if not is_production():
        return

    if _is_truthy("CT_ALLOW_INSECURE_RESET_LINK_RESPONSE"):
        raise StartupSecurityError("Reset-token responses must be disabled in production.")

    if not _env("SMTP_HOST"):
        logger.warning("password_reset_email_delivery_unavailable environment=production reason=smtp_not_configured")


def validate_stripe_config() -> None:
    if not is_production():
        return

    readiness = log_stripe_billing_readiness(context="startup")
    if not readiness["billing_enabled"]:
        return

    missing = list(readiness["missing_env_vars"])
    if missing:
        raise StartupSecurityError(
            f"{', '.join(missing)} required when billing is enabled in production."
        )


def validate_debug_admin_config() -> None:
    if not is_production():
        return

    legacy_query_token_flags = (
        "CT_ALLOW_ADMIN_QUERY_TOKEN",
        "CT_ALLOW_DEBUG_QUERY_TOKEN",
        "CT_ENABLE_ADMIN_TOKEN_QUERY_AUTH",
        "CT_ENABLE_DEBUG_TOKEN_QUERY_AUTH",
    )
    enabled = [name for name in legacy_query_token_flags if _is_truthy(name)]
    if enabled:
        raise StartupSecurityError("ADMIN_TOKEN query-string auth must be disabled in production.")


def validate_startup_security_config() -> None:
    validate_session_secret_config()
    validate_session_cookie_config()
    validate_bearer_session_auth_config()
    validate_cors_config()
    validate_entitlement_header_config()
    validate_password_reset_config()
    validate_stripe_config()
    validate_debug_admin_config()
