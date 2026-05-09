from __future__ import annotations

import hmac
import logging
import os

logger = logging.getLogger(__name__)

MIN_PRODUCTION_SESSION_SECRET_LENGTH = 32

DEFAULT_PRODUCTION_FRONTEND_ORIGINS = ("https://congress-tracker-two.vercel.app",)
DEFAULT_LOCAL_FRONTEND_ORIGINS = (
    "http://localhost:3000",
    "http://localhost:3001",
    "http://127.0.0.1:3000",
    "http://127.0.0.1:3001",
)

_TRUE_VALUES = {"1", "true", "yes", "on", "enabled"}
_PRODUCTION_ENVS = {"prod", "production"}
_NONPRODUCTION_ENVS = {"local", "dev", "development", "test", "testing", "ci"}


class StartupSecurityError(RuntimeError):
    """Raised when production security configuration is unsafe."""


def _env(name: str) -> str:
    return os.getenv(name, "").strip()


def _env_lower(name: str) -> str:
    return _env(name).lower()


def _is_truthy(name: str) -> bool:
    return _env_lower(name) in _TRUE_VALUES


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
    return any(
        _is_truthy(name)
        for name in (
            "BILLING_ENABLED",
            "STRIPE_ENABLED",
            "CT_BILLING_ENABLED",
            "CT_STRIPE_ENABLED",
        )
    )


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
    if not (is_production() and billing_enabled()):
        return

    if not _env("STRIPE_SECRET_KEY"):
        raise StartupSecurityError("STRIPE_SECRET_KEY is required when billing is enabled in production.")
    if not _env("STRIPE_WEBHOOK_SECRET"):
        raise StartupSecurityError("STRIPE_WEBHOOK_SECRET is required when billing is enabled in production.")


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
    validate_cors_config()
    validate_entitlement_header_config()
    validate_password_reset_config()
    validate_stripe_config()
    validate_debug_admin_config()
