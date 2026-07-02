from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import ProviderSetting, ProviderSettingAuditLog
from app.services.provider_registry import (
    ALLOWED_MODES,
    ALLOWED_PROVIDERS,
    PROVIDER_DOMAIN_DEFAULTS,
    ProviderDomainDefault,
    provider_domain_catalog,
    provider_uses_endpoint_url,
    validate_provider_selection,
)

PROVIDER_VALIDATION_CLEANUP_REASON = "provider_validation_cleanup"


def seed_default_provider_settings(db: Session) -> None:
    existing_settings = {
        row.domain_key: row
        for row in db.execute(select(ProviderSetting)).scalars().all()
    }
    existing = set(existing_settings)
    now = datetime.now(timezone.utc)
    for default in PROVIDER_DOMAIN_DEFAULTS:
        if default.domain_key in existing:
            continue
        db.add(
            ProviderSetting(
                domain_key=default.domain_key,
                active_provider=default.active_provider,
                fallback_provider=default.fallback_provider,
                primary_endpoint_url=default.primary_endpoint_url if provider_uses_endpoint_url(default.active_provider) else None,
                fallback_endpoint_url=default.fallback_endpoint_url if provider_uses_endpoint_url(default.fallback_provider) else None,
                mode=default.mode,
                is_enabled=default.is_enabled,
                allow_external_live_fetch=default.allow_external_live_fetch,
                allow_user_route_sync_fetch=default.allow_user_route_sync_fetch,
                builder_safe_required=default.builder_safe_required,
                notes=default.notes,
                updated_by="system",
                created_at=now,
                updated_at=now,
            )
        )
    for default in PROVIDER_DOMAIN_DEFAULTS:
        setting = existing_settings.get(default.domain_key)
        if setting is None:
            continue
        if (
            default.domain_key == "prices_intraday"
            and setting.fallback_provider in {None, "walnut_cache"}
            and setting.updated_by in {None, "system"}
            and provider_uses_endpoint_url(default.fallback_provider)
        ):
            setting.fallback_provider = default.fallback_provider
        if setting.primary_endpoint_url is None and provider_uses_endpoint_url(setting.active_provider):
            setting.primary_endpoint_url = default.primary_endpoint_url
        if setting.fallback_endpoint_url is None and provider_uses_endpoint_url(setting.fallback_provider):
            setting.fallback_endpoint_url = default.fallback_endpoint_url
        if not provider_uses_endpoint_url(setting.active_provider):
            setting.primary_endpoint_url = None
        if not provider_uses_endpoint_url(setting.fallback_provider):
            setting.fallback_endpoint_url = None
    db.flush()


def cleanup_invalid_provider_settings(db: Session) -> list[dict[str, Any]]:
    """Repair unambiguous legacy settings that the stricter registry rejects."""
    seed_default_provider_settings(db)
    settings = {
        row.domain_key: row
        for row in db.execute(select(ProviderSetting)).scalars().all()
    }
    cleaned: list[dict[str, Any]] = []
    now = datetime.now(timezone.utc)
    for domain_key in ("house_disclosures", "senate_disclosures"):
        setting = settings.get(domain_key)
        if not setting or setting.fallback_provider != "fmp":
            continue
        previous_fallback = setting.fallback_provider
        next_fallback = "walnut_cache" if setting.active_provider != "walnut_cache" else None
        previous_provider = setting.active_provider
        previous_mode = setting.mode
        setting.fallback_provider = next_fallback
        setting.updated_by = "system"
        setting.updated_at = now
        db.add(
            ProviderSettingAuditLog(
                domain_key=domain_key,
                previous_provider=previous_provider,
                new_provider=setting.active_provider,
                previous_mode=previous_mode,
                new_mode=setting.mode,
                changed_by="system",
                reason=PROVIDER_VALIDATION_CLEANUP_REASON,
            )
        )
        cleaned.append(
            {
                "domain_key": domain_key,
                "field": "fallback_provider",
                "previous_value": previous_fallback,
                "new_value": next_fallback,
                "reason": PROVIDER_VALIDATION_CLEANUP_REASON,
            }
        )
    if cleaned:
        db.flush()
    return cleaned


def get_provider_settings_by_domain(db: Session) -> dict[str, ProviderSetting]:
    seed_default_provider_settings(db)
    return {
        row.domain_key: row
        for row in db.execute(select(ProviderSetting)).scalars().all()
    }


def provider_setting_payload(setting: ProviderSetting) -> dict[str, Any]:
    return {
        "id": setting.id,
        "domain_key": setting.domain_key,
        "active_provider": setting.active_provider,
        "fallback_provider": setting.fallback_provider,
        "primary_endpoint_url": setting.primary_endpoint_url,
        "fallback_endpoint_url": setting.fallback_endpoint_url,
        "mode": setting.mode,
        "is_enabled": bool(setting.is_enabled),
        "allow_external_live_fetch": bool(setting.allow_external_live_fetch),
        "allow_user_route_sync_fetch": bool(setting.allow_user_route_sync_fetch),
        "builder_safe_required": bool(setting.builder_safe_required),
        "notes": setting.notes,
        "updated_by": setting.updated_by,
        "updated_at": setting.updated_at.isoformat() if setting.updated_at else None,
    }


def _validated_provider(value: str | None, *, nullable: bool = False) -> str | None:
    if value is None:
        if nullable:
            return None
        raise ValueError("Provider is required.")
    normalized = str(value).strip().lower()
    if not normalized and nullable:
        return None
    if normalized not in ALLOWED_PROVIDERS:
        raise ValueError(f"Unsupported provider: {value}")
    if normalized == "none" and nullable:
        return None
    return normalized


def _validated_endpoint_url(value: str | None, *, nullable: bool = True) -> str | None:
    if value is None:
        if nullable:
            return None
        raise ValueError("Endpoint URL is required.")
    cleaned = str(value).strip()
    if not cleaned:
        return None if nullable else ""
    if len(cleaned) > 1000:
        raise ValueError("Endpoint URL is too long.")
    lowered = cleaned.lower()
    if "apikey=" in lowered or "api_key=" in lowered:
        raise ValueError("Endpoint URL must not include an API key; the server appends FMP_API_KEY.")
    if any(ch.isspace() for ch in cleaned):
        raise ValueError("Endpoint URL must not contain whitespace.")
    if "://" in cleaned and not (lowered.startswith("https://") or lowered.startswith("http://")):
        raise ValueError("Endpoint URL must be an HTTP(S) URL, a path, or an FMP stable endpoint name.")
    return cleaned


def _sync_endpoint_defaults(setting: ProviderSetting, default: ProviderDomainDefault) -> None:
    if not provider_uses_endpoint_url(setting.active_provider):
        setting.primary_endpoint_url = None
    elif setting.primary_endpoint_url is None:
        setting.primary_endpoint_url = default.primary_endpoint_url

    if not provider_uses_endpoint_url(setting.fallback_provider):
        setting.fallback_endpoint_url = None
    elif setting.fallback_endpoint_url is None:
        setting.fallback_endpoint_url = default.fallback_endpoint_url


def update_provider_setting(
    db: Session,
    *,
    domain_key: str,
    changes: dict[str, Any],
    changed_by: str | None,
    reason: str | None = None,
) -> ProviderSetting:
    catalog = provider_domain_catalog()
    if domain_key not in catalog:
        raise KeyError(domain_key)
    default = catalog[domain_key]
    settings = get_provider_settings_by_domain(db)
    setting = settings[domain_key]
    previous_provider = setting.active_provider
    previous_mode = setting.mode

    if "active_provider" in changes:
        setting.active_provider = _validated_provider(changes.get("active_provider")) or setting.active_provider
    if "fallback_provider" in changes:
        setting.fallback_provider = _validated_provider(changes.get("fallback_provider"), nullable=True)
    if "primary_endpoint_url" in changes:
        setting.primary_endpoint_url = _validated_endpoint_url(changes.get("primary_endpoint_url"))
    if "fallback_endpoint_url" in changes:
        setting.fallback_endpoint_url = _validated_endpoint_url(changes.get("fallback_endpoint_url"))
    if "mode" in changes:
        mode = str(changes.get("mode") or "").strip().lower()
        if mode not in ALLOWED_MODES:
            raise ValueError(f"Unsupported mode: {changes.get('mode')}")
        setting.mode = mode
    if "is_enabled" in changes:
        setting.is_enabled = bool(changes.get("is_enabled"))
    if "allow_external_live_fetch" in changes:
        setting.allow_external_live_fetch = bool(changes.get("allow_external_live_fetch"))
    if "allow_user_route_sync_fetch" in changes:
        setting.allow_user_route_sync_fetch = bool(changes.get("allow_user_route_sync_fetch"))
    if "builder_safe_required" in changes:
        setting.builder_safe_required = bool(changes.get("builder_safe_required"))
    if "notes" in changes:
        notes = changes.get("notes")
        setting.notes = str(notes).strip() if notes is not None and str(notes).strip() else None

    if setting.active_provider == "disabled":
        setting.mode = "disabled"
        setting.is_enabled = False
    if setting.mode == "disabled":
        setting.is_enabled = False

    _sync_endpoint_defaults(setting, default)

    validate_provider_selection(
        domain_key,
        active_provider=setting.active_provider,
        fallback_provider=setting.fallback_provider,
        mode=setting.mode,
        is_enabled=bool(setting.is_enabled),
        allow_external_live_fetch=bool(setting.allow_external_live_fetch),
        allow_user_route_sync_fetch=bool(setting.allow_user_route_sync_fetch),
        builder_safe_required=bool(setting.builder_safe_required),
    )

    setting.updated_by = changed_by
    setting.updated_at = datetime.now(timezone.utc)
    db.add(
        ProviderSettingAuditLog(
            domain_key=domain_key,
            previous_provider=previous_provider,
            new_provider=setting.active_provider,
            previous_mode=previous_mode,
            new_mode=setting.mode,
            changed_by=changed_by,
            reason=reason,
        )
    )
    db.flush()
    return setting
