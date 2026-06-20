from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import ProviderSetting, ProviderSettingAuditLog

ALLOWED_MODES = ("primary", "fallback", "shadow", "dry_run", "disabled")
ALLOWED_PROVIDERS = (
    "fmp",
    "fred",
    "treasury_gov",
    "sec_edgar",
    "official_house",
    "official_senate",
    "walnut_official",
    "walnut_cache",
    "internal_computed",
    "disabled",
    "manual_admin_override",
    "future_vendor_fallback",
    "none",
)


@dataclass(frozen=True)
class ProviderDomainDefault:
    domain_key: str
    label: str
    active_provider: str
    fallback_provider: str | None
    mode: str
    source_type: str
    builder_safe_status: str
    endpoint_names: tuple[str, ...]
    cache_table: str | None = None
    is_enabled: bool = True
    allow_external_live_fetch: bool = False
    allow_user_route_sync_fetch: bool = False
    builder_safe_required: bool = True
    notes: str | None = None


PROVIDER_DOMAIN_DEFAULTS: tuple[ProviderDomainDefault, ...] = (
    ProviderDomainDefault("prices_eod", "EOD equity prices", "fmp", "walnut_cache", "primary", "external API", "warning", ("price:eod", "data_enrichment_jobs:price"), "price_cache", notes="Licensed market data provider with cache-first reads."),
    ProviderDomainDefault("prices_historical", "historical prices", "fmp", "walnut_cache", "primary", "external API", "warning", ("ticker:price-history", "ticker:chart-bundle"), "price_cache"),
    ProviderDomainDefault("prices_intraday", "current quote / delayed quote", "fmp", "walnut_cache", "primary", "external API", "warning", ("ticker:quote-snapshot", "quote"), "quotes_cache"),
    ProviderDomainDefault("fundamentals", "fundamentals", "fmp", "walnut_cache", "primary", "external API", "warning", ("ticker:financials", "fundamentals_cache"), "fundamentals_cache"),
    ProviderDomainDefault("ratios", "ratios / key metrics", "fmp", "walnut_cache", "primary", "external API", "warning", ("ratios-ttm", "key-metrics-ttm"), "ticker_financials_cache"),
    ProviderDomainDefault("technicals", "technical indicators", "walnut_cache", "fmp", "primary", "local cache", "safe", ("technical_indicators",), "price_cache"),
    ProviderDomainDefault("profiles", "company profile / ticker metadata", "fmp", "walnut_cache", "primary", "external API", "warning", ("profile", "ticker_meta"), "ticker_meta"),
    ProviderDomainDefault("earnings", "earnings calendar", "fmp", "walnut_cache", "primary", "external API", "warning", ("earnings-calendar",), "ticker_content_cache"),
    ProviderDomainDefault("analyst_estimates", "analyst estimates", "fmp", "walnut_cache", "primary", "external API", "warning", ("analyst-estimates",), "ticker_content_cache"),
    ProviderDomainDefault("institutional_13f", "institutional ownership / 13F", "fmp", "walnut_cache", "primary", "external API", "warning", ("institutional-buys",), "institutional_transactions"),
    ProviderDomainDefault("congress_trades", "Congress trades", "walnut_official", "fmp", "shadow", "public official source", "safe", ("official_congress_ingest", "run_recent_congress_ingest"), "congress_transactions_normalized", notes="Official-source pipeline is shadow-only until validated against current events."),
    ProviderDomainDefault("insider_trades", "insider trades / Form 4", "sec_edgar", "fmp", "shadow", "public official source", "safe", ("sec_form4_ingest", "ingest_insider_trades"), "insider_transactions_normalized", notes="SEC EDGAR parser is shadow-only until validation."),
    ProviderDomainDefault("senate_disclosures", "Senate disclosures", "official_senate", "fmp", "shadow", "public official source", "safe", ("official_senate_discovery", "ingest_senate"), "congress_disclosure_filings"),
    ProviderDomainDefault("house_disclosures", "House disclosures", "official_house", "fmp", "shadow", "public official source", "safe", ("official_house_discovery", "ingest_house"), "congress_disclosure_filings"),
    ProviderDomainDefault("pnl_enrichment", "PnL enrichment", "internal_computed", "walnut_cache", "primary", "internal computed", "safe", ("feed_pnl_enrichment", "trade_outcomes"), "trade_outcomes"),
    ProviderDomainDefault("signal_inputs", "signal scoring inputs", "internal_computed", "walnut_cache", "primary", "internal computed", "safe", ("confirmation_score", "signal_score"), "events"),
    ProviderDomainDefault("insights_macro", "Insights: US Macro", "fred", "walnut_cache", "primary", "local cache", "safe", ("refresh_fred_macro_cache", "insights_snapshots"), "fred_observations"),
    ProviderDomainDefault("insights_treasury", "Insights: Treasury", "fred", "treasury_gov", "primary", "local cache", "safe", ("refresh_fred_macro_cache", "DGS*"), "fred_observations"),
    ProviderDomainDefault("insights_us_market", "Insights: US market ETF proxies", "walnut_cache", "fmp", "primary", "local cache", "safe", ("SPY price_cache proxy",), "price_cache"),
    ProviderDomainDefault("insights_us_sectors", "Insights: sector ETF proxies", "walnut_cache", "fmp", "primary", "local cache", "safe", ("sector ETF proxy price_cache",), "price_cache"),
    ProviderDomainDefault("insights_global", "Insights: world/global proxies", "walnut_cache", "fmp", "primary", "local cache", "safe", ("global ETF proxy price_cache",), "price_cache"),
    ProviderDomainDefault("insights_fx", "Insights: FX", "disabled", None, "disabled", "external API", "safe", ("disabled",), None, is_enabled=False, notes="Launch-disabled in Builder-safe mode."),
    ProviderDomainDefault("insights_crypto", "Insights: crypto", "disabled", None, "disabled", "external API", "safe", ("disabled",), None, is_enabled=False, notes="Launch-disabled in Builder-safe mode."),
    ProviderDomainDefault("insights_commodities", "Insights: commodities", "disabled", "fmp", "disabled", "external API", "safe", ("disabled",), None, is_enabled=False, notes="Disabled until licensed/cache strategy is explicit."),
    ProviderDomainDefault("watchlist_alerts", "watchlist alerts / monitoring jobs", "internal_computed", "walnut_cache", "primary", "internal computed", "safe", ("confirmation_monitoring", "email_digest_jobs"), "monitoring_alerts"),
    ProviderDomainDefault("screener_fundamentals", "screener fundamentals", "walnut_cache", "fmp", "primary", "local cache", "safe", ("screener", "fundamentals_cache"), "fundamentals_cache"),
    ProviderDomainDefault("screener_technicals", "screener technicals", "walnut_cache", "fmp", "primary", "local cache", "safe", ("screener", "technical_indicators"), "price_cache"),
)


def provider_domain_catalog() -> dict[str, ProviderDomainDefault]:
    return {item.domain_key: item for item in PROVIDER_DOMAIN_DEFAULTS}


def seed_default_provider_settings(db: Session) -> None:
    existing = {
        key
        for (key,) in db.execute(select(ProviderSetting.domain_key)).all()
    }
    now = datetime.now(timezone.utc)
    for default in PROVIDER_DOMAIN_DEFAULTS:
        if default.domain_key in existing:
            continue
        db.add(
            ProviderSetting(
                domain_key=default.domain_key,
                active_provider=default.active_provider,
                fallback_provider=default.fallback_provider,
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
    db.flush()


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
    settings = get_provider_settings_by_domain(db)
    setting = settings[domain_key]
    previous_provider = setting.active_provider
    previous_mode = setting.mode

    if "active_provider" in changes:
        setting.active_provider = _validated_provider(changes.get("active_provider")) or setting.active_provider
    if "fallback_provider" in changes:
        setting.fallback_provider = _validated_provider(changes.get("fallback_provider"), nullable=True)
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

    if setting.mode == "disabled":
        setting.is_enabled = False
    if setting.allow_user_route_sync_fetch:
        raise ValueError("User-facing synchronous external fetches are not allowed from provider settings.")

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
