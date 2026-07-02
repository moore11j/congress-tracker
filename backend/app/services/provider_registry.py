from __future__ import annotations

from dataclasses import dataclass

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
    "manual_admin",
    "manual_admin_override",
    "future_vendor_fallback",
    "none",
)

FMP_STABLE_BASE_URL = "https://financialmodelingprep.com/stable"

PROVIDER_LABELS = {
    "fmp": "FMP",
    "fred": "FRED",
    "treasury_gov": "Treasury.gov",
    "sec_edgar": "SEC EDGAR",
    "official_house": "Official House Disclosures",
    "official_senate": "Official Senate Disclosures",
    "walnut_official": "Walnut Official Pipeline",
    "walnut_cache": "Local Walnut Cache",
    "internal_computed": "Internal Computed",
    "disabled": "Disabled",
    "manual_admin": "Manual Admin",
    "manual_admin_override": "Manual Admin Override",
    "future_vendor_fallback": "Future Vendor Fallback",
    "none": "None",
}

PROVIDER_HELP_TEXT = {
    "fmp": "Licensed market data provider. Some endpoints may require add-ons or exchange/provider entitlements.",
    "fred": "Federal Reserve Economic Data used for macro and Treasury-series cache refreshes.",
    "treasury_gov": "Treasury.gov source reserved for future direct Treasury-yield jobs.",
    "sec_edgar": "SEC EDGAR Form 4 source for insider-trade staging and comparison.",
    "official_house": "Official House disclosure source for House-specific discovery and shadow parsing.",
    "official_senate": "Official Senate disclosure source for Senate-specific discovery and shadow parsing.",
    "walnut_official": "Walnut's official-source aggregate pipeline for staged Congress disclosure ingestion.",
    "walnut_cache": "Local Walnut database/cache used by app routes instead of live external API calls.",
    "internal_computed": "Computed by Walnut from local stored data. Not an external provider.",
    "disabled": "This data domain is intentionally turned off.",
    "none": "No fallback provider.",
}

ENDPOINT_URL_PROVIDERS = {
    "fmp",
    "fred",
    "treasury_gov",
    "sec_edgar",
    "official_house",
    "official_senate",
    "manual_admin",
    "manual_admin_override",
    "future_vendor_fallback",
}


def provider_uses_endpoint_url(provider: str | None) -> bool:
    return bool(provider and provider in ENDPOINT_URL_PROVIDERS)


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
    primary_endpoint_url: str | None = None
    fallback_endpoint_url: str | None = None
    is_enabled: bool = True
    allow_external_live_fetch: bool = False
    allow_user_route_sync_fetch: bool = False
    builder_safe_required: bool = True
    notes: str | None = None
    allowed_providers: tuple[str, ...] = ()
    allowed_fallbacks: tuple[str, ...] = ("none",)
    allowed_modes: tuple[str, ...] = ("primary", "disabled")
    domain_help_text: str | None = None
    allow_same_provider_fallback: bool = False


MARKET_PROVIDERS = ("fmp", "walnut_cache", "disabled")
MARKET_FALLBACKS = ("walnut_cache", "fmp", "none")
MARKET_MODES = ("primary", "fallback", "disabled")
CACHE_FMP_PROVIDERS = ("walnut_cache", "fmp", "disabled")
CACHE_FMP_FALLBACKS = ("fmp", "walnut_cache", "none")
PNL_ENRICHMENT_PROVIDERS = ("internal_computed", "walnut_cache", "fmp", "disabled")
PNL_ENRICHMENT_FALLBACKS = ("walnut_cache", "fmp", "none")
INTERNAL_COMPUTED_PROVIDERS = ("internal_computed", "walnut_cache", "disabled")
INTERNAL_COMPUTED_FALLBACKS = ("walnut_cache", "none")
OFFICIAL_MODES = ("shadow", "dry_run", "primary", "disabled")
INSIGHTS_FRED_PROVIDERS = ("fred", "walnut_cache", "disabled")
INSIGHTS_FRED_FALLBACKS = ("walnut_cache", "none")
DISABLED_MARKET_PROVIDERS = ("disabled", "fmp", "walnut_cache")
DISABLED_MARKET_FALLBACKS = ("none", "walnut_cache", "fmp")


def _domain(
    domain_key: str,
    label: str,
    active_provider: str,
    fallback_provider: str | None,
    mode: str,
    source_type: str,
    builder_safe_status: str,
    endpoint_names: tuple[str, ...],
    cache_table: str | None = None,
    *,
    primary_endpoint_url: str | None = None,
    fallback_endpoint_url: str | None = None,
    allowed_providers: tuple[str, ...],
    allowed_fallbacks: tuple[str, ...],
    allowed_modes: tuple[str, ...],
    is_enabled: bool = True,
    notes: str | None = None,
    domain_help_text: str | None = None,
    allow_external_live_fetch: bool = False,
    allow_user_route_sync_fetch: bool = False,
    builder_safe_required: bool = True,
    allow_same_provider_fallback: bool = False,
) -> ProviderDomainDefault:
    return ProviderDomainDefault(
        domain_key=domain_key,
        label=label,
        active_provider=active_provider,
        fallback_provider=fallback_provider,
        mode=mode,
        source_type=source_type,
        builder_safe_status=builder_safe_status,
        endpoint_names=endpoint_names,
        cache_table=cache_table,
        primary_endpoint_url=primary_endpoint_url,
        fallback_endpoint_url=fallback_endpoint_url,
        is_enabled=is_enabled,
        allow_external_live_fetch=allow_external_live_fetch,
        allow_user_route_sync_fetch=allow_user_route_sync_fetch,
        builder_safe_required=builder_safe_required,
        notes=notes,
        allowed_providers=allowed_providers,
        allowed_fallbacks=allowed_fallbacks,
        allowed_modes=allowed_modes,
        domain_help_text=domain_help_text or notes,
        allow_same_provider_fallback=allow_same_provider_fallback,
    )


PROVIDER_DOMAIN_DEFAULTS: tuple[ProviderDomainDefault, ...] = (
    _domain("prices_eod", "EOD equity prices", "fmp", "walnut_cache", "primary", "external API", "warning", ("historical-price-eod/light", "data_enrichment_jobs:price"), "price_cache", primary_endpoint_url=f"{FMP_STABLE_BASE_URL}/historical-price-eod/light?symbol={{symbol}}", allowed_providers=MARKET_PROVIDERS, allowed_fallbacks=MARKET_FALLBACKS, allowed_modes=MARKET_MODES, notes="Licensed market data provider with cache-first reads."),
    _domain("prices_historical", "historical prices", "fmp", "walnut_cache", "primary", "external API", "warning", ("historical-price-eod/full", "ticker:chart-bundle"), "price_cache", primary_endpoint_url=f"{FMP_STABLE_BASE_URL}/historical-price-eod/full?symbol={{symbol}}", allowed_providers=MARKET_PROVIDERS, allowed_fallbacks=MARKET_FALLBACKS, allowed_modes=MARKET_MODES),
    _domain("prices_intraday", "current quote / delayed quote", "fmp", "fmp", "primary", "external API", "warning", ("historical-price-eod/light", "quote-short"), "quotes_cache", primary_endpoint_url=f"{FMP_STABLE_BASE_URL}/historical-price-eod/light?symbol={{symbol}}", fallback_endpoint_url=f"{FMP_STABLE_BASE_URL}/quote-short?symbol={{symbol}}", allowed_providers=MARKET_PROVIDERS, allowed_fallbacks=MARKET_FALLBACKS, allowed_modes=MARKET_MODES, notes="Primary avoids revoked quote entitlements by using historical EOD light; quote/quote-short can remain a secondary FMP endpoint if entitlement returns.", allow_same_provider_fallback=True),
    _domain("fundamentals", "fundamentals", "fmp", "walnut_cache", "primary", "external API", "warning", ("income-statement", "balance-sheet-statement", "cash-flow-statement"), "fundamentals_cache", primary_endpoint_url=f"{FMP_STABLE_BASE_URL}/income-statement?symbol={{symbol}}&period=annual&page=0&limit=1", allowed_providers=MARKET_PROVIDERS, allowed_fallbacks=MARKET_FALLBACKS, allowed_modes=MARKET_MODES),
    _domain("ratios", "ratios / key metrics", "fmp", "walnut_cache", "primary", "external API", "warning", ("ratios-ttm", "key-metrics-ttm"), "ticker_financials_cache", primary_endpoint_url=f"{FMP_STABLE_BASE_URL}/ratios-ttm?symbol={{symbol}}", allowed_providers=MARKET_PROVIDERS, allowed_fallbacks=MARKET_FALLBACKS, allowed_modes=MARKET_MODES),
    _domain("technicals", "technical indicators", "walnut_cache", "fmp", "primary", "local cache", "safe", ("technical_indicators",), "price_cache", allowed_providers=CACHE_FMP_PROVIDERS, allowed_fallbacks=CACHE_FMP_FALLBACKS, allowed_modes=MARKET_MODES, notes="Technicals can be computed locally from cached prices, so Local Walnut Cache is preferred."),
    _domain("profiles", "company profile / ticker metadata", "fmp", "walnut_cache", "primary", "external API", "warning", ("profile", "ticker_meta"), "ticker_meta", primary_endpoint_url=f"{FMP_STABLE_BASE_URL}/profile?symbol={{symbol}}", allowed_providers=MARKET_PROVIDERS, allowed_fallbacks=MARKET_FALLBACKS, allowed_modes=MARKET_MODES),
    _domain("earnings", "earnings calendar", "fmp", "walnut_cache", "primary", "external API", "warning", ("earnings-calendar",), "ticker_content_cache", primary_endpoint_url=f"{FMP_STABLE_BASE_URL}/earnings-calendar?symbol={{symbol}}&page=0&limit=8", allowed_providers=MARKET_PROVIDERS, allowed_fallbacks=MARKET_FALLBACKS, allowed_modes=MARKET_MODES),
    _domain("analyst_estimates", "analyst estimates", "fmp", "walnut_cache", "primary", "external API", "warning", ("analyst-estimates",), "ticker_content_cache", primary_endpoint_url=f"{FMP_STABLE_BASE_URL}/analyst-estimates?symbol={{symbol}}&period=annual&page=0&limit=2", allowed_providers=MARKET_PROVIDERS, allowed_fallbacks=MARKET_FALLBACKS, allowed_modes=MARKET_MODES),
    _domain("institutional_13f", "institutional ownership / 13F", "fmp", "walnut_cache", "primary", "external API", "warning", ("institutional-ownership/latest",), "institutional_transactions", primary_endpoint_url=f"{FMP_STABLE_BASE_URL}/institutional-ownership/latest?page=0&limit=1", allowed_providers=("fmp", "walnut_cache", "sec_edgar", "disabled"), allowed_fallbacks=("walnut_cache", "fmp", "sec_edgar", "none"), allowed_modes=MARKET_MODES, notes="SEC may be a future/partial raw source; FMP may remain the normalized source."),
    _domain("congress_trades", "Congress trades", "walnut_official", "fmp", "shadow", "public official source", "safe", ("official_congress_ingest", "run_recent_congress_ingest"), "congress_transactions_normalized", allowed_providers=("walnut_official", "fmp", "walnut_cache", "disabled"), allowed_fallbacks=("fmp", "walnut_cache", "none"), allowed_modes=OFFICIAL_MODES, notes="Official-source pipeline is shadow-only until validated against current events."),
    _domain("insider_trades", "insider trades / Form 4", "sec_edgar", "fmp", "shadow", "public official source", "safe", ("sec_form4_ingest", "ingest_insider_trades"), "insider_transactions_normalized", allowed_providers=("sec_edgar", "fmp", "walnut_cache", "disabled"), allowed_fallbacks=("fmp", "walnut_cache", "none"), allowed_modes=OFFICIAL_MODES, notes="SEC EDGAR parser is shadow-only until validation."),
    _domain("form4_filings", "SEC Form 4 filings", "sec_edgar", "walnut_cache", "shadow", "public official source", "safe", ("sec_form4_ingest",), "sec_form4_filings", allowed_providers=("sec_edgar", "walnut_cache", "disabled"), allowed_fallbacks=("walnut_cache", "none"), allowed_modes=OFFICIAL_MODES, notes="Raw SEC Form 4 discovery/parsing stays staged until explicitly promoted elsewhere."),
    _domain("senate_disclosures", "Senate disclosures", "official_senate", "walnut_cache", "shadow", "public official source", "safe", ("official_senate_discovery", "ingest_senate"), "congress_disclosure_filings", allowed_providers=("official_senate", "walnut_cache", "disabled"), allowed_fallbacks=("walnut_cache", "none"), allowed_modes=OFFICIAL_MODES),
    _domain("house_disclosures", "House disclosures", "official_house", "walnut_cache", "shadow", "public official source", "safe", ("official_house_discovery", "ingest_house"), "congress_disclosure_filings", allowed_providers=("official_house", "walnut_cache", "disabled"), allowed_fallbacks=("walnut_cache", "none"), allowed_modes=OFFICIAL_MODES),
    _domain("pnl_enrichment", "PnL enrichment", "internal_computed", "walnut_cache", "primary", "internal computed", "safe", ("feed_pnl_enrichment", "trade_outcomes"), "trade_outcomes", allowed_providers=PNL_ENRICHMENT_PROVIDERS, allowed_fallbacks=PNL_ENRICHMENT_FALLBACKS, allowed_modes=MARKET_MODES, notes="PnL/Gain-Loss is computed by Walnut from cached trades and EOD prices; FMP may supply price inputs but not the final PnL value."),
    _domain("signal_inputs", "signal scoring inputs", "internal_computed", "walnut_cache", "primary", "internal computed", "safe", ("confirmation_score", "signal_score"), "events", allowed_providers=INTERNAL_COMPUTED_PROVIDERS, allowed_fallbacks=INTERNAL_COMPUTED_FALLBACKS, allowed_modes=("primary", "disabled"), notes="Signals are Walnut-computed from cached/local inputs."),
    _domain("insights_macro", "Insights: US Macro", "fred", "walnut_cache", "primary", "local cache", "safe", ("refresh_fred_macro_cache", "insights_snapshots"), "fred_observations", allowed_providers=INSIGHTS_FRED_PROVIDERS, allowed_fallbacks=INSIGHTS_FRED_FALLBACKS, allowed_modes=("primary", "disabled"), notes="US Macro must come from FRED or the local FRED cache."),
    _domain("insights_treasury", "Insights: Treasury", "fred", "walnut_cache", "primary", "local cache", "safe", ("refresh_fred_macro_cache", "DGS*"), "fred_observations", allowed_providers=INSIGHTS_FRED_PROVIDERS, allowed_fallbacks=INSIGHTS_FRED_FALLBACKS, allowed_modes=("primary", "disabled"), notes="Treasury yields should be FRED/local cache; direct Treasury.gov can be added later."),
    _domain("insights_us_market", "Insights: US market ETF proxies", "walnut_cache", "fmp", "primary", "local cache", "safe", ("SPY price_cache proxy",), "price_cache", allowed_providers=CACHE_FMP_PROVIDERS, allowed_fallbacks=CACHE_FMP_FALLBACKS, allowed_modes=MARKET_MODES, notes="Builder-safe mode uses EOD ETF proxies from cache."),
    _domain("insights_us_sectors", "Insights: sector ETF proxies", "walnut_cache", "fmp", "primary", "local cache", "safe", ("sector ETF proxy price_cache",), "price_cache", allowed_providers=CACHE_FMP_PROVIDERS, allowed_fallbacks=CACHE_FMP_FALLBACKS, allowed_modes=MARKET_MODES, notes="Builder-safe mode uses sector ETF proxies from cache."),
    _domain("insights_global", "Insights: world/global proxies", "walnut_cache", "fmp", "primary", "local cache", "safe", ("global ETF proxy price_cache",), "price_cache", allowed_providers=CACHE_FMP_PROVIDERS, allowed_fallbacks=CACHE_FMP_FALLBACKS, allowed_modes=MARKET_MODES, notes="Use global ETF proxies or disable for launch."),
    _domain("insights_world", "Insights: world indexes", "walnut_cache", "fmp", "primary", "local cache", "safe", ("global ETF proxy price_cache",), "price_cache", allowed_providers=CACHE_FMP_PROVIDERS, allowed_fallbacks=CACHE_FMP_FALLBACKS, allowed_modes=MARKET_MODES, notes="Use global ETF proxies or disable for launch."),
    _domain("insights_fx", "Insights: FX", "disabled", None, "disabled", "external API", "safe", ("disabled",), None, allowed_providers=DISABLED_MARKET_PROVIDERS, allowed_fallbacks=DISABLED_MARKET_FALLBACKS, allowed_modes=("disabled", "primary"), is_enabled=False, notes="Disabled for launch unless licensed FX add-on/provider is enabled."),
    _domain("insights_crypto", "Insights: crypto", "disabled", None, "disabled", "external API", "safe", ("disabled",), None, allowed_providers=DISABLED_MARKET_PROVIDERS, allowed_fallbacks=DISABLED_MARKET_FALLBACKS, allowed_modes=("disabled", "primary"), is_enabled=False, notes="Disabled for launch unless licensed crypto provider/add-on is enabled."),
    _domain("insights_commodities", "Insights: commodities", "disabled", None, "disabled", "external API", "safe", ("disabled",), None, allowed_providers=("disabled", "walnut_cache", "fmp"), allowed_fallbacks=DISABLED_MARKET_FALLBACKS, allowed_modes=("disabled", "primary"), is_enabled=False, notes="Use ETF proxies or keep disabled for launch; direct commodities likely carry add-on risk."),
    _domain("watchlist_alerts", "watchlist alerts / monitoring jobs", "internal_computed", "walnut_cache", "primary", "internal computed", "safe", ("confirmation_monitoring", "email_digest_jobs"), "monitoring_alerts", allowed_providers=("internal_computed", "walnut_cache", "disabled"), allowed_fallbacks=("walnut_cache", "none"), allowed_modes=("primary", "disabled"), notes="Internal monitoring jobs read cached/computed signal state."),
    _domain("screener_fundamentals", "screener fundamentals", "walnut_cache", "fmp", "primary", "local cache", "safe", ("screener", "fundamentals_cache"), "fundamentals_cache", allowed_providers=CACHE_FMP_PROVIDERS, allowed_fallbacks=CACHE_FMP_FALLBACKS, allowed_modes=MARKET_MODES, notes="Screener should normally query local fundamentals cache, not FMP live."),
    _domain("screener_technicals", "screener technicals", "walnut_cache", "fmp", "primary", "local cache", "safe", ("screener", "technical_indicators"), "price_cache", allowed_providers=CACHE_FMP_PROVIDERS, allowed_fallbacks=CACHE_FMP_FALLBACKS, allowed_modes=MARKET_MODES, notes="Screener should normally query local computed/cached technicals."),
)


def provider_domain_catalog() -> dict[str, ProviderDomainDefault]:
    return {item.domain_key: item for item in PROVIDER_DOMAIN_DEFAULTS}


def provider_label(provider: str | None) -> str:
    if provider is None:
        return PROVIDER_LABELS["none"]
    return PROVIDER_LABELS.get(provider, provider.replace("_", " ").title())


def provider_labels_for(keys: tuple[str, ...] | list[str]) -> dict[str, str]:
    labels = {key: provider_label(key) for key in keys}
    labels["none"] = PROVIDER_LABELS["none"]
    return labels


def provider_help_for(keys: tuple[str, ...] | list[str]) -> dict[str, str]:
    return {key: PROVIDER_HELP_TEXT[key] for key in keys if key in PROVIDER_HELP_TEXT}


def _valid_provider_list(values: tuple[str, ...], *, fallback: bool = False) -> str:
    label_values = [provider_label(value) for value in values if value != "none" or fallback]
    return ", ".join(label_values)


def _provider_error(provider: str | None, domain: ProviderDomainDefault, allowed: tuple[str, ...], *, fallback: bool = False) -> str:
    provider_name = provider_label(provider or "none")
    valid = _valid_provider_list(allowed, fallback=fallback)
    if fallback:
        return f"Invalid fallback: {provider_name} is not allowed for {domain.label}. Valid fallback providers: {valid}."
    return f"Invalid provider: {provider_name} is not allowed for {domain.label}. Valid providers: {valid}."


def validate_provider_selection(
    domain_key: str,
    *,
    active_provider: str,
    fallback_provider: str | None,
    mode: str,
    is_enabled: bool,
    allow_external_live_fetch: bool,
    allow_user_route_sync_fetch: bool,
    builder_safe_required: bool,
) -> None:
    catalog = provider_domain_catalog()
    if domain_key not in catalog:
        raise KeyError(domain_key)
    domain = catalog[domain_key]
    fallback_key = fallback_provider or "none"
    if active_provider not in domain.allowed_providers:
        raise ValueError(_provider_error(active_provider, domain, domain.allowed_providers))
    if fallback_key not in domain.allowed_fallbacks:
        raise ValueError(_provider_error(fallback_key, domain, domain.allowed_fallbacks, fallback=True))
    if mode not in domain.allowed_modes:
        valid_modes = ", ".join(mode.replace("_", "-").title() for mode in domain.allowed_modes)
        raise ValueError(f"Invalid mode: {mode.replace('_', '-').title()} is not allowed for {domain.label}. Valid modes: {valid_modes}.")
    if (
        is_enabled
        and mode != "disabled"
        and active_provider != "disabled"
        and fallback_provider
        and fallback_provider == active_provider
        and not domain.allow_same_provider_fallback
    ):
        raise ValueError(f"{provider_label(active_provider)} is already the primary provider for {domain.label}; choose a different fallback or None.")
    if active_provider == "disabled" and mode != "disabled" and is_enabled:
        raise ValueError(f"{domain.label} uses Disabled as its provider, so the domain must also be disabled.")
    if mode == "disabled" and active_provider == "disabled" and is_enabled:
        raise ValueError(f"{domain.label} is in disabled mode, so Enabled must be off.")
    if allow_user_route_sync_fetch:
        raise ValueError("User-facing synchronous external fetches are not allowed from provider settings.")
    if allow_external_live_fetch and not domain.allow_external_live_fetch:
        raise ValueError(f"Live external fetches are not allowed for {domain.label}; use scheduled jobs and local cache instead.")
    if builder_safe_required and allow_user_route_sync_fetch:
        raise ValueError(f"{domain.label} is Builder-safe required and cannot use user-route live fetches.")


def provider_validation_warnings(domain_key: str, setting_payload: dict[str, object]) -> list[str]:
    try:
        validate_provider_selection(
            domain_key,
            active_provider=str(setting_payload.get("active_provider") or ""),
            fallback_provider=(str(setting_payload["fallback_provider"]) if setting_payload.get("fallback_provider") else None),
            mode=str(setting_payload.get("mode") or ""),
            is_enabled=bool(setting_payload.get("is_enabled")),
            allow_external_live_fetch=bool(setting_payload.get("allow_external_live_fetch")),
            allow_user_route_sync_fetch=bool(setting_payload.get("allow_user_route_sync_fetch")),
            builder_safe_required=bool(setting_payload.get("builder_safe_required")),
        )
    except ValueError as exc:
        return [str(exc)]
    return []
