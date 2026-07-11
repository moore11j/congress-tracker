from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.models import (
    CongressDisclosureFiling,
    CongressTransactionNormalized,
    DataEnrichmentJob,
    Event,
    Filing,
    FredObservation,
    FredSeriesRefresh,
    FundamentalsCache,
    InsiderTransaction,
    InsiderTransactionNormalized,
    MacroPositioningCache,
    PriceCache,
    ProviderUsageEvent,
    SecForm4Filing,
    TickerContentCache,
    TickerFinancialsCache,
    TickerMeta,
    TradeOutcome,
    Transaction,
)
from app.services.congress_assets import CONGRESS_DISCLOSURE_EVENT_TYPES
from app.services.provider_settings import (
    ALLOWED_MODES,
    ALLOWED_PROVIDERS,
    get_provider_settings_by_domain,
    provider_domain_catalog,
    provider_setting_payload,
)
from app.services.provider_registry import (
    PROVIDER_LABELS,
    provider_help_for,
    provider_labels_for,
    provider_uses_endpoint_url,
    provider_validation_warnings,
)
from app.services.provider_endpoints import (
    configured_endpoint_contract,
    configured_endpoint_url,
    endpoint_display_name,
    endpoint_contracts_for_setting,
    endpoint_test_category,
    endpoint_urls_for_setting,
    test_fmp_endpoint,
)


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _safe_scalar(db: Session, statement, default: Any = None) -> Any:
    try:
        value = db.execute(statement).scalar_one_or_none()
        return default if value is None else value
    except SQLAlchemyError:
        return default


def _safe_count(db: Session, model, *filters) -> int:
    try:
        statement = select(func.count()).select_from(model)
        for clause in filters:
            statement = statement.where(clause)
        return int(db.execute(statement).scalar_one() or 0)
    except SQLAlchemyError:
        return 0


def _percent(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100, 1)


def _latest_value(*values: Any) -> Any:
    candidates = [value for value in values if value is not None]
    return max(candidates) if candidates else None


def _queue_depth(db: Session, job_types: tuple[str, ...]) -> int:
    if not job_types:
        return 0
    return _safe_count(db, DataEnrichmentJob, DataEnrichmentJob.job_type.in_(job_types), DataEnrichmentJob.status.in_(("queued", "running")))


def _call_count_24h(db: Session, provider: str) -> int | None:
    if provider not in {"fmp", "fred", "sec_edgar", "treasury_gov"}:
        return None
    since = datetime.now(timezone.utc) - timedelta(hours=24)
    return _safe_count(
        db,
        ProviderUsageEvent,
        ProviderUsageEvent.provider == provider,
        ProviderUsageEvent.cache_status.is_(None),
        ProviderUsageEvent.created_at >= since,
    )


def _latest_endpoint_test(db: Session, domain_key: str, role: str) -> ProviderUsageEvent | None:
    try:
        return db.execute(
            select(ProviderUsageEvent)
            .where(ProviderUsageEvent.category == endpoint_test_category(domain_key, role))
            .order_by(ProviderUsageEvent.created_at.desc(), ProviderUsageEvent.id.desc())
            .limit(1)
        ).scalar_one_or_none()
    except SQLAlchemyError:
        return None


def _endpoint_test_payload(row: ProviderUsageEvent | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {
        "status": "healthy" if row.success else "error",
        "status_code": row.status_code,
        "error": row.error,
        "endpoint": row.endpoint,
        "tested_at": _iso(row.created_at),
    }


def _endpoint_tests(db: Session, domain_key: str) -> dict[str, dict[str, Any] | None]:
    return {
        "primary": _endpoint_test_payload(_latest_endpoint_test(db, domain_key, "primary")),
        "fallback": _endpoint_test_payload(_latest_endpoint_test(db, domain_key, "fallback")),
    }


def _endpoint_test_error(tests: dict[str, dict[str, Any] | None]) -> str | None:
    primary = tests.get("primary")
    if primary and primary.get("status") == "error":
        return str(primary.get("error") or "unknown_error")
    fallback = tests.get("fallback")
    if fallback and fallback.get("status") == "error":
        return str(fallback.get("error") or "unknown_error")
    return None


def _endpoint_tested_at(tests: dict[str, dict[str, Any] | None]) -> str | None:
    values = [
        test.get("tested_at")
        for test in tests.values()
        if test and test.get("tested_at")
    ]
    return max(values) if values else None


def _freshness(value: datetime | None, *, stale_after_hours: int = 24) -> str:
    if value is None:
        return "missing"
    candidate = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return "stale" if datetime.now(timezone.utc) - candidate > timedelta(hours=stale_after_hours) else "fresh"


def _latest_price_update(db: Session) -> datetime | None:
    return _safe_scalar(db, select(func.max(PriceCache.updated_at)))


def _latest_fundamentals_update(db: Session) -> datetime | None:
    return _safe_scalar(db, select(func.max(FundamentalsCache.updated_at)))


def _latest_ticker_content_update(db: Session, content_type: str | None = None) -> datetime | None:
    statement = select(func.max(TickerContentCache.fetched_at))
    if content_type:
        statement = statement.where(TickerContentCache.content_type == content_type)
    return _safe_scalar(db, statement)


def _latest_congress_official_update(db: Session) -> datetime | None:
    return _safe_scalar(db, select(func.max(CongressDisclosureFiling.parsed_at)))


def _latest_sec_update(db: Session) -> datetime | None:
    return _safe_scalar(db, select(func.max(SecForm4Filing.parsed_at)))


def _latest_event_update(db: Session, event_types: tuple[str, ...]) -> datetime | None:
    return _safe_scalar(db, select(func.max(Event.created_at)).where(Event.event_type.in_(event_types)))


def _promotion_preview_from_pairs(db: Session, pairs: list[tuple[str | None, str | None]]) -> dict[str, int]:
    eligible_pairs = [(provider, key) for provider, key in pairs if provider and key]
    if not eligible_pairs:
        return {"would_insert_count": 0, "would_skip_duplicate_count": 0}
    providers = {provider for provider, _key in eligible_pairs}
    keys = {key for _provider, key in eligible_pairs}
    try:
        existing = {
            (provider, key)
            for provider, key in db.execute(
                select(Event.source_provider, Event.source_filing_id)
                .where(Event.source_provider.in_(providers))
                .where(Event.source_filing_id.in_(keys))
            ).all()
        }
    except SQLAlchemyError:
        existing = set()
    would_skip = sum(1 for pair in eligible_pairs if pair in existing)
    return {
        "would_insert_count": len(eligible_pairs) - would_skip,
        "would_skip_duplicate_count": would_skip,
    }


def _congress_promotion_preview(db: Session) -> dict[str, int]:
    try:
        pairs = [
            (row[0], row[1])
            for row in db.execute(
                select(CongressTransactionNormalized.source_provider, CongressTransactionNormalized.normalized_hash)
                .where(CongressTransactionNormalized.is_duplicate.is_(False))
                .where(CongressTransactionNormalized.ticker_normalized.is_not(None))
            ).all()
        ]
    except SQLAlchemyError:
        pairs = []
    return _promotion_preview_from_pairs(db, pairs)


def _sec_promotion_preview(db: Session) -> dict[str, int]:
    try:
        pairs = [
            ("sec_edgar", row[0])
            for row in db.execute(
                select(InsiderTransactionNormalized.normalized_hash)
                .where(InsiderTransactionNormalized.is_duplicate.is_(False))
                .where(InsiderTransactionNormalized.ticker_normalized.is_not(None))
                .where(InsiderTransactionNormalized.transaction_type_normalized.in_(("open_market_purchase", "open_market_sale")))
            ).all()
        ]
    except SQLAlchemyError:
        pairs = []
    return _promotion_preview_from_pairs(db, pairs)


def _readiness_status(*, parsed_count: int, normalized_count: int, parse_failures: int, potential_conflicts_count: int) -> str:
    if parse_failures > 0 and parsed_count == 0:
        return "error"
    if parsed_count == 0 or normalized_count == 0:
        return "not_ready"
    if parse_failures > 0 or potential_conflicts_count > 0:
        return "shadow_healthy"
    return "ready_for_limited_forward_ingest"


def _cache_metrics(db: Session, domain_key: str) -> tuple[str | None, int | None, datetime | None, int]:
    if domain_key in {"prices_eod", "prices_historical", "prices_intraday", "technicals", "insights_us_market", "insights_us_sectors", "insights_global", "insights_world", "screener_technicals"}:
        return "price_cache", _safe_count(db, PriceCache), _latest_price_update(db), _queue_depth(db, ("price_eod", "price_series", "quote", "technical_indicators"))
    if domain_key in {"fundamentals", "screener_fundamentals"}:
        return "fundamentals_cache", _safe_count(db, FundamentalsCache), _latest_fundamentals_update(db), _queue_depth(db, ("fundamentals",))
    if domain_key in {"ratios"}:
        return "ticker_financials_cache", _safe_count(db, TickerFinancialsCache), _safe_scalar(db, select(func.max(TickerFinancialsCache.fetched_at))), _queue_depth(db, ("ticker_financials",))
    if domain_key in {"profiles"}:
        latest = _safe_scalar(db, select(func.max(TickerMeta.updated_at)))
        return "ticker_meta", _safe_count(db, TickerMeta), latest, _queue_depth(db, ("ticker_meta", "profile"))
    if domain_key in {"earnings", "analyst_estimates"}:
        content_type = "earnings" if domain_key == "earnings" else "analyst_estimates"
        return "ticker_content_cache", _safe_count(db, TickerContentCache, TickerContentCache.content_type == content_type), _latest_ticker_content_update(db, content_type), _queue_depth(db, (content_type,))
    if domain_key == "institutional_13f":
        return "institutional_transactions", None, None, _queue_depth(db, ("institutional_buys",))
    if domain_key in {"congress_trades", "house_disclosures", "senate_disclosures"}:
        provider_filter = []
        if domain_key == "house_disclosures":
            provider_filter.append(CongressDisclosureFiling.chamber == "house")
        elif domain_key == "senate_disclosures":
            provider_filter.append(CongressDisclosureFiling.chamber == "senate")
        row_count = _safe_count(db, CongressTransactionNormalized)
        if provider_filter:
            row_count = _safe_count(db, CongressDisclosureFiling, *provider_filter)
        return "congress_transactions_normalized", row_count, _latest_congress_official_update(db), _queue_depth(db, ("official_congress_ingest", "official_house_discovery", "official_senate_discovery"))
    if domain_key in {"insider_trades"}:
        return "insider_transactions_normalized", _safe_count(db, InsiderTransactionNormalized), _latest_sec_update(db), _queue_depth(db, ("sec_form4_ingest",))
    if domain_key == "form4_filings":
        return "sec_form4_filings", _safe_count(db, SecForm4Filing), _latest_sec_update(db), _queue_depth(db, ("sec_form4_ingest",))
    if domain_key == "pnl_enrichment":
        return "trade_outcomes", _safe_count(db, TradeOutcome), _safe_scalar(db, select(func.max(TradeOutcome.computed_at))), _queue_depth(db, ("pnl_refresh",))
    if domain_key == "signal_inputs":
        return "events", _safe_count(db, Event), _latest_event_update(db, ("congress_trade", "insider_trade", "government_contract")), 0
    if domain_key == "macro_positioning":
        return "macro_positioning_cache", _safe_count(db, MacroPositioningCache), _safe_scalar(db, select(func.max(MacroPositioningCache.generated_at))), _queue_depth(db, ("macro_positioning_weekly_refresh",))
    if domain_key in {"insights_macro", "insights_treasury"}:
        return "fred_observations", _safe_count(db, FredObservation), _safe_scalar(db, select(func.max(FredSeriesRefresh.last_refreshed_at))), _queue_depth(db, ("fred_macro_refresh", "insights_refresh"))
    if domain_key == "watchlist_alerts":
        return "monitoring_alerts", None, None, _queue_depth(db, ("watchlist_digest", "signal_alert", "monitoring_refresh"))
    return None, None, None, 0


def _builder_safe_status(default_status: str, setting_payload: dict[str, Any], source_type: str) -> str:
    if setting_payload["allow_user_route_sync_fetch"]:
        return "unsafe"
    if not setting_payload["is_enabled"] or setting_payload["mode"] == "disabled":
        return "safe"
    if setting_payload["builder_safe_required"] and setting_payload["active_provider"] == "fmp" and source_type == "external API":
        return "warning"
    if setting_payload["allow_external_live_fetch"]:
        return "warning"
    return default_status


def _domain_status_badges(setting_payload: dict[str, Any], stale_status: str, last_error: str | None, builder_safe_status: str) -> list[str]:
    badges: list[str] = []
    if setting_payload["mode"] == "shadow":
        badges.append("Shadow")
    elif setting_payload["mode"] == "dry_run":
        badges.append("Dry-run")
    elif setting_payload["mode"] == "disabled" or not setting_payload["is_enabled"]:
        badges.append("Disabled")
    elif setting_payload["mode"] == "fallback":
        badges.append("Fallback")
    else:
        badges.append("Active")
    if stale_status == "missing":
        badges.append("Missing")
    elif stale_status == "stale":
        badges.append("Stale")
    if last_error:
        badges.append("Error")
    if builder_safe_status == "safe":
        badges.append("Builder-safe")
    elif builder_safe_status == "warning":
        badges.append("Add-on risk")
    return badges


def _domain_rows(db: Session) -> list[dict[str, Any]]:
    catalog = provider_domain_catalog()
    settings = get_provider_settings_by_domain(db)
    rows: list[dict[str, Any]] = []
    for domain_key, default in catalog.items():
        setting = settings[domain_key]
        setting_payload = provider_setting_payload(setting)
        validation_warnings = provider_validation_warnings(domain_key, setting_payload)
        cache_table, row_count, last_refresh, queue_depth = _cache_metrics(db, domain_key)
        endpoint_tests = _endpoint_tests(db, domain_key)
        last_error = _endpoint_test_error(endpoint_tests)
        endpoint_urls = endpoint_urls_for_setting(setting, default)
        endpoint_contracts = endpoint_contracts_for_setting(setting, default)
        endpoint_names = [
            name
            for name in (
                endpoint_display_name(endpoint_urls["primary"]),
                endpoint_display_name(endpoint_urls["fallback"]),
                *default.endpoint_names,
            )
            if name
        ]
        endpoint_names = list(dict.fromkeys(endpoint_names))
        last_tested_at = _endpoint_tested_at(endpoint_tests)
        stale_status = _freshness(last_refresh, stale_after_hours=4 if domain_key.startswith("insights_") else 24)
        builder_safe_status = _builder_safe_status(default.builder_safe_status, setting_payload, default.source_type)
        rows.append(
            {
                "domain_key": domain_key,
                "data_domain": default.label,
                "active_provider": setting.active_provider,
                "fallback_provider": setting.fallback_provider,
                "source_type": default.source_type,
                "mode": setting.mode if setting.is_enabled else "disabled",
                "builder_safe_status": builder_safe_status,
                "endpoint_names": endpoint_names,
                "endpoint_urls": endpoint_urls,
                "endpoint_contracts": endpoint_contracts,
                "default_primary_endpoint_url": default.primary_endpoint_url,
                "default_fallback_endpoint_url": default.fallback_endpoint_url,
                "default_primary_endpoint_contract_json": default.primary_endpoint_contract_json,
                "default_fallback_endpoint_contract_json": default.fallback_endpoint_contract_json,
                "endpoint_tests": endpoint_tests,
                "last_endpoint_tested_at": last_tested_at,
                "last_successful_refresh": _iso(last_refresh),
                "last_attempted_refresh": last_tested_at or _iso(last_refresh),
                "stale_status": stale_status,
                "cache_table": cache_table or default.cache_table,
                "row_count": row_count,
                "coverage": None,
                "last_error": last_error,
                "call_count_24h": _call_count_24h(db, setting.active_provider),
                "queue_depth": queue_depth,
                "settings": setting_payload,
                "badges": _domain_status_badges(setting_payload, stale_status, last_error, builder_safe_status),
                "allowed_providers": list(default.allowed_providers),
                "allowed_fallbacks": list(default.allowed_fallbacks),
                "allowed_modes": list(default.allowed_modes),
                "default_provider": default.active_provider,
                "default_fallback": default.fallback_provider,
                "default_mode": default.mode,
                "provider_labels": provider_labels_for(
                    tuple(dict.fromkeys((*default.allowed_providers, *default.allowed_fallbacks, setting.active_provider, setting.fallback_provider or "none")))
                ),
                "provider_help_text": provider_help_for(tuple(dict.fromkeys((*default.allowed_providers, *default.allowed_fallbacks)))),
                "provider_endpoint_support": {
                    "primary": provider_uses_endpoint_url(setting.active_provider),
                    "fallback": provider_uses_endpoint_url(setting.fallback_provider),
                },
                "domain_help_text": default.domain_help_text,
                "validation_warnings": validation_warnings,
                "can_save": not validation_warnings,
                "admin_actions": {
                    "can_run_dry_run": domain_key in {"congress_trades", "house_disclosures", "senate_disclosures", "insider_trades", "insights_macro", "insights_treasury", "macro_positioning"},
                    "can_refresh_cache": domain_key in {"prices_eod", "fundamentals", "profiles", "insights_macro", "insights_treasury", "macro_positioning"},
                    "can_view_diagnostics": True,
                    "can_test_endpoint": provider_uses_endpoint_url(setting.active_provider) or provider_uses_endpoint_url(setting.fallback_provider),
                },
                "notes": setting.notes or default.notes,
            }
        )
    return rows


def _congress_diagnostics(db: Session) -> dict[str, Any]:
    latest_house = _safe_scalar(
        db,
        select(func.max(CongressDisclosureFiling.filing_date)).where(CongressDisclosureFiling.chamber == "house"),
    )
    latest_senate = _safe_scalar(
        db,
        select(func.max(CongressDisclosureFiling.filing_date)).where(CongressDisclosureFiling.chamber == "senate"),
    )
    current_feed_count = _safe_count(db, Event, Event.event_type.in_(CONGRESS_DISCLOSURE_EVENT_TYPES))
    normalized_count = _safe_count(db, CongressTransactionNormalized)
    parsed_count = _safe_count(db, CongressDisclosureFiling, CongressDisclosureFiling.parser_status == "parsed")
    parse_failures = _safe_count(db, CongressDisclosureFiling, CongressDisclosureFiling.parser_status == "error")
    duplicate_candidates = _safe_count(db, CongressTransactionNormalized, CongressTransactionNormalized.is_duplicate.is_(True))
    parse_confidence_warnings = _safe_count(db, CongressTransactionNormalized, CongressTransactionNormalized.parser_confidence < 0.75)
    unresolved = _safe_count(
        db,
        CongressTransactionNormalized,
        ~CongressTransactionNormalized.symbol_resolution_status.in_(("resolved", "admin_override", "treasury", "crypto", "etf")),
    )
    preview = _congress_promotion_preview(db)
    potential_conflicts_count = duplicate_candidates + parse_confidence_warnings + unresolved
    readiness = _readiness_status(
        parsed_count=parsed_count,
        normalized_count=normalized_count,
        parse_failures=parse_failures,
        potential_conflicts_count=potential_conflicts_count,
    )
    return {
        "mode": "shadow",
        "public_feed_impact": "none",
        "existing_data_preserved": True,
        "latest_source_check": _iso(_latest_value(latest_house, latest_senate)),
        "house_latest_source_check": _iso(latest_house),
        "senate_latest_source_check": _iso(latest_senate),
        "filings_discovered": _safe_count(db, CongressDisclosureFiling),
        "filings_parsed": parsed_count,
        "parse_failures": parse_failures,
        "normalized_transactions": normalized_count,
        "normalized_hash_coverage_percent": _percent(
            _safe_count(db, CongressTransactionNormalized, CongressTransactionNormalized.normalized_hash.is_not(None)),
            normalized_count,
        ),
        "unresolved_symbols": unresolved,
        "duplicate_candidates": duplicate_candidates,
        "potential_duplicate_insert_risk": "review_duplicates" if duplicate_candidates or preview["would_skip_duplicate_count"] else "low",
        "would_insert_count": preview["would_insert_count"],
        "would_skip_duplicate_count": preview["would_skip_duplicate_count"],
        "potential_conflicts_count": potential_conflicts_count,
        "promoted_events": _safe_count(db, Event, Event.source_provider.in_(("official_house", "official_senate", "walnut_official"))),
        "pnl_pending": _safe_count(db, TradeOutcome, TradeOutcome.scoring_status.in_(("pending", "provider_unavailable", "provider_429", "provider_402"))),
        "last_successful_shadow_ingest": _iso(_latest_congress_official_update(db)),
        "readiness_status": readiness,
        "safe_to_promote": readiness,
        "comparison": {
            "official_vs_current_feed_count": {
                "official_normalized": normalized_count,
                "current_feed": current_feed_count,
                "delta": normalized_count - current_feed_count,
            },
            "missing_in_official": max(current_feed_count - normalized_count, 0),
            "missing_in_current": max(normalized_count - current_feed_count, 0),
            "potential_duplicates": duplicate_candidates,
            "parse_confidence_warnings": parse_confidence_warnings,
        },
    }


def _insider_diagnostics(db: Session) -> dict[str, Any]:
    current_feed_count = _safe_count(db, Event, Event.event_type == "insider_trade")
    normalized_count = _safe_count(db, InsiderTransactionNormalized)
    parsed_count = _safe_count(db, SecForm4Filing, SecForm4Filing.parser_status == "parsed")
    parser_failures = _safe_count(db, SecForm4Filing, SecForm4Filing.parser_status == "error")
    duplicate_candidates = _safe_count(db, InsiderTransactionNormalized, InsiderTransactionNormalized.is_duplicate.is_(True))
    unresolved_ciks_tickers = _safe_count(db, InsiderTransactionNormalized, InsiderTransactionNormalized.ticker_normalized.is_(None))
    parse_confidence_warnings = _safe_count(db, InsiderTransactionNormalized, InsiderTransactionNormalized.parser_confidence < 0.75)
    preview = _sec_promotion_preview(db)
    potential_conflicts_count = duplicate_candidates + parse_confidence_warnings + unresolved_ciks_tickers
    readiness = _readiness_status(
        parsed_count=parsed_count,
        normalized_count=normalized_count,
        parse_failures=parser_failures,
        potential_conflicts_count=potential_conflicts_count,
    )
    code_rows = []
    try:
        code_rows = [
            {"transaction_code": row[0] or "unknown", "count": int(row[1] or 0)}
            for row in db.execute(
                select(InsiderTransactionNormalized.transaction_code, func.count(InsiderTransactionNormalized.id))
                .group_by(InsiderTransactionNormalized.transaction_code)
                .order_by(func.count(InsiderTransactionNormalized.id).desc())
            ).all()
        ]
    except SQLAlchemyError:
        code_rows = []
    return {
        "mode": "shadow",
        "public_feed_impact": "none",
        "existing_data_preserved": True,
        "sec_latest_check": _iso(_latest_sec_update(db)),
        "form4_filings_discovered": _safe_count(db, SecForm4Filing),
        "filings_parsed": parsed_count,
        "parser_failures": parser_failures,
        "normalized_transactions": normalized_count,
        "normalized_hash_coverage_percent": _percent(
            _safe_count(db, InsiderTransactionNormalized, InsiderTransactionNormalized.normalized_hash.is_not(None)),
            normalized_count,
        ),
        "transactions_by_code": code_rows,
        "open_market_buys": _safe_count(db, InsiderTransactionNormalized, InsiderTransactionNormalized.transaction_type_normalized == "open_market_purchase"),
        "open_market_sales": _safe_count(db, InsiderTransactionNormalized, InsiderTransactionNormalized.transaction_type_normalized == "open_market_sale"),
        "grants_options_exercises": _safe_count(
            db,
            InsiderTransactionNormalized,
            InsiderTransactionNormalized.transaction_type_normalized.in_(("grant_award", "option_exercise_conversion")),
        ),
        "unresolved_ciks_tickers": unresolved_ciks_tickers,
        "duplicate_candidates": duplicate_candidates,
        "potential_duplicate_insert_risk": "review_duplicates" if duplicate_candidates or preview["would_skip_duplicate_count"] else "low",
        "would_insert_count": preview["would_insert_count"],
        "would_skip_duplicate_count": preview["would_skip_duplicate_count"],
        "potential_conflicts_count": potential_conflicts_count,
        "promoted_events": _safe_count(db, Event, Event.source_provider == "sec_edgar"),
        "last_successful_sec_ingest": _iso(_latest_sec_update(db)),
        "readiness_status": readiness,
        "safe_to_promote": readiness,
        "comparison": {
            "sec_vs_current_feed_count": {
                "sec_normalized": normalized_count,
                "current_feed": current_feed_count,
                "fmp_raw_rows": _safe_count(db, InsiderTransaction),
                "delta": normalized_count - current_feed_count,
            },
            "missing_in_sec": max(current_feed_count - normalized_count, 0),
            "missing_in_current": max(normalized_count - current_feed_count, 0),
            "potential_duplicates": duplicate_candidates,
            "parse_confidence_warnings": parse_confidence_warnings,
        },
    }


def _endpoint_map(rows: list[dict[str, Any]]) -> dict[str, list[str]]:
    return {row["domain_key"]: row["endpoint_names"] for row in rows}


def current_data_source_map(db: Session) -> dict[str, str]:
    settings = get_provider_settings_by_domain(db)
    return {domain_key: row.active_provider for domain_key, row in settings.items()}


def build_data_sources_status(db: Session) -> dict[str, Any]:
    rows = _domain_rows(db)
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "provider_options": list(ALLOWED_PROVIDERS),
        "mode_options": list(ALLOWED_MODES),
        "provider_labels": PROVIDER_LABELS,
        "filters": ["All", "Safe", "Warning", "Unsafe", "External APIs", "Official Sources", "Cache-only", "Errors", "Stale", "Disabled"],
        "status_badges": ["Active", "Fallback", "Shadow", "Disabled", "Missing", "Stale", "Error", "Builder-safe", "Add-on risk"],
        "domains": rows,
        "current_data_source_map": current_data_source_map(db),
        "endpoint_map": _endpoint_map(rows),
        "tables": {
            "existing_core": ["filings", "transactions", "insider_transactions", "events", "price_cache", "fundamentals_cache", "ticker_content_cache", "insights_snapshots", "fred_observations", "provider_usage_events", "data_enrichment_jobs"],
            "official_shadow": ["provider_settings", "provider_setting_audit_log", "congress_disclosure_filings", "congress_transactions_normalized", "sec_form4_filings", "insider_transactions_normalized", "symbol_resolution_overrides"],
        },
        "diagnostics": {
            "congress": _congress_diagnostics(db),
            "insider": _insider_diagnostics(db),
            "production_source_counts": {
                "fmp_house_filings": _safe_count(db, Filing, Filing.source == "house_fmp"),
                "fmp_senate_filings": _safe_count(db, Filing, Filing.source == "senate_fmp"),
                "congress_transactions": _safe_count(db, Transaction),
                "fmp_insider_raw": _safe_count(db, InsiderTransaction),
            },
        },
        "dry_run_commands": {
            "congress_current_fmp": "python -m app.ingest_congress_recent --dry-run --days 7 --pages 5 --limit 100",
            "official_congress_shadow": "POST /api/admin/data-sources/run/congress_trades with {\"mode\":\"dry_run\"}",
            "sec_form4_shadow": "POST /api/admin/data-sources/run/insider_trades with {\"mode\":\"dry_run\"}",
            "frontend_typecheck": "npx.cmd tsc --noEmit --incremental false",
        },
        "risks": [
            "Official House/Senate documents have format drift; low-confidence rows stay in shadow diagnostics.",
            "SEC Form 4 transaction codes must remain distinct from open-market buy/sale signals.",
            "Provider switches affect future ingest jobs only; existing Walnut records remain stored.",
            "Historical backfills and public-feed promotion require separate admin actions.",
        ],
    }


def test_data_source_endpoint(
    db: Session,
    *,
    domain_key: str,
    symbol: str,
    requested_by: str | None,
    reason: str | None = None,
) -> dict[str, Any]:
    catalog = provider_domain_catalog()
    if domain_key not in catalog:
        raise KeyError(domain_key)
    setting = get_provider_settings_by_domain(db)[domain_key]
    default = catalog[domain_key]
    normalized_symbol = (symbol or "AAPL").strip().upper() or "AAPL"
    results: dict[str, Any] = {}
    for role, provider in (("primary", setting.active_provider), ("fallback", setting.fallback_provider)):
        if provider is None or provider in {"none", "disabled", "walnut_cache", "internal_computed", "walnut_official"}:
            results[role] = {
                "status": "skipped",
                "provider": provider or "none",
                "error": None,
                "endpoint": None,
                "tested_at": datetime.now(timezone.utc).isoformat(),
            }
            continue
        endpoint_url = configured_endpoint_url(setting, default, role)
        if provider != "fmp":
            results[role] = {
                "status": "skipped",
                "provider": provider,
                "error": "endpoint_test_not_supported",
                "endpoint": endpoint_display_name(endpoint_url),
                "tested_at": datetime.now(timezone.utc).isoformat(),
            }
            continue
        if not endpoint_url:
            raise ValueError(f"{role.title()} FMP endpoint URL is not configured for {default.label}.")
        endpoint_contract_json = configured_endpoint_contract(setting, default, role)
        result = test_fmp_endpoint(
            db,
            domain_key=domain_key,
            role=role,
            endpoint_url=endpoint_url,
            endpoint_contract_json=endpoint_contract_json,
            symbol=normalized_symbol,
            requested_by=requested_by,
        )
        result["provider"] = provider
        result["endpoint_url"] = endpoint_url
        results[role] = result
    return {
        "status": "tested",
        "domain_key": domain_key,
        "symbol": normalized_symbol,
        "reason": reason,
        "results": results,
    }


def enqueue_admin_data_source_run(db: Session, *, domain_key: str, mode: str, requested_by: str | None) -> dict[str, Any]:
    allowed_domains = set(provider_domain_catalog())
    if domain_key not in allowed_domains:
        raise KeyError(domain_key)
    normalized_mode = mode if mode in {"dry_run", "shadow"} else "dry_run"
    job_type = {
        "congress_trades": "official_congress_ingest",
        "house_disclosures": "official_house_discovery",
        "senate_disclosures": "official_senate_discovery",
        "insider_trades": "sec_form4_ingest",
        "insights_macro": "insights_refresh",
        "insights_treasury": "insights_refresh",
        "prices_eod": "price_eod",
        "fundamentals": "fundamentals",
        "profiles": "ticker_meta",
        "macro_positioning": "macro_positioning_weekly_refresh",
    }.get(domain_key, f"data_source_{domain_key}")
    now = datetime.now(timezone.utc)
    dedupe_key = f"admin-data-source|{domain_key}|{normalized_mode}|{now.date().isoformat()}"
    existing = db.execute(select(DataEnrichmentJob).where(DataEnrichmentJob.dedupe_key == dedupe_key)).scalar_one_or_none()
    payload = {
        "domain_key": domain_key,
        "mode": normalized_mode,
        "dry_run": True,
        "requested_by": requested_by,
        "source": "admin_data_sources",
    }
    if existing is None:
        existing = DataEnrichmentJob(
            job_type=job_type,
            dedupe_key=dedupe_key,
            priority=10,
            status="queued",
            attempts=0,
            max_attempts=1,
            source="admin_data_sources",
            reason=normalized_mode,
            payload_json=json.dumps(payload, sort_keys=True),
            next_run_at=now,
        )
        db.add(existing)
    else:
        existing.status = "queued"
        existing.reason = normalized_mode
        existing.error = None
        existing.next_run_at = now
        existing.updated_at = now
        existing.payload_json = json.dumps(payload, sort_keys=True)
    db.flush()
    return {
        "status": "queued",
        "domain_key": domain_key,
        "mode": normalized_mode,
        "dry_run": True,
        "job": {
            "id": existing.id,
            "job_type": existing.job_type,
            "dedupe_key": existing.dedupe_key,
            "status": existing.status,
        },
    }
