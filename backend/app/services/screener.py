from __future__ import annotations

import csv
import logging
import os
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from io import StringIO
from math import isfinite
from typing import Any
from urllib.parse import quote

from sqlalchemy import and_, bindparam, func, inspect, select, text
from sqlalchemy.orm import Session

from app.clients.fmp import fetch_company_screener
from app.entitlements import TierEntitlements, premium_required_error, required_tier_for_feature
from app.models import FundamentalsCache, PriceCache, QuoteCache, TickerMeta
from app.services.confirmation_score import (
    normalize_confirmation_state,
    redact_confirmation_bundle_sources,
    slim_confirmation_score_bundle,
)
from app.services.confirmation_context import build_confirmation_score_context
from app.services.government_contracts import (
    DEFAULT_GOVERNMENT_CONTRACTS_LOOKBACK_DAYS,
    DEFAULT_GOVERNMENT_CONTRACTS_MIN_AMOUNT,
    get_government_contracts_overlay_availability,
    get_government_contracts_summaries_for_symbols,
    inactive_government_contracts_summary,
    unavailable_government_contracts_summary,
)
from app.services.fundamentals_cache import cached_screener_rows
from app.services.data_enrichment_queue import enqueue_data_enrichment_job
from app.services.intelligence_overlays import (
    DEFAULT_INSTITUTIONAL_ACTIVITY_LOOKBACK_DAYS,
    DEFAULT_OPTIONS_FLOW_LOOKBACK_DAYS,
    load_intelligence_feature_flags,
)
from app.services.technical_indicators import _ema, _rsi
from app.utils.symbols import normalize_symbol

MAX_PAGE_SIZE = 100
DEFAULT_PAGE_SIZE = 10
MAX_FETCH_ROWS = 500
MAX_EXPORT_ROWS = MAX_FETCH_ROWS
TECHNICAL_HISTORY_DAYS = 120

logger = logging.getLogger(__name__)

SUPPORTED_SORTS = {
    "relevance",
    "confirmation_score",
    "market_cap",
    "price",
    "volume",
    "avg_volume",
    "rel_volume",
    "price_move_pct",
    "rsi",
    "beta",
    "dividend_yield",
    "congress_activity",
    "insider_activity",
    "freshness",
    "symbol",
    "trailing_pe",
    "forward_pe",
    "price_sales",
    "ev_ebitda",
    "gross_margin",
    "operating_margin",
    "net_margin",
    "roe",
    "roic",
    "revenue_growth",
    "eps_growth",
    "ebitda_growth",
    "fcf_growth",
    "debt_equity",
    "current_ratio",
    "net_debt_ebitda",
    "eps_ttm",
    "fcf",
    "fcf_margin",
    "earnings_yield",
    "government_contracts_score_contribution",
    "government_contracts_count",
    "government_contracts_total_amount",
    "government_contracts_largest_amount",
    "options_flow_score",
    "options_flow_total_premium",
    "options_flow_call_put_premium_ratio",
    "institutional_activity_net_activity",
    "institutional_activity_institution_count",
    "institutional_activity_total_value",
    "institutional_activity_ownership_pct",
    "institutional_activity_holder_breadth",
    "institutional_activity_materiality_score",
}

PREMIUM_SORTS = {
    "confirmation_score",
    "congress_activity",
    "insider_activity",
    "freshness",
    "government_contracts_score_contribution",
    "government_contracts_count",
    "government_contracts_total_amount",
    "government_contracts_largest_amount",
    "options_flow_score",
    "options_flow_total_premium",
    "options_flow_call_put_premium_ratio",
    "institutional_activity_net_activity",
    "institutional_activity_institution_count",
    "institutional_activity_total_value",
    "institutional_activity_ownership_pct",
    "institutional_activity_holder_breadth",
    "institutional_activity_materiality_score",
}
OPTIONS_FLOW_FILTER_KEYS = {
    "options_flow_active",
    "options_flow_direction",
    "options_flow_min_score",
    "options_flow_min_premium",
}
OPTIONS_FLOW_SORTS = {
    "options_flow_score",
    "options_flow_total_premium",
    "options_flow_call_put_premium_ratio",
}
INSTITUTIONAL_ACTIVITY_FILTER_KEYS = {
    "institutional_activity_active",
    "institutional_activity_type",
    "institutional_activity_direction",
    "institutional_activity_min_value",
    "institutional_activity_min_ownership_pct",
    "institutional_activity_holder_breadth",
}
INSTITUTIONAL_ACTIVITY_SORTS = {
    "institutional_activity_net_activity",
    "institutional_activity_institution_count",
    "institutional_activity_total_value",
    "institutional_activity_ownership_pct",
    "institutional_activity_holder_breadth",
    "institutional_activity_materiality_score",
}
PREMIUM_SIGNAL_FILTER_KEYS = {
    "confirmation_score_min",
    "confirmation_direction",
    "confirmation_band",
    "why_now_state",
    "freshness",
}
PREMIUM_SIGNAL_SORTS = {
    "confirmation_score",
    "freshness",
}

NUMERIC_ROW_SORTS = {
    "market_cap",
    "price",
    "volume",
    "avg_volume",
    "rel_volume",
    "price_move_pct",
    "rsi",
    "beta",
    "dividend_yield",
    "trailing_pe",
    "forward_pe",
    "price_sales",
    "ev_ebitda",
    "gross_margin",
    "operating_margin",
    "net_margin",
    "roe",
    "roic",
    "revenue_growth",
    "eps_growth",
    "ebitda_growth",
    "fcf_growth",
    "debt_equity",
    "current_ratio",
    "net_debt_ebitda",
    "eps_ttm",
    "fcf",
    "fcf_margin",
    "earnings_yield",
    "government_contracts_score_contribution",
    "government_contracts_count",
    "government_contracts_total_amount",
    "government_contracts_largest_amount",
    "options_flow_score",
    "options_flow_total_premium",
    "options_flow_call_put_premium_ratio",
    "institutional_activity_net_activity",
    "institutional_activity_institution_count",
    "institutional_activity_total_value",
}

FMP_FILTER_MAP = {
    "market_cap_min": "marketCapMoreThan",
    "market_cap_max": "marketCapLowerThan",
    "price_min": "priceMoreThan",
    "price_max": "priceLowerThan",
    "volume_min": "volumeMoreThan",
    "beta_min": "betaMoreThan",
    "beta_max": "betaLowerThan",
    "dividend_yield_min": "dividendMoreThan",
    "dividend_yield_max": "dividendLowerThan",
    "sector": "sector",
    "industry": "industry",
    "country": "country",
    "exchange": "exchange",
}


@dataclass(frozen=True)
class FundamentalFilterSpec:
    row_field: str
    param_base: str
    attr_base: str | None = None
    aliases: tuple[str, ...] = ()

    @property
    def attribute_base(self) -> str:
        return self.attr_base or self.row_field

    @property
    def public_keys(self) -> tuple[str, str]:
        return (f"{self.param_base}_min", f"{self.param_base}_max")


FUNDAMENTAL_FILTER_SPECS: tuple[FundamentalFilterSpec, ...] = (
    FundamentalFilterSpec("trailing_pe", "trailing_pe"),
    FundamentalFilterSpec("forward_pe", "forward_pe"),
    FundamentalFilterSpec("price_sales", "price_to_sales", aliases=("price_sales",)),
    FundamentalFilterSpec("ev_ebitda", "ev_to_ebitda", aliases=("ev_ebitda",)),
    FundamentalFilterSpec("gross_margin", "gross_margin"),
    FundamentalFilterSpec("operating_margin", "operating_margin"),
    FundamentalFilterSpec("net_margin", "net_margin"),
    FundamentalFilterSpec("roe", "roe"),
    FundamentalFilterSpec("roic", "roic"),
    FundamentalFilterSpec("revenue_growth", "revenue_growth"),
    FundamentalFilterSpec("eps_growth", "eps_growth"),
    FundamentalFilterSpec("ebitda_growth", "ebitda_growth"),
    FundamentalFilterSpec("fcf_growth", "fcf_growth"),
    FundamentalFilterSpec("debt_equity", "debt_to_equity", aliases=("debt_equity",)),
    FundamentalFilterSpec("current_ratio", "current_ratio"),
    FundamentalFilterSpec("net_debt_ebitda", "net_debt_to_ebitda", aliases=("net_debt_ebitda",)),
    FundamentalFilterSpec("eps_ttm", "eps_ttm"),
    FundamentalFilterSpec("fcf", "free_cash_flow", aliases=("fcf",)),
    FundamentalFilterSpec("fcf_margin", "fcf_margin"),
    FundamentalFilterSpec("earnings_yield", "earnings_yield"),
)
FUNDAMENTAL_CACHE_FIELD_BY_ROW_FIELD = {
    "price_sales": "price_to_sales",
    "ev_ebitda": "ev_to_ebitda",
    "fcf": "free_cash_flow",
    "debt_equity": "debt_to_equity",
    "net_debt_ebitda": "net_debt_to_ebitda",
}
FUNDAMENTAL_ROW_FIELDS = tuple(spec.row_field for spec in FUNDAMENTAL_FILTER_SPECS)


@dataclass(frozen=True)
class ScreenerParams:
    page: int = 1
    page_size: int = DEFAULT_PAGE_SIZE
    sort: str = "relevance"
    sort_dir: str = "desc"
    lookback_days: int = 30
    market_cap_min: float | None = None
    market_cap_max: float | None = None
    price_min: float | None = None
    price_max: float | None = None
    volume_min: float | None = None
    beta_min: float | None = None
    beta_max: float | None = None
    dividend_yield_min: float | None = None
    dividend_yield_max: float | None = None
    sector: str | None = None
    industry: str | None = None
    country: str | None = None
    exchange: str | None = None
    congress_activity: str | None = None
    insider_activity: str | None = None
    confirmation_score_min: int | None = None
    confirmation_direction: str | None = None
    confirmation_band: str | None = None
    why_now_state: str | None = None
    freshness: str | None = None
    government_contracts_active: bool | None = None
    government_contracts_min_amount: float | None = DEFAULT_GOVERNMENT_CONTRACTS_MIN_AMOUNT
    government_contracts_lookback_days: int = DEFAULT_GOVERNMENT_CONTRACTS_LOOKBACK_DAYS
    options_flow_active: bool | None = None
    options_flow_direction: str | None = None
    options_flow_min_score: int | None = None
    options_flow_min_premium: float | None = None
    options_flow_lookback_days: int = DEFAULT_OPTIONS_FLOW_LOOKBACK_DAYS
    institutional_activity_active: bool | None = None
    institutional_activity_type: str | None = None
    institutional_activity_direction: str | None = None
    institutional_activity_min_value: float | None = None
    institutional_activity_min_ownership_pct: float | None = None
    institutional_activity_holder_breadth: str | None = None
    institutional_activity_lookback: str | None = None
    institutional_activity_lookback_days: int = DEFAULT_INSTITUTIONAL_ACTIVITY_LOOKBACK_DAYS
    rel_volume_min: float | None = None
    rel_volume_max: float | None = None
    price_move_min: float | None = None
    price_move_max: float | None = None
    rsi_min: float | None = None
    rsi_max: float | None = None
    macd_state: str | None = None
    trend_state: str | None = None
    trailing_pe_min: float | None = None
    trailing_pe_max: float | None = None
    forward_pe_min: float | None = None
    forward_pe_max: float | None = None
    price_sales_min: float | None = None
    price_sales_max: float | None = None
    ev_ebitda_min: float | None = None
    ev_ebitda_max: float | None = None
    gross_margin_min: float | None = None
    gross_margin_max: float | None = None
    operating_margin_min: float | None = None
    operating_margin_max: float | None = None
    net_margin_min: float | None = None
    net_margin_max: float | None = None
    roe_min: float | None = None
    roe_max: float | None = None
    roic_min: float | None = None
    roic_max: float | None = None
    revenue_growth_min: float | None = None
    revenue_growth_max: float | None = None
    eps_growth_min: float | None = None
    eps_growth_max: float | None = None
    ebitda_growth_min: float | None = None
    ebitda_growth_max: float | None = None
    fcf_growth_min: float | None = None
    fcf_growth_max: float | None = None
    debt_equity_min: float | None = None
    debt_equity_max: float | None = None
    current_ratio_min: float | None = None
    current_ratio_max: float | None = None
    net_debt_ebitda_min: float | None = None
    net_debt_ebitda_max: float | None = None
    eps_ttm_min: float | None = None
    eps_ttm_max: float | None = None
    fcf_min: float | None = None
    fcf_max: float | None = None
    fcf_margin_min: float | None = None
    fcf_margin_max: float | None = None
    earnings_yield_min: float | None = None
    earnings_yield_max: float | None = None


def screener_params_from_mapping(
    params: Mapping[str, Any],
    *,
    page: int = 1,
    page_size: int = DEFAULT_PAGE_SIZE,
) -> ScreenerParams:
    return ScreenerParams(
        page=page,
        page_size=page_size,
        sort=_string_param(params.get("sort")) or "relevance",
        sort_dir="asc" if _string_param(params.get("sort_dir")) == "asc" else "desc",
        lookback_days=_int_param(params.get("lookback_days")) or 30,
        market_cap_min=_float_param(params.get("market_cap_min")),
        market_cap_max=_float_param(params.get("market_cap_max")),
        price_min=_float_param(params.get("price_min")),
        price_max=_float_param(params.get("price_max")),
        volume_min=_float_param(params.get("volume_min")),
        beta_min=_float_param(params.get("beta_min")),
        beta_max=_float_param(params.get("beta_max")),
        dividend_yield_min=_float_param(params.get("dividend_yield_min")),
        dividend_yield_max=_float_param(params.get("dividend_yield_max")),
        sector=_string_param(params.get("sector")),
        industry=_string_param(params.get("industry")),
        country=_string_param(params.get("country")),
        exchange=_string_param(params.get("exchange")),
        congress_activity=_string_param(params.get("congress_activity")),
        insider_activity=_string_param(params.get("insider_activity")),
        confirmation_score_min=_int_param(params.get("confirmation_score_min")),
        confirmation_direction=_string_param(params.get("confirmation_direction")),
        confirmation_band=_string_param(params.get("confirmation_band")),
        why_now_state=_string_param(params.get("why_now_state")),
        freshness=_string_param(params.get("freshness")),
        government_contracts_active=_bool_param(params.get("government_contracts_active")),
        government_contracts_min_amount=_float_param(params.get("government_contracts_min_amount"))
        if params.get("government_contracts_min_amount") is not None
        else DEFAULT_GOVERNMENT_CONTRACTS_MIN_AMOUNT,
        government_contracts_lookback_days=_int_param(params.get("government_contracts_lookback_days"))
        or DEFAULT_GOVERNMENT_CONTRACTS_LOOKBACK_DAYS,
        options_flow_active=_bool_param(params.get("options_flow_active")),
        options_flow_direction=_string_param(params.get("options_flow_direction")),
        options_flow_min_score=_int_param(params.get("options_flow_min_score")),
        options_flow_min_premium=_float_param(params.get("options_flow_min_premium")),
        options_flow_lookback_days=_int_param(params.get("options_flow_lookback_days")) or DEFAULT_OPTIONS_FLOW_LOOKBACK_DAYS,
        institutional_activity_active=_bool_param(params.get("institutional_activity_active")),
        institutional_activity_type=_string_param(params.get("institutional_activity_type")),
        institutional_activity_direction=_string_param(params.get("institutional_activity_direction")),
        institutional_activity_min_value=_float_param(params.get("institutional_activity_min_value")),
        institutional_activity_min_ownership_pct=_float_param(params.get("institutional_activity_min_ownership_pct")),
        institutional_activity_holder_breadth=_string_param(params.get("institutional_activity_holder_breadth")),
        institutional_activity_lookback=_string_param(params.get("institutional_activity_lookback")),
        institutional_activity_lookback_days=_institutional_lookback_days(
            params.get("institutional_activity_lookback"),
            params.get("institutional_activity_lookback_days"),
        ),
        rel_volume_min=_float_param(params.get("rel_volume_min")),
        rel_volume_max=_float_param(params.get("rel_volume_max")),
        price_move_min=_float_param(params.get("price_move_min")),
        price_move_max=_float_param(params.get("price_move_max")),
        rsi_min=_float_param(params.get("rsi_min")),
        rsi_max=_float_param(params.get("rsi_max")),
        macd_state=_string_param(params.get("macd_state")),
        trend_state=_string_param(params.get("trend_state")),
        **_fundamental_param_kwargs(params),
    )


def build_screener_response(db: Session, params: ScreenerParams) -> dict[str, Any]:
    page = max(1, int(params.page or 1))
    page_size = max(1, min(int(params.page_size or DEFAULT_PAGE_SIZE), MAX_PAGE_SIZE))
    lookback_days = max(1, min(int(params.lookback_days or 30), 365))
    sort = params.sort if params.sort in SUPPORTED_SORTS else "relevance"
    sort_dir = "asc" if params.sort_dir == "asc" else "desc"
    dataset = _build_screener_dataset(db, params, requested_rows=_requested_rows(params, page=page, page_size=page_size))
    rows = dataset["rows"]
    total_available = int(dataset.get("total_available") or len(rows))

    start = (page - 1) * page_size
    end = start + page_size
    paged = rows[start:end]
    return {
        "items": paged,
        "page": page,
        "page_size": page_size,
        "returned": len(paged),
        "total_available": total_available,
        "has_next": end < len(rows),
        "sort": {"sort_by": sort, "sort_dir": sort_dir},
        "filters": _response_filters(params),
        "supported_filters": list(FMP_FILTER_MAP.keys()) + list(_intelligence_filter_keys()) + list(_technical_filter_keys()) + list(_fundamental_filter_keys()),
        "source": "fmp_company_screener",
        "lookback_days": lookback_days,
        "overlay_availability": dataset["overlay_availability"],
        "ignored_filters": dataset["ignored_filters"],
        "feature_flags": dataset["feature_flags"],
    }


def build_screener_response_for_entitlements(
    db: Session,
    params: ScreenerParams,
    *,
    entitlements: TierEntitlements,
) -> dict[str, Any]:
    result_cap = max(1, min(int(entitlements.limit("screener_results")), MAX_FETCH_ROWS))
    page = max(1, int(params.page or 1))
    page_size = max(1, min(int(params.page_size or DEFAULT_PAGE_SIZE), MAX_PAGE_SIZE, result_cap))
    lookback_days = max(1, min(int(params.lookback_days or 30), 365))
    sort = params.sort if params.sort in SUPPORTED_SORTS else "relevance"
    sort_dir = "asc" if params.sort_dir == "asc" else "desc"
    dataset = _build_screener_dataset(
        db,
        params,
        requested_rows=_requested_rows(params, page=page, page_size=page_size, row_cap=result_cap),
        entitlements=entitlements,
    )
    rows = dataset["rows"]
    if not entitlements.has_feature("options_flow_feed"):
        rows = redact_options_flow_rows(rows)
    if not entitlements.has_feature("institutional_feed"):
        rows = redact_institutional_activity_rows(rows)
    total_available = int(dataset.get("total_available") or len(rows))

    start = (page - 1) * page_size
    end = min(start + page_size, result_cap)
    paged = rows[start:end]
    visible_total = min(len(rows), result_cap)
    overlay_availability = dataset["overlay_availability"]
    return {
        "items": paged,
        "page": page,
        "page_size": page_size,
        "returned": len(paged),
        "total_available": total_available,
        "has_next": end < visible_total,
        "sort": {"sort_by": sort, "sort_dir": sort_dir},
        "filters": _response_filters(params),
        "supported_filters": list(FMP_FILTER_MAP.keys()) + list(_intelligence_filter_keys()) + list(_technical_filter_keys()) + list(_fundamental_filter_keys()),
        "source": "fmp_company_screener",
        "lookback_days": lookback_days,
        "overlay_availability": overlay_availability,
        "ignored_filters": dataset["ignored_filters"],
        "result_cap": result_cap,
        "access": {
            "tier": entitlements.tier,
            "intelligence_locked": not entitlements.has_feature("screener_intelligence"),
            "options_flow_locked": not entitlements.has_feature("options_flow_feed"),
            "institutional_activity_locked": not entitlements.has_feature("institutional_feed"),
            "presets_locked": not entitlements.has_feature("screener_presets"),
            "saved_screens_limit": entitlements.limit("screener_saved_screens"),
            "monitoring_locked": not entitlements.has_feature("screener_monitoring"),
            "csv_export_locked": not entitlements.has_feature("screener_csv_export"),
            "csv_export_required_plan": required_tier_for_feature(db, "screener_csv_export"),
            "feature_flags": dataset["feature_flags"],
        },
    }


def build_screener_rows(
    db: Session,
    params: ScreenerParams,
    *,
    requested_rows: int | None = None,
    entitlements: TierEntitlements | None = None,
) -> list[dict[str, Any]]:
    dataset = _build_screener_dataset(db, params, requested_rows=requested_rows, entitlements=entitlements)
    return dataset["rows"]


def _build_screener_dataset(
    db: Session,
    params: ScreenerParams,
    *,
    requested_rows: int | None = None,
    entitlements: TierEntitlements | None = None,
) -> dict[str, Any]:
    lookback_days = max(1, min(int(params.lookback_days or 30), 365))
    sort = params.sort if params.sort in SUPPORTED_SORTS else "relevance"
    sort_dir = "asc" if params.sort_dir == "asc" else "desc"
    fetch_limit = requested_rows if requested_rows is not None else _requested_rows(params, page=params.page, page_size=params.page_size)
    feature_flags = load_intelligence_feature_flags(db)
    government_contracts_lookback_days = max(
        1,
        min(int(params.government_contracts_lookback_days or DEFAULT_GOVERNMENT_CONTRACTS_LOOKBACK_DAYS), 365 * 3),
    )
    government_contracts_cutoff = (datetime.now(timezone.utc) - timedelta(days=government_contracts_lookback_days)).date()
    government_contracts_min_amount = params.government_contracts_min_amount or 0.0

    cache_fetch_limit = MAX_FETCH_ROWS if _has_cache_filters(params) else fetch_limit
    cache_filters = _cache_filters(params)
    cached_rows = cached_screener_rows(db, limit=cache_fetch_limit, filters=cache_filters)
    total_available: int | None = None
    if _uses_broad_core_universe(params):
        core_universe_rows, core_universe_total = _cached_core_universe_rows(
            db,
            limit=cache_fetch_limit,
            filters=cache_filters,
        )
        normalized_rows = _merge_screener_rows(core_universe_rows, cached_rows)
        if core_universe_total is not None:
            total_available = max(core_universe_total, len(normalized_rows))
    else:
        normalized_rows = cached_rows
    if not normalized_rows:
        enqueue_data_enrichment_job(
            job_type="fundamentals_universe",
            window_key=f"limit:{cache_fetch_limit}",
            source="page_load",
            reason="fundamentals_cache_empty",
            priority=50,
        )
    if not normalized_rows and _allow_provider_screener_fallback():
        fmp_filters = _fmp_filters(params)
        raw_rows = fetch_company_screener(filters=fmp_filters, limit=fetch_limit)
        normalized_rows = [_normalize_fmp_row(row) for row in raw_rows]
        normalized_rows = [row for row in normalized_rows if row is not None]
    normalized_rows = [row for row in normalized_rows if _matches_core_filters(row, params)]
    if _has_technical_filters(params):
        normalized_rows = _enrich_rows_with_cached_technicals(db, normalized_rows)

    candidate_symbols = [row["symbol"] for row in normalized_rows]
    government_contracts_availability = get_government_contracts_overlay_availability(
        db,
        feature_enabled=feature_flags["feature_government_contracts_enabled"],
    )
    if feature_flags["feature_government_contracts_enabled"]:
        government_contracts_summaries = get_government_contracts_summaries_for_symbols(
            db,
            candidate_symbols,
            lookback_days=government_contracts_lookback_days,
            min_amount=params.government_contracts_min_amount,
        )
    else:
        government_contracts_summaries = {
            symbol: unavailable_government_contracts_summary()
            for symbol in candidate_symbols
        }
    if government_contracts_availability.get("status") != "ok":
        government_contracts_summaries = {
            symbol: unavailable_government_contracts_summary()
            for symbol in candidate_symbols
        }

    if params.government_contracts_active is not None:
        matching_government_symbols = sorted(
            symbol
            for symbol, summary in government_contracts_summaries.items()
            if summary.get("active") is True
        )
        if government_contracts_availability.get("filterable") is True:
            normalized_rows = [
                row
                for row in normalized_rows
                if _matches_boolean_filter(
                    (government_contracts_summaries.get(row["symbol"]) or {}).get("active"),
                    params.government_contracts_active,
                )
            ]
        logger.info(
            "screener_gov_filter incoming_active=%s cutoff_date=%s min_amount=%s matching_symbols=%s sample=%s rows_after_filter=%s availability=%s",
            params.government_contracts_active,
            government_contracts_cutoff.isoformat(),
            government_contracts_min_amount,
            len(matching_government_symbols),
            matching_government_symbols[:10],
            len(normalized_rows),
            government_contracts_availability.get("status"),
        )

    symbols = [row["symbol"] for row in normalized_rows]
    confirmation_context = build_confirmation_score_context(
        db,
        symbols,
        lookback_days=lookback_days,
        government_contracts_lookback_days=government_contracts_lookback_days,
        government_contracts_min_amount=params.government_contracts_min_amount,
        options_flow_lookback_days=params.options_flow_lookback_days or DEFAULT_OPTIONS_FLOW_LOOKBACK_DAYS,
        institutional_activity_lookback_days=params.institutional_activity_lookback_days or DEFAULT_INSTITUTIONAL_ACTIVITY_LOOKBACK_DAYS,
        feature_flags=feature_flags,
        government_contracts_availability=government_contracts_availability,
        government_contracts_summaries=government_contracts_summaries,
    )
    bundles = confirmation_context["bundles"]
    government_contracts_summaries = confirmation_context["government_contracts_summaries"]
    options_flow_summaries = confirmation_context["options_flow_summaries"]
    institutional_activity_summaries = confirmation_context["institutional_activity_summaries"]
    overlay_availability = _overlay_availability_for_entitlements(
        confirmation_context["overlay_availability"],
        entitlements,
    )
    if entitlements is not None:
        premium_locked_sources: set[str] = set()
        pro_locked_sources: set[str] = set()
        if not entitlements.has_feature("signals"):
            premium_locked_sources.add("signals")
        if not entitlements.has_feature("options_flow_feed"):
            pro_locked_sources.add("options_flow")
            options_flow_summaries = {
                symbol: _locked_options_flow_summary(symbol)
                for symbol in symbols
            }
        if not entitlements.has_feature("institutional_feed"):
            pro_locked_sources.add("institutional_activity")
            institutional_activity_summaries = {
                symbol: _locked_institutional_activity_summary()
                for symbol in symbols
            }
        if premium_locked_sources:
            bundles = {
                symbol: redact_confirmation_bundle_sources(
                    bundle,
                    premium_locked_sources,
                    lock_state="premium_locked",
                    required_plan="premium",
                )
                for symbol, bundle in bundles.items()
            }
        if pro_locked_sources:
            bundles = {
                symbol: redact_confirmation_bundle_sources(
                    bundle,
                    pro_locked_sources,
                    lock_state="pro_locked",
                    required_plan="pro",
                )
                for symbol, bundle in bundles.items()
            }
    rows = [
        _enrich_row(
            row,
            bundles.get(row["symbol"]),
            lookback_days=lookback_days,
            government_contracts_summary=government_contracts_summaries.get(row["symbol"]),
            options_flow_summary=options_flow_summaries.get(row["symbol"]),
            institutional_activity_summary=institutional_activity_summaries.get(row["symbol"]),
        )
        for row in normalized_rows
    ]
    ignored_filters = _ignored_overlay_filters(params, overlay_availability)
    technical_diagnostics = _technical_filter_summary(rows, params)
    if _has_technical_filters(params) and technical_diagnostics.get("rows_missing_technical_data"):
        for symbol in candidate_symbols[:100]:
            enqueue_data_enrichment_job(
                job_type="price_series",
                symbol=symbol,
                window_key=f"technical:{TECHNICAL_HISTORY_DAYS}d",
                source="page_load",
                reason="missing_technical_cache",
                priority=60,
            )
    rows = [row for row in rows if _row_matches_filters(row, params, overlay_availability=overlay_availability)]
    if _has_technical_filters(params) and not rows:
        logger.info("screener_technical_filter_no_results diagnostics=%s", technical_diagnostics)
    if params.government_contracts_active is not None:
        logger.info(
            "screener_gov_filter final_rows=%s sample=%s",
            len(rows),
            [row["symbol"] for row in rows[:10]],
        )
    if (
        total_available is None
        or _has_technical_filters(params)
        or _has_fundamental_filters(params)
        or _has_intelligence_filters(params)
        or _has_source_derived_filters(params)
    ):
        total_available = len(rows)
    rows.sort(key=lambda row: _sort_key(row, sort), reverse=sort_dir == "desc")
    return {
        "rows": rows,
        "total_available": total_available,
        "overlay_availability": overlay_availability,
        "ignored_filters": ignored_filters,
        "feature_flags": feature_flags,
    }


def build_screener_csv_export(
    db: Session,
    params: ScreenerParams,
    *,
    row_cap: int = MAX_EXPORT_ROWS,
) -> tuple[str, int]:
    rows = build_screener_rows(db, params, requested_rows=max(1, min(int(row_cap), MAX_EXPORT_ROWS)))
    output = StringIO(newline="")
    writer = csv.writer(output)
    writer.writerow(
        [
            "Symbol",
            "Company",
            "Sector",
            "Industry",
            "Country",
            "Exchange",
            "Market Cap",
            "Price",
            "Volume",
            "Beta",
            "Congress Activity",
            "Insider Activity",
            "Confirmation Score",
            "Confirmation Direction",
            "Confirmation Band",
            "Confirmation Status",
            "Why Now State",
            "Why Now Headline",
            "Freshness State",
            "Government Contracts Active",
            "Government Contracts Score Contribution",
            "Government Contracts Count",
            "Government Contracts Total Amount",
            "Government Contracts Largest Amount",
            "Government Contracts Latest Date",
            "Government Contracts Top Agency",
            "Options Flow Active",
            "Options Flow Score",
            "Options Flow Direction",
            "Options Flow Intensity",
            "Options Flow Total Premium",
            "Options Flow Latest Date",
            "Institutional Activity Active",
            "Institutional Activity Direction",
            "Institutional Activity Net Activity",
            "Institutional Activity Total Value",
            "Institutional Activity Latest Date",
            "Institutional Activity Status",
        ]
    )
    for row in rows:
        confirmation = row.get("confirmation") if isinstance(row.get("confirmation"), dict) else {}
        why_now = row.get("why_now") if isinstance(row.get("why_now"), dict) else {}
        freshness = row.get("signal_freshness") if isinstance(row.get("signal_freshness"), dict) else {}
        congress = row.get("congress_activity") if isinstance(row.get("congress_activity"), dict) else {}
        insiders = row.get("insider_activity") if isinstance(row.get("insider_activity"), dict) else {}
        writer.writerow(
            [
                row.get("symbol") or "",
                row.get("company_name") or "",
                row.get("sector") or "",
                row.get("industry") or "",
                row.get("country") or "",
                row.get("exchange") or "",
                _csv_number(row.get("market_cap")),
                _csv_number(row.get("price")),
                _csv_number(row.get("volume")),
                _csv_number(row.get("beta")),
                congress.get("label") or "",
                insiders.get("label") or "",
                _csv_number(confirmation.get("score"), digits=0),
                _csv_label(confirmation.get("direction")),
                _csv_label(confirmation.get("band")),
                confirmation.get("status") or "",
                _csv_label(why_now.get("state")),
                why_now.get("headline") or "",
                _csv_label(freshness.get("freshness_state")),
                row.get("government_contracts_active"),
                _csv_number(row.get("government_contracts_score_contribution"), digits=0),
                _csv_number(row.get("government_contracts_count"), digits=0),
                _csv_number(row.get("government_contracts_total_amount")),
                _csv_number(row.get("government_contracts_largest_amount")),
                row.get("government_contracts_latest_date") or "",
                row.get("government_contracts_top_agency") or "",
                row.get("options_flow_active"),
                _csv_number(row.get("options_flow_score"), digits=0),
                _csv_label(row.get("options_flow_direction")),
                _csv_label(row.get("options_flow_intensity")),
                _csv_number(row.get("options_flow_total_premium")),
                row.get("options_flow_latest_date") or "",
                row.get("institutional_activity_active"),
                _csv_label(row.get("institutional_activity_direction")),
                _csv_number(row.get("institutional_activity_net_activity")),
                _csv_number(row.get("institutional_activity_total_value")),
                row.get("institutional_activity_latest_date") or "",
                row.get("institutional_activity_status") or "",
            ]
        )
    return output.getvalue(), len(rows)


def _requested_rows(params: ScreenerParams, *, page: int, page_size: int, row_cap: int = MAX_FETCH_ROWS) -> int:
    requested_rows = min(MAX_FETCH_ROWS, row_cap, max(page * page_size + 1, page_size))
    if (
        _has_intelligence_filters(params)
        or _has_source_derived_filters(params)
        or params.sort in PREMIUM_SORTS
        or _has_technical_filters(params)
        or _has_fundamental_filters(params)
    ):
        requested_rows = min(MAX_FETCH_ROWS, row_cap)
    return requested_rows


def _uses_broad_core_universe(params: ScreenerParams) -> bool:
    return _has_core_filters(params) and not _has_fundamental_filters(params)


def _merge_screener_rows(*row_groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for rows in row_groups:
        for row in rows:
            symbol = normalize_symbol(row.get("symbol"))
            if not symbol or symbol in merged:
                continue
            merged[symbol] = {**row, "symbol": symbol}
    return list(merged.values())


def _cached_core_universe_rows(
    db: Session,
    *,
    limit: int,
    filters: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], int | None]:
    latest_dates = (
        select(PriceCache.symbol.label("symbol"), func.max(PriceCache.date).label("latest_date"))
        .group_by(PriceCache.symbol)
        .subquery()
    )
    latest_prices = (
        select(
            PriceCache.symbol.label("symbol"),
            PriceCache.close.label("close"),
            PriceCache.volume.label("volume"),
            PriceCache.day_volume.label("day_volume"),
        )
        .join(
            latest_dates,
            and_(
                latest_dates.c.symbol == PriceCache.symbol,
                latest_dates.c.latest_date == PriceCache.date,
            ),
        )
        .subquery()
    )
    price_column = func.coalesce(QuoteCache.price, latest_prices.c.close, FundamentalsCache.price)
    volume_column = func.coalesce(latest_prices.c.volume, latest_prices.c.day_volume, FundamentalsCache.volume)
    columns = {
        "symbol": TickerMeta.symbol.label("symbol"),
        "company_name": func.coalesce(
            TickerMeta.company_name,
            FundamentalsCache.company_name,
            TickerMeta.symbol,
        ).label("company_name"),
        "sector": func.coalesce(TickerMeta.sector, FundamentalsCache.sector).label("sector"),
        "industry": func.coalesce(TickerMeta.industry, FundamentalsCache.industry).label("industry"),
        "country": func.coalesce(TickerMeta.country, FundamentalsCache.country).label("country"),
        "exchange": func.coalesce(TickerMeta.exchange, FundamentalsCache.exchange).label("exchange"),
        "market_cap": FundamentalsCache.market_cap.label("market_cap"),
        "price": price_column.label("price"),
        "volume": volume_column.label("volume"),
        "avg_volume": FundamentalsCache.avg_volume.label("avg_volume"),
        "beta": FundamentalsCache.beta.label("beta"),
        "dividend_yield": FundamentalsCache.dividend_yield.label("dividend_yield"),
    }
    for row_field in FUNDAMENTAL_ROW_FIELDS:
        cache_field = FUNDAMENTAL_CACHE_FIELD_BY_ROW_FIELD.get(row_field, row_field)
        columns[row_field] = getattr(FundamentalsCache, cache_field).label(row_field)

    from_clause = (
        TickerMeta.__table__
        .outerjoin(QuoteCache.__table__, QuoteCache.symbol == TickerMeta.symbol)
        .outerjoin(latest_prices, latest_prices.c.symbol == TickerMeta.symbol)
        .outerjoin(
            FundamentalsCache.__table__,
            and_(
                FundamentalsCache.symbol == TickerMeta.symbol,
                FundamentalsCache.provider == "fmp",
                FundamentalsCache.status == "ok",
            ),
        )
    )
    query = select(*columns.values()).select_from(from_clause)
    query = _apply_core_universe_filters(
        query,
        filters,
        price_column=price_column,
        volume_column=volume_column,
        identity_columns=columns,
    )
    query = query.order_by(
        FundamentalsCache.market_cap.desc().nullslast(),
        TickerMeta.symbol.asc(),
    ).limit(max(1, int(limit)))

    count_query = select(func.count()).select_from(from_clause)
    count_query = _apply_core_universe_filters(
        count_query,
        filters,
        price_column=price_column,
        volume_column=volume_column,
        identity_columns=columns,
    )
    try:
        total = db.execute(count_query).scalar_one()
        result = db.execute(query).mappings().all()
    except Exception:
        logger.exception("screener_core_universe_query_failed")
        return [], None

    rows: list[dict[str, Any]] = []
    for record in result:
        symbol = normalize_symbol(record.get("symbol"))
        if not symbol:
            continue
        volume = _number(record.get("volume"))
        avg_volume = _number(record.get("avg_volume"))
        payload = dict(record)
        payload["symbol"] = symbol
        payload["company_name"] = payload.get("company_name") or symbol
        payload["rel_volume"] = _relative_volume(volume, avg_volume)
        payload["price_move_pct"] = None
        payload["rsi"] = None
        payload["macd_state"] = None
        payload["trend_state"] = None
        rows.append(payload)
    return rows, int(total or 0)


def _apply_core_universe_filters(
    query,
    filters: Mapping[str, Any],
    *,
    price_column,
    volume_column,
    identity_columns: Mapping[str, Any],
):
    query = _apply_core_universe_range_filter(
        query,
        FundamentalsCache.market_cap,
        filters.get("market_cap_min"),
        filters.get("market_cap_max"),
    )
    query = _apply_core_universe_range_filter(query, price_column, filters.get("price_min"), filters.get("price_max"))
    query = _apply_core_universe_range_filter(query, volume_column, filters.get("volume_min"), None)
    query = _apply_core_universe_range_filter(query, FundamentalsCache.beta, filters.get("beta_min"), filters.get("beta_max"))
    query = _apply_core_universe_range_filter(
        query,
        FundamentalsCache.dividend_yield,
        filters.get("dividend_yield_min"),
        filters.get("dividend_yield_max"),
    )
    for field in ("sector", "industry", "country", "exchange"):
        query = _apply_core_universe_text_filter(query, identity_columns[field], filters.get(field))
    return query


def _apply_core_universe_range_filter(query, column, minimum: Any, maximum: Any):
    min_value = _number(minimum)
    max_value = _number(maximum)
    if min_value is not None:
        query = query.where(column >= min_value)
    if max_value is not None:
        query = query.where(column <= max_value)
    return query


def _apply_core_universe_text_filter(query, column, value: Any):
    expected_values = _normalized_filter_values(value)
    if not expected_values:
        return query
    return query.where(func.lower(column).in_(sorted(expected_values)))


def _has_cache_filters(params: ScreenerParams) -> bool:
    return _has_core_filters(params) or _has_fundamental_filters(params)


def _cache_filters(params: ScreenerParams) -> dict[str, Any]:
    filters: dict[str, Any] = {}
    for public_name in FMP_FILTER_MAP:
        value = getattr(params, public_name)
        if value is not None:
            filters[public_name] = value

    for spec in FUNDAMENTAL_FILTER_SPECS:
        minimum, maximum = _fundamental_range(params, spec)
        if minimum is None and maximum is None:
            continue
        cache_field = FUNDAMENTAL_CACHE_FIELD_BY_ROW_FIELD.get(spec.row_field, spec.row_field)
        filters[f"{cache_field}_min"] = minimum
        filters[f"{cache_field}_max"] = maximum

    return filters


def _allow_provider_screener_fallback() -> bool:
    if not os.getenv("PYTEST_CURRENT_TEST"):
        return False
    return (os.getenv("SCREENER_PROVIDER_FALLBACK") or "").strip().lower() in {"1", "true", "yes"}


def _fmp_filters(params: ScreenerParams) -> dict[str, Any]:
    filters: dict[str, Any] = {}
    for public_name, fmp_name in FMP_FILTER_MAP.items():
        value = getattr(params, public_name)
        if value is None:
            continue
        if isinstance(value, str):
            cleaned = value.strip()
            if not cleaned:
                continue
            filters[fmp_name] = cleaned
            continue
        filters[fmp_name] = value
    return filters


def _response_filters(params: ScreenerParams) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for public_name in FMP_FILTER_MAP:
        value = getattr(params, public_name)
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        result[public_name] = value
    for public_name in _intelligence_filter_keys():
        value = getattr(params, public_name)
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        result[public_name] = value
    for public_name in _technical_filter_keys():
        value = getattr(params, public_name)
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        result[public_name] = value
    for spec in FUNDAMENTAL_FILTER_SPECS:
        for suffix in ("min", "max"):
            value = getattr(params, f"{spec.attribute_base}_{suffix}")
            if value is not None:
                result[f"{spec.param_base}_{suffix}"] = value
    return result


def _intelligence_filter_keys() -> tuple[str, ...]:
    return (
        "congress_activity",
        "insider_activity",
        "confirmation_score_min",
        "confirmation_direction",
        "confirmation_band",
        "why_now_state",
        "freshness",
        "government_contracts_active",
        "government_contracts_min_amount",
        "government_contracts_lookback_days",
        "options_flow_active",
        "options_flow_direction",
        "options_flow_min_score",
        "options_flow_min_premium",
        "options_flow_lookback_days",
        "institutional_activity_active",
        "institutional_activity_type",
        "institutional_activity_direction",
        "institutional_activity_min_value",
        "institutional_activity_min_ownership_pct",
        "institutional_activity_holder_breadth",
        "institutional_activity_lookback",
        "institutional_activity_lookback_days",
    )


def _technical_filter_keys() -> tuple[str, ...]:
    return (
        "rel_volume_min",
        "rel_volume_max",
        "price_move_min",
        "price_move_max",
        "rsi_min",
        "rsi_max",
        "macd_state",
        "trend_state",
    )


def _fundamental_filter_keys() -> tuple[str, ...]:
    return tuple(key for spec in FUNDAMENTAL_FILTER_SPECS for key in spec.public_keys)


def _fundamental_input_keys(spec: FundamentalFilterSpec, suffix: str) -> tuple[str, ...]:
    return (f"{spec.param_base}_{suffix}", *(f"{alias}_{suffix}" for alias in spec.aliases))


def _first_mapping_value(params: Mapping[str, Any], keys: tuple[str, ...]) -> Any:
    for key in keys:
        if key in params:
            return params.get(key)
    return None


def _fundamental_param_kwargs(params: Mapping[str, Any]) -> dict[str, float | None]:
    parsed: dict[str, float | None] = {}
    for spec in FUNDAMENTAL_FILTER_SPECS:
        parsed[f"{spec.attribute_base}_min"] = _float_param(
            _first_mapping_value(params, _fundamental_input_keys(spec, "min"))
        )
        parsed[f"{spec.attribute_base}_max"] = _float_param(
            _first_mapping_value(params, _fundamental_input_keys(spec, "max"))
        )
    return parsed


def _fundamental_range(params: ScreenerParams, spec: FundamentalFilterSpec) -> tuple[float | None, float | None]:
    return (
        getattr(params, f"{spec.attribute_base}_min"),
        getattr(params, f"{spec.attribute_base}_max"),
    )


def _has_intelligence_filters(params: ScreenerParams) -> bool:
    return any(
        getattr(params, key) is not None
        for key in PREMIUM_SIGNAL_FILTER_KEYS
    )


def _has_public_source_filters(params: ScreenerParams) -> bool:
    return (
        _normalized_str(params.congress_activity) is not None
        or _normalized_str(params.insider_activity) is not None
        or params.government_contracts_active is not None
    )


def _has_options_flow_filters(params: ScreenerParams) -> bool:
    if any(getattr(params, key) is not None for key in OPTIONS_FLOW_FILTER_KEYS):
        return True
    return int(params.options_flow_lookback_days or DEFAULT_OPTIONS_FLOW_LOOKBACK_DAYS) != DEFAULT_OPTIONS_FLOW_LOOKBACK_DAYS


def _has_institutional_activity_filters(params: ScreenerParams) -> bool:
    if any(getattr(params, key) is not None for key in INSTITUTIONAL_ACTIVITY_FILTER_KEYS):
        return True
    return (
        int(params.institutional_activity_lookback_days or DEFAULT_INSTITUTIONAL_ACTIVITY_LOOKBACK_DAYS)
        != DEFAULT_INSTITUTIONAL_ACTIVITY_LOOKBACK_DAYS
    )


def _has_source_derived_filters(params: ScreenerParams) -> bool:
    return _has_public_source_filters(params) or _has_options_flow_filters(params) or _has_institutional_activity_filters(params)


def _has_core_filters(params: ScreenerParams) -> bool:
    return any(getattr(params, key) is not None for key in FMP_FILTER_MAP)


def _has_technical_filters(params: ScreenerParams) -> bool:
    return any(getattr(params, key) is not None for key in _technical_filter_keys())


def _has_fundamental_filters(params: ScreenerParams) -> bool:
    return any(any(value is not None for value in _fundamental_range(params, spec)) for spec in FUNDAMENTAL_FILTER_SPECS)


def has_intelligence_sort(params: ScreenerParams) -> bool:
    return params.sort in PREMIUM_SIGNAL_SORTS


def require_screener_intelligence_access(params: ScreenerParams, entitlements: TierEntitlements) -> None:
    if (_has_options_flow_filters(params) or params.sort in OPTIONS_FLOW_SORTS) and not entitlements.has_feature("options_flow_filters"):
        raise premium_required_error(
            feature="options_flow_filters",
            message="Options flow screener filters, sorts, columns, and overlays require Pro.",
            entitlements=entitlements,
        )
    if (
        _has_institutional_activity_filters(params) or params.sort in INSTITUTIONAL_ACTIVITY_SORTS
    ) and not entitlements.has_feature("institutional_filters"):
        raise premium_required_error(
            feature="institutional_filters",
            message="Institutional activity screener filters, sorts, columns, and overlays require Pro.",
            entitlements=entitlements,
        )
    if entitlements.has_feature("screener_intelligence"):
        return
    if not _has_intelligence_filters(params) and not has_intelligence_sort(params):
        return
    raise premium_required_error(
        feature="screener_intelligence",
        message="Signal confirmation, Why Now, and freshness screener filters are included with Premium.",
        entitlements=entitlements,
    )


def redact_intelligence_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [_redact_intelligence_row(row) for row in rows]


def redact_options_flow_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [_redact_options_flow_row(row) for row in rows]


def redact_institutional_activity_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [_redact_institutional_activity_row(row) for row in rows]


def _number(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        parsed = float(value)
    elif isinstance(value, str):
        cleaned = value.replace("$", "").replace(",", "").replace("%", "").strip()
        if not cleaned:
            return None
        try:
            parsed = float(cleaned)
        except ValueError:
            return None
    else:
        return None
    return parsed if isfinite(parsed) else None


def _text(row: dict[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = row.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _first_number(row: dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        value = _number(row.get(key))
        if value is not None:
            return value
    return None


def _percent_value(row: dict[str, Any], *keys: str) -> float | None:
    value = _first_number(row, *keys)
    if value is None:
        return None
    return value * 100 if abs(value) <= 1 else value


def _relative_volume(volume: float | None, avg_volume: float | None) -> float | None:
    if volume is None or avg_volume is None or avg_volume <= 0:
        return None
    return volume / avg_volume


def _cached_price_histories(
    db: Session,
    symbols: list[str],
    *,
    end_date: date | None = None,
    history_days: int = TECHNICAL_HISTORY_DAYS,
) -> dict[str, list[float]]:
    unique_symbols = sorted({symbol for symbol in symbols if symbol})
    if not unique_symbols:
        return {}

    end = end_date or datetime.now(timezone.utc).date()
    start = end - timedelta(days=max(history_days - 1, 0))
    rows = db.execute(
        select(PriceCache.symbol, PriceCache.date, PriceCache.close)
        .where(PriceCache.symbol.in_(unique_symbols))
        .where(PriceCache.date >= start.isoformat())
        .where(PriceCache.date <= end.isoformat())
        .order_by(PriceCache.symbol.asc(), PriceCache.date.asc())
    ).all()
    histories: dict[str, list[float]] = {symbol: [] for symbol in unique_symbols}
    for symbol, _day, close in rows:
        parsed = _number(close)
        if parsed is not None and parsed > 0:
            histories.setdefault(str(symbol), []).append(parsed)
    return histories


def _cached_average_volumes(
    db: Session,
    symbols: list[str],
    *,
    end_date: date | None = None,
    history_days: int = 30,
) -> dict[str, float]:
    unique_symbols = sorted({symbol for symbol in symbols if symbol})
    if not unique_symbols:
        return {}

    bind = db.get_bind()
    if bind is None:
        return {}
    try:
        columns = {column["name"] for column in inspect(bind).get_columns("price_cache")}
    except Exception:
        return {}
    volume_column = next((column for column in ("volume", "day_volume", "avg_volume") if column in columns), None)
    if volume_column is None:
        return {}

    end = end_date or datetime.now(timezone.utc).date()
    start = end - timedelta(days=max(history_days - 1, 0))
    query = (
        text(
            f"""
            select symbol, avg({volume_column}) as avg_volume
            from price_cache
            where symbol in :symbols
              and date >= :start_date
              and date <= :end_date
              and {volume_column} is not null
            group by symbol
            """
        )
        .bindparams(bindparam("symbols", expanding=True))
    )
    try:
        rows = db.execute(
            query,
            {
                "symbols": unique_symbols,
                "start_date": start.isoformat(),
                "end_date": end.isoformat(),
            },
        ).all()
    except Exception:
        return {}

    averages: dict[str, float] = {}
    for symbol, avg_volume in rows:
        parsed = _number(avg_volume)
        if parsed is not None and parsed > 0:
            averages[str(symbol)] = parsed
    return averages


def _cached_price_move_pct(closes: list[float]) -> float | None:
    if len(closes) < 2 or closes[-2] <= 0:
        return None
    return round(((closes[-1] / closes[-2]) - 1.0) * 100.0, 4)


def _cached_macd_state(closes: list[float]) -> str | None:
    if len(closes) < 35:
        return None
    ema12 = _ema(closes, 12)
    ema26 = _ema(closes, 26)
    macd_line = [short - long for short, long in zip(ema12, ema26)]
    signal_series = _ema(macd_line, 9)
    if not macd_line or not signal_series:
        return None
    macd = macd_line[-1]
    signal = signal_series[-1]
    if len(macd_line) >= 2 and len(signal_series) >= 2:
        previous_macd = macd_line[-2]
        previous_signal = signal_series[-2]
        if previous_macd <= previous_signal and macd > signal:
            return "crossover_bullish"
        if previous_macd >= previous_signal and macd < signal:
            return "crossover_bearish"
    return "bullish" if macd > signal else "bearish" if macd < signal else None


def _cached_trend_state(closes: list[float]) -> str | None:
    if len(closes) < 26:
        return None
    short_ema = _ema(closes, 12)[-1]
    medium_ema = _ema(closes, 26)[-1]
    if short_ema > medium_ema:
        return "sma_above_lma"
    if short_ema < medium_ema:
        return "sma_below_lma"
    return None


def _enrich_rows_with_cached_technicals(db: Session, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    histories = _cached_price_histories(db, [row["symbol"] for row in rows])
    average_volumes = _cached_average_volumes(db, [row["symbol"] for row in rows])
    if not histories and not average_volumes:
        return rows

    enriched_rows: list[dict[str, Any]] = []
    for row in rows:
        closes = histories.get(row["symbol"]) or []
        average_volume = average_volumes.get(row["symbol"])
        if not closes and average_volume is None:
            enriched_rows.append(row)
            continue
        enriched = dict(row)
        if enriched.get("avg_volume") is None and average_volume is not None:
            enriched["avg_volume"] = average_volume
        if enriched.get("rel_volume") is None:
            enriched["rel_volume"] = _relative_volume(_number(enriched.get("volume")), _number(enriched.get("avg_volume")))
        if enriched.get("price_move_pct") is None:
            enriched["price_move_pct"] = _cached_price_move_pct(closes)
        if enriched.get("rsi") is None:
            rsi = _rsi(closes, 14)
            enriched["rsi"] = round(rsi, 2) if rsi is not None else None
        if enriched.get("macd_state") is None:
            enriched["macd_state"] = _cached_macd_state(closes)
        if enriched.get("trend_state") is None:
            enriched["trend_state"] = _cached_trend_state(closes)
        enriched_rows.append(enriched)
    return enriched_rows


def _macd_state(row: dict[str, Any]) -> str | None:
    explicit = _text(row, "macdState", "macd_state", "macdSignalState", "macd_signal_state")
    if explicit:
        normalized = explicit.lower().replace("-", "_").replace(" ", "_")
        if "crossover" in normalized and "bear" in normalized:
            return "crossover_bearish"
        if "crossover" in normalized and "bull" in normalized:
            return "crossover_bullish"
        if "bear" in normalized:
            return "bearish"
        if "bull" in normalized:
            return "bullish"
    macd = _first_number(row, "macd", "macdLine", "macd_line")
    signal = _first_number(row, "macdSignal", "macd_signal", "signalLine")
    if macd is None or signal is None:
        return None
    previous_macd = _first_number(row, "previousMacd", "macdPrevious", "prevMacd", "previous_macd")
    previous_signal = _first_number(row, "previousMacdSignal", "macdSignalPrevious", "prevMacdSignal", "previous_macd_signal")
    if previous_macd is not None and previous_signal is not None:
        if previous_macd <= previous_signal and macd > signal:
            return "crossover_bullish"
        if previous_macd >= previous_signal and macd < signal:
            return "crossover_bearish"
    return "bullish" if macd > signal else "bearish" if macd < signal else None


def _trend_state(row: dict[str, Any]) -> str | None:
    explicit = _text(row, "trendState", "trend_state", "smaTrend", "sma_trend")
    if explicit:
        normalized = explicit.lower().replace("-", "_").replace(" ", "_")
        if "below" in normalized or "<" in normalized or "bear" in normalized:
            return "sma_below_lma"
        if "above" in normalized or ">" in normalized or "bull" in normalized:
            return "sma_above_lma"
    short_ma = _first_number(row, "sma", "shortSma", "sma50", "sma_50", "emaShort", "ema_short", "ema20")
    long_ma = _first_number(row, "lma", "longSma", "sma200", "sma_200", "emaLong", "ema_long", "ema50")
    if short_ma is None or long_ma is None:
        return None
    return "sma_above_lma" if short_ma > long_ma else "sma_below_lma" if short_ma < long_ma else None


def _normalize_fmp_row(row: dict[str, Any]) -> dict[str, Any] | None:
    symbol = normalize_symbol(_text(row, "symbol", "ticker"))
    if not symbol:
        return None

    company_name = _text(row, "companyName", "company_name", "name") or symbol
    volume = _first_number(row, "volume", "dayVolume")
    avg_volume = _first_number(row, "avgVolume", "averageVolume", "avg_volume", "volumeAvg", "volume_avg")
    return {
        "symbol": symbol,
        "company_name": company_name,
        "sector": _text(row, "sector"),
        "industry": _text(row, "industry"),
        "market_cap": _first_number(row, "marketCap", "market_cap"),
        "price": _number(row.get("price")),
        "volume": volume if volume is not None else avg_volume,
        "avg_volume": avg_volume,
        "rel_volume": _relative_volume(volume, avg_volume),
        "price_move_pct": _percent_value(row, "price_move_pct", "priceMovePct", "changesPercentage", "changePercentage", "changePercent"),
        "rsi": _first_number(row, "rsi", "rsi14", "rsi_14"),
        "macd_state": _macd_state(row),
        "trend_state": _trend_state(row),
        "beta": _number(row.get("beta")),
        "country": _text(row, "country"),
        "exchange": _text(row, "exchangeShortName", "exchange", "exchangeName"),
        "dividend_yield": _percent_value(row, "dividendYield", "dividend_yield", "yield"),
        "trailing_pe": _first_number(row, "pe", "peRatio", "trailingPE", "trailing_pe"),
        "forward_pe": _first_number(row, "forwardPE", "forwardPe", "forward_pe"),
        "price_sales": _first_number(row, "priceToSalesRatio", "priceSalesRatio", "priceToSales", "price_to_sales", "price_sales", "psRatio"),
        "ev_ebitda": _first_number(row, "enterpriseValueOverEBITDA", "evToEbitda", "evToEbitdaRatio", "ev_to_ebitda", "evEbitda", "ev_ebitda"),
        "gross_margin": _percent_value(row, "grossProfitMargin", "grossMargin", "gross_margin"),
        "operating_margin": _percent_value(row, "operatingMargin", "operating_margin"),
        "net_margin": _percent_value(row, "netProfitMargin", "netMargin", "net_margin"),
        "roe": _percent_value(row, "returnOnEquity", "roe"),
        "roic": _percent_value(row, "returnOnInvestedCapital", "roic"),
        "revenue_growth": _percent_value(row, "revenueGrowth", "revenue_growth", "revenueGrowthTTM"),
        "eps_growth": _percent_value(row, "epsGrowth", "eps_growth", "epsGrowthTTM"),
        "ebitda_growth": _percent_value(row, "ebitdaGrowth", "ebitda_growth"),
        "fcf_growth": _percent_value(row, "freeCashFlowGrowth", "fcfGrowth", "fcf_growth"),
        "debt_equity": _first_number(row, "debtToEquity", "debtEquity", "debt_to_equity", "debt_equity"),
        "current_ratio": _first_number(row, "currentRatio", "current_ratio"),
        "net_debt_ebitda": _first_number(row, "netDebtToEBITDA", "netDebtToEbitda", "net_debt_to_ebitda", "net_debt_ebitda"),
        "eps_ttm": _first_number(row, "eps", "epsTTM", "eps_ttm"),
        "fcf": _first_number(row, "freeCashFlow", "free_cash_flow", "fcf"),
        "fcf_margin": _percent_value(row, "freeCashFlowMargin", "fcfMargin", "fcf_margin"),
        "earnings_yield": _percent_value(row, "earningsYield", "earnings_yield"),
    }


def _activity_from_bundle(bundle: dict[str, Any] | None, source_key: str, inactive_label: str) -> dict[str, Any]:
    source = {}
    if isinstance(bundle, dict):
        sources = bundle.get("sources")
        if isinstance(sources, dict) and isinstance(sources.get(source_key), dict):
            source = sources[source_key]
    present = source.get("present") is True
    return {
        "present": present,
        "label": source.get("label") if present and isinstance(source.get("label"), str) else inactive_label,
        "direction": source.get("direction") if isinstance(source.get("direction"), str) else "neutral",
        "freshness_days": source.get("freshness_days") if isinstance(source.get("freshness_days"), int) else None,
    }


def _enrich_row(
    row: dict[str, Any],
    bundle: dict[str, Any] | None,
    *,
    lookback_days: int,
    government_contracts_summary: dict[str, Any] | None = None,
    options_flow_summary: dict[str, Any] | None = None,
    institutional_activity_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if not isinstance(bundle, dict):
        bundle = {
            "ticker": row["symbol"],
            "lookback_days": lookback_days,
            "score": 0,
            "band": "inactive",
            "direction": "neutral",
            "status": "Inactive",
            "sources": {},
            "drivers": [],
        }
    summary = slim_confirmation_score_bundle(bundle)
    normalized_confirmation = normalize_confirmation_state(
        {
            "score": summary.get("confirmation_score"),
            "band": summary.get("confirmation_band"),
            "direction": summary.get("confirmation_direction"),
            "source_count": summary.get("confirmation_source_count"),
            "status": summary.get("confirmation_status"),
        },
        why_now=summary.get("why_now") if isinstance(summary.get("why_now"), dict) else None,
    )
    government_contracts_summary = government_contracts_summary if isinstance(government_contracts_summary, dict) else {}
    options_flow_summary = options_flow_summary if isinstance(options_flow_summary, dict) else {}
    institutional_activity_summary = institutional_activity_summary if isinstance(institutional_activity_summary, dict) else {}
    government_contracts_status = (
        government_contracts_summary.get("status")
        if isinstance(government_contracts_summary.get("status"), str)
        else "ok"
    )
    options_flow_status = (
        options_flow_summary.get("status")
        if isinstance(options_flow_summary.get("status"), str)
        else "unavailable"
    )
    options_flow_available = options_flow_status == "ok"
    institutional_activity_status = (
        institutional_activity_summary.get("status")
        if isinstance(institutional_activity_summary.get("status"), str)
        else "unavailable"
    )
    institutional_activity_available = institutional_activity_status == "ok"
    holders_increased = _int_param(institutional_activity_summary.get("holders_increased")) if institutional_activity_available else None
    holders_reduced = _int_param(institutional_activity_summary.get("holders_reduced")) if institutional_activity_available else None
    new_positions = _int_param(institutional_activity_summary.get("new_positions")) if institutional_activity_available else None
    exits = _int_param(institutional_activity_summary.get("exits")) if institutional_activity_available else None
    holder_breadth = (
        int(holders_increased or 0) + int(new_positions or 0) - int(holders_reduced or 0) - int(exits or 0)
        if institutional_activity_available
        else None
    )
    return {
        **row,
        "congress_activity": _activity_from_bundle(bundle, "congress", "No recent activity"),
        "insider_activity": _activity_from_bundle(bundle, "insiders", "No recent activity"),
        "government_contracts_status": government_contracts_status,
        "government_contracts_active": government_contracts_summary.get("active") is True if government_contracts_status == "ok" else None,
        "government_contracts_score_contribution": int(government_contracts_summary.get("score_contribution") or 0)
        if government_contracts_status == "ok"
        else None,
        "government_contracts_count": int(government_contracts_summary.get("contract_count") or 0)
        if government_contracts_status == "ok"
        else None,
        "government_contracts_total_amount": (_number(government_contracts_summary.get("total_award_amount")) or 0.0)
        if government_contracts_status == "ok"
        else None,
        "government_contracts_largest_amount": _number(government_contracts_summary.get("largest_award_amount"))
        if government_contracts_status == "ok"
        else None,
        "government_contracts_latest_date": government_contracts_summary.get("latest_award_date")
        if government_contracts_status == "ok" and isinstance(government_contracts_summary.get("latest_award_date"), str)
        else None,
        "government_contracts_top_agency": government_contracts_summary.get("top_agency")
        if government_contracts_status == "ok" and isinstance(government_contracts_summary.get("top_agency"), str)
        else None,
        "government_contracts_direction": government_contracts_summary.get("direction")
        if government_contracts_status == "ok" and isinstance(government_contracts_summary.get("direction"), str)
        else None,
        "options_flow_active": (options_flow_summary.get("active") is True) if options_flow_available else None,
        "options_flow_score": _int_param(options_flow_summary.get("score")) if options_flow_available else None,
        "options_flow_direction": options_flow_summary.get("direction") if options_flow_available and isinstance(options_flow_summary.get("direction"), str) else None,
        "options_flow_intensity": options_flow_summary.get("intensity") if options_flow_available and isinstance(options_flow_summary.get("intensity"), str) else None,
        "options_flow_total_premium": _number(options_flow_summary.get("total_premium")) if options_flow_available else None,
        "options_flow_call_put_premium_ratio": _number(options_flow_summary.get("call_put_premium_ratio")) if options_flow_available else None,
        "options_flow_latest_date": options_flow_summary.get("latest_flow_date") if options_flow_available and isinstance(options_flow_summary.get("latest_flow_date"), str) else None,
        "options_flow_status": options_flow_status,
        "institutional_activity_active": (institutional_activity_summary.get("active") is True) if institutional_activity_available else None,
        "institutional_activity_direction": institutional_activity_summary.get("direction") if institutional_activity_available and isinstance(institutional_activity_summary.get("direction"), str) else None,
        "institutional_activity_net_activity": _number(institutional_activity_summary.get("net_activity")) if institutional_activity_available else None,
        "institutional_activity_institution_count": _int_param(institutional_activity_summary.get("institution_count")) if institutional_activity_available else None,
        "institutional_activity_total_value": _number(institutional_activity_summary.get("total_value")) if institutional_activity_available else None,
        "institutional_activity_ownership_pct": _number(institutional_activity_summary.get("institutional_ownership_pct")) if institutional_activity_available else None,
        "institutional_activity_holders_increased": holders_increased,
        "institutional_activity_holders_reduced": holders_reduced,
        "institutional_activity_new_positions": new_positions,
        "institutional_activity_exits": exits,
        "institutional_activity_holder_breadth": holder_breadth,
        "institutional_activity_materiality_score": _number(institutional_activity_summary.get("materiality_score")) if institutional_activity_available else None,
        "institutional_activity_latest_date": institutional_activity_summary.get("latest_activity_date") if institutional_activity_available and isinstance(institutional_activity_summary.get("latest_activity_date"), str) else None,
        "institutional_activity_status": institutional_activity_status,
        "confirmation": {
            "score": int(summary.get("confirmation_score") or 0),
            "band": summary.get("confirmation_band") if isinstance(summary.get("confirmation_band"), str) else "inactive",
            "direction": summary.get("confirmation_direction") if isinstance(summary.get("confirmation_direction"), str) else "neutral",
            "status": summary.get("confirmation_status") if isinstance(summary.get("confirmation_status"), str) else "Inactive",
            "normalized_status": normalized_confirmation.status,
            "source_count": int(summary.get("confirmation_source_count") or 0),
        },
        "why_now": summary.get("why_now") if isinstance(summary.get("why_now"), dict) else {"state": "inactive", "headline": f"No active confirmation sources are currently putting {row['symbol']} on the radar."},
        "signal_freshness": summary.get("signal_freshness")
        if isinstance(summary.get("signal_freshness"), dict)
        else {"freshness_score": 0, "freshness_state": "inactive", "freshness_label": "No active setup"},
        "ticker_url": f"/ticker/{quote(row['symbol'], safe='')}",
    }


def _redact_intelligence_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        **row,
        "confirmation": {
            "score": None,
            "band": "locked",
            "direction": "locked",
            "status": "Premium intelligence locked",
            "source_count": None,
            "locked": True,
        },
        "why_now": {
            "state": "locked",
            "headline": "Upgrade to unlock Why Now context.",
            "locked": True,
        },
        "signal_freshness": {
            "freshness_score": None,
            "freshness_state": "locked",
            "freshness_label": "Premium intelligence locked",
            "locked": True,
        },
    }


def _redact_options_flow_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        **row,
        "options_flow_active": None,
        "options_flow_score": None,
        "options_flow_direction": None,
        "options_flow_intensity": None,
        "options_flow_total_premium": None,
        "options_flow_call_put_premium_ratio": None,
        "options_flow_latest_date": None,
        "options_flow_status": "pro_locked",
        "options_flow_locked": True,
    }


def _redact_institutional_activity_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        **row,
        "institutional_activity_active": None,
        "institutional_activity_direction": None,
        "institutional_activity_net_activity": None,
        "institutional_activity_institution_count": None,
        "institutional_activity_total_value": None,
        "institutional_activity_ownership_pct": None,
        "institutional_activity_holders_increased": None,
        "institutional_activity_holders_reduced": None,
        "institutional_activity_new_positions": None,
        "institutional_activity_exits": None,
        "institutional_activity_holder_breadth": None,
        "institutional_activity_materiality_score": None,
        "institutional_activity_latest_date": None,
        "institutional_activity_status": "pro_locked",
        "institutional_activity_locked": True,
    }


def _locked_options_flow_summary(symbol: str) -> dict[str, Any]:
    return {
        "active": None,
        "score": None,
        "direction": None,
        "intensity": None,
        "call_put_premium_ratio": None,
        "total_premium": None,
        "latest_flow_date": None,
        "source": None,
        "status": "pro_locked",
        "locked": True,
        "required_plan": "pro",
        "symbol": symbol,
    }


def _locked_institutional_activity_summary() -> dict[str, Any]:
    return {
        "active": None,
        "direction": None,
        "net_activity": None,
        "institution_count": None,
        "total_value": None,
        "institutional_ownership_pct": None,
        "holders_increased": None,
        "holders_reduced": None,
        "new_positions": None,
        "exits": None,
        "materiality_score": None,
        "latest_activity_date": None,
        "source_label": "Institutional Activity",
        "status": "pro_locked",
        "locked": True,
        "required_plan": "pro",
    }


def _sort_number(value: Any, missing: float = -1.0) -> float:
    parsed = _number(value)
    return parsed if parsed is not None else missing


def _sort_key(row: dict[str, Any], sort: str) -> tuple:
    confirmation = row.get("confirmation") if isinstance(row.get("confirmation"), dict) else {}
    congress = row.get("congress_activity") if isinstance(row.get("congress_activity"), dict) else {}
    insiders = row.get("insider_activity") if isinstance(row.get("insider_activity"), dict) else {}
    freshness = row.get("signal_freshness") if isinstance(row.get("signal_freshness"), dict) else {}
    if sort == "confirmation_score":
        return (_sort_number(confirmation.get("score")), _sort_number(row.get("market_cap")), row["symbol"])
    if sort == "market_cap":
        return (_sort_number(row.get("market_cap")), _sort_number(confirmation.get("score")), row["symbol"])
    if sort == "price":
        return (_sort_number(row.get("price")), _sort_number(confirmation.get("score")), row["symbol"])
    if sort == "volume":
        return (_sort_number(row.get("volume")), _sort_number(confirmation.get("score")), row["symbol"])
    if sort == "beta":
        return (_sort_number(row.get("beta")), _sort_number(confirmation.get("score")), row["symbol"])
    if sort in NUMERIC_ROW_SORTS:
        return (_sort_number(row.get(sort)), _sort_number(confirmation.get("score")), _sort_number(row.get("market_cap")), row["symbol"])
    if sort == "freshness":
        return (_sort_number(freshness.get("freshness_score")), _sort_number(confirmation.get("score")), row["symbol"])
    if sort == "congress_activity":
        return (1 if congress.get("present") is True else 0, _sort_number(confirmation.get("score")), row["symbol"])
    if sort == "insider_activity":
        return (1 if insiders.get("present") is True else 0, _sort_number(confirmation.get("score")), row["symbol"])
    if sort == "symbol":
        return (row["symbol"],)
    return (
        _sort_number(confirmation.get("score")),
        1 if congress.get("present") is True else 0,
        1 if insiders.get("present") is True else 0,
        _sort_number(freshness.get("freshness_score")),
        _sort_number(row.get("market_cap")),
        _sort_number(row.get("volume")),
        row["symbol"],
    )


def matches_confirmation_filters(row: dict[str, Any], params: ScreenerParams) -> bool:
    return confirmation_filter_diagnostics(row, params)["matches"] is True


def confirmation_filter_diagnostics(row: dict[str, Any], params: ScreenerParams) -> dict[str, Any]:
    confirmation = row.get("confirmation") if isinstance(row.get("confirmation"), dict) else {}
    why_now = row.get("why_now") if isinstance(row.get("why_now"), dict) else {}
    normalized = normalize_confirmation_state(confirmation, why_now=why_now)
    required_direction = _normalized_str(params.confirmation_direction)
    required_band = _normalized_str(params.confirmation_band)
    min_score = params.confirmation_score_min
    actual_score = _sort_number(confirmation.get("score"), missing=0.0)
    actual_band = confirmation.get("band")
    actual_direction = _normalized_str(confirmation.get("direction")) or normalized.direction

    result = {
        "matches": True,
        "reason": None,
        "required_direction": required_direction,
        "actual_direction": actual_direction,
        "required_status": "active" if required_direction else None,
        "actual_status": normalized.status,
        "required_band": required_band,
        "actual_band": actual_band,
        "required_min_score": min_score,
        "actual_score": actual_score,
    }

    if min_score is not None and actual_score < float(min_score):
        return {**result, "matches": False, "reason": "confirmation_score_below_minimum"}

    if required_direction and (
        normalized.status != "active"
        or normalized.direction != required_direction
        or actual_direction != required_direction
    ):
        return {**result, "matches": False, "reason": "confirmation_direction_or_status_mismatch"}

    if not _matches_confirmation_band(actual_band, params.confirmation_band):
        return {**result, "matches": False, "reason": "confirmation_band_mismatch"}

    return result


def _row_matches_filters(
    row: dict[str, Any],
    params: ScreenerParams,
    *,
    overlay_availability: dict[str, Any] | None = None,
) -> bool:
    congress = row.get("congress_activity") if isinstance(row.get("congress_activity"), dict) else {}
    insiders = row.get("insider_activity") if isinstance(row.get("insider_activity"), dict) else {}
    why_now = row.get("why_now") if isinstance(row.get("why_now"), dict) else {}
    freshness = row.get("signal_freshness") if isinstance(row.get("signal_freshness"), dict) else {}
    government_filterable = _overlay_filterable(overlay_availability, "government_contracts")
    options_filterable = _overlay_filterable(overlay_availability, "options_flow")
    institutional_filterable = _overlay_filterable(overlay_availability, "institutional_activity")

    if not _matches_activity_filter(congress, params.congress_activity):
        return False
    if not _matches_activity_filter(insiders, params.insider_activity):
        return False

    if government_filterable and not _matches_boolean_filter(row.get("government_contracts_active"), params.government_contracts_active):
        return False

    if not matches_confirmation_filters(row, params):
        return False

    why_now_state = _normalized_str(params.why_now_state)
    if why_now_state:
        expected_state = "mixed" if why_now_state == "limited" else why_now_state
        if why_now.get("state") != expected_state:
            return False

    freshness_state = _normalized_str(params.freshness)
    if freshness_state and freshness.get("freshness_state") != freshness_state:
        return False

    if options_filterable:
        if not _matches_boolean_filter(row.get("options_flow_active"), params.options_flow_active):
            return False
        options_direction = _normalized_str(params.options_flow_direction)
        if options_direction and _normalized_str(row.get("options_flow_direction")) != options_direction:
            return False
        if params.options_flow_min_score is not None and _sort_number(row.get("options_flow_score"), missing=-1.0) < float(params.options_flow_min_score):
            return False
        if params.options_flow_min_premium is not None and _sort_number(row.get("options_flow_total_premium"), missing=-1.0) < float(params.options_flow_min_premium):
            return False

    if institutional_filterable:
        if not _matches_boolean_filter(row.get("institutional_activity_active"), params.institutional_activity_active):
            return False
        if not _matches_institutional_activity_type(row, params.institutional_activity_type):
            return False
        institutional_direction = _normalized_str(params.institutional_activity_direction)
        if institutional_direction and _normalized_str(row.get("institutional_activity_direction")) != institutional_direction:
            return False
        if params.institutional_activity_min_value is not None and _institutional_reported_value(row) < float(params.institutional_activity_min_value):
            return False
        if params.institutional_activity_min_ownership_pct is not None and _sort_number(row.get("institutional_activity_ownership_pct"), missing=-1.0) < float(params.institutional_activity_min_ownership_pct):
            return False
        if not _matches_institutional_holder_breadth(row, params.institutional_activity_holder_breadth):
            return False

    if not _matches_technical_filters(row, params):
        return False
    if not _matches_fundamental_filters(row, params):
        return False

    return True


def _institutional_reported_value(row: dict[str, Any]) -> float:
    net = abs(_sort_number(row.get("institutional_activity_net_activity"), missing=0.0))
    total = abs(_sort_number(row.get("institutional_activity_total_value"), missing=0.0))
    return max(net, total)


def _matches_institutional_activity_type(row: dict[str, Any], value: str | None) -> bool:
    activity_type = _normalized_str(value)
    if not activity_type or activity_type == "any":
        return True
    direction = _normalized_str(row.get("institutional_activity_direction"))
    new_positions = _int_param(row.get("institutional_activity_new_positions")) or 0
    exits = _int_param(row.get("institutional_activity_exits")) or 0
    materiality = _sort_number(row.get("institutional_activity_materiality_score"), missing=0.0)
    breadth = abs(_int_param(row.get("institutional_activity_holder_breadth")) or 0)
    if activity_type == "accumulation":
        return direction == "bullish"
    if activity_type == "distribution":
        return direction == "bearish"
    if activity_type == "new_position":
        return new_positions > 0
    if activity_type == "exit":
        return exits > 0
    if activity_type == "major_holder_move":
        return materiality >= 70 or _institutional_reported_value(row) >= 50_000_000
    if activity_type == "cluster_move":
        return breadth >= 10 or max(
            _int_param(row.get("institutional_activity_holders_increased")) or 0,
            _int_param(row.get("institutional_activity_holders_reduced")) or 0,
        ) >= 10
    return True


def _matches_institutional_holder_breadth(row: dict[str, Any], value: str | None) -> bool:
    breadth_filter = _normalized_str(value)
    if not breadth_filter or breadth_filter == "any":
        return True
    breadth = _int_param(row.get("institutional_activity_holder_breadth")) or 0
    increased = _int_param(row.get("institutional_activity_holders_increased")) or 0
    if breadth_filter in {"net_3", "net+3", "net_plus_3"}:
        return breadth >= 3
    if breadth_filter in {"net_10", "net+10", "net_plus_10"}:
        return breadth >= 10
    if breadth_filter in {"increasing_10", "10_increasing", "inc_10"}:
        return increased >= 10
    if breadth_filter in {"increasing_25", "25_increasing", "inc_25"}:
        return increased >= 25
    return True


def _matches_core_filters(row: dict[str, Any], params: ScreenerParams) -> bool:
    ranges = (
        ("market_cap", params.market_cap_min, params.market_cap_max),
        ("price", params.price_min, params.price_max),
        ("volume", params.volume_min, None),
        ("beta", params.beta_min, params.beta_max),
        ("dividend_yield", params.dividend_yield_min, params.dividend_yield_max),
    )
    if not all(_matches_range(row, field, minimum, maximum) for field, minimum, maximum in ranges):
        return False

    for field, expected in (
        ("sector", params.sector),
        ("industry", params.industry),
        ("country", params.country),
        ("exchange", params.exchange),
    ):
        expected_values = _normalized_filter_values(expected)
        if not expected_values:
            continue
        actual = _normalized_str(row.get(field))
        if actual not in expected_values:
            return False
    return True


def _matches_range(row: dict[str, Any], field: str, minimum: float | None, maximum: float | None) -> bool:
    if minimum is None and maximum is None:
        return True
    value = _number(row.get(field))
    if value is None:
        return False
    if minimum is not None and value < float(minimum):
        return False
    if maximum is not None and value > float(maximum):
        return False
    return True


def _active_range(minimum: float | None, maximum: float | None) -> bool:
    return minimum is not None or maximum is not None


def _technical_filter_summary(rows: list[dict[str, Any]], params: ScreenerParams) -> dict[str, int]:
    summary = {
        "rows_scanned": len(rows),
        "excluded_by_rel_volume": 0,
        "excluded_by_price_move": 0,
        "excluded_by_rsi": 0,
        "excluded_by_macd": 0,
        "excluded_by_trend": 0,
        "rows_missing_technical_data": 0,
    }
    if not _has_technical_filters(params):
        return summary

    for row in rows:
        missing = False
        if _active_range(params.rel_volume_min, params.rel_volume_max):
            if _number(row.get("rel_volume")) is None:
                missing = True
            if not _matches_range(row, "rel_volume", params.rel_volume_min, params.rel_volume_max):
                summary["excluded_by_rel_volume"] += 1
        if _active_range(params.price_move_min, params.price_move_max):
            if _number(row.get("price_move_pct")) is None:
                missing = True
            if not _matches_range(row, "price_move_pct", params.price_move_min, params.price_move_max):
                summary["excluded_by_price_move"] += 1
        if _active_range(params.rsi_min, params.rsi_max):
            if _number(row.get("rsi")) is None:
                missing = True
            if not _matches_range(row, "rsi", params.rsi_min, params.rsi_max):
                summary["excluded_by_rsi"] += 1

        macd_state = _normalized_str(params.macd_state)
        if macd_state:
            if not _normalized_str(row.get("macd_state")):
                missing = True
            if _normalized_str(row.get("macd_state")) != macd_state:
                summary["excluded_by_macd"] += 1
        trend_state = _normalized_str(params.trend_state)
        if trend_state:
            if not _normalized_str(row.get("trend_state")):
                missing = True
            if _normalized_str(row.get("trend_state")) != trend_state:
                summary["excluded_by_trend"] += 1

        if missing:
            summary["rows_missing_technical_data"] += 1
    return summary


def _matches_technical_filters(row: dict[str, Any], params: ScreenerParams) -> bool:
    if not _matches_range(row, "rel_volume", params.rel_volume_min, params.rel_volume_max):
        return False
    if not _matches_range(row, "price_move_pct", params.price_move_min, params.price_move_max):
        return False
    if not _matches_range(row, "rsi", params.rsi_min, params.rsi_max):
        return False

    macd_state = _normalized_str(params.macd_state)
    if macd_state and _normalized_str(row.get("macd_state")) != macd_state:
        return False
    trend_state = _normalized_str(params.trend_state)
    if trend_state and _normalized_str(row.get("trend_state")) != trend_state:
        return False
    return True


def _matches_fundamental_filters(row: dict[str, Any], params: ScreenerParams) -> bool:
    return all(
        _matches_range(row, spec.row_field, minimum, maximum)
        for spec in FUNDAMENTAL_FILTER_SPECS
        for minimum, maximum in (_fundamental_range(params, spec),)
    )


def _normalized_str(value: Any) -> str | None:
    return value.strip().lower() if isinstance(value, str) and value.strip() else None


def _institutional_lookback_days(lookback: Any, legacy_days: Any = None) -> int:
    normalized = _normalized_str(lookback)
    if normalized in {"30d", "30", "30_days"}:
        return 30
    if normalized in {"90d", "90", "90_days", "latest_quarter"}:
        return 90
    if normalized in {"1y", "365", "365d", "365_days"}:
        return 365
    parsed = _int_param(legacy_days)
    if parsed is not None:
        return max(1, min(parsed, 365))
    return DEFAULT_INSTITUTIONAL_ACTIVITY_LOOKBACK_DAYS


def _normalized_filter_values(value: Any) -> set[str]:
    if not isinstance(value, str) or not value.strip():
        return set()
    return {
        cleaned
        for part in value.split(",")
        if (cleaned := _normalized_str(part))
    }


def _string_param(value: Any) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return None
    cleaned = value.strip()
    return None if cleaned.lower() == "any" else cleaned


def _bool_param(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return bool(int(value))
    if isinstance(value, str) and value.strip():
        lowered = value.strip().lower()
        if lowered in {"1", "true", "yes", "active"}:
            return True
        if lowered in {"0", "false", "no", "inactive"}:
            return False
    return None


def _int_param(value: Any) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str) and value.strip():
        try:
            return int(float(value.replace(",", "").strip()))
        except ValueError:
            return None
    return None


def _float_param(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        parsed = float(value)
    elif isinstance(value, str) and value.strip():
        try:
            parsed = float(value.replace(",", "").strip())
        except ValueError:
            return None
    else:
        return None
    return parsed if isfinite(parsed) else None


def _csv_number(value: Any, *, digits: int | None = None) -> str:
    parsed = _number(value)
    if parsed is None:
        return ""
    if digits == 0:
        return str(int(round(parsed)))
    if float(parsed).is_integer():
        return str(int(parsed))
    if digits is None:
        return f"{parsed:.6f}".rstrip("0").rstrip(".")
    return f"{parsed:.{digits}f}"


def _csv_label(value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        return ""
    cleaned = value.strip().replace("_", " ")
    return " ".join(part[:1].upper() + part[1:] for part in cleaned.split())


def _matches_activity_filter(activity: dict[str, Any], filter_value: str | None) -> bool:
    value = _normalized_str(filter_value)
    if not value:
        return True
    present = activity.get("present") is True
    direction = activity.get("direction")
    if value == "has_activity":
        return present
    if value == "no_activity":
        return not present
    if value == "buy_leaning":
        return present and direction == "bullish"
    if value == "sell_leaning":
        return present and direction == "bearish"
    return True


def _matches_boolean_filter(value: Any, expected: bool | None) -> bool:
    if expected is None:
        return True
    return (value is True) if expected else (value is not True)


def _overlay_filterable(overlay_availability: dict[str, Any] | None, key: str) -> bool:
    if not isinstance(overlay_availability, dict):
        return False
    overlay = overlay_availability.get(key)
    return isinstance(overlay, dict) and overlay.get("filterable") is True


def _locked_overlay_availability(source: str, availability: Any) -> dict[str, Any]:
    base = dict(availability) if isinstance(availability, dict) else {}
    return {
        **base,
        "source": source,
        "enabled": bool(base.get("enabled", True)),
        "status": "pro_locked",
        "filterable": False,
        "locked": True,
        "required_plan": "pro",
    }


def _overlay_availability_for_entitlements(
    overlay_availability: dict[str, Any] | None,
    entitlements: TierEntitlements | None,
) -> dict[str, Any]:
    result = dict(overlay_availability) if isinstance(overlay_availability, dict) else {}
    if entitlements is None:
        return result
    if not entitlements.has_feature("options_flow_filters"):
        result["options_flow"] = _locked_overlay_availability("options_flow", result.get("options_flow"))
    if not entitlements.has_feature("institutional_filters"):
        result["institutional_activity"] = _locked_overlay_availability(
            "institutional_activity",
            result.get("institutional_activity"),
        )
    return result


def _ignored_overlay_filters(params: ScreenerParams, overlay_availability: dict[str, Any]) -> list[str]:
    ignored: list[str] = []
    if not _overlay_filterable(overlay_availability, "options_flow"):
        for key in ("options_flow_active", "options_flow_direction", "options_flow_min_score", "options_flow_min_premium"):
            if getattr(params, key) is not None:
                ignored.append(key)
    if not _overlay_filterable(overlay_availability, "government_contracts"):
        for key in ("government_contracts_active",):
            if getattr(params, key) is not None:
                ignored.append(key)
    if not _overlay_filterable(overlay_availability, "institutional_activity"):
        for key in (
            "institutional_activity_active",
            "institutional_activity_type",
            "institutional_activity_direction",
            "institutional_activity_min_value",
            "institutional_activity_min_ownership_pct",
            "institutional_activity_holder_breadth",
        ):
            if getattr(params, key) is not None:
                ignored.append(key)
    return ignored


def _matches_confirmation_band(band: Any, filter_value: str | None) -> bool:
    value = _normalized_str(filter_value)
    if not value:
        return True
    if value == "moderate_plus":
        return band in {"moderate", "strong", "exceptional"}
    if value == "strong_plus":
        return band in {"strong", "exceptional"}
    if value == "exceptional":
        return band == "exceptional"
    return band == value
