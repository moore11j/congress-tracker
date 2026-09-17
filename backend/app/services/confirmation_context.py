from __future__ import annotations

from typing import Any
import logging

from sqlalchemy.orm import Session

from app.services.confirmation_score import get_confirmation_score_bundles_for_tickers
from app.services.government_contracts import (
    DEFAULT_GOVERNMENT_CONTRACTS_LOOKBACK_DAYS,
    DEFAULT_GOVERNMENT_CONTRACTS_MIN_AMOUNT,
    get_government_contracts_overlay_availability,
    get_government_contracts_summaries_for_symbols,
    inactive_government_contracts_summary,
    unavailable_government_contracts_summary,
)
from app.services.intelligence_overlays import (
    DEFAULT_INSTITUTIONAL_ACTIVITY_LOOKBACK_DAYS,
    DEFAULT_OPTIONS_FLOW_LOOKBACK_DAYS,
    get_institutional_activity_summaries_for_symbols,
    get_options_flow_summaries_for_symbols,
    load_intelligence_feature_flags,
)
from app.services.macro_positioning import get_macro_positioning_summaries_for_symbols
from app.utils.symbols import normalize_symbol

TICKER_CONFIRMATION_CONTEXT_VERSION = "ticker_confirmation_30d_v5_net_evidence"


def build_ticker_confirmation_context(db: Session, symbols: list[str]) -> dict[str, Any]:
    """The shared 30-day scoring path for ticker pages and Top Stocks jobs.

    The existing card input readers live in main; import them lazily to avoid
    a module initialization cycle. These readers use cached data only. History
    capture and HTTP/tier projection remain the caller's responsibility.
    """
    from app.main import (
        _mark_institutional_unavailable_in_confirmation_bundle,
        _merge_fresh_public_contexts_into_confirmation_bundle,
        build_ticker_signals_summary_contexts_from_cache,
    )

    context = build_confirmation_score_context(db, symbols, lookback_days=30)
    for symbol, bundle in context["bundles"].items():
        bundle = _mark_institutional_unavailable_in_confirmation_bundle(
            bundle,
            context["institutional_activity_summaries"].get(symbol),
            {"institutional_activity": {"locked": False}},
        )
        try:
            bundle = _merge_fresh_public_contexts_into_confirmation_bundle(
                bundle, build_ticker_signals_summary_contexts_from_cache(symbol, db=db)
            )
        except Exception:
            logging.getLogger(__name__).exception("ticker_confirmation_inputs_failed symbol=%s", symbol)
            bundle = {**bundle, "inputs_incomplete": True}
        bundle["score_context_version"] = TICKER_CONFIRMATION_CONTEXT_VERSION
        context["bundles"][symbol] = bundle
    return context


def build_confirmation_score_context(
    db: Session,
    symbols: list[str],
    *,
    lookback_days: int = 30,
    government_contracts_lookback_days: int = DEFAULT_GOVERNMENT_CONTRACTS_LOOKBACK_DAYS,
    government_contracts_min_amount: float | int | None = DEFAULT_GOVERNMENT_CONTRACTS_MIN_AMOUNT,
    options_flow_lookback_days: int = DEFAULT_OPTIONS_FLOW_LOOKBACK_DAYS,
    institutional_activity_lookback_days: int = DEFAULT_INSTITUTIONAL_ACTIVITY_LOOKBACK_DAYS,
    feature_flags: dict[str, bool] | None = None,
    government_contracts_availability: dict[str, Any] | None = None,
    government_contracts_summaries: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build the ticker confirmation inputs used by screener rows and ticker pages."""
    normalized_symbols = sorted({symbol for raw in symbols if (symbol := normalize_symbol(raw))})
    flags = feature_flags or load_intelligence_feature_flags(db)
    lookback = max(1, min(int(lookback_days or 30), 365))
    government_lookback = max(1, min(int(government_contracts_lookback_days or DEFAULT_GOVERNMENT_CONTRACTS_LOOKBACK_DAYS), 365 * 3))
    options_lookback = max(1, min(int(options_flow_lookback_days or DEFAULT_OPTIONS_FLOW_LOOKBACK_DAYS), 365))
    institutional_lookback = max(
        1,
        min(int(institutional_activity_lookback_days or DEFAULT_INSTITUTIONAL_ACTIVITY_LOOKBACK_DAYS), 365),
    )
    government_availability = government_contracts_availability or get_government_contracts_overlay_availability(
        db,
        feature_enabled=flags["feature_government_contracts_enabled"],
    )

    if not normalized_symbols:
        return {
            "bundles": {},
            "government_contracts_summaries": {},
            "options_flow_summaries": {},
            "institutional_activity_summaries": {},
            "overlay_availability": {
                "government_contracts": government_availability,
                "options_flow": {"status": "unavailable", "enabled": flags["feature_options_flow_enabled"]},
                "institutional_activity": {"status": "unavailable", "enabled": flags["feature_institutional_activity_enabled"]},
                "macro_positioning": {"status": "unavailable", "enabled": flags["feature_macro_positioning_enabled"]},
            },
        }

    if government_contracts_summaries is None:
        if flags["feature_government_contracts_enabled"] and government_availability.get("status") == "ok":
            government_contracts_summaries = get_government_contracts_summaries_for_symbols(
                db,
                normalized_symbols,
                lookback_days=government_lookback,
                min_amount=government_contracts_min_amount,
            )
        else:
            government_contracts_summaries = {
                symbol: unavailable_government_contracts_summary()
                for symbol in normalized_symbols
            }

    if government_availability.get("status") == "ok":
        government_contracts_summaries = {
            symbol: government_contracts_summaries.get(symbol, inactive_government_contracts_summary(lookback_days=government_lookback))
            for symbol in normalized_symbols
        }
    else:
        government_contracts_summaries = {
            symbol: unavailable_government_contracts_summary()
            for symbol in normalized_symbols
        }

    options_flow_summaries, options_flow_availability = get_options_flow_summaries_for_symbols(
        db,
        normalized_symbols,
        lookback_days=options_lookback,
        feature_enabled=flags["feature_options_flow_enabled"],
    )
    institutional_activity_summaries, institutional_availability = get_institutional_activity_summaries_for_symbols(
        db,
        normalized_symbols,
        lookback_days=institutional_lookback,
        feature_enabled=flags["feature_institutional_activity_enabled"],
    )
    macro_positioning_summaries, macro_positioning_availability = get_macro_positioning_summaries_for_symbols(
        db,
        normalized_symbols,
        feature_enabled=flags["feature_macro_positioning_enabled"],
    )
    bundles = get_confirmation_score_bundles_for_tickers(
        db,
        normalized_symbols,
        lookback_days=lookback,
        government_contracts_summaries=government_contracts_summaries,
        options_flow_summaries=options_flow_summaries,
        institutional_activity_summaries=institutional_activity_summaries,
        macro_positioning_summaries=macro_positioning_summaries,
    )
    return {
        "bundles": bundles,
        "government_contracts_summaries": government_contracts_summaries,
        "options_flow_summaries": options_flow_summaries,
        "institutional_activity_summaries": institutional_activity_summaries,
        "macro_positioning_summaries": macro_positioning_summaries,
        "overlay_availability": {
            "government_contracts": government_availability,
            "options_flow": options_flow_availability,
            "institutional_activity": institutional_availability,
            "macro_positioning": macro_positioning_availability,
        },
    }
