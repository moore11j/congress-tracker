from __future__ import annotations

from typing import Any

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
from app.utils.symbols import normalize_symbol


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
    bundles = get_confirmation_score_bundles_for_tickers(
        db,
        normalized_symbols,
        lookback_days=lookback,
        government_contracts_summaries=government_contracts_summaries,
        options_flow_summaries=options_flow_summaries,
        institutional_activity_summaries=institutional_activity_summaries,
    )
    return {
        "bundles": bundles,
        "government_contracts_summaries": government_contracts_summaries,
        "options_flow_summaries": options_flow_summaries,
        "institutional_activity_summaries": institutional_activity_summaries,
        "overlay_availability": {
            "government_contracts": government_availability,
            "options_flow": options_flow_availability,
            "institutional_activity": institutional_availability,
        },
    }
