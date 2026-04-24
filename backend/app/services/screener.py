from __future__ import annotations

import csv
from collections.abc import Mapping
from dataclasses import dataclass
from io import StringIO
from math import isfinite
from typing import Any
from urllib.parse import quote

from sqlalchemy.orm import Session

from app.clients.fmp import fetch_company_screener
from app.entitlements import TierEntitlements, premium_required_error
from app.services.confirmation_score import (
    get_confirmation_score_bundles_for_tickers,
    slim_confirmation_score_bundle,
)
from app.utils.symbols import normalize_symbol

MAX_PAGE_SIZE = 100
MAX_FETCH_ROWS = 500
MAX_EXPORT_ROWS = MAX_FETCH_ROWS

SUPPORTED_SORTS = {
    "relevance",
    "confirmation_score",
    "market_cap",
    "price",
    "volume",
    "beta",
    "congress_activity",
    "insider_activity",
    "freshness",
    "symbol",
}

PREMIUM_SORTS = {
    "confirmation_score",
    "congress_activity",
    "insider_activity",
    "freshness",
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
class ScreenerParams:
    page: int = 1
    page_size: int = 50
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


def screener_params_from_mapping(
    params: Mapping[str, Any],
    *,
    page: int = 1,
    page_size: int = 50,
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
    )


def build_screener_response(db: Session, params: ScreenerParams) -> dict[str, Any]:
    page = max(1, int(params.page or 1))
    page_size = max(1, min(int(params.page_size or 50), MAX_PAGE_SIZE))
    lookback_days = max(1, min(int(params.lookback_days or 30), 365))
    sort = params.sort if params.sort in SUPPORTED_SORTS else "relevance"
    sort_dir = "asc" if params.sort_dir == "asc" else "desc"
    rows = build_screener_rows(db, params, requested_rows=_requested_rows(params, page=page, page_size=page_size))

    start = (page - 1) * page_size
    end = start + page_size
    paged = rows[start:end]
    return {
        "items": paged,
        "page": page,
        "page_size": page_size,
        "returned": len(paged),
        "total_available": len(rows),
        "has_next": end < len(rows),
        "sort": {"sort_by": sort, "sort_dir": sort_dir},
        "filters": _response_filters(params),
        "supported_filters": list(FMP_FILTER_MAP.keys()) + list(_intelligence_filter_keys()),
        "source": "fmp_company_screener",
        "lookback_days": lookback_days,
    }


def build_screener_response_for_entitlements(
    db: Session,
    params: ScreenerParams,
    *,
    entitlements: TierEntitlements,
) -> dict[str, Any]:
    result_cap = max(1, min(int(entitlements.limit("screener_results")), MAX_FETCH_ROWS))
    page = max(1, int(params.page or 1))
    page_size = max(1, min(int(params.page_size or 50), MAX_PAGE_SIZE, result_cap))
    lookback_days = max(1, min(int(params.lookback_days or 30), 365))
    sort = params.sort if params.sort in SUPPORTED_SORTS else "relevance"
    sort_dir = "asc" if params.sort_dir == "asc" else "desc"
    rows = build_screener_rows(
        db,
        params,
        requested_rows=_requested_rows(params, page=page, page_size=page_size, row_cap=result_cap),
    )
    if not entitlements.has_feature("screener_intelligence"):
        rows = redact_intelligence_rows(rows)

    start = (page - 1) * page_size
    end = min(start + page_size, result_cap)
    paged = rows[start:end]
    return {
        "items": paged,
        "page": page,
        "page_size": page_size,
        "returned": len(paged),
        "total_available": min(len(rows), result_cap),
        "has_next": end < min(len(rows), result_cap),
        "sort": {"sort_by": sort, "sort_dir": sort_dir},
        "filters": _response_filters(params),
        "supported_filters": list(FMP_FILTER_MAP.keys()) + list(_intelligence_filter_keys()),
        "source": "fmp_company_screener",
        "lookback_days": lookback_days,
        "result_cap": result_cap,
        "access": {
            "tier": entitlements.tier,
            "intelligence_locked": not entitlements.has_feature("screener_intelligence"),
            "presets_locked": not entitlements.has_feature("screener_presets"),
            "saved_screens_limit": entitlements.limit("screener_saved_screens"),
            "monitoring_locked": not entitlements.has_feature("screener_monitoring"),
            "csv_export_locked": not entitlements.has_feature("screener_csv_export"),
        },
    }


def build_screener_rows(
    db: Session,
    params: ScreenerParams,
    *,
    requested_rows: int | None = None,
) -> list[dict[str, Any]]:
    lookback_days = max(1, min(int(params.lookback_days or 30), 365))
    sort = params.sort if params.sort in SUPPORTED_SORTS else "relevance"
    sort_dir = "asc" if params.sort_dir == "asc" else "desc"
    fetch_limit = requested_rows if requested_rows is not None else _requested_rows(params, page=params.page, page_size=params.page_size)

    fmp_filters = _fmp_filters(params)
    raw_rows = fetch_company_screener(filters=fmp_filters, limit=fetch_limit)
    normalized_rows = [_normalize_fmp_row(row) for row in raw_rows]
    normalized_rows = [row for row in normalized_rows if row is not None]

    symbols = [row["symbol"] for row in normalized_rows]
    bundles = get_confirmation_score_bundles_for_tickers(db, symbols, lookback_days=lookback_days)
    rows = [_enrich_row(row, bundles.get(row["symbol"]), lookback_days=lookback_days) for row in normalized_rows]
    rows = [row for row in rows if _row_matches_filters(row, params)]
    rows.sort(key=lambda row: _sort_key(row, sort), reverse=sort_dir == "desc")
    return rows


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
            ]
        )
    return output.getvalue(), len(rows)


def _requested_rows(params: ScreenerParams, *, page: int, page_size: int, row_cap: int = MAX_FETCH_ROWS) -> int:
    requested_rows = min(MAX_FETCH_ROWS, row_cap, max(page * page_size + 1, page_size))
    if _has_intelligence_filters(params):
        requested_rows = min(MAX_FETCH_ROWS, row_cap)
    return requested_rows


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
    )


def _has_intelligence_filters(params: ScreenerParams) -> bool:
    return any(getattr(params, key) is not None for key in _intelligence_filter_keys())


def has_intelligence_sort(params: ScreenerParams) -> bool:
    return params.sort in PREMIUM_SORTS


def require_screener_intelligence_access(params: ScreenerParams, entitlements: TierEntitlements) -> None:
    if entitlements.has_feature("screener_intelligence"):
        return
    if not _has_intelligence_filters(params) and not has_intelligence_sort(params):
        return
    raise premium_required_error(
        feature="screener_intelligence",
        message="Congress, insider, confirmation, Why Now, and freshness screener filters are included with Premium.",
        entitlements=entitlements,
    )


def redact_intelligence_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [_redact_intelligence_row(row) for row in rows]


def _number(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        parsed = float(value)
    elif isinstance(value, str):
        cleaned = value.replace("$", "").replace(",", "").strip()
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


def _normalize_fmp_row(row: dict[str, Any]) -> dict[str, Any] | None:
    symbol = normalize_symbol(_text(row, "symbol", "ticker"))
    if not symbol:
        return None

    company_name = _text(row, "companyName", "company_name", "name") or symbol
    return {
        "symbol": symbol,
        "company_name": company_name,
        "sector": _text(row, "sector"),
        "industry": _text(row, "industry"),
        "market_cap": _number(row.get("marketCap") or row.get("market_cap")),
        "price": _number(row.get("price")),
        "volume": _number(row.get("volume") or row.get("avgVolume") or row.get("averageVolume")),
        "beta": _number(row.get("beta")),
        "country": _text(row, "country"),
        "exchange": _text(row, "exchangeShortName", "exchange", "exchangeName"),
        "dividend_yield": _number(row.get("lastAnnualDividend") or row.get("dividendYield") or row.get("dividend")),
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


def _enrich_row(row: dict[str, Any], bundle: dict[str, Any] | None, *, lookback_days: int) -> dict[str, Any]:
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
    return {
        **row,
        "congress_activity": _activity_from_bundle(bundle, "congress", "No recent activity"),
        "insider_activity": _activity_from_bundle(bundle, "insiders", "No recent activity"),
        "confirmation": {
            "score": int(summary.get("confirmation_score") or 0),
            "band": summary.get("confirmation_band") if isinstance(summary.get("confirmation_band"), str) else "inactive",
            "direction": summary.get("confirmation_direction") if isinstance(summary.get("confirmation_direction"), str) else "neutral",
            "status": summary.get("confirmation_status") if isinstance(summary.get("confirmation_status"), str) else "Inactive",
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
        "congress_activity": {
            "present": False,
            "label": "Premium intelligence locked",
            "direction": None,
            "freshness_days": None,
            "locked": True,
        },
        "insider_activity": {
            "present": False,
            "label": "Premium intelligence locked",
            "direction": None,
            "freshness_days": None,
            "locked": True,
        },
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


def _row_matches_filters(row: dict[str, Any], params: ScreenerParams) -> bool:
    congress = row.get("congress_activity") if isinstance(row.get("congress_activity"), dict) else {}
    insiders = row.get("insider_activity") if isinstance(row.get("insider_activity"), dict) else {}
    confirmation = row.get("confirmation") if isinstance(row.get("confirmation"), dict) else {}
    why_now = row.get("why_now") if isinstance(row.get("why_now"), dict) else {}
    freshness = row.get("signal_freshness") if isinstance(row.get("signal_freshness"), dict) else {}

    if not _matches_activity_filter(congress, params.congress_activity):
        return False
    if not _matches_activity_filter(insiders, params.insider_activity):
        return False

    min_score = params.confirmation_score_min
    if min_score is not None and _sort_number(confirmation.get("score"), missing=0.0) < float(min_score):
        return False

    direction = _normalized_str(params.confirmation_direction)
    if direction and confirmation.get("direction") != direction:
        return False

    if not _matches_confirmation_band(confirmation.get("band"), params.confirmation_band):
        return False

    why_now_state = _normalized_str(params.why_now_state)
    if why_now_state:
        expected_state = "mixed" if why_now_state == "limited" else why_now_state
        if why_now.get("state") != expected_state:
            return False

    freshness_state = _normalized_str(params.freshness)
    if freshness_state and freshness.get("freshness_state") != freshness_state:
        return False

    return True


def _normalized_str(value: Any) -> str | None:
    return value.strip().lower() if isinstance(value, str) and value.strip() else None


def _string_param(value: Any) -> str | None:
    return value.strip() if isinstance(value, str) and value.strip() else None


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
