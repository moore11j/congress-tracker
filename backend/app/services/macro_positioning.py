from __future__ import annotations

import csv
import io
import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import os
from pathlib import Path
from typing import Any

import requests
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models import AppSetting, FundamentalsCache, MacroPositioningAsset, MacroPositioningCache, Security, TickerMeta
from app.utils.symbols import normalize_symbol

_CONFIG_PATH = Path(__file__).resolve().parents[1] / "data" / "macro_positioning_mappings.json"
_BIAS_SCORES = {"bearish": -1.0, "neutral": 0.0, "bullish": 1.0}
_INSIGHTS_STALE_AFTER_DAYS = 10
_CFTC_TIMEOUT_SECONDS = float(os.getenv("MACRO_POSITIONING_CFTC_TIMEOUT_SECONDS", "20"))
_CFTC_FINANCIAL_FUTURES_URL = os.getenv("MACRO_POSITIONING_CFTC_FINANCIAL_URL", "https://www.cftc.gov/dea/newcot/FinFutWk.txt")
_CFTC_DISAGG_FUTURES_URL = os.getenv("MACRO_POSITIONING_CFTC_DISAGG_URL", "https://www.cftc.gov/dea/newcot/f_disagg.txt")
INSIGHTS_MACRO_POSITIONING_MARKETS: tuple[dict[str, str], ...] = (
    {"id": "sp-500", "asset_key": "sp_futures", "name": "S&P 500"},
    {"id": "nasdaq-100", "asset_key": "nasdaq_futures", "name": "Nasdaq 100"},
    {"id": "russell-2000", "asset_key": "russell_2000_futures", "name": "Russell 2000"},
    {"id": "us-dollar", "asset_key": "us_dollar", "name": "US Dollar"},
    {"id": "gold", "asset_key": "gold_futures", "name": "Gold"},
    {"id": "silver", "asset_key": "silver", "name": "Silver"},
    {"id": "crude-oil", "asset_key": "crude_oil", "name": "Crude Oil"},
    {"id": "natural-gas", "asset_key": "natural_gas", "name": "Natural Gas"},
    {"id": "copper", "asset_key": "copper", "name": "Copper"},
    {"id": "bitcoin", "asset_key": "bitcoin_futures", "name": "Bitcoin"},
    {"id": "us-treasuries", "asset_key": "ten_year_treasury", "name": "US Treasuries"},
)


@dataclass(frozen=True)
class MacroMapping:
    key: str
    label: str
    thesis_label: str
    headline: str
    drivers: list[dict[str, str]]
    mapping_type: str


@dataclass(frozen=True)
class CftcMarketSpec:
    asset_key: str
    display_name: str
    source: str
    match_terms: tuple[str, ...]
    long_index: int
    short_index: int
    long_change_index: int | None = None
    short_change_index: int | None = None


_CFTC_MARKET_SPECS: tuple[CftcMarketSpec, ...] = (
    CftcMarketSpec("sp_futures", "S&P 500", "financial", ("S&P 500 Consolidated", "E-MINI S&P 500"), 11, 12, 28, 29),
    CftcMarketSpec("nasdaq_futures", "Nasdaq 100", "financial", ("NASDAQ-100 Consolidated", "NASDAQ-100 STOCK INDEX"), 11, 12, 28, 29),
    CftcMarketSpec("russell_2000_futures", "Russell 2000", "financial", ("RUSSELL E-MINI", "RUSSELL 2000"), 11, 12, 28, 29),
    CftcMarketSpec("us_dollar", "US Dollar", "financial", ("USD INDEX",), 11, 12, 28, 29),
    CftcMarketSpec("bitcoin_futures", "Bitcoin", "financial", ("BITCOIN - CHICAGO MERCANTILE EXCHANGE",), 11, 12, 28, 29),
    CftcMarketSpec("ten_year_treasury", "US Treasuries", "financial", ("UST 10Y NOTE",), 11, 12, 28, 29),
    CftcMarketSpec("gold_futures", "Gold", "disaggregated", ("GOLD - COMMODITY EXCHANGE INC.",), 14, 15),
    CftcMarketSpec("silver", "Silver", "disaggregated", ("SILVER - COMMODITY EXCHANGE INC.",), 14, 15),
    CftcMarketSpec("crude_oil", "Crude Oil", "disaggregated", ("CRUDE OIL, LIGHT SWEET-WTI", "CRUDE OIL, LIGHT SWEET"), 14, 15),
    CftcMarketSpec("natural_gas", "Natural Gas", "disaggregated", ("NAT GAS NYME", "HENRY HUB PENULTIMATE NAT GAS"), 14, 15),
    CftcMarketSpec("copper", "Copper", "disaggregated", ("COPPER- #1", "COPPER-GRADE #1"), 14, 15),
)


def load_macro_positioning_mappings() -> dict[str, Any]:
    try:
        with _CONFIG_PATH.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def macro_positioning_feature_enabled(db: Session) -> bool:
    row = db.get(AppSetting, "feature_macro_positioning_enabled")
    value = (row.value or "").strip().lower() if row and row.value else ""
    if value in {"0", "false", "no", "off"}:
        return False
    return True


def ingest_macro_positioning_assets(db: Session) -> dict[str, Any]:
    fetched_at = datetime.now(timezone.utc)
    source_rows = _fetch_cftc_positioning_rows()
    if not any(source_rows.values()):
        return {"status": "unavailable", "refreshed": 0, "missing": [spec.asset_key for spec in _CFTC_MARKET_SPECS]}

    refreshed = 0
    missing: list[str] = []
    for spec in _CFTC_MARKET_SPECS:
        parsed = _positioning_payload_from_rows(spec, source_rows.get(spec.source, []), fetched_at=fetched_at)
        if parsed is None:
            missing.append(spec.asset_key)
            continue
        row = db.get(MacroPositioningAsset, spec.asset_key)
        if row is None:
            row = MacroPositioningAsset(
                asset_key=spec.asset_key,
                display_name=spec.display_name,
                bias=parsed["bias"],
                rating=parsed["rating"],
                positioning_date=parsed["positioning_date"],
                payload_json=parsed["payload_json"],
                fetched_at=fetched_at,
            )
            db.add(row)
        else:
            row.display_name = spec.display_name
            row.bias = parsed["bias"]
            row.rating = parsed["rating"]
            row.positioning_date = parsed["positioning_date"]
            row.payload_json = parsed["payload_json"]
            row.fetched_at = fetched_at
        refreshed += 1
    if refreshed:
        db.commit()
    return {
        "status": "ok" if not missing else "partial",
        "refreshed": refreshed,
        "missing": missing,
        "fetched_at": fetched_at.isoformat(),
    }


def _fetch_cftc_positioning_rows() -> dict[str, list[list[str]]]:
    return {
        "financial": _download_cftc_rows(_CFTC_FINANCIAL_FUTURES_URL),
        "disaggregated": _download_cftc_rows(_CFTC_DISAGG_FUTURES_URL),
    }


def _download_cftc_rows(url: str) -> list[list[str]]:
    try:
        response = requests.get(url, timeout=_CFTC_TIMEOUT_SECONDS)
        response.raise_for_status()
    except requests.RequestException:
        return []
    return _parse_cftc_rows(response.text)


def _parse_cftc_rows(text: str) -> list[list[str]]:
    rows: list[list[str]] = []
    for row in csv.reader(io.StringIO(text or "")):
        if len(row) >= 16 and str(row[0] or "").strip():
            rows.append(row)
    return rows


def _positioning_payload_from_rows(
    spec: CftcMarketSpec,
    rows: list[list[str]],
    *,
    fetched_at: datetime,
) -> dict[str, Any] | None:
    row = _find_cftc_row(rows, spec.match_terms)
    if row is None:
        return None
    positioning_date = _parse_cftc_date(_value_at(row, 2))
    long_contracts = _cftc_number(_value_at(row, spec.long_index))
    short_contracts = _cftc_number(_value_at(row, spec.short_index))
    if positioning_date is None or long_contracts is None or short_contracts is None:
        return None
    gross = abs(long_contracts) + abs(short_contracts)
    if gross <= 0:
        return None
    net_contracts = long_contracts - short_contracts
    net_score = max(-1.0, min(1.0, net_contracts / gross))
    bias = _bias_from_score(net_score)
    rating = _rating_from_score(net_score)
    trend = _trend_from_change(row, spec.long_change_index, spec.short_change_index)
    payload = {
        "headline": _headline_for_ingested_positioning(bias=bias, trend=trend),
        "interpretation": _interpretation_for_ingested_positioning(bias=bias, trend=trend),
        "net_position": round(net_contracts),
        "net_score": round(net_score, 4),
        "long_contracts": round(long_contracts),
        "short_contracts": round(short_contracts),
        "source_report_date": positioning_date.isoformat(),
        "source_market": str(row[0] or "").strip(),
        "source_family": spec.source,
        "fetched_at": fetched_at.isoformat(),
    }
    if trend is not None:
        payload["trend"] = trend
    return {
        "bias": bias,
        "rating": rating,
        "positioning_date": positioning_date,
        "payload_json": json.dumps(payload, separators=(",", ":")),
    }


def _find_cftc_row(rows: list[list[str]], match_terms: tuple[str, ...]) -> list[str] | None:
    normalized_terms = tuple(term.upper() for term in match_terms)
    for term in normalized_terms:
        for row in rows:
            market = str(row[0] or "").upper()
            if term in market:
                return row
    return None


def _value_at(row: list[str], index: int | None) -> str | None:
    if index is None or index < 0 or index >= len(row):
        return None
    return row[index]


def _cftc_number(value: Any) -> float | None:
    text = str(value or "").strip().replace(",", "")
    if not text or text == ".":
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _parse_cftc_date(value: Any) -> date | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def _bias_from_score(score: float) -> str:
    if score >= 0.15:
        return "bullish"
    if score <= -0.15:
        return "bearish"
    return "neutral"


def _rating_from_score(score: float) -> int:
    absolute = abs(score)
    if absolute >= 0.65:
        return 5
    if absolute >= 0.35:
        return 4
    if absolute >= 0.15:
        return 3
    return 2


def _trend_from_change(row: list[str], long_change_index: int | None, short_change_index: int | None) -> str | None:
    long_change = _cftc_number(_value_at(row, long_change_index))
    short_change = _cftc_number(_value_at(row, short_change_index))
    if long_change is None or short_change is None:
        return None
    net_change = long_change - short_change
    if net_change > 0:
        return "increasing"
    if net_change < 0:
        return "decreasing"
    return "stable"


def _headline_for_ingested_positioning(*, bias: str, trend: str | None) -> str:
    if trend == "increasing":
        return "Institutional positioning is increasing."
    if trend == "decreasing":
        return "Institutional positioning is decreasing."
    if trend == "stable":
        return "Institutional positioning is stable."
    if bias == "bullish":
        return "Institutional positioning is net long."
    if bias == "bearish":
        return "Institutional positioning is net short."
    return "Institutional positioning is balanced."


def _interpretation_for_ingested_positioning(*, bias: str, trend: str | None) -> str:
    base = {
        "bullish": "Positioning remains supportive.",
        "bearish": "Positioning remains cautious.",
        "neutral": "Positioning is balanced.",
    }[bias]
    if trend == "increasing":
        return f"{base} Net positioning improved in the latest weekly report."
    if trend == "decreasing":
        return f"{base} Net positioning softened in the latest weekly report."
    return base


def unavailable_macro_positioning_summary(symbol: str, *, status: str = "unavailable") -> dict[str, Any]:
    normalized = normalize_symbol(symbol) or str(symbol or "").strip().upper()
    return {
        "symbol": normalized,
        "status": status,
        "active": False,
        "summary": None,
        "drivers": [],
        "updated": None,
        "mapped_sector": None,
        "mapped_asset_class": None,
    }


def locked_macro_positioning_summary(symbol: str) -> dict[str, Any]:
    payload = unavailable_macro_positioning_summary(symbol, status="pro_locked")
    payload.update(
        {
            "locked": True,
            "required_plan": "pro",
            "title": "Macro Positioning",
            "summary": "Understand whether institutional macro positioning supports or conflicts with your investment thesis.",
            "subtitle": "Included with Walnut Pro.",
        }
    )
    return payload


def locked_insights_macro_positioning_payload() -> dict[str, Any]:
    return {
        "status": "locked",
        "entitlement": {"required_plan": "pro", "unlocked": False},
        "summary": None,
        "markets": [],
        "updated_at": None,
        "stale": False,
        "message": "See whether institutional futures positioning is bullish, bearish, crowded, or shifting across major markets.",
        "subtitle": "Included with Walnut Pro.",
    }


def get_insights_macro_positioning(db: Session) -> dict[str, Any]:
    if not macro_positioning_feature_enabled(db):
        return _insights_unavailable_payload(status="unavailable", message="Macro positioning is temporarily unavailable.")

    rows = db.execute(select(MacroPositioningAsset)).scalars().all()
    by_asset = {row.asset_key: row for row in rows}
    markets = [
        market
        for target in INSIGHTS_MACRO_POSITIONING_MARKETS
        if (market := _insights_market_from_asset(target, by_asset.get(target["asset_key"]))) is not None
    ]
    if not markets:
        return _insights_unavailable_payload(status="awaiting_first_refresh", message="Macro positioning will appear after the next weekly data refresh.")

    updated_dates = [
        datetime.combine(row.positioning_date, datetime.min.time(), timezone.utc)
        for row in rows
        if row.asset_key in {target["asset_key"] for target in INSIGHTS_MACRO_POSITIONING_MARKETS} and isinstance(row.positioning_date, date)
    ]
    fetched_dates = [row.fetched_at for row in rows if row.asset_key in {target["asset_key"] for target in INSIGHTS_MACRO_POSITIONING_MARKETS} and isinstance(row.fetched_at, datetime)]
    latest_positioning_date = max((market["positioning_date"] for market in markets if market.get("positioning_date")), default=None)
    stale = _is_stale_positioning_date(latest_positioning_date)
    updated_at = (max(fetched_dates) if fetched_dates else max(updated_dates) if updated_dates else datetime.now(timezone.utc)).isoformat()
    return {
        "status": "stale" if stale else "available",
        "entitlement": {"required_plan": "pro", "unlocked": True},
        "summary": _insights_positioning_summary(markets),
        "markets": [{key: value for key, value in market.items() if key != "positioning_date"} for market in markets],
        "updated_at": updated_at,
        "stale": stale,
        "message": "Latest weekly positioning data is delayed." if stale else None,
    }


def _insights_unavailable_payload(*, status: str, message: str) -> dict[str, Any]:
    return {
        "status": status,
        "entitlement": {"required_plan": "pro", "unlocked": True},
        "summary": None,
        "markets": [],
        "updated_at": None,
        "stale": False,
        "message": message,
    }


def _insights_market_from_asset(target: dict[str, str], row: MacroPositioningAsset | None) -> dict[str, Any] | None:
    if row is None or not isinstance(row.positioning_date, date):
        return None
    bias = _bias_value(row.bias)
    if bias not in {"bullish", "bearish", "neutral"}:
        return None
    payload = _loads_dict(row.payload_json)
    percentile = _first_number(payload, ("percentile", "positioning_percentile", "historical_percentile", "net_percentile"))
    trend = _trend_value(
        payload.get("trend")
        or payload.get("weekly_trend")
        or payload.get("trend_direction")
        or payload.get("positioning_direction")
    )
    trend_weeks = _first_int(payload, ("trend_weeks", "consecutive_weeks", "streak_weeks"))
    crowding = _crowding_label(percentile)
    headline = _clean_public_text(payload.get("headline")) or _headline_for_market(trend=trend, crowded=bool(crowding))
    interpretation = _clean_public_text(payload.get("interpretation")) or _interpretation_for_market(bias=bias, trend=trend, crowded=bool(crowding))
    positioning_date = row.positioning_date
    return {
        "id": target["id"],
        "name": target["name"],
        "bias": bias,
        "rating": max(1, min(int(row.rating or 3), 5)),
        "percentile": round(percentile) if percentile is not None else None,
        "trend": trend,
        "trend_weeks": trend_weeks,
        "headline": headline,
        "interpretation": interpretation,
        "crowding": crowding,
        "updated_at": datetime.combine(positioning_date, datetime.min.time(), timezone.utc).isoformat(),
        "positioning_date": positioning_date,
    }


def _loads_dict(value: str | None) -> dict[str, Any]:
    try:
        parsed = json.loads(value or "{}")
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _first_number(payload: dict[str, Any], keys: tuple[str, ...]) -> float | None:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, (int, float)):
            return max(0.0, min(float(value), 100.0))
        if isinstance(value, str):
            cleaned = value.strip().replace("%", "")
            try:
                return max(0.0, min(float(cleaned), 100.0))
            except ValueError:
                continue
    return None


def _first_int(payload: dict[str, Any], keys: tuple[str, ...]) -> int | None:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, int):
            return max(1, value)
        if isinstance(value, str) and value.strip().isdigit():
            return max(1, int(value.strip()))
    return None


def _trend_value(value: Any) -> str | None:
    text = str(value or "").strip().lower()
    if text in {"increasing", "rising", "improving", "strengthening"}:
        return "increasing"
    if text in {"decreasing", "falling", "weakening"}:
        return "decreasing"
    if text in {"stable", "flat", "unchanged", "balanced"}:
        return "stable"
    return None


def _crowding_label(percentile: float | None) -> str | None:
    if percentile is None:
        return None
    if percentile >= 85:
        return "crowded"
    if percentile <= 15:
        return "positioning extreme"
    return None


def _headline_for_market(*, trend: str | None, crowded: bool) -> str:
    if crowded:
        return "Institutional positioning is becoming crowded."
    if trend == "increasing":
        return "Institutional positioning is increasing."
    if trend == "decreasing":
        return "Institutional positioning is decreasing."
    if trend == "stable":
        return "Institutional positioning is stable."
    return "Institutional positioning is available for the latest weekly report."


def _interpretation_for_market(*, bias: str, trend: str | None, crowded: bool) -> str:
    bias_text = {
        "bullish": "Positioning remains supportive.",
        "bearish": "Positioning remains cautious.",
        "neutral": "Positioning is balanced.",
    }[bias]
    if crowded:
        return f"{bias_text} The market is also showing a positioning extreme."
    if trend == "increasing":
        return f"{bias_text} Weekly positioning is improving."
    if trend == "decreasing":
        return f"{bias_text} Weekly positioning is softening."
    return bias_text


def _clean_public_text(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = " ".join(value.strip().split())
    if not cleaned:
        return None
    forbidden = ("cot", "commitment of traders", "cftc", "fmp", "endpoint", "provider")
    return None if any(term in cleaned.lower() for term in forbidden) else cleaned


def _is_stale_positioning_date(value: date | None) -> bool:
    return value is not None and value < (datetime.now(timezone.utc).date() - timedelta(days=_INSIGHTS_STALE_AFTER_DAYS))


def _insights_positioning_summary(markets: list[dict[str, Any]]) -> str:
    bullish = [market["name"] for market in markets if market.get("bias") == "bullish"]
    bearish = [market["name"] for market in markets if market.get("bias") == "bearish"]
    crowded = [market["name"] for market in markets if market.get("crowding")]
    improving = [market["name"] for market in markets if market.get("trend") == "increasing"]
    parts: list[str] = []
    if improving:
        parts.append(f"Risk appetite improved as institutional positioning strengthened in {_join_names(improving[:2])}.")
    elif bullish:
        parts.append(f"Institutional positioning is bullish in {_join_names(bullish[:2])}.")
    if bearish:
        parts.append(f"Positioning remains cautious in {_join_names(bearish[:2])}.")
    if crowded:
        parts.append(f"{_join_names(crowded[:2])} shows crowded positioning.")
    if not parts:
        parts.append("Institutional futures positioning is broadly balanced across the supported markets.")
    return " ".join(parts)


def _join_names(values: list[str]) -> str:
    if not values:
        return "supported markets"
    if len(values) == 1:
        return values[0]
    return f"{', '.join(values[:-1])} and {values[-1]}"


def get_macro_positioning_summary(db: Session, symbol: str, *, feature_enabled: bool | None = None) -> dict[str, Any]:
    normalized = normalize_symbol(symbol)
    if not normalized:
        return unavailable_macro_positioning_summary(symbol, status="invalid_symbol")
    if feature_enabled is None:
        feature_enabled = macro_positioning_feature_enabled(db)
    if not feature_enabled:
        return unavailable_macro_positioning_summary(normalized, status="disabled")
    row = db.get(MacroPositioningCache, normalized)
    if row is None:
        return unavailable_macro_positioning_summary(normalized, status="unavailable")
    return macro_positioning_cache_payload(row)


def get_macro_positioning_summaries_for_symbols(
    db: Session,
    symbols: list[str],
    *,
    feature_enabled: bool | None = None,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    normalized_symbols = sorted({symbol for raw in symbols if (symbol := normalize_symbol(raw))})
    if feature_enabled is None:
        feature_enabled = macro_positioning_feature_enabled(db)
    if not normalized_symbols:
        return {}, {"enabled": feature_enabled, "status": "unavailable", "filterable": False}
    if not feature_enabled:
        return (
            {symbol: unavailable_macro_positioning_summary(symbol, status="disabled") for symbol in normalized_symbols},
            {"enabled": False, "status": "disabled", "filterable": False},
        )
    rows = db.execute(
        select(MacroPositioningCache).where(func.upper(MacroPositioningCache.symbol).in_(normalized_symbols))
    ).scalars().all()
    by_symbol = {row.symbol.upper(): macro_positioning_cache_payload(row) for row in rows}
    payload = {
        symbol: by_symbol.get(symbol, unavailable_macro_positioning_summary(symbol, status="unavailable"))
        for symbol in normalized_symbols
    }
    status = "ok" if any(item.get("active") for item in payload.values()) else "unavailable"
    return payload, {"enabled": True, "status": status, "filterable": status == "ok"}


def macro_positioning_cache_payload(row: MacroPositioningCache) -> dict[str, Any]:
    try:
        drivers = json.loads(row.drivers_json or "[]")
    except Exception:
        drivers = []
    if not isinstance(drivers, list):
        drivers = []
    active = row.status == "ok" and row.overall in {"bullish", "bearish", "neutral"} and bool(drivers)
    payload = {
        "symbol": row.symbol,
        "status": row.status,
        "active": active,
        "summary": row.summary,
        "drivers": [
            {
                "name": str(driver.get("name") or "").strip(),
                "bias": _bias_value(driver.get("bias")),
            }
            for driver in drivers
            if isinstance(driver, dict) and str(driver.get("name") or "").strip()
        ],
        "updated": row.updated.isoformat() if row.updated else None,
        "mapped_sector": row.mapped_sector,
        "mapped_asset_class": row.mapped_asset_class,
        "generated_at": row.generated_at.isoformat() if row.generated_at else None,
    }
    if active:
        payload["overall"] = row.overall
        payload["rating"] = max(1, min(int(row.rating or 3), 5))
    return payload


def refresh_macro_positioning_cache(
    db: Session,
    *,
    symbols: list[str] | None = None,
    asof_date: date | None = None,
) -> dict[str, Any]:
    normalized_symbols = sorted({symbol for raw in symbols or _known_symbols(db) if (symbol := normalize_symbol(raw))})
    assets = _latest_asset_payloads(db)
    updated = asof_date or _latest_asset_date(assets) or datetime.now(timezone.utc).date()
    generated_at = datetime.now(timezone.utc)
    refreshed = 0
    skipped = 0
    for symbol in normalized_symbols:
        mapping = _mapping_for_symbol(db, symbol)
        if mapping is None:
            skipped += 1
            continue
        interpreted = _interpret_mapping(symbol, mapping, assets, updated=updated, generated_at=generated_at)
        if interpreted is None:
            skipped += 1
            continue
        row = db.get(MacroPositioningCache, symbol)
        if row is None:
            row = MacroPositioningCache(symbol=symbol, summary=interpreted["summary"], updated=updated, generated_at=generated_at)
            db.add(row)
        row.status = "ok"
        row.overall = interpreted["overall"]
        row.rating = interpreted["rating"]
        row.summary = interpreted["summary"]
        row.drivers_json = json.dumps(interpreted["drivers"], separators=(",", ":"))
        row.mapped_sector = interpreted["mapped_sector"]
        row.mapped_asset_class = interpreted["mapped_asset_class"]
        row.updated = updated
        row.generated_at = generated_at
        row.source_refresh_at = interpreted["source_refresh_at"]
        refreshed += 1
    db.commit()
    return {"status": "ok", "refreshed": refreshed, "skipped": skipped, "updated": updated.isoformat()}


def _known_symbols(db: Session) -> list[str]:
    values: set[str] = set()
    for statement in (
        select(Security.symbol).where(Security.symbol.is_not(None)),
        select(TickerMeta.symbol).where(TickerMeta.symbol.is_not(None)),
        select(FundamentalsCache.symbol).where(FundamentalsCache.symbol.is_not(None)),
    ):
        values.update(str(value or "").strip().upper() for value in db.execute(statement).scalars().all())
    return sorted(value for value in values if value)


def _latest_asset_payloads(db: Session) -> dict[str, dict[str, Any]]:
    rows = db.execute(select(MacroPositioningAsset)).scalars().all()
    return {
        row.asset_key: {
            "name": row.display_name,
            "bias": _bias_value(row.bias),
            "rating": max(1, min(int(row.rating or 3), 5)),
            "positioning_date": row.positioning_date,
            "fetched_at": row.fetched_at,
        }
        for row in rows
    }


def _latest_asset_date(assets: dict[str, dict[str, Any]]) -> date | None:
    dates = [item.get("positioning_date") for item in assets.values() if isinstance(item.get("positioning_date"), date)]
    return max(dates) if dates else None


def _mapping_for_symbol(db: Session, symbol: str) -> MacroMapping | None:
    profile = _profile_for_symbol(db, symbol)
    config = load_macro_positioning_mappings()
    asset_class = str(profile.get("asset_class") or "").strip().lower()
    sector = str(profile.get("sector") or "").strip().lower()
    if asset_class:
        mapping = _resolve_mapping(config.get("asset_class_mappings"), asset_class)
        if mapping:
            return _mapping_from_config(asset_class, mapping, "asset_class")
    if sector:
        mapping = _resolve_mapping(config.get("sector_mappings"), sector)
        if mapping:
            return _mapping_from_config(sector, mapping, "sector")
    return None


def _profile_for_symbol(db: Session, symbol: str) -> dict[str, Any]:
    security = db.execute(select(Security).where(func.upper(Security.symbol) == symbol).limit(1)).scalar_one_or_none()
    meta = db.execute(select(TickerMeta).where(func.upper(TickerMeta.symbol) == symbol).limit(1)).scalar_one_or_none()
    fundamentals = db.execute(
        select(FundamentalsCache)
        .where(func.upper(FundamentalsCache.symbol) == symbol)
        .order_by(FundamentalsCache.fetched_at.desc())
        .limit(1)
    ).scalar_one_or_none()
    return {
        "asset_class": (security.asset_class if security else None) or "Equity",
        "sector": (security.sector if security else None) or (meta.sector if meta else None) or (fundamentals.sector if fundamentals else None),
    }


def _resolve_mapping(mappings: Any, key: str) -> dict[str, Any] | None:
    if not isinstance(mappings, dict):
        return None
    mapping = mappings.get(key)
    if not isinstance(mapping, dict):
        return None
    extends = mapping.get("extends")
    if isinstance(extends, str) and extends in mappings and isinstance(mappings[extends], dict):
        return mappings[extends]
    return mapping


def _mapping_from_config(key: str, mapping: dict[str, Any], mapping_type: str) -> MacroMapping | None:
    drivers = mapping.get("drivers")
    if not isinstance(drivers, list) or not drivers:
        return None
    return MacroMapping(
        key=key,
        label=str(mapping.get("label") or key).strip(),
        thesis_label=str(mapping.get("thesis_label") or "this ticker").strip(),
        headline=str(mapping.get("headline") or "Neutral macro backdrop.").strip(),
        drivers=[driver for driver in drivers if isinstance(driver, dict)],
        mapping_type=mapping_type,
    )


def _interpret_mapping(
    symbol: str,
    mapping: MacroMapping,
    assets: dict[str, dict[str, Any]],
    *,
    updated: date,
    generated_at: datetime,
) -> dict[str, Any] | None:
    driver_payloads: list[dict[str, str]] = []
    scores: list[float] = []
    source_refreshes: list[datetime] = []
    for driver in mapping.drivers:
        asset_key = str(driver.get("asset_key") or "").strip()
        asset = assets.get(asset_key)
        if not asset:
            continue
        bias = _bias_value(asset.get("bias"))
        effect = str(driver.get("effect") or "direct").strip().lower()
        score = _mapped_bias_score(bias, effect)
        scores.append(score)
        driver_payloads.append({"name": str(driver.get("name") or asset.get("name") or asset_key).strip(), "bias": bias})
        fetched_at = asset.get("fetched_at")
        if isinstance(fetched_at, datetime):
            source_refreshes.append(fetched_at)
    if not scores or not driver_payloads:
        return None
    avg = sum(scores) / len(scores)
    overall = "bullish" if avg >= 0.34 else "bearish" if avg <= -0.34 else "neutral"
    rating = 5 if avg >= 0.67 else 4 if avg >= 0.34 else 2 if avg <= -0.34 else 3
    summary = _summary_for_bias(overall, mapping.thesis_label)
    return {
        "symbol": symbol,
        "overall": overall,
        "rating": rating,
        "summary": summary,
        "drivers": driver_payloads,
        "mapped_sector": mapping.label if mapping.mapping_type == "sector" else None,
        "mapped_asset_class": mapping.label if mapping.mapping_type == "asset_class" else None,
        "updated": updated,
        "generated_at": generated_at,
        "source_refresh_at": max(source_refreshes) if source_refreshes else None,
    }


def _mapped_bias_score(bias: str, effect: str) -> float:
    score = _BIAS_SCORES.get(_bias_value(bias), 0.0)
    if effect in {"inverse", "inverse_yield"}:
        return -score
    if effect == "neutral":
        return 0.0
    return score


def _summary_for_bias(overall: str, thesis_label: str) -> str:
    if overall == "bullish":
        return f"Institutional positioning currently supports {thesis_label}."
    if overall == "bearish":
        return f"Institutional positioning currently conflicts with {thesis_label}."
    return "Institutional positioning is currently neutral for this investment thesis."


def _bias_value(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text in {"bullish", "bearish", "neutral"} else "neutral"
