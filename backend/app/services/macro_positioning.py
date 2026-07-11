from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models import AppSetting, FundamentalsCache, MacroPositioningAsset, MacroPositioningCache, Security, TickerMeta
from app.utils.symbols import normalize_symbol

_CONFIG_PATH = Path(__file__).resolve().parents[1] / "data" / "macro_positioning_mappings.json"
_BIAS_SCORES = {"bearish": -1.0, "neutral": 0.0, "bullish": 1.0}


@dataclass(frozen=True)
class MacroMapping:
    key: str
    label: str
    thesis_label: str
    headline: str
    drivers: list[dict[str, str]]
    mapping_type: str


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


def unavailable_macro_positioning_summary(symbol: str, *, status: str = "unavailable") -> dict[str, Any]:
    normalized = normalize_symbol(symbol) or str(symbol or "").strip().upper()
    return {
        "symbol": normalized,
        "status": status,
        "active": False,
        "overall": "neutral",
        "rating": 3,
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
    return {
        "symbol": row.symbol,
        "status": row.status,
        "active": row.status == "ok" and row.overall in {"bullish", "bearish", "neutral"} and bool(drivers),
        "overall": row.overall if row.overall in {"bullish", "bearish", "neutral"} else "neutral",
        "rating": max(1, min(int(row.rating or 3), 5)),
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
