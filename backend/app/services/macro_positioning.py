from __future__ import annotations

import csv
import hashlib
import io
import json
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import os
from pathlib import Path
from typing import Any

import requests
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models import AppSetting, FundamentalsCache, InsightsSnapshot, MacroPositioningAsset, MacroPositioningCache, MacroPositioningFeedEvent, Security, TickerMeta
from app.services.openai_request_audit import audited_openai_request
from app.utils.symbols import normalize_symbol

logger = logging.getLogger(__name__)

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
_MARKET_META_BY_ASSET = {
    "sp_futures": {"id": "sp-500", "group": "equity_indexes"},
    "nasdaq_futures": {"id": "nasdaq-100", "group": "equity_indexes"},
    "russell_2000_futures": {"id": "russell-2000", "group": "equity_indexes"},
    "us_dollar": {"id": "us-dollar", "group": "currencies"},
    "gold_futures": {"id": "gold", "group": "commodities"},
    "silver": {"id": "silver", "group": "commodities"},
    "crude_oil": {"id": "crude-oil", "group": "commodities"},
    "natural_gas": {"id": "natural-gas", "group": "commodities"},
    "copper": {"id": "copper", "group": "commodities"},
    "bitcoin_futures": {"id": "bitcoin", "group": "crypto"},
    "ten_year_treasury": {"id": "us-treasuries", "group": "rates"},
}
_FEED_PAGE_SIZES = {25, 50, 100}
OPENAI_RESPONSES_ENDPOINT = "https://api.openai.com/v1/responses"
MACRO_AI_SUMMARY_KIND = "macro-positioning-ai-summary"
DEFAULT_MACRO_AI_SUMMARY_MODEL = "gpt-5.4-nano"
MACRO_AI_SUMMARY_MODEL = "MACRO_AI_SUMMARY_MODEL"
MACRO_AI_SUMMARY_MAX_CHARS = 520
MACRO_TICKER_INTERPRETATION_MODEL = "MACRO_TICKER_INTERPRETATION_MODEL"
DEFAULT_MACRO_TICKER_INTERPRETATION_MODEL = "gpt-5.4-nano"
MACRO_IMPACT_SCORES = {
    "STRONG_TAILWIND": 2,
    "TAILWIND": 1,
    "NEUTRAL": 0,
    "HEADWIND": -1,
    "STRONG_HEADWIND": -2,
}
MACRO_RELEVANCE_WEIGHTS = {"HIGH": 1.0, "MEDIUM": 0.6, "LOW": 0.25}
_TICKER_FACTOR_SPECS = (
    {"asset_key": "nasdaq_futures", "factor": "NASDAQ_100_FUTURES", "name": "Nasdaq 100 Futures", "category": "RISK APPETITE"},
    {"asset_key": "us_dollar", "factor": "US_DOLLAR", "name": "US Dollar", "category": "FX CONDITIONS"},
    {"asset_key": "ten_year_treasury", "factor": "US_10Y_YIELD", "name": "10-Year Treasury Yield", "category": "RATES / DISCOUNT RATE"},
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
        return f"Institutional positioning is {_positioning_stance(bias)} and increasing."
    if trend == "decreasing":
        return f"Institutional positioning is {_positioning_stance(bias)}, but decreasing."
    if trend == "stable":
        return f"Institutional positioning is {_positioning_stance(bias)} and little changed."
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
    feed_fallbacks = _latest_feed_current_state_by_market(db)
    markets = [
        market
        for target in INSIGHTS_MACRO_POSITIONING_MARKETS
        if (market := _insights_market_from_asset(target, by_asset.get(target["asset_key"])) or _insights_market_from_feed_event(target, feed_fallbacks.get(target["id"]))) is not None
    ]
    if not markets:
        return _insights_unavailable_payload(status="awaiting_first_refresh", message="Macro positioning will appear after the next weekly data refresh.")

    updated_dates = [
        datetime.combine(row.positioning_date, datetime.min.time(), timezone.utc)
        for row in rows
        if row.asset_key in {target["asset_key"] for target in INSIGHTS_MACRO_POSITIONING_MARKETS} and isinstance(row.positioning_date, date)
    ]
    fetched_dates = [row.fetched_at for row in rows if row.asset_key in {target["asset_key"] for target in INSIGHTS_MACRO_POSITIONING_MARKETS} and isinstance(row.fetched_at, datetime)]
    fetched_dates.extend(row.generated_at for row in feed_fallbacks.values() if isinstance(row.generated_at, datetime))
    latest_positioning_date = max((market["positioning_date"] for market in markets if market.get("positioning_date")), default=None)
    stale = _is_stale_positioning_date(latest_positioning_date)
    updated_at = (max(fetched_dates) if fetched_dates else max(updated_dates) if updated_dates else datetime.now(timezone.utc)).isoformat()
    summary = _market_picture_summary(db, markets)
    return {
        "status": "stale" if stale else "available",
        "entitlement": {"required_plan": "pro", "unlocked": True},
        "summary": summary["text"],
        "summary_source": summary["source"],
        "summary_generated_at": summary.get("generated_at"),
        "markets": [{key: value for key, value in market.items() if key != "positioning_date"} for market in markets],
        "updated_at": updated_at,
        "stale": stale,
        "message": "Latest weekly positioning data is delayed." if stale else None,
    }


def locked_macro_positioning_feed_payload() -> dict[str, Any]:
    return {
        "status": "locked",
        "entitlement": {"required_plan": "pro", "unlocked": False},
        "cadence": "weekly",
        "locked_copy": "Track major shifts, trends, and historical extremes in institutional futures positioning.",
        "items": [],
        "pagination": {"page": 1, "page_size": 25, "total": 0},
        "page_size_options": [25, 50, 100],
    }


def refresh_macro_positioning_feed_events(db: Session) -> dict[str, Any]:
    rows = db.execute(select(MacroPositioningAsset)).scalars().all()
    supported_asset_keys = {target["asset_key"] for target in INSIGHTS_MACRO_POSITIONING_MARKETS}
    latest_date = max((row.positioning_date for row in rows if row.asset_key in supported_asset_keys and isinstance(row.positioning_date, date)), default=None)
    if latest_date is None:
        return {"status": "unavailable", "generated": 0, "significant": 0, "suppressed": 0, "summary": "missing"}
    latest_rows = [row for row in rows if row.asset_key in supported_asset_keys and row.positioning_date == latest_date]
    if len({row.asset_key for row in latest_rows}) < len(supported_asset_keys):
        return {"status": "partial", "generated": 0, "significant": 0, "suppressed": len(latest_rows), "summary": "missing"}

    generated_at = datetime.now(timezone.utc)
    markets = [
        market
        for target in INSIGHTS_MACRO_POSITIONING_MARKETS
        for row in latest_rows
        if row.asset_key == target["asset_key"]
        if (market := _feed_market_from_asset(target, row, generated_at=generated_at)) is not None
    ]
    if not markets:
        return {"status": "unavailable", "generated": 0, "significant": 0, "suppressed": 0, "summary": "missing"}

    generated = 0
    significant = 0
    suppressed = 0
    for market in markets:
        current_event = _feed_event_payload(market, "current_state")
        _upsert_macro_feed_event(db, current_event)
        generated += 1
        event_kind, state, score = _significant_event_for_market(market)
        if event_kind:
            significant_event = _feed_event_payload(market, event_kind, state=state, significance=score)
            _upsert_macro_feed_event(db, significant_event)
            generated += 1
            significant += 1
        else:
            suppressed += 1

    summary = _macro_feed_summary(markets)
    summary_status = "created" if summary else "missing"
    if summary:
        summary_event = {
            "event_id": f"macro:summary:{latest_date.isoformat()}",
            "report_date": latest_date,
            "market_id": "summary",
            "market_name": "Weekly Summary",
            "market_group": "summary",
            "positioning": "summary",
            "crowded": False,
            "weekly_change": None,
            "percentile": None,
            "trend": None,
            "trend_weeks": None,
            "event_kind": "summary",
            "insight": None,
            "summary": summary,
            "significance": 0,
            "is_summary": True,
            "generated_at": generated_at,
        }
        _upsert_macro_feed_event(db, summary_event)
        generated += 1
    db.commit()
    return {
        "status": "ok",
        "generated": generated,
        "significant": significant,
        "suppressed": suppressed,
        "report_date": latest_date.isoformat(),
        "summary": summary_status,
    }


def get_macro_positioning_feed(
    db: Session,
    *,
    page: int = 1,
    page_size: int = 25,
    view: str = "significant",
    market: str | None = None,
    positioning: str | None = None,
    event: str | None = None,
    sort: str | None = None,
) -> dict[str, Any]:
    page = max(1, int(page or 1))
    page_size = int(page_size or 25)
    if page_size not in _FEED_PAGE_SIZES:
        page_size = 25
    normalized_view = "all" if str(view or "").strip().lower() == "all" else "significant"
    latest_report_date = db.execute(select(func.max(MacroPositioningFeedEvent.report_date))).scalar()
    summary_row = None
    if isinstance(latest_report_date, date):
        summary_row = db.execute(
            select(MacroPositioningFeedEvent)
            .where(MacroPositioningFeedEvent.report_date == latest_report_date, MacroPositioningFeedEvent.is_summary.is_(True))
            .limit(1)
        ).scalar_one_or_none()

    statement = select(MacroPositioningFeedEvent).where(MacroPositioningFeedEvent.is_summary.is_(False))
    if normalized_view == "all":
        statement = statement.where(MacroPositioningFeedEvent.event_kind == "current_state")
    else:
        statement = statement.where(MacroPositioningFeedEvent.event_kind != "current_state")
    if market and market != "all":
        statement = statement.where(MacroPositioningFeedEvent.market_group == market)
    if positioning and positioning != "all":
        if positioning == "crowded":
            statement = statement.where(MacroPositioningFeedEvent.crowded.is_(True))
        else:
            statement = statement.where(MacroPositioningFeedEvent.positioning == positioning)
    if event and event != "all":
        statement = statement.where(MacroPositioningFeedEvent.event_kind == event)

    filtered_rows = db.execute(statement).scalars().all()
    filtered_rows = _sort_macro_feed_rows(filtered_rows, sort)
    total = len(filtered_rows)
    page_rows = filtered_rows[(page - 1) * page_size : page * page_size]
    updated_at = max((row.generated_at for row in filtered_rows if isinstance(row.generated_at, datetime)), default=None)
    if summary_row and isinstance(summary_row.generated_at, datetime):
        updated_at = max(updated_at, summary_row.generated_at) if updated_at else summary_row.generated_at
    return {
        "status": "available" if latest_report_date else "awaiting_first_refresh",
        "entitlement": {"required_plan": "pro", "unlocked": True},
        "report_date": latest_report_date.isoformat() if isinstance(latest_report_date, date) else None,
        "updated_at": updated_at.isoformat() if updated_at else None,
        "cadence": "weekly",
        "summary": summary_row.summary if summary_row else None,
        "items": [_macro_feed_event_payload(row) for row in page_rows],
        "pagination": {"page": page, "page_size": page_size, "total": total},
        "page_size_options": [25, 50, 100],
        "view": normalized_view,
    }


def _feed_market_from_asset(target: dict[str, str], row: MacroPositioningAsset, *, generated_at: datetime) -> dict[str, Any] | None:
    base = _insights_market_from_asset(target, row)
    if base is None:
        return None
    payload = _loads_dict(row.payload_json)
    meta = _MARKET_META_BY_ASSET.get(target["asset_key"], {"id": target["id"], "group": "other"})
    net_score = _number_or_none(payload.get("net_score"))
    crowded = bool(base.get("crowding"))
    percentile = base.get("percentile")
    trend = base.get("trend")
    trend_weeks = base.get("trend_weeks")
    return {
        "market_id": meta["id"],
        "market_name": target["name"],
        "market_group": meta["group"],
        "positioning": base["bias"],
        "crowded": crowded,
        "weekly_change": _weekly_change_label(trend),
        "percentile": percentile if isinstance(percentile, (int, float)) else None,
        "trend": trend,
        "trend_weeks": trend_weeks if isinstance(trend_weeks, int) else None,
        "insight": base.get("interpretation"),
        "report_date": row.positioning_date,
        "updated_at": row.fetched_at,
        "generated_at": generated_at,
        "net_score": net_score,
    }


def _number_or_none(value: Any) -> float | None:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip())
        except ValueError:
            return None
    return None


def _weekly_change_label(trend: str | None) -> str | None:
    if trend == "increasing":
        return "Strengthening"
    if trend == "decreasing":
        return "Weakening"
    if trend == "stable":
        return "Little changed"
    return None


def _feed_event_payload(market: dict[str, Any], event_kind: str, *, state: str | None = None, significance: int = 0) -> dict[str, Any]:
    report_date = market["report_date"]
    event_state = state or ("state" if event_kind == "current_state" else str(market["positioning"]))
    event_id = f"macro:{market['market_id']}:{report_date.isoformat()}:{event_kind}:{event_state}"
    return {
        "event_id": event_id,
        "report_date": report_date,
        "market_id": market["market_id"],
        "market_name": market["market_name"],
        "market_group": market["market_group"],
        "positioning": market["positioning"],
        "crowded": bool(market.get("crowded")),
        "weekly_change": market.get("weekly_change"),
        "percentile": market.get("percentile"),
        "trend": market.get("trend"),
        "trend_weeks": market.get("trend_weeks"),
        "event_kind": event_kind,
        "insight": _event_insight(market, event_kind),
        "summary": None,
        "significance": significance,
        "is_summary": False,
        "generated_at": market["generated_at"],
    }


def _event_insight(market: dict[str, Any], event_kind: str) -> str:
    if event_kind == "historical_extreme" and market.get("crowded"):
        return f"{market['market_name']} positioning remains {market['positioning']}, although exposure is in a historically elevated range."
    if event_kind == "trend_milestone":
        trend = "strengthened" if market.get("trend") == "increasing" else "weakened"
        weeks = market.get("trend_weeks")
        return f"{market['market_name']} positioning {trend} for {weeks} consecutive reports."
    if event_kind == "crowding":
        return f"{market['market_name']} positioning remains {market['positioning']} and historically crowded."
    if event_kind == "major_shift":
        direction = "strongly net long" if market.get("positioning") == "bullish" else "strongly net short"
        return f"{market['market_name']} positioning is {direction} in the latest weekly report."
    return str(market.get("insight") or "Latest weekly positioning is available.")


def _significant_event_for_market(market: dict[str, Any]) -> tuple[str | None, str | None, int]:
    percentile = market.get("percentile")
    if isinstance(percentile, (int, float)):
        if percentile >= 95:
            return "historical_extreme", "upper_5", 95
        if percentile >= 90:
            return "historical_extreme", "upper_10", 90
        if percentile <= 5:
            return "historical_extreme", "lower_5", 95
        if percentile <= 10:
            return "historical_extreme", "lower_10", 90
    trend = market.get("trend")
    trend_weeks = market.get("trend_weeks")
    if trend in {"increasing", "decreasing"} and isinstance(trend_weeks, int) and trend_weeks >= 3:
        return "trend_milestone", f"{trend}_{trend_weeks}", 80 + min(trend_weeks, 10)
    if market.get("crowded") and market.get("positioning") in {"bullish", "bearish"}:
        return "crowding", str(market.get("positioning")), 75
    net_score = market.get("net_score")
    if isinstance(net_score, (int, float)) and abs(float(net_score)) >= 0.35 and market.get("positioning") in {"bullish", "bearish"}:
        return "major_shift", str(market.get("positioning")), 70
    return None, None, 0


def _upsert_macro_feed_event(db: Session, payload: dict[str, Any]) -> None:
    row = db.get(MacroPositioningFeedEvent, payload["event_id"])
    if row is None:
        row = MacroPositioningFeedEvent(event_id=payload["event_id"], report_date=payload["report_date"], generated_at=payload["generated_at"])
        db.add(row)
    row.report_date = payload["report_date"]
    row.market_id = payload["market_id"]
    row.market_name = payload["market_name"]
    row.market_group = payload["market_group"]
    row.positioning = payload["positioning"]
    row.crowded = bool(payload.get("crowded"))
    row.weekly_change = payload.get("weekly_change")
    row.percentile = payload.get("percentile")
    row.trend = payload.get("trend")
    row.trend_weeks = payload.get("trend_weeks")
    row.event_kind = payload["event_kind"]
    row.insight = payload.get("insight")
    row.summary = payload.get("summary")
    row.significance = int(payload.get("significance") or 0)
    row.is_summary = bool(payload.get("is_summary"))
    row.generated_at = payload["generated_at"]


def _macro_feed_summary(markets: list[dict[str, Any]]) -> str | None:
    if len(markets) < 3:
        return None
    strengthening = [market["market_name"] for market in markets if market.get("trend") == "increasing"]
    weakening = [market["market_name"] for market in markets if market.get("trend") == "decreasing"]
    crowded = [market["market_name"] for market in markets if market.get("crowded")]
    bullish = [market["market_name"] for market in markets if market.get("positioning") == "bullish"]
    bearish = [market["market_name"] for market in markets if market.get("positioning") == "bearish"]
    parts: list[str] = []
    if strengthening:
        parts.append(f"Institutional positioning strengthened in {_join_names(strengthening[:2])}.")
    elif bullish:
        parts.append(f"Institutional positioning remains supportive in {_join_names(bullish[:2])}.")
    if weakening:
        parts.append(f"Positioning weakened in {_join_names(weakening[:2])}.")
    elif bearish:
        parts.append(f"Positioning remains cautious in {_join_names(bearish[:2])}.")
    if crowded:
        parts.append(f"{_join_names(crowded[:2])} is in a historically crowded range.")
    return " ".join(parts) if parts else "Institutional futures positioning is broadly balanced across the supported weekly markets."


def _sort_macro_feed_rows(rows: list[MacroPositioningFeedEvent], sort: str | None) -> list[MacroPositioningFeedEvent]:
    normalized = str(sort or "latest").strip().lower()
    if normalized == "oldest":
        return sorted(rows, key=lambda row: (row.report_date, row.market_name))
    if normalized == "market":
        return sorted(rows, key=lambda row: (row.market_name, row.report_date), reverse=False)
    if normalized == "percentile":
        return sorted(rows, key=lambda row: (row.percentile is None, -(row.percentile or -1), row.market_name))
    return sorted(rows, key=lambda row: (row.report_date, row.significance, row.market_name), reverse=True)


def _macro_feed_event_payload(row: MacroPositioningFeedEvent) -> dict[str, Any]:
    return {
        "event_id": row.event_id,
        "market_id": row.market_id,
        "market_name": row.market_name,
        "market_group": row.market_group,
        "positioning": row.positioning,
        "crowded": bool(row.crowded),
        "weekly_change": row.weekly_change,
        "percentile": round(row.percentile) if isinstance(row.percentile, (int, float)) else None,
        "trend": row.trend,
        "trend_weeks": row.trend_weeks,
        "event_kind": row.event_kind,
        "insight": row.insight,
        "report_date": row.report_date.isoformat() if row.report_date else None,
        "updated_at": row.generated_at.isoformat() if row.generated_at else None,
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


def _latest_feed_current_state_by_market(db: Session) -> dict[str, MacroPositioningFeedEvent]:
    latest_report_date = db.execute(
        select(func.max(MacroPositioningFeedEvent.report_date))
        .where(MacroPositioningFeedEvent.event_kind == "current_state")
        .where(MacroPositioningFeedEvent.is_summary.is_(False))
    ).scalar()
    if not isinstance(latest_report_date, date):
        return {}
    rows = db.execute(
        select(MacroPositioningFeedEvent)
        .where(MacroPositioningFeedEvent.report_date == latest_report_date)
        .where(MacroPositioningFeedEvent.event_kind == "current_state")
        .where(MacroPositioningFeedEvent.is_summary.is_(False))
    ).scalars().all()
    return {row.market_id: row for row in rows if row.market_id}


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
    headline = _clean_public_text(payload.get("headline")) or _headline_for_market(bias=bias, trend=trend, crowded=bool(crowding))
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


def _insights_market_from_feed_event(target: dict[str, str], row: MacroPositioningFeedEvent | None) -> dict[str, Any] | None:
    if row is None or not isinstance(row.report_date, date):
        return None
    bias = _bias_value(row.positioning)
    return {
        "id": target["id"],
        "name": target["name"],
        "bias": bias,
        "rating": 4 if bias in {"bullish", "bearish"} else 3,
        "percentile": round(row.percentile) if isinstance(row.percentile, (int, float)) else None,
        "trend": _trend_value(row.trend),
        "trend_weeks": row.trend_weeks if isinstance(row.trend_weeks, int) else None,
        "headline": _clean_public_text(row.insight) or _headline_for_market(bias=bias, trend=_trend_value(row.trend), crowded=bool(row.crowded)),
        "interpretation": _clean_public_text(row.insight) or _interpretation_for_market(bias=bias, trend=_trend_value(row.trend), crowded=bool(row.crowded)),
        "crowding": "crowded" if row.crowded else None,
        "updated_at": datetime.combine(row.report_date, datetime.min.time(), timezone.utc).isoformat(),
        "positioning_date": row.report_date,
    }


def _loads_dict(value: str | None) -> dict[str, Any]:
    try:
        parsed = json.loads(value or "{}")
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _loads_list(value: str | None) -> list[dict[str, Any]]:
    try:
        parsed = json.loads(value or "[]")
    except Exception:
        return []
    return [item for item in parsed if isinstance(item, dict)] if isinstance(parsed, list) else []


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


def _headline_for_market(*, bias: str, trend: str | None, crowded: bool) -> str:
    if crowded:
        return f"Institutional positioning is {_positioning_stance(bias)} and becoming crowded."
    if trend == "increasing":
        return f"Institutional positioning is {_positioning_stance(bias)} and increasing."
    if trend == "decreasing":
        return f"Institutional positioning is {_positioning_stance(bias)}, but decreasing."
    if trend == "stable":
        return f"Institutional positioning is {_positioning_stance(bias)} and little changed."
    return "Institutional positioning is available for the latest weekly report."


def _positioning_stance(bias: str) -> str:
    if bias == "bullish":
        return "net long"
    if bias == "bearish":
        return "net short"
    return "balanced"


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


def _market_picture_summary(db: Session, markets: list[dict[str, Any]]) -> dict[str, str | None]:
    """Return one cached model synthesis for the exact current positioning set."""
    fallback = _insights_positioning_summary(markets)
    fingerprint = _market_picture_fingerprint(markets)
    cached = db.get(InsightsSnapshot, MACRO_AI_SUMMARY_KIND)
    if cached is not None:
        cached_payload = _loads_dict(cached.payload_json)
        if cached_payload.get("fingerprint") == fingerprint and isinstance(cached_payload.get("text"), str):
            return {
                "text": _clean_macro_ai_summary(cached_payload["text"]) or fallback,
                "source": "openai",
                "generated_at": cached_payload.get("generated_at") if isinstance(cached_payload.get("generated_at"), str) else None,
            }

    # ai_marketing imports confirmation-score, which imports this module. Keep
    # this dependency lazy so normal application startup never forms a cycle.
    from app.services.ai_marketing import OPENAI_API_KEY, resolved_setting_value

    api_key = resolved_setting_value(db, OPENAI_API_KEY)
    if not api_key:
        return {"text": fallback, "source": "derived", "generated_at": None}

    try:
        model = os.getenv(MACRO_AI_SUMMARY_MODEL, "").strip() or DEFAULT_MACRO_AI_SUMMARY_MODEL
        text = _generate_market_picture_summary(db, api_key=api_key, model=model, markets=markets)
        if not text:
            raise ValueError("OpenAI macro summary response was empty.")
        generated_at = datetime.now(timezone.utc).isoformat()
        payload = {"fingerprint": fingerprint, "text": text, "model": model, "generated_at": generated_at}
        if cached is None:
            db.add(InsightsSnapshot(kind=MACRO_AI_SUMMARY_KIND, payload_json=json.dumps(payload), source="openai", fetched_at=datetime.now(timezone.utc)))
        else:
            cached.payload_json = json.dumps(payload)
            cached.source = "openai"
            cached.fetched_at = datetime.now(timezone.utc)
            cached.updated_at = datetime.now(timezone.utc)
        db.commit()
        return {"text": text, "source": "openai", "generated_at": generated_at}
    except Exception:
        # Positioning remains usable even during an API outage; the next request
        # retries the model synthesis rather than persisting a generic fallback.
        logger.exception("macro_positioning_ai_summary_failed")
        db.rollback()
        return {"text": fallback, "source": "derived", "generated_at": None}


def _market_picture_fingerprint(markets: list[dict[str, Any]]) -> str:
    compact = [
        {
            "id": market.get("id"),
            "bias": market.get("bias"),
            "trend": market.get("trend"),
            "crowding": market.get("crowding"),
            "percentile": market.get("percentile"),
            "positioning_date": market.get("positioning_date").isoformat() if isinstance(market.get("positioning_date"), date) else None,
        }
        for market in sorted(markets, key=lambda value: str(value.get("id") or ""))
    ]
    return hashlib.sha256(json.dumps(compact, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


def _generate_market_picture_summary(db: Session, *, api_key: str, model: str, markets: list[dict[str, Any]]) -> str:
    from app.services.ai_marketing import _record_openai_usage_cost

    market_data = [
        {
            "market": market.get("name"),
            "id": market.get("id"),
            "positioning": market.get("bias"),
            "trend": market.get("trend"),
            "crowding": market.get("crowding"),
            "percentile": market.get("percentile"),
        }
        for market in markets
    ]
    prompt = "\n".join(
        [
            "Write the Market Summary for a professional investor dashboard from the supplied institutional positioning data.",
            "Synthesize the entire picture across equities, rates, the US dollar, gold, oil, bitcoin, and the resulting risk-on/risk-off regime.",
            "Use only the supplied data. State where signals agree or conflict and what that implies for the current market regime.",
            "Return two concise plain-English sentences, 160-420 characters total. No heading, bullets, disclaimer, trading instruction, or invented facts.",
            "For Treasuries, bullish positioning means a yields-down/rates-down read; bearish means yields-up/rates-up.",
            "Data:",
            json.dumps(market_data, sort_keys=True),
        ]
    )
    request_payload = {
        "model": model,
        "input": prompt,
        "store": False,
        "reasoning": {"effort": "none"},
        "text": {"verbosity": "low"},
        "max_output_tokens": 180,
    }
    response = audited_openai_request(
        feature="macro_positioning_summary",
        operation="market_picture_summary",
        method="POST",
        endpoint=OPENAI_RESPONSES_ENDPOINT,
        payload=request_payload,
        model=model,
        send=lambda: requests.post(
            OPENAI_RESPONSES_ENDPOINT,
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json=request_payload,
            timeout=20,
        ),
    )
    if response.status_code >= 400:
        raise RuntimeError(f"OpenAI macro summary request failed with status {response.status_code}.")
    data = response.json()
    _record_openai_usage_cost(db, model=model, data=data, feature="macro_positioning_summary", commit=False)
    return _clean_macro_ai_summary(_extract_openai_response_text(data)) or ""


def _clean_macro_ai_summary(value: Any) -> str | None:
    text = " ".join(str(value or "").split()).replace("Market Summary:", "").strip()
    if not text:
        return None
    text = text[:MACRO_AI_SUMMARY_MAX_CHARS].rstrip(" ,;:-")
    if text[-1] not in ".!?":
        text = f"{text}."
    return text


def _extract_openai_response_text(data: dict[str, Any]) -> str:
    output_text = data.get("output_text")
    if isinstance(output_text, str) and output_text.strip():
        return output_text.strip()
    texts: list[str] = []
    for item in _walk_response_dicts(data):
        if str(item.get("type") or "") in {"output_text", "text"} and isinstance(item.get("text"), str) and item["text"].strip():
            texts.append(item["text"].strip())
    return "\n".join(dict.fromkeys(texts))


def _walk_response_dicts(value: Any):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from _walk_response_dicts(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk_response_dicts(child)


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
    driver_payloads = [
        {
            **driver,
            "name": str(driver.get("name") or "").strip(),
            "bias": _bias_value(driver.get("bias")),
            "impact_score": int(driver.get("impact_score") or 0) if str(driver.get("impact_score") or "").lstrip("-").isdigit() else 0,
        }
        for driver in drivers
        if isinstance(driver, dict) and str(driver.get("name") or "").strip()
    ]
    active = row.status == "ok" and row.overall in {"bullish", "bearish", "neutral"} and bool(driver_payloads)
    display_overall = _overall_from_visible_drivers(row.overall, driver_payloads) if active else row.overall
    display_summary = row.summary
    if active and display_overall != row.overall:
        display_summary = _summary_for_bias(display_overall, "this investment thesis")
    payload = {
        "symbol": row.symbol,
        "status": row.status,
        "active": active,
        "summary": display_summary,
        "drivers": driver_payloads,
        "updated": row.updated.isoformat() if row.updated else None,
        "mapped_sector": row.mapped_sector,
        "mapped_asset_class": row.mapped_asset_class,
        "generated_at": row.generated_at.isoformat() if row.generated_at else None,
    }
    if active:
        payload["overall"] = display_overall
        payload["rating"] = max(1, min(int(row.rating or 3), 5))
        rich_factors = [driver for driver in driver_payloads if driver.get("factor")]
        if rich_factors:
            aggregate = aggregate_macro_factor_scores(rich_factors)
            payload["overall_state"] = macro_overall_state(aggregate)
            payload["aggregate_score"] = aggregate
            payload["counts"] = {
                "tailwinds": sum(1 for driver in rich_factors if int(driver.get("impact_score") or 0) > 0),
                "headwinds": sum(1 for driver in rich_factors if int(driver.get("impact_score") or 0) < 0),
                "neutral": sum(1 for driver in rich_factors if int(driver.get("impact_score") or 0) == 0),
            }
            payload["watch_items"] = [str(driver.get("watch_condition")) for driver in rich_factors if isinstance(driver.get("watch_condition"), str) and driver.get("watch_condition")][:3]
    return payload


def _overall_from_visible_drivers(overall: str, drivers: list[dict[str, str]]) -> str:
    normalized_overall = _bias_value(overall)
    if normalized_overall != "neutral":
        return normalized_overall
    biases = [_bias_value(driver.get("bias")) for driver in drivers]
    if biases and all(bias == "bullish" for bias in biases):
        return "bullish"
    if biases and all(bias == "bearish" for bias in biases):
        return "bearish"
    return normalized_overall


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
        interpreted = _interpret_mapping(symbol, mapping, assets, profile=_profile_for_symbol(db, symbol), updated=updated, generated_at=generated_at)
        if interpreted is None:
            skipped += 1
            continue
        row = db.get(MacroPositioningCache, symbol)
        interpreted = _apply_cached_ticker_interpretation(
            db,
            symbol=symbol,
            profile=_profile_for_symbol(db, symbol),
            interpreted=interpreted,
            previous=row,
        )
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
            "payload": _loads_dict(row.payload_json),
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
        "company_name": (meta.company_name if meta else None) or (fundamentals.company_name if fundamentals else None) or (security.name if security else None) or symbol,
        "industry": (meta.industry if meta else None) or (fundamentals.industry if fundamentals else None),
        "country": (meta.country if meta else None) or (fundamentals.country if fundamentals else None),
        "price_to_sales": getattr(fundamentals, "price_to_sales", None) if fundamentals else None,
        "market_cap": getattr(fundamentals, "market_cap", None) if fundamentals else None,
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
    profile: dict[str, Any],
    updated: date,
    generated_at: datetime,
) -> dict[str, Any] | None:
    drivers = _ticker_macro_factors(mapping, assets, profile)
    if not drivers:
        return None
    aggregate = aggregate_macro_factor_scores(drivers)
    overall_state = macro_overall_state(aggregate)
    overall = "bullish" if aggregate > 0.20 else "bearish" if aggregate < -0.20 else "neutral"
    rating = 5 if aggregate >= 1.25 else 4 if aggregate >= 0.35 else 2 if aggregate <= -0.35 else 3
    summary = _ticker_macro_summary(symbol, drivers, overall_state)
    source_refreshes = [driver["source_refresh_at"] for driver in drivers if isinstance(driver.get("source_refresh_at"), datetime)]
    for driver in drivers:
        driver.pop("source_refresh_at", None)
    return {
        "symbol": symbol,
        "overall": overall,
        "rating": rating,
        "summary": summary,
        "drivers": drivers,
        "mapped_sector": mapping.label if mapping.mapping_type == "sector" else None,
        "mapped_asset_class": mapping.label if mapping.mapping_type == "asset_class" else None,
        "updated": updated,
        "generated_at": generated_at,
        "source_refresh_at": max(source_refreshes) if source_refreshes else None,
        "aggregate_score": aggregate,
        "overall_state": overall_state,
    }


def _ticker_macro_factors(mapping: MacroMapping, assets: dict[str, dict[str, Any]], profile: dict[str, Any]) -> list[dict[str, Any]]:
    configured = {str(item.get("asset_key") or ""): item for item in mapping.drivers}
    factors: list[dict[str, Any]] = []
    for spec in _TICKER_FACTOR_SPECS:
        asset = assets.get(spec["asset_key"])
        if not asset:
            continue
        configured_driver = configured.get(spec["asset_key"], {})
        effect = str(configured_driver.get("effect") or _default_factor_effect(spec["asset_key"], mapping.key)).lower()
        relevance = _factor_relevance(spec["asset_key"], mapping.key, effect)
        bias = _bias_value(asset.get("bias"))
        # Treasury-futures positioning is first translated into an explicit
        # yield regime before ticker relevance is applied: bullish Treasury
        # positioning is a yields-easing read, not a generic "bullish rates"
        # label. This prevents directionally correct macro facts from being
        # displayed with the wrong ticker implication.
        if spec["asset_key"] == "ten_year_treasury" and effect == "inverse_yield":
            raw_score = _BIAS_SCORES[bias]
        elif spec["asset_key"] == "ten_year_treasury" and effect == "direct_yield":
            raw_score = -_BIAS_SCORES[bias]
        else:
            raw_score = _mapped_bias_score(bias, effect)
        impact_score = max(-2, min(2, int(round(raw_score * 2))))
        impact = _impact_from_score(impact_score)
        regime_label = _factor_regime_label(spec["asset_key"], _bias_value(asset.get("bias")), _loads_dict(asset.get("payload") if isinstance(asset.get("payload"), str) else None) or asset.get("payload") or {})
        factors.append(
            {
                "factor": spec["factor"],
                "name": spec["name"],
                "category": spec["category"],
                "bias": bias,
                "regime_label": regime_label,
                "ticker_impact": impact,
                "impact_score": impact_score,
                "confidence": "HIGH" if asset.get("positioning_date") else "MEDIUM",
                "relevance": relevance,
                "why_macro": _factor_why_macro(spec["asset_key"], regime_label, asset),
                "ticker_readthrough": _factor_readthrough(spec["asset_key"], profile, impact),
                "watch_condition": _factor_watch_condition(spec["asset_key"], regime_label, impact),
                "source_refresh_at": asset.get("fetched_at"),
            }
        )
    return factors


def _default_factor_effect(asset_key: str, sector: str) -> str:
    normalized = (sector or "").lower()
    if asset_key == "ten_year_treasury" and normalized in {"financials", "financial services"}:
        return "direct"
    if asset_key == "ten_year_treasury" and normalized in {"energy", "utilities"}:
        return "neutral"
    if asset_key == "us_dollar" and normalized in {"energy", "basic materials", "materials", "industrials"}:
        return "inverse"
    return "direct" if asset_key == "nasdaq_futures" else "inverse"


def _factor_relevance(asset_key: str, sector: str, effect: str) -> str:
    if effect == "neutral":
        return "LOW"
    normalized = (sector or "").lower()
    if normalized in {"technology", "communication services", "financials", "financial services", "real estate", "energy", "basic materials", "materials"}:
        return "HIGH" if asset_key != "nasdaq_futures" or normalized in {"technology", "communication services"} else "MEDIUM"
    return "MEDIUM"


def _factor_regime_label(asset_key: str, bias: str, payload: dict[str, Any]) -> str:
    trend = str(payload.get("trend") or "").lower()
    if asset_key == "nasdaq_futures":
        return "Risk-on / Positive positioning" if bias == "bullish" else "Risk-off / Defensive positioning" if bias == "bearish" else "Balanced risk appetite"
    if asset_key == "us_dollar":
        return "Dollar strengthening" if bias == "bullish" else "Dollar weakening" if bias == "bearish" else "Dollar range-bound"
    if asset_key == "ten_year_treasury":
        if bias == "bullish":
            return "Yields easing"
        if bias == "bearish":
            return "Yields rising"
        return "Yields stable"
    return "Positioning improving" if trend == "increasing" else "Positioning softening" if trend == "decreasing" else "Positioning balanced"


def _impact_from_score(score: int) -> str:
    return next((name for name, value in MACRO_IMPACT_SCORES.items() if value == score), "NEUTRAL")


def _factor_why_macro(asset_key: str, regime_label: str, asset: dict[str, Any]) -> str:
    report_date = asset.get("positioning_date")
    date_text = report_date.isoformat() if isinstance(report_date, date) else "the latest available report"
    if asset_key == "nasdaq_futures":
        return f"The latest CFTC futures-positioning report ({date_text}) shows a {regime_label.lower()} regime, indicating whether institutions are leaning into or away from growth-equity risk."
    if asset_key == "us_dollar":
        return f"The latest CFTC dollar futures-positioning report ({date_text}) is consistent with a {regime_label.lower()} regime."
    return f"The latest Treasury futures-positioning report ({date_text}) maps to {regime_label.lower()}; Treasury futures strength is read as easing yields."


def _factor_readthrough(asset_key: str, profile: dict[str, Any], impact: str) -> str:
    company = str(profile.get("company_name") or "This company")
    sector = str(profile.get("sector") or "the company’s sector").lower()
    if asset_key == "nasdaq_futures":
        if impact.endswith("TAILWIND"):
            return f"{company} is a {sector} equity whose valuation is sensitive to appetite for long-duration growth stocks. A risk-on Nasdaq backdrop can support its valuation multiple and capital flows into comparable equities; it is a sentiment signal, not a revenue forecast."
        if impact.endswith("HEADWIND"):
            return f"{company} is a {sector} equity whose valuation is sensitive to appetite for long-duration growth stocks. A defensive Nasdaq backdrop can pressure its valuation multiple and capital flows into comparable equities; it is a sentiment signal, not a revenue forecast."
        return f"{company} is a {sector} equity whose valuation is sensitive to appetite for long-duration growth stocks. The current Nasdaq backdrop is not providing a directional valuation signal."
    if asset_key == "us_dollar":
        if impact.endswith("HEADWIND"):
            return f"A stronger dollar can make {company}'s products less affordable in local-currency terms and reduce the dollar value of overseas sales, creating a potential headwind to global demand and translated results."
        if impact.endswith("TAILWIND"):
            return f"A weaker dollar can improve local-currency affordability for {company}'s products and lift the dollar value of overseas sales, supporting global demand and translated results."
        return f"Dollar conditions can affect overseas demand and translated results for {company}, but the current regime is not assigned a directional impact."
    if impact.endswith("TAILWIND"):
        return f"Easing Treasury yields lower the discount rate investors apply to {company}'s future cash flows. That generally supports the valuation of long-duration growth equities, including {company}, more than it changes near-term operating results."
    if impact.endswith("HEADWIND"):
        return f"Rising Treasury yields raise the discount rate investors apply to {company}'s future cash flows. That can pressure the valuation of long-duration growth equities, including {company}, more than it changes near-term operating results."
    return f"Treasury yields affect the discount rate investors apply to {company}'s future cash flows, but the current rates regime is not assigned a directional impact."


def _factor_watch_condition(asset_key: str, regime_label: str, impact: str) -> str:
    if asset_key == "nasdaq_futures":
        return "Sustained Nasdaq risk-on tone" if impact.endswith("TAILWIND") else "A reversal in Nasdaq risk appetite"
    if asset_key == "us_dollar":
        return "A DXY reversal would remove a headwind" if impact.endswith("HEADWIND") else "Whether dollar conditions remain supportive"
    return "Further easing in 10Y yields" if impact.endswith("TAILWIND") else "Whether 10Y yields keep rising"


def aggregate_macro_factor_scores(factors: list[dict[str, Any]]) -> float:
    weighted = [(float(factor.get("impact_score") or 0), MACRO_RELEVANCE_WEIGHTS.get(str(factor.get("relevance") or "MEDIUM"), 0.6)) for factor in factors]
    denominator = sum(weight for _score, weight in weighted)
    return round(sum(score * weight for score, weight in weighted) / denominator, 2) if denominator else 0.0


def macro_overall_state(score: float) -> str:
    if score >= 1.25:
        return "STRONGLY SUPPORTIVE"
    if score >= 0.35:
        return "MODERATELY SUPPORTIVE"
    if score <= -1.25:
        return "STRONGLY CHALLENGING"
    if score <= -0.35:
        return "MODERATELY CHALLENGING"
    return "NEUTRAL"


def _ticker_macro_summary(symbol: str, factors: list[dict[str, Any]], overall_state: str) -> str:
    supportive = [factor["name"] for factor in factors if int(factor.get("impact_score") or 0) > 0]
    challenging = [factor["name"] for factor in factors if int(factor.get("impact_score") or 0) < 0]
    if supportive and challenging:
        return f"Current macro conditions are a mixed but {overall_state.lower()} backdrop for {symbol}. {_join_names(supportive[:2])} support the setup, partly offset by {_join_names(challenging[:1])}."
    if supportive:
        return f"Current macro conditions are a {overall_state.lower()} backdrop for {symbol}, led by {_join_names(supportive[:2])}."
    if challenging:
        return f"Current macro conditions are a {overall_state.lower()} backdrop for {symbol}, with pressure from {_join_names(challenging[:2])}."
    return f"Current macro conditions are broadly neutral for {symbol}."


def _apply_cached_ticker_interpretation(
    db: Session,
    *,
    symbol: str,
    profile: dict[str, Any],
    interpreted: dict[str, Any],
    previous: MacroPositioningCache | None,
) -> dict[str, Any]:
    """Enrich a deterministic factor assessment in the background refresh only.

    The stored market facts, numeric scores, relevance and overall state never
    come from the model.  On a missing key, failed request, or stale cached
    interpretation the deterministic copy remains immediately renderable.
    """
    factors = interpreted["drivers"]
    fingerprint = _ticker_interpretation_fingerprint(symbol, profile, factors)
    previous_drivers = _loads_list(previous.drivers_json) if previous else []
    if previous_drivers and all(item.get("interpretation_fingerprint") == fingerprint for item in previous_drivers if isinstance(item, dict)):
        by_factor = {item.get("factor"): item for item in previous_drivers if isinstance(item, dict)}
        if all(isinstance(by_factor.get(item.get("factor")), dict) and by_factor[item.get("factor")].get("ticker_readthrough") for item in factors):
            for factor in factors:
                old = by_factor.get(factor.get("factor"), {})
                for key in ("why_macro", "ticker_readthrough", "watch_condition", "confidence"):
                    if isinstance(old.get(key), str) and old[key].strip():
                        factor[key] = old[key]
                factor["interpretation_fingerprint"] = fingerprint
                factor["interpretation_source"] = old.get("interpretation_source") or "cached"
            if previous and previous.summary:
                interpreted["summary"] = previous.summary
            return interpreted

    generated = _generate_ticker_interpretation(db, symbol=symbol, profile=profile, factors=factors)
    if generated:
        by_factor = {item["factor"]: item for item in generated["factors"]}
        for factor in factors:
            model_factor = by_factor.get(factor["factor"])
            if model_factor:
                # Validate categorical fields, but never allow the model to
                # alter the deterministic impact/relevance calculation.
                factor["why_macro"] = model_factor["why_macro"]
                factor["ticker_readthrough"] = model_factor["ticker_readthrough"]
                factor["watch_condition"] = model_factor["watch_condition"]
                factor["confidence"] = model_factor["confidence"]
                factor["interpretation_source"] = "openai"
        if generated.get("summary"):
            interpreted["summary"] = generated["summary"]
    for factor in factors:
        factor["interpretation_fingerprint"] = fingerprint
        factor.setdefault("interpretation_source", "derived")
    return interpreted


def _ticker_interpretation_fingerprint(symbol: str, profile: dict[str, Any], factors: list[dict[str, Any]]) -> str:
    material = {
        "symbol": symbol,
        "profile": {key: profile.get(key) for key in ("company_name", "sector", "industry", "country", "price_to_sales")},
        "factors": [{key: factor.get(key) for key in ("factor", "bias", "regime_label", "impact_score", "relevance")} for factor in factors],
    }
    return hashlib.sha256(json.dumps(material, sort_keys=True, default=str, separators=(",", ":")).encode("utf-8")).hexdigest()


def _ticker_interpretation_schema() -> dict[str, Any]:
    item = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "factor": {"type": "string", "enum": [spec["factor"] for spec in _TICKER_FACTOR_SPECS]},
            "confidence": {"type": "string", "enum": ["LOW", "MEDIUM", "HIGH"]},
            "why_macro": {"type": "string"},
            "ticker_readthrough": {"type": "string"},
            "watch_condition": {"type": "string"},
        },
        "required": ["factor", "confidence", "why_macro", "ticker_readthrough", "watch_condition"],
    }
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {"summary": {"type": "string"}, "factors": {"type": "array", "items": item, "minItems": 1, "maxItems": 3}},
        "required": ["summary", "factors"],
    }


def _generate_ticker_interpretation(db: Session, *, symbol: str, profile: dict[str, Any], factors: list[dict[str, Any]]) -> dict[str, Any] | None:
    from app.services.ai_marketing import OPENAI_API_KEY, _record_openai_usage_cost, resolved_setting_value

    api_key = resolved_setting_value(db, OPENAI_API_KEY)
    if not api_key:
        return None
    model = os.getenv(MACRO_TICKER_INTERPRETATION_MODEL, "").strip() or DEFAULT_MACRO_TICKER_INTERPRETATION_MODEL
    factual_factors = [{key: factor.get(key) for key in ("factor", "name", "category", "bias", "regime_label", "impact_score", "relevance")} for factor in factors]
    prompt = "\n".join(
        [
            "Produce concise ticker-specific macro interpretation from the supplied facts only.",
            "Do not invent catalysts or claim causation from a price move. Where no cause is supplied, describe the observed regime only.",
            "Do not alter or opine on impact_score or relevance; they are deterministic Walnut inputs.",
            "Return a 1-2 sentence summary plus one concise explanation, read-through and watch condition per supplied factor.",
            "Company context:", json.dumps({key: profile.get(key) for key in ("company_name", "sector", "industry", "country", "price_to_sales")}, default=str),
            "Factor facts:", json.dumps(factual_factors, default=str),
        ]
    )
    request_payload = {
        "model": model,
        "input": prompt,
        "store": False,
        "reasoning": {"effort": "none"},
        "text": {"format": {"type": "json_schema", "name": "ticker_macro_interpretation", "schema": _ticker_interpretation_schema(), "strict": True}},
        "max_output_tokens": 700,
    }
    try:
        response = audited_openai_request(
            feature="ticker_macro_positioning", operation="ticker_macro_interpretation", method="POST", endpoint=OPENAI_RESPONSES_ENDPOINT, payload=request_payload, model=model,
            send=lambda: requests.post(OPENAI_RESPONSES_ENDPOINT, headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}, json=request_payload, timeout=25),
        )
        if response.status_code >= 400:
            raise RuntimeError(f"OpenAI ticker macro request failed with status {response.status_code}.")
        data = response.json()
        _record_openai_usage_cost(db, model=model, data=data, feature="ticker_macro_positioning", commit=False)
        return validate_ticker_macro_interpretation(json.loads(_extract_openai_response_text(data)))
    except Exception:
        logger.exception("ticker_macro_interpretation_failed symbol=%s", symbol)
        db.rollback()
        return None


def validate_ticker_macro_interpretation(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != {"summary", "factors"} or not isinstance(value.get("summary"), str) or not isinstance(value.get("factors"), list):
        raise ValueError("Invalid ticker macro structured output.")
    summary = _clean_macro_ai_summary(value["summary"])
    if not summary:
        raise ValueError("Ticker macro summary was empty.")
    seen: set[str] = set()
    normalized: list[dict[str, str]] = []
    allowed = {spec["factor"] for spec in _TICKER_FACTOR_SPECS}
    for item in value["factors"]:
        if not isinstance(item, dict) or set(item) != {"factor", "confidence", "why_macro", "ticker_readthrough", "watch_condition"} or item.get("factor") not in allowed or item["factor"] in seen:
            raise ValueError("Ticker macro factor output was invalid.")
        fields = {key: " ".join(str(item.get(key) or "").split()) for key in ("why_macro", "ticker_readthrough", "watch_condition")}
        if not all(fields.values()) or any(len(text) > 520 for text in fields.values()):
            raise ValueError("Ticker macro explanatory output was invalid.")
        confidence = str(item.get("confidence") or "").upper()
        if confidence not in {"LOW", "MEDIUM", "HIGH"}:
            raise ValueError("Ticker macro confidence output was invalid.")
        seen.add(item["factor"])
        normalized.append({"factor": item["factor"], "confidence": confidence, **fields})
    if not normalized:
        raise ValueError("Ticker macro output had no factors.")
    return {"summary": summary, "factors": normalized}


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
