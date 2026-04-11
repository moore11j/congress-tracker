from __future__ import annotations

import json
import logging
import re
from datetime import date, datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import Float, Integer, String, and_, bindparam, case, func, or_, select, text
from sqlalchemy.orm import Session

from app.db import get_db
from app.models import Event, Security, TradeOutcome, WatchlistItem
from app.services.ticker_meta import get_cik_meta, get_ticker_meta, normalize_cik
from app.schemas import EventOut, EventsDebug, EventsPage, EventsPageDebug
from app.services.price_lookup import get_close_for_date_or_prior, get_eod_close, get_eod_close_series
from app.services.quote_lookup import get_current_prices_meta_db, quote_cache_get_many
from app.services.returns import signed_return_pct
from app.services.member_performance import INSIDER_METHODOLOGY_VERSION
from app.services.profile_performance_curve import build_normalized_profile_curve, build_timeline_dates
from app.services.signal_score import calculate_smart_score
from app.services.confirmation_metrics import ConfirmationMetrics, get_confirmation_metrics_for_symbols
from app.services.event_activity_filters import VISIBLE_INSIDER_TRADE_TYPES, insider_visibility_clause
from app.services.trade_outcome_display import (
    trade_outcome_display_metrics,
    trade_outcome_logical_key,
)
from app.services.foreign_trade_normalization import normalize_insider_price, normalization_payload
from app.utils.symbols import normalize_symbol

router = APIRouter(tags=["events"])
logger = logging.getLogger(__name__)

DEFAULT_LIMIT = 50
MAX_LIMIT = 200
MAX_SUGGEST_LIMIT = 50
DEFAULT_BASELINE_DAYS = 365
DEFAULT_MIN_BASELINE_COUNT = 3
ALLOWED_LOOKBACK_DAYS = {30, 90, 365}


def _normalize_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _parse_iso_datetime(value: str) -> datetime:
    cleaned = value.strip()
    if cleaned.endswith("Z"):
        cleaned = cleaned[:-1] + "+00:00"
    parsed = datetime.fromisoformat(cleaned)
    return _normalize_datetime(parsed)


def _parse_since(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return _parse_iso_datetime(value)
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid since datetime.") from exc


def _parse_cursor(cursor: str) -> tuple[datetime, int]:
    try:
        ts_str, id_str = cursor.split("|", 1)
        cursor_id = int(id_str)
        cursor_ts = _parse_iso_datetime(ts_str)
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid cursor format. Expected ts|id") from exc
    return cursor_ts, cursor_id


def _parse_csv(value: str | None) -> list[str]:
    if not value:
        return []
    return [part.strip() for part in value.split(",") if part.strip()]


def _validate_enum(value: str | None, allowed: set[str], label: str) -> str | None:
    if value is None:
        return None
    normalized = value.strip().lower()
    if normalized not in allowed:
        allowed_list = ", ".join(sorted(allowed))
        raise HTTPException(
            status_code=400,
            detail=f"Invalid {label}. Allowed values: {allowed_list}.",
        )
    return normalized


def _normalize_trade_type(trade_type: str | None) -> str | None:
    if trade_type is None:
        return None
    normalized = trade_type.strip().lower()
    if not normalized:
        return None
    alias_map = {
        "p-purchase": "purchase",
        "s-sale": "sale",
    }
    normalized = alias_map.get(normalized, normalized)

    allowed = {"purchase", "sale", "exchange", "received"}
    if normalized not in allowed:
        raise HTTPException(
            status_code=400,
            detail=(
                "Invalid trade_type. Allowed values: purchase, sale, exchange, received, p-purchase, s-sale."
            ),
        )
    return normalized


def _trade_type_values(trade_type: str) -> list[str]:
    if trade_type == "sale":
        return ["sale", "s-sale"]
    if trade_type == "purchase":
        return ["purchase", "p-purchase"]
    return [trade_type]



def _baseline_avg_subquery(baseline_since: datetime):
    return text(
        """
        SELECT symbol,
               AVG(amount_max) AS median_amount_max,
               COUNT(*) AS baseline_count
        FROM events
        WHERE event_type='congress_trade'
          AND amount_max IS NOT NULL
          AND symbol IS NOT NULL
          AND ts >= :baseline_since
        GROUP BY symbol
        """
    ).bindparams(bindparam("baseline_since", baseline_since)).columns(
        symbol=String,
        median_amount_max=Float,
        baseline_count=Integer,
    ).subquery()


def _congress_baseline_map(
    db: Session,
    events: list[Event],
    *,
    baseline_days: int = DEFAULT_BASELINE_DAYS,
    min_baseline_count: int = DEFAULT_MIN_BASELINE_COUNT,
) -> dict[str, tuple[float, int]]:
    symbols = sorted(
        {
            symbol
            for event in events
            for symbol in [_event_symbol(event, _parse_event_payload(event))]
            if event.event_type == "congress_trade" and event.amount_max is not None and symbol
        }
    )
    if not symbols:
        return {}

    baseline_since = datetime.now(timezone.utc) - timedelta(days=baseline_days)
    baseline_sq = _baseline_avg_subquery(baseline_since)
    baseline_rows = db.execute(
        select(
            baseline_sq.c.symbol,
            baseline_sq.c.median_amount_max,
            baseline_sq.c.baseline_count,
        ).where(baseline_sq.c.symbol.in_(symbols))
    ).all()

    return {
        row.symbol: (float(row.median_amount_max), int(row.baseline_count))
        for row in baseline_rows
        if row.symbol and row.median_amount_max and row.baseline_count >= min_baseline_count
    }



def _member_net_30d_map(db: Session, events: list[Event]) -> dict[str, float]:
    member_ids = sorted(
        {event.member_bioguide_id.strip() for event in events if event.member_bioguide_id and event.member_bioguide_id.strip()}
    )
    if not member_ids:
        return {}

    cutoff = datetime.now(timezone.utc) - timedelta(days=30)

    net_30d = (
        func.sum(
            case(
                (func.lower(func.trim(func.coalesce(Event.trade_type, ""))).in_(["purchase", "buy"]), Event.amount_max),
                else_=0,
            )
        )
        - func.sum(
            case(
                (func.lower(func.trim(func.coalesce(Event.trade_type, ""))).in_(["sale", "sell"]), Event.amount_max),
                else_=0,
            )
        )
    ).label("net_30d")

    rows = db.execute(
        select(Event.member_bioguide_id, net_30d)
        .where(Event.ts >= cutoff)
        .where(Event.member_bioguide_id.in_(member_ids))
        .group_by(Event.member_bioguide_id)
    ).all()

    return {member_id: float(value or 0) for member_id, value in rows if member_id}


def _symbol_net_30d_map(db: Session, events: list[Event]) -> dict[str, float]:
    symbols = sorted({event.symbol for event in events if event.event_type == "insider_trade" and event.symbol})
    if not symbols:
        return {}

    cutoff = datetime.now(timezone.utc) - timedelta(days=30)

    buy_amt = func.sum(case((Event.trade_type == "purchase", Event.amount_max), else_=0))
    sell_amt = func.sum(case((Event.trade_type == "sale", Event.amount_max), else_=0))
    net_30d = (func.coalesce(buy_amt, 0) - func.coalesce(sell_amt, 0)).label("net_30d")

    rows = db.execute(
        select(Event.symbol, net_30d)
        .where(Event.event_type == "insider_trade")
        .where(Event.ts >= cutoff)
        .where(Event.symbol.in_(symbols))
        .where(Event.trade_type.in_(["purchase", "sale"]))
        .group_by(Event.symbol)
    ).all()

    return {symbol: float(net or 0) for symbol, net in rows if symbol}



def _parse_event_payload(event: Event) -> dict:
    try:
        payload = json.loads(event.payload_json)
        if not isinstance(payload, dict):
            return {}
        return payload
    except Exception:
        return {}


def _congress_symbol_and_trade_date(event: Event, payload: dict) -> tuple[str, str | None]:
    sym = normalize_symbol(event.symbol or payload.get("symbol")) or ""
    trade_date = payload.get("trade_date") or payload.get("transaction_date")
    return sym, trade_date


def _parse_numeric(value) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        parsed = float(value)
        return parsed if parsed == parsed else None
    if isinstance(value, str):
        cleaned = value.replace("$", "").replace(",", "").strip()
        if not cleaned:
            return None
        try:
            return float(cleaned)
        except Exception:
            return None
    return None


def _first_non_empty_text(*values) -> str | None:
    for value in values:
        if isinstance(value, str):
            cleaned = value.strip()
            if cleaned:
                return cleaned
    return None


def _insider_display_name(event: Event, payload: dict) -> str | None:
    raw = payload.get("raw") if isinstance(payload.get("raw"), dict) else {}
    nested_insider = payload.get("insider") if isinstance(payload.get("insider"), dict) else {}

    return _first_non_empty_text(
        _first_text_field(
            payload,
            "insider_name",
            "insiderName",
            "reporting_name",
            "reportingName",
            "reporting_owner_name",
            "reportingOwnerName",
            "owner_name",
            "ownerName",
        ),
        nested_insider.get("name"),
        raw.get("reportingName"),
        raw.get("reporting_name"),
        raw.get("reportingOwnerName"),
        raw.get("ownerName"),
        raw.get("insiderName"),
        event.member_name,
    )


def _insider_symbol_and_trade_date(event: Event, payload: dict) -> tuple[str, str | None]:
    sym = _event_symbol(event, payload) or ""
    trade_date = payload.get("transaction_date") or payload.get("trade_date")
    return sym, trade_date


def _event_reporting_cik(payload: dict) -> str | None:
    if not isinstance(payload, dict):
        return None
    raw = payload.get("raw") if isinstance(payload.get("raw"), dict) else {}
    return normalize_cik(
        payload.get("reporting_cik")
        or payload.get("reportingCik")
        or raw.get("reportingCik")
        or raw.get("reportingCIK")
        or raw.get("rptOwnerCik")
    )


def _insider_role(payload: dict) -> str | None:
    if not isinstance(payload, dict):
        return None
    raw = payload.get("raw") if isinstance(payload.get("raw"), dict) else {}
    return _first_non_empty_text(
        _first_text_field(payload, "role", "typeOfOwner", "officerTitle", "insiderRole", "position"),
        raw.get("typeOfOwner"),
        raw.get("officerTitle"),
        raw.get("insiderRole"),
        raw.get("position"),
    )


def _insider_company_name(event: Event, payload: dict) -> str | None:
    raw = payload.get("raw") if isinstance(payload.get("raw"), dict) else {}
    nested_payload = payload.get("payload") if isinstance(payload.get("payload"), dict) else {}
    symbol = _event_symbol(event, payload)

    def _is_security_title(value: str | None) -> bool:
        if not value:
            return False
        cleaned = re.sub(r"\s+", " ", str(value)).strip().lower()
        if not cleaned:
            return False
        if "common stock" in cleaned:
            return True
        generic_titles = {
            "class a",
            "class b",
            "ordinary shares",
            "ordinary share",
            "common shares",
            "preferred stock",
            "restricted stock",
            "restricted stock units",
            "stock option",
            "stock options",
        }
        return cleaned in generic_titles

    def _valid_company_name(*values: object) -> str | None:
        for value in values:
            candidate = _first_non_empty_text(value)
            if not candidate:
                continue
            cleaned = candidate.strip()
            if not cleaned:
                continue
            lowered = cleaned.lower()
            if symbol and cleaned.upper() == symbol.upper():
                continue
            if lowered in {"unknown", "unknown company", "n/a", "na", "none"}:
                continue
            if _is_security_title(cleaned):
                continue
            return cleaned
        return None

    # Order: enriched payload company fields -> raw issuer/company fields.
    return _valid_company_name(
        payload.get("company_name"),
        payload.get("companyName"),
        payload.get("security_name"),
        payload.get("securityName"),
        nested_payload.get("company_name"),
        nested_payload.get("companyName"),
        nested_payload.get("security_name"),
        nested_payload.get("securityName"),
        payload.get("issuer_name"),
        payload.get("issuerName"),
        nested_payload.get("issuer_name"),
        nested_payload.get("issuerName"),
        raw.get("company_name"),
        raw.get("companyName"),
        raw.get("security_name"),
        raw.get("securityName"),
        raw.get("issuer_name"),
        raw.get("issuerName"),
        raw.get("issuer"),
    )


def _insider_security_name(payload: dict) -> str | None:
    return _first_text_field(payload, "security_name", "securityName")


def _insider_event_value_dicts(payload: dict) -> list[dict]:
    if not isinstance(payload, dict):
        return []
    value_dicts: list[dict] = [payload]
    nested_payload = payload.get("payload")
    if isinstance(nested_payload, dict):
        value_dicts.append(nested_payload)
    raw = payload.get("raw")
    if isinstance(raw, dict):
        value_dicts.append(raw)
    return value_dicts


def _first_numeric_field(payload: dict, *keys: str) -> float | None:
    for value_dict in _insider_event_value_dicts(payload):
        for key in keys:
            value = _parse_numeric(value_dict.get(key))
            if value is not None:
                return value
    return None


def _first_text_field(payload: dict, *keys: str) -> str | None:
    for value_dict in _insider_event_value_dicts(payload):
        value = _first_non_empty_text(*[value_dict.get(key) for key in keys])
        if value:
            return value
    return None


def _insider_trade_row(
    event: Event,
    payload: dict,
    outcome: TradeOutcome | None = None,
    fallback_pnl_pct: float | None = None,
) -> dict:
    raw = payload.get("raw") if isinstance(payload.get("raw"), dict) else {}
    symbol = _event_symbol(event, payload) or normalize_symbol(outcome.symbol if outcome else None)
    company_name = _insider_company_name(event, payload)
    security_name = _insider_security_name(payload)
    if not company_name and outcome is not None:
        company_name = _first_non_empty_text(outcome.symbol)
    transaction_date = _first_text_field(payload, "transaction_date", "transactionDate", "trade_date", "tradeDate")
    if not transaction_date:
        transaction_date = _insider_trade_date(event, payload)
    trade_type = _first_text_field(payload, "trade_type", "tradeType") or event.trade_type
    if not trade_type and outcome is not None:
        trade_type = outcome.trade_type
    normalized_price = normalize_insider_price(symbol=symbol, payload=payload, trade_date=transaction_date)
    price = normalized_price.display_price if normalized_price.is_comparable else None
    if price is None and outcome is not None and outcome.entry_price is not None:
        price = float(outcome.entry_price)
    reported_price = normalized_price.raw_price
    amount_min = _first_numeric_field(payload, "amount_min", "amountMin", "trade_value_min", "tradeValueMin")
    amount_max = _first_numeric_field(payload, "amount_max", "amountMax", "trade_value_max", "tradeValueMax")
    trade_value = _first_numeric_field(
        payload,
        "trade_value",
        "tradeValue",
        "actual_trade_value",
        "actualTradeValue",
        "transactionValue",
        "value",
    )

    if amount_min is None and event.amount_min is not None:
        amount_min = float(event.amount_min)
    if amount_max is None and event.amount_max is not None:
        amount_max = float(event.amount_max)
    if amount_min is None and outcome is not None and outcome.amount_min is not None:
        amount_min = float(outcome.amount_min)
    if amount_max is None and outcome is not None and outcome.amount_max is not None:
        amount_max = float(outcome.amount_max)
    shares = _first_numeric_field(payload, "shares", "transactionShares", "securitiesTransacted")
    if price is not None and shares is not None and shares > 0:
        trade_value = price * shares
    if trade_value is None:
        trade_value = amount_max if amount_max is not None else amount_min

    display_metrics = trade_outcome_display_metrics(outcome)
    payload_pnl_pct = _first_numeric_field(payload, "pnl_pct", "pnlPct", "pnl", "return_pct", "returnPct")
    pnl_pct = display_metrics.return_pct
    pnl_source = display_metrics.pnl_source
    if pnl_pct is None:
        pnl_pct = payload_pnl_pct if payload_pnl_pct is not None else fallback_pnl_pct
        if pnl_pct is not None:
            pnl_source = "persisted_payload"

    smart_score = _first_numeric_field(payload, "smart_score", "smartScore")
    smart_band = _first_text_field(payload, "smart_band", "smartBand")
    if smart_score is None or smart_band is None:
        try:
            unusual_multiple = _first_numeric_field(payload, "unusual_multiple", "unusualMultiple") or 1.0
        except Exception:
            unusual_multiple = 1.0
        calc_score, calc_band = calculate_smart_score(
            unusual_multiple=unusual_multiple,
            amount_max=event.amount_max,
            ts=event.ts,
        )
        smart_score = smart_score if smart_score is not None else calc_score
        smart_band = smart_band or calc_band

    return {
        "event_id": event.id,
        "symbol": symbol,
        "company_name": company_name,
        "companyName": company_name,
        "security_name": security_name,
        "securityName": security_name,
        "transaction_date": transaction_date,
        "trade_date": transaction_date,
        "filing_date": payload.get("filing_date") or raw.get("filingDate") or event.ts.isoformat(),
        "trade_type": trade_type,
        "tradeType": trade_type,
        "amount_min": amount_min,
        "amount_max": amount_max,
        "trade_value": trade_value,
        "tradeValue": trade_value,
        "shares": shares,
        "price": price,
        "display_price": price,
        "displayPrice": price,
        "display_price_currency": normalized_price.display_currency,
        "displayPriceCurrency": normalized_price.display_currency,
        "display_share_basis": normalized_price.display_share_basis,
        "displayShareBasis": normalized_price.display_share_basis,
        "reported_price": reported_price,
        "reportedPrice": reported_price,
        "reported_price_currency": normalized_price.raw_currency,
        "reportedPriceCurrency": normalized_price.raw_currency,
        "reported_share_basis": normalized_price.raw_share_basis,
        "reportedShareBasis": normalized_price.raw_share_basis,
        "price_normalization": normalization_payload(normalized_price),
        "priceNormalization": normalization_payload(normalized_price),
        "insider_name": _insider_display_name(event, payload),
        "reporting_cik": _event_reporting_cik(payload),
        "role": _insider_role(payload),
        "external_id": _first_non_empty_text(payload.get("external_id"), raw.get("id"), raw.get("transactionId")),
        "url": _first_non_empty_text(payload.get("url"), payload.get("document_url"), raw.get("url"), raw.get("filingUrl")),
        "pnl_pct": pnl_pct,
        "pnlPct": pnl_pct,
        "pnl": pnl_pct,
        "alpha_pct": display_metrics.alpha_pct,
        "alphaPct": display_metrics.alpha_pct,
        "pnl_source": pnl_source,
        "pnlSource": pnl_source,
        "smart_score": smart_score,
        "smartScore": smart_score,
        "smart_band": smart_band,
        "smartBand": smart_band,
    }




def _to_trade_outcome_member_series(
    row: TradeOutcome,
    cumulative_return: float,
    cumulative_alpha: float,
    running_benchmark_return_pct: float | None = None,
) -> dict:
    trade_date = row.trade_date.isoformat() if row.trade_date else None
    return {
        "event_id": row.event_id,
        "symbol": row.symbol,
        "trade_type": row.trade_type,
        "asof_date": trade_date,
        "return_pct": row.return_pct,
        "alpha_pct": row.alpha_pct,
        "benchmark_return_pct": row.benchmark_return_pct,
        "holding_days": row.holding_days,
        "cumulative_return_pct": cumulative_return,
        "running_benchmark_return_pct": running_benchmark_return_pct,
        "cumulative_alpha_pct": cumulative_alpha,
    }


def _to_trade_outcome_trade_view(row: TradeOutcome) -> dict:
    return {
        "event_id": row.event_id,
        "symbol": row.symbol or "—",
        "trade_type": row.trade_type,
        "asof_date": row.trade_date.isoformat() if row.trade_date else None,
        "return_pct": row.return_pct,
        "alpha_pct": row.alpha_pct,
        "holding_days": row.holding_days,
    }

def _insider_filing_date(event: Event, payload: dict) -> str:
    raw = payload.get("raw") if isinstance(payload.get("raw"), dict) else {}
    return (
        _first_non_empty_text(
            payload.get("filing_date"),
            payload.get("filingDate"),
            raw.get("filingDate"),
            raw.get("acceptedDate"),
        )
        or event.ts.isoformat()
    )


def _validated_lookback_days(lookback_days: int) -> int:
    if lookback_days not in ALLOWED_LOOKBACK_DAYS:
        raise HTTPException(status_code=400, detail="Invalid lookback_days. Allowed values: 30, 90, 365.")
    return lookback_days




def _insider_reporting_cik_prefilter_clause(normalized_cik: str):
    variants = {normalized_cik}
    stripped = normalized_cik.lstrip("0")
    if stripped:
        variants.add(stripped)

    patterns: list[str] = []
    for cik in variants:
        patterns.extend([
            f'"reporting_cik":"{cik}"',
            f'"reporting_cik": "{cik}"',
            f'"reportingCik":"{cik}"',
            f'"reportingCik": "{cik}"',
            f'"reportingCIK":"{cik}"',
            f'"reportingCIK": "{cik}"',
            f'"rptOwnerCik":"{cik}"',
            f'"rptOwnerCik": "{cik}"',
        ])

    return or_(*[Event.payload_json.contains(pattern) for pattern in patterns])


def _load_insider_events_for_cik(
    db: Session,
    reporting_cik: str,
    lookback_days: int,
    *,
    include_non_market_activity: bool = False,
) -> list[tuple[Event, dict]]:
    lookback = _validated_lookback_days(lookback_days)
    normalized_cik = normalize_cik(reporting_cik)
    if not normalized_cik:
        raise HTTPException(status_code=400, detail="Invalid reporting_cik.")

    since = datetime.now(timezone.utc) - timedelta(days=lookback)
    query = (
        select(Event)
        .where(Event.event_type == "insider_trade")
        .where(Event.ts >= since)
        .where(_insider_reporting_cik_prefilter_clause(normalized_cik))
        .order_by(func.coalesce(Event.event_date, Event.ts).desc(), Event.id.desc())
    )
    if not include_non_market_activity:
        query = query.where(insider_visibility_clause())

    rows = db.execute(query).scalars().all()

    matched: list[tuple[Event, dict]] = []
    for event in rows:
        payload = _parse_event_payload(event)
        if _event_reporting_cik(payload) != normalized_cik:
            continue
        trade_type = (event.trade_type or "").strip().lower()
        if not include_non_market_activity and trade_type not in VISIBLE_INSIDER_TRADE_TYPES:
            continue
        matched.append((event, payload))

    return matched


def _insider_trade_date(event: Event, payload: dict) -> str | None:
    value = _first_text_field(payload, "transaction_date", "transactionDate", "trade_date", "tradeDate")
    if not value:
        fallback_dt = event.event_date or event.ts
        if fallback_dt is not None:
            value = fallback_dt.date().isoformat()
    return value[:10] if value else None


def _load_insider_trade_outcomes(
    db: Session,
    matched: list[tuple[Event, dict]],
    normalized_cik: str,
    benchmark_symbol: str,
    lookback_days: int,
) -> tuple[dict[int, TradeOutcome], list[TradeOutcome]]:
    if not matched:
        return {}, []

    event_ids = [event.id for event, _ in matched]
    direct = db.execute(
        select(TradeOutcome)
        .where(TradeOutcome.event_id.in_(event_ids))
        .where(TradeOutcome.scoring_status == "ok")
        .where(TradeOutcome.benchmark_symbol == benchmark_symbol)
        .where(TradeOutcome.methodology_version == INSIDER_METHODOLOGY_VERSION)
        .where(TradeOutcome.trade_date.is_not(None))
    ).scalars().all()
    by_event_id: dict[int, TradeOutcome] = {row.event_id: row for row in direct}

    unmatched = [(event, payload) for event, payload in matched if event.id not in by_event_id]
    if not unmatched:
        ordered = sorted(by_event_id.values(), key=lambda row: (row.trade_date, row.event_id))
        return by_event_id, ordered

    cutoff = (datetime.now(timezone.utc) - timedelta(days=lookback_days)).date()
    fallback_query = (
        select(TradeOutcome)
        .where(TradeOutcome.scoring_status == "ok")
        .where(TradeOutcome.benchmark_symbol == benchmark_symbol)
        .where(TradeOutcome.methodology_version == INSIDER_METHODOLOGY_VERSION)
        .where(TradeOutcome.trade_date.is_not(None))
        .where(TradeOutcome.trade_date >= cutoff)
    )
    cik_variants: set[str] = {normalized_cik}
    stripped = normalized_cik.lstrip("0")
    if stripped:
        cik_variants.add(stripped)

    fallback = db.execute(
        fallback_query
        .where(TradeOutcome.member_id.in_(sorted(cik_variants)))
        .order_by(TradeOutcome.trade_date.asc(), TradeOutcome.event_id.asc())
    ).scalars().all()

    fallback_by_logical_key: dict[tuple[str | None, str | None, str | None, int | None, int | None], TradeOutcome] = {}
    for row in fallback:
        logical_key = trade_outcome_logical_key(
            symbol=row.symbol,
            trade_side=row.trade_type,
            trade_date=row.trade_date,
            amount_min=row.amount_min,
            amount_max=row.amount_max,
        )
        if logical_key[0] and logical_key[2]:
            fallback_by_logical_key.setdefault(logical_key, row)

    for event, payload in unmatched:
        sym = _event_symbol(event, payload)
        trade_date = _insider_trade_date(event, payload)
        side = event.trade_type or _first_text_field(payload, "trade_type", "tradeType")
        amount_min = _first_numeric_field(payload, "amount_min", "amountMin", "trade_value_min", "tradeValueMin")
        amount_max = _first_numeric_field(payload, "amount_max", "amountMax", "trade_value_max", "tradeValueMax")
        if amount_min is None and event.amount_min is not None:
            amount_min = float(event.amount_min)
        if amount_max is None and event.amount_max is not None:
            amount_max = float(event.amount_max)

        logical_key = trade_outcome_logical_key(
            symbol=sym,
            trade_side=side,
            trade_date=trade_date,
            amount_min=amount_min,
            amount_max=amount_max,
        )
        row = fallback_by_logical_key.get(logical_key)
        if row:
            by_event_id[event.id] = row

    ordered = sorted({row.id: row for row in by_event_id.values()}.values(), key=lambda row: (row.trade_date, row.event_id))
    return by_event_id, ordered




def _event_symbol(event: Event, payload: dict) -> str | None:
    raw_payload = payload.get("raw") if isinstance(payload, dict) else None
    payload_symbol = payload.get("symbol") if isinstance(payload, dict) else None
    raw_symbol = raw_payload.get("symbol") if isinstance(raw_payload, dict) else None
    return normalize_symbol(event.symbol or payload_symbol or raw_symbol)


def _event_cik(payload: dict) -> str | None:
    raw_payload = payload.get("raw") if isinstance(payload, dict) else None
    raw_cik = raw_payload.get("companyCik") if isinstance(raw_payload, dict) else None
    if not raw_cik and isinstance(raw_payload, dict):
        raw_cik = raw_payload.get("companyCIK")
    if not raw_cik and isinstance(payload, dict):
        raw_cik = payload.get("companyCik")
    return normalize_cik(raw_cik)


def _should_replace_company_name(existing: str | None, symbol: str | None) -> bool:
    if not existing:
        return True
    cleaned = existing.strip()
    if not cleaned:
        return True
    if symbol and cleaned.upper() == symbol.upper():
        return True
    return False


def _enrich_payload_company_name(
    event: Event,
    payload: dict,
    ticker_meta: dict[str, dict[str, str | None]],
    cik_names: dict[str, str | None],
) -> dict:
    symbol = _event_symbol(event, payload)
    company_name = None
    meta_name = None
    cik_name = None

    if symbol:
        meta = ticker_meta.get(symbol)
        meta_name = (meta or {}).get("company_name") if meta else None

    if event.event_type == "insider_trade":
        cik = _event_cik(payload)
        if cik:
            cik_name = cik_names.get(cik)

    if event.event_type == "insider_trade":
        company_name = meta_name or cik_name
    else:
        company_name = meta_name

    if not company_name:
        return payload

    if event.event_type != "insider_trade":
        if _should_replace_company_name(payload.get("security_name"), symbol):
            payload["security_name"] = company_name
        if _should_replace_company_name(payload.get("headline"), symbol):
            payload["headline"] = company_name
        return payload

    payload["company_name"] = company_name
    if symbol:
        payload["symbol"] = symbol
    raw = payload.get("raw")
    if not isinstance(raw, dict):
        raw = {}
        payload["raw"] = raw
    if symbol:
        raw["symbol"] = symbol
    raw["companyName"] = company_name

    return payload


def _ticker_meta_with_security_names(
    db: Session,
    symbols: list[str],
) -> dict[str, dict[str, str | None]]:
    normalized_symbols = sorted({symbol for raw in symbols for symbol in [normalize_symbol(raw)] if symbol})
    if not normalized_symbols:
        return {}

    ticker_meta = get_ticker_meta(db, normalized_symbols, allow_refresh=False)
    security_rows = db.execute(
        select(Security.symbol, Security.name)
        .where(Security.symbol.in_(normalized_symbols))
    ).all()

    for symbol, name in security_rows:
        normalized_symbol = normalize_symbol(symbol)
        company_name = _first_non_empty_text(name)
        if not normalized_symbol or not company_name or company_name.upper() == normalized_symbol.upper():
            continue
        row = ticker_meta.setdefault(normalized_symbol, {"company_name": None, "exchange": None})
        row["company_name"] = company_name

    return ticker_meta


def _insider_entry_price(
    event: Event,
    payload: dict,
    db: Session,
    price_memo: dict[tuple[str, str], float | None],
) -> tuple[float | None, str]:
    sym, trade_date = _insider_symbol_and_trade_date(event, payload)
    normalized = normalize_insider_price(symbol=sym, payload=payload, trade_date=trade_date)
    if normalized.is_comparable:
        return normalized.display_price, "normalized_filing" if normalized.status == "normalized" else "filing"
    if normalized.ordinary_shares_per_adr is not None:
        return None, "normalization_unavailable"

    if sym and trade_date:
        key = (sym, trade_date)
        if key not in price_memo:
            price_memo[key] = get_eod_close(db, sym, trade_date)
        fallback_price = price_memo[key]
        if fallback_price is not None and fallback_price > 0:
            return fallback_price, "eod"

    return None, "none"


def _event_payload(
    event: Event,
    db: Session,
    price_memo: dict[tuple[str, str], float | None],
    current_price_memo: dict[str, float],
    current_quote_meta: dict[str, dict],
    member_net_30d_map: dict[str, float],
    symbol_net_30d_map: dict[str, float],
    confirmation_metrics_map: dict[str, ConfirmationMetrics],
    ticker_meta: dict[str, dict[str, str | None]],
    cik_names: dict[str, str | None],
    baseline_map: dict[str, tuple[float, int]],
    enrich_prices: bool = True,
) -> EventOut:
    payload = _enrich_payload_company_name(event, _parse_event_payload(event), ticker_meta, cik_names)
    sym_norm = _event_symbol(event, payload)

    baseline_median_amount_max: float | None = None
    baseline_count: int | None = None
    unusual_multiple: float | None = None
    if event.event_type == "congress_trade":
        baseline_stats = baseline_map.get(sym_norm or "")
        if baseline_stats:
            baseline_median_amount_max, baseline_count = baseline_stats
            if event.amount_max is not None and baseline_median_amount_max > 0:
                unusual_multiple = float(event.amount_max) / baseline_median_amount_max
    else:
        try:
            unusual_multiple = float(payload.get("unusual_multiple") or 1.0)
        except Exception:
            unusual_multiple = 1.0

    confirmation_summary = (
        confirmation_metrics_map.get(sym_norm or "").as_dict()
        if sym_norm and sym_norm in confirmation_metrics_map
        else None
    )

    smart_score, smart_band = calculate_smart_score(
        unusual_multiple=unusual_multiple or 1.0,
        amount_max=event.amount_max,
        ts=event.ts,
        confirmation_30d=confirmation_summary,
    )

    estimated_price = None
    current_price = None
    display_amount_min = event.amount_min
    display_amount_max = event.amount_max
    pnl_pct = None
    pnl_source = "none"
    quote_asof_ts = None
    quote_is_stale = None
    if enrich_prices and event.event_type == "congress_trade":
        sym, trade_date = _congress_symbol_and_trade_date(event, payload)
        if sym and trade_date:
            key = (sym, trade_date)
            if key not in price_memo:
                price_memo[key] = get_eod_close(db, sym, trade_date)
            estimated_price = price_memo[key]
            if estimated_price is not None and estimated_price > 0:
                pnl_source = "eod"

        q = current_quote_meta.get(sym)
        if q:
            quote_asof_ts = q.get("asof_ts")
            quote_is_stale = q.get("is_stale")
        current_price = current_price_memo.get(sym)
        if current_price is not None and estimated_price is not None and estimated_price > 0:
            pnl_pct = signed_return_pct(
                current_price,
                estimated_price,
                event.trade_type
                or event.transaction_type
                or payload.get("transaction_type")
                or payload.get("trade_type"),
            )
    elif enrich_prices and event.event_type == "insider_trade":
        sym, trade_date = _insider_symbol_and_trade_date(event, payload)
        normalized = normalize_insider_price(symbol=sym, payload=payload, trade_date=trade_date)
        payload["reported_price"] = normalized.raw_price
        payload["reported_price_currency"] = normalized.raw_currency
        payload["reported_share_basis"] = normalized.raw_share_basis
        payload["display_price"] = normalized.display_price if normalized.is_comparable else None
        payload["display_price_currency"] = normalized.display_currency
        payload["display_share_basis"] = normalized.display_share_basis
        payload["price_normalization"] = normalization_payload(normalized)
        entry_price, entry_source = _insider_entry_price(event, payload, db, price_memo)
        estimated_price = entry_price
        shares = _first_numeric_field(payload, "shares", "transactionShares", "securitiesTransacted")
        if entry_price is not None and shares is not None and shares > 0:
            display_value = int(round(entry_price * shares))
            display_amount_min = display_value
            display_amount_max = display_value
            payload["display_trade_value"] = display_value
            payload["displayTradeValue"] = display_value
        pnl_source = entry_source
        q = current_quote_meta.get(sym)
        if q:
            quote_asof_ts = q.get("asof_ts")
            quote_is_stale = q.get("is_stale")
        current_price = current_price_memo.get(sym)
        if current_price is not None and entry_price is not None and entry_price > 0:
            pnl_pct = signed_return_pct(current_price, entry_price, event.trade_type or payload.get("trade_type"))

    resolved_member_name = event.member_name
    if event.event_type == "insider_trade":
        resolved_member_name = _insider_display_name(event, payload)
        if resolved_member_name and not _first_non_empty_text(payload.get("insider_name")):
            payload["insider_name"] = resolved_member_name

    return EventOut(
        id=event.id,
        event_type=event.event_type,
        ts=event.ts,
        symbol=sym_norm,
        source=event.source,
        member_name=resolved_member_name,
        member_bioguide_id=event.member_bioguide_id,
        party=event.party,
        chamber=event.chamber,
        trade_type=event.trade_type,
        amount_min=display_amount_min,
        amount_max=display_amount_max,
        impact_score=event.impact_score,
        payload=payload,
        estimated_price=estimated_price,
        current_price=current_price,
        pnl_pct=pnl_pct,
        pnl_source=pnl_source,
        quote_asof_ts=quote_asof_ts,
        quote_is_stale=quote_is_stale,
        smart_score=smart_score,
        smart_band=smart_band,
        baseline_median_amount_max=baseline_median_amount_max,
        baseline_count=baseline_count,
        unusual_multiple=unusual_multiple,
        member_net_30d=member_net_30d_map.get(event.member_bioguide_id or ""),
        symbol_net_30d=(symbol_net_30d_map.get(sym_norm or "", 0.0) if event.event_type == "insider_trade" else None),
        confirmation_30d=confirmation_summary,
    )


def _symbol_filter_clause(symbols: list[str]):
    return func.upper(Event.symbol).in_(symbols)


def _build_events_query(
    *,
    symbols: list[str],
    types: list[str],
    since: datetime | None,
    cursor: str | None,
    limit: int,
    extra_filters: list,
    congress_filters: list,
):
    q = select(Event)
    sort_ts = func.coalesce(Event.event_date, Event.ts)

    if symbols:
        q = q.where(_symbol_filter_clause(symbols))

    if types:
        q = q.where(Event.event_type.in_(types))

    if since is not None:
        q = q.where(sort_ts >= since)

    for clause in extra_filters:
        q = q.where(clause)

    for clause in congress_filters:
        q = q.where(clause)

    if cursor:
        cursor_ts, cursor_id = _parse_cursor(cursor)
        q = q.where(
            or_(
                sort_ts < cursor_ts,
                and_(sort_ts == cursor_ts, Event.id < cursor_id),
            )
        )

    q = q.order_by(sort_ts.desc(), Event.id.desc()).limit(limit + 1)
    return q


def _fetch_events_page(db: Session, q, limit: int, enrich_prices: bool = True) -> EventsPage:
    rows = db.execute(q).scalars().all()
    paged_rows = rows[:limit]

    price_memo: dict[tuple[str, str], float | None] = {}
    quote_symbols: set[str] = set()
    if enrich_prices:
        for event in paged_rows:
            payload = _parse_event_payload(event)
            if event.event_type == "congress_trade":
                sym, trade_date = _congress_symbol_and_trade_date(event, payload)
                if not sym or not trade_date:
                    continue
                key = (sym, trade_date)
                if key not in price_memo:
                    price_memo[key] = get_eod_close(db, sym, trade_date)
                if price_memo[key] is not None:
                    quote_symbols.add(sym)
            elif event.event_type == "insider_trade":
                sym, _ = _insider_symbol_and_trade_date(event, payload)
                if not sym:
                    continue
                entry_price, _ = _insider_entry_price(event, payload, db, price_memo)
                if entry_price is not None and entry_price > 0:
                    quote_symbols.add(sym)

    current_quote_meta = (
        get_current_prices_meta_db(db, sorted(quote_symbols), allow_cache_write=False)
        if enrich_prices and quote_symbols
        else {}
    )
    current_price_memo = {
        sym: meta["price"]
        for sym, meta in current_quote_meta.items()
        if isinstance(meta, dict) and "price" in meta
    }

    insider_symbols = {
        symbol
        for event in paged_rows
        for symbol in [_event_symbol(event, _parse_event_payload(event))]
        if event.event_type == "insider_trade" and symbol
    }
    try:
        ticker_meta = get_ticker_meta(db, sorted(insider_symbols))
    except Exception:
        logger.exception("ticker_meta resolver failed in /api/events")
        ticker_meta = {}

    insider_ciks = {
        cik
        for event in paged_rows
        for cik in [_event_cik(_parse_event_payload(event))]
        if event.event_type == "insider_trade" and cik
    }
    try:
        cik_names = get_cik_meta(db, sorted(insider_ciks))
    except Exception:
        logger.exception("cik_meta resolver failed in /api/events")
        cik_names = {}

    member_net_30d_map = _member_net_30d_map(db, paged_rows)
    symbol_net_30d_map = _symbol_net_30d_map(db, paged_rows)
    confirmation_metrics_map = get_confirmation_metrics_for_symbols(
        db,
        [symbol for event in paged_rows for symbol in [_event_symbol(event, _parse_event_payload(event))] if symbol],
    )
    baseline_map = _congress_baseline_map(db, paged_rows)
    items = [
        _event_payload(
            event,
            db,
            price_memo,
            current_price_memo,
            current_quote_meta,
            member_net_30d_map,
            symbol_net_30d_map,
            confirmation_metrics_map,
            ticker_meta,
            cik_names,
            baseline_map,
            enrich_prices=enrich_prices,
        )
        for event in paged_rows
    ]

    next_cursor = None
    if len(rows) > limit:
        last = rows[limit - 1]
        cursor_ts = last.event_date or last.ts
        next_cursor = f"{cursor_ts.isoformat()}|{last.id}"

    return EventsPage(items=items, next_cursor=next_cursor)


def _clean_suggestion(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = value.strip()
    return cleaned or None


@router.get("/suggest/symbol")
def suggest_symbol(
    db: Session = Depends(get_db),
    q: str = "",
    limit: int = Query(10, ge=1, le=MAX_SUGGEST_LIMIT),
    tape: str | None = None,
):
    prefix = q.strip()
    if not prefix:
        return {"items": []}

    query = (
        select(Event.symbol)
        .where(Event.symbol.is_not(None))
        .where(func.length(func.trim(Event.symbol)) > 0)
        .where(func.lower(Event.symbol).like(f"{prefix.lower()}%"))
    )

    tape_value = (tape or "").strip().lower()
    if tape_value == "congress":
        query = query.where(Event.event_type == "congress_trade")
    elif tape_value == "insider":
        query = query.where(Event.event_type == "insider_trade")

    rows = (
        db.execute(query.distinct().order_by(func.upper(Event.symbol)).limit(limit))
        .scalars()
        .all()
    )
    items = [symbol for symbol in (_clean_suggestion(row) for row in rows) if symbol is not None]
    return {"items": items}


@router.get("/suggest/member")
def suggest_member(
    db: Session = Depends(get_db),
    q: str = "",
    limit: int = Query(10, ge=1, le=MAX_SUGGEST_LIMIT),
):
    prefix = q.strip()
    if not prefix:
        return {"items": []}

    rows = (
        db.execute(
            select(Event.member_name)
            .where(Event.event_type == "congress_trade")
            .where(Event.member_name.is_not(None))
            .where(func.length(func.trim(Event.member_name)) > 0)
            .where(func.lower(Event.member_name).like(f"{prefix.lower()}%"))
            .distinct()
            .order_by(func.lower(Event.member_name))
            .limit(limit)
        )
        .scalars()
        .all()
    )
    items = [name for name in (_clean_suggestion(row) for row in rows) if name is not None]
    return {"items": items}


@router.get("/suggest/member-insider")
def suggest_member_insider(
    db: Session = Depends(get_db),
    q: str = "",
    limit: int = Query(10, ge=1, le=MAX_SUGGEST_LIMIT),
):
    prefix = q.strip()
    if not prefix:
        return {"items": []}

    rows = (
        db.execute(
            select(Event.member_name, Event.event_type)
            .where(Event.event_type.in_(["congress_trade", "insider_trade"]))
            .where(Event.member_name.is_not(None))
            .where(func.length(func.trim(Event.member_name)) > 0)
            .where(func.lower(Event.member_name).like(f"{prefix.lower()}%"))
            .distinct()
            .order_by(func.lower(Event.member_name), Event.event_type)
            .limit(limit * 3)
        )
        .all()
    )

    items: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for name, event_type in rows:
        cleaned_name = _clean_suggestion(name)
        if cleaned_name is None:
            continue

        category = "congress" if event_type == "congress_trade" else "insider"
        key = (cleaned_name.casefold(), category)
        if key in seen:
            continue
        seen.add(key)
        items.append(
            {
                "label": f"{cleaned_name} — {'Congress' if category == 'congress' else 'Insider'}",
                "value": cleaned_name,
                "category": category,
            }
        )
        if len(items) >= limit:
            break

    return {"items": items}


@router.get("/suggest/role")
def suggest_role(
    db: Session = Depends(get_db),
    q: str = "",
    limit: int = Query(10, ge=1, le=MAX_SUGGEST_LIMIT),
):
    prefix = q.strip().lower()
    if not prefix:
        return {"items": []}

    rows = (
        db.execute(
            select(Event.payload_json)
            .where(Event.event_type == "insider_trade")
            .where(Event.payload_json.is_not(None))
            .limit(1000)
        )
        .scalars()
        .all()
    )

    found: set[str] = set()
    for payload_json in rows:
        try:
            payload = json.loads(payload_json)
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue

        for key in ("role", "title"):
            raw_value = payload.get(key)
            if not isinstance(raw_value, str):
                continue
            value = raw_value.strip()
            if not value:
                continue
            if value.lower().startswith(prefix):
                found.add(value)

    items = sorted(found, key=lambda value: value.lower())[:limit]
    return {"items": items}


@router.get("/events", response_model=EventsPageDebug, response_model_exclude_none=True)
def list_events(
    db: Session = Depends(get_db),
    symbol: str | None = None,
    event_type: str | None = None,
    types: str | None = None,
    tape: str | None = None,
    since: str | None = None,
    member: str | None = None,
    member_id: str | None = None,
    chamber: str | None = None,
    party: str | None = None,
    trade_type: str | None = None,
    transaction_type: str | None = None,
    role: str | None = None,
    ownership: str | None = None,
    min_amount: float | None = Query(None, ge=0),
    max_amount: float | None = Query(None, ge=0),
    whale: bool | None = None,
    recent_days: int | None = Query(None, ge=1),
    cursor: str | None = None,
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=100),
    offset: int = Query(0, ge=0),
    include_total: bool = Query(False),
    enrich_prices: bool = Query(True),
    debug: bool | None = None,
):
    # Manual curl checks:
    # curl "http://localhost:8000/api/events?symbol=NVDA"
    # curl "http://localhost:8000/api/events?member=Pelosi"
    # curl "http://localhost:8000/api/events?chamber=house"
    # curl "http://localhost:8000/api/events?min_amount=250000"  # uses amount_max
    # curl "http://localhost:8000/api/events?trade_type=sale"
    # curl "http://localhost:8000/api/events?party=Democrat"
    # curl "http://localhost:8000/api/events?recent_days=30"
    # Smoke checks (after backfill):
    # curl "http://localhost:8000/api/events?limit=1"
    # curl "http://localhost:8000/api/events?event_type=congress_trade&limit=1"
    symbol_values = _parse_csv(symbol)
    combined_symbols = [value.upper() for value in symbol_values if value]
    raw_event_type = event_type if event_type is not None else types
    type_list = [item.strip().lower() for item in _parse_csv(raw_event_type)]
    tape_value = None
    if tape is not None:
        tape_value = tape.strip().lower()
        if tape_value not in {"congress", "insider", "all"}:
            raise HTTPException(status_code=400, detail="Invalid tape. Allowed values: congress, insider, all.")
    since_dt = _parse_since(since)
    recent_since = None
    if recent_days is not None:
        recent_since = datetime.now(timezone.utc) - timedelta(days=recent_days)

    chamber_value = _validate_enum(chamber, {"house", "senate"}, "chamber")
    party_value = _validate_enum(
        party, {"democrat", "republican", "independent", "other"}, "party"
    )
    trade_value = _normalize_trade_type(trade_type)

    if whale and (min_amount is None or min_amount < 250_000):
        min_amount = 250_000

    q = select(Event)
    sort_ts = func.coalesce(Event.event_date, Event.ts)
    applied_filters: list[str] = []

    q = q.where(insider_visibility_clause())
    applied_filters.append("insider_visibility")

    if combined_symbols:
        q = q.where(_symbol_filter_clause(combined_symbols))
        applied_filters.append("symbol")

    if type_list:
        q = q.where(Event.event_type.in_(type_list))
        applied_filters.append("types")
    elif tape_value == "congress":
        q = q.where(Event.event_type == "congress_trade")
        applied_filters.append("tape=congress")
    elif tape_value == "insider":
        q = q.where(Event.event_type == "insider_trade")
        applied_filters.append("tape=insider")

    if since_dt is not None:
        q = q.where(sort_ts >= since_dt)
        applied_filters.append("since")
    if recent_since is not None:
        q = q.where(sort_ts >= recent_since)
        applied_filters.append("recent_days")

    congress_filter_active = any(
        [
            member,
            member_id,
            chamber_value,
            party_value,
        ]
    )
    if congress_filter_active:
        q = q.where(Event.event_type == "congress_trade")
        applied_filters.append("event_type=congress_trade")

    insider_filter_active = any([transaction_type, role, ownership])
    if insider_filter_active:
        q = q.where(Event.event_type == "insider_trade")
        applied_filters.append("event_type=insider_trade")
    if member:
        member_like = f"%{member.strip()}%"
        q = q.where(Event.member_name.ilike(member_like))
        applied_filters.append("member")
    if member_id:
        q = q.where(func.lower(Event.member_bioguide_id) == member_id.strip().lower())
        applied_filters.append("member_id")
    if chamber_value:
        q = q.where(func.lower(Event.chamber) == chamber_value)
        applied_filters.append("chamber")
    if party_value:
        if party_value == "other":
            q = q.where(or_(Event.party.is_(None), func.lower(Event.party) == party_value))
        else:
            q = q.where(func.lower(Event.party) == party_value)
        applied_filters.append("party")

    if trade_value:
        trade_values = _trade_type_values(trade_value)
        effective_event_scope = "all"
        explicit_event_types = set(type_list)
        if explicit_event_types == {"congress_trade"} or tape_value == "congress" or (
            congress_filter_active and not insider_filter_active
        ):
            effective_event_scope = "congress_trade"
        elif explicit_event_types == {"insider_trade"} or tape_value == "insider" or (
            insider_filter_active and not congress_filter_active
        ):
            effective_event_scope = "insider_trade"

        if effective_event_scope == "congress_trade":
            q = q.where(func.lower(Event.trade_type).in_(trade_values))
        elif effective_event_scope == "insider_trade":
            q = q.where(func.lower(Event.trade_type).in_(trade_values))
        else:
            q = q.where(func.lower(Event.trade_type).in_(trade_values))
        applied_filters.append("trade_type")

    if transaction_type:
        q = q.where(func.lower(Event.transaction_type) == transaction_type.strip().lower())
        applied_filters.append("transaction_type")

    payload_lower = func.lower(Event.payload_json)
    if role:
        role_value = role.strip().lower()
        q = q.where(payload_lower.like(f'%"role"%{role_value}%'))
        applied_filters.append("role")
    if ownership:
        ownership_value = ownership.strip().lower()
        q = q.where(payload_lower.like(f'%"ownership"%{ownership_value}%'))
        applied_filters.append("ownership")
    if min_amount is not None:
        q = q.where(Event.amount_max >= min_amount)
        applied_filters.append("min_amount")
    if max_amount is not None:
        q = q.where(Event.amount_min <= max_amount)
        applied_filters.append("max_amount")

    if cursor:
        cursor_ts, cursor_id = _parse_cursor(cursor)
        q = q.where(
            or_(
                sort_ts < cursor_ts,
                and_(sort_ts == cursor_ts, Event.id < cursor_id),
            )
        )
        applied_filters.append("cursor")

    filtered_query = q.order_by(sort_ts.desc(), Event.id.desc())

    total = None
    if include_total and cursor is None:
        total = db.execute(select(func.count()).select_from(filtered_query.subquery())).scalar()

    if cursor:
        page = _fetch_events_page(db, filtered_query.limit(limit + 1), limit, enrich_prices=enrich_prices)
        if debug:
            count_query = select(func.count()).select_from(q.subquery())
            count_after_filters = db.execute(count_query).scalar_one()
            debug_payload = EventsDebug(
                received_params={
                    "symbol": symbol,
                    "event_type": event_type,
                    "types": types,
                    "tape": tape,
                    "member": member,
                    "chamber": chamber,
                    "party": party,
                    "trade_type": trade_type,
                    "transaction_type": transaction_type,
                    "role": role,
                    "ownership": ownership,
                    "min_amount": min_amount,
                    "max_amount": max_amount,
                    "recent_days": recent_days,
                    "cursor": cursor,
                    "offset": offset,
                    "include_total": include_total,
                    "enrich_prices": enrich_prices,
                },
                applied_filters=applied_filters,
                count_after_filters=count_after_filters,
                sql_hint=", ".join(applied_filters) if applied_filters else None,
            )
            return EventsPageDebug(items=page.items, next_cursor=page.next_cursor, debug=debug_payload)
        return page

    rows = db.execute(filtered_query.offset(offset).limit(limit)).scalars().all()
    price_memo: dict[tuple[str, str], float | None] = {}
    quote_symbols: set[str] = set()
    if enrich_prices:
        for event in rows:
            payload = _parse_event_payload(event)
            if event.event_type == "congress_trade":
                sym, trade_date = _congress_symbol_and_trade_date(event, payload)
                if not sym or not trade_date:
                    continue
                key = (sym, trade_date)
                if key not in price_memo:
                    price_memo[key] = get_eod_close(db, sym, trade_date)
                if price_memo[key] is not None:
                    quote_symbols.add(sym)
            elif event.event_type == "insider_trade":
                sym, _ = _insider_symbol_and_trade_date(event, payload)
                if not sym:
                    continue
                entry_price, _ = _insider_entry_price(event, payload, db, price_memo)
                if entry_price is not None and entry_price > 0:
                    quote_symbols.add(sym)

    current_quote_meta = (
        get_current_prices_meta_db(db, sorted(quote_symbols), allow_cache_write=False)
        if enrich_prices and quote_symbols
        else {}
    )
    current_price_memo = {
        sym: meta["price"]
        for sym, meta in current_quote_meta.items()
        if isinstance(meta, dict) and "price" in meta
    }

    ticker_symbols = [_event_symbol(event, _parse_event_payload(event)) for event in rows]
    try:
        ticker_meta = get_ticker_meta(db, [symbol for symbol in ticker_symbols if symbol], allow_refresh=False)
    except Exception:
        logger.exception("ticker_meta resolver failed in /api/events")
        ticker_meta = {}

    insider_ciks = {
        cik
        for event in rows
        for cik in [_event_cik(_parse_event_payload(event))]
        if event.event_type == "insider_trade" and cik
    }
    try:
        cik_names = get_cik_meta(db, sorted(insider_ciks), allow_refresh=False)
    except Exception:
        logger.exception("cik_meta resolver failed in /api/events")
        cik_names = {}

    member_net_30d_map = _member_net_30d_map(db, rows)
    symbol_net_30d_map = _symbol_net_30d_map(db, rows)
    confirmation_metrics_map = get_confirmation_metrics_for_symbols(
        db,
        [symbol for event in rows for symbol in [_event_symbol(event, _parse_event_payload(event))] if symbol],
    )
    baseline_map = _congress_baseline_map(db, rows)
    items = [
        _event_payload(
            event,
            db,
            price_memo,
            current_price_memo,
            current_quote_meta,
            member_net_30d_map,
            symbol_net_30d_map,
            confirmation_metrics_map,
            ticker_meta,
            cik_names,
            baseline_map,
            enrich_prices=enrich_prices,
        )
        for event in rows
    ]

    if debug:
        count_query = select(func.count()).select_from(q.subquery())
        count_after_filters = db.execute(count_query).scalar_one()
        debug_payload = EventsDebug(
            received_params={
                "symbol": symbol,
                "event_type": event_type,
                "types": types,
                "tape": tape,
                "member": member,
                "chamber": chamber,
                "party": party,
                "trade_type": trade_type,
                "transaction_type": transaction_type,
                "role": role,
                "ownership": ownership,
                "min_amount": min_amount,
                "max_amount": max_amount,
                "recent_days": recent_days,
                "cursor": cursor,
                "offset": offset,
                "include_total": include_total,
                "enrich_prices": enrich_prices,
            },
            applied_filters=applied_filters,
            count_after_filters=count_after_filters,
            sql_hint=", ".join(applied_filters) if applied_filters else None,
        )
        return EventsPageDebug(items=items, total=total, limit=limit, offset=offset, debug=debug_payload)

    return EventsPageDebug(items=items, total=total, limit=limit, offset=offset)


@router.get("/tickers/{symbol}/events", response_model=EventsPage)
def list_ticker_events(
    symbol: str,
    db: Session = Depends(get_db),
    types: str | None = None,
    since: str | None = None,
    cursor: str | None = None,
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
):
    symbol_list = [symbol.strip().upper()]
    type_list = [event_type.strip().lower() for event_type in _parse_csv(types)]
    since_dt = _parse_since(since)

    q = _build_events_query(
        symbols=symbol_list,
        types=type_list,
        since=since_dt,
        cursor=cursor,
        limit=limit,
        extra_filters=[],
        congress_filters=[],
    )
    return _fetch_events_page(db, q, limit)


@router.get("/watchlists/{id}/events", response_model=EventsPage)
def list_watchlist_events(
    id: int,
    db: Session = Depends(get_db),
    types: str | None = None,
    since: str | None = None,
    cursor: str | None = None,
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
):
    symbols = (
        db.execute(
            select(Security.symbol)
            .join(WatchlistItem, WatchlistItem.security_id == Security.id)
            .where(WatchlistItem.watchlist_id == id)
        )
        .scalars()
        .all()
    )

    if not symbols:
        return EventsPage(items=[], next_cursor=None)

    symbol_list = [symbol.upper() for symbol in symbols if symbol]
    type_list = [event_type.strip().lower() for event_type in _parse_csv(types)]
    since_dt = _parse_since(since)

    q = _build_events_query(
        symbols=symbol_list,
        types=type_list,
        since=since_dt,
        cursor=cursor,
        limit=limit,
        extra_filters=[],
        congress_filters=[],
    )
    return _fetch_events_page(db, q, limit)




@router.get("/insiders/{reporting_cik}/alpha-summary")
def insider_alpha_summary(
    reporting_cik: str,
    db: Session = Depends(get_db),
    lookback_days: int = Query(90),
    benchmark: str = "^GSPC",
):
    matched = _load_insider_events_for_cik(db, reporting_cik, lookback_days)
    normalized_cik = normalize_cik(reporting_cik)
    benchmark_symbol = (benchmark or "^GSPC").strip() or "^GSPC"

    if not matched:
        return {
            "reporting_cik": normalized_cik,
            "lookback_days": lookback_days,
            "benchmark_symbol": benchmark_symbol,
            "trades_analyzed": 0,
            "avg_return_pct": None,
            "avg_alpha_pct": None,
            "win_rate": None,
            "avg_holding_days": None,
            "best_trades": [],
            "worst_trades": [],
            "member_series": [],
            "benchmark_series": [],
            "performance_series": [],
        }

    outcome_by_event_id, outcomes = _load_insider_trade_outcomes(
        db,
        matched,
        normalized_cik,
        benchmark_symbol,
        lookback_days,
    )

    end_date = datetime.now(timezone.utc).date()
    start_date = end_date - timedelta(days=max(lookback_days, 1))
    benchmark_close_map = get_eod_close_series(
        db=db,
        symbol=benchmark_symbol,
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
    )
    benchmark_dates = sorted(benchmark_close_map.keys())
    timeline_dates = build_timeline_dates(start_date, end_date)

    scored = [row for row in outcomes if row.return_pct is not None]
    return_values = [row.return_pct for row in scored if row.return_pct is not None]
    alpha_values = [row.alpha_pct for row in scored if row.alpha_pct is not None]
    holding_day_values = [row.holding_days for row in scored if isinstance(row.holding_days, int)]

    best_trades = [_to_trade_outcome_trade_view(row) for row in sorted(scored, key=lambda item: item.return_pct, reverse=True)[:5]]
    worst_trades = [_to_trade_outcome_trade_view(row) for row in sorted(scored, key=lambda item: item.return_pct)[:5]]

    curve = build_normalized_profile_curve(
        outcomes=outcomes,
        timeline_dates=timeline_dates,
        benchmark_close_map=benchmark_close_map,
        benchmark_dates=benchmark_dates,
    )

    return {
        "reporting_cik": normalized_cik,
        "lookback_days": lookback_days,
        "benchmark_symbol": benchmark_symbol,
        "trades_analyzed": len(scored),
        "avg_return_pct": (sum(return_values) / len(return_values)) if return_values else None,
        "avg_alpha_pct": (sum(alpha_values) / len(alpha_values)) if alpha_values else None,
        "win_rate": (sum(1 for value in return_values if value > 0) / len(scored)) if scored else None,
        "avg_holding_days": (sum(holding_day_values) / len(holding_day_values)) if holding_day_values else None,
        "best_trades": best_trades,
        "worst_trades": worst_trades,
        "member_series": curve.member_series,
        "benchmark_series": curve.benchmark_series,
        "performance_series": curve.member_series,
    }

@router.get("/insiders/{reporting_cik}/summary")
def insider_summary(
    reporting_cik: str,
    db: Session = Depends(get_db),
    lookback_days: int = Query(90),
):
    matched = _load_insider_events_for_cik(db, reporting_cik, lookback_days, include_non_market_activity=True)
    normalized_cik = normalize_cik(reporting_cik)
    if not matched:
        return {
            "reporting_cik": normalized_cik,
            "insider_name": None,
            "primary_company_name": None,
            "primary_role": None,
            "primary_symbol": None,
            "lookback_days": lookback_days,
            "total_trades": 0,
            "buy_count": 0,
            "sell_count": 0,
            "unique_tickers": 0,
            "gross_buy_value": 0,
            "gross_sell_value": 0,
            "net_flow": 0,
            "latest_filing_date": None,
            "latest_transaction_date": None,
        }

    buy_count = 0
    sell_count = 0
    gross_buy_value = 0.0
    gross_sell_value = 0.0
    symbol_counts: dict[str, int] = {}
    name_counts: dict[str, int] = {}
    role_counts: dict[str, int] = {}
    latest_transaction_date: str | None = None

    for event, payload in matched:
        trade_type = (event.trade_type or "").strip().lower()
        amount = float(event.amount_max or event.amount_min or 0)
        if trade_type == "purchase":
            buy_count += 1
            gross_buy_value += amount
        elif trade_type == "sale":
            sell_count += 1
            gross_sell_value += amount

        symbol = _event_symbol(event, payload)
        if symbol:
            symbol_counts[symbol] = symbol_counts.get(symbol, 0) + 1

        insider_name = _insider_display_name(event, payload)
        if insider_name:
            name_counts[insider_name] = name_counts.get(insider_name, 0) + 1

        role = _insider_role(payload)
        if role:
            role_counts[role] = role_counts.get(role, 0) + 1

        tx_date = _first_non_empty_text(
            payload.get("transaction_date"),
            payload.get("trade_date"),
            (payload.get("raw") or {}).get("transactionDate") if isinstance(payload.get("raw"), dict) else None,
        )
        if tx_date and (latest_transaction_date is None or tx_date > latest_transaction_date):
            latest_transaction_date = tx_date

    latest_filing_date = _insider_filing_date(matched[0][0], matched[0][1])
    latest_company_name = None
    latest_trade_row_company_name = None
    metadata_company_name = None
    primary_symbol = max(symbol_counts.items(), key=lambda item: item[1])[0] if symbol_counts else None
    if matched:
        insider_symbols = sorted(
            {
                symbol
                for event, payload in matched
                for symbol in [_event_symbol(event, payload)]
                if symbol
            }
        )
        ticker_meta = _ticker_meta_with_security_names(db, insider_symbols) if insider_symbols else {}
        insider_ciks = sorted(
            {
                cik
                for _, payload in matched
                for cik in [_event_cik(payload)]
                if cik
            }
        )
        cik_names = get_cik_meta(db, insider_ciks, allow_refresh=False) if insider_ciks else {}
        if primary_symbol and primary_symbol in ticker_meta:
            metadata_company_name = _first_non_empty_text((ticker_meta.get(primary_symbol) or {}).get("company_name"))
        if not metadata_company_name:
            primary_company_cik = next((cik for cik in insider_ciks if cik), None)
            if primary_company_cik:
                metadata_company_name = _first_non_empty_text(cik_names.get(primary_company_cik))

        latest_event, latest_payload = matched[0]
        resolved = _enrich_payload_company_name(latest_event, latest_payload, ticker_meta, cik_names)
        latest_trade_row_company_name = _first_non_empty_text(_insider_trade_row(latest_event, resolved).get("company_name"))
        latest_company_name = _insider_company_name(latest_event, resolved)

    primary_company_name = latest_company_name or latest_trade_row_company_name or metadata_company_name

    fallback_name = None
    fallback_role = None
    if matched:
        latest_payload = matched[0][1]
        fallback_name = _insider_display_name(matched[0][0], latest_payload)
        fallback_role = _insider_role(latest_payload)

    return {
        "reporting_cik": normalized_cik,
        "insider_name": (max(name_counts.items(), key=lambda item: item[1])[0] if name_counts else fallback_name),
        "primary_company_name": primary_company_name,
        "primary_role": (max(role_counts.items(), key=lambda item: item[1])[0] if role_counts else fallback_role),
        "primary_symbol": primary_symbol,
        "lookback_days": lookback_days,
        "total_trades": len(matched),
        "buy_count": buy_count,
        "sell_count": sell_count,
        "unique_tickers": len(symbol_counts),
        "gross_buy_value": round(gross_buy_value, 2),
        "gross_sell_value": round(gross_sell_value, 2),
        "net_flow": round(gross_buy_value - gross_sell_value, 2),
        "latest_filing_date": latest_filing_date,
        "latest_transaction_date": latest_transaction_date,
    }


@router.get("/insiders/{reporting_cik}/trades")
def insider_trades(
    reporting_cik: str,
    db: Session = Depends(get_db),
    lookback_days: int = Query(90),
    limit: int = Query(50, ge=1, le=200),
):
    matched = _load_insider_events_for_cik(db, reporting_cik, lookback_days, include_non_market_activity=True)
    normalized_cik = normalize_cik(reporting_cik)
    visible = matched[:limit]

    insider_symbols = sorted(
        {
            symbol
            for event, payload in visible
            for symbol in [_event_symbol(event, payload)]
            if symbol
        }
    )
    ticker_meta = _ticker_meta_with_security_names(db, insider_symbols) if insider_symbols else {}
    cik_values = sorted(
        {
            cik
            for event, payload in visible
            for cik in [_event_cik(payload)]
            if cik
        }
    )
    cik_names = get_cik_meta(db, cik_values, allow_refresh=False) if cik_values else {}
    enriched = [
        (event, _enrich_payload_company_name(event, payload, ticker_meta, cik_names))
        for event, payload in visible
    ]

    outcome_by_event_id, _ = _load_insider_trade_outcomes(
        db,
        enriched,
        normalized_cik,
        "^GSPC",
        lookback_days,
    )
    quote_prices = quote_cache_get_many(db, insider_symbols) if insider_symbols else {}

    items = []
    for event, payload in enriched:
        fallback_pnl_pct = None
        symbol = _event_symbol(event, payload)
        current_price = quote_prices.get(symbol or "")
        filing_price = _first_numeric_field(payload, "price", "transactionPricePerShare", "transaction_price")
        if current_price is not None and filing_price is not None and filing_price > 0:
            fallback_pnl_pct = signed_return_pct(current_price, filing_price, event.trade_type or payload.get("trade_type"))
        items.append(_insider_trade_row(event, payload, outcome_by_event_id.get(event.id), fallback_pnl_pct))
    return {
        "reporting_cik": normalize_cik(reporting_cik),
        "lookback_days": lookback_days,
        "items": items,
    }


@router.get("/insiders/{reporting_cik}/top-tickers")
def insider_top_tickers(
    reporting_cik: str,
    db: Session = Depends(get_db),
    lookback_days: int = Query(90),
    limit: int = Query(10, ge=1, le=50),
):
    matched = _load_insider_events_for_cik(db, reporting_cik, lookback_days, include_non_market_activity=True)
    by_symbol: dict[str, dict] = {}
    for event, payload in matched:
        symbol = _event_symbol(event, payload)
        if not symbol:
            continue
        row = by_symbol.get(symbol)
        if row is None:
            row = {
                "symbol": symbol,
                "company_name": _insider_company_name(event, payload),
                "trades": 0,
                "buy_count": 0,
                "sell_count": 0,
                "net_flow": 0.0,
            }
            by_symbol[symbol] = row
        row["trades"] += 1
        side = (event.trade_type or "").strip().lower()
        amount = float(event.amount_max or event.amount_min or 0)
        if side == "purchase":
            row["buy_count"] += 1
            row["net_flow"] += amount
        elif side == "sale":
            row["sell_count"] += 1
            row["net_flow"] -= amount
        if not row.get("company_name"):
            row["company_name"] = _insider_company_name(event, payload)

    items = sorted(by_symbol.values(), key=lambda row: row["trades"], reverse=True)[:limit]
    return {
        "reporting_cik": normalize_cik(reporting_cik),
        "lookback_days": lookback_days,
        "items": items,
    }
