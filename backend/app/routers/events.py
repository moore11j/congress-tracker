from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import Float, Integer, String, and_, bindparam, case, func, or_, select, text
from sqlalchemy.orm import Session

from app.db import get_db
from app.models import Event, Security, WatchlistItem
from app.services.ticker_meta import get_cik_meta, get_ticker_meta, normalize_cik
from app.schemas import EventOut, EventsDebug, EventsPage, EventsPageDebug
from app.services.price_lookup import get_eod_close
from app.services.quote_lookup import get_current_prices_meta_db
from app.services.signal_score import calculate_smart_score
from app.utils.symbols import normalize_symbol

router = APIRouter(tags=["events"])
logger = logging.getLogger(__name__)

DEFAULT_LIMIT = 50
MAX_LIMIT = 200
MAX_SUGGEST_LIMIT = 50
VISIBLE_INSIDER_TRADE_TYPES = {"purchase", "sale"}
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


def _insider_visibility_clause():
    normalized_trade_type = func.lower(func.trim(func.coalesce(Event.trade_type, "")))
    return or_(
        Event.event_type != "insider_trade",
        normalized_trade_type.in_(VISIBLE_INSIDER_TRADE_TYPES),
    )


def _trade_direction(value: str | None) -> str | None:
    if not value:
        return None

    normalized = value.strip().lower()
    if not normalized:
        return None

    if normalized in {"s", "s-sale"}:
        return "sell"
    if normalized in {"p", "p-purchase"}:
        return "buy"

    sell_tokens = ("sale", "sell", "disposition", "dispose")
    if any(token in normalized for token in sell_tokens):
        return "sell"

    buy_tokens = ("buy", "purchase", "acquire", "acquisition")
    if any(token in normalized for token in buy_tokens):
        return "buy"

    return None


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
        payload.get("insider_name"),
        nested_insider.get("name"),
        raw.get("reportingName"),
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
        payload.get("role"),
        raw.get("typeOfOwner"),
    )


def _insider_company_name(event: Event, payload: dict) -> str | None:
    raw = payload.get("raw") if isinstance(payload.get("raw"), dict) else {}
    symbol = _event_symbol(event, payload)
    return _first_non_empty_text(
        payload.get("company_name"),
        payload.get("companyName"),
        raw.get("companyName"),
        payload.get("security_name"),
        raw.get("issuerName"),
        None if not symbol else None,
    )


def _insider_trade_row(event: Event, payload: dict) -> dict:
    raw = payload.get("raw") if isinstance(payload.get("raw"), dict) else {}
    return {
        "event_id": event.id,
        "symbol": _event_symbol(event, payload),
        "company_name": _insider_company_name(event, payload),
        "transaction_date": payload.get("transaction_date") or raw.get("transactionDate") or payload.get("trade_date"),
        "filing_date": payload.get("filing_date") or raw.get("filingDate") or event.ts.isoformat(),
        "trade_type": event.trade_type,
        "amount_min": event.amount_min,
        "amount_max": event.amount_max,
        "shares": _parse_numeric(payload.get("shares") or raw.get("securitiesTransacted") or raw.get("transactionShares")),
        "price": _parse_numeric(payload.get("price") or raw.get("price")),
        "insider_name": _insider_display_name(event, payload),
        "reporting_cik": _event_reporting_cik(payload),
        "role": _insider_role(payload),
        "external_id": _first_non_empty_text(payload.get("external_id"), raw.get("id"), raw.get("transactionId")),
        "url": _first_non_empty_text(payload.get("url"), payload.get("document_url"), raw.get("url"), raw.get("filingUrl")),
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


def _load_insider_events_for_cik(db: Session, reporting_cik: str, lookback_days: int) -> list[tuple[Event, dict]]:
    lookback = _validated_lookback_days(lookback_days)
    normalized_cik = normalize_cik(reporting_cik)
    if not normalized_cik:
        raise HTTPException(status_code=400, detail="Invalid reporting_cik.")

    since = datetime.now(timezone.utc) - timedelta(days=lookback)
    rows = db.execute(
        select(Event)
        .where(Event.event_type == "insider_trade")
        .where(Event.ts >= since)
        .where(_insider_visibility_clause())
        .order_by(func.coalesce(Event.event_date, Event.ts).desc(), Event.id.desc())
    ).scalars().all()

    matched: list[tuple[Event, dict]] = []
    for event in rows:
        payload = _parse_event_payload(event)
        if _event_reporting_cik(payload) != normalized_cik:
            continue
        trade_type = (event.trade_type or "").strip().lower()
        if trade_type not in VISIBLE_INSIDER_TRADE_TYPES:
            continue
        matched.append((event, payload))

    return matched




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


def _insider_entry_price(
    event: Event,
    payload: dict,
    db: Session,
    price_memo: dict[tuple[str, str], float | None],
) -> tuple[float | None, str]:
    filing_price = _parse_numeric(payload.get("price"))
    if filing_price is not None and filing_price > 0:
        return filing_price, "filing"

    sym, trade_date = _insider_symbol_and_trade_date(event, payload)
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
    ticker_meta: dict[str, dict[str, str | None]],
    cik_names: dict[str, str | None],
    baseline_map: dict[str, tuple[float, int]],
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

    smart_score, smart_band = calculate_smart_score(
        unusual_multiple=unusual_multiple or 1.0,
        amount_max=event.amount_max,
        ts=event.ts,
    )

    estimated_price = None
    current_price = None
    pnl_pct = None
    pnl_source = "none"
    quote_asof_ts = None
    quote_is_stale = None
    if event.event_type == "congress_trade":
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
            trade_direction = _trade_direction(
                event.trade_type
                or event.transaction_type
                or payload.get("transaction_type")
                or payload.get("trade_type")
            )
            direction_mult = -1.0 if trade_direction == "sell" else 1.0
            pnl_pct = (((current_price - estimated_price) / estimated_price) * 100) * direction_mult
    elif event.event_type == "insider_trade":
        sym, _ = _insider_symbol_and_trade_date(event, payload)
        entry_price, entry_source = _insider_entry_price(event, payload, db, price_memo)
        pnl_source = entry_source
        q = current_quote_meta.get(sym)
        if q:
            quote_asof_ts = q.get("asof_ts")
            quote_is_stale = q.get("is_stale")
        current_price = current_price_memo.get(sym)
        if current_price is not None and entry_price is not None and entry_price > 0:
            pnl_pct = ((current_price - entry_price) / entry_price) * 100

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
        amount_min=event.amount_min,
        amount_max=event.amount_max,
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


def _fetch_events_page(db: Session, q, limit: int) -> EventsPage:
    rows = db.execute(q).scalars().all()
    paged_rows = rows[:limit]

    price_memo: dict[tuple[str, str], float | None] = {}
    quote_symbols: set[str] = set()
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

    current_quote_meta = get_current_prices_meta_db(db, sorted(quote_symbols)) if quote_symbols else {}
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
            ticker_meta,
            cik_names,
            baseline_map,
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

    q = q.where(_insider_visibility_clause())
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
        page = _fetch_events_page(db, filtered_query.limit(limit + 1), limit)
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

    current_quote_meta = get_current_prices_meta_db(db, sorted(quote_symbols)) if quote_symbols else {}
    current_price_memo = {
        sym: meta["price"]
        for sym, meta in current_quote_meta.items()
        if isinstance(meta, dict) and "price" in meta
    }

    ticker_symbols = [_event_symbol(event, _parse_event_payload(event)) for event in rows]
    try:
        ticker_meta = get_ticker_meta(db, [symbol for symbol in ticker_symbols if symbol])
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
        cik_names = get_cik_meta(db, sorted(insider_ciks))
    except Exception:
        logger.exception("cik_meta resolver failed in /api/events")
        cik_names = {}

    member_net_30d_map = _member_net_30d_map(db, rows)
    symbol_net_30d_map = _symbol_net_30d_map(db, rows)
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
            ticker_meta,
            cik_names,
            baseline_map,
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


@router.get("/insiders/{reporting_cik}/summary")
def insider_summary(
    reporting_cik: str,
    db: Session = Depends(get_db),
    lookback_days: int = Query(90),
):
    matched = _load_insider_events_for_cik(db, reporting_cik, lookback_days)
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
    company_counts: dict[str, int] = {}
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

        company_name = _insider_company_name(event, payload)
        if company_name:
            company_counts[company_name] = company_counts.get(company_name, 0) + 1

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
    return {
        "reporting_cik": normalized_cik,
        "insider_name": max(name_counts.items(), key=lambda item: item[1])[0] if name_counts else None,
        "primary_company_name": max(company_counts.items(), key=lambda item: item[1])[0] if company_counts else None,
        "primary_role": max(role_counts.items(), key=lambda item: item[1])[0] if role_counts else None,
        "primary_symbol": max(symbol_counts.items(), key=lambda item: item[1])[0] if symbol_counts else None,
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
    matched = _load_insider_events_for_cik(db, reporting_cik, lookback_days)
    items = [_insider_trade_row(event, payload) for event, payload in matched[:limit]]
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
    matched = _load_insider_events_for_cik(db, reporting_cik, lookback_days)
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
