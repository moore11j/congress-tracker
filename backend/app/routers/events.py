from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import and_, func, or_, select
from sqlalchemy.orm import Session

from app.db import get_db
from app.models import Event, Security, WatchlistItem
from app.schemas import EventOut, EventsDebug, EventsPage, EventsPageDebug
from app.services.price_lookup import get_eod_close
from app.services.quote_lookup import get_current_prices
from app.utils.symbols import canonical_symbol

router = APIRouter(tags=["events"])

DEFAULT_LIMIT = 50
MAX_LIMIT = 200
MAX_SUGGEST_LIMIT = 50
VISIBLE_INSIDER_TRADE_TYPES = {"purchase", "sale"}


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




def _parse_event_payload(event: Event) -> dict:
    try:
        payload = json.loads(event.payload_json)
        if not isinstance(payload, dict):
            return {}
        return payload
    except Exception:
        return {}


def _congress_symbol_and_trade_date(event: Event, payload: dict) -> tuple[str, str | None]:
    sym = canonical_symbol(event.symbol or payload.get("symbol")) or ""
    trade_date = payload.get("trade_date") or payload.get("transaction_date")
    return sym, trade_date


def _event_payload(
    event: Event,
    db: Session,
    price_memo: dict[tuple[str, str], float | None],
    current_price_memo: dict[str, float],
) -> EventOut:
    payload = _parse_event_payload(event)

    estimated_price = None
    current_price = None
    pnl_pct = None
    if event.event_type == "congress_trade":
        sym, trade_date = _congress_symbol_and_trade_date(event, payload)
        if sym and trade_date:
            key = (sym, trade_date)
            if key not in price_memo:
                price_memo[key] = get_eod_close(db, sym, trade_date)
            estimated_price = price_memo[key]

        current_price = current_price_memo.get(sym)
        if current_price is not None and estimated_price is not None and estimated_price > 0:
            pnl_pct = ((current_price - estimated_price) / estimated_price) * 100

    return EventOut(
        id=event.id,
        event_type=event.event_type,
        ts=event.ts,
        symbol=event.symbol,
        source=event.source,
        member_name=event.member_name,
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
        if event.event_type != "congress_trade":
            continue
        payload = _parse_event_payload(event)
        sym, trade_date = _congress_symbol_and_trade_date(event, payload)
        if not sym or not trade_date:
            continue
        key = (sym, trade_date)
        if key not in price_memo:
            price_memo[key] = get_eod_close(db, sym, trade_date)
        if price_memo[key] is not None:
            quote_symbols.add(sym)

    current_price_memo = get_current_prices(sorted(quote_symbols)) if quote_symbols else {}

    items = [_event_payload(event, db, price_memo, current_price_memo) for event in paged_rows]

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
        if event.event_type != "congress_trade":
            continue
        payload = _parse_event_payload(event)
        sym, trade_date = _congress_symbol_and_trade_date(event, payload)
        if not sym or not trade_date:
            continue
        key = (sym, trade_date)
        if key not in price_memo:
            price_memo[key] = get_eod_close(db, sym, trade_date)
        if price_memo[key] is not None:
            quote_symbols.add(sym)

    current_price_memo = get_current_prices(sorted(quote_symbols)) if quote_symbols else {}

    items = [_event_payload(event, db, price_memo, current_price_memo) for event in rows]

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
