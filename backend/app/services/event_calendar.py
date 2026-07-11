from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from typing import Any, Literal

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import Security, UserAccount, Watchlist, WatchlistItem
from app.services.fmp_client import FMPControlledError, request_fmp_json
from app.utils.symbols import normalize_symbol, symbol_variants

CalendarEventKind = Literal["economic", "earnings", "dividend", "ipo", "split"]
CalendarScope = Literal["watchlist", "all"]


@dataclass(frozen=True)
class CalendarFetchResult:
    items: list[dict[str, Any]]
    errors: list[dict[str, str]]


_ENDPOINTS: tuple[tuple[CalendarEventKind, str], ...] = (
    ("economic", "economic-calendar"),
    ("earnings", "earnings-calendar"),
    ("dividend", "dividends-calendar"),
    ("ipo", "ipos-calendar"),
    ("split", "splits-calendar"),
)

_WATCHLIST_FILTERED_KINDS: set[CalendarEventKind] = {"earnings", "dividend", "split"}
_WATCHLIST_SYMBOL_ENDPOINTS: tuple[tuple[CalendarEventKind, str], ...] = (
    ("earnings", "earnings"),
    ("dividend", "dividends"),
    ("split", "splits"),
)


def watchlist_symbols_for_user(db: Session, user_id: int) -> list[str]:
    symbols: set[str] = set()
    for symbol in _watchlist_symbol_rows(db, user_id):
        symbols.update(_symbol_match_values(symbol))
    return sorted(symbols)


def watchlist_provider_symbols_for_user(db: Session, user_id: int) -> list[str]:
    symbols: set[str] = set()
    for symbol in _watchlist_symbol_rows(db, user_id):
        normalized = normalize_symbol(symbol)
        if normalized:
            symbols.add(normalized)
    return sorted(symbols)


def _watchlist_symbol_rows(db: Session, user_id: int) -> list[str]:
    rows = (
        db.execute(
            select(Security.symbol)
            .join(WatchlistItem, WatchlistItem.security_id == Security.id)
            .join(Watchlist, Watchlist.id == WatchlistItem.watchlist_id)
            .where(Watchlist.owner_user_id == user_id)
            .where(Security.symbol.is_not(None))
        )
        .scalars()
        .all()
    )
    return [symbol for symbol in rows if symbol and symbol.strip()]


def fetch_event_calendar(
    db: Session,
    user: UserAccount,
    *,
    start: date,
    end: date,
    scope: CalendarScope = "watchlist",
    source: str = "page_load",
    allow_live_fetch: bool = True,
    allow_user_request: bool = False,
) -> CalendarFetchResult:
    symbols = set(watchlist_symbols_for_user(db, user.id))
    provider_symbols = watchlist_provider_symbols_for_user(db, user.id)
    raw_items: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []
    params = {"from": start.isoformat(), "to": end.isoformat()}

    for kind, endpoint in _ENDPOINTS:
        if scope == "watchlist" and any(kind == symbol_kind for symbol_kind, _ in _WATCHLIST_SYMBOL_ENDPOINTS):
            continue
        try:
            payload = request_fmp_json(
                endpoint,
                params=params,
                category=f"calendar:{kind}",
                source=source,
                timeout_s=12,
                allow_live_fetch=allow_live_fetch,
                allow_user_request=allow_user_request,
            )
        except FMPControlledError as exc:
            errors.append({"kind": kind, "reason": exc.reason})
            continue

        for row in _rows_from_payload(payload):
            item = _calendar_item(kind, row)
            if item is None:
                continue
            if kind == "economic" and not _is_us_economic_event(item):
                continue
            if not _in_range(item, start=start, end=end):
                continue
            symbol_matches = _symbol_match_values(item.get("symbol"))
            if scope == "watchlist" and kind in _WATCHLIST_FILTERED_KINDS and (not symbol_matches or symbols.isdisjoint(symbol_matches)):
                continue
            raw_items.append(item)

    if scope == "watchlist":
        for kind, endpoint in _WATCHLIST_SYMBOL_ENDPOINTS:
            for provider_symbol in provider_symbols:
                try:
                    payload = request_fmp_json(
                        endpoint,
                        params={"symbol": provider_symbol},
                        category=f"calendar:{kind}:symbol",
                        symbol=provider_symbol,
                        source=source,
                        timeout_s=12,
                        allow_live_fetch=allow_live_fetch,
                        allow_user_request=allow_user_request,
                    )
                except FMPControlledError as exc:
                    errors.append({"kind": kind, "reason": exc.reason})
                    continue

                for row in _rows_from_payload(payload):
                    enriched_row = {**row, "symbol": row.get("symbol") or provider_symbol}
                    item = _calendar_item(kind, enriched_row)
                    if item is None or not _in_range(item, start=start, end=end):
                        continue
                    symbol_matches = _symbol_match_values(item.get("symbol"))
                    if symbol_matches and symbol_matches.isdisjoint(_symbol_match_values(provider_symbol)):
                        continue
                    raw_items.append(item)

    items = _dedupe_items(raw_items)
    items.sort(key=lambda item: (str(item.get("date") or ""), _kind_order(str(item.get("kind") or "")), str(item.get("symbol") or ""), str(item.get("title") or "")))
    return CalendarFetchResult(items=items, errors=errors)


def upcoming_event_calendar_items(
    db: Session,
    user: UserAccount,
    *,
    start: date,
    end: date,
    scope: CalendarScope = "watchlist",
    limit: int = 12,
) -> CalendarFetchResult:
    result = fetch_event_calendar(db, user, start=start, end=end, scope=scope, source="scheduled_job", allow_live_fetch=True)
    return CalendarFetchResult(items=result.items[: max(0, limit)], errors=result.errors)


def _rows_from_payload(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if isinstance(payload, dict):
        for key in ("items", "data", "results"):
            value = payload.get(key)
            if isinstance(value, list):
                return [row for row in value if isinstance(row, dict)]
            if isinstance(value, dict):
                return [value]
        return [payload] if payload else []
    return []


def _symbol_match_values(raw: Any) -> set[str]:
    text = str(raw or "").strip()
    if not text:
        return set()

    values = {text.upper()}
    normalized = normalize_symbol(text)
    if normalized:
        values.add(normalized)
        values.update(symbol_variants(normalized))
    return values


def _dedupe_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, ...]] = set()
    deduped: list[dict[str, Any]] = []
    for item in items:
        key = tuple(
            _dedupe_text(item.get(field))
            for field in ("kind", "date", "datetime", "symbol", "title", "subtitle", "country", "exchange")
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def _dedupe_text(value: Any) -> str:
    return " ".join(str(value or "").strip().lower().split())


def _is_us_economic_event(item: dict[str, Any]) -> bool:
    country = _dedupe_text(item.get("country"))
    if country in {"us", "usa", "united states", "united states of america"}:
        return True
    currency = _dedupe_text((item.get("payload") or {}).get("currency") if isinstance(item.get("payload"), dict) else None)
    return not country and currency in {"usd", "us dollar"}


def _calendar_item(kind: CalendarEventKind, row: dict[str, Any]) -> dict[str, Any] | None:
    event_date = _event_date(kind, row)
    if event_date is None:
        return None
    symbol = _text(row, "symbol", "ticker")
    company = _text(row, "company", "companyName", "name")
    title = _title(kind, row, symbol=symbol, company=company)
    payload = {key: value for key, value in row.items() if value is not None}
    item = {
        "id": _stable_id(kind, row, event_date),
        "kind": kind,
        "date": event_date.isoformat(),
        "datetime": _event_datetime(row, event_date),
        "symbol": symbol,
        "company": company,
        "title": title,
        "subtitle": _subtitle(kind, row),
        "country": _text(row, "country"),
        "exchange": _text(row, "exchange"),
        "importance": _importance(row),
        "payload": payload,
    }
    return item


def _in_range(item: dict[str, Any], *, start: date, end: date) -> bool:
    event_date = _parse_date(item.get("date"))
    return event_date is not None and start <= event_date <= end


def _event_date(kind: CalendarEventKind, row: dict[str, Any]) -> date | None:
    keys_by_kind: dict[CalendarEventKind, tuple[str, ...]] = {
        "economic": ("date",),
        "earnings": ("date", "reportDate"),
        "dividend": ("date", "paymentDate", "recordDate", "declarationDate"),
        "ipo": ("date", "ipoDate", "pricingDate"),
        "split": ("date", "splitDate"),
    }
    for key in keys_by_kind[kind]:
        parsed = _parse_date(row.get(key))
        if parsed is not None:
            return parsed
    return None


def _event_datetime(row: dict[str, Any], event_date: date) -> str | None:
    raw_time = _text(row, "time")
    if not raw_time:
        return None
    parsed = _parse_time(raw_time)
    if parsed is None:
        return None
    return datetime.combine(event_date, parsed, tzinfo=timezone.utc).isoformat()


def _title(kind: CalendarEventKind, row: dict[str, Any], *, symbol: str | None, company: str | None) -> str:
    if kind == "economic":
        return _text(row, "event", "title", "name") or "Economic release"
    if kind == "earnings":
        return f"{symbol or company or 'Company'} earnings"
    if kind == "dividend":
        return f"{symbol or company or 'Company'} dividend"
    if kind == "ipo":
        return f"{symbol or company or 'Company'} IPO"
    if kind == "split":
        return f"{symbol or company or 'Company'} split"
    return "Calendar event"


def _subtitle(kind: CalendarEventKind, row: dict[str, Any]) -> str | None:
    if kind == "economic":
        actual = _text(row, "actual")
        estimate = _text(row, "estimate", "consensus")
        previous = _text(row, "previous")
        parts = []
        if estimate:
            parts.append(f"est. {estimate}")
        if actual:
            parts.append(f"actual {actual}")
        if previous:
            parts.append(f"prev. {previous}")
        return " | ".join(parts) or _text(row, "currency", "country")
    if kind == "earnings":
        eps = _text(row, "epsEstimated", "epsEstimate")
        revenue = _text(row, "revenueEstimated", "revenueEstimate")
        return " | ".join(part for part in [f"EPS est. {eps}" if eps else "", f"Rev est. {revenue}" if revenue else ""] if part) or _text(row, "time")
    if kind == "dividend":
        dividend = _text(row, "dividend", "adjDividend")
        payment = _text(row, "paymentDate")
        return " | ".join(part for part in [f"div. {dividend}" if dividend else "", f"pay {payment}" if payment else ""] if part) or None
    if kind == "ipo":
        return _text(row, "priceRange", "price", "shares", "actions", "exchange")
    if kind == "split":
        ratio = _split_ratio(row)
        return ratio or _text(row, "label")
    return None


def _split_ratio(row: dict[str, Any]) -> str | None:
    numerator = _text(row, "numerator")
    denominator = _text(row, "denominator")
    if numerator and denominator:
        return f"{numerator}:{denominator}"
    return _text(row, "ratio", "splitRatio")


def _importance(row: dict[str, Any]) -> str | None:
    value = _text(row, "impact", "importance")
    return value.lower() if value else None


def _text(row: dict[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = row.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def _parse_date(value: Any) -> date | None:
    text = str(value or "").strip()
    if not text:
        return None
    if "T" in text:
        text = text.split("T", 1)[0]
    if " " in text:
        text = text.split(" ", 1)[0]
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _parse_time(value: str) -> time | None:
    text = value.strip().upper().replace("ET", "").replace("UTC", "").strip()
    for fmt in ("%H:%M:%S", "%H:%M", "%I:%M %p", "%I %p"):
        try:
            return datetime.strptime(text, fmt).time()
        except ValueError:
            continue
    return None


def _stable_id(kind: CalendarEventKind, row: dict[str, Any], event_date: date) -> str:
    basis = {
        "kind": kind,
        "date": event_date.isoformat(),
        "symbol": _text(row, "symbol", "ticker"),
        "event": _text(row, "event", "title", "name", "company", "companyName"),
        "payload": row,
    }
    digest = hashlib.sha1(json.dumps(basis, sort_keys=True, default=str).encode("utf-8")).hexdigest()
    return f"{kind}:{digest[:16]}"


def _kind_order(kind: str) -> int:
    return {"economic": 0, "earnings": 1, "dividend": 2, "ipo": 3, "split": 4}.get(kind, 99)
