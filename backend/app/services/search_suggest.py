from __future__ import annotations

import logging
import re
from time import perf_counter
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models import Event, InsiderTransaction, Member, Security, TickerMeta
from app.services.government_departments import department_suggestions
from app.services.ticker_identity import safe_company_identity_candidate
from app.utils.symbols import normalize_symbol

logger = logging.getLogger(__name__)

SearchSuggestItem = dict[str, str | int | float | None]
MAX_SEARCH_SUGGEST_LIMIT = 20


def normalize_search_query(q: str | None) -> str:
    cleaned = re.sub(r"\s+", " ", (q or "").strip())
    return cleaned[:80]


def _clean(value: Any) -> str | None:
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


def _score(query: str, *, symbol: str | None = None, label: str | None = None, popularity: int = 0) -> float:
    q = query.casefold()
    symbol_key = (symbol or "").casefold()
    label_key = (label or "").casefold()
    score = min(float(popularity), 100.0) / 10.0
    if symbol_key == q:
        score += 1000
    elif symbol_key.startswith(q):
        score += 750
    elif q in symbol_key and len(q) > 1:
        score += 420
    if label_key == q:
        score += 360
    elif label_key.startswith(q):
        score += 280
    elif q in label_key and len(q) > 1:
        score += 120
    return score


def _ticker_item(symbol: str, label: str | None, exchange: str | None, score: float) -> SearchSuggestItem:
    subtitle_parts = ["Ticker", label, exchange]
    return {
        "kind": "ticker",
        "id": symbol,
        "symbol": symbol,
        "label": label or symbol,
        "subtitle": " - ".join(part for part in subtitle_parts if part),
        "href": f"/ticker/{symbol}",
        "score": score,
    }


def _ticker_suggestions(db: Session, query: str, limit: int) -> list[SearchSuggestItem]:
    q_lower = query.casefold()
    prefix = f"{q_lower}%"
    contains = f"%{q_lower}%"
    name_clause = func.lower(func.coalesce(Security.name, "")).like(prefix if len(query) <= 1 else contains)
    meta_name_clause = func.lower(func.coalesce(TickerMeta.company_name, "")).like(prefix if len(query) <= 1 else contains)

    security_rows = db.execute(
        select(
            Security.symbol,
            Security.name.label("security_name"),
            TickerMeta.company_name.label("metadata_name"),
            TickerMeta.exchange,
        )
        .select_from(Security)
        .outerjoin(TickerMeta, func.upper(func.coalesce(TickerMeta.symbol, "")) == func.upper(func.coalesce(Security.symbol, "")))
        .where(Security.symbol.is_not(None))
        .where(func.length(func.trim(Security.symbol)) > 0)
        .where(
            (func.lower(Security.symbol) == q_lower)
            | func.lower(Security.symbol).like(prefix)
            | name_clause
            | meta_name_clause
        )
        .order_by(func.length(Security.symbol), func.upper(Security.symbol))
        .limit(limit * 4)
    ).all()

    event_rows = db.execute(
        select(
            Event.symbol,
            func.max(Security.name).label("security_name"),
            func.max(TickerMeta.company_name).label("metadata_name"),
            func.max(TickerMeta.exchange).label("exchange"),
            func.count(Event.id).label("activity_count"),
        )
        .select_from(Event)
        .outerjoin(Security, func.upper(func.coalesce(Security.symbol, "")) == func.upper(func.coalesce(Event.symbol, "")))
        .outerjoin(TickerMeta, func.upper(func.coalesce(TickerMeta.symbol, "")) == func.upper(func.coalesce(Event.symbol, "")))
        .where(Event.symbol.is_not(None))
        .where(func.length(func.trim(Event.symbol)) > 0)
        .where(func.lower(Event.symbol).like(prefix if len(query) <= 1 else contains))
        .group_by(Event.symbol)
        .order_by(func.count(Event.id).desc(), func.upper(Event.symbol))
        .limit(limit * 4)
    ).all()

    by_symbol: dict[str, SearchSuggestItem] = {}
    for row in [*security_rows, *event_rows]:
        symbol = normalize_symbol(row.symbol)
        if not symbol:
            continue
        label = safe_company_identity_candidate(_clean(row.metadata_name), symbol) or safe_company_identity_candidate(_clean(row.security_name), symbol)
        score = _score(query, symbol=symbol, label=label, popularity=int(getattr(row, "activity_count", 0) or 0))
        item = _ticker_item(symbol, label, _clean(getattr(row, "exchange", None)), score)
        existing = by_symbol.get(symbol)
        if existing is None or float(existing.get("score") or 0) < score:
            by_symbol[symbol] = item
    return sorted(by_symbol.values(), key=lambda item: (-(float(item.get("score") or 0)), str(item.get("symbol") or "")))[:limit]


def _member_suggestions(db: Session, query: str, limit: int) -> list[SearchSuggestItem]:
    q_lower = query.casefold()
    pattern = f"{q_lower}%" if len(query) <= 1 else f"%{q_lower}%"
    member_name_expr = func.trim(func.coalesce(Member.first_name, "") + " " + func.coalesce(Member.last_name, ""))
    rows = db.execute(
        select(Member.bioguide_id, member_name_expr.label("member_name"), Member.party, Member.state, Member.chamber)
        .where(Member.bioguide_id.is_not(None))
        .where(func.length(member_name_expr) > 0)
        .where(func.lower(member_name_expr).like(pattern))
        .order_by(func.lower(Member.last_name), func.lower(Member.first_name), func.lower(Member.bioguide_id))
        .limit(limit * 2)
    ).all()
    items: list[SearchSuggestItem] = []
    seen: set[str] = set()
    for row in rows:
        bioguide_id = _clean(row.bioguide_id)
        name = _clean(row.member_name)
        if not bioguide_id or not name:
            continue
        key = f"{name.casefold()}:{(row.chamber or '').casefold()}"
        if key in seen:
            continue
        seen.add(key)
        subtitle = " - ".join(part for part in ["Member", _clean(row.chamber), _clean(row.party), _clean(row.state)] if part)
        items.append(
            {
                "kind": "member",
                "id": bioguide_id,
                "symbol": None,
                "label": name,
                "subtitle": subtitle,
                "href": _member_href(name, bioguide_id),
                "score": _score(query, label=name) + 30,
            }
        )
        if len(items) >= limit:
            break
    return sorted(items, key=lambda item: (-(float(item.get("score") or 0)), str(item.get("label") or "")))[:limit]


def _member_href(member_name: str, bioguide_id: str) -> str:
    slug = member_name.strip().upper().replace(".", "").replace(",", "").replace("'", "").replace("-", " ")
    slug = "_".join(part for part in slug.split() if part)
    return f"/member/{slug or bioguide_id}"


def _insider_suggestions(db: Session, query: str, limit: int) -> list[SearchSuggestItem]:
    q_lower = query.casefold()
    pattern = f"{q_lower}%" if len(query) <= 1 else f"%{q_lower}%"
    rows = db.execute(
        select(
            InsiderTransaction.insider_name,
            InsiderTransaction.symbol,
            InsiderTransaction.reporting_cik,
            InsiderTransaction.role,
            func.max(InsiderTransaction.filing_date).label("latest_date"),
        )
        .where(InsiderTransaction.insider_name.is_not(None))
        .where(func.length(func.trim(InsiderTransaction.insider_name)) > 0)
        .where(func.lower(InsiderTransaction.insider_name).like(pattern))
        .group_by(InsiderTransaction.insider_name, InsiderTransaction.symbol, InsiderTransaction.reporting_cik, InsiderTransaction.role)
        .order_by(func.max(InsiderTransaction.filing_date).desc())
        .limit(limit * 4)
    ).all()
    items: list[SearchSuggestItem] = []
    seen: set[str] = set()
    for row in rows:
        name = _clean(row.insider_name)
        if not name:
            continue
        symbol = normalize_symbol(row.symbol)
        reporting_cik = _clean(row.reporting_cik)
        key = f"{name.casefold()}:{reporting_cik or symbol or ''}"
        if key in seen:
            continue
        seen.add(key)
        href = f"/feed?member={name}"
        if reporting_cik:
            href = f"/insider/{_insider_slug(name, reporting_cik)}"
            if symbol:
                href = f"{href}?issuer={symbol}"
        items.append(
            {
                "kind": "insider",
                "id": key,
                "symbol": symbol,
                "label": name,
                "subtitle": " - ".join(part for part in ["Insider", symbol, _clean(row.role)] if part),
                "href": href,
                "score": _score(query, symbol=symbol, label=name),
            }
        )
        if len(items) >= limit:
            break
    return sorted(items, key=lambda item: (-(float(item.get("score") or 0)), str(item.get("label") or "")))[:limit]


def _insider_slug(name: str, reporting_cik: str) -> str:
    slug = "".join(ch.lower() if ch.isalnum() else "-" for ch in name.strip())
    slug = "-".join(part for part in slug.split("-") if part)
    return f"{slug}-{reporting_cik}" if slug else reporting_cik


def _agency_suggestions(db: Session, query: str, limit: int) -> list[SearchSuggestItem]:
    items: list[SearchSuggestItem] = []
    for item in department_suggestions(db, query, limit=limit):
        href = _clean(item.get("route")) or f"/departments/{item.get('id')}"
        label = _clean(item.get("label")) or _clean(item.get("name")) or "Agency"
        items.append(
            {
                "kind": "agency",
                "id": _clean(item.get("id")) or href,
                "symbol": None,
                "label": label,
                "subtitle": _clean(item.get("subtitle")) or "Government agency",
                "href": href,
                "score": _score(query, label=label) + 10,
            }
        )
    return items


def search_suggestions(db: Session, q: str | None, limit: int = 8) -> dict[str, Any]:
    started_at = perf_counter()
    query = normalize_search_query(q)
    bounded_limit = max(1, min(int(limit or 8), MAX_SEARCH_SUGGEST_LIMIT))
    if not query:
        return {"items": [], "results": [], "query": query}

    results: list[SearchSuggestItem] = []
    per_kind_limit = max(bounded_limit, 8)
    for loader in (_ticker_suggestions, _member_suggestions, _insider_suggestions, _agency_suggestions):
        try:
            results.extend(loader(db, query, per_kind_limit))
        except Exception:
            logger.exception("search_suggest_loader_failed loader=%s query_length=%s", loader.__name__, len(query))

    results.sort(key=lambda item: (-(float(item.get("score") or 0)), str(item.get("kind") or ""), str(item.get("label") or "")))
    items = [{key: value for key, value in item.items() if key != "score"} for item in results[:bounded_limit]]
    duration_ms = (perf_counter() - started_at) * 1000
    logger.info(
        "search_suggest_timing duration_ms=%.1f query_length=%s result_count=%s",
        duration_ms,
        len(query),
        len(items),
    )
    return {"items": items, "results": items, "query": query}
