from __future__ import annotations

import logging
import re
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from time import perf_counter
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models import (
    Event,
    InsiderTransaction,
    Member,
    PageViewEvent,
    SavedScreenEvent,
    SavedScreenSnapshot,
    Security,
    TickerMeta,
    Watchlist,
    WatchlistItem,
)
from app.request_priority import get_request_context
from app.services.government_departments import department_suggestions
from app.services.ticker_identity import safe_company_identity_candidate
from app.utils.symbols import normalize_symbol

logger = logging.getLogger(__name__)

SearchSuggestItem = dict[str, str | int | float | None]
MAX_SEARCH_SUGGEST_LIMIT = 20
PERSONALIZATION_CACHE_TTL_SECONDS = 45
PERSONALIZATION_SYMBOL_LIMIT = 160
ANONYMOUS_SEARCH_CACHE_TTL_SECONDS = 20 * 60


@dataclass(frozen=True)
class SearchPersonalization:
    symbol_boosts: dict[str, float] = field(default_factory=dict)
    href_boosts: dict[str, float] = field(default_factory=dict)


_personalization_cache: dict[int, tuple[float, SearchPersonalization]] = {}
_anonymous_suggestion_cache: dict[tuple[str, int], tuple[float, dict[str, Any]]] = {}
_anonymous_suggestion_cache_lock = threading.Lock()
_WORD_RE = re.compile(r"[a-z0-9]+")


def normalize_search_query(q: str | None) -> str:
    cleaned = re.sub(r"\s+", " ", (q or "").strip())
    return cleaned[:80]


def _clean(value: Any) -> str | None:
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


def _search_key(value: str | None) -> str:
    return " ".join(_WORD_RE.findall((value or "").casefold().replace("&", " and ")))


def _compact_key(value: str | None) -> str:
    return "".join(_WORD_RE.findall((value or "").casefold()))


def _acronym(value: str | None) -> str:
    return "".join(word[0] for word in _WORD_RE.findall((value or "").casefold()))


def _bounded_edit_distance(left: str, right: str, max_distance: int) -> int:
    if abs(len(left) - len(right)) > max_distance:
        return max_distance + 1
    previous = list(range(len(right) + 1))
    for i, left_ch in enumerate(left, start=1):
        current = [i]
        row_min = current[0]
        for j, right_ch in enumerate(right, start=1):
            cost = 0 if left_ch == right_ch else 1
            value = min(previous[j] + 1, current[j - 1] + 1, previous[j - 1] + cost)
            current.append(value)
            row_min = min(row_min, value)
        if row_min > max_distance:
            return max_distance + 1
        previous = current
    return previous[-1]


def _subsequence_ratio(query: str, candidate: str) -> float:
    if not query or not candidate:
        return 0.0
    index = 0
    for char in candidate:
        if index < len(query) and query[index] == char:
            index += 1
    return index / len(query)


def _text_match_score(query: str, candidate: str | None, *, symbol_like: bool = False) -> float:
    q = _search_key(query)
    key = _search_key(candidate)
    if not q or not key:
        return 0.0

    compact_q = _compact_key(query)
    compact_key = _compact_key(candidate)
    if compact_key == compact_q:
        return 1000.0 if symbol_like else 420.0
    if compact_key.startswith(compact_q):
        return 760.0 if symbol_like else 360.0
    if len(compact_q) > 1 and compact_q in compact_key:
        return 430.0 if symbol_like else 175.0

    words = key.split()
    if any(word.startswith(q) for word in words):
        return 330.0
    if _acronym(candidate).startswith(compact_q) and len(compact_q) >= 2:
        return 210.0

    if len(compact_q) >= 3:
        typo_budget = 1 if len(compact_q) < 6 else 2
        best_distance = min(
            [_bounded_edit_distance(compact_q, word[: max(len(compact_q), len(word))], typo_budget) for word in words]
            + [_bounded_edit_distance(compact_q, compact_key[: max(len(compact_q), min(len(compact_key), len(compact_q) + 2))], typo_budget)]
        )
        if best_distance <= typo_budget:
            return 245.0 - (best_distance * 45.0)

    if len(compact_q) >= 3 and _subsequence_ratio(compact_q, compact_key) >= 0.86:
        return 115.0
    return 0.0


def _score(
    query: str,
    *,
    symbol: str | None = None,
    label: str | None = None,
    popularity: int = 0,
    context_boost: float = 0.0,
) -> float:
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
    text_score = max(_text_match_score(query, symbol, symbol_like=True), _text_match_score(query, label))
    score = max(score, text_score + min(float(popularity), 100.0) / 10.0)
    if text_score > 0 or symbol_key == q or symbol_key.startswith(q) or label_key == q or label_key.startswith(q):
        score += min(float(context_boost or 0.0), 720.0)
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


def _add_boost(boosts: dict[str, float], key: str | None, amount: float) -> None:
    cleaned = _clean(key)
    if not cleaned:
        return
    boosts[cleaned] = min(boosts.get(cleaned, 0.0) + amount, 720.0)


def _recent_path_entity(path: str | None) -> tuple[str, str] | None:
    clean_path = (path or "").split("?", 1)[0].strip()
    if clean_path.startswith("/ticker/"):
        symbol = normalize_symbol(clean_path.removeprefix("/ticker/"))
        return ("ticker", symbol) if symbol else None
    if clean_path.startswith("/member/") and len(clean_path) > len("/member/"):
        return "href", clean_path
    if clean_path.startswith("/insider/") and len(clean_path) > len("/insider/"):
        return "href", clean_path
    return None


def _personalization_for_user(db: Session, user_id: int | None) -> SearchPersonalization:
    if not user_id:
        return SearchPersonalization()

    now = perf_counter()
    cached = _personalization_cache.get(user_id)
    if cached and now - cached[0] <= PERSONALIZATION_CACHE_TTL_SECONDS:
        return cached[1]

    symbol_boosts: dict[str, float] = {}
    href_boosts: dict[str, float] = {}
    try:
        watchlist_symbols = db.execute(
            select(Security.symbol)
            .select_from(WatchlistItem)
            .join(Watchlist, Watchlist.id == WatchlistItem.watchlist_id)
            .join(Security, Security.id == WatchlistItem.security_id)
            .where(Watchlist.owner_user_id == user_id)
            .where(Security.symbol.is_not(None))
            .limit(PERSONALIZATION_SYMBOL_LIMIT)
        ).scalars()
        for symbol in watchlist_symbols:
            _add_boost(symbol_boosts, normalize_symbol(symbol), 740.0)

        saved_snapshot_symbols = db.execute(
            select(SavedScreenSnapshot.ticker)
            .where(SavedScreenSnapshot.user_id == user_id)
            .where(SavedScreenSnapshot.ticker.is_not(None))
            .order_by(SavedScreenSnapshot.updated_at.desc())
            .limit(PERSONALIZATION_SYMBOL_LIMIT)
        ).scalars()
        for symbol in saved_snapshot_symbols:
            _add_boost(symbol_boosts, normalize_symbol(symbol), 250.0)

        saved_event_symbols = db.execute(
            select(SavedScreenEvent.ticker)
            .where(SavedScreenEvent.user_id == user_id)
            .where(SavedScreenEvent.ticker.is_not(None))
            .order_by(SavedScreenEvent.created_at.desc())
            .limit(PERSONALIZATION_SYMBOL_LIMIT)
        ).scalars()
        for symbol in saved_event_symbols:
            _add_boost(symbol_boosts, normalize_symbol(symbol), 160.0)

        recent_cutoff = datetime.now(timezone.utc) - timedelta(days=30)
        recent_paths = db.execute(
            select(PageViewEvent.path)
            .where(PageViewEvent.user_id == user_id)
            .where(PageViewEvent.created_at >= recent_cutoff)
            .where(
                (PageViewEvent.path.like("/ticker/%"))
                | (PageViewEvent.path.like("/member/%"))
                | (PageViewEvent.path.like("/insider/%"))
            )
            .order_by(PageViewEvent.created_at.desc())
            .limit(60)
        ).scalars()
        for path in recent_paths:
            entity = _recent_path_entity(path)
            if not entity:
                continue
            kind, value = entity
            if kind == "ticker":
                _add_boost(symbol_boosts, value, 220.0)
            else:
                _add_boost(href_boosts, value, 180.0)
    except Exception:
        logger.exception("search_personalization_failed user_id=%s", user_id)

    personalization = SearchPersonalization(symbol_boosts=symbol_boosts, href_boosts=href_boosts)
    _personalization_cache[user_id] = (now, personalization)
    return personalization


def _candidate_clauses(query: str, symbol_col: Any, label_cols: list[Any]) -> Any:
    q_lower = query.casefold()
    prefix = f"{q_lower}%"
    contains = f"%{q_lower}%"
    clauses = [func.lower(symbol_col) == q_lower, func.lower(symbol_col).like(prefix)]
    if len(query) > 1:
        clauses.append(func.lower(symbol_col).like(contains))
    for label_col in label_cols:
        label_expr = func.lower(func.coalesce(label_col, ""))
        clauses.append(label_expr.like(prefix if len(query) <= 1 else contains))
        if len(query) >= 3:
            clauses.append(label_expr.like(f"{q_lower[:2]}%"))
    if len(query) >= 3:
        clauses.append(func.lower(symbol_col).like(f"{q_lower[:2]}%"))
    combined = clauses[0]
    for clause in clauses[1:]:
        combined = combined | clause
    return combined


def _ticker_suggestions(db: Session, query: str, limit: int, personalization: SearchPersonalization | None = None) -> list[SearchSuggestItem]:
    personalization = personalization or SearchPersonalization()
    boosted_symbols = sorted(personalization.symbol_boosts)
    candidate_limit = max(limit * 12, 80)

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
        .where(_candidate_clauses(query, Security.symbol, [Security.name, TickerMeta.company_name]))
        .order_by(func.length(Security.symbol), func.upper(Security.symbol))
        .limit(candidate_limit)
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
        .where(_candidate_clauses(query, Event.symbol, [Security.name, TickerMeta.company_name]))
        .group_by(Event.symbol)
        .order_by(func.count(Event.id).desc(), func.upper(Event.symbol))
        .limit(candidate_limit)
    ).all()

    context_rows = []
    if boosted_symbols:
        context_rows = db.execute(
            select(
                Security.symbol,
                Security.name.label("security_name"),
                TickerMeta.company_name.label("metadata_name"),
                TickerMeta.exchange,
            )
            .select_from(Security)
            .outerjoin(TickerMeta, func.upper(func.coalesce(TickerMeta.symbol, "")) == func.upper(func.coalesce(Security.symbol, "")))
            .where(func.upper(Security.symbol).in_(boosted_symbols[:PERSONALIZATION_SYMBOL_LIMIT]))
            .limit(PERSONALIZATION_SYMBOL_LIMIT)
        ).all()

    by_symbol: dict[str, SearchSuggestItem] = {}
    for row in [*security_rows, *event_rows, *context_rows]:
        symbol = normalize_symbol(row.symbol)
        if not symbol:
            continue
        label = safe_company_identity_candidate(_clean(row.metadata_name), symbol) or safe_company_identity_candidate(_clean(row.security_name), symbol)
        boost = personalization.symbol_boosts.get(symbol, 0.0)
        score = _score(query, symbol=symbol, label=label, popularity=int(getattr(row, "activity_count", 0) or 0), context_boost=boost)
        if score <= 0:
            continue
        item = _ticker_item(symbol, label, _clean(getattr(row, "exchange", None)), score)
        existing = by_symbol.get(symbol)
        if existing is None or float(existing.get("score") or 0) < score:
            by_symbol[symbol] = item
    return sorted(by_symbol.values(), key=lambda item: (-(float(item.get("score") or 0)), str(item.get("symbol") or "")))[:limit]


def _member_suggestions(db: Session, query: str, limit: int, personalization: SearchPersonalization | None = None) -> list[SearchSuggestItem]:
    q_lower = query.casefold()
    pattern = f"{q_lower}%" if len(query) <= 1 else f"%{q_lower}%"
    fuzzy_prefix = f"{q_lower[:2]}%" if len(query) >= 3 else pattern
    member_name_expr = func.trim(func.coalesce(Member.first_name, "") + " " + func.coalesce(Member.last_name, ""))
    rows = db.execute(
        select(Member.bioguide_id, member_name_expr.label("member_name"), Member.party, Member.state, Member.chamber)
        .where(Member.bioguide_id.is_not(None))
        .where(func.length(member_name_expr) > 0)
        .where(
            (func.lower(member_name_expr).like(pattern))
            | (func.lower(member_name_expr).like(fuzzy_prefix))
            | (func.lower(func.coalesce(Member.first_name, "")).like(fuzzy_prefix))
            | (func.lower(func.coalesce(Member.last_name, "")).like(fuzzy_prefix))
        )
        .order_by(func.lower(Member.last_name), func.lower(Member.first_name), func.lower(Member.bioguide_id))
        .limit(max(limit * 4, 24))
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
        score = _score(query, label=name, context_boost=(personalization or SearchPersonalization()).href_boosts.get(_member_href(name, bioguide_id), 0.0)) + 30
        if score <= 30:
            continue
        items.append(
            {
                "kind": "member",
                "id": bioguide_id,
                "symbol": None,
                "label": name,
                "subtitle": subtitle,
                "href": _member_href(name, bioguide_id),
                "score": score,
            }
        )
        if len(items) >= limit:
            break
    return sorted(items, key=lambda item: (-(float(item.get("score") or 0)), str(item.get("label") or "")))[:limit]


def _member_href(member_name: str, bioguide_id: str) -> str:
    slug = member_name.strip().upper().replace(".", "").replace(",", "").replace("'", "").replace("-", " ")
    slug = "_".join(part for part in slug.split() if part)
    return f"/member/{slug or bioguide_id}"


def _insider_suggestions(db: Session, query: str, limit: int, personalization: SearchPersonalization | None = None) -> list[SearchSuggestItem]:
    q_lower = query.casefold()
    pattern = f"{q_lower}%" if len(query) <= 1 else f"%{q_lower}%"
    fuzzy_contains = f"%{q_lower[:2]}%" if len(query) >= 3 else pattern
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
        .where((func.lower(InsiderTransaction.insider_name).like(pattern)) | (func.lower(InsiderTransaction.insider_name).like(fuzzy_contains)))
        .group_by(InsiderTransaction.insider_name, InsiderTransaction.symbol, InsiderTransaction.reporting_cik, InsiderTransaction.role)
        .order_by(func.max(InsiderTransaction.filing_date).desc())
        .limit(max(limit * 6, 36))
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
        href_boost = (personalization or SearchPersonalization()).href_boosts.get(href.split("?", 1)[0], 0.0)
        score = _score(query, symbol=symbol, label=name, context_boost=href_boost)
        if score <= 0:
            continue
        items.append(
            {
                "kind": "insider",
                "id": key,
                "symbol": symbol,
                "label": name,
                "subtitle": " - ".join(part for part in ["Insider", symbol, _clean(row.role)] if part),
                "href": href,
                "score": score,
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


def search_suggestions(db: Session, q: str | None, limit: int = 8, *, user_id: int | None = None) -> dict[str, Any]:
    started_at = perf_counter()
    query = normalize_search_query(q)
    bounded_limit = max(1, min(int(limit or 8), MAX_SEARCH_SUGGEST_LIMIT))
    if not query:
        return {"items": [], "results": [], "query": query}
    cache_key = (query.casefold(), bounded_limit)
    if user_id is None:
        now = perf_counter()
        with _anonymous_suggestion_cache_lock:
            cached = _anonymous_suggestion_cache.get(cache_key)
            if cached and now - cached[0] <= ANONYMOUS_SEARCH_CACHE_TTL_SECONDS:
                return cached[1]

    results: list[SearchSuggestItem] = []
    per_kind_limit = max(bounded_limit, 8)
    personalization = _personalization_for_user(db, user_id)
    loaders = (
        lambda: _ticker_suggestions(db, query, per_kind_limit, personalization),
        lambda: _member_suggestions(db, query, per_kind_limit, personalization),
        lambda: _insider_suggestions(db, query, per_kind_limit, personalization),
        lambda: _agency_suggestions(db, query, per_kind_limit),
    )
    for loader in loaders:
        try:
            results.extend(loader())
        except Exception:
            logger.exception("search_suggest_loader_failed query_length=%s", len(query))

    results.sort(key=lambda item: (-(float(item.get("score") or 0)), str(item.get("kind") or ""), str(item.get("label") or "")))
    items = [{key: value for key, value in item.items() if key != "score"} for item in results[:bounded_limit]]
    duration_ms = (perf_counter() - started_at) * 1000
    context = get_request_context() or {}
    logger.info(
        "search_suggest_timing duration_ms=%.1f query_length=%s result_count=%s db_query_count=%s db_checkout_count=%s db_checkout_slow_count=%s",
        duration_ms,
        len(query),
        len(items),
        context.get("db_query_count"),
        context.get("db_checkout_count"),
        context.get("db_checkout_slow_count"),
    )
    payload = {"items": items, "results": items, "query": query}
    if user_id is None:
        with _anonymous_suggestion_cache_lock:
            _anonymous_suggestion_cache[cache_key] = (perf_counter(), payload)
            if len(_anonymous_suggestion_cache) > 256:
                oldest_keys = sorted(_anonymous_suggestion_cache, key=lambda key: _anonymous_suggestion_cache[key][0])[:64]
                for oldest_key in oldest_keys:
                    _anonymous_suggestion_cache.pop(oldest_key, None)
    return payload
