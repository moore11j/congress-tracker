"""Provider-isolated prepared news, using the existing enrichment queue/cache table."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from sqlalchemy.exc import SQLAlchemyError

from app.db import SessionLocal
from app.models import InsightsSnapshot
from app.services.finnhub_research import FinnhubUnavailable, fetch_news

TTL = timedelta(minutes=15)
MAX_STALE = timedelta(hours=24)
CATEGORY_FEEDS = {"crypto": "crypto", "currencies": "forex"}
# These are headline filters, not native provider topic tagging.
CATEGORY_TERMS = {
    "us-macro": ("inflation", "employment", "payroll", "gdp", "federal reserve", "economy", "recession"),
    "us-treasury": ("treasury", "treasuries", "bond", "yield", "federal reserve"),
    "commodities": ("commodity", "commodities", "gold", "silver", "copper", "crude", "oil", "natural gas", "wheat"),
}


def _aware(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _empty(page: int, limit: int, *, reason: str, status: str = "unavailable") -> dict:
    return {"items": [], "status": status, "source": "finnhub", "page": page, "limit": limit,
            "has_next": False, "reason": reason, "message": "News coverage is temporarily unavailable."}


def _enqueue(symbol: str | None, category: str) -> None:
    from app.services.data_enrichment_queue import enqueue_data_enrichment_job
    feed = CATEGORY_FEEDS.get(category, "general")
    enqueue_data_enrichment_job(job_type="news_stock" if symbol else "news_general", symbol=symbol,
        source="page_load", priority=50, window_key=f"finnhub:{feed}",
        reason="replacement_news_refresh", payload={"category": category, "page": 0, "limit": 50})


def prepared_news(*, symbol: str | None = None, category: str = "general", page: int = 0,
                  limit: int = 20, public: bool = True, force_refresh: bool = False,
                  enqueue_on_miss: bool = True) -> dict:
    from app.utils.symbols import normalize_symbol
    page, limit = max(0, int(page or 0)), max(1, min(50, int(limit or 20)))
    if symbol is not None:
        symbol = normalize_symbol(symbol)
        if not symbol:
            return _empty(page, limit, reason="invalid_symbol")
    feed = CATEGORY_FEEDS.get(category, "general")
    if category not in {"general", "world-indexes", "us-indexes", "us-sectors", *CATEGORY_FEEDS, *CATEGORY_TERMS}:
        return _empty(page, limit, reason="unsupported_category")
    kind = f"finnhub-news:company:{symbol}" if symbol else f"finnhub-news:market:{feed}"
    now = datetime.now(timezone.utc)
    payload = None
    fetched_at = None
    failure = None
    try:
        with SessionLocal() as db:
            row = db.get(InsightsSnapshot, kind)
            if row is not None and row.source == "finnhub":
                candidate = json.loads(row.payload_json)
                if isinstance(candidate, dict) and candidate.get("source") == "finnhub":
                    payload, fetched_at = candidate, _aware(row.fetched_at)
            needs_refresh = force_refresh or fetched_at is None or now - fetched_at > TTL
            if needs_refresh and not public:
                # Shared per-scope lock prevents parallel workers publishing the
                # same cache refresh. Public reads never acquire locks or fetch.
                if db.get_bind().dialect.name == "postgresql":
                    from sqlalchemy import text
                    locked = db.execute(text("SELECT pg_try_advisory_xact_lock(hashtext(:kind))"), {"kind": kind}).scalar()
                    if not locked:
                        failure = "refresh_in_progress"
                    else:
                        db.expire_all()
                        row = db.get(InsightsSnapshot, kind)
                        if row is not None and row.source == "finnhub" and not force_refresh:
                            candidate = json.loads(row.payload_json)
                            stamp = _aware(row.fetched_at)
                            if candidate.get("source") == "finnhub" and now - stamp <= TTL:
                                payload, fetched_at, needs_refresh = candidate, stamp, False
                if failure is None and needs_refresh:
                    try:
                        fresh = fetch_news(symbol=symbol, category=feed, observed_at=now)
                    except FinnhubUnavailable as exc:
                        failure = str(exc)
                    else:
                        # Refetching the same URL does not change when Walnut
                        # first saw it, including for later alert publication.
                        previous = {item.get('url'): item for item in (payload or {}).get('items', [])}
                        for item in fresh['items']:
                            old = previous.get(item['url'])
                            if old and old.get('published_at') == item['published_at']:
                                item['observed_at'] = old['observed_at']
                        if row is None:
                            row = InsightsSnapshot(kind=kind, source="finnhub", fetched_at=now, payload_json="{}")
                            db.add(row)
                        row.payload_json, row.fetched_at = json.dumps(fresh, sort_keys=True), now
                        row.source = "finnhub"
                        db.commit()
                        payload, fetched_at = fresh, now
    except (SQLAlchemyError, ValueError, TypeError):
        failure = "cache_unavailable"
    stale = fetched_at is None or now - fetched_at > TTL or failure is not None
    if public and stale and enqueue_on_miss:
        _enqueue(symbol, category)
    if payload is None or fetched_at is None or now - fetched_at > MAX_STALE:
        return _empty(page, limit, reason=failure or "cache_miss", status="warming" if public else "unavailable")
    items = payload.get("items") or []
    # Age the items independently of the cache. A repeat fetch must not make
    # last week's unchanged stories appear current indefinitely.
    items = [item for item in items if datetime.fromisoformat(item["published_at"]) >= now - timedelta(days=7)]
    if category in CATEGORY_TERMS:
        import re
        terms = CATEGORY_TERMS[category]
        items = [item for item in items if any(re.search(r"\b" + re.escape(term) + r"\b", item["title"], re.I) for term in terms)]
    offset = page * limit
    page_items = items[offset:offset + limit]
    result = {**payload, "items": page_items, "page": page, "limit": limit,
              "has_next": len(items) > offset + limit, "item_count": len(page_items),
              "status": "ok" if page_items else "empty", "stale": stale,
              "as_of": fetched_at.isoformat(), "cache_age_seconds": max(0, (now - fetched_at).total_seconds()),
              "category_filter": "headline_keywords" if category in CATEGORY_TERMS else "provider_feed",
              "provider_category": feed, "cache_status": "stale" if stale else "hit"}
    if failure:
        result["reason"] = failure
    return result
