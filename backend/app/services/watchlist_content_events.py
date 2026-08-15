"""Persist watchlist news and issuer-release cache entries as unified events."""
from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, time, timezone
from typing import Any

from sqlalchemy import func, inspect, select
from sqlalchemy.orm import Session

from app.models import Event, Security, TickerContentCache, WatchlistItem


CONTENT_EVENT_TYPES = {"news": "news_article", "press_releases": "press_release"}


def sync_watchlist_content_events(db: Session, watchlist_id: int, *, per_symbol_limit: int = 20) -> int:
    """Materialize cached ticker content once so every delivery path sees it.

    Content ingestion already owns fetching and cache freshness. This adapter only
    turns successfully fetched cache entries into deduplicated Event records;
    it deliberately performs no network work in an email or page request.
    """
    symbols = [
        str(symbol).strip().upper()
        for symbol in db.execute(
            select(Security.symbol)
            .join(WatchlistItem, WatchlistItem.security_id == Security.id)
            .where(WatchlistItem.watchlist_id == watchlist_id)
            .where(WatchlistItem.target_type == "ticker")
        ).scalars()
        if symbol and str(symbol).strip()
    ]
    if not symbols:
        return 0
    if not inspect(db.get_bind()).has_table("ticker_content_cache"):
        return 0
    rows = db.execute(
        select(TickerContentCache)
        .where(TickerContentCache.content_type.in_(tuple(CONTENT_EVENT_TYPES)))
        .where(func.upper(TickerContentCache.symbol).in_(symbols))
        .where(TickerContentCache.status == "ok")
        .order_by(TickerContentCache.fetched_at.desc(), TickerContentCache.id.desc())
    ).scalars().all()
    created = 0
    seen_cache_scopes: set[tuple[str, str]] = set()
    for row in rows:
        symbol = str(row.symbol or "").strip().upper()
        scope = (row.content_type, symbol)
        if not symbol or scope in seen_cache_scopes:
            continue
        seen_cache_scopes.add(scope)
        try:
            payload = json.loads(row.payload_json or "{}")
        except Exception:
            continue
        items = payload.get("items") if isinstance(payload, dict) else None
        if not isinstance(items, list):
            continue
        for item in items[: max(1, per_symbol_limit)]:
            event = _content_event(row.content_type, symbol, row.source, row.fetched_at, item)
            if event is None:
                continue
            key, event_type, timestamp, content_payload = event
            exists = db.execute(
                select(Event.id).where(Event.source_filing_id == key).limit(1)
            ).scalar_one_or_none()
            if exists is not None:
                continue
            db.add(
                Event(
                    event_type=event_type,
                    ts=timestamp,
                    event_date=timestamp,
                    symbol=symbol,
                    source=f"{row.source or 'market'}_{event_type}",
                    source_filing_id=key,
                    source_document_url=content_payload.get("url"),
                    data_source=row.source or "market",
                    impact_score=0,
                    payload_json=json.dumps(content_payload, default=str),
                )
            )
            created += 1
    if created:
        db.flush()
    return created


def _content_event(
    content_type: str,
    symbol: str,
    source: str | None,
    fetched_at: datetime,
    item: Any,
) -> tuple[str, str, datetime, dict[str, Any]] | None:
    if not isinstance(item, dict) or content_type not in CONTENT_EVENT_TYPES:
        return None
    title = _text(item.get("title"))
    url = _text(item.get("url"))
    if not title:
        return None
    published_at = _datetime(item.get("published_at") or item.get("publishedDate") or item.get("date")) or _aware(fetched_at)
    fingerprint = "|".join((content_type, symbol, url or "", title, published_at.isoformat()))
    key = "watchlist-content:" + hashlib.sha256(fingerprint.encode("utf-8")).hexdigest()
    return key, CONTENT_EVENT_TYPES[content_type], published_at, {
        "content_event_key": key,
        "title": title,
        "url": url,
        "summary": _text(item.get("summary") or item.get("text") or item.get("description")),
        "publisher": _text(item.get("site") or item.get("publisher") or item.get("source")),
        "image_url": _text(item.get("image_url") or item.get("image")),
        "published_at": published_at.isoformat(),
        "symbol": symbol,
        "data_category": "news" if content_type == "news" else "press_releases",
        "provider": source or "market",
    }


def _text(value: Any) -> str | None:
    return value.strip() if isinstance(value, str) and value.strip() else None


def _aware(value: datetime) -> datetime:
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


def _datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return _aware(value)
    if isinstance(value, date):
        return datetime.combine(value, time.min, tzinfo=timezone.utc)
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    return _aware(parsed)
