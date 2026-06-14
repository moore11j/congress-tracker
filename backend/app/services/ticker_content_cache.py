from __future__ import annotations

import json
import logging
import os
from datetime import date, datetime, timezone
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.db import IS_SQLITE, SessionLocal
from app.models import TickerContentCache
from app.services.provider_usage import record_cache_hit
from app.utils.symbols import normalize_symbol

logger = logging.getLogger(__name__)

CONTENT_TTL_SECONDS = {
    "news": 15 * 60,
    "press_releases": 30 * 60,
    "sec_filings": 60 * 60,
}
CONTENT_STALE_TTL_SECONDS = 24 * 60 * 60


def persistent_ticker_content_cache_enabled() -> bool:
    if not IS_SQLITE:
        return True
    return os.getenv("TICKER_CONTENT_SQLITE_CACHE", "").strip().lower() in {"1", "true", "yes", "on"}


def ticker_content_window_key(
    content_type: str,
    *,
    from_date: str | None = None,
    to_date: str | None = None,
) -> str:
    if content_type != "sec_filings":
        return "latest"
    from_value = (from_date or "")[:10]
    to_value = (to_date or "")[:10]
    try:
        start = date.fromisoformat(from_value)
        end = date.fromisoformat(to_value)
        if 360 <= (end - start).days <= 370:
            return "365d"
    except ValueError:
        pass
    if from_value or to_value:
        return f"{from_value or 'any'}:{to_value or 'any'}"
    return "365d"


def ticker_content_cache_key(content_type: str, symbol: str, window_key: str = "latest") -> str:
    normalized = normalize_symbol(symbol) or str(symbol or "").strip().upper()
    return f"{content_type}:{normalized}:{window_key or 'latest'}"


def ticker_content_age_seconds(fetched_at: datetime | None) -> float | None:
    if fetched_at is None:
        return None
    if fetched_at.tzinfo is None:
        fetched_at = fetched_at.replace(tzinfo=timezone.utc)
    return max((datetime.now(timezone.utc) - fetched_at.astimezone(timezone.utc)).total_seconds(), 0)


def _loads_payload(row: TickerContentCache) -> dict[str, Any] | None:
    try:
        payload = json.loads(row.payload_json or "{}")
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def paginate_ticker_content_payload(
    payload: dict[str, Any],
    *,
    page: int,
    limit: int,
    stale: bool = False,
    cache_age_seconds: float | None = None,
) -> dict[str, Any]:
    all_items = payload.get("items")
    items = all_items if isinstance(all_items, list) else []
    offset = max(int(page or 0), 0) * max(int(limit or 1), 1)
    bounded_limit = max(int(limit or 1), 1)
    window = items[offset : offset + bounded_limit + 1]
    page_items = window[:bounded_limit]
    result = {
        **payload,
        "items": page_items,
        "page": max(int(page or 0), 0),
        "limit": bounded_limit,
        "has_next": len(window) > bounded_limit,
        "item_count": len(page_items),
        "status": "ok" if page_items else "empty",
        "cache_status": "stale" if stale else "hit",
    }
    if stale:
        result["stale"] = True
    if cache_age_seconds is not None:
        result["cache_age_seconds"] = round(cache_age_seconds, 1)
    return result


def db_ticker_content_cache_get(
    content_type: str,
    symbol: str,
    *,
    page: int = 0,
    limit: int = 20,
    window_key: str = "latest",
    session: Session | None = None,
) -> dict[str, Any] | None:
    if not persistent_ticker_content_cache_enabled():
        return None
    normalized = normalize_symbol(symbol)
    if not normalized:
        return None
    own_session = session is None
    db = session or SessionLocal()
    try:
        row = db.execute(
            select(TickerContentCache)
            .where(TickerContentCache.content_type == content_type)
            .where(func.upper(TickerContentCache.symbol) == normalized)
            .where(TickerContentCache.window_key == (window_key or "latest"))
            .limit(1)
        ).scalar_one_or_none()
        if row is None:
            return None
        age = ticker_content_age_seconds(row.fetched_at)
        if age is None or age > CONTENT_STALE_TTL_SECONDS:
            return None
        payload = _loads_payload(row)
        if payload is None:
            return None
        category = {
            "news": "news:stock",
            "press_releases": "news:press-releases",
            "sec_filings": "news:sec-filings",
        }.get(content_type, f"news:{content_type}")
        record_cache_hit(category=category, symbol=normalized, cache_age_seconds=age)
        stale = age > CONTENT_TTL_SECONDS.get(content_type, 15 * 60)
        return paginate_ticker_content_payload(payload, page=page, limit=limit, stale=stale, cache_age_seconds=age)
    except Exception:
        logger.info("ticker_content db cache read failed type=%s symbol=%s", content_type, symbol, exc_info=True)
        return None
    finally:
        if own_session:
            db.close()


def db_ticker_content_cache_set(
    content_type: str,
    symbol: str,
    payload: dict[str, Any],
    *,
    window_key: str = "latest",
    cache_key: str | None = None,
    source: str = "fmp",
    session: Session | None = None,
) -> None:
    if not persistent_ticker_content_cache_enabled():
        return
    normalized = normalize_symbol(symbol)
    if not normalized:
        return
    items = payload.get("items")
    item_count = len(items) if isinstance(items, list) else int(payload.get("item_count") or 0)
    status = str(payload.get("status") or "")
    if status != "ok" or item_count <= 0:
        return
    own_session = session is None
    db = session or SessionLocal()
    now = datetime.now(timezone.utc)
    cache_key_value = cache_key or ticker_content_cache_key(content_type, normalized, window_key)
    durable_payload = {
        **payload,
        "items": items,
        "item_count": item_count,
        "status": "ok",
        "updated_at": payload.get("updated_at") or now.isoformat(timespec="seconds").replace("+00:00", "Z"),
    }
    try:
        payload_json = json.dumps(durable_payload, sort_keys=True, default=str)
        row = db.execute(
            select(TickerContentCache)
            .where(TickerContentCache.content_type == content_type)
            .where(func.upper(TickerContentCache.symbol) == normalized)
            .where(TickerContentCache.window_key == (window_key or "latest"))
            .limit(1)
        ).scalar_one_or_none()
        if row is None:
            db.add(
                TickerContentCache(
                    content_type=content_type,
                    symbol=normalized,
                    window_key=window_key or "latest",
                    cache_key=cache_key_value,
                    status="ok",
                    item_count=item_count,
                    payload_json=payload_json,
                    source=source,
                    fetched_at=now,
                )
            )
        else:
            row.cache_key = cache_key_value
            row.status = "ok"
            row.item_count = item_count
            row.payload_json = payload_json
            row.source = source
            row.fetched_at = now
            row.updated_at = now
        db.commit()
    except Exception:
        db.rollback()
        logger.info("ticker_content db cache write failed type=%s symbol=%s", content_type, symbol, exc_info=True)
    finally:
        if own_session:
            db.close()


def latest_ticker_content_cache_row(
    db: Session,
    content_type: str,
    symbol: str,
) -> TickerContentCache | None:
    normalized = normalize_symbol(symbol)
    if not normalized:
        return None
    return db.execute(
        select(TickerContentCache)
        .where(TickerContentCache.content_type == content_type)
        .where(func.upper(TickerContentCache.symbol) == normalized)
        .order_by(TickerContentCache.fetched_at.desc(), TickerContentCache.id.desc())
        .limit(1)
    ).scalar_one_or_none()


def ticker_content_cache_has_items(
    db: Session,
    content_type: str,
    symbol: str,
) -> bool:
    row = latest_ticker_content_cache_row(db, content_type, symbol)
    return bool(row is not None and int(row.item_count or 0) > 0)


def ticker_content_cache_summary(
    db: Session,
    content_type: str,
    symbol: str,
) -> dict[str, Any]:
    row = latest_ticker_content_cache_row(db, content_type, symbol)
    if row is None:
        return {
            "rows_found": 0,
            "item_count": 0,
            "latest_date": None,
            "top_items": [],
            "cache_key": None,
            "symbol": normalize_symbol(symbol),
        }
    payload = _loads_payload(row) or {}
    items = payload.get("items") if isinstance(payload.get("items"), list) else []
    date_keys = ("published_at", "filing_date", "accepted_date", "date")
    latest = next((item.get(key) for item in items for key in date_keys if isinstance(item, dict) and item.get(key)), None)
    top_items = []
    for item in items[:3]:
        if not isinstance(item, dict):
            continue
        top_items.append(
            {
                "title": item.get("title"),
                "form_type": item.get("form_type"),
                "date": item.get("published_at") or item.get("filing_date") or item.get("accepted_date"),
                "url": item.get("url"),
            }
        )
    return {
        "rows_found": int(row.item_count or 0),
        "item_count": int(row.item_count or 0),
        "latest_date": latest,
        "top_items": top_items,
        "cache_key": row.cache_key,
        "symbol": row.symbol,
        "window_key": row.window_key,
        "status": row.status,
        "fetched_at": row.fetched_at.isoformat() if row.fetched_at else None,
    }
