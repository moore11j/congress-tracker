from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from time import perf_counter
from typing import Any

from sqlalchemy.orm import Session

from app.models import InsightsSnapshot
from app.services.fmp_market_snapshot import get_macro_snapshot
from app.services.fmp_news import get_general_news

logger = logging.getLogger(__name__)

INSIGHTS_SNAPSHOT_KIND = "macro-snapshot"
INSIGHTS_HEADLINES_KIND = "market-headlines"
INSIGHTS_SNAPSHOT_TTL = timedelta(minutes=5)
INSIGHTS_HEADLINES_TTL = timedelta(minutes=15)
HEADLINES_WARMING_MESSAGE = "Market headlines are warming. Check back shortly."


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _aware(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _empty_snapshot_payload(*, status: str = "warming") -> dict[str, Any]:
    now = _utcnow().isoformat()
    return {
        "world_indexes": [],
        "indexes": [],
        "treasury": [],
        "economics": [],
        "commodities": [],
        "currencies": [],
        "crypto": [],
        "sector_performance": [],
        "status": status,
        "generated_at": now,
    }


def _empty_headlines_payload(*, page: int = 0, limit: int = 20, status: str = "warming") -> dict[str, Any]:
    return {
        "items": [],
        "status": status,
        "message": HEADLINES_WARMING_MESSAGE,
        "page": page,
        "limit": limit,
        "has_next": False,
    }


def _decorate(payload: dict[str, Any], row: InsightsSnapshot | None, *, stale: bool, cache_hit: bool) -> dict[str, Any]:
    fetched_at = _aware(row.fetched_at) if row else None
    source = row.source if row else "fmp"
    as_of = fetched_at.isoformat() if fetched_at else payload.get("generated_at") or _utcnow().isoformat()
    return {
        **payload,
        "as_of": as_of,
        "stale": stale,
        "source": source,
        "category": INSIGHTS_SNAPSHOT_KIND,
        "cache_hit": cache_hit,
    }


def _load_row(db: Session, kind: str = INSIGHTS_SNAPSHOT_KIND) -> InsightsSnapshot | None:
    return db.get(InsightsSnapshot, kind)


def _loads_payload(row: InsightsSnapshot) -> dict[str, Any]:
    try:
        payload = json.loads(row.payload_json or "{}")
    except Exception:
        payload = {}
    return payload if isinstance(payload, dict) else {}


def _store_payload(db: Session, payload: dict[str, Any], *, kind: str = INSIGHTS_SNAPSHOT_KIND, source: str = "fmp") -> InsightsSnapshot:
    now = _utcnow()
    row = _load_row(db, kind)
    if row is None:
        row = InsightsSnapshot(kind=kind, payload_json=json.dumps(payload), source=source, fetched_at=now)
        db.add(row)
    else:
        row.payload_json = json.dumps(payload)
        row.source = source
        row.fetched_at = now
        row.updated_at = now
    db.commit()
    db.refresh(row)
    return row


def _paginate_headlines_payload(payload: dict[str, Any], *, page: int, limit: int, row: InsightsSnapshot | None = None, stale: bool = False, cache_hit: bool = True) -> dict[str, Any]:
    all_items = payload.get("items")
    items = all_items if isinstance(all_items, list) else []
    offset = page * limit
    window = items[offset : offset + limit + 1]
    page_payload = {
        **payload,
        "items": window[:limit],
        "page": page,
        "limit": limit,
        "has_next": len(window) > limit,
        "status": "ok" if window[:limit] else "empty",
    }
    if not window[:limit]:
        page_payload["message"] = "No recent market news found."
    return _decorate(page_payload, row, stale=stale, cache_hit=cache_hit)


def refresh_insights_headlines(db: Session, *, limit: int = 50) -> dict[str, Any]:
    try:
        payload = get_general_news(page=0, limit=limit)
        items = payload.get("items") if isinstance(payload, dict) else []
        if not isinstance(items, list) or not items:
            raise RuntimeError("empty headlines payload")
        durable_payload = {
            "items": items,
            "status": "ok",
            "page": 0,
            "limit": len(items),
            "has_next": bool(payload.get("has_next")),
        }
        row = _store_payload(db, durable_payload, kind=INSIGHTS_HEADLINES_KIND, source="fmp")
        logger.info("insights_headlines_refresh_timing kind=%s status=ok count=%s", INSIGHTS_HEADLINES_KIND, len(items))
        return _decorate(durable_payload, row, stale=False, cache_hit=False)
    except Exception:
        logger.exception("insights_headlines_refresh_failed kind=%s", INSIGHTS_HEADLINES_KIND)
        row = _load_row(db, INSIGHTS_HEADLINES_KIND)
        if row is not None:
            return _decorate(_loads_payload(row), row, stale=True, cache_hit=True)
        return _decorate(_empty_headlines_payload(limit=limit), None, stale=True, cache_hit=False)


def refresh_insights_snapshot(db: Session, kind: str = INSIGHTS_SNAPSHOT_KIND) -> dict[str, Any]:
    if kind not in {INSIGHTS_SNAPSHOT_KIND, INSIGHTS_HEADLINES_KIND, "all"}:
        raise ValueError(f"Unsupported insights snapshot kind: {kind}")
    if kind == INSIGHTS_HEADLINES_KIND:
        return refresh_insights_headlines(db)
    if kind == "all":
        snapshot = refresh_insights_snapshot(db, INSIGHTS_SNAPSHOT_KIND)
        refresh_insights_headlines(db)
        return snapshot
    started_at = perf_counter()
    try:
        payload = get_macro_snapshot()
        if not isinstance(payload, dict):
            payload = _empty_snapshot_payload(status="unavailable")
        row = _store_payload(db, payload, kind=INSIGHTS_SNAPSHOT_KIND, source="fmp")
        duration_ms = (perf_counter() - started_at) * 1000
        logger.info(
            "insights_snapshot_refresh_timing kind=%s duration_ms=%.1f status=%s",
            INSIGHTS_SNAPSHOT_KIND,
            duration_ms,
            payload.get("status"),
        )
        return _decorate(payload, row, stale=False, cache_hit=False)
    except Exception:
        duration_ms = (perf_counter() - started_at) * 1000
        logger.exception("insights_snapshot_refresh_failed kind=%s duration_ms=%.1f", INSIGHTS_SNAPSHOT_KIND, duration_ms)
        row = _load_row(db, INSIGHTS_SNAPSHOT_KIND)
        if row is not None:
            return _decorate(_loads_payload(row), row, stale=True, cache_hit=True)
        return _decorate(_empty_snapshot_payload(status="warming"), None, stale=True, cache_hit=False)


def get_insights_snapshot(db: Session, *, kind: str = INSIGHTS_SNAPSHOT_KIND) -> dict[str, Any]:
    started_at = perf_counter()
    row = _load_row(db, kind)
    cache_hit = row is not None
    if row is None:
        payload = _decorate(_empty_snapshot_payload(status="warming"), None, stale=True, cache_hit=False)
        logger.info(
            "insights_snapshot_timing kind=%s duration_ms=%.1f cache_hit=false stale=true",
            kind,
            (perf_counter() - started_at) * 1000,
        )
        return payload

    fetched_at = _aware(row.fetched_at)
    ttl = INSIGHTS_HEADLINES_TTL if kind == INSIGHTS_HEADLINES_KIND else INSIGHTS_SNAPSHOT_TTL
    stale = fetched_at is None or (_utcnow() - fetched_at) > ttl
    payload = _decorate(_loads_payload(row), row, stale=stale, cache_hit=cache_hit)
    logger.info(
        "insights_snapshot_timing kind=%s duration_ms=%.1f cache_hit=true stale=%s",
        kind,
        (perf_counter() - started_at) * 1000,
        stale,
    )
    return payload


def get_insights_headlines(db: Session, *, page: int = 0, limit: int = 20) -> dict[str, Any]:
    bounded_page = max(int(page or 0), 0)
    bounded_limit = max(1, min(int(limit or 20), 50))
    row = _load_row(db, INSIGHTS_HEADLINES_KIND)
    if row is None:
        return _decorate(
            _empty_headlines_payload(page=bounded_page, limit=bounded_limit),
            None,
            stale=True,
            cache_hit=False,
        )
    fetched_at = _aware(row.fetched_at)
    stale = fetched_at is None or (_utcnow() - fetched_at) > INSIGHTS_HEADLINES_TTL
    return _paginate_headlines_payload(
        _loads_payload(row),
        page=bounded_page,
        limit=bounded_limit,
        row=row,
        stale=stale,
        cache_hit=True,
    )
