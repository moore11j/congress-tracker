from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from time import perf_counter
from typing import Any

from sqlalchemy.orm import Session

from app.models import InsightsSnapshot
from app.services.fmp_market_snapshot import get_macro_snapshot

logger = logging.getLogger(__name__)

INSIGHTS_SNAPSHOT_KIND = "macro-snapshot"
INSIGHTS_SNAPSHOT_TTL = timedelta(minutes=5)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _aware(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _empty_payload(*, status: str = "unavailable") -> dict[str, Any]:
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


def refresh_insights_snapshot(db: Session, kind: str = INSIGHTS_SNAPSHOT_KIND) -> dict[str, Any]:
    if kind not in {INSIGHTS_SNAPSHOT_KIND, "all"}:
        raise ValueError(f"Unsupported insights snapshot kind: {kind}")
    started_at = perf_counter()
    try:
        payload = get_macro_snapshot()
        if not isinstance(payload, dict):
            payload = _empty_payload(status="unavailable")
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
        return _decorate(_empty_payload(status="unavailable"), None, stale=True, cache_hit=False)


def get_insights_snapshot(db: Session, *, kind: str = INSIGHTS_SNAPSHOT_KIND) -> dict[str, Any]:
    started_at = perf_counter()
    row = _load_row(db, kind)
    cache_hit = row is not None
    if row is None:
        payload = refresh_insights_snapshot(db, kind)
        logger.info(
            "insights_snapshot_timing kind=%s duration_ms=%.1f cache_hit=false stale=%s",
            kind,
            (perf_counter() - started_at) * 1000,
            payload.get("stale"),
        )
        return payload

    fetched_at = _aware(row.fetched_at)
    stale = fetched_at is None or (_utcnow() - fetched_at) > INSIGHTS_SNAPSHOT_TTL
    payload = _decorate(_loads_payload(row), row, stale=stale, cache_hit=cache_hit)
    logger.info(
        "insights_snapshot_timing kind=%s duration_ms=%.1f cache_hit=true stale=%s",
        kind,
        (perf_counter() - started_at) * 1000,
        stale,
    )
    return payload
