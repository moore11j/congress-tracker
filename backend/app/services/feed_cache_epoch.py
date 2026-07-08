from __future__ import annotations

import logging
import os
import threading
import time
from typing import Any

from sqlalchemy.orm import Session

from app.db import SessionLocal
from app.models import AppSetting

logger = logging.getLogger(__name__)

FEED_EVENTS_EPOCH_KEY = "feed.events.epoch"

_CACHE_LOCK = threading.Lock()
_CACHED_EPOCH = "0"
_CACHED_UNTIL = 0.0


def _read_ttl_seconds() -> float:
    try:
        return max(0.0, min(30.0, float(os.getenv("FEED_EVENTS_EPOCH_READ_TTL_SECONDS", "1") or 1)))
    except ValueError:
        return 1.0


def _clean_epoch(value: object) -> str:
    text = str(value or "").strip()
    return text if text else "0"


def _set_cached_epoch(value: str) -> None:
    global _CACHED_EPOCH, _CACHED_UNTIL
    with _CACHE_LOCK:
        _CACHED_EPOCH = _clean_epoch(value)
        _CACHED_UNTIL = time.monotonic() + _read_ttl_seconds()


def clear_feed_events_epoch_cache() -> None:
    global _CACHED_EPOCH, _CACHED_UNTIL
    with _CACHE_LOCK:
        _CACHED_EPOCH = "0"
        _CACHED_UNTIL = 0.0


def current_feed_events_epoch(db: Session | None = None) -> str:
    if db is None:
        now = time.monotonic()
        with _CACHE_LOCK:
            if _CACHED_UNTIL > now:
                return _CACHED_EPOCH

    own_session = db is None
    session = db or SessionLocal()
    try:
        row = session.get(AppSetting, FEED_EVENTS_EPOCH_KEY)
        value = _clean_epoch(row.value if row is not None else "0")
    except Exception as exc:
        logger.debug("feed_events_epoch_read_failed error=%s", exc.__class__.__name__)
        value = "0"
    finally:
        if own_session:
            session.close()

    if own_session:
        _set_cached_epoch(value)
    return value


def bump_feed_events_epoch(*, reason: str, db: Session | None = None) -> dict[str, Any]:
    own_session = db is None
    session = db or SessionLocal()
    previous = "0"
    try:
        row = session.get(AppSetting, FEED_EVENTS_EPOCH_KEY)
        if row is not None:
            previous = _clean_epoch(row.value)
        try:
            previous_int = int(previous)
        except ValueError:
            previous_int = 0
        next_value = str(max(previous_int + 1, time.time_ns()))
        if row is None:
            session.add(AppSetting(key=FEED_EVENTS_EPOCH_KEY, value=next_value))
        else:
            row.value = next_value
        if own_session:
            session.commit()
        else:
            session.flush()
        _set_cached_epoch(next_value)
        logger.info("feed_events_epoch_bumped reason=%s previous=%s epoch=%s", reason, previous, next_value)
        return {"status": "ok", "key": FEED_EVENTS_EPOCH_KEY, "previous": previous, "epoch": next_value, "reason": reason}
    except Exception:
        if own_session:
            session.rollback()
        raise
    finally:
        if own_session:
            session.close()


def try_bump_feed_events_epoch(*, reason: str) -> dict[str, Any]:
    try:
        return bump_feed_events_epoch(reason=reason)
    except Exception as exc:
        logger.exception("feed_events_epoch_bump_failed reason=%s", reason)
        return {"status": "failed", "key": FEED_EVENTS_EPOCH_KEY, "reason": reason, "error": exc.__class__.__name__}
