from __future__ import annotations

import json
import logging
import os
import time
from threading import Lock
from typing import Any

logger = logging.getLogger(__name__)

_TRUE_VALUES = {"1", "true", "yes", "on"}
_MEMORY: dict[str, tuple[float, Any]] = {}
_LOCK = Lock()
_MODE_LOGGED = False
_REDIS_CLIENT: Any | None = None


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in _TRUE_VALUES


def _redis_client() -> Any | None:
    global _REDIS_CLIENT
    if _REDIS_CLIENT is not None:
        return _REDIS_CLIENT
    url = os.getenv("REDIS_URL", "").strip()
    if not url or not _env_bool("CACHE_HOT_ENABLED", True):
        return None
    try:
        import redis  # type: ignore

        _REDIS_CLIENT = redis.Redis.from_url(url, socket_connect_timeout=1, socket_timeout=1)
        _REDIS_CLIENT.ping()
        return _REDIS_CLIENT
    except Exception:
        logger.info("cache_store redis unavailable; falling back", exc_info=True)
        _REDIS_CLIENT = None
        return None


def cache_mode() -> str:
    if _redis_client() is not None:
        return "redis"
    if _env_bool("CACHE_HOT_FALLBACK_MEMORY", True) and _env_bool("CACHE_HOT_ENABLED", True):
        return "memory"
    return "postgres_only"


def log_cache_store_mode_once() -> None:
    global _MODE_LOGGED
    if _MODE_LOGGED:
        return
    logger.info("cache_store mode=%s", cache_mode())
    os.environ["CACHE_STORE_MODE"] = cache_mode()
    _MODE_LOGGED = True


def hot_cache_get(key: str) -> Any | None:
    log_cache_store_mode_once()
    client = _redis_client()
    if client is not None:
        try:
            raw = client.get(key)
            return json.loads(raw) if raw else None
        except Exception:
            logger.info("cache_store redis_get_failed key=%s", key, exc_info=True)
    if cache_mode() != "memory":
        return None
    now = time.time()
    with _LOCK:
        cached = _MEMORY.get(key)
        if not cached:
            return None
        expires_at, value = cached
        if expires_at <= now:
            _MEMORY.pop(key, None)
            return None
        return value


def hot_cache_set(key: str, value: Any, ttl_seconds: int | None) -> None:
    if ttl_seconds is None or ttl_seconds <= 0:
        return
    log_cache_store_mode_once()
    client = _redis_client()
    if client is not None:
        try:
            client.setex(key, int(ttl_seconds), json.dumps(value, default=str))
            return
        except Exception:
            logger.info("cache_store redis_set_failed key=%s", key, exc_info=True)
    if cache_mode() != "memory":
        return
    with _LOCK:
        _MEMORY[key] = (time.time() + ttl_seconds, value)
