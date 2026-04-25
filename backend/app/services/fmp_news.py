from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timezone
from threading import Lock
from typing import Any, Literal

import requests

from app.clients.fmp import FMP_BASE_URL

logger = logging.getLogger(__name__)

NEWS_CACHE_TTL_SECONDS = 15 * 60
_NEWS_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_NEWS_CACHE_LOCK = Lock()

NewsStatus = Literal["ok", "empty", "unavailable", "disabled"]


class FMPNewsError(RuntimeError):
    """Raised when the upstream news provider fails."""


class FMPNewsPlanUnavailable(FMPNewsError):
    """Raised when the current FMP plan does not expose a news endpoint."""


def _api_key() -> str:
    key = os.getenv("FMP_API_KEY", "").strip()
    if not key:
        raise FMPNewsError("Missing FMP_API_KEY")
    return key


def clear_news_cache() -> None:
    with _NEWS_CACHE_LOCK:
        _NEWS_CACHE.clear()


def _cache_get(key: str) -> dict[str, Any] | None:
    now = time.time()
    with _NEWS_CACHE_LOCK:
        cached = _NEWS_CACHE.get(key)
        if not cached:
            return None
        expires_at, payload = cached
        if expires_at <= now:
            _NEWS_CACHE.pop(key, None)
            return None
        return payload


def _cache_set(key: str, payload: dict[str, Any], *, ttl_seconds: int = NEWS_CACHE_TTL_SECONDS) -> dict[str, Any]:
    with _NEWS_CACHE_LOCK:
        _NEWS_CACHE[key] = (time.time() + ttl_seconds, payload)
    return payload


def _fmp_get_rows(
    endpoint: str,
    *,
    params: dict[str, Any] | None = None,
    timeout_s: int = 30,
) -> list[dict[str, Any]]:
    request_params = {"apikey": _api_key()}
    for key, value in (params or {}).items():
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        request_params[key] = value

    url = endpoint if endpoint.startswith("http") else f"{FMP_BASE_URL}/{endpoint}"
    try:
        response = requests.get(url, params=request_params, timeout=timeout_s)
    except requests.RequestException as exc:
        raise FMPNewsError(f"FMP news request failed: {exc}") from exc

    if response.status_code == 402:
        raise FMPNewsPlanUnavailable("News is unavailable under the current data plan.")
    if response.status_code in {401, 403}:
        raise FMPNewsError(f"FMP news auth failed ({response.status_code}): {response.text[:200]}")
    if response.status_code == 429:
        raise FMPNewsError("FMP news rate-limited (429)")
    if response.status_code in {400, 404}:
        return []

    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        raise FMPNewsError(f"FMP news error ({response.status_code}): {response.text[:200]}") from exc

    data = response.json()
    if isinstance(data, list):
        return [row for row in data if isinstance(row, dict)]
    if isinstance(data, dict):
        rows = data.get("data")
        if isinstance(rows, list):
            return [row for row in rows if isinstance(row, dict)]
    return []


def _trimmed(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned or None


def _normalize_symbols(value: Any) -> list[str]:
    candidates: list[str] = []
    if isinstance(value, str):
        candidates = [chunk.strip().upper() for chunk in value.replace("|", ",").split(",")]
    elif isinstance(value, list):
        candidates = [str(chunk).strip().upper() for chunk in value]
    elif isinstance(value, tuple):
        candidates = [str(chunk).strip().upper() for chunk in value]
    return [symbol for symbol in candidates if symbol]


def _normalize_published_at(value: Any) -> str | None:
    raw = _trimmed(value)
    if not raw:
        return None
    cleaned = raw.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(cleaned)
    except ValueError:
        return raw
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat()


def _normalize_article(row: dict[str, Any], *, symbol_override: str | None = None, source_type: str = "news") -> dict[str, Any] | None:
    title = _trimmed(row.get("title")) or _trimmed(row.get("headline"))
    url = _trimmed(row.get("url")) or _trimmed(row.get("link"))
    if not title or not url:
        return None

    related_symbols = _normalize_symbols(
        row.get("symbol")
        or row.get("symbols")
        or row.get("stockSymbol")
        or row.get("stockSymbols")
        or row.get("ticker")
        or row.get("tickers")
    )
    if symbol_override and symbol_override not in related_symbols:
        related_symbols = [symbol_override, *related_symbols]

    return {
        "symbol": symbol_override or (related_symbols[0] if related_symbols else None),
        "related_symbols": related_symbols,
        "title": title,
        "site": _trimmed(row.get("site")) or _trimmed(row.get("source")) or _trimmed(row.get("publisher")) or "Unknown",
        "published_at": _normalize_published_at(
            row.get("publishedDate") or row.get("publishedAt") or row.get("date")
        ),
        "url": url,
        "image_url": _trimmed(row.get("image")) or _trimmed(row.get("image_url")) or _trimmed(row.get("imageUrl")),
        "summary": _trimmed(row.get("text")) or _trimmed(row.get("summary")) or _trimmed(row.get("snippet")),
        "source": "fmp",
        "source_type": source_type,
    }


def _dedupe_articles(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_url: dict[str, dict[str, Any]] = {}
    for item in items:
        url = _trimmed(item.get("url"))
        if not url:
            continue
        existing = by_url.get(url)
        if existing is None:
            by_url[url] = item
            continue
        existing_symbols = existing.get("related_symbols") or []
        merged_symbols = list(dict.fromkeys([*existing_symbols, *(item.get("related_symbols") or [])]))
        if merged_symbols:
            existing["related_symbols"] = merged_symbols
        if not existing.get("symbol") and item.get("symbol"):
            existing["symbol"] = item["symbol"]

    def sort_key(item: dict[str, Any]) -> tuple[int, str]:
        published_at = _trimmed(item.get("published_at")) or ""
        return (0 if published_at else 1, published_at)

    return sorted(by_url.values(), key=sort_key, reverse=True)


def _empty_payload(*, status: NewsStatus, message: str | None = None) -> dict[str, Any]:
    return {"items": [], "status": status, "message": message}


def _fetch_market_dataset() -> dict[str, Any]:
    cache_key = "market"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    items: list[dict[str, Any]] = []
    try:
        general_rows = _fmp_get_rows("news/general-latest", params={"page": 0, "limit": 100})
        items.extend(filter(None, (_normalize_article(row, source_type="market_news") for row in general_rows)))
    except FMPNewsPlanUnavailable:
        payload = _empty_payload(
            status="unavailable",
            message="News is unavailable under the current data plan.",
        )
        return _cache_set(cache_key, payload)
    except FMPNewsError:
        logger.exception("general market news fetch failed")
        payload = _empty_payload(status="unavailable", message="Market news is temporarily unavailable.")
        return _cache_set(cache_key, payload)

    try:
        press_rows = _fmp_get_rows("news/press-releases-latest", params={"page": 0, "limit": 50})
        items.extend(filter(None, (_normalize_article(row, source_type="press_release") for row in press_rows)))
    except FMPNewsPlanUnavailable:
        logger.info("press releases unavailable under current FMP plan")
    except FMPNewsError:
        logger.exception("press releases fetch failed")

    deduped = _dedupe_articles(items)
    payload = {
        "items": deduped,
        "status": "ok" if deduped else "empty",
        "message": None if deduped else "No recent market news found.",
    }
    return _cache_set(cache_key, payload)


def _fetch_ticker_dataset(symbols: list[str]) -> dict[str, Any]:
    normalized_symbols = sorted({symbol.strip().upper() for symbol in symbols if symbol and symbol.strip()})
    cache_key = f"ticker::{','.join(normalized_symbols)}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    if not normalized_symbols:
        payload = _empty_payload(status="empty", message="No ticker news requested.")
        return _cache_set(cache_key, payload)

    joined = ",".join(normalized_symbols)
    items: list[dict[str, Any]] = []
    endpoints = (
        ("news/stock", {"symbols": joined, "page": 0, "limit": 100}),
        ("https://financialmodelingprep.com/api/v3/stock_news", {"tickers": joined, "page": 0, "limit": 100}),
    )

    last_error: str | None = None
    for endpoint, params in endpoints:
        try:
            rows = _fmp_get_rows(endpoint, params=params)
            items = list(filter(None, (_normalize_article(row, source_type="stock_news") for row in rows)))
            break
        except FMPNewsPlanUnavailable:
            payload = _empty_payload(
                status="unavailable",
                message="News is unavailable under the current data plan.",
            )
            return _cache_set(cache_key, payload)
        except FMPNewsError as exc:
            last_error = str(exc)
            continue

    if not items and last_error:
        logger.warning("ticker news fetch failed symbols=%s error=%s", joined, last_error)
        payload = _empty_payload(status="unavailable", message="Ticker news is temporarily unavailable.")
        return _cache_set(cache_key, payload)

    deduped = _dedupe_articles(items)
    payload = {
        "items": deduped,
        "status": "ok" if deduped else "empty",
        "message": None if deduped else "No recent news found for this ticker.",
    }
    return _cache_set(cache_key, payload)


def slice_news_payload(
    payload: dict[str, Any],
    *,
    limit: int,
    offset: int = 0,
) -> dict[str, Any]:
    items = list(payload.get("items") or [])
    bounded_offset = max(int(offset or 0), 0)
    bounded_limit = max(int(limit or 0), 1)
    sliced = items[bounded_offset: bounded_offset + bounded_limit]
    status = payload.get("status") or ("ok" if sliced else "empty")
    message = payload.get("message")
    if not sliced and status == "ok":
        status = "empty"
        message = "No articles found for this view."
    return {
        "items": sliced,
        "status": status,
        "message": message,
        "total": len(items),
        "offset": bounded_offset,
        "limit": bounded_limit,
    }


def get_market_news(*, limit: int = 25, offset: int = 0) -> dict[str, Any]:
    return slice_news_payload(_fetch_market_dataset(), limit=limit, offset=offset)


def get_ticker_news(*, symbols: list[str], limit: int = 25, offset: int = 0) -> dict[str, Any]:
    return slice_news_payload(_fetch_ticker_dataset(symbols), limit=limit, offset=offset)
