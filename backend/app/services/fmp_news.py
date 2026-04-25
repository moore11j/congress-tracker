from __future__ import annotations

import logging
import os
import time
from datetime import date, datetime, timedelta, timezone
from threading import Lock
from typing import Any, Literal

import requests

from app.clients.fmp import FMP_BASE_URL

logger = logging.getLogger(__name__)

NewsStatus = Literal["ok", "empty", "unavailable"]
GENERAL_NEWS_TTL_SECONDS = 15 * 60
STOCK_NEWS_TTL_SECONDS = 15 * 60
PRESS_RELEASES_TTL_SECONDS = 30 * 60
SEC_FILINGS_TTL_SECONDS = 60 * 60

_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_CACHE_LOCK = Lock()
_UNAVAILABLE_MESSAGE = "News data is unavailable from the current provider."


class FMPNewsError(RuntimeError):
    """Raised when the upstream provider fails."""


class FMPNewsUnavailable(FMPNewsError):
    """Raised when the provider cannot serve the current request."""


def _api_key() -> str:
    key = os.getenv("FMP_API_KEY", "").strip()
    if not key:
        raise FMPNewsUnavailable(_UNAVAILABLE_MESSAGE)
    return key


def clear_news_cache() -> None:
    with _CACHE_LOCK:
        _CACHE.clear()


def _cache_get(key: str) -> dict[str, Any] | None:
    now = time.time()
    with _CACHE_LOCK:
        cached = _CACHE.get(key)
        if not cached:
            return None
        expires_at, payload = cached
        if expires_at <= now:
            _CACHE.pop(key, None)
            return None
        return payload


def _cache_set(key: str, payload: dict[str, Any], *, ttl_seconds: int) -> dict[str, Any]:
    with _CACHE_LOCK:
        _CACHE[key] = (time.time() + ttl_seconds, payload)
    return payload


def _cache_key(prefix: str, params: dict[str, Any]) -> str:
    ordered = "&".join(
        f"{key}={params[key]}"
        for key in sorted(params)
        if params[key] is not None and str(params[key]).strip()
    )
    return f"{prefix}?{ordered}"


def _trimmed(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned or None


def _normalize_symbol(value: Any) -> str | None:
    symbol = _trimmed(value)
    return symbol.upper() if symbol else None


def _normalize_symbols(value: Any) -> list[str]:
    symbols: list[str] = []
    if isinstance(value, str):
        symbols = [chunk.strip().upper() for chunk in value.replace("|", ",").split(",")]
    elif isinstance(value, list):
        symbols = [str(chunk).strip().upper() for chunk in value]
    elif isinstance(value, tuple):
        symbols = [str(chunk).strip().upper() for chunk in value]
    return [symbol for symbol in symbols if symbol]


def _normalize_timestamp(value: Any) -> str | None:
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


def _normalize_dateish(value: Any) -> str | None:
    raw = _trimmed(value)
    if not raw:
        return None
    if len(raw) == 10 or ("T" not in raw and "+" not in raw and raw.count(":") >= 1):
        return raw
    return _normalize_timestamp(raw) or raw


def _fmp_get_rows(endpoint: str, *, params: dict[str, Any], timeout_s: int = 30) -> list[dict[str, Any]]:
    request_params = {"apikey": _api_key()}
    for key, value in params.items():
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        request_params[key] = value

    try:
        response = requests.get(f"{FMP_BASE_URL}/{endpoint}", params=request_params, timeout=timeout_s)
    except requests.RequestException as exc:
        raise FMPNewsUnavailable(_UNAVAILABLE_MESSAGE) from exc

    if response.status_code in {402, 403, 429}:
        raise FMPNewsUnavailable(_UNAVAILABLE_MESSAGE)
    if response.status_code in {400, 404}:
        return []

    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        raise FMPNewsUnavailable(_UNAVAILABLE_MESSAGE) from exc

    data = response.json()
    if isinstance(data, list):
        return [row for row in data if isinstance(row, dict)]
    if isinstance(data, dict):
        rows = data.get("data")
        if isinstance(rows, list):
            return [row for row in rows if isinstance(row, dict)]
    return []


def _dedupe_by_url(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen_urls: set[str] = set()
    seen_fallbacks: set[str] = set()
    for item in items:
        url = _trimmed(item.get("url"))
        if url:
            if url in seen_urls:
                continue
            seen_urls.add(url)
            deduped.append(item)
            continue
        fallback = "|".join(
            [
                str(item.get("symbol") or ""),
                str(item.get("title") or ""),
                str(item.get("published_at") or item.get("filing_date") or ""),
                str(item.get("form_type") or ""),
            ]
        )
        if fallback in seen_fallbacks:
            continue
        seen_fallbacks.add(fallback)
        deduped.append(item)
    return deduped


def _sort_key(item: dict[str, Any]) -> tuple[int, str]:
    timestamp = _trimmed(item.get("published_at")) or _trimmed(item.get("filing_date")) or ""
    return (0 if timestamp else 1, timestamp)


def _paginate_items(items: list[dict[str, Any]], *, page: int, limit: int) -> tuple[list[dict[str, Any]], bool]:
    offset = page * limit
    window = items[offset: offset + limit + 1]
    has_next = len(window) > limit
    return window[:limit], has_next


def _unavailable_payload(*, page: int, limit: int) -> dict[str, Any]:
    return {
        "items": [],
        "status": "unavailable",
        "message": _UNAVAILABLE_MESSAGE,
        "page": page,
        "limit": limit,
        "has_next": False,
    }


def _normalize_general_article(row: dict[str, Any]) -> dict[str, Any] | None:
    title = _trimmed(row.get("title")) or _trimmed(row.get("headline"))
    url = _trimmed(row.get("url")) or _trimmed(row.get("link"))
    if not title or not url:
        return None
    return {
        "title": title,
        "site": _trimmed(row.get("site")) or _trimmed(row.get("publisher")) or "Unknown",
        "published_at": _normalize_timestamp(row.get("publishedDate") or row.get("date") or row.get("publishedAt")),
        "url": url,
        "image_url": _trimmed(row.get("image")) or _trimmed(row.get("image_url")) or _trimmed(row.get("imageUrl")),
        "summary": _trimmed(row.get("text")) or _trimmed(row.get("summary")) or _trimmed(row.get("snippet")),
        "symbol": _normalize_symbol(row.get("symbol")),
        "source": "fmp_general_news",
    }


def _normalize_stock_article(row: dict[str, Any], *, symbol: str) -> dict[str, Any] | None:
    title = _trimmed(row.get("title")) or _trimmed(row.get("headline"))
    url = _trimmed(row.get("url")) or _trimmed(row.get("link"))
    if not title or not url:
        return None
    related = _normalize_symbols(
        row.get("symbol")
        or row.get("symbols")
        or row.get("stockSymbol")
        or row.get("stockSymbols")
        or row.get("ticker")
        or row.get("tickers")
    )
    if related and symbol not in related:
        return None
    return {
        "symbol": symbol,
        "title": title,
        "site": _trimmed(row.get("site")) or _trimmed(row.get("publisher")) or "Unknown",
        "published_at": _normalize_timestamp(row.get("publishedDate") or row.get("date") or row.get("publishedAt")),
        "url": url,
        "image_url": _trimmed(row.get("image")) or _trimmed(row.get("image_url")) or _trimmed(row.get("imageUrl")),
        "summary": _trimmed(row.get("text")) or _trimmed(row.get("summary")) or _trimmed(row.get("snippet")),
        "source": "fmp_stock_news",
    }


def _normalize_press_release(row: dict[str, Any], *, symbol: str) -> dict[str, Any] | None:
    title = _trimmed(row.get("title")) or _trimmed(row.get("headline"))
    url = _trimmed(row.get("url")) or _trimmed(row.get("link"))
    if not title and not url:
        return None
    related = _normalize_symbols(
        row.get("symbol")
        or row.get("symbols")
        or row.get("stockSymbol")
        or row.get("stockSymbols")
        or row.get("ticker")
        or row.get("tickers")
    )
    if related and symbol not in related:
        return None
    return {
        "symbol": symbol,
        "title": title or "Press release",
        "site": _trimmed(row.get("site")) or _trimmed(row.get("publisher")),
        "published_at": _normalize_timestamp(row.get("publishedDate") or row.get("date") or row.get("publishedAt")),
        "url": url,
        "summary": _trimmed(row.get("text")) or _trimmed(row.get("summary")) or _trimmed(row.get("snippet")),
        "source": "fmp_press_release",
    }


def _normalize_sec_filing(row: dict[str, Any], *, symbol: str) -> dict[str, Any] | None:
    normalized = {
        "symbol": symbol,
        "filing_date": _normalize_dateish(row.get("fillingDate") or row.get("filingDate") or row.get("date")),
        "accepted_date": _normalize_dateish(row.get("acceptedDate")),
        "form_type": _trimmed(row.get("formType")) or _trimmed(row.get("type")) or _trimmed(row.get("form")) or "",
        "title": _trimmed(row.get("title")) or _trimmed(row.get("companyName")) or _trimmed(row.get("company")),
        "url": _trimmed(row.get("finalLink")) or _trimmed(row.get("link")) or _trimmed(row.get("url")),
        "source": "fmp_sec_filings",
    }
    if not normalized["filing_date"] and not normalized["accepted_date"]:
        return None
    if not normalized["form_type"]:
        normalized["form_type"] = "Filing"
    return normalized


def get_general_news(*, page: int = 0, limit: int = 20) -> dict[str, Any]:
    bounded_page = max(int(page or 0), 0)
    bounded_limit = max(1, min(int(limit or 20), 50))
    cache_key = _cache_key("general", {"page": bounded_page, "limit": bounded_limit})
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    try:
        rows = _fmp_get_rows(
            "news/general-latest",
            params={"page": bounded_page, "limit": bounded_limit},
        )
    except FMPNewsUnavailable:
        return _cache_set(cache_key, _unavailable_payload(page=bounded_page, limit=bounded_limit), ttl_seconds=GENERAL_NEWS_TTL_SECONDS)

    items = _dedupe_by_url(list(filter(None, (_normalize_general_article(row) for row in rows))))
    items.sort(key=_sort_key, reverse=True)
    sliced = items[:bounded_limit]
    payload = {
        "items": sliced,
        "page": bounded_page,
        "limit": bounded_limit,
        "has_next": len(items) == bounded_limit,
    }
    return _cache_set(cache_key, payload, ttl_seconds=GENERAL_NEWS_TTL_SECONDS)


def _collect_symbol_filtered_rows(
    *,
    endpoint: str,
    symbol: str,
    page: int,
    limit: int,
    normalizer: Any,
    ttl_seconds: int,
    request_limit: int = 50,
    extra_params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    bounded_page = max(int(page or 0), 0)
    bounded_limit = max(1, min(int(limit or 20), 50 if endpoint != "sec-filings-search/symbol" else 100))
    cache_key = _cache_key(
        endpoint,
        {"symbol": symbol, "page": bounded_page, "limit": bounded_limit, **(extra_params or {})},
    )
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    target_count = (bounded_page + 1) * bounded_limit + 1
    collected: list[dict[str, Any]] = []
    provider_page = 0
    max_provider_pages = 8

    try:
        while provider_page < max_provider_pages and len(collected) < target_count:
            rows = _fmp_get_rows(
                endpoint,
                params={
                    "page": provider_page,
                    "limit": request_limit,
                    **(extra_params or {}),
                    **({"symbol": symbol} if endpoint == "sec-filings-search/symbol" else {}),
                },
            )
            if not rows:
                break
            normalized = list(filter(None, (normalizer(row, symbol=symbol) for row in rows)))
            if normalized:
                collected.extend(normalized)
                collected = _dedupe_by_url(collected)
                collected.sort(key=_sort_key, reverse=True)
            if len(rows) < request_limit:
                break
            provider_page += 1
    except FMPNewsUnavailable:
        return _cache_set(cache_key, _unavailable_payload(page=bounded_page, limit=bounded_limit), ttl_seconds=ttl_seconds)

    sliced, has_next = _paginate_items(collected, page=bounded_page, limit=bounded_limit)
    payload = {
        "items": sliced,
        "page": bounded_page,
        "limit": bounded_limit,
        "has_next": has_next,
    }
    return _cache_set(cache_key, payload, ttl_seconds=ttl_seconds)


def get_stock_news(*, symbol: str, page: int = 0, limit: int = 20) -> dict[str, Any]:
    normalized_symbol = _normalize_symbol(symbol)
    if not normalized_symbol:
        return {"items": [], "page": 0, "limit": max(1, min(int(limit or 20), 50)), "has_next": False}
    return _collect_symbol_filtered_rows(
        endpoint="news/stock-latest",
        symbol=normalized_symbol,
        page=page,
        limit=limit,
        normalizer=_normalize_stock_article,
        ttl_seconds=STOCK_NEWS_TTL_SECONDS,
        request_limit=50,
    )


def get_press_releases(*, symbol: str, page: int = 0, limit: int = 20) -> dict[str, Any]:
    normalized_symbol = _normalize_symbol(symbol)
    if not normalized_symbol:
        return {"items": [], "page": 0, "limit": max(1, min(int(limit or 20), 50)), "has_next": False}
    return _collect_symbol_filtered_rows(
        endpoint="news/press-releases-latest",
        symbol=normalized_symbol,
        page=page,
        limit=limit,
        normalizer=_normalize_press_release,
        ttl_seconds=PRESS_RELEASES_TTL_SECONDS,
        request_limit=50,
    )


def get_sec_filings(
    *,
    symbol: str,
    from_date: str | None = None,
    to_date: str | None = None,
    page: int = 0,
    limit: int = 100,
) -> dict[str, Any]:
    normalized_symbol = _normalize_symbol(symbol)
    bounded_page = max(int(page or 0), 0)
    bounded_limit = max(1, min(int(limit or 100), 100))
    if not normalized_symbol:
        return {"items": [], "page": bounded_page, "limit": bounded_limit, "has_next": False}

    today = date.today()
    default_from = today - timedelta(days=7)
    from_value = from_date or default_from.isoformat()
    to_value = to_date or today.isoformat()
    cache_key = _cache_key(
        "sec-filings",
        {"symbol": normalized_symbol, "from": from_value, "to": to_value, "page": bounded_page, "limit": bounded_limit},
    )
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    try:
        rows = _fmp_get_rows(
            "sec-filings-search/symbol",
            params={
                "symbol": normalized_symbol,
                "from": from_value,
                "to": to_value,
                "page": bounded_page,
                "limit": bounded_limit + 1,
            },
        )
    except FMPNewsUnavailable:
        payload = _unavailable_payload(page=bounded_page, limit=bounded_limit)
        return _cache_set(cache_key, payload, ttl_seconds=SEC_FILINGS_TTL_SECONDS)

    items = _dedupe_by_url(list(filter(None, (_normalize_sec_filing(row, symbol=normalized_symbol) for row in rows))))
    items.sort(key=_sort_key, reverse=True)
    payload = {
        "items": items[:bounded_limit],
        "page": bounded_page,
        "limit": bounded_limit,
        "has_next": len(items) > bounded_limit,
    }
    return _cache_set(cache_key, payload, ttl_seconds=SEC_FILINGS_TTL_SECONDS)
