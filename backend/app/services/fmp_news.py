from __future__ import annotations

import logging
import os
import time
from datetime import date, datetime, timedelta, timezone
from threading import Lock
from typing import Any, Callable, Literal

import requests

from app.clients.fmp import FMP_BASE_URL

logger = logging.getLogger(__name__)

NewsStatus = Literal["ok", "empty", "unavailable"]
GENERAL_NEWS_TTL_SECONDS = 15 * 60
STOCK_NEWS_TTL_SECONDS = 15 * 60
PRESS_RELEASES_TTL_SECONDS = 30 * 60
SEC_FILINGS_TTL_SECONDS = 60 * 60
PROVIDER_TIMEOUT_SECONDS = 8
SYMBOL_SCAN_MAX_PAGES = 2
SYMBOL_SCAN_MAX_ITEMS = 100
GENERAL_UNAVAILABLE_MESSAGE = "News data is unavailable from the current provider."
TICKER_CONTEXT_UNAVAILABLE_MESSAGE = "Ticker news is temporarily unavailable."
TICKER_NEWS_EMPTY_MESSAGE = "No recent news found for this ticker."
TICKER_NEWS_PLAN_MESSAGE = "Ticker news is unavailable under the current data plan."
TICKER_NEWS_RATE_LIMIT_MESSAGE = "Ticker news is temporarily rate-limited."
TICKER_PRESS_EMPTY_MESSAGE = "No recent press releases found for this ticker."
TICKER_PRESS_PLAN_MESSAGE = "Ticker press releases are unavailable under the current data plan."
TICKER_PRESS_RATE_LIMIT_MESSAGE = "Ticker press releases are temporarily rate-limited."
_BULLISH_KEYWORDS = (
    "beat",
    "beats",
    "raises",
    "raised",
    "upgrade",
    "upgraded",
    "growth",
    "record",
    "strong",
    "outperforms",
    "partnership",
    "approval",
    "expands",
    "launches",
    "buyback",
    "dividend increase",
)
_BEARISH_KEYWORDS = (
    "miss",
    "misses",
    "cuts",
    "cut",
    "downgrade",
    "downgraded",
    "lawsuit",
    "probe",
    "investigation",
    "recall",
    "weak",
    "declines",
    "falls",
    "warning",
    "lowers",
    "bankruptcy",
    "layoffs",
)

_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_CACHE_LOCK = Lock()


class FMPNewsError(RuntimeError):
    """Raised when the upstream provider fails."""


class FMPNewsUnavailable(FMPNewsError):
    """Raised when the provider cannot serve the current request."""


def _api_key() -> str:
    key = os.getenv("FMP_API_KEY", "").strip()
    if not key:
        raise FMPNewsUnavailable(GENERAL_UNAVAILABLE_MESSAGE)
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
    elif isinstance(value, (list, tuple)):
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


def _body_excerpt(value: Any, *, limit: int = 220) -> str:
    text = str(value or "").strip().replace("\n", " ")
    if len(text) <= limit:
        return text
    return f"{text[:limit]}..."


def _ticker_news_debug_log(
    *,
    symbol: str,
    status: Any,
    parsed_count: int,
    body_preview: Any,
    app_endpoint: str = "/api/tickers/{symbol}/news",
    fmp_path: str = "/stable/news/stock",
) -> None:
    logger.info(
        "ticker_news_debug app_endpoint=%s symbol=%s fmp_path=%s status=%s count=%s body_preview=%s",
        app_endpoint,
        symbol,
        fmp_path,
        status,
        parsed_count,
        _body_excerpt(body_preview, limit=300),
    )


def _ticker_press_debug_log(
    *,
    symbol: str,
    status: Any,
    parsed_count: int,
    body_preview: Any,
    app_endpoint: str = "/api/tickers/{symbol}/press-releases",
    fmp_path: str = "/stable/news/press-releases",
) -> None:
    logger.info(
        "ticker_press_debug app_endpoint=%s symbol=%s fmp_path=%s status=%s count=%s body_preview=%s",
        app_endpoint,
        symbol,
        fmp_path,
        status,
        parsed_count,
        _body_excerpt(body_preview, limit=300),
    )


def _log_ticker_context_error(*, endpoint: str, symbol: str | None, status: Any, detail: Any) -> None:
    logger.warning(
        "fmp_ticker_context_error endpoint=%s symbol=%s status=%s detail=%s",
        endpoint,
        symbol or "-",
        status,
        _body_excerpt(detail),
    )


def _request_rows(
    endpoint: str,
    *,
    params: dict[str, Any],
    timeout_s: int = PROVIDER_TIMEOUT_SECONDS,
    context_symbol: str | None = None,
) -> tuple[list[dict[str, Any]], bool]:
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
        _log_ticker_context_error(endpoint=endpoint, symbol=context_symbol, status="request_error", detail=exc)
        raise FMPNewsUnavailable(GENERAL_UNAVAILABLE_MESSAGE) from exc

    if response.status_code in {400, 404}:
        _log_ticker_context_error(
            endpoint=endpoint,
            symbol=context_symbol,
            status=response.status_code,
            detail=response.text,
        )
        return [], False
    if response.status_code in {402, 403, 429}:
        _log_ticker_context_error(
            endpoint=endpoint,
            symbol=context_symbol,
            status=response.status_code,
            detail=response.text,
        )
        raise FMPNewsUnavailable(GENERAL_UNAVAILABLE_MESSAGE)

    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        _log_ticker_context_error(
            endpoint=endpoint,
            symbol=context_symbol,
            status=response.status_code,
            detail=response.text or exc,
        )
        raise FMPNewsUnavailable(GENERAL_UNAVAILABLE_MESSAGE) from exc

    try:
        data = response.json()
    except ValueError as exc:
        _log_ticker_context_error(
            endpoint=endpoint,
            symbol=context_symbol,
            status=response.status_code,
            detail="invalid_json",
        )
        raise FMPNewsUnavailable(GENERAL_UNAVAILABLE_MESSAGE) from exc

    if isinstance(data, list):
        return [row for row in data if isinstance(row, dict)], True
    if isinstance(data, dict):
        rows = data.get("data")
        if isinstance(rows, list):
            return [row for row in rows if isinstance(row, dict)], True
    _log_ticker_context_error(
        endpoint=endpoint,
        symbol=context_symbol,
        status=response.status_code,
        detail=f"unexpected_payload_type={type(data).__name__}",
    )
    raise FMPNewsUnavailable(GENERAL_UNAVAILABLE_MESSAGE)


def _request_ticker_news_rows(*, symbol: str, page: int, limit: int) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    api_key = _api_key()
    endpoint = "news/stock"
    request_params = {
        "symbols": symbol,
        "page": page,
        "limit": limit,
        "apikey": api_key,
    }

    try:
        response = requests.get(f"{FMP_BASE_URL}/{endpoint}", params=request_params, timeout=PROVIDER_TIMEOUT_SECONDS)
    except requests.RequestException as exc:
        _ticker_news_debug_log(symbol=symbol, status="request_error", parsed_count=0, body_preview=str(exc))
        return [], _unavailable_payload(page=page, limit=limit, message=TICKER_CONTEXT_UNAVAILABLE_MESSAGE)

    raw_text = response.text

    if response.status_code == 200:
        try:
            data = response.json()
        except ValueError:
            _ticker_news_debug_log(symbol=symbol, status=200, parsed_count=0, body_preview="invalid_json")
            return [], _unavailable_payload(page=page, limit=limit, message=TICKER_CONTEXT_UNAVAILABLE_MESSAGE)

        rows: list[dict[str, Any]]
        if isinstance(data, list):
            rows = [row for row in data if isinstance(row, dict)]
        else:
            _ticker_news_debug_log(
                symbol=symbol,
                status=200,
                parsed_count=0,
                body_preview=raw_text or f"unexpected_payload_type={type(data).__name__}",
            )
            return [], _unavailable_payload(page=page, limit=limit, message=TICKER_CONTEXT_UNAVAILABLE_MESSAGE)

        _ticker_news_debug_log(symbol=symbol, status=200, parsed_count=len(rows), body_preview=raw_text)
        return rows, None

    if response.status_code in {401, 402, 403}:
        _ticker_news_debug_log(symbol=symbol, status=response.status_code, parsed_count=0, body_preview=raw_text)
        return [], _unavailable_payload(page=page, limit=limit, message=TICKER_NEWS_PLAN_MESSAGE)

    if response.status_code == 429:
        _ticker_news_debug_log(symbol=symbol, status=429, parsed_count=0, body_preview=raw_text)
        return [], _unavailable_payload(page=page, limit=limit, message=TICKER_NEWS_RATE_LIMIT_MESSAGE)

    _ticker_news_debug_log(symbol=symbol, status=response.status_code, parsed_count=0, body_preview=raw_text)
    return [], _unavailable_payload(page=page, limit=limit, message=TICKER_CONTEXT_UNAVAILABLE_MESSAGE)


def _request_ticker_press_rows(*, symbol: str, page: int, limit: int) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    api_key = _api_key()
    endpoint = "news/press-releases"
    request_params = {
        "symbols": symbol,
        "page": page,
        "limit": limit,
        "apikey": api_key,
    }

    try:
        response = requests.get(f"{FMP_BASE_URL}/{endpoint}", params=request_params, timeout=PROVIDER_TIMEOUT_SECONDS)
    except requests.RequestException as exc:
        _ticker_press_debug_log(symbol=symbol, status="request_error", parsed_count=0, body_preview=str(exc))
        return [], _unavailable_payload(page=page, limit=limit, message=TICKER_CONTEXT_UNAVAILABLE_MESSAGE)

    raw_text = response.text

    if response.status_code == 200:
        try:
            data = response.json()
        except ValueError:
            _ticker_press_debug_log(symbol=symbol, status=200, parsed_count=0, body_preview="invalid_json")
            return [], _unavailable_payload(page=page, limit=limit, message=TICKER_CONTEXT_UNAVAILABLE_MESSAGE)

        if isinstance(data, list):
            rows = [row for row in data if isinstance(row, dict)]
            _ticker_press_debug_log(symbol=symbol, status=200, parsed_count=len(rows), body_preview=raw_text)
            return rows, None

        _ticker_press_debug_log(
            symbol=symbol,
            status=200,
            parsed_count=0,
            body_preview=raw_text or f"unexpected_payload_type={type(data).__name__}",
        )
        return [], _unavailable_payload(page=page, limit=limit, message=TICKER_CONTEXT_UNAVAILABLE_MESSAGE)

    if response.status_code in {401, 402, 403}:
        _ticker_press_debug_log(symbol=symbol, status=response.status_code, parsed_count=0, body_preview=raw_text)
        return [], _unavailable_payload(page=page, limit=limit, message=TICKER_PRESS_PLAN_MESSAGE)

    if response.status_code == 429:
        _ticker_press_debug_log(symbol=symbol, status=429, parsed_count=0, body_preview=raw_text)
        return [], _unavailable_payload(page=page, limit=limit, message=TICKER_PRESS_RATE_LIMIT_MESSAGE)

    _ticker_press_debug_log(symbol=symbol, status=response.status_code, parsed_count=0, body_preview=raw_text)
    return [], _unavailable_payload(page=page, limit=limit, message=TICKER_CONTEXT_UNAVAILABLE_MESSAGE)


def _classify_market_read(*, title: str | None, summary: str | None) -> Literal["bullish", "bearish", "neutral"]:
    haystack = " ".join(part for part in [title, summary] if part).lower()
    bullish = any(keyword in haystack for keyword in _BULLISH_KEYWORDS)
    bearish = any(keyword in haystack for keyword in _BEARISH_KEYWORDS)
    if bullish and bearish:
        return "neutral"
    if bullish:
        return "bullish"
    if bearish:
        return "bearish"
    return "neutral"


def _mentions_symbol(row: dict[str, Any], symbol: str) -> bool:
    target = symbol.upper()
    fields = [
        _trimmed(row.get("title")),
        _trimmed(row.get("headline")),
        _trimmed(row.get("text")),
        _trimmed(row.get("summary")),
        _trimmed(row.get("snippet")),
        _trimmed(row.get("companyName")),
        _trimmed(row.get("company")),
    ]
    padded = " ".join(part for part in fields if part).upper()
    if not padded:
        return False
    return f" {target} " in f" {padded} "


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
    window = items[offset : offset + limit + 1]
    has_next = len(window) > limit
    return window[:limit], has_next


def _unavailable_payload(*, page: int, limit: int, message: str) -> dict[str, Any]:
    return {
        "items": [],
        "status": "unavailable",
        "message": message,
        "page": page,
        "limit": limit,
        "has_next": False,
    }


def _payload_from_items(items: list[dict[str, Any]], *, page: int, limit: int, has_next: bool) -> dict[str, Any]:
    return {
        "items": items,
        "status": "ok" if items else "empty",
        "page": page,
        "limit": limit,
        "has_next": has_next,
    }


def _normalize_general_article(row: dict[str, Any]) -> dict[str, Any] | None:
    title = _trimmed(row.get("title")) or _trimmed(row.get("headline"))
    url = _trimmed(row.get("url")) or _trimmed(row.get("link"))
    if not title or not url:
        return None
    summary = _trimmed(row.get("text")) or _trimmed(row.get("summary")) or _trimmed(row.get("snippet"))
    return {
        "title": title,
        "site": _trimmed(row.get("site")) or _trimmed(row.get("publisher")) or "Unknown",
        "published_at": _normalize_timestamp(row.get("publishedDate") or row.get("date") or row.get("publishedAt")),
        "url": url,
        "image_url": _trimmed(row.get("image")) or _trimmed(row.get("image_url")) or _trimmed(row.get("imageUrl")),
        "summary": summary,
        "symbol": _normalize_symbol(row.get("symbol")),
        "market_read": _classify_market_read(title=title, summary=summary),
        "source": "fmp_general_news",
    }


def _normalize_stock_article(row: dict[str, Any], *, symbol: str, strict_symbol_filter: bool) -> dict[str, Any] | None:
    title = _trimmed(row.get("title"))
    url = _trimmed(row.get("url"))
    if not title or not url:
        return None
    item_symbol = _normalize_symbol(row.get("symbol"))
    if not item_symbol or item_symbol != symbol:
        return None
    summary = _trimmed(row.get("text"))
    return {
        "symbol": item_symbol,
        "title": title,
        "site": _trimmed(row.get("site")) or _trimmed(row.get("publisher")),
        "published_at": _trimmed(row.get("publishedDate")),
        "url": url,
        "image_url": _trimmed(row.get("image")),
        "summary": summary,
        "market_read": _classify_market_read(title=title, summary=summary),
        "source": "fmp_stock_news",
    }


def _normalize_press_release(row: dict[str, Any], *, symbol: str, strict_symbol_filter: bool) -> dict[str, Any] | None:
    title = _trimmed(row.get("title"))
    url = _trimmed(row.get("url"))
    if not title and not url:
        return None
    item_symbol = _normalize_symbol(row.get("symbol"))
    if not item_symbol or item_symbol != symbol:
        return None
    summary = _trimmed(row.get("text"))
    normalized_title = title or "Press release"
    return {
        "symbol": item_symbol,
        "title": normalized_title,
        "site": _trimmed(row.get("site")) or _trimmed(row.get("publisher")),
        "published_at": _trimmed(row.get("publishedDate")),
        "url": url,
        "image_url": _trimmed(row.get("image")),
        "summary": summary,
        "market_read": _classify_market_read(title=normalized_title, summary=summary),
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


def _normalize_and_sort(
    rows: list[dict[str, Any]],
    *,
    symbol: str,
    normalizer: Callable[[dict[str, Any], str, bool], dict[str, Any] | None],
    strict_symbol_filter: bool,
) -> list[dict[str, Any]]:
    items = list(filter(None, (normalizer(row, symbol, strict_symbol_filter) for row in rows)))
    items = _dedupe_by_url(items)
    items.sort(key=_sort_key, reverse=True)
    return items


def _normalize_symbol_rows(
    rows: list[dict[str, Any]],
    *,
    symbol: str,
    normalizer: Callable[[dict[str, Any], str, bool], dict[str, Any] | None],
    strict_symbol_filter: bool,
) -> list[dict[str, Any]]:
    return _normalize_and_sort(rows, symbol=symbol, normalizer=normalizer, strict_symbol_filter=strict_symbol_filter)


def _normalize_stock_row(row: dict[str, Any], symbol: str, strict_symbol_filter: bool) -> dict[str, Any] | None:
    return _normalize_stock_article(row, symbol=symbol, strict_symbol_filter=strict_symbol_filter)


def _normalize_press_row(row: dict[str, Any], symbol: str, strict_symbol_filter: bool) -> dict[str, Any] | None:
    return _normalize_press_release(row, symbol=symbol, strict_symbol_filter=strict_symbol_filter)


def _normalize_sec_row(row: dict[str, Any], symbol: str, strict_symbol_filter: bool) -> dict[str, Any] | None:
    return _normalize_sec_filing(row, symbol=symbol)


def _try_direct_symbol_search(
    *,
    attempts: list[tuple[str, str]],
    symbol: str,
    page: int,
    limit: int,
    normalizer: Callable[[dict[str, Any], str, bool], dict[str, Any] | None],
    base_params: dict[str, Any] | None = None,
    empty_is_terminal: bool = True,
) -> tuple[dict[str, Any] | None, bool]:
    provider_failed = False
    supported_any = False
    for endpoint, symbol_param in attempts:
        try:
            rows, supported = _request_rows(
                endpoint,
                params={**(base_params or {}), symbol_param: symbol, "page": page, "limit": limit + 1},
                context_symbol=symbol,
            )
        except FMPNewsUnavailable:
            provider_failed = True
            continue
        if not supported:
            continue
        supported_any = True
        if not rows:
            if empty_is_terminal:
                return _payload_from_items([], page=page, limit=limit, has_next=False), provider_failed
            continue
        items = _normalize_symbol_rows(rows, symbol=symbol, normalizer=normalizer, strict_symbol_filter=False)
        if not items:
            if empty_is_terminal:
                return _payload_from_items([], page=page, limit=limit, has_next=False), provider_failed
            continue
        return _payload_from_items(items[:limit], page=page, limit=limit, has_next=len(items) > limit), provider_failed
    if supported_any and empty_is_terminal:
        return _payload_from_items([], page=page, limit=limit, has_next=False), provider_failed
    return None, provider_failed


def _scan_global_symbol_feed(
    *,
    endpoint: str,
    symbol: str,
    page: int,
    limit: int,
    normalizer: Callable[[dict[str, Any], str, bool], dict[str, Any] | None],
) -> dict[str, Any]:
    target_count = (page + 1) * limit + 1
    collected: list[dict[str, Any]] = []
    provider_page = 0
    items_examined = 0

    while provider_page < SYMBOL_SCAN_MAX_PAGES and len(collected) < target_count and items_examined < SYMBOL_SCAN_MAX_ITEMS:
        request_limit = min(50, SYMBOL_SCAN_MAX_ITEMS - items_examined)
        if request_limit <= 0:
            break
        rows, supported = _request_rows(
            endpoint,
            params={"page": provider_page, "limit": request_limit},
            context_symbol=symbol,
        )
        if not supported or not rows:
            break
        rows = rows[: max(0, SYMBOL_SCAN_MAX_ITEMS - items_examined)]
        items_examined += len(rows)
        normalized = _normalize_symbol_rows(rows, symbol=symbol, normalizer=normalizer, strict_symbol_filter=True)
        if normalized:
            collected.extend(normalized)
            collected = _dedupe_by_url(collected)
            collected.sort(key=_sort_key, reverse=True)
        if len(rows) < request_limit:
            break
        provider_page += 1

    sliced, has_next = _paginate_items(collected, page=page, limit=limit)
    return _payload_from_items(sliced, page=page, limit=limit, has_next=has_next)


def get_general_news(*, page: int = 0, limit: int = 20) -> dict[str, Any]:
    bounded_page = max(int(page or 0), 0)
    bounded_limit = max(1, min(int(limit or 20), 50))
    cache_key = _cache_key("general", {"page": bounded_page, "limit": bounded_limit})
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    try:
        rows, supported = _request_rows(
            "news/general-latest",
            params={"page": bounded_page, "limit": bounded_limit + 1},
        )
        if not supported:
            raise FMPNewsUnavailable(GENERAL_UNAVAILABLE_MESSAGE)
    except FMPNewsUnavailable:
        payload = _unavailable_payload(page=bounded_page, limit=bounded_limit, message=GENERAL_UNAVAILABLE_MESSAGE)
        return _cache_set(cache_key, payload, ttl_seconds=GENERAL_NEWS_TTL_SECONDS)

    items = _dedupe_by_url(list(filter(None, (_normalize_general_article(row) for row in rows))))
    items.sort(key=_sort_key, reverse=True)
    payload = _payload_from_items(items[:bounded_limit], page=bounded_page, limit=bounded_limit, has_next=len(items) > bounded_limit)
    return _cache_set(cache_key, payload, ttl_seconds=GENERAL_NEWS_TTL_SECONDS)


def get_stock_news(*, symbol: str, page: int = 0, limit: int = 20) -> dict[str, Any]:
    normalized_symbol = _normalize_symbol(symbol)
    bounded_page = max(int(page or 0), 0)
    bounded_limit = max(1, min(int(limit or 20), 50))
    if not normalized_symbol:
        return _payload_from_items([], page=0, limit=bounded_limit, has_next=False)

    cache_key = _cache_key("stock-news", {"symbol": normalized_symbol, "page": bounded_page, "limit": bounded_limit})
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    try:
        rows, error_payload = _request_ticker_news_rows(symbol=normalized_symbol, page=bounded_page, limit=bounded_limit)
    except FMPNewsUnavailable:
        payload = _unavailable_payload(
            page=bounded_page,
            limit=bounded_limit,
            message=TICKER_CONTEXT_UNAVAILABLE_MESSAGE,
        )
        return _cache_set(cache_key, payload, ttl_seconds=STOCK_NEWS_TTL_SECONDS)

    if error_payload is not None:
        return _cache_set(cache_key, error_payload, ttl_seconds=STOCK_NEWS_TTL_SECONDS)

    items = _normalize_symbol_rows(rows, symbol=normalized_symbol, normalizer=_normalize_stock_row, strict_symbol_filter=False)
    if not items:
        payload = {
            **_payload_from_items([], page=bounded_page, limit=bounded_limit, has_next=False),
            "message": TICKER_NEWS_EMPTY_MESSAGE,
        }
        return _cache_set(cache_key, payload, ttl_seconds=STOCK_NEWS_TTL_SECONDS)

    payload = _payload_from_items(items[:bounded_limit], page=bounded_page, limit=bounded_limit, has_next=len(rows) >= bounded_limit)
    return _cache_set(cache_key, payload, ttl_seconds=STOCK_NEWS_TTL_SECONDS)


def get_press_releases(*, symbol: str, page: int = 0, limit: int = 20) -> dict[str, Any]:
    normalized_symbol = _normalize_symbol(symbol)
    bounded_page = max(int(page or 0), 0)
    bounded_limit = max(1, min(int(limit or 20), 50))
    if not normalized_symbol:
        return _payload_from_items([], page=0, limit=bounded_limit, has_next=False)

    cache_key = _cache_key("press-releases", {"symbol": normalized_symbol, "page": bounded_page, "limit": bounded_limit})
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    try:
        rows, error_payload = _request_ticker_press_rows(symbol=normalized_symbol, page=bounded_page, limit=bounded_limit)
    except FMPNewsUnavailable:
        payload = _unavailable_payload(
            page=bounded_page,
            limit=bounded_limit,
            message=TICKER_CONTEXT_UNAVAILABLE_MESSAGE,
        )
        return _cache_set(cache_key, payload, ttl_seconds=PRESS_RELEASES_TTL_SECONDS)

    if error_payload is not None:
        return _cache_set(cache_key, error_payload, ttl_seconds=PRESS_RELEASES_TTL_SECONDS)

    items = _normalize_symbol_rows(rows, symbol=normalized_symbol, normalizer=_normalize_press_row, strict_symbol_filter=False)
    if not items:
        payload = {
            **_payload_from_items([], page=bounded_page, limit=bounded_limit, has_next=False),
            "message": TICKER_PRESS_EMPTY_MESSAGE,
        }
        return _cache_set(cache_key, payload, ttl_seconds=PRESS_RELEASES_TTL_SECONDS)

    payload = _payload_from_items(items[:bounded_limit], page=bounded_page, limit=bounded_limit, has_next=len(rows) >= bounded_limit)
    return _cache_set(cache_key, payload, ttl_seconds=PRESS_RELEASES_TTL_SECONDS)


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
        return _payload_from_items([], page=bounded_page, limit=bounded_limit, has_next=False)

    today = date.today()
    default_from = today - timedelta(days=30)
    from_value = from_date or default_from.isoformat()
    to_value = to_date or today.isoformat()
    cache_key = _cache_key(
        "sec-filings",
        {"symbol": normalized_symbol, "from": from_value, "to": to_value, "page": bounded_page, "limit": bounded_limit},
    )
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    direct_payload, provider_failed = _try_direct_symbol_search(
        attempts=[
            ("sec-filings-search/symbol", "symbol"),
            ("sec-filings-company-search/symbol", "symbol"),
        ],
        symbol=normalized_symbol,
        page=bounded_page,
        limit=bounded_limit,
        normalizer=_normalize_sec_row,
        base_params={"from": from_value, "to": to_value},
        empty_is_terminal=True,
    )
    if direct_payload is not None:
        return _cache_set(cache_key, direct_payload, ttl_seconds=SEC_FILINGS_TTL_SECONDS)

    if provider_failed:
        payload = _unavailable_payload(
            page=bounded_page,
            limit=bounded_limit,
            message=TICKER_CONTEXT_UNAVAILABLE_MESSAGE,
        )
        return _cache_set(cache_key, payload, ttl_seconds=SEC_FILINGS_TTL_SECONDS)
    payload = _payload_from_items([], page=bounded_page, limit=bounded_limit, has_next=False)
    return _cache_set(cache_key, payload, ttl_seconds=SEC_FILINGS_TTL_SECONDS)
