from __future__ import annotations

import logging
import os
import time
from datetime import date, datetime, timedelta, timezone
from threading import Lock
from typing import Any, Callable, Literal

import requests
from sqlalchemy import select

from app.clients.fmp import FMP_BASE_URL
from app.db import SessionLocal
from app.models import DataEnrichmentJob
from app.request_priority import get_request_context
from app.services.data_enrichment_queue import ACTIVE_STATUSES, build_dedupe_key, enqueue_data_enrichment_job
from app.services.provider_usage import (
    ProviderUnavailable,
    ensure_fmp_live_allowed,
    fallback_payload,
    reason_for_status,
    reason_from_exception,
    record_cache_hit,
    record_cache_miss,
    record_content_write,
    record_fallback,
    record_provider_response,
)
from app.services.ticker_content_cache import (
    db_ticker_content_cache_get,
    db_ticker_content_cache_set,
    ticker_content_cache_key,
    ticker_content_window_key,
)
from app.utils.symbols import normalize_symbol

logger = logging.getLogger(__name__)

NewsStatus = Literal["ok", "empty", "unavailable"]
GENERAL_NEWS_TTL_SECONDS = 15 * 60
INSIGHTS_CATEGORY_NEWS_TTL_SECONDS = 15 * 60
STOCK_NEWS_TTL_SECONDS = 15 * 60
PRESS_RELEASES_TTL_SECONDS = 30 * 60
SEC_FILINGS_TTL_SECONDS = 60 * 60
NEWS_STALE_TTL_SECONDS = 24 * 60 * 60
PROVIDER_TIMEOUT_SECONDS = 8
SYMBOL_SCAN_MAX_PAGES = 2
SYMBOL_SCAN_MAX_ITEMS = 100
GENERAL_UNAVAILABLE_MESSAGE = "Market data is temporarily unavailable."
INSIGHTS_CATEGORY_UNAVAILABLE_MESSAGE = "Headlines are temporarily unavailable."
TICKER_CONTEXT_UNAVAILABLE_MESSAGE = "Data temporarily unavailable."
TICKER_NEWS_EMPTY_MESSAGE = "No recent news found for this ticker."
TICKER_NEWS_UNAVAILABLE_MESSAGE = "News is temporarily unavailable."
TICKER_NEWS_PLAN_MESSAGE = TICKER_NEWS_UNAVAILABLE_MESSAGE
TICKER_NEWS_RATE_LIMIT_MESSAGE = TICKER_NEWS_UNAVAILABLE_MESSAGE
TICKER_PRESS_EMPTY_MESSAGE = "No recent press releases found for this ticker."
TICKER_PRESS_UNAVAILABLE_MESSAGE = "Press releases are temporarily unavailable."
TICKER_PRESS_PLAN_MESSAGE = TICKER_PRESS_UNAVAILABLE_MESSAGE
TICKER_PRESS_RATE_LIMIT_MESSAGE = TICKER_PRESS_UNAVAILABLE_MESSAGE
TICKER_SEC_UNAVAILABLE_MESSAGE = "Filings are temporarily unavailable."
SEC_FORM_TITLES = {
    "3": "Initial Statement of Beneficial Ownership",
    "4": "Statement of Changes in Beneficial Ownership",
    "5": "Annual Statement of Beneficial Ownership",
    "6-K": "Report of Foreign Private Issuer",
    "8-K": "Current Report",
    "10-K": "Annual Report",
    "10-Q": "Quarterly Report",
    "20-F": "Annual Report of Foreign Private Issuer",
    "13F-HR": "Institutional Holdings Report",
    "SD": "Specialized Disclosure Report",
}
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

INSIGHTS_CATEGORY_NEWS_ENDPOINTS = {
    "world-indexes": "news/general-latest",
    "us-macro": "news/stock-latest",
    "us-treasury": "news/stock-latest",
    "us-indexes": "news/stock-latest",
    "us-sectors": "news/stock-latest",
    "crypto": "news/crypto-latest",
    "currencies": "news/forex-latest",
    "commodities": "news/general-latest",
}

COMMODITY_NEWS_TERMS = (
    "commodity",
    "commodities",
    "gold",
    "silver",
    "copper",
    "brent",
    "crude",
    "oil",
    "natural gas",
    "wheat",
    "corn",
    "soybean",
)

_CACHE: dict[str, tuple[float, float, float, dict[str, Any]]] = {}
_CACHE_LOCK = Lock()
_TRUE_VALUES = {"1", "true", "yes", "on"}


class FMPNewsError(RuntimeError):
    """Raised when the upstream provider fails."""


class FMPNewsUnavailable(FMPNewsError):
    """Raised when the provider cannot serve the current request."""

    def __init__(self, message: str, *, reason_code: str = "provider_unavailable") -> None:
        super().__init__(message)
        self.reason = reason_code
        self.reason_code = reason_code


def _api_key() -> str:
    key = os.getenv("FMP_API_KEY", "").strip()
    if not key:
        raise FMPNewsUnavailable(GENERAL_UNAVAILABLE_MESSAGE)
    return key


def _debug_logs_enabled() -> bool:
    return os.getenv("PROVIDER_DEBUG_LOGS", "false").strip().lower() in _TRUE_VALUES


def clear_news_cache() -> None:
    with _CACHE_LOCK:
        _CACHE.clear()


def _cache_get(key: str, *, category: str, symbol: str | None = None) -> dict[str, Any] | None:
    now = time.time()
    with _CACHE_LOCK:
        cached = _CACHE.get(key)
        if not cached:
            return None
        fetched_at, expires_at, stale_until, payload = cached
        if expires_at <= now:
            if stale_until <= now:
                _CACHE.pop(key, None)
            return None
        record_cache_hit(category=category, symbol=symbol, cache_age_seconds=max(now - fetched_at, 0))
        return payload


def _cache_get_stale(key: str, *, category: str, symbol: str | None = None) -> tuple[dict[str, Any], float] | None:
    now = time.time()
    with _CACHE_LOCK:
        cached = _CACHE.get(key)
        if not cached:
            return None
        fetched_at, expires_at, stale_until, payload = cached
        if expires_at > now:
            return None
        if stale_until <= now:
            _CACHE.pop(key, None)
            return None
        age = max(now - fetched_at, 0)
        record_cache_hit(category=category, symbol=symbol, cache_age_seconds=age)
        return payload, age


def _cache_set(
    key: str,
    payload: dict[str, Any],
    *,
    ttl_seconds: int,
    category: str | None = None,
    symbol: str | None = None,
) -> dict[str, Any]:
    now = time.time()
    items = payload.get("items")
    item_count = len(items) if isinstance(items, list) else int(payload.get("item_count") or 0)
    normalized_payload = {
        **payload,
        "item_count": item_count,
        "updated_at": payload.get("updated_at") or datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
    }
    with _CACHE_LOCK:
        _CACHE[key] = (now, now + ttl_seconds, now + NEWS_STALE_TTL_SECONDS, normalized_payload)
    if category is not None:
        record_content_write(category=category, symbol=symbol, item_count=item_count)
        logger.info(
            "ticker_content_cache_write category=%s symbol=%s status=%s item_count=%s key=%s",
            category,
            symbol,
            normalized_payload.get("status"),
            item_count,
            key,
        )
    return normalized_payload


def _cache_key(prefix: str, params: dict[str, Any]) -> str:
    ordered = "&".join(
        f"{key}={params[key]}"
        for key in sorted(params)
        if params[key] is not None and str(params[key]).strip()
    )
    return f"{prefix}?{ordered}"


def _payload_has_items(payload: dict[str, Any] | None) -> bool:
    if not isinstance(payload, dict):
        return False
    items = payload.get("items")
    if isinstance(items, list) and items:
        return True
    try:
        return int(payload.get("item_count") or 0) > 0
    except Exception:
        return False


def _is_active_ticker_panel_request(allowed_panels: set[str]) -> bool:
    context = get_request_context() or {}
    route = str(context.get("path") or "")
    request_source = str(context.get("request_source") or "").lower()
    route_family = str(context.get("route_family") or "").lower()
    panel = str(context.get("panel") or "")
    return (
        route.startswith("/api/tickers/")
        and request_source in {"client", "visibility"}
        and route_family == "ticker"
        and panel in allowed_panels
    )


def _trimmed(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned or None


def _normalize_symbol(value: Any) -> str | None:
    return normalize_symbol(str(value)) if value is not None else None


def _normalize_symbols(value: Any) -> list[str]:
    symbols: list[str] = []
    if isinstance(value, str):
        symbols = [symbol for chunk in value.replace("|", ",").split(",") if (symbol := normalize_symbol(chunk))]
    elif isinstance(value, (list, tuple)):
        symbols = [symbol for chunk in value if (symbol := normalize_symbol(str(chunk)))]
    return symbols


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
    if not _debug_logs_enabled():
        return
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
    if not _debug_logs_enabled():
        return
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
    category = f"news:{endpoint}"
    try:
        ensure_fmp_live_allowed(
            category=category,
            symbol=context_symbol,
            allow_user_request=_is_active_ticker_panel_request({"TickerFilingsPanel"}),
        )
    except ProviderUnavailable as exc:
        raise FMPNewsUnavailable(str(exc)) from exc
    request_params = {"apikey": _api_key()}
    for key, value in params.items():
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        request_params[key] = value

    try:
        response = requests.get(f"{FMP_BASE_URL}/{endpoint}", params=request_params, timeout=timeout_s)
        record_provider_response(category=category, symbol=context_symbol, status_code=response.status_code)
    except requests.Timeout as exc:
        _log_ticker_context_error(endpoint=endpoint, symbol=context_symbol, status="provider_timeout", detail=exc)
        raise FMPNewsUnavailable(GENERAL_UNAVAILABLE_MESSAGE, reason_code="provider_timeout") from exc
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
    category = "news:stock"
    try:
        ensure_fmp_live_allowed(
            category=category,
            symbol=symbol,
            allow_user_request=_is_active_ticker_panel_request({"TickerNewsPanel"}),
        )
    except ProviderUnavailable as exc:
        reason = reason_from_exception(exc)
        record_fallback(category=category, symbol=symbol, reason=reason)
        return [], _unavailable_payload(page=page, limit=limit, message=TICKER_NEWS_UNAVAILABLE_MESSAGE, reason=reason)
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
        record_provider_response(category=category, symbol=symbol, status_code=response.status_code)
    except requests.Timeout as exc:
        _ticker_news_debug_log(symbol=symbol, status="provider_timeout", parsed_count=0, body_preview=str(exc))
        return [], _unavailable_payload(page=page, limit=limit, message=TICKER_NEWS_UNAVAILABLE_MESSAGE, reason="provider_timeout")
    except requests.RequestException as exc:
        _ticker_news_debug_log(symbol=symbol, status="request_error", parsed_count=0, body_preview=str(exc))
        return [], _unavailable_payload(page=page, limit=limit, message=TICKER_NEWS_UNAVAILABLE_MESSAGE)

    raw_text = response.text

    if response.status_code == 200:
        try:
            data = response.json()
        except ValueError:
            _ticker_news_debug_log(symbol=symbol, status=200, parsed_count=0, body_preview="invalid_json")
            return [], _unavailable_payload(page=page, limit=limit, message=TICKER_NEWS_UNAVAILABLE_MESSAGE, reason="provider_unavailable")

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
            return [], _unavailable_payload(page=page, limit=limit, message=TICKER_NEWS_UNAVAILABLE_MESSAGE, reason="provider_unavailable")

        _ticker_news_debug_log(symbol=symbol, status=200, parsed_count=len(rows), body_preview=raw_text)
        return rows, None

    if response.status_code in {401, 402, 403}:
        _ticker_news_debug_log(symbol=symbol, status=response.status_code, parsed_count=0, body_preview=raw_text)
        return [], _unavailable_payload(page=page, limit=limit, message=TICKER_NEWS_PLAN_MESSAGE, reason=reason_for_status(response.status_code))

    if response.status_code == 429:
        _ticker_news_debug_log(symbol=symbol, status=429, parsed_count=0, body_preview=raw_text)
        return [], _unavailable_payload(page=page, limit=limit, message=TICKER_NEWS_RATE_LIMIT_MESSAGE, reason="provider_rate_limited")

    _ticker_news_debug_log(symbol=symbol, status=response.status_code, parsed_count=0, body_preview=raw_text)
    return [], _unavailable_payload(page=page, limit=limit, message=TICKER_NEWS_UNAVAILABLE_MESSAGE, reason=reason_for_status(response.status_code))


def _request_ticker_press_rows(*, symbol: str, page: int, limit: int) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    category = "news:press-releases"
    try:
        ensure_fmp_live_allowed(
            category=category,
            symbol=symbol,
            allow_user_request=_is_active_ticker_panel_request({"TickerPressPanel"}),
        )
    except ProviderUnavailable as exc:
        reason = reason_from_exception(exc)
        record_fallback(category=category, symbol=symbol, reason=reason)
        return [], _unavailable_payload(page=page, limit=limit, message=TICKER_PRESS_UNAVAILABLE_MESSAGE, reason=reason)
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
        record_provider_response(category=category, symbol=symbol, status_code=response.status_code)
    except requests.Timeout as exc:
        _ticker_press_debug_log(symbol=symbol, status="provider_timeout", parsed_count=0, body_preview=str(exc))
        return [], _unavailable_payload(page=page, limit=limit, message=TICKER_PRESS_UNAVAILABLE_MESSAGE, reason="provider_timeout")
    except requests.RequestException as exc:
        _ticker_press_debug_log(symbol=symbol, status="request_error", parsed_count=0, body_preview=str(exc))
        return [], _unavailable_payload(page=page, limit=limit, message=TICKER_PRESS_UNAVAILABLE_MESSAGE)

    raw_text = response.text

    if response.status_code == 200:
        try:
            data = response.json()
        except ValueError:
            _ticker_press_debug_log(symbol=symbol, status=200, parsed_count=0, body_preview="invalid_json")
            return [], _unavailable_payload(page=page, limit=limit, message=TICKER_PRESS_UNAVAILABLE_MESSAGE, reason="provider_unavailable")

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
        return [], _unavailable_payload(page=page, limit=limit, message=TICKER_PRESS_UNAVAILABLE_MESSAGE)

    if response.status_code in {401, 402, 403}:
        _ticker_press_debug_log(symbol=symbol, status=response.status_code, parsed_count=0, body_preview=raw_text)
        return [], _unavailable_payload(page=page, limit=limit, message=TICKER_PRESS_PLAN_MESSAGE, reason=reason_for_status(response.status_code))

    if response.status_code == 429:
        _ticker_press_debug_log(symbol=symbol, status=429, parsed_count=0, body_preview=raw_text)
        return [], _unavailable_payload(page=page, limit=limit, message=TICKER_PRESS_RATE_LIMIT_MESSAGE, reason="provider_rate_limited")

    _ticker_press_debug_log(symbol=symbol, status=response.status_code, parsed_count=0, body_preview=raw_text)
    return [], _unavailable_payload(page=page, limit=limit, message=TICKER_PRESS_UNAVAILABLE_MESSAGE, reason=reason_for_status(response.status_code))


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


def _structured_fallback_fields(*, reason: str, message: str) -> dict[str, Any]:
    context = get_request_context() or {}
    if not context.get("path"):
        return {}
    return fallback_payload(reason=reason, message=message)


def _is_public_request_context() -> bool:
    context = get_request_context() or {}
    route = str(context.get("path") or "")
    if _is_active_ticker_panel_request({"TickerNewsPanel", "TickerPressPanel", "TickerFilingsPanel"}):
        return False
    return route.startswith("/api/") and not route.startswith("/api/admin/")


def _unavailable_payload(*, page: int, limit: int, message: str, reason: str = "provider_unavailable") -> dict[str, Any]:
    payload = {
        "items": [],
        "status": "unavailable",
        "message": message,
        "page": page,
        "limit": limit,
        "has_next": False,
        **_structured_fallback_fields(reason=reason, message=message),
    }
    if not _is_public_request_context():
        payload["reason"] = reason
    return payload


def _warming_payload(*, page: int, limit: int) -> dict[str, Any]:
    return {
        "items": [],
        "status": "warming",
        "page": page,
        "limit": limit,
        "has_next": False,
        "cache_status": "warming",
    }


def _public_cache_miss_payload(
    *,
    cache_key: str,
    category: str,
    symbol: str,
    page: int,
    limit: int,
    job_type: str,
    stale_message: str,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    queued = _enqueue_news_refresh(
        job_type=job_type,
        symbol=symbol,
        reason="cache_miss",
        payload=payload or {"page": page, "limit": limit},
    )
    stale = _cache_get_stale(cache_key, category=category, symbol=symbol)
    if stale is not None:
        stale_payload, age = stale
        record_fallback(category=category, symbol=symbol, reason="cache_miss", cache_age_seconds=age)
        return _stale_payload(stale_payload, reason="cache_miss", message=stale_message, age_seconds=age)
    if queued or _active_refresh_exists(job_type=job_type, symbol=symbol):
        return _warming_payload(page=page, limit=limit)
    return {
        **_payload_from_items([], page=page, limit=limit, has_next=False),
        "message": "No recent data found.",
    }


def _stale_payload(payload: dict[str, Any], *, reason: str, message: str, age_seconds: float) -> dict[str, Any]:
    stale = {
        **payload,
        "stale": True,
        "unavailable": False,
        "cache_status": "stale",
        "cache_age_seconds": round(age_seconds, 1),
    }
    if not _is_public_request_context():
        stale["reason"] = reason
        stale["message"] = payload.get("message") or message
    else:
        stale.pop("message", None)
        stale.pop("reason", None)
        stale.pop("data", None)
    return stale


def _active_refresh_exists(*, job_type: str, symbol: str | None = None) -> bool:
    dedupe_key = build_dedupe_key(job_type=job_type, symbol=symbol)
    if not dedupe_key.strip("|"):
        return False
    db = SessionLocal()
    try:
        return bool(
            db.execute(
                select(DataEnrichmentJob.id)
                .where(DataEnrichmentJob.dedupe_key == dedupe_key)
                .where(DataEnrichmentJob.status.in_(sorted(ACTIVE_STATUSES)))
                .limit(1)
            ).scalar_one_or_none()
        )
    except Exception:
        logger.exception("news_refresh_active_check_failed job_type=%s symbol=%s", job_type, symbol)
        return False
    finally:
        db.close()


def _enqueue_news_refresh(
    *,
    job_type: str,
    symbol: str | None = None,
    reason: str,
    payload: dict[str, Any] | None = None,
) -> bool:
    return enqueue_data_enrichment_job(
        job_type=job_type,
        symbol=symbol,
        source="page_load",
        reason=reason,
        priority=50,
        payload=payload,
    )


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


def _normalize_category_article(row: dict[str, Any], category: str) -> dict[str, Any] | None:
    item = _normalize_general_article(row)
    if not item:
        return None
    return {**item, "source": f"insights_{category.replace('-', '_')}_news"}


def _matches_commodity_terms(item: dict[str, Any]) -> bool:
    text = " ".join(
        str(item.get(key) or "")
        for key in ("title", "summary", "symbol")
    ).lower()
    return any(term in text for term in COMMODITY_NEWS_TERMS)


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


def _normalize_sec_form(value: str | None) -> str:
    return (value or "").strip().upper().removeprefix("FORM ").strip()


def _sec_filing_title(form_type: str | None, raw_title: str | None) -> str:
    title = _trimmed(raw_title)
    if title and title.lower() != "sec filing":
        return title
    return SEC_FORM_TITLES.get(_normalize_sec_form(form_type), "SEC Filing")


def _normalize_sec_filing(row: dict[str, Any], *, symbol: str) -> dict[str, Any] | None:
    form_type = _trimmed(row.get("formType")) or _trimmed(row.get("type")) or _trimmed(row.get("form")) or ""
    title = (
        _trimmed(row.get("title"))
        or _trimmed(row.get("description"))
        or _trimmed(row.get("formDescription"))
    )
    normalized = {
        "symbol": symbol,
        "filing_date": _normalize_dateish(row.get("fillingDate") or row.get("filingDate") or row.get("date")),
        "accepted_date": _normalize_dateish(row.get("acceptedDate")),
        "form_type": form_type,
        "title": _sec_filing_title(form_type, title),
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
    cached = _cache_get(cache_key, category="news:general")
    if cached is not None:
        return cached
    record_cache_miss(category="news:general")

    try:
        rows, supported = _request_rows(
            "news/general-latest",
            params={"page": bounded_page, "limit": bounded_limit + 1},
        )
        if not supported:
            raise FMPNewsUnavailable(GENERAL_UNAVAILABLE_MESSAGE)
    except FMPNewsUnavailable as exc:
        reason = reason_from_exception(exc)
        record_fallback(category="news:general", reason=reason)
        _enqueue_news_refresh(
            job_type="news_general",
            reason=reason,
            payload={"page": bounded_page, "limit": bounded_limit},
        )
        stale = _cache_get_stale(cache_key, category="news:general")
        if stale is not None:
            stale_payload, age = stale
            record_fallback(category="news:general", reason=reason, cache_age_seconds=age)
            return _stale_payload(stale_payload, reason=reason, message=GENERAL_UNAVAILABLE_MESSAGE, age_seconds=age)
        if _is_public_request_context():
            return _warming_payload(page=bounded_page, limit=bounded_limit)
        payload = _unavailable_payload(page=bounded_page, limit=bounded_limit, message=GENERAL_UNAVAILABLE_MESSAGE, reason=reason)
        return _cache_set(cache_key, payload, ttl_seconds=GENERAL_NEWS_TTL_SECONDS, category="news:general")

    items = _dedupe_by_url(list(filter(None, (_normalize_general_article(row) for row in rows))))
    items.sort(key=_sort_key, reverse=True)
    payload = _payload_from_items(items[:bounded_limit], page=bounded_page, limit=bounded_limit, has_next=len(items) > bounded_limit)
    return _cache_set(cache_key, payload, ttl_seconds=GENERAL_NEWS_TTL_SECONDS, category="news:general")


def get_insights_category_news(category: str, *, page: int = 0, limit: int = 20) -> dict[str, Any]:
    category_key = (category or "").strip().lower()
    endpoint = INSIGHTS_CATEGORY_NEWS_ENDPOINTS.get(category_key)
    bounded_page = max(int(page or 0), 0)
    bounded_limit = max(1, min(int(limit or 20), 50))
    if not endpoint:
        return _payload_from_items([], page=bounded_page, limit=bounded_limit, has_next=False)

    provider_limit = bounded_limit
    usage_category = f"news:insights:{category_key}"
    cache_key = _cache_key("insights-category-news", {"category": category_key, "page": bounded_page, "limit": bounded_limit})
    cached = _cache_get(cache_key, category=usage_category)
    if cached is not None:
        return cached
    record_cache_miss(category=usage_category)

    try:
        rows, supported = _request_rows(
            endpoint,
            params={"page": bounded_page, "limit": provider_limit},
        )
        if not supported:
            raise FMPNewsUnavailable(INSIGHTS_CATEGORY_UNAVAILABLE_MESSAGE)
    except FMPNewsUnavailable as exc:
        reason = reason_from_exception(exc)
        record_fallback(category=usage_category, reason=reason)
        stale = _cache_get_stale(cache_key, category=usage_category)
        if stale is not None:
            stale_payload, age = stale
            record_fallback(category=usage_category, reason=reason, cache_age_seconds=age)
            return _stale_payload(stale_payload, reason=reason, message=INSIGHTS_CATEGORY_UNAVAILABLE_MESSAGE, age_seconds=age)
        if _is_public_request_context():
            return _warming_payload(page=bounded_page, limit=bounded_limit)
        payload = _unavailable_payload(page=bounded_page, limit=bounded_limit, message=INSIGHTS_CATEGORY_UNAVAILABLE_MESSAGE, reason=reason)
        return _cache_set(cache_key, payload, ttl_seconds=INSIGHTS_CATEGORY_NEWS_TTL_SECONDS, category=usage_category)

    items = _dedupe_by_url(list(filter(None, (_normalize_category_article(row, category_key) for row in rows))))
    if category_key == "commodities":
        items = [item for item in items if _matches_commodity_terms(item)]
    items.sort(key=_sort_key, reverse=True)
    payload = _payload_from_items(items[:bounded_limit], page=bounded_page, limit=bounded_limit, has_next=len(items) > bounded_limit)
    return _cache_set(cache_key, payload, ttl_seconds=INSIGHTS_CATEGORY_NEWS_TTL_SECONDS, category=usage_category)


def get_stock_news(*, symbol: str, page: int = 0, limit: int = 20) -> dict[str, Any]:
    normalized_symbol = _normalize_symbol(symbol)
    bounded_page = max(int(page or 0), 0)
    bounded_limit = max(1, min(int(limit or 20), 50))
    if not normalized_symbol:
        return _payload_from_items([], page=0, limit=bounded_limit, has_next=False)

    active_panel_request = _is_active_ticker_panel_request({"TickerNewsPanel"})
    cache_key = _cache_key("stock-news", {"symbol": normalized_symbol, "page": bounded_page, "limit": bounded_limit})
    cached = _cache_get(cache_key, category="news:stock", symbol=normalized_symbol)
    if _payload_has_items(cached):
        return cached
    record_cache_miss(category="news:stock", symbol=normalized_symbol)
    db_cached = db_ticker_content_cache_get(
        "news",
        normalized_symbol,
        page=bounded_page,
        limit=bounded_limit,
        window_key="latest",
    )
    if db_cached is not None and (_payload_has_items(db_cached) or not active_panel_request):
        return _cache_set(cache_key, db_cached, ttl_seconds=STOCK_NEWS_TTL_SECONDS, category="news:stock", symbol=normalized_symbol)
    if cached is not None and not active_panel_request:
        return cached
    if _is_public_request_context():
        return _public_cache_miss_payload(
            cache_key=cache_key,
            category="news:stock",
            symbol=normalized_symbol,
            page=bounded_page,
            limit=bounded_limit,
            job_type="news_stock",
            stale_message=TICKER_NEWS_UNAVAILABLE_MESSAGE,
        )

    try:
        rows, error_payload = _request_ticker_news_rows(symbol=normalized_symbol, page=bounded_page, limit=bounded_limit)
    except FMPNewsUnavailable as exc:
        reason = reason_from_exception(exc)
        record_fallback(category="news:stock", symbol=normalized_symbol, reason=reason)
        _enqueue_news_refresh(
            job_type="news_stock",
            symbol=normalized_symbol,
            reason=reason,
            payload={"page": bounded_page, "limit": bounded_limit},
        )
        stale = _cache_get_stale(cache_key, category="news:stock", symbol=normalized_symbol)
        if stale is not None:
            stale_payload, age = stale
            record_fallback(category="news:stock", symbol=normalized_symbol, reason=reason, cache_age_seconds=age)
            return _stale_payload(stale_payload, reason=reason, message=TICKER_NEWS_UNAVAILABLE_MESSAGE, age_seconds=age)
        if _is_public_request_context():
            return _warming_payload(page=bounded_page, limit=bounded_limit)
        payload = _unavailable_payload(page=bounded_page, limit=bounded_limit, message=TICKER_NEWS_UNAVAILABLE_MESSAGE, reason=reason)
        return _cache_set(cache_key, payload, ttl_seconds=STOCK_NEWS_TTL_SECONDS, category="news:stock", symbol=normalized_symbol)

    if error_payload is not None:
        reason = str(error_payload.get("reason") or "provider_unavailable")
        _enqueue_news_refresh(
            job_type="news_stock",
            symbol=normalized_symbol,
            reason=reason,
            payload={"page": bounded_page, "limit": bounded_limit},
        )
        stale = _cache_get_stale(cache_key, category="news:stock", symbol=normalized_symbol)
        if stale is not None:
            stale_payload, age = stale
            record_fallback(category="news:stock", symbol=normalized_symbol, reason=reason, cache_age_seconds=age)
            return _stale_payload(stale_payload, reason=reason, message=TICKER_NEWS_UNAVAILABLE_MESSAGE, age_seconds=age)
        if _is_public_request_context():
            return _warming_payload(page=bounded_page, limit=bounded_limit)
        return _cache_set(cache_key, error_payload, ttl_seconds=STOCK_NEWS_TTL_SECONDS, category="news:stock", symbol=normalized_symbol)

    items = _normalize_symbol_rows(rows, symbol=normalized_symbol, normalizer=_normalize_stock_row, strict_symbol_filter=False)
    if not items:
        payload = {
            **_payload_from_items([], page=bounded_page, limit=bounded_limit, has_next=False),
            "message": TICKER_NEWS_EMPTY_MESSAGE,
        }
        return _cache_set(cache_key, payload, ttl_seconds=STOCK_NEWS_TTL_SECONDS, category="news:stock", symbol=normalized_symbol)

    payload = _payload_from_items(items[:bounded_limit], page=bounded_page, limit=bounded_limit, has_next=len(rows) >= bounded_limit)
    cached_payload = _cache_set(cache_key, payload, ttl_seconds=STOCK_NEWS_TTL_SECONDS, category="news:stock", symbol=normalized_symbol)
    db_ticker_content_cache_set(
        "news",
        normalized_symbol,
        cached_payload,
        window_key="latest",
        cache_key=ticker_content_cache_key("news", normalized_symbol, "latest"),
    )
    return cached_payload


def get_press_releases(*, symbol: str, page: int = 0, limit: int = 20) -> dict[str, Any]:
    normalized_symbol = _normalize_symbol(symbol)
    bounded_page = max(int(page or 0), 0)
    bounded_limit = max(1, min(int(limit or 20), 50))
    if not normalized_symbol:
        return _payload_from_items([], page=0, limit=bounded_limit, has_next=False)

    active_panel_request = _is_active_ticker_panel_request({"TickerPressPanel"})
    cache_key = _cache_key("press-releases", {"symbol": normalized_symbol, "page": bounded_page, "limit": bounded_limit})
    cached = _cache_get(cache_key, category="news:press-releases", symbol=normalized_symbol)
    if _payload_has_items(cached):
        return cached
    record_cache_miss(category="news:press-releases", symbol=normalized_symbol)
    db_cached = db_ticker_content_cache_get(
        "press_releases",
        normalized_symbol,
        page=bounded_page,
        limit=bounded_limit,
        window_key="latest",
    )
    if db_cached is not None and (_payload_has_items(db_cached) or not active_panel_request):
        return _cache_set(cache_key, db_cached, ttl_seconds=PRESS_RELEASES_TTL_SECONDS, category="news:press-releases", symbol=normalized_symbol)
    if cached is not None and not active_panel_request:
        return cached
    if _is_public_request_context():
        return _public_cache_miss_payload(
            cache_key=cache_key,
            category="news:press-releases",
            symbol=normalized_symbol,
            page=bounded_page,
            limit=bounded_limit,
            job_type="press_releases",
            stale_message=TICKER_PRESS_UNAVAILABLE_MESSAGE,
        )

    try:
        rows, error_payload = _request_ticker_press_rows(symbol=normalized_symbol, page=bounded_page, limit=bounded_limit)
    except FMPNewsUnavailable as exc:
        reason = reason_from_exception(exc)
        record_fallback(category="news:press-releases", symbol=normalized_symbol, reason=reason)
        _enqueue_news_refresh(
            job_type="press_releases",
            symbol=normalized_symbol,
            reason=reason,
            payload={"page": bounded_page, "limit": bounded_limit},
        )
        stale = _cache_get_stale(cache_key, category="news:press-releases", symbol=normalized_symbol)
        if stale is not None:
            stale_payload, age = stale
            record_fallback(category="news:press-releases", symbol=normalized_symbol, reason=reason, cache_age_seconds=age)
            return _stale_payload(stale_payload, reason=reason, message=TICKER_PRESS_UNAVAILABLE_MESSAGE, age_seconds=age)
        if _is_public_request_context():
            return _warming_payload(page=bounded_page, limit=bounded_limit)
        payload = _unavailable_payload(page=bounded_page, limit=bounded_limit, message=TICKER_PRESS_UNAVAILABLE_MESSAGE, reason=reason)
        return _cache_set(cache_key, payload, ttl_seconds=PRESS_RELEASES_TTL_SECONDS, category="news:press-releases", symbol=normalized_symbol)

    if error_payload is not None:
        reason = str(error_payload.get("reason") or "provider_unavailable")
        _enqueue_news_refresh(
            job_type="press_releases",
            symbol=normalized_symbol,
            reason=reason,
            payload={"page": bounded_page, "limit": bounded_limit},
        )
        stale = _cache_get_stale(cache_key, category="news:press-releases", symbol=normalized_symbol)
        if stale is not None:
            stale_payload, age = stale
            record_fallback(category="news:press-releases", symbol=normalized_symbol, reason=reason, cache_age_seconds=age)
            return _stale_payload(stale_payload, reason=reason, message=TICKER_PRESS_UNAVAILABLE_MESSAGE, age_seconds=age)
        if _is_public_request_context():
            return _warming_payload(page=bounded_page, limit=bounded_limit)
        return _cache_set(cache_key, error_payload, ttl_seconds=PRESS_RELEASES_TTL_SECONDS, category="news:press-releases", symbol=normalized_symbol)

    items = _normalize_symbol_rows(rows, symbol=normalized_symbol, normalizer=_normalize_press_row, strict_symbol_filter=False)
    if not items:
        payload = {
            **_payload_from_items([], page=bounded_page, limit=bounded_limit, has_next=False),
            "message": TICKER_PRESS_EMPTY_MESSAGE,
        }
        return _cache_set(cache_key, payload, ttl_seconds=PRESS_RELEASES_TTL_SECONDS, category="news:press-releases", symbol=normalized_symbol)

    payload = _payload_from_items(items[:bounded_limit], page=bounded_page, limit=bounded_limit, has_next=len(rows) >= bounded_limit)
    cached_payload = _cache_set(cache_key, payload, ttl_seconds=PRESS_RELEASES_TTL_SECONDS, category="news:press-releases", symbol=normalized_symbol)
    db_ticker_content_cache_set(
        "press_releases",
        normalized_symbol,
        cached_payload,
        window_key="latest",
        cache_key=ticker_content_cache_key("press_releases", normalized_symbol, "latest"),
    )
    return cached_payload


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
    default_from = today - timedelta(days=365)
    from_value = from_date or default_from.isoformat()
    to_value = to_date or today.isoformat()
    window_key = ticker_content_window_key("sec_filings", from_date=from_value, to_date=to_value)
    active_panel_request = _is_active_ticker_panel_request({"TickerFilingsPanel"})
    cache_key = _cache_key(
        "sec-filings",
        {"symbol": normalized_symbol, "from": from_value, "to": to_value, "page": bounded_page, "limit": bounded_limit},
    )
    cached = _cache_get(cache_key, category="news:sec-filings", symbol=normalized_symbol)
    if _payload_has_items(cached):
        return cached
    record_cache_miss(category="news:sec-filings", symbol=normalized_symbol)
    db_cached = db_ticker_content_cache_get(
        "sec_filings",
        normalized_symbol,
        page=bounded_page,
        limit=bounded_limit,
        window_key=window_key,
        from_date=from_value,
        to_date=to_value,
    )
    if db_cached is not None and (_payload_has_items(db_cached) or not active_panel_request):
        return _cache_set(cache_key, db_cached, ttl_seconds=SEC_FILINGS_TTL_SECONDS, category="news:sec-filings", symbol=normalized_symbol)
    if cached is not None and not active_panel_request:
        return cached
    if _is_public_request_context():
        return _public_cache_miss_payload(
            cache_key=cache_key,
            category="news:sec-filings",
            symbol=normalized_symbol,
            page=bounded_page,
            limit=bounded_limit,
            job_type="sec_filings",
            stale_message=TICKER_SEC_UNAVAILABLE_MESSAGE,
            payload={"from_date": from_value, "to_date": to_value, "page": bounded_page, "limit": bounded_limit},
        )

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
        cached_payload = _cache_set(cache_key, direct_payload, ttl_seconds=SEC_FILINGS_TTL_SECONDS, category="news:sec-filings", symbol=normalized_symbol)
        db_ticker_content_cache_set(
            "sec_filings",
            normalized_symbol,
            cached_payload,
            window_key=window_key,
            cache_key=ticker_content_cache_key("sec_filings", normalized_symbol, window_key),
        )
        return cached_payload

    if provider_failed:
        _enqueue_news_refresh(
            job_type="sec_filings",
            symbol=normalized_symbol,
            reason="provider_unavailable",
            payload={"from_date": from_value, "to_date": to_value, "page": bounded_page, "limit": bounded_limit},
        )
        stale = _cache_get_stale(cache_key, category="news:sec-filings", symbol=normalized_symbol)
        if stale is not None:
            stale_payload, age = stale
            record_fallback(category="news:sec-filings", symbol=normalized_symbol, reason="provider_unavailable", cache_age_seconds=age)
            return _stale_payload(stale_payload, reason="provider_unavailable", message=TICKER_SEC_UNAVAILABLE_MESSAGE, age_seconds=age)
        if _is_public_request_context():
            return _warming_payload(page=bounded_page, limit=bounded_limit)
        payload = _unavailable_payload(
            page=bounded_page,
            limit=bounded_limit,
            message=TICKER_SEC_UNAVAILABLE_MESSAGE,
        )
        return payload
    payload = _payload_from_items([], page=bounded_page, limit=bounded_limit, has_next=False)
    return _cache_set(cache_key, payload, ttl_seconds=SEC_FILINGS_TTL_SECONDS, category="news:sec-filings", symbol=normalized_symbol)
