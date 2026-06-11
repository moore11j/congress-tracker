from __future__ import annotations

import logging
import os
import time
from typing import Any

import requests

from app.clients.fmp import FMP_BASE_URL
from app.services.provider_usage import (
    ProviderUnavailable,
    ensure_fmp_live_allowed,
    reason_for_status,
    record_fallback,
    record_provider_response,
)

logger = logging.getLogger(__name__)


class FMPControlledError(RuntimeError):
    def __init__(self, reason: str, message: str | None = None):
        super().__init__(message or reason)
        self.reason = reason


def fmp_provider_enabled() -> bool:
    return os.getenv("FMP_PROVIDER_ENABLED", "true").strip().lower() not in {"0", "false", "no", "off"}


def live_fetch_allowed_for_source(source: str, *, allow_live_fetch: bool = False) -> bool:
    if allow_live_fetch:
        return True
    if source == "page_load":
        return os.getenv("FMP_LIVE_FETCH_ON_PAGE_LOAD", "false").strip().lower() in {"1", "true", "yes", "on"}
    if source in {"scheduled_job", "admin_refresh", "explicit_user_refresh"}:
        return True
    return os.getenv("FMP_CACHE_MISS_LIVE_FETCH_ENABLED", "false").strip().lower() in {"1", "true", "yes", "on"}


def request_fmp_json(
    endpoint: str,
    *,
    params: dict[str, Any] | None = None,
    category: str,
    symbol: str | None = None,
    source: str = "page_load",
    timeout_s: float = 10,
    allow_live_fetch: bool = False,
) -> Any:
    if not fmp_provider_enabled():
        record_fallback(category=category, symbol=symbol, reason="provider_disabled")
        raise FMPControlledError("provider_disabled")
    if not live_fetch_allowed_for_source(source, allow_live_fetch=allow_live_fetch):
        record_fallback(category=category, symbol=symbol, reason="provider_disabled")
        raise FMPControlledError("provider_disabled")
    api_key = os.getenv("FMP_API_KEY", "").strip()
    if not api_key:
        record_fallback(category=category, symbol=symbol, reason="provider_disabled")
        raise FMPControlledError("provider_disabled")

    try:
        ensure_fmp_live_allowed(category=category, symbol=symbol)
    except ProviderUnavailable as exc:
        raise FMPControlledError(getattr(exc, "reason", "provider_unavailable")) from exc

    request_params = {"apikey": api_key}
    for key, value in (params or {}).items():
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        request_params[key] = value

    started = time.perf_counter()
    try:
        response = requests.get(f"{FMP_BASE_URL}/{endpoint}", params=request_params, timeout=timeout_s)
    except requests.RequestException as exc:
        logger.info("fmp_client request failed category=%s endpoint=%s symbol=%s error=%s", category, endpoint, symbol, exc)
        record_fallback(category=category, symbol=symbol, reason="provider_error")
        raise FMPControlledError("provider_error") from exc

    duration_ms = round((time.perf_counter() - started) * 1000, 1)
    record_provider_response(category=category, symbol=symbol, status_code=response.status_code)
    if response.status_code >= 400:
        reason = reason_for_status(response.status_code)
        record_fallback(category=category, symbol=symbol, reason=reason)
        logger.info(
            "fmp_client controlled_error category=%s endpoint=%s symbol=%s status=%s duration_ms=%s",
            category,
            endpoint,
            symbol,
            response.status_code,
            duration_ms,
        )
        raise FMPControlledError(reason)
    try:
        return response.json()
    except ValueError as exc:
        record_fallback(category=category, symbol=symbol, reason="provider_error")
        raise FMPControlledError("provider_error") from exc
