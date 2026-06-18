from __future__ import annotations

from contextvars import ContextVar
from enum import Enum
from typing import Mapping


class RoutePriority(str, Enum):
    CRITICAL = "critical"
    NORMAL = "normal"
    HEAVY = "heavy"


_REQUEST_CONTEXT: ContextVar[dict] = ContextVar("walnut_request_context", default={})


def set_request_context(context: dict):
    return _REQUEST_CONTEXT.set(context)


def reset_request_context(token) -> None:
    _REQUEST_CONTEXT.reset(token)


def get_request_context() -> dict:
    return _REQUEST_CONTEXT.get({})


def _param_value(query_params: Mapping[str, str], key: str) -> str:
    value = query_params.get(key, "")
    return str(value or "").strip().lower()


def _truthy_query_value(value: str) -> bool:
    return value not in {"", "0", "false", "no", "off", "none"}


def classify_request(path: str, query_params: Mapping[str, str]) -> RoutePriority:
    normalized_path = (path or "/").rstrip("/") or "/"
    lower_path = normalized_path.lower()

    if lower_path == "/health":
        return RoutePriority.CRITICAL

    critical_exact = {
        "/api/auth/me",
        "/api/search/global",
        "/api/admin/settings",
        "/api/entitlements",
        "/api/monitoring/unread-count",
    }
    if lower_path in critical_exact:
        return RoutePriority.CRITICAL

    critical_prefixes = (
        "/api/auth/google/",
        "/api/account/",
    )
    if lower_path.startswith(critical_prefixes):
        return RoutePriority.CRITICAL

    if lower_path == "/api/events":
        if _param_value(query_params, "symbol") or _param_value(query_params, "ticker"):
            return RoutePriority.NORMAL
        if _truthy_query_value(_param_value(query_params, "enrich_prices")):
            return RoutePriority.HEAVY
        return RoutePriority.NORMAL

    if lower_path == "/api/leaderboards/congress-traders":
        return RoutePriority.NORMAL

    if lower_path.startswith("/api/tickers/") and lower_path.endswith(
        (
            "/chart-bundle",
            "/financials",
            "/hydration-request",
            "/hydration-status",
            "/news",
            "/press-releases",
            "/sec-filings",
            "/signals-summary",
        )
    ):
        return RoutePriority.NORMAL

    if lower_path.startswith("/api/tickers/"):
        suffix = lower_path[len("/api/tickers/"):]
        if suffix and "/" not in suffix:
            return RoutePriority.NORMAL

    if lower_path.startswith("/api/insiders/") and lower_path.endswith("/trades"):
        return RoutePriority.NORMAL

    heavy_prefixes = (
        "/api/tickers/",
        "/api/insiders/",
        "/api/leaderboards/",
        "/api/backtests/",
        "/api/screener",
    )
    if lower_path.startswith(heavy_prefixes):
        return RoutePriority.HEAVY

    if lower_path == "/api/tickers":
        return RoutePriority.HEAVY

    if lower_path.startswith("/api/members/") and any(
        lower_path.endswith(suffix)
        for suffix in (
            "/performance",
            "/portfolio-performance",
            "/trades",
            "/alpha-summary",
        )
    ):
        return RoutePriority.HEAVY

    if lower_path == "/api/signals/all":
        return RoutePriority.HEAVY if _param_value(query_params, "symbol") else RoutePriority.NORMAL

    if lower_path.startswith("/api/watchlists/") and (
        lower_path.endswith("/events")
        or lower_path.endswith("/signals")
        or lower_path.endswith("/feed")
        or lower_path.endswith("/confirmation-monitoring/refresh")
    ):
        return RoutePriority.HEAVY

    normal_prefixes = (
        "/api/plan-config",
        "/api/insights/news",
        "/api/insights/macro-snapshot",
        "/api/watchlists",
        "/api/monitoring/inbox",
    )
    if lower_path.startswith(normal_prefixes):
        return RoutePriority.NORMAL

    return RoutePriority.NORMAL


def retry_after_for_priority(priority: RoutePriority | str) -> int:
    return 2 if str(priority) == RoutePriority.HEAVY.value else 1
