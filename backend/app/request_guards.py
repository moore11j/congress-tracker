from __future__ import annotations

import logging
import re
from urllib.parse import urlparse

from fastapi import Request
from fastapi.responses import Response

from app.auth import SESSION_COOKIE_NAME

_SAFE_TIER_VALUES = {"logged_out", "free", "premium", "pro", "admin"}


def bounded_log_value(value: str | None, *, max_length: int = 96) -> str:
    cleaned = re.sub(r"\s+", " ", (value or "").strip())
    if not cleaned:
        return "none"
    return cleaned[:max_length]


def classify_user_agent(request: Request) -> str:
    headers = getattr(request, "headers", None)
    header = lambda name: str((headers.get(name) if headers is not None else "") or "").lower()
    purpose = header("purpose")
    sec_purpose = header("sec-purpose")
    next_router_prefetch = header("next-router-prefetch")
    middleware_prefetch = header("x-middleware-prefetch")
    nextjs_data = header("x-nextjs-data")
    walnut_source = header("x-walnut-request-source")
    if (
        purpose == "prefetch"
        or "prefetch" in sec_purpose
        or next_router_prefetch in {"1", "true", "prefetch"}
        or middleware_prefetch in {"1", "true", "prefetch"}
        or nextjs_data in {"1", "true", "prefetch"}
        or walnut_source == "prefetch"
    ):
        return "prefetch"
    user_agent = header("user-agent")
    if not user_agent:
        return "unknown"
    crawler_terms = ("googlebot", "bingbot", "slurp", "duckduckbot", "baiduspider", "yandexbot", "semrushbot", "ahrefsbot")
    bot_terms = ("bot", "crawler", "spider", "preview", "facebookexternalhit", "linkedinbot", "twitterbot", "uptimerobot")
    if any(term in user_agent for term in crawler_terms):
        return "crawler"
    if any(term in user_agent for term in bot_terms):
        return "bot"
    browser_terms = ("mozilla", "chrome", "safari", "firefox", "edg/", "opr/")
    if any(term in user_agent for term in browser_terms):
        return "browser"
    return "unknown"


def is_explicit_prefetch_request(request: Request) -> bool:
    return classify_user_agent(request) == "prefetch"


def request_auth_state(request: Request) -> tuple[str, str]:
    tier = (request.headers.get("x-ct-entitlement-tier") or request.cookies.get("ct_entitlement_hint") or "").strip().lower()
    if tier not in _SAFE_TIER_VALUES:
        tier = "unknown"
    if tier == "admin":
        return "admin", "admin"
    has_session = bool(request.cookies.get(SESSION_COOKIE_NAME))
    if has_session:
        return "logged_in", tier if tier != "unknown" else "unknown"
    if tier in {"free", "premium", "pro"}:
        return "logged_in", tier
    return "logged_out", "logged_out"


def request_source(request: Request, user_agent_class: str) -> str:
    raw = bounded_log_value(request.headers.get("x-walnut-request-source"), max_length=32).lower()
    if raw in {
        "ssr",
        "client",
        "client_fetch",
        "prefetch",
        "visibility",
        "idle",
        "bot_shell",
        "prefetch_204",
        "cron",
        "direct_api",
        "monitor_probe",
    }:
        return "client" if raw == "client_fetch" else raw
    if bounded_log_value(request.headers.get("x-walnut-monitor-probe"), max_length=16).lower() in {"1", "true", "yes", "monitor"}:
        return "monitor_probe"
    if "codex-prod-monitor" in bounded_log_value(request.headers.get("user-agent"), max_length=120).lower():
        return "monitor_probe"
    if user_agent_class == "prefetch":
        return "prefetch"
    if user_agent_class in {"bot", "crawler"}:
        return "bot_shell"
    if request.headers.get("x-supercronic") or request.headers.get("x-walnut-cron"):
        return "cron"
    if is_logged_out_direct_api_request(request):
        return "direct_api"
    return "unknown"


def sanitize_referer(value: str | None) -> tuple[str, str]:
    if not value:
        return "none", "none"
    try:
        parsed = urlparse(value)
    except Exception:
        return "invalid", "invalid"
    host = bounded_log_value(parsed.netloc.lower(), max_length=80)
    path = bounded_log_value(parsed.path or "/", max_length=120)
    return host, path


def is_logged_out_bot_or_crawler_request(request: Request) -> bool:
    if not hasattr(request, "headers"):
        return False
    if classify_user_agent(request) not in {"bot", "crawler"}:
        return False
    auth_state, _plan_tier = request_auth_state(request)
    return auth_state == "logged_out"


def is_inactive_logged_out_ssr_request(request: Request) -> bool:
    if not hasattr(request, "headers"):
        return False
    active_marker = bounded_log_value(request.headers.get("x-walnut-active-user"), max_length=16).lower()
    if active_marker in {"1", "true", "yes", "browser"}:
        return False
    auth_state, _plan_tier = request_auth_state(request)
    if auth_state != "logged_out":
        return False
    user_agent_class = classify_user_agent(request)
    if user_agent_class != "unknown":
        return False
    if request_source(request, user_agent_class) != "ssr":
        return False
    route_family = bounded_log_value(request.headers.get("x-walnut-route-family"), max_length=32).lower()
    if route_family in {"feed", "insider"}:
        return False
    referer_host, _referer_path = sanitize_referer(request.headers.get("referer"))
    return referer_host == "none"


def is_inactive_logged_out_api_request(request: Request) -> bool:
    return is_logged_out_bot_or_crawler_request(request) or is_inactive_logged_out_ssr_request(request)


def _looks_like_browser_page_navigation(request: Request) -> bool:
    accept = bounded_log_value(request.headers.get("accept"), max_length=160).lower()
    sec_fetch_mode = bounded_log_value(request.headers.get("sec-fetch-mode"), max_length=32).lower()
    sec_fetch_dest = bounded_log_value(request.headers.get("sec-fetch-dest"), max_length=32).lower()
    if "text/html" in accept:
        return True
    return sec_fetch_mode == "navigate" or sec_fetch_dest in {"document", "iframe"}


def is_logged_out_direct_api_request(request: Request) -> bool:
    if not hasattr(request, "headers"):
        return False
    if getattr(getattr(request, "url", None), "path", "") and not request.url.path.startswith("/api/"):
        return False
    auth_state, _plan_tier = request_auth_state(request)
    if auth_state != "logged_out":
        return False
    if sanitize_referer(request.headers.get("referer"))[0] != "none":
        return False
    if _looks_like_browser_page_navigation(request):
        return False
    raw_source = bounded_log_value(request.headers.get("x-walnut-request-source"), max_length=32).lower()
    if raw_source in {"ssr", "client", "visibility", "idle"}:
        return False
    active_marker = bounded_log_value(request.headers.get("x-walnut-active-user"), max_length=16).lower()
    if active_marker in {"1", "true", "yes", "browser"}:
        return False
    return True


def api_prefetch_response(
    request: Request | None,
    *,
    endpoint: str,
    logger: logging.Logger | None = None,
) -> Response | None:
    if request is None or not is_explicit_prefetch_request(request):
        return None
    if logger is not None:
        logger.info(
            "api_prefetch_bypass endpoint=%s path=%s panel=%s",
            endpoint,
            request.url.path,
            request.headers.get("x-walnut-panel") or request.headers.get("x-walnut-component") or "unknown",
        )
    return Response(status_code=204, headers={"cache-control": "no-store", "x-walnut-prefetch-bypass": "1"})
