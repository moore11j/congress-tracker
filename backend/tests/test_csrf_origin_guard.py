from __future__ import annotations

from starlette.requests import Request

from app.auth import SESSION_COOKIE_NAME
from app.main import _csrf_origin_allowed, _csrf_origin_check_required


def _request(headers: dict[str, str] | None = None) -> Request:
    raw_headers = [
        (name.lower().encode("latin-1"), value.encode("latin-1"))
        for name, value in (headers or {}).items()
    ]
    return Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/api/account/resend-verification",
            "headers": raw_headers,
            "query_string": b"",
            "scheme": "https",
            "server": ("congress-tracker-api.fly.dev", 443),
        }
    )


def _session_cookie() -> str:
    return f"{SESSION_COOKIE_NAME}=test-session"


def test_csrf_origin_guard_allows_valid_app_origin():
    request = _request({"cookie": _session_cookie(), "origin": "https://app.walnutmarkets.com"})

    assert _csrf_origin_check_required(request) is True
    assert _csrf_origin_allowed(request) is True


def test_csrf_origin_guard_allows_valid_app_referer():
    request = _request({"cookie": _session_cookie(), "referer": "https://app.walnutmarkets.com/account/settings"})

    assert _csrf_origin_check_required(request) is True
    assert _csrf_origin_allowed(request) is True


def test_csrf_origin_guard_rejects_authenticated_post_without_origin_or_referer():
    request = _request({"cookie": _session_cookie()})

    assert _csrf_origin_check_required(request) is True
    assert _csrf_origin_allowed(request) is False
