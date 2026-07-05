from __future__ import annotations

from starlette.requests import Request

from app.main import _is_public_get_cacheable_path, _public_get_cache_key


def _request(path: str, *, headers: dict[str, str] | None = None) -> Request:
    raw_headers = [
        (key.lower().encode("latin-1"), value.encode("latin-1"))
        for key, value in (headers or {}).items()
    ]
    return Request(
        {
            "type": "http",
            "method": "GET",
            "scheme": "https",
            "server": ("app.walnutmarkets.com", 443),
            "path": path.split("?", 1)[0],
            "query_string": path.split("?", 1)[1].encode("latin-1") if "?" in path else b"",
            "headers": raw_headers,
        }
    )


def test_public_get_response_cache_allowlist_only_market_data_paths():
    assert _is_public_get_cacheable_path("/api/feed")
    assert _is_public_get_cacheable_path("/api/events")
    assert _is_public_get_cacheable_path("/api/plan-config")
    assert _is_public_get_cacheable_path("/api/search/suggest")
    assert _is_public_get_cacheable_path("/api/tickers/AAPL")
    assert _is_public_get_cacheable_path("/api/tickers/AAPL/chart-bundle")
    assert _is_public_get_cacheable_path("/api/tickers/AAPL/signals-summary")
    assert _is_public_get_cacheable_path("/api/tickers/NVDA/government-contracts")
    assert _is_public_get_cacheable_path("/api/insiders/0001451612/summary")

    assert not _is_public_get_cacheable_path("/api/auth/me")
    assert not _is_public_get_cacheable_path("/api/entitlements")
    assert not _is_public_get_cacheable_path("/api/billing/customer-portal")
    assert not _is_public_get_cacheable_path("/api/watchlists")
    assert not _is_public_get_cacheable_path("/api/signals/all")
    assert not _is_public_get_cacheable_path("/api/admin/settings")


def test_public_get_response_cache_key_skips_user_specific_and_prefetch_variants():
    assert _public_get_cache_key(_request("/api/events?limit=25")) is not None
    assert _public_get_cache_key(_request("/api/tickers/AAPL/signals-summary")) is not None

    assert _public_get_cache_key(_request("/api/events", headers={"Authorization": "Bearer token"})) is None
    assert _public_get_cache_key(_request("/api/events", headers={"Cookie": "walnut_session=abc"})) is None
    assert _public_get_cache_key(_request("/api/events", headers={"Purpose": "prefetch"})) is None
    assert _public_get_cache_key(_request("/api/events", headers={"User-Agent": "Googlebot/2.1"})) is None


def test_public_get_response_cache_key_separates_request_source_variants():
    browser_key = _public_get_cache_key(
        _request(
            "/api/tickers/PLTR/signals-summary",
            headers={"User-Agent": "Mozilla/5.0", "X-Walnut-Request-Source": "app"},
        )
    )
    load_test_key = _public_get_cache_key(
        _request(
            "/api/tickers/PLTR/signals-summary",
            headers={"User-Agent": "k6/0.49.0", "X-Walnut-Request-Source": "load_test"},
        )
    )
    assert browser_key is not None
    assert load_test_key is not None
    assert browser_key != load_test_key
