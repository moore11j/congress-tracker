from __future__ import annotations

from app.main import _is_public_get_cacheable_path


def test_public_get_response_cache_allowlist_only_market_data_paths():
    assert _is_public_get_cacheable_path("/api/feed")
    assert _is_public_get_cacheable_path("/api/events")
    assert _is_public_get_cacheable_path("/api/plan-config")
    assert _is_public_get_cacheable_path("/api/search/suggest")
    assert _is_public_get_cacheable_path("/api/tickers/AAPL")
    assert _is_public_get_cacheable_path("/api/tickers/AAPL/chart-bundle")
    assert _is_public_get_cacheable_path("/api/insiders/0001451612/summary")

    assert not _is_public_get_cacheable_path("/api/auth/me")
    assert not _is_public_get_cacheable_path("/api/entitlements")
    assert not _is_public_get_cacheable_path("/api/billing/customer-portal")
    assert not _is_public_get_cacheable_path("/api/watchlists")
    assert not _is_public_get_cacheable_path("/api/signals/all")
    assert not _is_public_get_cacheable_path("/api/admin/settings")
