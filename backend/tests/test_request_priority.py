from __future__ import annotations

from app.request_priority import RoutePriority, classify_request


def test_ticker_hydration_request_is_not_heavy_route_gated():
    assert classify_request("/api/tickers/NBIS/hydration-request", {}) == RoutePriority.NORMAL


def test_ticker_shell_is_not_heavy_route_gated():
    assert classify_request("/api/tickers/NBIS", {}) == RoutePriority.NORMAL


def test_ticker_hydration_status_is_not_heavy_route_gated():
    assert classify_request("/api/tickers/NBIS/hydration-status", {}) == RoutePriority.NORMAL


def test_ticker_signals_summary_is_not_heavy_route_gated():
    assert classify_request("/api/tickers/NBIS/signals-summary", {}) == RoutePriority.NORMAL


def test_ticker_cache_first_section_routes_are_not_outer_heavy_gated():
    for suffix in ("chart-bundle", "financials", "news", "press-releases", "sec-filings"):
        assert classify_request(f"/api/tickers/NBIS/{suffix}", {}) == RoutePriority.NORMAL
