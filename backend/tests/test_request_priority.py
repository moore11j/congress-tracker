from __future__ import annotations

from app.request_priority import RoutePriority, classify_request


def test_ticker_hydration_request_is_not_heavy_route_gated():
    assert classify_request("/api/tickers/NBIS/hydration-request", {}) == RoutePriority.NORMAL


def test_ticker_signals_summary_is_not_heavy_route_gated():
    assert classify_request("/api/tickers/NBIS/signals-summary", {}) == RoutePriority.NORMAL


def test_ticker_chart_bundle_remains_heavy():
    assert classify_request("/api/tickers/NBIS/chart-bundle", {}) == RoutePriority.HEAVY
