from __future__ import annotations

from app.main import _analytics_panel_name, _is_secondary_analytics_path
from app.request_priority import RoutePriority, classify_request


def test_ticker_hydration_request_is_not_heavy_route_gated():
    assert classify_request("/api/tickers/NBIS/hydration-request", {}) == RoutePriority.NORMAL


def test_ticker_shell_is_not_heavy_route_gated():
    assert classify_request("/api/tickers/NBIS", {}) == RoutePriority.NORMAL


def test_ticker_hydration_status_is_not_heavy_route_gated():
    assert classify_request("/api/tickers/NBIS/hydration-status", {}) == RoutePriority.NORMAL


def test_ticker_signals_summary_is_not_heavy_route_gated():
    assert classify_request("/api/tickers/NBIS/signals-summary", {}) == RoutePriority.NORMAL


def test_insider_profile_secondary_routes_use_heavy_lane():
    assert classify_request("/api/insiders/0001824159/summary", {}) == RoutePriority.HEAVY
    assert classify_request("/api/insiders/0001824159/trades", {}) == RoutePriority.HEAVY
    assert classify_request("/api/insiders/0001824159/alpha-summary", {}) == RoutePriority.HEAVY
    assert classify_request("/api/insiders/0001824159/top-tickers", {}) == RoutePriority.HEAVY
    assert classify_request("/api/insiders/0001824159/stock-chart", {}) == RoutePriority.HEAVY


def test_member_profile_secondary_routes_use_heavy_lane():
    assert classify_request("/api/members/P000197/performance", {}) == RoutePriority.HEAVY
    assert classify_request("/api/members/P000197/portfolio-performance", {}) == RoutePriority.HEAVY
    assert classify_request("/api/members/P000197/trades", {}) == RoutePriority.HEAVY
    assert classify_request("/api/members/P000197/alpha-summary", {}) == RoutePriority.HEAVY


def test_secondary_analytics_fail_soft_includes_member_trades():
    assert _is_secondary_analytics_path("/api/members/P000197/performance")
    assert _is_secondary_analytics_path("/api/members/P000197/portfolio-performance")
    assert _is_secondary_analytics_path("/api/members/P000197/trades")
    assert _is_secondary_analytics_path("/api/members/P000197/alpha-summary")
    assert _analytics_panel_name("/api/members/P000197/trades") == "trades"


def test_core_routes_stay_outside_insider_heavy_lane():
    assert classify_request("/api/events", {"limit": "5", "enrich_prices": "0"}) == RoutePriority.NORMAL
    assert classify_request("/api/tickers/AAPL/signals-summary", {}) == RoutePriority.NORMAL
    assert classify_request("/api/tickers/NVDA/government-contracts", {}) == RoutePriority.NORMAL
    assert classify_request("/api/tickers/AAPL", {}) == RoutePriority.NORMAL


def test_ticker_cache_first_section_routes_are_not_outer_heavy_gated():
    for suffix in ("chart-bundle", "financials", "government-contracts", "news", "press-releases", "sec-filings"):
        assert classify_request(f"/api/tickers/NBIS/{suffix}", {}) == RoutePriority.NORMAL


def test_symbol_scoped_events_with_legacy_enrich_param_are_not_heavy_route_gated():
    assert classify_request("/api/events", {"symbol": "NBIS", "enrich_prices": "1"}) == RoutePriority.NORMAL
    assert classify_request("/api/events", {"ticker": "NBIS", "enrich_prices": "true"}) == RoutePriority.NORMAL
