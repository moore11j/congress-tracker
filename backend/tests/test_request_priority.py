from __future__ import annotations

from starlette.requests import Request

from app.main import (
    _analytics_panel_name,
    _classify_user_agent,
    _is_secondary_analytics_path,
    _request_attribution_fields,
    _request_route_family,
    _sanitize_referer,
    _should_log_request_attribution,
)
from app.request_priority import RoutePriority, classify_request


def _request(path: str, headers: dict[str, str] | None = None) -> Request:
    raw_headers = [
        (key.lower().encode("latin-1"), value.encode("latin-1"))
        for key, value in (headers or {}).items()
    ]
    return Request(
        {
            "type": "http",
            "method": "GET",
            "path": path,
            "raw_path": path.encode("ascii"),
            "query_string": b"",
            "headers": raw_headers,
            "server": ("testserver", 80),
            "scheme": "https",
        }
    )


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


def test_request_attribution_classifies_bots_prefetch_and_browsers():
    assert _classify_user_agent(_request("/api/events", {"user-agent": "Mozilla/5.0 Chrome/126"})) == "browser"
    assert _classify_user_agent(_request("/api/events", {"user-agent": "Googlebot/2.1"})) == "crawler"
    assert _classify_user_agent(_request("/api/events", {"user-agent": "Some uptime bot"})) == "bot"
    assert _classify_user_agent(_request("/api/events", {"user-agent": "Mozilla/5.0", "purpose": "prefetch"})) == "prefetch"
    assert _classify_user_agent(_request("/api/events")) == "unknown"


def test_request_attribution_sanitizes_referer_and_secrets():
    request = _request(
        "/api/tickers/AAPL/signals-summary",
        {
            "host": "congress-tracker-api.fly.dev",
            "user-agent": "Mozilla/5.0 Chrome/126",
            "referer": "https://app.walnutmarkets.com/ticker/AAPL?token=secret",
            "cookie": "ct_session=secret-session-token",
            "x-ct-entitlement-tier": "admin",
            "x-walnut-route-family": "ticker",
            "x-walnut-panel": "TickerSignalsSummary",
            "x-walnut-request-source": "ssr",
        },
    )

    fields = _request_attribution_fields(request, priority=RoutePriority.NORMAL)

    assert fields["route_family"] == "ticker"
    assert fields["panel"] == "TickerSignalsSummary"
    assert fields["auth_state"] == "admin"
    assert fields["plan_tier"] == "admin"
    assert fields["request_source"] == "ssr"
    assert fields["referer_host"] == "app.walnutmarkets.com"
    assert fields["referer_path"] == "/ticker/AAPL"
    assert "secret" not in str(fields).lower()
    assert "ct_session" not in str(fields).lower()


def test_request_attribution_route_family_fallbacks():
    assert _request_route_family("/api/events") == "feed"
    assert _request_route_family("/api/market/quotes") == "market_quotes"
    assert _request_route_family("/api/insiders/0001/trades") == "insider"
    assert _request_route_family("/institution/0001067983") == "institution"


def test_request_attribution_logs_slow_degraded_and_sampled_fast(monkeypatch):
    assert _should_log_request_attribution(
        path="/api/events",
        status_code=503,
        duration_ms=10,
        priority=RoutePriority.NORMAL,
    )
    assert _should_log_request_attribution(
        path="/api/events",
        status_code=200,
        duration_ms=2500,
        priority=RoutePriority.NORMAL,
    )
    monkeypatch.setenv("WALNUT_REQUEST_ATTRIBUTION_SAMPLE_RATE", "0")
    assert not _should_log_request_attribution(
        path="/api/events",
        status_code=200,
        duration_ms=10,
        priority=RoutePriority.NORMAL,
    )
    monkeypatch.setenv("WALNUT_REQUEST_ATTRIBUTION_DEBUG", "true")
    assert _should_log_request_attribution(
        path="/api/events",
        status_code=200,
        duration_ms=10,
        priority=RoutePriority.NORMAL,
    )
