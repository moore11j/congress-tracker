from __future__ import annotations

from starlette.requests import Request

from app.main import (
    _analytics_panel_name,
    _classify_user_agent,
    _is_inactive_logged_out_ssr_request,
    _is_secondary_analytics_path,
    _log_request_attribution,
    _request_attribution_fields,
    _request_route_family,
    _request_source,
    _sanitize_referer,
    _should_bypass_heavy_route_slot,
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


def test_insider_profile_identity_and_recent_trades_are_not_heavy_route_gated():
    assert classify_request("/api/insiders/0001824159/summary", {}) == RoutePriority.NORMAL
    assert classify_request("/api/insiders/0001824159/trades", {}) == RoutePriority.NORMAL
    assert classify_request("/api/insiders/0001824159/trades", {"limit": "50"}) == RoutePriority.NORMAL


def test_insider_profile_broad_and_analytics_routes_use_heavy_lane():
    assert classify_request("/api/insiders/0001824159/trades", {"limit": "51"}) == RoutePriority.HEAVY
    assert classify_request("/api/insiders/0001824159/alpha-summary", {}) == RoutePriority.HEAVY
    assert classify_request("/api/insiders/0001824159/top-tickers", {}) == RoutePriority.HEAVY
    assert classify_request("/api/insiders/0001824159/stock-chart", {}) == RoutePriority.HEAVY


def test_member_profile_secondary_routes_use_heavy_lane():
    assert classify_request("/api/members/P000197/performance", {}) == RoutePriority.HEAVY
    assert classify_request("/api/members/P000197/portfolio-performance", {}) == RoutePriority.HEAVY
    assert classify_request("/api/members/P000197/trades", {}) == RoutePriority.HEAVY
    assert classify_request("/api/members/P000197/alpha-summary", {}) == RoutePriority.HEAVY


def test_inactive_logged_out_heavy_routes_bypass_outer_heavy_gate():
    assert _should_bypass_heavy_route_slot(
        _request("/api/insiders/0002005219/summary", {"x-walnut-request-source": "ssr"}),
        RoutePriority.HEAVY,
    )
    assert _should_bypass_heavy_route_slot(
        _request("/api/insiders/0002005219/trades", {"x-walnut-request-source": "ssr"}),
        RoutePriority.HEAVY,
    )


def test_active_or_logged_in_heavy_routes_still_use_outer_heavy_gate():
    assert not _should_bypass_heavy_route_slot(
        _request(
            "/api/insiders/0002005219/summary",
            {
                "x-walnut-request-source": "ssr",
                "referer": "https://app.walnutmarkets.com/insider/vivo-opportunity-llc-0002005219",
                "x-walnut-active-user": "browser",
            },
        ),
        RoutePriority.HEAVY,
    )
    assert not _should_bypass_heavy_route_slot(
        _request(
            "/api/insiders/0002005219/summary",
            {"x-walnut-request-source": "ssr", "cookie": "ct_session=session-id"},
        ),
        RoutePriority.HEAVY,
    )
    assert not _should_bypass_heavy_route_slot(
        _request("/api/events", {"x-walnut-request-source": "ssr"}),
        RoutePriority.NORMAL,
    )


def test_secondary_analytics_fail_soft_includes_member_trades():
    assert _is_secondary_analytics_path("/api/members/P000197/performance")
    assert _is_secondary_analytics_path("/api/members/P000197/portfolio-performance")
    assert _is_secondary_analytics_path("/api/members/P000197/trades")
    assert _is_secondary_analytics_path("/api/members/P000197/alpha-summary")
    assert _analytics_panel_name("/api/members/P000197/trades") == "trades"


def test_core_routes_stay_outside_insider_heavy_lane():
    assert classify_request("/api/events", {"limit": "5", "enrich_prices": "0"}) == RoutePriority.NORMAL
    assert classify_request("/api/events", {"limit": "10", "enrich_prices": "1"}) == RoutePriority.NORMAL
    assert classify_request("/api/events", {"limit": "50", "enrich_prices": "1"}) == RoutePriority.NORMAL
    assert classify_request("/api/tickers/AAPL/signals-summary", {}) == RoutePriority.NORMAL
    assert classify_request("/api/tickers/NVDA/government-contracts", {}) == RoutePriority.NORMAL
    assert classify_request("/api/tickers/AAPL", {}) == RoutePriority.NORMAL


def test_broad_or_filter_heavy_events_still_use_heavy_lane():
    assert classify_request("/api/events", {"limit": "51", "enrich_prices": "1"}) == RoutePriority.HEAVY
    assert classify_request("/api/events", {"limit": "51", "enrich_prices": "0"}) == RoutePriority.HEAVY
    assert classify_request("/api/events", {"limit": "50", "enrich_prices": "1", "include_total": "1"}) == RoutePriority.HEAVY
    assert classify_request("/api/events", {"limit": "50", "enrich_prices": "1", "pnl_min": "10"}) == RoutePriority.HEAVY


def test_ticker_cache_first_section_routes_are_not_outer_heavy_gated():
    for suffix in (
        "chart-bundle",
        "context-bundle",
        "financials",
        "government-contracts",
        "news",
        "press-releases",
        "sec-filings",
    ):
        assert classify_request(f"/api/tickers/NBIS/{suffix}", {}) == RoutePriority.NORMAL


def test_symbol_scoped_events_with_legacy_enrich_param_are_not_heavy_route_gated():
    assert classify_request("/api/events", {"symbol": "NBIS", "enrich_prices": "1"}) == RoutePriority.NORMAL
    assert classify_request("/api/events", {"ticker": "NBIS", "enrich_prices": "true"}) == RoutePriority.NORMAL


def test_request_attribution_classifies_bots_prefetch_and_browsers():
    assert _classify_user_agent(_request("/api/events", {"user-agent": "Mozilla/5.0 Chrome/126"})) == "browser"
    assert _classify_user_agent(_request("/api/events", {"user-agent": "Googlebot/2.1"})) == "crawler"
    assert _classify_user_agent(_request("/api/events", {"user-agent": "Some uptime bot"})) == "bot"
    assert _classify_user_agent(_request("/api/events", {"user-agent": "Mozilla/5.0", "purpose": "prefetch"})) == "prefetch"
    assert _classify_user_agent(_request("/api/events", {"user-agent": "Mozilla/5.0", "next-router-prefetch": "1"})) == "prefetch"
    assert _classify_user_agent(_request("/api/events", {"user-agent": "Mozilla/5.0", "x-walnut-request-source": "prefetch"})) == "prefetch"
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


def test_request_attribution_classifies_logged_out_direct_api():
    request = _request(
        "/api/tickers/AAPL/context-bundle",
        {
            "accept": "application/json",
            "user-agent": "Mozilla/5.0 Chrome/126",
        },
    )

    fields = _request_attribution_fields(request, priority=RoutePriority.NORMAL)

    assert _request_source(request, "browser") == "direct_api"
    assert fields["request_source"] == "direct_api"
    assert fields["referer_host"] == "none"
    assert fields["user_agent_class"] == "browser"
    assert fields["accept"] == "application/json"


def test_request_attribution_keeps_browser_navigation_out_of_direct_api():
    request = _request(
        "/api/tickers/AAPL/context-bundle",
        {
            "accept": "text/html,application/xhtml+xml",
            "sec-fetch-mode": "navigate",
            "sec-fetch-dest": "document",
            "user-agent": "Mozilla/5.0 Chrome/126",
        },
    )

    assert _request_source(request, "browser") == "unknown"


def test_inactive_logged_out_ssr_requires_unknown_ua_no_referer_and_no_auth():
    assert _is_inactive_logged_out_ssr_request(
        _request("/api/tickers/AAPL/context-bundle", {"x-walnut-request-source": "ssr"})
    )
    assert not _is_inactive_logged_out_ssr_request(
        _request("/api/tickers/AAPL/context-bundle", {"x-walnut-request-source": "ssr", "user-agent": "Mozilla/5.0 Chrome/126"})
    )
    assert not _is_inactive_logged_out_ssr_request(
        _request("/api/tickers/AAPL/context-bundle", {"x-walnut-request-source": "ssr", "referer": "https://app.walnutmarkets.com/ticker/AAPL"})
    )
    assert not _is_inactive_logged_out_ssr_request(
        _request("/api/tickers/AAPL/context-bundle", {"x-walnut-request-source": "client"})
    )
    assert not _is_inactive_logged_out_ssr_request(
        _request("/api/tickers/AAPL/context-bundle", {"x-walnut-request-source": "ssr", "x-walnut-active-user": "browser"})
    )
    assert _is_inactive_logged_out_ssr_request(
        _request(
            "/api/insiders/0001728970/summary",
            {"x-walnut-request-source": "ssr", "x-walnut-route-family": "insider"},
        )
    )
    assert not _is_inactive_logged_out_ssr_request(
        _request(
            "/api/events",
            {"x-walnut-request-source": "ssr", "x-walnut-route-family": "feed"},
        )
    )


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
    assert _should_log_request_attribution(
        path="/api/events",
        status_code=200,
        duration_ms=10,
        priority=RoutePriority.NORMAL,
        user_agent_class="prefetch",
    )
    monkeypatch.setenv("WALNUT_REQUEST_ATTRIBUTION_DEBUG", "true")
    assert _should_log_request_attribution(
        path="/api/events",
        status_code=200,
        duration_ms=10,
        priority=RoutePriority.NORMAL,
    )


def test_request_attribution_bot_prefetch_is_warning_visible(caplog):
    request = _request(
        "/api/tickers/AAPL/signals-summary",
        {
            "host": "congress-tracker-api.fly.dev",
            "user-agent": "Googlebot/2.1",
            "purpose": "prefetch",
            "x-walnut-route-family": "ticker",
            "x-walnut-panel": "AttributionProbe",
            "x-walnut-request-source": "prefetch",
        },
    )

    _log_request_attribution(
        request,
        status_code=200,
        duration_ms=12.3,
        priority=RoutePriority.NORMAL,
        reason="sampled",
    )

    assert "request_attribution" in caplog.text
    assert "reason=bot_prefetch" in caplog.text
    assert "ua_class=prefetch" in caplog.text
