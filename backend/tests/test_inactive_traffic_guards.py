from __future__ import annotations

from starlette.requests import Request

import app.main as main_module
import app.routers.events as events_module
import app.routers.institutional as institutional_module


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


def test_events_prefetch_returns_204_before_db_work():
    response = events_module.list_events(
        request=_request("/api/events", {"purpose": "prefetch", "x-walnut-request-source": "prefetch"}),
        db=object(),
        limit=5,
    )

    assert response.status_code == 204
    assert response.headers["x-walnut-prefetch-bypass"] == "1"


def test_events_unknown_logged_out_ssr_returns_lightweight_page(monkeypatch):
    monkeypatch.setattr(
        events_module,
        "_can_view_institutional_events",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("inactive SSR must not inspect entitlements")),
    )

    payload = events_module.list_events(
        request=_request("/api/events", {"x-walnut-request-source": "ssr"}),
        db=object(),
        limit=5,
        offset=0,
        include_total=True,
    )

    assert payload.items == []
    assert payload.has_more is False
    assert payload.total == 0


def test_insider_secondary_unknown_logged_out_ssr_does_not_load_events(monkeypatch):
    monkeypatch.setattr(
        events_module,
        "_load_insider_events_for_cik",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("inactive SSR must not load insider events")),
    )

    payload = events_module.insider_trades(
        "0001045810",
        _request("/api/insiders/0001045810/trades", {"x-walnut-request-source": "ssr"}),
        db=object(),
    )

    assert payload["status"] == "skipped"
    assert payload["items"] == []


def test_member_secondary_unknown_logged_out_ssr_does_not_resolve_member(monkeypatch):
    monkeypatch.setattr(
        main_module,
        "_resolve_member_analytics_aliases",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("inactive SSR must not resolve member analytics")),
    )

    payload = main_module.member_alpha_summary(
        "P000197",
        _request("/api/members/P000197/alpha-summary", {"x-walnut-request-source": "ssr"}),
        lookback_days=365,
        db=object(),
    )

    assert payload["status"] == "skipped"
    assert payload["trades_analyzed"] == 0


def test_chart_bundle_unknown_logged_out_ssr_does_not_enter_builder(monkeypatch):
    monkeypatch.setattr(
        main_module,
        "_coalesced_ticker_chart_bundle",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("inactive SSR must not build chart bundle")),
    )

    payload = main_module.ticker_chart_bundle(
        _request("/api/tickers/AAPL/chart-bundle", {"x-walnut-request-source": "ssr"}),
        "AAPL",
        days=365,
        db=object(),
    )

    assert payload["symbol"] == "AAPL"
    assert payload["status"] == "unavailable"
    assert payload["prices"] == []


def test_institution_profile_unknown_logged_out_ssr_does_not_check_entitlements(monkeypatch):
    monkeypatch.setattr(
        institutional_module,
        "current_entitlements",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("inactive SSR must not check entitlements")),
    )

    payload = institutional_module.institution_profile(
        "0001067983",
        _request("/api/institutions/0001067983", {"x-walnut-request-source": "ssr"}),
        db=object(),
    )

    assert payload["status"] == "skipped"
    assert payload["locked"] is True
