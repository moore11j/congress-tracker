from __future__ import annotations

import json
import threading
import time
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace

from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from starlette.requests import Request

import app.main as main_module
import app.routers.signals as signals_module
import app.services.confirmation_score as confirmation_score_module
from app.db import Base
from app.entitlements import ENTITLEMENTS
from app.main import _ticker_profiles_response, ticker_signals_summary
from app.models import DataEnrichmentJob, Event, FundamentalsCache, GovernmentContract, PriceCache, TickerContextBundleCache, TickerMeta
from app.services.confirmation_context import build_confirmation_score_context
from app.services.confirmation_score import confirmation_score_bundle_from_source_contexts, slim_confirmation_score_bundle


def _engine():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    return engine


def _request(path: str = "/", headers: dict[str, str] | None = None) -> Request:
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


def _complete_context_bundle_payload(symbol: str = "AAPL", **extra) -> dict:
    payload = {
        "symbol": symbol,
        "status": "ok",
        "quote": {"current_price": 308.63, "stale": False},
        "source_cards": {
            "price_volume": {"status": "ok"},
            "fundamentals": {"status": "ok"},
            "insiders": {"status": "ok"},
            "congress": {"status": "ok"},
            "government_contracts": {"status": "ok"},
        },
        "signals_summary": {"status": "ok", "items": []},
    }
    payload.update(extra)
    return payload


def _mock_signal_auth(monkeypatch, tier: str = "premium"):
    monkeypatch.setattr(main_module, "current_user", lambda *args, **kwargs: object())
    monkeypatch.setattr(main_module, "current_entitlements", lambda *args, **kwargs: ENTITLEMENTS[tier])
    monkeypatch.setattr(main_module, "require_feature", lambda *args, **kwargs: None)
    main_module._TICKER_SIGNALS_SUMMARY_CACHE.clear()
    main_module._TICKER_SIGNALS_SUMMARY_INFLIGHT.clear()
    main_module._TICKER_CONTEXT_BUNDLE_INFLIGHT.clear()
    main_module._TICKER_CONTEXT_BUNDLE_MEMORY_CACHE.clear()


def _mock_logged_out_signal_context(monkeypatch):
    monkeypatch.setattr(main_module, "current_user", lambda *args, **kwargs: None)
    monkeypatch.setattr(main_module, "current_entitlements", lambda *args, **kwargs: ENTITLEMENTS["free"])
    main_module._TICKER_SIGNALS_SUMMARY_CACHE.clear()
    main_module._TICKER_SIGNALS_SUMMARY_INFLIGHT.clear()
    main_module._TICKER_CONTEXT_BUNDLE_MEMORY_CACHE.clear()


def test_symbol_scoped_signal_queries_use_smaller_candidate_floor():
    assert signals_module.SYMBOL_SCOPED_SIGNAL_CANDIDATE_FLOOR == 25
    assert signals_module.BROAD_SIGNAL_CANDIDATE_FLOOR == 100
    assert signals_module.SYMBOL_SCOPED_SIGNAL_CANDIDATE_FLOOR < signals_module.BROAD_SIGNAL_CANDIDATE_FLOOR


def test_ticker_government_contracts_fails_soft_when_widget_lane_saturated(monkeypatch):
    class BusySemaphore:
        def acquire(self, timeout=None):
            return False

        def release(self):
            raise AssertionError("release should not run without acquire")

    monkeypatch.setattr(main_module, "_TICKER_WIDGET_SEMAPHORE", BusySemaphore())
    monkeypatch.setattr(
        main_module,
        "get_government_contracts_for_symbol",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("route should fail before DB lookup")),
    )

    payload = main_module.ticker_government_contracts(
        _request("/api/tickers/NVDA/government-contracts"),
        "NVDA",
        lookback_days=365,
        min_amount=1_000_000,
        limit=10,
        page=0,
        db=object(),
    )

    assert payload["symbol"] == "NVDA"
    assert payload["status"] == "unavailable"
    assert payload["source_status"] == "busy"
    assert payload["items"] == []


def test_incomplete_fresh_fundamentals_row_refreshes_before_return(monkeypatch):
    engine = _engine()
    old_fetched_at = datetime.now(timezone.utc) - timedelta(days=2)
    refreshed_at = datetime.now(timezone.utc)

    def fake_fetch(symbol: str):
        assert symbol == "AAPL"
        return SimpleNamespace(
            status="ok",
            error=None,
            values={
                "symbol": "AAPL",
                "provider": "fmp",
                "fetched_at": refreshed_at,
                "status": "ok",
                "revenue_growth": 6.5,
                "roe": 146.7,
                "ev_to_ebitda": 29.0,
                "operating_margin_expansion": 3.6,
                "net_debt_to_ebitda": 0.3,
            },
        )

    monkeypatch.setattr(main_module, "fetch_fundamentals_for_symbol", fake_fetch)

    with Session(engine) as db:
        db.add(
            FundamentalsCache(
                symbol="AAPL",
                provider="fmp",
                fetched_at=old_fetched_at,
                status="ok",
            )
        )
        db.commit()

        row = main_module._cached_ticker_fundamentals_row(db, "AAPL")

    assert row is not None
    assert row.revenue_growth == 6.5
    assert row.roe == 146.7
    assert row.ev_to_ebitda == 29.0
    assert row.operating_margin_expansion == 3.6
    assert row.net_debt_to_ebitda == 0.3


def test_ticker_api_prefetch_requests_bypass_expensive_builders(monkeypatch):
    request = _request(
        "/api/tickers/AAPL/context-bundle",
        {"purpose": "prefetch", "x-walnut-request-source": "prefetch"},
    )
    monkeypatch.setattr(
        main_module,
        "_build_ticker_context_bundle",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("prefetch must not build context bundle")),
    )

    response = main_module.ticker_context_bundle(
        request,
        "AAPL",
        side="all",
        limit=3,
        lookback_days=30,
        db=object(),
    )

    assert response.status_code == 204
    assert response.headers["x-walnut-prefetch-bypass"] == "1"


def test_ticker_context_bundle_bot_uses_cached_or_lightweight_payload(monkeypatch):
    request = _request(
        "/api/tickers/AAPL/context-bundle",
        {"user-agent": "Googlebot/2.1"},
    )
    monkeypatch.setattr(
        main_module,
        "_build_ticker_context_bundle",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("bot must not build fresh context bundle")),
    )
    monkeypatch.setattr(main_module, "_ticker_context_bundle_cached_for_segment", lambda *args, **kwargs: None)

    response = main_module.ticker_context_bundle(
        request,
        "AAPL",
        side="all",
        limit=3,
        lookback_days=30,
        db=object(),
    )
    payload = json.loads(response.body)

    assert response.headers["retry-after"] == "60"
    assert payload["symbol"] == "AAPL"
    assert payload["status"] == "lightweight"
    assert payload["signals_summary"]["items"] == []


def test_ticker_context_bundle_unknown_direct_api_builds_complete_payload(monkeypatch):
    request = _request(
        "/api/tickers/AAPL/context-bundle",
        {"accept": "application/json", "user-agent": "Mozilla/5.0 Chrome/126"},
    )
    built_payload = _complete_context_bundle_payload("AAPL")
    monkeypatch.setattr(main_module, "_build_ticker_context_bundle", lambda **_kwargs: built_payload)
    monkeypatch.setattr(
        main_module,
        "_ticker_context_bundle_cached_for_segment",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("direct API should build complete payload")),
    )

    payload = main_module.ticker_context_bundle(
        request,
        "AAPL",
        side="all",
        limit=3,
        lookback_days=30,
        db=object(),
    )

    assert payload is built_payload


def test_ticker_context_bundle_load_test_request_builds_complete_payload(monkeypatch):
    request = _request(
        "/api/tickers/AAPL/context-bundle",
        {"accept": "application/json", "user-agent": "k6/0.49.0", "x-walnut-request-source": "load_test"},
    )
    built_payload = _complete_context_bundle_payload("AAPL")
    monkeypatch.setattr(main_module, "_build_ticker_context_bundle", lambda **_kwargs: built_payload)
    monkeypatch.setattr(
        main_module,
        "_ticker_context_bundle_cached_for_segment",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("load test should exercise complete payload path")),
    )

    payload = main_module.ticker_context_bundle(
        request,
        "AAPL",
        side="all",
        limit=3,
        lookback_days=30,
        db=object(),
    )

    assert payload is built_payload


def test_ticker_context_bundle_normal_client_request_still_builds_full_payload(monkeypatch):
    request = _request(
        "/api/tickers/AAPL/context-bundle",
        {
            "accept": "*/*",
            "referer": "https://app.walnutmarkets.com/ticker/AAPL",
            "x-walnut-request-source": "client",
            "x-walnut-active-user": "browser",
        },
    )
    built_payload = {"symbol": "AAPL", "status": "ok", "signals_summary": {"status": "ok", "items": []}}
    monkeypatch.setattr(main_module, "_build_ticker_context_bundle", lambda **_kwargs: built_payload)
    monkeypatch.setattr(
        main_module,
        "_ticker_context_bundle_cached_for_segment",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("normal client should not take cached-only shortcut")),
    )

    payload = main_module.ticker_context_bundle(
        request,
        "AAPL",
        side="all",
        limit=3,
        lookback_days=30,
        db=object(),
    )

    assert payload is built_payload


def test_ticker_context_bundle_logged_in_request_still_builds_full_payload(monkeypatch):
    request = _request(
        "/api/tickers/AAPL/context-bundle",
        {
            "accept": "application/json",
            "cookie": "ct_session=session-token",
            "x-walnut-request-source": "client",
        },
    )
    built_payload = {"symbol": "AAPL", "status": "ok", "signals_summary": {"status": "ok", "items": []}}
    monkeypatch.setattr(main_module, "_build_ticker_context_bundle", lambda **_kwargs: built_payload)
    monkeypatch.setattr(
        main_module,
        "_ticker_context_bundle_cached_for_segment",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("logged-in request should not take cached-only shortcut")),
    )

    payload = main_module.ticker_context_bundle(
        request,
        "AAPL",
        side="all",
        limit=3,
        lookback_days=30,
        db=object(),
    )

    assert payload is built_payload


def test_ticker_context_bundle_unknown_logged_out_ssr_uses_lightweight_payload(monkeypatch):
    request = _request(
        "/api/tickers/AAPL/context-bundle",
        {"x-walnut-request-source": "ssr"},
    )
    monkeypatch.setattr(
        main_module,
        "_build_ticker_context_bundle",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("inactive SSR must not build fresh context bundle")),
    )
    monkeypatch.setattr(main_module, "_ticker_context_bundle_cached_for_segment", lambda *args, **kwargs: None)

    response = main_module.ticker_context_bundle(
        request,
        "AAPL",
        side="all",
        limit=3,
        lookback_days=30,
        db=object(),
    )
    payload = json.loads(response.body)

    assert response.headers["retry-after"] == "60"
    assert payload["symbol"] == "AAPL"
    assert payload["status"] == "lightweight"
    assert payload["signals_summary"]["items"] == []


def test_ticker_context_bundle_logged_out_ssr_active_marker_builds_public_context(monkeypatch):
    request = _request(
        "/api/tickers/AAPL/context-bundle",
        {
            "x-walnut-request-source": "ssr",
            "x-walnut-active-user": "browser",
            "user-agent": "node",
        },
    )
    built_payload = {"symbol": "AAPL", "status": "ok", "signals_summary": {"status": "ok", "items": []}}
    monkeypatch.setattr(main_module, "_build_ticker_context_bundle", lambda **_kwargs: built_payload)
    monkeypatch.setattr(
        main_module,
        "_ticker_context_bundle_cached_for_segment",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("active SSR should not take cached-only shortcut")),
    )

    payload = main_module.ticker_context_bundle(
        request,
        "AAPL",
        side="sell",
        limit=3,
        lookback_days=365,
        db=object(),
    )

    assert payload is built_payload


def test_ticker_government_contracts_prefetch_and_bot_do_not_query_details(monkeypatch):
    monkeypatch.setattr(
        main_module,
        "get_government_contracts_for_symbol",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("prefetch/bot must not query contract details")),
    )

    prefetch = main_module.ticker_government_contracts(
        _request("/api/tickers/NVDA/government-contracts", {"next-router-prefetch": "1"}),
        "NVDA",
        lookback_days=365,
        min_amount=1_000_000,
        limit=10,
        page=0,
        db=object(),
    )
    bot = main_module.ticker_government_contracts(
        _request("/api/tickers/NVDA/government-contracts", {"user-agent": "Googlebot/2.1"}),
        "NVDA",
        lookback_days=365,
        min_amount=1_000_000,
        limit=10,
        page=0,
        db=object(),
    )

    assert prefetch.status_code == 204
    assert bot["symbol"] == "NVDA"
    assert bot["status"] == "skipped"
    assert bot["items"] == []


def test_ticker_government_contracts_unknown_logged_out_ssr_does_not_query_details(monkeypatch):
    monkeypatch.setattr(
        main_module,
        "get_government_contracts_for_symbol",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("inactive SSR must not query contract details")),
    )

    payload = main_module.ticker_government_contracts(
        _request("/api/tickers/NVDA/government-contracts", {"x-walnut-request-source": "ssr"}),
        "NVDA",
        lookback_days=365,
        min_amount=1_000_000,
        limit=10,
        page=0,
        db=object(),
    )

    assert payload["symbol"] == "NVDA"
    assert payload["status"] == "skipped"
    assert payload["items"] == []


def test_ticker_signals_summary_bot_does_not_query_unified_signals(monkeypatch):
    monkeypatch.setattr(
        main_module,
        "_query_unified_signals",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("bot must not query unified signals")),
    )

    payload = main_module.ticker_signals_summary(
        _request("/api/tickers/AAPL/signals-summary", {"user-agent": "Googlebot/2.1"}),
        "AAPL",
        side="all",
        limit=3,
        lookback_days=30,
        db=object(),
    )

    assert payload["symbol"] == "AAPL"
    assert payload["status"] == "skipped"
    assert payload["items"] == []


def test_ticker_signals_summary_unknown_logged_out_ssr_does_not_query_unified_signals(monkeypatch):
    monkeypatch.setattr(
        main_module,
        "_query_unified_signals",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("inactive SSR must not query unified signals")),
    )

    payload = main_module.ticker_signals_summary(
        _request("/api/tickers/AAPL/signals-summary", {"x-walnut-request-source": "ssr"}),
        "AAPL",
        side="all",
        limit=3,
        lookback_days=30,
        db=object(),
    )

    assert payload["symbol"] == "AAPL"
    assert payload["status"] == "skipped"
    assert payload["items"] == []


def _event(
    event_id: int,
    *,
    symbol: str,
    event_type: str,
    trade_type: str,
    days_ago: int = 1,
    amount: int = 25_000,
    member_name: str = "Example Actor",
) -> Event:
    ts = datetime.now(timezone.utc) - timedelta(days=days_ago)
    payload = {"symbol": symbol, "reporting_cik": f"{event_id:010d}", "insider_name": member_name}
    return Event(
        id=event_id,
        event_type=event_type,
        ts=ts,
        event_date=ts,
        symbol=symbol,
        source="test",
        trade_type=trade_type,
        amount_min=amount,
        amount_max=amount,
        member_name=member_name,
        member_bioguide_id=f"M{event_id}" if event_type == "congress_trade" else None,
        payload_json=json.dumps(payload),
    )


def _signal_item(
    event_id: int,
    *,
    symbol: str = "NVDA",
    days_ago: int = 2,
    smart_score: int = 78,
    smart_band: str = "strong",
    trade_type: str = "purchase",
) -> SimpleNamespace:
    ts = (datetime.now(timezone.utc) - timedelta(days=days_ago)).isoformat()
    return SimpleNamespace(
        model_dump=lambda mode="json": {
            "kind": "insider",
            "event_id": event_id,
            "ts": ts,
            "symbol": symbol,
            "who": "Mark A Stevens",
            "trade_type": trade_type,
            "amount_min": 100_000,
            "amount_max": 100_000,
            "smart_score": smart_score,
            "smart_band": smart_band,
            "reporting_cik": "0001045810",
        }
    )


def _fixed_signal_event(
    event_id: int,
    *,
    symbol: str,
    event_type: str,
    trade_type: str,
    ts: datetime,
    amount: int,
    member_name: str,
    source: str = "test",
) -> Event:
    payload = {"symbol": symbol, "reporting_cik": f"{event_id:010d}", "insider_name": member_name}
    return Event(
        id=event_id,
        event_type=event_type,
        ts=ts,
        event_date=ts,
        symbol=symbol,
        source=source,
        trade_type=trade_type,
        amount_min=amount,
        amount_max=amount,
        member_name=member_name,
        member_bioguide_id=f"M{event_id}" if event_type == "congress_trade" else None,
        chamber="House" if event_type == "congress_trade" else None,
        payload_json=json.dumps(payload),
    )


def _seed_abnormal_signal_fixture(db: Session, *, as_of: datetime) -> None:
    specs = [
        (4101, "TSM", "insider_trade", "sale", datetime(2026, 5, 19, tzinfo=timezone.utc), "TSM Insider", "test"),
        (4201, "FCNCA", "insider_trade", "sale", datetime(2026, 5, 12, tzinfo=timezone.utc), "FCNCA Insider", "test"),
        (4301, "CVX", "congress_trade", "sale", datetime(2026, 4, 10, tzinfo=timezone.utc), "CVX Member", "house"),
        (4401, "INWIN", "insider_trade", "sale", as_of - timedelta(days=5), "In Window Insider", "test"),
    ]
    for event_id, symbol, event_type, trade_type, signal_ts, member_name, source in specs:
        for index, baseline_ts in enumerate(
            [
                datetime(2026, 1, 10, tzinfo=timezone.utc),
                datetime(2026, 2, 10, tzinfo=timezone.utc),
                datetime(2026, 3, 10, tzinfo=timezone.utc),
            ],
            start=1,
        ):
            db.add(
                _fixed_signal_event(
                    event_id=event_id + index,
                    symbol=symbol,
                    event_type=event_type,
                    trade_type=trade_type,
                    ts=baseline_ts,
                    amount=10_000,
                    member_name=member_name,
                    source=source,
                )
            )
        db.add(
            _fixed_signal_event(
                event_id=event_id,
                symbol=symbol,
                event_type=event_type,
                trade_type=trade_type,
                ts=signal_ts,
                amount=500_000,
                member_name=member_name,
                source=source,
            )
        )


def _freeze_signal_now(monkeypatch, as_of: datetime) -> None:
    class FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return as_of.astimezone(tz) if tz is not None else as_of.replace(tzinfo=None)

    monkeypatch.setattr(signals_module, "datetime", FrozenDateTime)
    monkeypatch.setattr(confirmation_score_module, "datetime", FrozenDateTime)


def _seed_score_contract_fixture(db: Session) -> None:
    today = date.today()
    price_points = {
        "AAPL": (100, 108),
        "MSTR": (100, 96),
        "NBIS": (100, 99),
        "SPY": (100, 102),
    }
    for symbol, (start_close, end_close) in price_points.items():
        db.add(PriceCache(symbol=symbol, date=(today - timedelta(days=29)).isoformat(), close=start_close))
        db.add(PriceCache(symbol=symbol, date=today.isoformat(), close=end_close))

    db.add_all(
        [
            _event(101, symbol="AAPL", event_type="congress_trade", trade_type="purchase", days_ago=2, amount=220_000, member_name="Rep Apple"),
            _event(102, symbol="AAPL", event_type="congress_trade", trade_type="purchase", days_ago=95, amount=20_000, member_name="Rep Apple"),
            _event(103, symbol="AAPL", event_type="congress_trade", trade_type="purchase", days_ago=125, amount=20_000, member_name="Rep Apple"),
            _event(104, symbol="AAPL", event_type="congress_trade", trade_type="purchase", days_ago=155, amount=20_000, member_name="Rep Apple"),
            _event(201, symbol="MSTR", event_type="insider_trade", trade_type="sale", days_ago=1, amount=240_000, member_name="MSTR Insider"),
            _event(202, symbol="MSTR", event_type="insider_trade", trade_type="sale", days_ago=90, amount=20_000, member_name="MSTR Insider"),
            _event(203, symbol="MSTR", event_type="insider_trade", trade_type="sale", days_ago=120, amount=20_000, member_name="MSTR Insider"),
            _event(204, symbol="MSTR", event_type="insider_trade", trade_type="sale", days_ago=150, amount=20_000, member_name="MSTR Insider"),
            _event(205, symbol="MSTR", event_type="congress_trade", trade_type="purchase", days_ago=3, amount=180_000, member_name="Rep Buyer"),
            _event(301, symbol="NBIS", event_type="insider_trade", trade_type="sale", days_ago=2, amount=180_000, member_name="NBIS Insider"),
            _event(302, symbol="NBIS", event_type="insider_trade", trade_type="sale", days_ago=95, amount=15_000, member_name="NBIS Insider"),
            _event(303, symbol="NBIS", event_type="insider_trade", trade_type="sale", days_ago=125, amount=15_000, member_name="NBIS Insider"),
            _event(304, symbol="NBIS", event_type="insider_trade", trade_type="sale", days_ago=155, amount=15_000, member_name="NBIS Insider"),
        ]
    )


def test_ticker_signals_summary_uses_fixed_30d_signal_window(monkeypatch):
    captured: dict[str, object] = {}
    query_calls: list[dict[str, object]] = []

    def fake_query(**kwargs):
        query_calls.append(dict(kwargs))
        captured.update(kwargs)
        return [
            SimpleNamespace(
                model_dump=lambda mode="json": {
                    "symbol": kwargs["symbol"],
                    "ts": "2026-06-12T12:00:00Z",
                    "smart_score": 82,
                }
            )
        ]

    _mock_signal_auth(monkeypatch)
    monkeypatch.setattr(main_module, "_query_unified_signals", fake_query)
    monkeypatch.setattr(
        main_module,
        "_ticker_price_volume_summary",
        lambda db, symbol: {"status": "limited", "summary": "Limited price history", "score": None, "lines": ["Limited price history"]},
    )
    monkeypatch.setattr(
        main_module,
        "_ticker_trade_activity_summary",
        lambda *args, **kwargs: {
            "status": "inactive",
            "direction": "neutral",
            "title": "No recent activity",
            "subtitle": "No matching trades.",
            "buy_count": 0,
            "sell_count": 0,
            "net_flow": None,
        },
    )
    monkeypatch.setattr(
        main_module,
        "get_government_contracts_summary",
        lambda *args, **kwargs: {
            "status": "ok",
            "active": False,
            "contract_count": 0,
            "total_award_amount": 0,
            "detail": "No qualifying contracts found in the last 30 Days.",
        },
    )
    monkeypatch.setattr(
        main_module,
        "build_confirmation_score_context",
        lambda db, tickers, **kwargs: captured.update(
            {
                "confirmation_tickers": list(tickers),
                "confirmation_lookback_days": kwargs.get("lookback_days"),
            }
        )
        or {
            "bundles": {
                "NBIS": main_module.inactive_confirmation_score_bundle(
                    "NBIS",
                    lookback_days=int(kwargs.get("lookback_days") or 30),
                )
            },
            "options_flow_summaries": {},
            "government_contracts_summaries": {},
            "institutional_activity_summaries": {},
        },
    )

    response = ticker_signals_summary(object(), "nbis", side="buy", limit=3, lookback_days=365, db=object())

    assert captured["symbol"] == "NBIS"
    assert query_calls[0]["limit"] == 3
    assert query_calls[0]["symbol"] == "NBIS"
    assert query_calls[0]["side"] == "buy"
    assert query_calls[0]["congress_recent_days"] == 30
    assert query_calls[0]["insider_recent_days"] == 30
    assert len(query_calls) == 1
    assert captured["confirmation_tickers"] == ["NBIS"]
    assert captured["confirmation_lookback_days"] == 30
    assert response["symbol"] == "NBIS"
    assert response["latest_signal_score"] == 82
    assert response["recent_signal_count"] == 1
    assert response["lookback_days"] == 30
    assert response["effective_window_days"] == 30
    assert "signal_activity" not in response
    assert "signal_activity_total" not in response
    assert "signal_activity_state" not in response
    assert response["items"][0]["symbol"] == "NBIS"
    assert response["price_volume"]["status"] == "limited"
    assert response["price_volume"]["title"] == "Limited price history"
    assert response["confirmation_score_bundle"]["lookback_days"] == 30


def test_ticker_signals_summary_coalesces_identical_inflight_requests(monkeypatch):
    _mock_signal_auth(monkeypatch)
    query_started = threading.Event()
    release_query = threading.Event()
    query_call_count = 0
    query_lock = threading.Lock()

    def fake_query(**kwargs):
        nonlocal query_call_count
        with query_lock:
            query_call_count += 1
        query_started.set()
        assert release_query.wait(timeout=2)
        return [
            SimpleNamespace(
                model_dump=lambda mode="json": {
                    "symbol": kwargs["symbol"],
                    "ts": "2026-06-12T12:00:00Z",
                    "smart_score": 82,
                }
            )
        ]

    monkeypatch.setattr(main_module, "_query_unified_signals", fake_query)
    monkeypatch.setattr(
        main_module,
        "build_ticker_signals_summary_contexts_from_cache",
        lambda symbol, **kwargs: _public_summary_context(symbol),
    )
    monkeypatch.setattr(
        main_module,
        "_ticker_confirmation_context",
        lambda db, symbol: {"confirmation_score_bundle": _full_source_confirmation_bundle(symbol)},
    )

    responses: list[dict] = []
    errors: list[BaseException] = []

    def run_request():
        try:
            responses.append(ticker_signals_summary(object(), "NVDA", side="all", limit=3, lookback_days=30, db=object()))
        except BaseException as exc:
            errors.append(exc)

    leader = threading.Thread(target=run_request)
    follower = threading.Thread(target=run_request)
    leader.start()
    assert query_started.wait(timeout=2)
    follower.start()
    time.sleep(0.05)
    release_query.set()
    leader.join(timeout=2)
    follower.join(timeout=2)

    assert not leader.is_alive()
    assert not follower.is_alive()
    assert errors == []
    assert query_call_count == 1
    assert len(responses) == 2
    assert [response["symbol"] for response in responses] == ["NVDA", "NVDA"]
    assert [response["latest_signal_score"] for response in responses] == [82, 82]
    assert main_module._TICKER_SIGNALS_SUMMARY_INFLIGHT == {}


def test_ticker_signals_summary_logged_out_returns_public_context_with_locked_paid_sources(monkeypatch):
    _mock_logged_out_signal_context(monkeypatch)
    monkeypatch.setattr(
        main_module,
        "_query_unified_signals",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("logged-out context must not query premium signal rows")),
    )
    monkeypatch.setattr(
        main_module,
        "_ticker_confirmation_context",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("logged-out context must not build canonical confirmation")),
    )
    monkeypatch.setattr(
        main_module,
        "_ticker_price_volume_summary",
        lambda db, symbol: {
            "status": "active",
            "direction": "bullish",
            "title": "Bullish tape confirmation",
            "summary": "Bullish tape confirmation",
            "score": 72,
            "lines": ["Price / volume available"],
        },
    )
    trade_calls = []

    def fake_trade_activity(db, symbol, event_type, **kwargs):
        trade_calls.append((symbol, event_type, kwargs.get("lookback_days")))
        if event_type == "insider_trade":
            return {
                "status": "active",
                "direction": "bearish",
                "title": "Insider selling active",
                "subtitle": "1 sell in the last 30 Days.",
                "buy_count": 0,
                "sell_count": 1,
                "net_flow": -125_000,
            }
        return {
            "status": "active",
            "direction": "bullish",
            "title": "Congress buying active",
            "subtitle": "1 purchase in the last 30 Days.",
            "buy_count": 1,
            "sell_count": 0,
            "net_flow": 50_000,
        }

    monkeypatch.setattr(
        main_module,
        "_ticker_trade_activity_summary",
        fake_trade_activity,
    )
    monkeypatch.setattr(
        main_module,
        "get_government_contracts_summary",
        lambda *args, **kwargs: {
            "status": "ok",
            "active": True,
            "contract_count": 2,
            "total_award_amount": 25_000_000,
            "latest_award_date": date.today().isoformat(),
            "detail": "2 contracts above threshold.",
        },
    )

    response = ticker_signals_summary(object(), "AAPL", side="all", limit=3, lookback_days=365, db=object())

    assert response["status"] == "ok"
    assert response["price_volume"]["status"] == "active"
    assert response["source_entitlements"]["price_volume"]["lock_state"] == "available"
    assert response["source_entitlements"]["price_volume"]["required_plan"] is None
    assert response["source_entitlements"]["insiders"]["locked"] is False
    assert response["source_entitlements"]["insiders"]["required_plan"] is None
    assert response["source_entitlements"]["congress"]["locked"] is False
    assert response["source_entitlements"]["congress"]["required_plan"] is None
    assert response["source_entitlements"]["government_contracts"]["locked"] is False
    assert response["source_entitlements"]["government_contracts"]["required_plan"] is None
    assert response["source_entitlements"]["signals"]["lock_state"] == "premium_locked"
    assert response["source_entitlements"]["institutional_activity"]["lock_state"] == "pro_locked"
    assert response["source_entitlements"]["options_flow"]["lock_state"] == "pro_locked"
    assert response["insiders"]["status"] == "active"
    assert response["insiders"]["sell_count"] == 1
    assert response["congress"]["status"] == "active"
    assert response["congress"]["buy_count"] == 1
    assert response["government_contracts"]["status"] == "active"
    assert response["government_contracts"]["contract_count"] == 2
    assert response["confirmation_score_bundle"]["score"] > 0
    assert response["signal_freshness"] is not None
    assert response["items"] == []
    assert response["recent_signal_count"] == 0
    assert ("AAPL", "insider_trade", 30) in trade_calls
    assert ("AAPL", "congress_trade", 30) in trade_calls


def _full_source_confirmation_bundle(symbol: str) -> dict:
    return confirmation_score_bundle_from_source_contexts(
        symbol,
        source_contexts={
            "congress": {
                "status": "active",
                "direction": "bullish",
                "buy_count": 2,
                "sell_count": 0,
                "net_flow": 250_000,
                "title": "Congress buying active",
            },
            "insiders": {
                "status": "active",
                "direction": "bullish",
                "buy_count": 1,
                "sell_count": 0,
                "net_flow": 125_000,
                "title": "Insider buying active",
            },
            "signals": {
                "status": "active",
                "direction": "bullish",
                "recent_count": 1,
                "latest_score": 86,
                "title": "Signal conviction active",
            },
            "price_volume": {
                "status": "active",
                "direction": "bullish",
                "score": 76,
                "price_points": 60,
                "latest_volume": 2_000_000,
                "title": "Bullish tape confirmation",
            },
            "government_contracts": {
                "status": "active",
                "contract_count": 2,
                "contract_value": 22_000_000,
                "latest_date": date.today().isoformat(),
                "title": "Government contracts active",
            },
            "options_flow": {
                "status": "active",
                "direction": "bullish",
                "score": 92,
                "freshness_days": 1,
                "title": "Options flow confirming",
            },
            "institutional_activity": {
                "status": "active",
                "direction": "bullish",
                "freshness_days": 1,
                "title": "Institutional activity active",
            },
        },
    )


def _public_summary_context(symbol: str) -> dict[str, dict]:
    return {
        "price_volume": {
            "status": "active",
            "direction": "bullish",
            "title": "Bullish tape confirmation",
            "summary": "Bullish tape confirmation",
            "score": 76,
            "price_points": 60,
            "latest_volume": 2_000_000,
        },
        "fundamentals": {
            "status": "bullish",
            "headline": "Fundamental strength",
            "data_quality": {"scored_metric_count": 5},
            "metrics": {
                "revenue_growth": {"state": "bullish"},
                "return_on_equity": {"state": "bullish"},
                "ev_to_ebitda": {"state": "neutral"},
                "operating_margin_expansion": {"state": "bullish"},
                "net_debt_to_ebitda": {"state": "bullish"},
            },
        },
        "insiders": {
            "status": "active",
            "direction": "bullish",
            "title": "Insider buying active",
            "subtitle": "1 buy in the last 30 Days.",
            "buy_count": 1,
            "sell_count": 0,
            "net_flow": 125_000,
        },
        "congress": {
            "status": "active",
            "direction": "bullish",
            "title": "Congress buying active",
            "subtitle": "2 buys in the last 30 Days.",
            "buy_count": 2,
            "sell_count": 0,
            "net_flow": 250_000,
        },
        "signals": {
            "status": "active",
            "direction": "bullish",
            "title": "Signal conviction active",
            "subtitle": "1 premium signal.",
            "recent_count": 1,
            "latest_score": 86,
        },
        "government_contracts": {
            "status": "active",
            "direction": "bullish",
            "title": "Government contracts active",
            "subtitle": "2 contracts above threshold.",
            "contract_count": 2,
            "contract_value": 22_000_000,
            "latest_date": date.today().isoformat(),
        },
    }


def test_confirmation_score_fundamentals_have_direct_score_impact():
    base_context = {
        "congress": {
            "status": "active",
            "direction": "bearish",
            "title": "Congress trades active",
            "subtitle": "1 buys / 3 sells",
            "buy_count": 1,
            "sell_count": 3,
            "freshness_days": 6,
        },
        "price_volume": {
            "status": "active",
            "direction": "mixed",
            "title": "Mixed tape confirmation",
            "summary": "Mixed tape confirmation",
            "score": 25,
            "price_points": 120,
            "latest_volume": 146_341_084,
            "freshness_days": 1,
        },
    }
    with_fundamentals = confirmation_score_bundle_from_source_contexts(
        "NVDA",
        source_contexts={
            **base_context,
            "fundamentals": {
                "status": "bullish",
                "headline": "Fundamental strength",
                "freshness_days": 0,
                "metrics": {
                    "revenue_growth": {"state": "bullish"},
                    "return_on_equity": {"state": "bullish"},
                    "ev_to_ebitda": {"state": "neutral"},
                    "operating_margin_expansion": {"state": "bullish"},
                    "net_debt_to_ebitda": {"state": "bullish"},
                },
                "data_quality": {"scored_metric_count": 5},
            },
        },
    )
    without_fundamentals = confirmation_score_bundle_from_source_contexts(
        "NVDA",
        source_contexts=base_context,
    )

    assert with_fundamentals["sources"]["fundamentals"]["present"] is True
    assert with_fundamentals["score"] >= without_fundamentals["score"] + 4
    assert "fundamentals" in with_fundamentals["active_sources"]


def _active_institutional_summary() -> dict:
    return {
        "status": "ok",
        "active": True,
        "direction": "bullish",
        "institution_count": 3,
        "total_value": 25_000_000,
        "latest_activity_date": date.today().isoformat(),
    }


def test_ticker_signals_summary_free_user_gets_public_context_with_locked_sources(monkeypatch):
    _mock_signal_auth(monkeypatch, tier="free")
    monkeypatch.setattr(
        main_module,
        "require_feature",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("ticker context must not be feature-gated as a whole")),
    )
    monkeypatch.setattr(
        main_module,
        "_query_unified_signals",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("free context must not query premium signal rows")),
    )

    engine = _engine()
    with Session(engine) as db:
        _seed_score_contract_fixture(db)
        db.commit()

        responses = [
            ticker_signals_summary(object(), symbol, side="all", limit=3, lookback_days=365, db=db)
            for symbol in ("AAPL", "MSTR")
        ]

    for response in responses:
        source_entitlements = response["source_entitlements"]
        assert source_entitlements["price_volume"]["locked"] is False
        assert source_entitlements["insiders"]["locked"] is False
        assert source_entitlements["congress"]["locked"] is False
        assert source_entitlements["government_contracts"]["locked"] is False
        assert source_entitlements["signals"]["required_plan"] == "premium"
        assert source_entitlements["signals"]["locked"] is True
        assert source_entitlements["institutional_activity"]["required_plan"] == "pro"
        assert source_entitlements["institutional_activity"]["locked"] is True
        assert source_entitlements["options_flow"]["required_plan"] == "pro"
        assert source_entitlements["options_flow"]["locked"] is True
        assert response["items"] == []
        assert response["recent_signal_count"] == 0
        assert response["latest_signal_score"] is None
        assert response["confirmation_score_bundle"]["score"] > 0
        assert response["confirmation_score_bundle"]["lookback_days"] == 30


def test_ticker_signals_summary_logged_out_seeded_tickers_keep_public_activity(monkeypatch):
    _mock_logged_out_signal_context(monkeypatch)
    monkeypatch.setattr(
        main_module,
        "_query_unified_signals",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("guest context must not query premium signal rows")),
    )
    monkeypatch.setattr(
        main_module,
        "_ticker_confirmation_context",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("guest context must not build full authenticated confirmation")),
    )

    engine = _engine()
    with Session(engine) as db:
        _seed_score_contract_fixture(db)
        db.commit()

        responses = {
            symbol: ticker_signals_summary(object(), symbol, side="all", limit=3, lookback_days=365, db=db)
            for symbol in ("AAPL", "MSTR", "NBIS")
        }

    for response in responses.values():
        source_entitlements = response["source_entitlements"]
        for public_source in ("price_volume", "insiders", "congress", "government_contracts"):
            assert source_entitlements[public_source]["locked"] is False
            assert source_entitlements[public_source]["required_plan"] is None
        assert source_entitlements["signals"]["locked"] is True
        assert source_entitlements["signals"]["required_plan"] == "premium"
        assert response["confirmation_score_bundle"] is not None
        assert response["confirmation_score_bundle"]["lookback_days"] == 30
        assert response["confirmation_score_bundle"]["score"] > 0
        assert response["confirmation_score_bundle"]["status"] != "Inactive"
        assert response["signal_freshness"] is not None

    assert responses["AAPL"]["congress"]["status"] == "active"
    assert responses["AAPL"]["confirmation_score_bundle"]["sources"]["congress"]["present"] is True
    assert responses["MSTR"]["insiders"]["status"] == "active"
    assert responses["MSTR"]["congress"]["status"] == "active"
    assert responses["MSTR"]["confirmation_score_bundle"]["sources"]["insiders"]["present"] is True
    assert responses["MSTR"]["confirmation_score_bundle"]["sources"]["congress"]["present"] is True
    assert responses["NBIS"]["insiders"]["status"] == "active"
    assert responses["NBIS"]["confirmation_score_bundle"]["sources"]["insiders"]["present"] is True


def test_ticker_context_source_entitlements_match_required_plan_model():
    logged_out = main_module._ticker_context_source_entitlements(None, authenticated=False)
    free = main_module._ticker_context_source_entitlements(ENTITLEMENTS["free"])
    premium = main_module._ticker_context_source_entitlements(ENTITLEMENTS["premium"])
    pro = main_module._ticker_context_source_entitlements(ENTITLEMENTS["pro"])
    admin = main_module._ticker_context_source_entitlements(ENTITLEMENTS["admin"])

    for public_source in ("price_volume", "insiders", "congress", "government_contracts"):
        assert logged_out[public_source]["locked"] is False
        assert logged_out[public_source]["required_plan"] is None
        assert free[public_source]["locked"] is False
        assert free[public_source]["required_plan"] is None
        assert premium[public_source]["locked"] is False
        assert premium[public_source]["required_plan"] is None
        assert pro[public_source]["locked"] is False
        assert pro[public_source]["required_plan"] is None
        assert admin[public_source]["locked"] is False
        assert admin[public_source]["required_plan"] is None

    assert logged_out["signals"]["required_plan"] == "premium"
    assert logged_out["signals"]["locked"] is True
    assert free["signals"]["locked"] is True
    assert premium["signals"]["locked"] is False
    assert pro["signals"]["locked"] is False
    assert admin["signals"]["locked"] is False
    assert logged_out["institutional_activity"]["required_plan"] == "pro"
    assert logged_out["institutional_activity"]["locked"] is True
    assert free["institutional_activity"]["locked"] is True
    assert premium["institutional_activity"]["locked"] is True
    assert pro["institutional_activity"]["locked"] is False
    assert admin["institutional_activity"]["locked"] is False
    assert logged_out["options_flow"]["required_plan"] == "pro"
    assert logged_out["options_flow"]["locked"] is True
    assert free["options_flow"]["locked"] is True
    assert premium["options_flow"]["locked"] is True
    assert pro["options_flow"]["locked"] is False
    assert admin["options_flow"]["locked"] is False


def test_ticker_signals_summary_admin_does_not_lock_paid_sources(monkeypatch):
    _mock_signal_auth(monkeypatch, tier="admin")
    full_bundle = _full_source_confirmation_bundle("AAPL")
    monkeypatch.setattr(main_module, "_query_unified_signals", lambda **kwargs: [])
    monkeypatch.setattr(
        main_module,
        "build_ticker_signals_summary_contexts_from_cache",
        lambda symbol, **kwargs: _public_summary_context(symbol),
    )
    monkeypatch.setattr(
        main_module,
        "_ticker_confirmation_context",
        lambda db, symbol: {
            "confirmation_score_bundle": full_bundle,
            "institutional_activity_summary": _active_institutional_summary(),
        },
    )

    response = ticker_signals_summary(object(), "AAPL", side="all", limit=3, lookback_days=365, db=object())
    source_entitlements = response["source_entitlements"]
    bundle = response["confirmation_score_bundle"]

    assert source_entitlements["signals"]["lock_state"] == "available"
    assert source_entitlements["options_flow"]["lock_state"] == "available"
    assert source_entitlements["institutional_activity"]["lock_state"] == "available"
    assert bundle["sources"]["signals"].get("locked") is not True
    assert bundle["sources"]["options_flow"].get("locked") is not True
    assert bundle["sources"]["institutional_activity"].get("locked") is not True
    assert bundle["sources"]["signals"]["present"] is True
    assert bundle["sources"]["options_flow"]["present"] is True
    assert bundle["sources"]["institutional_activity"]["present"] is True
    assert "signals" in bundle["active_sources"]
    assert "options_flow" in bundle["active_sources"]
    assert "institutional_activity" in bundle["active_sources"]
    assert bundle["score"] >= full_bundle["score"]


def test_ticker_signals_summary_pro_does_not_lock_paid_sources(monkeypatch):
    _mock_signal_auth(monkeypatch, tier="pro")
    full_bundle = _full_source_confirmation_bundle("AAPL")
    monkeypatch.setattr(main_module, "_query_unified_signals", lambda **kwargs: [])
    monkeypatch.setattr(
        main_module,
        "build_ticker_signals_summary_contexts_from_cache",
        lambda symbol, **kwargs: _public_summary_context(symbol),
    )
    monkeypatch.setattr(
        main_module,
        "_ticker_confirmation_context",
        lambda db, symbol: {
            "confirmation_score_bundle": full_bundle,
            "institutional_activity_summary": _active_institutional_summary(),
        },
    )

    response = ticker_signals_summary(object(), "AAPL", side="all", limit=3, lookback_days=365, db=object())
    source_entitlements = response["source_entitlements"]
    bundle = response["confirmation_score_bundle"]

    assert source_entitlements["signals"]["lock_state"] == "available"
    assert source_entitlements["options_flow"]["lock_state"] == "available"
    assert source_entitlements["institutional_activity"]["lock_state"] == "available"
    assert bundle["sources"]["signals"].get("locked") is not True
    assert bundle["sources"]["options_flow"].get("locked") is not True
    assert bundle["sources"]["institutional_activity"].get("locked") is not True
    assert bundle["sources"]["signals"]["present"] is True
    assert bundle["sources"]["options_flow"]["present"] is True
    assert bundle["sources"]["institutional_activity"]["present"] is True


def test_ticker_signals_summary_authenticated_merges_fresh_price_with_institutional(monkeypatch):
    _mock_signal_auth(monkeypatch, tier="pro")
    stale_authenticated_bundle = confirmation_score_bundle_from_source_contexts(
        "NVDA",
        source_contexts={
            "congress": {
                "status": "active",
                "direction": "bearish",
                "title": "Congress trades active",
                "buy_count": 1,
                "sell_count": 3,
            },
            "fundamentals": {
                "status": "bullish",
                "headline": "Fundamental strength",
                "data_quality": {"scored_metric_count": 5},
                "metrics": {
                    "revenue_growth": {"state": "bullish"},
                    "return_on_equity": {"state": "bullish"},
                    "ev_to_ebitda": {"state": "neutral"},
                    "operating_margin_expansion": {"state": "bullish"},
                    "net_debt_to_ebitda": {"state": "bullish"},
                },
            },
            "institutional_activity": {
                "status": "active",
                "direction": "bullish",
                "score": 45,
                "title": "Institutional Activity",
                "subtitle": "Net reported accumulation",
            },
        },
    )
    fresh_context = {
        "price_volume": {
            "status": "active",
            "direction": "mixed",
            "title": "Mixed tape confirmation",
            "summary": "Mixed tape confirmation",
            "score": 25,
            "price_points": 120,
            "latest_volume": 146_341_084,
        },
        "fundamentals": {
            "status": "bullish",
            "headline": "Fundamental strength",
            "data_quality": {"scored_metric_count": 5},
            "metrics": {
                "revenue_growth": {"state": "bullish"},
                "return_on_equity": {"state": "bullish"},
                "ev_to_ebitda": {"state": "neutral"},
                "operating_margin_expansion": {"state": "bullish"},
                "net_debt_to_ebitda": {"state": "bullish"},
            },
        },
        "insiders": {"status": "inactive", "direction": "neutral", "buy_count": 0, "sell_count": 0},
        "congress": {
            "status": "active",
            "direction": "bearish",
            "title": "Congress trades active",
            "buy_count": 1,
            "sell_count": 3,
        },
        "signals": {"status": "premium_locked", "direction": "neutral", "recent_count": 0},
        "government_contracts": {"status": "inactive", "direction": "neutral", "contract_count": 0},
    }
    monkeypatch.setattr(main_module, "_query_unified_signals", lambda **kwargs: [])
    monkeypatch.setattr(
        main_module,
        "build_ticker_signals_summary_contexts_from_cache",
        lambda symbol, **kwargs: fresh_context,
    )
    monkeypatch.setattr(
        main_module,
        "_ticker_confirmation_context",
        lambda db, symbol: {
            "confirmation_score_bundle": stale_authenticated_bundle,
            "institutional_activity_summary": _active_institutional_summary(),
        },
    )

    response = ticker_signals_summary(object(), "NVDA", side="all", limit=3, lookback_days=365, db=object())
    bundle = response["confirmation_score_bundle"]

    assert bundle["sources"]["price_volume"]["present"] is True
    assert bundle["sources"]["institutional_activity"]["present"] is True
    assert "price_volume" in bundle["active_sources"]
    assert "institutional_activity" in bundle["active_sources"]


def test_ticker_confirmation_context_merges_fresh_public_context(monkeypatch):
    stale_bundle = confirmation_score_bundle_from_source_contexts(
        "TSM",
        source_contexts={
            "insiders": {"status": "active", "direction": "bullish", "buy_count": 36, "sell_count": 0},
            "congress": {"status": "active", "direction": "bullish", "buy_count": 1, "sell_count": 0},
            "price_volume": {"status": "active", "direction": "bullish", "score": 35, "price_points": 120},
            "fundamentals": {
                "status": "bullish",
                "headline": "Fundamental strength",
                "data_quality": {"scored_metric_count": 5},
                "metrics": {
                    "revenue_growth": {"state": "bullish"},
                    "return_on_equity": {"state": "bullish"},
                    "ev_to_ebitda": {"state": "neutral"},
                    "operating_margin_expansion": {"state": "bullish"},
                    "net_debt_to_ebitda": {"state": "bullish"},
                },
            },
            "institutional_activity": {
                "status": "active",
                "direction": "bullish",
                "score": 45,
                "title": "Institutional Activity",
                "subtitle": "Net reported accumulation",
            },
        },
    )
    fresh_context = {
        "price_volume": {
            "status": "active",
            "direction": "mixed",
            "title": "Mixed tape confirmation",
            "summary": "Mixed tape confirmation",
            "score": 25,
            "price_points": 120,
        },
        "fundamentals": {
            "status": "bullish",
            "headline": "Fundamental strength",
            "data_quality": {"scored_metric_count": 5},
            "metrics": {
                "revenue_growth": {"state": "bullish"},
                "return_on_equity": {"state": "bullish"},
                "ev_to_ebitda": {"state": "neutral"},
                "operating_margin_expansion": {"state": "bullish"},
                "net_debt_to_ebitda": {"state": "bullish"},
            },
        },
        "insiders": {"status": "active", "direction": "bullish", "buy_count": 36, "sell_count": 0},
        "congress": {"status": "active", "direction": "bullish", "buy_count": 1, "sell_count": 0},
        "signals": {"status": "inactive", "direction": "neutral", "recent_count": 0},
        "government_contracts": {"status": "inactive", "direction": "neutral", "contract_count": 0},
    }
    monkeypatch.setattr(
        main_module,
        "build_confirmation_score_context",
        lambda *_args, **_kwargs: {
            "bundles": {"TSM": stale_bundle},
            "options_flow_summaries": {},
            "government_contracts_summaries": {},
            "institutional_activity_summaries": {"TSM": _active_institutional_summary()},
        },
    )
    monkeypatch.setattr(
        main_module,
        "build_ticker_signals_summary_contexts_from_cache",
        lambda symbol, **kwargs: fresh_context,
    )

    context = main_module._ticker_confirmation_context(object(), "TSM")
    bundle = context["confirmation_score_bundle"]

    assert bundle["sources"]["price_volume"]["direction"] == "mixed"
    assert bundle["sources"]["institutional_activity"]["present"] is True
    assert bundle["direction"] == "bullish"
    assert bundle["score"] >= 58


def test_ticker_signals_summary_premium_redacts_pro_sources_but_keeps_authorized_score(monkeypatch):
    _mock_signal_auth(monkeypatch, tier="premium")
    full_bundle = _full_source_confirmation_bundle("AAPL")
    monkeypatch.setattr(main_module, "_query_unified_signals", lambda **kwargs: [])
    monkeypatch.setattr(
        main_module,
        "build_ticker_signals_summary_contexts_from_cache",
        lambda symbol, **kwargs: _public_summary_context(symbol),
    )
    monkeypatch.setattr(
        main_module,
        "_ticker_confirmation_context",
        lambda db, symbol: {"confirmation_score_bundle": full_bundle},
    )

    response = ticker_signals_summary(object(), "AAPL", side="all", limit=3, lookback_days=365, db=object())
    bundle = response["confirmation_score_bundle"]

    assert bundle["score"] > 0
    assert response["source_entitlements"]["signals"]["locked"] is False
    assert response["source_entitlements"]["signals"]["lock_state"] == "available"
    assert bundle["sources"]["signals"]["present"] is True
    assert bundle["sources"]["signals"].get("locked") is not True
    assert bundle["sources"]["options_flow"]["locked"] is True
    assert bundle["sources"]["options_flow"]["lock_state"] == "pro_locked"
    assert bundle["sources"]["options_flow"]["present"] is False
    assert bundle["sources"]["options_flow"]["strength"] is None
    assert bundle["sources"]["institutional_activity"]["locked"] is True
    assert bundle["sources"]["institutional_activity"]["lock_state"] == "pro_locked"
    assert bundle["sources"]["institutional_activity"]["present"] is False
    assert "options_flow" not in bundle["active_sources"]
    assert "institutional_activity" not in bundle["active_sources"]
    assert response["source_entitlements"]["options_flow"]["lock_state"] == "pro_locked"
    assert response["source_entitlements"]["institutional_activity"]["lock_state"] == "pro_locked"


def test_ticker_signals_summary_premium_missing_signal_data_is_inactive_not_locked(monkeypatch):
    _mock_signal_auth(monkeypatch, tier="premium")
    inactive_context = _public_summary_context("AAPL")
    inactive_context["signals"] = {
        "status": "inactive",
        "direction": "neutral",
        "title": "No current signal conviction",
        "subtitle": "No premium signals in this window.",
        "recent_count": 0,
        "latest_score": None,
    }
    inactive_bundle = confirmation_score_bundle_from_source_contexts(
        "AAPL",
        source_contexts=inactive_context,
    )
    monkeypatch.setattr(main_module, "_query_unified_signals", lambda **kwargs: [])
    monkeypatch.setattr(
        main_module,
        "build_ticker_signals_summary_contexts_from_cache",
        lambda symbol, **kwargs: inactive_context,
    )
    monkeypatch.setattr(
        main_module,
        "_ticker_confirmation_context",
        lambda db, symbol: {"confirmation_score_bundle": inactive_bundle},
    )

    response = ticker_signals_summary(object(), "AAPL", side="all", limit=3, lookback_days=365, db=object())
    bundle = response["confirmation_score_bundle"]

    assert response["source_entitlements"]["signals"]["locked"] is False
    assert response["source_entitlements"]["signals"]["lock_state"] == "available"
    assert response["signals"]["status"] == "inactive"
    assert response["signals"]["status"] != "premium_locked"
    assert bundle["sources"]["signals"].get("locked") is not True
    assert bundle["sources"]["signals"]["present"] is False
    assert bundle["sources"]["signals"].get("lock_state") not in {"premium_locked", "pro_locked"}


def test_ticker_signals_summary_authorized_rows_repair_inactive_signal_source(monkeypatch):
    inactive_context = _public_summary_context("NVDA")
    inactive_context["signals"] = {
        "status": "inactive",
        "direction": "neutral",
        "title": "No active signal stack in the last 30 Days",
        "subtitle": "No qualifying signal entries found in the 30 Day context window.",
        "recent_count": 0,
        "latest_score": None,
    }
    inactive_bundle = confirmation_score_bundle_from_source_contexts("NVDA", source_contexts=inactive_context)
    active_context = _public_summary_context("NVDA")
    active_context["signals"] = {
        "status": "active",
        "direction": "bullish",
        "title": "Signal conviction active",
        "subtitle": "3 recent signals.",
        "recent_count": 3,
        "latest_score": 78,
    }

    monkeypatch.setattr(
        main_module,
        "_query_unified_signals",
        lambda **kwargs: [
            _signal_item(1, symbol="NVDA", days_ago=2, smart_score=78, smart_band="strong"),
            _signal_item(2, symbol="NVDA", days_ago=4, smart_score=78, smart_band="strong"),
            _signal_item(3, symbol="NVDA", days_ago=2, smart_score=69, smart_band="notable"),
        ],
    )
    monkeypatch.setattr(
        main_module,
        "build_ticker_signals_summary_contexts_from_cache",
        lambda symbol, **kwargs: active_context,
    )
    monkeypatch.setattr(
        main_module,
        "_ticker_confirmation_context",
        lambda db, symbol: {"confirmation_score_bundle": inactive_bundle, "institutional_activity_summary": {"status": "ok", "active": False}},
    )

    for tier in ("premium", "admin"):
        _mock_signal_auth(monkeypatch, tier=tier)

        response = ticker_signals_summary(object(), "NVDA", side="all", limit=3, lookback_days=30, db=object())
        bundle = response["confirmation_score_bundle"]

        assert response["signals"]["status"] == "active"
        assert response["recent_signal_count"] == 3
        assert response["items"][0]["smart_score"] == 78
        assert response["source_entitlements"]["signals"]["locked"] is False
        assert bundle["sources"]["signals"]["present"] is True
        assert bundle["sources"]["signals"].get("locked") is not True
        assert bundle["sources"]["signals"]["label"] == "Signal conviction active"
        assert "signals" in bundle["active_sources"]
        assert response["signal_freshness"]["timing"]["active_source_count"] >= 1


def test_ticker_signals_summary_free_rows_stay_locked(monkeypatch):
    _mock_signal_auth(monkeypatch, tier="free")
    monkeypatch.setattr(
        main_module,
        "_query_unified_signals",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("free users must not query signal rows")),
    )
    monkeypatch.setattr(
        main_module,
        "build_ticker_signals_summary_contexts_from_cache",
        lambda symbol, **kwargs: _public_summary_context(symbol),
    )
    monkeypatch.setattr(
        main_module,
        "_ticker_confirmation_context",
        lambda db, symbol: {"confirmation_score_bundle": _full_source_confirmation_bundle(symbol), "institutional_activity_summary": {"status": "ok", "active": True}},
    )

    response = ticker_signals_summary(object(), "NVDA", side="all", limit=3, lookback_days=30, db=object())
    bundle = response["confirmation_score_bundle"]

    assert response["items"] == []
    assert response["source_entitlements"]["signals"]["locked"] is True
    assert bundle["sources"]["signals"]["locked"] is True
    assert bundle["sources"]["signals"]["lock_state"] == "premium_locked"
    assert bundle["sources"]["signals"]["present"] is False


def test_ticker_signals_summary_entitled_institutional_not_configured_is_unavailable(monkeypatch):
    full_bundle = _full_source_confirmation_bundle("NVDA")
    monkeypatch.setattr(main_module, "_query_unified_signals", lambda **kwargs: [])
    monkeypatch.setattr(
        main_module,
        "build_ticker_signals_summary_contexts_from_cache",
        lambda symbol, **kwargs: _public_summary_context(symbol),
    )
    monkeypatch.setattr(
        main_module,
        "_ticker_confirmation_context",
        lambda db, symbol: {
            "confirmation_score_bundle": full_bundle,
            "institutional_activity_summary": {"status": "not_configured", "active": False},
        },
    )

    for tier in ("pro", "admin"):
        _mock_signal_auth(monkeypatch, tier=tier)

        response = ticker_signals_summary(object(), "NVDA", side="all", limit=3, lookback_days=30, db=object())
        source = response["confirmation_score_bundle"]["sources"]["institutional_activity"]

        assert response["source_entitlements"]["institutional_activity"]["locked"] is False
        assert source["present"] is False
        assert source["status"] == "unavailable"
        assert source["reason"] == "not_configured"
        assert source.get("locked") is not True
        assert "institutional_activity" not in response["confirmation_score_bundle"]["active_sources"]


def test_ticker_confirmation_context_marks_absent_institutional_provider_unavailable(monkeypatch):
    inactive_bundle = confirmation_score_bundle_from_source_contexts("NVDA", source_contexts={})

    monkeypatch.setattr(
        main_module,
        "build_confirmation_score_context",
        lambda db, tickers, **kwargs: {
            "bundles": {"NVDA": inactive_bundle},
            "options_flow_summaries": {},
            "government_contracts_summaries": {},
            "institutional_activity_summaries": {"NVDA": {"status": "not_configured", "active": False}},
        },
    )

    context = main_module._ticker_confirmation_context(object(), "NVDA")
    source = context["confirmation_score_bundle"]["sources"]["institutional_activity"]

    assert source["present"] is False
    assert source["status"] == "unavailable"
    assert source["reason"] == "not_configured"
    assert source.get("locked") is not True
    assert context["options_flow_summary"]["state"] == "unavailable"


def test_ticker_confirmation_context_keeps_available_no_institutional_activity_inactive(monkeypatch):
    inactive_bundle = confirmation_score_bundle_from_source_contexts("NVDA", source_contexts={})

    monkeypatch.setattr(
        main_module,
        "build_confirmation_score_context",
        lambda db, tickers, **kwargs: {
            "bundles": {"NVDA": inactive_bundle},
            "options_flow_summaries": {},
            "government_contracts_summaries": {},
            "institutional_activity_summaries": {"NVDA": {"status": "ok", "active": False}},
        },
    )

    context = main_module._ticker_confirmation_context(object(), "NVDA")
    source = context["confirmation_score_bundle"]["sources"]["institutional_activity"]

    assert source["present"] is False
    assert source.get("status") != "unavailable"
    assert source.get("locked") is not True
    assert context["institutional_activity_summary"]["status"] == "ok"


def test_ticker_signals_summary_free_redacts_signals_and_pro_sources_but_keeps_public_score(monkeypatch):
    _mock_signal_auth(monkeypatch, tier="free")
    full_bundle = _full_source_confirmation_bundle("AAPL")
    monkeypatch.setattr(
        main_module,
        "_query_unified_signals",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("free users must not query premium signals")),
    )
    monkeypatch.setattr(
        main_module,
        "build_ticker_signals_summary_contexts_from_cache",
        lambda symbol, **kwargs: _public_summary_context(symbol),
    )
    monkeypatch.setattr(
        main_module,
        "_ticker_confirmation_context",
        lambda db, symbol: {"confirmation_score_bundle": full_bundle},
    )

    response = ticker_signals_summary(object(), "AAPL", side="all", limit=3, lookback_days=365, db=object())
    bundle = response["confirmation_score_bundle"]

    assert bundle["score"] > 0
    assert bundle["sources"]["signals"]["locked"] is True
    assert bundle["sources"]["signals"]["lock_state"] == "premium_locked"
    assert bundle["sources"]["signals"]["present"] is False
    assert bundle["sources"]["options_flow"]["locked"] is True
    assert bundle["sources"]["options_flow"]["lock_state"] == "pro_locked"
    assert bundle["sources"]["institutional_activity"]["locked"] is True
    assert bundle["sources"]["institutional_activity"]["lock_state"] == "pro_locked"
    assert "signals" not in bundle["active_sources"]
    assert "options_flow" not in bundle["active_sources"]
    assert "institutional_activity" not in bundle["active_sources"]
    assert response["source_entitlements"]["signals"]["lock_state"] == "premium_locked"
    assert response["source_entitlements"]["options_flow"]["lock_state"] == "pro_locked"


def _mock_ticker_context_bundle_dependencies(monkeypatch, *, tier: str = "premium") -> dict[str, int]:
    counters = {"profile": 0, "signals": 0}
    _mock_signal_auth(monkeypatch, tier=tier)
    monkeypatch.setattr(
        main_module,
        "_ticker_profile_response",
        lambda symbol, db: counters.__setitem__("profile", counters["profile"] + 1) or {
            "status": "ok",
            "ticker": {
                "symbol": symbol,
                "name": "Apple Inc.",
                "asset_class": "STOCK",
                "sector": "Technology",
                "industry": "Consumer Electronics",
                "country": "US",
                "exchange": "NASDAQ",
                "market_cap": 3_000_000_000_000,
                "volume": 50_000_000,
                "avg_volume": 55_000_000,
            },
            "top_members": [],
            "trades": [],
            "confirmation_score_bundle": None,
            "options_flow_summary": None,
            "why_now": None,
            "signal_freshness": None,
            "technical_indicators": None,
        },
    )
    monkeypatch.setattr(
        main_module,
        "get_current_prices_meta_db",
        lambda db, symbols, **kwargs: {
            "AAPL": {
                "symbol": "AAPL",
                "price": 308.63,
                "change": 14.25,
                "change_percent": 4.84,
                "volume": 51_000_000,
                "asof_ts": datetime(2026, 7, 2, 20, 0, tzinfo=timezone.utc),
                "is_stale": False,
            }
        },
    )

    def fake_query(**kwargs):
        counters["signals"] += 1
        return [
            {
                "kind": "congress",
                "event_id": 99,
                "ts": "2026-07-01T12:00:00Z",
                "symbol": kwargs["symbol"],
                "who": "Nancy Pelosi",
                "trade_type": "purchase",
                "amount_min": 100_000,
                "amount_max": 250_000,
                "smart_score": 88,
                "smart_band": "exceptional",
            }
        ]

    monkeypatch.setattr(main_module, "_query_unified_signals", fake_query)
    monkeypatch.setattr(
        main_module,
        "build_ticker_signals_summary_contexts_from_cache",
        lambda symbol, **kwargs: _public_summary_context(symbol) | {
            "signals": {
                "status": "active" if kwargs.get("signal_rows") else "inactive",
                "direction": "bullish" if kwargs.get("signal_rows") else "neutral",
                "title": "Signal conviction active" if kwargs.get("signal_rows") else "No active signal stack",
                "subtitle": "1 recent signal." if kwargs.get("signal_rows") else "No qualifying signal entries found.",
                "recent_count": len(kwargs.get("signal_rows") or []),
                "latest_score": kwargs.get("latest_signal_score"),
                "latest_date": "2026-07-01T12:00:00Z" if kwargs.get("signal_rows") else None,
                "freshness_days": 1 if kwargs.get("signal_rows") else None,
            },
        },
    )
    monkeypatch.setattr(
        main_module,
        "_ticker_confirmation_context",
        lambda db, symbol: {
            "confirmation_score_bundle": _full_source_confirmation_bundle(symbol),
            "options_flow_summary": {
                "ticker": symbol,
                "lookback_days": 30,
                "state": "bullish",
                "label": "Options flow active",
                "is_active": True,
                "confidence": "moderate",
                "freshness_days": 1,
                "summary": "Options flow active.",
                "signals": ["Options flow active"],
                "metrics": {
                    "put_call_premium_ratio": 0.8,
                    "net_premium_skew": 100_000,
                    "freshness_days": 1,
                },
                "can_confirm": True,
                "provider": "test",
            },
            "institutional_activity_summary": {"status": "ok", "active": True},
        },
    )
    return counters


def test_ticker_context_bundle_uses_segment_entitlements_and_canonical_context(monkeypatch):
    engine = _engine()
    with Session(engine) as db:
        _mock_ticker_context_bundle_dependencies(monkeypatch, tier="premium")
        premium = main_module._build_ticker_context_bundle(
            request=object(),
            symbol="aapl",
            side="all",
            limit=3,
            lookback_days=365,
            db=db,
        )
        assert premium["symbol"] == "AAPL"
        assert premium["ticker"]["name"] == "Apple Inc."
        assert premium["quote"]["current_price"] == 308.63
        assert premium["source_entitlements"]["signals"]["locked"] is False
    assert premium["source_entitlements"]["institutional_activity"]["locked"] is True
    assert premium["confirmation_score_bundle"]["sources"]["institutional_activity"]["locked"] is True
    assert premium["signals_summary"]["items"][0]["smart_score"] == 88
    serialized = json.dumps(premium).lower()
    assert '"provider"' not in serialized
    assert '"vendor"' not in serialized
    assert '"cache"' not in serialized

    with Session(engine) as db:
        _mock_ticker_context_bundle_dependencies(monkeypatch, tier="admin")
        admin = main_module._build_ticker_context_bundle(
            request=object(),
            symbol="aapl",
            side="all",
            limit=3,
            lookback_days=365,
            db=db,
        )
        assert admin["source_entitlements"]["signals"]["locked"] is False
        assert admin["source_entitlements"]["institutional_activity"]["locked"] is False
        assert admin["confirmation_score_bundle"]["sources"]["institutional_activity"].get("locked") is not True


def test_ticker_context_bundle_cache_hit_avoids_rebuild(monkeypatch):
    engine = _engine()
    with Session(engine) as db:
        counters = _mock_ticker_context_bundle_dependencies(monkeypatch, tier="premium")
        first = main_module._build_ticker_context_bundle(
            request=object(),
            symbol="AAPL",
            side="all",
            limit=3,
            lookback_days=30,
            db=db,
        )
        second = main_module._build_ticker_context_bundle(
            request=object(),
            symbol="AAPL",
            side="all",
            limit=3,
            lookback_days=30,
            db=db,
        )
        cache_rows = db.query(TickerContextBundleCache).all()

    assert first["symbol"] == "AAPL"
    assert second["symbol"] == "AAPL"
    assert counters["profile"] == 1
    assert counters["signals"] == 1
    assert len(cache_rows) == 1


def test_ticker_context_bundle_memory_cache_hit_avoids_db_lookup(monkeypatch):
    main_module._TICKER_CONTEXT_BUNDLE_MEMORY_CACHE.clear()
    cache_key = main_module._ticker_context_bundle_cache_key(
        "AAPL",
        user_segment="premium",
        side="all",
        limit=3,
        lookback_days=30,
    )
    now = datetime.now(timezone.utc)
    main_module._ticker_context_bundle_memory_cache_set(
        cache_key,
        payload=_complete_context_bundle_payload("AAPL", from_memory=True),
        stale_after=now + timedelta(minutes=1),
        expires_at=now + timedelta(minutes=5),
    )

    class NoDbLookup:
        def get(self, *_args, **_kwargs):
            raise AssertionError("memory context-bundle hit must not touch the DB cache table")

        def rollback(self):
            raise AssertionError("memory context-bundle hit must not roll back")

    response = main_module._ticker_context_bundle_cache_get(
        NoDbLookup(),
        cache_key,
        symbol="AAPL",
        user_segment="premium",
        started_at=time.perf_counter(),
    )

    assert response["symbol"] == "AAPL"
    assert response["from_memory"] is True


def test_ticker_context_bundle_memory_cache_rejects_lightweight_payload(monkeypatch):
    main_module._TICKER_CONTEXT_BUNDLE_MEMORY_CACHE.clear()
    cache_key = main_module._ticker_context_bundle_cache_key(
        "AAPL",
        user_segment="premium",
        side="all",
        limit=3,
        lookback_days=30,
    )
    now = datetime.now(timezone.utc)
    main_module._ticker_context_bundle_memory_cache_set(
        cache_key,
        payload={"symbol": "AAPL", "status": "lightweight", "quote": None, "source_cards": {}},
        stale_after=now + timedelta(minutes=1),
        expires_at=now + timedelta(minutes=5),
    )

    class NoDbRow:
        def get(self, *_args, **_kwargs):
            return None

        def rollback(self):
            raise AssertionError("missing DB row should not roll back")

    response = main_module._ticker_context_bundle_cache_get(
        NoDbRow(),
        cache_key,
        symbol="AAPL",
        user_segment="premium",
        started_at=time.perf_counter(),
    )

    assert response is None


def test_ticker_context_bundle_db_cache_populates_memory_cache(monkeypatch):
    main_module._TICKER_CONTEXT_BUNDLE_MEMORY_CACHE.clear()
    engine = _engine()
    cache_key = main_module._ticker_context_bundle_cache_key(
        "AAPL",
        user_segment="premium",
        side="all",
        limit=3,
        lookback_days=30,
    )
    now = datetime.now(timezone.utc)
    with Session(engine) as db:
        db.add(
            TickerContextBundleCache(
                cache_key=cache_key,
                symbol="AAPL",
                user_segment="premium",
                payload_json=json.dumps(_complete_context_bundle_payload("AAPL", db_cached=True)),
                generated_at=now,
                stale_after=now + timedelta(minutes=1),
                expires_at=now + timedelta(minutes=5),
            )
        )
        db.commit()

        first = main_module._ticker_context_bundle_cache_get(
            db,
            cache_key,
            symbol="AAPL",
            user_segment="premium",
            started_at=time.perf_counter(),
        )

    class NoDbLookup:
        def get(self, *_args, **_kwargs):
            raise AssertionError("second cache hit should use process memory")

        def rollback(self):
            raise AssertionError("second cache hit should not roll back")

    second = main_module._ticker_context_bundle_cache_get(
        NoDbLookup(),
        cache_key,
        symbol="AAPL",
        user_segment="premium",
        started_at=time.perf_counter(),
    )

    assert first["symbol"] == "AAPL"
    assert first["db_cached"] is True
    assert second["symbol"] == "AAPL"
    assert second["db_cached"] is True


def test_ticker_context_bundle_quote_releases_connection_before_live_fetch(monkeypatch):
    captured: dict[str, object] = {}

    def fake_quote_lookup(db, symbols, **kwargs):
        captured.update(kwargs)
        return {
            "AAPL": {
                "symbol": "AAPL",
                "price": 308.63,
                "change": 1.25,
                "change_percent": 0.4,
                "volume": 51_000_000,
                "asof_ts": datetime(2026, 7, 2, 20, 0, tzinfo=timezone.utc),
                "is_stale": False,
            }
        }

    monkeypatch.setattr(main_module, "get_current_prices_meta_db", fake_quote_lookup)

    quote = main_module._ticker_context_bundle_quote(
        object(),
        "AAPL",
        {"symbol": "AAPL", "name": "Apple Inc.", "exchange": "NASDAQ"},
    )

    assert quote["current_price"] == 308.63
    assert captured["release_connection_before_fetch"] is True
    assert captured["force_quote_endpoint"] is True
    assert captured["allow_live_user_fetch"] is True
    assert captured["bypass_miss_cache"] is True


def test_ticker_context_bundle_quote_fetches_live_without_identity(monkeypatch):
    captured: dict[str, object] = {}

    def fake_quote_lookup(db, symbols, **kwargs):
        captured.update({"symbols": symbols, **kwargs})
        return {
            "ZZZ": {
                "symbol": "ZZZ",
                "price": 12.34,
                "asof_ts": datetime(2026, 7, 2, 20, 0, tzinfo=timezone.utc),
                "is_stale": False,
            }
        }

    monkeypatch.setattr(main_module, "get_current_prices_meta_db", fake_quote_lookup)

    quote = main_module._ticker_context_bundle_quote(
        object(),
        "ZZZ",
        {
            "symbol": "ZZZ",
            "identity_status": "unknown",
            "volume": None,
            "avg_volume": None,
            "market_cap": None,
        },
    )

    assert quote["current_price"] == 12.34
    assert captured["symbols"] == ["ZZZ"]
    assert captured["allow_live_user_fetch"] is True
    assert captured["force_quote_endpoint"] is True
    assert captured["bypass_miss_cache"] is True


def test_ticker_context_bundle_stale_cache_hit_avoids_rebuild(monkeypatch):
    engine = _engine()
    with Session(engine) as db:
        counters = _mock_ticker_context_bundle_dependencies(monkeypatch, tier="premium")
        cache_key = main_module._ticker_context_bundle_cache_key(
            "AAPL",
            user_segment="premium",
            side="all",
            limit=3,
            lookback_days=30,
        )
        now = datetime.now(timezone.utc)
        db.add(
            TickerContextBundleCache(
                cache_key=cache_key,
                symbol="AAPL",
                user_segment="premium",
                payload_json=json.dumps(_complete_context_bundle_payload("AAPL", stale_fixture=True)),
                generated_at=now - timedelta(minutes=10),
                stale_after=now - timedelta(minutes=5),
                expires_at=now + timedelta(minutes=5),
            )
        )
        db.commit()

        response = main_module._build_ticker_context_bundle(
            request=object(),
            symbol="AAPL",
            side="all",
            limit=3,
            lookback_days=30,
            db=db,
        )

    assert response["stale_fixture"] is True
    assert counters["profile"] == 0
    assert counters["signals"] == 0


def test_ticker_context_bundle_build_coalescing_returns_leader_payload(monkeypatch):
    main_module._TICKER_CONTEXT_BUNDLE_INFLIGHT.clear()
    monkeypatch.setattr(main_module, "_ticker_context_bundle_coalesce_wait_seconds", lambda: 1.0)
    state, leader = main_module._ticker_context_bundle_build_inflight_start(
        "bundle-key",
        symbol="AAPL",
        user_segment="premium",
    )
    waiter_state, waiter_leader = main_module._ticker_context_bundle_build_inflight_start(
        "bundle-key",
        symbol="AAPL",
        user_segment="premium",
    )
    assert leader is True
    assert waiter_leader is False
    assert waiter_state is state

    result: list[dict] = []

    def wait_for_payload():
        payload = main_module._ticker_context_bundle_build_inflight_wait(
            object(),
            "bundle-key",
            waiter_state,
            symbol="AAPL",
            user_segment="premium",
            started_at=time.perf_counter(),
        )
        if payload is not None:
            result.append(payload)

    thread = threading.Thread(target=wait_for_payload)
    thread.start()
    time.sleep(0.05)
    main_module._ticker_context_bundle_build_inflight_finalize(
        "bundle-key",
        state,
        leader=True,
        payload={"symbol": "AAPL", "coalesced": True},
    )
    thread.join(timeout=2)

    assert result == [{"symbol": "AAPL", "coalesced": True}]
    assert "bundle-key" not in main_module._TICKER_CONTEXT_BUNDLE_INFLIGHT


def test_ticker_context_bundle_free_does_not_query_premium_signals(monkeypatch):
    engine = _engine()
    with Session(engine) as db:
        _mock_ticker_context_bundle_dependencies(monkeypatch, tier="free")
        monkeypatch.setattr(
            main_module,
            "_query_unified_signals",
            lambda **kwargs: (_ for _ in ()).throw(AssertionError("free bundle must not query premium signal rows")),
        )
        response = main_module._build_ticker_context_bundle(
            request=object(),
            symbol="AAPL",
            side="all",
            limit=3,
            lookback_days=30,
            db=db,
        )

    assert response["signals_summary"]["items"] == []
    assert response["source_entitlements"]["signals"]["locked"] is True
    assert response["source_entitlements"]["institutional_activity"]["locked"] is True
    assert response["confirmation_score_bundle"]["sources"]["signals"]["locked"] is True


def test_ticker_context_bundle_locked_segments_share_canonical_side_and_lookback_cache(monkeypatch):
    engine = _engine()
    with Session(engine) as db:
        counters = _mock_ticker_context_bundle_dependencies(monkeypatch, tier="free")
        first = main_module._build_ticker_context_bundle(
            request=object(),
            symbol="AAPL",
            side="sell",
            limit=3,
            lookback_days=365,
            db=db,
        )
        second = main_module._build_ticker_context_bundle(
            request=object(),
            symbol="AAPL",
            side="all",
            limit=3,
            lookback_days=30,
            db=db,
        )

    assert first["symbol"] == "AAPL"
    assert second["symbol"] == "AAPL"
    assert counters["profile"] == 1
    assert counters["signals"] == 0


def test_ticker_signals_summary_matches_screener_score_context_for_30d(monkeypatch):
    _mock_signal_auth(monkeypatch)
    monkeypatch.setattr(main_module, "_query_unified_signals", lambda **kwargs: [])
    monkeypatch.setattr(
        main_module,
        "_merge_fresh_public_contexts_into_confirmation_bundle",
        lambda bundle, _source_contexts: bundle,
    )

    engine = _engine()
    symbols = ["AAPL", "MSTR", "NBIS"]
    with Session(engine) as db:
        _seed_score_contract_fixture(db)
        db.commit()

        expected_context = build_confirmation_score_context(db, symbols, lookback_days=30)
        expected_by_symbol = expected_context["bundles"]
        for symbol in symbols:
            response = ticker_signals_summary(object(), symbol, side="all", limit=3, lookback_days=365, db=db)
            actual_slim = slim_confirmation_score_bundle(response["confirmation_score_bundle"])
            expected = slim_confirmation_score_bundle(expected_by_symbol[symbol])

            assert response["lookback_days"] == 30
            assert response["effective_window_days"] == 30
            assert response["confirmation_score_bundle"]["lookback_days"] == 30
            assert actual_slim["confirmation_score"] == expected["confirmation_score"]
            assert actual_slim["confirmation_direction"] == expected["confirmation_direction"]
            assert actual_slim["confirmation_status"] == expected["confirmation_status"]
            assert actual_slim["confirmation_source_count"] == expected["confirmation_source_count"]
            assert response["signal_freshness"]["freshness_score"] == expected["signal_freshness"]["freshness_score"]
            assert response["signal_freshness"]["timing"]["active_source_count"] == expected["signal_freshness"]["timing"]["active_source_count"]


def test_ticker_signals_summary_returns_congress_activity_when_events_exist(monkeypatch):
    _mock_signal_auth(monkeypatch)
    monkeypatch.setattr(main_module, "_query_unified_signals", lambda **kwargs: [])
    monkeypatch.setattr(
        main_module,
        "_ticker_price_volume_summary",
        lambda db, symbol: {"status": "inactive", "summary": "No active tape confirmation", "score": 0, "lines": ["RSI near neutral"], "direction": "neutral"},
    )

    engine = _engine()
    with Session(engine) as db:
        db.add(_event(1, symbol="AAPL", event_type="congress_trade", trade_type="purchase", amount=50_000, member_name="Rep One"))
        db.add(_event(2, symbol="AAPL", event_type="congress_trade", trade_type="sale", amount=10_000, member_name="Rep Two"))
        db.add(GovernmentContract(symbol="MSFT", award_date=date.today(), award_amount=2_000_000, source="test"))
        db.commit()

        response = ticker_signals_summary(object(), "AAPL", side="all", limit=3, lookback_days=30, db=db)

    assert response["congress"]["status"] == "active"
    assert response["congress"]["buy_count"] == 1
    assert response["congress"]["sell_count"] == 1
    assert response["congress"]["title"] != "No recent Congress trades"
    assert response["insiders"]["status"] == "inactive"
    assert response["confirmation_score_bundle"]["score"] > 0
    assert response["confirmation_score_bundle"]["status"] != "Inactive"


def test_ticker_signals_summary_returns_nbis_insider_sell_activity(monkeypatch):
    _mock_signal_auth(monkeypatch)
    monkeypatch.setattr(main_module, "_query_unified_signals", lambda **kwargs: [])
    monkeypatch.setattr(
        main_module,
        "_ticker_price_volume_summary",
        lambda db, symbol: {"status": "inactive", "summary": "No active tape confirmation", "score": 0, "lines": ["RSI near neutral"], "direction": "neutral"},
    )

    engine = _engine()
    with Session(engine) as db:
        db.add(_event(10, symbol="NBIS", event_type="insider_trade", trade_type="sale", amount=125_000, member_name="NBIS Insider"))
        db.add(GovernmentContract(symbol="MSFT", award_date=date.today(), award_amount=2_000_000, source="test"))
        db.commit()

        response = ticker_signals_summary(object(), "NBIS", side="all", limit=3, lookback_days=30, db=db)

    assert response["insiders"]["status"] == "active"
    assert response["insiders"]["direction"] == "bearish"
    assert response["insiders"]["sell_count"] == 1
    assert response["insiders"]["buy_count"] == 0
    assert response["insiders"]["net_flow"] == -125_000
    assert response["confirmation_score_bundle"]["direction"] == "bearish"
    assert response["confirmation_score_bundle"]["score"] > 0
    assert response["signal_freshness"]["timing"]["active_source_count"] == 1


def test_ticker_signals_summary_conflicting_sources_produce_mixed_confirmation(monkeypatch):
    _mock_signal_auth(monkeypatch)
    monkeypatch.setattr(main_module, "_query_unified_signals", lambda **kwargs: [])
    monkeypatch.setattr(
        main_module,
        "_ticker_price_volume_summary",
        lambda db, symbol: {"status": "unavailable", "summary": "Price and volume unavailable", "score": None, "lines": ["Price and volume unavailable"], "direction": "neutral"},
    )

    engine = _engine()
    with Session(engine) as db:
        db.add(_event(20, symbol="MSTR", event_type="insider_trade", trade_type="sale", amount=125_000, member_name="MSTR Insider"))
        db.add(_event(21, symbol="MSTR", event_type="congress_trade", trade_type="purchase", amount=125_000, member_name="Rep Buyer"))
        db.commit()

        response = ticker_signals_summary(object(), "MSTR", side="all", limit=3, lookback_days=365, db=db)

    assert response["lookback_days"] == 30
    assert response["effective_window_days"] == 30
    assert response["confirmation_score_bundle"]["lookback_days"] == 30
    assert response["confirmation_score_bundle"]["direction"] == "mixed"
    assert response["confirmation_score_bundle"]["status"] != "Inactive"
    assert response["confirmation_score_bundle"]["score"] > 0
    assert response["signal_freshness"]["timing"]["active_source_count"] == 2


def test_ticker_signals_summary_inactive_government_contracts_when_dataset_has_no_symbol_rows(monkeypatch):
    _mock_signal_auth(monkeypatch)
    monkeypatch.setattr(main_module, "_query_unified_signals", lambda **kwargs: [])
    monkeypatch.setattr(
        main_module,
        "_ticker_price_volume_summary",
        lambda db, symbol: {"status": "inactive", "summary": "No active tape confirmation", "score": 0, "lines": ["RSI near neutral"], "direction": "neutral"},
    )

    engine = _engine()
    with Session(engine) as db:
        db.add(GovernmentContract(symbol="MSFT", award_date=date.today(), award_amount=2_000_000, source="test"))
        db.commit()

        response = ticker_signals_summary(object(), "AAPL", side="all", limit=3, lookback_days=30, db=db)

    assert response["government_contracts"]["status"] == "inactive"
    assert response["government_contracts"]["title"] == "No major government contracts"
    assert response["government_contracts"]["contract_count"] == 0


def test_ticker_profiles_batch_returns_lightweight_shells(monkeypatch):
    def fail_full_profile(*args, **kwargs):
        raise AssertionError("batch ticker profiles should not build full ticker pages")

    monkeypatch.setattr(main_module, "_build_ticker_profile", fail_full_profile)
    monkeypatch.setattr(main_module, "enqueue_data_enrichment_job", lambda *args, **kwargs: True)

    engine = _engine()
    with Session(engine) as db:
        db.add(TickerMeta(symbol="SAIC", company_name="Science Applications International", exchange="NYSE", sector="Technology", industry="Information Technology Services", country="US"))
        db.add(TickerMeta(symbol="BAH", company_name="Booz Allen Hamilton", exchange="NYSE", sector="Industrials", industry="Consulting Services", country="US"))
        db.commit()

        response = _ticker_profiles_response("SAIC,BAH", db)

    assert response["tickers"]["SAIC"]["ticker"]["name"] == "Science Applications International"
    assert response["tickers"]["BAH"]["ticker"]["name"] == "Booz Allen Hamilton"
    assert response["tickers"]["SAIC"]["ticker"]["sector"] == "Technology"


def test_ticker_price_volume_summary_distinguishes_missing_and_inactive(monkeypatch):
    monkeypatch.setattr(
        main_module,
        "build_ticker_technical_indicators",
        lambda *args, **kwargs: {
            "price_points": 0,
            "rsi": {"status": "unavailable", "signal": "unavailable", "message": "RSI temporarily unavailable"},
            "macd": {"status": "unavailable", "signal": "unavailable", "message": "MACD temporarily unavailable"},
            "ema_trend": {"status": "unavailable", "signal": "unavailable", "message": "EMA trend temporarily unavailable"},
        },
    )
    missing = main_module._ticker_price_volume_summary(object(), "NBIS")
    assert missing["status"] == "unavailable"
    assert missing["summary"] == "Price and volume unavailable"

    monkeypatch.setattr(
        main_module,
        "build_ticker_technical_indicators",
        lambda *args, **kwargs: {
            "price_points": 60,
            "rsi": {"status": "ok", "signal": "neutral", "message": "RSI near neutral"},
            "macd": {"status": "ok", "signal": "neutral", "message": "MACD mixed"},
            "ema_trend": {"status": "ok", "signal": "neutral", "message": "EMA trend mixed"},
        },
    )
    engine = _engine()
    today = date.today()
    with Session(engine) as db:
        for offset in range(40):
            day = today - timedelta(days=39 - offset)
            db.add(PriceCache(symbol="NBIS", date=day.isoformat(), close=100 + offset, volume=1_000_000 + offset))
        db.commit()
        inactive = main_module._ticker_price_volume_summary(db, "NBIS")

    assert inactive["status"] == "active"
    assert inactive["direction"] == "neutral"
    assert inactive["summary"] == "Price and volume available"
    assert inactive["latest_close"] is not None
    assert inactive["latest_volume"] is not None
    assert inactive["avg_volume_20d"] is not None
    assert inactive["avg_volume_30d"] == inactive["avg_volume_20d"]


def test_ticker_price_volume_summary_uses_cached_price_rows_without_provider(monkeypatch):
    def fail_provider_loader(*_args, **_kwargs):
        raise AssertionError("signals-summary price volume must not hydrate providers")

    monkeypatch.setattr("app.services.technical_indicators.get_daily_close_series_with_fallback", fail_provider_loader)
    engine = _engine()
    today = date.today()
    with Session(engine) as db:
        for symbol in ("AAPL", "MSTR", "NBIS"):
            for offset in range(70):
                day = today - timedelta(days=69 - offset)
                db.add(
                    PriceCache(
                        symbol=symbol,
                        date=day.isoformat(),
                        close=100 + offset,
                        volume=1_000_000 + offset * 10_000,
                        day_volume=1_000_000 + offset * 10_000,
                    )
                )
        db.commit()

        for symbol in ("AAPL", "MSTR", "NBIS"):
            summary = main_module._ticker_price_volume_summary(db, symbol)
            assert summary["status"] == "active"
            assert summary["status"] != "unavailable"
            assert summary["inputs"]["has_price_series"] is True
            assert summary["inputs"]["has_volume"] is True
            assert summary["inputs"]["point_count"] >= 35
            assert summary["latest_close"] is not None
            assert summary["latest_volume"] is not None
            assert summary["avg_volume_20d"] is not None
            assert summary["avg_volume_30d"] == summary["avg_volume_20d"]
            assert summary["volume_vs_avg"] is not None
            assert summary["rsi"]["status"] == "ok"
            assert summary["rsi"]["value"] is not None
            assert summary["macd"]["status"] == "ok"
            assert summary["macd"]["signal"] in {"bullish", "bearish", "neutral"}


def test_ticker_price_volume_summary_computes_volume_ratio_against_30_day_average(monkeypatch):
    monkeypatch.setattr(main_module, "_is_public_api_request_context", lambda: False)
    engine = _engine()
    today = date.today()
    with Session(engine) as db:
        for offset in range(40):
            day = today - timedelta(days=39 - offset)
            db.add(
                PriceCache(
                    symbol="AAPL",
                    date=day.isoformat(),
                    close=100 + offset,
                    volume=1_000_000 + offset,
                    day_volume=1_000_000 + offset,
                )
            )
        db.commit()

        summary = main_module._ticker_price_volume_summary(db, "AAPL")

    expected_avg = sum(1_000_000 + offset for offset in range(10, 40)) / 30
    assert summary["avg_volume_30d"] == expected_avg
    assert summary["avg_volume_20d"] == expected_avg
    assert summary["volume_vs_avg"] == round((1_000_000 + 39) / expected_avg, 4)
    assert any("Volume vs 30D avg" in line for line in summary["lines"])


def test_ticker_price_volume_summary_uses_fundamentals_volume_when_latest_daily_row_is_close_only(monkeypatch):
    monkeypatch.setattr(main_module, "_is_public_api_request_context", lambda: False)
    engine = _engine()
    today = date.today()
    with Session(engine) as db:
        for offset in range(70):
            day = today - timedelta(days=69 - offset)
            db.add(
                PriceCache(
                    symbol="NVDA",
                    date=day.isoformat(),
                    close=150 + offset,
                    volume=None if offset == 69 else 100_000_000 + offset,
                    day_volume=None if offset == 69 else 100_000_000 + offset,
                )
            )
        db.add(
            FundamentalsCache(
                symbol="NVDA",
                provider="fmp",
                fetched_at=datetime.now(timezone.utc),
                status="ok",
                volume=164_369_080,
                avg_volume=151_121_552,
            )
        )
        db.commit()

        summary = main_module._ticker_price_volume_summary(db, "NVDA")

    assert summary["status"] == "active"
    assert summary["title"] != "Limited price/volume history"
    assert summary["latest_volume"] == 164_369_080
    assert summary["avg_volume_20d"] is not None
    assert summary["avg_volume_30d"] == summary["avg_volume_20d"]
    assert summary["volume_vs_avg"] is not None


def test_ticker_price_volume_summary_uses_intraday_one_minute_snapshot_for_live_requests(monkeypatch):
    today = date.today()
    monkeypatch.setattr(main_module, "_is_public_api_request_context", lambda: True)
    monkeypatch.setattr(
        main_module,
        "_ticker_intraday_price_volume_snapshot",
        lambda symbol: {
            "price": 225.5,
            "date": today.isoformat(),
            "day_volume": 12_500_000,
            "source": "historical-chart/1min",
        },
    )

    engine = _engine()
    with Session(engine) as db:
        for offset in range(70):
            day = today - timedelta(days=70 - offset)
            db.add(
                PriceCache(
                    symbol="NVDA",
                    date=day.isoformat(),
                    close=150 + offset,
                    volume=100_000_000 + offset,
                    day_volume=100_000_000 + offset,
                )
            )
        db.commit()

        summary = main_module._ticker_price_volume_summary(db, "NVDA")

    assert summary["status"] == "active"
    assert summary["latest_close"] == 225.5
    assert summary["latest_date"] == today.isoformat()
    assert summary["latest_source"] == "historical-chart/1min"
    assert summary["latest_volume"] == 12_500_000
    assert any(line.startswith("Latest price: 225.50") for line in summary["lines"])
    assert summary["title"] != "Limited price/volume history"


def test_ticker_intraday_price_volume_snapshot_uses_stable_one_minute_chart(monkeypatch):
    main_module._TICKER_INTRADAY_PRICE_VOLUME_CACHE.clear()
    calls: list[tuple[str, dict]] = []

    class FakeResponse:
        status_code = 200

        def json(self):
            return [
                {"date": "2026-07-17 15:59:00", "close": 225.5, "volume": 200},
                {"date": "2026-07-17 15:58:00", "close": 224.9, "volume": 300},
                {"date": "2026-07-16 15:59:00", "close": 220.0, "volume": 10_000},
            ]

    def fake_get(url, params=None, timeout=3):
        calls.append((url, dict(params or {})))
        return FakeResponse()

    monkeypatch.setenv("FMP_API_KEY", "secret-key")
    monkeypatch.setenv("FMP_PERSIST_USAGE_EVENTS", "0")
    monkeypatch.setattr(main_module.requests, "get", fake_get)

    snapshot = main_module._ticker_intraday_price_volume_snapshot("NVDA")

    assert calls
    assert calls[0][0].endswith("/stable/historical-chart/1min")
    assert calls[0][1]["symbol"] == "NVDA"
    assert calls[0][1]["from"]
    assert calls[0][1]["to"]
    assert snapshot["price"] == 225.5
    assert snapshot["date"] == "2026-07-17"
    assert snapshot["day_volume"] == 500
    assert snapshot["source"] == "historical-chart/1min"


def test_ticker_price_volume_summary_loading_only_when_hydration_pending(monkeypatch):
    monkeypatch.setattr(
        main_module,
        "build_ticker_technical_indicators",
        lambda *args, **kwargs: {
            "price_points": 0,
            "rsi": {"status": "unavailable", "signal": "unavailable", "message": "RSI temporarily unavailable"},
            "macd": {"status": "unavailable", "signal": "unavailable", "message": "MACD temporarily unavailable"},
            "ema_trend": {"status": "unavailable", "signal": "unavailable", "message": "EMA trend temporarily unavailable"},
        },
    )
    engine = _engine()
    with Session(engine) as db:
        db.add(
            DataEnrichmentJob(
                job_type="technical_indicators",
                symbol="NBIS",
                dedupe_key="technical_indicators|NBIS|technical:90d|",
                status="queued",
            )
        )
        db.commit()

        summary = main_module._ticker_price_volume_summary(db, "NBIS")

    assert summary["status"] == "unavailable"
    assert summary["summary"] == "Updating price and volume data"
