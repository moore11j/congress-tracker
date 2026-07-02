from __future__ import annotations

import json
import threading
import time
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

import app.main as main_module
import app.routers.signals as signals_module
import app.services.confirmation_score as confirmation_score_module
from app.db import Base
from app.entitlements import ENTITLEMENTS
from app.main import _ticker_profiles_response, ticker_signals_summary
from app.models import DataEnrichmentJob, Event, GovernmentContract, PriceCache, TickerMeta
from app.services.confirmation_context import build_confirmation_score_context
from app.services.confirmation_score import confirmation_score_bundle_from_source_contexts, slim_confirmation_score_bundle


def _engine():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    return engine


def _mock_signal_auth(monkeypatch, tier: str = "premium"):
    monkeypatch.setattr(main_module, "current_user", lambda *args, **kwargs: object())
    monkeypatch.setattr(main_module, "current_entitlements", lambda *args, **kwargs: ENTITLEMENTS[tier])
    monkeypatch.setattr(main_module, "require_feature", lambda *args, **kwargs: None)
    main_module._TICKER_SIGNALS_SUMMARY_CACHE.clear()
    main_module._TICKER_SIGNALS_SUMMARY_INFLIGHT.clear()


def _mock_logged_out_signal_context(monkeypatch):
    monkeypatch.setattr(main_module, "current_user", lambda *args, **kwargs: None)
    monkeypatch.setattr(main_module, "current_entitlements", lambda *args, **kwargs: ENTITLEMENTS["free"])
    main_module._TICKER_SIGNALS_SUMMARY_CACHE.clear()
    main_module._TICKER_SIGNALS_SUMMARY_INFLIGHT.clear()


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
        "^GSPC": (100, 102),
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
    assert bundle["score"] == full_bundle["score"]


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


def test_ticker_signals_summary_matches_screener_score_context_for_30d(monkeypatch):
    _mock_signal_auth(monkeypatch)
    monkeypatch.setattr(main_module, "_query_unified_signals", lambda **kwargs: [])

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
            assert summary["volume_vs_avg"] is not None


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
