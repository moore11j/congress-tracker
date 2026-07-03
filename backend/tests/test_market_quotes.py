from datetime import datetime

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import app.main as app_main
from app.db import Base
from app.main import _build_market_quotes_response
from app.models import PriceCache, QuoteCache, TickerMeta
from app.request_priority import RoutePriority, classify_request


@pytest.fixture(autouse=True)
def clear_market_quote_caches():
    app_main._MARKET_QUOTES_EOD_CACHE.clear()
    app_main._MARKET_QUOTES_RESPONSE_CACHE.clear()
    yield
    app_main._MARKET_QUOTES_EOD_CACHE.clear()
    app_main._MARKET_QUOTES_RESPONSE_CACHE.clear()


def _session():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    TestSession = sessionmaker(bind=engine, future=True)
    return TestSession()


def test_market_quotes_reads_cached_quotes_without_requiring_all_symbols(monkeypatch):
    db = _session()
    db.add(TickerMeta(symbol="NVDA", company_name="NVIDIA Corp", exchange="NASDAQ"))
    db.add(PriceCache(symbol="NVDA", date="2026-06-02", close=120.0))
    db.commit()

    def fake_quotes(_db_arg, symbols, **_kwargs):
        assert symbols == ["NVDA", "AAPL"]
        return {"NVDA": {"symbol": "NVDA", "price": 123.45, "asof_ts": datetime(2026, 6, 3, 20, 0, 0)}}

    monkeypatch.setattr(app_main, "get_current_prices_meta_db", fake_quotes)

    response = _build_market_quotes_response("nvda,AAPL,INVALID!,NVDA", db)

    assert response["status"] == "partial"
    assert response["items"][0] == {
        "symbol": "NVDA",
        "company_name": "NVIDIA Corp",
        "current_price": 123.45,
        "day_change_pct": (123.45 - 120.0) / 120.0 * 100,
        "as_of": "2026-06-03T20:00:00",
    }
    assert response["items"][1] == {
        "symbol": "AAPL",
        "company_name": "AAPL",
        "current_price": None,
        "day_change_pct": None,
        "as_of": None,
    }


def test_market_quotes_use_live_user_quote_path(monkeypatch):
    db = _session()
    calls = []

    def fake_quotes(db_arg, symbols, **kwargs):
        calls.append((db_arg, symbols, kwargs))
        return {
            "NVDA": {
                "symbol": "NVDA",
                "price": 160.25,
                "change_percent": 1.75,
                "asof_ts": datetime(2026, 7, 3, 15, 30, 0),
                "is_stale": False,
            }
        }

    monkeypatch.setattr(app_main, "get_current_prices_meta_db", fake_quotes)

    response = _build_market_quotes_response("NVDA", db)

    assert response["status"] == "ok"
    assert response["items"][0]["current_price"] == 160.25
    assert response["items"][0]["day_change_pct"] == 1.75
    assert calls
    _, symbols, kwargs = calls[0]
    assert symbols == ["NVDA"]
    assert kwargs["allow_cache_write"] is False
    assert kwargs["allow_live_user_fetch"] is True
    assert kwargs["stale_while_revalidate"] is False
    assert kwargs["release_connection_before_fetch"] is True
    assert kwargs["force_quote_endpoint"] is True


def test_market_quotes_prefer_quote_change_percent_over_historical_close(monkeypatch):
    db = _session()
    db.add(PriceCache(symbol="AAPL", date="2026-07-02", close=100.0))
    db.commit()

    monkeypatch.setattr(
        app_main,
        "get_current_prices_meta_db",
        lambda *_args, **_kwargs: {
            "AAPL": {
                "symbol": "AAPL",
                "price": 210.0,
                "change_percent": 0.5,
                "asof_ts": datetime(2026, 7, 3, 15, 30, 0),
                "is_stale": False,
            }
        },
    )

    response = _build_market_quotes_response("AAPL", db)

    assert response["items"][0]["current_price"] == 210.0
    assert response["items"][0]["day_change_pct"] == 0.5
    assert response["items"][0]["day_change_pct"] != 110.0


def test_market_quotes_use_eod_light_previous_close_before_local_cache(monkeypatch):
    db = _session()
    db.add(PriceCache(symbol="AAPL", date="2026-07-02", close=100.0))
    db.commit()

    monkeypatch.setattr(
        app_main,
        "get_current_prices_meta_db",
        lambda *_args, **_kwargs: {
            "AAPL": {
                "symbol": "AAPL",
                "price": 210.0,
                "asof_ts": datetime(2026, 7, 3, 15, 30, 0),
                "is_stale": False,
            }
        },
    )
    monkeypatch.setattr(
        app_main,
        "_latest_eod_light_closes_by_symbol",
        lambda _symbols: {"AAPL": [{"date": "2026-07-02", "close": 200.0}]},
    )

    response = _build_market_quotes_response("AAPL", db)

    assert response["items"][0]["current_price"] == 210.0
    assert response["items"][0]["day_change_pct"] == 5.0
    assert response["items"][0]["day_change_pct"] != 110.0


def test_market_quotes_use_eod_light_close_when_same_date_as_intraday(monkeypatch):
    db = _session()

    monkeypatch.setattr(
        app_main,
        "get_current_prices_meta_db",
        lambda *_args, **_kwargs: {
            "AAPL": {
                "symbol": "AAPL",
                "price": 308.25,
                "asof_ts": datetime(2026, 7, 2, 15, 59, 0),
                "is_stale": False,
            }
        },
    )
    monkeypatch.setattr(
        app_main,
        "_latest_eod_light_closes_by_symbol",
        lambda _symbols: {
            "AAPL": [
                {"date": "2026-07-02", "close": 308.63},
                {"date": "2026-07-01", "close": 294.38},
            ]
        },
    )

    response = _build_market_quotes_response("AAPL", db)

    assert response["items"][0]["current_price"] == 308.63
    assert response["items"][0]["day_change_pct"] == (308.63 - 294.38) / 294.38 * 100
    assert response["items"][0]["as_of"] == "2026-07-02T16:00:00"


def test_market_quotes_eod_light_previous_close_is_cached(monkeypatch):
    calls = []

    class FakeResponse:
        status_code = 200

        def json(self):
            return [{"symbol": "AAPL", "date": "2026-07-02", "price": 200.0}]

    def fake_get(url, params=None, timeout=10):
        calls.append((url, dict(params or {})))
        return FakeResponse()

    monkeypatch.setenv("FMP_API_KEY", "secret-key")
    monkeypatch.setattr(app_main.requests, "get", fake_get)

    first = app_main._latest_eod_light_closes_by_symbol(["AAPL"])
    second = app_main._latest_eod_light_closes_by_symbol(["AAPL"])

    assert first == {"AAPL": [{"date": "2026-07-02", "close": 200.0}]}
    assert second == first
    assert len(calls) == 1


def test_market_quotes_response_cache_hit_avoids_repeated_quote_fetch(monkeypatch):
    db = _session()
    calls = []

    def fake_quotes(_db_arg, symbols, **_kwargs):
        calls.append(tuple(symbols))
        return {
            "AAPL": {
                "symbol": "AAPL",
                "price": 210.0,
                "asof_ts": datetime(2026, 7, 3, 15, 30, 0),
                "is_stale": False,
            }
        }

    monkeypatch.setattr(app_main, "get_current_prices_meta_db", fake_quotes)
    monkeypatch.setattr(app_main, "_latest_eod_light_closes_by_symbol", lambda _symbols: {})

    first = _build_market_quotes_response("AAPL", db)
    second = _build_market_quotes_response("AAPL", db)

    assert first == second
    assert calls == [("AAPL",)]


def test_market_quotes_response_cache_key_respects_symbols(monkeypatch):
    db = _session()
    calls = []

    def fake_quotes(_db_arg, symbols, **_kwargs):
        calls.append(tuple(symbols))
        return {
            symbol: {
                "symbol": symbol,
                "price": 100.0,
                "asof_ts": datetime(2026, 7, 3, 15, 30, 0),
                "is_stale": False,
            }
            for symbol in symbols
        }

    monkeypatch.setattr(app_main, "get_current_prices_meta_db", fake_quotes)
    monkeypatch.setattr(app_main, "_latest_eod_light_closes_by_symbol", lambda _symbols: {})

    _build_market_quotes_response("AAPL", db)
    _build_market_quotes_response("NVDA", db)
    _build_market_quotes_response("AAPL", db)

    assert calls == [("AAPL",), ("NVDA",)]


def test_market_quotes_eod_failure_does_not_break_entire_response(monkeypatch):
    db = _session()

    monkeypatch.setattr(
        app_main,
        "get_current_prices_meta_db",
        lambda *_args, **_kwargs: {
            "AAPL": {
                "symbol": "AAPL",
                "price": 210.0,
                "asof_ts": datetime(2026, 7, 3, 15, 30, 0),
                "is_stale": False,
            },
            "NVDA": {
                "symbol": "NVDA",
                "price": 160.0,
                "asof_ts": datetime(2026, 7, 3, 15, 30, 0),
                "is_stale": False,
            },
        },
    )

    class FakeResponse:
        def __init__(self, status_code, payload):
            self.status_code = status_code
            self._payload = payload

        def json(self):
            return self._payload

    def fake_get(_url, params=None, timeout=10):
        if dict(params or {}).get("symbol") == "AAPL":
            return FakeResponse(200, [{"symbol": "AAPL", "date": "2026-07-02", "price": 200.0}])
        return FakeResponse(503, [])

    monkeypatch.setenv("FMP_API_KEY", "secret-key")
    monkeypatch.setattr(app_main.requests, "get", fake_get)

    response = _build_market_quotes_response("AAPL,NVDA", db)

    assert response["status"] == "ok"
    assert response["items"][0]["symbol"] == "AAPL"
    assert response["items"][0]["day_change_pct"] == 5.0
    assert response["items"][1]["symbol"] == "NVDA"
    assert response["items"][1]["current_price"] == 160.0
    assert response["items"][1]["day_change_pct"] is None


def test_market_quotes_response_has_no_provider_wording(monkeypatch):
    db = _session()
    monkeypatch.setattr(
        app_main,
        "get_current_prices_meta_db",
        lambda *_args, **_kwargs: {
            "AAPL": {
                "symbol": "AAPL",
                "price": 210.0,
                "change_percent": 0.5,
                "asof_ts": datetime(2026, 7, 3, 15, 30, 0),
                "is_stale": False,
            }
        },
    )

    response = _build_market_quotes_response("AAPL", db)
    rendered = str(response).lower()

    assert "fmp" not in rendered
    assert "provider" not in rendered
    assert "vendor" not in rendered
    assert "cache" not in rendered


def test_market_quotes_are_bounded_and_normal_priority():
    db = _session()
    symbols = ",".join(f"S{index}" for index in range(20))

    response = _build_market_quotes_response(symbols, db)

    assert len(response["items"]) == 12
    assert response["status"] == "unavailable"
    assert classify_request("/api/market/quotes", {"symbols": "NVDA,AAPL"}) == RoutePriority.NORMAL


def test_persisted_congress_leaderboard_is_not_global_heavy_priority():
    assert classify_request("/api/leaderboards/congress-traders", {"performance_model": "portfolio"}) == RoutePriority.NORMAL
    assert classify_request("/api/leaderboards/other-expensive-view", {}) == RoutePriority.HEAVY
