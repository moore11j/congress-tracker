import os
from types import SimpleNamespace

os.environ.setdefault("DATABASE_URL", "sqlite://")

import pytest
import requests
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.models import TickerContentCache
from app.services import options_calculator as service


@pytest.fixture
def provider(monkeypatch):
    engine = create_engine("sqlite://")
    TickerContentCache.__table__.create(engine)
    monkeypatch.setattr(service, "SessionLocal", sessionmaker(bind=engine))
    monkeypatch.setenv("MASSIVE_API_KEY", "test-secret-not-for-output")
    calls = []
    payload = {"results": [{"c": 4.25, "t": 1789689600000}], "status": "OK"}
    def get(url, **kwargs):
        calls.append((url, kwargs))
        return SimpleNamespace(status_code=200, json=lambda: payload)
    monkeypatch.setattr(service.requests, "get", get)
    yield calls, payload
    engine.dispose()


def test_close_is_dated_cached_and_credential_free(provider):
    calls, _ = provider
    result = service.previous_close("O:SPY261016C00100000")
    assert result["price"] == 4.25
    assert result["price_basis"] == "previous_session_close"
    assert result["as_of"].startswith("2026-")
    assert service.previous_close("O:SPY261016C00100000") == result
    assert len(calls) == 1
    assert "test-secret" not in calls[0][0]
    assert calls[0][1]["headers"]["Authorization"].startswith("Bearer ")
    assert "apiKey" not in calls[0][1]["params"]
    assert "test-secret" not in str(result)


def test_sliding_budget_counts_requests_but_not_cache_hits(provider):
    calls, _ = provider
    for ticker in ["SPY", "QQQ", "AAPL", "MSFT", "NVDA"]:
        service.previous_close(ticker)
    service.previous_close("SPY")
    with pytest.raises(service.OptionsDataError, match="5 requests") as error:
        service.previous_close("TSLA")
    assert error.value.status == 429
    assert len(calls) == 5


def test_reference_filters_adjusted_contracts_and_strips_pagination_url(provider):
    calls, payload = provider
    normal = {"ticker": "O:SPY261016C00100000", "contract_type": "call", "strike_price": 100, "expiration_date": "2026-10-16", "shares_per_contract": 100, "exercise_style": "american"}
    payload.update(results=[normal, {**normal, "shares_per_contract": 10}, {**normal, "additional_underlyings": [{"amount": 5}]}], next_url="https://api.massive.com/?apiKey=secret")
    result = service.contracts("SPY", "2026-10-16")
    assert len(result["contracts"]) == 1
    assert result["excluded"] == 2
    assert result["truncated"] is True
    assert "secret" not in str(result)
    assert calls[0][1]["params"]["expiration_date"] == "2026-10-16"


@pytest.mark.parametrize("status,expected", [(403, 503), (429, 429), (500, 503)])
def test_provider_errors_are_sanitized(provider, monkeypatch, status, expected):
    monkeypatch.setattr(service.requests, "get", lambda *a, **kw: SimpleNamespace(status_code=status))
    with pytest.raises(service.OptionsDataError) as error:
        service.previous_close("SPY")
    assert error.value.status == expected
    assert "test-secret" not in str(error.value)


def test_failed_requests_consume_budget(provider, monkeypatch):
    def fail(*args, **kwargs):
        raise requests.Timeout("secret in original exception")
    monkeypatch.setattr(service.requests, "get", fail)
    for _ in range(5):
        with pytest.raises(service.OptionsDataError, match="could not be loaded"):
            service.previous_close("SPY")
    with pytest.raises(service.OptionsDataError, match="5 requests"):
        service.previous_close("SPY")


def test_no_trade_is_not_a_zero_premium(provider):
    _, payload = provider
    payload["results"] = []
    with pytest.raises(service.OptionsDataError, match="No previous-session trade") as error:
        service.previous_close("SPY")
    assert error.value.status == 404


def test_missing_key_has_manual_fallback(provider, monkeypatch):
    monkeypatch.delenv("MASSIVE_API_KEY")
    monkeypatch.delenv("POLYGON_API_KEY", raising=False)
    with pytest.raises(service.OptionsDataError, match="enter prices manually"):
        service.previous_close("SPY")
    assert not provider[0]


def test_routes_reject_paths_and_invalid_dates_before_provider_calls(provider):
    import asyncio
    from fastapi import FastAPI
    from app.routers.options_calculator import router
    from app.rate_limit import rate_limit_provider_backed
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[rate_limit_provider_backed] = lambda: None
    async def status(path, query):
        messages = []
        async def send(message):
            messages.append(message)
        async def receive():
            return {"type": "http.request", "body": b"", "more_body": False}
        await app({"type": "http", "asgi": {"version": "3.0"}, "http_version": "1.1", "method": "GET", "scheme": "http", "path": path, "raw_path": path.encode(), "query_string": query.encode(), "headers": [], "server": ("test", 80), "client": ("test", 1)}, receive, send)
        return next(m["status"] for m in messages if m["type"] == "http.response.start")
    assert asyncio.run(status("/tools/options/close", "ticker=../../private")) == 422
    assert asyncio.run(status("/tools/options/contracts", "symbol=SPY&expiration=invalid")) == 422
    assert not provider[0]
