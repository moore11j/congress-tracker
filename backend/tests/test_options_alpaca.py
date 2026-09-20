import os
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

os.environ.setdefault("DATABASE_URL", "sqlite://")

import pytest
import requests
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.models import TickerContentCache
from app.services import options_alpaca as service

A = "O:SPY261016C00750000"
B = "O:SPY261016P00750000"


@pytest.fixture
def provider(monkeypatch):
    engine = create_engine("sqlite://")
    TickerContentCache.__table__.create(engine)
    monkeypatch.setattr(service, "SessionLocal", sessionmaker(bind=engine))
    monkeypatch.setenv("APCA_API_KEY_ID", "test-key-id")
    monkeypatch.setenv("APCA_API_SECRET_KEY", "test-secret")
    monkeypatch.setenv("OPTIONS_PRICE_PROVIDER", "alpaca")
    calls = []
    stamp = (datetime.now(timezone.utc) - timedelta(days=1)).replace(hour=4, minute=0, second=0, microsecond=0).isoformat()
    payload = {"bars": {A[2:]: [{"c": 4.25, "t": stamp}]}, "next_page_token": None}
    def get(url, **kwargs):
        calls.append((url, kwargs))
        return SimpleNamespace(status_code=200, json=lambda: payload)
    monkeypatch.setattr(service.requests, "get", get)
    yield calls, payload
    engine.dispose()


def test_actual_daily_closes_are_batched_cached_and_dated(provider):
    calls, payload = provider
    result = service.prices([B, A])
    assert result["closes"] == [{"ticker": A, "price": 4.25, "as_of": payload["bars"][A[2:]][0]["t"], "source": "Alpaca", "price_basis": "daily_bar_close"}]
    assert result["no_trade"] == [B]
    assert service.prices([A, B]) == result
    assert len(calls) == 1
    url, request = calls[0]
    assert url == "https://data.alpaca.markets/v1beta1/options/bars"
    assert request["params"]["symbols"] == f"{A[2:]},{B[2:]}"
    assert request["params"]["timeframe"] == "1Day"
    assert "feed" not in request["params"]
    assert request["headers"]["APCA-API-SECRET-KEY"] == "test-secret"
    assert "test-secret" not in str(result) + url + str(request["params"])
    assert datetime.fromisoformat(request["params"]["end"]) <= datetime.now(timezone.utc) - timedelta(minutes=15)


def test_all_pages_are_visited_before_reporting_no_trade(provider, monkeypatch):
    calls, payload = provider
    stamp = payload["bars"][A[2:]][0]["t"]
    def get(url, **kwargs):
        calls.append(kwargs)
        page = {"bars": {B[2:]: [{"c": 0, "t": stamp}]}} if kwargs["params"].get("page_token") else {**payload, "next_page_token": "second"}
        return SimpleNamespace(status_code=200, json=lambda: page)
    monkeypatch.setattr(service.requests, "get", get)
    result = service.prices([A, B])
    assert len(calls) == 2
    assert len(result["closes"]) == 2
    assert result["closes"][1]["price"] == 0
    assert result["no_trade"] == []


def test_latest_valid_completed_bar_wins_and_partial_day_is_excluded(provider):
    _, payload = provider
    valid = payload["bars"][A[2:]][0]
    payload["bars"][A[2:]] = [valid, {"c": 123, "t": datetime.now(timezone.utc).isoformat()},
        {"c": 55, "t": (datetime.now(timezone.utc) - timedelta(days=10)).isoformat()},
        {"c": float("nan"), "t": valid["t"]}, {"c": -1, "t": valid["t"]},
        {"c": True, "t": valid["t"]}, {"c": 6, "t": "2026-01-01"}, {"c": 6, "t": None}]
    assert service.prices([A])["closes"][0]["price"] == 4.25


def test_empty_bars_are_missing_not_zero(provider):
    provider[1]["bars"] = {}
    assert service.prices([A])["no_trade"] == [A]
    assert service.prices([A])["closes"] == []


@pytest.mark.parametrize("tickers", [[], [A] * 101, ["../../secret"], ["SPY"]])
def test_invalid_batch_is_rejected_without_provider_request(provider, tickers):
    with pytest.raises(service.OptionsDataError) as error:
        service.prices(tickers)
    assert error.value.status == 422
    assert not provider[0]


@pytest.mark.parametrize("status,expected", [(401, 503), (403, 503), (429, 429), (500, 503)])
def test_access_errors_are_sanitized_and_never_switch_to_indicative(provider, monkeypatch, status, expected):
    calls = []
    def get(url, **kwargs):
        calls.append(url)
        return SimpleNamespace(status_code=status)
    monkeypatch.setattr(service.requests, "get", get)
    with pytest.raises(service.OptionsDataError) as error:
        service.prices([A])
    assert error.value.status == expected
    assert "test-secret" not in str(error.value)
    assert calls == ["https://data.alpaca.markets/v1beta1/options/bars"]


def test_repeated_pagination_never_marks_unvisited_contracts_missing(provider):
    provider[1]["next_page_token"] = "repeated"
    with pytest.raises(service.OptionsDataError, match="pagination did not complete"):
        service.prices([A, B])


def test_failure_uses_budget_and_does_not_leak_exception(provider, monkeypatch):
    def fail(*args, **kwargs):
        raise requests.Timeout("test-secret")
    monkeypatch.setattr(service.requests, "get", fail)
    with pytest.raises(service.OptionsDataError, match="could not be loaded") as error:
        service.prices([A])
    assert "test-secret" not in str(error.value)
    with service.SessionLocal() as db:
        rows = db.query(TickerContentCache).all()
        assert len(rows) == 1
        assert "hits" in rows[0].payload_json


def test_requires_explicit_opt_in_and_both_keys(provider, monkeypatch):
    assert service.enabled()
    monkeypatch.delenv("OPTIONS_PRICE_PROVIDER")
    assert not service.enabled()
    monkeypatch.setenv("OPTIONS_PRICE_PROVIDER", "alpaca")
    monkeypatch.delenv("APCA_API_SECRET_KEY")
    assert not service.enabled()
