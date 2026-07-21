from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import requests
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.main import insights_overview
from app.models import InsightsSnapshot
from app.services.insights_quote_overview import (
    QUOTE_GROUPS,
    get_insights_quote_overview,
    normalize_insights_quote_response,
)


class _FakeResponse:
    def __init__(self, payload, status_code: int = 200):
        self._payload = payload
        self.status_code = status_code
        self.text = ""

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")


def _db():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine, future=True)
    return Session()


def _fake_quote_payload(symbol: str):
    return {
        "chart": {
            "result": [
                {
                    "meta": {"regularMarketPrice": 50.125, "chartPreviousClose": 49.5},
                    "timestamp": [1782585600],
                    "indicators": {"quote": [{"close": [49.5, 50.125], "volume": [1200]}]},
                }
            ]
        }
    }


def _fake_frankfurter_payload():
    return {
        "base": "USD",
        "rates": {
            "2026-06-26": {"CAD": 1.36, "JPY": 156.1, "SEK": 10.1, "CHF": 0.9, "EUR": 0.92, "GBP": 0.78},
            "2026-06-27": {"CAD": 1.37, "JPY": 156.5, "SEK": 10.2, "CHF": 0.91, "EUR": 0.91, "GBP": 0.77},
        },
    }


def _fake_frankfurter_v2_payload():
    return [
        {"date": "2026-06-26", "base": "USD", "quote": "CAD", "rate": 1.36},
        {"date": "2026-06-26", "base": "USD", "quote": "CHF", "rate": 0.9},
        {"date": "2026-06-26", "base": "USD", "quote": "EUR", "rate": 0.92},
        {"date": "2026-06-26", "base": "USD", "quote": "GBP", "rate": 0.78},
        {"date": "2026-06-26", "base": "USD", "quote": "JPY", "rate": 156.1},
        {"date": "2026-06-26", "base": "USD", "quote": "SEK", "rate": 10.1},
        {"date": "2026-06-27", "base": "USD", "quote": "CAD", "rate": 1.37},
        {"date": "2026-06-27", "base": "USD", "quote": "CHF", "rate": 0.91},
        {"date": "2026-06-27", "base": "USD", "quote": "EUR", "rate": 0.91},
        {"date": "2026-06-27", "base": "USD", "quote": "GBP", "rate": 0.77},
        {"date": "2026-06-27", "base": "USD", "quote": "JPY", "rate": 156.5},
        {"date": "2026-06-27", "base": "USD", "quote": "SEK", "rate": 10.2},
    ]


def _fake_coingecko_payload(coin_id: str):
    return {
        coin_id: {
            "usd": 100.25,
            "usd_24h_change": 1.52,
            "usd_24h_vol": 1000000,
        }
    }


def _fake_silv_payload():
    return {
        "commodities": {
            "gold": {"price": 4066.27, "last_updated": "2026-07-21T05:55:58.000Z", "change_24h": {"amount": 54.81, "percent": 1.36}},
            "silver": {"price": 58.08, "last_updated": "2026-07-21T05:55:59.000Z", "change_24h": {"amount": 1.48, "percent": 2.62}},
            "copper": {"price": 6.42, "last_updated": "2026-07-21T05:55:43.000Z", "change_24h": {"amount": 0.16, "percent": 2.49}},
        }
    }


def test_quote_short_response_normalizes_to_public_contract():
    config = QUOTE_GROUPS["global_markets"][1]
    item = normalize_insights_quote_response(
        "global_markets",
        config,
        [{"symbol": "MCHI", "price": "62.44", "volume": 12345}],
        fetched_at=datetime(2026, 6, 27, 12, 0, tzinfo=timezone.utc),
    )

    assert item == {
        "group": "global_markets",
        "label": "China",
        "symbol": "MCHI",
        "display_symbol": "MCHI",
        "price": 62.44,
        "change": None,
        "change_percent": None,
        "volume": 12345.0,
        "as_of": "2026-06-27T12:00:00+00:00",
        "status": "ok",
    }


def test_quote_response_normalizes_change_fields():
    config = QUOTE_GROUPS["crypto"][0]
    item = normalize_insights_quote_response(
        "crypto",
        config,
        {
            "symbol": "BTCUSD",
            "price": 61234.56,
            "change": -125.5,
            "changesPercentage": "-0.20%",
            "volume": 456789,
            "timestamp": 1782585600,
        },
    )

    assert item["group"] == "crypto"
    assert item["label"] == "BTC/USD"
    assert item["symbol"] == "BTCUSD"
    assert item["display_symbol"] == "BTCUSD"
    assert item["price"] == 61234.56
    assert item["change"] == -125.5
    assert item["change_percent"] == -0.2
    assert item["volume"] == 456789.0
    assert item["as_of"].startswith("2026-06-27T")
    assert item["status"] == "ok"


def test_failed_fetch_marks_only_that_symbol_unavailable(monkeypatch):
    db = _db()

    def fake_get(url, params=None, timeout=10, **_kwargs):
        if "query1.finance.yahoo.com" in url:
            symbol = url.rsplit("/", 1)[-1]
            if symbol == "EWG":
                raise requests.Timeout("provider timeout")
            return _FakeResponse(_fake_quote_payload(symbol))
        if "frankfurter.dev" in url:
            return _FakeResponse(_fake_frankfurter_payload())
        if "data.silv.app" in url:
            return _FakeResponse(_fake_silv_payload())
        if "api.coingecko.com" in url:
            return _FakeResponse(_fake_coingecko_payload(params["ids"]))
        raise AssertionError(f"unexpected url {url}")

    monkeypatch.setattr("app.services.insights_quote_overview.requests.get", fake_get)
    try:
        payload = get_insights_quote_overview(db)
    finally:
        db.close()

    global_rows = {item["symbol"]: item for item in payload["global_markets"]}
    assert global_rows["MCHI"]["status"] == "ok"
    assert global_rows["EWG"]["status"] == "unavailable"
    assert global_rows["IJP"]["status"] == "ok"
    assert "secret-key" not in json.dumps(payload)


def test_free_currency_provider_returns_dxy_and_pairs(monkeypatch):
    db = _db()

    def fake_get(url, params=None, timeout=10, **_kwargs):
        if "frankfurter.dev" not in url:
            raise requests.Timeout("provider timeout")
        return _FakeResponse(_fake_frankfurter_payload())

    monkeypatch.setattr("app.services.insights_quote_overview.requests.get", fake_get)
    try:
        payload = get_insights_quote_overview(db)
    finally:
        db.close()

    currency_rows = {item["symbol"]: item for item in payload["currencies"]}
    assert currency_rows["DXY"]["status"] == "ok"
    assert currency_rows["USDCAD"]["price"] == 1.37
    assert currency_rows["EURUSD"]["status"] == "ok"


def test_free_currency_provider_parses_v2_rows(monkeypatch):
    db = _db()

    def fake_get(url, params=None, timeout=10, **_kwargs):
        if "frankfurter.dev" not in url:
            raise requests.Timeout("provider timeout")
        assert params["from"]
        assert params["quotes"] == "CAD,JPY,SEK,CHF,EUR,GBP"
        return _FakeResponse(_fake_frankfurter_v2_payload())

    monkeypatch.setattr("app.services.insights_quote_overview.requests.get", fake_get)
    try:
        payload = get_insights_quote_overview(db)
    finally:
        db.close()

    currency_rows = {item["symbol"]: item for item in payload["currencies"]}
    assert currency_rows["DXY"]["status"] == "ok"
    assert currency_rows["USDCAD"]["price"] == 1.37
    assert round(currency_rows["USDCAD"]["change"], 2) == 0.01


def test_free_commodity_provider_returns_metals_and_copper(monkeypatch):
    db = _db()

    def fake_get(url, params=None, timeout=10, **_kwargs):
        if "data.silv.app" not in url:
            raise requests.Timeout("provider timeout")
        return _FakeResponse(_fake_silv_payload())

    monkeypatch.setattr("app.services.insights_quote_overview.requests.get", fake_get)
    try:
        payload = get_insights_quote_overview(db)
    finally:
        db.close()

    commodity_rows = {item["symbol"]: item for item in payload["commodities"]}
    assert commodity_rows["GCUSD"]["price"] == 4066.27
    assert commodity_rows["SILUSD"]["change_percent"] == 2.62
    assert commodity_rows["HGUSD"]["status"] == "ok"


def test_fresh_unavailable_cache_does_not_block_free_provider(monkeypatch):
    db = _db()
    db.add(
        InsightsSnapshot(
            kind="insights-quote:crypto:BTCUSD:coingecko",
            payload_json=json.dumps({
                "group": "crypto",
                "label": "BTC/USD",
                "symbol": "BTCUSD",
                "display_symbol": "BTCUSD",
                "price": None,
                "change": None,
                "change_percent": None,
                "volume": None,
                "as_of": None,
                "status": "unavailable",
            }),
            source="test",
            fetched_at=datetime.now(timezone.utc),
        )
    )
    db.commit()

    def fake_get(url, params=None, timeout=10, **_kwargs):
        if "api.coingecko.com" in url:
            return _FakeResponse(_fake_coingecko_payload(params["ids"]))
        raise requests.Timeout("only crypto fetch should be needed")

    monkeypatch.setattr("app.services.insights_quote_overview.requests.get", fake_get)
    try:
        payload = get_insights_quote_overview(db)
    finally:
        db.close()

    crypto_rows = {item["symbol"]: item for item in payload["crypto"]}
    assert crypto_rows["BTCUSD"]["status"] == "ok"
    assert crypto_rows["BTCUSD"]["price"] == 100.25


def test_failed_fetch_uses_stale_cached_quote(monkeypatch):
    db = _db()
    cached = {
        "group": "global_markets",
        "label": "China",
        "symbol": "MCHI",
        "display_symbol": "MCHI",
        "price": 61.0,
        "change": None,
        "change_percent": None,
        "volume": None,
        "as_of": "2026-06-27T11:00:00+00:00",
        "status": "ok",
    }
    db.add(
        InsightsSnapshot(
            kind="insights-quote:global_markets:MCHI:yahoo_chart",
            payload_json=json.dumps(cached),
            source="test",
            fetched_at=datetime.now(timezone.utc) - timedelta(minutes=30),
        )
    )
    db.commit()

    def fail_get(*_args, **_kwargs):
        raise requests.Timeout("provider timeout")

    monkeypatch.setattr("app.services.insights_quote_overview.requests.get", fail_get)
    try:
        payload = get_insights_quote_overview(db)
    finally:
        db.close()

    global_rows = {item["symbol"]: item for item in payload["global_markets"]}
    assert global_rows["MCHI"] == cached


def test_insights_overview_endpoint_returns_all_requested_symbols(monkeypatch):
    db = _db()
    calls: list[str] = []

    def fake_get(url, params=None, timeout=10, **_kwargs):
        if "query1.finance.yahoo.com" in url:
            symbol = url.rsplit("/", 1)[-1]
            calls.append(symbol)
            return _FakeResponse(_fake_quote_payload(symbol))
        if "frankfurter.dev" in url:
            calls.append("frankfurter")
            return _FakeResponse(_fake_frankfurter_payload())
        if "data.silv.app" in url:
            calls.append("silv")
            return _FakeResponse(_fake_silv_payload())
        if "api.coingecko.com" in url:
            calls.append(params["ids"])
            return _FakeResponse(_fake_coingecko_payload(params["ids"]))
        raise AssertionError(f"unexpected url {url}")

    monkeypatch.setattr("app.services.insights_quote_overview.requests.get", fake_get)
    try:
        payload = insights_overview(db)
    finally:
        db.close()

    expected = [
        "ACWI",
        "MCHI",
        "EWG",
        "IJP",
        "ISF",
        "VFV",
        "GCUSD",
        "SILUSD",
        "HGUSD",
        "DXY",
        "USDCAD",
        "EURUSD",
        "GBPUSD",
        "USDJPY",
        "EURCAD",
        "BTCUSD",
        "ETHUSD",
        "SOLUSD",
        "XRPUSD",
        "BNBUSD",
    ]
    returned = [
        item["symbol"]
        for group in ("global_markets", "commodities", "currencies", "crypto")
        for item in payload[group]
    ]

    assert returned == expected
    assert calls == [
        "ACWI",
        "MCHI",
        "EWG",
        "IJP.AX",
        "ISF.L",
        "VFV.TO",
        "silv",
        "silv",
        "silv",
        "frankfurter",
        "frankfurter",
        "frankfurter",
        "frankfurter",
        "frankfurter",
        "frankfurter",
        "bitcoin",
        "ethereum",
        "solana",
        "ripple",
        "binancecoin",
    ]
    assert payload["updated_at"] is not None
    assert "secret-key" not in json.dumps(payload)
