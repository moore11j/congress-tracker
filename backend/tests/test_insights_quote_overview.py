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
    if symbol.endswith("USD") and symbol in {"BTCUSD", "ETHUSD", "SOLUSD", "XRPUSD", "BNBUSD"}:
        return {
            "symbol": symbol,
            "price": 100.25,
            "change": 1.5,
            "changesPercentage": 1.52,
            "volume": 1000000,
            "timestamp": 1782585600,
        }
    return [{"symbol": symbol, "price": 50.125, "volume": 1200}]


def test_quote_short_response_normalizes_to_public_contract():
    config = QUOTE_GROUPS["global_markets"][0]
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
    monkeypatch.setenv("FMP_API_KEY", "secret-key")

    def fake_get(url, params=None, timeout=10):
        symbol = params["symbol"]
        assert "secret-key" not in url
        if symbol == "EWG":
            raise requests.Timeout("provider timeout")
        return _FakeResponse(_fake_quote_payload(symbol))

    monkeypatch.setattr("app.services.insights_quote_overview.requests.get", fake_get)
    try:
        payload = get_insights_quote_overview(db)
    finally:
        db.close()

    global_rows = {item["symbol"]: item for item in payload["global_markets"]}
    assert global_rows["MCHI"]["status"] == "ok"
    assert global_rows["EWG"]["status"] == "unavailable"
    assert global_rows["IJP.AX"]["status"] == "ok"
    assert "secret-key" not in json.dumps(payload)


def test_failed_fetch_uses_stale_cached_quote(monkeypatch):
    db = _db()
    monkeypatch.setenv("FMP_API_KEY", "secret-key")
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
            kind="insights-quote:global_markets:MCHI:historical-chart/1min",
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

    assert payload["global_markets"][0] == cached


def test_insights_overview_endpoint_returns_all_requested_symbols(monkeypatch):
    db = _db()
    monkeypatch.setenv("FMP_API_KEY", "secret-key")
    calls: list[str] = []

    def fake_get(url, params=None, timeout=10):
        assert url.startswith("https://financialmodelingprep.com/stable/")
        symbol = params["symbol"]
        calls.append(symbol)
        return _FakeResponse(_fake_quote_payload(symbol))

    monkeypatch.setattr("app.services.insights_quote_overview.requests.get", fake_get)
    try:
        payload = insights_overview(db)
    finally:
        db.close()

    expected = [
        "MCHI",
        "EWG",
        "IJP.AX",
        "ISF.L",
        "VFV.TO",
        "GCUSD",
        "SILUSD",
        "BZUSD",
        "HGUSD",
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
    assert calls == expected
    assert payload["updated_at"] is not None
    assert "secret-key" not in json.dumps(payload)
