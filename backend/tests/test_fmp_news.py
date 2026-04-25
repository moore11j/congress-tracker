from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.main import list_insights_news, ticker_news, ticker_press_releases, ticker_sec_filings
from app.services.fmp_news import clear_news_cache


class _FakeResponse:
    def __init__(self, status_code: int, payload, text: str = ""):
        self.status_code = status_code
        self._payload = payload
        self.text = text

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            import requests

            raise requests.HTTPError(f"HTTP {self.status_code}")


def _session():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    Base.metadata.create_all(engine)
    return Session()


def test_insights_news_uses_general_latest_and_returns_has_next(monkeypatch):
    db = _session()
    clear_news_cache()

    def fake_get(url, params=None, timeout=30):
        assert url.endswith("/stable/news/general-latest")
        assert params["page"] == 0
        assert params["limit"] == 1
        return _FakeResponse(
            200,
            [
                {
                    "title": "Macro headline",
                    "site": "Reuters",
                    "publishedDate": "2026-04-25T15:30:00Z",
                    "url": "https://example.com/macro",
                    "image": "https://example.com/macro.jpg",
                    "text": "Markets moved on a major update.",
                },
            ],
        )

    monkeypatch.setenv("FMP_API_KEY", "test-key")
    monkeypatch.setattr("app.services.fmp_news.requests.get", fake_get)

    response = list_insights_news(page=0, limit=1)

    assert response["page"] == 0
    assert response["limit"] == 1
    assert response["has_next"] is True
    assert response["items"][0]["source"] == "fmp_general_news"
    assert response["items"][0]["image_url"] == "https://example.com/macro.jpg"


def test_ticker_news_filters_stock_latest_by_symbol_and_caches(monkeypatch):
    db = _session()
    clear_news_cache()
    calls = {"count": 0}

    def fake_get(url, params=None, timeout=30):
        calls["count"] += 1
        assert url.endswith("/stable/news/stock-latest")
        return _FakeResponse(
            200,
            [
                {
                    "symbol": "MSFT",
                    "title": "Microsoft item",
                    "site": "CNBC",
                    "publishedDate": "2026-04-25T15:00:00Z",
                    "url": "https://example.com/msft",
                    "text": "Ignore this item.",
                },
                {
                    "symbol": "AAPL",
                    "title": "Apple item",
                    "site": "Reuters",
                    "publishedDate": "2026-04-25T16:00:00Z",
                    "url": "https://example.com/aapl",
                    "image": "https://example.com/aapl.jpg",
                    "text": "Keep this item.",
                },
            ],
        )

    monkeypatch.setenv("FMP_API_KEY", "test-key")
    monkeypatch.setattr("app.services.fmp_news.requests.get", fake_get)

    first = ticker_news("AAPL", page=0, limit=20)
    second = ticker_news("AAPL", page=0, limit=20)

    assert calls["count"] == 1
    assert first["items"] == second["items"]
    assert first["items"][0] == {
        "symbol": "AAPL",
        "title": "Apple item",
        "site": "Reuters",
        "published_at": "2026-04-25T16:00:00+00:00",
        "url": "https://example.com/aapl",
        "image_url": "https://example.com/aapl.jpg",
        "summary": "Keep this item.",
        "source": "fmp_stock_news",
    }


def test_ticker_press_releases_filters_latest_feed(monkeypatch):
    db = _session()
    clear_news_cache()

    def fake_get(url, params=None, timeout=30):
        assert url.endswith("/stable/news/press-releases-latest")
        return _FakeResponse(
            200,
            [
                {
                    "symbols": "NVDA",
                    "title": "Nvidia release",
                    "publishedDate": "2026-04-25T12:00:00Z",
                    "url": "https://example.com/nvda-pr",
                    "text": "Ignore this item.",
                },
                {
                    "symbols": "AAPL,MSFT",
                    "title": "Apple release",
                    "site": "Business Wire",
                    "publishedDate": "2026-04-25T13:00:00Z",
                    "url": "https://example.com/apple-pr",
                    "text": "Apple announced an update.",
                },
            ],
        )

    monkeypatch.setenv("FMP_API_KEY", "test-key")
    monkeypatch.setattr("app.services.fmp_news.requests.get", fake_get)

    response = ticker_press_releases("AAPL", page=0, limit=20)

    assert response["items"][0] == {
        "symbol": "AAPL",
        "title": "Apple release",
        "site": "Business Wire",
        "published_at": "2026-04-25T13:00:00+00:00",
        "url": "https://example.com/apple-pr",
        "summary": "Apple announced an update.",
        "source": "fmp_press_release",
    }


def test_ticker_sec_filings_uses_symbol_endpoint_and_defaults_date_range(monkeypatch):
    db = _session()
    clear_news_cache()

    captured = {"params": None}

    def fake_get(url, params=None, timeout=30):
        captured["params"] = params
        assert url.endswith("/stable/sec-filings-search/symbol")
        return _FakeResponse(
            200,
            [
                {
                    "symbol": "AAPL",
                    "filingDate": "2026-04-24",
                    "acceptedDate": "2026-04-24 16:31:00",
                    "formType": "8-K",
                    "title": "Current report",
                    "finalLink": "https://example.com/8k",
                }
            ],
        )

    monkeypatch.setenv("FMP_API_KEY", "test-key")
    monkeypatch.setattr("app.services.fmp_news.requests.get", fake_get)

    response = ticker_sec_filings("AAPL", from_date=None, to_date=None, page=0, limit=100)

    assert captured["params"]["symbol"] == "AAPL"
    assert captured["params"]["page"] == 0
    assert captured["params"]["limit"] == 101
    assert response["items"][0] == {
        "symbol": "AAPL",
        "filing_date": "2026-04-24",
        "accepted_date": "2026-04-24 16:31:00",
        "form_type": "8-K",
        "title": "Current report",
        "url": "https://example.com/8k",
        "source": "fmp_sec_filings",
    }


def test_provider_unavailable_degrades_gracefully(monkeypatch):
    db = _session()
    clear_news_cache()

    def fake_get(url, params=None, timeout=30):
        return _FakeResponse(403, [], text="Forbidden")

    monkeypatch.setenv("FMP_API_KEY", "test-key")
    monkeypatch.setattr("app.services.fmp_news.requests.get", fake_get)

    response = ticker_news("AAPL", page=0, limit=20)

    assert response == {
        "items": [],
        "status": "unavailable",
        "message": "News data is unavailable from the current provider.",
        "page": 0,
        "limit": 20,
        "has_next": False,
    }
