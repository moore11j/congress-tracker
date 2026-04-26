from __future__ import annotations

from datetime import date, timedelta

import requests
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.main import insights_macro_snapshot, list_insights_news, ticker_news, ticker_press_releases, ticker_sec_filings
from app.services.fmp_market_snapshot import clear_macro_snapshot_cache
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
            raise requests.HTTPError(f"HTTP {self.status_code}")


def _session():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    Base.metadata.create_all(engine)
    return Session()


def test_insights_news_uses_general_latest_and_returns_has_next(monkeypatch):
    _session()
    clear_news_cache()

    def fake_get(url, params=None, timeout=30):
        assert url.endswith("/stable/news/general-latest")
        assert params["page"] == 0
        assert params["limit"] == 2
        assert timeout == 8
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
                {
                    "title": "Second headline",
                    "site": "AP",
                    "publishedDate": "2026-04-25T14:30:00Z",
                    "url": "https://example.com/second",
                    "text": "Another market update.",
                },
            ],
        )

    monkeypatch.setenv("FMP_API_KEY", "test-key")
    monkeypatch.setattr("app.services.fmp_news.requests.get", fake_get)

    response = list_insights_news(page=0, limit=1)

    assert response["page"] == 0
    assert response["limit"] == 1
    assert response["status"] == "ok"
    assert response["has_next"] is True
    assert response["items"][0]["source"] == "fmp_general_news"
    assert response["items"][0]["image_url"] == "https://example.com/macro.jpg"
    assert response["items"][0]["market_read"] == "neutral"


def test_ticker_news_uses_symbol_specific_query_and_caches(monkeypatch):
    _session()
    clear_news_cache()
    calls = {"count": 0}

    def fake_get(url, params=None, timeout=30):
        calls["count"] += 1
        assert url.endswith("/stable/news/stock")
        assert params["symbols"] == "AAPL"
        assert params["page"] == 0
        assert params["limit"] == 20
        assert timeout == 8
        return _FakeResponse(
            200,
            [
                {
                    "title": "Apple raises dividend after strong growth",
                    "site": "Reuters",
                    "publishedDate": "2026-04-25T16:00:00Z",
                    "url": "https://example.com/aapl",
                    "image": "https://example.com/aapl.jpg",
                    "text": "Apple raised its dividend after record growth.",
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
        "title": "Apple raises dividend after strong growth",
        "site": "Reuters",
        "published_at": "2026-04-25T16:00:00+00:00",
        "url": "https://example.com/aapl",
        "image_url": "https://example.com/aapl.jpg",
        "summary": "Apple raised its dividend after record growth.",
        "market_read": "bullish",
        "source": "fmp_stock_news",
    }


def test_ticker_news_logs_debug_status_count_and_preview(monkeypatch, caplog):
    _session()
    clear_news_cache()
    caplog.set_level("INFO")

    def fake_get(url, params=None, timeout=30):
        return _FakeResponse(
            200,
            [
                {
                    "symbol": "AAPL",
                    "title": "AAPL page 0",
                    "site": "CNBC",
                    "publishedDate": "2026-04-25T16:00:00Z",
                    "url": "https://example.com/aapl-0",
                    "text": "AAPL update.",
                }
            ],
            text='[{"symbol":"AAPL","title":"AAPL page 0"}]',
        )

    monkeypatch.setenv("FMP_API_KEY", "test-key")
    monkeypatch.setattr("app.services.fmp_news.requests.get", fake_get)

    response = ticker_news("AAPL", page=0, limit=20)

    assert response["status"] == "ok"
    assert len(response["items"]) == 1
    assert response["items"][0]["symbol"] == "AAPL"
    assert "ticker_news_debug app_endpoint=/api/tickers/{symbol}/news symbol=AAPL fmp_path=/stable/news/stock status=200 count=1" in caplog.text


def test_ticker_press_releases_uses_search_endpoint_and_market_read(monkeypatch):
    _session()
    clear_news_cache()

    def fake_get(url, params=None, timeout=30):
        assert url.endswith("/stable/news/press-releases")
        assert params["symbols"] == "AAPL"
        assert timeout == 8
        return _FakeResponse(
            200,
            [
                {
                    "title": "Apple faces lawsuit over device recall",
                    "site": "Business Wire",
                    "publishedDate": "2026-04-25T13:00:00Z",
                    "url": "https://example.com/apple-pr",
                    "text": "A lawsuit was filed after the recall warning.",
                },
            ],
        )

    monkeypatch.setenv("FMP_API_KEY", "test-key")
    monkeypatch.setattr("app.services.fmp_news.requests.get", fake_get)

    response = ticker_press_releases("AAPL", page=0, limit=20)

    assert response["items"][0] == {
        "symbol": "AAPL",
        "title": "Apple faces lawsuit over device recall",
        "site": "Business Wire",
        "published_at": "2026-04-25T13:00:00+00:00",
        "url": "https://example.com/apple-pr",
        "summary": "A lawsuit was filed after the recall warning.",
        "market_read": "bearish",
        "source": "fmp_press_release",
    }


def test_empty_provider_response_returns_empty_state(monkeypatch):
    _session()
    clear_news_cache()

    def fake_get(url, params=None, timeout=30):
        return _FakeResponse(200, [])

    monkeypatch.setenv("FMP_API_KEY", "test-key")
    monkeypatch.setattr("app.services.fmp_news.requests.get", fake_get)

    response = ticker_press_releases("AAPL", page=0, limit=20)

    assert response["status"] == "empty"
    assert response["items"] == []
    assert response["message"] == "No recent press releases or SEC filings found in the selected window."


def test_market_read_heuristic_returns_neutral_when_both_sides_match(monkeypatch):
    _session()
    clear_news_cache()

    def fake_get(url, params=None, timeout=30):
        return _FakeResponse(
            200,
            [
                {
                    "title": "Apple raises guidance but faces lawsuit",
                    "site": "Reuters",
                    "publishedDate": "2026-04-25T16:00:00Z",
                    "url": "https://example.com/mixed",
                    "text": "The company raised guidance but disclosed a probe.",
                },
            ],
        )

    monkeypatch.setenv("FMP_API_KEY", "test-key")
    monkeypatch.setattr("app.services.fmp_news.requests.get", fake_get)

    response = ticker_news("AAPL", page=0, limit=20)

    assert response["items"][0]["market_read"] == "neutral"


def test_ticker_sec_filings_uses_symbol_endpoint_and_defaults_date_range(monkeypatch):
    _session()
    clear_news_cache()

    captured = {"params": None}

    def fake_get(url, params=None, timeout=30):
        captured["params"] = params
        assert url.endswith("/stable/sec-filings-search/symbol")
        assert timeout == 8
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
    expected_from = (date.today() - timedelta(days=30)).isoformat()
    expected_to = date.today().isoformat()
    assert captured["params"]["from"] == expected_from
    assert captured["params"]["to"] == expected_to
    assert response["status"] == "ok"
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
    _session()
    clear_news_cache()

    def fake_get(url, params=None, timeout=30):
        return _FakeResponse(403, [], text="Forbidden")

    monkeypatch.setenv("FMP_API_KEY", "test-key")
    monkeypatch.setattr("app.services.fmp_news.requests.get", fake_get)

    response = ticker_news("AAPL", page=0, limit=20)

    assert response == {
        "items": [],
        "status": "unavailable",
        "message": "Ticker news is unavailable under the current data plan.",
        "page": 0,
        "limit": 20,
        "has_next": False,
    }


def test_ticker_news_empty_response_stays_empty_not_unavailable(monkeypatch):
    _session()
    clear_news_cache()

    def fake_get(url, params=None, timeout=30):
        assert timeout == 8
        return _FakeResponse(200, [])

    monkeypatch.setenv("FMP_API_KEY", "test-key")
    monkeypatch.setattr("app.services.fmp_news.requests.get", fake_get)

    response = ticker_news("AAPL", page=0, limit=20)

    assert response["status"] == "empty"
    assert response["items"] == []
    assert response["message"] == "No recent news found for this ticker."


def test_ticker_news_rate_limit_returns_specific_unavailable_message(monkeypatch):
    _session()
    clear_news_cache()

    def fake_get(url, params=None, timeout=30):
        return _FakeResponse(429, [], text="Rate limit exceeded")

    monkeypatch.setenv("FMP_API_KEY", "test-key")
    monkeypatch.setattr("app.services.fmp_news.requests.get", fake_get)

    response = ticker_news("AAPL", page=0, limit=20)

    assert response["status"] == "unavailable"
    assert response["message"] == "Ticker news is temporarily rate-limited."


def test_ticker_news_timeout_returns_temporary_unavailable(monkeypatch):
    _session()
    clear_news_cache()

    def fake_get(url, params=None, timeout=30):
        raise requests.Timeout("timeout")

    monkeypatch.setenv("FMP_API_KEY", "test-key")
    monkeypatch.setattr("app.services.fmp_news.requests.get", fake_get)

    response = ticker_news("AAPL", page=0, limit=20)

    assert response["status"] == "unavailable"
    assert response["message"] == "Ticker news is temporarily unavailable."


def test_press_releases_empty_response_stays_empty_not_unavailable(monkeypatch):
    _session()
    clear_news_cache()

    def fake_get(url, params=None, timeout=30):
        assert timeout == 8
        return _FakeResponse(200, [])

    monkeypatch.setenv("FMP_API_KEY", "test-key")
    monkeypatch.setattr("app.services.fmp_news.requests.get", fake_get)

    response = ticker_press_releases("AAPL", page=0, limit=20)

    assert response["status"] == "empty"
    assert response["items"] == []
    assert response["message"] == "No recent press releases or SEC filings found in the selected window."


def test_macro_snapshot_tolerates_partial_failures(monkeypatch):
    _session()
    clear_macro_snapshot_cache()
    sector_attempts = {"count": 0}

    def fake_get(url, params=None, timeout=30):
        assert timeout == 8
        if url.endswith("/stable/quote"):
            return _FakeResponse(
                200,
                [
                    {"symbol": "^GSPC", "price": 5100.12, "changesPercentage": 0.42},
                    {"symbol": "^DJI", "price": 38990.0, "changesPercentage": -0.15},
                    {"symbol": "^IXIC", "price": 16010.45, "changesPercentage": 0.88},
                ],
            )
        if url.endswith("/stable/treasury-rates"):
            raise requests.Timeout("timeout")
        if url.endswith("/stable/economic-indicators"):
            name = params["name"]
            return _FakeResponse(200, [{"date": "2026-03-01", "value": {"GDP": 2.8, "unemployment rate": 4.1, "CPI": 3.0}[name]}])
        if url.endswith("/stable/sector-performance-snapshot"):
            sector_attempts["count"] += 1
            if sector_attempts["count"] == 1:
                return _FakeResponse(200, [])
            return _FakeResponse(200, [{"sector": "Technology", "changesPercentage": 1.25}])
        raise AssertionError(f"Unexpected URL {url}")

    monkeypatch.setenv("FMP_API_KEY", "test-key")
    monkeypatch.setattr("app.services.fmp_market_snapshot.requests.get", fake_get)

    response = insights_macro_snapshot()

    assert response["status"] == "partial"
    assert len(response["indexes"]) == 3
    assert response["treasury"] == []
    assert response["economics"] == [
        {"label": "GDP", "value": 2.8, "date": "2026-03-01"},
        {"label": "Unemployment", "value": 4.1, "date": "2026-03-01"},
        {"label": "CPI", "value": 3.0, "date": "2026-03-01"},
    ]
    assert response["sector_performance"] == [{"sector": "Technology", "change_pct": 1.25}]
