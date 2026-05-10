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
                    "symbol": "AAPL",
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
        "published_at": "2026-04-25T16:00:00Z",
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


def test_ticker_press_releases_uses_exact_symbols_endpoint_and_market_read(monkeypatch):
    _session()
    clear_news_cache()

    def fake_get(url, params=None, timeout=30):
        assert url.endswith("/stable/news/press-releases")
        assert params["symbols"] == "AAPL"
        assert "symbol" not in params
        assert "ticker" not in params
        assert "tickers" not in params
        assert params["page"] == 0
        assert params["limit"] == 20
        assert timeout == 8
        return _FakeResponse(
            200,
            [
                {
                    "symbol": "AAPL",
                    "title": "Apple faces lawsuit over device recall",
                    "publisher": "Business Wire",
                    "publishedDate": "2026-04-25 13:00:00",
                    "url": "https://example.com/apple-pr",
                    "image": "https://example.com/apple-pr.jpg",
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
        "published_at": "2026-04-25 13:00:00",
        "url": "https://example.com/apple-pr",
        "image_url": "https://example.com/apple-pr.jpg",
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
                    "symbol": "AAPL",
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


def test_ticker_press_logs_debug_status_count_and_preview(monkeypatch, caplog):
    _session()
    clear_news_cache()
    caplog.set_level("INFO")

    def fake_get(url, params=None, timeout=30):
        return _FakeResponse(
            200,
            [
                {
                    "symbol": "AAPL",
                    "title": "Apple press release",
                    "site": "Apple",
                    "publishedDate": "2026-04-25 13:00:00",
                    "url": "https://example.com/apple-press",
                    "text": "Press release body.",
                }
            ],
            text='[{"symbol":"AAPL","title":"Apple press release"}]',
        )

    monkeypatch.setenv("FMP_API_KEY", "test-key")
    monkeypatch.setattr("app.services.fmp_news.requests.get", fake_get)

    response = ticker_press_releases("AAPL", page=0, limit=20)

    assert response["status"] == "ok"
    assert len(response["items"]) == 1
    assert "ticker_press_debug app_endpoint=/api/tickers/{symbol}/press-releases symbol=AAPL fmp_path=/stable/news/press-releases status=200 count=1" in caplog.text


def test_ticker_press_rate_limit_returns_specific_unavailable_message(monkeypatch):
    _session()
    clear_news_cache()

    def fake_get(url, params=None, timeout=30):
        return _FakeResponse(429, [], text="Rate limit exceeded")

    monkeypatch.setenv("FMP_API_KEY", "test-key")
    monkeypatch.setattr("app.services.fmp_news.requests.get", fake_get)

    response = ticker_press_releases("AAPL", page=0, limit=20)

    assert response["status"] == "unavailable"
    assert response["message"] == "Ticker press releases are temporarily rate-limited."


def test_ticker_press_plan_limit_returns_specific_unavailable_message(monkeypatch):
    _session()
    clear_news_cache()

    def fake_get(url, params=None, timeout=30):
        return _FakeResponse(403, [], text="Forbidden")

    monkeypatch.setenv("FMP_API_KEY", "test-key")
    monkeypatch.setattr("app.services.fmp_news.requests.get", fake_get)

    response = ticker_press_releases("AAPL", page=0, limit=20)

    assert response["status"] == "unavailable"
    assert response["message"] == "Ticker press releases are unavailable under the current data plan."


def test_macro_snapshot_tolerates_partial_failures(monkeypatch):
    _session()
    clear_macro_snapshot_cache()
    sector_attempts = {"count": 0}

    def fake_get(url, params=None, timeout=30):
        assert timeout == 8
        if url.endswith("/stable/batch-index-quotes"):
            return _FakeResponse(
                200,
                [
                    {"symbol": "^GSPC", "price": 5100.12, "changesPercentage": 0.42},
                    {"symbol": "^IXIC", "price": 16010.45, "changePercentage": 0.88},
                    {"symbol": "^DJI", "price": 38990.0, "changesPercentage": -0.15},
                    {"symbol": "^RUT", "price": 2020.0, "change": 0.0, "previousClose": 2020.0},
                ],
            )
        if url.endswith("/stable/treasury-rates"):
            raise requests.Timeout("timeout")
        if url.endswith("/stable/economic-indicators"):
            name = params["name"]
            values = {"GDP": 2.8, "unemployment rate": 4.1, "CPI": 3.0, "federal funds rate": 4.33, "retail sales": 0.6}
            return _FakeResponse(200, [{"date": "2026-03-01", "value": values.get(name)}] if name in values else [])
        if url.endswith("/stable/sector-performance-snapshot"):
            sector_attempts["count"] += 1
            if sector_attempts["count"] == 1:
                return _FakeResponse(200, [])
            return _FakeResponse(200, [{"sector": "Technology", "averageChange": 1.25}])
        raise AssertionError(f"Unexpected URL {url}")

    monkeypatch.setenv("FMP_API_KEY", "test-key")
    monkeypatch.setattr("app.services.fmp_market_snapshot.requests.get", fake_get)

    response = insights_macro_snapshot()

    assert response["status"] == "partial"
    assert response["indexes"] == [
        {"label": "S&P 500", "symbol": "^GSPC", "value": 5100.12, "change_pct": 0.42, "is_proxy": False, "source": "index"},
        {"label": "Nasdaq", "symbol": "^IXIC", "value": 16010.45, "change_pct": 0.88, "is_proxy": False, "source": "index"},
        {"label": "Dow", "symbol": "^DJI", "value": 38990.0, "change_pct": -0.15, "is_proxy": False, "source": "index"},
        {"label": "Russell 2000", "symbol": "^RUT", "value": 2020.0, "change_pct": 0.0, "is_proxy": False, "source": "index"},
    ]
    assert response["treasury"] == []
    assert response["economics"] == [
        {
            "label": "Fed Overnight Rate",
            "value": 4.33,
            "date": "2026-03-01",
            "context_label": "Latest available",
            "unit_label": "%",
        },
        {"label": "CPI", "value": 3.0, "date": "2026-03-01", "context_label": "Latest release", "unit_label": "%"},
        {"label": "Unemployment", "value": 4.1, "date": "2026-03-01", "context_label": "Latest release", "unit_label": "%"},
        {"label": "GDP", "value": 2.8, "date": "2026-03-01", "context_label": "Latest release", "unit_label": "%"},
        {"label": "Retail Sales", "value": 0.6, "date": "2026-03-01", "context_label": "Latest release", "unit_label": "%"},
    ]
    assert response["sector_performance"] == [{"sector": "Technology", "change_pct": 1.25}]


def test_macro_snapshot_falls_back_to_single_index_quotes(monkeypatch):
    _session()
    clear_macro_snapshot_cache()

    def fake_get(url, params=None, timeout=30):
        assert timeout == 8
        if url.endswith("/stable/batch-index-quotes"):
            return _FakeResponse(200, [])
        if url.endswith("/stable/quote"):
            symbol = params["symbol"]
            return _FakeResponse(200, [{"symbol": symbol, "price": 100.0, "change": 1.0, "previousClose": 99.0}])
        if url.endswith("/stable/treasury-rates"):
            return _FakeResponse(200, [])
        if url.endswith("/stable/economic-indicators"):
            return _FakeResponse(200, [])
        if url.endswith("/stable/sector-performance-snapshot"):
            return _FakeResponse(200, [])
        raise AssertionError(f"Unexpected URL {url}")

    monkeypatch.setenv("FMP_API_KEY", "test-key")
    monkeypatch.setattr("app.services.fmp_market_snapshot.requests.get", fake_get)

    response = insights_macro_snapshot()

    assert response["status"] == "partial"
    assert len(response["indexes"]) == 5
    assert response["indexes"][0]["change_pct"] == 1.0101010101010102
    assert response["indexes"][0]["source"] == "index"
    assert response["sector_performance"] == []


def test_macro_snapshot_adds_context_quotes_and_fed_rate(monkeypatch):
    _session()
    clear_macro_snapshot_cache()

    def fake_get(url, params=None, timeout=30):
        assert timeout == 8
        if url.endswith("/stable/batch-index-quotes"):
            return _FakeResponse(200, [])
        if url.endswith("/stable/batch-commodity-quotes"):
            return _FakeResponse(
                200,
                [
                    {"symbol": "GCUSD", "price": 2300.0, "changesPercentage": 0.5},
                    {"symbol": "SIUSD", "price": 29.0, "changesPercentage": -0.2},
                    {"symbol": "HGUSD", "price": 4.8, "changesPercentage": 0.3},
                    {"symbol": "BZUSD", "price": 82.0, "changesPercentage": 0.9},
                    {"symbol": "ZWUSD", "price": 615.0, "changesPercentage": -0.4},
                ],
            )
        if url.endswith("/stable/batch-quote"):
            symbols = set(str(params.get("symbols", "")).split(","))
            rows = []
            quote_map = {
                "USDCAD": {"symbol": "USDCAD", "price": 1.37, "changesPercentage": 0.05},
                "EURUSD": {"symbol": "EURUSD", "price": 1.08, "changesPercentage": -0.04},
                "GBPUSD": {"symbol": "GBPUSD", "price": 1.27, "changesPercentage": 0.02},
                "USDJPY": {"symbol": "USDJPY", "price": 155.2, "changesPercentage": 0.1},
                "EURCAD": {"symbol": "EURCAD", "price": 1.48, "changesPercentage": -0.01},
                "BTCUSD": {"symbol": "BTCUSD", "price": 64000.0, "changesPercentage": 2.0},
                "ETHUSD": {"symbol": "ETHUSD", "price": 3100.0, "changesPercentage": 1.4},
                "SOLUSD": {"symbol": "SOLUSD", "price": 145.0, "changesPercentage": -0.7},
                "XRPUSD": {"symbol": "XRPUSD", "price": 0.55, "changesPercentage": 0.6},
            }
            for symbol in symbols:
                if symbol in quote_map:
                    rows.append(quote_map[symbol])
            return _FakeResponse(200, rows)
        if url.endswith("/stable/quote") or url.endswith("/stable/quote-short"):
            return _FakeResponse(200, [])
        if url.endswith("/stable/treasury-rates"):
            return _FakeResponse(
                200,
                [
                    {"date": "2026-05-08", "year2": 4.0, "year5": 4.1, "year10": 4.3, "year30": 4.5, "month3": 4.2},
                    {"date": "2026-05-07", "year2": 3.96, "year5": 4.05, "year10": 4.27, "year30": 4.48, "month3": 4.18},
                ],
            )
        if url.endswith("/stable/economic-indicators"):
            values = {
                "GDP": 2.8,
                "unemployment rate": 4.1,
                "CPI": 3.0,
                "federalFunds": 4.33,
                "federal funds rate": 4.33,
                "retail sales": 0.6,
            }
            name = params["name"]
            return _FakeResponse(200, [{"date": "2026-03-01", "value": values.get(name)}] if name in values else [])
        if url.endswith("/stable/sector-performance-snapshot"):
            return _FakeResponse(200, [{"sector": "Technology", "averageChange": 1.25}])
        raise AssertionError(f"Unexpected URL {url}")

    monkeypatch.setenv("FMP_API_KEY", "test-key")
    monkeypatch.setattr("app.services.fmp_market_snapshot.requests.get", fake_get)

    response = insights_macro_snapshot()

    assert response["commodities"][0] == {
        "label": "Gold",
        "symbol": "GCUSD",
        "value": 2300.0,
        "change": None,
        "change_pct": 0.5,
        "timeframe_label": "1D change",
        "unit_label": "USD",
        "status": "ok",
    }
    assert response["currencies"][0]["label"] == "USD/CAD"
    assert response["currencies"][0]["status"] == "ok"
    assert response["currencies"][-1]["label"] == "EUR/CAD"
    assert response["commodities"][2]["label"] == "Copper"
    assert response["commodities"][-1]["label"] == "Wheat"
    assert response["crypto"][0]["label"] == "BTC/USD"
    assert response["crypto"][-1]["label"] == "BNB/USD"
    assert response["crypto"][-1]["status"] == "unavailable"
    assert [item["label"] for item in response["treasury"]] == [
        "3M Treasury",
        "2Y Treasury",
        "5Y Treasury",
        "10Y Treasury",
        "30Y Treasury",
    ]
    assert round(response["treasury"][3]["change"], 1) == 3.0
    assert response["treasury"][3]["change_unit"] == "bps"
    assert response["treasury"][3]["timeframe_label"] == "1D change"
    assert response["economics"][0]["label"] == "Fed Overnight Rate"
    assert response["economics"][0]["context_label"] == "Latest available"
    assert response["economics"][-1]["label"] == "Retail Sales"


def test_macro_snapshot_resolves_world_index_and_copper_aliases(monkeypatch):
    _session()
    clear_macro_snapshot_cache()

    def fake_get(url, params=None, timeout=30):
        assert timeout == 8
        if url.endswith("/stable/batch-index-quotes"):
            return _FakeResponse(200, [])
        if url.endswith("/stable/quote") or url.endswith("/stable/quote-short"):
            symbol = params["symbol"]
            rows = {
                ".GSPTSE": [{"symbol": ".GSPTSE", "price": 30500.0, "changesPercentage": 0.2}],
                "DAX40": [{"symbol": "DAX40", "price": 24000.0, "changesPercentage": -0.1}],
            }
            return _FakeResponse(200, rows.get(symbol, []))
        if url.endswith("/stable/batch-commodity-quotes"):
            return _FakeResponse(200, [{"symbol": "HGUSD.CMX", "price": 4.95, "changesPercentage": 0.7}])
        if url.endswith("/stable/batch-quote"):
            return _FakeResponse(200, [])
        if url.endswith("/stable/treasury-rates"):
            return _FakeResponse(200, [])
        if url.endswith("/stable/economic-indicators"):
            return _FakeResponse(200, [])
        if url.endswith("/stable/sector-performance-snapshot"):
            return _FakeResponse(200, [])
        raise AssertionError(f"Unexpected URL {url}")

    monkeypatch.setenv("FMP_API_KEY", "test-key")
    monkeypatch.setattr("app.services.fmp_market_snapshot.requests.get", fake_get)

    response = insights_macro_snapshot()

    canada_tsx = next(item for item in response["world_indexes"] if item["label"] == "Canada TSX")
    dax = next(item for item in response["world_indexes"] if item["label"] == "DAX")
    copper = response["commodities"][2]

    assert canada_tsx["symbol"] == ".GSPTSE"
    assert canada_tsx["value"] == 30500.0
    assert dax["symbol"] == "DAX40"
    assert dax["value"] == 24000.0
    assert copper["label"] == "Copper"
    assert copper["symbol"] == "HGUSD.CMX"
    assert copper["value"] == 4.95
    assert copper["status"] == "ok"


def test_macro_snapshot_uses_honest_canada_tsx_proxy_after_aliases_fail(monkeypatch):
    _session()
    clear_macro_snapshot_cache()

    def fake_get(url, params=None, timeout=30):
        assert timeout == 8
        if url.endswith("/stable/batch-index-quotes"):
            return _FakeResponse(200, [])
        if url.endswith("/stable/quote") or url.endswith("/stable/quote-short"):
            symbol = params["symbol"]
            if symbol == "XIC.TO":
                return _FakeResponse(200, [{"symbol": "XIC.TO", "price": 42.5, "changesPercentage": 0.18}])
            return _FakeResponse(200, [])
        if url.endswith("/stable/batch-commodity-quotes"):
            return _FakeResponse(200, [])
        if url.endswith("/stable/batch-quote"):
            return _FakeResponse(200, [])
        if url.endswith("/stable/treasury-rates"):
            return _FakeResponse(200, [])
        if url.endswith("/stable/economic-indicators"):
            return _FakeResponse(200, [])
        if url.endswith("/stable/sector-performance-snapshot"):
            return _FakeResponse(200, [])
        raise AssertionError(f"Unexpected URL {url}")

    monkeypatch.setenv("FMP_API_KEY", "test-key")
    monkeypatch.setattr("app.services.fmp_market_snapshot.requests.get", fake_get)

    response = insights_macro_snapshot()

    canada_tsx = next(item for item in response["world_indexes"] if item["label"] == "Canada TSX")
    assert canada_tsx["symbol"] == "XIC.TO proxy"
    assert canada_tsx["value"] == 42.5
    assert canada_tsx["change_pct"] == 0.18
    assert canada_tsx["is_proxy"] is True
    assert canada_tsx["source"] == "etf_proxy"


def test_macro_snapshot_uses_etf_proxies_when_index_endpoints_unavailable(monkeypatch):
    _session()
    clear_macro_snapshot_cache()

    def fake_get(url, params=None, timeout=30):
        assert timeout == 8
        if url.endswith("/stable/batch-index-quotes"):
            return _FakeResponse(403, [], text="Forbidden")
        if url.endswith("/stable/quote") and str(params.get("symbol", "")).startswith("^"):
            return _FakeResponse(403, [], text="Forbidden")
        if url.endswith("/stable/quote-short") and str(params.get("symbol", "")).startswith("^"):
            return _FakeResponse(403, [], text="Forbidden")
        if url.endswith("/stable/batch-quote"):
            return _FakeResponse(
                200,
                [
                    {"symbol": "SPY", "price": 510.0, "changesPercentage": 0.4},
                    {"symbol": "QQQ", "price": 430.0, "changesPercentage": 0.8},
                    {"symbol": "DIA", "price": 390.0, "changesPercentage": -0.1},
                    {"symbol": "IWM", "price": 202.0, "changesPercentage": 0.2},
                ],
            )
        if url.endswith("/stable/treasury-rates"):
            return _FakeResponse(200, [])
        if url.endswith("/stable/economic-indicators"):
            return _FakeResponse(200, [])
        if url.endswith("/stable/sector-performance-snapshot"):
            return _FakeResponse(200, [{"sector": "Technology", "averageChange": 1.25}])
        raise AssertionError(f"Unexpected URL {url}")

    monkeypatch.setenv("FMP_API_KEY", "test-key")
    monkeypatch.setattr("app.services.fmp_market_snapshot.requests.get", fake_get)

    response = insights_macro_snapshot()

    assert response["status"] == "partial"
    assert response["indexes"] == [
        {"label": "S&P 500 proxy", "symbol": "SPY", "value": 510.0, "change_pct": 0.4, "is_proxy": True, "source": "etf_proxy"},
        {"label": "Nasdaq proxy", "symbol": "QQQ", "value": 430.0, "change_pct": 0.8, "is_proxy": True, "source": "etf_proxy"},
        {"label": "Dow proxy", "symbol": "DIA", "value": 390.0, "change_pct": -0.1, "is_proxy": True, "source": "etf_proxy"},
        {"label": "Russell 2000 proxy", "symbol": "IWM", "value": 202.0, "change_pct": 0.2, "is_proxy": True, "source": "etf_proxy"},
    ]
    assert response["sector_performance"] == [{"sector": "Technology", "change_pct": 1.25}]
