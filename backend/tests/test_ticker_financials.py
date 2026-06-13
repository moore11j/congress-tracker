from __future__ import annotations

import requests

import app.services.ticker_financials as financials_module
from app.db import Base
from app.main import ticker_financials
from app.models import TickerFinancialsCache
from app.request_priority import reset_request_context, set_request_context
from app.services.ticker_financials import clear_financials_cache
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker


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


def test_ticker_financials_normalizes_statement_earnings_and_summary(monkeypatch):
    clear_financials_cache()

    def fake_get(url, params=None, timeout=30):
        assert timeout == 5
        assert params["symbol"] == "AAPL"
        if url.endswith("/stable/income-statement") and params["period"] == "annual":
            return _FakeResponse(
                200,
                [
                    {
                        "date": "2025-09-30",
                        "calendarYear": "2025",
                        "period": "FY",
                        "revenue": 400_000_000_000,
                        "grossProfit": 184_000_000_000,
                        "operatingIncome": 128_000_000_000,
                        "netIncome": 100_000_000_000,
                        "eps": 6.25,
                        "companyName": "Apple Inc.",
                    },
                    {
                        "date": "2024-09-30",
                        "calendarYear": "2024",
                        "period": "FY",
                        "revenue": 380_000_000_000,
                        "grossProfit": 170_000_000_000,
                        "operatingIncome": 120_000_000_000,
                        "netIncome": 95_000_000_000,
                        "eps": 5.9,
                    },
                ],
            )
        if url.endswith("/stable/income-statement") and params["period"] == "quarter":
            return _FakeResponse(
                200,
                [
                    {
                        "date": "2026-03-31",
                        "calendarYear": "2026",
                        "period": "Q2",
                        "revenue": 100_000_000_000,
                        "grossProfit": 46_000_000_000,
                        "operatingIncome": 32_000_000_000,
                        "netIncome": 25_000_000_000,
                        "eps": 1.6,
                    },
                    {
                        "date": "2025-12-31",
                        "calendarYear": "2026",
                        "period": "Q1",
                        "revenue": 120_000_000_000,
                        "grossProfit": 55_000_000_000,
                        "operatingIncome": 40_000_000_000,
                        "netIncome": 30_000_000_000,
                        "eps": 1.9,
                    },
                    {
                        "date": "2025-09-30",
                        "calendarYear": "2025",
                        "period": "Q4",
                        "revenue": 95_000_000_000,
                        "grossProfit": 44_000_000_000,
                        "operatingIncome": 30_000_000_000,
                        "netIncome": 22_000_000_000,
                        "eps": 1.4,
                    },
                    {
                        "date": "2025-06-30",
                        "calendarYear": "2025",
                        "period": "Q3",
                        "revenue": 90_000_000_000,
                        "grossProfit": 41_000_000_000,
                        "operatingIncome": 28_000_000_000,
                        "netIncome": 20_000_000_000,
                        "eps": 1.3,
                    },
                    {
                        "date": "2025-03-31",
                        "calendarYear": "2025",
                        "period": "Q2",
                        "revenue": 88_000_000_000,
                        "netIncome": 19_000_000_000,
                        "eps": 1.2,
                    },
                ],
            )
        if url.endswith("/stable/cash-flow-statement"):
            return _FakeResponse(200, [])
        if url.endswith("/stable/balance-sheet-statement"):
            return _FakeResponse(200, [])
        if url.endswith("/stable/earnings"):
            return _FakeResponse(
                200,
                [
                    {"date": "2026-04-30", "period": "Q2", "fiscalYear": "2026", "epsActual": 1.6},
                    {"date": "2026-01-30", "period": "Q1", "fiscalYear": "2026", "epsActual": 1.9, "epsEstimate": 1.95},
                ],
            )
        if url.endswith("/stable/earnings-calendar"):
            return _FakeResponse(
                200,
                [
                    {"date": "2026-04-30", "period": "Q2", "fiscalYear": "2026", "epsEstimated": 1.5},
                    {"date": "2026-01-30", "period": "Q1", "fiscalYear": "2026", "epsEstimated": 1.95},
                ],
            )
        if url.endswith("/stable/analyst-estimates") and params["period"] == "quarter":
            return _FakeResponse(
                200,
                [
                    {
                        "date": "2026-06-30",
                        "period": "Q3",
                        "fiscalYear": "2026",
                        "revenueAvg": 101_000_000_000,
                        "revenueLow": 98_000_000_000,
                        "revenueHigh": 104_000_000_000,
                        "epsAvg": 1.72,
                        "epsLow": 1.65,
                        "epsHigh": 1.78,
                        "netIncomeAvg": 26_000_000_000,
                        "netIncomeLow": 24_500_000_000,
                        "netIncomeHigh": 27_500_000_000,
                    }
                ],
            )
        if url.endswith("/stable/analyst-estimates") and params["period"] == "annual":
            return _FakeResponse(
                200,
                [
                    {
                        "date": "2026-09-30",
                        "period": "FY",
                        "calendarYear": "2026",
                        "revenueAvg": 410_000_000_000,
                        "revenueLow": 400_000_000_000,
                        "revenueHigh": 420_000_000_000,
                        "epsAvg": 6.8,
                        "epsLow": 6.4,
                        "epsHigh": 7.1,
                        "netIncomeAvg": 108_000_000_000,
                        "netIncomeLow": 103_000_000_000,
                        "netIncomeHigh": 112_000_000_000,
                    }
                ],
            )
        if url.endswith("/stable/quote"):
            return _FakeResponse(200, [{"price": 170.0}])
        if url.endswith("/stable/ratios-ttm"):
            return _FakeResponse(200, [{"priceToEarningsRatioTTM": 27.4}])
        if url.endswith("/stable/key-metrics-ttm"):
            return _FakeResponse(200, [])
        raise AssertionError(f"Unexpected URL {url}")

    monkeypatch.setenv("FMP_API_KEY", "test-key")
    monkeypatch.setattr("app.services.ticker_financials.requests.get", fake_get)

    response = ticker_financials("aapl")

    assert response["symbol"] == "AAPL"
    assert response["companyName"] == "Apple Inc."
    assert response["status"] == "ok"
    assert response["summary"]["revenueTtm"] == 405_000_000_000
    assert response["summary"]["netIncomeTtm"] == 97_000_000_000
    assert round(response["summary"]["epsTtm"], 2) == 6.2
    assert response["summary"]["trailingPE"] == 27.4
    assert response["summary"]["forwardPE"] == 25.0
    assert response["summary"]["latestQuarter"] == "Q2 2026"
    assert round(response["quarterly"][-1]["grossMargin"], 1) == 46.0
    assert response["annual"][-1]["period"] == "2025"
    assert response["earnings"][-1]["epsEstimate"] == 1.5
    assert response["earnings"][-1]["result"] == "beat"
    assert round(response["earnings"][-1]["surprisePct"], 1) == 6.7
    assert response["section_statuses"]["income"] == "ok"
    assert response["section_statuses"]["earnings"] == "ok"
    assert response["section_statuses"]["forecasts"] == "ok"
    assert response["sections"]["income"]["annual"]
    assert response["sections"]["analyst_estimates"]["nextQuarter"]["revenueEstimate"] == 101_000_000_000
    assert response["forecasts"]["nextQuarter"]["revenueEstimate"] == 101_000_000_000
    assert response["forecasts"]["nextQuarter"]["revenueLow"] == 98_000_000_000
    assert response["forecasts"]["nextQuarter"]["revenueHigh"] == 104_000_000_000
    assert response["forecasts"]["nextQuarter"]["earningsLow"] == 24_500_000_000
    assert response["forecasts"]["nextQuarter"]["earningsHigh"] == 27_500_000_000
    assert response["forecasts"]["nextFiscalYear"]["epsEstimate"] == 6.8
    assert response["forecasts"]["nextFiscalYear"]["epsLow"] == 6.4
    assert response["forecasts"]["nextFiscalYear"]["epsHigh"] == 7.1


def test_ticker_financials_estimates_402_returns_partial_statements_and_caches(monkeypatch):
    clear_financials_cache()
    calls = {"count": 0}

    def fake_get(url, params=None, timeout=30):
        calls["count"] += 1
        assert params["symbol"] == "NBIS"
        if url.endswith("/stable/income-statement") and params["period"] == "annual":
            return _FakeResponse(
                200,
                [
                    {
                        "date": "2025-12-31",
                        "calendarYear": "2025",
                        "period": "FY",
                        "revenue": 4_000_000_000,
                        "grossProfit": 2_000_000_000,
                        "operatingIncome": 250_000_000,
                        "netIncome": 120_000_000,
                        "eps": 0.44,
                        "companyName": "Nebius Group N.V.",
                    }
                ],
            )
        if url.endswith("/stable/income-statement") and params["period"] == "quarter":
            return _FakeResponse(
                200,
                [
                    {
                        "date": "2026-03-31",
                        "calendarYear": "2026",
                        "period": "Q1",
                        "revenue": 1_200_000_000,
                        "grossProfit": 620_000_000,
                        "operatingIncome": 100_000_000,
                        "netIncome": 80_000_000,
                        "eps": 0.2,
                    },
                    {
                        "date": "2025-12-31",
                        "calendarYear": "2025",
                        "period": "Q4",
                        "revenue": 1_000_000_000,
                        "grossProfit": 500_000_000,
                        "operatingIncome": 70_000_000,
                        "netIncome": 40_000_000,
                        "eps": 0.1,
                    },
                    {
                        "date": "2025-09-30",
                        "calendarYear": "2025",
                        "period": "Q3",
                        "revenue": 900_000_000,
                        "netIncome": 20_000_000,
                        "eps": 0.05,
                    },
                    {
                        "date": "2025-06-30",
                        "calendarYear": "2025",
                        "period": "Q2",
                        "revenue": 800_000_000,
                        "netIncome": 10_000_000,
                        "eps": 0.03,
                    },
                ],
            )
        if url.endswith("/stable/cash-flow-statement"):
            return _FakeResponse(200, [])
        if url.endswith("/stable/balance-sheet-statement"):
            return _FakeResponse(
                200,
                [
                    {
                        "date": "2026-03-31",
                        "totalDebt": 500_000_000,
                        "totalStockholdersEquity": 2_000_000_000,
                        "totalCurrentAssets": 1_500_000_000,
                        "totalCurrentLiabilities": 750_000_000,
                        "totalAssets": 5_000_000_000,
                        "totalLiabilities": 1_250_000_000,
                    }
                ],
            )
        if url.endswith("/stable/earnings") or url.endswith("/stable/earnings-calendar"):
            return _FakeResponse(200, [])
        if url.endswith("/stable/analyst-estimates"):
            return _FakeResponse(402, {"message": "Restricted Endpoint"})
        if url.endswith("/stable/quote"):
            return _FakeResponse(200, [{"price": 80.0}])
        if url.endswith("/stable/ratios-ttm"):
            return _FakeResponse(200, [{"priceToEarningsRatioTTM": 44.0, "currentRatioTTM": 2.0}])
        if url.endswith("/stable/key-metrics-ttm"):
            return _FakeResponse(200, [{"debtToEquityTTM": 0.25}])
        raise AssertionError(f"Unexpected URL {url}")

    monkeypatch.setenv("FMP_API_KEY", "test-key")
    monkeypatch.setattr("app.services.ticker_financials.requests.get", fake_get)

    response = ticker_financials("nbis")

    assert response["symbol"] == "NBIS"
    assert response["status"] == "partial"
    assert response["annual"]
    assert response["quarterly"]
    assert response["summary"]["revenueTtm"] == 3_900_000_000
    assert response["summary"]["trailingPE"] == 44.0
    assert response["summary"]["currentRatio"] == 2.0
    assert response["forecasts"] == {"nextQuarter": None, "nextFiscalYear": None}
    assert response["section_statuses"]["income"] == "ok"
    assert response["section_statuses"]["forecasts"] == "unavailable"
    assert set(response["sections_present"]) == {"income", "health", "valuation"}
    assert response["sections"]["income"]["annual"]
    assert response["sections"]["analyst_estimates"] == {"nextQuarter": None, "nextFiscalYear": None}
    assert response["subsections"]["analyst_estimates"]["status"] == "unavailable"
    assert response["subsections"]["analyst_estimates"]["reason_code"] == "provider_entitlement"

    first_call_count = calls["count"]

    def fail_get(*_args, **_kwargs):
        raise AssertionError("cached partial financials should not refetch")

    monkeypatch.setattr("app.services.ticker_financials.requests.get", fail_get)
    cached = ticker_financials("NBIS")

    assert cached["summary"]["revenueTtm"] == 3_900_000_000
    assert cached["subsections"]["analyst_estimates"]["reason_code"] == "provider_entitlement"
    assert calls["count"] == first_call_count


def test_ticker_financials_public_endpoint_reads_prewarmed_db_cache(monkeypatch):
    clear_financials_cache()
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    monkeypatch.setenv("TICKER_FINANCIALS_SQLITE_CACHE", "1")
    monkeypatch.setenv("FMP_API_KEY", "test-key")
    monkeypatch.setattr(financials_module, "SessionLocal", Session)

    def fake_get(url, params=None, timeout=30):
        assert params["symbol"] == "NBIS"
        if url.endswith("/stable/income-statement") and params["period"] == "annual":
            return _FakeResponse(200, [{"date": "2025-12-31", "period": "FY", "revenue": 10, "netIncome": 2, "eps": 0.1}])
        if url.endswith("/stable/income-statement") and params["period"] == "quarter":
            return _FakeResponse(200, [{"date": "2026-03-31", "period": "Q1", "revenue": 12, "netIncome": 3, "eps": 0.2}])
        if url.endswith("/stable/analyst-estimates"):
            return _FakeResponse(402, {"message": "Restricted Endpoint"})
        if url.endswith("/stable/quote"):
            return _FakeResponse(200, [{"price": 80.0}])
        if url.endswith("/stable/ratios-ttm"):
            return _FakeResponse(200, [{"priceToEarningsRatioTTM": 44.0}])
        return _FakeResponse(200, [])

    monkeypatch.setattr("app.services.ticker_financials.requests.get", fake_get)
    worker_response = ticker_financials("NBIS")
    assert worker_response["status"] == "partial"

    db = Session()
    try:
        cached_row = db.execute(select(TickerFinancialsCache).where(TickerFinancialsCache.symbol == "NBIS")).scalar_one()
        assert cached_row.status == "partial"
    finally:
        db.close()

    clear_financials_cache()

    def fail_get(*_args, **_kwargs):
        raise AssertionError("public endpoint should read prewarmed financials from DB cache")

    monkeypatch.setattr("app.services.ticker_financials.requests.get", fail_get)
    token = set_request_context({"path": "/api/tickers/NBIS/financials", "priority": "heavy"})
    try:
        public_response = ticker_financials("NBIS")
    finally:
        reset_request_context(token)

    assert public_response["status"] == "partial"
    assert public_response["sections_present"]
    assert public_response["sections"]["income"]["annual"]
    assert public_response["section_statuses"]["forecasts"] == "unavailable"


def test_ticker_financials_unavailable_when_provider_missing(monkeypatch):
    clear_financials_cache()
    monkeypatch.delenv("FMP_API_KEY", raising=False)

    response = ticker_financials("INFQ")

    assert response["symbol"] == "INFQ"
    assert response["status"] == "unavailable"
    assert response["annual"] == []
    assert response["quarterly"] == []
    assert response["earnings"] == []
    assert response["message"] == "Financial data is temporarily unavailable."


def test_ticker_financials_empty_provider_response_is_no_data(monkeypatch):
    clear_financials_cache()

    def fake_get(url, params=None, timeout=30):
        return _FakeResponse(200, [])

    monkeypatch.setenv("FMP_API_KEY", "test-key")
    monkeypatch.setattr("app.services.ticker_financials.requests.get", fake_get)

    response = ticker_financials("INFQ")

    assert response["symbol"] == "INFQ"
    assert response["status"] == "no_data"
    assert response["annual"] == []
    assert response["quarterly"] == []
    assert response["earnings"] == []
    assert response["message"] == "Financial data is not available for this ticker yet."
    assert response["sections_present"] == []
