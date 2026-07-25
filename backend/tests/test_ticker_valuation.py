import app.services.ticker_valuation as valuation_module
from app.main import ticker_valuation


def test_ticker_valuation_uses_custom_dcf_and_consensus(monkeypatch):
    calls = []

    def fake_request(endpoint, *, params, category, symbol=None, timeout_s=30, allow_user_request=False):
        calls.append((endpoint, category, params, symbol, allow_user_request))
        assert params["symbol"] == "AAPL"
        assert symbol == "AAPL"
        assert allow_user_request is True
        if endpoint == "price-target-consensus":
            return [{"targetConsensus": 205, "targetHigh": 250, "targetLow": 175, "targetMedian": 210}]
        if endpoint == "custom-discounted-cash-flow":
            return [
                {
                    "year": "2026",
                    "Stock Price": 190,
                    "dcf": 228,
                    "freeCashFlow": 100_000_000_000,
                    "discountedCashFlow": 92_000_000_000,
                    "revenueGrowthPct": 0.08,
                    "costOfEquity": 9.2,
                    "riskFreeRate": 4.1,
                },
                {
                    "year": "2027",
                    "freeCashFlow": 112_000_000_000,
                    "discountedCashFlow": 94_000_000_000,
                },
            ]
        raise AssertionError(endpoint)

    monkeypatch.setattr(valuation_module, "_request_stable_rows", fake_request)

    response = ticker_valuation("aapl")

    assert response["symbol"] == "AAPL"
    assert response["status"] == "ok"
    assert response["dcf"]["fairValue"] == 228
    assert response["dcf"]["currentPrice"] == 190
    assert round(response["dcf"]["bearValue"], 2) == 193.8
    assert round(response["dcf"]["bullValue"], 2) == 262.2
    assert round(response["dcf"]["upsideDownsidePct"], 2) == 20.0
    assert response["dcf"]["judgment"] == "Undervalued"
    assert response["dcf"]["cashFlows"][0] == {
        "year": "2026",
        "actualCashFlow": 100_000_000_000,
        "discountedCashFlow": 92_000_000_000,
    }
    assert response["consensus"]["targetConsensus"] == 205
    assert [call[0] for call in calls] == ["price-target-consensus", "custom-discounted-cash-flow"]


def test_ticker_valuation_keeps_dcf_when_consensus_unavailable(monkeypatch):
    def fake_request(endpoint, *, params, category, symbol=None, timeout_s=30, allow_user_request=False):
        if endpoint == "price-target-consensus":
            raise valuation_module.FMPClientError("temporary")
        return [{"year": "2026", "stockPrice": 100, "equityValuePerShare": 115}]

    monkeypatch.setattr(valuation_module, "_request_stable_rows", fake_request)

    response = ticker_valuation("msft")

    assert response["status"] == "ok"
    assert response["dcf"]["fairValue"] == 115
    assert response["consensus"]["status"] == "unavailable"
    assert response["consensus"]["targetConsensus"] is None
