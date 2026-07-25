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
        if endpoint == "income-statement-ttm":
            return [
                {
                    "date": "2026-09-30",
                    "revenue": 220,
                    "ebitda": 66,
                    "ebit": 48.4,
                    "sellingGeneralAndAdministrativeExpenses": 24.2,
                    "incomeBeforeTax": 40,
                    "incomeTaxExpense": 8,
                    "interestExpense": 2,
                }
            ]
        if endpoint == "cash-flow-statement-ttm":
            return [{"date": "2026-09-30", "operatingCashFlow": 55, "capitalExpenditure": -11, "depreciationAndAmortization": 8.8}]
        if endpoint == "balance-sheet-statement-ttm":
            return [
                {
                    "date": "2026-09-30",
                    "cashAndShortTermInvestments": 17.6,
                    "netReceivables": 30.8,
                    "inventory": 39.6,
                    "accountPayables": 19.8,
                    "totalDebt": 40,
                }
            ]
        if endpoint in {"income-statement-growth", "cash-flow-statement-growth", "balance-sheet-statement-growth"}:
            return []
        if endpoint == "income-statement":
            return [{"date": "2025-09-30", "fiscalYear": "2025", "revenue": 100}]
        if endpoint in {"cash-flow-statement", "balance-sheet-statement"}:
            return []
        if endpoint == "analyst-estimates":
            return [
                {"date": "2027-09-30", "revenueAvg": 144},
                {"date": "2026-09-30", "revenueAvg": 120},
                {"date": "2025-09-30", "revenueAvg": 105},
            ]
        if endpoint == "profile":
            return [{"beta": 1.5}]
        if endpoint == "custom-discounted-cash-flow":
            for key in valuation_module.DCF_INPUT_PARAM_KEYS:
                assert key in params
            assert params["revenueGrowthPct"] == 0.2
            assert params["ebitdaPct"] == 0.3
            assert params["taxRate"] == 0.2
            assert params["beta"] == 1.5
            assert params["costOfEquity"] == 11.51
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
    assert "custom-discounted-cash-flow" in [call[0] for call in calls]
    assumptions = {item["key"]: item["value"] for item in response["dcf"]["assumptions"]}
    assert assumptions["revenueGrowthPct"] == 0.2
    assert assumptions["capitalExpenditurePct"] == 0.05


def test_ticker_valuation_keeps_dcf_when_consensus_unavailable(monkeypatch):
    def fake_request(endpoint, *, params, category, symbol=None, timeout_s=30, allow_user_request=False):
        if endpoint == "price-target-consensus":
            raise valuation_module.FMPClientError("temporary")
        if endpoint == "custom-discounted-cash-flow":
            return [{"year": "2026", "stockPrice": 100, "equityValuePerShare": 115}]
        if endpoint == "profile":
            return [{"beta": 1.0}]
        if endpoint in {
            "income-statement-ttm",
            "cash-flow-statement-ttm",
            "balance-sheet-statement-ttm",
            "income-statement",
            "cash-flow-statement",
            "balance-sheet-statement",
            "analyst-estimates",
            "income-statement-growth",
            "cash-flow-statement-growth",
            "balance-sheet-statement-growth",
        }:
            return []
        return [{"year": "2026", "stockPrice": 100, "equityValuePerShare": 115}]

    monkeypatch.setattr(valuation_module, "_request_stable_rows", fake_request)

    response = ticker_valuation("msft")

    assert response["status"] == "ok"
    assert response["dcf"]["fairValue"] == 115
    assert response["consensus"]["status"] == "unavailable"
    assert response["consensus"]["targetConsensus"] is None


def test_ticker_valuation_rejects_negative_fair_value_and_clamps_tax_rate(monkeypatch):
    def fake_request(endpoint, *, params, category, symbol=None, timeout_s=30, allow_user_request=False):
        if endpoint == "price-target-consensus":
            return [{"targetConsensus": 227}]
        if endpoint == "income-statement-ttm":
            return [{"revenue": 100, "incomeBeforeTax": 50, "incomeTaxExpense": -104.09}]
        if endpoint in {
            "cash-flow-statement-ttm",
            "balance-sheet-statement-ttm",
            "income-statement",
            "cash-flow-statement",
            "balance-sheet-statement",
            "analyst-estimates",
            "income-statement-growth",
            "cash-flow-statement-growth",
            "balance-sheet-statement-growth",
            "profile",
        }:
            return []
        return [
            {
                "year": "2026",
                "stockPrice": 187.77,
                "dcf": -65,
                "taxRate": -208.18,
                "longTermGrowthRate": 2,
            }
        ]

    monkeypatch.setattr(valuation_module, "_request_stable_rows", fake_request)

    response = ticker_valuation("nbis")
    assumptions = {item["key"]: item["value"] for item in response["dcf"]["assumptions"]}

    assert response["status"] == "ok"
    assert response["dcf"]["fairValue"] == 0
    assert response["dcf"]["bearValue"] == 0
    assert response["dcf"]["bullValue"] == 0
    assert round(response["dcf"]["upsideDownsidePct"], 2) == -100
    assert assumptions["taxRate"] == 0
    assert response["consensus"]["targetConsensus"] == 227


def test_forward_revenue_growth_uses_financial_estimate_horizon_cagr():
    growth = valuation_module._forward_revenue_growth_pct(
        [{"date": "2025-09-30", "revenue": 100}],
        [
            {"date": "2030-09-27", "revenueLow": 230, "revenueHigh": 260, "revenueAvg": 248.832},
            {"date": "2027-09-30", "revenueAvg": 144},
            {"date": "2026-09-30", "revenueAvg": 120},
            {"date": "2025-09-30", "revenueAvg": 105},
        ],
    )

    assert round(growth, 4) == 0.2


def test_forward_revenue_growth_caps_unsustainable_single_year_estimate():
    growth = valuation_module._forward_revenue_growth_pct(
        [{"date": "2025-09-30", "revenue": 100}],
        [{"date": "2026-09-30", "revenueAvg": 1_000}],
    )

    assert growth == 0.35


def test_dcf_inputs_normalize_extreme_beta(monkeypatch):
    def fake_request(endpoint, *, params, category, symbol=None, timeout_s=30, allow_user_request=False):
        if endpoint == "profile":
            return [{"beta": 0.5}]
        if endpoint == "income-statement-ttm":
            return [{"revenue": 100, "ebitda": 20, "ebit": 15, "incomeBeforeTax": 10, "incomeTaxExpense": 2}]
        if endpoint in {
            "price-target-consensus",
            "custom-discounted-cash-flow",
            "cash-flow-statement-ttm",
            "balance-sheet-statement-ttm",
            "income-statement",
            "cash-flow-statement",
            "balance-sheet-statement",
            "analyst-estimates",
            "income-statement-growth",
            "cash-flow-statement-growth",
            "balance-sheet-statement-growth",
        }:
            return []
        raise AssertionError(endpoint)

    monkeypatch.setattr(valuation_module, "_request_stable_rows", fake_request)

    assumptions = valuation_module._walnut_dcf_assumptions("SPCX")

    assert assumptions["beta"] == 1.0
    assert assumptions["costOfEquity"] == 9.15
