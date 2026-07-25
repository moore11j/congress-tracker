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
    assert response["dcf"]["fairValue"] == 209
    assert response["dcf"]["modelValue"] == 228
    assert response["dcf"]["valuationAnchor"] == 190
    assert response["dcf"]["anchorWeight"] == 0.5
    assert response["dcf"]["currentPrice"] == 190
    assert round(response["dcf"]["bearValue"], 2) == 177.65
    assert round(response["dcf"]["bullValue"], 2) == 240.35
    assert round(response["dcf"]["upsideDownsidePct"], 2) == 10.0
    assert response["dcf"]["judgment"] == "Fairly valued"
    assert response["dcf"]["rangeSource"] == "fair_value_anchor"
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
    assert response["dcf"]["fairValue"] == 107.5
    assert response["dcf"]["modelValue"] == 115
    assert response["dcf"]["valuationAnchor"] == 100
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
    assert response["dcf"]["fairValue"] == 93.885
    assert response["dcf"]["modelValue"] == 0
    assert round(response["dcf"]["bearValue"], 4) == 79.8022
    assert round(response["dcf"]["bullValue"], 4) == 107.9678
    assert round(response["dcf"]["upsideDownsidePct"], 2) == -50
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


def test_dcf_inputs_use_conservative_ramp_for_pre_profit_ticker(monkeypatch):
    def fake_request(endpoint, *, params, category, symbol=None, timeout_s=30, allow_user_request=False):
        if endpoint == "profile":
            return [{"beta": 0.5}]
        if endpoint == "income-statement-ttm":
            return [{"revenue": 100, "ebitda": 50, "ebit": 0, "incomeBeforeTax": 0, "incomeTaxExpense": 0}]
        if endpoint == "cash-flow-statement-ttm":
            return [{"operatingCashFlow": 22, "capitalExpenditure": -75, "depreciationAndAmortization": 35}]
        if endpoint == "balance-sheet-statement-ttm":
            return [{"cashAndShortTermInvestments": 100}]
        if endpoint == "analyst-estimates":
            return [{"date": "2026-12-31", "revenueAvg": 300}]
        if endpoint in {
            "price-target-consensus",
            "custom-discounted-cash-flow",
            "income-statement",
            "cash-flow-statement",
            "balance-sheet-statement",
            "income-statement-growth",
            "cash-flow-statement-growth",
            "balance-sheet-statement-growth",
        }:
            return []
        raise AssertionError(endpoint)

    monkeypatch.setattr(valuation_module, "_request_stable_rows", fake_request)

    assumptions = valuation_module._walnut_dcf_assumptions("SPCX")

    assert assumptions["revenueGrowthPct"] == 0.20
    assert assumptions["ebitdaPct"] == 0.15
    assert assumptions["ebitPct"] == 0.0
    assert assumptions["operatingCashFlowPct"] == 0.05
    assert assumptions["taxRate"] == 0.21
    assert assumptions["beta"] == 1.8
    assert round(assumptions["costOfEquity"], 4) == 12.926


def test_dcf_inputs_lower_beta_ceiling_for_quality_compounder(monkeypatch):
    def fake_request(endpoint, *, params, category, symbol=None, timeout_s=30, allow_user_request=False):
        if endpoint == "profile":
            return [{"beta": 2.2}]
        if endpoint == "income-statement-ttm":
            return [{"revenue": 100, "ebitda": 75, "ebit": 65, "incomeBeforeTax": 65, "incomeTaxExpense": 10}]
        if endpoint == "cash-flow-statement-ttm":
            return [{"operatingCashFlow": 50, "capitalExpenditure": -3, "depreciationAndAmortization": 1}]
        if endpoint == "analyst-estimates":
            return [{"date": "2026-12-31", "revenueAvg": 135}]
        if endpoint in {
            "price-target-consensus",
            "custom-discounted-cash-flow",
            "balance-sheet-statement-ttm",
            "income-statement",
            "cash-flow-statement",
            "balance-sheet-statement",
            "income-statement-growth",
            "cash-flow-statement-growth",
            "balance-sheet-statement-growth",
        }:
            return []
        raise AssertionError(endpoint)

    monkeypatch.setattr(valuation_module, "_request_stable_rows", fake_request)

    assumptions = valuation_module._walnut_dcf_assumptions("NVDA")

    assert assumptions["beta"] == 1.35
    assert assumptions["costOfEquity"] == 10.802


def test_dcf_inputs_credit_shareholder_yield_for_cash_return_compounder(monkeypatch):
    def fake_request(endpoint, *, params, category, symbol=None, timeout_s=30, allow_user_request=False):
        if endpoint == "profile":
            return [{"beta": 1.1, "sharesOutstanding": 1_000_000_000, "price": 100}]
        if endpoint == "income-statement-ttm":
            return [{"revenue": 100_000_000_000, "ebitda": 35_000_000_000, "ebit": 32_000_000_000, "incomeBeforeTax": 32_000_000_000, "incomeTaxExpense": 5_000_000_000}]
        if endpoint == "cash-flow-statement-ttm":
            return [{"operatingCashFlow": 31_000_000_000, "capitalExpenditure": -2_000_000_000, "dividendsPaid": -500_000_000, "commonStockRepurchased": -3_500_000_000}]
        if endpoint == "income-statement":
            return [{"date": "2025-09-30", "revenue": 100_000_000_000}]
        if endpoint == "analyst-estimates":
            return [{"date": "2026-09-30", "revenueAvg": 110_000_000_000}]
        if endpoint in {
            "price-target-consensus",
            "custom-discounted-cash-flow",
            "balance-sheet-statement-ttm",
            "cash-flow-statement",
            "balance-sheet-statement",
            "income-statement-growth",
            "cash-flow-statement-growth",
            "balance-sheet-statement-growth",
        }:
            return []
        raise AssertionError(endpoint)

    monkeypatch.setattr(valuation_module, "_request_stable_rows", fake_request)

    assumptions = valuation_module._walnut_dcf_assumptions("AAPL")

    assert round(assumptions["revenueGrowthPct"], 4) == 0.14


def test_ticker_valuation_uses_asset_nav_for_asset_heavy_pre_profit_company(monkeypatch):
    def fake_request(endpoint, *, params, category, symbol=None, timeout_s=30, allow_user_request=False):
        if endpoint == "price-target-consensus":
            return [{"targetConsensus": 30}]
        if endpoint == "profile":
            return [{"beta": 1.2, "sharesOutstanding": 100_000_000}]
        if endpoint == "income-statement-ttm":
            return [{"revenue": 100_000_000, "ebitda": 0, "ebit": -5_000_000, "incomeBeforeTax": -5_000_000, "incomeTaxExpense": 0}]
        if endpoint == "cash-flow-statement-ttm":
            return [
                {
                    "operatingCashFlow": 5_000_000,
                    "capitalExpenditure": 0,
                    "dividendsPaid": -1_000_000,
                    "commonStockRepurchased": -1_000_000,
                    "depreciationAndAmortization": 1_000_000,
                }
            ]
        if endpoint == "balance-sheet-statement-ttm":
            return [{"totalAssets": 2_000_000_000, "totalLiabilities": 200_000_000, "cashAndShortTermInvestments": 1_700_000_000}]
        if endpoint == "custom-discounted-cash-flow":
            return [{"year": "2026", "stockPrice": 14.5, "dcf": 2}]
        if endpoint in {
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

    response = ticker_valuation("bmnr")

    assert response["status"] == "ok"
    assert round(response["dcf"]["fairValue"], 4) == 16.2633
    assert round(response["dcf"]["modelValue"], 4) == 18.0266
    assert response["dcf"]["method"] == "Asset / NAV"
    assert response["dcf"]["rangeSource"] == "fair_value_anchor"
    assert response["dcf"]["judgment"] == "Fairly valued"
    assert response["dcf"]["methodSignals"][0] == {"method": "DCF", "signal": "Cash-flow limited"}
