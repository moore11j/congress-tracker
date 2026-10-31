from copy import deepcopy

import pytest

from app.clients.fmp import FMPClientError, FMPSubscriptionRestrictedError
from app.services import ticker_valuation as valuation
from app.services.provider_usage import ProviderDisabled


@pytest.mark.parametrize("ttm_rows, needs_fallback", [
    ([], True), ([{}], True),
    ([{"date": "2026-09-30"}], False),
    ([{"operatingCashFlow": 200, "totalDebt": 400}], False),
])
def test_annual_fallback_matches_existing_row_selection(monkeypatch, ttm_rows, needs_fallback):
    calls = []

    def request(endpoint, **kwargs):
        calls.append(endpoint)
        if endpoint.endswith("-ttm"):
            return deepcopy(ttm_rows)
        if endpoint == "cash-flow-statement":
            return [{"operatingCashFlow": 987}]
        if endpoint == "balance-sheet-statement":
            return [{"totalDebt": 654}]
        return []

    monkeypatch.setattr(valuation, "_request_stable_rows", request)
    rows = valuation._fetch_dcf_input_rows("AAPL")
    assert ("cash-flow-statement" in calls) is needs_fallback
    assert ("balance-sheet-statement" in calls) is needs_fallback
    assert "income-statement" in calls  # still needed for the estimate horizon
    assert "cash-flow-statement-growth" not in calls
    assert "balance-sheet-statement-growth" not in calls
    assert len(calls) == (10 if needs_fallback else 8)
    if needs_fallback:
        assert rows["annual_cash"][0]["operatingCashFlow"] == 987
        assert rows["annual_balance"][0]["totalDebt"] == 654
    else:
        assert rows["annual_cash"] == rows["annual_balance"] == []


@pytest.mark.parametrize("error", [FMPClientError, FMPSubscriptionRestrictedError])
def test_failed_ttm_retains_annual_fallback(monkeypatch, error):
    calls = []

    def request(endpoint, **kwargs):
        calls.append(endpoint)
        if endpoint.endswith("-ttm"):
            raise error("controlled failure")
        if endpoint in {"cash-flow-statement", "balance-sheet-statement"}:
            return [{"date": "2025-12-31", "operatingCashFlow": 80, "totalDebt": 200}]
        return []

    monkeypatch.setattr(valuation, "_request_stable_rows", request)
    rows = valuation._fetch_dcf_input_rows("AAPL")
    assert rows["annual_cash"][0]["operatingCashFlow"] == 80
    assert rows["annual_balance"][0]["totalDebt"] == 200
    assert len(calls) == 10


def test_cash_and_balance_fallback_are_independent(monkeypatch):
    calls = []

    def request(endpoint, **kwargs):
        calls.append(endpoint)
        return [{"operatingCashFlow": 200}] if endpoint == "cash-flow-statement-ttm" else []

    monkeypatch.setattr(valuation, "_request_stable_rows", request)
    valuation._fetch_dcf_input_rows("AAPL")
    assert "cash-flow-statement" not in calls
    assert "balance-sheet-statement" in calls
    assert len(calls) == 9


def test_provider_guard_still_stops_at_required_annual_fallback(monkeypatch):
    calls = []

    def request(endpoint, **kwargs):
        calls.append(endpoint)
        if endpoint == "cash-flow-statement":
            raise ProviderDisabled("controlled stop")
        return []

    monkeypatch.setattr(valuation, "_request_stable_rows", request)
    result = valuation.get_ticker_valuation("AAPL")
    assert result["status"] == "unavailable"
    assert calls[-1] == "cash-flow-statement"
    assert "balance-sheet-statement" not in calls


def test_normalized_model_is_offline_and_does_not_mutate_inputs(monkeypatch):
    def no_provider(*args, **kwargs):
        pytest.fail("Normalized model must not request provider data")

    monkeypatch.setattr(valuation, "_request_stable_rows", no_provider)
    rows = {key: [] for key in (
        "ttm_income", "ttm_cash", "ttm_balance", "annual_income", "annual_cash",
        "annual_balance", "annual_estimates", "income_growth", "profile", "dcf_baseline",
    )}
    rows["ttm_income"] = [{"revenue": 1000, "ebitda": 300, "ebit": 200}]
    rows["ttm_cash"] = [{"operatingCashFlow": 250, "capitalExpenditure": -50}]
    rows["ttm_balance"] = [{"totalDebt": 100, "cashAndShortTermInvestments": 50}]
    rows["profile"] = [{"sector": "Technology", "sharesOutstanding": 100}]
    original = deepcopy(rows)
    result = valuation._valuation_model_from_rows(rows)
    assert result["multiplesValue"] == 53.5
    assert result["assumptions"]["operatingCashFlowPct"] == 0.25
    assert len(result["projectedCashFlows"]) == 5
    assert rows == original
    assert valuation._valuation_model_from_rows(rows) == result
