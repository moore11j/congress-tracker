import pytest
import requests

from app.services import ticker_valuation as valuation
from app.services.provider_usage import ProviderBudgetExceeded, ProviderDisabled


@pytest.mark.parametrize("error_type", [ProviderDisabled, ProviderBudgetExceeded])
@pytest.mark.parametrize("stage", ["consensus", "inputs", "calculation"])
def test_provider_guard_returns_contract_and_stops_requests(monkeypatch, error_type, stage):
    calls = []
    def request(endpoint, **kwargs):
        category = kwargs["category"]
        calls.append(category)
        target = {
            "consensus": "valuation:price-target-consensus",
            "inputs": "valuation:dcf-inputs:income-statement-ttm",
            "calculation": "valuation:custom-discounted-cash-flow",
        }[stage]
        if category == target:
            raise error_type("controlled_provider_stop")
        if endpoint == "price-target-consensus":
            return [{"targetConsensus": 205, "targetHigh": 250, "targetLow": 175, "targetMedian": 210}]
        return []
    monkeypatch.setattr(valuation, "_request_stable_rows", request)
    result = valuation.get_ticker_valuation("aapl")
    assert result["symbol"] == "AAPL"
    assert result["status"] == "unavailable"
    assert result["message"] == valuation.TEMPORARILY_UNAVAILABLE_MESSAGE
    assert result["dcf"]["fairValue"] is None
    assert result["dcf"]["cashFlows"] == []
    assert len(calls) == {"consensus": 1, "inputs": 2, "calculation": 14}[stage]
    if stage == "consensus":
        assert result["consensus"]["targetConsensus"] is None
    else:
        assert result["consensus"]["targetConsensus"] == 205


def test_actual_disable_guard_has_no_http_and_keeps_unavailable(monkeypatch):
    monkeypatch.setenv("FMP_API_KEY", "offline-placeholder-not-a-secret")
    monkeypatch.setenv("FMP_PROVIDER_DISABLED", "true")
    monkeypatch.setenv("FMP_PERSIST_USAGE_EVENTS", "false")
    def no_network(*args, **kwargs):
        pytest.fail("Disabled FMP must not make an HTTP request")
    monkeypatch.setattr(requests.sessions.Session, "request", no_network)
    result = valuation.get_ticker_valuation("AAPL")
    assert result["status"] == "unavailable"
    assert result["dcf"]["fairValue"] is None


def test_unexpected_errors_are_not_hidden(monkeypatch):
    def broken(*args, **kwargs):
        raise ValueError("unexpected defect")
    monkeypatch.setattr(valuation, "_request_stable_rows", broken)
    with pytest.raises(ValueError, match="unexpected defect"):
        valuation.get_ticker_valuation("AAPL")
