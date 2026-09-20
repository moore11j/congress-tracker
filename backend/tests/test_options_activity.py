from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.models import TickerContentCache
from app.services import options_activity as service
from app.services.intelligence_overlays import get_options_flow_summaries_for_symbols


@pytest.fixture
def provider(monkeypatch):
    engine = create_engine("sqlite://")
    TickerContentCache.__table__.create(engine)
    factory = sessionmaker(bind=engine)
    monkeypatch.setattr(service.reference, "SessionLocal", factory)
    monkeypatch.setattr(service.alpaca, "enabled", lambda: True)
    monkeypatch.setattr(service.reference, "_request", lambda *a: {"results": [{"expiration_date": "2027-01-15"}]})
    monkeypatch.setattr(service.reference, "contracts", lambda *a: {"contracts": [
        {"ticker": "O:CALL", "kind": "call"}, {"ticker": "O:PUT", "kind": "put"}], "next_cursor": None})
    calls = []
    stamp = (datetime.now(timezone.utc) - timedelta(days=1)).replace(hour=4).isoformat()
    payload = {"bars": {"CALL": [{"t": stamp, "v": 100, "vw": 2, "c": 99}], "PUT": [{"t": stamp, "v": 20, "vw": 1, "c": 99}]}}
    def request(params):
        calls.append(dict(params))
        return payload
    monkeypatch.setattr(service.alpaca, "_request", request)
    yield factory, calls, payload
    engine.dispose()


def test_refresh_uses_vwap_not_close_and_reuses_cache(provider):
    factory, calls, payload = provider
    result = service.refresh("SPY")
    assert result["metrics"]["call_premium"] == 20000
    assert result["metrics"]["put_premium"] == 2000
    assert result["metrics"]["recent_contract_volume"] == 120
    assert result["state"] == "call_heavy"
    assert result["can_confirm"] is False
    assert result["score"] is None
    assert result["coverage"]["scope"] == "single_expiration"
    assert result["metrics"]["volume_multiple"] is None
    assert service.refresh("SPY") == result
    assert len(calls) == 1
    assert calls[0]["timeframe"] == "1Day"
    assert "feed" not in calls[0]
    assert datetime.fromisoformat(calls[0]["end"]) < datetime.now(timezone.utc) - timedelta(minutes=15)


def test_cached_overlay_is_local_and_keeps_scope_and_requested_window(provider, monkeypatch):
    factory, _, _ = provider
    service.refresh("SPY")
    monkeypatch.setattr(service.alpaca, "_request", lambda *a: pytest.fail("Overlay must not fetch externally"))
    with factory() as db:
        rows, availability = get_options_flow_summaries_for_symbols(db, ["SPY", "QQQ"])
        assert availability["status"] == "ok"
        assert rows["SPY"]["coverage"]["expiration"] == "2027-01-15"
        assert rows["SPY"]["can_confirm"] is False
        assert rows["QQQ"]["status"] == "unavailable"
        rows, _ = get_options_flow_summaries_for_symbols(db, ["SPY"], lookback_days=7)
        assert rows["SPY"]["status"] == "unavailable"
        rows, _ = get_options_flow_summaries_for_symbols(db, ["SPY"], feature_enabled=False)
        assert rows["SPY"]["status"] == "unavailable"


@pytest.mark.parametrize("field,value", [("vw", None), ("vw", float("nan")), ("v", None), ("v", -1), ("v", 1.5)])
def test_bad_data_never_saves_partial_totals(provider, field, value):
    factory, _, payload = provider
    payload["bars"]["CALL"][0][field] = value
    with pytest.raises(service.reference.OptionsDataError):
        service.refresh("SPY")
    with factory() as db:
        assert service.cached_summaries(db, ["SPY"]) == {}


def test_repeated_history_cursor_fails_closed(provider):
    factory, _, payload = provider
    payload["next_page_token"] = "same"
    with pytest.raises(service.reference.OptionsDataError, match="incomplete"):
        service.refresh("SPY")
    with factory() as db:
        assert service.cached_summaries(db, ["SPY"]) == {}


def test_chain_pagination_and_duplicate_bars(provider, monkeypatch):
    factory, calls, payload = provider
    monkeypatch.setattr(service.reference, "contracts", lambda symbol, expiry, cursor=None: {
        "contracts": [{"ticker": "O:PUT" if cursor else "O:CALL", "kind": "put" if cursor else "call"}],
        "next_cursor": None if cursor else "second"})
    original = service.alpaca._request
    def request(params):
        result = original(params)
        return {**result, "next_page_token": None if params.get("page_token") else "second"}
    monkeypatch.setattr(service.alpaca, "_request", request)
    result = service.refresh("SPY")
    assert result["total_premium"] == 22000
    assert result["coverage"]["listed_contracts"] == 2
    assert len(calls) == 2


def test_volume_spike_uses_prior_sessions_not_latest():
    now = datetime.now(timezone.utc)
    contracts = {"CALL": {"kind": "call"}, "PUT": {"kind": "put"}}
    bars = {("CALL", (now - timedelta(days=day)).date().isoformat()): {"v": 100 if day > 1 else 300, "vw": 2} for day in range(1, 7)}
    result = service.summarize("SPY", "2027-01-15", contracts, bars, now)
    assert result["metrics"]["volume_multiple"] == 3
    assert result["metrics"]["baseline_sessions"] == 5
    assert result["intensity"] == "high"


def test_cache_expiry_and_corruption_are_unavailable(provider):
    factory, _, _ = provider
    service.refresh("SPY")
    with factory() as db:
        row = db.query(TickerContentCache).one()
        row.fetched_at = datetime.now(timezone.utc) - timedelta(hours=25)
        db.commit()
        assert service.cached_summaries(db, ["SPY"]) == {}
        row.fetched_at = datetime.now(timezone.utc)
        row.payload_json = "invalid"
        db.commit()
        assert service.cached_summaries(db, ["SPY"]) == {}


def test_activity_never_contributes_to_confirmation(provider):
    from app.services.confirmation_score import _options_flow_source
    result = service.refresh("SPY")
    assert _options_flow_source("SPY", 30, summary=result).present is False


def test_route_checks_entitlement_before_fetch(monkeypatch):
    from fastapi import HTTPException, Request, Response
    from app.entitlements import ENTITLEMENTS
    from app.routers import options_calculator as router
    request = Request({"type": "http", "headers": []})
    monkeypatch.setattr(router, "current_entitlements", lambda *a: ENTITLEMENTS["free"])
    monkeypatch.setattr(service, "refresh", lambda *a: pytest.fail("Unauthorized request reached provider"))
    with pytest.raises(HTTPException) as error:
        router.option_activity(request, Response(), "SPY", None)
    assert error.value.status_code == 402
    monkeypatch.setattr(router, "current_entitlements", lambda *a: ENTITLEMENTS["pro"])
    monkeypatch.setattr(service, "refresh", lambda *a: {"can_confirm": False})
    response = Response()
    assert router.option_activity(request, response, "SPY", None) == {"can_confirm": False}
    assert response.headers["cache-control"] == "private, no-store"


def test_truncated_chain_without_cursor_is_not_used(provider, monkeypatch):
    monkeypatch.setattr(service.reference, "contracts", lambda *a: {
        "contracts": [{"ticker": "O:CALL", "kind": "call"}], "truncated": True})
    with pytest.raises(service.reference.OptionsDataError, match="incomplete"):
        service.refresh("SPY")
