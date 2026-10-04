from datetime import date, datetime, timedelta, timezone
import json

import pytest
from fastapi import HTTPException
from pydantic import ValidationError
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from starlette.requests import Request

from app.db import Base
from app.models import Event, GovernmentContract, FundamentalsCache, PriceCache
from app.routers import backtests
from app.services.backtesting.signal_mixer import SignalMixerConfig, MixerEvent, select_setups, evaluate_setups, load_mixer_events, run_signal_mixer
from app.services.fundamentals_cache import fundamentals_summary_from_cache_row


def config(**kwargs):
    return SignalMixerConfig(start_date=date(2024, 1, 1), end_date=date(2025, 6, 1), **kwargs)


def event(day="2024-01-10", actor="buyer", event_id="1"):
    return MixerEvent("AAA", date.fromisoformat(day), event_id, actor)


def prices():
    start = date(2023, 9, 1)
    days = [start + timedelta(days=i) for i in range(650)]
    return {"AAA": {day.isoformat(): 100 + i for i, day in enumerate(days)}, "SPY": {day.isoformat(): 100 for day in days}}


def test_sequence_rejects_future_same_day_and_outside_window():
    trigger = [event()]
    for day in ("2024-01-10", "2024-01-11", "2023-10-01"):
        assert not select_setups(config(), trigger, [event(day)], {})[0]
    matches, diagnostics = select_setups(config(), trigger, [event("2024-01-09")], {})
    assert len(matches) == 1
    assert diagnostics["triggers"] == 1


def test_distinct_buyers_same_day_and_deduplication():
    triggers = [event(actor="a", event_id="1"), event(actor="b", event_id="2"), event(actor="a", event_id="3")]
    setups, counts = select_setups(config(minimum_buyers=2), triggers, [event("2024-01-09")], {})
    assert len(setups) == 1
    assert counts["duplicate_symbol_day"] == 2
    assert not select_setups(config(minimum_buyers=3), triggers, [event("2024-01-09")], {})[0]


def test_sma_uses_no_future_data_and_requires_history():
    history = prices()
    setups, _ = select_setups(config(above_sma50=True), [event()], [event("2024-01-09")], history)
    assert len(setups) == 1
    for day in history["AAA"]:
        if day > "2024-01-10":
            history["AAA"][day] = 0.01
    assert len(select_setups(config(above_sma50=True), [event()], [event("2024-01-09")], history)[0]) == 1
    assert not select_setups(config(above_sma50=True), [event()], [event("2024-01-09")], {"AAA": {"2024-01-10": 100}})[0]


def test_next_close_costs_and_same_dates_for_benchmark():
    history = prices()
    rows = evaluate_setups(config(fee_bps=5, slippage_bps=10), [(event(), event("2024-01-09"))], history)
    sample = rows[0]["examples"][0]
    assert sample["entry_date"] == "2024-01-11"
    assert sample["exit_date"] == "2024-02-10"
    expected = (history["AAA"]["2024-02-10"] * .9985 / (history["AAA"]["2024-01-11"] * 1.0015) - 1) * 100
    assert sample["net_return_pct"] == pytest.approx(expected, abs=.0001)
    assert sample["net_return_pct"] < sample["gross_return_pct"]
    assert sample["spy_return_pct"] < 0
    assert rows[0]["beat_spy_rate_pct"] == 100


def test_overlap_and_pending_not_shortened():
    cfg = SignalMixerConfig(start_date=date(2024, 1, 1), end_date=date(2024, 3, 1))
    setups = [(event(), event("2024-01-09")), (event("2024-01-12"), event("2024-01-09"))]
    rows = evaluate_setups(cfg, setups, prices())
    assert rows[0]["sample_size"] == 1
    assert rows[0]["exclusions"]["overlapping_setups"] == 1
    assert rows[1]["sample_size"] == 0
    assert rows[1]["exclusions"]["pending"] == 1
    assert rows[1]["median_net_return_pct"] is None


def test_missing_benchmark_and_stale_prices_are_excluded():
    history = {"AAA": {"2024-02-01": 100}, "SPY": {"2024-02-01": 100}}
    rows = evaluate_setups(config(), [(event(), event("2024-01-09"))], history)
    assert rows[0]["exclusions"]["missing_entry_prices"] == 1
    assert rows[0]["sample_size"] == 0
    history = prices()
    history["SPY"] = {}
    assert evaluate_setups(config(), [(event(), event("2024-01-09"))], history)[0]["sample_size"] == 0


@pytest.mark.parametrize("values", [{"trigger": "insider", "confirmation": "insider"}, {"slippage_bps": float("nan")}, {"fee_bps": -1}, {"window_days": 91}, {"minimum_buyers": 0}])
def test_invalid_configs(values):
    with pytest.raises(ValidationError):
        config(**values)


def test_explicit_filing_dates_and_contract_observation():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as db:
        stamp = datetime(2024, 1, 10, tzinfo=timezone.utc)
        for index, payload in enumerate([{"filing_date": "2024-01-10", "transaction_date": "2023-12-01"}, {"transaction_date": "2023-12-01"}]):
            db.add(Event(id=index + 1, event_type="insider_trade", ts=stamp, event_date=stamp, symbol="AAA", source="fixture", trade_type="purchase", payload_json=json.dumps(payload)))
        db.add(GovernmentContract(symbol="AAA", award_date=date(2023, 12, 1), award_amount=1000, created_at=stamp))
        db.commit()
        events, missing = load_mixer_events(db, "insider", date(2024, 1, 1), date(2024, 2, 1))
        assert len(events) == 1 and missing == 1
        assert events[0].day == date(2024, 1, 10)
        contracts, _ = load_mixer_events(db, "government_contract", date(2024, 1, 1), date(2024, 2, 1))
        assert contracts[0].day == date(2024, 1, 10)
        result = run_signal_mixer(db, config())
        assert result["matched_setups"] == 0  # same-day contract excluded


def test_route_keeps_premium_gate(monkeypatch):
    monkeypatch.setattr(backtests, "current_user", lambda *a, **k: object())
    monkeypatch.setattr(backtests, "current_entitlements", lambda *a: object())
    def reject(*args, **kwargs):
        raise HTTPException(403, "Premium required")
    monkeypatch.setattr(backtests, "require_feature", reject)
    monkeypatch.setattr(backtests, "run_signal_mixer", lambda *a: pytest.fail("must not run before access check"))
    with pytest.raises(HTTPException) as error:
        backtests.signal_mixer_run(config(), Request({"type": "http", "headers": []}), None)
    assert error.value.status_code == 403


def test_route_requires_sign_in(monkeypatch):
    def reject(*args, **kwargs):
        raise HTTPException(401, "Sign in required")
    monkeypatch.setattr(backtests, "current_user", reject)
    monkeypatch.setattr(backtests, "run_signal_mixer", lambda *a: pytest.fail("must not run anonymously"))
    with pytest.raises(HTTPException) as error:
        backtests.signal_mixer_run(config(), Request({"type": "http", "headers": []}), None)
    assert error.value.status_code == 401


def test_database_end_to_end_returns_real_fixture_outcomes():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as db:
        stamp = datetime(2024, 1, 10, tzinfo=timezone.utc)
        db.add(Event(event_type="insider_trade", ts=stamp, symbol="AAA", source="fixture", trade_type="purchase", payload_json=json.dumps({"filing_date": "2024-01-10"})))
        db.add(GovernmentContract(symbol="AAA", award_date=date(2024, 1, 3), award_amount=1000, created_at=datetime(2024, 1, 4, tzinfo=timezone.utc)))
        for symbol, history in prices().items():
            for day, close in history.items():
                db.add(PriceCache(symbol=symbol, date=day, close=close))
        db.commit()
        result = run_signal_mixer(db, config())
        assert result["matched_setups"] == 1
        assert result["horizons"][0]["sample_size"] == 1
        assert result["horizons"][0]["examples"][0]["confirmation_date"] == "2024-01-04"


def test_cash_context_does_not_change_fundamental_score():
    row = FundamentalsCache(symbol="AAA", provider="fixture", fetched_at=datetime(2024, 1, 1, tzinfo=timezone.utc), revenue_growth=15, roe=20, ev_to_ebitda=10, operating_margin_expansion=2, net_debt_to_ebitda=1, free_cash_flow=100)
    positive = fundamentals_summary_from_cache_row(row)
    row.free_cash_flow = -100
    negative = fundamentals_summary_from_cache_row(row)
    assert positive["status"] == negative["status"]
    assert positive["metrics"] == negative["metrics"]
    assert negative["context"]["free_cash_flow"] == -100
