from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.models import PriceCache, QuoteCache
from app.services.custom_alert_rules import _metric_value
from app.services.price_alert_reference import daily_price_observation, refresh_daily_price_references, valid_daily_price_evidence


def sessions():
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)


def test_onds_old_close_is_rejected_then_exact_close_produces_correct_move():
    factory = sessions()
    now = datetime(2026, 9, 16, 13, 39, tzinfo=timezone.utc)
    with factory() as db:
        db.add_all([PriceCache(symbol="ONDS", date="2026-09-08", close=7.62), QuoteCache(symbol="ONDS", price=7.135, asof_ts=now)])
        db.commit()
        condition = {"metric": "price_change_pct", "time_window": {"value": 1, "unit": "day"}}
        assert _metric_value(db, "ONDS", condition, now)[0] is None
        assert daily_price_observation(db, "ONDS", now)["status"] == "missing_reference_close"
        db.add(PriceCache(symbol="ONDS", date="2026-09-15", close=7.24))
        db.commit()
        evidence = daily_price_observation(db, "ONDS", now)
        assert round(evidence["change_pct"], 2) == -1.45
        assert evidence["reference_date"] == "2026-09-15"
        assert valid_daily_price_evidence(evidence, now)
        assert not valid_daily_price_evidence({**evidence, "reference_date": "2026-09-08"}, now)
        assert not valid_daily_price_evidence({**evidence, "change_pct": -6.36}, now)


@pytest.mark.parametrize("day,prior", [("2026-09-14", "2026-09-11"), ("2026-09-08", "2026-09-04")])
def test_reference_requires_actual_previous_market_session_on_weekends_and_holidays(day, prior):
    now = datetime.fromisoformat(day+"T15:00:00+00:00")
    factory = sessions()
    with factory() as db:
        db.add_all([PriceCache(symbol="TEST", date=prior, close=100), QuoteCache(symbol="TEST", price=94, asof_ts=now)])
        db.commit()
        observation = daily_price_observation(db, "TEST", now)
        assert observation["reference_date"] == prior
        assert round(observation["change_pct"], 2) == -6


def test_refresh_does_not_accept_provider_success_without_required_date(monkeypatch):
    factory = sessions()
    now = datetime(2026, 9, 16, 15, tzinfo=timezone.utc)
    provider = MagicMock(return_value={"status": "ok", "latest_date": "2026-09-08"})
    monkeypatch.setattr("app.services.price_alert_reference.refresh_recent_price_history", provider)
    result = refresh_daily_price_references(factory, now=now, symbols=["ONDS"])
    assert result["unavailable"] == 1
    assert provider.call_args.kwargs["end_date"] == "2026-09-15"


def test_refresh_repairs_exact_date_once_and_reuses_it(monkeypatch):
    factory = sessions()
    now = datetime(2026, 9, 16, 15, tzinfo=timezone.utc)
    def provider(db, ticker, **kwargs):
        db.add(PriceCache(symbol=ticker, date=kwargs["end_date"], close=7.24))
        db.commit()
    fetch = MagicMock(side_effect=provider)
    monkeypatch.setattr("app.services.price_alert_reference.refresh_recent_price_history", fetch)
    first = refresh_daily_price_references(factory, now=now, symbols=["ONDS"])
    second = refresh_daily_price_references(factory, now=now, symbols=["ONDS"])
    assert first["available"] == second["available"] == 1
    assert first["fetched"] == 1 and second["fetched"] == 0
    fetch.assert_called_once()
