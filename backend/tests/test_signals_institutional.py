from __future__ import annotations

from datetime import date

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

import app.routers.signals as signals_module
from app.db import Base
from app.models import InstitutionalActivityEvent
from app.routers.signals import _institutional_unusual_multiple, _query_unified_signals


def _db() -> Session:
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    return Session(engine)


def _institutional_event(event_id: int, event_type: str, **kwargs) -> InstitutionalActivityEvent:
    return InstitutionalActivityEvent(
        id=event_id,
        symbol=kwargs.pop("symbol", "AAPL"),
        normalized_symbol=kwargs.pop("normalized_symbol", "AAPL"),
        cik=kwargs.pop("cik", f"000000{event_id}"),
        holder_name=kwargs.pop("holder_name", f"Holder {event_id}"),
        event_type=event_type,
        direction=kwargs.pop("direction", "bullish"),
        title=kwargs.pop("title", "Institutional Activity"),
        summary=kwargs.pop("summary", "Reported institutional activity."),
        filing_date=kwargs.pop("filing_date", date(2026, 6, 30)),
        report_year=kwargs.pop("report_year", 2026),
        report_quarter=kwargs.pop("report_quarter", 2),
        reported_value_usd=kwargs.pop("reported_value_usd", 150_000_000.0),
        value_delta_usd=kwargs.pop("value_delta_usd", 50_000_000.0),
        holder_breadth=kwargs.pop("holder_breadth", 1),
        materiality_score=kwargs.pop("materiality_score", 85.0),
        confirmation_score=kwargs.pop("confirmation_score", 0.0),
        feed_visible=kwargs.pop("feed_visible", True),
        freshness_status=kwargs.pop("freshness_status", "active"),
        **kwargs,
    )


def _query(db: Session, *, side: str):
    return _query_unified_signals(
        db=db,
        mode="institutional",
        sort="recent",
        limit=20,
        offset=0,
        baseline_days=365,
        congress_recent_days=180,
        insider_recent_days=60,
        congress_min_baseline_count=3,
        insider_min_baseline_count=3,
        congress_multiple=1.75,
        insider_multiple=1.5,
        congress_min_amount=10_000,
        insider_min_amount=10_000,
        min_smart_score=None,
        side=side,
        symbol=None,
        confirmation_band="all",
        confirmation_direction="all",
        min_confirmation_sources=None,
        multi_source_only=False,
        include_institutional=True,
        institutional_lookback_days=365,
        institutional_direction="all",
        institutional_min_value=None,
    )


def test_institutional_signals_side_filter_maps_increases_to_buy_and_reductions_to_sell(monkeypatch):
    monkeypatch.setattr(signals_module, "get_confirmation_metrics_for_symbols", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(signals_module, "get_slim_confirmation_score_bundles_for_tickers", lambda *_args, **_kwargs: {})
    with _db() as db:
        db.add_all(
            [
                _institutional_event(1, "institutional_accumulation", direction="bullish", value_delta_usd=75_000_000.0),
                _institutional_event(2, "major_holder_reduction", direction="bearish", value_delta_usd=-50_000_000.0),
                _institutional_event(3, "major_holder_exit", direction="bearish", value_delta_usd=-120_000_000.0),
                _institutional_event(4, "new_institutional_position", direction="bullish", value_delta_usd=25_000_000.0),
            ]
        )
        db.commit()

        buy_items = _query(db, side="buy")
        sell_items = _query(db, side="sell")

    assert {item.trade_type for item in buy_items} == {"Reported Increase", "Reported New Position"}
    assert {item.trade_type for item in sell_items} == {"Reported Reduction", "Reported Exit"}


def test_institutional_delta_uses_real_value_delta_not_materiality_fallback():
    missing_delta = _institutional_event(10, "institutional_accumulation", reported_value_usd=150_000_000.0, value_delta_usd=None, materiality_score=100.0)
    derived_delta = _institutional_event(11, "major_holder_reduction", reported_value_usd=50_000_000.0, value_delta_usd=-50_000_000.0, materiality_score=100.0)

    assert _institutional_unusual_multiple(missing_delta, {}) == 1.0
    assert _institutional_unusual_multiple(derived_delta, {}) == 0.5
