from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.db import Base
from app.models import Event
from app.services.confirmation_metrics import get_confirmation_metrics_for_symbols


def _event(
    *,
    event_id: int,
    symbol: str,
    event_type: str,
    trade_type: str,
    event_date: datetime,
):
    return Event(
        id=event_id,
        event_type=event_type,
        ts=event_date,
        event_date=event_date,
        symbol=symbol,
        source="test",
        payload_json=json.dumps({"symbol": symbol}),
        trade_type=trade_type,
        amount_min=1_000,
        amount_max=5_000,
    )


def test_confirmation_ignores_non_visible_insider_rows_in_30d_window():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)

    now = datetime.now(timezone.utc)
    inside_window = now - timedelta(days=5)

    with Session(engine) as db:
        # Mirror the CRM-like mismatch: insider rows exist but all are non-market/non-visible.
        db.add_all(
            [
                _event(
                    event_id=idx,
                    symbol="CRM",
                    event_type="insider_trade",
                    trade_type="award",
                    event_date=inside_window,
                )
                for idx in range(1, 39)
            ]
        )
        db.commit()

        metrics = get_confirmation_metrics_for_symbols(db, ["CRM"], window_days=30)["CRM"]

        assert metrics.insider_trade_count_30d == 0
        assert metrics.insider_buy_count_30d == 0
        assert metrics.insider_sell_count_30d == 0
        assert metrics.insider_active_30d is False
        assert metrics.cross_source_confirmed_30d is False


def test_confirmation_counts_cross_source_when_both_visible_sources_are_active():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)

    now = datetime.now(timezone.utc)
    inside_window = now - timedelta(days=3)

    with Session(engine) as db:
        db.add_all(
            [
                _event(
                    event_id=1,
                    symbol="CRM",
                    event_type="congress_trade",
                    trade_type="purchase",
                    event_date=inside_window,
                ),
                _event(
                    event_id=2,
                    symbol="CRM",
                    event_type="insider_trade",
                    trade_type="sale",
                    event_date=inside_window,
                ),
            ]
        )
        db.commit()

        metrics = get_confirmation_metrics_for_symbols(db, ["CRM"], window_days=30)["CRM"]

        assert metrics.congress_trade_count_30d == 1
        assert metrics.insider_trade_count_30d == 1
        assert metrics.congress_active_30d is True
        assert metrics.insider_active_30d is True
        assert metrics.cross_source_confirmed_30d is True
