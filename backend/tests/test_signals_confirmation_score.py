from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone

from sqlalchemy import create_engine, event as sqlalchemy_event
from sqlalchemy.orm import Session

from app.db import Base
from app.schemas import SignalFreshnessOut, WhyNowOut, UnifiedSignalOut
from app.models import Event, TradeOutcome
from app.routers.signals import _apply_confirmation_summary, _query_unified_signals


def _event(
    *,
    event_id: int,
    symbol: str,
    event_date: datetime,
    amount_max: int,
) -> Event:
    return Event(
        id=event_id,
        event_type="congress_trade",
        ts=event_date,
        event_date=event_date,
        symbol=symbol,
        source="test",
        payload_json=json.dumps({"symbol": symbol}),
        member_name="Test Member",
        member_bioguide_id=f"{symbol}1",
        chamber="House",
        party="I",
        trade_type="purchase",
        amount_min=100,
        amount_max=amount_max,
    )


def _seed_unusual_congress_rows(db: Session) -> None:
    now = datetime.now(timezone.utc)
    event_id = 1
    for symbol in ("AAA", "BBB"):
        for days_back in (120, 100, 80):
            db.add(
                _event(
                    event_id=event_id,
                    symbol=symbol,
                    event_date=now - timedelta(days=days_back),
                    amount_max=100,
                )
            )
            event_id += 1
        db.add(
            _event(
                event_id=event_id,
                symbol=symbol,
                event_date=now - timedelta(days=3),
                amount_max=1_000,
            )
        )
        event_id += 1
    db.commit()


def test_unified_signals_can_filter_and_sort_by_confirmation_score(monkeypatch):
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)

    def fake_confirmation_bundles(_db, symbols, *, lookback_days=30):
        assert lookback_days == 30
        assert set(symbols) == {"AAA", "BBB"}
        return {
            "AAA": {
                "confirmation_score": 82,
                "confirmation_band": "exceptional",
                "confirmation_direction": "bullish",
                "confirmation_status": "2-source bullish confirmation",
                "confirmation_source_count": 2,
                "confirmation_explanation": "Congress buy-skewed",
                "is_multi_source": True,
                "signal_freshness": {
                    "ticker": "AAA",
                    "lookback_days": 30,
                    "freshness_score": 86,
                    "freshness_state": "fresh",
                    "freshness_label": "Fresh multi-source setup",
                    "explanation": "Recent Congress activity and smart signal remain tightly clustered.",
                    "timing": {
                        "freshest_source_days": 2,
                        "stalest_active_source_days": 6,
                        "active_source_count": 2,
                        "overlap_window_days": 4,
                    },
                },
            },
            "BBB": {
                "confirmation_score": 34,
                "confirmation_band": "weak",
                "confirmation_direction": "bullish",
                "confirmation_status": "Single-source bullish",
                "confirmation_source_count": 1,
                "confirmation_explanation": "Congress buy-skewed",
                "is_multi_source": False,
                "signal_freshness": {
                    "ticker": "BBB",
                    "lookback_days": 30,
                    "freshness_score": 62,
                    "freshness_state": "early",
                    "freshness_label": "Early setup",
                    "explanation": "A single recent source is active, but broader confirmation is still limited.",
                    "timing": {
                        "freshest_source_days": 3,
                        "stalest_active_source_days": 3,
                        "active_source_count": 1,
                        "overlap_window_days": 0,
                    },
                },
            },
        }

    monkeypatch.setattr("app.routers.signals.get_slim_confirmation_score_bundles_for_tickers", fake_confirmation_bundles)

    with Session(engine) as db:
        _seed_unusual_congress_rows(db)

        items = _query_unified_signals(
            db=db,
            mode="all",
            sort="confirmation",
            limit=10,
            offset=0,
            baseline_days=365,
            congress_recent_days=30,
            insider_recent_days=30,
            congress_min_baseline_count=3,
            insider_min_baseline_count=3,
            congress_multiple=1.5,
            insider_multiple=1.5,
            congress_min_amount=0,
            insider_min_amount=0,
            min_smart_score=None,
            side="all",
            symbol=None,
            confirmation_band="strong_plus",
            confirmation_direction="bullish",
            min_confirmation_sources=2,
        )

    assert [item.symbol for item in items] == ["AAA"]
    assert items[0].confirmation_score == 82
    assert items[0].confirmation_band == "exceptional"
    assert items[0].confirmation_source_count == 2
    assert items[0].is_multi_source is True
    assert items[0].signal_freshness is not None
    assert items[0].signal_freshness.freshness_state == "fresh"
    assert items[0].signal_freshness.freshness_score == 86


def test_unified_signals_include_normalized_price_and_pnl_fields():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)

    now = datetime.now(timezone.utc)
    with Session(engine) as db:
        for index, days_back in enumerate((120, 100, 80), start=1):
            db.add(
                _event(
                    event_id=index,
                    symbol="AAPL",
                    event_date=now - timedelta(days=days_back),
                    amount_max=100,
                )
            )
        db.add(
            Event(
                id=10,
                event_type="congress_trade",
                ts=now - timedelta(days=3),
                event_date=now - timedelta(days=3),
                symbol="AAPL",
                source="test",
                payload_json=json.dumps({"symbol": "AAPL", "estimated_price": 189.42}),
                member_name="Test Member",
                member_bioguide_id="AAPL1",
                chamber="House",
                party="I",
                trade_type="purchase",
                amount_min=100,
                amount_max=1_000,
            )
        )
        db.add(
            TradeOutcome(
                event_id=10,
                member_id="AAPL1",
                member_name="Test Member",
                symbol="AAPL",
                trade_type="purchase",
                source="test",
                trade_date=date.today(),
                entry_price=188.0,
                current_price=200.0,
                return_pct=6.38,
                benchmark_symbol="^GSPC",
                scoring_status="ok",
                methodology_version="congress_v1",
            )
        )
        db.commit()

        items = _query_unified_signals(
            db=db,
            mode="all",
            sort="smart",
            limit=10,
            offset=0,
            baseline_days=365,
            congress_recent_days=30,
            insider_recent_days=30,
            congress_min_baseline_count=3,
            insider_min_baseline_count=3,
            congress_multiple=1.5,
            insider_multiple=1.5,
            congress_min_amount=0,
            insider_min_amount=0,
            min_smart_score=None,
            side="all",
            symbol="AAPL",
        )

    assert len(items) == 1
    assert items[0].price == 189.42
    assert items[0].estimated_price == 189.42
    assert items[0].current_price == 200.0
    assert items[0].pnl_pct == 6.38
    assert items[0].pnlPct == 6.38


def test_unified_recent_symbol_signals_are_backend_scoped_and_limited():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)

    now = datetime.now(timezone.utc)
    with Session(engine) as db:
        event_id = 100
        for symbol in ("INTC", "AMD"):
            for days_back in (120, 100, 80):
                db.add(_event(event_id=event_id, symbol=symbol, event_date=now - timedelta(days=days_back), amount_max=100))
                event_id += 1
            for days_back in range(1, 26):
                db.add(_event(event_id=event_id, symbol=symbol, event_date=now - timedelta(days=days_back), amount_max=1_000))
                event_id += 1
        db.commit()

    captured_signal_sql: list[tuple[str, object]] = []

    def capture_sql(_conn, _cursor, statement, parameters, _context, _executemany):
        if "UNION ALL" in statement and " LIMIT " in statement:
            captured_signal_sql.append((statement, parameters))

    sqlalchemy_event.listen(engine, "before_cursor_execute", capture_sql)
    try:
        with Session(engine) as db:
            items = _query_unified_signals(
                db=db,
                mode="all",
                sort="recent",
                limit=20,
                offset=0,
                baseline_days=365,
                congress_recent_days=30,
                insider_recent_days=30,
                congress_min_baseline_count=3,
                insider_min_baseline_count=3,
                congress_multiple=1.0,
                insider_multiple=1.0,
                congress_min_amount=0,
                insider_min_amount=0,
                min_smart_score=None,
                side="all",
                symbol="INTC",
            )
    finally:
        sqlalchemy_event.remove(engine, "before_cursor_execute", capture_sql)

    assert len(items) == 20
    assert {item.symbol for item in items} == {"INTC"}
    assert [item.ts for item in items] == sorted([item.ts for item in items], reverse=True)
    assert captured_signal_sql
    statement, parameters = captured_signal_sql[0]
    param_values = list(parameters.values()) if isinstance(parameters, dict) else list(parameters)
    assert "upper(events.symbol) IN" in statement
    assert "events.ts >= " in statement
    assert "ORDER BY anon_1.ts DESC" in statement
    assert " LIMIT " in statement
    assert 20 in param_values
    assert "INTC" in param_values


def test_apply_confirmation_summary_coerces_nested_models_after_assignment():
    item = UnifiedSignalOut(
        kind="congress",
        event_id=1,
        ts=datetime.now(timezone.utc),
        symbol="AAPL",
        who="Test Member",
        position=None,
        reporting_cik=None,
        reportingCik=None,
        member_bioguide_id="A000001",
        party="I",
        chamber="House",
        trade_type="purchase",
        amount_min=100,
        amount_max=1_000,
        baseline_median_amount_max=100,
        baseline_count=3,
        unusual_multiple=10.0,
        smart_score=80,
        smart_band="high",
        source="test",
    )

    _apply_confirmation_summary(
        item,
        {
            "confirmation_score": 82,
            "confirmation_band": "exceptional",
            "confirmation_direction": "bullish",
            "confirmation_status": "2-source bullish confirmation",
            "confirmation_source_count": 2,
            "confirmation_explanation": "Congress buy-skewed",
            "is_multi_source": True,
            "why_now": {
                "ticker": "AAPL",
                "lookback_days": 30,
                "state": "strong",
                "headline": "AAPL has aligned multi-source confirmation.",
                "evidence": ["2-source bullish confirmation", "Congress buy-skewed"],
                "caveat": None,
            },
            "signal_freshness": {
                "ticker": "AAPL",
                "lookback_days": 30,
                "freshness_score": 86,
                "freshness_state": "fresh",
                "freshness_label": "Fresh multi-source setup",
                "explanation": "Recent signals remain tightly clustered.",
                "timing": {
                    "freshest_source_days": 2,
                    "stalest_active_source_days": 6,
                    "active_source_count": 2,
                    "overlap_window_days": 4,
                },
            },
        },
    )

    assert isinstance(item.why_now, WhyNowOut)
    assert item.why_now.state == "strong"
    assert isinstance(item.signal_freshness, SignalFreshnessOut)
    assert item.signal_freshness.freshness_state == "fresh"
