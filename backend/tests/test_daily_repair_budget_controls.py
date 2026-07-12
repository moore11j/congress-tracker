from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from types import SimpleNamespace

from sqlalchemy import create_engine, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

from app import compute_trade_outcomes as compute_module
from app import ingest_run
from app.db import Base
from app.models import Event, TradeOutcome
from app.services import member_performance
from app.services.provider_usage import (
    log_provider_budget_summary,
    provider_budget_log_summary,
    record_fallback,
    reset_provider_usage,
)


def _congress_event(event_id: int, symbol: str = "AAPL") -> SimpleNamespace:
    ts = datetime(2026, 5, 1, tzinfo=timezone.utc)
    return SimpleNamespace(
        id=event_id,
        event_type="congress_trade",
        symbol=symbol,
        trade_type="purchase",
        payload_json={"trade_date": "2026-05-01", "trade_type": "purchase", "symbol": symbol},
        event_date=ts,
        ts=ts,
        source="test",
        amount_min=1,
        amount_max=2,
        member_bioguide_id="M000001",
        member_name="Budget Test",
    )


def test_compute_trade_outcomes_stops_after_provider_budget_exceeded(monkeypatch):
    calls: list[str] = []

    def fake_entry_lookup(_db, symbol, _trade_date, _price_memo):
        calls.append(symbol)
        return {
            "close": None,
            "status": member_performance.PROVIDER_BUDGET_STATUS,
            "error": "provider_budget_exceeded",
            "symbol": symbol,
        }

    def fail_quote_lookup(*_args, **_kwargs):
        raise AssertionError("quote lookup should stop after provider budget exhaustion")

    monkeypatch.setattr(member_performance, "_entry_price_for_congress_event", fake_entry_lookup)
    monkeypatch.setattr(member_performance, "get_current_prices_meta_db", fail_quote_lookup)

    rows = member_performance.compute_congress_trade_outcomes(
        db=SimpleNamespace(),
        events=[
            _congress_event(1, "AAPL"),
            _congress_event(2, "MSFT"),
            _congress_event(3, "NVDA"),
        ],
        benchmark_symbol="SPY",
    )

    assert calls == ["AAPL"]
    assert [row["scoring_status"] for row in rows] == [
        member_performance.PROVIDER_BUDGET_STATUS,
        member_performance.PROVIDER_BUDGET_STATUS,
        member_performance.PROVIDER_BUDGET_STATUS,
    ]


def test_run_compute_marks_remaining_work_retry_later_when_max_seconds_reached(monkeypatch):
    engine = create_engine("sqlite:///:memory:", future=True)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    Base.metadata.create_all(bind=engine, tables=[Event.__table__, TradeOutcome.__table__])

    event_ts = datetime(2026, 5, 1, tzinfo=timezone.utc)
    with SessionLocal() as db:
        db.add_all(
            [
                Event(
                    id=9301,
                    event_type="congress_trade",
                    ts=event_ts,
                    event_date=event_ts,
                    symbol="AAPL",
                    source="test",
                    payload_json=json.dumps({"trade_date": "2026-05-01", "trade_type": "purchase", "symbol": "AAPL"}),
                    member_name="Budget Test",
                    member_bioguide_id="M000001",
                    trade_type="purchase",
                    transaction_type="purchase",
                    amount_min=1,
                    amount_max=2,
                ),
                Event(
                    id=9302,
                    event_type="congress_trade",
                    ts=event_ts,
                    event_date=event_ts,
                    symbol="MSFT",
                    source="test",
                    payload_json=json.dumps({"trade_date": "2026-05-01", "trade_type": "purchase", "symbol": "MSFT"}),
                    member_name="Budget Test",
                    member_bioguide_id="M000001",
                    trade_type="purchase",
                    transaction_type="purchase",
                    amount_min=1,
                    amount_max=2,
                ),
            ]
        )
        db.commit()

    monkeypatch.setattr(compute_module, "engine", engine)
    monkeypatch.setattr(compute_module, "SessionLocal", SessionLocal)
    monkeypatch.setattr(compute_module, "ensure_event_columns", lambda: None)
    monkeypatch.setattr(compute_module, "ensure_trade_outcomes_amount_bigint", lambda: None)

    report = compute_module.run_compute(
        replace=False,
        limit=None,
        member_id=None,
        event_type="congress_trade",
        benchmark_symbol="SPY",
        lookback_days=None,
        trade_date_after=None,
        only_missing=True,
        retry_failed_status=None,
        retry_failed_statuses=None,
        max_seconds=0,
        max_price_lookups=10,
    )

    assert report["status"] == "partial"
    assert report["partial_reason"] == "max_seconds_exceeded"
    assert report["retry_later"] == 2
    with SessionLocal() as db:
        statuses = {
            row.event_id: row.scoring_status
            for row in db.execute(select(TradeOutcome)).scalars().all()
        }
    assert statuses == {9301: member_performance.RETRY_LATER_STATUS, 9302: member_performance.RETRY_LATER_STATUS}


def test_trade_outcome_persist_retries_duplicate_event_id_race(tmp_path, monkeypatch):
    engine = create_engine(f"sqlite:///{tmp_path / 'race.db'}", future=True)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    Base.metadata.create_all(bind=engine, tables=[TradeOutcome.__table__])

    event = SimpleNamespace(id=9401, event_type="congress_trade")
    outcome = {
        "event_id": 9401,
        "member_id": "M000001",
        "member_name": "Race Test",
        "symbol": "AAPL",
        "trade_type": "purchase",
        "source": "test",
        "trade_date": "2026-05-01",
        "entry_price": 100.0,
        "entry_price_date": "2026-05-01",
        "current_price": 110.0,
        "current_price_date": "2026-05-02",
        "benchmark_symbol": "SPY",
        "benchmark_entry_price": 5000.0,
        "benchmark_current_price": 5050.0,
        "return_pct": 10.0,
        "benchmark_return_pct": 1.0,
        "alpha_pct": 9.0,
        "holding_days": 1,
        "amount_min": 1000,
        "amount_max": 1500,
        "scoring_status": "ok",
        "scoring_error": None,
        "methodology_version": "congress_v1",
    }

    with SessionLocal() as db:
        original_commit = db.commit
        commit_calls = 0

        def commit_with_race():
            nonlocal commit_calls
            commit_calls += 1
            if commit_calls == 1:
                with SessionLocal() as race_db:
                    race_db.add(TradeOutcome(event_id=event.id, scoring_status="ok"))
                    race_db.commit()
                raise IntegrityError(
                    "duplicate key value violates unique constraint ix_trade_outcomes_event_id",
                    {},
                    Exception('duplicate key value violates unique constraint "ix_trade_outcomes_event_id"'),
                )
            original_commit()

        monkeypatch.setattr(db, "commit", commit_with_race)

        report = compute_module._persist_trade_outcomes(
            db,
            eligible_events=[event],
            outcome_by_event_id={event.id: outcome},
            existing_by_event_id={},
            replace=True,
            retry_status_set=set(),
            benchmark_symbol="SPY",
        )

    assert report["commit_retry_count"] == 1
    assert report["inserted"] == 0
    assert report["updated"] == 1
    with SessionLocal() as db:
        row = db.execute(select(TradeOutcome).where(TradeOutcome.event_id == event.id)).scalar_one()
    assert row.symbol == "AAPL"
    assert row.return_pct == 10.0


def test_daily_repair_stops_cleanly_when_price_lookup_budget_is_exhausted(monkeypatch):
    calls: list[dict] = []

    def fake_run_compute(**kwargs):
        calls.append(kwargs)
        return {
            "event_type": kwargs["event_type"],
            "status": "partial",
            "partial_reason": "price_lookup_budget_exceeded",
            "scanned": 10,
            "eligible": 10,
            "inserted": 0,
            "updated": 2,
            "skipped": 8,
            "status_counts": {"retry_later": 8},
            "skipped_budget": 0,
            "retry_later": 8,
            "price_lookup_attempts": kwargs["max_price_lookups"],
        }

    monkeypatch.setenv("DAILY_REPAIR_MAX_EVENTS", "7")
    monkeypatch.setenv("DAILY_REPAIR_MAX_SECONDS", "240")
    monkeypatch.setenv("DAILY_REPAIR_PRICE_LOOKUP_BUDGET", "5")
    monkeypatch.delenv("OUTCOME_REPAIR_LIMIT", raising=False)
    monkeypatch.setattr(ingest_run, "run_compute", fake_run_compute)
    monkeypatch.setattr(
        ingest_run,
        "_daily_outcome_coverage_report",
        lambda *, lookback_days: {"lookback_days": lookback_days, "failed_statuses": {}},
    )
    monkeypatch.setattr(
        ingest_run,
        "log_provider_budget_summary",
        lambda *, reset=False: [{"category": "price:eod", "count": 5, "suppressed": 3}],
    )

    report = ingest_run._run_daily_outcome_repair()

    assert len(calls) == 1
    assert calls[0]["event_type"] == "congress_trade"
    assert calls[0]["limit"] == 7
    assert calls[0]["max_price_lookups"] == 5
    assert report["status"] == "partial"
    assert report["stages_run"] == ["congress_trade"]
    assert report["insider"]["partial_reason"] == "price_lookup_budget_exceeded"
    assert report["retry_later"] == 8


def test_provider_budget_fallback_logs_are_rate_limited_and_summarized(monkeypatch, caplog):
    monkeypatch.setenv("FMP_PERSIST_USAGE_EVENTS", "0")
    monkeypatch.setenv("FMP_PROVIDER_BUDGET_LOG_LIMIT_PER_CATEGORY", "2")
    reset_provider_usage()
    caplog.set_level(logging.INFO, logger="app.services.provider_usage")

    for index in range(5):
        record_fallback(
            category="price:eod",
            symbol=f"SYM{index}",
            reason="provider_budget_exceeded",
        )

    fallback_logs = [
        record
        for record in caplog.records
        if record.name == "app.services.provider_usage" and record.message.startswith("provider_fallback")
    ]
    assert len(fallback_logs) == 2

    summary = provider_budget_log_summary()
    assert summary[0]["category"] == "price:eod"
    assert summary[0]["count"] == 5
    assert summary[0]["suppressed"] == 3

    log_provider_budget_summary(reset=True)
    assert "provider_budget_summary category=price:eod" in caplog.text
    assert provider_budget_log_summary() == []
