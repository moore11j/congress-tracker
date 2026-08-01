from __future__ import annotations

import json
from datetime import date, datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import app.jobs.plan_fundamentals_coverage_expansion as planner
from app.db import Base
from app.models import DataEnrichmentJob, Event, InsiderTransactionNormalized, PriceCache, TickerFinancialsCache


def _session():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    Base.metadata.create_all(bind=engine)
    return SessionLocal


def _event(symbol: str, event_id: int) -> Event:
    return Event(
        id=event_id,
        event_type="congress_trade",
        ts=datetime(2026, 7, 1, tzinfo=timezone.utc),
        symbol=symbol,
        source="test",
        impact_score=0.0,
        payload_json=json.dumps({"transactionType": "Purchase"}),
        trade_type="purchase",
        amount_max=10000,
    )


def _insider(symbol: str, row_id: int, *, director: bool = False, officer: bool = False) -> InsiderTransactionNormalized:
    return InsiderTransactionNormalized(
        id=row_id,
        accession_number=f"0000000000-26-{row_id:06d}",
        normalized_hash=f"hash-{row_id}",
        ticker_raw=symbol,
        ticker_normalized=symbol,
        reporting_owner_cik=f"owner-{row_id}",
        reporting_owner_name="Owner",
        issuer_cik=f"issuer-{row_id}",
        issuer_name=symbol,
        transaction_date=date(2026, 6, 30),
        filing_date=date(2026, 7, 1),
        transaction_type_normalized="open_market_purchase",
        value=5000,
        is_duplicate=False,
        is_director=director,
        is_officer=officer,
    )


def _prices(symbol: str, count: int, *, dollar_volume: float = 2_000_000.0) -> list[PriceCache]:
    return [
        PriceCache(
            symbol=symbol,
            date=f"2026-01-{index + 1:02d}",
            close=100.0 + index,
            adjusted_close=100.0 + index,
            dollar_volume=dollar_volume,
        )
        for index in range(count)
    ]


def test_plan_prioritizes_missing_financial_cache_with_price_coverage(monkeypatch):
    SessionLocal = _session()
    db = SessionLocal()
    try:
        db.add(_event("AAPL", 1))
        db.add(_event("AAPL", 2))
        db.add(_insider("AAPL", 1, director=True))
        db.add(_insider("MSFT", 2))
        db.add_all(_prices("AAPL", 65))
        db.add_all(_prices("MSFT", 65))
        db.add(
            TickerFinancialsCache(
                symbol="MSFT",
                status="ok",
                payload_json="{}",
                fetched_at=datetime(2026, 8, 1, tzinfo=timezone.utc),
            )
        )
        db.commit()
    finally:
        db.close()
    monkeypatch.setattr(planner, "SessionLocal", SessionLocal)

    result = planner.plan_fundamentals_coverage_expansion(
        start_date=date(2026, 1, 1),
        end_date=date(2026, 12, 31),
        min_adjusted_price_rows=60,
        limit=10,
    )

    assert result["coverage"]["strategy_signal_symbols"] == 2
    assert result["coverage"]["symbols_missing_financial_cache"] == 1
    assert result["symbols"][0]["symbol"] == "AAPL"
    assert result["symbols"][0]["avg_dollar_volume"] == 2_000_000.0
    assert result["symbols"][0]["congress_purchases"] == 2
    assert result["symbols"][0]["insider_director_purchases"] == 1
    assert result["batches"] == [["AAPL"]]


def test_plan_excludes_active_financial_jobs_unless_requested(monkeypatch):
    SessionLocal = _session()
    db = SessionLocal()
    try:
        db.add(_insider("NVDA", 1))
        db.add_all(_prices("NVDA", 65))
        db.add(
            DataEnrichmentJob(
                job_type="ticker_financials",
                symbol="NVDA",
                dedupe_key="ticker_financials:NVDA",
                priority=100,
                status="queued",
                source="test",
                reason="test",
                next_run_at=datetime(2026, 8, 1, tzinfo=timezone.utc),
            )
        )
        db.commit()
    finally:
        db.close()
    monkeypatch.setattr(planner, "SessionLocal", SessionLocal)

    result = planner.plan_fundamentals_coverage_expansion(
        start_date=date(2026, 1, 1),
        end_date=date(2026, 12, 31),
        min_adjusted_price_rows=60,
        limit=10,
    )
    assert result["symbols"] == []
    assert result["coverage"]["symbols_with_active_financial_jobs"] == 1

    with_queued = planner.plan_fundamentals_coverage_expansion(
        start_date=date(2026, 1, 1),
        end_date=date(2026, 12, 31),
        min_adjusted_price_rows=60,
        include_queued=True,
        limit=10,
    )
    assert with_queued["symbols"][0]["symbol"] == "NVDA"


def test_plan_excludes_illiquid_symbols_by_default(monkeypatch):
    SessionLocal = _session()
    db = SessionLocal()
    try:
        db.add(_insider("LOWV", 1))
        db.add_all(_prices("LOWV", 65, dollar_volume=250_000.0))
        db.commit()
    finally:
        db.close()
    monkeypatch.setattr(planner, "SessionLocal", SessionLocal)

    result = planner.plan_fundamentals_coverage_expansion(
        start_date=date(2026, 1, 1),
        end_date=date(2026, 12, 31),
        min_adjusted_price_rows=60,
        limit=10,
    )

    assert result["symbols"] == []
    assert result["coverage"]["missing_cache_with_min_liquidity"] == 0
    assert result["coverage"]["missing_cache_without_min_liquidity"] == 1

    relaxed = planner.plan_fundamentals_coverage_expansion(
        start_date=date(2026, 1, 1),
        end_date=date(2026, 12, 31),
        min_adjusted_price_rows=60,
        min_avg_dollar_volume=0,
        limit=10,
    )

    assert relaxed["symbols"][0]["symbol"] == "LOWV"


def test_enqueue_fundamentals_coverage_batch_uses_planned_uncached_symbols(monkeypatch):
    plan = {
        "run_timestamp": "2026-08-01T00:00:00+00:00",
        "parameters": {
            "start_date": "2026-01-01",
            "end_date": "2026-12-31",
            "min_adjusted_price_rows": 60,
            "min_avg_dollar_volume": 1_000_000,
            "include_cached": False,
            "include_queued": False,
            "limit": 2,
            "batch_size": 2,
        },
        "coverage": {"eligible_selected": 2},
        "symbols": [{"symbol": "AAPL"}, {"symbol": "MSFT"}],
    }
    calls = []

    def fake_plan(**kwargs):
        calls.append(("plan", kwargs))
        return plan

    def fake_enqueue(**kwargs):
        calls.append(("enqueue", kwargs))
        return kwargs["symbol"] == "AAPL"

    monkeypatch.setattr(planner, "plan_fundamentals_coverage_expansion", fake_plan)
    monkeypatch.setattr(planner, "enqueue_data_enrichment_job", fake_enqueue)

    result = planner.enqueue_fundamentals_coverage_batch(
        start_date=date(2026, 1, 1),
        end_date=date(2026, 12, 31),
        limit=2,
        batch_size=2,
        priority=70,
        reason="test_reason",
    )

    assert result["mode"] == "apply_enqueue"
    assert result["selected_symbols"] == ["AAPL", "MSFT"]
    assert result["enqueued_symbols"] == ["AAPL"]
    assert result["skipped_symbols"] == ["MSFT"]
    assert calls[0][0] == "plan"
    assert calls[1][1]["job_type"] == "ticker_financials"
    assert calls[1][1]["symbol"] == "AAPL"
    assert calls[1][1]["priority"] == 70
    assert calls[2][1]["symbol"] == "MSFT"
    assert calls[2][1]["priority"] == 71


def test_fundamentals_coverage_queue_status_reports_jobs_and_cache(monkeypatch):
    SessionLocal = _session()
    db = SessionLocal()
    try:
        db.add(
            DataEnrichmentJob(
                job_type="ticker_financials",
                symbol="AAPL",
                dedupe_key="ticker_financials:AAPL",
                priority=70,
                status="done",
                source="strategy_research",
                reason="batch1",
                next_run_at=datetime(2026, 8, 1, tzinfo=timezone.utc),
            )
        )
        db.add(
            DataEnrichmentJob(
                job_type="ticker_financials",
                symbol="MSFT",
                dedupe_key="ticker_financials:MSFT",
                priority=71,
                status="queued",
                source="strategy_research",
                reason="batch1",
                next_run_at=datetime(2026, 8, 1, tzinfo=timezone.utc),
            )
        )
        db.add(
            TickerFinancialsCache(
                symbol="AAPL",
                status="ok",
                payload_json="{}",
                fetched_at=datetime(2026, 8, 1, tzinfo=timezone.utc),
            )
        )
        db.commit()
    finally:
        db.close()
    monkeypatch.setattr(planner, "SessionLocal", SessionLocal)

    result = planner.fundamentals_coverage_queue_status(reason="batch1")

    assert result["job_status_counts"] == {"done": 1, "queued": 1}
    assert result["cache_status_counts"] == {"ok": 1}
    assert result["cache_row_count"] == 1
    assert [job["symbol"] for job in result["jobs"]] == ["AAPL", "MSFT"]


def test_process_fundamentals_coverage_batch_filters_to_reason_and_symbols(monkeypatch):
    calls = []

    def fake_process(**kwargs):
        calls.append(("process", kwargs))
        return {"processed": 2, "succeeded": 2, "failed": 0, "skipped": 0}

    def fake_status(**kwargs):
        calls.append(("status", kwargs))
        return {"job_status_counts": {"done": 2}, "cache_row_count": 2}

    monkeypatch.setattr(planner, "process_data_enrichment_jobs", fake_process)
    monkeypatch.setattr(planner, "fundamentals_coverage_queue_status", fake_status)

    result = planner.process_fundamentals_coverage_batch(
        reason="batch1",
        symbols=["aapl", "MSFT"],
        limit=2,
        max_seconds=30,
    )

    assert result["mode"] == "process_batch"
    assert result["process_result"]["processed"] == 2
    assert calls[0] == (
        "process",
        {
            "limit": 2,
            "max_seconds": 30,
            "job_type": "ticker_financials",
            "reason": "batch1",
            "symbols": ["AAPL", "MSFT"],
        },
    )
    assert calls[1] == ("status", {"reason": "batch1", "symbols": ["AAPL", "MSFT"]})
