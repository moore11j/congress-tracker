from __future__ import annotations

from datetime import date

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from app.db import Base, ensure_strategy_storage_schema
from app.models import (
    StrategyBacktestRun,
    StrategyCurrentHolding,
    StrategyDefinition,
    StrategyEquityCurvePoint,
    StrategyHoldingRow,
    StrategyHoldingsSnapshot,
    StrategyPerformanceSnapshot,
)
from app.services.strategy_refresh import persist_candidate_strategy_artifact
from app.strategy_research.candidate_strategy_artifacts import CandidateStrategyArtifact
from app.strategy_research.candidate_strategy_validation import CandidateDefinition
from app.strategy_research.congress_buys import Lot, PriceBar, Signal


def _session():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    Base.metadata.create_all(bind=engine)
    ensure_strategy_storage_schema(engine)
    return SessionLocal


def _artifact() -> CandidateStrategyArtifact:
    candidate = CandidateDefinition(
        slug="congress-macd-bullish-90d",
        name="Congress + MACD Bullish",
        strategy_kind="technical",
        universe_source="fundamentals_snapshots",
        source="congress",
        rule="macd_bullish",
        hold_days=90,
    )
    signal = Signal(
        event_id=101,
        symbol="NVDA",
        disclosure_date=date(2026, 1, 2),
        raw_entry_date=date(2026, 1, 3),
        amount_min=1000,
        amount_max=15000,
        member_name="Example Member",
        member_bioguide_id="X001",
        chamber="House",
        party="D",
        source_filing_id="filing-101",
        source_document_url="https://example.test/filing",
    )
    open_signal = Signal(
        event_id=102,
        symbol="MSFT",
        disclosure_date=date(2026, 3, 30),
        raw_entry_date=date(2026, 3, 31),
        amount_min=1000,
        amount_max=15000,
        member_name="Example Member",
        member_bioguide_id="X001",
        chamber="House",
        party="D",
        source_filing_id="filing-102",
        source_document_url="https://example.test/filing-2",
    )
    lot = Lot(
        signal=signal,
        entry_date=date(2026, 1, 5),
        exit_date=date(2026, 4, 6),
        entry_price=100.0,
        exit_price=115.0,
        gross_return=0.15,
        net_return=0.149,
    )
    return CandidateStrategyArtifact(
        candidate=candidate,
        metadata={
            "strategy_name": candidate.name,
            "strategy_slug": candidate.slug,
            "strategy_kind": candidate.strategy_kind,
            "methodology_version": "candidate_strategy_artifact_v1",
            "universe_source": candidate.universe_source,
            "snapshot_source_kind": "ticker_financials_cache_statement_proxy",
            "min_snapshots_per_symbol": 1,
            "benchmark": "SPY",
            "weighting": "equal",
            "rebalance_frequency": "event",
            "execution_timing": "first trading day strictly after public disclosure date",
            "slippage_bps_per_side": 5.0,
            "fee_bps_per_side": 0.0,
            "require_adjusted_prices": True,
            "data_state": "test",
        },
        performance={
            "status": "ok",
            "start_date": "2026-01-05",
            "end_date": "2026-04-06",
            "hold_days": 90,
            "lots": 1,
            "independent_signals": 1,
            "trade_count": 2,
            "total_return_pct": 15.0,
            "cagr_pct": 78.0,
            "benchmark_total_return_pct": 5.0,
            "benchmark_cagr_pct": 21.0,
            "alpha_cagr_pct": 57.0,
            "beta": 1.1,
            "sharpe": 1.2,
            "sortino": 1.8,
            "max_drawdown_pct": -4.0,
            "annualized_volatility_pct": 18.0,
            "win_rate_pct": 100.0,
            "avg_active_lots": 1.0,
            "turnover_events": 2,
            "rolling_12m_beating_spy_pct": None,
            "skipped": {},
        },
        diagnostics={
            "data_quality_confidence": "medium",
            "concentration_flags": [],
            "lots": 1,
        },
        simulation={
            "timeline": [
                {"date": "2026-01-05", "strategy_value": 100.0, "benchmark_value": 100.0, "active_lots": 1},
                {"date": "2026-02-05", "strategy_value": 108.0, "benchmark_value": 101.0, "active_lots": 1},
                {"date": "2026-04-06", "strategy_value": 115.0, "benchmark_value": 105.0, "active_lots": 0},
            ]
        },
        lots=[lot],
        primary_signals=[signal],
        confirmed_signals=[signal, open_signal],
        price_maps={
            "NVDA": {
                date(2026, 1, 5): PriceBar(day=date(2026, 1, 5), close=100.0, dollar_volume=1_000_000),
                date(2026, 2, 5): PriceBar(day=date(2026, 2, 5), close=108.0, dollar_volume=1_000_000),
                date(2026, 4, 6): PriceBar(day=date(2026, 4, 6), close=115.0, dollar_volume=1_000_000),
            },
            "MSFT": {
                date(2026, 3, 31): PriceBar(day=date(2026, 3, 31), close=50.0, dollar_volume=1_000_000),
                date(2026, 4, 6): PriceBar(day=date(2026, 4, 6), close=55.0, dollar_volume=1_000_000),
            }
        },
        benchmark_prices={
            date(2026, 1, 5): PriceBar(day=date(2026, 1, 5), close=100.0, dollar_volume=1_000_000),
            date(2026, 3, 31): PriceBar(day=date(2026, 3, 31), close=103.0, dollar_volume=1_000_000),
            date(2026, 4, 6): PriceBar(day=date(2026, 4, 6), close=105.0, dollar_volume=1_000_000),
        },
        universe=("NVDA", "MSFT"),
        filter_skips={},
    )


def _count(db, model) -> int:
    return len(db.execute(select(model)).scalars().all())


def test_persist_candidate_strategy_artifact_dry_run_writes_nothing():
    SessionLocal = _session()
    db = SessionLocal()
    try:
        result = persist_candidate_strategy_artifact(
            db,
            _artifact(),
            validation_result={"walnut_strategy_score": {"score": 48.39}},
            code_version="test-sha",
            apply=False,
        )

        assert result["mode"] == "dry_run"
        assert result["equity_points"] == 3
        assert result["current_holdings"] == 1
        assert result["current_signal_lots"] == 1
        assert _count(db, StrategyDefinition) == 0
        assert _count(db, StrategyBacktestRun) == 0
    finally:
        db.close()


def test_persist_candidate_strategy_artifact_apply_is_idempotent():
    SessionLocal = _session()
    db = SessionLocal()
    try:
        kwargs = {
            "validation_result": {"walnut_strategy_score": {"score": 48.39}},
            "code_version": "test-sha",
            "apply": True,
        }
        first = persist_candidate_strategy_artifact(db, _artifact(), **kwargs)
        second = persist_candidate_strategy_artifact(db, _artifact(), **kwargs)

        assert first["run_key"] == second["run_key"]
        assert first["strategy_id"] == second["strategy_id"]
        assert first["run_id"] == second["run_id"]
        assert _count(db, StrategyDefinition) == 1
        assert _count(db, StrategyBacktestRun) == 1
        assert _count(db, StrategyPerformanceSnapshot) == 5
        assert _count(db, StrategyEquityCurvePoint) == 3
        assert _count(db, StrategyHoldingsSnapshot) == 1
        assert _count(db, StrategyHoldingRow) == 1
        assert _count(db, StrategyCurrentHolding) == 1
    finally:
        db.close()
