from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from app.db import Base, ensure_strategy_storage_schema
from app.models import (
    Member,
    ReplicatedPortfolioPoint,
    ReplicatedPortfolioPosition,
    ReplicatedPortfolioRun,
    StrategyBacktestRun,
    StrategyCurrentHolding,
    StrategyDefinition,
    StrategyEquityCurvePoint,
    StrategyHoldingRow,
    StrategyHoldingsSnapshot,
    StrategyHistoricalTransaction,
    StrategyPerformanceSnapshot,
)
from app.services.replicated_portfolio_strategy_refresh import persist_top_congress_portfolio_strategies


def _session():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    Base.metadata.create_all(bind=engine)
    ensure_strategy_storage_schema(engine)
    return SessionLocal


def _count(db, model) -> int:
    return len(db.execute(select(model)).scalars().all())


def _seed_replicated_run(db) -> ReplicatedPortfolioRun:
    db.add(Member(bioguide_id="X001", first_name="Ada", last_name="Lovelace", chamber="House", party="Democrat", state="CA"))
    run = ReplicatedPortfolioRun(
        entity_type="congress_member",
        entity_id="X001",
        mode="realistic_disclosure_lag",
        lookback_days=1095,
        benchmark_symbol="SPY",
        start_date=date(2023, 1, 3),
        end_date=date(2024, 1, 5),
        starting_value=100000.0,
        ending_value=130000.0,
        benchmark_ending_value=115000.0,
        total_return_pct=30.0,
        benchmark_return_pct=15.0,
        alpha_pct=15.0,
        cagr_pct=29.7,
        max_drawdown_pct=8.0,
        volatility_pct=19.0,
        sharpe_ratio=1.4,
        win_rate_pct=58.0,
        average_exposure_pct=72.0,
        ending_cash_pct=18.0,
        points_count=368,
        positions_count=12,
        skipped_events_count=2,
        status="ok",
        status_message=json.dumps({"curve_diagnostics": {"curve_quality_status": "good"}}),
        methodology_version="replicated_portfolio_v4",
        computed_at=datetime(2024, 1, 6, tzinfo=timezone.utc),
    )
    db.add(run)
    db.flush()
    start = date(2023, 1, 3)
    for offset in range(368):
        day = start + timedelta(days=offset)
        value = 100000.0 + offset * 80.0
        benchmark = 100000.0 + offset * 40.0
        db.add(
            ReplicatedPortfolioPoint(
                run_id=run.id,
                asof_date=day,
                strategy_value=value,
                benchmark_value=benchmark,
                strategy_return_pct=(value / 100000.0 - 1.0) * 100.0,
                benchmark_return_pct=(benchmark / 100000.0 - 1.0) * 100.0,
                alpha_pct=((value - benchmark) / 100000.0) * 100.0,
                daily_return_pct=0.08 if offset else 0.0,
                active_positions=2,
                exposure_pct=82.0,
                cash_pct=18.0,
            )
        )
    db.add_all(
        [
            ReplicatedPortfolioPosition(
                run_id=run.id,
                source_event_id=1,
                symbol="NVDA",
                side="purchase",
                entry_date=date(2023, 6, 1),
                entry_price=100.0,
                exit_price=130.0,
                shares=10.0,
                market_value=1300.0,
                return_pct=30.0,
                amount_min=1000,
                amount_max=15000,
                status="open",
                source_type="disclosed_trade",
            ),
            ReplicatedPortfolioPosition(
                run_id=run.id,
                source_event_id=2,
                symbol="MSFT",
                side="purchase",
                entry_date=date(2023, 8, 1),
                entry_price=200.0,
                exit_price=210.0,
                shares=5.0,
                market_value=1050.0,
                return_pct=5.0,
                amount_min=1000,
                amount_max=15000,
                status="open",
                source_type="disclosed_trade",
            ),
        ]
    )
    db.commit()
    return run


def test_persist_top_congress_portfolio_strategies_dry_run_writes_nothing():
    SessionLocal = _session()
    with SessionLocal() as db:
        _seed_replicated_run(db)
        result = persist_top_congress_portfolio_strategies(db, top=1, min_positions=1, min_points=2)
        assert result["metadata"]["mode"] == "dry_run"
        assert result["metadata"]["eligible_runs"] == 1
        assert result["rows"][0]["slug"] == "congress-portfolio-x001-1095d"
        assert _count(db, StrategyDefinition) == 0
        assert _count(db, StrategyBacktestRun) == 0


def test_persist_top_congress_portfolio_strategies_can_target_named_members():
    SessionLocal = _session()
    with SessionLocal() as db:
        _seed_replicated_run(db)
        result = persist_top_congress_portfolio_strategies(
            db,
            top=10,
            min_positions=1,
            min_points=2,
            entity_ids=["x001"],
        )

        assert result["metadata"]["entity_ids"] == ["X001"]
        assert result["metadata"]["eligible_runs"] == 1
        assert [row["slug"] for row in result["rows"]] == ["congress-portfolio-x001-1095d"]


def test_persist_top_congress_portfolio_strategies_apply_is_idempotent():
    SessionLocal = _session()
    with SessionLocal() as db:
        _seed_replicated_run(db)
        kwargs = {"top": 1, "min_positions": 1, "min_points": 2, "code_version": "test-sha", "apply": True}
        first = persist_top_congress_portfolio_strategies(db, **kwargs)
        second = persist_top_congress_portfolio_strategies(db, **kwargs)

        assert first["rows"][0]["strategy_id"] == second["rows"][0]["strategy_id"]
        assert first["rows"][0]["run_id"] == second["rows"][0]["run_id"]
        assert _count(db, StrategyDefinition) == 1
        assert _count(db, StrategyBacktestRun) == 1
        assert _count(db, StrategyPerformanceSnapshot) == 5
        assert _count(db, StrategyEquityCurvePoint) == 368
        assert _count(db, StrategyHoldingsSnapshot) == 1
        assert _count(db, StrategyHoldingRow) == 2
        assert _count(db, StrategyCurrentHolding) == 2
        assert _count(db, StrategyHistoricalTransaction) == 2
        strategy = db.execute(select(StrategyDefinition)).scalars().one()
        assert strategy.status == "draft"
        assert strategy.family == "individual_portfolio"
