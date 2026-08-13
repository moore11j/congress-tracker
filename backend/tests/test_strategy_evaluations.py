from __future__ import annotations

from datetime import date

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from app.db import Base, ensure_strategy_storage_schema
from app.models import StrategyDefinition, StrategyEvent, StrategyLiveHolding, StrategyTrade, StrategyVersion
from app.services.strategy_evaluations import StrategyEvaluationCandidate, evaluate_strategy_candidates


def _session():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    Base.metadata.create_all(bind=engine)
    ensure_strategy_storage_schema(engine)
    return SessionLocal


def _strategy(db):
    strategy = StrategyDefinition(
        slug="prospective-test",
        name="Prospective Test",
        category="congress",
        status="draft",
        access_tier="premium",
        methodology_version="v1",
    )
    db.add(strategy)
    db.flush()
    version = StrategyVersion(strategy_id=strategy.id, version=1, status="approved")
    db.add(version)
    db.commit()
    return strategy, version


def test_evaluation_creates_auditable_trades_events_and_is_idempotent():
    SessionLocal = _session()
    db = SessionLocal()
    try:
        strategy, version = _strategy(db)
        first = evaluate_strategy_candidates(
            db,
            strategy_id=strategy.id,
            strategy_version_id=version.id,
            evaluation_date=date(2026, 8, 10),
            universe_count=500,
            candidates=[
                StrategyEvaluationCandidate("NVDA", weight_pct=50, score=82, source_count=4, entry_price=125.5),
                StrategyEvaluationCandidate("PLTR", weight_pct=50, score=72, source_count=3, entry_price=34.1),
            ],
        )
        assert first["idempotent"] is False
        assert first["changes"] == {"added": 2, "exited": 0, "rebalanced": 0}
        assert [trade.action for trade in db.execute(select(StrategyTrade).order_by(StrategyTrade.symbol)).scalars()] == ["buy", "buy"]
        assert [event.event_type for event in db.execute(select(StrategyEvent).order_by(StrategyEvent.id)).scalars()] == ["trade_added", "trade_added", "rebalance_completed"]

        repeat = evaluate_strategy_candidates(
            db,
            strategy_id=strategy.id,
            strategy_version_id=version.id,
            evaluation_date=date(2026, 8, 10),
            universe_count=500,
            candidates=[StrategyEvaluationCandidate("NVDA", weight_pct=50)],
        )
        assert repeat["idempotent"] is True
        assert db.execute(select(StrategyTrade)).scalars().all().__len__() == 2
        assert db.execute(select(StrategyEvent)).scalars().all().__len__() == 3
    finally:
        db.close()


def test_evaluation_records_exits_and_rebalances_without_rewriting_history():
    SessionLocal = _session()
    db = SessionLocal()
    try:
        strategy, version = _strategy(db)
        evaluate_strategy_candidates(
            db,
            strategy_id=strategy.id,
            strategy_version_id=version.id,
            evaluation_date=date(2026, 8, 10),
            universe_count=100,
            candidates=[
                StrategyEvaluationCandidate("NVDA", weight_pct=50),
                StrategyEvaluationCandidate("PLTR", weight_pct=50),
            ],
        )
        result = evaluate_strategy_candidates(
            db,
            strategy_id=strategy.id,
            strategy_version_id=version.id,
            evaluation_date=date(2026, 8, 17),
            universe_count=100,
            candidates=[
                StrategyEvaluationCandidate("NVDA", weight_pct=100),
                StrategyEvaluationCandidate("MSFT", weight_pct=10),
            ],
            closing_prices={"PLTR": 40.0},
        )
        assert result["changes"] == {"added": 1, "exited": 1, "rebalanced": 1}
        trades = db.execute(select(StrategyTrade).order_by(StrategyTrade.id)).scalars().all()
        assert [(trade.symbol, trade.action, trade.status) for trade in trades] == [
            ("NVDA", "buy", "open"),
            ("PLTR", "buy", "closed"),
            ("PLTR", "sell", "completed"),
            ("MSFT", "buy", "open"),
            ("NVDA", "rebalance", "completed"),
        ]
        assert trades[1].exit_price == 40.0
        live = db.execute(select(StrategyLiveHolding).order_by(StrategyLiveHolding.rank)).scalars().all()
        assert [(holding.symbol, holding.weight_pct) for holding in live] == [("NVDA", 100.0), ("MSFT", 10.0)]
        assert live[0].entry_date == date(2026, 8, 10)
        assert [event.event_type for event in db.execute(select(StrategyEvent).order_by(StrategyEvent.id)).scalars()][-4:] == [
            "trade_exited",
            "trade_added",
            "position_rebalanced",
            "rebalance_completed",
        ]
    finally:
        db.close()


def test_initialization_writes_holdings_without_emitting_subscriber_events():
    SessionLocal = _session()
    db = SessionLocal()
    try:
        strategy, version = _strategy(db)
        result = evaluate_strategy_candidates(
            db,
            strategy_id=strategy.id,
            strategy_version_id=version.id,
            evaluation_date=date(2026, 8, 10),
            universe_count=1,
            candidates=[StrategyEvaluationCandidate("NVDA", weight_pct=100)],
            initialize=True,
        )
        assert result["changes"] == {"added": 1, "exited": 0, "rebalanced": 0}
        assert db.execute(select(StrategyEvent)).scalars().all() == []
        assert [holding.symbol for holding in db.execute(select(StrategyLiveHolding)).scalars()] == ["NVDA"]
    finally:
        db.close()
