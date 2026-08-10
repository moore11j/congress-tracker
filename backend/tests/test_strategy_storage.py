from __future__ import annotations

import json
import json
from datetime import date, datetime, timezone

from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker

from app.db import Base, ensure_strategy_storage_schema
from app.entitlements import ENTITLEMENTS
from app.models import (
    StrategyBacktestRun,
    StrategyCurrentHolding,
    StrategyDefinition,
    StrategyEquityCurvePoint,
    StrategyEvaluationRun,
    StrategyEvent,
    StrategyEventDelivery,
    StrategyPerformanceSnapshot,
    StrategySubscription,
    StrategyTrade,
    StrategyVersion,
)
from app.services.strategies import list_strategy_cards, set_strategy_publication, strategy_detail


def _session():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    Base.metadata.create_all(bind=engine)
    ensure_strategy_storage_schema(engine)
    return SessionLocal, engine


def test_ensure_strategy_storage_schema_creates_expected_tables_and_indexes():
    _, engine = _session()
    inspector = inspect(engine)

    expected_tables = {
        "strategy_definitions",
        "strategy_backtest_runs",
        "strategy_performance_snapshots",
        "strategy_equity_curve_points",
        "strategy_holdings_snapshots",
        "strategy_holding_rows",
        "strategy_current_holdings",
        "strategy_versions",
        "strategy_evaluation_runs",
        "strategy_live_holdings",
        "strategy_trades",
        "strategy_events",
        "strategy_subscriptions",
        "strategy_event_deliveries",
    }
    assert expected_tables <= set(inspector.get_table_names())
    indexes = {index["name"] for index in inspector.get_indexes("strategy_definitions")}
    assert "ix_strategy_definitions_slug" in indexes


def test_strategy_service_lists_persisted_cards_and_sorts_lowest_drawdown_first():
    SessionLocal, _ = _session()
    db = SessionLocal()
    try:
        conservative = StrategyDefinition(
            slug="low-drawdown",
            name="Low Drawdown",
            category="congress",
            status="published",
            access_tier="premium",
            methodology_version="v1",
            sort_order=2,
        )
        aggressive = StrategyDefinition(
            slug="high-return",
            name="High Return",
            category="congress",
            status="published",
            access_tier="premium",
            methodology_version="v1",
            sort_order=1,
        )
        db.add_all([conservative, aggressive])
        db.flush()
        now = datetime.now(timezone.utc)
        run_one = StrategyBacktestRun(
            strategy_id=conservative.id,
            run_key="run-low",
            status="ok",
            completed_at=now,
            methodology_version="v1",
            walnut_strategy_score=55,
            diagnostics_json=json.dumps({"validation": {"walnut_strategy_score": {"score": 55, "score_version": "walnut_strategy_score_v2"}}}),
        )
        run_two = StrategyBacktestRun(
            strategy_id=aggressive.id,
            run_key="run-high",
            status="ok",
            completed_at=now,
            methodology_version="v1",
            walnut_strategy_score=70,
            diagnostics_json=json.dumps({"validation": {"walnut_strategy_score": {"score": 70, "score_version": "walnut_strategy_score_v2"}}}),
        )
        db.add_all([run_one, run_two])
        db.flush()
        db.add_all(
            [
                StrategyPerformanceSnapshot(
                    strategy_id=conservative.id,
                    run_id=run_one.id,
                    as_of_date=date(2026, 7, 31),
                    period="max",
                    cagr_pct=12,
                    max_drawdown_pct=-8,
                    walnut_strategy_score=55,
                ),
                StrategyPerformanceSnapshot(
                    strategy_id=aggressive.id,
                    run_id=run_two.id,
                    as_of_date=date(2026, 7, 31),
                    period="max",
                    cagr_pct=25,
                    max_drawdown_pct=-35,
                    walnut_strategy_score=70,
                ),
            ]
        )
        db.commit()

        by_drawdown = list_strategy_cards(db, entitlements=ENTITLEMENTS["premium"], sort="drawdown")
        assert [item["slug"] for item in by_drawdown["items"]] == ["low-drawdown", "high-return"]

        by_score = list_strategy_cards(db, entitlements=ENTITLEMENTS["premium"], sort="walnut_score")
        assert [item["slug"] for item in by_score["items"]] == ["high-return", "low-drawdown"]

        by_default = list_strategy_cards(db, entitlements=ENTITLEMENTS["premium"])
        assert [item["slug"] for item in by_default["items"]] == ["high-return", "low-drawdown"]
    finally:
        db.close()


def test_strategy_publication_requires_a_completed_run_and_max_snapshot():
    SessionLocal, _ = _session()
    db = SessionLocal()
    try:
        strategy = StrategyDefinition(
            slug="reviewed-strategy",
            name="Reviewed Strategy",
            category="congress",
            status="draft",
            access_tier="premium",
            methodology_version="v1",
        )
        db.add(strategy)
        db.commit()

        try:
            set_strategy_publication(
                db,
                slug=strategy.slug,
                published=True,
                entitlements=ENTITLEMENTS["premium"],
            )
            raise AssertionError("Expected publication to require a completed run and snapshot")
        except Exception as exc:
            assert getattr(exc, "status_code", None) == 422

        run = StrategyBacktestRun(
            strategy_id=strategy.id,
            run_key="reviewed-run",
            status="ok",
            completed_at=datetime.now(timezone.utc),
            methodology_version="v1",
        )
        db.add(run)
        db.flush()
        db.add(
            StrategyPerformanceSnapshot(
                strategy_id=strategy.id,
                run_id=run.id,
                as_of_date=date(2026, 7, 31),
                period="max",
                cagr_pct=12,
            )
        )
        db.commit()

        published = set_strategy_publication(
            db,
            slug=strategy.slug,
            published=True,
            entitlements=ENTITLEMENTS["premium"],
        )
        assert published["status"] == "published"
        assert published["publishedAt"] is not None
        assert [item["slug"] for item in list_strategy_cards(db, entitlements=ENTITLEMENTS["premium"])["items"]] == [strategy.slug]

        unpublished = set_strategy_publication(
            db,
            slug=strategy.slug,
            published=False,
            entitlements=ENTITLEMENTS["premium"],
        )
        assert unpublished["status"] == "draft"
        assert unpublished["publishedAt"] is None
        assert list_strategy_cards(db, entitlements=ENTITLEMENTS["premium"])["items"] == []
    finally:
        db.close()


def test_strategy_detail_hides_holdings_and_curve_when_user_lacks_required_tier():
    SessionLocal, _ = _session()
    db = SessionLocal()
    try:
        strategy = StrategyDefinition(
            slug="pro-only",
            name="Pro Only",
            category="cross_source",
            status="published",
            access_tier="pro",
            methodology_version="v1",
            rule_json=json.dumps({"kind": "test"}),
        )
        db.add(strategy)
        db.flush()
        run = StrategyBacktestRun(
            strategy_id=strategy.id,
            run_key="run-pro",
            status="ok",
            completed_at=datetime.now(timezone.utc),
            methodology_version="v1",
            walnut_strategy_score=80,
        )
        db.add(run)
        db.flush()
        db.add(
            StrategyEquityCurvePoint(
                strategy_id=strategy.id,
                run_id=run.id,
                date=date(2026, 7, 31),
                strategy_value=140,
                benchmark_value=120,
            )
        )
        db.add(
            StrategyCurrentHolding(
                strategy_id=strategy.id,
                run_id=run.id,
                as_of_date=date(2026, 7, 31),
                symbol="NVDA",
                rank=1,
                weight_pct=10,
            )
        )
        db.commit()

        locked = strategy_detail(db, slug="pro-only", entitlements=ENTITLEMENTS["premium"])
        assert locked["access"]["locked"] is True
        assert locked["equityCurve"] == []
        assert locked["currentHoldings"] == []

        unlocked = strategy_detail(db, slug="pro-only", entitlements=ENTITLEMENTS["pro"])
        assert unlocked["access"]["locked"] is False
        assert unlocked["equityCurve"][0]["strategyValue"] == 140
        assert unlocked["currentHoldings"][0]["symbol"] == "NVDA"
    finally:
        db.close()
