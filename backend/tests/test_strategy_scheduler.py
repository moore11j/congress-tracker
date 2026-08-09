from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.models import AppSetting, StrategyDefinition, StrategyEvaluationRun, StrategyVersion
from app.services.strategy_scheduler import SCHEDULER_STATUS_KEY, run_active_strategy_evaluations, scheduler_status
from app.services.strategy_versions import activate_strategy_version


def _session():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False)


def _strategy(db, slug: str, *, version_status: str = "active"):
    strategy = StrategyDefinition(
        slug=slug,
        name=slug,
        category="cross_source",
        status="published",
        access_tier="premium",
        methodology_version="v1",
    )
    db.add(strategy)
    db.flush()
    version = StrategyVersion(
        strategy_id=strategy.id,
        version=1,
        status=version_status,
        rules_json='{"candidate_source":"confirmation_score_snapshots","direction":"bullish"}',
    )
    db.add(version)
    db.commit()
    return strategy, version


def test_scheduler_is_disabled_by_default_and_records_status(monkeypatch):
    SessionLocal = _session()
    db = SessionLocal()
    try:
        monkeypatch.delenv("STRATEGY_EVALUATIONS_ENABLED", raising=False)
        result = run_active_strategy_evaluations(db, scheduled_for=datetime(2026, 8, 10, 23, tzinfo=timezone.utc))
        assert result["status"] == "disabled"
        assert db.get(AppSetting, SCHEDULER_STATUS_KEY) is not None
        assert scheduler_status(db)["enabled"] is False
        assert db.execute(select(StrategyEvaluationRun)).scalars().all() == []
    finally:
        db.close()


def test_scheduler_enforces_run_limit_and_persists_completed_runs(monkeypatch):
    SessionLocal = _session()
    db = SessionLocal()
    try:
        _strategy(db, "first")
        _strategy(db, "second")
        monkeypatch.setenv("STRATEGY_EVALUATIONS_ENABLED", "true")
        monkeypatch.setenv("STRATEGY_EVALUATIONS_MAX_STRATEGIES", "1")
        result = run_active_strategy_evaluations(db, scheduled_for=datetime(2026, 8, 10, 23, tzinfo=timezone.utc))
        assert result["status"] == "ok"
        assert result["eligibleActiveVersions"] == 1
        assert result["processed"] == 1
        runs = db.execute(select(StrategyEvaluationRun).order_by(StrategyEvaluationRun.id)).scalars().all()
        assert len(runs) == 1
        assert runs[0].status == "completed"
    finally:
        db.close()


def test_scheduler_records_a_failed_run_when_an_active_version_is_invalid(monkeypatch):
    SessionLocal = _session()
    db = SessionLocal()
    try:
        strategy, version = _strategy(db, "broken")
        version.rules_json = '{"candidate_source":"unsupported"}'
        db.commit()
        monkeypatch.setenv("STRATEGY_EVALUATIONS_ENABLED", "true")
        result = run_active_strategy_evaluations(db, scheduled_for=datetime(2026, 8, 10, 23, tzinfo=timezone.utc))
        assert result["status"] == "partial"
        assert result["failed"] == 1
        failed = db.execute(select(StrategyEvaluationRun).where(StrategyEvaluationRun.strategy_id == strategy.id)).scalar_one()
        assert failed.status == "failed"
        assert "UnsupportedStrategyCandidateSource" in (failed.error or "")
    finally:
        db.close()


def test_activation_requires_published_strategy_and_deactivates_prior_version():
    SessionLocal = _session()
    db = SessionLocal()
    try:
        strategy, first = _strategy(db, "activate", version_status="approved")
        second = StrategyVersion(
            strategy_id=strategy.id,
            version=2,
            status="approved",
            rules_json='{"candidate_source":"confirmation_score_snapshots","direction":"bullish"}',
        )
        db.add(second)
        db.commit()

        activated_first = activate_strategy_version(db, slug=strategy.slug, version_id=first.id)
        assert activated_first["status"] == "active"
        activated_second = activate_strategy_version(db, slug=strategy.slug, version_id=second.id)
        assert activated_second["status"] == "active"
        db.refresh(first)
        assert first.status == "approved"
    finally:
        db.close()
