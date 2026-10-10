import json
from datetime import datetime, timezone
import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session
from app.db import Base
from app.models import ConfirmationMethodologyVersion, LeaderboardSnapshot
from app.jobs import prepare_analyst_transition as job
from app.services import top_stocks
from app.services.outcome_ledger import current_methodology_configuration


def test_preparation_preserves_current_methodology_and_rolls_back_atomically(monkeypatch):
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    future = top_stocks.CONFIRMATION_SCORING_VERSION + "_analyst_display_only_v1"
    monkeypatch.setattr(job, "CONFIRMATION_SCORING_VERSION", future)
    monkeypatch.setattr(job, "CURRENT_CONFIRMATION_METHODOLOGY_VERSION", "future")
    with Session(engine) as db:
        old = ConfirmationMethodologyVersion(version="prior", description="prior", configuration_json="{}",
            deployed_at=datetime.now(timezone.utc), is_current=True)
        db.add(old); db.commit()
        def ranking(db, **kwargs):
            assert kwargs == dict(staged=True, expected_candidates=1, commit=False)
            db.add(LeaderboardSnapshot(leaderboard_key="staged", generated_at=datetime.now(timezone.utc), payload_json="{}"))
            return dict(generated_at="2026-10-10T00:00:00Z", returned=1)
        monkeypatch.setattr(job, "refresh_top_stocks_leaderboard", ranking)
        actual_register = job.register_confirmation_methodology_version
        monkeypatch.setattr(job, "register_confirmation_methodology_version", lambda db, **kwargs: actual_register(db, version="future", **kwargs))
        result = job.prepare_analyst_transition(db, expected_candidates=1)
        assert not result["public_selector_changed"] and not result["historical_records_changed"]
        assert old.is_current
        prepared = db.scalar(select(ConfirmationMethodologyVersion).where(ConfirmationMethodologyVersion.version == "future"))
        assert not prepared.is_current
        db.rollback()
        assert db.scalar(select(LeaderboardSnapshot)) is None
        assert [r.version for r in db.scalars(select(ConfirmationMethodologyVersion))] == ["prior"]
        job.prepare_analyst_transition(db, expected_candidates=1); db.commit()
        assert old.is_current
        assert len(db.scalars(select(ConfirmationMethodologyVersion)).all()) == 2


def test_preparation_rejects_default_selector_before_any_database_work(monkeypatch):
    monkeypatch.setattr(job, "CONFIRMATION_SCORING_VERSION", "prior")
    with pytest.raises(ValueError, match="fresh process"):
        job.prepare_analyst_transition(object(), expected_candidates=1)


def test_preparation_rejects_conflicting_methodology(monkeypatch):
    engine = create_engine("sqlite:///:memory:"); Base.metadata.create_all(engine)
    monkeypatch.setattr(job, "CONFIRMATION_SCORING_VERSION", "test_analyst_display_only_v1")
    monkeypatch.setattr(job, "CURRENT_CONFIRMATION_METHODOLOGY_VERSION", "conflicting")
    with Session(engine) as db:
        db.add(ConfirmationMethodologyVersion(version="conflicting", description="old", configuration_json="{}",
            deployed_at=datetime.now(timezone.utc), is_current=False)); db.commit()
        with pytest.raises(ValueError, match="configuration differs"):
            job.prepare_analyst_transition(db, expected_candidates=1)
        assert db.scalar(select(LeaderboardSnapshot)) is None
