from __future__ import annotations

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

import app.services.feed_cache_epoch as epoch_module
from app.db import Base
from app.models import AppSetting


def _session_factory():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False)


def test_feed_events_epoch_persists_and_changes(monkeypatch) -> None:
    SessionLocal = _session_factory()
    monkeypatch.setattr(epoch_module, "SessionLocal", SessionLocal)
    epoch_module.clear_feed_events_epoch_cache()

    assert epoch_module.current_feed_events_epoch() == "0"

    first = epoch_module.bump_feed_events_epoch(reason="test")
    second = epoch_module.bump_feed_events_epoch(reason="test")

    assert first["status"] == "ok"
    assert second["status"] == "ok"
    assert int(second["epoch"]) > int(first["epoch"])
    assert epoch_module.current_feed_events_epoch() == second["epoch"]

    db = SessionLocal()
    try:
        row = db.execute(select(AppSetting).where(AppSetting.key == epoch_module.FEED_EVENTS_EPOCH_KEY)).scalar_one()
        assert row.value == second["epoch"]
    finally:
        db.close()
        epoch_module.clear_feed_events_epoch_cache()
