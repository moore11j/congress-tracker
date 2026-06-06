from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.models import InsightsSnapshot
from app.services.insights_snapshots import get_insights_snapshot, refresh_insights_snapshot


def _db():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    return Session()


def _payload(status: str = "ok") -> dict:
    return {
        "world_indexes": [],
        "indexes": [{"label": "S&P 500", "symbol": "^GSPC", "value": 5000, "change_pct": 1.2}],
        "treasury": [],
        "economics": [],
        "commodities": [],
        "currencies": [],
        "crypto": [],
        "sector_performance": [],
        "status": status,
        "generated_at": "2026-06-05T12:00:00+00:00",
    }


def test_insights_snapshot_returns_cached_data_without_provider_call(monkeypatch):
    db = _db()
    try:
        row = InsightsSnapshot(
            kind="macro-snapshot",
            payload_json=json.dumps(_payload()),
            source="test",
            fetched_at=datetime.now(timezone.utc),
        )
        db.add(row)
        db.commit()

        def fail_provider():
            raise AssertionError("provider should not be called on cache hit")

        monkeypatch.setattr("app.services.insights_snapshots.get_macro_snapshot", fail_provider)

        payload = get_insights_snapshot(db)

        assert payload["status"] == "ok"
        assert payload["cache_hit"] is True
        assert payload["stale"] is False
        assert payload["source"] == "test"
        assert payload["as_of"]
    finally:
        db.close()


def test_insights_snapshot_marks_old_cache_stale():
    db = _db()
    try:
        db.add(
            InsightsSnapshot(
                kind="macro-snapshot",
                payload_json=json.dumps(_payload()),
                source="test",
                fetched_at=datetime.now(timezone.utc) - timedelta(minutes=30),
            )
        )
        db.commit()

        payload = get_insights_snapshot(db)

        assert payload["status"] == "ok"
        assert payload["stale"] is True
        assert payload["cache_hit"] is True
    finally:
        db.close()


def test_insights_refresh_returns_stale_cache_when_provider_fails(monkeypatch):
    db = _db()
    try:
        db.add(
            InsightsSnapshot(
                kind="macro-snapshot",
                payload_json=json.dumps(_payload()),
                source="test",
                fetched_at=datetime.now(timezone.utc) - timedelta(hours=1),
            )
        )
        db.commit()

        def fail_provider():
            raise RuntimeError("provider down")

        monkeypatch.setattr("app.services.insights_snapshots.get_macro_snapshot", fail_provider)

        payload = refresh_insights_snapshot(db)

        assert payload["status"] == "ok"
        assert payload["stale"] is True
        assert payload["cache_hit"] is True
    finally:
        db.close()
