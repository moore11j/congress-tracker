from datetime import datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.models import LeaderboardSnapshot
from app.services import leaderboard_snapshots


def _session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine, tables=[LeaderboardSnapshot.__table__])
    return sessionmaker(bind=engine, autoflush=False, autocommit=False)()


def test_snapshot_reader_returns_a_safe_empty_payload_before_daily_refresh():
    db = _session()
    try:
        payload = leaderboard_snapshots.read_leaderboard_snapshot(db, leaderboard_snapshots.CONGRESS_LEADERBOARD_KEY)
        assert payload["key"] == leaderboard_snapshots.CONGRESS_LEADERBOARD_KEY
        assert payload["items"] == []
    finally:
        db.close()


def test_insider_snapshot_does_not_mislabel_issuer_grouped_runs_as_people():
    payload = leaderboard_snapshots._build_insider_payload(datetime(2026, 8, 29, tzinfo=timezone.utc))

    assert payload["items"] == []
    assert "personal-insider" in payload["methodology"]
    assert "Form 4" in payload["empty_message"]
