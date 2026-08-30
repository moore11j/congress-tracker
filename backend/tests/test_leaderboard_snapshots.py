from datetime import datetime, timezone
from types import SimpleNamespace
import sys

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


def test_insider_snapshot_uses_existing_trade_outcome_ranking(monkeypatch):
    db = _session()
    try:
        monkeypatch.setitem(
            sys.modules,
            "app.main",
            SimpleNamespace(
                _load_member_leaderboard_rows=lambda *_args, **_kwargs: [
                    {"member_id": "000123", "member_name": "", "avg_return": 12.5, "avg_alpha": 7.5, "win_rate": 0.75, "trade_count_scored": 4}
                ]
            ),
        )
        monkeypatch.setattr(
            leaderboard_snapshots,
            "_insider_details",
            lambda *_args: {"000123": {"insider_name": "Jane Doe", "company_name": "Example Corp", "role": "CEO", "symbol": "EXM", "reporting_cik": "000123"}},
        )

        payload = leaderboard_snapshots._build_insider_payload(db, datetime(2026, 8, 29, tzinfo=timezone.utc))

        assert payload["items"] == [{"rank": 1, "name": "Jane Doe", "company_name": "Example Corp", "role": "CEO", "symbol": "EXM", "reporting_cik": "000123", "avg_return_pct": 12.5, "avg_alpha_pct": 7.5, "win_rate_pct": 75.0, "trade_count": 4}]
        assert payload["sort"] == "avg_alpha_pct"
        assert "not a CAGR" in payload["methodology"]
    finally:
        db.close()
