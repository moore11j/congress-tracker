from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import sessionmaker

from app import ingest_run
from app.db import Base
from app.models import Event, TradeOutcome


def _event(event_id: int, event_type: str, symbol: str | None) -> Event:
    ts = datetime(2026, 6, 16, tzinfo=timezone.utc)
    return Event(
        id=event_id,
        event_type=event_type,
        ts=ts,
        event_date=ts,
        symbol=symbol,
        source="test",
        payload_json="{}",
    )


def test_daily_outcome_repair_uses_only_missing_safe_retry_statuses(monkeypatch):
    calls: list[dict] = []

    def fake_run_compute(**kwargs):
        calls.append(kwargs)
        return {
            "event_type": kwargs["event_type"],
            "inserted": 1,
            "updated": 2,
            "skipped": 3,
            "status_counts": {"ok": 1},
        }

    monkeypatch.setattr(ingest_run, "run_compute", fake_run_compute)
    monkeypatch.setattr(
        ingest_run,
        "_daily_outcome_coverage_report",
        lambda *, lookback_days: {"lookback_days": lookback_days, "failed_statuses": {}},
    )
    monkeypatch.delenv("OUTCOME_REPAIR_LIMIT", raising=False)

    report = ingest_run._run_daily_outcome_repair()

    assert report["job"] == "daily-repair"
    assert [call["event_type"] for call in calls] == ["congress_trade", "insider_trade"]
    assert all(call["only_missing"] is True for call in calls)
    assert all(call["replace"] is False for call in calls)
    assert all(call["lookback_days"] == 1095 for call in calls)
    assert all(call["retry_failed_statuses"] == ingest_run.SAFE_OUTCOME_RETRY_STATUSES for call in calls)


def test_top_missing_symbols_statement_groups_by_raw_symbol_for_postgres() -> None:
    sql = str(
        ingest_run._top_missing_symbols_statement(
            datetime(2026, 6, 1, tzinfo=timezone.utc)
        ).compile(dialect=postgresql.dialect())
    ).lower()

    assert "group by events.symbol" in sql
    assert "group by coalesce(events.symbol" not in sql


def test_daily_outcome_coverage_report_normalizes_unresolved_symbols(monkeypatch) -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    Base.metadata.create_all(bind=engine, tables=[Event.__table__, TradeOutcome.__table__])

    with SessionLocal() as db:
        db.add_all(
            [
                _event(1, "congress_trade", None),
                _event(2, "insider_trade", ""),
                _event(3, "insider_trade", "   "),
                _event(4, "insider_trade", "AAPL"),
                _event(5, "insider_trade", "AAPL"),
                _event(6, "congress_trade", "MSFT"),
                TradeOutcome(event_id=6),
            ]
        )
        db.commit()

    monkeypatch.setattr(ingest_run, "SessionLocal", SessionLocal)

    report = ingest_run._daily_outcome_coverage_report(lookback_days=30)

    assert report["missing_outcomes"] == {"congress_trade": 1, "insider_trade": 4}
    assert report["unresolved_symbols_remaining"] == 3
    assert report["top_symbols_with_missing_outcomes"][:2] == [
        {"symbol": "<unresolved>", "missing": 3},
        {"symbol": "AAPL", "missing": 2},
    ]
