"""Bound evidence loading without changing horizon fairness or ledger history."""
from datetime import date, datetime, timezone
from types import SimpleNamespace

import pytest
from sqlalchemy import event, select
from sqlalchemy.orm import Session, sessionmaker

from app import ingest_run
from app.models import ConfirmationScoreSnapshot, OutcomeEntry
from app.services.outcome_integrity import materialize_outcome_entry
from test_outcome_integrity import _engine, _snapshot, _bar


@pytest.mark.parametrize("limit,expected", [
    (1, [10]), (2, [4, 10]), (4, [4, 1, 5, 10]),
    (5, [4, 1, 5, 7, 10]), (20, [4, 1, 2, 3, 5, 6, 7, 8, 9, 10]),
])
def test_hydrator_preserves_selection_and_only_loads_selected_evidence(monkeypatch, limit, expected):
    engine = _engine()
    with Session(engine) as db:
        for index in range(10):
            day = date(2026, 11, 2) if index == 3 else date(2026, 5, index + 4)
            symbol = f"T{index + 1}"
            db.merge(_bar("SPY", day, 500))
            db.add(_bar(symbol, day, 100))
            snapshot = _snapshot(db, datetime.combine(day, datetime.min.time(), timezone.utc).replace(hour=13),
                                 ticker=symbol, security_id=index + 1,
                                 source_contributions_json='{"evidence":"' + "x" * 100_000 + '"}')
            # Large evidence strings must not be loaded just to select a batch.
            if index < 4:
                assert materialize_outcome_entry(db, snapshot) is not None
        db.commit()
        before = [(row.id, row.input_hash, row.source_contributions_json) for row in db.scalars(select(ConfirmationScoreSnapshot))]
        entry_count = len(db.scalars(select(OutcomeEntry)).all())

    statements = []
    event.listen(engine, "before_cursor_execute", lambda conn, cursor, statement, params, context, many: statements.append(statement))
    monkeypatch.setenv("OUTCOME_LEDGER_PRICE_HYDRATOR_SNAPSHOT_LIMIT", str(limit))
    monkeypatch.setenv("OUTCOME_LEDGER_PRICE_HYDRATOR_ENABLED", "true")
    monkeypatch.setattr(ingest_run, "SessionLocal", sessionmaker(bind=engine))
    monkeypatch.setattr(ingest_run, "engine", engine)
    monkeypatch.setattr(ingest_run, "ensure_price_cache_volume_columns", lambda *_: None)
    monkeypatch.setattr(ingest_run, "ensure_outcome_ledger_schema", lambda *_: None)
    monkeypatch.setattr(ingest_run, "check_background_job_guard", lambda *_: SimpleNamespace(proceed=True))
    monkeypatch.setattr(ingest_run, "get_expected_latest_market_date", lambda: date(2026, 11, 10))
    monkeypatch.setattr(ingest_run, "repair_public_outcome_horizons", lambda *a, **k: {})
    monkeypatch.setattr(ingest_run, "materialize_cached_outcome_horizons", lambda *a, **k: {"observations_created": 0})
    monkeypatch.setattr(ingest_run, "outcome_leaderboard_symbols", lambda *_: ["T4"])
    monkeypatch.setattr(ingest_run, "hydrate_split_adjusted_ohlc", lambda *a, **k: 0)
    selected = []

    def materialize(db, snapshot):
        assert len(snapshot.source_contributions_json) > 100_000
        selected.append(snapshot.id)
        return None

    monkeypatch.setattr(ingest_run, "materialize_outcome_entry", materialize)
    result = ingest_run._run_outcome_ledger_price_hydrator_job()
    assert selected == expected
    assert result["snapshot_count"] == len(expected)
    evidence_reads = [sql for sql in statements if sql.lstrip().upper().startswith("SELECT") and
                      "confirmation_score_snapshots.source_contributions_json" in sql.split("FROM")[0]]
    assert len(evidence_reads) == 1
    assert "confirmation_score_snapshots.id IN" in evidence_reads[0]
    assert len(expected) <= limit
    with Session(engine) as db:
        assert [(row.id, row.input_hash, row.source_contributions_json) for row in db.scalars(select(ConfirmationScoreSnapshot))] == before
        assert len(db.scalars(select(OutcomeEntry)).all()) == entry_count
