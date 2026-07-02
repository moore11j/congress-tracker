import logging

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.exc import OperationalError

from app.db import ensure_optional_performance_indexes, ensure_provider_usage_schema


def test_provider_usage_schema_does_not_require_optional_tables():
    engine = create_engine("sqlite:///:memory:", future=True)

    ensure_provider_usage_schema(engine)
    ensure_provider_usage_schema(engine)

    with engine.connect() as conn:
        table = conn.execute(
            text("SELECT name FROM sqlite_master WHERE type='table' AND name='provider_usage_events'")
        ).scalar()

    assert table == "provider_usage_events"


def test_provider_usage_schema_does_not_create_optional_hot_table_indexes():
    engine = create_engine("sqlite:///:memory:", future=True)
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE members (id INTEGER PRIMARY KEY, first_name TEXT, last_name TEXT)"))
        conn.execute(text("CREATE TABLE events (id INTEGER PRIMARY KEY, member_name TEXT)"))

    ensure_provider_usage_schema(engine)

    with engine.connect() as conn:
        indexes = {
            row[0]
            for row in conn.execute(
                text("SELECT name FROM sqlite_master WHERE type='index'")
            ).fetchall()
        }

    assert "ix_members_name_lower" not in indexes
    assert "ix_events_member_name_lower" not in indexes


def test_optional_performance_indexes_index_optional_tables_when_requested():
    engine = create_engine("sqlite:///:memory:", future=True)
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE members (id INTEGER PRIMARY KEY, first_name TEXT, last_name TEXT)"))
        conn.execute(
            text(
                "CREATE TABLE events ("
                "id INTEGER PRIMARY KEY, "
                "member_name TEXT, "
                "symbol TEXT, "
                "event_type TEXT, "
                "event_date TIMESTAMP, "
                "ts TIMESTAMP, "
                "payload_json TEXT"
                ")"
            )
        )

    result = ensure_optional_performance_indexes(engine)

    with engine.connect() as conn:
        indexes = {
            row[0]
            for row in conn.execute(
                text("SELECT name FROM sqlite_master WHERE type='index'")
            ).fetchall()
        }

    assert result["attempted"] == 6
    assert result["completed"] == 6
    assert "ix_members_name_lower" in indexes
    assert "ix_events_member_name_lower" in indexes
    assert "ix_events_symbol_type_effective_ts_id" in indexes
    assert "ix_events_symbol_effective_ts_id" in indexes
    assert "idx_events_effective_date_id_desc" in indexes
    assert "ix_events_insider_payload_json_trgm" in indexes


def test_optional_performance_index_lock_timeout_logs_and_continues(caplog):
    engine = create_engine("sqlite:///:memory:", future=True)
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE members (id INTEGER PRIMARY KEY, first_name TEXT, last_name TEXT)"))
        conn.execute(text("CREATE TABLE events (id INTEGER PRIMARY KEY, member_name TEXT)"))

    @event.listens_for(engine, "before_cursor_execute")
    def fail_event_index(_conn, _cursor, statement, parameters, _context, _executemany):
        if "ix_events_member_name_lower" in statement:
            raise OperationalError(
                statement,
                parameters,
                Exception("canceling statement due to lock timeout"),
            )

    with caplog.at_level(logging.WARNING, logger="app.db"):
        result = ensure_optional_performance_indexes(
            engine,
            index_names={"ix_members_name_lower", "ix_events_member_name_lower"},
        )

    assert result["attempted"] == 2
    assert result["completed"] == 1
    assert result["skipped"] == 1
    assert any(
        "startup_step_skipped name=optional_index reason=lock_timeout index=ix_events_member_name_lower" in record.getMessage()
        for record in caplog.records
    )


def test_provider_usage_schema_critical_table_failure_still_fails():
    engine = create_engine("sqlite:///:memory:", future=True)

    @event.listens_for(engine, "before_cursor_execute")
    def fail_provider_usage_table(_conn, _cursor, statement, parameters, _context, _executemany):
        if "CREATE TABLE IF NOT EXISTS provider_usage_events" in statement:
            raise OperationalError(statement, parameters, Exception("forced critical schema failure"))

    with pytest.raises(OperationalError, match="forced critical schema failure"):
        ensure_provider_usage_schema(engine)
