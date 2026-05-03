from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from sqlalchemy import Column, Index, Integer, MetaData, Table, UniqueConstraint, create_engine, text
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from scripts import migrate_sqlite_to_postgres
from scripts import verify_postgres_migration


def _sqlite_db(path: Path):
    return create_engine(f"sqlite:///{path}")


def test_sqlite_datetime_reflection_compiles_to_postgres_timestamp() -> None:
    source_engine = create_engine("sqlite:///:memory:")
    with source_engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE app_settings (
                    key TEXT NOT NULL,
                    value TEXT,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,
                    enabled BOOLEAN DEFAULT 1 NOT NULL,
                    PRIMARY KEY (key)
                )
                """
            )
        )

    source_md = MetaData()
    source_md.reflect(bind=source_engine)
    target_md = MetaData()
    target_table = migrate_sqlite_to_postgres._portable_table_from_sqlite(
        source_md.tables["app_settings"],
        target_md,
        source_engine,
    )

    ddl = str(CreateTable(target_table).compile(dialect=postgresql.dialect()))

    assert "DATETIME" not in ddl.upper()
    assert "TIMESTAMP" in ddl.upper()
    assert "DEFAULT CURRENT_TIMESTAMP" in ddl.upper()
    assert "DEFAULT true" in ddl


def test_schema_failure_writes_failure_log_and_stops_before_copy(tmp_path: Path, monkeypatch) -> None:
    sqlite_path = tmp_path / "source.db"
    log_path = tmp_path / "migration-failure.json"
    source_engine = _sqlite_db(sqlite_path)
    with source_engine.begin() as conn:
        conn.execute(text("CREATE TABLE sample (id INTEGER PRIMARY KEY, created_at DATETIME)"))

    copy_called = False

    def fail_schema(*_args, **_kwargs):
        raise migrate_sqlite_to_postgres.SchemaCreationError(
            "synthetic schema failure",
            table="sample",
            column="created_at",
            object_type="index",
            object_name="ix_sample_created_at",
            sql="CREATE INDEX ix_sample_created_at ON sample (created_at)",
        )

    def copy_table(*_args, **_kwargs):
        nonlocal copy_called
        copy_called = True
        return {"source_count": 1, "inserted_count": 1}

    target_engine = create_engine("sqlite:///:memory:")
    with target_engine.begin() as conn:
        conn.execute(text("CREATE TABLE sample (id INTEGER PRIMARY KEY)"))

    monkeypatch.setattr(migrate_sqlite_to_postgres, "_target_engine", lambda _url: target_engine)
    monkeypatch.setattr(migrate_sqlite_to_postgres, "_create_missing_target_tables", fail_schema)
    monkeypatch.setattr(migrate_sqlite_to_postgres, "_copy_table", copy_table)

    args = argparse.Namespace(
        sqlite_path=str(sqlite_path),
        postgres_url="postgresql://example.invalid/db",
        batch_size=1000,
        allow_non_empty=False,
        replace_target=False,
        confirm_replace_target="",
        log_path=str(log_path),
    )

    try:
        migrate_sqlite_to_postgres.run(args)
    except migrate_sqlite_to_postgres.SchemaCreationError:
        pass
    else:  # pragma: no cover - defensive
        raise AssertionError("expected schema creation failure")

    failure_log = json.loads(log_path.read_text(encoding="utf-8"))
    assert copy_called is False
    assert failure_log["phase"] == "schema_creation"
    assert failure_log["status"] == "failed"
    assert failure_log["failed_table"] == "sample"
    assert failure_log["failed_column"] == "created_at"
    assert failure_log["failed_object_type"] == "index"
    assert failure_log["failed_object_name"] == "ix_sample_created_at"
    assert failure_log["failed_sql"] == "CREATE INDEX ix_sample_created_at ON sample (created_at)"
    assert failure_log["copied_rows"] == 0
    assert "target_postgres_url" not in failure_log


def test_duplicate_unique_index_for_unique_constraint_is_skipped() -> None:
    source_engine = create_engine("sqlite:///:memory:")
    with source_engine.begin() as conn:
        conn.execute(
            text(
                "CREATE TABLE monitoring_alerts ("
                "id INTEGER PRIMARY KEY, "
                "user_id INTEGER, "
                "source_type TEXT, "
                "source_id TEXT, "
                "event_id INTEGER, "
                "CONSTRAINT uq_monitoring_alert_source_event "
                "UNIQUE (user_id, source_type, source_id, event_id)"
                ")"
            )
        )
        conn.execute(
            text(
                """
                CREATE UNIQUE INDEX uq_monitoring_alert_source_event
                ON monitoring_alerts (user_id, source_type, source_id, event_id)
                """
            )
        )

    source_md = MetaData()
    source_md.reflect(bind=source_engine)
    target_md = MetaData()
    target_table = migrate_sqlite_to_postgres._portable_table_from_sqlite(
        source_md.tables["monitoring_alerts"],
        target_md,
        source_engine,
    )

    unique_constraints = [
        constraint
        for constraint in target_table.constraints
        if isinstance(constraint, UniqueConstraint)
        and constraint.name == "uq_monitoring_alert_source_event"
    ]
    matching_indexes = [
        index
        for index in target_table.indexes
        if index.name == "uq_monitoring_alert_source_event"
    ]

    assert len(unique_constraints) == 1
    assert matching_indexes == []


def test_sqlite_autoindexes_are_skipped(monkeypatch) -> None:
    source_engine = create_engine("sqlite:///:memory:")
    with source_engine.begin() as conn:
        conn.execute(text("CREATE TABLE sample (id INTEGER PRIMARY KEY, email TEXT)"))

    class FakeInspector:
        def get_unique_constraints(self, _table_name):
            return []

        def get_foreign_keys(self, _table_name):
            return []

        def get_indexes(self, _table_name):
            return [
                {
                    "name": "sqlite_autoindex_sample_1",
                    "column_names": ["email"],
                    "unique": 1,
                }
            ]

    source_md = MetaData()
    source_md.reflect(bind=source_engine)
    monkeypatch.setattr(migrate_sqlite_to_postgres, "inspect", lambda _engine: FakeInspector())

    target_table = migrate_sqlite_to_postgres._portable_table_from_sqlite(
        source_md.tables["sample"],
        MetaData(),
        source_engine,
    )

    assert target_table.indexes == set()


def test_duplicate_schema_object_names_with_different_definitions_fail_preflight() -> None:
    md = MetaData()
    left = Table("left_table", md, Column("id", Integer, primary_key=True), Column("value", Integer))
    right = Table("right_table", md, Column("id", Integer, primary_key=True), Column("other_value", Integer))
    Index("ix_conflict", left.c.value)
    Index("ix_conflict", right.c.other_value)

    try:
        migrate_sqlite_to_postgres._compile_schema_preflight(create_engine("sqlite:///:memory:"), [left, right])
    except migrate_sqlite_to_postgres.SchemaCreationError as exc:
        assert exc.object_type == "index"
        assert exc.object_name == "ix_conflict"
    else:  # pragma: no cover - defensive
        raise AssertionError("expected duplicate schema object name failure")


def test_no_destructive_sql_is_used_by_default(tmp_path: Path, monkeypatch) -> None:
    sqlite_path = tmp_path / "source.db"
    source_engine = _sqlite_db(sqlite_path)
    with source_engine.begin() as conn:
        conn.execute(text("CREATE TABLE sample (id INTEGER PRIMARY KEY)"))

    truncate_called = False

    def truncate_target(*_args, **_kwargs):
        nonlocal truncate_called
        truncate_called = True

    target_engine = create_engine("sqlite:///:memory:")
    with target_engine.begin() as conn:
        conn.execute(text("CREATE TABLE sample (id INTEGER PRIMARY KEY)"))

    monkeypatch.setattr(migrate_sqlite_to_postgres, "_target_engine", lambda _url: target_engine)
    monkeypatch.setattr(migrate_sqlite_to_postgres, "_create_missing_target_tables", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(migrate_sqlite_to_postgres, "_table_names", lambda engine: ["sample"])
    monkeypatch.setattr(migrate_sqlite_to_postgres, "_total_rows", lambda *_args, **_kwargs: 0)
    monkeypatch.setattr(migrate_sqlite_to_postgres, "_truncate_target", truncate_target)
    monkeypatch.setattr(
        migrate_sqlite_to_postgres,
        "_copy_table",
        lambda *_args, **_kwargs: {"source_count": 0, "inserted_count": 0},
    )
    monkeypatch.setattr(migrate_sqlite_to_postgres, "_reset_postgres_sequences", lambda *_args, **_kwargs: None)

    args = argparse.Namespace(
        sqlite_path=str(sqlite_path),
        postgres_url="postgresql://example.invalid/db",
        batch_size=1000,
        allow_non_empty=False,
        replace_target=False,
        confirm_replace_target="",
        log_path=str(tmp_path / "migration.json"),
    )

    migrate_sqlite_to_postgres.run(args)

    assert truncate_called is False


def test_verification_returns_nonzero_on_mismatch(tmp_path: Path, monkeypatch) -> None:
    sqlite_path = tmp_path / "sqlite.db"
    postgres_path = tmp_path / "postgres.db"
    with _sqlite_db(sqlite_path).begin() as conn:
        conn.execute(text("CREATE TABLE sample (id INTEGER PRIMARY KEY, value TEXT)"))
        conn.execute(text("INSERT INTO sample (id, value) VALUES (1, 'sqlite')"))
    with _sqlite_db(postgres_path).begin() as conn:
        conn.execute(text("CREATE TABLE sample (id INTEGER PRIMARY KEY, value TEXT)"))
        conn.execute(text("INSERT INTO sample (id, value) VALUES (1, 'postgres')"))

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "verify_postgres_migration.py",
            "--sqlite-path",
            str(sqlite_path),
            "--postgres-url",
            f"sqlite:///{postgres_path}",
            "--sample-limit",
            "5",
        ],
    )

    assert verify_postgres_migration.run() == 1
