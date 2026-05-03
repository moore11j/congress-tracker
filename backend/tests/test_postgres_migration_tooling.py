from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from sqlalchemy import BigInteger, Column, Index, Integer, MetaData, Table, UniqueConstraint, create_engine, text
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


def test_large_amount_integer_columns_compile_as_bigint() -> None:
    source_engine = create_engine("sqlite:///:memory:")
    with source_engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE events (
                    id INTEGER PRIMARY KEY,
                    amount_min INTEGER,
                    amount_max INTEGER
                )
                """
            )
        )
        conn.execute(
            text(
                "INSERT INTO events (id, amount_min, amount_max) "
                "VALUES (1, 2147483648, 3000000000)"
            )
        )

    source_md = MetaData()
    source_md.reflect(bind=source_engine)
    target_table = migrate_sqlite_to_postgres._portable_table_from_sqlite(
        source_md.tables["events"],
        MetaData(),
        source_engine,
    )
    ddl = str(CreateTable(target_table).compile(dialect=postgresql.dialect())).upper()

    assert isinstance(target_table.c.amount_min.type, BigInteger)
    assert isinstance(target_table.c.amount_max.type, BigInteger)
    assert "AMOUNT_MIN BIGINT" in ddl
    assert "AMOUNT_MAX BIGINT" in ddl


def test_integer_bounds_promote_non_heuristic_column_to_bigint() -> None:
    source_engine = create_engine("sqlite:///:memory:")
    with source_engine.begin() as conn:
        conn.execute(text("CREATE TABLE sample (id INTEGER PRIMARY KEY, ordinary INTEGER)"))
        conn.execute(text("INSERT INTO sample (id, ordinary) VALUES (1, 2147483648)"))

    source_md = MetaData()
    source_md.reflect(bind=source_engine)
    target_table = migrate_sqlite_to_postgres._portable_table_from_sqlite(
        source_md.tables["sample"],
        MetaData(),
        source_engine,
    )

    assert isinstance(target_table.c.ordinary.type, BigInteger)


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
        table_batch_size=[],
        only_table=[],
        skip_table=[],
        allow_non_empty=False,
        replace_target=False,
        confirm_replace_target="",
        log_path=str(tmp_path / "migration.json"),
    )

    migrate_sqlite_to_postgres.run(args)

    assert truncate_called is False


def test_default_batch_size_is_conservative(monkeypatch) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "migrate_sqlite_to_postgres.py",
            "--sqlite-path",
            "source.db",
            "--postgres-url",
            "postgresql://example.invalid/db",
        ],
    )

    assert migrate_sqlite_to_postgres.parse_args().batch_size == 500


def test_table_filters_select_expected_tables() -> None:
    tables = ["app_settings", "events", "members"]

    assert migrate_sqlite_to_postgres._filter_source_tables(tables, ["events"], None) == ["events"]
    assert migrate_sqlite_to_postgres._filter_source_tables(tables, None, ["events"]) == [
        "app_settings",
        "members",
    ]


def test_copy_table_commits_each_batch_before_failure(tmp_path: Path, monkeypatch) -> None:
    source_path = tmp_path / "source.db"
    target_path = tmp_path / "target.db"
    source_engine = _sqlite_db(source_path)
    target_engine = _sqlite_db(target_path)
    with source_engine.begin() as conn:
        conn.execute(text("CREATE TABLE sample (id INTEGER PRIMARY KEY, value TEXT)"))
        conn.execute(text("INSERT INTO sample (id, value) VALUES (1, 'a'), (2, 'b'), (3, 'c')"))
    with target_engine.begin() as conn:
        conn.execute(text("CREATE TABLE sample (id INTEGER PRIMARY KEY, value TEXT)"))

    source_md = MetaData()
    target_md = MetaData()
    source_md.reflect(bind=source_engine)
    target_md.reflect(bind=target_engine)
    calls = 0

    def insert_batch(conn, table, rows):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("synthetic connection drop")
        conn.execute(table.insert(), rows)
        return len(rows)

    monkeypatch.setattr(migrate_sqlite_to_postgres, "_insert_batch", insert_batch)

    try:
        migrate_sqlite_to_postgres._copy_table(
            source_engine,
            target_engine,
            source_md.tables["sample"],
            target_md.tables["sample"],
            batch_size=2,
        )
    except migrate_sqlite_to_postgres.RowCopyError as exc:
        assert exc.table == "sample"
        assert exc.inserted_rows_before_failure == 2
        assert exc.batch_size == 2
    else:  # pragma: no cover - defensive
        raise AssertionError("expected row copy failure")

    with target_engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM sample")).scalar_one() == 2


def test_events_preflight_reports_counts_and_bounds_without_payloads() -> None:
    source_engine = create_engine("sqlite:///:memory:")
    with source_engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE events (
                    id INTEGER PRIMARY KEY,
                    event_type TEXT,
                    symbol TEXT,
                    source_type TEXT,
                    source_id TEXT,
                    filing_url TEXT,
                    amount_min INTEGER,
                    amount_max INTEGER
                )
                """
            )
        )
        conn.execute(
            text(
                "INSERT INTO events "
                "(id, event_type, symbol, source_type, source_id, filing_url, amount_min, amount_max) "
                "VALUES (1, 'trade', 'ABCD', 'source', 'secret-source-id', 'https://example.invalid/private', 5, 3000000000)"
            )
        )

    md = MetaData()
    md.reflect(bind=source_engine)
    summary = migrate_sqlite_to_postgres._events_source_preflight(source_engine, md.tables["events"])

    assert summary["row_count"] == 1
    assert summary["amount_min_min"] == 5
    assert summary["amount_max_max"] == 3000000000
    assert summary["source_id_max_length"] == len("secret-source-id")
    assert "secret-source-id" not in json.dumps(summary)
    assert "https://example.invalid/private" not in json.dumps(summary)


def test_row_copy_failure_log_contains_progress_without_url(tmp_path: Path) -> None:
    log_path = tmp_path / "failure.json"
    args = argparse.Namespace(batch_size=250, log_path=str(log_path))
    exc = migrate_sqlite_to_postgres.RowCopyError(
        "server closed the connection unexpectedly",
        table="events",
        inserted_rows_before_failure=1500,
        batch_size=250,
    )

    migrate_sqlite_to_postgres._write_failure_log(
        args,
        "2026-05-03T00:00:00+00:00",
        "row_copy",
        exc,
        copied_tables=[{"table": "app_settings", "inserted_count": 9}],
    )

    failure_log = json.loads(log_path.read_text(encoding="utf-8"))
    assert failure_log["failed_phase"] == "row_copy"
    assert failure_log["failed_table"] == "events"
    assert failure_log["inserted_rows_before_failure"] == 1500
    assert failure_log["batch_size"] == 250
    assert failure_log["copied_rows"] == 9
    assert "target_postgres_url" not in failure_log


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
