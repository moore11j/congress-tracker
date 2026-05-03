from __future__ import annotations

import argparse
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    ForeignKeyConstraint,
    Index,
    Integer,
    JSON,
    LargeBinary,
    MetaData,
    Numeric,
    String,
    Table,
    Text,
    UniqueConstraint,
    create_engine,
    func,
    inspect,
    select,
    text,
)
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.dialects.postgresql import insert as postgres_insert
from sqlalchemy.engine import Engine, make_url
from sqlalchemy.schema import CreateTable

APPROVAL_ENV = "POSTGRES_MIGRATION_TARGET_APPROVED"
APPROVAL_VALUE = "copy-sqlite-to-postgres"


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")


def _redact_url(raw_url: str) -> str:
    try:
        url = make_url(raw_url)
        return str(url.set(password="***") if url.password else url)
    except Exception:
        return "<unparseable-url>"


def _sqlite_readonly_url(path: Path) -> str:
    return f"sqlite:///file:{path.resolve().as_posix()}?mode=ro&uri=true"


def _is_probably_production_postgres(raw_url: str) -> bool:
    parsed = urlparse(raw_url)
    haystack = " ".join([parsed.hostname or "", parsed.path or "", parsed.username or ""]).lower()
    return any(token in haystack for token in ("prod", "production", "fly", "render", "railway"))


def _target_engine(raw_url: str) -> Engine:
    if raw_url.startswith("postgres://"):
        raw_url = raw_url.replace("postgres://", "postgresql+psycopg://", 1)
    elif raw_url.startswith("postgresql://"):
        raw_url = raw_url.replace("postgresql://", "postgresql+psycopg://", 1)
    return create_engine(
        raw_url,
        pool_pre_ping=True,
        pool_size=int(os.getenv("DB_POOL_SIZE", "5")),
        max_overflow=int(os.getenv("DB_MAX_OVERFLOW", "5")),
        pool_timeout=int(os.getenv("DB_POOL_TIMEOUT", "30")),
    )


def _table_names(engine: Engine) -> list[str]:
    return sorted(name for name in inspect(engine).get_table_names() if not name.startswith("sqlite_"))


def _total_rows(engine: Engine, tables: list[str]) -> int:
    total = 0
    md = MetaData()
    md.reflect(bind=engine, only=tables)
    with engine.connect() as conn:
        for table_name in tables:
            total += int(conn.execute(select(func.count()).select_from(md.tables[table_name])).scalar_one() or 0)
    return total


class SchemaCreationError(RuntimeError):
    def __init__(
        self,
        message: str,
        table: str | None = None,
        column: str | None = None,
        object_type: str | None = None,
        object_name: str | None = None,
        sql: str | None = None,
    ) -> None:
        super().__init__(message)
        self.table = table
        self.column = column
        self.object_type = object_type
        self.object_name = object_name
        self.sql = sql


def _portable_column_type(column) -> Any:
    raw_type = str(column.type).upper()
    if "DATETIME" in raw_type or "TIMESTAMP" in raw_type:
        return DateTime(timezone=True)
    if raw_type == "DATE" or raw_type.startswith("DATE("):
        return Date()
    if "BOOL" in raw_type:
        return Boolean()
    if "BIGINT" in raw_type:
        return BigInteger()
    if "INT" in raw_type:
        return Integer()
    if any(token in raw_type for token in ("DOUBLE", "FLOAT", "REAL")):
        return Float()
    if any(token in raw_type for token in ("NUMERIC", "DECIMAL")):
        return Numeric()
    if "JSON" in raw_type:
        return JSON()
    if any(token in raw_type for token in ("BLOB", "BINARY")):
        return LargeBinary()
    if any(token in raw_type for token in ("CHAR", "CLOB", "TEXT", "VARCHAR", "NCHAR", "NVARCHAR")):
        length = getattr(column.type, "length", None)
        return String(length) if length else Text()
    return Text()


def _portable_server_default(column) -> Any | None:
    if column.server_default is None:
        return None
    raw_default = getattr(column.server_default.arg, "text", None)
    if raw_default is None:
        return None
    stripped = raw_default.strip()
    if not stripped:
        return None
    upper = stripped.upper()
    if isinstance(_portable_column_type(column), Boolean):
        normalized = stripped.strip("'\"()").lower()
        if normalized in {"1", "true", "t", "yes", "y", "on"}:
            return text("true")
        if normalized in {"0", "false", "f", "no", "n", "off"}:
            return text("false")
    if upper in {"CURRENT_TIMESTAMP", "CURRENT_DATE", "CURRENT_TIME"}:
        return text(upper)
    if re.fullmatch(r"-?\d+(\.\d+)?", stripped):
        return text(stripped)
    if (stripped.startswith("'") and stripped.endswith("'")) or (stripped.startswith('"') and stripped.endswith('"')):
        return text(stripped)
    return text(stripped)


def _normalized_name(name: str | None) -> str | None:
    return name.lower() if name else None


def _normalized_columns(columns: list[str] | tuple[str, ...]) -> tuple[str, ...]:
    return tuple(column.lower() for column in columns)


def _is_sqlite_autoindex(name: str | None) -> bool:
    return bool(name and name.lower().startswith("sqlite_autoindex_"))


def _register_schema_object(
    seen_names: dict[str, tuple[str, tuple[str, ...], bool]],
    table_name: str,
    object_type: str,
    object_name: str | None,
    columns: tuple[str, ...],
    unique: bool,
) -> None:
    normalized = _normalized_name(object_name)
    if not normalized:
        return
    signature = (table_name.lower(), columns, unique)
    existing = seen_names.get(normalized)
    if existing is None:
        seen_names[normalized] = signature
        return
    if existing == signature:
        return
    raise SchemaCreationError(
        "Duplicate schema object name with different definitions.",
        table=table_name,
        object_type=object_type,
        object_name=object_name,
    )


def _portable_table_from_sqlite(source_table: Table, target_md: MetaData, source_engine: Engine) -> Table:
    columns = []
    for source_column in source_table.columns:
        kwargs: dict[str, Any] = {
            "primary_key": source_column.primary_key,
            "nullable": source_column.nullable,
            "autoincrement": source_column.autoincrement,
        }
        default = _portable_server_default(source_column)
        if default is not None:
            kwargs["server_default"] = default
        columns.append(Column(source_column.name, _portable_column_type(source_column), **kwargs))

    constraints = []
    inspector = inspect(source_engine)
    pk_columns = _normalized_columns([column.name for column in source_table.primary_key.columns])
    unique_column_sets: set[tuple[str, ...]] = set()
    index_signatures: set[tuple[tuple[str, ...], bool]] = set()
    seen_names: dict[str, tuple[str, tuple[str, ...], bool]] = {}
    for unique in inspector.get_unique_constraints(source_table.name):
        unique_name = unique.get("name")
        if _is_sqlite_autoindex(unique_name):
            continue
        unique_columns = [name for name in unique.get("column_names") or [] if name in source_table.c]
        if not unique_columns:
            continue
        normalized_columns = _normalized_columns(unique_columns)
        if normalized_columns == pk_columns:
            continue
        if normalized_columns in unique_column_sets:
            continue
        _register_schema_object(seen_names, source_table.name, "constraint", unique_name, normalized_columns, True)
        unique_column_sets.add(normalized_columns)
        constraints.append(UniqueConstraint(*unique_columns, name=unique_name))
    for fk in inspector.get_foreign_keys(source_table.name):
        constrained = [name for name in fk.get("constrained_columns") or [] if name in source_table.c]
        referred_table = fk.get("referred_table")
        referred_columns = fk.get("referred_columns") or []
        if constrained and referred_table and len(constrained) == len(referred_columns):
            constraints.append(
                ForeignKeyConstraint(
                    constrained,
                    [f"{referred_table}.{column}" for column in referred_columns],
                    name=fk.get("name"),
                )
            )

    target_table = Table(source_table.name, target_md, *columns, *constraints)
    for index in inspector.get_indexes(source_table.name):
        index_name = index.get("name")
        if _is_sqlite_autoindex(index_name):
            continue
        index_columns = [name for name in index.get("column_names") or [] if name in target_table.c]
        if not index_columns or len(index_columns) != len(index.get("column_names") or []):
            print(f"Skipping unsupported index on {source_table.name}: {index_name or '<unnamed>'}")
            continue
        normalized_columns = _normalized_columns(index_columns)
        unique = bool(index.get("unique"))
        if normalized_columns == pk_columns:
            continue
        if unique and normalized_columns in unique_column_sets:
            continue
        index_signature = (normalized_columns, unique)
        if index_signature in index_signatures:
            continue
        _register_schema_object(seen_names, source_table.name, "index", index_name, normalized_columns, unique)
        index_signatures.add(index_signature)
        Index(index_name, *[target_table.c[name] for name in index_columns], unique=unique)
    return target_table


def _find_sqlite_type_column(table: Table, error_text: str) -> str | None:
    lowered = error_text.lower()
    for column in table.columns:
        raw_type = str(column.type).lower()
        if raw_type and raw_type in lowered:
            return column.name
    return None


def _compile_schema_preflight(target_engine: Engine, tables: list[Table]) -> None:
    dialect = target_engine.dialect
    seen_names: dict[str, tuple[str, str, tuple[str, ...], bool]] = {}
    for table in tables:
        try:
            raw_ddl = str(CreateTable(table).compile(dialect=dialect))
            ddl = raw_ddl.upper()
        except Exception as exc:  # pragma: no cover - defensive context wrapping
            raise SchemaCreationError(str(exc), table=table.name, object_type="table", object_name=table.name) from exc
        forbidden = (" DATETIME", "\tDATETIME", "\nDATETIME")
        if any(token in ddl for token in forbidden):
            column_name = _find_sqlite_type_column(table, ddl)
            raise SchemaCreationError(
                "PostgreSQL DDL contains raw SQLite DATETIME type.",
                table=table.name,
                column=column_name,
                object_type="table",
                object_name=table.name,
                sql=raw_ddl,
            )
        for item in list(table.constraints) + list(table.indexes):
            name = _normalized_name(getattr(item, "name", None))
            if not name:
                continue
            columns = _normalized_columns([column.name for column in getattr(item, "columns", [])])
            object_type = "index" if isinstance(item, Index) else "constraint"
            unique = bool(getattr(item, "unique", False) or isinstance(item, UniqueConstraint))
            signature = (table.name.lower(), object_type, columns, unique)
            existing = seen_names.get(name)
            if existing is None:
                seen_names[name] = signature
                continue
            if existing == signature:
                continue
            raise SchemaCreationError(
                "Duplicate schema object name would conflict in PostgreSQL.",
                table=table.name,
                object_type=object_type,
                object_name=getattr(item, "name", None),
            )


def _create_missing_target_tables(source_engine: Engine, target_engine: Engine, source_tables: list[str]) -> None:
    target_existing = set(_table_names(target_engine))
    missing = [name for name in source_tables if name not in target_existing]
    if not missing:
        return
    source_md = MetaData()
    source_md.reflect(bind=source_engine, only=missing)
    target_md = MetaData()
    for table_name in missing:
        _portable_table_from_sqlite(source_md.tables[table_name], target_md, source_engine)
    tables = list(target_md.sorted_tables)
    _compile_schema_preflight(target_engine, tables)
    for table in tables:
        try:
            table.create(bind=target_engine, checkfirst=True)
        except SQLAlchemyError as exc:
            raise SchemaCreationError(
                str(exc),
                table=table.name,
                column=_find_sqlite_type_column(table, str(exc)),
                object_type="table",
                object_name=table.name,
            ) from exc


def _coerce_value(value: Any, column) -> Any:
    if value is None:
        return None
    if isinstance(column.type, Boolean):
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "t", "yes", "y", "on"}
    return value


def _copy_table(source: Engine, target: Engine, table: Table, target_table: Table, batch_size: int) -> dict[str, int]:
    source_count = 0
    inserted_count = 0
    rows: list[dict[str, Any]] = []
    target_columns = {column.name: column for column in target_table.columns}
    source_columns = [column.name for column in table.columns if column.name in target_columns]

    with source.connect() as source_conn, target.begin() as target_conn:
        result = source_conn.execute(select(*[table.c[name] for name in source_columns]))
        for row in result.mappings():
            source_count += 1
            rows.append({name: _coerce_value(row[name], target_columns[name]) for name in source_columns})
            if len(rows) >= batch_size:
                inserted_count += _insert_batch(target_conn, target_table, rows)
                rows = []
        if rows:
            inserted_count += _insert_batch(target_conn, target_table, rows)
    return {"source_count": source_count, "inserted_count": inserted_count}


def _insert_batch(conn, table: Table, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    conn.execute(postgres_insert(table), rows)
    return len(rows)


def _reset_postgres_sequences(engine: Engine, tables: list[Table]) -> None:
    with engine.begin() as conn:
        for table in tables:
            pk_cols = list(table.primary_key.columns)
            if len(pk_cols) != 1:
                continue
            pk_name = pk_cols[0].name
            seq = conn.execute(
                text("SELECT pg_get_serial_sequence(:table_name, :pk_name)"),
                {"table_name": table.name, "pk_name": pk_name},
            ).scalar_one_or_none()
            if not seq:
                continue
            max_id = conn.execute(select(func.max(table.c[pk_name]))).scalar_one()
            if max_id is not None:
                conn.execute(text("SELECT setval(:seq, :value, true)"), {"seq": seq, "value": int(max_id)})


def _truncate_target(engine: Engine, tables: list[str]) -> None:
    quoted = ", ".join(f'"{name}"' for name in tables)
    if not quoted:
        return
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {quoted} RESTART IDENTITY CASCADE"))


def _write_failure_log(args: argparse.Namespace, started_at: str, phase: str, exc: BaseException) -> None:
    if not args.log_path:
        return
    log = {
        "started_at": started_at,
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "phase": phase,
        "status": "failed",
        "failed_table": getattr(exc, "table", None),
        "failed_column": getattr(exc, "column", None),
        "failed_object_type": getattr(exc, "object_type", None),
        "failed_object_name": getattr(exc, "object_name", None),
        "failed_sql": getattr(exc, "sql", None),
        "error": str(exc),
        "copied_rows": 0,
        "tables": [],
    }
    Path(args.log_path).write_text(json.dumps(log, indent=2, sort_keys=True), encoding="utf-8")


def run(args: argparse.Namespace) -> int:
    source_path = Path(args.sqlite_path)
    if not source_path.exists():
        raise SystemExit(f"Source SQLite file does not exist: {source_path}")
    if not args.postgres_url.startswith(("postgres://", "postgresql://", "postgresql+psycopg://")):
        raise SystemExit("Target must be a PostgreSQL DATABASE_URL.")
    if _is_probably_production_postgres(args.postgres_url) and os.getenv(APPROVAL_ENV) != APPROVAL_VALUE:
        raise SystemExit(f"Refusing probable production target without {APPROVAL_ENV}={APPROVAL_VALUE}.")
    if args.replace_target and args.confirm_replace_target != "TRUNCATE POSTGRES TARGET":
        raise SystemExit("--replace-target requires --confirm-replace-target 'TRUNCATE POSTGRES TARGET'.")

    source_engine = create_engine(_sqlite_readonly_url(source_path), pool_pre_ping=True)
    target_engine = _target_engine(args.postgres_url)

    started_at = datetime.now(timezone.utc).isoformat()
    source_tables = _table_names(source_engine)
    if not source_tables:
        raise SystemExit("Source SQLite database has no tables.")

    try:
        _create_missing_target_tables(source_engine, target_engine, source_tables)
    except Exception as exc:
        _write_failure_log(args, started_at, "schema_creation", exc)
        raise
    target_tables = _table_names(target_engine)
    shared_tables = [name for name in source_tables if name in target_tables]

    existing_rows = _total_rows(target_engine, shared_tables)
    if existing_rows and not args.allow_non_empty and not args.replace_target:
        raise SystemExit(
            f"Target Postgres is not empty ({existing_rows} rows). "
            "Use --allow-non-empty for additive testing or explicit --replace-target for truncation."
        )
    if args.replace_target:
        _truncate_target(target_engine, shared_tables)

    source_md = MetaData()
    source_md.reflect(bind=source_engine, only=shared_tables)
    target_md = MetaData()
    target_md.reflect(bind=target_engine, only=shared_tables)

    log: dict[str, Any] = {
        "started_at": started_at,
        "finished_at": None,
        "source_sqlite_path": str(source_path),
        "target_postgres_url": _redact_url(args.postgres_url),
        "tables": [],
    }

    for source_table in source_md.sorted_tables:
        table_name = source_table.name
        target_table = target_md.tables[table_name]
        stats = _copy_table(source_engine, target_engine, source_table, target_table, args.batch_size)
        with target_engine.connect() as conn:
            target_count = int(conn.execute(select(func.count()).select_from(target_table)).scalar_one() or 0)
        entry = {"table": table_name, **stats, "target_count_after": target_count}
        log["tables"].append(entry)
        print(f"{table_name}: source={stats['source_count']} inserted={stats['inserted_count']} target_after={target_count}")
        if stats["source_count"] != stats["inserted_count"]:
            raise SystemExit(f"Copy mismatch for {table_name}: {entry}")

    _reset_postgres_sequences(target_engine, [target_md.tables[name] for name in shared_tables])
    log["finished_at"] = datetime.now(timezone.utc).isoformat()

    log_path = Path(args.log_path) if args.log_path else Path.cwd() / f"postgres-migration-run-{_utc_stamp()}.json"
    log_path.write_text(json.dumps(log, indent=2, sort_keys=True), encoding="utf-8")
    print(f"Migration copy complete. Run log: {log_path}")
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read-only SQLite to PostgreSQL copy migration.")
    parser.add_argument("--sqlite-path", required=True, help="Explicit source SQLite file path.")
    parser.add_argument("--postgres-url", required=True, help="Explicit target PostgreSQL DATABASE_URL.")
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--allow-non-empty", action="store_true")
    parser.add_argument("--replace-target", action="store_true")
    parser.add_argument("--confirm-replace-target", default="")
    parser.add_argument("--log-path")
    return parser.parse_args()


if __name__ == "__main__":
    raise SystemExit(run(parse_args()))
