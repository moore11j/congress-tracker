from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from sqlalchemy import MetaData, Table, create_engine, func, inspect, select, text
from sqlalchemy.dialects.postgresql import insert as postgres_insert
from sqlalchemy.engine import Engine, make_url
from sqlalchemy.sql.sqltypes import Boolean

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


def _create_missing_target_tables(source_engine: Engine, target_engine: Engine, source_tables: list[str]) -> None:
    target_existing = set(_table_names(target_engine))
    missing = [name for name in source_tables if name not in target_existing]
    if not missing:
        return
    source_md = MetaData()
    source_md.reflect(bind=source_engine, only=missing)
    target_md = MetaData()
    for table_name in missing:
        source_md.tables[table_name].to_metadata(target_md)
    target_md.create_all(bind=target_engine)


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

    _create_missing_target_tables(source_engine, target_engine, source_tables)
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
