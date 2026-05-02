from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from sqlalchemy import MetaData, create_engine, func, inspect, select, text
from sqlalchemy.engine import Engine, make_url


def _sqlite_readonly_url(path: Path) -> str:
    return f"sqlite:///file:{path.resolve().as_posix()}?mode=ro&uri=true"


def _postgres_url(raw_url: str) -> str:
    if raw_url.startswith("postgres://"):
        return raw_url.replace("postgres://", "postgresql+psycopg://", 1)
    if raw_url.startswith("postgresql://"):
        return raw_url.replace("postgresql://", "postgresql+psycopg://", 1)
    return raw_url


def _engine(raw_url: str) -> Engine:
    return create_engine(raw_url, pool_pre_ping=True)


def _normalize(value: Any) -> Any:
    if isinstance(value, datetime):
        text_value = value.isoformat()
        return text_value.replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, str):
        stripped = value.strip()
        if stripped in {"0", "1"}:
            return stripped
        try:
            parsed = datetime.fromisoformat(stripped.replace("Z", "+00:00"))
            return parsed.isoformat().replace("+00:00", "Z")
        except ValueError:
            return value
    return value


def _row_hash(row: dict[str, Any]) -> str:
    normalized = {key: _normalize(value) for key, value in sorted(row.items())}
    payload = json.dumps(normalized, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _table_names(engine: Engine) -> list[str]:
    return sorted(name for name in inspect(engine).get_table_names() if not name.startswith("sqlite_"))


def _primary_keys(engine: Engine, table_name: str) -> list[str]:
    return list(inspect(engine).get_pk_constraint(table_name).get("constrained_columns") or [])


def _unique_constraints(engine: Engine, table_name: str) -> list[list[str]]:
    inspector = inspect(engine)
    uniques = [list(item.get("column_names") or []) for item in inspector.get_unique_constraints(table_name)]
    for index in inspector.get_indexes(table_name):
        if index.get("unique"):
            uniques.append(list(index.get("column_names") or []))
    return [columns for columns in uniques if columns]


def _count(engine: Engine, table) -> int:
    with engine.connect() as conn:
        return int(conn.execute(select(func.count()).select_from(table)).scalar_one() or 0)


def _scalar_sql(engine: Engine, statement: str, params: dict[str, Any] | None = None) -> Any:
    with engine.connect() as conn:
        return conn.execute(text(statement), params or {}).scalar_one_or_none()


def _group_counts(engine: Engine, table, column_name: str) -> dict[str, int]:
    if column_name not in table.c:
        return {}
    with engine.connect() as conn:
        rows = conn.execute(select(table.c[column_name], func.count()).group_by(table.c[column_name])).all()
    return {str(key): int(value or 0) for key, value in rows}


def _latest_by_type(engine: Engine, table) -> dict[str, str | None]:
    if "event_type" not in table.c:
        return {}
    date_col = table.c.event_date if "event_date" in table.c else table.c.ts if "ts" in table.c else None
    if date_col is None:
        return {}
    with engine.connect() as conn:
        rows = conn.execute(select(table.c.event_type, func.max(date_col)).group_by(table.c.event_type)).all()
    return {str(key): str(_normalize(value)) if value is not None else None for key, value in rows}


def _sample_hashes(engine: Engine, table, pk_cols: list[str], limit: int) -> list[dict[str, Any]]:
    order_cols = [table.c[name] for name in pk_cols if name in table.c] or list(table.c)[:1]
    with engine.connect() as conn:
        rows = conn.execute(select(table).order_by(*order_cols).limit(limit)).mappings().all()
    sample = []
    for row in rows:
        row_dict = dict(row)
        key = {name: _normalize(row_dict.get(name)) for name in pk_cols}
        sample.append({"key": key, "sha256": _row_hash(row_dict)})
    return sample


def _compare_dict(name: str, sqlite_value: Any, postgres_value: Any, mismatches: list[dict[str, Any]]) -> None:
    if sqlite_value != postgres_value:
        mismatches.append({"check": name, "sqlite": sqlite_value, "postgres": postgres_value})


def _table_report(sqlite_engine: Engine, postgres_engine: Engine, table_name: str, sample_limit: int) -> dict[str, Any]:
    sqlite_md = MetaData()
    postgres_md = MetaData()
    sqlite_md.reflect(bind=sqlite_engine, only=[table_name])
    postgres_md.reflect(bind=postgres_engine, only=[table_name])
    sqlite_table = sqlite_md.tables[table_name]
    postgres_table = postgres_md.tables[table_name]
    pk_cols = _primary_keys(sqlite_engine, table_name)
    report: dict[str, Any] = {"table": table_name, "mismatches": []}

    sqlite_count = _count(sqlite_engine, sqlite_table)
    postgres_count = _count(postgres_engine, postgres_table)
    report["row_counts"] = {"sqlite": sqlite_count, "postgres": postgres_count}
    _compare_dict("row_count", sqlite_count, postgres_count, report["mismatches"])

    report["primary_key"] = {"columns": pk_cols}
    for pk_col in pk_cols:
        if pk_col not in sqlite_table.c or pk_col not in postgres_table.c:
            continue
        sqlite_pk = {
            "min": _normalize(_scalar_sql(sqlite_engine, f'SELECT MIN("{pk_col}") FROM "{table_name}"')),
            "max": _normalize(_scalar_sql(sqlite_engine, f'SELECT MAX("{pk_col}") FROM "{table_name}"')),
            "nulls": int(_scalar_sql(sqlite_engine, f'SELECT COUNT(*) FROM "{table_name}" WHERE "{pk_col}" IS NULL') or 0),
            "duplicates": int(
                _scalar_sql(
                    sqlite_engine,
                    f'SELECT COUNT(*) FROM (SELECT "{pk_col}" FROM "{table_name}" GROUP BY "{pk_col}" HAVING COUNT(*) > 1) x',
                )
                or 0
            ),
        }
        postgres_pk = {
            "min": _normalize(_scalar_sql(postgres_engine, f'SELECT MIN("{pk_col}") FROM "{table_name}"')),
            "max": _normalize(_scalar_sql(postgres_engine, f'SELECT MAX("{pk_col}") FROM "{table_name}"')),
            "nulls": int(_scalar_sql(postgres_engine, f'SELECT COUNT(*) FROM "{table_name}" WHERE "{pk_col}" IS NULL') or 0),
            "duplicates": int(
                _scalar_sql(
                    postgres_engine,
                    f'SELECT COUNT(*) FROM (SELECT "{pk_col}" FROM "{table_name}" GROUP BY "{pk_col}" HAVING COUNT(*) > 1) x',
                )
                or 0
            ),
        }
        report["primary_key"][pk_col] = {"sqlite": sqlite_pk, "postgres": postgres_pk}
        _compare_dict(f"{table_name}.{pk_col}.pk_summary", sqlite_pk, postgres_pk, report["mismatches"])

    unique_reports = []
    for columns in _unique_constraints(sqlite_engine, table_name):
        if not all(column in sqlite_table.c and column in postgres_table.c for column in columns):
            continue
        column_sql = ", ".join(f'"{column}"' for column in columns)
        duplicate_sql = f'SELECT COUNT(*) FROM (SELECT {column_sql} FROM "{table_name}" GROUP BY {column_sql} HAVING COUNT(*) > 1) x'
        sqlite_dupes = int(_scalar_sql(sqlite_engine, duplicate_sql) or 0)
        postgres_dupes = int(_scalar_sql(postgres_engine, duplicate_sql) or 0)
        entry = {"columns": columns, "sqlite_duplicates": sqlite_dupes, "postgres_duplicates": postgres_dupes}
        unique_reports.append(entry)
        _compare_dict(f"{table_name}.{columns}.unique_duplicates", sqlite_dupes, postgres_dupes, report["mismatches"])
    report["unique_constraints"] = unique_reports

    sqlite_sample = _sample_hashes(sqlite_engine, sqlite_table, pk_cols, sample_limit)
    postgres_sample = _sample_hashes(postgres_engine, postgres_table, pk_cols, sample_limit)
    report["sample_hashes"] = {"sqlite": sqlite_sample, "postgres": postgres_sample}
    _compare_dict(f"{table_name}.sample_hashes", sqlite_sample, postgres_sample, report["mismatches"])
    return report


def _domain_report(sqlite_engine: Engine, postgres_engine: Engine, shared_tables: set[str]) -> dict[str, Any]:
    report: dict[str, Any] = {"mismatches": []}
    sqlite_md = MetaData()
    postgres_md = MetaData()
    sqlite_md.reflect(bind=sqlite_engine, only=list(shared_tables))
    postgres_md.reflect(bind=postgres_engine, only=list(shared_tables))

    def compare_group(table_name: str, column_name: str, label: str) -> None:
        if table_name not in shared_tables:
            return
        sqlite_counts = _group_counts(sqlite_engine, sqlite_md.tables[table_name], column_name)
        postgres_counts = _group_counts(postgres_engine, postgres_md.tables[table_name], column_name)
        report[label] = {"sqlite": sqlite_counts, "postgres": postgres_counts}
        _compare_dict(label, sqlite_counts, postgres_counts, report["mismatches"])

    compare_group("events", "event_type", "event_counts_by_event_type")
    compare_group("trade_outcomes", "scoring_status", "trade_outcome_counts_by_status")
    compare_group("user_accounts", "entitlement_tier", "user_counts_by_plan")
    compare_group("government_contracts", "symbol", "government_contract_counts_by_symbol")
    compare_group("price_cache", "symbol", "price_cache_counts_by_symbol")

    if "events" in shared_tables:
        sqlite_latest = _latest_by_type(sqlite_engine, sqlite_md.tables["events"])
        postgres_latest = _latest_by_type(postgres_engine, postgres_md.tables["events"])
        report["latest_event_dates_by_event_type"] = {"sqlite": sqlite_latest, "postgres": postgres_latest}
        _compare_dict("latest_event_dates_by_event_type", sqlite_latest, postgres_latest, report["mismatches"])

    if "user_accounts" in shared_tables:
        checks = {
            "admin_user_count": (
                "lower(coalesce(role, '')) = 'admin'",
                "lower(coalesce(role, '')) = 'admin'",
            ),
            "suspended_user_count": (
                "coalesce(is_suspended, 0) = 1",
                "coalesce(is_suspended, false) = true",
            ),
        }
        for label, (sqlite_where, postgres_where) in checks.items():
            sqlite_value = int(_scalar_sql(sqlite_engine, f'SELECT COUNT(*) FROM "user_accounts" WHERE {sqlite_where}') or 0)
            postgres_value = int(_scalar_sql(postgres_engine, f'SELECT COUNT(*) FROM "user_accounts" WHERE {postgres_where}') or 0)
            report[label] = {"sqlite": sqlite_value, "postgres": postgres_value}
            _compare_dict(label, sqlite_value, postgres_value, report["mismatches"])

    for table_name, user_col in {
        "watchlists": "owner_user_id",
        "saved_screens": "user_id",
        "monitoring_alerts": "user_id",
    }.items():
        compare_group(table_name, user_col, f"{table_name}_counts_by_user")

    for settings_table in ("app_settings", "plan_limits", "plan_prices"):
        if settings_table in shared_tables:
            table = sqlite_md.tables[settings_table]
            pk_cols = _primary_keys(sqlite_engine, settings_table)
            sqlite_sample = _sample_hashes(sqlite_engine, table, pk_cols, 500)
            postgres_sample = _sample_hashes(postgres_engine, postgres_md.tables[settings_table], pk_cols, 500)
            label = f"{settings_table}_settings_hashes"
            report[label] = {"sqlite": sqlite_sample, "postgres": postgres_sample}
            _compare_dict(label, sqlite_sample, postgres_sample, report["mismatches"])

    return report


def run() -> int:
    parser = argparse.ArgumentParser(description="Verify a SQLite to PostgreSQL migration without exposing row contents.")
    parser.add_argument("--sqlite-path", required=True)
    parser.add_argument("--postgres-url", required=True)
    parser.add_argument("--json-report", default="")
    parser.add_argument("--sample-limit", type=int, default=25)
    args = parser.parse_args()

    sqlite_path = Path(args.sqlite_path)
    if not sqlite_path.exists():
        raise SystemExit(f"SQLite source does not exist: {sqlite_path}")

    sqlite_engine = _engine(_sqlite_readonly_url(sqlite_path))
    postgres_engine = _engine(_postgres_url(args.postgres_url))

    sqlite_tables = set(_table_names(sqlite_engine))
    postgres_tables = set(_table_names(postgres_engine))
    shared_tables = sqlite_tables & postgres_tables
    report: dict[str, Any] = {
        "status": "PASS",
        "table_list": {
            "sqlite_only": sorted(sqlite_tables - postgres_tables),
            "postgres_only": sorted(postgres_tables - sqlite_tables),
            "shared": sorted(shared_tables),
        },
        "tables": [],
        "domain_checks": {},
        "total_mismatches": 0,
    }
    mismatches: list[dict[str, Any]] = []
    if sqlite_tables != postgres_tables:
        mismatches.append({"check": "table_list", **report["table_list"]})

    for table_name in sorted(shared_tables):
        table_report = _table_report(sqlite_engine, postgres_engine, table_name, args.sample_limit)
        report["tables"].append(table_report)
        mismatches.extend({"table": table_name, **item} for item in table_report["mismatches"])

    domain_report = _domain_report(sqlite_engine, postgres_engine, shared_tables)
    report["domain_checks"] = domain_report
    mismatches.extend({"domain": True, **item} for item in domain_report["mismatches"])

    report["total_mismatches"] = len(mismatches)
    report["mismatches"] = mismatches
    report["status"] = "FAIL" if mismatches else "PASS"

    output = json.dumps(report, indent=2, sort_keys=True, default=str)
    if args.json_report:
        Path(args.json_report).write_text(output, encoding="utf-8")

    print(f"Verification: {report['status']} mismatches={report['total_mismatches']}")
    for item in mismatches[:50]:
        print(json.dumps(item, sort_keys=True, default=str))
    if len(mismatches) > 50:
        print(f"... {len(mismatches) - 50} additional mismatches written to JSON report.")
    return 1 if mismatches else 0


if __name__ == "__main__":
    raise SystemExit(run())
