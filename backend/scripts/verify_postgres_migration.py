from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from sqlalchemy import MetaData, create_engine, func, inspect, select, text
from sqlalchemy.engine import Engine, make_url
from sqlalchemy.sql.schema import Column


SENSITIVE_COLUMN_PARTS = (
    "email",
    "token",
    "password",
    "secret",
    "key",
    "stripe",
    "customer",
    "subscription",
)


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


def _column_type_name(column: Column[Any] | None) -> str:
    return column.type.__class__.__name__.lower() if column is not None else ""


def _is_boolean_column(column: Column[Any] | None) -> bool:
    return "bool" in _column_type_name(column)


def _is_numeric_column(column: Column[Any] | None) -> bool:
    return any(part in _column_type_name(column) for part in ("int", "numeric", "decimal", "float", "real", "double"))


def _is_temporal_column(column: Column[Any] | None) -> bool:
    return any(part in _column_type_name(column) for part in ("date", "time"))


def _is_text_column(column: Column[Any] | None) -> bool:
    return any(part in _column_type_name(column) for part in ("char", "text", "string"))


def _decimal_to_canonical(value: Decimal) -> int | str:
    normalized = value.normalize()
    if normalized == normalized.to_integral_value():
        return int(normalized)
    return format(normalized, "f").rstrip("0").rstrip(".")


def _normalize_datetime(value: datetime) -> str:
    if value.tzinfo is not None:
        value = value.astimezone(timezone.utc).replace(tzinfo=None)
    return value.isoformat(timespec="microseconds").rstrip("0").rstrip(".")


def _try_parse_datetime(value: str) -> str | None:
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    return _normalize_datetime(parsed)


def _normalize_json(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _normalize_json(item) for key, item in sorted(value.items(), key=lambda item: str(item[0]))}
    if isinstance(value, list):
        return [_normalize_json(item) for item in value]
    if isinstance(value, Decimal):
        return _decimal_to_canonical(value)
    if isinstance(value, float):
        return _decimal_to_canonical(Decimal(str(value)))
    if isinstance(value, datetime):
        return _normalize_datetime(value)
    if isinstance(value, date):
        return value.isoformat()
    return value


def _try_parse_json(value: str) -> Any:
    stripped = value.strip()
    if not stripped or stripped[0] not in "[{":
        return None
    try:
        parsed = json.loads(stripped)
    except json.JSONDecodeError:
        return None
    return _normalize_json(parsed)


def _normalize(value: Any, column: Column[Any] | None = None) -> Any:
    if value is None:
        return None
    if _is_boolean_column(column):
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float, Decimal)) and value in {0, 1}:
            return bool(value)
        if isinstance(value, str) and value.strip().lower() in {"0", "1", "true", "false"}:
            return value.strip().lower() in {"1", "true"}
    if isinstance(value, datetime):
        return _normalize_datetime(value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return _decimal_to_canonical(value)
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, float):
        return _decimal_to_canonical(Decimal(str(value)))
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, str):
        stripped = value.strip()
        if _is_numeric_column(column):
            try:
                return _decimal_to_canonical(Decimal(stripped))
            except Exception:
                pass
        if _is_temporal_column(column):
            parsed_datetime = _try_parse_datetime(stripped)
            if parsed_datetime is not None:
                return parsed_datetime
        parsed_json = _try_parse_json(value)
        if parsed_json is not None:
            return parsed_json
    return value


def _raw_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, bytes):
        return value.hex()
    return value


def _normalized_row(row: dict[str, Any], table) -> dict[str, Any]:
    return {key: _normalize(value, table.c.get(key)) for key, value in sorted(row.items())}


def _row_hash(row: dict[str, Any], table=None, normalized: bool = True) -> str:
    payload_row = _normalized_row(row, table) if normalized and table is not None else {
        key: _raw_value(value) for key, value in sorted(row.items())
    }
    payload = json.dumps(payload_row, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _fingerprint(value: Any) -> str:
    payload = json.dumps(_raw_value(value), sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def _is_sensitive_column(column_name: str) -> bool:
    lowered = column_name.lower()
    return any(part in lowered for part in SENSITIVE_COLUMN_PARTS)


def _value_summary(value: Any, column_name: str, column: Column[Any] | None) -> dict[str, Any]:
    summary: dict[str, Any] = {"type": type(value).__name__}
    if _is_sensitive_column(column_name):
        summary["value"] = "<redacted>"
        summary["sha256_16"] = _fingerprint(value)
        return summary
    normalized = _normalize(value, column)
    if isinstance(normalized, (dict, list)):
        summary["value"] = "<json>"
        summary["sha256_16"] = _fingerprint(normalized)
    elif isinstance(value, bytes):
        summary["value"] = "<bytes>"
        summary["length"] = len(value)
        summary["sha256_16"] = _fingerprint(value)
    else:
        text_value = str(normalized)
        summary["value"] = text_value if len(text_value) <= 120 else text_value[:117] + "..."
    return summary


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
        key = {
            name: (
                _value_summary(row_dict.get(name), name, table.c.get(name))
                if _is_sensitive_column(name)
                else _normalize(row_dict.get(name), table.c.get(name))
            )
            for name in pk_cols
        }
        sample.append(
            {
                "key": key,
                "raw_sha256": _row_hash(row_dict, table, normalized=False),
                "normalized_sha256": _row_hash(row_dict, table, normalized=True),
            }
        )
    return sample


def _compare_dict(
    name: str,
    sqlite_value: Any,
    postgres_value: Any,
    mismatches: list[dict[str, Any]],
    classification: str,
) -> None:
    if sqlite_value != postgres_value:
        mismatches.append(
            {
                "classification": classification,
                "check": name,
                "sqlite": sqlite_value,
                "postgres": postgres_value,
            }
        )


def _fetch_sample_rows(engine: Engine, table, pk_cols: list[str], limit: int) -> list[dict[str, Any]]:
    order_cols = [table.c[name] for name in pk_cols if name in table.c] or list(table.c)[:1]
    with engine.connect() as conn:
        return [dict(row) for row in conn.execute(select(table).order_by(*order_cols).limit(limit)).mappings().all()]


def _fetch_rows_by_keys(engine: Engine, table, source_rows: list[dict[str, Any]], pk_cols: list[str]) -> list[dict[str, Any]]:
    if not pk_cols:
        return _fetch_sample_rows(engine, table, [], len(source_rows))
    rows: list[dict[str, Any]] = []
    with engine.connect() as conn:
        for source_row in source_rows:
            criteria = [table.c[name] == source_row.get(name) for name in pk_cols if name in table.c]
            if len(criteria) != len(pk_cols):
                continue
            row = conn.execute(select(table).where(*criteria)).mappings().first()
            if row is not None:
                rows.append(dict(row))
    return rows


def _row_key(row: dict[str, Any], table, pk_cols: list[str]) -> dict[str, Any]:
    return {name: _normalize(row.get(name), table.c.get(name)) for name in pk_cols}


def _sample_entry(row: dict[str, Any], table, pk_cols: list[str]) -> dict[str, Any]:
    return {
        "key": {
            name: (
                _value_summary(row.get(name), name, table.c.get(name))
                if _is_sensitive_column(name)
                else _normalize(row.get(name), table.c.get(name))
            )
            for name in pk_cols
        },
        "raw_sha256": _row_hash(row, table, normalized=False),
        "normalized_sha256": _row_hash(row, table, normalized=True),
    }


def _sample_comparison(
    sqlite_rows: list[dict[str, Any]],
    postgres_rows: list[dict[str, Any]],
    sqlite_table,
    postgres_table,
    pk_cols: list[str],
) -> dict[str, Any]:
    sqlite_by_key = {json.dumps(_row_key(row, sqlite_table, pk_cols), sort_keys=True, default=str): row for row in sqlite_rows}
    postgres_by_key = {
        json.dumps(_row_key(row, postgres_table, pk_cols), sort_keys=True, default=str): row for row in postgres_rows
    }
    diagnostics: list[dict[str, Any]] = []
    actual_mismatches = 0
    normalized_matches = 0

    for key in sorted(set(sqlite_by_key) | set(postgres_by_key)):
        sqlite_row = sqlite_by_key.get(key)
        postgres_row = postgres_by_key.get(key)
        safe_key = json.loads(key)
        if sqlite_row is None or postgres_row is None:
            actual_mismatches += 1
            diagnostics.append(
                {
                    "classification": "actual_value_mismatch",
                    "key": safe_key,
                    "columns": ["<row_missing>"],
                }
            )
            continue

        sqlite_raw_hash = _row_hash(sqlite_row, sqlite_table, normalized=False)
        postgres_raw_hash = _row_hash(postgres_row, postgres_table, normalized=False)
        sqlite_normalized = _normalized_row(sqlite_row, sqlite_table)
        postgres_normalized = _normalized_row(postgres_row, postgres_table)
        if sqlite_normalized == postgres_normalized:
            if sqlite_raw_hash != postgres_raw_hash:
                normalized_matches += 1
                diagnostics.append(
                    {
                        "classification": "normalized_match_raw_mismatch",
                        "key": safe_key,
                        "columns": [
                            name
                            for name in sorted(sqlite_row)
                            if _raw_value(sqlite_row.get(name)) != _raw_value(postgres_row.get(name))
                        ],
                    }
                )
            continue

        actual_mismatches += 1
        details = []
        for name in sorted(set(sqlite_row) | set(postgres_row)):
            sqlite_column = sqlite_table.c.get(name)
            postgres_column = postgres_table.c.get(name)
            sqlite_value = _normalize(sqlite_row.get(name), sqlite_column)
            postgres_value = _normalize(postgres_row.get(name), postgres_column)
            if sqlite_value == postgres_value:
                continue
            details.append(
                {
                    "column": name,
                    "sqlite": _value_summary(sqlite_row.get(name), name, sqlite_column),
                    "postgres": _value_summary(postgres_row.get(name), name, postgres_column),
                    "normalized_equal": False,
                }
            )
        diagnostics.append(
            {
                "classification": "actual_value_mismatch",
                "key": safe_key,
                "columns": [item["column"] for item in details],
                "details": details,
            }
        )

    return {
        "sqlite": [_sample_entry(row, sqlite_table, pk_cols) for row in sqlite_rows],
        "postgres": [_sample_entry(row, postgres_table, pk_cols) for row in postgres_rows],
        "diagnostics": diagnostics,
        "actual_mismatches": actual_mismatches,
        "normalized_matches": normalized_matches,
    }


def _table_report(sqlite_engine: Engine, postgres_engine: Engine, table_name: str, sample_limit: int) -> dict[str, Any]:
    sqlite_md = MetaData()
    postgres_md = MetaData()
    sqlite_md.reflect(bind=sqlite_engine, only=[table_name])
    postgres_md.reflect(bind=postgres_engine, only=[table_name])
    sqlite_table = sqlite_md.tables[table_name]
    postgres_table = postgres_md.tables[table_name]
    pk_cols = _primary_keys(sqlite_engine, table_name)
    report: dict[str, Any] = {"table": table_name, "mismatches": [], "warnings": []}

    sqlite_count = _count(sqlite_engine, sqlite_table)
    postgres_count = _count(postgres_engine, postgres_table)
    report["row_counts"] = {"sqlite": sqlite_count, "postgres": postgres_count}
    _compare_dict("row_count", sqlite_count, postgres_count, report["mismatches"], "row_count_mismatch")

    report["primary_key"] = {"columns": pk_cols}
    for pk_col in pk_cols:
        if pk_col not in sqlite_table.c or pk_col not in postgres_table.c:
            continue
        sqlite_pk = {
            "min": _normalize(_scalar_sql(sqlite_engine, f'SELECT MIN("{pk_col}") FROM "{table_name}"'), sqlite_table.c.get(pk_col)),
            "max": _normalize(_scalar_sql(sqlite_engine, f'SELECT MAX("{pk_col}") FROM "{table_name}"'), sqlite_table.c.get(pk_col)),
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
            "min": _normalize(
                _scalar_sql(postgres_engine, f'SELECT MIN("{pk_col}") FROM "{table_name}"'),
                postgres_table.c.get(pk_col),
            ),
            "max": _normalize(
                _scalar_sql(postgres_engine, f'SELECT MAX("{pk_col}") FROM "{table_name}"'),
                postgres_table.c.get(pk_col),
            ),
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
        comparable_sqlite_pk = dict(sqlite_pk)
        comparable_postgres_pk = dict(postgres_pk)
        if _is_text_column(sqlite_table.c.get(pk_col)) or _is_text_column(postgres_table.c.get(pk_col)):
            comparable_sqlite_pk.pop("min", None)
            comparable_sqlite_pk.pop("max", None)
            comparable_postgres_pk.pop("min", None)
            comparable_postgres_pk.pop("max", None)
            if sqlite_pk != postgres_pk and comparable_sqlite_pk == comparable_postgres_pk:
                report["warnings"].append(
                    {
                        "classification": "normalized_match_raw_mismatch",
                        "check": f"{table_name}.{pk_col}.pk_summary",
                        "columns": [pk_col],
                        "details": [
                            {
                                "column": pk_col,
                                "sqlite": {"type": "pk_summary", "value": {"min": sqlite_pk["min"], "max": sqlite_pk["max"]}},
                                "postgres": {
                                    "type": "pk_summary",
                                    "value": {"min": postgres_pk["min"], "max": postgres_pk["max"]},
                                },
                                "normalized_equal": True,
                            }
                        ],
                    }
                )
        _compare_dict(
            f"{table_name}.{pk_col}.pk_summary",
            comparable_sqlite_pk,
            comparable_postgres_pk,
            report["mismatches"],
            "aggregate_mismatch",
        )

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
        _compare_dict(
            f"{table_name}.{columns}.unique_duplicates",
            sqlite_dupes,
            postgres_dupes,
            report["mismatches"],
            "aggregate_mismatch",
        )
    report["unique_constraints"] = unique_reports

    sqlite_sample_rows = _fetch_sample_rows(sqlite_engine, sqlite_table, pk_cols, sample_limit)
    postgres_sample_rows = _fetch_rows_by_keys(postgres_engine, postgres_table, sqlite_sample_rows, pk_cols)
    sample_comparison = _sample_comparison(
        sqlite_sample_rows,
        postgres_sample_rows,
        sqlite_table,
        postgres_table,
        pk_cols,
    )
    report["sample_hashes"] = {
        "sqlite": sample_comparison["sqlite"],
        "postgres": sample_comparison["postgres"],
    }
    report["sample_diagnostics"] = sample_comparison["diagnostics"]
    for item in sample_comparison["diagnostics"]:
        target = report["warnings"] if item["classification"] == "normalized_match_raw_mismatch" else report["mismatches"]
        target.append({"check": f"{table_name}.sample_hashes", **item})
    return report


def _domain_report(sqlite_engine: Engine, postgres_engine: Engine, shared_tables: set[str]) -> dict[str, Any]:
    report: dict[str, Any] = {"mismatches": [], "warnings": []}
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
        _compare_dict(label, sqlite_counts, postgres_counts, report["mismatches"], "aggregate_mismatch")

    compare_group("events", "event_type", "event_counts_by_event_type")
    compare_group("trade_outcomes", "scoring_status", "trade_outcome_counts_by_status")
    compare_group("user_accounts", "entitlement_tier", "user_counts_by_plan")
    compare_group("government_contracts", "symbol", "government_contract_counts_by_symbol")
    compare_group("price_cache", "symbol", "price_cache_counts_by_symbol")

    if "events" in shared_tables:
        sqlite_latest = _latest_by_type(sqlite_engine, sqlite_md.tables["events"])
        postgres_latest = _latest_by_type(postgres_engine, postgres_md.tables["events"])
        report["latest_event_dates_by_event_type"] = {"sqlite": sqlite_latest, "postgres": postgres_latest}
        _compare_dict(
            "latest_event_dates_by_event_type",
            sqlite_latest,
            postgres_latest,
            report["mismatches"],
            "aggregate_mismatch",
        )

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
            _compare_dict(label, sqlite_value, postgres_value, report["mismatches"], "aggregate_mismatch")

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
            postgres_table = postgres_md.tables[settings_table]
            sqlite_sample_rows = _fetch_sample_rows(sqlite_engine, table, pk_cols, 500)
            sample_comparison = _sample_comparison(
                sqlite_sample_rows,
                _fetch_rows_by_keys(postgres_engine, postgres_table, sqlite_sample_rows, pk_cols),
                table,
                postgres_table,
                pk_cols,
            )
            label = f"{settings_table}_settings_hashes"
            report[label] = {
                "sqlite": sample_comparison["sqlite"],
                "postgres": sample_comparison["postgres"],
                "diagnostics": sample_comparison["diagnostics"],
            }
            for item in sample_comparison["diagnostics"]:
                target = report["warnings"] if item["classification"] == "normalized_match_raw_mismatch" else report["mismatches"]
                target.append({"check": label, **item})

    return report


def _classification_counts(items: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in items:
        classification = str(item.get("classification", "unclassified"))
        counts[classification] = counts.get(classification, 0) + 1
    return counts


def _print_diagnostics(report: dict[str, Any]) -> None:
    print("Sanitized mismatch diagnostics:")
    for table in report["tables"]:
        table_name = table["table"]
        for item in table.get("warnings", []):
            print(
                json.dumps(
                    {
                        "table": table_name,
                        "classification": item.get("classification"),
                        "key": item.get("key"),
                        "columns": item.get("columns", []),
                    },
                    sort_keys=True,
                    default=str,
                )
            )
        for item in table.get("mismatches", []):
            print(
                json.dumps(
                    {
                        "table": table_name,
                        "classification": item.get("classification"),
                        "key": item.get("key"),
                        "columns": item.get("columns", []),
                        "details": item.get("details", []),
                    },
                    sort_keys=True,
                    default=str,
                )
            )
    for item in report.get("domain_checks", {}).get("warnings", []):
        print(
            json.dumps(
                {
                    "domain": True,
                    "classification": item.get("classification"),
                    "check": item.get("check"),
                    "key": item.get("key"),
                    "columns": item.get("columns", []),
                },
                sort_keys=True,
                default=str,
            )
        )
    for item in report.get("domain_checks", {}).get("mismatches", []):
        print(
            json.dumps(
                {
                    "domain": True,
                    "classification": item.get("classification"),
                    "check": item.get("check"),
                    "key": item.get("key"),
                    "columns": item.get("columns", []),
                    "details": item.get("details", []),
                },
                sort_keys=True,
                default=str,
            )
        )


def run() -> int:
    parser = argparse.ArgumentParser(description="Verify a SQLite to PostgreSQL migration without exposing row contents.")
    parser.add_argument("--sqlite-path", required=True)
    parser.add_argument("--postgres-url", required=True)
    parser.add_argument("--json-report", default="")
    parser.add_argument("--sample-limit", type=int, default=25)
    parser.add_argument("--diagnose-mismatches", action="store_true")
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
        "total_warnings": 0,
        "classification_counts": {},
        "warning_classification_counts": {},
    }
    mismatches: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    if sqlite_tables != postgres_tables:
        mismatches.append({"classification": "aggregate_mismatch", "check": "table_list", **report["table_list"]})

    for table_name in sorted(shared_tables):
        table_report = _table_report(sqlite_engine, postgres_engine, table_name, args.sample_limit)
        report["tables"].append(table_report)
        mismatches.extend({"table": table_name, **item} for item in table_report["mismatches"])
        warnings.extend({"table": table_name, **item} for item in table_report.get("warnings", []))

    domain_report = _domain_report(sqlite_engine, postgres_engine, shared_tables)
    report["domain_checks"] = domain_report
    mismatches.extend({"domain": True, **item} for item in domain_report["mismatches"])
    warnings.extend({"domain": True, **item} for item in domain_report.get("warnings", []))

    report["total_mismatches"] = len(mismatches)
    report["total_warnings"] = len(warnings)
    report["mismatches"] = mismatches
    report["warnings"] = warnings
    report["classification_counts"] = _classification_counts(mismatches)
    report["warning_classification_counts"] = _classification_counts(warnings)
    report["status"] = "FAIL" if mismatches else "PASS"

    output = json.dumps(report, indent=2, sort_keys=True, default=str)
    if args.json_report:
        Path(args.json_report).write_text(output, encoding="utf-8")

    warning_text = f" warnings={report['total_warnings']}" if report["total_warnings"] else ""
    print(f"Verification: {report['status']} mismatches={report['total_mismatches']}{warning_text}")
    for item in mismatches[:50]:
        print(json.dumps(item, sort_keys=True, default=str))
    if len(mismatches) > 50:
        print(f"... {len(mismatches) - 50} additional mismatches written to JSON report.")
    if args.diagnose_mismatches:
        _print_diagnostics(report)
    return 1 if mismatches else 0


if __name__ == "__main__":
    raise SystemExit(run())
