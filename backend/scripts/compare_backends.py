from __future__ import annotations

import argparse
from contextlib import contextmanager
import hashlib
import json
import re
import socket
from datetime import datetime
from collections.abc import Iterator
from typing import Any
from urllib.parse import urljoin

import requests


DEFAULT_ENDPOINTS = [
    "/health",
    "/api/events?limit=25",
    "/api/events?event_type=congress_trade&limit=25",
    "/api/events?event_type=insider_trade&limit=25",
    "/api/events?event_type=government_contract&limit=25",
    "/api/tickers/AAPL",
    "/api/tickers/MSFT",
    "/api/signals/all",
    "/api/leaderboards/congress-traders?source_mode=congress",
    "/api/leaderboards/congress-traders?source_mode=insiders",
    "/api/plan-config",
]

ADMIN_ENDPOINTS = [
    "/api/admin/settings",
    "/api/admin/users",
    "/api/watchlists",
]

TIMESTAMP_RE = re.compile(r"^\d{4}-\d{2}-\d{2}[T ].*")
GENERATED_LIVE_FIELDS = {
    "current_price",
    "pnl_pct",
    "quote_asof_ts",
    "quote_is_stale",
}
SENSITIVE_KEY_FRAGMENTS = (
    "authorization",
    "bearer",
    "customer",
    "email",
    "key",
    "password",
    "secret",
    "stripe",
    "subscription",
    "token",
)

IDENTITY_KEYS = (
    "id",
    "event_id",
    "symbol",
    "ticker",
    "event_type",
    "source",
    "trade_type",
    "transaction_type",
)
SORT_KEYS = ("report_date", "event_date", "created_at", "updated_at", "id", "symbol")
SAFE_KEY_NAMES = {"feature_key"}


@contextmanager
def _temporary_resolver(overrides: dict[str, str]) -> Iterator[None]:
    if not overrides:
        yield
        return

    original_getaddrinfo = socket.getaddrinfo
    normalized = {host.lower(): ip for host, ip in overrides.items()}

    def getaddrinfo(host: str, port: int, family=0, type=0, proto=0, flags=0):
        override = normalized.get(host.lower())
        if override:
            return original_getaddrinfo(override, port, family, type, proto, flags)
        return original_getaddrinfo(host, port, family, type, proto, flags)

    socket.getaddrinfo = getaddrinfo
    try:
        yield
    finally:
        socket.getaddrinfo = original_getaddrinfo


def _parse_resolve(values: list[str]) -> dict[str, str]:
    overrides: dict[str, str] = {}
    for value in values:
        if "=" not in value:
            raise SystemExit(f"Invalid --resolve value {value!r}; expected host=ip.")
        host, ip = value.split("=", 1)
        host = host.strip().lower()
        ip = ip.strip()
        if not host or not ip:
            raise SystemExit(f"Invalid --resolve value {value!r}; expected host=ip.")
        overrides[host] = ip
    return overrides


def _is_sensitive_key(key: str) -> bool:
    if key.lower() in SAFE_KEY_NAMES:
        return False
    lowered = key.lower()
    return any(fragment in lowered for fragment in SENSITIVE_KEY_FRAGMENTS)


def _normalize(value: Any) -> Any:
    if isinstance(value, dict):
        normalized = {}
        for key in sorted(value):
            if _is_sensitive_key(key):
                normalized[key] = "<redacted>"
            elif key in GENERATED_LIVE_FIELDS:
                continue
            else:
                normalized[key] = _normalize(value[key])
        return normalized
    if isinstance(value, list):
        normalized_items = [_normalize(item) for item in value]
        if all(isinstance(item, dict) and "feature_key" in item and "id" not in item for item in normalized_items):
            return sorted(
                normalized_items,
                key=lambda item: (
                    str(item.get("feature_key") or ""),
                    str(item.get("tier") or ""),
                    str(item.get("required_tier") or ""),
                ),
            )
        return normalized_items
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str):
        if TIMESTAMP_RE.match(value):
            try:
                dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
                return dt.replace(tzinfo=None).isoformat(timespec="seconds")
            except ValueError:
                return value
        return value
    return value


def _type_name(value: Any) -> str:
    if isinstance(value, bool):
        return "bool"
    if value is None:
        return "null"
    if isinstance(value, list):
        return "list"
    if isinstance(value, dict):
        return "object"
    if isinstance(value, (int, float)):
        return "number"
    return type(value).__name__


def _hash(payload: Any) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")).hexdigest()


def _scalar_summary(key: str, value: Any) -> Any:
    if _is_sensitive_key(key):
        return "<redacted>"
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, str):
        if key in IDENTITY_KEYS or key in SORT_KEYS:
            return value
        return {"type": "str", "length": len(value), "sha256": _hash(value)}
    return {"type": _type_name(value), "sha256": _hash(value)}


def _top_level_keys(value: Any) -> list[str]:
    return sorted(value) if isinstance(value, dict) else []


def _find_primary_list(value: Any) -> tuple[str, list[Any]] | tuple[None, None]:
    if isinstance(value, list):
        return "$", value
    if isinstance(value, dict):
        for key in ("items", "events", "data", "results", "leaders", "plans", "limits", "prices"):
            child = value.get(key)
            if isinstance(child, list):
                return key, child
    return None, None


def _row_identity(row: Any) -> dict[str, Any]:
    if not isinstance(row, dict):
        return {"type": _type_name(row), "sha256": _hash(row)}
    return {key: _scalar_summary(key, row.get(key)) for key in IDENTITY_KEYS if key in row}


def _row_sort_fields(row: Any) -> dict[str, Any]:
    if not isinstance(row, dict):
        return {}
    return {key: _scalar_summary(key, row.get(key)) for key in SORT_KEYS if key in row}


def _identity_token(row: Any) -> str:
    identity = _row_identity(row)
    if identity:
        return json.dumps(identity, sort_keys=True, separators=(",", ":"), default=str)
    return _hash(row)


def _field_diff(left: Any, right: Any) -> dict[str, Any]:
    if not isinstance(left, dict) or not isinstance(right, dict):
        return {
            "sqlite_type": _type_name(left),
            "postgres_type": _type_name(right),
            "sqlite_sha256": _hash(left),
            "postgres_sha256": _hash(right),
        }
    left_keys = set(left)
    right_keys = set(right)
    common = sorted(left_keys & right_keys)
    differing = []
    for key in common:
        if left[key] != right[key]:
            differing.append(
                {
                    "field": key,
                    "sqlite": _scalar_summary(key, left[key]),
                    "postgres": _scalar_summary(key, right[key]),
                    "sqlite_type": _type_name(left[key]),
                    "postgres_type": _type_name(right[key]),
                }
            )
        if len(differing) >= 8:
            break
    return {
        "sqlite_only_fields": sorted(left_keys - right_keys),
        "postgres_only_fields": sorted(right_keys - left_keys),
        "differing_fields": differing,
        "nested_differing_paths": _nested_differing_paths(left, right),
    }


def _nested_differing_paths(left: Any, right: Any, path: str = "$", limit: int = 12) -> list[dict[str, Any]]:
    diffs: list[dict[str, Any]] = []

    def walk(a: Any, b: Any, current_path: str) -> None:
        if len(diffs) >= limit:
            return
        if a == b:
            return
        if isinstance(a, dict) and isinstance(b, dict):
            keys = sorted(set(a) | set(b))
            for key in keys:
                child_path = f"{current_path}.{key}"
                if key not in a:
                    diffs.append({"path": child_path, "sqlite": "<missing>", "postgres": _scalar_summary(key, b[key])})
                elif key not in b:
                    diffs.append({"path": child_path, "sqlite": _scalar_summary(key, a[key]), "postgres": "<missing>"})
                else:
                    walk(a[key], b[key], child_path)
                if len(diffs) >= limit:
                    break
            return
        if isinstance(a, list) and isinstance(b, list):
            if len(a) != len(b):
                diffs.append({"path": current_path, "sqlite_length": len(a), "postgres_length": len(b)})
                return
            for index, (left_item, right_item) in enumerate(zip(a, b, strict=True)):
                walk(left_item, right_item, f"{current_path}[{index}]")
                if len(diffs) >= limit:
                    break
            return
        key = current_path.rsplit(".", 1)[-1]
        diffs.append(
            {
                "path": current_path,
                "sqlite": _scalar_summary(key, a),
                "postgres": _scalar_summary(key, b),
                "sqlite_type": _type_name(a),
                "postgres_type": _type_name(b),
            }
        )

    walk(left, right, path)
    return diffs


def _diagnose_bodies(sqlite_body: Any, postgres_body: Any, sample_size: int = 5) -> dict[str, Any]:
    sqlite_list_path, sqlite_list = _find_primary_list(sqlite_body)
    postgres_list_path, postgres_list = _find_primary_list(postgres_body)
    diagnostic: dict[str, Any] = {
        "top_level": {
            "sqlite_type": _type_name(sqlite_body),
            "postgres_type": _type_name(postgres_body),
            "sqlite_keys": _top_level_keys(sqlite_body),
            "postgres_keys": _top_level_keys(postgres_body),
            "sqlite_sha256": _hash(sqlite_body),
            "postgres_sha256": _hash(postgres_body),
        }
    }

    if sqlite_list is not None or postgres_list is not None:
        sqlite_list = sqlite_list or []
        postgres_list = postgres_list or []
        sqlite_tokens = [_identity_token(item) for item in sqlite_list]
        postgres_tokens = [_identity_token(item) for item in postgres_list]
        diagnostic["primary_list"] = {
            "sqlite_path": sqlite_list_path,
            "postgres_path": postgres_list_path,
            "sqlite_length": len(sqlite_list),
            "postgres_length": len(postgres_list),
            "same_identity_set": sorted(sqlite_tokens) == sorted(postgres_tokens),
            "order_only_mismatch": sqlite_tokens != postgres_tokens and sorted(sqlite_tokens) == sorted(postgres_tokens),
            "sqlite_first": [
                {"identity": _row_identity(item), "sort_fields": _row_sort_fields(item)}
                for item in sqlite_list[:sample_size]
            ],
            "postgres_first": [
                {"identity": _row_identity(item), "sort_fields": _row_sort_fields(item)}
                for item in postgres_list[:sample_size]
            ],
        }

        by_identity_postgres = {_identity_token(item): item for item in postgres_list}
        row_diffs = []
        for item in sqlite_list:
            token = _identity_token(item)
            other = by_identity_postgres.get(token)
            if other is None or item == other:
                continue
            row_diffs.append(
                {
                    "identity": _row_identity(item),
                    "sort_fields": {
                        "sqlite": _row_sort_fields(item),
                        "postgres": _row_sort_fields(other),
                    },
                    "field_diff": _field_diff(item, other),
                    "sqlite_sha256": _hash(item),
                    "postgres_sha256": _hash(other),
                }
            )
            if len(row_diffs) >= sample_size:
                break
        diagnostic["row_diffs"] = row_diffs
    else:
        diagnostic["field_diff"] = _field_diff(sqlite_body, postgres_body)

    return diagnostic


def _fetch(base_url: str, endpoint: str, token: str | None) -> dict[str, Any]:
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    url = urljoin(base_url.rstrip("/") + "/", endpoint.lstrip("/"))
    response = requests.get(url, headers=headers, timeout=30)
    content_type = response.headers.get("content-type", "")
    body: Any
    if "application/json" in content_type:
        body = response.json()
    else:
        body = response.text
    return {"status_code": response.status_code, "body": _normalize(body)}


def run() -> int:
    parser = argparse.ArgumentParser(description="Compare GET endpoint behavior between SQLite and PostgreSQL backends.")
    parser.add_argument("--sqlite-backend-url", required=True)
    parser.add_argument("--postgres-backend-url", required=True)
    parser.add_argument("--token", default="")
    parser.add_argument("--admin-token", default="")
    parser.add_argument("--json-report", default="")
    parser.add_argument(
        "--resolve",
        action="append",
        default=[],
        help="Temporarily resolve host=ip inside this process while preserving the original HTTPS hostname for TLS.",
    )
    parser.add_argument(
        "--include-bodies",
        action="store_true",
        help="Include normalized response bodies for mismatches. May expose sensitive data; default is hashes only.",
    )
    parser.add_argument(
        "--diagnose-public-mismatches",
        action="store_true",
        help="Print bounded, redacted structural diagnostics for public endpoint mismatches.",
    )
    args = parser.parse_args()

    endpoints = list(DEFAULT_ENDPOINTS)
    token = args.admin_token or args.token or None
    if token:
        endpoints.extend(ADMIN_ENDPOINTS)

    mismatches = []
    report = {"status": "PASS", "endpoints": []}
    with _temporary_resolver(_parse_resolve(args.resolve)):
        for endpoint in endpoints:
            sqlite_result = _fetch(args.sqlite_backend_url, endpoint, token)
            postgres_result = _fetch(args.postgres_backend_url, endpoint, token)
            equal = sqlite_result == postgres_result
            entry = {
                "endpoint": endpoint,
                "match": equal,
                "sqlite_status": sqlite_result["status_code"],
                "postgres_status": postgres_result["status_code"],
                "sqlite_sha256": _hash(sqlite_result["body"]),
                "postgres_sha256": _hash(postgres_result["body"]),
            }
            if not equal and args.include_bodies:
                entry["sqlite_body"] = sqlite_result["body"]
                entry["postgres_body"] = postgres_result["body"]
            if not equal and args.diagnose_public_mismatches and endpoint in DEFAULT_ENDPOINTS:
                entry["diagnostic"] = _diagnose_bodies(sqlite_result["body"], postgres_result["body"])
            if not equal:
                mismatches.append(entry)
            report["endpoints"].append(entry)
            print(f"{endpoint}: {'PASS' if equal else 'FAIL'} sqlite={entry['sqlite_status']} postgres={entry['postgres_status']}")

    report["status"] = "FAIL" if mismatches else "PASS"
    report["mismatches"] = mismatches
    if args.json_report:
        with open(args.json_report, "w", encoding="utf-8") as handle:
            json.dump(report, handle, indent=2, sort_keys=True, default=str)

    if mismatches:
        print("Endpoint mismatches:")
        for mismatch in mismatches:
            printable = {key: value for key, value in mismatch.items() if key not in {"sqlite_body", "postgres_body"}}
            print(json.dumps(printable, indent=2, sort_keys=True, default=str))
    return 1 if mismatches else 0


if __name__ == "__main__":
    raise SystemExit(run())
