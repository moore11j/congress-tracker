from __future__ import annotations

import argparse
import hashlib
import json
import re
from datetime import datetime
from typing import Any
from urllib.parse import urljoin

import requests


DEFAULT_ENDPOINTS = [
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
SENSITIVE_KEYS = {
    "email",
    "customer_email",
    "password",
    "password_hash",
    "token",
    "access_token",
    "refresh_token",
    "stripe_customer_id",
    "stripe_subscription_id",
}


def _normalize(value: Any) -> Any:
    if isinstance(value, dict):
        normalized = {}
        for key in sorted(value):
            if key.lower() in SENSITIVE_KEYS:
                normalized[key] = "<redacted>"
            else:
                normalized[key] = _normalize(value[key])
        return normalized
    if isinstance(value, list):
        return [_normalize(item) for item in value]
    if isinstance(value, str):
        if TIMESTAMP_RE.match(value):
            try:
                return datetime.fromisoformat(value.replace("Z", "+00:00")).isoformat().replace("+00:00", "Z")
            except ValueError:
                return value
        return value
    return value


def _hash(payload: Any) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")).hexdigest()


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
        "--include-bodies",
        action="store_true",
        help="Include normalized response bodies for mismatches. May expose sensitive data; default is hashes only.",
    )
    args = parser.parse_args()

    endpoints = list(DEFAULT_ENDPOINTS)
    token = args.admin_token or args.token or None
    if token:
        endpoints.extend(ADMIN_ENDPOINTS)

    mismatches = []
    report = {"status": "PASS", "endpoints": []}
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
            print(json.dumps(mismatch, indent=2, sort_keys=True, default=str))
    return 1 if mismatches else 0


if __name__ == "__main__":
    raise SystemExit(run())
