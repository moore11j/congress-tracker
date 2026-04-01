from __future__ import annotations

from datetime import date
from typing import Any

import requests

USASPENDING_BASE_URL = "https://api.usaspending.gov"


class USAspendingClientError(RuntimeError):
    pass


def _as_float(value: Any) -> float:
    try:
        parsed = float(value)
    except Exception:
        return 0.0
    return parsed if parsed == parsed else 0.0


def _as_int(value: Any) -> int:
    try:
        return int(value)
    except Exception:
        return 0


def fetch_recipient_contract_spending(
    *,
    start_date: date,
    end_date: date,
    page: int = 1,
    limit: int = 100,
    timeout_s: int = 45,
) -> dict[str, Any]:
    """Return recipient-level contract aggregates from USAspending."""

    payload = {
        "filters": {
            "time_period": [{"start_date": start_date.isoformat(), "end_date": end_date.isoformat()}],
            "award_type_codes": ["A", "B", "C", "D"],
        },
        "category": "recipient",
        "limit": limit,
        "page": page,
    }

    try:
        response = requests.post(
            f"{USASPENDING_BASE_URL}/api/v2/search/spending_by_category/recipient/",
            json=payload,
            timeout=timeout_s,
        )
    except requests.RequestException as exc:
        raise USAspendingClientError(f"USAspending request failed: {exc}") from exc

    if response.status_code == 429:
        raise USAspendingClientError("USAspending rate-limited (429)")
    if response.status_code >= 400:
        raise USAspendingClientError(f"USAspending error ({response.status_code}): {response.text[:200]}")

    try:
        payload = response.json()
    except ValueError as exc:
        raise USAspendingClientError("USAspending returned invalid JSON") from exc

    results = payload.get("results") if isinstance(payload, dict) else None
    page_metadata = payload.get("page_metadata") if isinstance(payload, dict) else None

    rows: list[dict[str, Any]] = []
    if isinstance(results, list):
        for row in results:
            if not isinstance(row, dict):
                continue
            rows.append(
                {
                    "recipient_name": row.get("name") or row.get("recipient_name") or "",
                    "amount": _as_float(row.get("amount") or row.get("aggregated_amount") or row.get("obligated_amount")),
                    "award_count": _as_int(row.get("count") or row.get("award_count") or row.get("transaction_count")),
                    "raw": row,
                }
            )

    has_next = False
    if isinstance(page_metadata, dict):
        has_next = bool(page_metadata.get("hasNext") or page_metadata.get("has_next_page"))
        if not has_next:
            current_page = _as_int(page_metadata.get("page") or page)
            total_pages = _as_int(page_metadata.get("total_pages") or 0)
            has_next = total_pages > 0 and current_page < total_pages

    return {"results": rows, "has_next": has_next}


def fetch_recipient_contract_award_details(
    *,
    start_date: date,
    end_date: date,
    recipient_name: str,
    page: int = 1,
    limit: int = 25,
    timeout_s: int = 45,
) -> dict[str, Any]:
    """Return contract award/transaction-level rows for a recipient."""

    payload = {
        "fields": [
            "Award ID",
            "generated_unique_award_id",
            "Recipient Name",
            "Action Date",
            "Award Amount",
            "Base Obligation Date",
            "Awarding Agency",
            "Awarding Sub Agency",
            "Awarding Department",
            "Description",
            "PIID",
            "Contract Award Unique Key",
            "recipient_id",
        ],
        "filters": {
            "time_period": [{"start_date": start_date.isoformat(), "end_date": end_date.isoformat()}],
            "award_type_codes": ["A", "B", "C", "D"],
            "recipient_search_text": [recipient_name],
        },
        "page": page,
        "limit": limit,
        "sort": "Action Date",
        "order": "desc",
    }

    try:
        response = requests.post(
            f"{USASPENDING_BASE_URL}/api/v2/search/spending_by_award/",
            json=payload,
            timeout=timeout_s,
        )
    except requests.RequestException as exc:
        raise USAspendingClientError(f"USAspending request failed: {exc}") from exc

    if response.status_code == 429:
        raise USAspendingClientError("USAspending rate-limited (429)")
    if response.status_code >= 400:
        raise USAspendingClientError(f"USAspending error ({response.status_code}): {response.text[:200]}")

    try:
        payload = response.json()
    except ValueError as exc:
        raise USAspendingClientError("USAspending returned invalid JSON") from exc

    results = payload.get("results") if isinstance(payload, dict) else None
    page_metadata = payload.get("page_metadata") if isinstance(payload, dict) else None

    rows: list[dict[str, Any]] = []
    if isinstance(results, list):
        for row in results:
            if not isinstance(row, dict):
                continue
            rows.append(
                {
                    "recipient_name": row.get("Recipient Name") or row.get("recipient_name") or recipient_name,
                    "award_amount": _as_float(
                        row.get("Award Amount")
                        or row.get("award_amount")
                        or row.get("Transaction Amount")
                        or row.get("transaction_amount")
                        or row.get("Federal Action Obligation")
                        or row.get("federal_action_obligation")
                    ),
                    "award_date": row.get("Action Date") or row.get("action_date") or row.get("Base Obligation Date"),
                    "awarding_agency": row.get("Awarding Agency") or row.get("awarding_agency"),
                    "awarding_department": row.get("Awarding Department")
                    or row.get("awarding_department")
                    or row.get("Awarding Sub Agency")
                    or row.get("awarding_sub_agency"),
                    "award_description": row.get("Description") or row.get("description"),
                    "award_id": row.get("Award ID") or row.get("award_id") or row.get("generated_unique_award_id"),
                    "contract_id": row.get("PIID")
                    or row.get("piid")
                    or row.get("Contract Award Unique Key")
                    or row.get("contract_award_unique_key"),
                    "raw": row,
                }
            )

    has_next = False
    if isinstance(page_metadata, dict):
        has_next = bool(page_metadata.get("hasNext") or page_metadata.get("has_next_page"))
        if not has_next:
            current_page = _as_int(page_metadata.get("page") or page)
            total_pages = _as_int(page_metadata.get("total_pages") or 0)
            has_next = total_pages > 0 and current_page < total_pages

    return {"results": rows, "has_next": has_next}
