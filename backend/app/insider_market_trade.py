from __future__ import annotations

from typing import Any


def classify_insider_market_trade(
    raw_type: str | None,
    raw_payload: dict[str, Any] | None = None,
) -> tuple[str | None, bool]:
    """
    Returns (canonical_trade_type, is_market_trade).

    canonical_trade_type: 'purchase' | 'sale' | None
    """
    if raw_type:
        normalized = raw_type.strip().lower()
        if normalized.startswith("s-") or normalized.startswith("sale"):
            return ("sale", True)
        if normalized.startswith("p-") or normalized.startswith("purchase"):
            return ("purchase", True)
        if "sale" in normalized:
            return ("sale", True)
        if "purchase" in normalized:
            return ("purchase", True)

    code = None
    if raw_payload and isinstance(raw_payload, dict):
        raw_transaction_type = (
            raw_payload.get("transactionType")
            or raw_payload.get("transaction_type")
            or raw_payload.get("transactionCode")
            or raw_payload.get("transaction_code")
        )
        if isinstance(raw_transaction_type, str) and raw_transaction_type.strip():
            code = raw_transaction_type.strip()[:1].upper()

    if code == "S":
        return ("sale", True)
    if code == "P":
        return ("purchase", True)

    return (None, False)

