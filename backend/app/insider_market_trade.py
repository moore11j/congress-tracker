from __future__ import annotations


def canonicalize_market_trade_type(raw: str | None) -> str | None:
    if not raw:
        return None

    s = raw.strip().lower()
    if not s:
        return None

    if s.startswith("s-") or s.startswith("sale") or "sale" in s:
        return "sale"
    if s.startswith("p-") or s.startswith("purchase") or "purchase" in s:
        return "purchase"
    if s == "s":
        return "sale"
    if s == "p":
        return "purchase"
    return None


def classify_insider_market_trade(
    raw_type: str | None,
    raw_payload: dict | None = None,
) -> tuple[str | None, bool]:
    """Backward-compatible wrapper for older modules."""
    candidate = raw_type
    if (not candidate or not str(candidate).strip()) and isinstance(raw_payload, dict):
        from_payload = raw_payload.get("transactionType") or raw_payload.get("transaction_type")
        candidate = from_payload if isinstance(from_payload, str) else candidate
    canonical = canonicalize_market_trade_type(candidate)
    return (canonical, canonical is not None)
