from __future__ import annotations

import re

from app.utils.symbols import normalize_symbol


_SECURITY_TITLE_EXACT = {
    "class a",
    "class b",
    "common shares",
    "common stock",
    "ordinary share",
    "ordinary shares",
    "preferred stock",
    "restricted stock",
    "restricted stock units",
    "rsu",
    "rsus",
    "stock option",
    "stock options",
    "warrant",
    "warrants",
}

_SECURITY_TITLE_FRAGMENTS = (
    "right to buy",
    "right to purchase",
    "stock option",
    "restricted stock unit",
    "restricted stock units",
)


def _normalized_text(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = re.sub(r"\s+", " ", value).strip()
    return cleaned or None


def is_filing_security_title(value: object) -> bool:
    cleaned = _normalized_text(value)
    if not cleaned:
        return False
    lowered = cleaned.lower()
    if lowered in _SECURITY_TITLE_EXACT:
        return True
    if "common stock" in lowered:
        return True
    return any(fragment in lowered for fragment in _SECURITY_TITLE_FRAGMENTS)


def safe_company_identity_candidate(value: object, symbol: str | None = None) -> str | None:
    cleaned = _normalized_text(value)
    if not cleaned:
        return None
    normalized_symbol = normalize_symbol(symbol)
    if normalized_symbol and cleaned.upper() == normalized_symbol:
        return None
    if cleaned.lower() in {"unknown", "unknown company", "n/a", "na", "none"}:
        return None
    if is_filing_security_title(cleaned):
        return None
    return cleaned


def resolve_ticker_identity(
    symbol: str,
    *,
    canonical_profile_name: object = None,
    issuer_company_names: list[object] | tuple[object, ...] = (),
    metadata_name: object = None,
) -> str:
    normalized_symbol = normalize_symbol(symbol) or str(symbol or "").strip().upper()
    for candidate in (
        canonical_profile_name,
        *issuer_company_names,
        metadata_name,
    ):
        safe = safe_company_identity_candidate(candidate, normalized_symbol)
        if safe:
            return safe
    return normalized_symbol

