from __future__ import annotations

import re

_VALID_SYMBOL_RE = re.compile(r"^[A-Z\^][A-Z0-9./-]{0,14}$")
_MUTUAL_FUND_RE = re.compile(r"^[A-Z]{5}X$")
_CUSIP_LIKE_RE = re.compile(r"^[A-Z0-9]{9}$")
_SHARE_CLASS_RE = re.compile(r"^([A-Z]{1,6})[./-]([A-Z])$")


def canonical_symbol(raw: str | None) -> str | None:
    if not raw:
        return None

    symbol = raw.strip().upper()
    while symbol.startswith("$"):
        symbol = symbol[1:].strip()

    return symbol or None


def normalize_symbol(raw: str | None) -> str | None:
    if not raw:
        return None

    symbol = str(raw).strip()
    if not symbol:
        return None

    if ":" in symbol:
        symbol = symbol.split(":", 1)[1].strip()

    symbol = symbol.replace(" ", "")
    return canonical_symbol(symbol)


def symbol_variants(raw: str | None) -> list[str]:
    normalized = normalize_symbol(raw)
    if not normalized:
        return []

    variants: list[str] = [normalized]
    share_match = _SHARE_CLASS_RE.match(normalized)
    if share_match:
        root, share_class = share_match.group(1), share_match.group(2)
        variants.extend([
            f"{root}.{share_class}",
            f"{root}/{share_class}",
            f"{root}{share_class}",
        ])
    elif "/" in normalized:
        variants.append(normalized.replace("/", "."))
    elif "-" in normalized:
        variants.append(normalized.replace("-", "."))

    seen: set[str] = set()
    deduped: list[str] = []
    for symbol in variants:
        normalized_variant = normalize_symbol(symbol)
        if normalized_variant and normalized_variant not in seen:
            deduped.append(normalized_variant)
            seen.add(normalized_variant)
    return deduped


def classify_symbol(raw: str | None) -> tuple[str, str | None, str | None]:
    normalized = normalize_symbol(raw)
    if not normalized:
        return "no_symbol", None, "Missing symbol on event/payload"

    if not _VALID_SYMBOL_RE.match(normalized):
        return "unsupported_symbol", normalized, f"Unsupported symbol format: {normalized}"

    if _MUTUAL_FUND_RE.match(normalized):
        return "non_equity_or_unpriced_asset", normalized, f"Likely mutual fund or money-market ticker: {normalized}"

    if _CUSIP_LIKE_RE.match(normalized) and any(ch.isdigit() for ch in normalized):
        return "non_equity_or_unpriced_asset", normalized, f"Likely bond/CUSIP-like identifier: {normalized}"

    if normalized.count(".") > 1 or normalized.count("/") > 1:
        return "unsupported_symbol", normalized, f"Unsupported multi-class/share format: {normalized}"

    return "eligible", normalized, None
