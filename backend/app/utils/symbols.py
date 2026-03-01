from __future__ import annotations


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

    return canonical_symbol(symbol)
