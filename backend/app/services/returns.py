from __future__ import annotations


def trade_direction(trade_type: str | None) -> str | None:
    if not trade_type:
        return None

    normalized = trade_type.strip().lower()
    if not normalized:
        return None

    if normalized in {"s", "s-sale"}:
        return "sell"
    if normalized in {"p", "p-purchase"}:
        return "buy"

    sell_tokens = ("sale", "sell", "disposition", "dispose")
    if any(token in normalized for token in sell_tokens):
        return "sell"

    buy_tokens = ("buy", "purchase", "acquire", "acquisition")
    if any(token in normalized for token in buy_tokens):
        return "buy"

    return None


def signed_return_pct(current_price: float | int | None, entry_price: float | int | None, trade_type: str | None) -> float | None:
    if current_price is None or entry_price is None:
        return None

    current = float(current_price)
    entry = float(entry_price)
    if entry <= 0:
        return None

    ratio = current / entry
    direction = trade_direction(trade_type)
    if direction == "sell":
        return float((1 - ratio) * 100)
    return float((ratio - 1) * 100)

