from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from app.models import TradeOutcome
from app.utils.symbols import normalize_symbol


@dataclass(frozen=True)
class TradeOutcomeDisplayMetrics:
    return_pct: float | None
    alpha_pct: float | None
    pnl_source: str | None


def normalize_trade_side(value: str | None) -> str | None:
    if not value:
        return None
    normalized = value.strip().lower()
    if not normalized:
        return None

    if normalized in {"sale", "s-sale", "sell", "s"}:
        return "sale"
    if normalized in {"purchase", "p-purchase", "buy", "p"}:
        return "purchase"

    if "sale" in normalized or "sell" in normalized or "disposition" in normalized:
        return "sale"
    if "purchase" in normalized or "buy" in normalized or "acquisition" in normalized:
        return "purchase"

    return normalized


def _normalized_amount(value: int | float | None) -> int | None:
    if value is None:
        return None
    return int(round(float(value)))


def trade_outcome_logical_key(
    *,
    symbol: str | None,
    trade_side: str | None,
    trade_date: date | str | None,
    amount_min: int | float | None,
    amount_max: int | float | None,
) -> tuple[str | None, str | None, str | None, int | None, int | None]:
    if isinstance(trade_date, date):
        trade_date_key = trade_date.isoformat()
    elif isinstance(trade_date, str):
        trade_date_key = trade_date[:10] if trade_date else None
    else:
        trade_date_key = None

    return (
        normalize_symbol(symbol),
        normalize_trade_side(trade_side),
        trade_date_key,
        _normalized_amount(amount_min),
        _normalized_amount(amount_max),
    )


def trade_outcome_display_metrics(outcome: TradeOutcome | None) -> TradeOutcomeDisplayMetrics:
    if outcome is None:
        return TradeOutcomeDisplayMetrics(return_pct=None, alpha_pct=None, pnl_source=None)

    has_return = outcome.return_pct is not None
    return TradeOutcomeDisplayMetrics(
        return_pct=outcome.return_pct,
        alpha_pct=outcome.alpha_pct,
        pnl_source="trade_outcome" if has_return else None,
    )
