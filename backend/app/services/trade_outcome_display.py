from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import math
from types import SimpleNamespace

from app.models import TradeOutcome
from app.services.returns import signed_return_pct
from app.utils.symbols import normalize_symbol


@dataclass(frozen=True)
class TradeOutcomeDisplayMetrics:
    return_pct: float | None
    alpha_pct: float | None
    trade_price: float | None
    current_or_horizon_price: float | None
    benchmark_return_pct: float | None
    holding_period_days: int | None
    outcome_horizon: str | None
    pnl_source: str | None


_PRICE_SCALE_DIVISORS = (1000.0, 100.0)


def _safe_float(value: int | float | None) -> float | None:
    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(numeric):
        return None
    return numeric


def corrected_current_price_for_display(
    *,
    entry_price: int | float | None,
    current_price: int | float | None,
) -> float | None:
    """Normalize obvious provider scale slips in persisted display outcomes.

    Some delayed quote rows arrive in cents/mills while the trade entry price is
    already in dollars. We only correct extreme mismatches where a common scale
    divisor brings the current price back near the entry basis.
    """

    entry = _safe_float(entry_price)
    current = _safe_float(current_price)
    if entry is None or current is None or entry <= 0 or current <= 0:
        return current

    raw_ratio = current / entry
    if entry > 25 or current < 100 or raw_ratio < 50:
        return current

    best: tuple[float, float] | None = None
    for divisor in _PRICE_SCALE_DIVISORS:
        scaled = current / divisor
        scaled_ratio = scaled / entry
        if 0.05 <= scaled_ratio <= 20:
            score = abs(math.log(scaled_ratio))
            if best is None or score < best[0]:
                best = (score, scaled)

    return best[1] if best is not None else current


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
        return TradeOutcomeDisplayMetrics(
            return_pct=None,
            alpha_pct=None,
            trade_price=None,
            current_or_horizon_price=None,
            benchmark_return_pct=None,
            holding_period_days=None,
            outcome_horizon=None,
            pnl_source=None,
        )

    entry_price = _safe_float(outcome.entry_price)
    raw_current_price = _safe_float(outcome.current_price)
    current_price = corrected_current_price_for_display(
        entry_price=entry_price,
        current_price=raw_current_price,
    )
    return_pct = outcome.return_pct
    alpha_pct = outcome.alpha_pct
    if (
        entry_price is not None
        and current_price is not None
        and raw_current_price is not None
        and current_price != raw_current_price
    ):
        return_pct = signed_return_pct(current_price, entry_price, outcome.trade_type)
        alpha_pct = (
            float(return_pct - outcome.benchmark_return_pct)
            if return_pct is not None and outcome.benchmark_return_pct is not None
            else None
        )

    has_return = return_pct is not None
    horizon = f"{outcome.holding_days}D Return" if isinstance(outcome.holding_days, int) and outcome.holding_days > 0 else "Scored Return"
    return TradeOutcomeDisplayMetrics(
        return_pct=return_pct,
        alpha_pct=alpha_pct,
        trade_price=entry_price,
        current_or_horizon_price=current_price,
        benchmark_return_pct=outcome.benchmark_return_pct,
        holding_period_days=outcome.holding_days,
        outcome_horizon=horizon if has_return else None,
        pnl_source="trade_outcome" if has_return else None,
    )


def trade_outcome_display_row(outcome: TradeOutcome):
    metrics = trade_outcome_display_metrics(outcome)
    return SimpleNamespace(
        id=getattr(outcome, "id", None),
        event_id=getattr(outcome, "event_id", None),
        member_id=getattr(outcome, "member_id", None),
        member_name=getattr(outcome, "member_name", None),
        symbol=getattr(outcome, "symbol", None),
        trade_type=getattr(outcome, "trade_type", None),
        source=getattr(outcome, "source", None),
        trade_date=getattr(outcome, "trade_date", None),
        entry_price=metrics.trade_price,
        entry_price_date=getattr(outcome, "entry_price_date", None),
        current_price=metrics.current_or_horizon_price,
        current_price_date=getattr(outcome, "current_price_date", None),
        benchmark_symbol=getattr(outcome, "benchmark_symbol", None),
        benchmark_entry_price=getattr(outcome, "benchmark_entry_price", None),
        benchmark_current_price=getattr(outcome, "benchmark_current_price", None),
        return_pct=metrics.return_pct,
        benchmark_return_pct=metrics.benchmark_return_pct,
        alpha_pct=metrics.alpha_pct,
        holding_days=metrics.holding_period_days,
        amount_min=getattr(outcome, "amount_min", None),
        amount_max=getattr(outcome, "amount_max", None),
        scoring_status=getattr(outcome, "scoring_status", None),
        scoring_error=getattr(outcome, "scoring_error", None),
        methodology_version=getattr(outcome, "methodology_version", None),
        computed_at=getattr(outcome, "computed_at", None),
    )
