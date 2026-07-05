from __future__ import annotations

import pytest

from app.models import TradeOutcome
from app.services.trade_outcome_display import (
    corrected_current_price_for_display,
    trade_outcome_display_metrics,
    trade_outcome_display_row,
)
from app.services.trade_outcomes import rank_extreme_trade_outcomes


def _outcome(
    *,
    event_id: int,
    symbol: str,
    trade_type: str,
    entry_price: float,
    current_price: float,
    return_pct: float,
    alpha_pct: float | None = None,
    benchmark_return_pct: float | None = None,
) -> TradeOutcome:
    return TradeOutcome(
        event_id=event_id,
        symbol=symbol,
        trade_type=trade_type,
        entry_price=entry_price,
        current_price=current_price,
        return_pct=return_pct,
        alpha_pct=alpha_pct,
        benchmark_return_pct=benchmark_return_pct,
        holding_days=3,
        scoring_status="ok",
        methodology_version="insider_v1",
    )


def test_trade_outcome_display_corrects_milliprice_current_quote_scale() -> None:
    outcome = _outcome(
        event_id=295502,
        symbol="INM",
        trade_type="purchase",
        entry_price=1.55,
        current_price=1517.6,
        return_pct=97809.67741935483,
        benchmark_return_pct=1.0799520807,
        alpha_pct=97808.59746727414,
    )

    metrics = trade_outcome_display_metrics(outcome)

    assert metrics.current_or_horizon_price == pytest.approx(1.5176)
    assert metrics.return_pct == pytest.approx(-2.09032258)
    assert metrics.alpha_pct == pytest.approx(-3.17027466)
    assert metrics.trade_price == 1.55
    assert metrics.pnl_source == "trade_outcome"


def test_trade_outcome_display_corrects_cent_price_current_quote_scale() -> None:
    assert corrected_current_price_for_display(entry_price=7.85, current_price=719.4) == pytest.approx(7.194)


def test_trade_outcome_display_does_not_adjust_normal_large_price_outcomes() -> None:
    assert corrected_current_price_for_display(entry_price=125.0, current_price=250.0) == 250.0


def test_trade_outcome_display_suppresses_implausible_recent_returns() -> None:
    outcome = _outcome(
        event_id=295412,
        symbol="BOLD",
        trade_type="purchase",
        entry_price=2.49,
        current_price=14.25,
        return_pct=472.289156626506,
        benchmark_return_pct=1.0,
        alpha_pct=471.2,
    )

    metrics = trade_outcome_display_metrics(outcome)

    assert metrics.current_or_horizon_price is None
    assert metrics.return_pct is None
    assert metrics.alpha_pct is None
    assert metrics.pnl_source is None


def test_display_row_and_extreme_ranking_use_corrected_returns() -> None:
    scaled_bad = _outcome(
        event_id=1,
        symbol="INM",
        trade_type="purchase",
        entry_price=1.55,
        current_price=1517.6,
        return_pct=97809.67741935483,
        benchmark_return_pct=1.0,
        alpha_pct=97808.6,
    )
    actual_winner = _outcome(
        event_id=2,
        symbol="AAPL",
        trade_type="purchase",
        entry_price=100.0,
        current_price=130.0,
        return_pct=30.0,
        benchmark_return_pct=1.0,
        alpha_pct=29.0,
    )

    display_bad = trade_outcome_display_row(scaled_bad)
    best, worst = rank_extreme_trade_outcomes([scaled_bad, actual_winner])

    assert display_bad.return_pct == pytest.approx(-2.09032258)
    assert best == [actual_winner]
    assert worst == [scaled_bad]
