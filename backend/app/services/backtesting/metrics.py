from __future__ import annotations

from math import sqrt
from statistics import stdev
from typing import Sequence


def pct_return(start_value: float | None, end_value: float | None) -> float:
    if start_value is None or end_value is None or start_value <= 0:
        return 0.0
    return float(((end_value / start_value) - 1.0) * 100.0)


def compute_max_drawdown_pct(values: Sequence[float]) -> float:
    running_peak: float | None = None
    max_drawdown = 0.0
    for value in values:
        if value <= 0:
            continue
        running_peak = value if running_peak is None else max(running_peak, value)
        if running_peak <= 0:
            continue
        drawdown = ((value / running_peak) - 1.0) * 100.0
        max_drawdown = min(max_drawdown, drawdown)
    return float(abs(max_drawdown))


def compute_volatility_pct(values: Sequence[float]) -> float:
    return compute_volatility_pct_from_daily_returns(daily_returns_from_values(values))


def daily_returns_from_values(values: Sequence[float]) -> list[float]:
    if len(values) < 2:
        return []
    daily_returns: list[float] = []
    for previous, current in zip(values, values[1:]):
        if previous <= 0 or current <= 0:
            continue
        daily_returns.append((current / previous) - 1.0)
    return daily_returns


def compute_volatility_pct_from_daily_returns(daily_returns: Sequence[float]) -> float:
    if len(daily_returns) < 2:
        return 0.0
    return float(stdev(daily_returns) * sqrt(252.0) * 100.0)


def compute_sharpe_ratio(daily_returns: Sequence[float]) -> float | None:
    if len(daily_returns) < 2:
        return None
    volatility = stdev(daily_returns)
    if volatility <= 0:
        return None
    return float((sum(daily_returns) / len(daily_returns)) / volatility * sqrt(252.0))


def compute_cagr_pct(total_return_pct: float, years: float) -> float:
    if years <= 0:
        return 0.0
    growth = 1.0 + (total_return_pct / 100.0)
    if growth <= 0:
        return -100.0
    return float((growth ** (1.0 / years) - 1.0) * 100.0)


def compute_win_rate_pct(position_returns: Sequence[float]) -> float:
    if not position_returns:
        return 0.0
    wins = sum(1 for value in position_returns if value > 0)
    return float((wins / len(position_returns)) * 100.0)
