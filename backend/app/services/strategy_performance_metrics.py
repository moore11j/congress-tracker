"""Point-in-time performance metrics for persisted strategy equity curves."""

from __future__ import annotations

import math
from datetime import date
from typing import Any, Iterable


PERIOD_DAYS = {"30d": 30, "1y": 365, "2y": 730, "3y": 1095}


def _value(point: Any, key: str) -> Any:
    if isinstance(point, dict):
        return point.get(key)
    return getattr(point, key, None)


def _day(point: Any) -> date | None:
    value = _value(point, "date") or _value(point, "asof_date")
    if isinstance(value, date):
        return value
    if not value:
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _float(point: Any, key: str) -> float | None:
    value = _value(point, key)
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _sorted_points(points: Iterable[Any]) -> list[Any]:
    return sorted((point for point in points if _day(point) is not None), key=lambda point: _day(point) or date.min)


def _window(points: list[Any], days: int | None) -> list[Any]:
    if len(points) < 2 or days is None:
        return points
    start_day = _day(points[0])
    end_day = _day(points[-1])
    if start_day is None or end_day is None or (end_day - start_day).days < days:
        return []
    target = end_day.toordinal() - days
    start_index = 0
    for index, point in enumerate(points):
        point_day = _day(point)
        if point_day is not None and point_day.toordinal() <= target:
            start_index = index
        else:
            break
    return points[start_index:]


def _return_pct(points: list[Any], key: str) -> float | None:
    if len(points) < 2:
        return None
    start = _float(points[0], key)
    end = _float(points[-1], key)
    if start is None or end is None or start <= 0:
        return None
    return round(((end / start) - 1.0) * 100.0, 4)


def _annualized_return(return_pct: float | None, elapsed_days: int) -> float | None:
    if return_pct is None or elapsed_days <= 0:
        return None
    growth = 1.0 + return_pct / 100.0
    if growth <= 0:
        return -100.0
    return round(((growth ** (365.25 / float(elapsed_days))) - 1.0) * 100.0, 4)


def _daily_pairs(points: list[Any]) -> list[tuple[float, float]]:
    pairs: list[tuple[float, float]] = []
    for previous, current in zip(points, points[1:]):
        strategy_previous = _float(previous, "strategy_value")
        strategy_current = _float(current, "strategy_value")
        benchmark_previous = _float(previous, "benchmark_value")
        benchmark_current = _float(current, "benchmark_value")
        if (
            strategy_previous is None
            or strategy_current is None
            or benchmark_previous is None
            or benchmark_current is None
            or strategy_previous <= 0
            or benchmark_previous <= 0
        ):
            continue
        pairs.append(((strategy_current / strategy_previous) - 1.0, (benchmark_current / benchmark_previous) - 1.0))
    return pairs


def _sample_std(values: list[float]) -> float | None:
    if len(values) < 2:
        return None
    mean = sum(values) / len(values)
    variance = sum((value - mean) ** 2 for value in values) / (len(values) - 1)
    return math.sqrt(variance)


def _risk_metrics(points: list[Any]) -> dict[str, float | None]:
    pairs = _daily_pairs(points)
    returns = [strategy for strategy, _ in pairs]
    volatility = _sample_std(returns)
    sharpe = None
    if volatility is not None and volatility > 0:
        sharpe = round((sum(returns) / len(returns) / volatility) * math.sqrt(252.0), 4)

    downside = [min(0.0, value) for value in returns]
    downside_deviation = math.sqrt(sum(value * value for value in downside) / len(downside)) if downside else None
    sortino = None
    if downside_deviation is not None and downside_deviation > 0:
        sortino = round((sum(returns) / len(returns) / downside_deviation) * math.sqrt(252.0), 4)

    beta = None
    if len(pairs) >= 10:
        strategy_mean = sum(strategy for strategy, _ in pairs) / len(pairs)
        benchmark_mean = sum(benchmark for _, benchmark in pairs) / len(pairs)
        covariance = sum((strategy - strategy_mean) * (benchmark - benchmark_mean) for strategy, benchmark in pairs) / len(pairs)
        benchmark_variance = sum((benchmark - benchmark_mean) ** 2 for _, benchmark in pairs) / len(pairs)
        if benchmark_variance > 0:
            beta = round(covariance / benchmark_variance, 4)

    peak = None
    max_drawdown = None
    for point in points:
        value = _float(point, "strategy_value")
        if value is None or value <= 0:
            continue
        peak = value if peak is None else max(peak, value)
        drawdown = ((value / peak) - 1.0) * 100.0
        max_drawdown = drawdown if max_drawdown is None else min(max_drawdown, drawdown)

    active_holdings = []
    for point in points:
        holdings = _float(point, "active_holdings")
        if holdings is None:
            holdings = _float(point, "active_positions")
        if holdings is None:
            holdings = _float(point, "active_lots")
        active_holdings.append(holdings)
    known_holdings = [value for value in active_holdings if value is not None]
    return {
        "beta": beta,
        "sharpe": sharpe,
        "sortino": sortino,
        "max_drawdown_pct": round(max_drawdown, 4) if max_drawdown is not None else None,
        "annualized_volatility_pct": round(volatility * math.sqrt(252.0) * 100.0, 4) if volatility is not None else None,
        "avg_holdings": round(sum(known_holdings) / len(known_holdings), 4) if known_holdings else None,
        "observations": len(pairs),
    }


def trailing_snapshot_values(
    *,
    strategy_id: int,
    run_id: int,
    as_of_date: date,
    points: Iterable[Any],
    baseline_metrics: dict[str, Any],
    walnut_score: float | None,
) -> list[dict[str, Any]]:
    """Return max and trailing snapshots using the exact persisted curve values.

    A trailing period is emitted with null fields only when the stored curve does
    not cover that full period. It never borrows current or maximum-history risk
    values for a shorter period.
    """
    ordered = _sorted_points(points)
    results: list[dict[str, Any]] = []
    for period, days in (("max", None), *PERIOD_DAYS.items()):
        window = _window(ordered, days)
        period_start = _day(window[0]) if window else None
        period_end = _day(window[-1]) if window else None
        elapsed_days = (period_end - period_start).days if period_start and period_end else 0
        total_return = _return_pct(window, "strategy_value")
        benchmark_return = _return_pct(window, "benchmark_value")
        cagr = _annualized_return(total_return, elapsed_days)
        benchmark_cagr = _annualized_return(benchmark_return, elapsed_days)
        risk = _risk_metrics(window)
        if period == "max":
            total_return = baseline_metrics.get("total_return_pct", total_return)
            cagr = baseline_metrics.get("cagr_pct", cagr)
            benchmark_return = baseline_metrics.get("benchmark_total_return_pct", benchmark_return)
            benchmark_cagr = baseline_metrics.get("benchmark_cagr_pct", benchmark_cagr)
            alpha = baseline_metrics.get("alpha_cagr_pct")
            if alpha is None and cagr is not None and benchmark_cagr is not None:
                alpha = round(float(cagr) - float(benchmark_cagr), 4)
            for field in ("beta", "sharpe", "sortino", "max_drawdown_pct", "annualized_volatility_pct", "avg_active_lots"):
                if baseline_metrics.get(field) is not None:
                    target = "avg_holdings" if field == "avg_active_lots" else field
                    risk[target] = baseline_metrics[field]
        else:
            alpha = round(cagr - benchmark_cagr, 4) if cagr is not None and benchmark_cagr is not None else None

        results.append(
            {
                "strategy_id": strategy_id,
                "run_id": run_id,
                "as_of_date": as_of_date,
                "period": period,
                "total_return_pct": total_return,
                "cagr_pct": cagr,
                "benchmark_return_pct": benchmark_return,
                "benchmark_cagr_pct": benchmark_cagr,
                "alpha_cagr_pct": alpha,
                "beta": risk["beta"],
                "sharpe": risk["sharpe"],
                "sortino": risk["sortino"],
                "max_drawdown_pct": risk["max_drawdown_pct"],
                "annualized_volatility_pct": risk["annualized_volatility_pct"],
                "win_rate_pct": baseline_metrics.get("win_rate_pct") if period == "max" else None,
                "trade_count": baseline_metrics.get("trade_count"),
                "independent_signal_count": baseline_metrics.get("independent_signals"),
                "avg_holdings": risk["avg_holdings"],
                "turnover_events": baseline_metrics.get("turnover_events") if period == "max" else None,
                "rolling_12m_beating_spy_pct": baseline_metrics.get("rolling_12m_beating_spy_pct") if period == "max" else None,
                "walnut_strategy_score": walnut_score,
                "metrics_json": {
                    "period": period,
                    "days": days,
                    "coverage_start": period_start.isoformat() if period_start else None,
                    "coverage_end": period_end.isoformat() if period_end else None,
                    "observations": risk["observations"],
                    "source": "persisted_equity_curve",
                },
            }
        )
    return results
