from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

from app.services.price_lookup import get_close_for_date_or_prior


@dataclass(frozen=True)
class ProfileCurveSeries:
    member_series: list[dict[str, Any]]
    benchmark_series: list[dict[str, Any]]


def build_timeline_dates(start_date: date, end_date: date) -> list[date]:
    if end_date < start_date:
        return []
    return [start_date + timedelta(days=offset) for offset in range((end_date - start_date).days + 1)]


def build_normalized_profile_curve(
    *,
    outcomes: list[Any],
    timeline_dates: list[date],
    benchmark_close_map: dict[str, float],
    benchmark_dates: list[str],
) -> ProfileCurveSeries:
    """
    Build chart-only profile performance using a normalized equal-weight event portfolio.

    Methodology (chart series only, summary cards remain unchanged):
    - Each scored trade outcome (return_pct present) is one normalized lot.
    - A lot enters on trade_date with normalized value: 1 + return_pct / 100.
    - Portfolio NAV on a day is the average value of all entered lots; if no lots entered,
      NAV remains 1.0 (flat 0% return).
    - Chart return = (portfolio_nav - 1) * 100.
    - Benchmark is S&P cumulative return over the same full selected timeline window.
    """

    lots_by_day: dict[date, list[Any]] = defaultdict(list)
    for outcome in outcomes:
        trade_day = getattr(outcome, "trade_date", None)
        return_pct = getattr(outcome, "return_pct", None)
        if trade_day is None or return_pct is None:
            continue
        lots_by_day[trade_day].append(outcome)

    benchmark_base = benchmark_close_map.get(benchmark_dates[0]) if benchmark_dates else None

    benchmark_series: list[dict[str, Any]] = []
    if benchmark_base is not None and benchmark_base > 0:
        for timeline_day in timeline_dates:
            asof_date = timeline_day.isoformat()
            close_value = get_close_for_date_or_prior(asof_date, benchmark_close_map, benchmark_dates)
            if close_value is None or close_value <= 0:
                continue
            benchmark_series.append(
                {
                    "asof_date": asof_date,
                    "cumulative_return_pct": float(((close_value - benchmark_base) / benchmark_base) * 100),
                }
            )

    lot_values: list[float] = []
    member_series: list[dict[str, Any]] = []
    for timeline_index, timeline_day in enumerate(timeline_dates):
        asof_date = timeline_day.isoformat()
        day_outcomes = sorted(
            lots_by_day.get(timeline_day, []),
            key=lambda item: (
                getattr(item, "event_id", 0) if getattr(item, "event_id", None) is not None else 0
            ),
        )
        for outcome in day_outcomes:
            return_pct = getattr(outcome, "return_pct", None)
            if return_pct is None:
                continue
            lot_values.append(1.0 + (float(return_pct) / 100.0))

        portfolio_nav = (sum(lot_values) / len(lot_values)) if lot_values else 1.0
        cumulative_return_pct = float((portfolio_nav - 1.0) * 100.0)

        running_benchmark_return_pct = None
        if benchmark_base is not None and benchmark_base > 0:
            benchmark_close = get_close_for_date_or_prior(asof_date, benchmark_close_map, benchmark_dates)
            if benchmark_close is not None and benchmark_close > 0:
                running_benchmark_return_pct = float(((benchmark_close - benchmark_base) / benchmark_base) * 100)

        cumulative_alpha_pct = (
            float(cumulative_return_pct - running_benchmark_return_pct)
            if running_benchmark_return_pct is not None
            else None
        )

        day_event = day_outcomes[-1] if day_outcomes else None
        member_series.append(
            {
                "event_id": (getattr(day_event, "event_id", None) if day_event is not None else -(timeline_index + 1)),
                "symbol": (getattr(day_event, "symbol", None) if day_event is not None else None),
                "trade_type": (getattr(day_event, "trade_type", None) if day_event is not None else None),
                "asof_date": asof_date,
                "return_pct": (getattr(day_event, "return_pct", None) if day_event is not None else None),
                "alpha_pct": (getattr(day_event, "alpha_pct", None) if day_event is not None else None),
                "benchmark_return_pct": (
                    getattr(day_event, "benchmark_return_pct", None) if day_event is not None else None
                ),
                "holding_days": (getattr(day_event, "holding_days", None) if day_event is not None else None),
                "cumulative_return_pct": cumulative_return_pct,
                "running_benchmark_return_pct": running_benchmark_return_pct,
                "cumulative_alpha_pct": cumulative_alpha_pct,
            }
        )

    return ProfileCurveSeries(member_series=member_series, benchmark_series=benchmark_series)
