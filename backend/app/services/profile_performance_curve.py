from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

from app.services.price_lookup import get_close_for_date_or_prior, get_eod_close_series
from app.services.returns import signed_return_pct


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
    price_close_maps: dict[str, dict[str, float]] | None = None,
) -> ProfileCurveSeries:
    """
    Build chart-only profile performance using a normalized equal-weight event portfolio.

    Methodology (chart series only, summary cards remain unchanged):
    - Each scored trade outcome is one normalized lot.
    - Return mode prefers cached daily closes to mark entered lots from their entry price,
      which makes the visible curve behave like a normalized equity curve without writes.
    - When symbol history is unavailable, the curve falls back to the scored outcome return.
    - Alpha mode keeps the scored-outcome cumulative method used by existing profile charts.
    - Benchmark is S&P cumulative return over the same full selected timeline window.

    This is intentionally not the capital-constrained backtest engine. Profile summaries
    average persisted trade outcomes individually; backtests simulate allocation,
    monthly rebalancing, disclosure-timed entries, and a configurable hold period.
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

    active_outcomes: list[Any] = []
    scored_lot_values: list[float] = []
    sorted_price_dates_by_symbol = {
        symbol: sorted(close_map.keys())
        for symbol, close_map in (price_close_maps or {}).items()
    }
    member_series: list[dict[str, Any]] = []

    def _marked_lot_return_pct(outcome: Any, asof_date: str, timeline_index: int) -> float | None:
        trade_day = getattr(outcome, "trade_date", None)
        if timeline_index == 0 or (trade_day is not None and asof_date == trade_day.isoformat()):
            return 0.0

        symbol = str(getattr(outcome, "symbol", "") or "").strip().upper()
        try:
            entry_price = float(getattr(outcome, "entry_price", None))
        except (TypeError, ValueError):
            entry_price = None
        close_map = (price_close_maps or {}).get(symbol)
        close_dates = sorted_price_dates_by_symbol.get(symbol, [])
        if close_map and close_dates and entry_price is not None and entry_price > 0:
            close = get_close_for_date_or_prior(asof_date, close_map, close_dates)
            if close is not None and close > 0:
                return signed_return_pct(close, entry_price, getattr(outcome, "trade_type", None))

        fallback_return = getattr(outcome, "return_pct", None)
        return float(fallback_return) if fallback_return is not None else None

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
            active_outcomes.append(outcome)
            scored_lot_values.append(1.0 + (float(return_pct) / 100.0))

        scored_nav = (sum(scored_lot_values) / len(scored_lot_values)) if scored_lot_values else 1.0
        scored_cumulative_return_pct = float((scored_nav - 1.0) * 100.0)
        marked_returns = [
            value
            for value in (
                _marked_lot_return_pct(outcome, asof_date, timeline_index)
                for outcome in active_outcomes
            )
            if value is not None
        ]
        marked_cumulative_return_pct = (
            float(sum(marked_returns) / len(marked_returns))
            if marked_returns
            else 0.0
        )

        running_benchmark_return_pct = None
        if benchmark_base is not None and benchmark_base > 0:
            benchmark_close = get_close_for_date_or_prior(asof_date, benchmark_close_map, benchmark_dates)
            if benchmark_close is not None and benchmark_close > 0:
                running_benchmark_return_pct = float(((benchmark_close - benchmark_base) / benchmark_base) * 100)

        cumulative_alpha_pct = (
            float(scored_cumulative_return_pct - running_benchmark_return_pct)
            if running_benchmark_return_pct is not None
            else None
        )

        day_event = day_outcomes[-1] if day_outcomes else None
        member_series.append(
            {
                "event_id": (getattr(day_event, "event_id", None) if day_event is not None else -(timeline_index + 1)),
                "date": asof_date,
                "symbol": (getattr(day_event, "symbol", None) if day_event is not None else None),
                "trade_type": (getattr(day_event, "trade_type", None) if day_event is not None else None),
                "asof_date": asof_date,
                "return_pct": (getattr(day_event, "return_pct", None) if day_event is not None else None),
                "alpha_pct": (getattr(day_event, "alpha_pct", None) if day_event is not None else None),
                "benchmark_return_pct": (
                    getattr(day_event, "benchmark_return_pct", None) if day_event is not None else None
                ),
                "holding_days": (getattr(day_event, "holding_days", None) if day_event is not None else None),
                "cumulative_return_pct": scored_cumulative_return_pct,
                "running_benchmark_return_pct": running_benchmark_return_pct,
                "cumulative_alpha_pct": cumulative_alpha_pct,
                "strategy_return_pct": marked_cumulative_return_pct,
                "benchmark_running_return_pct": running_benchmark_return_pct,
                "alpha": cumulative_alpha_pct,
                "active_positions": len(active_outcomes),
            }
        )

    return ProfileCurveSeries(member_series=member_series, benchmark_series=benchmark_series)


def load_profile_price_close_maps(
    *,
    db: Any,
    outcomes: list[Any],
    start_date: date,
    end_date: date,
) -> dict[str, dict[str, float]]:
    """Read cached daily closes for profile curves without refreshing or backfilling."""

    symbols = sorted(
        {
            str(getattr(outcome, "symbol", "") or "").strip().upper()
            for outcome in outcomes
            if str(getattr(outcome, "symbol", "") or "").strip()
        }
    )
    return {
        symbol: close_map
        for symbol in symbols
        for close_map in [get_eod_close_series(db, symbol, start_date.isoformat(), end_date.isoformat())]
        if close_map
    }
