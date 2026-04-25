from __future__ import annotations

from bisect import bisect_left
from calendar import monthrange
from collections import Counter
from dataclasses import dataclass
from datetime import date, timedelta
import logging
from typing import Iterable

from fastapi import HTTPException
from sqlalchemy.orm import Session

from app.services.backtesting.metrics import (
    compute_cagr_pct,
    compute_max_drawdown_pct,
    compute_sharpe_ratio,
    compute_volatility_pct_from_daily_returns,
    compute_win_rate_pct,
    cumulative_return_pct_from_daily_returns,
    indexed_curve_from_daily_returns,
)
from app.services.backtesting.models import (
    DEFAULT_BENCHMARK,
    BacktestDiagnostics,
    BacktestPositionPoint,
    BacktestRunResponse,
    BacktestSignal,
    BacktestStrategyConfig,
    BacktestSummary,
    BacktestTimelinePoint,
    ContributionFrequency,
    ResolvedPosition,
)
from app.services.backtesting.queries import (
    MAX_PRICE_FALLBACK_TRADING_DAYS,
    first_price_on_or_after,
    last_price_on_or_before,
    load_congress_signals,
    load_insider_signals,
    load_owned_saved_screen,
    load_owned_watchlist,
    load_price_histories,
    load_saved_screen_current_symbols,
    load_saved_screen_entry_signals,
    load_watchlist_symbols,
    nearest_price_on_date,
    price_on_or_before,
    ResolvedPrice,
    sorted_price_dates,
)

logger = logging.getLogger(__name__)

EPSILON = 1e-8


@dataclass(frozen=True)
class SkippedPosition:
    symbol: str
    reason: str
    source_event_id: int | None = None


@dataclass(frozen=True)
class PositionBuildResult:
    positions: list[ResolvedPosition]
    skipped: list[SkippedPosition]


@dataclass
class PortfolioState:
    cash: float
    shares_by_position: dict[int, float]


@dataclass(frozen=True)
class SimulationResult:
    timeline: list[BacktestTimelinePoint]
    strategy_daily_returns: list[float]
    benchmark_daily_returns: list[float]
    total_contributions: float
    diagnostics: BacktestDiagnostics
    warnings: list[SkippedPosition]
    price_fallback_positions: set[tuple[str, int | None, str]]


def _rounded(value: float | None) -> float:
    return float(round(float(value or 0.0), 6))


def _clamp_cash(value: float) -> float:
    if abs(value) <= EPSILON:
        return 0.0
    return float(value)


def _add_months(anchor: date, months: int) -> date:
    total_month = (anchor.month - 1) + months
    year = anchor.year + (total_month // 12)
    month = (total_month % 12) + 1
    day = min(anchor.day, monthrange(year, month)[1])
    return date(year, month, day)


def _frequency_months(frequency: ContributionFrequency | str) -> int | None:
    if frequency == "none":
        return None
    if frequency == "monthly":
        return 1
    if frequency == "quarterly":
        return 3
    if frequency == "semi_annually":
        return 6
    if frequency == "annually":
        return 12
    return None


def _scheduled_trading_days(master_dates: list[str], *, anchor: date, end_date: date, months: int | None) -> list[str]:
    if months is None or not master_dates:
        return []
    scheduled_days: list[str] = []
    cursor = _add_months(anchor, months)
    while cursor <= end_date:
        index = bisect_left(master_dates, cursor.isoformat())
        if index < len(master_dates):
            trading_day = master_dates[index]
            if trading_day not in scheduled_days:
                scheduled_days.append(trading_day)
        cursor = _add_months(cursor, months)
    return scheduled_days


def _aggregate_skip_reasons(skipped: list[SkippedPosition]) -> list[str]:
    counts = Counter(item.reason for item in skipped)
    return [f"{reason} ({count})" for reason, count in sorted(counts.items())]


def _warn(warnings: list[SkippedPosition], *, symbol: str, reason: str, source_event_id: int | None = None) -> None:
    warnings.append(SkippedPosition(symbol=symbol, reason=reason, source_event_id=source_event_id))
    logger.warning("Backtest warning for %s: %s", symbol, reason)


def _position_key(position: ResolvedPosition) -> tuple[str, int | None, str]:
    return (position.symbol, position.source_event_id, position.entry_date.isoformat())


def _position_points(positions: list[ResolvedPosition]) -> list[BacktestPositionPoint]:
    return [
        BacktestPositionPoint(
            symbol=position.symbol,
            entry_date=position.entry_date.isoformat(),
            exit_date=position.exit_date.isoformat(),
            entry_price=position.entry_price,
            exit_price=position.exit_price,
            return_pct=position.return_pct,
            source_event_id=position.source_event_id,
            source_label=position.source_label,
            price_fallback_used=position.price_fallback_used,
        )
        for position in sorted(positions, key=lambda item: (item.entry_date, item.symbol, item.source_event_id or 0))
    ]


def _build_trading_calendar(
    *,
    positions: list[ResolvedPosition],
    price_histories: dict[str, dict[str, float]],
    benchmark_history: dict[str, float],
    start_date: date,
    end_date: date,
) -> list[str]:
    benchmark_dates = [
        day for day in sorted_price_dates(benchmark_history) if start_date.isoformat() <= day <= end_date.isoformat()
    ]
    if benchmark_dates:
        return benchmark_dates
    return sorted(
        {
            day
            for position in positions
            for day in sorted_price_dates(price_histories.get(position.symbol, {}))
            if start_date.isoformat() <= day <= end_date.isoformat()
        }
    )


def _exact_price_on_date(target_day: str, price_map: dict[str, float]) -> float | None:
    value = price_map.get(target_day)
    if value is None:
        return None
    return float(value)


def _valuation_price_on_date(target_day: str, price_map: dict[str, float], sorted_dates: list[str]) -> float | None:
    return price_on_or_before(target_day, price_map, sorted_dates)


def _portfolio_snapshot(
    *,
    state: PortfolioState,
    current_day: str,
    position_lookup: dict[int, ResolvedPosition],
    price_histories: dict[str, dict[str, float]],
    sorted_symbol_dates: dict[str, list[str]],
) -> tuple[float, dict[int, float]]:
    values_by_position: dict[int, float] = {}
    total_value = float(state.cash)
    for index, shares in state.shares_by_position.items():
        if shares <= 0:
            continue
        position = position_lookup.get(index)
        if position is None:
            continue
        valuation_price = _valuation_price_on_date(
            current_day,
            price_histories.get(position.symbol, {}),
            sorted_symbol_dates.get(position.symbol, []),
        )
        if valuation_price is None or valuation_price <= 0:
            continue
        position_value = shares * valuation_price
        values_by_position[index] = position_value
        total_value += position_value
    return float(total_value), values_by_position


def _trade_price_on_day(
    *,
    target_day: str,
    price_map: dict[str, float],
    prefer_previous: bool,
) -> ResolvedPrice | None:
    return nearest_price_on_date(
        date.fromisoformat(target_day),
        price_map,
        prefer_previous=prefer_previous,
        max_backward_trading_days=MAX_PRICE_FALLBACK_TRADING_DAYS,
        max_forward_trading_days=MAX_PRICE_FALLBACK_TRADING_DAYS,
    )


def _rebalance_portfolio(
    *,
    state: PortfolioState,
    current_day: str,
    position_lookup: dict[int, ResolvedPosition],
    price_histories: dict[str, dict[str, float]],
    sorted_symbol_dates: dict[str, list[str]],
    target_allocations: dict[int, float] | None,
    warnings: list[SkippedPosition],
    price_fallback_positions: set[tuple[str, int | None, str]],
) -> None:
    current_value, values_by_position = _portfolio_snapshot(
        state=state,
        current_day=current_day,
        position_lookup=position_lookup,
        price_histories=price_histories,
        sorted_symbol_dates=sorted_symbol_dates,
    )

    active_indexes = [
        index
        for index, position in position_lookup.items()
        if position.entry_date.isoformat() <= current_day and position.exit_date.isoformat() > current_day
    ]

    for index in list(state.shares_by_position.keys()):
        if index in active_indexes:
            continue
        position = position_lookup[index]
        resolved_trade_price = _trade_price_on_day(
            target_day=current_day,
            price_map=price_histories.get(position.symbol, {}),
            prefer_previous=True,
        )
        if resolved_trade_price is None or resolved_trade_price.close <= 0:
            _warn(
                warnings,
                symbol=position.symbol,
                source_event_id=position.source_event_id,
                reason="No valid close was found within the fallback window for an exit trade.",
            )
            continue
        if resolved_trade_price.used_fallback:
            price_fallback_positions.add(_position_key(position))
        state.cash += state.shares_by_position.pop(index, 0.0) * resolved_trade_price.close

    current_value, values_by_position = _portfolio_snapshot(
        state=state,
        current_day=current_day,
        position_lookup=position_lookup,
        price_histories=price_histories,
        sorted_symbol_dates=sorted_symbol_dates,
    )
    if not active_indexes or current_value <= 0:
        state.cash = _clamp_cash(state.cash)
        return

    trade_prices: dict[int, float] = {}
    tradable_active_indexes: list[int] = []
    frozen_holdings_value = 0.0
    carried_forward_indexes: dict[int, float] = {}
    for index in active_indexes:
        position = position_lookup[index]
        resolved_trade_price = _trade_price_on_day(
            target_day=current_day,
            price_map=price_histories.get(position.symbol, {}),
            prefer_previous=True,
        )
        if resolved_trade_price is None or resolved_trade_price.close <= 0:
            if index in state.shares_by_position:
                frozen_holdings_value += values_by_position.get(index, 0.0)
                carried_forward_indexes[index] = state.shares_by_position[index]
            _warn(
                warnings,
                symbol=position.symbol,
                source_event_id=position.source_event_id,
                reason="No valid close was found within the fallback window for a rebalance trade.",
            )
            continue
        if resolved_trade_price.used_fallback:
            price_fallback_positions.add(_position_key(position))
        trade_prices[index] = resolved_trade_price.close
        tradable_active_indexes.append(index)

    for index, shares in state.shares_by_position.items():
        if index in active_indexes or shares <= EPSILON:
            continue
        position = position_lookup[index]
        resolved_trade_price = _trade_price_on_day(
            target_day=current_day,
            price_map=price_histories.get(position.symbol, {}),
            prefer_previous=True,
        )
        if resolved_trade_price is not None and resolved_trade_price.close > 0:
            continue
        frozen_holdings_value += values_by_position.get(index, 0.0)
        carried_forward_indexes[index] = shares

    if target_allocations:
        desired_value_by_index = {
            index: max(current_value * target_allocations.get(index, 0.0), 0.0)
            for index in active_indexes
        }
    else:
        equal_weight = 1.0 / max(len(active_indexes), 1)
        desired_value_by_index = {index: current_value * equal_weight for index in active_indexes}

    tradable_budget = max(current_value - frozen_holdings_value, 0.0)
    desired_tradable_value = sum(desired_value_by_index.get(index, 0.0) for index in tradable_active_indexes)
    scale = min(tradable_budget / desired_tradable_value, 1.0) if desired_tradable_value > EPSILON else 0.0

    next_shares = dict(carried_forward_indexes)
    for index in tradable_active_indexes:
        trade_price = trade_prices[index]
        target_value = desired_value_by_index.get(index, 0.0) * scale
        target_shares = target_value / trade_price if trade_price > 0 else 0.0
        if target_shares > EPSILON:
            next_shares[index] = target_shares

    state.shares_by_position = next_shares
    holdings_value = frozen_holdings_value + sum(
        shares * trade_prices[index]
        for index, shares in next_shares.items()
        if index in trade_prices
    )
    state.cash = _clamp_cash(current_value - holdings_value)
    if state.cash < -EPSILON:
        _warn(
            warnings,
            symbol="PORTFOLIO",
            reason="Rebalance attempted to over-allocate capital; cash was clamped back to zero.",
        )
        state.cash = 0.0


def build_static_positions(
    *,
    symbols: Iterable[str],
    price_histories: dict[str, dict[str, float]],
    start_date: date,
    end_date: date,
    source_label: str | None = None,
) -> PositionBuildResult:
    positions: list[ResolvedPosition] = []
    skipped: list[SkippedPosition] = []
    for symbol in sorted({symbol for symbol in symbols if symbol}):
        price_map = price_histories.get(symbol, {})
        if not price_map:
            skipped.append(SkippedPosition(symbol=symbol, reason="Missing daily close history in the selected window."))
            continue
        entry = first_price_on_or_after(start_date, price_map, max_trading_days=MAX_PRICE_FALLBACK_TRADING_DAYS)
        if entry is None:
            skipped.append(SkippedPosition(symbol=symbol, reason="No entry close was found within the fallback window."))
            continue
        exit_point = nearest_price_on_date(
            end_date,
            price_map,
            prefer_previous=True,
            max_backward_trading_days=MAX_PRICE_FALLBACK_TRADING_DAYS,
            max_forward_trading_days=MAX_PRICE_FALLBACK_TRADING_DAYS,
        )
        if exit_point is None:
            skipped.append(SkippedPosition(symbol=symbol, reason="No exit close was found within the fallback window."))
            continue
        entry_date, entry_price = entry.date, entry.close
        exit_date, exit_price = exit_point.date, exit_point.close
        if exit_date < entry_date or entry_price <= 0 or exit_price <= 0:
            skipped.append(SkippedPosition(symbol=symbol, reason="Entry or exit pricing was invalid for simulation."))
            continue
        positions.append(
            ResolvedPosition(
                symbol=symbol,
                entry_date=entry_date,
                exit_date=exit_date,
                entry_price=entry_price,
                exit_price=exit_price,
                return_pct=_rounded(((exit_price / entry_price) - 1.0) * 100.0),
                source_label=source_label,
                truncated_at_end=True,
                price_fallback_used=entry.used_fallback or exit_point.used_fallback,
            )
        )
    return PositionBuildResult(positions=positions, skipped=skipped)


def build_signal_positions(
    *,
    signals: list[BacktestSignal],
    price_histories: dict[str, dict[str, float]],
    end_date: date,
    hold_days: int,
    source_label: str | None = None,
) -> PositionBuildResult:
    positions: list[ResolvedPosition] = []
    skipped: list[SkippedPosition] = []
    for signal in sorted(signals, key=lambda item: (item.signal_date, item.symbol, item.source_event_id or 0)):
        price_map = price_histories.get(signal.symbol, {})
        if not price_map:
            skipped.append(
                SkippedPosition(
                    symbol=signal.symbol,
                    source_event_id=signal.source_event_id,
                    reason="Missing daily close history for the signal window.",
                )
            )
            continue
        entry = first_price_on_or_after(signal.signal_date, price_map, max_trading_days=MAX_PRICE_FALLBACK_TRADING_DAYS)
        if entry is None:
            skipped.append(
                SkippedPosition(
                    symbol=signal.symbol,
                    source_event_id=signal.source_event_id,
                    reason="No entry close was found within the fallback window.",
                )
            )
            continue
        entry_date, entry_price = entry.date, entry.close
        if entry_date > end_date or entry_price <= 0:
            skipped.append(
                SkippedPosition(
                    symbol=signal.symbol,
                    source_event_id=signal.source_event_id,
                    reason="Entry close fell outside the selected backtest range.",
                )
            )
            continue

        planned_exit_date = entry_date + timedelta(days=hold_days)
        truncated = planned_exit_date > end_date
        exit_target_date = end_date if truncated else planned_exit_date
        exit_point = nearest_price_on_date(
            exit_target_date,
            price_map,
            prefer_previous=True,
            max_backward_trading_days=MAX_PRICE_FALLBACK_TRADING_DAYS,
            max_forward_trading_days=MAX_PRICE_FALLBACK_TRADING_DAYS,
        )
        if exit_point is None and not truncated:
            truncated = True
            exit_point = nearest_price_on_date(
                end_date,
                price_map,
                prefer_previous=True,
                max_backward_trading_days=MAX_PRICE_FALLBACK_TRADING_DAYS,
                max_forward_trading_days=MAX_PRICE_FALLBACK_TRADING_DAYS,
            )
        if exit_point is None:
            skipped.append(
                SkippedPosition(
                    symbol=signal.symbol,
                    source_event_id=signal.source_event_id,
                    reason="No exit close was found within the fallback window.",
                )
            )
            continue
        exit_date, exit_price = exit_point.date, exit_point.close
        if exit_date < entry_date or exit_price <= 0:
            skipped.append(
                SkippedPosition(
                    symbol=signal.symbol,
                    source_event_id=signal.source_event_id,
                    reason="Exit pricing was invalid for simulation.",
                )
            )
            continue

        positions.append(
            ResolvedPosition(
                symbol=signal.symbol,
                entry_date=entry_date,
                exit_date=exit_date,
                entry_price=entry_price,
                exit_price=exit_price,
                return_pct=_rounded(((exit_price / entry_price) - 1.0) * 100.0),
                source_event_id=signal.source_event_id,
                source_label=source_label or signal.source_label,
                truncated_at_end=truncated,
                price_fallback_used=entry.used_fallback or exit_point.used_fallback,
            )
        )
    return PositionBuildResult(positions=positions, skipped=skipped)


def build_equity_timeline(
    *,
    positions: list[ResolvedPosition],
    price_histories: dict[str, dict[str, float]],
    benchmark_history: dict[str, float],
    start_date: date,
    end_date: date,
    start_balance: float,
    contribution_amount: float,
    contribution_frequency: ContributionFrequency,
    rebalancing_frequency: str,
    custom_allocations: dict[str, float] | None = None,
) -> SimulationResult:
    master_dates = _build_trading_calendar(
        positions=positions,
        price_histories=price_histories,
        benchmark_history=benchmark_history,
        start_date=start_date,
        end_date=end_date,
    )
    if not master_dates:
        empty_diagnostics = BacktestDiagnostics(
            average_active_positions=0.0,
            max_active_positions=0,
            average_invested_pct=0.0,
            max_invested_pct=0.0,
            max_position_weight_observed=0.0,
            skipped_positions_count=0,
            skipped_reasons=[],
        )
        return SimulationResult(
            timeline=[],
            strategy_daily_returns=[],
            benchmark_daily_returns=[],
            total_contributions=_rounded(start_balance),
            diagnostics=empty_diagnostics,
            warnings=[],
            price_fallback_positions=set(),
        )

    position_lookup = {index: position for index, position in enumerate(positions)}
    sorted_symbol_dates = {symbol: sorted_price_dates(price_map) for symbol, price_map in price_histories.items()}
    benchmark_sorted_dates = sorted_price_dates(benchmark_history)
    position_target_allocations = (
        {
            index: max(float(custom_allocations.get(position.symbol, 0.0)) / 100.0, 0.0)
            for index, position in position_lookup.items()
            if position.symbol in custom_allocations
        }
        if custom_allocations
        else None
    )
    scheduled_rebalance_days = set(
        [master_dates[0]]
        + _scheduled_trading_days(
            master_dates,
            anchor=start_date,
            end_date=end_date,
            months=_frequency_months(rebalancing_frequency),
        )
    )
    contribution_days = Counter(
        _scheduled_trading_days(
            master_dates,
            anchor=start_date,
            end_date=end_date,
            months=_frequency_months(contribution_frequency),
        )
    )

    strategy_state = PortfolioState(cash=float(start_balance), shares_by_position={})
    benchmark_cash = float(start_balance)
    benchmark_shares = 0.0
    total_contributions = float(start_balance)
    warnings: list[SkippedPosition] = []
    price_fallback_positions: set[tuple[str, int | None, str]] = {
        _position_key(position) for position in positions if position.price_fallback_used
    }
    timeline: list[BacktestTimelinePoint] = []
    strategy_daily_returns: list[float] = []
    benchmark_daily_returns: list[float] = []
    observed_position_weights: list[float] = []

    previous_strategy_value = float(start_balance)
    previous_benchmark_value = float(start_balance)

    for current_day in master_dates:
        beginning_strategy_value = previous_strategy_value
        beginning_benchmark_value = previous_benchmark_value
        contribution_today = float(contribution_amount * contribution_days.get(current_day, 0)) if contribution_amount > 0 else 0.0
        if contribution_today > 0:
            strategy_state.cash += contribution_today
            benchmark_cash += contribution_today
            total_contributions += contribution_today

        for index in list(strategy_state.shares_by_position.keys()):
            position = position_lookup[index]
            if position.exit_date.isoformat() > current_day:
                continue
            resolved_trade_price = _trade_price_on_day(
                target_day=current_day,
                price_map=price_histories.get(position.symbol, {}),
                prefer_previous=True,
            )
            if resolved_trade_price is None or resolved_trade_price.close <= 0:
                _warn(
                    warnings,
                    symbol=position.symbol,
                    source_event_id=position.source_event_id,
                    reason="No valid close was found within the fallback window for an exit trade.",
                )
                continue
            if resolved_trade_price.used_fallback:
                price_fallback_positions.add(_position_key(position))
            strategy_state.cash += strategy_state.shares_by_position.pop(index, 0.0) * resolved_trade_price.close

        if current_day in scheduled_rebalance_days:
            _rebalance_portfolio(
                state=strategy_state,
                current_day=current_day,
                position_lookup=position_lookup,
                price_histories=price_histories,
                sorted_symbol_dates=sorted_symbol_dates,
                target_allocations=position_target_allocations,
                warnings=warnings,
                price_fallback_positions=price_fallback_positions,
            )

        benchmark_trade_price = _trade_price_on_day(
            target_day=current_day,
            price_map=benchmark_history,
            prefer_previous=True,
        )
        if benchmark_cash > EPSILON:
            if benchmark_trade_price is None or benchmark_trade_price.close <= 0:
                _warn(
                    warnings,
                    symbol=DEFAULT_BENCHMARK,
                    reason="No valid close was found within the fallback window for the benchmark.",
                )
            else:
                benchmark_shares += benchmark_cash / benchmark_trade_price.close
                benchmark_cash = 0.0

        ending_strategy_value, strategy_values_by_position = _portfolio_snapshot(
            state=strategy_state,
            current_day=current_day,
            position_lookup=position_lookup,
            price_histories=price_histories,
            sorted_symbol_dates=sorted_symbol_dates,
        )
        benchmark_valuation_price = _valuation_price_on_date(current_day, benchmark_history, benchmark_sorted_dates)
        ending_benchmark_value = benchmark_cash + (
            benchmark_shares * benchmark_valuation_price
            if benchmark_valuation_price is not None and benchmark_valuation_price > 0
            else 0.0
        )

        strategy_daily_return = (
            (ending_strategy_value - beginning_strategy_value - contribution_today) / beginning_strategy_value
            if beginning_strategy_value > EPSILON
            else 0.0
        )
        benchmark_daily_return = (
            (ending_benchmark_value - beginning_benchmark_value - contribution_today) / beginning_benchmark_value
            if beginning_benchmark_value > EPSILON
            else 0.0
        )
        strategy_daily_returns.append(float(strategy_daily_return))
        benchmark_daily_returns.append(float(benchmark_daily_return))

        invested_value = max(ending_strategy_value - strategy_state.cash, 0.0)
        invested_pct = (invested_value / ending_strategy_value) * 100.0 if ending_strategy_value > EPSILON else 0.0
        current_position_weights = [
            (value / ending_strategy_value) * 100.0
            for value in strategy_values_by_position.values()
            if ending_strategy_value > EPSILON
        ]
        max_weight_today = max(current_position_weights, default=0.0)
        observed_position_weights.append(max_weight_today)
        if invested_pct > 100.0 + 1e-4:
            _warn(warnings, symbol="PORTFOLIO", reason="Gross exposure exceeded 100% before clamping.")

        timeline.append(
            BacktestTimelinePoint(
                date=current_day,
                strategy_value=_rounded(ending_strategy_value),
                benchmark_value=_rounded(ending_benchmark_value),
                strategy_return_pct=_rounded(cumulative_return_pct_from_daily_returns(strategy_daily_returns)),
                benchmark_return_pct=_rounded(cumulative_return_pct_from_daily_returns(benchmark_daily_returns)),
                active_positions=sum(
                    1
                    for position in positions
                    if position.entry_date.isoformat() <= current_day <= position.exit_date.isoformat()
                ),
                invested_pct=_rounded(min(max(invested_pct, 0.0), 100.0)),
                cash=_rounded(max(strategy_state.cash, 0.0)),
                daily_return_pct=_rounded(strategy_daily_return * 100.0),
            )
        )

        previous_strategy_value = ending_strategy_value
        previous_benchmark_value = ending_benchmark_value

    diagnostics = BacktestDiagnostics(
        average_active_positions=_rounded(
            sum(point.active_positions for point in timeline) / len(timeline) if timeline else 0.0
        ),
        max_active_positions=max((point.active_positions for point in timeline), default=0),
        average_invested_pct=_rounded(
            sum(point.invested_pct for point in timeline) / len(timeline) if timeline else 0.0
        ),
        max_invested_pct=_rounded(max((point.invested_pct for point in timeline), default=0.0)),
        max_position_weight_observed=_rounded(max(observed_position_weights, default=0.0)),
        skipped_positions_count=len(warnings),
        skipped_reasons=_aggregate_skip_reasons(warnings),
        price_fallback_positions_count=len(price_fallback_positions),
    )
    return SimulationResult(
        timeline=timeline,
        strategy_daily_returns=strategy_daily_returns,
        benchmark_daily_returns=benchmark_daily_returns,
        total_contributions=_rounded(total_contributions),
        diagnostics=diagnostics,
        warnings=warnings,
        price_fallback_positions=price_fallback_positions,
    )


def _base_assumptions(config: BacktestStrategyConfig) -> list[str]:
    return [
        "The portfolio uses a capital-constrained model. Total exposure is capped at 100%, with equal-weight allocations unless custom weights are provided.",
        "New entries only enter on scheduled rebalance dates, and exits move proceeds back to cash at the close.",
        "Returns, drawdown, Sharpe, volatility, and CAGR are time-weighted so contributions do not inflate performance.",
        "Congress and insider entries use disclosure or filing timing where available. Daily closes only. No leverage, shorting, transaction costs, or slippage in v1.",
    ]


def run_backtest(db: Session, config: BacktestStrategyConfig, *, user_id: int | None = None) -> BacktestRunResponse:
    benchmark_symbol = DEFAULT_BENCHMARK
    assumptions = _base_assumptions(config)

    positions: list[ResolvedPosition] = []
    skipped: list[SkippedPosition] = []
    price_histories: dict[str, dict[str, float]] = {}
    trade_count = 0

    if config.strategy_type == "watchlist":
        if user_id is None:
            raise HTTPException(status_code=401, detail="Sign in required.")
        watchlist = load_owned_watchlist(db, watchlist_id=int(config.watchlist_id or 0), user_id=user_id)
        if watchlist is None:
            raise HTTPException(status_code=404, detail="Watchlist not found.")
        symbols = load_watchlist_symbols(db, watchlist_id=watchlist.id)
        trade_count = len(symbols)
        price_histories = load_price_histories(db, symbols + [benchmark_symbol], config.start_date, config.end_date)
        position_result = build_static_positions(
            symbols=symbols,
            price_histories=price_histories,
            start_date=config.start_date,
            end_date=config.end_date,
            source_label=watchlist.name,
        )
        positions = position_result.positions
        skipped = position_result.skipped
        assumptions.append("Watchlist v1 uses the current watchlist constituents held across the selected period.")
    elif config.strategy_type == "custom_tickers":
        symbols = config.tickers
        trade_count = len(symbols)
        price_histories = load_price_histories(db, symbols + [benchmark_symbol], config.start_date, config.end_date)
        position_result = build_static_positions(
            symbols=symbols,
            price_histories=price_histories,
            start_date=config.start_date,
            end_date=config.end_date,
            source_label=config.source_label or "Custom tickers",
        )
        positions = position_result.positions
        skipped = position_result.skipped
        assumptions.append("Custom tickers v1 holds the selected symbols from the chosen start date through the selected end date.")
    elif config.strategy_type == "saved_screen":
        if user_id is None:
            raise HTTPException(status_code=401, detail="Sign in required.")
        screen = load_owned_saved_screen(db, saved_screen_id=int(config.saved_screen_id or 0), user_id=user_id)
        if screen is None:
            raise HTTPException(status_code=404, detail="Saved screen not found.")
        historical_signals = load_saved_screen_entry_signals(
            db,
            screen=screen,
            start_date=config.start_date,
            end_date=config.end_date,
            hold_days=config.hold_days,
        )
        if historical_signals:
            signal_symbols = sorted({signal.symbol for signal in historical_signals})
            history_start = min((signal.signal_date for signal in historical_signals), default=config.start_date)
            trade_count = len(historical_signals)
            price_histories = load_price_histories(db, signal_symbols + [benchmark_symbol], history_start, config.end_date)
            position_result = build_signal_positions(
                signals=historical_signals,
                price_histories=price_histories,
                end_date=config.end_date,
                hold_days=config.hold_days,
            )
            positions = position_result.positions
            skipped = position_result.skipped
            assumptions.append("Saved screen v1 uses saved-screen entry events as historical signals when available.")
        else:
            symbols, source_mode = load_saved_screen_current_symbols(db, screen=screen)
            trade_count = len(symbols)
            price_histories = load_price_histories(db, symbols + [benchmark_symbol], config.start_date, config.end_date)
            position_result = build_static_positions(
                symbols=symbols,
                price_histories=price_histories,
                start_date=config.start_date,
                end_date=config.end_date,
                source_label=screen.name,
            )
            positions = position_result.positions
            skipped = position_result.skipped
            if source_mode == "snapshot":
                assumptions.append(
                    "Saved screen v1 falls back to the current saved-screen snapshot universe when historical entry events are unavailable."
                )
            else:
                assumptions.append(
                    "Saved screen v1 uses available saved-screen monitoring history where present; otherwise it backtests the current matching universe over the selected historical period."
                )
    else:
        signals = load_congress_signals(db, config) if config.strategy_type == "congress" else load_insider_signals(db, config)
        signal_symbols = sorted({signal.symbol for signal in signals})
        history_start = min((signal.signal_date for signal in signals), default=config.start_date)
        trade_count = len(signals)
        price_histories = load_price_histories(db, signal_symbols + [benchmark_symbol], history_start, config.end_date)
        position_result = build_signal_positions(
            signals=signals,
            price_histories=price_histories,
            end_date=config.end_date,
            hold_days=config.hold_days,
        )
        positions = position_result.positions
        skipped = position_result.skipped

    benchmark_history = price_histories.get(benchmark_symbol, {})
    position_price_histories = {
        symbol: price_map for symbol, price_map in price_histories.items() if symbol != benchmark_symbol
    }
    simulation = build_equity_timeline(
        positions=positions,
        price_histories=position_price_histories,
        benchmark_history=benchmark_history,
        start_date=config.start_date,
        end_date=config.end_date,
        start_balance=config.start_balance,
        contribution_amount=config.contribution_amount,
        contribution_frequency=config.contribution_frequency,
        rebalancing_frequency=config.rebalancing_frequency,
        custom_allocations=config.custom_allocations,
    )

    all_skipped = skipped + simulation.warnings
    if simulation.price_fallback_positions:
        assumptions.append("Missing exact-date closes use a bounded nearest-trading-day fallback before a position is skipped.")
    if all_skipped:
        assumptions.append("Only positions that still lacked a valid close after fallback are reported as skipped in diagnostics.")

    closed_position_returns = [position.return_pct for position in positions if not position.truncated_at_end]
    diagnostics = BacktestDiagnostics(
        average_active_positions=simulation.diagnostics.average_active_positions,
        max_active_positions=simulation.diagnostics.max_active_positions,
        average_invested_pct=simulation.diagnostics.average_invested_pct,
        max_invested_pct=simulation.diagnostics.max_invested_pct,
        max_position_weight_observed=simulation.diagnostics.max_position_weight_observed,
        skipped_positions_count=len(all_skipped),
        skipped_reasons=_aggregate_skip_reasons(all_skipped),
        price_fallback_positions_count=len(simulation.price_fallback_positions),
    )

    if not simulation.timeline:
        return BacktestRunResponse(
            summary=BacktestSummary(
                start_balance=_rounded(config.start_balance),
                ending_balance=_rounded(config.start_balance),
                benchmark_ending_balance=_rounded(config.start_balance),
                total_contributions=_rounded(config.start_balance),
                net_profit=0.0,
                strategy_return_pct=0.0,
                time_weighted_return_pct=0.0,
                benchmark_return_pct=0.0,
                alpha_pct=0.0,
                cagr_pct=0.0,
                sharpe_ratio=None,
                win_rate=_rounded(compute_win_rate_pct(closed_position_returns)),
                max_drawdown_pct=0.0,
                volatility_pct=0.0,
                trade_count=trade_count,
                positions_count=len(positions),
                skipped_positions_count=len(all_skipped),
                skipped_reasons=diagnostics.skipped_reasons,
                price_fallback_positions_count=len(simulation.price_fallback_positions),
            ),
            timeline=[],
            positions=_position_points(positions),
            assumptions=assumptions,
            diagnostics=diagnostics,
        )

    strategy_values = [point.strategy_value for point in simulation.timeline]
    benchmark_values = [point.benchmark_value for point in simulation.timeline]
    strategy_return_pct = _rounded(cumulative_return_pct_from_daily_returns(simulation.strategy_daily_returns))
    benchmark_return_pct = _rounded(cumulative_return_pct_from_daily_returns(simulation.benchmark_daily_returns))
    days_elapsed = max(
        (date.fromisoformat(simulation.timeline[-1].date) - date.fromisoformat(simulation.timeline[0].date)).days,
        1,
    )
    sharpe_ratio = compute_sharpe_ratio(simulation.strategy_daily_returns)
    indexed_curve = indexed_curve_from_daily_returns(simulation.strategy_daily_returns)

    return BacktestRunResponse(
        summary=BacktestSummary(
            start_balance=_rounded(config.start_balance),
            ending_balance=_rounded(strategy_values[-1]),
            benchmark_ending_balance=_rounded(benchmark_values[-1]),
            total_contributions=_rounded(simulation.total_contributions),
            net_profit=_rounded(strategy_values[-1] - simulation.total_contributions),
            strategy_return_pct=strategy_return_pct,
            time_weighted_return_pct=strategy_return_pct,
            benchmark_return_pct=benchmark_return_pct,
            alpha_pct=_rounded(strategy_return_pct - benchmark_return_pct),
            cagr_pct=_rounded(compute_cagr_pct(strategy_return_pct, days_elapsed / 365.25)),
            sharpe_ratio=_rounded(sharpe_ratio) if sharpe_ratio is not None else None,
            win_rate=_rounded(compute_win_rate_pct(closed_position_returns)),
            max_drawdown_pct=_rounded(compute_max_drawdown_pct(indexed_curve)),
            volatility_pct=_rounded(compute_volatility_pct_from_daily_returns(simulation.strategy_daily_returns)),
            trade_count=trade_count,
            positions_count=len(positions),
            skipped_positions_count=len(all_skipped),
            skipped_reasons=diagnostics.skipped_reasons,
            price_fallback_positions_count=len(simulation.price_fallback_positions),
        ),
        timeline=simulation.timeline,
        positions=_position_points(positions),
        assumptions=assumptions,
        diagnostics=diagnostics,
    )
