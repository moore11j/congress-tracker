from __future__ import annotations

from bisect import bisect_left
from calendar import monthrange
from collections import Counter
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Iterable

from fastapi import HTTPException
from sqlalchemy.orm import Session

from app.services.backtesting.metrics import (
    compute_cagr_pct,
    compute_max_drawdown_pct,
    compute_sharpe_ratio,
    compute_volatility_pct_from_daily_returns,
    compute_win_rate_pct,
)
from app.services.backtesting.models import (
    DEFAULT_BENCHMARK,
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
    price_on_or_before,
    sorted_price_dates,
)


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


def _rounded(value: float | None) -> float:
    return float(round(float(value or 0.0), 6))


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
    benchmark_dates = [day for day in sorted_price_dates(benchmark_history) if start_date.isoformat() <= day <= end_date.isoformat()]
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
        entry = first_price_on_or_after(start_date, price_map)
        if entry is None:
            skipped.append(SkippedPosition(symbol=symbol, reason="No entry close on or after the selected start date."))
            continue
        exit_point = last_price_on_or_before(end_date, price_map)
        if exit_point is None:
            skipped.append(SkippedPosition(symbol=symbol, reason="No exit close on or before the selected end date."))
            continue
        entry_date, entry_price = entry
        exit_date, exit_price = exit_point
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
            )
        )
    return PositionBuildResult(positions=positions, skipped=skipped)


def build_signal_positions(
    *,
    signals: list[BacktestSignal],
    price_histories: dict[str, dict[str, float]],
    end_date: date,
    hold_days: int,
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
        entry = first_price_on_or_after(signal.signal_date, price_map)
        if entry is None:
            skipped.append(
                SkippedPosition(
                    symbol=signal.symbol,
                    source_event_id=signal.source_event_id,
                    reason="No entry close on or after the disclosure or filing date.",
                )
            )
            continue
        entry_date, entry_price = entry
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
        if truncated:
            exit_point = last_price_on_or_before(end_date, price_map)
        else:
            exit_point = first_price_on_or_after(planned_exit_date, price_map)
            if exit_point is None or exit_point[0] > end_date:
                truncated = True
                exit_point = last_price_on_or_before(end_date, price_map)
        if exit_point is None:
            skipped.append(
                SkippedPosition(
                    symbol=signal.symbol,
                    source_event_id=signal.source_event_id,
                    reason="No exit close was available before the selected end date.",
                )
            )
            continue
        exit_date, exit_price = exit_point
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
                source_label=signal.source_label,
                truncated_at_end=truncated,
            )
        )
    return PositionBuildResult(positions=positions, skipped=skipped)


def _rebalance_portfolio(
    *,
    active_indexes: list[int],
    current_day: str,
    position_lookup: dict[int, ResolvedPosition],
    price_histories: dict[str, dict[str, float]],
    sorted_symbol_dates: dict[str, list[str]],
    state: PortfolioState,
) -> None:
    if not active_indexes:
        state.shares_by_position = {}
        return

    total_value = state.cash
    prices_by_position: dict[int, float] = {}
    next_shares: dict[int, float] = {}

    for index, shares in list(state.shares_by_position.items()):
        position = position_lookup.get(index)
        if position is None:
            continue
        current_price = price_on_or_before(
            current_day,
            price_histories.get(position.symbol, {}),
            sorted_symbol_dates.get(position.symbol, []),
        )
        if current_price is None or current_price <= 0:
            continue
        prices_by_position[index] = current_price
        total_value += shares * current_price

    for index in active_indexes:
        position = position_lookup[index]
        current_price = price_on_or_before(
            current_day,
            price_histories.get(position.symbol, {}),
            sorted_symbol_dates.get(position.symbol, []),
        )
        if current_price is None or current_price <= 0:
            continue
        prices_by_position[index] = current_price

    if not prices_by_position:
        state.shares_by_position = {}
        return

    equal_weight_value = total_value / len(active_indexes)
    invested_value = 0.0
    for index in active_indexes:
        current_price = prices_by_position.get(index)
        if current_price is None or current_price <= 0:
            continue
        shares = equal_weight_value / current_price
        next_shares[index] = shares
        invested_value += shares * current_price

    state.shares_by_position = next_shares
    state.cash = max(total_value - invested_value, 0.0)


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
) -> tuple[list[BacktestTimelinePoint], list[float], float]:
    master_dates = _build_trading_calendar(
        positions=positions,
        price_histories=price_histories,
        benchmark_history=benchmark_history,
        start_date=start_date,
        end_date=end_date,
    )
    if not master_dates:
        return [], [], 0.0

    sorted_symbol_dates = {symbol: sorted_price_dates(price_map) for symbol, price_map in price_histories.items()}
    benchmark_sorted_dates = sorted_price_dates(benchmark_history)
    position_lookup = {index: position for index, position in enumerate(positions)}

    position_entries_by_day: dict[str, list[int]] = {}
    position_exits_by_day: dict[str, list[int]] = {}
    for index, position in position_lookup.items():
        if position.entry_date.isoformat() <= master_dates[-1] and position.exit_date.isoformat() >= master_dates[0]:
            position_entries_by_day.setdefault(position.entry_date.isoformat(), []).append(index)
            position_exits_by_day.setdefault(position.exit_date.isoformat(), []).append(index)

    scheduled_rebalance_days = set(
        _scheduled_trading_days(
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
    total_contributions = 0.0

    prior_strategy_value = float(start_balance)
    prior_benchmark_value = float(start_balance)
    strategy_index = 100.0
    benchmark_index = 100.0
    strategy_daily_returns: list[float] = []
    timeline: list[BacktestTimelinePoint] = []

    first_day = master_dates[0]

    for offset, current_day in enumerate(master_dates):
        display_active_indexes = [
            index
            for index, position in position_lookup.items()
            if position.entry_date.isoformat() <= current_day <= position.exit_date.isoformat()
        ]
        rebalance_active_indexes = [
            index
            for index, position in position_lookup.items()
            if position.entry_date.isoformat() <= current_day
            and (position.exit_date.isoformat() > current_day or (position.exit_date.isoformat() == current_day and position.truncated_at_end))
        ]

        strategy_market_value = strategy_state.cash
        for index, shares in strategy_state.shares_by_position.items():
            position = position_lookup[index]
            current_price = price_on_or_before(
                current_day,
                price_histories.get(position.symbol, {}),
                sorted_symbol_dates.get(position.symbol, []),
            )
            if current_price is None or current_price <= 0:
                continue
            strategy_market_value += shares * current_price

        benchmark_price = price_on_or_before(current_day, benchmark_history, benchmark_sorted_dates)
        benchmark_market_value = benchmark_cash + ((benchmark_shares * benchmark_price) if benchmark_price is not None and benchmark_price > 0 else 0.0)

        if offset > 0 and prior_strategy_value > 0:
            strategy_daily_return = (strategy_market_value / prior_strategy_value) - 1.0
            strategy_daily_returns.append(strategy_daily_return)
            strategy_index *= 1.0 + strategy_daily_return
        if offset > 0 and prior_benchmark_value > 0:
            benchmark_daily_return = (benchmark_market_value / prior_benchmark_value) - 1.0
            benchmark_index *= 1.0 + benchmark_daily_return

        contribution_count = contribution_days.get(current_day, 0)
        if contribution_count > 0 and contribution_amount > 0:
            contribution_total = contribution_amount * contribution_count
            total_contributions += contribution_total
            strategy_state.cash += contribution_total
            benchmark_cash += contribution_total

        for index in position_exits_by_day.get(current_day, []):
            position = position_lookup[index]
            if position.truncated_at_end:
                continue
            shares = strategy_state.shares_by_position.pop(index, 0.0)
            if shares <= 0:
                continue
            current_price = price_on_or_before(
                current_day,
                price_histories.get(position.symbol, {}),
                sorted_symbol_dates.get(position.symbol, []),
            )
            if current_price is None or current_price <= 0:
                continue
            strategy_state.cash += shares * current_price

        should_rebalance = (
            current_day == first_day
            or current_day in scheduled_rebalance_days
            or bool(position_entries_by_day.get(current_day))
            or bool(position_exits_by_day.get(current_day))
        )
        if should_rebalance:
            _rebalance_portfolio(
                active_indexes=rebalance_active_indexes,
                current_day=current_day,
                position_lookup=position_lookup,
                price_histories=price_histories,
                sorted_symbol_dates=sorted_symbol_dates,
                state=strategy_state,
            )

        if benchmark_price is not None and benchmark_price > 0 and (current_day == first_day or contribution_count > 0 or benchmark_shares == 0.0):
            benchmark_shares += benchmark_cash / benchmark_price
            benchmark_cash = 0.0

        strategy_end_value = strategy_state.cash
        for index, shares in strategy_state.shares_by_position.items():
            position = position_lookup[index]
            current_price = price_on_or_before(
                current_day,
                price_histories.get(position.symbol, {}),
                sorted_symbol_dates.get(position.symbol, []),
            )
            if current_price is None or current_price <= 0:
                continue
            strategy_end_value += shares * current_price

        if benchmark_price is not None and benchmark_price > 0:
            benchmark_end_value = benchmark_cash + (benchmark_shares * benchmark_price)
        else:
            benchmark_end_value = benchmark_cash

        timeline.append(
            BacktestTimelinePoint(
                date=current_day,
                strategy_value=_rounded(strategy_end_value),
                benchmark_value=_rounded(benchmark_end_value),
                strategy_return_pct=_rounded(strategy_index - 100.0),
                benchmark_return_pct=_rounded(benchmark_index - 100.0),
                active_positions=len(display_active_indexes),
                cash=_rounded(strategy_state.cash),
            )
        )

        prior_strategy_value = strategy_end_value
        prior_benchmark_value = benchmark_end_value

    return timeline, strategy_daily_returns, _rounded(total_contributions)


def _base_assumptions(config: BacktestStrategyConfig) -> list[str]:
    assumptions = [
        "Capital-constrained portfolio simulation",
        "Total position weight capped at 100%",
        "Equal-weight active positions with configured schedule plus turnover rebalances",
        "Daily close prices",
        "No leverage",
        "No shorting",
        "No transaction costs or slippage in v1",
        "Congress and insider entries use disclosure or filing timing where available",
        "Benchmark uses the same contribution schedule",
    ]
    if config.contribution_amount > 0 and config.contribution_frequency != "none":
        assumptions.append(
            f"Recurring contributions are applied on the first trading close on or after each {config.contribution_frequency.replace('_', ' ')} schedule date."
        )
    else:
        assumptions.append("No recurring contributions in this run.")
    assumptions.append(
        f"Rebalancing frequency is set to {config.rebalancing_frequency.replace('_', ' ')}."
    )
    assumptions.append(
        "Strategy return %, benchmark return %, alpha, and CAGR use the time-weighted portfolio curve so deposits do not inflate performance."
    )
    return assumptions


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
    position_price_histories = {symbol: price_map for symbol, price_map in price_histories.items() if symbol != benchmark_symbol}
    timeline, strategy_daily_returns, total_contributions = build_equity_timeline(
        positions=positions,
        price_histories=position_price_histories,
        benchmark_history=benchmark_history,
        start_date=config.start_date,
        end_date=config.end_date,
        start_balance=config.start_balance,
        contribution_amount=config.contribution_amount,
        contribution_frequency=config.contribution_frequency,
        rebalancing_frequency=config.rebalancing_frequency,
    )

    assumptions.append("Open positions are marked to market at the selected end date when the holding window extends beyond the range.")
    if skipped:
        assumptions.append(f"Skipped positions: {len(skipped)} due to missing or invalid price inputs.")

    if not timeline:
        return BacktestRunResponse(
            summary=BacktestSummary(
                start_balance=_rounded(config.start_balance),
                ending_balance=_rounded(config.start_balance),
                benchmark_ending_balance=_rounded(config.start_balance),
                total_contributions=0.0,
                net_profit=0.0,
                strategy_return_pct=0.0,
                time_weighted_return_pct=0.0,
                benchmark_return_pct=0.0,
                alpha_pct=0.0,
                cagr_pct=0.0,
                sharpe_ratio=None,
                win_rate=_rounded(compute_win_rate_pct([position.return_pct for position in positions])),
                max_drawdown_pct=0.0,
                volatility_pct=0.0,
                trade_count=trade_count,
                positions_count=len(positions),
                skipped_positions_count=len(skipped),
                skipped_reasons=_aggregate_skip_reasons(skipped),
            ),
            timeline=[],
            positions=_position_points(positions),
            assumptions=assumptions,
        )

    strategy_values = [point.strategy_value for point in timeline]
    benchmark_values = [point.benchmark_value for point in timeline]
    strategy_return_pct = _rounded(timeline[-1].strategy_return_pct)
    benchmark_return_pct = _rounded(timeline[-1].benchmark_return_pct)
    timeline_start = date.fromisoformat(timeline[0].date)
    timeline_end = date.fromisoformat(timeline[-1].date)
    years = max((timeline_end - timeline_start).days, 1) / 365.25
    net_profit = strategy_values[-1] - config.start_balance - total_contributions
    sharpe_ratio = compute_sharpe_ratio(strategy_daily_returns)

    return BacktestRunResponse(
        summary=BacktestSummary(
            start_balance=_rounded(config.start_balance),
            ending_balance=_rounded(strategy_values[-1]),
            benchmark_ending_balance=_rounded(benchmark_values[-1]),
            total_contributions=_rounded(total_contributions),
            net_profit=_rounded(net_profit),
            strategy_return_pct=strategy_return_pct,
            time_weighted_return_pct=strategy_return_pct,
            benchmark_return_pct=benchmark_return_pct,
            alpha_pct=_rounded(strategy_return_pct - benchmark_return_pct),
            cagr_pct=_rounded(compute_cagr_pct(strategy_return_pct, years)),
            sharpe_ratio=_rounded(sharpe_ratio) if sharpe_ratio is not None else None,
            win_rate=_rounded(compute_win_rate_pct([position.return_pct for position in positions])),
            max_drawdown_pct=_rounded(compute_max_drawdown_pct(strategy_values)),
            volatility_pct=_rounded(compute_volatility_pct_from_daily_returns(strategy_daily_returns)),
            trade_count=trade_count,
            positions_count=len(positions),
            skipped_positions_count=len(skipped),
            skipped_reasons=_aggregate_skip_reasons(skipped),
        ),
        timeline=timeline,
        positions=_position_points(positions),
        assumptions=assumptions,
    )
