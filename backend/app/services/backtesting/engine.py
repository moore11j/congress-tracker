from __future__ import annotations

from datetime import timedelta
from statistics import mean
from typing import Iterable

from fastapi import HTTPException
from sqlalchemy.orm import Session

from app.services.backtesting.metrics import compute_max_drawdown_pct, compute_volatility_pct, compute_win_rate_pct, pct_return
from app.services.backtesting.models import (
    DEFAULT_BENCHMARK,
    BacktestPositionPoint,
    BacktestRunResponse,
    BacktestSignal,
    BacktestStrategyConfig,
    BacktestSummary,
    BacktestTimelinePoint,
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


def _rounded(value: float) -> float:
    return float(round(value, 6))


def build_static_positions(
    *,
    symbols: Iterable[str],
    price_histories: dict[str, dict[str, float]],
    start_date,
    end_date,
    source_label: str | None = None,
) -> list[ResolvedPosition]:
    positions: list[ResolvedPosition] = []
    for symbol in sorted({symbol for symbol in symbols if symbol}):
        price_map = price_histories.get(symbol, {})
        entry = first_price_on_or_after(start_date, price_map)
        exit_point = last_price_on_or_before(end_date, price_map)
        if entry is None or exit_point is None:
            continue
        entry_date, entry_price = entry
        exit_date, exit_price = exit_point
        if exit_date < entry_date or entry_price <= 0 or exit_price <= 0:
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
    return positions


def build_signal_positions(
    *,
    signals: list[BacktestSignal],
    price_histories: dict[str, dict[str, float]],
    end_date,
    hold_days: int,
) -> list[ResolvedPosition]:
    positions: list[ResolvedPosition] = []
    for signal in sorted(signals, key=lambda item: (item.signal_date, item.symbol, item.source_event_id or 0)):
        price_map = price_histories.get(signal.symbol, {})
        if not price_map:
            continue
        entry = first_price_on_or_after(signal.signal_date, price_map)
        if entry is None:
            continue
        entry_date, entry_price = entry
        if entry_date > end_date or entry_price <= 0:
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
            continue
        exit_date, exit_price = exit_point
        if exit_date < entry_date or exit_price <= 0:
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
    return positions


def build_equity_timeline(
    *,
    positions: list[ResolvedPosition],
    price_histories: dict[str, dict[str, float]],
    benchmark_history: dict[str, float],
    start_date,
    end_date,
) -> list[BacktestTimelinePoint]:
    benchmark_dates = [day for day in sorted_price_dates(benchmark_history) if start_date.isoformat() <= day <= end_date.isoformat()]
    if benchmark_dates:
        master_dates = benchmark_dates
    else:
        master_dates = sorted(
            {
                day
                for position in positions
                for day in sorted_price_dates(price_histories.get(position.symbol, {}))
                if start_date.isoformat() <= day <= end_date.isoformat()
            }
        )
    if not master_dates:
        return []

    sorted_symbol_dates = {symbol: sorted_price_dates(price_map) for symbol, price_map in price_histories.items()}
    benchmark_sorted_dates = sorted_price_dates(benchmark_history)
    benchmark_base = price_on_or_before(master_dates[0], benchmark_history, benchmark_sorted_dates) or 100.0

    strategy_value = 100.0
    timeline: list[BacktestTimelinePoint] = [
        BacktestTimelinePoint(
            date=master_dates[0],
            strategy_value=strategy_value,
            benchmark_value=100.0,
            active_positions=sum(1 for position in positions if position.entry_date.isoformat() <= master_dates[0] <= position.exit_date.isoformat()),
        )
    ]

    for previous_day, current_day in zip(master_dates, master_dates[1:]):
        active_interval = [
            position
            for position in positions
            if position.entry_date.isoformat() <= previous_day and position.exit_date.isoformat() >= current_day
        ]
        interval_returns: list[float] = []
        for position in active_interval:
            price_map = price_histories.get(position.symbol, {})
            symbol_dates = sorted_symbol_dates.get(position.symbol, [])
            previous_price = price_on_or_before(previous_day, price_map, symbol_dates)
            current_price = price_on_or_before(current_day, price_map, symbol_dates)
            if previous_price is None or current_price is None or previous_price <= 0:
                continue
            interval_returns.append((current_price / previous_price) - 1.0)
        if interval_returns:
            strategy_value *= 1.0 + mean(interval_returns)

        benchmark_price = price_on_or_before(current_day, benchmark_history, benchmark_sorted_dates) or benchmark_base
        benchmark_value = 100.0 if benchmark_base <= 0 else _rounded((benchmark_price / benchmark_base) * 100.0)
        timeline.append(
            BacktestTimelinePoint(
                date=current_day,
                strategy_value=_rounded(strategy_value),
                benchmark_value=benchmark_value,
                active_positions=sum(
                    1 for position in positions if position.entry_date.isoformat() <= current_day <= position.exit_date.isoformat()
                ),
            )
        )
    return timeline


def _base_assumptions() -> list[str]:
    return [
        "Equal-weight portfolio",
        "Daily close prices",
        "No leverage",
        "No transaction costs or slippage model in v1",
        "Congress/insider entries use disclosure/filing timing where available",
        "Congress and insider sells are ignored in v1 long-only backtests",
    ]


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


def run_backtest(db: Session, config: BacktestStrategyConfig, *, user_id: int | None = None) -> BacktestRunResponse:
    benchmark_symbol = DEFAULT_BENCHMARK
    assumptions = _base_assumptions()

    if config.strategy_type == "watchlist":
        if user_id is None:
            raise HTTPException(status_code=401, detail="Sign in required.")
        watchlist = load_owned_watchlist(db, watchlist_id=int(config.watchlist_id or 0), user_id=user_id)
        if watchlist is None:
            raise HTTPException(status_code=404, detail="Watchlist not found.")
        symbols = load_watchlist_symbols(db, watchlist_id=watchlist.id)
        price_histories = load_price_histories(db, symbols + [benchmark_symbol], config.start_date, config.end_date)
        positions = build_static_positions(
            symbols=symbols,
            price_histories=price_histories,
            start_date=config.start_date,
            end_date=config.end_date,
            source_label=watchlist.name,
        )
        trade_count = len(positions)
        assumptions.append("Watchlist v1 uses the current watchlist constituents held statically across the selected period.")
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
            price_histories = load_price_histories(db, signal_symbols + [benchmark_symbol], config.start_date, config.end_date)
            positions = build_signal_positions(
                signals=historical_signals,
                price_histories=price_histories,
                end_date=config.end_date,
                hold_days=config.hold_days,
            )
            trade_count = len(historical_signals)
            assumptions.append("Saved screen v1 uses saved-screen entry events as historical signals when available.")
        else:
            symbols, source_mode = load_saved_screen_current_symbols(db, screen=screen)
            price_histories = load_price_histories(db, symbols + [benchmark_symbol], config.start_date, config.end_date)
            positions = build_static_positions(
                symbols=symbols,
                price_histories=price_histories,
                start_date=config.start_date,
                end_date=config.end_date,
                source_label=screen.name,
            )
            trade_count = len(positions)
            if source_mode == "snapshot":
                assumptions.append(
                    "Saved screen v1 falls back to the current saved-screen snapshot universe when historical entry events are unavailable."
                )
            else:
                assumptions.append(
                    "Saved screen v1 uses available saved-screen monitoring history where present; otherwise it backtests the current matching universe over the selected historical period."
                )
    else:
        if config.strategy_type == "congress":
            signals = load_congress_signals(db, config)
        else:
            signals = load_insider_signals(db, config)
        signal_symbols = sorted({signal.symbol for signal in signals})
        price_histories = load_price_histories(db, signal_symbols + [benchmark_symbol], config.start_date, config.end_date)
        positions = build_signal_positions(
            signals=signals,
            price_histories=price_histories,
            end_date=config.end_date,
            hold_days=config.hold_days,
        )
        trade_count = len(signals)

    benchmark_history = price_histories.get(benchmark_symbol, {})
    position_price_histories = {symbol: price_map for symbol, price_map in price_histories.items() if symbol != benchmark_symbol}
    timeline = build_equity_timeline(
        positions=positions,
        price_histories=position_price_histories,
        benchmark_history=benchmark_history,
        start_date=config.start_date,
        end_date=config.end_date,
    )

    assumptions.append("Open positions are marked to market at the selected end date when the holding window extends beyond the range.")

    if not timeline:
        return BacktestRunResponse(
            summary=BacktestSummary(
                strategy_return_pct=0.0,
                benchmark_return_pct=0.0,
                alpha_pct=0.0,
                win_rate=_rounded(compute_win_rate_pct([position.return_pct for position in positions])),
                max_drawdown_pct=0.0,
                volatility_pct=0.0,
                trade_count=trade_count,
                positions_count=len(positions),
            ),
            timeline=[],
            positions=_position_points(positions),
            assumptions=assumptions,
        )

    strategy_values = [point.strategy_value for point in timeline]
    benchmark_values = [point.benchmark_value for point in timeline]
    strategy_return_pct = _rounded(pct_return(strategy_values[0], strategy_values[-1]))
    benchmark_return_pct = _rounded(pct_return(benchmark_values[0], benchmark_values[-1]))

    return BacktestRunResponse(
        summary=BacktestSummary(
            strategy_return_pct=strategy_return_pct,
            benchmark_return_pct=benchmark_return_pct,
            alpha_pct=_rounded(strategy_return_pct - benchmark_return_pct),
            win_rate=_rounded(compute_win_rate_pct([position.return_pct for position in positions])),
            max_drawdown_pct=_rounded(compute_max_drawdown_pct(strategy_values)),
            volatility_pct=_rounded(compute_volatility_pct(strategy_values)),
            trade_count=trade_count,
            positions_count=len(positions),
        ),
        timeline=timeline,
        positions=_position_points(positions),
        assumptions=assumptions,
    )
