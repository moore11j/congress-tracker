from __future__ import annotations

import argparse
import json
import math
from bisect import bisect_left, bisect_right
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from statistics import stdev
from typing import Any, Iterable, Literal

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db import SessionLocal
from app.models import Event, PriceCache
from app.services.backtesting.queries import first_text, parse_iso_date, parse_payload
from app.services.trade_outcome_display import normalize_trade_side
from app.utils.symbols import normalize_symbol, symbol_variants

MethodologyVersion = Literal["congress_buys_research_v1"]
WeightingMode = Literal["equal", "transaction_value"]
RebalanceFrequency = Literal["event", "weekly", "monthly"]

METHODOLOGY_VERSION: MethodologyVersion = "congress_buys_research_v1"
DEFAULT_UNIVERSE = (
    "MSFT",
    "NVDA",
    "AAPL",
    "GOOGL",
    "META",
    "NFLX",
    "CRM",
    "CRWD",
    "PLTR",
    "DELL",
    "ANET",
    "NET",
    "AMZN",
    "JPM",
    "JNJ",
    "WFC",
    "UNH",
    "AVGO",
    "TSLA",
    "LLY",
    "MRK",
    "HD",
    "V",
    "T",
)


@dataclass(frozen=True)
class ResearchConfig:
    strategy_name: str
    universe: tuple[str, ...]
    benchmark: str
    start_date: date | None
    end_date: date
    hold_days: tuple[int, ...]
    weighting: WeightingMode
    rebalance_frequency: RebalanceFrequency
    slippage_bps: float
    fee_bps: float
    require_adjusted: bool
    min_lots: int


@dataclass(frozen=True)
class PriceBar:
    day: date
    close: float
    dollar_volume: float | None


@dataclass(frozen=True)
class Signal:
    event_id: int
    symbol: str
    disclosure_date: date
    raw_entry_date: date
    amount_min: int | None
    amount_max: int | None
    member_name: str | None
    member_bioguide_id: str | None
    chamber: str | None
    party: str | None
    source_filing_id: str | None
    source_document_url: str | None

    @property
    def transaction_value_weight(self) -> float:
        if self.amount_max and self.amount_max > 0:
            return float(self.amount_max)
        if self.amount_min and self.amount_min > 0:
            return float(self.amount_min)
        return 1.0


@dataclass(frozen=True)
class Lot:
    signal: Signal
    entry_date: date
    exit_date: date
    entry_price: float
    exit_price: float
    gross_return: float
    net_return: float


def _annualized_years(start: date, end: date) -> float:
    return max((end - start).days / 365.25, 0.0)


def _cagr_pct(total_return_pct: float, years: float) -> float:
    if years <= 0:
        return 0.0
    growth = 1.0 + total_return_pct / 100.0
    if growth <= 0:
        return -100.0
    return (growth ** (1.0 / years) - 1.0) * 100.0


def _stdev(values: list[float]) -> float:
    return stdev(values) if len(values) >= 2 else 0.0


def _sharpe(daily_returns: list[float]) -> float | None:
    sigma = _stdev(daily_returns)
    if sigma <= 0:
        return None
    return (sum(daily_returns) / len(daily_returns)) / sigma * math.sqrt(252.0)


def _sortino(daily_returns: list[float]) -> float | None:
    downside = [value for value in daily_returns if value < 0]
    sigma = _stdev(downside)
    if sigma <= 0:
        return None
    return (sum(daily_returns) / len(daily_returns)) / sigma * math.sqrt(252.0)


def _max_drawdown_pct(curve: list[float]) -> float:
    peak: float | None = None
    worst = 0.0
    for value in curve:
        if value <= 0:
            continue
        peak = value if peak is None else max(peak, value)
        if peak and peak > 0:
            worst = min(worst, (value / peak - 1.0) * 100.0)
    return worst


def _beta(strategy_returns: list[float], benchmark_returns: list[float]) -> float | None:
    pairs = [(s, b) for s, b in zip(strategy_returns, benchmark_returns) if math.isfinite(s) and math.isfinite(b)]
    if len(pairs) < 2:
        return None
    s_values = [item[0] for item in pairs]
    b_values = [item[1] for item in pairs]
    mean_s = sum(s_values) / len(s_values)
    mean_b = sum(b_values) / len(b_values)
    variance_b = sum((value - mean_b) ** 2 for value in b_values)
    if variance_b <= 0:
        return None
    covariance = sum((s_value - mean_s) * (b_value - mean_b) for s_value, b_value in pairs)
    return covariance / variance_b


def _float_pct(value: float | None) -> float | None:
    return None if value is None else round(float(value), 4)


def _parse_day(value: str) -> date:
    return date.fromisoformat(value[:10])


def _first_trading_day_after(target: date, dates: list[date]) -> date | None:
    index = bisect_right(dates, target)
    if index >= len(dates):
        return None
    return dates[index]


def _first_trading_day_on_or_after(target: date, dates: list[date]) -> date | None:
    index = bisect_left(dates, target)
    if index >= len(dates):
        return None
    return dates[index]


def _price_on_or_before(target: date, prices: dict[date, PriceBar], dates: list[date]) -> float | None:
    index = bisect_right(dates, target) - 1
    if index < 0:
        return None
    return prices[dates[index]].close


def _calendar_month_key(day: date) -> tuple[int, int]:
    return day.year, day.month


def _is_rebalance_day(day: date, previous_day: date | None, frequency: RebalanceFrequency) -> bool:
    if frequency == "event":
        return True
    if previous_day is None:
        return True
    if frequency == "weekly":
        return day.isocalendar()[:2] != previous_day.isocalendar()[:2]
    if frequency == "monthly":
        return _calendar_month_key(day) != _calendar_month_key(previous_day)
    return True


def _next_rebalance_on_or_after(raw_entry: date, trading_dates: list[date], frequency: RebalanceFrequency) -> date | None:
    if frequency == "event":
        return raw_entry
    index = bisect_left(trading_dates, raw_entry)
    previous = trading_dates[index - 1] if index > 0 else None
    for day in trading_dates[index:]:
        if _is_rebalance_day(day, previous, frequency):
            return day
        previous = day
    return None


def _disclosure_date(event: Event, payload: dict[str, Any]) -> date | None:
    payload_date = parse_iso_date(
        first_text(
            payload,
            "filing_date",
            "filingDate",
            "report_date",
            "reportDate",
            "disclosure_date",
            "disclosureDate",
        )
    )
    if payload_date is not None:
        return payload_date
    if event.event_date is not None:
        return event.event_date.date()
    if event.ts is not None:
        return event.ts.date()
    return None


def _signal_key(signal: Signal) -> tuple[object, ...]:
    return (
        signal.symbol,
        signal.member_bioguide_id or signal.member_name,
        signal.disclosure_date,
        signal.amount_min,
        signal.amount_max,
        signal.source_filing_id or signal.source_document_url or signal.event_id,
    )


def _normalize_universe(raw_symbols: Iterable[str]) -> tuple[str, ...]:
    symbols = [normalize_symbol(symbol) for symbol in raw_symbols]
    return tuple(dict.fromkeys(symbol for symbol in symbols if symbol))


def load_adjusted_price_histories(
    db: Session,
    symbols: Iterable[str],
    *,
    start_date: date,
    end_date: date,
    require_adjusted: bool,
) -> dict[str, dict[date, PriceBar]]:
    normalized_symbols = _normalize_universe(symbols)
    lookup_symbols = sorted(
        {
            variant
            for symbol in normalized_symbols
            for variant in (symbol_variants(symbol) or [symbol])
            if variant
        }
    )
    if not lookup_symbols:
        return {}

    rows = db.execute(
        select(
            PriceCache.symbol,
            PriceCache.date,
            PriceCache.adjusted_close,
            PriceCache.close,
            PriceCache.dollar_volume,
        )
        .where(PriceCache.symbol.in_(lookup_symbols))
        .where(PriceCache.date >= start_date.isoformat())
        .where(PriceCache.date <= end_date.isoformat())
        .order_by(PriceCache.symbol.asc(), PriceCache.date.asc())
    ).all()

    variant_maps: dict[str, dict[date, PriceBar]] = defaultdict(dict)
    for raw_symbol, raw_day, adjusted_close, close, dollar_volume in rows:
        price_value = adjusted_close if adjusted_close is not None else (None if require_adjusted else close)
        if price_value is None or float(price_value) <= 0:
            continue
        day = _parse_day(str(raw_day))
        variant_maps[str(raw_symbol).upper()][day] = PriceBar(
            day=day,
            close=float(price_value),
            dollar_volume=float(dollar_volume) if dollar_volume is not None else None,
        )

    price_maps: dict[str, dict[date, PriceBar]] = {}
    for requested_symbol in normalized_symbols:
        candidates = symbol_variants(requested_symbol) or [requested_symbol]
        best_symbol = max(
            candidates,
            key=lambda candidate: (
                len(variant_maps.get(candidate.upper(), {})),
                1 if candidate.upper() == requested_symbol else 0,
            ),
        )
        if variant_maps.get(best_symbol.upper()):
            price_maps[requested_symbol] = dict(variant_maps[best_symbol.upper()])
    return price_maps


def load_congress_purchase_signals(
    db: Session,
    *,
    universe: Iterable[str],
    start_date: date,
    end_date: date,
) -> list[Signal]:
    normalized_universe = set(_normalize_universe(universe))
    query_start = datetime.combine(start_date - timedelta(days=14), time.min, tzinfo=timezone.utc)
    query_end = datetime.combine(end_date + timedelta(days=1), time.min, tzinfo=timezone.utc)
    rows = (
        db.execute(
            select(Event)
            .where(Event.event_type == "congress_trade")
            .where(Event.ts >= query_start)
            .where(Event.ts < query_end)
            .order_by(Event.ts.asc(), Event.id.asc())
        )
        .scalars()
        .all()
    )
    signals: list[Signal] = []
    seen: set[tuple[object, ...]] = set()
    for row in rows:
        payload = parse_payload(row.payload_json)
        side = normalize_trade_side(
            row.trade_type
            or row.transaction_type
            or first_text(payload, "trade_type", "tradeType", "transaction_type", "transactionType")
        )
        if side != "purchase":
            continue
        symbol = normalize_symbol(row.symbol or first_text(payload, "symbol", "ticker"))
        disclosure_date = _disclosure_date(row, payload)
        if not symbol or symbol not in normalized_universe or disclosure_date is None:
            continue
        if disclosure_date < start_date or disclosure_date > end_date:
            continue
        raw_entry = disclosure_date + timedelta(days=1)
        signal = Signal(
            event_id=int(row.id),
            symbol=symbol,
            disclosure_date=disclosure_date,
            raw_entry_date=raw_entry,
            amount_min=int(row.amount_min) if row.amount_min is not None else None,
            amount_max=int(row.amount_max) if row.amount_max is not None else None,
            member_name=row.member_name,
            member_bioguide_id=row.member_bioguide_id,
            chamber=row.chamber,
            party=row.party,
            source_filing_id=row.source_filing_id,
            source_document_url=row.source_document_url,
        )
        key = _signal_key(signal)
        if key in seen:
            continue
        seen.add(key)
        signals.append(signal)
    return signals


def build_lots(
    signals: list[Signal],
    price_maps: dict[str, dict[date, PriceBar]],
    *,
    benchmark_dates: list[date],
    hold_days: int,
    rebalance_frequency: RebalanceFrequency,
    per_side_cost_rate: float,
) -> tuple[list[Lot], dict[str, int]]:
    lots: list[Lot] = []
    skipped: dict[str, int] = defaultdict(int)
    price_dates_by_symbol = {symbol: sorted(prices) for symbol, prices in price_maps.items()}
    for signal in signals:
        symbol_prices = price_maps.get(signal.symbol)
        symbol_dates = price_dates_by_symbol.get(signal.symbol) or []
        if not symbol_prices or not symbol_dates:
            skipped["missing_symbol_prices"] += 1
            continue
        first_entry = _first_trading_day_on_or_after(signal.raw_entry_date, symbol_dates)
        if first_entry is None:
            skipped["missing_entry_price"] += 1
            continue
        entry_date = _next_rebalance_on_or_after(first_entry, benchmark_dates, rebalance_frequency)
        if entry_date is None:
            skipped["missing_rebalance_entry"] += 1
            continue
        entry_date = _first_trading_day_on_or_after(entry_date, symbol_dates) or entry_date
        exit_target = entry_date + timedelta(days=hold_days)
        exit_date = _first_trading_day_on_or_after(exit_target, symbol_dates)
        if exit_date is None:
            skipped["missing_exit_price"] += 1
            continue
        entry_bar = symbol_prices.get(entry_date)
        exit_bar = symbol_prices.get(exit_date)
        if entry_bar is None or exit_bar is None:
            skipped["missing_exact_entry_or_exit"] += 1
            continue
        gross_return = (exit_bar.close / entry_bar.close) - 1.0
        net_return = ((1.0 + gross_return) * (1.0 - per_side_cost_rate) * (1.0 - per_side_cost_rate)) - 1.0
        lots.append(
            Lot(
                signal=signal,
                entry_date=entry_date,
                exit_date=exit_date,
                entry_price=entry_bar.close,
                exit_price=exit_bar.close,
                gross_return=gross_return,
                net_return=net_return,
            )
        )
    return lots, dict(sorted(skipped.items()))


def _lot_weight(lot: Lot, weighting: WeightingMode) -> float:
    if weighting == "transaction_value":
        return max(lot.signal.transaction_value_weight, 1.0)
    return 1.0


def simulate_active_lot_portfolio(
    lots: list[Lot],
    price_maps: dict[str, dict[date, PriceBar]],
    benchmark_prices: dict[date, PriceBar],
    *,
    weighting: WeightingMode,
    per_side_cost_rate: float,
) -> dict[str, Any]:
    if not lots or not benchmark_prices:
        return {"timeline": [], "daily_returns": [], "benchmark_daily_returns": []}
    start = min(lot.entry_date for lot in lots)
    end = max(lot.exit_date for lot in lots)
    calendar = [day for day in sorted(benchmark_prices) if start <= day <= end]
    if len(calendar) < 2:
        return {"timeline": [], "daily_returns": [], "benchmark_daily_returns": []}

    dates_by_symbol = {symbol: sorted(prices) for symbol, prices in price_maps.items()}
    active_by_previous_day: dict[date, list[Lot]] = {}
    entry_costs_by_day: dict[date, float] = defaultdict(float)
    exit_costs_by_day: dict[date, float] = defaultdict(float)

    for day in calendar[:-1]:
        active_by_previous_day[day] = [lot for lot in lots if lot.entry_date <= day < lot.exit_date]

    strategy_curve = [100.0]
    benchmark_curve = [100.0]
    daily_returns: list[float] = []
    benchmark_daily_returns: list[float] = []
    holdings_counts: list[int] = []
    largest_position_weights: list[float] = []
    turnover_events = 0

    for lot in lots:
        entry_costs_by_day[lot.entry_date] += _lot_weight(lot, weighting)
        exit_costs_by_day[lot.exit_date] += _lot_weight(lot, weighting)
        turnover_events += 2

    for previous_day, current_day in zip(calendar, calendar[1:]):
        active_lots = active_by_previous_day.get(previous_day, [])
        holdings_counts.append(len(active_lots))
        weighted_returns: list[tuple[float, float]] = []
        for lot in active_lots:
            symbol_prices = price_maps.get(lot.signal.symbol) or {}
            symbol_dates = dates_by_symbol.get(lot.signal.symbol) or []
            previous_price = _price_on_or_before(previous_day, symbol_prices, symbol_dates)
            current_price = _price_on_or_before(current_day, symbol_prices, symbol_dates)
            if previous_price is None or current_price is None or previous_price <= 0:
                continue
            weighted_returns.append(((current_price / previous_price) - 1.0, _lot_weight(lot, weighting)))

        total_weight = sum(weight for _, weight in weighted_returns)
        gross_daily = (
            sum(daily_return * weight for daily_return, weight in weighted_returns) / total_weight
            if total_weight > 0
            else 0.0
        )
        if total_weight > 0:
            cost_weight = (entry_costs_by_day.get(current_day, 0.0) + exit_costs_by_day.get(current_day, 0.0)) / total_weight
            net_daily = gross_daily - (per_side_cost_rate * cost_weight)
            largest_position_weights.append(max(weight for _, weight in weighted_returns) / total_weight)
        else:
            net_daily = 0.0
            largest_position_weights.append(0.0)

        previous_benchmark = benchmark_prices[previous_day].close
        current_benchmark = benchmark_prices[current_day].close
        benchmark_daily = (current_benchmark / previous_benchmark) - 1.0 if previous_benchmark > 0 else 0.0

        daily_returns.append(net_daily)
        benchmark_daily_returns.append(benchmark_daily)
        strategy_curve.append(strategy_curve[-1] * (1.0 + net_daily))
        benchmark_curve.append(benchmark_curve[-1] * (1.0 + benchmark_daily))

    timeline = [
        {
            "date": day.isoformat(),
            "strategy_value": round(strategy_curve[index], 6),
            "benchmark_value": round(benchmark_curve[index], 6),
            "active_lots": holdings_counts[index - 1] if index > 0 and index - 1 < len(holdings_counts) else 0,
        }
        for index, day in enumerate(calendar)
    ]
    return {
        "timeline": timeline,
        "daily_returns": daily_returns,
        "benchmark_daily_returns": benchmark_daily_returns,
        "strategy_curve": strategy_curve,
        "benchmark_curve": benchmark_curve,
        "avg_active_lots": sum(holdings_counts) / len(holdings_counts) if holdings_counts else 0.0,
        "largest_position_concentration_pct": max(largest_position_weights) * 100.0 if largest_position_weights else 0.0,
        "turnover_events": turnover_events,
        "start": calendar[0],
        "end": calendar[-1],
    }


def _trailing_return_pct(curve: list[float], dates: list[str], days: int) -> float | None:
    if len(curve) < 2 or not dates:
        return None
    end_day = _parse_day(dates[-1])
    target = end_day - timedelta(days=days)
    date_values = [_parse_day(day) for day in dates]
    index = bisect_left(date_values, target)
    if index >= len(curve) or curve[index] <= 0:
        return None
    return (curve[-1] / curve[index] - 1.0) * 100.0


def _annual_returns(timeline: list[dict[str, Any]], value_key: str) -> dict[str, float]:
    by_year: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for point in timeline:
        by_year[_parse_day(str(point["date"])).year].append(point)
    returns: dict[str, float] = {}
    for year, points in sorted(by_year.items()):
        if len(points) < 2:
            continue
        start = float(points[0][value_key])
        end = float(points[-1][value_key])
        if start > 0:
            returns[str(year)] = round((end / start - 1.0) * 100.0, 4)
    return returns


def _rolling_12m_beating_spy_pct(timeline: list[dict[str, Any]]) -> float | None:
    if len(timeline) < 252:
        return None
    dates = [_parse_day(str(point["date"])) for point in timeline]
    wins = 0
    periods = 0
    for end_index, end_day in enumerate(dates):
        start_target = end_day - timedelta(days=365)
        start_index = bisect_left(dates, start_target)
        if start_index >= end_index:
            continue
        strategy_start = float(timeline[start_index]["strategy_value"])
        benchmark_start = float(timeline[start_index]["benchmark_value"])
        if strategy_start <= 0 or benchmark_start <= 0:
            continue
        strategy_return = float(timeline[end_index]["strategy_value"]) / strategy_start - 1.0
        benchmark_return = float(timeline[end_index]["benchmark_value"]) / benchmark_start - 1.0
        wins += 1 if strategy_return > benchmark_return else 0
        periods += 1
    return (wins / periods * 100.0) if periods else None


def compute_metrics(
    *,
    lots: list[Lot],
    simulation: dict[str, Any],
    hold_days: int,
    skipped: dict[str, int],
) -> dict[str, Any]:
    timeline = simulation.get("timeline") or []
    daily_returns: list[float] = list(simulation.get("daily_returns") or [])
    benchmark_daily_returns: list[float] = list(simulation.get("benchmark_daily_returns") or [])
    strategy_curve: list[float] = list(simulation.get("strategy_curve") or [])
    benchmark_curve: list[float] = list(simulation.get("benchmark_curve") or [])
    dates = [str(point["date"]) for point in timeline]
    if not timeline or not strategy_curve or not benchmark_curve:
        return {"hold_days": hold_days, "lots": len(lots), "skipped": skipped, "status": "insufficient_timeline"}

    start = _parse_day(dates[0])
    end = _parse_day(dates[-1])
    years = _annualized_years(start, end)
    total_return_pct = (strategy_curve[-1] / strategy_curve[0] - 1.0) * 100.0
    benchmark_total_return_pct = (benchmark_curve[-1] / benchmark_curve[0] - 1.0) * 100.0
    cagr_pct = _cagr_pct(total_return_pct, years)
    benchmark_cagr_pct = _cagr_pct(benchmark_total_return_pct, years)
    gross_lot_returns = [lot.gross_return for lot in lots]
    net_lot_returns = [lot.net_return for lot in lots]
    wins = [value for value in net_lot_returns if value > 0]
    losses = [value for value in net_lot_returns if value < 0]
    annualized_volatility_pct = _stdev(daily_returns) * math.sqrt(252.0) * 100.0
    max_drawdown_pct = _max_drawdown_pct(strategy_curve)
    beta = _beta(daily_returns, benchmark_daily_returns)

    return {
        "hold_days": hold_days,
        "status": "ok",
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "lots": len(lots),
        "independent_signals": len({_signal_key(lot.signal) for lot in lots}),
        "trade_count": len(lots) * 2,
        "total_return_pct": _float_pct(total_return_pct),
        "cagr_pct": _float_pct(cagr_pct),
        "trailing_30d_return_pct": _float_pct(_trailing_return_pct(strategy_curve, dates, 30)),
        "trailing_1y_return_pct": _float_pct(_trailing_return_pct(strategy_curve, dates, 365)),
        "trailing_2y_return_pct": _float_pct(_trailing_return_pct(strategy_curve, dates, 365 * 2)),
        "trailing_3y_return_pct": _float_pct(_trailing_return_pct(strategy_curve, dates, 365 * 3)),
        "benchmark_total_return_pct": _float_pct(benchmark_total_return_pct),
        "benchmark_cagr_pct": _float_pct(benchmark_cagr_pct),
        "alpha_cagr_pct": _float_pct(cagr_pct - benchmark_cagr_pct),
        "beta": _float_pct(beta),
        "sharpe": _float_pct(_sharpe(daily_returns)),
        "sortino": _float_pct(_sortino(daily_returns)),
        "max_drawdown_pct": _float_pct(max_drawdown_pct),
        "annualized_volatility_pct": _float_pct(annualized_volatility_pct),
        "calmar_ratio": _float_pct(cagr_pct / abs(max_drawdown_pct)) if max_drawdown_pct < 0 else None,
        "win_rate_pct": _float_pct((len(wins) / len(net_lot_returns) * 100.0) if net_lot_returns else 0.0),
        "average_win_pct": _float_pct((sum(wins) / len(wins) * 100.0) if wins else None),
        "average_loss_pct": _float_pct((sum(losses) / len(losses) * 100.0) if losses else None),
        "profit_factor": _float_pct((sum(wins) / abs(sum(losses))) if losses else None),
        "best_lot_return_pct": _float_pct(max(gross_lot_returns) * 100.0 if gross_lot_returns else None),
        "worst_lot_return_pct": _float_pct(min(gross_lot_returns) * 100.0 if gross_lot_returns else None),
        "avg_active_lots": _float_pct(simulation.get("avg_active_lots")),
        "turnover_events": int(simulation.get("turnover_events") or 0),
        "largest_position_concentration_pct": _float_pct(simulation.get("largest_position_concentration_pct")),
        "rolling_12m_beating_spy_pct": _float_pct(_rolling_12m_beating_spy_pct(timeline)),
        "annual_returns_pct": _annual_returns(timeline, "strategy_value"),
        "benchmark_annual_returns_pct": _annual_returns(timeline, "benchmark_value"),
        "skipped": skipped,
    }


def run_research(db: Session, config: ResearchConfig) -> dict[str, Any]:
    price_start = config.start_date or date(1990, 1, 1)
    price_maps = load_adjusted_price_histories(
        db,
        (*config.universe, config.benchmark),
        start_date=price_start,
        end_date=config.end_date,
        require_adjusted=config.require_adjusted,
    )
    benchmark_prices = price_maps.get(config.benchmark, {})
    benchmark_dates = sorted(benchmark_prices)
    if not benchmark_dates:
        raise RuntimeError(f"Missing benchmark prices for {config.benchmark}.")

    universe_price_starts = [
        min(prices)
        for symbol, prices in price_maps.items()
        if symbol != config.benchmark and prices
    ]
    first_price_day = min(universe_price_starts) if universe_price_starts else (config.start_date or config.end_date)
    signal_start = config.start_date or first_price_day
    signals = load_congress_purchase_signals(
        db,
        universe=config.universe,
        start_date=signal_start,
        end_date=config.end_date,
    )
    per_side_cost_rate = max((config.slippage_bps + config.fee_bps) / 10000.0, 0.0)
    runs: list[dict[str, Any]] = []
    for hold_days in config.hold_days:
        lots, skipped = build_lots(
            signals,
            {symbol: prices for symbol, prices in price_maps.items() if symbol != config.benchmark},
            benchmark_dates=benchmark_dates,
            hold_days=hold_days,
            rebalance_frequency=config.rebalance_frequency,
            per_side_cost_rate=per_side_cost_rate,
        )
        simulation = simulate_active_lot_portfolio(
            lots,
            {symbol: prices for symbol, prices in price_maps.items() if symbol != config.benchmark},
            benchmark_prices,
            weighting=config.weighting,
            per_side_cost_rate=per_side_cost_rate,
        )
        metrics = compute_metrics(lots=lots, simulation=simulation, hold_days=hold_days, skipped=skipped)
        if metrics.get("status") == "ok" and metrics.get("lots", 0) < config.min_lots:
            metrics["status"] = "insufficient_lots"
        runs.append(metrics)

    return {
        "metadata": {
            "strategy_name": config.strategy_name,
            "plain_english_rule": (
                "Buy every eligible Congress purchase in the selected universe on the next trading day after "
                "public filing/disclosure availability; hold for the configured fixed period."
            ),
            "methodology_version": METHODOLOGY_VERSION,
            "run_timestamp": datetime.now(timezone.utc).isoformat(),
            "universe": list(config.universe),
            "benchmark": config.benchmark,
            "start_date": signal_start.isoformat(),
            "end_date": config.end_date.isoformat(),
            "weighting": config.weighting,
            "rebalance_frequency": config.rebalance_frequency,
            "execution_timing": "first trading day strictly after public disclosure date",
            "fees_bps_per_side": config.fee_bps,
            "slippage_bps_per_side": config.slippage_bps,
            "require_adjusted_prices": config.require_adjusted,
            "price_source": "price_cache.adjusted_close",
            "data_state": "production PostgreSQL read-only research query",
        },
        "signal_count": len(signals),
        "price_coverage": {
            symbol: {
                "rows": len(prices),
                "start": min(prices).isoformat() if prices else None,
                "end": max(prices).isoformat() if prices else None,
            }
            for symbol, prices in sorted(price_maps.items())
        },
        "runs": runs,
    }


def _parse_symbols(value: str | None) -> tuple[str, ...]:
    if not value:
        return DEFAULT_UNIVERSE
    return _normalize_universe(part.strip() for part in value.split(","))


def _parse_holds(value: str) -> tuple[int, ...]:
    return tuple(int(part.strip()) for part in value.split(",") if part.strip())


def _print_text_report(result: dict[str, Any]) -> None:
    meta = result["metadata"]
    print(
        "RUN "
        f"strategy={meta['strategy_name']} methodology={meta['methodology_version']} "
        f"signals={result['signal_count']} universe={len(meta['universe'])} "
        f"weighting={meta['weighting']} rebalance={meta['rebalance_frequency']} "
        f"cost_bps_per_side={meta['fees_bps_per_side'] + meta['slippage_bps_per_side']}"
    )
    for row in result["runs"]:
        if row.get("status") != "ok":
            print(
                f"H{row.get('hold_days')} status={row.get('status')} "
                f"lots={row.get('lots')} skipped={row.get('skipped')}"
            )
            continue
        print(
            "H{hold_days} status={status} lots={lots} start={start_date} end={end_date} "
            "total={total_return_pct}% cagr={cagr_pct}% spy_cagr={benchmark_cagr_pct}% "
            "alpha={alpha_cagr_pct}% max_dd={max_drawdown_pct}% sharpe={sharpe} "
            "sortino={sortino} beta={beta} vol={annualized_volatility_pct}% "
            "win={win_rate_pct}% avg_active={avg_active_lots} trades={trade_count} "
            "roll12m_beat_spy={rolling_12m_beating_spy_pct}%"
            .format(**{key: row.get(key) for key in row.keys() | {"start_date", "end_date"}})
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Read-only Congress Buys strategy research runner.")
    parser.add_argument("--symbols", help="Comma-separated universe. Defaults to the approved 24-symbol research universe.")
    parser.add_argument("--benchmark", default="SPY")
    parser.add_argument("--start-date")
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--hold-days", default="30,90,180,365")
    parser.add_argument("--weighting", choices=("equal", "transaction_value"), default="equal")
    parser.add_argument("--rebalance-frequency", choices=("event", "weekly", "monthly"), default="event")
    parser.add_argument("--slippage-bps", type=float, default=5.0)
    parser.add_argument("--fee-bps", type=float, default=0.0)
    parser.add_argument("--allow-raw-prices", action="store_true")
    parser.add_argument("--min-lots", type=int, default=50)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    config = ResearchConfig(
        strategy_name="Congress Buys",
        universe=_parse_symbols(args.symbols),
        benchmark=normalize_symbol(args.benchmark) or "SPY",
        start_date=parse_iso_date(args.start_date) if args.start_date else None,
        end_date=parse_iso_date(args.end_date) or date.today(),
        hold_days=_parse_holds(args.hold_days),
        weighting=args.weighting,
        rebalance_frequency=args.rebalance_frequency,
        slippage_bps=float(args.slippage_bps),
        fee_bps=float(args.fee_bps),
        require_adjusted=not args.allow_raw_prices,
        min_lots=int(args.min_lots),
    )
    with SessionLocal() as db:
        result = run_research(db, config)
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        _print_text_report(result)


if __name__ == "__main__":
    main()
