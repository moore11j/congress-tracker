from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable, Literal

from sqlalchemy.orm import Session

from app.db import SessionLocal
from app.strategy_research.congress_buys import (
    DEFAULT_UNIVERSE,
    METHODOLOGY_VERSION as BASE_ENGINE_VERSION,
    PriceBar,
    ResearchConfig,
    Signal,
    build_lots,
    compute_metrics,
    load_adjusted_price_histories,
    load_congress_purchase_signals,
    parse_iso_date,
    simulate_active_lot_portfolio,
    _normalize_universe,
)
from app.strategy_research.insider_buys import (
    InsiderRole,
    load_insider_open_market_purchase_signals,
    load_normalized_purchase_universe,
)
from app.strategy_research.institutional_activity_signals import load_institutional_activity_signals
from app.strategy_research.fundamental_confirmation_sweep import load_fundamentals_snapshot_universe
from app.utils.symbols import normalize_symbol

MethodologyVersion = Literal["technical_confirmation_research_v1"]
PrimarySource = Literal["congress", "insider", "institutional"]
TechnicalRule = Literal[
    "sma50_above_sma200",
    "price_above_sma50_sma200",
    "golden_cross_30d",
    "macd_bullish",
    "technical_alignment",
]

METHODOLOGY_VERSION: MethodologyVersion = "technical_confirmation_research_v1"


@dataclass(frozen=True)
class TechnicalState:
    status: str
    close: float | None = None
    sma50: float | None = None
    sma200: float | None = None
    previous_sma50: float | None = None
    previous_sma200: float | None = None
    macd: float | None = None
    macd_signal: float | None = None
    golden_cross_days_ago: int | None = None


def _sma(values: list[float], period: int) -> float | None:
    if len(values) < period:
        return None
    return sum(values[-period:]) / period


def _ema(values: list[float], period: int) -> list[float]:
    if not values:
        return []
    multiplier = 2 / (period + 1)
    ema_values = [values[0]]
    for value in values[1:]:
        ema_values.append((value - ema_values[-1]) * multiplier + ema_values[-1])
    return ema_values


def _macd(values: list[float]) -> tuple[float | None, float | None]:
    if len(values) < 35:
        return None, None
    ema12 = _ema(values, 12)
    ema26 = _ema(values, 26)
    macd_line = [short - long for short, long in zip(ema12, ema26)]
    signal_line = _ema(macd_line, 9)
    if not macd_line or not signal_line:
        return None, None
    return macd_line[-1], signal_line[-1]


def _values_on_or_before(prices: dict[date, PriceBar], as_of: date) -> tuple[list[date], list[float]]:
    days = [day for day in sorted(prices) if day <= as_of]
    return days, [prices[day].close for day in days]


def technical_state_as_of(prices: dict[date, PriceBar], as_of: date) -> TechnicalState:
    days, values = _values_on_or_before(prices, as_of)
    if len(values) < 200:
        return TechnicalState(status="insufficient_price_history")
    sma50 = _sma(values, 50)
    sma200 = _sma(values, 200)
    previous_sma50 = _sma(values[:-1], 50)
    previous_sma200 = _sma(values[:-1], 200)
    macd_value, macd_signal = _macd(values)
    golden_cross_days_ago = _golden_cross_days_ago(prices, days[-1], max_days=30)
    return TechnicalState(
        status="ok",
        close=values[-1],
        sma50=sma50,
        sma200=sma200,
        previous_sma50=previous_sma50,
        previous_sma200=previous_sma200,
        macd=macd_value,
        macd_signal=macd_signal,
        golden_cross_days_ago=golden_cross_days_ago,
    )


def _golden_cross_days_ago(prices: dict[date, PriceBar], as_of: date, *, max_days: int) -> int | None:
    days, values = _values_on_or_before(prices, as_of)
    if len(values) < 201:
        return None
    start_index = max(200, len(values) - max_days - 1)
    previous_relation: bool | None = None
    crosses: list[int] = []
    for index in range(start_index, len(values)):
        sma50 = _sma(values[: index + 1], 50)
        sma200 = _sma(values[: index + 1], 200)
        if sma50 is None or sma200 is None:
            continue
        relation = sma50 > sma200
        if previous_relation is False and relation is True:
            crosses.append(len(values) - 1 - index)
        previous_relation = relation
    return min(crosses) if crosses else None


def technical_rule_matches(state: TechnicalState, rule: TechnicalRule) -> bool:
    if state.status != "ok":
        return False
    if rule == "sma50_above_sma200":
        return state.sma50 is not None and state.sma200 is not None and state.sma50 > state.sma200
    if rule == "price_above_sma50_sma200":
        return (
            state.close is not None
            and state.sma50 is not None
            and state.sma200 is not None
            and state.close > state.sma50 > state.sma200
        )
    if rule == "golden_cross_30d":
        return state.golden_cross_days_ago is not None and state.golden_cross_days_ago <= 30
    if rule == "macd_bullish":
        return state.macd is not None and state.macd_signal is not None and state.macd > state.macd_signal
    if rule == "technical_alignment":
        return (
            state.close is not None
            and state.sma50 is not None
            and state.sma200 is not None
            and state.macd is not None
            and state.macd_signal is not None
            and state.close > state.sma50 > state.sma200
            and state.macd > state.macd_signal
        )
    return False


def _load_primary_signals(
    db: Session,
    source: PrimarySource,
    *,
    universe: Iterable[str],
    start_date: date,
    end_date: date,
    insider_role: InsiderRole,
) -> list[Signal]:
    if source == "congress":
        return load_congress_purchase_signals(db, universe=universe, start_date=start_date, end_date=end_date)
    if source == "insider":
        return load_insider_open_market_purchase_signals(
            db,
            universe=universe,
            start_date=start_date,
            end_date=end_date,
            role=insider_role,
        )
    if source == "institutional":
        return load_institutional_activity_signals(
            db,
            universe=universe,
            start_date=start_date,
            end_date=end_date,
            min_materiality=80.0,
        )
    raise ValueError(f"Unsupported primary source: {source}")


def filter_signals_by_technical_rule(
    signals: list[Signal],
    price_maps: dict[str, dict[date, PriceBar]],
    *,
    rule: TechnicalRule,
) -> tuple[list[Signal], dict[str, int]]:
    filtered: list[Signal] = []
    skipped: dict[str, int] = {"insufficient_price_history": 0, "rule_not_matched": 0, "missing_symbol_prices": 0}
    for signal in signals:
        prices = price_maps.get(signal.symbol)
        if not prices:
            skipped["missing_symbol_prices"] += 1
            continue
        state = technical_state_as_of(prices, signal.disclosure_date)
        if state.status != "ok":
            skipped[state.status] = skipped.get(state.status, 0) + 1
            continue
        if not technical_rule_matches(state, rule):
            skipped["rule_not_matched"] += 1
            continue
        filtered.append(signal)
    return filtered, {key: value for key, value in sorted(skipped.items()) if value}


def run_research(
    db: Session,
    config: ResearchConfig,
    *,
    source: PrimarySource,
    rule: TechnicalRule,
    insider_role: InsiderRole,
) -> dict[str, Any]:
    price_start = (config.start_date - timedelta(days=420)) if config.start_date else date(1990, 1, 1)
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
    primary_signals = _load_primary_signals(
        db,
        source,
        universe=config.universe,
        start_date=signal_start,
        end_date=config.end_date,
        insider_role=insider_role,
    )
    universe_price_maps = {symbol: prices for symbol, prices in price_maps.items() if symbol != config.benchmark}
    signals, technical_skips = filter_signals_by_technical_rule(primary_signals, universe_price_maps, rule=rule)
    per_side_cost_rate = max((config.slippage_bps + config.fee_bps) / 10000.0, 0.0)
    runs: list[dict[str, Any]] = []
    for hold_days in config.hold_days:
        lots, skipped = build_lots(
            signals,
            universe_price_maps,
            benchmark_dates=benchmark_dates,
            hold_days=hold_days,
            rebalance_frequency=config.rebalance_frequency,
            per_side_cost_rate=per_side_cost_rate,
        )
        simulation = simulate_active_lot_portfolio(
            lots,
            universe_price_maps,
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
                f"Buy eligible {source} purchase signals only when {rule} is true using adjusted-price history "
                "available on or before the public signal date; enter on the next trading day."
            ),
            "methodology_version": METHODOLOGY_VERSION,
            "base_engine_version": BASE_ENGINE_VERSION,
            "run_timestamp": datetime.now(timezone.utc).isoformat(),
            "source": source,
            "insider_role": insider_role if source == "insider" else None,
            "technical_rule": rule,
            "universe": list(config.universe),
            "universe_size": len(config.universe),
            "benchmark": config.benchmark,
            "start_date": signal_start.isoformat(),
            "end_date": config.end_date.isoformat(),
            "weighting": config.weighting,
            "rebalance_frequency": config.rebalance_frequency,
            "execution_timing": "first trading day strictly after public disclosure date",
            "technical_as_of": "computed only from adjusted prices on or before the signal disclosure date",
            "fees_bps_per_side": config.fee_bps,
            "slippage_bps_per_side": config.slippage_bps,
            "require_adjusted_prices": config.require_adjusted,
            "price_source": "price_cache.adjusted_close",
            "data_quality_confidence": "medium",
            "data_state": "production PostgreSQL read-only research query",
        },
        "primary_signal_count": len(primary_signals),
        "signal_count": len(signals),
        "filtered_out": technical_skips,
        "aligned_symbol_count": len({signal.symbol for signal in signals}),
        "runs": runs,
    }


def _parse_symbols(value: str | None) -> tuple[str, ...]:
    if not value:
        return DEFAULT_UNIVERSE
    return _normalize_universe(part.strip() for part in value.split(","))


def _parse_exclude_symbols(value: str | None) -> set[str]:
    if not value:
        return set()
    return set(_normalize_universe(part.strip() for part in value.split(",")))


def _parse_holds(value: str) -> tuple[int, ...]:
    return tuple(int(part.strip()) for part in value.split(",") if part.strip())


def _label(source: PrimarySource, rule: TechnicalRule, insider_role: InsiderRole) -> str:
    source_label = "Congress" if source == "congress" else f"Insider {insider_role.replace('_', ' ').title()}"
    return f"{source_label} + {rule.replace('_', ' ').title()}"


def _print_text_report(result: dict[str, Any]) -> None:
    meta = result["metadata"]
    print(
        "RUN "
        f"strategy={meta['strategy_name']} methodology={meta['methodology_version']} "
        f"source={meta['source']} rule={meta['technical_rule']} "
        f"signals={result['signal_count']} primary_signals={result['primary_signal_count']} "
        f"aligned_symbols={result['aligned_symbol_count']} filtered_out={json.dumps(result['filtered_out'], sort_keys=True)} "
        f"universe={len(meta['universe'])} weighting={meta['weighting']} rebalance={meta['rebalance_frequency']} "
        f"cost_bps_per_side={meta['fees_bps_per_side'] + meta['slippage_bps_per_side']} "
        f"confidence={meta['data_quality_confidence']}"
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
            .format(**row)
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Read-only technical confirmation strategy research runner.")
    parser.add_argument("--source", choices=("congress", "insider"), required=True)
    parser.add_argument(
        "--rule",
        choices=("sma50_above_sma200", "price_above_sma50_sma200", "golden_cross_30d", "macd_bullish", "technical_alignment"),
        required=True,
    )
    parser.add_argument("--insider-role", choices=("all", "ceo", "cfo", "director", "officer", "ten_percent_owner"), default="all")
    parser.add_argument("--symbols", help="Comma-separated universe. Defaults to the approved 24-symbol research universe.")
    parser.add_argument(
        "--universe-source",
        choices=("explicit", "normalized_insider_purchases", "fundamentals_snapshots"),
        default="explicit",
    )
    parser.add_argument("--exclude-symbols", help="Comma-separated symbols to exclude from the selected universe.")
    parser.add_argument("--snapshot-source-kind", default="ticker_financials_cache_statement_proxy")
    parser.add_argument("--min-snapshots-per-symbol", type=int, default=1)
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

    end_date = parse_iso_date(args.end_date) or date.today()
    start_date = parse_iso_date(args.start_date) if args.start_date else None
    exclude_symbols = _parse_exclude_symbols(args.exclude_symbols)
    universe = _parse_symbols(args.symbols)
    with SessionLocal() as db:
        if args.universe_source == "normalized_insider_purchases":
            universe = load_normalized_purchase_universe(
                db,
                start_date=start_date or date(1990, 1, 1),
                end_date=end_date,
                exclude_symbols=exclude_symbols,
            )
        elif args.universe_source == "fundamentals_snapshots":
            universe = load_fundamentals_snapshot_universe(
                db,
                end_date=end_date,
                source_kind=args.snapshot_source_kind if args.snapshot_source_kind else None,
                min_snapshots=int(args.min_snapshots_per_symbol),
                exclude_symbols=exclude_symbols,
            )
        result = run_research(
            db,
            ResearchConfig(
                strategy_name=_label(args.source, args.rule, args.insider_role),
                universe=universe,
                benchmark=normalize_symbol(args.benchmark) or "SPY",
                start_date=start_date,
                end_date=end_date,
                hold_days=_parse_holds(args.hold_days),
                weighting=args.weighting,
                rebalance_frequency=args.rebalance_frequency,
                slippage_bps=float(args.slippage_bps),
                fee_bps=float(args.fee_bps),
                require_adjusted=not args.allow_raw_prices,
                min_lots=int(args.min_lots),
            ),
            source=args.source,
            rule=args.rule,
            insider_role=args.insider_role,
        )
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        _print_text_report(result)


if __name__ == "__main__":
    main()
