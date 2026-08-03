from __future__ import annotations

import argparse
import json
from datetime import date, datetime, timedelta, timezone
from typing import Any

from sqlalchemy.orm import Session

from app.db import SessionLocal
from app.strategy_research.congress_buys import (
    DEFAULT_UNIVERSE,
    ResearchConfig,
    build_lots,
    compute_metrics,
    load_adjusted_price_histories,
    parse_iso_date,
    simulate_active_lot_portfolio,
    _normalize_universe,
)
from app.strategy_research.cross_source_alignment import (
    AlignmentPair,
    _pair_sources,
    _source_signals,
    build_alignment_signals,
)
from app.strategy_research.fundamental_confirmation_sweep import load_fundamentals_snapshot_universe
from app.strategy_research.insider_buys import InsiderRole, load_normalized_purchase_universe
from app.strategy_research.strategy_quality_diagnostics import load_current_sector_map, summarize_strategy_quality
from app.strategy_research.technical_confirmation import (
    PrimarySource,
    TechnicalRule,
    _load_primary_signals,
    filter_signals_by_technical_rule,
)
from app.utils.symbols import normalize_symbol

METHODOLOGY_VERSION = "candidate_strategy_diagnostics_v1"


def _parse_symbols(value: str | None) -> tuple[str, ...]:
    if not value:
        return DEFAULT_UNIVERSE
    return _normalize_universe(part.strip() for part in value.split(","))


def _parse_exclude_symbols(value: str | None) -> tuple[str, ...]:
    if not value:
        return ()
    return _normalize_universe(part.strip() for part in value.split(","))


def _load_universe(
    db: Session,
    *,
    universe_source: str,
    symbols: str | None,
    start_date: date | None,
    end_date: date,
    exclude_symbols: tuple[str, ...],
    snapshot_source_kind: str | None,
    min_snapshots_per_symbol: int,
) -> tuple[str, ...]:
    if universe_source == "normalized_insider_purchases":
        return load_normalized_purchase_universe(
            db,
            start_date=start_date or date(1990, 1, 1),
            end_date=end_date,
            exclude_symbols=exclude_symbols,
        )
    if universe_source == "fundamentals_snapshots":
        return load_fundamentals_snapshot_universe(
            db,
            end_date=end_date,
            source_kind=snapshot_source_kind,
            min_snapshots=int(min_snapshots_per_symbol),
            exclude_symbols=exclude_symbols,
        )
    universe = _parse_symbols(symbols)
    excluded = set(exclude_symbols)
    return tuple(symbol for symbol in universe if symbol not in excluded)


def _price_context(
    db: Session,
    config: ResearchConfig,
    *,
    price_start: date,
) -> tuple[dict[str, Any], dict[str, Any], list[date], date]:
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
    signal_start = config.start_date or (min(universe_price_starts) if universe_price_starts else config.end_date)
    return price_maps, benchmark_prices, benchmark_dates, signal_start


def _performance_for_lots(
    *,
    config: ResearchConfig,
    lots: list[Any],
    skipped: dict[str, int],
    price_maps: dict[str, Any],
    benchmark_prices: dict[str, Any],
    hold_days: int,
    per_side_cost_rate: float,
) -> dict[str, Any]:
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
    return metrics


def run_technical_diagnostics(
    db: Session,
    config: ResearchConfig,
    *,
    source: PrimarySource,
    rule: TechnicalRule,
    insider_role: InsiderRole,
    limit: int,
) -> dict[str, Any]:
    price_start = (config.start_date - timedelta(days=420)) if config.start_date else date(1990, 1, 1)
    price_maps, benchmark_prices, benchmark_dates, signal_start = _price_context(db, config, price_start=price_start)
    primary_signals = _load_primary_signals(
        db,
        source,
        universe=config.universe,
        start_date=signal_start,
        end_date=config.end_date,
        insider_role=insider_role,
    )
    universe_price_maps = {symbol: prices for symbol, prices in price_maps.items() if symbol != config.benchmark}
    confirmed_signals, technical_skips = filter_signals_by_technical_rule(primary_signals, universe_price_maps, rule=rule)
    per_side_cost_rate = max((config.slippage_bps + config.fee_bps) / 10000.0, 0.0)
    hold_days = config.hold_days[0]
    lots, skipped = build_lots(
        confirmed_signals,
        universe_price_maps,
        benchmark_dates=benchmark_dates,
        hold_days=hold_days,
        rebalance_frequency=config.rebalance_frequency,
        per_side_cost_rate=per_side_cost_rate,
    )
    sector_by_symbol = load_current_sector_map(db, {signal.symbol for signal in confirmed_signals})
    return {
        "metadata": {
            "strategy_kind": "technical",
            "strategy_name": config.strategy_name,
            "methodology_version": METHODOLOGY_VERSION,
            "source": source,
            "technical_rule": rule,
            "insider_role": insider_role if source == "insider" else None,
            "universe_size": len(config.universe),
            "start_date": signal_start.isoformat(),
            "end_date": config.end_date.isoformat(),
            "hold_days": hold_days,
            "benchmark": config.benchmark,
            "weighting": config.weighting,
            "rebalance_frequency": config.rebalance_frequency,
            "execution_timing": "first trading day strictly after public disclosure date",
            "slippage_bps_per_side": config.slippage_bps,
            "fee_bps_per_side": config.fee_bps,
            "require_adjusted_prices": config.require_adjusted,
            "data_state": "production PostgreSQL read-only research query",
            "run_timestamp": datetime.now(timezone.utc).isoformat(),
        },
        "filter_skips": technical_skips,
        "performance": _performance_for_lots(
            config=config,
            lots=lots,
            skipped=skipped,
            price_maps=price_maps,
            benchmark_prices=benchmark_prices,
            hold_days=hold_days,
            per_side_cost_rate=per_side_cost_rate,
        ),
        "diagnostics": summarize_strategy_quality(
            primary_signals=primary_signals,
            confirmed_signals=confirmed_signals,
            lots=lots,
            skipped=skipped,
            sector_by_symbol=sector_by_symbol,
            limit=limit,
        ),
    }


def run_primary_diagnostics(
    db: Session,
    config: ResearchConfig,
    *,
    source: PrimarySource,
    insider_role: InsiderRole,
    limit: int,
) -> dict[str, Any]:
    price_maps, benchmark_prices, benchmark_dates, signal_start = _price_context(
        db,
        config,
        price_start=config.start_date or date(1990, 1, 1),
    )
    primary_signals = _load_primary_signals(
        db,
        source,
        universe=config.universe,
        start_date=signal_start,
        end_date=config.end_date,
        insider_role=insider_role,
    )
    per_side_cost_rate = max((config.slippage_bps + config.fee_bps) / 10000.0, 0.0)
    hold_days = config.hold_days[0]
    universe_price_maps = {symbol: prices for symbol, prices in price_maps.items() if symbol != config.benchmark}
    lots, skipped = build_lots(
        primary_signals,
        universe_price_maps,
        benchmark_dates=benchmark_dates,
        hold_days=hold_days,
        rebalance_frequency=config.rebalance_frequency,
        per_side_cost_rate=per_side_cost_rate,
    )
    sector_by_symbol = load_current_sector_map(db, {signal.symbol for signal in primary_signals})
    return {
        "metadata": {
            "strategy_kind": "primary",
            "strategy_name": config.strategy_name,
            "methodology_version": METHODOLOGY_VERSION,
            "source": source,
            "insider_role": insider_role if source == "insider" else None,
            "universe_size": len(config.universe),
            "start_date": signal_start.isoformat(),
            "end_date": config.end_date.isoformat(),
            "hold_days": hold_days,
            "benchmark": config.benchmark,
            "weighting": config.weighting,
            "rebalance_frequency": config.rebalance_frequency,
            "execution_timing": "first trading day strictly after public disclosure date",
            "slippage_bps_per_side": config.slippage_bps,
            "fee_bps_per_side": config.fee_bps,
            "require_adjusted_prices": config.require_adjusted,
            "data_state": "production PostgreSQL read-only research query",
            "run_timestamp": datetime.now(timezone.utc).isoformat(),
        },
        "performance": _performance_for_lots(
            config=config,
            lots=lots,
            skipped=skipped,
            price_maps=price_maps,
            benchmark_prices=benchmark_prices,
            hold_days=hold_days,
            per_side_cost_rate=per_side_cost_rate,
        ),
        "diagnostics": summarize_strategy_quality(
            primary_signals=primary_signals,
            confirmed_signals=primary_signals,
            lots=lots,
            skipped=skipped,
            sector_by_symbol=sector_by_symbol,
            limit=limit,
        ),
    }


def run_cross_source_diagnostics(
    db: Session,
    config: ResearchConfig,
    *,
    pair: AlignmentPair,
    lookback_days: int,
    min_confirming_signals: int,
    min_contract_amount: float,
    limit: int,
    min_institutional_materiality: float = 80.0,
) -> dict[str, Any]:
    price_maps, benchmark_prices, benchmark_dates, signal_start = _price_context(
        db,
        config,
        price_start=config.start_date or date(1990, 1, 1),
    )
    primary_source, confirming_source = _pair_sources(pair)
    primary_signals = _source_signals(
        db,
        primary_source,
        universe=config.universe,
        start_date=signal_start,
        end_date=config.end_date,
        min_contract_amount=min_contract_amount,
        min_institutional_materiality=min_institutional_materiality,
    )
    confirming_signals = _source_signals(
        db,
        confirming_source,
        universe=config.universe,
        start_date=signal_start - timedelta(days=max(lookback_days, 0)),
        end_date=config.end_date,
        min_contract_amount=min_contract_amount,
        min_institutional_materiality=min_institutional_materiality,
    )
    alignment_rows = build_alignment_signals(
        primary_signals,
        confirming_signals,
        lookback_days=lookback_days,
        min_confirming_signals=min_confirming_signals,
        primary_source=primary_source,
        confirming_source=confirming_source,
    )
    confirmed_signals = [row.signal for row in alignment_rows]
    per_side_cost_rate = max((config.slippage_bps + config.fee_bps) / 10000.0, 0.0)
    hold_days = config.hold_days[0]
    universe_price_maps = {symbol: prices for symbol, prices in price_maps.items() if symbol != config.benchmark}
    lots, skipped = build_lots(
        confirmed_signals,
        universe_price_maps,
        benchmark_dates=benchmark_dates,
        hold_days=hold_days,
        rebalance_frequency=config.rebalance_frequency,
        per_side_cost_rate=per_side_cost_rate,
    )
    sector_by_symbol = load_current_sector_map(db, {signal.symbol for signal in confirmed_signals})
    return {
        "metadata": {
            "strategy_kind": "cross_source",
            "strategy_name": config.strategy_name,
            "methodology_version": METHODOLOGY_VERSION,
            "pair": pair,
            "primary_source": primary_source,
            "confirming_source": confirming_source,
            "lookback_days": lookback_days,
            "min_confirming_signals": min_confirming_signals,
            "min_contract_amount": min_contract_amount,
            "min_institutional_materiality": min_institutional_materiality,
            "universe_size": len(config.universe),
            "start_date": signal_start.isoformat(),
            "end_date": config.end_date.isoformat(),
            "hold_days": hold_days,
            "benchmark": config.benchmark,
            "weighting": config.weighting,
            "rebalance_frequency": config.rebalance_frequency,
            "execution_timing": "first trading day strictly after the later source disclosure/proxy date",
            "slippage_bps_per_side": config.slippage_bps,
            "fee_bps_per_side": config.fee_bps,
            "require_adjusted_prices": config.require_adjusted,
            "data_state": "production PostgreSQL read-only research query",
            "run_timestamp": datetime.now(timezone.utc).isoformat(),
        },
        "confirming_signal_count": len(confirming_signals),
        "performance": _performance_for_lots(
            config=config,
            lots=lots,
            skipped=skipped,
            price_maps=price_maps,
            benchmark_prices=benchmark_prices,
            hold_days=hold_days,
            per_side_cost_rate=per_side_cost_rate,
        ),
        "diagnostics": summarize_strategy_quality(
            primary_signals=primary_signals,
            confirmed_signals=confirmed_signals,
            lots=lots,
            skipped=skipped,
            sector_by_symbol=sector_by_symbol,
            limit=limit,
        ),
    }


def _label(args: argparse.Namespace) -> str:
    if args.strategy_kind == "primary":
        if args.source == "congress":
            return "Congress Buys"
        if args.source == "institutional":
            return "Institutional Accumulation"
        return f"Insider {args.insider_role.replace('_', ' ').title()} Buys"
    if args.strategy_kind == "technical":
        if args.source == "congress":
            source = "Congress"
        elif args.source == "institutional":
            source = "Institutional"
        else:
            source = f"Insider {args.insider_role.replace('_', ' ').title()}"
        return f"{source} + {args.rule.replace('_', ' ').title()}"
    return {
        "congress_insider": "Congress + Insider Confirmation",
        "congress_contracts": "Congress + Government Contracts",
        "congress_institutional": "Congress + Institutional Accumulation",
        "insider_contracts": "Insider + Government Contracts",
        "insider_institutional": "Insider + Institutional Accumulation",
    }[args.pair]


def _print_text_report(result: dict[str, Any]) -> None:
    meta = result["metadata"]
    perf = result["performance"]
    diagnostics = result["diagnostics"]
    print(
        "RUN "
        f"strategy={meta['strategy_name']} methodology={meta['methodology_version']} "
        f"kind={meta['strategy_kind']} universe={meta['universe_size']} hold_days={meta['hold_days']} "
        f"confidence={diagnostics['data_quality_confidence']} "
        f"flags={','.join(diagnostics['concentration_flags']) or 'none'}"
    )
    print(
        "PERFORMANCE "
        f"status={perf.get('status')} lots={perf.get('lots')} start={perf.get('start_date')} end={perf.get('end_date')} "
        f"cagr={perf.get('cagr_pct')}% spy_cagr={perf.get('benchmark_cagr_pct')}% "
        f"alpha={perf.get('alpha_cagr_pct')}% sharpe={perf.get('sharpe')} "
        f"max_dd={perf.get('max_drawdown_pct')}% win={perf.get('win_rate_pct')}% "
        f"roll12m_beat_spy={perf.get('rolling_12m_beating_spy_pct')}%"
    )
    print(
        "DIAGNOSTICS "
        f"signals={diagnostics['confirmed_signals']}/{diagnostics['primary_signals']} "
        f"lots={diagnostics['lots']} symbols={diagnostics['unique_symbols']} "
        f"actors={diagnostics['unique_actors']} amount_missing={diagnostics['amount_missing_pct']}%"
    )
    print(f"LOT_SKIPS {json.dumps(diagnostics['skipped_lots'], sort_keys=True)}")
    for key in (
        "top_symbols",
        "top_actors",
        "top_sectors",
        "top_disclosure_months",
        "top_symbols_by_net_return",
        "top_actors_by_net_return",
        "top_sectors_by_net_return",
    ):
        print(f"{key.upper()} {json.dumps(diagnostics[key], sort_keys=True)}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Read-only concentration diagnostics for candidate strategy research.")
    parser.add_argument("--strategy-kind", choices=("primary", "technical", "cross_source"), required=True)
    parser.add_argument("--source", choices=("congress", "insider", "institutional"), default="congress")
    parser.add_argument(
        "--rule",
        choices=("sma50_above_sma200", "price_above_sma50_sma200", "golden_cross_30d", "macd_bullish", "technical_alignment"),
        default="technical_alignment",
    )
    parser.add_argument("--insider-role", choices=("all", "ceo", "cfo", "director", "officer", "ten_percent_owner"), default="all")
    parser.add_argument(
        "--pair",
        choices=(
            "congress_insider",
            "congress_contracts",
            "congress_institutional",
            "insider_contracts",
            "insider_institutional",
        ),
        default="congress_insider",
    )
    parser.add_argument("--lookback-days", type=int, default=90)
    parser.add_argument("--min-confirming-signals", type=int, default=1)
    parser.add_argument("--min-contract-amount", type=float, default=1_000_000.0)
    parser.add_argument("--min-institutional-materiality", type=float, default=80.0)
    parser.add_argument("--symbols")
    parser.add_argument(
        "--universe-source",
        choices=("explicit", "normalized_insider_purchases", "fundamentals_snapshots"),
        default="explicit",
    )
    parser.add_argument("--snapshot-source-kind", default="ticker_financials_cache_statement_proxy")
    parser.add_argument("--min-snapshots-per-symbol", type=int, default=1)
    parser.add_argument("--exclude-symbols")
    parser.add_argument("--benchmark", default="SPY")
    parser.add_argument("--start-date")
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--hold-days", type=int, default=90)
    parser.add_argument("--weighting", choices=("equal", "transaction_value"), default="equal")
    parser.add_argument("--rebalance-frequency", choices=("event", "weekly", "monthly"), default="event")
    parser.add_argument("--slippage-bps", type=float, default=5.0)
    parser.add_argument("--fee-bps", type=float, default=0.0)
    parser.add_argument("--allow-raw-prices", action="store_true")
    parser.add_argument("--min-lots", type=int, default=50)
    parser.add_argument("--top", type=int, default=10)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    end_date = parse_iso_date(args.end_date) or date.today()
    start_date = parse_iso_date(args.start_date) if args.start_date else None
    exclude_symbols = _parse_exclude_symbols(args.exclude_symbols)
    with SessionLocal() as db:
        universe = _load_universe(
            db,
            universe_source=args.universe_source,
            symbols=args.symbols,
            start_date=start_date,
            end_date=end_date,
            exclude_symbols=exclude_symbols,
            snapshot_source_kind=args.snapshot_source_kind if args.snapshot_source_kind else None,
            min_snapshots_per_symbol=args.min_snapshots_per_symbol,
        )
        config = ResearchConfig(
            strategy_name=_label(args),
            universe=universe,
            benchmark=normalize_symbol(args.benchmark) or "SPY",
            start_date=start_date,
            end_date=end_date,
            hold_days=(int(args.hold_days),),
            weighting=args.weighting,
            rebalance_frequency=args.rebalance_frequency,
            slippage_bps=float(args.slippage_bps),
            fee_bps=float(args.fee_bps),
            require_adjusted=not args.allow_raw_prices,
            min_lots=int(args.min_lots),
        )
        if args.strategy_kind == "primary":
            result = run_primary_diagnostics(
                db,
                config,
                source=args.source,
                insider_role=args.insider_role,
                limit=int(args.top),
            )
        elif args.strategy_kind == "technical":
            result = run_technical_diagnostics(
                db,
                config,
                source=args.source,
                rule=args.rule,
                insider_role=args.insider_role,
                limit=int(args.top),
            )
        else:
            result = run_cross_source_diagnostics(
                db,
                config,
                pair=args.pair,
                lookback_days=int(args.lookback_days),
                min_confirming_signals=int(args.min_confirming_signals),
                min_contract_amount=float(args.min_contract_amount),
                min_institutional_materiality=float(args.min_institutional_materiality),
                limit=int(args.top),
            )
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        _print_text_report(result)


if __name__ == "__main__":
    main()
