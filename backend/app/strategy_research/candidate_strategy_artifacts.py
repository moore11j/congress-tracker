from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any

from sqlalchemy.orm import Session

from app.strategy_research.candidate_strategy_diagnostics import _load_universe, _price_context
from app.strategy_research.candidate_strategy_validation import CandidateDefinition
from app.strategy_research.congress_buys import (
    Lot,
    PriceBar,
    ResearchConfig,
    Signal,
    build_lots,
    compute_metrics,
    simulate_active_lot_portfolio,
)
from app.strategy_research.cross_source_alignment import _pair_sources, _source_signals, build_alignment_signals
from app.strategy_research.strategy_quality_diagnostics import load_current_sector_map, summarize_strategy_quality
from app.strategy_research.technical_confirmation import _load_primary_signals, filter_signals_by_technical_rule

METHODOLOGY_VERSION = "candidate_strategy_artifact_v1"


@dataclass(frozen=True)
class CandidateStrategyArtifact:
    candidate: CandidateDefinition
    metadata: dict[str, Any]
    performance: dict[str, Any]
    diagnostics: dict[str, Any]
    simulation: dict[str, Any]
    lots: list[Lot]
    primary_signals: list[Signal]
    confirmed_signals: list[Signal]
    price_maps: dict[str, dict[date, PriceBar]]
    benchmark_prices: dict[date, PriceBar]
    universe: tuple[str, ...]
    filter_skips: dict[str, int]


def load_candidate_universe(
    db: Session,
    candidate: CandidateDefinition,
    *,
    end_date: date,
    snapshot_source_kind: str | None,
    min_snapshots_per_symbol: int,
) -> tuple[str, ...]:
    return _load_universe(
        db,
        universe_source=candidate.universe_source,
        symbols=None,
        start_date=None,
        end_date=end_date,
        exclude_symbols=candidate.exclude_symbols,
        snapshot_source_kind=snapshot_source_kind,
        min_snapshots_per_symbol=min_snapshots_per_symbol,
    )


def _metadata(
    *,
    candidate: CandidateDefinition,
    config: ResearchConfig,
    signal_start: date,
    universe_source: str,
    snapshot_source_kind: str | None,
    min_snapshots_per_symbol: int,
    extra: dict[str, Any],
) -> dict[str, Any]:
    return {
        "strategy_name": candidate.name,
        "strategy_slug": candidate.slug,
        "strategy_kind": candidate.strategy_kind,
        "methodology_version": METHODOLOGY_VERSION,
        "universe_source": universe_source,
        "snapshot_source_kind": snapshot_source_kind,
        "min_snapshots_per_symbol": min_snapshots_per_symbol,
        "universe_size": len(config.universe),
        "start_date": signal_start.isoformat(),
        "end_date": config.end_date.isoformat(),
        "hold_days": config.hold_days[0],
        "benchmark": config.benchmark,
        "weighting": config.weighting,
        "rebalance_frequency": config.rebalance_frequency,
        "execution_timing": (
            "first trading day strictly after public disclosure date"
            if candidate.strategy_kind == "technical"
            else "first trading day strictly after the later source disclosure/proxy date"
        ),
        "slippage_bps_per_side": config.slippage_bps,
        "fee_bps_per_side": config.fee_bps,
        "require_adjusted_prices": config.require_adjusted,
        "min_lots": config.min_lots,
        "data_state": "production PostgreSQL read-only research query",
        "run_timestamp": datetime.now(timezone.utc).isoformat(),
        **extra,
    }


def _simulate(
    *,
    config: ResearchConfig,
    lots: list[Lot],
    skipped: dict[str, int],
    price_maps: dict[str, dict[date, PriceBar]],
    benchmark_prices: dict[date, PriceBar],
    hold_days: int,
    per_side_cost_rate: float,
) -> tuple[dict[str, Any], dict[str, Any]]:
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
    return simulation, metrics


def build_candidate_strategy_artifact(
    db: Session,
    candidate: CandidateDefinition,
    *,
    start_date: date | None,
    end_date: date,
    benchmark: str,
    slippage_bps: float,
    fee_bps: float,
    require_adjusted: bool,
    min_lots: int,
    snapshot_source_kind: str | None,
    min_snapshots_per_symbol: int,
    top: int,
) -> CandidateStrategyArtifact:
    universe = load_candidate_universe(
        db,
        candidate,
        end_date=end_date,
        snapshot_source_kind=snapshot_source_kind,
        min_snapshots_per_symbol=min_snapshots_per_symbol,
    )
    config = ResearchConfig(
        strategy_name=candidate.name,
        universe=universe,
        benchmark=benchmark,
        start_date=start_date,
        end_date=end_date,
        hold_days=(candidate.hold_days,),
        weighting="equal",
        rebalance_frequency="event",
        slippage_bps=slippage_bps,
        fee_bps=fee_bps,
        require_adjusted=require_adjusted,
        min_lots=min_lots,
    )
    if candidate.strategy_kind == "technical":
        price_start = (config.start_date - timedelta(days=420)) if config.start_date else date(1990, 1, 1)
    else:
        price_start = config.start_date or date(1990, 1, 1)
    price_maps, benchmark_prices, benchmark_dates, signal_start = _price_context(db, config, price_start=price_start)
    universe_price_maps = {symbol: prices for symbol, prices in price_maps.items() if symbol != config.benchmark}
    per_side_cost_rate = max((config.slippage_bps + config.fee_bps) / 10000.0, 0.0)
    hold_days = config.hold_days[0]

    if candidate.strategy_kind == "technical":
        primary_signals = _load_primary_signals(
            db,
            candidate.source,
            universe=config.universe,
            start_date=signal_start,
            end_date=config.end_date,
            insider_role=candidate.insider_role,
        )
        confirmed_signals, filter_skips = filter_signals_by_technical_rule(
            primary_signals,
            universe_price_maps,
            rule=candidate.rule,
        )
        metadata_extra = {
            "source": candidate.source,
            "technical_rule": candidate.rule,
            "insider_role": candidate.insider_role if candidate.source == "insider" else None,
            "technical_as_of": "computed only from adjusted prices on or before the signal disclosure date",
        }
    else:
        primary_source, confirming_source = _pair_sources(candidate.pair)
        primary_signals = _source_signals(
            db,
            primary_source,
            universe=config.universe,
            start_date=signal_start,
            end_date=config.end_date,
            min_contract_amount=candidate.min_contract_amount,
        )
        confirming_signals = _source_signals(
            db,
            confirming_source,
            universe=config.universe,
            start_date=signal_start - timedelta(days=max(candidate.lookback_days, 0)),
            end_date=config.end_date,
            min_contract_amount=candidate.min_contract_amount,
        )
        alignment_rows = build_alignment_signals(
            primary_signals,
            confirming_signals,
            lookback_days=candidate.lookback_days,
            min_confirming_signals=candidate.min_confirming_signals,
            primary_source=primary_source,
            confirming_source=confirming_source,
        )
        confirmed_signals = [row.signal for row in alignment_rows]
        filter_skips = {}
        metadata_extra = {
            "pair": candidate.pair,
            "primary_source": primary_source,
            "confirming_source": confirming_source,
            "confirming_signal_count": len(confirming_signals),
            "lookback_days": candidate.lookback_days,
            "min_confirming_signals": candidate.min_confirming_signals,
            "min_contract_amount": candidate.min_contract_amount,
        }

    lots, skipped = build_lots(
        confirmed_signals,
        universe_price_maps,
        benchmark_dates=benchmark_dates,
        hold_days=hold_days,
        rebalance_frequency=config.rebalance_frequency,
        per_side_cost_rate=per_side_cost_rate,
    )
    simulation, performance = _simulate(
        config=config,
        lots=lots,
        skipped=skipped,
        price_maps=price_maps,
        benchmark_prices=benchmark_prices,
        hold_days=hold_days,
        per_side_cost_rate=per_side_cost_rate,
    )
    sector_by_symbol = load_current_sector_map(db, {signal.symbol for signal in confirmed_signals})
    diagnostics = summarize_strategy_quality(
        primary_signals=primary_signals,
        confirmed_signals=confirmed_signals,
        lots=lots,
        skipped=skipped,
        sector_by_symbol=sector_by_symbol,
        limit=top,
    )
    return CandidateStrategyArtifact(
        candidate=candidate,
        metadata=_metadata(
            candidate=candidate,
            config=config,
            signal_start=signal_start,
            universe_source=candidate.universe_source,
            snapshot_source_kind=snapshot_source_kind,
            min_snapshots_per_symbol=min_snapshots_per_symbol,
            extra=metadata_extra,
        ),
        performance=performance,
        diagnostics=diagnostics,
        simulation=simulation,
        lots=lots,
        primary_signals=primary_signals,
        confirmed_signals=confirmed_signals,
        price_maps=price_maps,
        benchmark_prices=benchmark_prices,
        universe=universe,
        filter_skips=filter_skips,
    )
