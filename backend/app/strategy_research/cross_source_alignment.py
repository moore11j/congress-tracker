from __future__ import annotations

import argparse
import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable, Literal

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.db import SessionLocal
from app.models import GovernmentContract
from app.strategy_research.congress_buys import (
    DEFAULT_UNIVERSE,
    METHODOLOGY_VERSION as BASE_ENGINE_VERSION,
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
    load_insider_open_market_purchase_signals,
    load_normalized_purchase_universe,
)
from app.utils.symbols import normalize_symbol

MethodologyVersion = Literal["cross_source_alignment_research_v1"]
AlignmentPair = Literal["congress_insider", "congress_contracts", "insider_contracts"]

METHODOLOGY_VERSION: MethodologyVersion = "cross_source_alignment_research_v1"
CONTRACT_AWARD_DATE_PROXY_NOTE = (
    "government contract award_date is used as a public-availability proxy; lower confidence until a true "
    "point-in-time publication/discovery date is stored"
)


@dataclass(frozen=True)
class AlignmentSignal:
    signal: Signal
    primary_count: int
    confirming_count: int
    primary_source: str
    confirming_source: str


def load_government_contract_signals(
    db: Session,
    *,
    universe: Iterable[str],
    start_date: date,
    end_date: date,
    min_amount: float,
) -> list[Signal]:
    normalized_universe = set(_normalize_universe(universe))
    rows = (
        db.execute(
            select(GovernmentContract)
            .where(func.upper(GovernmentContract.symbol).in_(normalized_universe))
            .where(GovernmentContract.award_date >= start_date)
            .where(GovernmentContract.award_date <= end_date)
            .where(GovernmentContract.award_amount >= float(min_amount))
            .order_by(GovernmentContract.award_date.asc(), GovernmentContract.id.asc())
        )
        .scalars()
        .all()
    )
    signals: list[Signal] = []
    seen: set[tuple[object, ...]] = set()
    for row in rows:
        symbol = normalize_symbol(row.symbol)
        if not symbol or row.award_date is None:
            continue
        amount = int(round(float(row.award_amount or 0.0))) if row.award_amount is not None else None
        dedupe_key = (
            row.award_id or row.dedupe_key or row.id,
            symbol,
            row.award_date,
            amount,
        )
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        signals.append(
            Signal(
                event_id=int(row.id),
                symbol=symbol,
                disclosure_date=row.award_date,
                raw_entry_date=row.award_date + timedelta(days=1),
                amount_min=amount,
                amount_max=amount,
                member_name=row.recipient_name,
                member_bioguide_id=None,
                chamber=row.awarding_agency,
                party=None,
                source_filing_id=row.award_id or row.dedupe_key,
                source_document_url=row.source_url,
                dedupe_key=dedupe_key,
            )
        )
    return signals


def _source_signals(
    db: Session,
    source: str,
    *,
    universe: Iterable[str],
    start_date: date,
    end_date: date,
    min_contract_amount: float,
) -> list[Signal]:
    if source == "congress":
        return load_congress_purchase_signals(db, universe=universe, start_date=start_date, end_date=end_date)
    if source == "insider":
        return load_insider_open_market_purchase_signals(
            db,
            universe=universe,
            start_date=start_date,
            end_date=end_date,
            role="all",
        )
    if source == "contracts":
        return load_government_contract_signals(
            db,
            universe=universe,
            start_date=start_date,
            end_date=end_date,
            min_amount=min_contract_amount,
        )
    raise ValueError(f"Unsupported alignment source: {source}")


def _pair_sources(pair: AlignmentPair) -> tuple[str, str]:
    if pair == "congress_insider":
        return "congress", "insider"
    if pair == "congress_contracts":
        return "congress", "contracts"
    if pair == "insider_contracts":
        return "insider", "contracts"
    raise ValueError(f"Unsupported alignment pair: {pair}")


def _signal_amount(signal: Signal) -> int | None:
    if signal.amount_max is not None and signal.amount_max > 0:
        return signal.amount_max
    if signal.amount_min is not None and signal.amount_min > 0:
        return signal.amount_min
    return None


def build_alignment_signals(
    primary_signals: list[Signal],
    confirming_signals: list[Signal],
    *,
    lookback_days: int,
    min_confirming_signals: int,
    primary_source: str,
    confirming_source: str,
) -> list[AlignmentSignal]:
    by_symbol: dict[str, list[Signal]] = defaultdict(list)
    for signal in confirming_signals:
        by_symbol[signal.symbol].append(signal)
    for rows in by_symbol.values():
        rows.sort(key=lambda signal: (signal.disclosure_date, signal.event_id))

    grouped: dict[tuple[str, date], dict[str, Any]] = {}
    for primary in primary_signals:
        window_start = primary.disclosure_date - timedelta(days=max(lookback_days, 0))
        confirming = [
            signal
            for signal in by_symbol.get(primary.symbol, [])
            if window_start <= signal.disclosure_date <= primary.disclosure_date
        ]
        if len(confirming) < min_confirming_signals:
            continue
        key = (primary.symbol, primary.disclosure_date)
        amount_values = [_signal_amount(primary), *(_signal_amount(signal) for signal in confirming)]
        amount = max((value for value in amount_values if value is not None), default=None)
        row = grouped.setdefault(
            key,
            {
                "primary": primary,
                "primary_count": 0,
                "confirming": {},
                "amount": amount,
            },
        )
        row["primary_count"] += 1
        row["amount"] = max((row.get("amount"), amount), key=lambda value: value or 0)
        for signal in confirming:
            row["confirming"][signal.dedupe_key or (signal.symbol, signal.disclosure_date, signal.event_id)] = signal

    alignment_signals: list[AlignmentSignal] = []
    for (symbol, disclosure_date), row in sorted(grouped.items(), key=lambda item: (item[0][1], item[0][0])):
        primary = row["primary"]
        confirming = list(row["confirming"].values())
        amount = row.get("amount")
        signal = Signal(
            event_id=int(primary.event_id),
            symbol=symbol,
            disclosure_date=disclosure_date,
            raw_entry_date=disclosure_date + timedelta(days=1),
            amount_min=amount,
            amount_max=amount,
            member_name=primary.member_name,
            member_bioguide_id=primary.member_bioguide_id,
            chamber=primary.chamber,
            party=primary.party,
            source_filing_id=f"{primary_source}:{primary.source_filing_id or primary.event_id}",
            source_document_url=primary.source_document_url,
            dedupe_key=(
                "alignment",
                primary_source,
                confirming_source,
                symbol,
                disclosure_date,
                primary.event_id,
                tuple(sorted(signal.event_id for signal in confirming)),
            ),
        )
        alignment_signals.append(
            AlignmentSignal(
                signal=signal,
                primary_count=int(row["primary_count"]),
                confirming_count=len(confirming),
                primary_source=primary_source,
                confirming_source=confirming_source,
            )
        )
    return alignment_signals


def run_research(
    db: Session,
    config: ResearchConfig,
    *,
    pair: AlignmentPair,
    lookback_days: int,
    min_confirming_signals: int,
    min_contract_amount: float,
) -> dict[str, Any]:
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
    primary_source, confirming_source = _pair_sources(pair)
    primary_signals = _source_signals(
        db,
        primary_source,
        universe=config.universe,
        start_date=signal_start,
        end_date=config.end_date,
        min_contract_amount=min_contract_amount,
    )
    confirming_signals = _source_signals(
        db,
        confirming_source,
        universe=config.universe,
        start_date=signal_start - timedelta(days=max(lookback_days, 0)),
        end_date=config.end_date,
        min_contract_amount=min_contract_amount,
    )
    alignment_rows = build_alignment_signals(
        primary_signals,
        confirming_signals,
        lookback_days=lookback_days,
        min_confirming_signals=min_confirming_signals,
        primary_source=primary_source,
        confirming_source=confirming_source,
    )
    signals = [row.signal for row in alignment_rows]
    per_side_cost_rate = max((config.slippage_bps + config.fee_bps) / 10000.0, 0.0)
    universe_price_maps = {symbol: prices for symbol, prices in price_maps.items() if symbol != config.benchmark}
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

    contract_proxy = "contracts" in {primary_source, confirming_source}
    return {
        "metadata": {
            "strategy_name": config.strategy_name,
            "plain_english_rule": (
                f"Buy when a {primary_source} bullish event is confirmed by at least "
                f"{min_confirming_signals} {confirming_source} event(s) for the same ticker in the prior "
                f"{lookback_days} calendar days; enter on the next trading day after the later public date."
            ),
            "methodology_version": METHODOLOGY_VERSION,
            "base_engine_version": BASE_ENGINE_VERSION,
            "run_timestamp": datetime.now(timezone.utc).isoformat(),
            "pair": pair,
            "primary_source": primary_source,
            "confirming_source": confirming_source,
            "lookback_days": lookback_days,
            "min_confirming_signals": min_confirming_signals,
            "min_contract_amount": min_contract_amount,
            "universe": list(config.universe),
            "benchmark": config.benchmark,
            "start_date": signal_start.isoformat(),
            "end_date": config.end_date.isoformat(),
            "weighting": config.weighting,
            "rebalance_frequency": config.rebalance_frequency,
            "execution_timing": "first trading day strictly after the later source disclosure/proxy date",
            "fees_bps_per_side": config.fee_bps,
            "slippage_bps_per_side": config.slippage_bps,
            "require_adjusted_prices": config.require_adjusted,
            "price_source": "price_cache.adjusted_close",
            "data_quality_confidence": "lower" if contract_proxy else "medium",
            "data_quality_note": CONTRACT_AWARD_DATE_PROXY_NOTE if contract_proxy else None,
            "data_state": "production PostgreSQL read-only research query",
        },
        "signal_count": len(signals),
        "primary_signal_count": len(primary_signals),
        "confirming_signal_count": len(confirming_signals),
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


def _label(pair: AlignmentPair) -> str:
    return {
        "congress_insider": "Congress + Insider Confirmation",
        "congress_contracts": "Congress + Government Contracts",
        "insider_contracts": "Insider + Government Contracts",
    }[pair]


def _print_text_report(result: dict[str, Any]) -> None:
    meta = result["metadata"]
    print(
        "RUN "
        f"strategy={meta['strategy_name']} methodology={meta['methodology_version']} "
        f"pair={meta['pair']} signals={result['signal_count']} aligned_symbols={result['aligned_symbol_count']} "
        f"primary_signals={result['primary_signal_count']} confirming_signals={result['confirming_signal_count']} "
        f"universe={len(meta['universe'])} lookback_days={meta['lookback_days']} "
        f"weighting={meta['weighting']} rebalance={meta['rebalance_frequency']} "
        f"cost_bps_per_side={meta['fees_bps_per_side'] + meta['slippage_bps_per_side']} "
        f"confidence={meta['data_quality_confidence']}"
    )
    if meta.get("data_quality_note"):
        print(f"DATA_QUALITY_NOTE {meta['data_quality_note']}")
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
    parser = argparse.ArgumentParser(description="Read-only cross-source alignment strategy research runner.")
    parser.add_argument("--pair", choices=("congress_insider", "congress_contracts", "insider_contracts"), required=True)
    parser.add_argument("--symbols", help="Comma-separated universe. Defaults to the approved 24-symbol research universe.")
    parser.add_argument(
        "--universe-source",
        choices=("explicit", "normalized_insider_purchases"),
        default="explicit",
    )
    parser.add_argument("--exclude-symbols", help="Comma-separated symbols to exclude from the selected universe.")
    parser.add_argument("--benchmark", default="SPY")
    parser.add_argument("--start-date")
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--hold-days", default="30,90,180,365")
    parser.add_argument("--lookback-days", type=int, default=90)
    parser.add_argument("--min-confirming-signals", type=int, default=1)
    parser.add_argument("--min-contract-amount", type=float, default=1_000_000.0)
    parser.add_argument("--weighting", choices=("equal", "transaction_value"), default="equal")
    parser.add_argument("--rebalance-frequency", choices=("event", "weekly", "monthly"), default="event")
    parser.add_argument("--slippage-bps", type=float, default=5.0)
    parser.add_argument("--fee-bps", type=float, default=0.0)
    parser.add_argument("--allow-raw-prices", action="store_true")
    parser.add_argument("--min-lots", type=int, default=30)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    pair = args.pair
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
        result = run_research(
            db,
            ResearchConfig(
                strategy_name=_label(pair),
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
            pair=pair,
            lookback_days=int(args.lookback_days),
            min_confirming_signals=int(args.min_confirming_signals),
            min_contract_amount=float(args.min_contract_amount),
        )
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        _print_text_report(result)


if __name__ == "__main__":
    main()
