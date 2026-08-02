from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from datetime import date
from typing import Any

from sqlalchemy.orm import Session

from app.db import SessionLocal
from app.strategy_research.congress_buys import (
    ResearchConfig,
    Signal,
    build_lots,
    load_adjusted_price_histories,
    parse_iso_date,
)
from app.strategy_research.fundamental_confirmation import (
    FundamentalRule,
    FundamentalsSnapshotLookup,
    filter_signals_by_fundamental_rule,
)
from app.strategy_research.fundamental_confirmation_sweep import load_fundamentals_snapshot_universe
from app.strategy_research.insider_buys import InsiderRole, load_insider_open_market_purchase_signals
from app.utils.symbols import normalize_symbol


def _pct(part: int | float, total: int | float) -> float:
    return round((float(part) / float(total) * 100.0), 4) if total else 0.0


def _top_counts(counter: Counter[str], *, total: int, limit: int) -> list[dict[str, Any]]:
    return [
        {"key": key, "count": count, "pct": _pct(count, total)}
        for key, count in counter.most_common(limit)
    ]


def _signal_owner(signal: Signal) -> str:
    return signal.member_bioguide_id or signal.member_name or "unknown"


def _month_key(value: date) -> str:
    return f"{value.year:04d}-{value.month:02d}"


def _parse_exclude_symbols(value: str | None) -> tuple[str, ...]:
    if not value:
        return ()
    return tuple(symbol for part in value.split(",") if (symbol := normalize_symbol(part.strip())))


def _concentration_flags(summary: dict[str, Any]) -> list[str]:
    flags: list[str] = []
    total_lots = int(summary.get("lots") or 0)
    unique_symbols = int(summary.get("unique_symbols") or 0)
    unique_owners = int(summary.get("unique_owners") or 0)
    top_symbol_pct = (summary.get("top_symbols") or [{}])[0].get("pct", 0.0) if summary.get("top_symbols") else 0.0
    top_owner_pct = (summary.get("top_owners") or [{}])[0].get("pct", 0.0) if summary.get("top_owners") else 0.0
    top_month_pct = (summary.get("top_disclosure_months") or [{}])[0].get("pct", 0.0) if summary.get("top_disclosure_months") else 0.0
    amount_missing_pct = float(summary.get("amount_missing_pct") or 0.0)
    if total_lots < 100:
        flags.append("sample_size_below_100_lots")
    if unique_symbols < 20:
        flags.append("symbol_breadth_below_20")
    if unique_owners < 20:
        flags.append("owner_breadth_below_20")
    if top_symbol_pct >= 25.0:
        flags.append("top_symbol_exceeds_25pct_of_lots")
    if top_owner_pct >= 25.0:
        flags.append("top_owner_exceeds_25pct_of_lots")
    if top_month_pct >= 25.0:
        flags.append("top_month_exceeds_25pct_of_lots")
    if amount_missing_pct >= 25.0:
        flags.append("amount_missing_for_at_least_25pct_of_signals")
    return flags


def summarize_insider_strategy_quality(
    *,
    primary_signals: list[Signal],
    confirmed_signals: list[Signal],
    lots: list[Any],
    skipped: dict[str, int],
    limit: int = 10,
) -> dict[str, Any]:
    lot_signals = [lot.signal for lot in lots]
    lot_symbols = Counter(signal.symbol for signal in lot_signals)
    lot_owners = Counter(_signal_owner(signal) for signal in lot_signals)
    lot_filings = Counter(signal.source_filing_id or f"event:{signal.event_id}" for signal in lot_signals)
    lot_months = Counter(_month_key(signal.disclosure_date) for signal in lot_signals)
    confirmed_symbols = {signal.symbol for signal in confirmed_signals}
    confirmed_owners = {_signal_owner(signal) for signal in confirmed_signals}
    amount_missing = sum(1 for signal in confirmed_signals if not signal.amount_max or signal.amount_max <= 0)

    returns_by_symbol: dict[str, list[float]] = defaultdict(list)
    returns_by_owner: dict[str, list[float]] = defaultdict(list)
    for lot in lots:
        returns_by_symbol[lot.signal.symbol].append(float(lot.net_return))
        returns_by_owner[_signal_owner(lot.signal)].append(float(lot.net_return))

    def top_returns(values: dict[str, list[float]]) -> list[dict[str, Any]]:
        rows = [
            {
                "key": key,
                "lots": len(returns),
                "avg_net_return_pct": round(sum(returns) / len(returns) * 100.0, 4),
                "sum_net_return_pct": round(sum(returns) * 100.0, 4),
            }
            for key, returns in values.items()
            if returns
        ]
        return sorted(rows, key=lambda row: (row["sum_net_return_pct"], row["lots"]), reverse=True)[:limit]

    summary = {
        "primary_signals": len(primary_signals),
        "confirmed_signals": len(confirmed_signals),
        "lots": len(lots),
        "unique_symbols": len(confirmed_symbols),
        "unique_owners": len(confirmed_owners),
        "unique_filings_in_lots": len(lot_filings),
        "amount_missing_signals": amount_missing,
        "amount_missing_pct": _pct(amount_missing, len(confirmed_signals)),
        "skipped_lots": dict(sorted(skipped.items())),
        "top_symbols": _top_counts(lot_symbols, total=len(lots), limit=limit),
        "top_owners": _top_counts(lot_owners, total=len(lots), limit=limit),
        "top_filings": _top_counts(lot_filings, total=len(lots), limit=limit),
        "top_disclosure_months": _top_counts(lot_months, total=len(lots), limit=limit),
        "top_symbols_by_net_return": top_returns(returns_by_symbol),
        "top_owners_by_net_return": top_returns(returns_by_owner),
    }
    summary["concentration_flags"] = _concentration_flags(summary)
    summary["data_quality_confidence"] = (
        "low" if summary["concentration_flags"] else "medium_proxy"
    )
    return summary


def run_diagnostics(
    db: Session,
    *,
    universe: tuple[str, ...],
    role: InsiderRole,
    rule: FundamentalRule,
    start_date: date | None,
    end_date: date,
    hold_days: int,
    benchmark: str,
    slippage_bps: float,
    fee_bps: float,
    require_adjusted: bool,
    provider: str,
    limit: int = 10,
) -> dict[str, Any]:
    price_start = start_date or date(1990, 1, 1)
    price_maps = load_adjusted_price_histories(
        db,
        (*universe, benchmark),
        start_date=price_start,
        end_date=end_date,
        require_adjusted=require_adjusted,
    )
    benchmark_prices = price_maps.get(benchmark, {})
    benchmark_dates = sorted(benchmark_prices)
    if not benchmark_dates:
        raise RuntimeError(f"Missing benchmark prices for {benchmark}.")
    universe_price_starts = [
        min(prices)
        for symbol, prices in price_maps.items()
        if symbol != benchmark and prices
    ]
    signal_start = start_date or (min(universe_price_starts) if universe_price_starts else end_date)
    primary_signals = load_insider_open_market_purchase_signals(
        db,
        universe=universe,
        start_date=signal_start,
        end_date=end_date,
        role=role,
    )
    snapshot_lookup = FundamentalsSnapshotLookup.load(
        db,
        symbols=universe,
        max_as_of=end_date,
        provider=provider,
        data_mode="snapshots",
    )
    confirmed_signals, fundamental_skips = filter_signals_by_fundamental_rule(
        db,
        primary_signals,
        rule=rule,
        provider=provider,
        data_mode="snapshots",
        snapshot_lookup=snapshot_lookup,
    )
    per_side_cost_rate = max((slippage_bps + fee_bps) / 10000.0, 0.0)
    lots, lot_skips = build_lots(
        confirmed_signals,
        {symbol: prices for symbol, prices in price_maps.items() if symbol != benchmark},
        benchmark_dates=benchmark_dates,
        hold_days=hold_days,
        rebalance_frequency="event",
        per_side_cost_rate=per_side_cost_rate,
    )
    summary = summarize_insider_strategy_quality(
        primary_signals=primary_signals,
        confirmed_signals=confirmed_signals,
        lots=lots,
        skipped=lot_skips,
        limit=limit,
    )
    return {
        "metadata": {
            "strategy": f"Insider {role.replace('_', ' ').title()} + {rule.replace('_', ' ').title()}",
            "plain_english_rule": (
                f"Open-market insider purchases for role={role}, confirmed by {rule}, entered after filing date "
                f"and held for {hold_days} days."
            ),
            "role": role,
            "rule": rule,
            "hold_days": hold_days,
            "universe_size": len(universe),
            "start_date": signal_start.isoformat(),
            "end_date": end_date.isoformat(),
            "benchmark": benchmark,
            "slippage_bps_per_side": slippage_bps,
            "fee_bps_per_side": fee_bps,
            "require_adjusted_prices": require_adjusted,
            "provider": provider,
            "data_state": "production PostgreSQL read-only research query",
            "methodology_version": "insider_strategy_concentration_diagnostics_v1",
        },
        "fundamental_skips": fundamental_skips,
        "diagnostics": summary,
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read-only concentration diagnostics for insider fundamentals strategies.")
    parser.add_argument("--role", choices=("all", "ceo", "cfo", "director", "officer", "ten_percent_owner"), default="all")
    parser.add_argument(
        "--rule",
        choices=("growth_margin_proxy", "cash_flow_growth_proxy", "eps_revenue_growth_proxy"),
        default="eps_revenue_growth_proxy",
    )
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--start-date")
    parser.add_argument("--hold-days", type=int, default=90)
    parser.add_argument("--benchmark", default="SPY")
    parser.add_argument("--slippage-bps", type=float, default=5.0)
    parser.add_argument("--fee-bps", type=float, default=0.0)
    parser.add_argument("--allow-raw-prices", action="store_true")
    parser.add_argument("--provider", default="fmp")
    parser.add_argument("--exclude-symbols", help="Comma-separated symbols to exclude from the diagnostics universe.")
    parser.add_argument("--top", type=int, default=10)
    parser.add_argument("--json", action="store_true")
    return parser.parse_args()


def _print_text_report(result: dict[str, Any]) -> None:
    meta = result["metadata"]
    diagnostics = result["diagnostics"]
    print(
        "DIAGNOSTIC "
        f"strategy={meta['strategy']} hold={meta['hold_days']} universe={meta['universe_size']} "
        f"signals={diagnostics['confirmed_signals']}/{diagnostics['primary_signals']} lots={diagnostics['lots']} "
        f"symbols={diagnostics['unique_symbols']} owners={diagnostics['unique_owners']} "
        f"confidence={diagnostics['data_quality_confidence']} flags={','.join(diagnostics['concentration_flags']) or 'none'}"
    )
    print(f"FUNDAMENTAL_SKIPS {json.dumps(result['fundamental_skips'], sort_keys=True)}")
    print(f"LOT_SKIPS {json.dumps(diagnostics['skipped_lots'], sort_keys=True)}")
    for key in ("top_symbols", "top_owners", "top_filings", "top_disclosure_months", "top_symbols_by_net_return"):
        print(f"{key.upper()} {json.dumps(diagnostics[key], sort_keys=True)}")


def main() -> None:
    args = _parse_args()
    end_date = parse_iso_date(args.end_date) or date.today()
    start_date = parse_iso_date(args.start_date) if args.start_date else None
    benchmark = normalize_symbol(args.benchmark) or "SPY"
    with SessionLocal() as db:
        exclude_symbols = _parse_exclude_symbols(args.exclude_symbols)
        universe = load_fundamentals_snapshot_universe(
            db,
            end_date=end_date,
            provider=args.provider,
            source_kind="ticker_financials_cache_statement_proxy",
            min_snapshots=1,
            exclude_symbols=exclude_symbols,
        )
        result = run_diagnostics(
            db,
            universe=universe,
            role=args.role,
            rule=args.rule,
            start_date=start_date,
            end_date=end_date,
            hold_days=int(args.hold_days),
            benchmark=benchmark,
            slippage_bps=float(args.slippage_bps),
            fee_bps=float(args.fee_bps),
            require_adjusted=not args.allow_raw_prices,
            provider=args.provider,
            limit=int(args.top),
        )
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        _print_text_report(result)


if __name__ == "__main__":
    main()
