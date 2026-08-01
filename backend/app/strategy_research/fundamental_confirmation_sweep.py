from __future__ import annotations

import argparse
import json
from datetime import date
from typing import Any, Iterable

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db import SessionLocal
from app.models import FundamentalsSnapshot
from app.strategy_research.congress_buys import ResearchConfig, parse_iso_date, _normalize_universe
from app.strategy_research.fundamental_confirmation import (
    FundamentalRule,
    PrimarySource,
    _label,
    run_research,
)
from app.strategy_research.insider_buys import InsiderRole, load_normalized_purchase_universe
from app.utils.symbols import normalize_symbol

DEFAULT_RULES: tuple[FundamentalRule, ...] = (
    "quality_growth",
    "reasonable_growth_value",
    "garp",
    "strong_profitability",
    "low_leverage",
    "dividend_growth_proxy",
)
DEFAULT_SOURCES: tuple[PrimarySource, ...] = ("congress", "insider")
DEFAULT_INSIDER_ROLES: tuple[InsiderRole, ...] = ("all", "director", "officer", "ceo", "cfo")


def load_fundamentals_snapshot_universe(
    db: Session,
    *,
    end_date: date,
    provider: str = "fmp",
    source_kind: str | None = "ticker_financials_cache_statement_proxy",
    min_snapshots: int = 1,
    exclude_symbols: Iterable[str] = (),
) -> tuple[str, ...]:
    excluded = set(_normalize_universe(exclude_symbols))
    query = (
        select(FundamentalsSnapshot.symbol)
        .where(FundamentalsSnapshot.provider == provider)
        .where(FundamentalsSnapshot.status == "ok")
        .where(FundamentalsSnapshot.snapshot_date <= end_date)
    )
    if source_kind:
        query = query.where(FundamentalsSnapshot.source_kind == source_kind)
    counts: dict[str, int] = {}
    for (raw_symbol,) in db.execute(query).all():
        symbol = normalize_symbol(raw_symbol)
        if not symbol or symbol in excluded:
            continue
        counts[symbol] = counts.get(symbol, 0) + 1
    return tuple(sorted(symbol for symbol, count in counts.items() if count >= min_snapshots))


def _parse_csv(value: str | None, default: tuple[str, ...]) -> tuple[str, ...]:
    if not value:
        return default
    return tuple(part.strip() for part in value.split(",") if part.strip())


def _parse_holds(value: str) -> tuple[int, ...]:
    return tuple(int(part.strip()) for part in value.split(",") if part.strip())


def _parse_exclude_symbols(value: str | None) -> tuple[str, ...]:
    if not value:
        return ()
    return _normalize_universe(part.strip() for part in value.split(","))


def _metric_value(row: dict[str, Any], metric: str) -> float:
    value = row.get(metric)
    if value is None:
        return float("-inf")
    return float(value)


def flatten_result(result: dict[str, Any], *, universe_source: str) -> list[dict[str, Any]]:
    meta = result["metadata"]
    rows: list[dict[str, Any]] = []
    for run in result["runs"]:
        row = {
            "strategy": meta["strategy_name"],
            "source": meta["source"],
            "insider_role": meta.get("insider_role"),
            "rule": meta["fundamental_rule"],
            "hold_days": run.get("hold_days"),
            "status": run.get("status"),
            "start_date": run.get("start_date") or meta.get("start_date"),
            "end_date": run.get("end_date") or meta.get("end_date"),
            "primary_signals": result.get("primary_signal_count"),
            "confirmed_signals": result.get("signal_count"),
            "aligned_symbols": result.get("aligned_symbol_count"),
            "lots": run.get("lots"),
            "trade_count": run.get("trade_count"),
            "cagr_pct": run.get("cagr_pct"),
            "benchmark_cagr_pct": run.get("benchmark_cagr_pct"),
            "alpha_cagr_pct": run.get("alpha_cagr_pct"),
            "sharpe": run.get("sharpe"),
            "max_drawdown_pct": run.get("max_drawdown_pct"),
            "annualized_volatility_pct": run.get("annualized_volatility_pct"),
            "win_rate_pct": run.get("win_rate_pct"),
            "turnover_events": run.get("turnover_events"),
            "avg_active_lots": run.get("avg_active_lots"),
            "rolling_12m_beating_spy_pct": run.get("rolling_12m_beating_spy_pct"),
            "data_quality_confidence": meta.get("data_quality_confidence"),
            "snapshot_provenance": meta.get("fundamental_snapshot_provenance"),
            "universe_size": len(meta.get("universe") or []),
            "universe_source": universe_source,
            "filtered_out": result.get("filtered_out"),
            "methodology_version": meta.get("methodology_version"),
            "base_engine_version": meta.get("base_engine_version"),
            "run_timestamp": meta.get("run_timestamp"),
        }
        rows.append(row)
    return rows


def sort_sweep_rows(rows: list[dict[str, Any]], *, metric: str = "alpha_cagr_pct") -> list[dict[str, Any]]:
    return sorted(
        rows,
        key=lambda row: (
            1 if row.get("status") == "ok" else 0,
            _metric_value(row, metric),
            _metric_value(row, "sharpe"),
            int(row.get("lots") or 0),
        ),
        reverse=True,
    )


def run_sweep(
    db: Session,
    *,
    universe: tuple[str, ...],
    universe_source: str,
    start_date: date | None,
    end_date: date,
    sources: tuple[PrimarySource, ...],
    rules: tuple[FundamentalRule, ...],
    insider_roles: tuple[InsiderRole, ...],
    hold_days: tuple[int, ...],
    weighting: str,
    rebalance_frequency: str,
    benchmark: str,
    slippage_bps: float,
    fee_bps: float,
    require_adjusted: bool,
    min_lots: int,
    provider: str,
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    for source in sources:
        roles: tuple[InsiderRole, ...] = insider_roles if source == "insider" else ("all",)
        for role in roles:
            for rule in rules:
                try:
                    result = run_research(
                        db,
                        ResearchConfig(
                            strategy_name=_label(source, rule, role),
                            universe=universe,
                            benchmark=benchmark,
                            start_date=start_date,
                            end_date=end_date,
                            hold_days=hold_days,
                            weighting=weighting,  # type: ignore[arg-type]
                            rebalance_frequency=rebalance_frequency,  # type: ignore[arg-type]
                            slippage_bps=slippage_bps,
                            fee_bps=fee_bps,
                            require_adjusted=require_adjusted,
                            min_lots=min_lots,
                        ),
                        source=source,
                        rule=rule,
                        insider_role=role,
                        provider=provider,
                    )
                except Exception as exc:  # pragma: no cover - production diagnostics path
                    errors.append(
                        {
                            "source": source,
                            "insider_role": role if source == "insider" else None,
                            "rule": rule,
                            "error": f"{type(exc).__name__}: {exc}",
                        }
                    )
                    continue
                rows.extend(flatten_result(result, universe_source=universe_source))

    ranked = sort_sweep_rows(rows)
    return {
        "metadata": {
            "universe_source": universe_source,
            "universe_size": len(universe),
            "start_date": start_date.isoformat() if start_date else None,
            "end_date": end_date.isoformat(),
            "sources": list(sources),
            "rules": list(rules),
            "insider_roles": list(insider_roles),
            "hold_days": list(hold_days),
            "weighting": weighting,
            "rebalance_frequency": rebalance_frequency,
            "benchmark": benchmark,
            "slippage_bps_per_side": slippage_bps,
            "fee_bps_per_side": fee_bps,
            "require_adjusted_prices": require_adjusted,
            "min_lots": min_lots,
            "provider": provider,
            "data_state": "production PostgreSQL read-only research query",
        },
        "rows": rows,
        "leaderboard": ranked,
        "errors": errors,
    }


def _print_text_report(result: dict[str, Any], *, top: int) -> None:
    meta = result["metadata"]
    print(
        "SWEEP "
        f"universe_source={meta['universe_source']} universe={meta['universe_size']} "
        f"start={meta['start_date']} end={meta['end_date']} sources={','.join(meta['sources'])} "
        f"rules={len(meta['rules'])} holds={','.join(str(value) for value in meta['hold_days'])} "
        f"min_lots={meta['min_lots']} benchmark={meta['benchmark']} "
        f"cost_bps_per_side={meta['slippage_bps_per_side'] + meta['fee_bps_per_side']}"
    )
    if result["errors"]:
        print(f"ERRORS count={len(result['errors'])} details={json.dumps(result['errors'], sort_keys=True)}")
    for index, row in enumerate(result["leaderboard"][:top], start=1):
        print(
            "#{rank} status={status} strategy={strategy} hold={hold_days} "
            "signals={confirmed_signals}/{primary_signals} symbols={aligned_symbols} lots={lots} "
            "cagr={cagr_pct} spy_cagr={benchmark_cagr_pct} alpha={alpha_cagr_pct} "
            "sharpe={sharpe} max_dd={max_drawdown_pct} win={win_rate_pct} "
            "confidence={data_quality_confidence}"
            .format(rank=index, **row)
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Read-only sweep for fundamental-confirmed strategy variants.")
    parser.add_argument("--universe-source", choices=("fundamentals_snapshots", "normalized_insider_purchases", "explicit"), default="fundamentals_snapshots")
    parser.add_argument("--snapshot-source-kind", default="ticker_financials_cache_statement_proxy", help="Set to 'all' to include every snapshot source kind.")
    parser.add_argument("--min-snapshots-per-symbol", type=int, default=1)
    parser.add_argument("--symbols", help="Comma-separated symbols when --universe-source=explicit.")
    parser.add_argument("--exclude-symbols")
    parser.add_argument("--sources", default=",".join(DEFAULT_SOURCES))
    parser.add_argument("--rules", default=",".join(DEFAULT_RULES))
    parser.add_argument("--insider-roles", default="all,director,officer")
    parser.add_argument("--benchmark", default="SPY")
    parser.add_argument("--start-date")
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--hold-days", default="90,180")
    parser.add_argument("--weighting", choices=("equal", "transaction_value"), default="equal")
    parser.add_argument("--rebalance-frequency", choices=("event", "weekly", "monthly"), default="event")
    parser.add_argument("--slippage-bps", type=float, default=5.0)
    parser.add_argument("--fee-bps", type=float, default=0.0)
    parser.add_argument("--allow-raw-prices", action="store_true")
    parser.add_argument("--min-lots", type=int, default=20)
    parser.add_argument("--provider", default="fmp")
    parser.add_argument("--top", type=int, default=20)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    end_date = parse_iso_date(args.end_date) or date.today()
    start_date = parse_iso_date(args.start_date) if args.start_date else None
    exclude_symbols = _parse_exclude_symbols(args.exclude_symbols)
    sources = _parse_csv(args.sources, DEFAULT_SOURCES)  # type: ignore[assignment]
    rules = _parse_csv(args.rules, DEFAULT_RULES)  # type: ignore[assignment]
    insider_roles = _parse_csv(args.insider_roles, DEFAULT_INSIDER_ROLES)  # type: ignore[assignment]
    benchmark = normalize_symbol(args.benchmark) or "SPY"

    with SessionLocal() as db:
        if args.universe_source == "fundamentals_snapshots":
            source_kind = None if args.snapshot_source_kind == "all" else args.snapshot_source_kind
            universe = load_fundamentals_snapshot_universe(
                db,
                end_date=end_date,
                provider=args.provider,
                source_kind=source_kind,
                min_snapshots=max(int(args.min_snapshots_per_symbol), 1),
                exclude_symbols=exclude_symbols,
            )
        elif args.universe_source == "normalized_insider_purchases":
            universe = load_normalized_purchase_universe(
                db,
                start_date=start_date or date(1990, 1, 1),
                end_date=end_date,
                exclude_symbols=set(exclude_symbols),
            )
        else:
            universe = _normalize_universe(part.strip() for part in (args.symbols or "").split(","))

        result = run_sweep(
            db,
            universe=universe,
            universe_source=args.universe_source,
            start_date=start_date,
            end_date=end_date,
            sources=sources,  # type: ignore[arg-type]
            rules=rules,  # type: ignore[arg-type]
            insider_roles=insider_roles,  # type: ignore[arg-type]
            hold_days=_parse_holds(args.hold_days),
            weighting=args.weighting,
            rebalance_frequency=args.rebalance_frequency,
            benchmark=benchmark,
            slippage_bps=float(args.slippage_bps),
            fee_bps=float(args.fee_bps),
            require_adjusted=not args.allow_raw_prices,
            min_lots=int(args.min_lots),
            provider=args.provider,
        )

    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        _print_text_report(result, top=int(args.top))


if __name__ == "__main__":
    main()
