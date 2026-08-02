from __future__ import annotations

import argparse
import json
from bisect import bisect_right
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timezone
from time import perf_counter
from typing import Any, Iterable, Literal

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.db import SessionLocal
from app.models import FundamentalsSnapshot
from app.services.fundamentals_snapshots import latest_fundamentals_snapshot_on_or_before
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
    InsiderRole,
    load_insider_open_market_purchase_signals,
    load_normalized_purchase_universe,
)
from app.utils.symbols import normalize_symbol

MethodologyVersion = Literal["fundamental_confirmation_research_v1"]
PrimarySource = Literal["congress", "insider"]
FundamentalRule = Literal[
    "quality_growth",
    "reasonable_growth_value",
    "garp",
    "strong_profitability",
    "low_leverage",
    "dividend_growth_proxy",
    "growth_margin_proxy",
    "cash_flow_growth_proxy",
    "eps_revenue_growth_proxy",
]
FundamentalDataMode = Literal["snapshots", "current_cache_proxy"]

METHODOLOGY_VERSION: MethodologyVersion = "fundamental_confirmation_research_v1"


@dataclass(frozen=True)
class FundamentalState:
    status: str
    revenue_growth: float | None = None
    eps_growth: float | None = None
    roe: float | None = None
    roic: float | None = None
    gross_margin: float | None = None
    operating_margin: float | None = None
    operating_margin_expansion: float | None = None
    fcf_yield: float | None = None
    free_cash_flow: float | None = None
    fcf_growth: float | None = None
    dividend_yield: float | None = None
    forward_pe: float | None = None
    trailing_pe: float | None = None
    debt_to_equity: float | None = None
    net_debt_to_ebitda: float | None = None
    market_cap: float | None = None
    snapshot_date: date | None = None
    methodology_version: str | None = None


class FundamentalsSnapshotLookup:
    def __init__(
        self,
        rows_by_symbol: dict[str, list[FundamentalsSnapshot]],
        *,
        data_mode: FundamentalDataMode,
    ) -> None:
        self.data_mode = data_mode
        self._rows_by_symbol = rows_by_symbol
        self._dates_by_symbol = {
            symbol: [row.snapshot_date for row in rows]
            for symbol, rows in rows_by_symbol.items()
        }

    @classmethod
    def load(
        cls,
        db: Session,
        *,
        symbols: Iterable[str],
        max_as_of: date,
        provider: str,
        data_mode: FundamentalDataMode,
    ) -> "FundamentalsSnapshotLookup":
        normalized_symbols = sorted({symbol for symbol in _normalize_universe(symbols)})
        if not normalized_symbols:
            return cls({}, data_mode=data_mode)

        query = (
            select(FundamentalsSnapshot)
            .where(func.upper(FundamentalsSnapshot.symbol).in_(normalized_symbols))
            .where(FundamentalsSnapshot.provider == provider)
            .where(FundamentalsSnapshot.status == "ok")
            .order_by(
                FundamentalsSnapshot.symbol.asc(),
                FundamentalsSnapshot.snapshot_date.asc(),
                FundamentalsSnapshot.observed_at.asc(),
            )
        )
        if data_mode == "snapshots":
            query = query.where(FundamentalsSnapshot.snapshot_date <= max_as_of)

        rows_by_symbol: dict[str, list[FundamentalsSnapshot]] = defaultdict(list)
        for row in db.execute(query).scalars().all():
            symbol = normalize_symbol(row.symbol)
            if symbol:
                rows_by_symbol[symbol].append(row)
        fallback_observed_at = datetime.min.replace(tzinfo=timezone.utc)
        for rows in rows_by_symbol.values():
            rows.sort(key=lambda row: (row.snapshot_date, row.observed_at or fallback_observed_at))
        return cls(dict(rows_by_symbol), data_mode=data_mode)

    def latest_on_or_before(self, symbol: str, *, as_of: date) -> FundamentalsSnapshot | None:
        normalized = normalize_symbol(symbol)
        if not normalized:
            return None
        rows = self._rows_by_symbol.get(normalized) or []
        if not rows:
            return None
        if self.data_mode == "current_cache_proxy":
            return rows[-1]
        dates = self._dates_by_symbol.get(normalized) or []
        index = bisect_right(dates, as_of) - 1
        if index < 0:
            return None
        return rows[index]


def _number(value: Any) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed


def _positive(value: float | None) -> bool:
    return value is not None and value > 0


def _less_or_missing(value: float | None, threshold: float) -> bool:
    return value is None or value <= threshold


def _at_or_below(value: float | None, threshold: float) -> bool:
    return value is not None and value <= threshold


def _percent_like(value: float | None) -> float | None:
    if value is None:
        return None
    if -1.5 <= value <= 1.5:
        return value * 100.0
    return value


def _percent_like_at_least(value: float | None, threshold: float) -> bool:
    parsed = _percent_like(value)
    return parsed is not None and parsed >= threshold


def _pe(state: FundamentalState) -> float | None:
    return state.forward_pe if state.forward_pe is not None and state.forward_pe > 0 else state.trailing_pe


def fundamental_state_from_snapshot(snapshot: FundamentalsSnapshot | None) -> FundamentalState:
    if snapshot is None:
        return FundamentalState(status="missing_snapshot")
    return FundamentalState(
        status="ok",
        revenue_growth=_number(snapshot.revenue_growth),
        eps_growth=_number(snapshot.eps_growth),
        roe=_number(snapshot.roe),
        roic=_number(snapshot.roic),
        gross_margin=_number(snapshot.gross_margin),
        operating_margin=_number(snapshot.operating_margin),
        operating_margin_expansion=_number(snapshot.operating_margin_expansion),
        fcf_yield=_number(snapshot.fcf_yield),
        free_cash_flow=_number(snapshot.free_cash_flow),
        fcf_growth=_number(snapshot.fcf_growth),
        dividend_yield=_number(snapshot.dividend_yield),
        forward_pe=_number(snapshot.forward_pe),
        trailing_pe=_number(snapshot.trailing_pe),
        debt_to_equity=_number(snapshot.debt_to_equity),
        net_debt_to_ebitda=_number(snapshot.net_debt_to_ebitda),
        market_cap=_number(snapshot.market_cap),
        snapshot_date=snapshot.snapshot_date,
        methodology_version=snapshot.methodology_version,
    )


def fundamental_rule_matches(state: FundamentalState, rule: FundamentalRule) -> bool:
    if state.status != "ok":
        return False
    if rule == "quality_growth":
        return (
            (state.revenue_growth is not None and state.revenue_growth >= 8.0)
            and (state.roe is not None and state.roe >= 12.0)
            and (_positive(state.free_cash_flow) or _positive(state.fcf_yield) or _positive(state.fcf_growth))
            and _less_or_missing(state.net_debt_to_ebitda, 4.0)
        )
    if rule == "reasonable_growth_value":
        pe = _pe(state)
        return (
            (state.revenue_growth is not None and state.revenue_growth >= 8.0)
            and pe is not None
            and 0.0 < pe <= 35.0
            and _less_or_missing(state.net_debt_to_ebitda, 4.0)
        )
    if rule == "garp":
        pe = _pe(state)
        growth = max(value for value in (state.revenue_growth, state.eps_growth) if value is not None) if (
            state.revenue_growth is not None or state.eps_growth is not None
        ) else None
        return growth is not None and growth >= 8.0 and pe is not None and 0.0 < pe <= max(growth * 2.0, 20.0)
    if rule == "strong_profitability":
        return (
            ((state.roe is not None and state.roe >= 15.0) or (state.roic is not None and state.roic >= 12.0))
            and (state.gross_margin is None or _percent_like_at_least(state.gross_margin, 25.0))
            and (state.operating_margin is None or _percent_like_at_least(state.operating_margin, 10.0))
        )
    if rule == "low_leverage":
        return _at_or_below(state.net_debt_to_ebitda, 2.5) or _at_or_below(state.debt_to_equity, 2.0)
    if rule == "dividend_growth_proxy":
        return (
            state.dividend_yield is not None
            and state.dividend_yield >= 1.0
            and ((state.fcf_growth is not None and state.fcf_growth > 0) or (state.eps_growth is not None and state.eps_growth > 0))
            and _less_or_missing(state.net_debt_to_ebitda, 3.5)
        )
    if rule == "growth_margin_proxy":
        return (
            ((state.revenue_growth is not None and state.revenue_growth >= 8.0) or (state.eps_growth is not None and state.eps_growth >= 8.0))
            and (
                state.gross_margin is None
                or _percent_like_at_least(state.gross_margin, 25.0)
                or _percent_like_at_least(state.operating_margin, 8.0)
            )
        )
    if rule == "cash_flow_growth_proxy":
        return (
            (state.revenue_growth is not None and state.revenue_growth >= 5.0)
            and _positive(state.free_cash_flow)
            and (state.fcf_growth is None or state.fcf_growth >= 0.0)
        )
    if rule == "eps_revenue_growth_proxy":
        return (
            (state.revenue_growth is not None and state.revenue_growth >= 8.0)
            and (state.eps_growth is not None and state.eps_growth >= 8.0)
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
    raise ValueError(f"Unsupported primary source: {source}")


def latest_snapshot_on_or_before(
    db: Session,
    symbol: str,
    *,
    as_of: date,
    provider: str,
    data_mode: FundamentalDataMode,
) -> FundamentalsSnapshot | None:
    if data_mode == "snapshots":
        return latest_fundamentals_snapshot_on_or_before(db, symbol, as_of=as_of, provider=provider)
    normalized = normalize_symbol(symbol)
    if not normalized:
        return None
    return (
        db.execute(
            select(FundamentalsSnapshot)
            .where(FundamentalsSnapshot.symbol == normalized)
            .where(FundamentalsSnapshot.provider == provider)
            .where(FundamentalsSnapshot.status == "ok")
            .order_by(FundamentalsSnapshot.snapshot_date.desc(), FundamentalsSnapshot.observed_at.desc())
            .limit(1)
        )
        .scalars()
        .first()
    )


def filter_signals_by_fundamental_rule(
    db: Session,
    signals: list[Signal],
    *,
    rule: FundamentalRule,
    provider: str = "fmp",
    data_mode: FundamentalDataMode = "snapshots",
    snapshot_lookup: FundamentalsSnapshotLookup | None = None,
) -> tuple[list[Signal], dict[str, int]]:
    lookup = snapshot_lookup or FundamentalsSnapshotLookup.load(
        db,
        symbols=(signal.symbol for signal in signals),
        max_as_of=max((signal.disclosure_date for signal in signals), default=date.min),
        provider=provider,
        data_mode=data_mode,
    )
    filtered: list[Signal] = []
    skipped: dict[str, int] = {"missing_snapshot": 0, "rule_not_matched": 0}
    for signal in signals:
        snapshot = lookup.latest_on_or_before(signal.symbol, as_of=signal.disclosure_date)
        state = fundamental_state_from_snapshot(snapshot)
        if state.status != "ok":
            skipped[state.status] = skipped.get(state.status, 0) + 1
            continue
        if not fundamental_rule_matches(state, rule):
            skipped["rule_not_matched"] += 1
            continue
        filtered.append(signal)
    return filtered, {key: value for key, value in sorted(skipped.items()) if value}


def snapshot_provenance_summary(
    db: Session,
    signals: list[Signal],
    *,
    provider: str = "fmp",
    data_mode: FundamentalDataMode = "snapshots",
    snapshot_lookup: FundamentalsSnapshotLookup | None = None,
) -> dict[str, Any]:
    lookup = snapshot_lookup or FundamentalsSnapshotLookup.load(
        db,
        symbols=(signal.symbol for signal in signals),
        max_as_of=max((signal.disclosure_date for signal in signals), default=date.min),
        provider=provider,
        data_mode=data_mode,
    )
    source_kind_counts: dict[str, int] = {}
    confidence_counts: dict[str, int] = {}
    methodology_counts: dict[str, int] = {}
    for signal in signals:
        snapshot = lookup.latest_on_or_before(signal.symbol, as_of=signal.disclosure_date)
        if snapshot is None:
            continue
        source_kind = snapshot.source_kind or "unknown"
        confidence = snapshot.data_quality_confidence or "unknown"
        methodology = snapshot.methodology_version or "unknown"
        source_kind_counts[source_kind] = source_kind_counts.get(source_kind, 0) + 1
        confidence_counts[confidence] = confidence_counts.get(confidence, 0) + 1
        methodology_counts[methodology] = methodology_counts.get(methodology, 0) + 1
    return {
        "source_kind_counts": dict(sorted(source_kind_counts.items())),
        "data_quality_confidence_counts": dict(sorted(confidence_counts.items())),
        "methodology_version_counts": dict(sorted(methodology_counts.items())),
    }


def _overall_snapshot_confidence(provenance: dict[str, Any], data_mode: FundamentalDataMode) -> str:
    if data_mode != "snapshots":
        return "low"
    confidence_counts = provenance.get("data_quality_confidence_counts") or {}
    if not confidence_counts:
        return "unavailable"
    if any(key in confidence_counts for key in ("low", "current_cache_proxy")):
        return "low"
    if any("proxy" in str(key) or key == "medium" for key in confidence_counts):
        return "medium_proxy"
    if set(confidence_counts) <= {"high"}:
        return "high"
    return "mixed"


def run_research(
    db: Session,
    config: ResearchConfig,
    *,
    source: PrimarySource,
    rule: FundamentalRule,
    insider_role: InsiderRole,
    data_mode: FundamentalDataMode = "snapshots",
    provider: str = "fmp",
    collect_timings: bool = False,
) -> dict[str, Any]:
    started_at = perf_counter()
    timings: dict[str, float] = {}

    def mark(name: str, phase_start: float) -> None:
        if collect_timings:
            timings[name] = round(perf_counter() - phase_start, 4)

    price_start = config.start_date or date(1990, 1, 1)
    phase_start = perf_counter()
    price_maps = load_adjusted_price_histories(
        db,
        (*config.universe, config.benchmark),
        start_date=price_start,
        end_date=config.end_date,
        require_adjusted=config.require_adjusted,
    )
    mark("load_adjusted_price_histories_seconds", phase_start)
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
    phase_start = perf_counter()
    primary_signals = _load_primary_signals(
        db,
        source,
        universe=config.universe,
        start_date=signal_start,
        end_date=config.end_date,
        insider_role=insider_role,
    )
    mark("load_primary_signals_seconds", phase_start)
    phase_start = perf_counter()
    snapshot_lookup = FundamentalsSnapshotLookup.load(
        db,
        symbols={signal.symbol for signal in primary_signals},
        max_as_of=config.end_date,
        provider=provider,
        data_mode=data_mode,
    )
    mark("load_fundamental_snapshots_seconds", phase_start)
    phase_start = perf_counter()
    signals, fundamental_skips = filter_signals_by_fundamental_rule(
        db,
        primary_signals,
        rule=rule,
        provider=provider,
        data_mode=data_mode,
        snapshot_lookup=snapshot_lookup,
    )
    mark("filter_fundamentals_seconds", phase_start)
    phase_start = perf_counter()
    provenance = snapshot_provenance_summary(
        db,
        signals,
        provider=provider,
        data_mode=data_mode,
        snapshot_lookup=snapshot_lookup,
    )
    mark("snapshot_provenance_seconds", phase_start)
    per_side_cost_rate = max((config.slippage_bps + config.fee_bps) / 10000.0, 0.0)
    universe_price_maps = {symbol: prices for symbol, prices in price_maps.items() if symbol != config.benchmark}
    runs: list[dict[str, Any]] = []
    for hold_days in config.hold_days:
        hold_timings: dict[str, float] = {}
        phase_start = perf_counter()
        lots, skipped = build_lots(
            signals,
            universe_price_maps,
            benchmark_dates=benchmark_dates,
            hold_days=hold_days,
            rebalance_frequency=config.rebalance_frequency,
            per_side_cost_rate=per_side_cost_rate,
        )
        if collect_timings:
            hold_timings["build_lots_seconds"] = round(perf_counter() - phase_start, 4)
        phase_start = perf_counter()
        simulation = simulate_active_lot_portfolio(
            lots,
            universe_price_maps,
            benchmark_prices,
            weighting=config.weighting,
            per_side_cost_rate=per_side_cost_rate,
        )
        if collect_timings:
            hold_timings["simulate_portfolio_seconds"] = round(perf_counter() - phase_start, 4)
        phase_start = perf_counter()
        metrics = compute_metrics(lots=lots, simulation=simulation, hold_days=hold_days, skipped=skipped)
        if collect_timings:
            hold_timings["compute_metrics_seconds"] = round(perf_counter() - phase_start, 4)
        if metrics.get("status") == "ok" and metrics.get("lots", 0) < config.min_lots:
            metrics["status"] = "insufficient_lots"
        if collect_timings:
            metrics["timings"] = hold_timings
        runs.append(metrics)

    confidence = _overall_snapshot_confidence(provenance, data_mode)
    result = {
        "metadata": {
            "strategy_name": config.strategy_name,
            "plain_english_rule": (
                f"Buy eligible {source} purchase signals only when the {rule} fundamentals rule passes as of the "
                "public signal date; enter on the next trading day."
            ),
            "methodology_version": METHODOLOGY_VERSION,
            "base_engine_version": BASE_ENGINE_VERSION,
            "run_timestamp": datetime.now(timezone.utc).isoformat(),
            "source": source,
            "insider_role": insider_role if source == "insider" else None,
            "fundamental_rule": rule,
            "fundamental_data_mode": data_mode,
            "fundamental_provider": provider,
            "universe": list(config.universe),
            "benchmark": config.benchmark,
            "start_date": signal_start.isoformat(),
            "end_date": config.end_date.isoformat(),
            "weighting": config.weighting,
            "rebalance_frequency": config.rebalance_frequency,
            "execution_timing": "first trading day strictly after public disclosure date",
            "fundamentals_as_of": (
                "latest fundamentals_snapshots row on or before the signal disclosure date"
                if data_mode == "snapshots"
                else "latest available fundamentals snapshot regardless of signal date; research proxy only"
            ),
            "fees_bps_per_side": config.fee_bps,
            "slippage_bps_per_side": config.slippage_bps,
            "require_adjusted_prices": config.require_adjusted,
            "price_source": "price_cache.adjusted_close",
            "data_quality_confidence": confidence,
            "fundamental_snapshot_provenance": provenance,
            "data_state": "production PostgreSQL read-only research query",
            "warning": None
            if data_mode == "snapshots"
            else "current_cache_proxy is not point-in-time and must not be published as historical performance",
        },
        "primary_signal_count": len(primary_signals),
        "signal_count": len(signals),
        "filtered_out": fundamental_skips,
        "aligned_symbol_count": len({signal.symbol for signal in signals}),
        "runs": runs,
    }
    if collect_timings:
        timings["total_seconds"] = round(perf_counter() - started_at, 4)
        result["timings"] = timings
    return result


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


def _label(source: PrimarySource, rule: FundamentalRule, insider_role: InsiderRole) -> str:
    source_label = "Congress" if source == "congress" else f"Insider {insider_role.replace('_', ' ').title()}"
    return f"{source_label} + {rule.replace('_', ' ').title()}"


def _print_text_report(result: dict[str, Any]) -> None:
    meta = result["metadata"]
    print(
        "RUN "
        f"strategy={meta['strategy_name']} methodology={meta['methodology_version']} "
        f"source={meta['source']} rule={meta['fundamental_rule']} data_mode={meta['fundamental_data_mode']} "
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
    parser = argparse.ArgumentParser(description="Read-only fundamental confirmation strategy research runner.")
    parser.add_argument("--source", choices=("congress", "insider"), required=True)
    parser.add_argument(
        "--rule",
        choices=(
            "quality_growth",
            "reasonable_growth_value",
            "garp",
            "strong_profitability",
            "low_leverage",
            "dividend_growth_proxy",
            "growth_margin_proxy",
            "cash_flow_growth_proxy",
            "eps_revenue_growth_proxy",
        ),
        required=True,
    )
    parser.add_argument("--insider-role", choices=("all", "ceo", "cfo", "director", "officer", "ten_percent_owner"), default="all")
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
    parser.add_argument("--weighting", choices=("equal", "transaction_value"), default="equal")
    parser.add_argument("--rebalance-frequency", choices=("event", "weekly", "monthly"), default="event")
    parser.add_argument("--slippage-bps", type=float, default=5.0)
    parser.add_argument("--fee-bps", type=float, default=0.0)
    parser.add_argument("--allow-raw-prices", action="store_true")
    parser.add_argument("--min-lots", type=int, default=50)
    parser.add_argument("--fundamental-data-mode", choices=("snapshots", "current_cache_proxy"), default="snapshots")
    parser.add_argument("--provider", default="fmp")
    parser.add_argument("--timing", action="store_true", help="Include phase timing diagnostics in output.")
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
            data_mode=args.fundamental_data_mode,
            provider=args.provider,
            collect_timings=bool(args.timing),
        )
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        _print_text_report(result)


if __name__ == "__main__":
    main()
