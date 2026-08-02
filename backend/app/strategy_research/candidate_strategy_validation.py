from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

from sqlalchemy.orm import Session

from app.db import SessionLocal
from app.strategy_research.candidate_strategy_diagnostics import (
    _load_universe,
    run_cross_source_diagnostics,
    run_technical_diagnostics,
)
from app.strategy_research.congress_buys import ResearchConfig, parse_iso_date
from app.strategy_research.cross_source_alignment import AlignmentPair
from app.strategy_research.insider_buys import InsiderRole
from app.strategy_research.technical_confirmation import PrimarySource, TechnicalRule
from app.utils.symbols import normalize_symbol

METHODOLOGY_VERSION = "candidate_strategy_validation_v1"


@dataclass(frozen=True)
class CandidateDefinition:
    slug: str
    name: str
    strategy_kind: str
    universe_source: str
    hold_days: int
    source: PrimarySource = "congress"
    rule: TechnicalRule = "technical_alignment"
    insider_role: InsiderRole = "all"
    pair: AlignmentPair = "congress_insider"
    lookback_days: int = 90
    min_confirming_signals: int = 1
    min_contract_amount: float = 1_000_000.0
    exclude_symbols: tuple[str, ...] = ()


DEFAULT_CANDIDATES: tuple[CandidateDefinition, ...] = (
    CandidateDefinition(
        slug="congress-macd-bullish-90d",
        name="Congress + MACD Bullish",
        strategy_kind="technical",
        universe_source="fundamentals_snapshots",
        source="congress",
        rule="macd_bullish",
        hold_days=90,
    ),
    CandidateDefinition(
        slug="congress-insider-confirmation-90d",
        name="Congress + Insider Confirmation",
        strategy_kind="cross_source",
        universe_source="fundamentals_snapshots",
        pair="congress_insider",
        hold_days=90,
        lookback_days=90,
    ),
    CandidateDefinition(
        slug="insider-technical-alignment-90d",
        name="Insider + Technical Alignment",
        strategy_kind="technical",
        universe_source="normalized_insider_purchases",
        source="insider",
        rule="technical_alignment",
        insider_role="all",
        hold_days=90,
    ),
    CandidateDefinition(
        slug="insider-sma-trend-180d",
        name="Insider + SMA50/SMA200 Trend",
        strategy_kind="technical",
        universe_source="normalized_insider_purchases",
        source="insider",
        rule="price_above_sma50_sma200",
        insider_role="all",
        hold_days=180,
    ),
)


def _clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, value))


def _number(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _component_scores(
    *,
    full: dict[str, Any],
    validation: dict[str, Any],
    test: dict[str, Any],
    diagnostics: dict[str, Any],
) -> dict[str, float]:
    test_alpha = _number(test.get("alpha_cagr_pct"))
    test_cagr = _number(test.get("cagr_pct"))
    test_sharpe = _number(test.get("sharpe"))
    test_drawdown = abs(_number(test.get("max_drawdown_pct")))
    test_rolling = _number(test.get("rolling_12m_beating_spy_pct"), 50.0)
    validation_alpha = _number(validation.get("alpha_cagr_pct"))
    full_alpha = _number(full.get("alpha_cagr_pct"))
    lots = int(test.get("lots") or 0)

    return {
        "out_of_sample_cagr": _clamp(test_cagr / 30.0 * 100.0),
        "out_of_sample_alpha": _clamp((test_alpha + 10.0) / 25.0 * 100.0),
        "risk_adjusted_return": _clamp(test_sharpe / 1.5 * 100.0),
        "drawdown_control": _clamp((35.0 - test_drawdown) / 35.0 * 100.0),
        "rolling_consistency": _clamp(test_rolling),
        "sample_size": _clamp(lots / 200.0 * 100.0),
        "validation_alpha": _clamp((validation_alpha + 10.0) / 25.0 * 100.0),
        "full_history_alpha": _clamp((full_alpha + 10.0) / 25.0 * 100.0),
        "concentration_quality": 100.0 if not diagnostics.get("concentration_flags") else 35.0,
    }


def walnut_strategy_score(
    *,
    full: dict[str, Any],
    validation: dict[str, Any],
    test: dict[str, Any],
    diagnostics: dict[str, Any],
) -> dict[str, Any]:
    components = _component_scores(full=full, validation=validation, test=test, diagnostics=diagnostics)
    weights = {
        "out_of_sample_cagr": 0.18,
        "out_of_sample_alpha": 0.18,
        "risk_adjusted_return": 0.14,
        "drawdown_control": 0.10,
        "rolling_consistency": 0.10,
        "sample_size": 0.10,
        "validation_alpha": 0.08,
        "full_history_alpha": 0.04,
        "concentration_quality": 0.08,
    }
    raw_score = sum(components[key] * weight for key, weight in weights.items())
    penalties: list[dict[str, Any]] = []

    test_status = test.get("status")
    test_lots = int(test.get("lots") or 0)
    validation_lots = int(validation.get("lots") or 0)
    if test_status != "ok":
        penalties.append({"reason": f"test_status_{test_status}", "points": 35})
    if test_lots < 100:
        penalties.append({"reason": "test_sample_below_100_lots", "points": 15})
    if validation_lots < 100:
        penalties.append({"reason": "validation_sample_below_100_lots", "points": 10})
    if _number(test.get("alpha_cagr_pct")) < 0:
        penalties.append({"reason": "negative_test_alpha", "points": 20})
    if _number(validation.get("alpha_cagr_pct")) < 0:
        penalties.append({"reason": "negative_validation_alpha", "points": 10})
    if diagnostics.get("concentration_flags"):
        penalties.append({"reason": "concentration_flags", "points": 20, "flags": diagnostics.get("concentration_flags")})
    if abs(_number(test.get("max_drawdown_pct"))) > 35:
        penalties.append({"reason": "test_drawdown_over_35pct", "points": 10})

    final_score = _clamp(raw_score - sum(float(item["points"]) for item in penalties))
    return {
        "score": round(final_score, 2),
        "raw_score": round(raw_score, 2),
        "components": {key: round(value, 2) for key, value in components.items()},
        "penalties": penalties,
    }


def _run_candidate_period(
    db: Session,
    candidate: CandidateDefinition,
    *,
    universe: tuple[str, ...],
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
) -> dict[str, Any]:
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
        return run_technical_diagnostics(
            db,
            config,
            source=candidate.source,
            rule=candidate.rule,
            insider_role=candidate.insider_role,
            limit=top,
        )
    return run_cross_source_diagnostics(
        db,
        config,
        pair=candidate.pair,
        lookback_days=candidate.lookback_days,
        min_confirming_signals=candidate.min_confirming_signals,
        min_contract_amount=candidate.min_contract_amount,
        limit=top,
    )


def validate_candidate(
    db: Session,
    candidate: CandidateDefinition,
    *,
    train_start: date,
    train_end: date,
    validation_start: date,
    validation_end: date,
    test_start: date,
    test_end: date,
    benchmark: str,
    slippage_bps: float,
    fee_bps: float,
    require_adjusted: bool,
    min_lots: int,
    snapshot_source_kind: str | None,
    min_snapshots_per_symbol: int,
    top: int,
) -> dict[str, Any]:
    period_kwargs = {
        "universe": _load_universe(
            db,
            universe_source=candidate.universe_source,
            symbols=None,
            start_date=None,
            end_date=test_end,
            exclude_symbols=candidate.exclude_symbols,
            snapshot_source_kind=snapshot_source_kind,
            min_snapshots_per_symbol=min_snapshots_per_symbol,
        ),
        "benchmark": benchmark,
        "slippage_bps": slippage_bps,
        "fee_bps": fee_bps,
        "require_adjusted": require_adjusted,
        "min_lots": min_lots,
        "snapshot_source_kind": snapshot_source_kind,
        "min_snapshots_per_symbol": min_snapshots_per_symbol,
        "top": top,
    }
    full = _run_candidate_period(db, candidate, start_date=None, end_date=test_end, **period_kwargs)
    train = _run_candidate_period(db, candidate, start_date=train_start, end_date=train_end, **period_kwargs)
    validation = _run_candidate_period(db, candidate, start_date=validation_start, end_date=validation_end, **period_kwargs)
    test = _run_candidate_period(db, candidate, start_date=test_start, end_date=test_end, **period_kwargs)
    score = walnut_strategy_score(
        full=full["performance"],
        validation=validation["performance"],
        test=test["performance"],
        diagnostics=full["diagnostics"],
    )
    return {
        "slug": candidate.slug,
        "name": candidate.name,
        "definition": {
            "strategy_kind": candidate.strategy_kind,
            "universe_source": candidate.universe_source,
            "source": candidate.source,
            "rule": candidate.rule,
            "insider_role": candidate.insider_role,
            "pair": candidate.pair,
            "lookback_days": candidate.lookback_days,
            "hold_days": candidate.hold_days,
            "exclude_symbols": list(candidate.exclude_symbols),
            "universe_size": len(period_kwargs["universe"]),
            "universe_basis": "candidate universe loaded once as of final test_end and reused across splits",
        },
        "periods": {
            "full": {"performance": full["performance"], "diagnostics": full["diagnostics"]},
            "train": {"performance": train["performance"], "diagnostics": train["diagnostics"]},
            "validation": {"performance": validation["performance"], "diagnostics": validation["diagnostics"]},
            "test": {"performance": test["performance"], "diagnostics": test["diagnostics"]},
        },
        "walnut_strategy_score": score,
    }


def _candidate_lookup() -> dict[str, CandidateDefinition]:
    return {candidate.slug: candidate for candidate in DEFAULT_CANDIDATES}


def _parse_candidate_slugs(value: str | None) -> tuple[str, ...]:
    if not value:
        return tuple(candidate.slug for candidate in DEFAULT_CANDIDATES)
    return tuple(part.strip() for part in value.split(",") if part.strip())


def _print_text_report(result: dict[str, Any]) -> None:
    meta = result["metadata"]
    print(
        "VALIDATION "
        f"methodology={meta['methodology_version']} benchmark={meta['benchmark']} "
        f"train={meta['train_start']}..{meta['train_end']} "
        f"validation={meta['validation_start']}..{meta['validation_end']} "
        f"test={meta['test_start']}..{meta['test_end']} candidates={len(result['rows'])}"
    )
    for row in result["leaderboard"]:
        score = row["walnut_strategy_score"]
        test = row["periods"]["test"]["performance"]
        validation = row["periods"]["validation"]["performance"]
        full = row["periods"]["full"]["performance"]
        full_diag = row["periods"]["full"]["diagnostics"]
        print(
            "ROW "
            f"slug={row['slug']} score={score['score']} raw={score['raw_score']} "
            f"test_status={test.get('status')} test_lots={test.get('lots')} "
            f"test_cagr={test.get('cagr_pct')}% test_spy={test.get('benchmark_cagr_pct')}% "
            f"test_alpha={test.get('alpha_cagr_pct')}% test_sharpe={test.get('sharpe')} "
            f"val_lots={validation.get('lots')} val_alpha={validation.get('alpha_cagr_pct')}% "
            f"full_cagr={full.get('cagr_pct')}% full_alpha={full.get('alpha_cagr_pct')}% "
            f"flags={','.join(full_diag.get('concentration_flags') or []) or 'none'} "
            f"penalties={json.dumps(score['penalties'], sort_keys=True)}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Read-only train/validation/test scoring for candidate strategies.")
    parser.add_argument("--candidates", help="Comma-separated candidate slugs. Defaults to all built-in candidates.")
    parser.add_argument("--train-start", default="2014-01-01")
    parser.add_argument("--train-end", default="2019-12-31")
    parser.add_argument("--validation-start", default="2020-01-01")
    parser.add_argument("--validation-end", default="2023-12-31")
    parser.add_argument("--test-start", default="2024-01-01")
    parser.add_argument("--test-end", required=True)
    parser.add_argument("--benchmark", default="SPY")
    parser.add_argument("--slippage-bps", type=float, default=5.0)
    parser.add_argument("--fee-bps", type=float, default=0.0)
    parser.add_argument("--allow-raw-prices", action="store_true")
    parser.add_argument("--min-lots", type=int, default=50)
    parser.add_argument("--snapshot-source-kind", default="ticker_financials_cache_statement_proxy")
    parser.add_argument("--min-snapshots-per-symbol", type=int, default=1)
    parser.add_argument("--top", type=int, default=5)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    train_start = parse_iso_date(args.train_start) or date(2014, 1, 1)
    train_end = parse_iso_date(args.train_end) or date(2019, 12, 31)
    validation_start = parse_iso_date(args.validation_start) or date(2020, 1, 1)
    validation_end = parse_iso_date(args.validation_end) or date(2023, 12, 31)
    test_start = parse_iso_date(args.test_start) or date(2024, 1, 1)
    test_end = parse_iso_date(args.test_end) or date.today()
    benchmark = normalize_symbol(args.benchmark) or "SPY"
    lookup = _candidate_lookup()
    rows: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    with SessionLocal() as db:
        for slug in _parse_candidate_slugs(args.candidates):
            candidate = lookup.get(slug)
            if not candidate:
                errors.append({"slug": slug, "error": "unknown_candidate"})
                continue
            try:
                rows.append(
                    validate_candidate(
                        db,
                        candidate,
                        train_start=train_start,
                        train_end=train_end,
                        validation_start=validation_start,
                        validation_end=validation_end,
                        test_start=test_start,
                        test_end=test_end,
                        benchmark=benchmark,
                        slippage_bps=float(args.slippage_bps),
                        fee_bps=float(args.fee_bps),
                        require_adjusted=not args.allow_raw_prices,
                        min_lots=int(args.min_lots),
                        snapshot_source_kind=args.snapshot_source_kind if args.snapshot_source_kind else None,
                        min_snapshots_per_symbol=int(args.min_snapshots_per_symbol),
                        top=int(args.top),
                    )
                )
            except Exception as exc:  # pragma: no cover - production diagnostics path
                errors.append({"slug": slug, "error": f"{type(exc).__name__}: {exc}"})

    leaderboard = sorted(rows, key=lambda row: row["walnut_strategy_score"]["score"], reverse=True)
    result = {
        "metadata": {
            "methodology_version": METHODOLOGY_VERSION,
            "run_timestamp": datetime.now(timezone.utc).isoformat(),
            "benchmark": benchmark,
            "train_start": train_start.isoformat(),
            "train_end": train_end.isoformat(),
            "validation_start": validation_start.isoformat(),
            "validation_end": validation_end.isoformat(),
            "test_start": test_start.isoformat(),
            "test_end": test_end.isoformat(),
            "slippage_bps_per_side": float(args.slippage_bps),
            "fee_bps_per_side": float(args.fee_bps),
            "require_adjusted_prices": not args.allow_raw_prices,
            "min_lots": int(args.min_lots),
            "data_state": "production PostgreSQL read-only research query",
        },
        "rows": rows,
        "leaderboard": leaderboard,
        "errors": errors,
    }
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        _print_text_report(result)
        if errors:
            print(f"ERRORS {json.dumps(errors, sort_keys=True)}")


if __name__ == "__main__":
    main()
