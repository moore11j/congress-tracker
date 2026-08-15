from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from app.models import (
    StrategyBacktestRun,
    StrategyCurrentHolding,
    StrategyDefinition,
    StrategyEquityCurvePoint,
    StrategyHoldingRow,
    StrategyHoldingsSnapshot,
    StrategyPerformanceSnapshot,
)
from app.strategy_research.candidate_strategy_artifacts import CandidateStrategyArtifact
from app.strategy_research.candidate_strategy_validation import CandidateDefinition
from app.strategy_research.congress_buys import (
    Lot,
    _first_trading_day_on_or_after,
    _next_rebalance_on_or_after,
    _price_on_or_before,
)
from app.services.strategy_performance_metrics import trailing_snapshot_values

PERSISTENCE_METHODOLOGY_VERSION = "strategy_persistence_v1"


def _json_default(value: Any) -> str:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return str(value)


def json_dumps(value: Any) -> str:
    return json.dumps(value, default=_json_default, separators=(",", ":"), sort_keys=True)


def _parse_day(value: str | date | None) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(json_dumps(value).encode("utf-8")).hexdigest()


def strategy_category(candidate: CandidateDefinition) -> str:
    if candidate.strategy_kind == "cross_source":
        return "cross_source"
    if candidate.source == "insider":
        return "insider"
    if candidate.source == "institutional":
        return "institutional"
    return "congress"


def strategy_family(candidate: CandidateDefinition) -> str:
    if candidate.strategy_kind == "cross_source":
        return str(candidate.pair)
    if candidate.strategy_kind == "primary":
        if candidate.source == "institutional":
            return "accumulation"
        return "purchases"
    return str(candidate.rule)


def strategy_rule(candidate: CandidateDefinition) -> dict[str, Any]:
    return {
        "kind": candidate.strategy_kind,
        "source": candidate.source if candidate.strategy_kind in {"primary", "technical"} else None,
        "technical_rule": candidate.rule if candidate.strategy_kind == "technical" else None,
        "insider_role": candidate.insider_role if candidate.source == "insider" else None,
        "pair": candidate.pair if candidate.strategy_kind == "cross_source" else None,
        "lookback_days": candidate.lookback_days if candidate.strategy_kind == "cross_source" else None,
        "min_confirming_signals": candidate.min_confirming_signals if candidate.strategy_kind == "cross_source" else None,
        "min_contract_amount": candidate.min_contract_amount if candidate.strategy_kind == "cross_source" else None,
        "min_institutional_materiality": (
            candidate.min_institutional_materiality
            if candidate.source == "institutional" or "institutional" in str(candidate.pair)
            else None
        ),
        "holding_period_days": candidate.hold_days,
        "execution": "enter on the first trading day strictly after the public disclosure/proxy date",
    }


def strategy_parameters(candidate: CandidateDefinition, artifact: CandidateStrategyArtifact) -> dict[str, Any]:
    metadata = artifact.metadata
    return {
        "hold_days": candidate.hold_days,
        "weighting": metadata.get("weighting"),
        "rebalance_frequency": metadata.get("rebalance_frequency"),
        "benchmark": metadata.get("benchmark"),
        "slippage_bps_per_side": metadata.get("slippage_bps_per_side"),
        "fee_bps_per_side": metadata.get("fee_bps_per_side"),
        "require_adjusted_prices": metadata.get("require_adjusted_prices"),
        "min_lots": metadata.get("min_lots"),
        "candidate": strategy_rule(candidate),
    }


def strategy_universe(candidate: CandidateDefinition, artifact: CandidateStrategyArtifact) -> dict[str, Any]:
    return {
        "source": candidate.universe_source,
        "snapshot_source_kind": artifact.metadata.get("snapshot_source_kind"),
        "min_snapshots_per_symbol": artifact.metadata.get("min_snapshots_per_symbol"),
        "size": len(artifact.universe),
        "exclude_symbols": list(candidate.exclude_symbols),
        "basis": "candidate universe loaded as of run end date; not selected from future returns",
    }


def risk_notes(artifact: CandidateStrategyArtifact) -> list[str]:
    notes = list(artifact.diagnostics.get("concentration_flags") or [])
    if artifact.candidate.strategy_kind == "cross_source" and "contracts" in str(artifact.candidate.pair):
        notes.append("government_contract_award_date_publication_proxy")
    if artifact.candidate.source == "institutional" or "institutional" in str(artifact.candidate.pair):
        notes.append("institutional_13f_short_history")
        notes.append("institutional_activity_is_reported_holdings_not_live_trading")
    confidence = artifact.diagnostics.get("data_quality_confidence")
    if confidence == "low" and not notes:
        notes.append("low_data_quality_confidence")
    return notes


def strategy_definition_values(
    candidate: CandidateDefinition,
    artifact: CandidateStrategyArtifact,
    *,
    publish: bool,
) -> dict[str, Any]:
    diagnostics = artifact.diagnostics
    rule = strategy_rule(candidate)
    return {
        "slug": candidate.slug,
        "name": candidate.name,
        "category": strategy_category(candidate),
        "family": strategy_family(candidate),
        "status": "published" if publish else "draft",
        "access_tier": "premium",
        "is_featured": False,
        "sort_order": 100,
        "short_description": _short_description(candidate),
        "walnut_take": _walnut_take(candidate, diagnostics),
        "methodology": _methodology_text(candidate),
        "rule_json": json_dumps(rule),
        "parameters_json": json_dumps(strategy_parameters(candidate, artifact)),
        "universe_json": json_dumps(strategy_universe(candidate, artifact)),
        "tags_json": json_dumps(_tags(candidate)),
        "risk_notes_json": json_dumps(risk_notes(artifact)),
        "data_quality_confidence": _definition_confidence(candidate, diagnostics),
        "methodology_version": str(artifact.metadata.get("methodology_version") or PERSISTENCE_METHODOLOGY_VERSION),
        "created_by": "strategy_refresh_writer",
        "published_at": datetime.now(timezone.utc) if publish else None,
    }


def _short_description(candidate: CandidateDefinition) -> str:
    if candidate.strategy_kind == "cross_source":
        return "Stocks where Walnut finds agreement across separate disclosure sources."
    if candidate.strategy_kind == "primary" and candidate.source == "congress":
        return "Congress purchase disclosures copied after realistic public filing availability."
    if candidate.strategy_kind == "primary" and candidate.source == "insider":
        return "Qualifying open-market insider purchases copied after public Form 4 availability."
    if candidate.strategy_kind == "primary" and candidate.source == "institutional":
        return "Bullish reported 13F institutional activity copied after public filing availability."
    if candidate.source == "insider":
        return "Open-market insider purchases filtered through point-in-time technical confirmation."
    return "Congress purchase disclosures filtered through point-in-time technical confirmation."


def _definition_confidence(candidate: CandidateDefinition, diagnostics: dict[str, Any]) -> str:
    confidence = str(diagnostics.get("data_quality_confidence") or "unknown")
    if candidate.strategy_kind == "cross_source" and "contracts" in str(candidate.pair):
        return "low" if confidence in {"unknown", "medium"} else confidence
    if candidate.source == "institutional" or "institutional" in str(candidate.pair):
        return "low" if confidence in {"unknown", "medium"} else confidence
    return confidence


def _walnut_take(candidate: CandidateDefinition, diagnostics: dict[str, Any]) -> str:
    confidence = diagnostics.get("data_quality_confidence") or "unknown"
    return f"Research candidate with {confidence} current data-quality confidence; review diagnostics before publication."


def _methodology_text(candidate: CandidateDefinition) -> str:
    if candidate.strategy_kind == "cross_source":
        if "institutional" in str(candidate.pair):
            return (
                "Select qualifying primary-source purchase signals only when bullish reported 13F institutional activity "
                "is present inside the configured lookback window. Entries use the later public filing/disclosure date "
                "and adjusted prices; institutional activity is filing-date context, not live trading."
            )
        return (
            "Select qualifying primary-source purchase signals only when a confirming Walnut source is present inside "
            "the configured lookback window. Entries occur after the disclosure/proxy date using adjusted prices."
        )
    if candidate.strategy_kind == "primary":
        if candidate.source == "institutional":
            return (
                "Select bullish reported 13F institutional activity for the configured universe using the filing date, "
                "not the quarter-end holdings date or an assumed trade date. Entries occur on the next available trading "
                "day after public filing availability using adjusted prices."
            )
        if candidate.source == "insider":
            return (
                "Select qualifying open-market insider purchase disclosures for the configured universe. Entries occur "
                "on the next available trading day after public disclosure using adjusted prices."
            )
        return (
            "Select qualifying Congress purchase disclosures for the configured universe. Entries occur on the next "
            "available trading day after public disclosure using adjusted prices."
        )
    return (
        "Select qualifying purchase signals only when the configured technical rule is true using price data available "
        "on or before the disclosure date. Entries occur on the next available trading day using adjusted prices."
    )


def _tags(candidate: CandidateDefinition) -> list[str]:
    tags = [strategy_category(candidate), strategy_family(candidate), f"{candidate.hold_days}d"]
    if candidate.strategy_kind == "technical":
        tags.append(str(candidate.source))
    return tags


def run_key_for_artifact(
    artifact: CandidateStrategyArtifact,
    *,
    code_version: str | None,
    validation_result: dict[str, Any] | None,
) -> str:
    payload = {
        "slug": artifact.candidate.slug,
        "code_version": code_version,
        "methodology_version": artifact.metadata.get("methodology_version"),
        "start_date": artifact.performance.get("start_date"),
        "end_date": artifact.performance.get("end_date"),
        "parameters": strategy_parameters(artifact.candidate, artifact),
        "universe_hash": _sha256_json(list(artifact.universe)),
        "validation_score": (validation_result or {}).get("walnut_strategy_score", {}).get("score"),
    }
    return _sha256_json(payload)[:24]


def _upsert_definition(
    db: Session,
    candidate: CandidateDefinition,
    artifact: CandidateStrategyArtifact,
    *,
    publish: bool,
) -> StrategyDefinition:
    values = strategy_definition_values(candidate, artifact, publish=publish)
    strategy = db.execute(select(StrategyDefinition).where(StrategyDefinition.slug == candidate.slug)).scalars().first()
    if strategy is None:
        strategy = StrategyDefinition(**values)
        db.add(strategy)
        db.flush()
        return strategy

    preserve_status = strategy.status if not publish else values["status"]
    preserve_published_at = strategy.published_at if not publish else values["published_at"]
    for key, value in values.items():
        if key == "status":
            value = preserve_status
        elif key == "published_at":
            value = preserve_published_at
        setattr(strategy, key, value)
    db.flush()
    return strategy


def _max_snapshot_values(
    *,
    strategy_id: int,
    run_id: int,
    as_of_date: date,
    performance: dict[str, Any],
    walnut_score: float | None,
) -> dict[str, Any]:
    return {
        "strategy_id": strategy_id,
        "run_id": run_id,
        "as_of_date": as_of_date,
        "period": "max",
        "total_return_pct": performance.get("total_return_pct"),
        "cagr_pct": performance.get("cagr_pct"),
        "benchmark_return_pct": performance.get("benchmark_total_return_pct"),
        "benchmark_cagr_pct": performance.get("benchmark_cagr_pct"),
        "alpha_cagr_pct": performance.get("alpha_cagr_pct"),
        "beta": performance.get("beta"),
        "sharpe": performance.get("sharpe"),
        "sortino": performance.get("sortino"),
        "max_drawdown_pct": performance.get("max_drawdown_pct"),
        "annualized_volatility_pct": performance.get("annualized_volatility_pct"),
        "win_rate_pct": performance.get("win_rate_pct"),
        "trade_count": performance.get("trade_count"),
        "independent_signal_count": performance.get("independent_signals"),
        "avg_holdings": performance.get("avg_active_lots"),
        "turnover_events": performance.get("turnover_events"),
        "rolling_12m_beating_spy_pct": performance.get("rolling_12m_beating_spy_pct"),
        "walnut_strategy_score": walnut_score,
        "metrics_json": json_dumps(performance),
    }


def _period_return(points: list[dict[str, Any]], *, days: int, value_key: str) -> float | None:
    if len(points) < 2:
        return None
    end_day = _parse_day(points[-1].get("date"))
    if end_day is None:
        return None
    first_day = _parse_day(points[0].get("date"))
    if first_day is None or (end_day - first_day).days < days:
        return None
    target = end_day.toordinal() - days
    start_point = None
    for point in points:
        day = _parse_day(point.get("date"))
        if day is not None and day.toordinal() >= target:
            start_point = point
            break
    if not start_point:
        return None
    start = float(start_point.get(value_key) or 0.0)
    end = float(points[-1].get(value_key) or 0.0)
    if start <= 0:
        return None
    return round((end / start - 1.0) * 100.0, 4)


def _annualized_return(return_pct: float | None, days: int) -> float | None:
    if return_pct is None or days < 365:
        return None
    growth = 1.0 + float(return_pct) / 100.0
    if growth <= 0:
        return -100.0
    return round((growth ** (365.25 / float(days)) - 1.0) * 100.0, 4)


def _trailing_snapshots(
    *,
    strategy_id: int,
    run_id: int,
    as_of_date: date,
    timeline: list[dict[str, Any]],
    performance: dict[str, Any],
    walnut_score: float | None,
) -> list[StrategyPerformanceSnapshot]:
    return [
        StrategyPerformanceSnapshot(**{
            **values,
            "metrics_json": json_dumps(values["metrics_json"]),
        })
        for values in trailing_snapshot_values(
            strategy_id=strategy_id,
            run_id=run_id,
            as_of_date=as_of_date,
            points=timeline,
            baseline_metrics=performance,
            walnut_score=walnut_score,
        )
    ]


def _equity_points(
    *,
    strategy_id: int,
    run_id: int,
    timeline: list[dict[str, Any]],
) -> list[StrategyEquityCurvePoint]:
    points: list[StrategyEquityCurvePoint] = []
    peak = 0.0
    for point in timeline:
        strategy_value = float(point.get("strategy_value") or 0.0)
        peak = max(peak, strategy_value)
        drawdown = ((strategy_value / peak) - 1.0) * 100.0 if peak > 0 else None
        points.append(
            StrategyEquityCurvePoint(
                strategy_id=strategy_id,
                run_id=run_id,
                date=_parse_day(point.get("date")) or date.today(),
                strategy_value=strategy_value,
                benchmark_value=float(point.get("benchmark_value")) if point.get("benchmark_value") is not None else None,
                drawdown_pct=round(drawdown, 4) if drawdown is not None else None,
                active_holdings=int(point.get("active_lots") or 0),
            )
        )
    return points


def _lot_signal_payload(lot: Lot) -> dict[str, Any]:
    signal = lot.signal
    return {
        "event_id": signal.event_id,
        "symbol": signal.symbol,
        "disclosure_date": signal.disclosure_date.isoformat(),
        "entry_date": lot.entry_date.isoformat(),
        "exit_date": lot.exit_date.isoformat(),
        "amount_min": signal.amount_min,
        "amount_max": signal.amount_max,
        "actor": signal.member_bioguide_id or signal.member_name,
        "filing_id": signal.source_filing_id,
        "source_url": signal.source_document_url,
    }


def _open_lots_as_of(
    artifact: CandidateStrategyArtifact,
    *,
    as_of_date: date,
) -> list[Lot]:
    benchmark_dates = sorted(artifact.benchmark_prices)
    if not benchmark_dates:
        return []

    open_lots: list[Lot] = []
    seen: set[tuple[int, str, date]] = set()
    hold_days = int(artifact.candidate.hold_days)
    for signal in artifact.confirmed_signals:
        key = (int(signal.event_id), signal.symbol, signal.disclosure_date)
        if key in seen:
            continue
        seen.add(key)
        symbol_prices = artifact.price_maps.get(signal.symbol) or {}
        symbol_dates = sorted(symbol_prices)
        if not symbol_dates:
            continue
        first_entry = _first_trading_day_on_or_after(signal.raw_entry_date, symbol_dates)
        if first_entry is None:
            continue
        entry_date = _next_rebalance_on_or_after(first_entry, benchmark_dates, "event")
        if entry_date is None:
            continue
        entry_date = _first_trading_day_on_or_after(entry_date, symbol_dates) or entry_date
        exit_date = entry_date + timedelta(days=hold_days)
        if not (entry_date <= as_of_date < exit_date):
            continue
        entry_bar = symbol_prices.get(entry_date)
        last_price = _price_on_or_before(as_of_date, symbol_prices, symbol_dates)
        if entry_bar is None or last_price is None:
            continue
        gross_return = (last_price / entry_bar.close) - 1.0 if entry_bar.close > 0 else 0.0
        open_lots.append(
            Lot(
                signal=signal,
                entry_date=entry_date,
                exit_date=exit_date,
                entry_price=entry_bar.close,
                exit_price=last_price,
                gross_return=gross_return,
                net_return=gross_return,
            )
        )
    return open_lots


def _holding_rows(
    *,
    strategy_id: int,
    run_id: int,
    snapshot_id: int,
    as_of_date: date,
    artifact: CandidateStrategyArtifact,
) -> list[StrategyHoldingRow]:
    active = _open_lots_as_of(artifact, as_of_date=as_of_date)
    grouped: dict[str, list[Lot]] = defaultdict(list)
    for lot in active:
        grouped[lot.signal.symbol].append(lot)
    raw_weights = {symbol: float(len(lots)) for symbol, lots in grouped.items()}
    total = sum(raw_weights.values())
    rows: list[StrategyHoldingRow] = []
    for rank, (symbol, lots) in enumerate(sorted(grouped.items(), key=lambda item: (-raw_weights[item[0]], item[0])), start=1):
        symbol_prices = artifact.price_maps.get(symbol) or {}
        symbol_dates = sorted(symbol_prices)
        last_price = _price_on_or_before(as_of_date, symbol_prices, symbol_dates) if symbol_dates else None
        avg_entry = sum(float(lot.entry_price) for lot in lots) / len(lots) if lots else None
        return_pct = ((last_price / avg_entry) - 1.0) * 100.0 if last_price is not None and avg_entry and avg_entry > 0 else None
        rows.append(
            StrategyHoldingRow(
                strategy_id=strategy_id,
                snapshot_id=snapshot_id,
                run_id=run_id,
                symbol=symbol,
                rank=rank,
                weight_pct=round(raw_weights[symbol] / total * 100.0, 4) if total > 0 else None,
                entry_date=min(lot.entry_date for lot in lots) if lots else None,
                avg_entry_price=round(avg_entry, 6) if avg_entry is not None else None,
                last_price=round(last_price, 6) if last_price is not None else None,
                return_pct=round(return_pct, 4) if return_pct is not None else None,
                source_signal_count=len(lots),
                source_signals_json=json_dumps([_lot_signal_payload(lot) for lot in lots]),
                payload_json=json_dumps({"as_of_date": as_of_date.isoformat()}),
            )
        )
    return rows


def _current_holding_from_row(
    row: StrategyHoldingRow,
    *,
    as_of_date: date,
) -> StrategyCurrentHolding:
    return StrategyCurrentHolding(
        strategy_id=row.strategy_id,
        run_id=row.run_id,
        as_of_date=as_of_date,
        symbol=row.symbol,
        company_name=row.company_name,
        sector=row.sector,
        rank=row.rank,
        weight_pct=row.weight_pct,
        entry_date=row.entry_date,
        last_price=row.last_price,
        return_pct=row.return_pct,
        source_signal_count=row.source_signal_count,
        source_signals_json=row.source_signals_json,
        payload_json=row.payload_json,
    )


def _delete_run_children(db: Session, *, strategy_id: int, run_id: int) -> None:
    db.execute(delete(StrategyCurrentHolding).where(StrategyCurrentHolding.strategy_id == strategy_id))
    db.execute(delete(StrategyHoldingRow).where(StrategyHoldingRow.run_id == run_id))
    db.execute(delete(StrategyHoldingsSnapshot).where(StrategyHoldingsSnapshot.run_id == run_id))
    db.execute(delete(StrategyEquityCurvePoint).where(StrategyEquityCurvePoint.run_id == run_id))
    db.execute(delete(StrategyPerformanceSnapshot).where(StrategyPerformanceSnapshot.run_id == run_id))


def persist_candidate_strategy_artifact(
    db: Session,
    artifact: CandidateStrategyArtifact,
    *,
    validation_result: dict[str, Any] | None = None,
    code_version: str | None = None,
    publish: bool = False,
    apply: bool = False,
) -> dict[str, Any]:
    timeline = list(artifact.simulation.get("timeline") or [])
    as_of_date = _parse_day(artifact.performance.get("end_date")) or _parse_day(timeline[-1].get("date") if timeline else None)
    walnut_score = (validation_result or {}).get("walnut_strategy_score", {}).get("score")
    run_key = run_key_for_artifact(artifact, code_version=code_version, validation_result=validation_result)
    open_lots = _open_lots_as_of(artifact, as_of_date=as_of_date) if as_of_date else []
    open_symbols = {lot.signal.symbol for lot in open_lots}
    preview = {
        "slug": artifact.candidate.slug,
        "name": artifact.candidate.name,
        "apply": apply,
        "publish": publish,
        "run_key": run_key,
        "status": artifact.performance.get("status"),
        "as_of_date": as_of_date.isoformat() if as_of_date else None,
        "lots": len(artifact.lots),
        "equity_points": len(timeline),
        "current_holdings": len(open_symbols),
        "current_signal_lots": len(open_lots),
        "performance_snapshots": 5,
        "data_quality_confidence": artifact.diagnostics.get("data_quality_confidence"),
        "walnut_strategy_score": walnut_score,
    }
    if not apply:
        return {"mode": "dry_run", **preview}
    if as_of_date is None:
        raise ValueError("Cannot persist strategy artifact without an as_of_date.")

    strategy = _upsert_definition(db, artifact.candidate, artifact, publish=publish)
    run = (
        db.execute(
            select(StrategyBacktestRun)
            .where(StrategyBacktestRun.strategy_id == int(strategy.id))
            .where(StrategyBacktestRun.run_key == run_key)
        )
        .scalars()
        .first()
    )
    now = datetime.now(timezone.utc)
    run_values = {
        "strategy_id": int(strategy.id),
        "run_key": run_key,
        "run_type": "research_refresh",
        "status": "ok" if artifact.performance.get("status") == "ok" else str(artifact.performance.get("status") or "error"),
        "started_at": now,
        "completed_at": now,
        "backtest_start_date": _parse_day(artifact.performance.get("start_date")),
        "backtest_end_date": _parse_day(artifact.performance.get("end_date")),
        "benchmark": str(artifact.metadata.get("benchmark") or "SPY"),
        "methodology_version": str(artifact.metadata.get("methodology_version") or PERSISTENCE_METHODOLOGY_VERSION),
        "code_version": code_version,
        "dataset_versions_json": json_dumps({"price_source": "adjusted_price_cache", "signal_source": artifact.metadata.get("data_state")}),
        "parameters_json": json_dumps(strategy_parameters(artifact.candidate, artifact)),
        "universe_hash": _sha256_json(list(artifact.universe)),
        "universe_json": json_dumps(strategy_universe(artifact.candidate, artifact)),
        "execution_timing": str(artifact.metadata.get("execution_timing") or ""),
        "fee_bps_per_side": float(artifact.metadata.get("fee_bps_per_side") or 0.0),
        "slippage_bps_per_side": float(artifact.metadata.get("slippage_bps_per_side") or 0.0),
        "metrics_json": json_dumps(artifact.performance),
        "diagnostics_json": json_dumps(
            {
                "diagnostics": artifact.diagnostics,
                "filter_skips": artifact.filter_skips,
                "validation": validation_result,
            }
        ),
        "walnut_strategy_score": float(walnut_score) if walnut_score is not None else None,
        "data_quality_confidence": str(artifact.diagnostics.get("data_quality_confidence") or "unknown"),
        "error": None if artifact.performance.get("status") == "ok" else json_dumps(artifact.performance),
    }
    if run is None:
        run = StrategyBacktestRun(**run_values)
        db.add(run)
        db.flush()
        db.execute(delete(StrategyCurrentHolding).where(StrategyCurrentHolding.strategy_id == int(strategy.id)))
    else:
        for key, value in run_values.items():
            setattr(run, key, value)
        db.flush()
        _delete_run_children(db, strategy_id=int(strategy.id), run_id=int(run.id))

    db.add_all(_trailing_snapshots(
        strategy_id=int(strategy.id),
        run_id=int(run.id),
        as_of_date=as_of_date,
        timeline=timeline,
        performance=artifact.performance,
        walnut_score=float(walnut_score) if walnut_score is not None else None,
    ))
    db.add_all(_equity_points(strategy_id=int(strategy.id), run_id=int(run.id), timeline=timeline))
    holdings = _holding_rows(
        strategy_id=int(strategy.id),
        run_id=int(run.id),
        snapshot_id=0,
        as_of_date=as_of_date,
        artifact=artifact,
    )
    snapshot = StrategyHoldingsSnapshot(
        strategy_id=int(strategy.id),
        run_id=int(run.id),
        as_of_date=as_of_date,
        holdings_count=len(holdings),
        total_weight_pct=round(sum(float(row.weight_pct or 0.0) for row in holdings), 4),
        cash_weight_pct=round(max(0.0, 100.0 - sum(float(row.weight_pct or 0.0) for row in holdings)), 4),
        diagnostics_json=json_dumps({"source": "fixed_holding_period_active_lots"}),
    )
    db.add(snapshot)
    db.flush()
    for row in holdings:
        row.snapshot_id = int(snapshot.id)
    db.add_all(holdings)
    db.add_all([_current_holding_from_row(row, as_of_date=as_of_date) for row in holdings])
    db.commit()
    return {
        "mode": "apply",
        **preview,
        "strategy_id": int(strategy.id),
        "run_id": int(run.id),
        "holdings_snapshot_id": int(snapshot.id),
    }
