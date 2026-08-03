from __future__ import annotations

import hashlib
import json
import math
from collections import defaultdict
from datetime import date, datetime, timezone
from typing import Any

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from app.models import (
    Member,
    ReplicatedPortfolioPoint,
    ReplicatedPortfolioPosition,
    ReplicatedPortfolioRun,
    StrategyBacktestRun,
    StrategyCurrentHolding,
    StrategyDefinition,
    StrategyEquityCurvePoint,
    StrategyHoldingRow,
    StrategyHoldingsSnapshot,
    StrategyPerformanceSnapshot,
)
from app.services.replicated_portfolios import PORTFOLIO_METHODOLOGY_VERSION
from app.services.strategy_refresh import _delete_run_children, _parse_day, json_dumps
from app.utils.symbols import normalize_symbol

PERSISTENCE_METHODOLOGY_VERSION = "replicated_portfolio_strategy_v1"
DEFAULT_MODE = "realistic_disclosure_lag"


def _json_loads(value: str | None) -> dict[str, Any]:
    if not value:
        return {}
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _member_name(member: Member | None, fallback: str) -> str:
    if member is None:
        return fallback
    name = " ".join(part for part in [member.first_name, member.last_name] if part).strip()
    return name or fallback


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(json_dumps(value).encode("utf-8")).hexdigest()


def _annualized_return(total_return_pct: float | None, start_date: date | None, end_date: date | None) -> float | None:
    if total_return_pct is None or start_date is None or end_date is None:
        return None
    days = max((end_date - start_date).days, 0)
    if days < 365:
        return None
    growth = 1.0 + float(total_return_pct) / 100.0
    if growth <= 0:
        return -100.0
    return round((growth ** (365.25 / float(days)) - 1.0) * 100.0, 4)


def _annualized_return_for_days(total_return_pct: float | None, days: int) -> float | None:
    if total_return_pct is None or days < 365:
        return None
    growth = 1.0 + float(total_return_pct) / 100.0
    if growth <= 0:
        return -100.0
    return round((growth ** (365.25 / float(days)) - 1.0) * 100.0, 4)


def _period_return(points: list[ReplicatedPortfolioPoint], *, days: int, value_key: str) -> float | None:
    if len(points) < 2:
        return None
    end_day = points[-1].asof_date
    if (end_day - points[0].asof_date).days < days:
        return None
    target = end_day.toordinal() - days
    start_point = next((point for point in points if point.asof_date.toordinal() >= target), None)
    if start_point is None:
        return None
    start = getattr(start_point, value_key)
    end = getattr(points[-1], value_key)
    if start is None or end is None or float(start) <= 0:
        return None
    return round((float(end) / float(start) - 1.0) * 100.0, 4)


def _daily_benchmark_returns(points: list[ReplicatedPortfolioPoint]) -> list[float | None]:
    values: list[float | None] = [None]
    previous = points[0].benchmark_value if points else None
    for point in points[1:]:
        current = point.benchmark_value
        if current is None or previous is None or float(previous) <= 0:
            values.append(None)
        else:
            values.append((float(current) / float(previous)) - 1.0)
        previous = current
    return values


def _beta(points: list[ReplicatedPortfolioPoint]) -> float | None:
    benchmark_returns = _daily_benchmark_returns(points)
    pairs = [
        (float(point.daily_return_pct) / 100.0, bench)
        for point, bench in zip(points, benchmark_returns)
        if bench is not None
    ]
    if len(pairs) < 30:
        return None
    strategy_mean = sum(pair[0] for pair in pairs) / len(pairs)
    benchmark_mean = sum(pair[1] for pair in pairs) / len(pairs)
    covariance = sum((s - strategy_mean) * (b - benchmark_mean) for s, b in pairs) / len(pairs)
    variance = sum((b - benchmark_mean) ** 2 for _, b in pairs) / len(pairs)
    if variance <= 0:
        return None
    return round(covariance / variance, 4)


def _sortino(points: list[ReplicatedPortfolioPoint]) -> float | None:
    returns = [float(point.daily_return_pct) / 100.0 for point in points if point.daily_return_pct is not None]
    downside = [min(0.0, value) for value in returns]
    if len(returns) < 30 or not any(value < 0 for value in downside):
        return None
    mean_return = sum(returns) / len(returns)
    downside_deviation = math.sqrt(sum(value * value for value in downside) / len(downside))
    if downside_deviation <= 0:
        return None
    return round((mean_return / downside_deviation) * math.sqrt(252.0), 4)


def _curve_points(
    *,
    strategy_id: int,
    strategy_run_id: int,
    points: list[ReplicatedPortfolioPoint],
) -> list[StrategyEquityCurvePoint]:
    peak = 0.0
    rows: list[StrategyEquityCurvePoint] = []
    for point in points:
        strategy_value = float(point.strategy_value or 0.0)
        peak = max(peak, strategy_value)
        drawdown = ((strategy_value / peak) - 1.0) * 100.0 if peak > 0 else None
        rows.append(
            StrategyEquityCurvePoint(
                strategy_id=strategy_id,
                run_id=strategy_run_id,
                date=point.asof_date,
                strategy_value=strategy_value,
                benchmark_value=float(point.benchmark_value) if point.benchmark_value is not None else None,
                drawdown_pct=round(drawdown, 4) if drawdown is not None else None,
                active_holdings=int(point.active_positions or 0),
            )
        )
    return rows


def _score_run(run: ReplicatedPortfolioRun, *, status_payload: dict[str, Any]) -> float:
    curve_quality = status_payload.get("curve_diagnostics", {}).get("curve_quality_status")
    cagr = float(run.cagr_pct or 0.0)
    alpha = float(run.alpha_pct or 0.0)
    sharpe = float(run.sharpe_ratio or 0.0)
    drawdown = abs(float(run.max_drawdown_pct or 0.0))
    positions = int(run.positions_count or 0)
    exposure = float(run.average_exposure_pct or 0.0)
    score = 0.0
    score += max(0.0, min(35.0, cagr / 50.0 * 35.0))
    score += max(0.0, min(25.0, (alpha + 10.0) / 80.0 * 25.0))
    score += max(0.0, min(15.0, sharpe / 2.0 * 15.0))
    score += max(0.0, min(10.0, (50.0 - drawdown) / 50.0 * 10.0))
    score += max(0.0, min(10.0, positions / 75.0 * 10.0))
    score += max(0.0, min(5.0, exposure / 80.0 * 5.0))
    if curve_quality in {"poor", "warning"}:
        score -= 15.0
    if positions < 10:
        score -= 15.0
    if drawdown > 50.0:
        score -= 10.0
    return round(max(0.0, min(100.0, score)), 2)


def _latest_ranked_runs(
    db: Session,
    *,
    lookback_days: int,
    benchmark: str,
    min_positions: int,
    min_points: int,
) -> list[tuple[ReplicatedPortfolioRun, Member | None, float, dict[str, Any]]]:
    benchmark_symbol = normalize_symbol(benchmark) or "SPY"
    rows = (
        db.execute(
            select(ReplicatedPortfolioRun)
            .where(ReplicatedPortfolioRun.entity_type == "congress_member")
            .where(ReplicatedPortfolioRun.lookback_days == lookback_days)
            .where(ReplicatedPortfolioRun.mode == DEFAULT_MODE)
            .where(ReplicatedPortfolioRun.benchmark_symbol == benchmark_symbol)
            .where(ReplicatedPortfolioRun.issuer_cik.is_(None))
            .where(ReplicatedPortfolioRun.issuer_symbol.is_(None))
            .where(ReplicatedPortfolioRun.methodology_version == PORTFOLIO_METHODOLOGY_VERSION)
            .where(ReplicatedPortfolioRun.status == "ok")
        )
        .scalars()
        .all()
    )
    latest: dict[str, ReplicatedPortfolioRun] = {}
    for run in rows:
        entity_id = (run.entity_id or "").strip()
        if not entity_id or entity_id.upper().startswith("FMP_"):
            continue
        current = latest.get(entity_id)
        if current is None or (run.computed_at, run.id) > (current.computed_at, current.id):
            latest[entity_id] = run

    members = {
        member.bioguide_id: member
        for member in db.execute(select(Member).where(Member.bioguide_id.in_(list(latest)))).scalars().all()
    }
    ranked: list[tuple[ReplicatedPortfolioRun, Member | None, float, dict[str, Any]]] = []
    for run in latest.values():
        if int(run.positions_count or 0) < min_positions or int(run.points_count or 0) < min_points:
            continue
        payload = _json_loads(run.status_message)
        score = _score_run(run, status_payload=payload)
        ranked.append((run, members.get(run.entity_id), score, payload))
    ranked.sort(
        key=lambda item: (
            item[2],
            item[0].cagr_pct if item[0].cagr_pct is not None else -999.0,
            item[0].alpha_pct if item[0].alpha_pct is not None else -999.0,
            item[0].positions_count or 0,
        ),
        reverse=True,
    )
    return ranked


def _definition_values(
    run: ReplicatedPortfolioRun,
    member: Member | None,
    *,
    score: float,
    publish: bool,
) -> dict[str, Any]:
    name = _member_name(member, run.entity_id)
    chamber = (member.chamber if member else None) or "congress"
    rule = {
        "kind": "replicated_individual_congress_portfolio",
        "member_bioguide_id": run.entity_id,
        "member_name": name,
        "mode": run.mode,
        "lookback_days": run.lookback_days,
        "execution": "replicate disclosed purchases after public filing availability and hold until reported sale when known",
    }
    parameters = {
        "starting_value": run.starting_value,
        "benchmark": run.benchmark_symbol,
        "mode": run.mode,
        "lookback_days": run.lookback_days,
        "source_methodology_version": run.methodology_version,
        "ranking_score": score,
    }
    universe = {
        "source": "replicated_portfolio_runs",
        "entity_type": run.entity_type,
        "entity_id": run.entity_id,
        "member_name": name,
        "member_chamber": chamber,
        "member_party": member.party if member else None,
        "basis": "latest current-methodology realistic disclosure-lag member portfolio run",
    }
    return {
        "slug": f"congress-portfolio-{run.entity_id.lower()}-{run.lookback_days}d",
        "name": f"{name} Portfolio ({run.lookback_days}D)",
        "category": "congress",
        "family": "individual_portfolio",
        "status": "published" if publish else "draft",
        "access_tier": "premium",
        "is_featured": False,
        "sort_order": 150,
        "short_description": f"Replicated {name} Congress trading portfolio using realistic disclosure-lag timing.",
        "walnut_take": "Draft individual Congress portfolio candidate; inspect sample size, concentration and disclosure-quality notes before publication.",
        "methodology": (
            "Replicates a single Congress member's disclosed public-equity purchases after realistic filing availability, "
            "marks holdings with adjusted prices, and exits when a matching reported sale is available. Missing or non-equity "
            "assets remain recorded as skipped diagnostics."
        ),
        "rule_json": json_dumps(rule),
        "parameters_json": json_dumps(parameters),
        "universe_json": json_dumps(universe),
        "tags_json": json_dumps(["congress", "individual_portfolio", chamber.lower(), f"{run.lookback_days}d"]),
        "risk_notes_json": json_dumps(["individual_trader_concentration", "hold_until_reported_sale"]),
        "data_quality_confidence": "medium",
        "methodology_version": PERSISTENCE_METHODOLOGY_VERSION,
        "created_by": "replicated_portfolio_strategy_refresh",
        "published_at": datetime.now(timezone.utc) if publish else None,
    }


def _run_key(
    run: ReplicatedPortfolioRun,
    *,
    strategy_slug: str,
    code_version: str | None,
    score: float,
) -> str:
    return _sha256_json(
        {
            "slug": strategy_slug,
            "source_run_id": int(run.id),
            "source_computed_at": run.computed_at,
            "source_methodology_version": run.methodology_version,
            "strategy_methodology_version": PERSISTENCE_METHODOLOGY_VERSION,
            "code_version": code_version,
            "score": score,
        }
    )[:24]


def _metrics(run: ReplicatedPortfolioRun, points: list[ReplicatedPortfolioPoint]) -> dict[str, Any]:
    benchmark_cagr = _annualized_return(run.benchmark_return_pct, run.start_date, run.end_date)
    alpha_cagr = (
        round(float(run.cagr_pct) - float(benchmark_cagr), 4)
        if run.cagr_pct is not None and benchmark_cagr is not None
        else None
    )
    avg_active_positions = (
        round(sum(float(point.active_positions or 0) for point in points) / len(points), 4)
        if points
        else None
    )
    return {
        "status": run.status,
        "source_replicated_portfolio_run_id": int(run.id),
        "start_date": run.start_date.isoformat(),
        "end_date": run.end_date.isoformat(),
        "total_return_pct": run.total_return_pct,
        "cagr_pct": run.cagr_pct,
        "benchmark_total_return_pct": run.benchmark_return_pct,
        "benchmark_cagr_pct": benchmark_cagr,
        "alpha_pct": run.alpha_pct,
        "alpha_cagr_pct": alpha_cagr,
        "beta": _beta(points),
        "sharpe": run.sharpe_ratio,
        "sortino": _sortino(points),
        "max_drawdown_pct": run.max_drawdown_pct,
        "annualized_volatility_pct": run.volatility_pct,
        "win_rate_pct": run.win_rate_pct,
        "trade_count": run.positions_count,
        "independent_signals": run.positions_count,
        "avg_active_lots": avg_active_positions,
        "turnover_events": run.positions_count,
        "points_count": run.points_count,
        "positions_count": run.positions_count,
        "skipped_events_count": run.skipped_events_count,
        "ending_cash_pct": run.ending_cash_pct,
    }


def _snapshot_values(
    *,
    strategy_id: int,
    strategy_run_id: int,
    as_of_date: date,
    period: str,
    metrics: dict[str, Any],
    walnut_score: float,
    points: list[ReplicatedPortfolioPoint],
) -> dict[str, Any]:
    if period == "max":
        return {
            "strategy_id": strategy_id,
            "run_id": strategy_run_id,
            "as_of_date": as_of_date,
            "period": period,
            "total_return_pct": metrics.get("total_return_pct"),
            "cagr_pct": metrics.get("cagr_pct"),
            "benchmark_return_pct": metrics.get("benchmark_total_return_pct"),
            "benchmark_cagr_pct": metrics.get("benchmark_cagr_pct"),
            "alpha_cagr_pct": metrics.get("alpha_cagr_pct"),
            "beta": metrics.get("beta"),
            "sharpe": metrics.get("sharpe"),
            "sortino": metrics.get("sortino"),
            "max_drawdown_pct": metrics.get("max_drawdown_pct"),
            "annualized_volatility_pct": metrics.get("annualized_volatility_pct"),
            "win_rate_pct": metrics.get("win_rate_pct"),
            "trade_count": metrics.get("trade_count"),
            "independent_signal_count": metrics.get("independent_signals"),
            "avg_holdings": metrics.get("avg_active_lots"),
            "turnover_events": metrics.get("turnover_events"),
            "walnut_strategy_score": walnut_score,
            "metrics_json": json_dumps(metrics),
        }
    period_days = {"30d": 30, "1y": 365, "2y": 730, "3y": 1095}[period]
    total_return = _period_return(points, days=period_days, value_key="strategy_value")
    benchmark_return = _period_return(points, days=period_days, value_key="benchmark_value")
    cagr = _annualized_return_for_days(total_return, period_days)
    benchmark_cagr = _annualized_return_for_days(benchmark_return, period_days)
    return {
        "strategy_id": strategy_id,
        "run_id": strategy_run_id,
        "as_of_date": as_of_date,
        "period": period,
        "total_return_pct": total_return,
        "cagr_pct": cagr,
        "benchmark_return_pct": benchmark_return,
        "benchmark_cagr_pct": benchmark_cagr,
        "alpha_cagr_pct": round(cagr - benchmark_cagr, 4) if cagr is not None and benchmark_cagr is not None else None,
        "trade_count": metrics.get("trade_count"),
        "independent_signal_count": metrics.get("independent_signals"),
        "walnut_strategy_score": walnut_score,
        "metrics_json": json_dumps({"period": period, "days": period_days, "source": "replicated_portfolio_points"}),
    }


def _performance_snapshots(
    *,
    strategy_id: int,
    strategy_run_id: int,
    as_of_date: date,
    metrics: dict[str, Any],
    walnut_score: float,
    points: list[ReplicatedPortfolioPoint],
) -> list[StrategyPerformanceSnapshot]:
    return [
        StrategyPerformanceSnapshot(
            **_snapshot_values(
                strategy_id=strategy_id,
                strategy_run_id=strategy_run_id,
                as_of_date=as_of_date,
                period=period,
                metrics=metrics,
                walnut_score=walnut_score,
                points=points,
            )
        )
        for period in ("max", "30d", "1y", "2y", "3y")
    ]


def _active_position(position: ReplicatedPortfolioPosition, as_of_date: date) -> bool:
    if position.status == "skipped" or not position.symbol or position.entry_date is None:
        return False
    if position.entry_date > as_of_date:
        return False
    if position.status == "open":
        return True
    return position.exit_date is None or position.exit_date > as_of_date


def _position_signal(position: ReplicatedPortfolioPosition) -> dict[str, Any]:
    return {
        "source_event_id": position.source_event_id,
        "symbol": position.symbol,
        "side": position.side,
        "entry_date": position.entry_date.isoformat() if position.entry_date else None,
        "exit_date": position.exit_date.isoformat() if position.exit_date else None,
        "amount_min": position.amount_min,
        "amount_max": position.amount_max,
        "source_type": position.source_type,
        "source_reason": position.source_reason,
        "confidence": position.confidence,
        "source_document_id": position.source_document_id,
        "source_url": position.source_url,
    }


def _holding_rows(
    *,
    strategy_id: int,
    strategy_run_id: int,
    snapshot_id: int,
    as_of_date: date,
    positions: list[ReplicatedPortfolioPosition],
) -> list[StrategyHoldingRow]:
    grouped: dict[str, list[ReplicatedPortfolioPosition]] = defaultdict(list)
    for position in positions:
        if _active_position(position, as_of_date):
            symbol = normalize_symbol(position.symbol)
            if symbol:
                grouped[symbol].append(position)
    weights: dict[str, float] = {}
    for symbol, rows in grouped.items():
        value = sum(float(row.market_value or 0.0) for row in rows)
        if value <= 0:
            value = sum(max(float(row.shares or 0.0), 0.0) * max(float(row.exit_price or row.entry_price or 0.0), 0.0) for row in rows)
        weights[symbol] = value if value > 0 else float(len(rows))
    total = sum(weights.values())
    holdings: list[StrategyHoldingRow] = []
    ordered = sorted(grouped.items(), key=lambda item: (-weights[item[0]], item[0]))
    for rank, (symbol, rows) in enumerate(ordered, start=1):
        last_price_values = [float(row.exit_price) for row in rows if row.exit_price is not None]
        entry_values = [float(row.entry_price) for row in rows if row.entry_price is not None]
        last_price = sum(last_price_values) / len(last_price_values) if last_price_values else None
        avg_entry = sum(entry_values) / len(entry_values) if entry_values else None
        return_pct = ((last_price / avg_entry) - 1.0) * 100.0 if last_price is not None and avg_entry and avg_entry > 0 else None
        holdings.append(
            StrategyHoldingRow(
                strategy_id=strategy_id,
                snapshot_id=snapshot_id,
                run_id=strategy_run_id,
                symbol=symbol,
                rank=rank,
                weight_pct=round(weights[symbol] / total * 100.0, 4) if total > 0 else None,
                entry_date=min(row.entry_date for row in rows if row.entry_date is not None),
                avg_entry_price=round(avg_entry, 6) if avg_entry is not None else None,
                last_price=round(last_price, 6) if last_price is not None else None,
                return_pct=round(return_pct, 4) if return_pct is not None else None,
                source_signal_count=len(rows),
                source_signals_json=json_dumps([_position_signal(row) for row in rows]),
                payload_json=json_dumps({"as_of_date": as_of_date.isoformat(), "source": "replicated_portfolio_open_positions"}),
            )
        )
    return holdings


def _current_holding(row: StrategyHoldingRow, *, as_of_date: date) -> StrategyCurrentHolding:
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


def persist_top_congress_portfolio_strategies(
    db: Session,
    *,
    lookback_days: int = 1095,
    top: int = 10,
    benchmark: str = "SPY",
    min_positions: int = 10,
    min_points: int = 250,
    code_version: str | None = None,
    publish: bool = False,
    apply: bool = False,
) -> dict[str, Any]:
    ranked = _latest_ranked_runs(
        db,
        lookback_days=lookback_days,
        benchmark=benchmark,
        min_positions=min_positions,
        min_points=min_points,
    )
    selected = ranked[: max(0, top)]
    rows: list[dict[str, Any]] = []
    for rank, (source_run, member, score, status_payload) in enumerate(selected, start=1):
        strategy_values = _definition_values(source_run, member, score=score, publish=publish)
        strategy_slug = str(strategy_values["slug"])
        points = (
            db.execute(
                select(ReplicatedPortfolioPoint)
                .where(ReplicatedPortfolioPoint.run_id == int(source_run.id))
                .order_by(ReplicatedPortfolioPoint.asof_date.asc())
            )
            .scalars()
            .all()
        )
        positions = (
            db.execute(
                select(ReplicatedPortfolioPosition)
                .where(ReplicatedPortfolioPosition.run_id == int(source_run.id))
                .order_by(ReplicatedPortfolioPosition.id.asc())
            )
            .scalars()
            .all()
        )
        metrics = _metrics(source_run, points)
        run_key = _run_key(source_run, strategy_slug=strategy_slug, code_version=code_version, score=score)
        as_of_date = source_run.end_date
        current_symbols = {
            normalize_symbol(position.symbol)
            for position in positions
            if _active_position(position, as_of_date) and normalize_symbol(position.symbol)
        }
        preview = {
            "rank": rank,
            "slug": strategy_slug,
            "name": strategy_values["name"],
            "mode": "apply" if apply else "dry_run",
            "publish": publish,
            "source_run_id": int(source_run.id),
            "run_key": run_key,
            "as_of_date": as_of_date.isoformat(),
            "lookback_days": int(source_run.lookback_days),
            "cagr_pct": source_run.cagr_pct,
            "total_return_pct": source_run.total_return_pct,
            "alpha_pct": source_run.alpha_pct,
            "sharpe": source_run.sharpe_ratio,
            "max_drawdown_pct": source_run.max_drawdown_pct,
            "positions_count": source_run.positions_count,
            "equity_points": len(points),
            "current_holdings": len(current_symbols),
            "walnut_strategy_score": score,
            "curve_quality_status": status_payload.get("curve_diagnostics", {}).get("curve_quality_status"),
        }
        if not apply:
            rows.append(preview)
            continue

        strategy = db.execute(select(StrategyDefinition).where(StrategyDefinition.slug == strategy_slug)).scalars().first()
        if strategy is None:
            strategy = StrategyDefinition(**strategy_values)
            db.add(strategy)
            db.flush()
        else:
            preserve_status = strategy.status if not publish else strategy_values["status"]
            preserve_published_at = strategy.published_at if not publish else strategy_values["published_at"]
            for key, value in strategy_values.items():
                if key == "status":
                    value = preserve_status
                elif key == "published_at":
                    value = preserve_published_at
                setattr(strategy, key, value)
            db.flush()

        existing_run = (
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
            "run_type": "replicated_portfolio_refresh",
            "status": source_run.status,
            "started_at": now,
            "completed_at": now,
            "backtest_start_date": source_run.start_date,
            "backtest_end_date": source_run.end_date,
            "benchmark": source_run.benchmark_symbol,
            "methodology_version": PERSISTENCE_METHODOLOGY_VERSION,
            "code_version": code_version,
            "dataset_versions_json": json_dumps(
                {
                    "source": "replicated_portfolio_runs",
                    "source_run_id": int(source_run.id),
                    "source_methodology_version": source_run.methodology_version,
                    "source_computed_at": source_run.computed_at,
                }
            ),
            "parameters_json": strategy_values["parameters_json"],
            "universe_hash": _sha256_json(strategy_values["universe_json"]),
            "universe_json": strategy_values["universe_json"],
            "execution_timing": "realistic public disclosure-lag portfolio replication",
            "fee_bps_per_side": 0.0,
            "slippage_bps_per_side": 0.0,
            "metrics_json": json_dumps(metrics),
            "diagnostics_json": json_dumps({"source_status_message": status_payload}),
            "walnut_strategy_score": score,
            "data_quality_confidence": "medium",
            "error": None if source_run.status == "ok" else source_run.status_message,
        }
        if existing_run is None:
            strategy_run = StrategyBacktestRun(**run_values)
            db.add(strategy_run)
            db.flush()
            db.execute(delete(StrategyCurrentHolding).where(StrategyCurrentHolding.strategy_id == int(strategy.id)))
        else:
            strategy_run = existing_run
            for key, value in run_values.items():
                setattr(strategy_run, key, value)
            db.flush()
            _delete_run_children(db, strategy_id=int(strategy.id), run_id=int(strategy_run.id))

        db.add_all(
            _performance_snapshots(
                strategy_id=int(strategy.id),
                strategy_run_id=int(strategy_run.id),
                as_of_date=as_of_date,
                metrics=metrics,
                walnut_score=score,
                points=points,
            )
        )
        db.add_all(_curve_points(strategy_id=int(strategy.id), strategy_run_id=int(strategy_run.id), points=points))
        holdings = _holding_rows(
            strategy_id=int(strategy.id),
            strategy_run_id=int(strategy_run.id),
            snapshot_id=0,
            as_of_date=as_of_date,
            positions=positions,
        )
        snapshot = StrategyHoldingsSnapshot(
            strategy_id=int(strategy.id),
            run_id=int(strategy_run.id),
            as_of_date=as_of_date,
            holdings_count=len(holdings),
            total_weight_pct=round(sum(float(row.weight_pct or 0.0) for row in holdings), 4),
            cash_weight_pct=round(max(0.0, 100.0 - sum(float(row.weight_pct or 0.0) for row in holdings)), 4),
            diagnostics_json=json_dumps({"source": "replicated_portfolio_open_positions"}),
        )
        db.add(snapshot)
        db.flush()
        for holding in holdings:
            holding.snapshot_id = int(snapshot.id)
        db.add_all(holdings)
        db.add_all([_current_holding(holding, as_of_date=as_of_date) for holding in holdings])
        rows.append({**preview, "strategy_id": int(strategy.id), "run_id": int(strategy_run.id)})

    if apply:
        db.commit()
    return {
        "metadata": {
            "mode": "apply" if apply else "dry_run",
            "publish": publish,
            "lookback_days": lookback_days,
            "top": top,
            "benchmark": normalize_symbol(benchmark) or "SPY",
            "min_positions": min_positions,
            "min_points": min_points,
            "eligible_runs": len(ranked),
            "rows": len(rows),
        },
        "rows": rows,
    }
