from __future__ import annotations

import json
from datetime import date, datetime, timezone
from typing import Any

from fastapi import HTTPException
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.entitlements import TierEntitlements
from app.models import (
    StrategyBacktestRun,
    StrategyCurrentHolding,
    StrategyDefinition,
    StrategyEquityCurvePoint,
    StrategyLiveHolding,
    StrategyPerformanceSnapshot,
    StrategyTrade,
    StrategyVersion,
    ReplicatedPortfolioPosition,
)

STRATEGY_SORT_FIELDS = {
    "walnut_score": "walnut_strategy_score",
    "cagr": "cagr_pct",
    "return": "total_return_pct",
    "alpha": "alpha_cagr_pct",
    "sharpe": "sharpe",
    "drawdown": "max_drawdown_pct",
}


def _json_loads(value: str | None, fallback: Any) -> Any:
    if not value:
        return fallback
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return fallback


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return str(value)


def _can_follow_strategy(entitlements: TierEntitlements) -> bool:
    """Strategy following is a Premium capability backed by the existing alert entitlement."""
    return entitlements.has_feature("notification_digests")


def _definition_payload(strategy: StrategyDefinition, *, entitlements: TierEntitlements) -> dict[str, Any]:
    return {
        "id": int(strategy.id),
        "slug": strategy.slug,
        "name": strategy.name,
        "category": strategy.category,
        "family": strategy.family,
        "status": strategy.status,
        "accessTier": strategy.access_tier,
        "access": {
            "requiredTier": "free",
            "userTier": entitlements.tier,
            "canAccess": True,
            "locked": False,
        },
        "isFeatured": bool(strategy.is_featured),
        "sortOrder": int(strategy.sort_order or 100),
        "shortDescription": strategy.short_description,
        "walnutTake": strategy.walnut_take,
        "methodology": strategy.methodology,
        "methodologyVersion": strategy.methodology_version,
        "dataQualityConfidence": strategy.data_quality_confidence,
        "rule": _json_loads(strategy.rule_json, {}),
        "parameters": _json_loads(strategy.parameters_json, {}),
        "universe": _json_loads(strategy.universe_json, {}),
        "tags": _json_loads(strategy.tags_json, []),
        "riskNotes": _json_loads(strategy.risk_notes_json, []),
        "publishedAt": _iso(strategy.published_at),
        "updatedAt": _iso(strategy.updated_at),
    }


def _run_payload(run: StrategyBacktestRun | None) -> dict[str, Any] | None:
    if run is None:
        return None
    return {
        "id": int(run.id),
        "runKey": run.run_key,
        "runType": run.run_type,
        "status": run.status,
        "startedAt": _iso(run.started_at),
        "completedAt": _iso(run.completed_at),
        "backtestStartDate": _iso(run.backtest_start_date),
        "backtestEndDate": _iso(run.backtest_end_date),
        "benchmark": run.benchmark,
        "methodologyVersion": run.methodology_version,
        "codeVersion": run.code_version,
        "datasetVersions": _json_loads(run.dataset_versions_json, {}),
        "parameters": _json_loads(run.parameters_json, {}),
        "universeHash": run.universe_hash,
        "universe": _json_loads(run.universe_json, {}),
        "executionTiming": run.execution_timing,
        "feesBpsPerSide": float(run.fee_bps_per_side or 0.0),
        "slippageBpsPerSide": float(run.slippage_bps_per_side or 0.0),
        "metrics": _json_loads(run.metrics_json, {}),
        "diagnostics": _json_loads(run.diagnostics_json, {}),
        "walnutStrategyScore": run.walnut_strategy_score,
        "dataQualityConfidence": run.data_quality_confidence,
        "error": run.error,
    }


def _performance_payload(snapshot: StrategyPerformanceSnapshot | None) -> dict[str, Any] | None:
    if snapshot is None:
        return None
    return {
        "id": int(snapshot.id),
        "period": snapshot.period,
        "asOfDate": _iso(snapshot.as_of_date),
        "totalReturnPct": snapshot.total_return_pct,
        "cagrPct": snapshot.cagr_pct,
        "benchmarkReturnPct": snapshot.benchmark_return_pct,
        "benchmarkCagrPct": snapshot.benchmark_cagr_pct,
        "alphaCagrPct": snapshot.alpha_cagr_pct,
        "beta": snapshot.beta,
        "sharpe": snapshot.sharpe,
        "sortino": snapshot.sortino,
        "maxDrawdownPct": snapshot.max_drawdown_pct,
        "annualizedVolatilityPct": snapshot.annualized_volatility_pct,
        "winRatePct": snapshot.win_rate_pct,
        "tradeCount": snapshot.trade_count,
        "independentSignalCount": snapshot.independent_signal_count,
        "avgHoldings": snapshot.avg_holdings,
        "turnoverEvents": snapshot.turnover_events,
        "rolling12mBeatingSpyPct": snapshot.rolling_12m_beating_spy_pct,
        "walnutStrategyScore": snapshot.walnut_strategy_score,
        "metrics": _json_loads(snapshot.metrics_json, {}),
    }


def _latest_run(db: Session, strategy_id: int) -> StrategyBacktestRun | None:
    return (
        db.execute(
            select(StrategyBacktestRun)
            .where(StrategyBacktestRun.strategy_id == strategy_id)
            .where(StrategyBacktestRun.status == "ok")
            .order_by(StrategyBacktestRun.completed_at.desc().nullslast(), StrategyBacktestRun.id.desc())
            .limit(1)
        )
        .scalars()
        .first()
    )


def _has_active_prospective_version(db: Session, strategy_id: int) -> bool:
    return (
        db.execute(
            select(StrategyVersion.id)
            .where(StrategyVersion.strategy_id == strategy_id, StrategyVersion.status == "active")
            .limit(1)
        ).scalar_one_or_none()
        is not None
    )


def _latest_performance(db: Session, *, strategy_id: int, run_id: int, period: str) -> StrategyPerformanceSnapshot | None:
    return (
        db.execute(
            select(StrategyPerformanceSnapshot)
            .where(StrategyPerformanceSnapshot.strategy_id == strategy_id)
            .where(StrategyPerformanceSnapshot.run_id == run_id)
            .where(StrategyPerformanceSnapshot.period == period)
            .order_by(StrategyPerformanceSnapshot.as_of_date.desc(), StrategyPerformanceSnapshot.id.desc())
            .limit(1)
        )
        .scalars()
        .first()
    )


def _sort_value(item: dict[str, Any], sort: str) -> float:
    performance = item.get("performance") or {}
    if sort == "drawdown":
        value = performance.get("maxDrawdownPct")
        return 0.0 if value is None else -abs(float(value))
    if sort == "walnut_score":
        validation = ((item.get("latestRun") or {}).get("diagnostics") or {}).get("validation") or {}
        score = validation.get("walnut_strategy_score") or {}
        if score.get("score_version") != "walnut_strategy_score_v2":
            return -1.0
        return float(score.get("score") or -1.0)
    field = STRATEGY_SORT_FIELDS.get(sort, "walnut_strategy_score")
    camel = {
        "cagr_pct": "cagrPct",
        "total_return_pct": "totalReturnPct",
        "alpha_cagr_pct": "alphaCagrPct",
        "sharpe": "sharpe",
        "max_drawdown_pct": "maxDrawdownPct",
    }.get(field, "walnutStrategyScore")
    return float(performance.get(camel) or 0.0)


def list_strategy_cards(
    db: Session,
    *,
    entitlements: TierEntitlements,
    category: str | None = None,
    period: str = "max",
    sort: str = "cagr",
    include_drafts: bool = False,
) -> dict[str, Any]:
    statement = select(StrategyDefinition)
    if not include_drafts:
        statement = statement.where(StrategyDefinition.status == "published")
    if category:
        statement = statement.where(StrategyDefinition.category == category)
    strategies = db.execute(statement.order_by(StrategyDefinition.sort_order.asc(), StrategyDefinition.name.asc())).scalars().all()

    items: list[dict[str, Any]] = []
    for strategy in strategies:
        run = _latest_run(db, int(strategy.id))
        performance = _latest_performance(db, strategy_id=int(strategy.id), run_id=int(run.id), period=period) if run else None
        payload = _definition_payload(strategy, entitlements=entitlements)
        payload["prospectiveActive"] = _has_active_prospective_version(db, int(strategy.id))
        payload["latestRun"] = _run_payload(run)
        payload["performance"] = _performance_payload(performance)
        items.append(payload)

    items.sort(key=lambda item: (_sort_value(item, sort), -(item.get("sortOrder") or 100)), reverse=True)
    return {
        "metadata": {
            "period": period,
            "sort": sort,
            "category": category,
            "includeDrafts": include_drafts,
            "count": len(items),
            "storage": "persisted_strategy_snapshots",
        },
        "items": items,
    }


def set_strategy_publication(
    db: Session,
    *,
    slug: str,
    published: bool,
    entitlements: TierEntitlements,
) -> dict[str, Any]:
    """Change only catalogue visibility; historical research artifacts stay intact."""
    strategy = db.execute(select(StrategyDefinition).where(StrategyDefinition.slug == slug)).scalars().first()
    if strategy is None:
        raise HTTPException(status_code=404, detail="Strategy not found.")

    if published:
        run = _latest_run(db, int(strategy.id))
        snapshot = _latest_performance(db, strategy_id=int(strategy.id), run_id=int(run.id), period="max") if run else None
        if run is None or snapshot is None:
            raise HTTPException(
                status_code=422,
                detail="A successful reproducible run and max-period performance snapshot are required before publication.",
            )
        strategy.status = "published"
        strategy.published_at = datetime.now(timezone.utc)
    else:
        strategy.status = "draft"
        strategy.published_at = None

    db.commit()
    return strategy_detail(db, slug=slug, entitlements=entitlements, include_drafts=True)


def strategy_detail(
    db: Session,
    *,
    slug: str,
    entitlements: TierEntitlements,
    period: str = "max",
    equity_limit: int = 1500,
    holdings_offset: int = 0,
    holdings_limit: int = 20,
    include_drafts: bool = False,
) -> dict[str, Any]:
    statement = select(StrategyDefinition).where(StrategyDefinition.slug == slug)
    if not include_drafts:
        statement = statement.where(StrategyDefinition.status == "published")
    strategy = db.execute(statement).scalars().first()
    if strategy is None:
        raise HTTPException(status_code=404, detail="Strategy not found.")

    payload = _definition_payload(strategy, entitlements=entitlements)
    payload["prospectiveActive"] = _has_active_prospective_version(db, int(strategy.id))
    run = _latest_run(db, int(strategy.id))
    performance = _latest_performance(db, strategy_id=int(strategy.id), run_id=int(run.id), period=period) if run else None
    payload["latestRun"] = _run_payload(run)
    payload["performance"] = _performance_payload(performance)

    equity_curve: list[dict[str, Any]] = []
    current_holdings: list[dict[str, Any]] = []
    current_holdings_count = 0
    can_follow = _can_follow_strategy(entitlements)
    if run:
        points = (
            db.execute(
                select(StrategyEquityCurvePoint)
                .where(StrategyEquityCurvePoint.run_id == int(run.id))
                .order_by(StrategyEquityCurvePoint.date.asc(), StrategyEquityCurvePoint.id.asc())
                .limit(max(1, min(equity_limit, 5000)))
            )
            .scalars()
            .all()
        )
        equity_curve = [
            {
                "date": _iso(point.date),
                "strategyValue": point.strategy_value,
                "benchmarkValue": point.benchmark_value,
                "drawdownPct": point.drawdown_pct,
                "activeHoldings": point.active_holdings,
            }
            for point in points
        ]
    holdings_model = StrategyLiveHolding if payload["prospectiveActive"] else StrategyCurrentHolding
    holdings_offset = max(0, int(holdings_offset))
    holdings_limit = max(1, min(100, int(holdings_limit)))
    current_holdings_count = int(
        db.execute(
            select(func.count()).select_from(holdings_model).where(holdings_model.strategy_id == int(strategy.id))
        ).scalar_one()
        or 0
    )
    if can_follow:
        holdings = db.execute(
            select(holdings_model)
            .where(holdings_model.strategy_id == int(strategy.id))
            .order_by(holdings_model.rank.asc().nullslast(), holdings_model.symbol.asc())
            .offset(holdings_offset)
            .limit(holdings_limit)
        ).scalars().all()
        if payload["prospectiveActive"]:
            current_holdings = [
                {
                    "symbol": row.symbol,
                    "companyName": row.company_name,
                    "sector": row.sector,
                    "rank": row.rank,
                    "weightPct": row.weight_pct,
                    "entryDate": _iso(row.entry_date),
                    "lastPrice": row.entry_price,
                    "returnPct": None,
                    "sourceSignalCount": row.source_count or 0,
                    "sourceSignals": [],
                    "payload": _json_loads(row.qualification_snapshot_json, {}),
                    "asOfDate": _iso(row.as_of_date),
                }
                for row in holdings
            ]
        else:
            current_holdings = [
                {
                    "symbol": row.symbol,
                    "companyName": row.company_name,
                    "sector": row.sector,
                    "rank": row.rank,
                    "weightPct": row.weight_pct,
                    "entryDate": _iso(row.entry_date),
                    "lastPrice": row.last_price,
                    "returnPct": row.return_pct,
                    "sourceSignalCount": row.source_signal_count,
                    "sourceSignals": _json_loads(row.source_signals_json, []),
                    "payload": _json_loads(row.payload_json, {}),
                    "asOfDate": _iso(row.as_of_date),
                }
                for row in holdings
            ]

    payload["equityCurve"] = equity_curve
    payload["currentHoldings"] = current_holdings
    payload["currentHoldingsCount"] = current_holdings_count
    payload["currentHoldingsTotal"] = current_holdings_count
    payload["currentHoldingsOffset"] = holdings_offset
    payload["holdingsSource"] = "prospective_monitor" if payload["prospectiveActive"] else "historical_backtest"
    transaction_history: list[dict[str, Any]] = []
    transaction_total = 0
    if can_follow:
        model_trades = db.execute(
            select(StrategyTrade)
            .where(StrategyTrade.strategy_id == int(strategy.id))
            .order_by(StrategyTrade.effective_date.desc().nullslast(), StrategyTrade.id.desc())
        ).scalars().all()
        source_positions: list[ReplicatedPortfolioPosition] = []
        if not model_trades and run is not None:
            dataset = _json_loads(run.dataset_versions_json, {})
            source_run_id = dataset.get("source_run_id") if isinstance(dataset, dict) else None
            if source_run_id is not None:
                source_positions = db.execute(
                    select(ReplicatedPortfolioPosition)
                    .where(ReplicatedPortfolioPosition.run_id == int(source_run_id))
                    .order_by(ReplicatedPortfolioPosition.entry_date.desc().nullslast(), ReplicatedPortfolioPosition.id.desc())
                ).scalars().all()
        if model_trades:
            transaction_total = len(model_trades)
            transaction_history = [
                {
                    "recordType": "model_trade",
                    "symbol": row.symbol,
                    "tickerAtTime": row.ticker_at_time,
                    "action": row.action,
                    "status": row.status,
                    "effectiveDate": _iso(row.effective_date),
                    "entryPrice": row.entry_price,
                    "exitPrice": row.exit_price,
                    "weightPct": row.weight_pct,
                    "exitReason": row.exit_reason,
                }
                for row in model_trades[holdings_offset : holdings_offset + holdings_limit]
            ]
        else:
            transaction_total = len(source_positions)
            transaction_history = [
                {
                    "recordType": "reconstructed_position",
                    "symbol": row.symbol,
                    "action": row.side,
                    "status": row.status,
                    "effectiveDate": _iso(row.entry_date),
                    "entryPrice": row.entry_price,
                    "exitPrice": row.exit_price,
                    "returnPct": row.return_pct,
                    "sourceType": row.source_type,
                    "confidence": row.confidence,
                }
                for row in source_positions[holdings_offset : holdings_offset + holdings_limit]
            ]
    payload["transactionHistory"] = transaction_history
    payload["transactionHistoryTotal"] = transaction_total
    payload["transactionHistoryOffset"] = holdings_offset
    payload["strategyAccess"] = {
        "canViewCurrentHoldings": can_follow,
        "canFollow": can_follow,
        "requiredTier": "premium",
    }
    return payload
