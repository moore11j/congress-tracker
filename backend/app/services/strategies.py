from __future__ import annotations

import json
from datetime import date, datetime, timezone
from typing import Any

from fastapi import HTTPException
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.entitlements import PLAN_RANKS, TierEntitlements
from app.models import (
    ReplicatedPortfolioPosition,
    StrategyBacktestRun,
    StrategyCurrentHolding,
    StrategyDefinition,
    StrategyEquityCurvePoint,
    StrategyPerformanceSnapshot,
    StrategyTrade,
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


def _tier_allowed(required_tier: str, entitlements: TierEntitlements) -> bool:
    required = (required_tier or "premium").lower()
    if required == "free":
        return True
    return entitlements.rank >= PLAN_RANKS.get(required, PLAN_RANKS["premium"])


def _definition_payload(strategy: StrategyDefinition, *, entitlements: TierEntitlements) -> dict[str, Any]:
    can_access = _tier_allowed(strategy.access_tier, entitlements)
    return {
        "id": int(strategy.id),
        "slug": strategy.slug,
        "name": strategy.name,
        "category": strategy.category,
        "family": strategy.family,
        "status": strategy.status,
        "accessTier": strategy.access_tier,
        "access": {
            "requiredTier": strategy.access_tier,
            "userTier": entitlements.tier,
            "canAccess": can_access,
            "locked": not can_access,
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


def _transaction_history_payload(
    db: Session,
    *,
    strategy_id: int,
    run: StrategyBacktestRun,
    offset: int,
    limit: int,
) -> tuple[list[dict[str, Any]], int]:
    """Return persisted historical source positions or prospective model-trade records.

    Replicated Congress portfolios predate the live strategy event stream. Their
    source positions are intentionally surfaced as reconstructed history instead
    of being mislabeled as prospective Walnut model trades.
    """
    dataset_versions = _json_loads(run.dataset_versions_json, {})
    source_run_id = dataset_versions.get("source_run_id") if isinstance(dataset_versions, dict) else None
    source_name = dataset_versions.get("source") if isinstance(dataset_versions, dict) else None

    if source_name == "replicated_portfolio_runs" and source_run_id is not None:
        source_run_id = int(source_run_id)
        statement = (
            select(ReplicatedPortfolioPosition)
            .where(ReplicatedPortfolioPosition.run_id == source_run_id)
            .where(ReplicatedPortfolioPosition.skip_reason.is_(None))
        )
        total = int(db.execute(select(func.count()).select_from(statement.subquery())).scalar_one() or 0)
        positions = (
            db.execute(
                statement.order_by(
                    ReplicatedPortfolioPosition.entry_date.desc().nullslast(),
                    ReplicatedPortfolioPosition.id.desc(),
                )
                .offset(offset)
                .limit(limit)
            )
            .scalars()
            .all()
        )
        return [
            {
                "id": f"replicated-{row.id}",
                "recordType": "reconstructed_position",
                "symbol": row.symbol,
                "action": row.side or "buy",
                "status": row.status,
                "signalDate": None,
                "effectiveDate": _iso(row.entry_date),
                "exitDate": _iso(row.exit_date),
                "entryPrice": row.entry_price,
                "exitPrice": row.exit_price,
                "returnPct": row.return_pct,
                "weightPct": None,
                "sourceType": row.source_type,
                "sourceReason": row.source_reason,
                "confidence": row.confidence,
                "sourceDocumentId": row.source_document_id,
                "sourceUrl": row.source_url,
            }
            for row in positions
        ], total

    statement = select(StrategyTrade).where(StrategyTrade.strategy_id == strategy_id)
    total = int(db.execute(select(func.count()).select_from(statement.subquery())).scalar_one() or 0)
    trades = (
        db.execute(
            statement.order_by(StrategyTrade.effective_date.desc().nullslast(), StrategyTrade.id.desc())
            .offset(offset)
            .limit(limit)
        )
        .scalars()
        .all()
    )
    return [
        {
            "id": f"model-{row.id}",
            "recordType": "model_trade",
            "symbol": row.symbol,
            "action": row.action,
            "status": row.status,
            "signalDate": _iso(row.signal_date),
            "effectiveDate": _iso(row.effective_date),
            "exitDate": None,
            "entryPrice": row.entry_price,
            "exitPrice": row.exit_price,
            "returnPct": None,
            "weightPct": row.weight_pct,
            "sourceType": "prospective_model_trade",
            "sourceReason": row.exit_reason,
            "confidence": None,
            "sourceDocumentId": None,
            "sourceUrl": None,
        }
        for row in trades
    ], total


def _sort_value(item: dict[str, Any], sort: str) -> float:
    performance = item.get("performance") or {}
    if sort == "drawdown":
        value = performance.get("maxDrawdownPct")
        return 0.0 if value is None else -abs(float(value))
    if sort == "walnut_score":
        value = performance.get("walnutStrategyScore")
        if value is None:
            value = (item.get("latestRun") or {}).get("walnutStrategyScore")
        return float(value or 0.0)
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
    run = _latest_run(db, int(strategy.id))
    performance = _latest_performance(db, strategy_id=int(strategy.id), run_id=int(run.id), period=period) if run else None
    payload["latestRun"] = _run_payload(run)
    payload["performance"] = _performance_payload(performance)

    if not payload["access"]["canAccess"]:
        payload["equityCurve"] = []
        payload["currentHoldings"] = []
        payload["currentHoldingsTotal"] = 0
        payload["currentHoldingsOffset"] = 0
        payload["currentHoldingsLimit"] = 0
        payload["transactionHistory"] = []
        payload["transactionHistoryTotal"] = 0
        payload["transactionHistoryOffset"] = 0
        payload["transactionHistoryLimit"] = 0
        return payload

    equity_curve: list[dict[str, Any]] = []
    current_holdings: list[dict[str, Any]] = []
    normalized_holdings_offset = max(0, int(holdings_offset))
    normalized_holdings_limit = max(1, min(int(holdings_limit), 100))
    current_holdings_total = 0
    transaction_history: list[dict[str, Any]] = []
    transaction_history_total = 0
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
        current_holdings_total = int(
            db.execute(
                select(func.count())
                .select_from(StrategyCurrentHolding)
                .where(StrategyCurrentHolding.strategy_id == int(strategy.id))
            ).scalar_one()
            or 0
        )
        holdings = (
            db.execute(
                select(StrategyCurrentHolding)
                .where(StrategyCurrentHolding.strategy_id == int(strategy.id))
                .order_by(StrategyCurrentHolding.rank.asc().nullslast(), StrategyCurrentHolding.symbol.asc())
                .offset(normalized_holdings_offset)
                .limit(normalized_holdings_limit)
            )
            .scalars()
            .all()
        )
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
        transaction_history, transaction_history_total = _transaction_history_payload(
            db,
            strategy_id=int(strategy.id),
            run=run,
            offset=normalized_holdings_offset,
            limit=normalized_holdings_limit,
        )

    payload["equityCurve"] = equity_curve
    payload["currentHoldings"] = current_holdings
    payload["currentHoldingsTotal"] = current_holdings_total
    payload["currentHoldingsOffset"] = normalized_holdings_offset
    payload["currentHoldingsLimit"] = normalized_holdings_limit
    payload["transactionHistory"] = transaction_history
    payload["transactionHistoryTotal"] = transaction_history_total
    payload["transactionHistoryOffset"] = normalized_holdings_offset
    payload["transactionHistoryLimit"] = normalized_holdings_limit
    return payload
