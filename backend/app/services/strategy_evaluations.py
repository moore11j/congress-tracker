"""Deterministic prospective strategy evaluations.

Historical research stays in ``StrategyBacktestRun``.  This module records only
forward-looking rebalance decisions and their canonical events; it deliberately
does not send notifications or mutate historical holdings.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date, datetime, time, timezone
from typing import Any

from sqlalchemy import delete, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.models import Security, StrategyDefinition, StrategyEvaluationRun, StrategyEvent, StrategyLiveHolding, StrategyTrade, StrategyVersion


@dataclass(frozen=True)
class StrategyEvaluationCandidate:
    symbol: str
    weight_pct: float
    ticker_at_time: str | None = None
    security_id: int | None = None
    score: float | None = None
    source_count: int | None = None
    entry_price: float | None = None
    effective_date: date | None = None
    qualification_snapshot: dict[str, Any] = field(default_factory=dict)

    @property
    def normalized_symbol(self) -> str:
        return self.symbol.strip().upper()


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _run_payload(run: StrategyEvaluationRun, *, idempotent: bool) -> dict[str, Any]:
    metadata = json.loads(run.metadata_json or "{}")
    return {
        "runId": int(run.id),
        "strategyId": int(run.strategy_id),
        "strategyVersionId": int(run.strategy_version_id),
        "evaluationDate": run.evaluation_date.isoformat(),
        "status": run.status,
        "idempotent": idempotent,
        "universeCount": int(run.universe_count or 0),
        "qualifyingCount": int(run.qualifying_count or 0),
        "changes": metadata.get("changes", {}),
    }


def _event(
    db: Session,
    *,
    strategy_id: int,
    strategy_version_id: int,
    run_id: int,
    trade: StrategyTrade | None,
    event_type: str,
    occurred_at: datetime,
    dedupe_key: str,
    payload: dict[str, Any],
) -> None:
    db.add(
        StrategyEvent(
            strategy_id=strategy_id,
            strategy_version_id=strategy_version_id,
            strategy_run_id=run_id,
            strategy_trade_id=int(trade.id) if trade else None,
            security_id=trade.security_id if trade else None,
            symbol=trade.symbol if trade else None,
            ticker_at_time=trade.ticker_at_time if trade else None,
            event_type=event_type,
            occurred_at=occurred_at,
            dedupe_key=dedupe_key,
            payload_json=_json(payload),
        )
    )


def _refresh_live_holdings(db: Session, *, strategy_id: int, run_id: int, evaluation_date: date) -> None:
    """Rebuild only the disposable live projection from the append-only trade ledger."""
    opening_trades = db.execute(
        select(StrategyTrade)
        .where(
            StrategyTrade.strategy_id == strategy_id,
            StrategyTrade.action == "buy",
            StrategyTrade.status == "open",
        )
        .order_by(StrategyTrade.symbol.asc(), StrategyTrade.id.asc())
    ).scalars().all()
    rebalances = db.execute(
        select(StrategyTrade)
        .where(
            StrategyTrade.strategy_id == strategy_id,
            StrategyTrade.action == "rebalance",
        )
        .order_by(StrategyTrade.symbol.asc(), StrategyTrade.id.desc())
    ).scalars().all()
    # The query is newest-first per symbol, so retain the first row only.
    latest_rebalance = {}
    for trade in rebalances:
        latest_rebalance.setdefault(trade.symbol.upper(), trade)

    rows: list[StrategyLiveHolding] = []
    ranked = sorted(
        opening_trades,
        key=lambda trade: (-float((latest_rebalance.get(trade.symbol.upper()) or trade).weight_pct or 0.0), trade.symbol),
    )
    for rank, opening in enumerate(ranked, start=1):
        current = latest_rebalance.get(opening.symbol.upper()) or opening
        security = db.get(Security, current.security_id or opening.security_id) if (current.security_id or opening.security_id) else None
        rows.append(
            StrategyLiveHolding(
                strategy_id=strategy_id,
                strategy_version_id=current.strategy_version_id,
                strategy_run_id=run_id,
                opening_trade_id=opening.id,
                security_id=current.security_id or opening.security_id,
                symbol=opening.symbol.upper(),
                ticker_at_time=current.ticker_at_time or opening.ticker_at_time,
                company_name=security.name if security else None,
                sector=security.sector if security else None,
                rank=rank,
                weight_pct=current.weight_pct,
                entry_date=opening.effective_date,
                entry_price=opening.entry_price,
                score=current.score_at_entry if current.score_at_entry is not None else opening.score_at_entry,
                source_count=current.source_count_at_entry if current.source_count_at_entry is not None else opening.source_count_at_entry,
                qualification_snapshot_json=current.qualification_snapshot_json or opening.qualification_snapshot_json,
                as_of_date=evaluation_date,
            )
        )

    db.execute(delete(StrategyLiveHolding).where(StrategyLiveHolding.strategy_id == strategy_id))
    db.add_all(rows)


def evaluate_strategy_candidates(
    db: Session,
    *,
    strategy_id: int,
    strategy_version_id: int,
    evaluation_date: date,
    candidates: list[StrategyEvaluationCandidate],
    universe_count: int,
    scheduled_for: datetime | None = None,
    idempotency_key: str | None = None,
    closing_prices: dict[str, float] | None = None,
    initialize: bool = False,
) -> dict[str, Any]:
    """Persist one point-in-time rebalance and emit its idempotent event stream.

    Callers must pass candidates already selected from point-in-time inputs.  This
    function intentionally does not resolve signals or prices itself, which keeps
    the strategy resolver independently testable and makes the persisted decision
    auditable.
    """
    strategy = db.get(StrategyDefinition, strategy_id)
    if strategy is None:
        raise ValueError(f"Unknown strategy id {strategy_id}.")
    version = db.get(StrategyVersion, strategy_version_id)
    if version is None or int(version.strategy_id) != int(strategy_id):
        raise ValueError("Strategy version must belong to the evaluated strategy.")

    normalized = {candidate.normalized_symbol: candidate for candidate in candidates}
    if len(normalized) != len(candidates) or any(not symbol for symbol in normalized):
        raise ValueError("Candidates must contain unique, non-empty symbols.")
    if any(float(candidate.weight_pct) <= 0 for candidate in candidates):
        raise ValueError("Candidate weights must be positive.")

    run_key = idempotency_key or f"strategy:{strategy_id}:version:{strategy_version_id}:evaluation:{evaluation_date.isoformat()}"
    existing = db.execute(
        select(StrategyEvaluationRun).where(
            StrategyEvaluationRun.strategy_id == strategy_id,
            StrategyEvaluationRun.idempotency_key == run_key,
        )
    ).scalars().first()
    if existing is not None:
        return _run_payload(existing, idempotent=True)

    occurred_at = scheduled_for or datetime.combine(evaluation_date, time.min, tzinfo=timezone.utc)
    run = StrategyEvaluationRun(
        strategy_id=strategy_id,
        strategy_version_id=strategy_version_id,
        idempotency_key=run_key,
        evaluation_date=evaluation_date,
        scheduled_for=scheduled_for,
        status="pending",
        universe_count=max(0, int(universe_count)),
        qualifying_count=len(normalized),
    )
    db.add(run)
    try:
        db.flush()
    except IntegrityError:
        # Another worker may have created this exact scheduled evaluation first.
        db.rollback()
        existing = db.execute(
            select(StrategyEvaluationRun).where(
                StrategyEvaluationRun.strategy_id == strategy_id,
                StrategyEvaluationRun.idempotency_key == run_key,
            )
        ).scalars().first()
        if existing is not None:
            return _run_payload(existing, idempotent=True)
        raise

    open_positions = {
        trade.symbol.upper(): trade
        for trade in db.execute(
            select(StrategyTrade).where(
                StrategyTrade.strategy_id == strategy_id,
                StrategyTrade.status == "open",
                StrategyTrade.action == "buy",
            )
        ).scalars()
    }
    prices = {symbol.strip().upper(): price for symbol, price in (closing_prices or {}).items()}
    changes = {"added": 0, "exited": 0, "rebalanced": 0}

    for symbol in sorted(set(open_positions) - set(normalized)):
        prior = open_positions[symbol]
        prior.status = "closed"
        prior.exit_price = prices.get(symbol)
        prior.exit_reason = "no_longer_qualifies"
        exit_trade = StrategyTrade(
            strategy_id=strategy_id,
            strategy_version_id=strategy_version_id,
            strategy_run_id=run.id,
            security_id=prior.security_id,
            symbol=symbol,
            ticker_at_time=prior.ticker_at_time,
            action="sell",
            status="completed",
            signal_date=evaluation_date,
            effective_date=evaluation_date,
            exit_price=prices.get(symbol),
            weight_pct=prior.weight_pct,
            score_at_exit=prior.score_at_entry,
            qualification_snapshot_json=_json({"priorTradeId": int(prior.id)}),
            exit_reason="no_longer_qualifies",
        )
        db.add(exit_trade)
        db.flush()
        if not initialize:
            _event(
                db,
                strategy_id=strategy_id,
                strategy_version_id=strategy_version_id,
                run_id=run.id,
                trade=exit_trade,
                event_type="trade_exited",
                occurred_at=occurred_at,
                dedupe_key=f"strategy:{strategy_id}:run:{run.id}:trade_exited:{symbol}",
                payload={"reason": "no_longer_qualifies", "weightPct": prior.weight_pct},
            )
        changes["exited"] += 1

    for symbol, candidate in sorted(normalized.items()):
        prior = open_positions.get(symbol)
        effective_date = candidate.effective_date or evaluation_date
        if prior is None:
            trade = StrategyTrade(
                strategy_id=strategy_id,
                strategy_version_id=strategy_version_id,
                strategy_run_id=run.id,
                security_id=candidate.security_id,
                symbol=symbol,
                ticker_at_time=(candidate.ticker_at_time or symbol).strip().upper(),
                action="buy",
                status="open",
                signal_date=evaluation_date,
                effective_date=effective_date,
                entry_price=candidate.entry_price,
                weight_pct=candidate.weight_pct,
                score_at_entry=candidate.score,
                source_count_at_entry=candidate.source_count,
                qualification_snapshot_json=_json(candidate.qualification_snapshot),
            )
            db.add(trade)
            db.flush()
            if not initialize:
                _event(
                    db,
                    strategy_id=strategy_id,
                    strategy_version_id=strategy_version_id,
                    run_id=run.id,
                    trade=trade,
                    event_type="trade_added",
                    occurred_at=occurred_at,
                    dedupe_key=f"strategy:{strategy_id}:run:{run.id}:trade_added:{symbol}",
                    payload={"weightPct": candidate.weight_pct, "score": candidate.score, "sourceCount": candidate.source_count},
                )
            changes["added"] += 1
        elif abs(float(prior.weight_pct or 0) - float(candidate.weight_pct)) > 0.0001:
            trade = StrategyTrade(
                strategy_id=strategy_id,
                strategy_version_id=strategy_version_id,
                strategy_run_id=run.id,
                security_id=candidate.security_id or prior.security_id,
                symbol=symbol,
                ticker_at_time=(candidate.ticker_at_time or prior.ticker_at_time or symbol).strip().upper(),
                action="rebalance",
                status="completed",
                signal_date=evaluation_date,
                effective_date=effective_date,
                weight_pct=candidate.weight_pct,
                score_at_entry=candidate.score,
                source_count_at_entry=candidate.source_count,
                qualification_snapshot_json=_json(candidate.qualification_snapshot),
            )
            db.add(trade)
            db.flush()
            if not initialize:
                _event(
                    db,
                    strategy_id=strategy_id,
                    strategy_version_id=strategy_version_id,
                    run_id=run.id,
                    trade=trade,
                    event_type="position_rebalanced",
                    occurred_at=occurred_at,
                    dedupe_key=f"strategy:{strategy_id}:run:{run.id}:position_rebalanced:{symbol}",
                    payload={"previousWeightPct": prior.weight_pct, "weightPct": candidate.weight_pct},
                )
            changes["rebalanced"] += 1

    run.status = "completed"
    run.executed_at = datetime.now(timezone.utc)
    run.metadata_json = _json({"changes": changes, "candidateSymbols": sorted(normalized), "initialization": bool(initialize)})
    db.flush()
    _refresh_live_holdings(db, strategy_id=strategy_id, run_id=int(run.id), evaluation_date=evaluation_date)
    if not initialize:
        _event(
            db,
            strategy_id=strategy_id,
            strategy_version_id=strategy_version_id,
            run_id=run.id,
            trade=None,
            event_type="rebalance_completed",
            occurred_at=occurred_at,
            dedupe_key=f"strategy:{strategy_id}:run:{run.id}:rebalance_completed",
            payload={"evaluationDate": evaluation_date.isoformat(), "changes": changes, "qualifyingCount": len(normalized)},
        )
    db.commit()
    return _run_payload(run, idempotent=False)
