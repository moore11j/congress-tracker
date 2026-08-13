"""Opt-in scheduled evaluation of explicitly active prospective strategies."""

from __future__ import annotations

import json
import os
from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import AppSetting, StrategyDefinition, StrategyEvaluationRun, StrategyVersion
from app.services.strategy_candidate_resolver import resolve_strategy_candidates
from app.services.strategy_evaluations import evaluate_strategy_candidates

SCHEDULER_STATUS_KEY = "strategy_evaluation_scheduler_status"


def _enabled() -> bool:
    return os.getenv("STRATEGY_EVALUATIONS_ENABLED", "false").strip().lower() in {"1", "true", "yes", "on"}


def _max_strategies() -> int:
    try:
        return max(1, min(25, int(os.getenv("STRATEGY_EVALUATIONS_MAX_STRATEGIES", "5") or 5)))
    except ValueError:
        return 5


def _loads(value: str | None) -> dict:
    try:
        parsed = json.loads(value or "{}")
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _write_status(db: Session, payload: dict) -> None:
    row = db.get(AppSetting, SCHEDULER_STATUS_KEY)
    if row is None:
        row = AppSetting(key=SCHEDULER_STATUS_KEY)
        db.add(row)
    row.value = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    db.commit()


def scheduler_status(db: Session) -> dict:
    row = db.get(AppSetting, SCHEDULER_STATUS_KEY)
    return {
        "enabled": _enabled(),
        "maxStrategiesPerRun": _max_strategies(),
        "lastRun": _loads(row.value if row else None) or None,
    }


def _record_failed_run(
    db: Session,
    *,
    strategy_id: int,
    strategy_version_id: int,
    evaluation_date: date,
    scheduled_for: datetime,
    error: Exception,
) -> None:
    key = f"strategy:{strategy_id}:version:{strategy_version_id}:evaluation:{evaluation_date.isoformat()}"
    existing = db.execute(
        select(StrategyEvaluationRun).where(
            StrategyEvaluationRun.strategy_id == strategy_id,
            StrategyEvaluationRun.idempotency_key == key,
        )
    ).scalars().first()
    if existing is not None:
        return
    db.add(
        StrategyEvaluationRun(
            strategy_id=strategy_id,
            strategy_version_id=strategy_version_id,
            idempotency_key=key,
            evaluation_date=evaluation_date,
            scheduled_for=scheduled_for,
            executed_at=datetime.now(timezone.utc),
            status="failed",
            metadata_json=json.dumps({"scheduler": "strategy_evaluations"}, sort_keys=True),
            error=f"{error.__class__.__name__}: {str(error)[:500]}",
        )
    )
    db.commit()


def run_active_strategy_evaluations(
    db: Session,
    *,
    scheduled_for: datetime | None = None,
) -> dict:
    """Run active versions only; disabled configuration exits without touching portfolios."""
    now = scheduled_for or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    evaluation_date = now.astimezone(ZoneInfo("America/Los_Angeles")).date()
    limit = _max_strategies()
    if not _enabled():
        payload = {
            "status": "disabled",
            "scheduledFor": now.isoformat(),
            "evaluationDate": evaluation_date.isoformat(),
            "maxStrategiesPerRun": limit,
            "processed": 0,
            "failed": 0,
            "reason": "strategy_evaluations_disabled",
        }
        _write_status(db, payload)
        return payload

    active = db.execute(
        select(StrategyDefinition, StrategyVersion)
        .join(StrategyVersion, StrategyVersion.strategy_id == StrategyDefinition.id)
        .where(StrategyDefinition.status == "published", StrategyVersion.status == "active")
        .where((StrategyVersion.effective_from.is_(None)) | (StrategyVersion.effective_from <= evaluation_date))
        .where((StrategyVersion.effective_to.is_(None)) | (StrategyVersion.effective_to >= evaluation_date))
        .order_by(StrategyDefinition.sort_order.asc(), StrategyDefinition.id.asc(), StrategyVersion.version.desc())
        .limit(limit)
    ).all()
    results: list[dict] = []
    failures: list[dict] = []
    for strategy, version in active:
        try:
            is_initialization = db.execute(
                select(StrategyEvaluationRun.id)
                .where(StrategyEvaluationRun.strategy_id == strategy.id, StrategyEvaluationRun.strategy_version_id == version.id)
                .limit(1)
            ).scalar_one_or_none() is None
            resolution = resolve_strategy_candidates(
                db,
                strategy_version_id=int(version.id),
                evaluation_date=evaluation_date,
                available_at=now,
            )
            result = evaluate_strategy_candidates(
                db,
                strategy_id=int(strategy.id),
                strategy_version_id=int(version.id),
                evaluation_date=evaluation_date,
                candidates=resolution.candidates,
                universe_count=resolution.universe_count,
                scheduled_for=now,
                initialize=is_initialization,
            )
            results.append({"slug": strategy.slug, "version": int(version.version), "initialization": is_initialization, **result})
        except Exception as exc:
            db.rollback()
            _record_failed_run(
                db,
                strategy_id=int(strategy.id),
                strategy_version_id=int(version.id),
                evaluation_date=evaluation_date,
                scheduled_for=now,
                error=exc,
            )
            failures.append({"slug": strategy.slug, "version": int(version.version), "error": exc.__class__.__name__})

    payload = {
        "status": "partial" if failures else "ok",
        "scheduledFor": now.isoformat(),
        "evaluationDate": evaluation_date.isoformat(),
        "maxStrategiesPerRun": limit,
        "eligibleActiveVersions": len(active),
        "processed": len(results),
        "failed": len(failures),
        "results": results,
        "failures": failures,
    }
    _write_status(db, payload)
    return payload
