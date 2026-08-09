"""Admin-facing immutable strategy version authoring and dry-run previews."""

from __future__ import annotations

import json
from datetime import date
from typing import Any

from fastapi import HTTPException
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models import StrategyDefinition, StrategyVersion
from app.services.strategy_candidate_resolver import (
    UnsupportedStrategyCandidateSource,
    resolve_strategy_candidates,
    validate_strategy_candidate_rules,
)


def _json(value: dict[str, Any] | None) -> str:
    return json.dumps(value or {}, sort_keys=True, separators=(",", ":"))


def _loads(value: str | None) -> dict[str, Any]:
    try:
        parsed = json.loads(value or "{}")
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _payload(version: StrategyVersion) -> dict[str, Any]:
    return {
        "id": int(version.id),
        "strategyId": int(version.strategy_id),
        "version": int(version.version),
        "status": version.status,
        "rules": _loads(version.rules_json),
        "parameters": _loads(version.parameters_json),
        "universe": _loads(version.universe_json),
        "methodology": version.methodology,
        "effectiveFrom": version.effective_from.isoformat() if version.effective_from else None,
        "effectiveTo": version.effective_to.isoformat() if version.effective_to else None,
        "createdBy": version.created_by,
        "createdAt": version.created_at.isoformat() if version.created_at else None,
    }


def _strategy(db: Session, slug: str) -> StrategyDefinition:
    strategy = db.execute(select(StrategyDefinition).where(StrategyDefinition.slug == slug)).scalars().first()
    if strategy is None:
        raise HTTPException(status_code=404, detail="Strategy not found.")
    return strategy


def _version(db: Session, strategy_id: int, version_id: int) -> StrategyVersion:
    version = db.get(StrategyVersion, version_id)
    if version is None or int(version.strategy_id) != int(strategy_id):
        raise HTTPException(status_code=404, detail="Strategy version not found.")
    return version


def list_strategy_versions(db: Session, *, slug: str) -> dict[str, Any]:
    strategy = _strategy(db, slug)
    versions = db.execute(
        select(StrategyVersion)
        .where(StrategyVersion.strategy_id == strategy.id)
        .order_by(StrategyVersion.version.desc(), StrategyVersion.id.desc())
    ).scalars().all()
    return {"strategyId": int(strategy.id), "items": [_payload(version) for version in versions]}


def create_strategy_version(
    db: Session,
    *,
    slug: str,
    rules: dict[str, Any],
    parameters: dict[str, Any] | None = None,
    universe: dict[str, Any] | None = None,
    methodology: str | None = None,
    effective_from: date | None = None,
    created_by: str | None = None,
) -> dict[str, Any]:
    strategy = _strategy(db, slug)
    if not isinstance(rules, dict):
        raise HTTPException(status_code=422, detail="Strategy rules must be an object.")
    latest = db.execute(
        select(func.max(StrategyVersion.version)).where(StrategyVersion.strategy_id == strategy.id)
    ).scalar_one()
    version = StrategyVersion(
        strategy_id=strategy.id,
        version=int(latest or 0) + 1,
        status="draft",
        rules_json=_json(rules),
        parameters_json=_json(parameters),
        universe_json=_json(universe),
        methodology=methodology,
        effective_from=effective_from,
        created_by=created_by or "admin_strategy_console",
    )
    db.add(version)
    db.commit()
    return _payload(version)


def approve_strategy_version(db: Session, *, slug: str, version_id: int) -> dict[str, Any]:
    strategy = _strategy(db, slug)
    version = _version(db, int(strategy.id), version_id)
    if version.status == "retired":
        raise HTTPException(status_code=422, detail="A retired strategy version cannot be approved.")
    try:
        validate_strategy_candidate_rules(_loads(version.rules_json))
    except (UnsupportedStrategyCandidateSource, ValueError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    version.status = "approved"
    db.commit()
    return _payload(version)


def preview_strategy_version(
    db: Session,
    *,
    slug: str,
    version_id: int,
    evaluation_date: date,
) -> dict[str, Any]:
    strategy = _strategy(db, slug)
    version = _version(db, int(strategy.id), version_id)
    try:
        resolution = resolve_strategy_candidates(
            db,
            strategy_version_id=int(version.id),
            evaluation_date=evaluation_date,
            allow_draft=True,
        )
    except (UnsupportedStrategyCandidateSource, ValueError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return {
        "strategyId": int(strategy.id),
        "version": _payload(version),
        "mode": "dry_run",
        "evaluationDate": evaluation_date.isoformat(),
        "availableAt": resolution.available_at.isoformat(),
        "source": resolution.source,
        "universeCount": resolution.universe_count,
        "qualifyingCount": len(resolution.candidates),
        "candidates": [
            {
                "symbol": candidate.symbol,
                "weightPct": candidate.weight_pct,
                "score": candidate.score,
                "sourceCount": candidate.source_count,
                "entryPrice": candidate.entry_price,
                "qualificationSnapshot": candidate.qualification_snapshot,
            }
            for candidate in resolution.candidates
        ],
    }
