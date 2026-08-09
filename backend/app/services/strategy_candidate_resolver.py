"""Point-in-time candidate resolvers for prospective strategy versions."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import ConfirmationScoreSnapshot, Security, StrategyVersion
from app.services.strategy_evaluations import StrategyEvaluationCandidate


class UnsupportedStrategyCandidateSource(ValueError):
    pass


@dataclass(frozen=True)
class StrategyCandidateResolution:
    source: str
    candidates: list[StrategyEvaluationCandidate]
    universe_count: int
    available_at: datetime


def _load_json(value: str | None) -> dict[str, Any]:
    try:
        parsed = json.loads(value or "{}")
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _available_at(evaluation_date: date, value: datetime | None) -> datetime:
    if value is None:
        return datetime.combine(evaluation_date, time.max, tzinfo=timezone.utc)
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


def resolve_strategy_candidates(
    db: Session,
    *,
    strategy_version_id: int,
    evaluation_date: date,
    available_at: datetime | None = None,
) -> StrategyCandidateResolution:
    """Resolve candidates using only data visible by ``available_at``.

    This intentionally supports the immutable confirmation ledger first.  Other
    rule sources must add their own public-availability-aware resolver rather than
    treating current data as if it existed historically.
    """
    version = db.get(StrategyVersion, strategy_version_id)
    if version is None:
        raise ValueError(f"Unknown strategy version id {strategy_version_id}.")
    if version.status not in {"approved", "active"}:
        raise ValueError("Only approved or active strategy versions may be evaluated.")
    rules = _load_json(version.rules_json)
    source = str(rules.get("candidate_source") or "").strip()
    if source != "confirmation_score_snapshots":
        raise UnsupportedStrategyCandidateSource(f"Unsupported prospective candidate source: {source or 'missing'}.")

    direction = str(rules.get("direction") or "bullish").lower()
    if direction != "bullish":
        raise UnsupportedStrategyCandidateSource("The current resolver supports long-only bullish confirmation candidates.")
    min_score = max(0, min(100, int(rules.get("min_score") or 60)))
    min_sources = max(0, int(rules.get("min_active_sources") or 1))
    max_positions = max(1, min(100, int(rules.get("max_positions") or 10)))
    max_snapshot_age_days = max(0, min(30, int(rules.get("max_snapshot_age_days") or 3)))
    cutoff = evaluation_date - timedelta(days=max_snapshot_age_days)
    visible_at = _available_at(evaluation_date, available_at)

    rows = db.execute(
        select(ConfirmationScoreSnapshot, Security)
        .outerjoin(Security, Security.id == ConfirmationScoreSnapshot.security_id)
        .where(
            ConfirmationScoreSnapshot.calculation_type == "live",
            ConfirmationScoreSnapshot.direction == direction,
            ConfirmationScoreSnapshot.score >= min_score,
            ConfirmationScoreSnapshot.active_source_count >= min_sources,
            ConfirmationScoreSnapshot.market_date >= cutoff,
            ConfirmationScoreSnapshot.market_date <= evaluation_date,
            ConfirmationScoreSnapshot.calculated_at <= visible_at,
        )
        .order_by(
            ConfirmationScoreSnapshot.security_id.asc(),
            ConfirmationScoreSnapshot.market_date.desc(),
            ConfirmationScoreSnapshot.calculated_at.desc(),
            ConfirmationScoreSnapshot.id.desc(),
        )
    ).all()

    latest_by_security: dict[int, tuple[ConfirmationScoreSnapshot, Security | None]] = {}
    for snapshot, security in rows:
        latest_by_security.setdefault(int(snapshot.security_id), (snapshot, security))

    ordered = sorted(
        latest_by_security.values(),
        key=lambda item: (-int(item[0].score), -int(item[0].active_source_count), item[0].ticker_at_time),
    )[:max_positions]
    weight = round(100.0 / len(ordered), 8) if ordered else 0.0
    candidates = [
        StrategyEvaluationCandidate(
            symbol=snapshot.ticker_at_time,
            ticker_at_time=snapshot.ticker_at_time,
            security_id=int(snapshot.security_id),
            weight_pct=weight,
            score=float(snapshot.score),
            source_count=int(snapshot.active_source_count),
            entry_price=snapshot.reference_price,
            effective_date=evaluation_date,
            qualification_snapshot={
                "source": source,
                "confirmationSnapshotId": int(snapshot.id),
                "marketDate": snapshot.market_date.isoformat(),
                "calculatedAt": snapshot.calculated_at.isoformat(),
                "methodologyVersionId": int(snapshot.methodology_version_id),
                "activeSources": json.loads(snapshot.active_sources_json or "[]"),
                "sourceContributions": json.loads(snapshot.source_contributions_json or "{}"),
                "companyName": security.name if security else None,
                "sector": security.sector if security else None,
            },
        )
        for snapshot, security in ordered
        if snapshot.reference_price is not None and float(snapshot.reference_price) > 0
    ]
    if candidates and len(candidates) != len(ordered):
        weight = round(100.0 / len(candidates), 8)
        candidates = [
            StrategyEvaluationCandidate(
                **{**candidate.__dict__, "weight_pct": weight},
            )
            for candidate in candidates
        ]
    return StrategyCandidateResolution(
        source=source,
        candidates=candidates,
        universe_count=len(latest_by_security),
        available_at=visible_at,
    )
