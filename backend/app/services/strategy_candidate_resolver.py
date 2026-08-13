"""Point-in-time candidate resolvers for prospective strategy versions."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import ConfirmationScoreSnapshot, Event, PriceCache, Security, StrategyVersion
from app.services.replicated_portfolios import _portfolio_event_from_event
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


def validate_strategy_candidate_rules(rules: dict[str, Any]) -> None:
    source = str(rules.get("candidate_source") or "").strip()
    if source == "congress_member_disclosures":
        if not str(rules.get("member_bioguide_id") or "").strip():
            raise ValueError("Congress monitoring requires a member bioguide id.")
        return
    if source != "confirmation_score_snapshots":
        raise UnsupportedStrategyCandidateSource(f"Unsupported prospective candidate source: {source or 'missing'}.")
    if str(rules.get("direction") or "bullish").lower() != "bullish":
        raise UnsupportedStrategyCandidateSource("The current resolver supports long-only bullish confirmation candidates.")
    try:
        int(rules.get("min_score") or 60)
        int(rules.get("min_active_sources") or 1)
        int(rules.get("max_positions") or 10)
        int(rules.get("max_snapshot_age_days") or 3)
    except (TypeError, ValueError) as exc:
        raise ValueError("Confirmation strategy thresholds must be integers.") from exc


def _resolve_congress_member_candidates(
    db: Session,
    *,
    version: StrategyVersion,
    rules: dict[str, Any],
    evaluation_date: date,
    visible_at: datetime,
) -> StrategyCandidateResolution:
    """Replay only post-activation, publicly ingested member filings.

    A prospective strategy starts monitoring when its immutable version becomes
    effective.  It never converts an older backtest into a live trade ledger.
    This protects subscribers from a baseline of historical alerts while keeping
    every later decision tied to the filing that Walnut had actually ingested.
    """
    member_id = str(rules.get("member_bioguide_id") or "").strip().upper()
    monitor_start = version.effective_from or evaluation_date
    start_at = datetime.combine(monitor_start, time.min, tzinfo=timezone.utc)
    rows = (
        db.execute(
            select(Event)
            .where(
                Event.event_type == "congress_trade",
                Event.member_bioguide_id == member_id,
                Event.created_at >= start_at,
                Event.created_at <= visible_at,
            )
            .order_by(Event.created_at.asc(), Event.id.asc())
        )
        .scalars()
        .all()
    )

    open_positions: dict[str, tuple[Event, Any]] = {}
    eligible_events = 0
    for event in rows:
        portfolio_event, _skip = _portfolio_event_from_event(
            event,
            entity_type="congress_member",
            entity_id=member_id,
            db=db,
        )
        if portfolio_event is None or portfolio_event.public_date > evaluation_date:
            continue
        eligible_events += 1
        if portfolio_event.side == "purchase":
            open_positions[portfolio_event.symbol] = (event, portfolio_event)
        else:
            # Range disclosures cannot reliably quantify partial sales.  The
            # prospective rule therefore exits the modeled issuer position on a
            # matching public sale, exactly as the disclosed-sale methodology says.
            open_positions.pop(portfolio_event.symbol, None)

    symbols = sorted(open_positions)
    securities = {
        str(security.symbol).upper(): security
        for security in db.execute(select(Security).where(Security.symbol.in_(symbols))).scalars()
        if security.symbol
    }
    weight = round(100.0 / len(symbols), 8) if symbols else 0.0
    candidates: list[StrategyEvaluationCandidate] = []
    for symbol in symbols:
        event, portfolio_event = open_positions[symbol]
        security = securities.get(symbol)
        execution_floor = evaluation_date + timedelta(days=1)
        price_row = db.execute(
            select(PriceCache)
            .where(PriceCache.symbol == symbol, PriceCache.date >= execution_floor.isoformat())
            .order_by(PriceCache.date.asc())
            .limit(1)
        ).scalar_one_or_none()
        effective_date = date.fromisoformat(price_row.date) if price_row is not None else execution_floor
        entry_price = (
            float(price_row.adjusted_close if price_row.adjusted_close is not None else price_row.close)
            if price_row is not None and (price_row.adjusted_close is not None or price_row.close is not None)
            else None
        )
        candidates.append(
            StrategyEvaluationCandidate(
                symbol=symbol,
                ticker_at_time=symbol,
                security_id=int(security.id) if security else None,
                weight_pct=weight,
                # The first cached market date after daily ingestion controls the
                # model execution. If the price has not arrived yet, preserve the
                # next-day schedule without inventing an execution price.
                entry_price=entry_price,
                effective_date=effective_date,
                source_count=1,
                qualification_snapshot={
                    "source": "congress_member_disclosures",
                    "eventId": int(event.id),
                    "memberBioguideId": member_id,
                    "memberName": event.member_name,
                    "publicDate": portfolio_event.public_date.isoformat(),
                    "ingestedAt": event.created_at.isoformat() if event.created_at else None,
                    "execution": "next_trading_day_after_daily_ingest",
                    "executionPriceDate": effective_date.isoformat() if price_row is not None else None,
                    "exitRule": "matching_reported_sale",
                    "companyName": security.name if security else None,
                    "sector": security.sector if security else None,
                },
            )
        )
    return StrategyCandidateResolution(
        source="congress_member_disclosures",
        candidates=candidates,
        universe_count=eligible_events,
        available_at=visible_at,
    )


def resolve_strategy_candidates(
    db: Session,
    *,
    strategy_version_id: int,
    evaluation_date: date,
    available_at: datetime | None = None,
    allow_draft: bool = False,
) -> StrategyCandidateResolution:
    """Resolve candidates using only data visible by ``available_at``.

    Every source owns its own point-in-time resolver.  None may use a current
    score or portfolio state as though it had existed on an earlier day.
    """
    version = db.get(StrategyVersion, strategy_version_id)
    if version is None:
        raise ValueError(f"Unknown strategy version id {strategy_version_id}.")
    allowed_statuses = {"approved", "active"}
    if allow_draft:
        allowed_statuses.add("draft")
    if version.status not in allowed_statuses:
        raise ValueError("Only approved or active strategy versions may be evaluated.")
    rules = _load_json(version.rules_json)
    validate_strategy_candidate_rules(rules)
    source = str(rules.get("candidate_source") or "").strip()
    visible_at = _available_at(evaluation_date, available_at)
    if source == "congress_member_disclosures":
        return _resolve_congress_member_candidates(
            db,
            version=version,
            rules=rules,
            evaluation_date=evaluation_date,
            visible_at=visible_at,
        )
    direction = "bullish"
    min_score = max(0, min(100, int(rules.get("min_score") or 60)))
    min_sources = max(0, int(rules.get("min_active_sources") or 1))
    max_positions = max(1, min(100, int(rules.get("max_positions") or 10)))
    max_snapshot_age_days = max(0, min(30, int(rules.get("max_snapshot_age_days") or 3)))
    cutoff = evaluation_date - timedelta(days=max_snapshot_age_days)

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
