"""Point-in-time candidate resolvers for prospective strategy versions."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models import ConfirmationScoreSnapshot, Event, OutcomeEntry, PriceCache, Security, StrategyVersion
from app.services.outcome_integrity import adjusted_price, as_utc
from app.services.replicated_portfolios import _portfolio_event_from_event
from app.services.strategy_evaluations import StrategyEvaluationCandidate


class UnsupportedStrategyCandidateSource(ValueError):
    pass


def _canonical_open(row: PriceCache | None) -> float | None:
    if row is None or row.adjustment_status != "split_adjusted_price_return":
        return None
    value = adjusted_price(row, "open")
    return float(value) if value is not None and value > 0 else None


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
    if source == "disclosure_portfolio":
        if str(rules.get("trade_source") or "").strip() not in {"congress", "insider"}:
            raise ValueError("Disclosure monitoring requires a Congress or insider source.")
        return
    if source == "cross_source_disclosure_alignment":
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


def _price_history(db: Session, symbol: str, evaluation_date: date, limit: int = 220) -> list[float]:
    rows = db.execute(
        select(PriceCache)
        .where(PriceCache.symbol == symbol, PriceCache.date <= evaluation_date.isoformat())
        .order_by(PriceCache.date.desc())
        .limit(limit)
    ).scalars().all()
    return [float(row.adjusted_close if row.adjusted_close is not None else row.close) for row in reversed(rows) if (row.adjusted_close if row.adjusted_close is not None else row.close) not in {None, 0}]


def _ema(values: list[float], length: int) -> float | None:
    if len(values) < length:
        return None
    factor = 2.0 / (length + 1)
    value = sum(values[:length]) / length
    for price in values[length:]:
        value = price * factor + value * (1.0 - factor)
    return value


def _passes_technical_rule(db: Session, symbol: str, evaluation_date: date, rule: str | None) -> bool:
    if not rule:
        return True
    prices = _price_history(db, symbol, evaluation_date)
    if rule in {"price_above_sma50_sma200", "technical_alignment"}:
        if len(prices) < 200:
            return False
        return prices[-1] > sum(prices[-50:]) / 50 and prices[-1] > sum(prices[-200:]) / 200
    if rule == "macd_bullish":
        if len(prices) < 35:
            return False
        macd_series: list[float] = []
        for index in range(26, len(prices) + 1):
            fast = _ema(prices[:index], 12)
            slow = _ema(prices[:index], 26)
            if fast is not None and slow is not None:
                macd_series.append(fast - slow)
        signal = _ema(macd_series, 9)
        return bool(macd_series and signal is not None and macd_series[-1] > signal)
    raise UnsupportedStrategyCandidateSource(f"Unsupported technical rule: {rule}.")


def _candidate_from_event(
    db: Session,
    *,
    event: Event,
    portfolio_event: Any,
    evaluation_date: date,
    source_count: int,
    source: str,
) -> StrategyEvaluationCandidate | None:
    symbol = portfolio_event.symbol
    security = db.execute(select(Security).where(func.upper(Security.symbol) == symbol)).scalars().first()
    execution_floor = evaluation_date + timedelta(days=1)
    price_row = db.execute(
        select(PriceCache)
        .where(PriceCache.symbol == symbol, PriceCache.date >= execution_floor.isoformat())
        .order_by(PriceCache.date.asc())
        .limit(1)
    ).scalar_one_or_none()
    return StrategyEvaluationCandidate(
        symbol=symbol,
        ticker_at_time=symbol,
        security_id=int(security.id) if security else None,
        weight_pct=1.0,
        entry_price=_canonical_open(price_row),
        effective_date=date.fromisoformat(price_row.date) if price_row else execution_floor,
        source_count=source_count,
        qualification_snapshot={
            "source": source,
            "eventId": int(event.id),
            "publicDate": portfolio_event.public_date.isoformat(),
            "ingestedAt": event.created_at.isoformat() if event.created_at else None,
            "execution": "next_trading_session_official_open",
            "companyName": security.name if security else None,
            "sector": security.sector if security else None,
        },
    )


def _resolve_disclosure_portfolio_candidates(
    db: Session,
    *,
    version: StrategyVersion,
    rules: dict[str, Any],
    evaluation_date: date,
    visible_at: datetime,
) -> StrategyCandidateResolution:
    trade_source = str(rules["trade_source"])
    entity_type = "insider" if trade_source == "insider" else "congress_member"
    event_type = "insider_trade" if trade_source == "insider" else "congress_trade"
    start_at = datetime.combine(version.effective_from or evaluation_date, time.min, tzinfo=timezone.utc)
    rows = db.execute(
        select(Event)
        .where(Event.event_type == event_type, Event.created_at >= start_at, Event.created_at <= visible_at)
        .order_by(Event.created_at.asc(), Event.id.asc())
    ).scalars().all()
    open_positions: dict[str, tuple[Event, Any]] = {}
    eligible = 0
    for event in rows:
        portfolio_event, _skip = _portfolio_event_from_event(event, entity_type=entity_type, entity_id="strategy", db=db)
        if portfolio_event is None or portfolio_event.public_date > evaluation_date:
            continue
        eligible += 1
        if portfolio_event.side == "purchase":
            open_positions[portfolio_event.symbol] = (event, portfolio_event)
        else:
            open_positions.pop(portfolio_event.symbol, None)
    technical_rule = rules.get("technical_rule")
    holding_period_days = max(1, min(730, int(rules.get("holding_period_days") or 90)))
    candidates = [
        _candidate_from_event(db, event=event, portfolio_event=portfolio_event, evaluation_date=evaluation_date, source_count=1, source=event_type)
        for symbol, (event, portfolio_event) in sorted(open_positions.items())
        if portfolio_event.public_date + timedelta(days=holding_period_days) >= evaluation_date
        and _passes_technical_rule(db, symbol, evaluation_date, technical_rule)
    ]
    candidates = [candidate for candidate in candidates if candidate is not None]
    weight = round(100.0 / len(candidates), 8) if candidates else 0.0
    candidates = [StrategyEvaluationCandidate(**{**candidate.__dict__, "weight_pct": weight}) for candidate in candidates]
    return StrategyCandidateResolution(source="disclosure_portfolio", candidates=candidates, universe_count=eligible, available_at=visible_at)


def _resolve_cross_source_candidates(
    db: Session,
    *,
    version: StrategyVersion,
    rules: dict[str, Any],
    evaluation_date: date,
    visible_at: datetime,
) -> StrategyCandidateResolution:
    start_at = datetime.combine(version.effective_from or evaluation_date, time.min, tzinfo=timezone.utc)
    lookback = max(1, min(365, int(rules.get("alignment_lookback_days") or 90)))
    cutoff = evaluation_date - timedelta(days=lookback)
    rows = db.execute(
        select(Event)
        .where(Event.event_type.in_(["congress_trade", "insider_trade"]), Event.created_at >= start_at, Event.created_at <= visible_at)
        .order_by(Event.created_at.asc(), Event.id.asc())
    ).scalars().all()
    source_events: dict[str, dict[str, tuple[Event, Any]]] = {}
    eligible = 0
    for event in rows:
        entity_type = "insider" if event.event_type == "insider_trade" else "congress_member"
        portfolio_event, _skip = _portfolio_event_from_event(event, entity_type=entity_type, entity_id="strategy", db=db)
        if portfolio_event is None or portfolio_event.public_date > evaluation_date or portfolio_event.public_date < cutoff:
            continue
        eligible += 1
        by_source = source_events.setdefault(portfolio_event.symbol, {})
        if portfolio_event.side == "purchase":
            by_source[event.event_type] = (event, portfolio_event)
        else:
            by_source.pop(event.event_type, None)
    candidates = [
        _candidate_from_event(db, event=values["insider_trade"][0], portfolio_event=values["insider_trade"][1], evaluation_date=evaluation_date, source_count=2, source="congress_insider_alignment")
        for _symbol, values in sorted(source_events.items())
        if {"congress_trade", "insider_trade"}.issubset(values)
        and values["insider_trade"][1].public_date + timedelta(days=max(1, min(730, int(rules.get("holding_period_days") or 90)))) >= evaluation_date
    ]
    candidates = [candidate for candidate in candidates if candidate is not None]
    weight = round(100.0 / len(candidates), 8) if candidates else 0.0
    candidates = [StrategyEvaluationCandidate(**{**candidate.__dict__, "weight_pct": weight}) for candidate in candidates]
    return StrategyCandidateResolution(source="cross_source_disclosure_alignment", candidates=candidates, universe_count=eligible, available_at=visible_at)


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
        entry_price = _canonical_open(price_row)
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
                    "execution": "next_trading_session_official_open",
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
    if source == "disclosure_portfolio":
        return _resolve_disclosure_portfolio_candidates(
            db,
            version=version,
            rules=rules,
            evaluation_date=evaluation_date,
            visible_at=visible_at,
        )
    if source == "cross_source_disclosure_alignment":
        return _resolve_cross_source_candidates(
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
    entries_by_snapshot = {
        int(entry.snapshot_id): entry
        for entry in db.execute(
            select(OutcomeEntry).where(OutcomeEntry.snapshot_id.in_([int(item[0].id) for item in ordered]))
        ).scalars().all()
    } if ordered else {}
    candidates = [
        StrategyEvaluationCandidate(
            symbol=snapshot.ticker_at_time,
            ticker_at_time=snapshot.ticker_at_time,
            security_id=int(snapshot.security_id),
            weight_pct=weight,
            score=float(snapshot.score),
            source_count=int(snapshot.active_source_count),
            entry_price=entries_by_snapshot[int(snapshot.id)].entry_price,
            effective_date=entries_by_snapshot[int(snapshot.id)].entry_session_date,
            qualification_snapshot={
                "source": source,
                "confirmationSnapshotId": int(snapshot.id),
                "marketDate": snapshot.market_date.isoformat(),
                "calculatedAt": snapshot.calculated_at.isoformat(),
                "entryPriceAt": entries_by_snapshot[int(snapshot.id)].entry_price_at.isoformat(),
                "entryPriceType": entries_by_snapshot[int(snapshot.id)].entry_price_type,
                "methodologyVersionId": int(snapshot.methodology_version_id),
                "activeSources": json.loads(snapshot.active_sources_json or "[]"),
                "sourceContributions": json.loads(snapshot.source_contributions_json or "{}"),
                "companyName": security.name if security else None,
                "sector": security.sector if security else None,
            },
        )
        for snapshot, security in ordered
        if int(snapshot.id) in entries_by_snapshot
        and as_utc(entries_by_snapshot[int(snapshot.id)].entry_price_at) <= as_utc(visible_at)
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
