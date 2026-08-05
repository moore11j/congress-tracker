from __future__ import annotations

import math
import os
from bisect import bisect_left
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models import (
    AnalystConsensusSnapshot,
    AnalystGradeEvent,
    ConfirmationMonitoringEvent,
    ConfirmationMonitoringSnapshot,
    PriceCache,
)
from app.utils.symbols import normalize_symbol

METHODOLOGY_VERSION = "analyst_consensus_shadow_review_v1"
DEFAULT_LOOKBACK_DAYS = 365
DEFAULT_HORIZON_DAYS = 30
DEFAULT_MAX_SNAPSHOTS = 5000
DEFAULT_MIN_BACKTEST_SAMPLES = 100
DEFAULT_MIN_BACKTEST_SYMBOLS = 10
DEFAULT_MAX_CONFIRMATION_CORRELATION = 0.65


def analyst_consensus_live_weight_enabled() -> bool:
    return str(os.getenv("ANALYST_CONSENSUS_LIVE_WEIGHT_ENABLED", "0")).strip().lower() in {
        "1",
        "true",
        "on",
        "yes",
    }


def analyst_consensus_shadow_component_score(inputs: dict[str, Any]) -> int | None:
    if not isinstance(inputs, dict):
        return None
    rating = _number(inputs.get("weightedRatingValue"))
    upside = _number(inputs.get("consensusImpliedUpsidePct"))
    if rating is None and upside is None:
        return None
    score = 50.0
    if rating is not None:
        score += max(-25.0, min(25.0, rating * 12.5))
    if upside is not None:
        score += max(-20.0, min(20.0, upside / 2.0))
    return int(round(max(0.0, min(100.0, score))))


def analyst_consensus_confidence_adjustment(inputs: dict[str, Any]) -> float:
    level = str(inputs.get("coverageLevel") or "").strip().lower()
    freshness = str(inputs.get("freshnessStatus") or "").strip().lower()
    coverage_factor = {"high": 1.0, "moderate": 0.75, "low": 0.5, "insufficient": 0.25}.get(level, 0.25)
    freshness_factor = {"fresh": 1.0, "stale": 0.6, "unavailable": 0.25}.get(freshness, 0.8)
    return round(max(0.0, min(1.0, coverage_factor * freshness_factor)), 2)


def shadow_review_payload(
    db: Session,
    *,
    symbols: Iterable[str] | None = None,
    days: int = DEFAULT_LOOKBACK_DAYS,
    horizon_days: int = DEFAULT_HORIZON_DAYS,
    max_snapshots: int = DEFAULT_MAX_SNAPSHOTS,
    min_backtest_samples: int | None = None,
    min_backtest_symbols: int | None = None,
    max_confirmation_correlation: float | None = None,
    as_of: date | None = None,
) -> dict[str, Any]:
    end_day = as_of or datetime.now(timezone.utc).date()
    bounded_days = max(1, min(int(days or DEFAULT_LOOKBACK_DAYS), 1825))
    bounded_horizon = max(1, min(int(horizon_days or DEFAULT_HORIZON_DAYS), 365))
    bounded_limit = max(1, min(int(max_snapshots or DEFAULT_MAX_SNAPSHOTS), 25000))
    min_samples = _env_int("ANALYST_CONSENSUS_MIN_BACKTEST_SAMPLES", DEFAULT_MIN_BACKTEST_SAMPLES)
    if min_backtest_samples is not None:
        min_samples = max(1, int(min_backtest_samples))
    min_symbols = _env_int("ANALYST_CONSENSUS_MIN_BACKTEST_SYMBOLS", DEFAULT_MIN_BACKTEST_SYMBOLS)
    if min_backtest_symbols is not None:
        min_symbols = max(1, int(min_backtest_symbols))
    max_corr = _env_float("ANALYST_CONSENSUS_MAX_CONFIRMATION_CORRELATION", DEFAULT_MAX_CONFIRMATION_CORRELATION)
    if max_confirmation_correlation is not None:
        max_corr = max(0.0, min(1.0, float(max_confirmation_correlation)))

    normalized_symbols = sorted({symbol for symbol in (normalize_symbol(item) for item in (symbols or [])) if symbol})
    start_day = end_day - timedelta(days=bounded_days - 1)
    snapshots = _load_snapshots(db, start_day, end_day, normalized_symbols, bounded_limit)
    snapshot_samples = _forward_return_samples(db, snapshots, bounded_horizon)
    historical_grade_events = _load_grade_events(db, start_day, end_day, normalized_symbols, bounded_limit)
    grade_event_samples = _grade_event_forward_return_samples(db, historical_grade_events, bounded_horizon)
    samples = snapshot_samples + grade_event_samples
    confirmation_samples = _confirmation_correlation_samples(db, snapshots, historical_grade_events)
    backtest = _backtest_summary(samples, min_samples=min_samples, min_symbols=min_symbols, horizon_days=bounded_horizon)
    correlation = _correlation_summary(
        samples,
        confirmation_samples,
        min_samples=min_samples,
        max_confirmation_correlation=max_corr,
    )
    double_counting = _double_counting_summary(correlation)
    activation = _activation_review(
        backtest,
        correlation,
        double_counting,
        live_weight_enabled=analyst_consensus_live_weight_enabled(),
    )
    return {
        "methodologyVersion": METHODOLOGY_VERSION,
        "activationState": "shadow",
        "includedInLiveScore": False,
        "liveWeightAssigned": False,
        "liveWeightFlagEnabled": analyst_consensus_live_weight_enabled(),
        "scope": {
            "symbols": normalized_symbols,
            "days": bounded_days,
            "horizonDays": bounded_horizon,
            "maxSnapshots": bounded_limit,
            "startDate": start_day.isoformat(),
            "endDate": end_day.isoformat(),
        },
        "coverage": {
            "snapshotCount": len(snapshots),
            "sampleCount": len(samples),
            "snapshotSampleCount": len(snapshot_samples),
            "historicalGradeEventCount": len(historical_grade_events),
            "historicalGradeEventSampleCount": len(grade_event_samples),
            "symbolCount": len({sample["symbol"] for sample in samples}),
            "confirmationCorrelationSampleCount": len(confirmation_samples),
        },
        "backtest": backtest,
        "correlationReview": correlation,
        "doubleCountingReview": double_counting,
        "activationReview": activation,
        "notes": [
            "Analyst consensus remains shadow-only and is excluded from Walnut's live confirmation score.",
            "Live weighting requires positive backtest coverage, acceptable correlation, double-counting review, and explicit activation approval.",
        ],
    }


def _load_snapshots(
    db: Session,
    start_day: date,
    end_day: date,
    symbols: list[str],
    max_snapshots: int,
) -> list[AnalystConsensusSnapshot]:
    filters = [
        AnalystConsensusSnapshot.snapshot_date >= start_day,
        AnalystConsensusSnapshot.snapshot_date <= end_day,
        AnalystConsensusSnapshot.availability_status.in_(("available", "partial")),
    ]
    if symbols:
        filters.append(func.upper(AnalystConsensusSnapshot.symbol).in_(symbols))
    return list(
        db.execute(
            select(AnalystConsensusSnapshot)
            .where(*filters)
            .order_by(AnalystConsensusSnapshot.snapshot_date.asc(), AnalystConsensusSnapshot.symbol.asc())
            .limit(max_snapshots)
        )
        .scalars()
        .all()
    )


def _forward_return_samples(
    db: Session,
    snapshots: list[AnalystConsensusSnapshot],
    horizon_days: int,
) -> list[dict[str, Any]]:
    if not snapshots:
        return []
    symbols = sorted({snapshot.symbol.upper() for snapshot in snapshots if snapshot.symbol})
    min_day = min(snapshot.snapshot_date for snapshot in snapshots)
    max_day = max(snapshot.snapshot_date for snapshot in snapshots) + timedelta(days=horizon_days + 7)
    prices = _load_prices(db, symbols, min_day, max_day)
    samples: list[dict[str, Any]] = []
    for snapshot in snapshots:
        symbol = (snapshot.symbol or "").upper()
        points = prices.get(symbol) or []
        start_price = _first_close_on_or_after(points, snapshot.snapshot_date)
        end_price = _first_close_on_or_after(points, snapshot.snapshot_date + timedelta(days=horizon_days))
        if start_price is None or end_price is None or start_price <= 0:
            continue
        inputs = _inputs_from_snapshot(snapshot)
        component_score = analyst_consensus_shadow_component_score(inputs)
        if component_score is None:
            continue
        samples.append(
            {
                "symbol": symbol,
                "snapshotDate": snapshot.snapshot_date.isoformat(),
                "componentScore": component_score,
                "sourceType": "consensus_snapshot",
                "weightedRatingValue": _number(snapshot.weighted_rating_value),
                "consensusImpliedUpsidePct": _number(snapshot.consensus_implied_upside_pct),
                "forwardReturnPct": round(((end_price / start_price) - 1.0) * 100.0, 4),
                "horizonDays": horizon_days,
            }
        )
    return samples


def _load_grade_events(
    db: Session,
    start_day: date,
    end_day: date,
    symbols: list[str],
    max_events: int,
) -> list[AnalystGradeEvent]:
    filters = [
        AnalystGradeEvent.published_date >= start_day,
        AnalystGradeEvent.published_date <= end_day,
    ]
    if symbols:
        filters.append(func.upper(AnalystGradeEvent.symbol).in_(symbols))
    return list(
        db.execute(
            select(AnalystGradeEvent)
            .where(*filters)
            .order_by(AnalystGradeEvent.published_date.asc(), AnalystGradeEvent.symbol.asc(), AnalystGradeEvent.id.asc())
            .limit(max_events)
        )
        .scalars()
        .all()
    )


def _grade_event_forward_return_samples(
    db: Session,
    events: list[AnalystGradeEvent],
    horizon_days: int,
) -> list[dict[str, Any]]:
    scoped = [event for event in events if event.published_date and event.symbol]
    if not scoped:
        return []
    symbols = sorted({event.symbol.upper() for event in scoped if event.symbol})
    min_day = min(event.published_date for event in scoped if event.published_date)
    max_day = max(event.published_date for event in scoped if event.published_date) + timedelta(days=horizon_days + 7)
    prices = _load_prices(db, symbols, min_day, max_day)
    samples: list[dict[str, Any]] = []
    for event in scoped:
        symbol = (event.symbol or "").upper()
        published = event.published_date
        if published is None:
            continue
        points = prices.get(symbol) or []
        start_price = _first_close_on_or_after(points, published)
        end_price = _first_close_on_or_after(points, published + timedelta(days=horizon_days))
        if start_price is None or end_price is None or start_price <= 0:
            continue
        component_score = _grade_event_component_score(event)
        if component_score is None:
            continue
        samples.append(
            {
                "symbol": symbol,
                "snapshotDate": published.isoformat(),
                "componentScore": component_score,
                "sourceType": "historical_grade_event",
                "action": event.action,
                "newGrade": event.new_grade,
                "previousGrade": event.previous_grade,
                "forwardReturnPct": round(((end_price / start_price) - 1.0) * 100.0, 4),
                "horizonDays": horizon_days,
            }
        )
    return samples


def _load_prices(db: Session, symbols: list[str], start_day: date, end_day: date) -> dict[str, list[tuple[date, float]]]:
    rows = db.execute(
        select(PriceCache.symbol, PriceCache.date, PriceCache.close)
        .where(func.upper(PriceCache.symbol).in_(symbols))
        .where(PriceCache.date >= start_day.isoformat())
        .where(PriceCache.date <= end_day.isoformat())
        .order_by(PriceCache.symbol.asc(), PriceCache.date.asc())
    ).all()
    prices: dict[str, list[tuple[date, float]]] = defaultdict(list)
    for row in rows:
        point_day = _date(row.date)
        close = _number(row.close)
        symbol = str(row.symbol or "").upper()
        if point_day is None or close is None or close <= 0 or not symbol:
            continue
        prices[symbol].append((point_day, close))
    return dict(prices)


def _first_close_on_or_after(points: list[tuple[date, float]], target_day: date) -> float | None:
    if not points:
        return None
    index = bisect_left([point[0] for point in points], target_day)
    if index >= len(points):
        return None
    return points[index][1]


def _confirmation_correlation_samples(
    db: Session,
    snapshots: list[AnalystConsensusSnapshot],
    grade_events: list[AnalystGradeEvent],
) -> list[dict[str, Any]]:
    if not snapshots and not grade_events:
        return []
    symbols = sorted(
        {
            *[snapshot.symbol.upper() for snapshot in snapshots if snapshot.symbol],
            *[event.symbol.upper() for event in grade_events if event.symbol],
        }
    )
    candidate_days = [snapshot.snapshot_date for snapshot in snapshots]
    candidate_days.extend(event.published_date for event in grade_events if event.published_date)
    if not symbols or not candidate_days:
        return []
    min_day = min(candidate_days)
    max_day = max(candidate_days)
    history = _load_confirmation_history(db, symbols, min_day, max_day)
    samples: list[dict[str, Any]] = []
    for snapshot in snapshots:
        symbol = (snapshot.symbol or "").upper()
        score = analyst_consensus_shadow_component_score(_inputs_from_snapshot(snapshot))
        confirmation_score = (history.get(symbol) or {}).get(snapshot.snapshot_date)
        if score is None or confirmation_score is None:
            continue
        samples.append(
            {
                "symbol": symbol,
                "snapshotDate": snapshot.snapshot_date.isoformat(),
                "componentScore": score,
                "sourceType": "consensus_snapshot",
                "confirmationScore": confirmation_score,
            }
        )
    for event in grade_events:
        if event.published_date is None:
            continue
        symbol = (event.symbol or "").upper()
        score = _grade_event_component_score(event)
        confirmation_score = (history.get(symbol) or {}).get(event.published_date)
        if score is None or confirmation_score is None:
            continue
        samples.append(
            {
                "symbol": symbol,
                "snapshotDate": event.published_date.isoformat(),
                "componentScore": score,
                "sourceType": "historical_grade_event",
                "confirmationScore": confirmation_score,
            }
        )
    return samples


def _load_confirmation_history(
    db: Session,
    symbols: list[str],
    start_day: date,
    end_day: date,
) -> dict[str, dict[date, int]]:
    start_dt = datetime.combine(start_day, datetime.min.time(), tzinfo=timezone.utc)
    end_dt = datetime.combine(end_day + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc)
    history: dict[str, dict[date, int]] = defaultdict(dict)
    event_rows = db.execute(
        select(
            ConfirmationMonitoringEvent.ticker,
            ConfirmationMonitoringEvent.created_at,
            ConfirmationMonitoringEvent.score_after,
        )
        .where(func.upper(ConfirmationMonitoringEvent.ticker).in_(symbols))
        .where(ConfirmationMonitoringEvent.created_at >= start_dt)
        .where(ConfirmationMonitoringEvent.created_at < end_dt)
        .order_by(ConfirmationMonitoringEvent.created_at.asc(), ConfirmationMonitoringEvent.id.asc())
    ).all()
    for row in event_rows:
        symbol = str(row.ticker or "").upper()
        observed_day = _date(row.created_at)
        score = _int(row.score_after)
        if symbol and observed_day and score is not None:
            history[symbol][observed_day] = score
    snapshot_rows = db.execute(
        select(
            ConfirmationMonitoringSnapshot.ticker,
            ConfirmationMonitoringSnapshot.observed_at,
            ConfirmationMonitoringSnapshot.score,
        )
        .where(func.upper(ConfirmationMonitoringSnapshot.ticker).in_(symbols))
        .where(ConfirmationMonitoringSnapshot.observed_at >= start_dt)
        .where(ConfirmationMonitoringSnapshot.observed_at < end_dt)
        .order_by(ConfirmationMonitoringSnapshot.observed_at.asc(), ConfirmationMonitoringSnapshot.id.asc())
    ).all()
    for row in snapshot_rows:
        symbol = str(row.ticker or "").upper()
        observed_day = _date(row.observed_at)
        score = _int(row.score)
        if symbol and observed_day and score is not None:
            history[symbol][observed_day] = score
    return {symbol: dict(points) for symbol, points in history.items()}


def _backtest_summary(
    samples: list[dict[str, Any]],
    *,
    min_samples: int,
    min_symbols: int,
    horizon_days: int,
) -> dict[str, Any]:
    symbol_count = len({sample["symbol"] for sample in samples})
    buckets = {
        "bullish": [sample for sample in samples if sample["componentScore"] >= 60],
        "neutral": [sample for sample in samples if 40 < sample["componentScore"] < 60],
        "bearish": [sample for sample in samples if sample["componentScore"] <= 40],
    }
    bucket_payload = {
        name: _bucket_summary(name, bucket)
        for name, bucket in buckets.items()
    }
    bullish_avg = bucket_payload["bullish"]["averageForwardReturnPct"]
    bearish_avg = bucket_payload["bearish"]["averageForwardReturnPct"]
    spread = round(bullish_avg - bearish_avg, 4) if bullish_avg is not None and bearish_avg is not None else None
    score_return_correlation = _pearson(
        [float(sample["componentScore"]) for sample in samples],
        [float(sample["forwardReturnPct"]) for sample in samples],
    )
    has_coverage = len(samples) >= min_samples and symbol_count >= min_symbols
    has_signal_shape = spread is not None and spread > 0
    return {
        "status": "passed" if has_coverage and has_signal_shape else "insufficient_data",
        "requiredBeforeActivation": True,
        "horizonDays": horizon_days,
        "sampleCount": len(samples),
        "symbolCount": symbol_count,
        "minimumSamples": min_samples,
        "minimumSymbols": min_symbols,
        "averageForwardReturnPct": _average([sample["forwardReturnPct"] for sample in samples]),
        "scoreForwardReturnCorrelation": score_return_correlation,
        "bullishMinusBearishReturnPct": spread,
        "sourceBreakdown": _source_breakdown(samples),
        "buckets": bucket_payload,
    }


def _bucket_summary(name: str, samples: list[dict[str, Any]]) -> dict[str, Any]:
    returns = [sample["forwardReturnPct"] for sample in samples]
    if name == "bearish":
        hits = [value < 0 for value in returns]
    elif name == "neutral":
        hits = [abs(value) < 2.0 for value in returns]
    else:
        hits = [value > 0 for value in returns]
    return {
        "sampleCount": len(samples),
        "averageForwardReturnPct": _average(returns),
        "hitRatePct": round(sum(1 for hit in hits if hit) / len(hits) * 100.0, 2) if hits else None,
    }


def _correlation_summary(
    forward_samples: list[dict[str, Any]],
    confirmation_samples: list[dict[str, Any]],
    *,
    min_samples: int,
    max_confirmation_correlation: float,
) -> dict[str, Any]:
    confirmation_correlation = _pearson(
        [float(sample["componentScore"]) for sample in confirmation_samples],
        [float(sample["confirmationScore"]) for sample in confirmation_samples],
    )
    forward_correlation = _pearson(
        [float(sample["componentScore"]) for sample in forward_samples],
        [float(sample["forwardReturnPct"]) for sample in forward_samples],
    )
    enough = len(confirmation_samples) >= min_samples
    acceptable = confirmation_correlation is not None and abs(confirmation_correlation) <= max_confirmation_correlation
    return {
        "status": "passed" if enough and acceptable else "insufficient_data",
        "requiredBeforeActivation": True,
        "sampleCount": len(confirmation_samples),
        "minimumSamples": min_samples,
        "maxAllowedAbsoluteCorrelation": max_confirmation_correlation,
        "analystVsConfirmationScoreCorrelation": confirmation_correlation,
        "analystVsForwardReturnCorrelation": forward_correlation,
        "interpretation": (
            "acceptable_independence"
            if enough and acceptable
            else "needs_more_monitoring_history"
            if not enough
            else "possible_double_counting"
        ),
    }


def _double_counting_summary(correlation: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": "manual_review_required",
        "requiredBeforeActivation": True,
        "dynamicCorrelationStatus": correlation.get("status"),
        "riskMatrix": [
            {"source": "fundamentals", "risk": "high", "reason": "Analyst targets frequently embed growth, margins, and balance-sheet assumptions."},
            {"source": "valuation", "risk": "high", "reason": "Price targets can overlap with valuation and implied-upside views."},
            {"source": "price_volume", "risk": "medium", "reason": "Analyst revisions may react to recent price action."},
            {"source": "options_flow", "risk": "low", "reason": "Options flow captures trader positioning rather than analyst estimates."},
            {"source": "congress", "risk": "low", "reason": "Legislative trading activity is an independent disclosure stream."},
            {"source": "insiders", "risk": "low", "reason": "Insider transactions are independent regulatory disclosures."},
            {"source": "institutional_activity", "risk": "medium", "reason": "Institutional positioning can share fundamental narratives with analyst revisions."},
        ],
    }


def _source_breakdown(samples: list[dict[str, Any]]) -> dict[str, Any]:
    source_types = sorted({str(sample.get("sourceType") or "unknown") for sample in samples})
    return {
        source_type: {
            "sampleCount": sum(1 for sample in samples if str(sample.get("sourceType") or "unknown") == source_type),
            "averageForwardReturnPct": _average(
                sample["forwardReturnPct"]
                for sample in samples
                if str(sample.get("sourceType") or "unknown") == source_type
            ),
        }
        for source_type in source_types
    }


def _activation_review(
    backtest: dict[str, Any],
    correlation: dict[str, Any],
    double_counting: dict[str, Any],
    *,
    live_weight_enabled: bool,
) -> dict[str, Any]:
    gate_statuses = {
        "historicalBacktest": backtest.get("status"),
        "correlationReview": correlation.get("status"),
        "doubleCountingReview": double_counting.get("status"),
        "explicitLiveFlag": "enabled" if live_weight_enabled else "disabled",
    }
    can_activate = (
        live_weight_enabled
        and gate_statuses["historicalBacktest"] == "passed"
        and gate_statuses["correlationReview"] == "passed"
        and gate_statuses["doubleCountingReview"] == "passed"
    )
    return {
        "canActivateLiveWeight": can_activate,
        "recommendation": "eligible_for_controlled_activation" if can_activate else "keep_shadow_only",
        "gateStatuses": gate_statuses,
        "liveWeight": 0 if not can_activate else None,
    }


def _grade_event_component_score(event: AnalystGradeEvent) -> int | None:
    base = _grade_text_component_score(event.new_grade) or _grade_text_component_score(event.previous_grade)
    action = str(event.action or event.provider_action or "").strip().lower()
    if base is None:
        if "upgrade" in action:
            base = 65
        elif "downgrade" in action:
            base = 35
        elif "initiat" in action or "resume" in action:
            base = 55
        elif "suspend" in action:
            base = 45
    if base is None:
        return None
    if "upgrade" in action:
        base = max(base, 65)
    elif "downgrade" in action:
        base = min(base, 35)
    elif "suspend" in action:
        base = min(base, 45)
    return int(max(0, min(100, round(base))))


def _grade_text_component_score(value: Any) -> int | None:
    text = str(value or "").strip().lower()
    if not text:
        return None
    if any(token in text for token in ("strong buy", "conviction buy", "top pick")):
        return 85
    if any(token in text for token in ("buy", "outperform", "overweight", "accumulate", "positive")):
        return 72
    if any(token in text for token in ("hold", "neutral", "market perform", "sector perform", "equal-weight", "equal weight", "peer perform")):
        return 50
    if any(token in text for token in ("underperform", "underweight", "reduce", "negative")):
        return 30
    if "sell" in text:
        return 20
    return None


def _inputs_from_snapshot(snapshot: AnalystConsensusSnapshot) -> dict[str, Any]:
    rating_count = _int(snapshot.total_rating_count)
    if rating_count is None:
        coverage = "insufficient"
    elif rating_count >= 20:
        coverage = "high"
    elif rating_count >= 8:
        coverage = "moderate"
    elif rating_count >= 3:
        coverage = "low"
    else:
        coverage = "insufficient"
    return {
        "weightedRatingValue": snapshot.weighted_rating_value,
        "consensusImpliedUpsidePct": snapshot.consensus_implied_upside_pct,
        "coverageLevel": coverage,
        "targetDispersionPct": snapshot.target_dispersion_pct,
        "freshnessStatus": "fresh" if snapshot.availability_status in {"available", "partial"} else "unavailable",
    }


def _average(values: Iterable[Any]) -> float | None:
    parsed = [_number(value) for value in values]
    valid = [value for value in parsed if value is not None]
    if not valid:
        return None
    return round(sum(valid) / len(valid), 4)


def _pearson(left: list[float], right: list[float]) -> float | None:
    if len(left) != len(right) or len(left) < 3:
        return None
    left_mean = sum(left) / len(left)
    right_mean = sum(right) / len(right)
    numerator = sum((x - left_mean) * (y - right_mean) for x, y in zip(left, right, strict=True))
    left_den = math.sqrt(sum((x - left_mean) ** 2 for x in left))
    right_den = math.sqrt(sum((y - right_mean) ** 2 for y in right))
    if left_den <= 0 or right_den <= 0:
        return None
    return round(numerator / (left_den * right_den), 4)


def _number(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        parsed = float(value)
        return parsed if math.isfinite(parsed) else None
    if isinstance(value, str):
        cleaned = value.replace("$", "").replace(",", "").replace("%", "").strip()
        if not cleaned:
            return None
        try:
            parsed = float(cleaned)
        except ValueError:
            return None
        return parsed if math.isfinite(parsed) else None
    return None


def _int(value: Any) -> int | None:
    parsed = _number(value)
    if parsed is None:
        return None
    return int(parsed)


def _date(value: Any) -> date | None:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if value is None:
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default
