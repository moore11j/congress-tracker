from __future__ import annotations

import hashlib
import json
import math
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from statistics import median
from typing import Any, Iterable

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.clients.fmp import (
    FMPClientError,
    FMPSubscriptionRestrictedError,
    fetch_grade_events,
    fetch_grades_summary,
    fetch_historical_grades,
    fetch_price_target_consensus,
    fetch_price_target_news,
    fetch_price_target_summary,
)
from app.models import (
    AnalystConsensusIngestionRun,
    AnalystConsensusSnapshot,
    AnalystGradeEvent,
    AnalystPriceTargetEvent,
    AnalystSymbolBackfillStatus,
    PriceCache,
    QuoteCache,
    Security,
    TickerMeta,
)
from app.utils.symbols import classify_symbol, normalize_symbol

METHODOLOGY_VERSION = "analyst_consensus_v1"
SOURCE = "fmp"
GRADE_BACKFILL_JOB = "analyst_historical_grade_events_backfill"
PRICE_TARGET_BACKFILL_JOB = "analyst_historical_price_targets_backfill"
GRADE_DAILY_REFRESH_JOB = "analyst_grade_events_daily_refresh"
PRICE_TARGET_DAILY_REFRESH_JOB = "analyst_price_target_events_daily_refresh"
DEFAULT_HISTORY_DAYS = 365
MAX_HISTORY_DAYS = 730
FRESHNESS_DAYS = 7
STALE_DAYS = 14
LIVE_CACHE_MISS_TIMEOUT_SECONDS = 5


@dataclass(frozen=True)
class PricePoint:
    price: float | None
    source: str | None
    as_of: datetime | None


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, default=str, separators=(",", ":"))


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


def _positive(value: Any) -> float | None:
    parsed = _number(value)
    if parsed is None or parsed <= 0:
        return None
    return parsed


def _text(value: Any) -> str | None:
    if value is None:
        return None
    trimmed = str(value).strip()
    return trimmed or None


def _first_present(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row and row.get(key) is not None:
            return row.get(key)
    return None


def _date(value: Any) -> date | None:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    text_value = _text(value)
    if not text_value:
        return None
    try:
        return datetime.fromisoformat(text_value.replace("Z", "+00:00")).date()
    except ValueError:
        pass
    try:
        return date.fromisoformat(text_value[:10])
    except ValueError:
        return None


def _datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    else:
        text_value = _text(value)
        if not text_value:
            return None
        try:
            parsed = datetime.fromisoformat(text_value.replace("Z", "+00:00"))
        except ValueError:
            parsed_date = _date(text_value)
            if parsed_date is None:
                return None
            parsed = datetime.combine(parsed_date, datetime.min.time(), tzinfo=timezone.utc)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _iso_dt(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _iso_date(value: date | None) -> str | None:
    return value.isoformat() if value else None


def analyst_symbol_rejection_reason(raw_symbol: str | None) -> tuple[str | None, str | None]:
    status, normalized, reason = classify_symbol(raw_symbol)
    symbol_text = normalized or normalize_symbol(raw_symbol) or str(raw_symbol or "").strip().upper() or None
    if status != "eligible" or not normalized:
        return symbol_text, reason or status
    return normalized, None


def rating_counts_from_row(row: dict[str, Any]) -> dict[str, int | None]:
    return {
        "strong_buy_count": _int(_first_present(row, "strongBuy", "strong_buy", "analystRatingsStrongBuy")),
        "buy_count": _int(_first_present(row, "buy", "analystRatingsBuy")),
        "hold_count": _int(_first_present(row, "hold", "analystRatingsHold")),
        "sell_count": _int(_first_present(row, "sell", "analystRatingsSell")),
        "strong_sell_count": _int(_first_present(row, "strongSell", "strong_sell", "analystRatingsStrongSell")),
    }


def total_rating_count(counts: dict[str, int | None]) -> int | None:
    values = [counts.get(key) for key in ("strong_buy_count", "buy_count", "hold_count", "sell_count", "strong_sell_count")]
    if any(value is None for value in values):
        return None
    return sum(int(value or 0) for value in values)


def weighted_sentiment(counts: dict[str, int | None]) -> float | None:
    total = total_rating_count(counts)
    if total is None or total <= 0:
        return None
    weighted = (
        2 * int(counts.get("strong_buy_count") or 0)
        + int(counts.get("buy_count") or 0)
        - int(counts.get("sell_count") or 0)
        - 2 * int(counts.get("strong_sell_count") or 0)
    )
    return weighted / total


def recommendation_label(weighted_value: float | None, rating_count: int | None) -> str:
    if rating_count is None or rating_count < int(os.getenv("ANALYST_CONSENSUS_MIN_COVERAGE", "3") or 3):
        return "Insufficient Coverage"
    if weighted_value is None:
        return "Insufficient Coverage"
    if weighted_value >= 1.25:
        return "Strong Bullish"
    if weighted_value >= 0.35:
        return "Bullish"
    if weighted_value > -0.35:
        return "Neutral"
    if weighted_value > -1.25:
        return "Bearish"
    return "Strong Bearish"


def implied_upside(target: float | None, current_price: float | None) -> float | None:
    if target is None or current_price is None or target <= 0 or current_price <= 0:
        return None
    return ((target / current_price) - 1) * 100


def target_dispersion(high: float | None, low: float | None, median_target: float | None) -> float | None:
    if high is None or low is None or median_target is None or high <= 0 or low <= 0 or median_target <= 0:
        return None
    return ((high - low) / median_target) * 100


def buy_equivalent_pct(snapshot: AnalystConsensusSnapshot | None) -> float | None:
    if snapshot is None or not snapshot.total_rating_count:
        return None
    bullish = int(snapshot.strong_buy_count or 0) + int(snapshot.buy_count or 0)
    return bullish / snapshot.total_rating_count * 100


def sell_equivalent_pct(snapshot: AnalystConsensusSnapshot | None) -> float | None:
    if snapshot is None or not snapshot.total_rating_count:
        return None
    bearish = int(snapshot.sell_count or 0) + int(snapshot.strong_sell_count or 0)
    return bearish / snapshot.total_rating_count * 100


def _delta(current: float | int | None, prior: float | int | None) -> float | None:
    if current is None or prior is None:
        return None
    return float(current) - float(prior)


def _target_consensus_values(row: dict[str, Any]) -> dict[str, float | None]:
    return {
        "price_target_high": _positive(_first_present(row, "targetHigh", "target_high")),
        "price_target_low": _positive(_first_present(row, "targetLow", "target_low")),
        "price_target_median": _positive(_first_present(row, "targetMedian", "target_median")),
        "price_target_consensus": _positive(_first_present(row, "targetConsensus", "target_consensus", "consensus")),
    }


def _target_summary_values(row: dict[str, Any]) -> dict[str, float | int | None]:
    all_time_avg = _positive(_first_present(row, "allTimeAvgPriceTarget", "allTimeAveragePriceTarget"))
    last_year_avg = _positive(row.get("lastYearAvgPriceTarget"))
    return {
        "price_target_average": all_time_avg or last_year_avg,
        "price_target_analyst_count": _int(_first_present(row, "allTimeCount", "lastYearCount")),
    }


def _availability(
    *,
    counts: dict[str, int | None],
    targets: dict[str, float | None],
    provider_error: str | None = None,
) -> tuple[str, str]:
    if provider_error:
        has_any = any(value is not None for value in counts.values()) or any(value is not None for value in targets.values())
        return ("partial" if has_any else "unavailable", "provider_error")
    has_ratings = total_rating_count(counts) is not None
    has_targets = any(targets.get(key) is not None for key in ("price_target_median", "price_target_consensus", "price_target_high", "price_target_low"))
    if has_ratings and has_targets:
        return "available", "available"
    if has_ratings or has_targets:
        return "partial", "available"
    return "unavailable", "available"


def latest_cached_price(db: Session, symbol: str) -> PricePoint:
    normalized = normalize_symbol(symbol)
    if not normalized:
        return PricePoint(None, None, None)
    quote = db.get(QuoteCache, normalized)
    if quote and quote.price is not None and quote.price > 0:
        as_of = quote.asof_ts
        if as_of and as_of.tzinfo is None:
            as_of = as_of.replace(tzinfo=timezone.utc)
        return PricePoint(float(quote.price), "quotes_cache", as_of)
    price = db.execute(
        select(PriceCache.date, PriceCache.close)
        .where(PriceCache.symbol == normalized)
        .order_by(PriceCache.date.desc())
        .limit(1)
    ).first()
    if price and price.close is not None and price.close > 0:
        as_of_date = _date(price.date)
        as_of = datetime.combine(as_of_date, datetime.min.time(), tzinfo=timezone.utc) if as_of_date else None
        return PricePoint(float(price.close), "price_cache", as_of)
    return PricePoint(None, None, None)


def build_snapshot_payload(
    symbol: str,
    *,
    grades_summary_rows: list[dict[str, Any]],
    price_target_consensus_rows: list[dict[str, Any]],
    price_target_summary_rows: list[dict[str, Any]],
    price: PricePoint | None = None,
    observed_at: datetime | None = None,
    provider_error: str | None = None,
) -> dict[str, Any]:
    normalized = normalize_symbol(symbol)
    if not normalized:
        raise ValueError("invalid symbol")
    observed = observed_at or utc_now()
    summary_row = grades_summary_rows[0] if grades_summary_rows else {}
    target_row = price_target_consensus_rows[0] if price_target_consensus_rows else {}
    target_summary_row = price_target_summary_rows[0] if price_target_summary_rows else {}
    counts = rating_counts_from_row(summary_row)
    rating_count = total_rating_count(counts)
    weighted = weighted_sentiment(counts)
    targets = _target_consensus_values(target_row)
    target_summary = _target_summary_values(target_summary_row)
    current_price = price.price if price else None
    availability_status, provider_status = _availability(counts=counts, targets=targets, provider_error=provider_error)
    return {
        "symbol": normalized,
        "provider_symbol": _text(summary_row.get("symbol") or target_row.get("symbol") or target_summary_row.get("symbol")) or normalized,
        "snapshot_date": observed.astimezone(timezone.utc).date() if observed.tzinfo else observed.date(),
        **counts,
        "total_rating_count": rating_count,
        "weighted_rating_value": weighted,
        "recommendation_label": recommendation_label(weighted, rating_count),
        **targets,
        **target_summary,
        "current_price_at_snapshot": current_price,
        "current_price_source": price.source if price else None,
        "current_price_as_of": price.as_of if price else None,
        "median_implied_upside_pct": implied_upside(targets["price_target_median"], current_price),
        "consensus_implied_upside_pct": implied_upside(targets["price_target_consensus"] or target_summary["price_target_average"], current_price),
        "target_dispersion_pct": target_dispersion(
            targets["price_target_high"],
            targets["price_target_low"],
            targets["price_target_median"],
        ),
        "availability_status": availability_status,
        "provider_status": provider_status,
        "provider_error": provider_error,
        "source": SOURCE,
        "source_updated_at": None,
        "methodology_version": METHODOLOGY_VERSION,
        "raw_payload_json": _json(
            {
                "grades_consensus": grades_summary_rows,
                "price_target_consensus": price_target_consensus_rows,
                "price_target_summary": price_target_summary_rows,
            }
        ),
        "ingested_at": observed,
    }


def upsert_consensus_snapshot(db: Session, values: dict[str, Any]) -> tuple[AnalystConsensusSnapshot, bool]:
    symbol = values["symbol"]
    snapshot_date = values["snapshot_date"]
    if db.get_bind().dialect.name == "postgresql":
        from sqlalchemy.dialects.postgresql import insert as pg_insert

        existing_id = db.execute(
            select(AnalystConsensusSnapshot.id)
            .where(AnalystConsensusSnapshot.symbol == symbol)
            .where(AnalystConsensusSnapshot.snapshot_date == snapshot_date)
        ).scalar_one_or_none()
        write_values = {**values, "updated_at": values.get("ingested_at") or utc_now()}
        stmt = pg_insert(AnalystConsensusSnapshot).values(**write_values)
        update_values = {
            key: getattr(stmt.excluded, key)
            for key in write_values
            if key not in {"symbol", "snapshot_date"}
        }
        row_id = db.execute(
            stmt.on_conflict_do_update(
                index_elements=["symbol", "snapshot_date"],
                set_=update_values,
            ).returning(AnalystConsensusSnapshot.id)
        ).scalar_one()
        row = db.get(AnalystConsensusSnapshot, row_id)
        if row is None:
            raise RuntimeError("Analyst consensus upsert did not return a snapshot row")
        return row, existing_id is None

    row = db.execute(
        select(AnalystConsensusSnapshot)
        .where(AnalystConsensusSnapshot.symbol == symbol)
        .where(AnalystConsensusSnapshot.snapshot_date == snapshot_date)
    ).scalar_one_or_none()
    created = row is None
    if row is None:
        row = AnalystConsensusSnapshot(symbol=symbol, snapshot_date=snapshot_date)
        db.add(row)
        db.flush()
    for key, value in values.items():
        setattr(row, key, value)
    row.updated_at = values.get("ingested_at") or utc_now()
    return row, created


def _previous_consensus_snapshot(db: Session, symbol: str, snapshot_date: date) -> AnalystConsensusSnapshot | None:
    return db.execute(
        select(AnalystConsensusSnapshot)
        .where(AnalystConsensusSnapshot.symbol == symbol)
        .where(AnalystConsensusSnapshot.snapshot_date < snapshot_date)
        .where(AnalystConsensusSnapshot.availability_status.in_(("available", "partial", "stale")))
        .order_by(AnalystConsensusSnapshot.snapshot_date.desc(), AnalystConsensusSnapshot.ingested_at.desc())
        .limit(1)
    ).scalar_one_or_none()


def _pct_change(current: float | None, prior: float | None) -> float | None:
    if current is None or prior is None or prior == 0:
        return None
    return ((current / prior) - 1.0) * 100.0


def _consensus_change_payload(
    current: AnalystConsensusSnapshot,
    prior: AnalystConsensusSnapshot,
) -> dict[str, Any] | None:
    if str(current.availability_status or "").lower() not in {"available", "partial", "stale"}:
        return None
    label_changed = (current.recommendation_label or "") != (prior.recommendation_label or "")
    upside_delta = None
    if current.consensus_implied_upside_pct is not None and prior.consensus_implied_upside_pct is not None:
        upside_delta = float(current.consensus_implied_upside_pct) - float(prior.consensus_implied_upside_pct)
    weighted_delta = None
    if current.weighted_rating_value is not None and prior.weighted_rating_value is not None:
        weighted_delta = float(current.weighted_rating_value) - float(prior.weighted_rating_value)
    target_delta_pct = _pct_change(
        float(current.price_target_consensus) if current.price_target_consensus is not None else None,
        float(prior.price_target_consensus) if prior.price_target_consensus is not None else None,
    )
    if not (
        label_changed
        or (upside_delta is not None and abs(upside_delta) >= 5.0)
        or (weighted_delta is not None and abs(weighted_delta) >= 0.25)
        or (target_delta_pct is not None and abs(target_delta_pct) >= 3.0)
    ):
        return None
    direction = "bullish" if (upside_delta or weighted_delta or 0) > 0 else "bearish" if (upside_delta or weighted_delta or 0) < 0 else "neutral"
    return {
        "event_type": "analyst_consensus_change",
        "symbol": current.symbol,
        "direction": direction,
        "snapshotDate": _iso_date(current.snapshot_date),
        "priorSnapshotDate": _iso_date(prior.snapshot_date),
        "recommendationLabel": current.recommendation_label,
        "priorRecommendationLabel": prior.recommendation_label,
        "recommendationChanged": label_changed,
        "consensusImpliedUpsidePct": current.consensus_implied_upside_pct,
        "priorConsensusImpliedUpsidePct": prior.consensus_implied_upside_pct,
        "consensusUpsideDeltaPct": round(upside_delta, 4) if upside_delta is not None else None,
        "weightedRatingValue": current.weighted_rating_value,
        "priorWeightedRatingValue": prior.weighted_rating_value,
        "weightedRatingDelta": round(weighted_delta, 4) if weighted_delta is not None else None,
        "priceTargetConsensus": current.price_target_consensus,
        "priorPriceTargetConsensus": prior.price_target_consensus,
        "priceTargetConsensusDeltaPct": round(target_delta_pct, 4) if target_delta_pct is not None else None,
        "methodologyVersion": METHODOLOGY_VERSION,
    }


def _create_consensus_change_event(
    db: Session,
    current: AnalystConsensusSnapshot,
    prior: AnalystConsensusSnapshot | None,
    observed_at: datetime,
) -> bool:
    return False


def normalize_action(action: Any) -> str | None:
    raw = _text(action)
    if not raw:
        return None
    cleaned = raw.lower().replace("_", " ").replace("-", " ").strip()
    if "upgrade" in cleaned:
        return "Upgrade"
    if "downgrade" in cleaned:
        return "Downgrade"
    if "initiat" in cleaned:
        return "Initiated"
    if "reiterat" in cleaned:
        return "Reiterated"
    if "maintain" in cleaned:
        return "Maintained"
    if "resume" in cleaned:
        return "Resumed"
    if "suspend" in cleaned:
        return "Suspended"
    if cleaned in {"other", "unknown"}:
        return cleaned.title()
    return raw


def event_fingerprint(symbol: str, row: dict[str, Any]) -> str:
    provider_id = _text(row.get("id") or row.get("eventId") or row.get("providerEventId"))
    if provider_id:
        basis = {"provider_id": provider_id}
    else:
        basis = {
            "symbol": normalize_symbol(symbol),
            "date": _iso_date(_date(row.get("date") or row.get("publishedDate") or row.get("published_date"))),
            "grading_company": _text(_first_present(row, "gradingCompany", "grading_company", "firm", "company", "analystRatingsCompany")),
            "analyst_name": _text(_first_present(row, "analystName", "analyst_name", "analyst")),
            "previous_grade": _text(_first_present(row, "previousGrade", "previous_grade", "previousRating", "fromGrade", "oldGrade")),
            "new_grade": _text(_first_present(row, "newGrade", "new_grade", "newRating", "toGrade", "grade", "rating")),
            "action": _text(_first_present(row, "action", "ratingAction", "gradeAction")),
        }
    return hashlib.sha256(_json(basis).encode("utf-8")).hexdigest()


def event_values(symbol: str, row: dict[str, Any], *, ingested_at: datetime | None = None) -> dict[str, Any]:
    normalized = normalize_symbol(symbol)
    if not normalized:
        raise ValueError("invalid symbol")
    provider_action = _text(_first_present(row, "action", "ratingAction", "gradeAction"))
    counts = rating_counts_from_row(row)
    rating_count = total_rating_count(counts)
    derived_grade = recommendation_label(weighted_sentiment(counts), rating_count) if rating_count else None
    return {
        "symbol": normalized,
        "provider_symbol": _text(row.get("symbol")) or normalized,
        "grading_company": _text(_first_present(row, "gradingCompany", "grading_company", "firm", "company", "analystRatingsCompany")),
        "analyst_name": _text(_first_present(row, "analystName", "analyst_name", "analyst")),
        "previous_grade": _text(_first_present(row, "previousGrade", "previous_grade", "previousRating", "fromGrade", "oldGrade")),
        "new_grade": _text(_first_present(row, "newGrade", "new_grade", "newRating", "toGrade", "grade", "rating")) or derived_grade,
        "action": normalize_action(provider_action),
        "provider_action": provider_action,
        "published_date": _date(row.get("date") or row.get("publishedDate") or row.get("published_date")),
        "source_url": _text(row.get("url") or row.get("sourceUrl") or row.get("source_url")),
        "provider_event_id": _text(row.get("id") or row.get("eventId") or row.get("providerEventId")),
        "event_fingerprint": event_fingerprint(normalized, row),
        "source": SOURCE,
        "raw_payload_json": _json(row),
        "ingested_at": ingested_at or utc_now(),
    }


def upsert_grade_event(db: Session, values: dict[str, Any]) -> tuple[AnalystGradeEvent, bool]:
    row = db.execute(
        select(AnalystGradeEvent)
        .where(AnalystGradeEvent.source == values["source"])
        .where(AnalystGradeEvent.event_fingerprint == values["event_fingerprint"])
    ).scalar_one_or_none()
    created = row is None
    if row is None:
        row = AnalystGradeEvent(
            symbol=values["symbol"],
            event_fingerprint=values["event_fingerprint"],
            source=values["source"],
        )
        db.add(row)
        db.flush()
    for key, value in values.items():
        setattr(row, key, value)
    row.updated_at = values.get("ingested_at") or utc_now()
    return row, created


def price_target_event_fingerprint(symbol: str, row: dict[str, Any]) -> str:
    provider_id = _text(row.get("id") or row.get("eventId") or row.get("providerEventId") or row.get("newsURL"))
    if provider_id:
        basis = {"provider_id": provider_id}
    else:
        basis = {
            "symbol": normalize_symbol(symbol),
            "published_at": _iso_dt(_datetime(row.get("publishedDate") or row.get("published_at"))),
            "analyst_company": _text(row.get("analystCompany") or row.get("analyst_company")),
            "analyst_name": _text(row.get("analystName") or row.get("analyst_name")),
            "price_target": _number(row.get("priceTarget") or row.get("price_target")),
            "adjusted_price_target": _number(row.get("adjPriceTarget") or row.get("adjusted_price_target")),
            "news_title": _text(row.get("newsTitle") or row.get("news_title")),
        }
    return hashlib.sha256(_json(basis).encode("utf-8")).hexdigest()


def price_target_event_values(symbol: str, row: dict[str, Any], *, ingested_at: datetime | None = None) -> dict[str, Any]:
    normalized = normalize_symbol(symbol)
    if not normalized:
        raise ValueError("invalid symbol")
    published_at = _datetime(row.get("publishedDate") or row.get("published_at"))
    return {
        "symbol": normalized,
        "provider_symbol": _text(row.get("symbol")) or normalized,
        "analyst_company": _text(row.get("analystCompany") or row.get("analyst_company")),
        "analyst_name": _text(row.get("analystName") or row.get("analyst_name")),
        "price_target": _positive(row.get("priceTarget") or row.get("price_target")),
        "adjusted_price_target": _positive(row.get("adjPriceTarget") or row.get("adjusted_price_target")),
        "price_when_posted": _positive(row.get("priceWhenPosted") or row.get("price_when_posted")),
        "published_at": published_at,
        "published_date": published_at.date() if published_at else _date(row.get("publishedDate") or row.get("date")),
        "news_title": _text(row.get("newsTitle") or row.get("news_title")),
        "news_publisher": _text(row.get("newsPublisher") or row.get("news_publisher")),
        "news_url": _text(row.get("newsURL") or row.get("newsUrl") or row.get("url") or row.get("news_url")),
        "provider_event_id": _text(row.get("id") or row.get("eventId") or row.get("providerEventId") or row.get("newsURL")),
        "event_fingerprint": price_target_event_fingerprint(normalized, row),
        "source": SOURCE,
        "raw_payload_json": _json(row),
        "ingested_at": ingested_at or utc_now(),
    }


def upsert_price_target_event(db: Session, values: dict[str, Any]) -> tuple[AnalystPriceTargetEvent, bool]:
    row = db.execute(
        select(AnalystPriceTargetEvent)
        .where(AnalystPriceTargetEvent.source == values["source"])
        .where(AnalystPriceTargetEvent.event_fingerprint == values["event_fingerprint"])
    ).scalar_one_or_none()
    created = row is None
    if row is None:
        row = AnalystPriceTargetEvent(
            symbol=values["symbol"],
            event_fingerprint=values["event_fingerprint"],
            source=values["source"],
        )
        db.add(row)
        db.flush()
    for key, value in values.items():
        setattr(row, key, value)
    row.updated_at = values.get("ingested_at") or utc_now()
    return row, created


def ingest_symbol_consensus(
    db: Session,
    symbol: str,
    *,
    observed_at: datetime | None = None,
    provider_timeout_s: int = 15,
    parallel_provider_fetch: bool = False,
) -> dict[str, Any]:
    normalized, rejection_reason = analyst_symbol_rejection_reason(symbol)
    if rejection_reason or not normalized:
        return {"symbol": normalized or symbol, "status": "unsupported", "error": rejection_reason}
    observed = observed_at or utc_now()
    provider_error: str | None = None
    rows: dict[str, list[dict[str, Any]]] = {
        "grades_summary": [],
        "price_target_consensus": [],
        "price_target_summary": [],
    }
    fetchers = (
        ("grades_summary", fetch_grades_summary),
        ("price_target_consensus", fetch_price_target_consensus),
        ("price_target_summary", fetch_price_target_summary),
    )

    def fetch_rows(key: str, fetcher: Any) -> tuple[str, list[dict[str, Any]], str | None]:
        try:
            return key, fetcher(symbol=normalized, timeout_s=provider_timeout_s), None
        except FMPSubscriptionRestrictedError as exc:
            return key, [], f"subscription_restricted:{exc.__class__.__name__}"
        except FMPClientError as exc:
            return key, [], exc.__class__.__name__

    if parallel_provider_fetch:
        with ThreadPoolExecutor(max_workers=len(fetchers), thread_name_prefix="analyst-consensus") as executor:
            futures = [executor.submit(fetch_rows, key, fetcher) for key, fetcher in fetchers]
            for future in as_completed(futures):
                key, result_rows, error = future.result()
                rows[key] = result_rows
                if error and provider_error is None:
                    provider_error = error
    else:
        for key, fetcher in fetchers:
            result_key, result_rows, error = fetch_rows(key, fetcher)
            rows[result_key] = result_rows
            if error and provider_error is None:
                provider_error = error
    price = latest_cached_price(db, normalized)
    values = build_snapshot_payload(
        normalized,
        grades_summary_rows=rows["grades_summary"],
        price_target_consensus_rows=rows["price_target_consensus"],
        price_target_summary_rows=rows["price_target_summary"],
        price=price,
        observed_at=observed,
        provider_error=provider_error,
    )
    snapshot, created = upsert_consensus_snapshot(db, values)
    return {
        "symbol": normalized,
        "status": snapshot.availability_status,
        "created": created,
        "snapshot_id": snapshot.id,
        "provider_error": provider_error,
        "change_event_created": False,
    }


def refresh_consensus_on_cache_miss(db: Session, symbol: str) -> dict[str, Any]:
    """Fetch and cache a current consensus snapshot only when none exists yet.

    Ticker tabs are user-facing, so a first view must not wait for the daily
    enrichment worker. Existing snapshots remain cache-first; a cache miss gets
    one bounded, parallel provider read and stores the resulting availability.
    """
    normalized, rejection_reason = analyst_symbol_rejection_reason(symbol)
    if rejection_reason or not normalized:
        return {"attempted": False, "reason": rejection_reason or "unsupported"}
    if latest_snapshot(db, normalized) is not None:
        return {"attempted": False, "reason": "cache_hit"}

    result = ingest_symbol_consensus(
        db,
        normalized,
        provider_timeout_s=LIVE_CACHE_MISS_TIMEOUT_SECONDS,
        parallel_provider_fetch=True,
    )
    db.commit()
    return {"attempted": True, "reason": "cache_miss", "result": result}


def ingest_symbol_grade_events(db: Session, symbol: str, *, observed_at: datetime | None = None) -> dict[str, Any]:
    normalized, rejection_reason = analyst_symbol_rejection_reason(symbol)
    if rejection_reason or not normalized:
        return {"symbol": normalized or symbol, "status": "unsupported", "error": rejection_reason}
    observed = observed_at or utc_now()
    try:
        rows = fetch_grade_events(symbol=normalized, timeout_s=20)
    except FMPSubscriptionRestrictedError as exc:
        return {"symbol": normalized, "status": "provider_error", "error": f"subscription_restricted:{exc.__class__.__name__}"}
    except FMPClientError as exc:
        return {"symbol": normalized, "status": "provider_error", "error": exc.__class__.__name__}
    inserted = updated = skipped = 0
    for row in rows:
        try:
            event, created = upsert_grade_event(db, event_values(normalized, row, ingested_at=observed))
        except ValueError:
            skipped += 1
            continue
        if event.id and created:
            inserted += 1
        else:
            updated += 1
    return {"symbol": normalized, "status": "available" if rows else "unavailable", "rows_seen": len(rows), "inserted": inserted, "updated": updated, "skipped": skipped}


def ingest_symbol_historical_grade_events(
    db: Session,
    symbol: str,
    *,
    observed_at: datetime | None = None,
    timeout_s: int = 30,
) -> dict[str, Any]:
    normalized, rejection_reason = analyst_symbol_rejection_reason(symbol)
    if rejection_reason or not normalized:
        return {"symbol": normalized or symbol, "status": "unsupported", "error": rejection_reason}
    observed = observed_at or utc_now()
    try:
        rows = fetch_historical_grades(symbol=normalized, timeout_s=timeout_s)
    except FMPSubscriptionRestrictedError as exc:
        return {"symbol": normalized, "status": "provider_error", "error": f"subscription_restricted:{exc.__class__.__name__}"}
    except FMPClientError as exc:
        return {"symbol": normalized, "status": "provider_error", "error": exc.__class__.__name__}
    inserted = updated = skipped = 0
    for row in rows:
        try:
            event, created = upsert_grade_event(db, event_values(normalized, row, ingested_at=observed))
        except ValueError:
            skipped += 1
            continue
        if event.id and created:
            inserted += 1
        else:
            updated += 1
    return {
        "symbol": normalized,
        "status": "available" if rows else "unavailable",
        "source": "grades-historical",
        "rows_seen": len(rows),
        "inserted": inserted,
        "updated": updated,
        "skipped": skipped,
    }


def ingest_symbol_price_target_events(
    db: Session,
    symbol: str,
    *,
    observed_at: datetime | None = None,
    pages: int = 1,
    page_size: int = 100,
    timeout_s: int = 30,
) -> dict[str, Any]:
    normalized, rejection_reason = analyst_symbol_rejection_reason(symbol)
    if rejection_reason or not normalized:
        return {"symbol": normalized or symbol, "status": "unsupported", "error": rejection_reason}
    observed = observed_at or utc_now()
    inserted = updated = skipped = rows_seen = 0
    bounded_pages = max(1, min(int(pages or 1), 10))
    bounded_page_size = max(1, min(int(page_size or 100), 100))
    for page in range(bounded_pages):
        try:
            rows = fetch_price_target_news(symbol=normalized, page=page, limit=bounded_page_size, timeout_s=timeout_s)
        except FMPSubscriptionRestrictedError as exc:
            return {"symbol": normalized, "status": "provider_error", "error": f"subscription_restricted:{exc.__class__.__name__}"}
        except FMPClientError as exc:
            return {"symbol": normalized, "status": "provider_error", "error": exc.__class__.__name__}
        rows_seen += len(rows)
        if not rows:
            break
        for row in rows:
            try:
                event, created = upsert_price_target_event(db, price_target_event_values(normalized, row, ingested_at=observed))
            except ValueError:
                skipped += 1
                continue
            if event.id and created:
                inserted += 1
            else:
                updated += 1
        if len(rows) < bounded_page_size:
            break
    return {
        "symbol": normalized,
        "status": "available" if rows_seen else "unavailable",
        "source": "price-target-news",
        "rows_seen": rows_seen,
        "inserted": inserted,
        "updated": updated,
        "skipped": skipped,
    }


def _latest_backfill_progress(db: Session, *, job_name: str, event_model: Any) -> dict[str, datetime]:
    progress: dict[str, datetime] = {}
    attempt_rows = db.execute(
        select(
            AnalystSymbolBackfillStatus.symbol,
            func.max(AnalystSymbolBackfillStatus.last_attempted_at),
        )
        .where(AnalystSymbolBackfillStatus.job_name == job_name)
        .group_by(AnalystSymbolBackfillStatus.symbol)
    ).all()
    for symbol, attempted_at in attempt_rows:
        if symbol and attempted_at:
            progress[symbol] = attempted_at
    event_rows = db.execute(
        select(
            event_model.symbol,
            func.max(event_model.ingested_at),
        ).group_by(event_model.symbol)
    ).all()
    for symbol, ingested_at in event_rows:
        if not symbol or not ingested_at:
            continue
        current = progress.get(symbol)
        if current is None or ingested_at > current:
            progress[symbol] = ingested_at
    return progress


def _backfill_attempt_status(result: dict[str, Any]) -> str:
    status = str(result.get("status") or "unknown")
    if status == "unavailable" and int(result.get("rows_seen") or 0) == 0:
        return "no_provider_data"
    return status


def record_symbol_backfill_attempt(
    db: Session,
    *,
    job_name: str,
    symbol: str,
    result: dict[str, Any],
    attempted_at: datetime | None = None,
) -> AnalystSymbolBackfillStatus | None:
    normalized, rejection_reason = analyst_symbol_rejection_reason(symbol)
    if rejection_reason or not normalized:
        normalized = normalize_symbol(symbol)
    if not normalized:
        return None
    row = db.execute(
        select(AnalystSymbolBackfillStatus)
        .where(AnalystSymbolBackfillStatus.job_name == job_name)
        .where(AnalystSymbolBackfillStatus.symbol == normalized)
    ).scalar_one_or_none()
    if row is None:
        row = AnalystSymbolBackfillStatus(job_name=job_name, symbol=normalized, last_attempted_at=attempted_at or utc_now())
        db.add(row)
    row.status = _backfill_attempt_status(result)
    row.rows_seen = int(result.get("rows_seen") or 0)
    row.records_inserted = int(result.get("inserted") or 0)
    row.records_updated = int(result.get("updated") or 0)
    row.error_summary = str(result.get("error") or "")[:500] or None
    row.last_attempted_at = attempted_at or utc_now()
    row.updated_at = utc_now()
    db.flush()
    return row


def eligible_equity_symbols(db: Session, symbols: Iterable[str] | None = None, *, limit: int | None = None) -> list[str]:
    if symbols is not None:
        candidates = [analyst_symbol_rejection_reason(symbol)[0] for symbol in symbols]
        result = sorted({symbol for symbol in candidates if symbol})
        return result[:limit] if limit else result
    latest_snapshot_dates = (
        select(
            AnalystConsensusSnapshot.symbol.label("symbol"),
            func.max(AnalystConsensusSnapshot.snapshot_date).label("latest_snapshot_date"),
        )
        .group_by(AnalystConsensusSnapshot.symbol)
        .subquery()
    )
    rows = db.execute(
        select(Security.symbol)
        .outerjoin(latest_snapshot_dates, Security.symbol == latest_snapshot_dates.c.symbol)
        .where(func.lower(Security.asset_class).in_(("stock", "equity")))
        .order_by(
            latest_snapshot_dates.c.latest_snapshot_date.is_(None).desc(),
            latest_snapshot_dates.c.latest_snapshot_date.asc(),
            Security.symbol.asc(),
        )
        .limit(5000)
    ).scalars().all()
    if not rows:
        rows = db.execute(
            select(TickerMeta.symbol)
            .outerjoin(latest_snapshot_dates, TickerMeta.symbol == latest_snapshot_dates.c.symbol)
            .order_by(
                latest_snapshot_dates.c.latest_snapshot_date.is_(None).desc(),
                latest_snapshot_dates.c.latest_snapshot_date.asc(),
                TickerMeta.symbol.asc(),
            )
            .limit(5000)
        ).scalars().all()
    result: list[str] = []
    seen: set[str] = set()
    for raw in rows:
        symbol, rejection = analyst_symbol_rejection_reason(raw)
        if symbol and rejection is None and symbol not in seen:
            result.append(symbol)
            seen.add(symbol)
    return result[:limit] if limit else result


def eligible_historical_grade_symbols(
    db: Session,
    symbols: Iterable[str] | None = None,
    *,
    limit: int | None = None,
    job_name: str = GRADE_BACKFILL_JOB,
) -> list[str]:
    if symbols is not None:
        return eligible_equity_symbols(db, symbols, limit=limit)
    progress = _latest_backfill_progress(db, job_name=job_name, event_model=AnalystGradeEvent)
    result: list[str] = []
    seen: set[str] = set()
    order: dict[str, int] = {}
    category: dict[str, int] = {}

    def append_rows(rows: Iterable[str | None], category_rank: int) -> None:
        for raw in rows:
            symbol, rejection = analyst_symbol_rejection_reason(raw)
            if symbol and rejection is None and symbol not in seen:
                order[symbol] = len(order)
                category[symbol] = category_rank
                result.append(symbol)
                seen.add(symbol)

    consensus_rows = db.execute(
        select(AnalystConsensusSnapshot.symbol)
        .where(AnalystConsensusSnapshot.availability_status.in_(("available", "partial", "stale")))
        .order_by(AnalystConsensusSnapshot.symbol.asc())
        .limit(5000)
    ).scalars().all()
    append_rows(consensus_rows, 0)

    ticker_rows = db.execute(
        select(TickerMeta.symbol)
        .where(func.upper(TickerMeta.exchange).in_(("NASDAQ", "NYSE", "AMEX", "NYSE AMERICAN")))
        .order_by(TickerMeta.symbol.asc())
        .limit(5000)
    ).scalars().all()
    append_rows(ticker_rows, 1)

    security_rows = db.execute(
        select(Security.symbol)
        .where(func.lower(Security.asset_class).in_(("stock", "equity")))
        .order_by(Security.symbol.asc())
        .limit(5000)
    ).scalars().all()
    append_rows(security_rows, 2)
    result.sort(key=lambda symbol: (category[symbol], progress.get(symbol) is not None, progress.get(symbol) or datetime.min.replace(tzinfo=timezone.utc), order[symbol], symbol))
    return result[:limit] if limit else result


def eligible_price_target_event_symbols(
    db: Session,
    symbols: Iterable[str] | None = None,
    *,
    limit: int | None = None,
    job_name: str = PRICE_TARGET_BACKFILL_JOB,
) -> list[str]:
    if symbols is not None:
        return eligible_equity_symbols(db, symbols, limit=limit)
    progress = _latest_backfill_progress(db, job_name=job_name, event_model=AnalystPriceTargetEvent)
    result: list[str] = []
    seen: set[str] = set()
    order: dict[str, int] = {}
    category: dict[str, int] = {}

    def append_rows(rows: Iterable[str | None], category_rank: int) -> None:
        for raw in rows:
            symbol, rejection = analyst_symbol_rejection_reason(raw)
            if symbol and rejection is None and symbol not in seen:
                order[symbol] = len(order)
                category[symbol] = category_rank
                result.append(symbol)
                seen.add(symbol)

    consensus_rows = db.execute(
        select(AnalystConsensusSnapshot.symbol)
        .where(AnalystConsensusSnapshot.availability_status.in_(("available", "partial", "stale")))
        .order_by(AnalystConsensusSnapshot.symbol.asc())
        .limit(5000)
    ).scalars().all()
    append_rows(consensus_rows, 0)

    ticker_rows = db.execute(
        select(TickerMeta.symbol)
        .where(func.upper(TickerMeta.exchange).in_(("NASDAQ", "NYSE", "AMEX", "NYSE AMERICAN")))
        .order_by(TickerMeta.symbol.asc())
        .limit(5000)
    ).scalars().all()
    append_rows(ticker_rows, 1)

    security_rows = db.execute(
        select(Security.symbol)
        .where(func.lower(Security.asset_class).in_(("stock", "equity")))
        .order_by(Security.symbol.asc())
        .limit(5000)
    ).scalars().all()
    append_rows(security_rows, 2)
    result.sort(key=lambda symbol: (category[symbol], progress.get(symbol) is not None, progress.get(symbol) or datetime.min.replace(tzinfo=timezone.utc), order[symbol], symbol))
    return result[:limit] if limit else result


def start_ingestion_run(db: Session, job_name: str, *, metadata: dict[str, Any] | None = None) -> AnalystConsensusIngestionRun:
    run = AnalystConsensusIngestionRun(
        job_name=job_name,
        started_at=utc_now(),
        status="running",
        metadata_json=_json(metadata or {}),
    )
    db.add(run)
    db.flush()
    return run


def finish_ingestion_run(
    run: AnalystConsensusIngestionRun,
    *,
    status: str,
    attempted: int,
    succeeded: int,
    failed: int,
    inserted: int,
    updated: int,
    provider_errors: list[dict[str, Any]] | None = None,
    error_summary: str | None = None,
) -> None:
    run.completed_at = utc_now()
    run.status = status
    run.symbols_attempted = attempted
    run.symbols_succeeded = succeeded
    run.symbols_failed = failed
    run.records_inserted = inserted
    run.records_updated = updated
    run.provider_errors_json = _json(provider_errors or [])
    run.error_summary = error_summary
    if provider_errors and any("429" in str(item.get("error")) for item in provider_errors):
        run.rate_limit_response = "429"


def _nearest_prior_snapshot(db: Session, symbol: str, on_or_before: date) -> AnalystConsensusSnapshot | None:
    return db.execute(
        select(AnalystConsensusSnapshot)
        .where(AnalystConsensusSnapshot.symbol == symbol)
        .where(AnalystConsensusSnapshot.snapshot_date <= on_or_before)
        .where(AnalystConsensusSnapshot.availability_status.in_(("available", "partial", "stale")))
        .order_by(AnalystConsensusSnapshot.snapshot_date.desc(), AnalystConsensusSnapshot.ingested_at.desc())
        .limit(1)
    ).scalar_one_or_none()


def _grade_event_counts(row: AnalystGradeEvent | None) -> tuple[dict[str, int | None], int | None, float | None] | None:
    if row is None:
        return None
    try:
        raw = json.loads(row.raw_payload_json or "{}")
    except (TypeError, ValueError):
        return None
    if not isinstance(raw, dict):
        return None
    counts = rating_counts_from_row(raw)
    total = total_rating_count(counts)
    weighted = weighted_sentiment(counts)
    if total is None or weighted is None:
        return None
    return counts, total, weighted


def _nearest_prior_grade_counts_event(db: Session, symbol: str, on_or_before: date) -> AnalystGradeEvent | None:
    rows = db.execute(
        select(AnalystGradeEvent)
        .where(AnalystGradeEvent.symbol == symbol)
        .where(AnalystGradeEvent.published_date <= on_or_before)
        .order_by(AnalystGradeEvent.published_date.desc(), AnalystGradeEvent.id.desc())
        .limit(20)
    ).scalars().all()
    for row in rows:
        if _grade_event_counts(row) is not None:
            return row
    return None


def _buy_equivalent_pct_from_counts(counts: dict[str, int | None], total: int | None) -> float | None:
    if not total:
        return None
    bullish = int(counts.get("strong_buy_count") or 0) + int(counts.get("buy_count") or 0)
    return bullish / total * 100


def _sell_equivalent_pct_from_counts(counts: dict[str, int | None], total: int | None) -> float | None:
    if not total:
        return None
    bearish = int(counts.get("sell_count") or 0) + int(counts.get("strong_sell_count") or 0)
    return bearish / total * 100


def _consensus_change_from_grade_counts(
    current: AnalystConsensusSnapshot,
    prior_event: AnalystGradeEvent | None,
) -> dict[str, Any] | None:
    parsed = _grade_event_counts(prior_event)
    if parsed is None or prior_event is None:
        return None
    prior_counts, prior_total, prior_weighted = parsed
    return {
        "comparisonDate": _iso_date(prior_event.published_date),
        "weightedSentimentChange": _delta(current.weighted_rating_value, prior_weighted),
        "medianTargetChange": None,
        "consensusTargetChange": None,
        "analystCountChange": _delta(current.total_rating_count, prior_total),
        "buyEquivalentPctChange": _delta(buy_equivalent_pct(current), _buy_equivalent_pct_from_counts(prior_counts, prior_total)),
        "sellEquivalentPctChange": _delta(sell_equivalent_pct(current), _sell_equivalent_pct_from_counts(prior_counts, prior_total)),
        "targetDispersionChange": None,
        "comparisonSource": "historical_rating_distribution",
    }


def _target_value(row: AnalystPriceTargetEvent) -> float | None:
    return _positive(row.adjusted_price_target) or _positive(row.price_target)


def _price_target_event_summary(
    db: Session,
    symbol: str,
    on_or_before: date,
    *,
    window_days: int = 45,
) -> dict[str, Any] | None:
    rows = db.execute(
        select(AnalystPriceTargetEvent)
        .where(AnalystPriceTargetEvent.symbol == symbol)
        .where(AnalystPriceTargetEvent.published_date <= on_or_before)
        .where(AnalystPriceTargetEvent.published_date >= on_or_before - timedelta(days=window_days))
        .order_by(AnalystPriceTargetEvent.published_date.desc(), AnalystPriceTargetEvent.id.desc())
        .limit(250)
    ).scalars().all()
    values = [value for row in rows if (value := _target_value(row)) is not None]
    if not values:
        fallback = db.execute(
            select(AnalystPriceTargetEvent)
            .where(AnalystPriceTargetEvent.symbol == symbol)
            .where(AnalystPriceTargetEvent.published_date <= on_or_before)
            .order_by(AnalystPriceTargetEvent.published_date.desc(), AnalystPriceTargetEvent.id.desc())
            .limit(1)
        ).scalar_one_or_none()
        fallback_value = _target_value(fallback) if fallback else None
        if fallback is None or fallback_value is None:
            return None
        rows = [fallback]
        values = [fallback_value]
    latest_date = max((row.published_date for row in rows if row.published_date), default=None)
    return {
        "comparisonDate": _iso_date(latest_date),
        "medianTarget": float(median(values)),
        "consensusTarget": sum(values) / len(values),
        "targetObservationCount": len(values),
        "comparisonSource": "historical_price_target_news",
    }


def analyst_trend_series(db: Session, current: AnalystConsensusSnapshot | None, *, days: int = 90) -> dict[str, Any]:
    if current is None:
        return {"startDate": None, "endDate": None, "points": []}
    bounded_days = max(1, min(int(days or 90), 365))
    end = current.snapshot_date
    start = end - timedelta(days=bounded_days)
    target_rows = db.execute(
        select(AnalystPriceTargetEvent.published_date)
        .where(AnalystPriceTargetEvent.symbol == current.symbol)
        .where(AnalystPriceTargetEvent.published_date >= start)
        .where(AnalystPriceTargetEvent.published_date <= end)
        .where(AnalystPriceTargetEvent.published_date.is_not(None))
        .group_by(AnalystPriceTargetEvent.published_date)
        .order_by(AnalystPriceTargetEvent.published_date.asc())
    ).scalars().all()
    grade_rows = db.execute(
        select(AnalystGradeEvent)
        .where(AnalystGradeEvent.symbol == current.symbol)
        .where(AnalystGradeEvent.published_date >= start)
        .where(AnalystGradeEvent.published_date <= end)
        .where(AnalystGradeEvent.published_date.is_not(None))
        .order_by(AnalystGradeEvent.published_date.asc(), AnalystGradeEvent.id.asc())
    ).scalars().all()
    sentiment_by_date: dict[date, dict[str, Any]] = {}
    for row in grade_rows:
        if row.published_date is None:
            continue
        parsed = _grade_event_counts(row)
        if parsed is None:
            continue
        counts, total, weighted = parsed
        sentiment_by_date[row.published_date] = {
            "weightedSentiment": weighted,
            "ratingCount": total,
            "recommendationLabel": recommendation_label(weighted, total),
            "source": "historical_rating_distribution",
            "strongBuyCount": counts.get("strong_buy_count"),
            "buyCount": counts.get("buy_count"),
            "holdCount": counts.get("hold_count"),
            "sellCount": counts.get("sell_count"),
            "strongSellCount": counts.get("strong_sell_count"),
            "buyEquivalentPct": _buy_equivalent_pct_from_counts(counts, total),
            "sellEquivalentPct": _sell_equivalent_pct_from_counts(counts, total),
        }
    dates = {start, end, *[value for value in target_rows if value], *sentiment_by_date.keys()}
    points: list[dict[str, Any]] = []
    latest_sentiment: dict[str, Any] | None = None
    for point_date in sorted(dates):
        if point_date in sentiment_by_date:
            latest_sentiment = sentiment_by_date[point_date]
        target_summary = _price_target_event_summary(db, current.symbol, point_date)
        if point_date == end:
            target_summary = {
                "comparisonDate": _iso_date(end),
                "medianTarget": current.price_target_median,
                "consensusTarget": current.price_target_consensus or current.price_target_average,
                "targetObservationCount": current.price_target_analyst_count,
                "comparisonSource": "consensus_snapshot",
            }
            latest_sentiment = {
                "weightedSentiment": current.weighted_rating_value,
                "ratingCount": current.total_rating_count,
                "recommendationLabel": current.recommendation_label,
                "source": "consensus_snapshot",
                "strongBuyCount": current.strong_buy_count,
                "buyCount": current.buy_count,
                "holdCount": current.hold_count,
                "sellCount": current.sell_count,
                "strongSellCount": current.strong_sell_count,
                "buyEquivalentPct": buy_equivalent_pct(current),
                "sellEquivalentPct": sell_equivalent_pct(current),
            }
        if not target_summary and not latest_sentiment:
            continue
        points.append(
            {
                "date": _iso_date(point_date),
                "consensusTarget": target_summary.get("consensusTarget") if target_summary else None,
                "medianTarget": target_summary.get("medianTarget") if target_summary else None,
                "targetObservationCount": target_summary.get("targetObservationCount") if target_summary else None,
                "targetSource": target_summary.get("comparisonSource") if target_summary else None,
                "weightedSentiment": latest_sentiment.get("weightedSentiment") if latest_sentiment else None,
                "ratingCount": latest_sentiment.get("ratingCount") if latest_sentiment else None,
                "recommendationLabel": latest_sentiment.get("recommendationLabel") if latest_sentiment else None,
                "sentimentSource": latest_sentiment.get("source") if latest_sentiment else None,
                "strongBuyCount": latest_sentiment.get("strongBuyCount") if latest_sentiment else None,
                "buyCount": latest_sentiment.get("buyCount") if latest_sentiment else None,
                "holdCount": latest_sentiment.get("holdCount") if latest_sentiment else None,
                "sellCount": latest_sentiment.get("sellCount") if latest_sentiment else None,
                "strongSellCount": latest_sentiment.get("strongSellCount") if latest_sentiment else None,
                "buyEquivalentPct": latest_sentiment.get("buyEquivalentPct") if latest_sentiment else None,
                "sellEquivalentPct": latest_sentiment.get("sellEquivalentPct") if latest_sentiment else None,
            }
        )
    return {"startDate": _iso_date(start), "endDate": _iso_date(end), "points": points}


def _apply_price_target_change_fallback(
    db: Session,
    current: AnalystConsensusSnapshot,
    change: dict[str, Any],
    on_or_before: date,
) -> dict[str, Any]:
    if change.get("medianTargetChange") is not None or change.get("consensusTargetChange") is not None:
        return change
    summary = _price_target_event_summary(db, current.symbol, on_or_before)
    if not summary:
        return change
    enriched = dict(change)
    enriched["comparisonDate"] = enriched.get("comparisonDate") or summary.get("comparisonDate")
    enriched["medianTargetChange"] = _delta(current.price_target_median, summary.get("medianTarget"))
    enriched["consensusTargetChange"] = _delta(current.price_target_consensus, summary.get("consensusTarget"))
    enriched["targetObservationCount"] = summary.get("targetObservationCount")
    existing_source = enriched.get("comparisonSource")
    enriched["targetComparisonSource"] = summary.get("comparisonSource")
    if not existing_source:
        enriched["comparisonSource"] = summary.get("comparisonSource")
    return enriched


def consensus_changes(db: Session, current: AnalystConsensusSnapshot | None) -> dict[str, Any]:
    if current is None:
        return {"days30": {}, "days90": {}}
    result: dict[str, Any] = {}
    for days in (30, 90):
        comparison_day = current.snapshot_date - timedelta(days=days)
        prior = _nearest_prior_snapshot(db, current.symbol, comparison_day)
        if prior:
            change = {
                "comparisonDate": _iso_date(prior.snapshot_date),
                "weightedSentimentChange": _delta(current.weighted_rating_value, prior.weighted_rating_value),
                "medianTargetChange": _delta(current.price_target_median, prior.price_target_median),
                "consensusTargetChange": _delta(current.price_target_consensus, prior.price_target_consensus),
                "analystCountChange": _delta(current.total_rating_count, prior.total_rating_count),
                "buyEquivalentPctChange": _delta(buy_equivalent_pct(current), buy_equivalent_pct(prior)),
                "sellEquivalentPctChange": _delta(sell_equivalent_pct(current), sell_equivalent_pct(prior)),
                "targetDispersionChange": _delta(current.target_dispersion_pct, prior.target_dispersion_pct),
                "comparisonSource": "consensus_snapshot",
            }
            result[f"days{days}"] = _apply_price_target_change_fallback(db, current, change, comparison_day)
            continue
        prior_event = _nearest_prior_grade_counts_event(db, current.symbol, comparison_day)
        change = _consensus_change_from_grade_counts(current, prior_event) or {
            "comparisonDate": None,
            "weightedSentimentChange": None,
            "medianTargetChange": None,
            "consensusTargetChange": None,
            "analystCountChange": None,
            "buyEquivalentPctChange": None,
            "sellEquivalentPctChange": None,
            "targetDispersionChange": None,
        }
        result[f"days{days}"] = _apply_price_target_change_fallback(db, current, change, comparison_day)
    return result


def grade_event_stats(db: Session, symbol: str, *, as_of: date | None = None) -> dict[str, Any]:
    normalized = normalize_symbol(symbol)
    if not normalized:
        return {}
    today = as_of or utc_now().date()
    rows = db.execute(
        select(AnalystGradeEvent)
        .where(AnalystGradeEvent.symbol == normalized)
        .where(AnalystGradeEvent.published_date >= today - timedelta(days=90))
        .order_by(AnalystGradeEvent.published_date.desc(), AnalystGradeEvent.id.desc())
    ).scalars().all()
    stats: dict[str, Any] = {}
    for days in (30, 90):
        cutoff = today - timedelta(days=days)
        scoped = [row for row in rows if row.published_date and row.published_date >= cutoff]
        upgrades = sum(1 for row in scoped if row.action == "Upgrade")
        downgrades = sum(1 for row in scoped if row.action == "Downgrade")
        stats[f"days{days}"] = {"upgrades": upgrades, "downgrades": downgrades, "netActions": upgrades - downgrades}
    recent = rows[0] if rows else None
    stats["mostRecentEvent"] = grade_event_payload(recent) if recent else None
    stats["daysSinceMostRecentEvent"] = (today - recent.published_date).days if recent and recent.published_date else None
    return stats


def coverage_confidence(snapshot: AnalystConsensusSnapshot | None, changes: dict[str, Any]) -> dict[str, Any]:
    if snapshot is None:
        return {"level": "Insufficient", "reasons": ["No stored analyst consensus snapshot is available."]}
    reasons: list[str] = []
    rating_count = snapshot.total_rating_count or 0
    target_count = snapshot.price_target_analyst_count or 0
    freshness_days = (utc_now().date() - snapshot.snapshot_date).days
    if rating_count >= 15:
        reasons.append("Rating coverage is broad.")
    elif rating_count >= 5:
        reasons.append("Rating coverage is moderate.")
    elif rating_count > 0:
        reasons.append("Rating coverage is thin.")
    else:
        reasons.append("Rating coverage is unavailable.")
    if target_count >= 10:
        reasons.append("Price-target coverage is broad.")
    elif target_count > 0:
        reasons.append("Price-target coverage is available but limited.")
    else:
        reasons.append("Price-target analyst count is unavailable.")
    if freshness_days <= FRESHNESS_DAYS:
        reasons.append("Snapshot is fresh.")
    else:
        reasons.append("Snapshot is older than the freshness threshold.")
    has_history = any((changes.get(key) or {}).get("comparisonDate") for key in ("days30", "days90"))
    if has_history:
        reasons.append("Historical comparison observations are available.")
    if rating_count >= 15 and target_count >= 10 and freshness_days <= FRESHNESS_DAYS and has_history:
        level = "High"
    elif rating_count >= 5 and freshness_days <= STALE_DAYS:
        level = "Moderate"
    elif rating_count > 0 or target_count > 0:
        level = "Low"
    else:
        level = "Insufficient"
    return {"level": level, "reasons": reasons}


def target_dispersion_level(value: float | None) -> str:
    if value is None:
        return "Unavailable"
    if value < 20:
        return "Low"
    if value < 50:
        return "Moderate"
    return "High"


def interpret_consensus(snapshot: AnalystConsensusSnapshot | None, changes: dict[str, Any], event_stats: dict[str, Any]) -> dict[str, Any]:
    if snapshot is None:
        return {
            "currentAnalystDirection": "Unavailable",
            "trendDirection": "Unavailable",
            "combinedLabel": "Unavailable",
            "coverageLevel": "Insufficient",
            "targetDispersionLevel": "Unavailable",
            "supportingFacts": [],
            "contradictingFacts": [],
            "freshness": {"status": "unavailable"},
            "dataAvailability": "unavailable",
        }
    freshness_days = (utc_now().date() - snapshot.snapshot_date).days
    freshness_status = "fresh" if freshness_days <= FRESHNESS_DAYS else "stale" if freshness_days > STALE_DAYS else "aging"
    current = snapshot.recommendation_label or "Insufficient Coverage"
    change30 = changes.get("days30") or {}
    change90 = changes.get("days90") or {}
    score = 0.0
    for weight, bucket in ((1.0, change30), (0.5, change90)):
        score += weight * (bucket.get("weightedSentimentChange") or 0)
        score += weight * ((bucket.get("medianTargetChange") or 0) / max(abs(snapshot.current_price_at_snapshot or 1), 1))
        score += weight * ((bucket.get("consensusTargetChange") or 0) / max(abs(snapshot.current_price_at_snapshot or 1), 1))
    score += 0.25 * ((event_stats.get("days30") or {}).get("netActions") or 0)
    if snapshot.consensus_implied_upside_pct is not None:
        score += max(min(snapshot.consensus_implied_upside_pct / 100, 0.3), -0.3)
    if score > 0.25:
        trend = "Improving"
    elif score < -0.25:
        trend = "Weakening"
    else:
        trend = "Stable"
    base = "Neutral"
    if "Bullish" in current:
        base = "Bullish"
    elif "Bearish" in current:
        base = "Bearish"
    if current == "Insufficient Coverage":
        combined = "Insufficient coverage"
    elif base == "Bullish" and trend == "Improving":
        combined = "Bullish and improving"
    elif base == "Bullish" and trend == "Weakening":
        combined = "Bullish but weakening"
    elif base == "Bullish":
        combined = "Bullish and stable"
    elif base == "Bearish" and trend == "Improving":
        combined = "Bearish but improving"
    elif base == "Bearish" and trend == "Weakening":
        combined = "Bearish and worsening"
    elif base == "Bearish":
        combined = "Bearish and stable"
    elif trend == "Improving":
        combined = "Neutral and improving"
    elif trend == "Weakening":
        combined = "Neutral and weakening"
    else:
        combined = "Neutral"
    supporting: list[str] = []
    contradicting: list[str] = []
    if snapshot.consensus_implied_upside_pct is not None:
        fact = f"Consensus target implies {round(snapshot.consensus_implied_upside_pct, 1)}% upside/downside."
        (supporting if snapshot.consensus_implied_upside_pct >= 0 else contradicting).append(fact)
    if (event_stats.get("days30") or {}).get("netActions"):
        supporting.append(f"Net rating actions over 30 days: {(event_stats.get('days30') or {}).get('netActions')}.")
    confidence = coverage_confidence(snapshot, changes)
    return {
        "currentAnalystDirection": current,
        "trendDirection": trend,
        "combinedLabel": combined,
        "coverageLevel": confidence["level"],
        "targetDispersionLevel": target_dispersion_level(snapshot.target_dispersion_pct),
        "supportingFacts": supporting,
        "contradictingFacts": contradicting,
        "freshness": {"status": freshness_status, "daysOld": freshness_days},
        "dataAvailability": snapshot.availability_status,
        "methodologyVersion": METHODOLOGY_VERSION,
    }


def latest_snapshot(db: Session, symbol: str) -> AnalystConsensusSnapshot | None:
    normalized = normalize_symbol(symbol)
    if not normalized:
        return None
    return db.execute(
        select(AnalystConsensusSnapshot)
        .where(AnalystConsensusSnapshot.symbol == normalized)
        .order_by(AnalystConsensusSnapshot.snapshot_date.desc(), AnalystConsensusSnapshot.ingested_at.desc())
        .limit(1)
    ).scalar_one_or_none()


def _snapshot_availability(snapshot: AnalystConsensusSnapshot) -> str:
    days_old = (utc_now().date() - snapshot.snapshot_date).days
    if snapshot.provider_status == "provider_error":
        return "provider_error"
    if days_old > STALE_DAYS:
        return "stale"
    return snapshot.availability_status


def snapshot_payload(snapshot: AnalystConsensusSnapshot | None) -> dict[str, Any] | None:
    if snapshot is None:
        return None
    return {
        "symbol": snapshot.symbol,
        "snapshotDate": _iso_date(snapshot.snapshot_date),
        "recommendationDistribution": {
            "strongBuy": snapshot.strong_buy_count,
            "buy": snapshot.buy_count,
            "hold": snapshot.hold_count,
            "sell": snapshot.sell_count,
            "strongSell": snapshot.strong_sell_count,
            "total": snapshot.total_rating_count,
        },
        "weightedRatingValue": snapshot.weighted_rating_value,
        "recommendationLabel": snapshot.recommendation_label,
        "priceTargetRange": {
            "high": snapshot.price_target_high,
            "low": snapshot.price_target_low,
            "median": snapshot.price_target_median,
            "consensus": snapshot.price_target_consensus,
            "average": snapshot.price_target_average,
            "analystCount": snapshot.price_target_analyst_count,
        },
        "currentPriceAtSnapshot": snapshot.current_price_at_snapshot,
        "currentPriceSource": snapshot.current_price_source,
        "currentPriceAsOf": _iso_dt(snapshot.current_price_as_of),
        "impliedUpside": {
            "medianPct": snapshot.median_implied_upside_pct,
            "consensusPct": snapshot.consensus_implied_upside_pct,
        },
        "targetDispersionPct": snapshot.target_dispersion_pct,
        "availabilityStatus": _snapshot_availability(snapshot),
        "providerStatus": snapshot.provider_status,
        "providerError": snapshot.provider_error,
        "source": snapshot.source,
        "sourceUpdatedAt": _iso_dt(snapshot.source_updated_at),
        "ingestedAt": _iso_dt(snapshot.ingested_at),
    }


def snapshot_summary_payload(snapshot: AnalystConsensusSnapshot | None) -> dict[str, Any] | None:
    if snapshot is None:
        return None
    return {
        "symbol": snapshot.symbol,
        "snapshotDate": _iso_date(snapshot.snapshot_date),
        "recommendationLabel": snapshot.recommendation_label,
        "totalRatingCount": snapshot.total_rating_count,
        "consensusImpliedUpsidePct": snapshot.consensus_implied_upside_pct,
        "medianImpliedUpsidePct": snapshot.median_implied_upside_pct,
        "availabilityStatus": _snapshot_availability(snapshot),
        "providerStatus": snapshot.provider_status,
        "ingestedAt": _iso_dt(snapshot.ingested_at),
    }


def _summary_interpretation(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "currentAnalystDirection": payload.get("currentAnalystDirection"),
        "trendDirection": payload.get("trendDirection"),
        "combinedLabel": payload.get("combinedLabel"),
        "coverageLevel": payload.get("coverageLevel"),
        "freshness": payload.get("freshness"),
        "dataAvailability": payload.get("dataAvailability"),
        "methodologyVersion": payload.get("methodologyVersion"),
    }


def current_consensus_payload(db: Session, symbol: str, *, include_details: bool = True) -> dict[str, Any]:
    normalized, rejection = analyst_symbol_rejection_reason(symbol)
    if rejection or not normalized:
        return {
            "symbol": normalized or symbol,
            "access": {"detailLevel": "current_summary", "detailsLocked": not include_details, "requiredPlanForDetails": "premium"},
            "availability": {"status": "unsupported", "reason": rejection},
            "providerStatus": {"status": "unsupported"},
        }
    snapshot = latest_snapshot(db, normalized)
    changes = consensus_changes(db, snapshot)
    event_stats = grade_event_stats(db, normalized, as_of=snapshot.snapshot_date if snapshot else None)
    interpretation = interpret_consensus(snapshot, changes, event_stats)
    confidence = coverage_confidence(snapshot, changes)
    availability = _snapshot_availability(snapshot) if snapshot else "unavailable"
    access = {
        "detailLevel": "full_detail" if include_details else "current_summary",
        "detailsLocked": not include_details,
        "requiredPlanForDetails": "premium",
    }
    if not include_details:
        return {
            "symbol": normalized,
            "access": access,
            "currentSnapshot": snapshot_summary_payload(snapshot),
            "currentSummary": {
                "recommendationLabel": snapshot.recommendation_label if snapshot else None,
                "combinedLabel": interpretation.get("combinedLabel"),
                "trendDirection": interpretation.get("trendDirection"),
                "coverageLevel": confidence["level"],
                "consensusImpliedUpsidePct": snapshot.consensus_implied_upside_pct if snapshot else None,
                "medianImpliedUpsidePct": snapshot.median_implied_upside_pct if snapshot else None,
            },
            "interpretation": _summary_interpretation(interpretation),
            "freshness": interpretation.get("freshness"),
            "availability": {"status": availability},
            "providerStatus": {
                "status": snapshot.provider_status if snapshot else "unavailable",
                "error": snapshot.provider_error if snapshot else None,
            },
        }
    return {
        "symbol": normalized,
        "access": access,
        "currentSnapshot": snapshot_payload(snapshot),
        "changes": changes,
        "trendSeries": analyst_trend_series(db, snapshot, days=90),
        "gradeEventStats": event_stats,
        "interpretation": interpretation,
        "coverage": confidence,
        "freshness": interpretation.get("freshness"),
        "availability": {"status": availability},
        "providerStatus": {
            "status": snapshot.provider_status if snapshot else "unavailable",
            "error": snapshot.provider_error if snapshot else None,
        },
    }


def history_payload(
    db: Session,
    symbol: str,
    *,
    days: int | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
) -> dict[str, Any]:
    normalized = normalize_symbol(symbol)
    if not normalized:
        return {"symbol": symbol, "points": [], "availability": {"status": "unsupported"}}
    bounded_days = max(1, min(int(days or DEFAULT_HISTORY_DAYS), MAX_HISTORY_DAYS))
    end = end_date or utc_now().date()
    start = start_date or (end - timedelta(days=bounded_days))
    if (end - start).days > MAX_HISTORY_DAYS:
        start = end - timedelta(days=MAX_HISTORY_DAYS)
    rows = db.execute(
        select(AnalystConsensusSnapshot)
        .where(AnalystConsensusSnapshot.symbol == normalized)
        .where(AnalystConsensusSnapshot.snapshot_date >= start)
        .where(AnalystConsensusSnapshot.snapshot_date <= end)
        .order_by(AnalystConsensusSnapshot.snapshot_date.asc(), AnalystConsensusSnapshot.id.asc())
    ).scalars().all()
    return {
        "symbol": normalized,
        "startDate": start.isoformat(),
        "endDate": end.isoformat(),
        "maxDays": MAX_HISTORY_DAYS,
        "points": [
            {
                "date": _iso_date(row.snapshot_date),
                "weightedSentiment": row.weighted_rating_value,
                "recommendationCounts": {
                    "strongBuy": row.strong_buy_count,
                    "buy": row.buy_count,
                    "hold": row.hold_count,
                    "sell": row.sell_count,
                    "strongSell": row.strong_sell_count,
                    "total": row.total_rating_count,
                },
                "medianTarget": row.price_target_median,
                "consensusTarget": row.price_target_consensus,
                "highTarget": row.price_target_high,
                "lowTarget": row.price_target_low,
                "currentPriceAtSnapshot": row.current_price_at_snapshot,
                "targetAnalystCount": row.price_target_analyst_count,
                "medianImpliedUpsidePct": row.median_implied_upside_pct,
                "consensusImpliedUpsidePct": row.consensus_implied_upside_pct,
                "targetDispersionPct": row.target_dispersion_pct,
                "availabilityStatus": row.availability_status,
            }
            for row in rows
        ],
    }


def grade_event_payload(row: AnalystGradeEvent | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {
        "id": row.id,
        "symbol": row.symbol,
        "gradingCompany": row.grading_company,
        "analystName": row.analyst_name,
        "previousGrade": row.previous_grade,
        "newGrade": row.new_grade,
        "action": row.action,
        "providerAction": row.provider_action,
        "publishedDate": _iso_date(row.published_date),
        "source": row.source,
        "ingestedAt": _iso_dt(row.ingested_at),
    }


def price_target_event_payload(row: AnalystPriceTargetEvent | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {
        "id": row.id,
        "symbol": row.symbol,
        "analystCompany": row.analyst_company,
        "analystName": row.analyst_name,
        "priceTarget": row.price_target,
        "adjustedPriceTarget": row.adjusted_price_target,
        "priceWhenPosted": row.price_when_posted,
        "publishedDate": _iso_date(row.published_date),
        "publishedAt": _iso_dt(row.published_at),
        "newsTitle": row.news_title,
        "newsPublisher": row.news_publisher,
        "newsUrl": row.news_url,
        "source": row.source,
        "ingestedAt": _iso_dt(row.ingested_at),
    }


def events_payload(
    db: Session,
    symbol: str,
    *,
    limit: int = 100,
    start_date: date | None = None,
    end_date: date | None = None,
    action: str | None = None,
) -> dict[str, Any]:
    normalized = normalize_symbol(symbol)
    if not normalized:
        return {"symbol": symbol, "items": [], "availability": {"status": "unsupported"}}
    bounded_limit = max(1, min(int(limit or 100), 500))
    statement = select(AnalystGradeEvent).where(AnalystGradeEvent.symbol == normalized)
    if start_date:
        statement = statement.where(AnalystGradeEvent.published_date >= start_date)
    if end_date:
        statement = statement.where(AnalystGradeEvent.published_date <= end_date)
    normalized_action = normalize_action(action)
    if normalized_action:
        statement = statement.where(AnalystGradeEvent.action == normalized_action)
    rows = db.execute(
        statement.order_by(AnalystGradeEvent.published_date.desc(), AnalystGradeEvent.id.desc()).limit(bounded_limit)
    ).scalars().all()
    target_statement = select(AnalystPriceTargetEvent).where(AnalystPriceTargetEvent.symbol == normalized)
    if start_date:
        target_statement = target_statement.where(AnalystPriceTargetEvent.published_date >= start_date)
    if end_date:
        target_statement = target_statement.where(AnalystPriceTargetEvent.published_date <= end_date)
    target_rows = db.execute(
        target_statement.order_by(AnalystPriceTargetEvent.published_date.desc(), AnalystPriceTargetEvent.id.desc()).limit(bounded_limit)
    ).scalars().all()
    return {
        "symbol": normalized,
        "limit": bounded_limit,
        "items": [grade_event_payload(row) for row in rows],
        "targetItems": [price_target_event_payload(row) for row in target_rows],
    }


def compare_consensus_payload(
    db: Session,
    symbols: Iterable[str],
    *,
    max_symbols: int = 8,
    include_details: bool = True,
) -> dict[str, Any]:
    normalized_symbols = []
    for raw in symbols:
        normalized = normalize_symbol(raw)
        if normalized and normalized not in normalized_symbols:
            normalized_symbols.append(normalized)
    normalized_symbols = normalized_symbols[:max_symbols]
    if not normalized_symbols:
        return {"symbols": [], "items": {}, "maxSymbols": max_symbols}
    latest_dates = (
        select(
            AnalystConsensusSnapshot.symbol.label("symbol"),
            func.max(AnalystConsensusSnapshot.snapshot_date).label("snapshot_date"),
        )
        .where(AnalystConsensusSnapshot.symbol.in_(normalized_symbols))
        .group_by(AnalystConsensusSnapshot.symbol)
        .subquery()
    )
    rows = db.execute(
        select(AnalystConsensusSnapshot)
        .join(
            latest_dates,
            (AnalystConsensusSnapshot.symbol == latest_dates.c.symbol)
            & (AnalystConsensusSnapshot.snapshot_date == latest_dates.c.snapshot_date),
        )
    ).scalars().all()
    by_symbol = {
        row.symbol: {
            "symbol": row.symbol,
            "currentSnapshot": snapshot_payload(row) if include_details else snapshot_summary_payload(row),
            "summary": {
                "recommendationLabel": row.recommendation_label,
                "weightedRatingValue": row.weighted_rating_value if include_details else None,
                "consensusImpliedUpsidePct": row.consensus_implied_upside_pct,
                "targetDispersionPct": row.target_dispersion_pct if include_details else None,
                "availabilityStatus": _snapshot_availability(row),
                "providerStatus": row.provider_status,
            },
        }
        for row in rows
    }
    return {
        "symbols": normalized_symbols,
        "maxSymbols": max_symbols,
        "access": {
            "detailLevel": "full_detail" if include_details else "current_summary",
            "detailsLocked": not include_details,
            "requiredPlanForDetails": "premium",
        },
        "items": {
            symbol: by_symbol.get(symbol)
            or {
                "symbol": symbol,
                "currentSnapshot": None,
                "summary": {"availabilityStatus": "unavailable", "providerStatus": "unavailable"},
            }
            for symbol in normalized_symbols
        },
    }


def analyst_consensus_component_inputs(db: Session, symbol: str) -> dict[str, Any]:
    snapshot = latest_snapshot(db, symbol)
    changes = consensus_changes(db, snapshot)
    event_stats = grade_event_stats(db, normalize_symbol(symbol) or symbol, as_of=snapshot.snapshot_date if snapshot else None)
    confidence = coverage_confidence(snapshot, changes)
    return {
        "symbol": normalize_symbol(symbol),
        "methodologyVersion": METHODOLOGY_VERSION,
        "liveWeightAssigned": False,
        "inputs": {
            "weightedRatingValue": snapshot.weighted_rating_value if snapshot else None,
            "weightedSentimentChange30d": (changes.get("days30") or {}).get("weightedSentimentChange"),
            "weightedSentimentChange90d": (changes.get("days90") or {}).get("weightedSentimentChange"),
            "netRatingActions30d": (event_stats.get("days30") or {}).get("netActions"),
            "netRatingActions90d": (event_stats.get("days90") or {}).get("netActions"),
            "medianTargetChange30d": (changes.get("days30") or {}).get("medianTargetChange"),
            "consensusTargetChange30d": (changes.get("days30") or {}).get("consensusTargetChange"),
            "consensusImpliedUpsidePct": snapshot.consensus_implied_upside_pct if snapshot else None,
            "coverageLevel": confidence["level"],
            "targetDispersionPct": snapshot.target_dispersion_pct if snapshot else None,
            "freshnessStatus": _snapshot_availability(snapshot) if snapshot else "unavailable",
        },
        "notes": [
            "Analyst consensus is treated as a capped Walnut confirmation input.",
            "Live weighting remains behind the analyst-consensus live-weight kill switch.",
        ],
    }
