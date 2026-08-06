from __future__ import annotations

import argparse
import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.db import Base, SessionLocal, engine, ensure_outcome_ledger_schema, ensure_price_cache_volume_columns
from app.models import (
    ConfirmationMethodologyVersion,
    ConfirmationMonitoringEvent,
    ConfirmationScoreSnapshot,
    PriceCache,
    Security,
    TickerMeta,
)
from app.services.confirmation_score import (
    CONFIRMATION_CLASSIFICATION_VERSION,
    SOURCE_ORDER,
    confirmation_band_for_score,
)
from app.services.outcome_ledger import (
    OUTCOME_HORIZONS,
    current_code_commit_sha,
    current_confirmation_methodology,
)
from app.services.price_lookup import get_daily_close_series_with_fallback
from app.utils.symbols import normalize_symbol

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class HistoricalScorePoint:
    ticker: str
    observed_at: datetime
    score: int
    band: str
    direction: str
    source_count: int
    status: str
    source_kind: str
    source_id: int


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _parse_payload(raw: str | None) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _coerce_score(value: Any) -> int | None:
    try:
        return max(0, min(100, int(round(float(value)))))
    except (TypeError, ValueError):
        return None


def _coerce_band(value: Any, score: int) -> str:
    text = str(value or "").strip().lower()
    if text in {"inactive", "weak", "moderate", "strong", "exceptional"}:
        return text
    return confirmation_band_for_score(score)


def _coerce_direction(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text in {"bullish", "bearish", "neutral", "mixed"}:
        return text
    return "neutral"


def _resolve_security(db: Session, symbol: str) -> Security | None:
    security = db.execute(select(Security).where(func.upper(Security.symbol) == symbol)).scalar_one_or_none()
    if security is not None:
        return security
    ticker_meta = db.get(TickerMeta, symbol)
    security = Security(
        symbol=symbol,
        name=(ticker_meta.company_name if ticker_meta else None) or symbol,
        asset_class="equity",
        sector=(ticker_meta.sector if ticker_meta else None),
    )
    db.add(security)
    db.flush()
    return security


def _price_on_or_before(db: Session, symbol: str, target_day: date) -> PriceCache | None:
    normalized = symbol.strip().upper()
    return db.execute(
        select(PriceCache)
        .where(
            PriceCache.symbol == normalized,
            PriceCache.date <= target_day.isoformat(),
        )
        .order_by(PriceCache.date.desc())
        .limit(1)
    ).scalar_one_or_none()


def _price_day(row: PriceCache) -> date | None:
    try:
        return date.fromisoformat(str(row.date)[:10])
    except (TypeError, ValueError):
        return None


def _hydrate_price_window(db: Session, symbol: str, start_day: date, end_day: date) -> None:
    get_daily_close_series_with_fallback(
        db,
        symbol,
        start_day.isoformat(),
        end_day.isoformat(),
        release_connection_before_provider=True,
    )


def _bundle_for_point(point: HistoricalScorePoint) -> dict[str, Any]:
    return {
        "ticker": point.ticker,
        "score": point.score,
        "band": point.band,
        "direction": point.direction,
        "status": point.status,
        "classification_version": CONFIRMATION_CLASSIFICATION_VERSION,
        "active_sources": [],
        "sources": {},
        "source_count": point.source_count,
        "observed_at": point.observed_at.isoformat(),
        "backfill_source": point.source_kind,
        "backfill_source_id": point.source_id,
    }


def _input_hash(point: HistoricalScorePoint, methodology: ConfirmationMethodologyVersion) -> str:
    payload = {
        "methodology_version": methodology.version,
        "ticker": point.ticker,
        "score": point.score,
        "band": point.band,
        "direction": point.direction,
        "status": point.status,
        "source_count": point.source_count,
        "observed_at": point.observed_at.date().isoformat(),
        "source_kind": point.source_kind,
        "source_id": point.source_id,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _active_sources_placeholder(point: HistoricalScorePoint) -> list[str]:
    # We know the count from the monitoring event, but not the exact source payload.
    return list(SOURCE_ORDER[: max(0, min(point.source_count, len(SOURCE_ORDER)))])


def _source_contributions_placeholder(point: HistoricalScorePoint) -> dict[str, Any]:
    return {
        source: {
            "present": True,
            "direction": None,
            "strength": None,
            "quality": None,
            "score_contribution": None,
            "label": None,
            "detail": None,
            "summary": None,
        }
        for source in _active_sources_placeholder(point)
    }


def _event_points(event: ConfirmationMonitoringEvent, *, include_before: bool) -> list[HistoricalScorePoint]:
    ticker = normalize_symbol(event.ticker)
    if not ticker or event.created_at is None:
        return []
    created_at = _utc(event.created_at)
    payload = _parse_payload(event.payload_json)
    points: list[HistoricalScorePoint] = []

    if include_before:
        score_before = _coerce_score(event.score_before)
        if score_before is not None:
            points.append(
                HistoricalScorePoint(
                    ticker=ticker,
                    observed_at=created_at - timedelta(seconds=1),
                    score=score_before,
                    band=_coerce_band(event.band_before or payload.get("band_before"), score_before),
                    direction=_coerce_direction(event.direction_before or payload.get("direction_before")),
                    source_count=max(0, int(event.source_count_before or payload.get("source_count_before") or 0)),
                    status=str(payload.get("status_before") or event.event_type or "Historical signal"),
                    source_kind="confirmation_monitoring_event_before",
                    source_id=int(event.id),
                )
            )

    score_after = _coerce_score(event.score_after)
    if score_after is not None:
        points.append(
            HistoricalScorePoint(
                ticker=ticker,
                observed_at=created_at,
                score=score_after,
                band=_coerce_band(event.band_after or payload.get("band_after"), score_after),
                direction=_coerce_direction(event.direction_after or payload.get("direction_after")),
                source_count=max(0, int(event.source_count_after or payload.get("source_count_after") or 0)),
                status=str(payload.get("status_after") or event.event_type or "Historical signal"),
                source_kind="confirmation_monitoring_event_after",
                source_id=int(event.id),
            )
        )
    return points


def _load_points(
    db: Session,
    *,
    since: datetime,
    limit: int,
    min_score: int,
    min_source_count: int,
    include_before: bool,
) -> list[HistoricalScorePoint]:
    rows = (
        db.execute(
            select(ConfirmationMonitoringEvent)
            .where(ConfirmationMonitoringEvent.created_at >= since)
            .where(ConfirmationMonitoringEvent.ticker.is_not(None))
            .order_by(ConfirmationMonitoringEvent.created_at.asc(), ConfirmationMonitoringEvent.id.asc())
            .limit(max(1, limit))
        )
        .scalars()
        .all()
    )
    points: list[HistoricalScorePoint] = []
    seen: set[tuple[str, date, int, str, str]] = set()
    for event in rows:
        for point in _event_points(event, include_before=include_before):
            if point.score < min_score and point.source_count < min_source_count:
                continue
            key = (point.ticker, point.observed_at.date(), point.score, point.direction, point.source_kind)
            if key in seen:
                continue
            seen.add(key)
            points.append(point)
    return points


def _snapshot_exists(
    db: Session,
    *,
    security_id: int,
    methodology_id: int,
    market_date: date,
    input_hash: str,
    calculation_type: str,
) -> bool:
    return (
        db.execute(
            select(ConfirmationScoreSnapshot.id).where(
                ConfirmationScoreSnapshot.security_id == security_id,
                ConfirmationScoreSnapshot.methodology_version_id == methodology_id,
                ConfirmationScoreSnapshot.market_date == market_date,
                ConfirmationScoreSnapshot.input_hash == input_hash,
                ConfirmationScoreSnapshot.calculation_type == calculation_type,
            )
        ).scalar_one_or_none()
        is not None
    )


def backfill_outcome_ledger_history(
    db: Session,
    *,
    since_days: int = 365,
    limit: int = 1000,
    min_score: int = 40,
    min_source_count: int = 1,
    include_before: bool = True,
    hydrate_prices: bool = True,
    dry_run: bool = False,
    calculation_type: str = "historical_reconstruction",
) -> dict[str, Any]:
    methodology = current_confirmation_methodology(db)
    since = datetime.now(timezone.utc) - timedelta(days=max(1, int(since_days or 365)))
    points = _load_points(
        db,
        since=since,
        limit=max(1, int(limit or 1000)),
        min_score=max(0, int(min_score or 0)),
        min_source_count=max(0, int(min_source_count or 0)),
        include_before=include_before,
    )
    report: dict[str, Any] = {
        "candidate_points": len(points),
        "created": 0,
        "skipped_existing": 0,
        "skipped_missing_price": 0,
        "hydrated_price_windows": 0,
        "items": [],
    }
    today = datetime.now(timezone.utc).date()

    for point in points:
        security = _resolve_security(db, point.ticker)
        if security is None:
            continue
        observed_day = point.observed_at.date()
        end_day = min(today, observed_day + timedelta(days=max(OUTCOME_HORIZONS) + 7))
        if hydrate_prices:
            for symbol in (point.ticker, "SPY"):
                try:
                    _hydrate_price_window(db, symbol, observed_day - timedelta(days=5), end_day)
                    report["hydrated_price_windows"] += 1
                except Exception as exc:
                    logger.info("outcome_ledger_history_price_hydration_failed symbol=%s error=%s", symbol, exc)

        entry_price = _price_on_or_before(db, point.ticker, observed_day)
        entry_day = _price_day(entry_price) if entry_price is not None else None
        if entry_price is None or entry_day is None:
            report["skipped_missing_price"] += 1
            continue

        input_hash = _input_hash(point, methodology)
        if _snapshot_exists(
            db,
            security_id=security.id,
            methodology_id=methodology.id,
            market_date=entry_day,
            input_hash=input_hash,
            calculation_type=calculation_type,
        ):
            report["skipped_existing"] += 1
            continue

        active_sources = _active_sources_placeholder(point)
        snapshot = ConfirmationScoreSnapshot(
            security_id=security.id,
            ticker_at_time=point.ticker,
            calculated_at=point.observed_at,
            market_date=entry_day,
            score=point.score,
            direction=point.direction,
            strength=point.band,
            reference_price=float(entry_price.close),
            reference_price_at=datetime.combine(entry_day, time(21, 0), tzinfo=timezone.utc),
            reference_price_source=entry_price.price_source or "price_cache",
            active_source_count=point.source_count,
            active_sources_json=json.dumps(active_sources, sort_keys=True, separators=(",", ":")),
            source_contributions_json=json.dumps(_source_contributions_placeholder(point), sort_keys=True, separators=(",", ":")),
            source_freshness_json=json.dumps({}, sort_keys=True, separators=(",", ":")),
            input_hash=input_hash,
            methodology_version_id=methodology.id,
            calculation_type=calculation_type,
            code_commit_sha=current_code_commit_sha(),
            correction_reason=f"backfilled:{point.source_kind}:{point.source_id}",
        )
        report["created"] += 1
        if len(report["items"]) < 25:
            report["items"].append(
                {
                    "ticker": point.ticker,
                    "market_date": entry_day.isoformat(),
                    "score": point.score,
                    "direction": point.direction,
                    "source": point.source_kind,
                    "source_id": point.source_id,
                    "status": "would_create" if dry_run else "created",
                }
            )
        if not dry_run:
            db.add(snapshot)
            if report["created"] % 100 == 0:
                db.commit()

    if dry_run:
        db.rollback()
    else:
        db.commit()
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill Outcome Ledger rows from stored monitoring score history.")
    parser.add_argument("--since-days", type=int, default=365)
    parser.add_argument("--limit", type=int, default=1000)
    parser.add_argument("--min-score", type=int, default=40)
    parser.add_argument("--min-source-count", type=int, default=1)
    parser.add_argument("--skip-before", action="store_true", help="Only write the post-change score from each monitoring event.")
    parser.add_argument("--skip-price-hydration", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--calculation-type",
        choices=("historical_reconstruction", "data_correction", "manual_test"),
        default="historical_reconstruction",
        help="Ledger calculation type to use for reconstructed historical rows.",
    )
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()
    logging.basicConfig(level=getattr(logging, str(args.log_level).upper(), logging.INFO))
    Base.metadata.create_all(bind=engine)
    ensure_price_cache_volume_columns(engine)
    ensure_outcome_ledger_schema(engine)
    with SessionLocal() as db:
        report = backfill_outcome_ledger_history(
            db,
            since_days=args.since_days,
            limit=args.limit,
            min_score=args.min_score,
            min_source_count=args.min_source_count,
            include_before=not args.skip_before,
            hydrate_prices=not args.skip_price_hydration,
            dry_run=args.dry_run,
            calculation_type=args.calculation_type,
        )
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
