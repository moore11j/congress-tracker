from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, getcontext
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from app.models import (
    ConfirmationScoreSnapshot,
    OutcomeEntry,
    OutcomeEvidenceProvenance,
    OutcomeHorizonObservation,
    PriceCache,
    TickerContextBundleCache,
)


getcontext().prec = 34
OUTCOME_AUDIT_VERSION = "outcomes-integrity-v1"
OUTCOME_RETURN_METHODOLOGY = "outcomes-v3-next-executable-open-calendar-horizons"
OUTCOME_HORIZONS = (7, 30, 90, 180, 365)
MARKET_TZ = ZoneInfo("America/New_York")
UTC = timezone.utc


def as_utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def qualifying_event_at(snapshot: ConfirmationScoreSnapshot) -> datetime:
    """Earliest defensible publication time for an Outcome signal."""
    calculated = as_utc(snapshot.calculated_at)
    created = getattr(snapshot, "created_at", None)
    if str(snapshot.calculation_type or "").strip().lower() == "live" and isinstance(created, datetime):
        return max(calculated, as_utc(created))
    return calculated


def market_open_at(day: date) -> datetime:
    return datetime.combine(day, time(9, 30), tzinfo=MARKET_TZ).astimezone(UTC)


def market_close_at(day: date) -> datetime:
    return datetime.combine(day, time(16, 0), tzinfo=MARKET_TZ).astimezone(UTC)


def _decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    parsed = Decimal(str(value))
    return parsed if parsed.is_finite() else None


def adjusted_price(row: PriceCache | None, kind: str) -> Decimal | None:
    if row is None:
        return None
    if kind == "close":
        return _decimal(row.adjusted_close if row.adjusted_close is not None else row.close)
    raw_value = {"open": row.open_price, "high": row.high_price, "low": row.low_price}[kind]
    value = _decimal(raw_value)
    if value is None:
        return None
    adjusted = _decimal(row.adjusted_close)
    raw_close = _decimal(row.raw_close)
    if adjusted is not None and raw_close is not None and adjusted > 0 and raw_close > 0:
        value *= adjusted / raw_close
    return value


def _price_day(row: PriceCache) -> date:
    return date.fromisoformat(str(row.date)[:10])


def _canonical_price_basis(row: PriceCache | None) -> bool:
    """Require one internally consistent split-adjusted price-return basis."""
    return row is not None and row.adjustment_status == "split_adjusted_price_return"


def _first_price_row(db: Session, symbol: str, start_day: date, *, strict: bool = False) -> PriceCache | None:
    comparison = PriceCache.date > start_day.isoformat() if strict else PriceCache.date >= start_day.isoformat()
    return db.execute(
        select(PriceCache)
        .where(PriceCache.symbol == symbol.strip().upper(), comparison)
        .order_by(PriceCache.date.asc())
        .limit(1)
    ).scalar_one_or_none()


def canonical_entry_session_row(db: Session, snapshot: ConfirmationScoreSnapshot) -> PriceCache | None:
    local = qualifying_event_at(snapshot).astimezone(MARKET_TZ)
    strict = local.timetz().replace(tzinfo=None) >= time(9, 30)
    return _first_price_row(db, snapshot.ticker_at_time, local.date(), strict=strict)


def _consistent_ohlc(row: PriceCache) -> tuple[Decimal, Decimal, Decimal] | None:
    opened = adjusted_price(row, "open")
    high = adjusted_price(row, "high")
    low = adjusted_price(row, "low")
    if opened is None or high is None or low is None or min(opened, high, low) <= 0:
        return None
    return opened, high, low


def _entry_key(snapshot: ConfirmationScoreSnapshot, session_day: date) -> str:
    raw = f"{snapshot.id}:{snapshot.security_id}:{session_day.isoformat()}:{OUTCOME_RETURN_METHODOLOGY}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def evidence_provenance_from_bundle(bundle: dict[str, Any], calculated_at: datetime) -> list[dict[str, Any]]:
    """Freeze contributor payloads at capture time.

    Contributor builders are responsible for applying public-availability filters.
    This captures their exact payload hash and conservative availability time; any
    explicitly supplied availability after calculation is rejected.
    """
    sources = bundle.get("sources") if isinstance(bundle.get("sources"), dict) else {}
    active = bundle.get("active_sources") if isinstance(bundle.get("active_sources"), list) else []
    captured = as_utc(calculated_at)
    provenance: list[dict[str, Any]] = []
    explicit = bundle.get("evidence_provenance") if isinstance(bundle.get("evidence_provenance"), list) else []
    explicit_by_source: dict[str, list[dict[str, Any]]] = {}
    for item in explicit:
        if isinstance(item, dict) and item.get("source_key"):
            explicit_by_source.setdefault(str(item["source_key"]), []).append(item)
    for source_key in active:
        items = explicit_by_source.get(str(source_key), [])
        if not items:
            payload = sources.get(source_key) if isinstance(sources.get(source_key), dict) else {}
            normalized = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
            items = [{
                "source_key": source_key,
                "evidence_id": f"capture:{hashlib.sha256(normalized.encode('utf-8')).hexdigest()}",
                "available_at": captured.isoformat(),
                "source_timestamp": None,
                "source_payload_hash": hashlib.sha256(normalized.encode("utf-8")).hexdigest(),
            }]
        for item in items:
            raw_available = item.get("available_at")
            try:
                available = as_utc(datetime.fromisoformat(str(raw_available).replace("Z", "+00:00")))
            except (TypeError, ValueError):
                continue
            if available > captured:
                continue
            provenance.append({**item, "source_key": str(source_key), "available_at": available})
    return provenance


def entry_price_invariant(row: PriceCache, entry_price: Decimal, entry_at: datetime, qualifying_at: datetime) -> bool:
    """Permanent executable-entry invariant used by capture, rebuilds, and tests."""
    ohlc = _consistent_ohlc(row)
    if ohlc is None or entry_price <= 0:
        return False
    official_open, high, low = ohlc
    tolerance = max(Decimal("0.000001"), abs(official_open) * Decimal("0.000001"))
    return (
        as_utc(entry_at) >= as_utc(qualifying_at)
        and low <= entry_price <= high
        and abs(entry_price - official_open) <= tolerance
    )


def persist_evidence_provenance(
    db: Session,
    snapshot: ConfirmationScoreSnapshot,
    bundle: dict[str, Any],
) -> int:
    qualifying_at = as_utc(snapshot.calculated_at)
    items = evidence_provenance_from_bundle(bundle, qualifying_at)
    active = bundle.get("active_sources") if isinstance(bundle.get("active_sources"), list) else []
    if {str(item.get("source_key")) for item in items} != {str(source) for source in active}:
        return 0
    created = 0
    for item in items:
        evidence_id = str(item.get("evidence_id") or "").strip()
        if not evidence_id:
            continue
        existing = db.execute(
            select(OutcomeEvidenceProvenance.id).where(
                OutcomeEvidenceProvenance.snapshot_id == snapshot.id,
                OutcomeEvidenceProvenance.source_key == item["source_key"],
                OutcomeEvidenceProvenance.evidence_id == evidence_id,
            )
        ).scalar_one_or_none()
        if existing is not None:
            continue
        source_timestamp = item.get("source_timestamp")
        if isinstance(source_timestamp, str):
            try:
                source_timestamp = as_utc(datetime.fromisoformat(source_timestamp.replace("Z", "+00:00")))
            except ValueError:
                source_timestamp = None
        db.add(
            OutcomeEvidenceProvenance(
                snapshot_id=int(snapshot.id),
                source_key=item["source_key"],
                evidence_id=evidence_id,
                available_at=item["available_at"],
                qualifying_event_at=qualifying_at,
                source_timestamp=source_timestamp if isinstance(source_timestamp, datetime) else None,
                source_payload_hash=str(item.get("source_payload_hash") or "") or None,
            )
        )
        created += 1
    db.flush()
    return created


def materialize_outcome_entry(db: Session, snapshot: ConfirmationScoreSnapshot) -> OutcomeEntry | None:
    existing = db.execute(select(OutcomeEntry).where(OutcomeEntry.snapshot_id == snapshot.id)).scalar_one_or_none()
    if existing is not None:
        return existing
    active_sources = json.loads(snapshot.active_sources_json or "[]")
    provenance_sources = set(
        db.execute(
            select(OutcomeEvidenceProvenance.source_key).where(OutcomeEvidenceProvenance.snapshot_id == snapshot.id)
        ).scalars().all()
    )
    if not active_sources or provenance_sources != {str(source) for source in active_sources}:
        return None
    entry_row = canonical_entry_session_row(db, snapshot)
    if entry_row is None or not _canonical_price_basis(entry_row):
        return None
    session_day = _price_day(entry_row)
    entry_at = market_open_at(session_day)
    qualifying_at = qualifying_event_at(snapshot)
    if entry_at < qualifying_at:
        return None
    ohlc = _consistent_ohlc(entry_row)
    benchmark_row = db.get(PriceCache, {"symbol": "SPY", "date": session_day.isoformat()})
    if not _canonical_price_basis(benchmark_row):
        return None
    benchmark_ohlc = _consistent_ohlc(benchmark_row) if benchmark_row is not None else None
    if ohlc is None or benchmark_ohlc is None:
        return None
    entry_price, high, low = ohlc
    benchmark_entry, benchmark_high, benchmark_low = benchmark_ohlc
    if not entry_price_invariant(entry_row, entry_price, entry_at, qualifying_at):
        return None
    if not (benchmark_low <= benchmark_entry <= benchmark_high):
        return None
    entry = OutcomeEntry(
        snapshot_id=int(snapshot.id),
        security_id=int(snapshot.security_id),
        ticker_at_time=snapshot.ticker_at_time,
        entry_key=_entry_key(snapshot, session_day),
        qualifying_event_at=qualifying_at,
        evidence_cutoff_at=qualifying_at,
        entry_session_date=session_day,
        entry_price=float(entry_price),
        entry_price_at=entry_at,
        entry_price_type="official_open",
        entry_price_source=entry_row.price_source or "price_cache",
        adjustment_type=entry_row.adjustment_status or "adjustment_unknown",
        benchmark_symbol="SPY",
        benchmark_entry_price=float(benchmark_entry),
        benchmark_entry_price_at=entry_at,
        benchmark_price_source=benchmark_row.price_source or "price_cache",
        methodology_version=OUTCOME_RETURN_METHODOLOGY,
        audit_version=OUTCOME_AUDIT_VERSION,
    )
    db.add(entry)
    db.flush()
    return entry


def _return_pct(start: Decimal, end: Decimal) -> Decimal:
    return ((end / start) - Decimal("1")) * Decimal("100")


def materialize_outcome_horizons(
    db: Session,
    entry: OutcomeEntry,
    *,
    as_of: date | None = None,
) -> list[OutcomeHorizonObservation]:
    today = as_of or datetime.now(UTC).date()
    observations: list[OutcomeHorizonObservation] = []
    entry_price = _decimal(entry.entry_price)
    benchmark_entry = _decimal(entry.benchmark_entry_price)
    assert entry_price is not None and benchmark_entry is not None
    for horizon_days in OUTCOME_HORIZONS:
        existing = db.execute(
            select(OutcomeHorizonObservation).where(
                OutcomeHorizonObservation.entry_id == entry.id,
                OutcomeHorizonObservation.horizon_days == horizon_days,
            )
        ).scalar_one_or_none()
        if existing is not None:
            observations.append(existing)
            continue
        target = entry.entry_session_date + timedelta(days=horizon_days)
        if target > today:
            continue
        # SPY is the US-session calendar. Both legs must use the same session;
        # a missing security observation remains missing (for example delisting).
        benchmark_row = _first_price_row(db, entry.benchmark_symbol, target)
        benchmark_day = _price_day(benchmark_row) if benchmark_row is not None else None
        security_row = (
            db.get(PriceCache, {"symbol": entry.ticker_at_time, "date": benchmark_day.isoformat()})
            if benchmark_day is not None
            else None
        )
        security_price = adjusted_price(security_row, "close")
        benchmark_price = adjusted_price(benchmark_row, "close")
        if (
            security_row is None
            or benchmark_row is None
            or not _canonical_price_basis(security_row)
            or not _canonical_price_basis(benchmark_row)
            or security_price is None
            or benchmark_price is None
        ):
            continue
        security_return = _return_pct(entry_price, security_price)
        benchmark_return = _return_pct(benchmark_entry, benchmark_price)
        observation = OutcomeHorizonObservation(
            entry_id=int(entry.id),
            snapshot_id=int(entry.snapshot_id),
            horizon_days=horizon_days,
            target_date=target,
            security_price=float(security_price),
            security_price_at=market_close_at(_price_day(security_row)),
            security_session_date=_price_day(security_row),
            security_price_source=security_row.price_source or "price_cache",
            benchmark_price=float(benchmark_price),
            benchmark_price_at=market_close_at(_price_day(benchmark_row)),
            benchmark_session_date=_price_day(benchmark_row),
            benchmark_price_source=benchmark_row.price_source or "price_cache",
            price_type="official_close",
            adjustment_type=security_row.adjustment_status or entry.adjustment_type,
            security_return_pct=float(security_return),
            benchmark_return_pct=float(benchmark_return),
            excess_return_pct=float(security_return - benchmark_return),
            audit_version=OUTCOME_AUDIT_VERSION,
        )
        db.add(observation)
        db.flush()
        observations.append(observation)
    return observations


def canonical_outcome_payload(
    entry: OutcomeEntry,
    observations: list[OutcomeHorizonObservation],
    *,
    as_of: date | None = None,
) -> dict[str, Any]:
    today = as_of or datetime.now(UTC).date()
    by_days = {row.horizon_days: row for row in observations}
    payload: dict[str, Any] = {}
    for days in OUTCOME_HORIZONS:
        target = entry.entry_session_date + timedelta(days=days)
        row = by_days.get(days)
        if row is None:
            payload[f"{days}D"] = {
                "status": "pending" if target > today else "missing_price",
                "horizon_days": days,
                "target_date": target.isoformat(),
            }
            continue
        payload[f"{days}D"] = {
            "status": "matured",
            "horizon_days": days,
            "target_date": row.target_date.isoformat(),
            "price": row.security_price,
            "price_date": row.security_session_date.isoformat(),
            "price_at": row.security_price_at.isoformat(),
            "price_type": row.price_type,
            "return_pct": row.security_return_pct,
            "spy_return_pct": row.benchmark_return_pct,
            "excess_return_pct": row.excess_return_pct,
            "audit_version": row.audit_version,
        }
    return payload


def canonical_price_path(db: Session, snapshot_id: int, *, horizon_days: int = 30) -> dict[str, Any] | None:
    entry = db.execute(select(OutcomeEntry).where(OutcomeEntry.snapshot_id == snapshot_id)).scalar_one_or_none()
    if entry is None:
        return None
    bounded_horizon = horizon_days if horizon_days in OUTCOME_HORIZONS else 30
    observation = db.execute(
        select(OutcomeHorizonObservation).where(
            OutcomeHorizonObservation.entry_id == entry.id,
            OutcomeHorizonObservation.horizon_days == bounded_horizon,
        )
    ).scalar_one_or_none()
    end_day = observation.security_session_date if observation is not None else entry.entry_session_date + timedelta(days=bounded_horizon)
    security_rows = db.execute(
        select(PriceCache)
        .where(
            PriceCache.symbol == entry.ticker_at_time,
            PriceCache.date >= entry.entry_session_date.isoformat(),
            PriceCache.date <= end_day.isoformat(),
        )
        .order_by(PriceCache.date.asc())
    ).scalars().all()
    benchmark_rows = db.execute(
        select(PriceCache)
        .where(
            PriceCache.symbol == entry.benchmark_symbol,
            PriceCache.date >= entry.entry_session_date.isoformat(),
            PriceCache.date <= end_day.isoformat(),
        )
        .order_by(PriceCache.date.asc())
    ).scalars().all()
    security_by_day = {_price_day(row): row for row in security_rows}
    entry_price = _decimal(entry.entry_price)
    benchmark_entry = _decimal(entry.benchmark_entry_price)
    if entry_price is None or benchmark_entry is None:
        return None
    points: list[dict[str, Any]] = [
        {
            "date": entry.entry_price_at.isoformat(),
            "session_date": entry.entry_session_date.isoformat(),
            "price_type": "official_open",
            "security_return_pct": 0.0,
            "benchmark_return_pct": 0.0,
            "excess_return_pct": 0.0,
        }
    ]
    for benchmark_row in benchmark_rows:
        day = _price_day(benchmark_row)
        security_row = security_by_day.get(day)
        security_close = adjusted_price(security_row, "close")
        benchmark_close = adjusted_price(benchmark_row, "close")
        if security_close is None or benchmark_close is None:
            continue
        security_return = _return_pct(entry_price, security_close)
        benchmark_return = _return_pct(benchmark_entry, benchmark_close)
        points.append(
            {
                "date": market_close_at(day).isoformat(),
                "session_date": day.isoformat(),
                "price_type": "official_close",
                "security_return_pct": float(security_return),
                "benchmark_return_pct": float(benchmark_return),
                "excess_return_pct": float(security_return - benchmark_return),
            }
        )
    return {
        "snapshot_id": snapshot_id,
        "symbol": entry.ticker_at_time,
        "benchmark_symbol": entry.benchmark_symbol,
        "horizon_days": bounded_horizon,
        "methodology": entry.methodology_version,
        "points": points,
    }


def invalidate_outcome_persistent_caches(db: Session) -> int:
    result = db.execute(delete(TickerContextBundleCache).where(TickerContextBundleCache.symbol == "__OUTCOME_LEDGER__"))
    return int(result.rowcount or 0)
