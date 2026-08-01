from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timezone
from typing import Any, Iterable

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models import FundamentalsCache, FundamentalsSnapshot
from app.services.fundamentals_cache import CACHE_ROW_FIELDS
from app.utils.symbols import classify_symbol, normalize_symbol

METHODOLOGY_VERSION = "fundamentals_snapshot_v1"
SNAPSHOT_COPY_FIELDS = tuple(
    field
    for field in CACHE_ROW_FIELDS
    if field
    not in {
        "symbol",
        "provider",
        "fetched_at",
        "period_date",
        "status",
        "error",
    }
)
SNAPSHOT_EXCLUDED_SYMBOL_SUBSTRINGS = ("RESEARCH", "WALNUT", "CHART=", "%")


def snapshot_symbol_rejection_reason(raw_symbol: str | None) -> tuple[str | None, str | None]:
    status, normalized, reason = classify_symbol(raw_symbol)
    symbol_text = normalized or normalize_symbol(raw_symbol) or str(raw_symbol or "").strip().upper() or None
    if symbol_text and any(marker in symbol_text for marker in SNAPSHOT_EXCLUDED_SYMBOL_SUBSTRINGS):
        return symbol_text, "symbol_contains_non_ticker_artifact"
    if status != "eligible" or not normalized:
        return normalized, status
    if any(marker in normalized for marker in SNAPSHOT_EXCLUDED_SYMBOL_SUBSTRINGS):
        return normalized, "symbol_contains_non_ticker_artifact"
    if normalized.endswith(")") or "(" in normalized or ")" in normalized:
        return normalized, "symbol_contains_parentheses"
    return normalized, None


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _snapshot_date(observed_at: datetime) -> date:
    if observed_at.tzinfo is None:
        return observed_at.date()
    return observed_at.astimezone(timezone.utc).date()


def _normalized_symbols(symbols: Iterable[str] | None) -> list[str] | None:
    if symbols is None:
        return None
    normalized = sorted(
        {
            symbol
            for raw in symbols
            if (result := snapshot_symbol_rejection_reason(raw))
            and (symbol := result[0])
            and result[1] is None
        }
    )
    return normalized


def _row_payload(row: FundamentalsCache) -> dict[str, Any]:
    payload = {
        "symbol": row.symbol,
        "provider": row.provider,
        "source_fetched_at": row.fetched_at.isoformat() if row.fetched_at else None,
        "period_date": row.period_date.isoformat() if row.period_date else None,
        "status": row.status,
        "error": row.error,
    }
    for field in SNAPSHOT_COPY_FIELDS:
        payload[field] = getattr(row, field)
    return payload


def _payload_hash(row: FundamentalsCache) -> str:
    payload = json.dumps(_row_payload(row), sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _apply_cache_row_to_snapshot(
    snapshot: FundamentalsSnapshot,
    row: FundamentalsCache,
    *,
    observed_at: datetime,
) -> None:
    snapshot.symbol = normalize_symbol(row.symbol) or row.symbol
    snapshot.provider = row.provider or "fmp"
    snapshot.snapshot_date = _snapshot_date(observed_at)
    snapshot.observed_at = observed_at
    snapshot.source_fetched_at = row.fetched_at
    snapshot.period_date = row.period_date
    snapshot.status = row.status or "ok"
    snapshot.error = row.error
    snapshot.source_payload_hash = _payload_hash(row)
    snapshot.methodology_version = METHODOLOGY_VERSION
    for field in SNAPSHOT_COPY_FIELDS:
        setattr(snapshot, field, getattr(row, field))


def upsert_fundamentals_snapshot(
    db: Session,
    row: FundamentalsCache,
    *,
    observed_at: datetime | None = None,
) -> FundamentalsSnapshot:
    observed = observed_at or _utc_now()
    symbol, rejection_reason = snapshot_symbol_rejection_reason(row.symbol)
    if rejection_reason or not symbol:
        raise ValueError(f"Cannot snapshot invalid fundamentals symbol {row.symbol!r}: {rejection_reason}")
    provider = row.provider or "fmp"
    snapshot_day = _snapshot_date(observed)
    snapshot = db.execute(
        select(FundamentalsSnapshot)
        .where(func.upper(FundamentalsSnapshot.symbol) == symbol)
        .where(FundamentalsSnapshot.provider == provider)
        .where(FundamentalsSnapshot.snapshot_date == snapshot_day)
    ).scalar_one_or_none()
    if snapshot is None:
        snapshot = FundamentalsSnapshot(
            symbol=symbol,
            provider=provider,
            snapshot_date=snapshot_day,
            observed_at=observed,
            source_fetched_at=row.fetched_at,
        )
        db.add(snapshot)
        db.flush()
    _apply_cache_row_to_snapshot(snapshot, row, observed_at=observed)
    return snapshot


def snapshot_current_fundamentals(
    db: Session,
    *,
    symbols: Iterable[str] | None = None,
    provider: str = "fmp",
    observed_at: datetime | None = None,
) -> dict[str, Any]:
    normalized_symbols = _normalized_symbols(symbols)
    statement = (
        select(FundamentalsCache)
        .where(FundamentalsCache.provider == provider)
        .where(FundamentalsCache.status == "ok")
        .order_by(FundamentalsCache.symbol.asc())
    )
    if normalized_symbols is not None:
        if not normalized_symbols:
            return {"status": "ok", "rows_seen": 0, "snapshots_written": 0, "symbols": []}
        statement = statement.where(func.upper(FundamentalsCache.symbol).in_(normalized_symbols))

    rows = db.execute(statement).scalars().all()
    observed = observed_at or _utc_now()
    written_symbols: list[str] = []
    skipped_invalid_symbols: dict[str, int] = {}
    for row in rows:
        _, rejection_reason = snapshot_symbol_rejection_reason(row.symbol)
        if rejection_reason:
            skipped_invalid_symbols[rejection_reason] = skipped_invalid_symbols.get(rejection_reason, 0) + 1
            continue
        snapshot = upsert_fundamentals_snapshot(db, row, observed_at=observed)
        written_symbols.append(snapshot.symbol)
    return {
        "status": "ok",
        "rows_seen": len(rows),
        "snapshots_written": len(written_symbols),
        "skipped_invalid_symbols": dict(sorted(skipped_invalid_symbols.items())),
        "snapshot_date": _snapshot_date(observed).isoformat(),
        "observed_at": observed.isoformat(),
        "provider": provider,
        "symbols": written_symbols,
        "methodology_version": METHODOLOGY_VERSION,
    }


def latest_fundamentals_snapshot_on_or_before(
    db: Session,
    symbol: str,
    *,
    as_of: date,
    provider: str = "fmp",
) -> FundamentalsSnapshot | None:
    normalized = normalize_symbol(symbol)
    if not normalized:
        return None
    return db.execute(
        select(FundamentalsSnapshot)
        .where(func.upper(FundamentalsSnapshot.symbol) == normalized)
        .where(FundamentalsSnapshot.provider == provider)
        .where(FundamentalsSnapshot.snapshot_date <= as_of)
        .where(FundamentalsSnapshot.status == "ok")
        .order_by(FundamentalsSnapshot.snapshot_date.desc(), FundamentalsSnapshot.observed_at.desc())
        .limit(1)
    ).scalar_one_or_none()
