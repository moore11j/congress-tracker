from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.clients.fmp import fetch_index_constituents
from app.models import IndexMembership
from app.utils.symbols import normalize_symbol

logger = logging.getLogger(__name__)

INDEX_MEMBERSHIP_STALE_AFTER_DAYS = 7


@dataclass(frozen=True)
class IndexUniverseDefinition:
    code: str
    label: str
    provider_code: str
    min_count: int
    max_count: int


@dataclass(frozen=True)
class IndexMembershipSnapshot:
    code: str
    symbols: list[str]
    membership_count: int
    source: str | None
    source_as_of: str | None
    refreshed_at: str | None
    status: str
    reason: str | None = None

    @property
    def supported(self) -> bool:
        return self.status in {"available", "stale"}


@dataclass(frozen=True)
class IndexMembershipRefreshResult:
    code: str
    status: str
    incoming_count: int
    active_count: int
    inserted_count: int = 0
    retained_count: int = 0
    end_dated_count: int = 0
    reason: str | None = None


INDEX_UNIVERSES: dict[str, IndexUniverseDefinition] = {
    "sp500": IndexUniverseDefinition(
        code="sp500",
        label="S&P 500",
        provider_code="sp500",
        min_count=450,
        max_count=550,
    ),
    "nasdaq100": IndexUniverseDefinition(
        code="nasdaq100",
        label="Nasdaq 100",
        provider_code="nasdaq100",
        min_count=90,
        max_count=125,
    ),
}


def active_index_membership_snapshot(db: Session, index_code: str, *, now: datetime | None = None) -> IndexMembershipSnapshot:
    definition = INDEX_UNIVERSES.get(_normalize_index_code(index_code))
    if definition is None:
        return IndexMembershipSnapshot(
            code=_normalize_index_code(index_code),
            symbols=[],
            membership_count=0,
            source=None,
            source_as_of=None,
            refreshed_at=None,
            status="unavailable",
            reason="unsupported_index",
        )

    rows = db.execute(
        select(IndexMembership)
        .where(IndexMembership.index_code == definition.code, IndexMembership.is_active.is_(True))
        .order_by(IndexMembership.symbol.asc())
    ).scalars().all()
    symbols = [row.symbol for row in rows]
    count = len(symbols)
    source = _single_metadata_value(row.source for row in rows)
    source_as_of = max((row.source_as_of for row in rows if row.source_as_of), default=None)
    refreshed_at = max((row.refreshed_at for row in rows if row.refreshed_at), default=None)

    reason = _validation_reason(definition, symbols, source=source, source_as_of=source_as_of, refreshed_at=refreshed_at)
    if reason:
        return IndexMembershipSnapshot(
            code=definition.code,
            symbols=symbols,
            membership_count=count,
            source=source,
            source_as_of=source_as_of.isoformat() if source_as_of else None,
            refreshed_at=_dt_iso(refreshed_at),
            status="unavailable",
            reason=reason,
        )

    reference_now = now or datetime.now(timezone.utc)
    status = "available"
    if refreshed_at and reference_now - _aware_utc(refreshed_at) > timedelta(days=INDEX_MEMBERSHIP_STALE_AFTER_DAYS):
        status = "stale"
    return IndexMembershipSnapshot(
        code=definition.code,
        symbols=symbols,
        membership_count=count,
        source=source,
        source_as_of=source_as_of.isoformat() if source_as_of else None,
        refreshed_at=_dt_iso(refreshed_at),
        status=status,
    )


def index_universe_capabilities(db: Session) -> dict[str, dict[str, Any]]:
    return {code: _snapshot_capability(active_index_membership_snapshot(db, code)) for code in INDEX_UNIVERSES}


def refresh_index_memberships_from_provider(db: Session, index_code: str) -> IndexMembershipRefreshResult:
    definition = _require_definition(index_code)
    rows = fetch_index_constituents(definition.provider_code)
    return refresh_index_memberships(
        db,
        index_code=definition.code,
        rows=rows,
        source="fmp:index-constituents",
        source_as_of=date.today(),
    )


def refresh_all_index_memberships_from_provider(db: Session) -> list[IndexMembershipRefreshResult]:
    return [refresh_index_memberships_from_provider(db, code) for code in INDEX_UNIVERSES]


def refresh_index_memberships(
    db: Session,
    *,
    index_code: str,
    rows: Iterable[Any],
    source: str,
    source_as_of: date,
    refreshed_at: datetime | None = None,
) -> IndexMembershipRefreshResult:
    definition = _require_definition(index_code)
    symbols = _symbols_from_rows(rows)
    active_rows = db.execute(
        select(IndexMembership)
        .where(IndexMembership.index_code == definition.code, IndexMembership.is_active.is_(True))
        .order_by(IndexMembership.symbol.asc())
    ).scalars().all()
    active_by_symbol = {row.symbol: row for row in active_rows}

    reason = _validation_reason(
        definition,
        symbols,
        source=source,
        source_as_of=source_as_of,
        refreshed_at=refreshed_at or datetime.now(timezone.utc),
    )
    if reason:
        logger.warning(
            "index_membership_refresh_rejected index_code=%s incoming_count=%s active_count=%s reason=%s",
            definition.code,
            len(symbols),
            len(active_by_symbol),
            reason,
        )
        return IndexMembershipRefreshResult(
            code=definition.code,
            status="rejected",
            incoming_count=len(symbols),
            active_count=len(active_by_symbol),
            reason=reason,
        )

    refreshed_at = refreshed_at or datetime.now(timezone.utc)
    incoming = set(symbols)
    inserted = 0
    retained = 0
    end_dated = 0

    for symbol, active in active_by_symbol.items():
        if symbol in incoming:
            active.source = source
            active.source_as_of = source_as_of
            active.refreshed_at = refreshed_at
            retained += 1
        else:
            active.effective_to = source_as_of
            active.is_active = False
            active.refreshed_at = refreshed_at
            end_dated += 1

    for symbol in symbols:
        if symbol in active_by_symbol:
            continue
        db.add(
            IndexMembership(
                index_code=definition.code,
                symbol=symbol,
                effective_from=source_as_of,
                effective_to=None,
                source=source,
                source_as_of=source_as_of,
                refreshed_at=refreshed_at,
                is_active=True,
            )
        )
        inserted += 1

    db.commit()
    logger.info(
        "index_membership_refresh_complete index_code=%s incoming_count=%s active_count=%s inserted=%s retained=%s end_dated=%s source=%s source_as_of=%s",
        definition.code,
        len(symbols),
        len(incoming),
        inserted,
        retained,
        end_dated,
        source,
        source_as_of.isoformat(),
    )
    return IndexMembershipRefreshResult(
        code=definition.code,
        status="ok",
        incoming_count=len(symbols),
        active_count=len(incoming),
        inserted_count=inserted,
        retained_count=retained,
        end_dated_count=end_dated,
    )


def _snapshot_capability(snapshot: IndexMembershipSnapshot) -> dict[str, Any]:
    return {
        "supported": snapshot.supported,
        "membershipCount": snapshot.membership_count,
        "source": snapshot.source,
        "sourceAsOf": snapshot.source_as_of,
        "refreshedAt": snapshot.refreshed_at,
        "status": snapshot.status,
        "reason": snapshot.reason,
    }


def _symbols_from_rows(rows: Iterable[Any]) -> list[str]:
    symbols: list[str] = []
    for row in rows:
        raw_symbol: Any = None
        if isinstance(row, str):
            raw_symbol = row
        elif isinstance(row, dict):
            raw_symbol = row.get("symbol") or row.get("ticker")
        else:
            raw_symbol = getattr(row, "symbol", None) or getattr(row, "ticker", None)
        symbol = normalize_symbol(raw_symbol)
        if symbol:
            symbols.append(symbol)
    return sorted(set(symbols))


def _validation_reason(
    definition: IndexUniverseDefinition,
    symbols: list[str],
    *,
    source: str | None,
    source_as_of: date | None,
    refreshed_at: datetime | None,
) -> str | None:
    if not symbols:
        return "empty_membership"
    if len(symbols) < definition.min_count:
        return "membership_count_too_low"
    if len(symbols) > definition.max_count:
        return "membership_count_too_high"
    if not source:
        return "missing_source"
    if source_as_of is None:
        return "missing_source_as_of"
    if refreshed_at is None:
        return "missing_refreshed_at"
    return None


def _single_metadata_value(values: Iterable[str | None]) -> str | None:
    normalized = sorted({value for value in values if value})
    if not normalized:
        return None
    return normalized[0] if len(normalized) == 1 else "mixed"


def _normalize_index_code(index_code: str | None) -> str:
    return (index_code or "").strip().lower().replace("-", "").replace("_", "")


def _require_definition(index_code: str) -> IndexUniverseDefinition:
    normalized = _normalize_index_code(index_code)
    if normalized == "nasdaq100":
        key = "nasdaq100"
    elif normalized == "sp500":
        key = "sp500"
    else:
        key = normalized
    try:
        return INDEX_UNIVERSES[key]
    except KeyError as exc:
        raise ValueError(f"unsupported index universe: {index_code}") from exc


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _dt_iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    return _aware_utc(value).isoformat().replace("+00:00", "Z")
