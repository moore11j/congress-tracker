from __future__ import annotations

from datetime import date, timedelta
from typing import Iterable

from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from app.models import InstitutionalActivityEvent
from app.strategy_research.congress_buys import Signal, _normalize_universe
from app.utils.symbols import normalize_symbol

METHODOLOGY_VERSION = "institutional_activity_signals_v1"

BULLISH_INSTITUTIONAL_EVENT_TYPES = (
    "institutional_accumulation",
    "new_institutional_position",
    "cluster_accumulation",
    "smart_money_confirmation",
    "contrarian_accumulation",
)


def load_institutional_activity_signals(
    db: Session,
    *,
    universe: Iterable[str],
    start_date: date,
    end_date: date,
    min_materiality: float = 80.0,
    min_value_usd: float = 0.0,
) -> list[Signal]:
    normalized_universe = set(_normalize_universe(universe))
    if not normalized_universe:
        return []
    query = (
        select(InstitutionalActivityEvent)
        .where(func.upper(InstitutionalActivityEvent.normalized_symbol).in_(normalized_universe))
        .where(InstitutionalActivityEvent.filing_date >= start_date)
        .where(InstitutionalActivityEvent.filing_date <= end_date)
        .where(InstitutionalActivityEvent.direction == "bullish")
        .where(InstitutionalActivityEvent.event_type.in_(BULLISH_INSTITUTIONAL_EVENT_TYPES))
        .where(InstitutionalActivityEvent.materiality_score >= float(min_materiality))
        .where(or_(InstitutionalActivityEvent.freshness_status.is_(None), InstitutionalActivityEvent.freshness_status != "superseded"))
        .order_by(InstitutionalActivityEvent.filing_date.asc(), InstitutionalActivityEvent.id.asc())
    )
    if min_value_usd > 0:
        query = query.where(
            func.abs(
                func.coalesce(
                    InstitutionalActivityEvent.value_delta_usd,
                    InstitutionalActivityEvent.reported_value_usd,
                    0,
                )
            )
            >= float(min_value_usd)
        )
    rows = db.execute(query).scalars().all()
    signals: list[Signal] = []
    seen: set[tuple[object, ...]] = set()
    for row in rows:
        symbol = normalize_symbol(row.normalized_symbol or row.symbol)
        if not symbol or row.filing_date is None:
            continue
        amount = row.value_delta_usd if row.value_delta_usd is not None else row.reported_value_usd
        amount_int = int(round(abs(float(amount)))) if amount is not None else None
        dedupe_key = (
            "institutional_activity",
            row.id,
            row.cik,
            symbol,
            row.event_type,
            row.report_year,
            row.report_quarter,
        )
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        signals.append(
            Signal(
                event_id=int(row.id),
                symbol=symbol,
                disclosure_date=row.filing_date,
                raw_entry_date=row.filing_date + timedelta(days=1),
                amount_min=amount_int,
                amount_max=amount_int,
                member_name=row.holder_name or "Institutional holders",
                member_bioguide_id=row.cik,
                chamber=row.event_type,
                party=row.direction,
                source_filing_id=f"13f:{row.cik or 'aggregate'}:{row.report_year}Q{row.report_quarter}:{row.id}",
                source_document_url=None,
                dedupe_key=dedupe_key,
            )
        )
    return signals
