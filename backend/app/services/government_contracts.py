from __future__ import annotations

import json
import logging
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from math import isfinite
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models import Event, GovernmentContract
from app.services.ticker_events import GOVERNMENT_CONTRACT_EVENT_TYPES
from app.utils.symbols import normalize_symbol

logger = logging.getLogger(__name__)

DEFAULT_GOVERNMENT_CONTRACTS_MIN_AMOUNT = 1_000_000
DEFAULT_GOVERNMENT_CONTRACTS_LOOKBACK_DAYS = 365


def get_government_contracts_overlay_availability(
    db: Session,
    *,
    feature_enabled: bool = True,
) -> dict[str, Any]:
    _ensure_government_contracts_table(db)
    if not feature_enabled:
        return {
            "enabled": False,
            "status": "disabled",
            "filterable": False,
            "source": "local_index",
            "reason": "feature_disabled",
        }

    total_rows = _government_contract_row_count(db)
    if total_rows <= 0:
        sync_government_contracts_from_events(db)
        total_rows = _government_contract_row_count(db)

    if total_rows <= 0:
        return {
            "enabled": True,
            "status": "unavailable",
            "filterable": False,
            "source": "local_index",
            "reason": "empty_dataset",
            "indexed_row_count": 0,
        }

    return {
        "enabled": True,
        "status": "ok",
        "filterable": True,
        "source": "local_index",
        "reason": None,
        "indexed_row_count": int(total_rows),
    }


def get_government_contracts_summary(
    db: Session,
    symbol: str,
    lookback_days: int = DEFAULT_GOVERNMENT_CONTRACTS_LOOKBACK_DAYS,
    min_amount: float | int | None = DEFAULT_GOVERNMENT_CONTRACTS_MIN_AMOUNT,
) -> dict[str, Any]:
    summaries = get_government_contracts_summaries_for_symbols(
        db,
        [symbol],
        lookback_days=lookback_days,
        min_amount=min_amount,
    )
    normalized = normalize_symbol(symbol)
    return summaries.get(normalized or "", unavailable_government_contracts_summary())


def get_government_contracts_summaries_for_symbols(
    db: Session,
    symbols: list[str],
    *,
    lookback_days: int = DEFAULT_GOVERNMENT_CONTRACTS_LOOKBACK_DAYS,
    min_amount: float | int | None = DEFAULT_GOVERNMENT_CONTRACTS_MIN_AMOUNT,
) -> dict[str, dict[str, Any]]:
    normalized_symbols = sorted({normalize_symbol(symbol) for symbol in symbols if normalize_symbol(symbol)})
    if not normalized_symbols:
        return {}

    sync_government_contracts_from_events(db, symbols=normalized_symbols)
    availability = get_government_contracts_overlay_availability(db)
    if availability.get("status") != "ok":
        return {symbol: unavailable_government_contracts_summary() for symbol in normalized_symbols}

    cutoff_date = _cutoff_date(lookback_days)
    minimum_amount = _non_negative_float(min_amount) or 0.0

    summary_rows = db.execute(
        select(
            func.upper(GovernmentContract.symbol).label("symbol"),
            func.count(GovernmentContract.id).label("contract_count"),
            func.sum(GovernmentContract.award_amount).label("total_award_amount"),
            func.max(GovernmentContract.award_amount).label("largest_award_amount"),
            func.max(GovernmentContract.award_date).label("latest_award_date"),
        )
        .where(func.upper(GovernmentContract.symbol).in_(normalized_symbols))
        .where(GovernmentContract.award_date >= cutoff_date)
        .where(GovernmentContract.award_amount >= minimum_amount)
        .group_by(func.upper(GovernmentContract.symbol))
    ).mappings().all()

    agency_rows = db.execute(
        select(
            func.upper(GovernmentContract.symbol).label("symbol"),
            GovernmentContract.awarding_agency.label("awarding_agency"),
            func.sum(GovernmentContract.award_amount).label("agency_total"),
        )
        .where(func.upper(GovernmentContract.symbol).in_(normalized_symbols))
        .where(GovernmentContract.award_date >= cutoff_date)
        .where(GovernmentContract.award_amount >= minimum_amount)
        .where(GovernmentContract.awarding_agency.is_not(None))
        .group_by(func.upper(GovernmentContract.symbol), GovernmentContract.awarding_agency)
    ).mappings().all()

    top_agency_by_symbol: dict[str, str] = {}
    top_agency_amounts: dict[str, float] = {}
    for row in agency_rows:
        symbol = normalize_symbol(row.get("symbol"))
        agency = row.get("awarding_agency")
        amount = _non_negative_float(row.get("agency_total"))
        if not symbol or not isinstance(agency, str) or amount is None:
            continue
        if symbol not in top_agency_amounts or amount > top_agency_amounts[symbol]:
            top_agency_amounts[symbol] = amount
            top_agency_by_symbol[symbol] = agency.strip()

    results = {symbol: inactive_government_contracts_summary() for symbol in normalized_symbols}
    for row in summary_rows:
        symbol = normalize_symbol(row.get("symbol"))
        if not symbol:
            continue
        contract_count = int(row.get("contract_count") or 0)
        total_award_amount = round(float(row.get("total_award_amount") or 0.0), 2)
        largest_award_amount = float(row.get("largest_award_amount") or 0.0)
        latest_award_date = row.get("latest_award_date")
        score_contribution = max(
            6,
            min(
                24,
                int(
                    round(
                        6
                        + min(total_award_amount / 25_000_000, 10.0)
                        + min(contract_count, 6) * 1.5
                        + min(largest_award_amount / 50_000_000, 4.0)
                    )
                ),
            ),
        )
        results[symbol] = {
            "status": "ok",
            "active": contract_count > 0,
            "contract_count": contract_count,
            "total_award_amount": total_award_amount,
            "largest_award_amount": round(largest_award_amount, 2) if largest_award_amount > 0 else None,
            "latest_award_date": latest_award_date.isoformat() if isinstance(latest_award_date, date) else None,
            "top_agency": top_agency_by_symbol.get(symbol),
            "direction": "bullish" if contract_count > 0 else "neutral",
            "score_contribution": score_contribution if contract_count > 0 else 0,
        }
    return results


def get_government_contracts_for_symbol(
    db: Session,
    symbol: str,
    *,
    lookback_days: int = DEFAULT_GOVERNMENT_CONTRACTS_LOOKBACK_DAYS,
    min_amount: float | int | None = DEFAULT_GOVERNMENT_CONTRACTS_MIN_AMOUNT,
    limit: int = 10,
) -> dict[str, Any]:
    normalized_symbol = normalize_symbol(symbol)
    bounded_limit = max(1, min(int(limit or 10), 100))
    cutoff_date = _cutoff_date(lookback_days)
    minimum_amount = _non_negative_float(min_amount) or 0.0

    if not normalized_symbol:
        return {
            "symbol": None,
            "status": "unavailable",
            "lookback_days": max(1, min(int(lookback_days or DEFAULT_GOVERNMENT_CONTRACTS_LOOKBACK_DAYS), 365 * 3)),
            "cutoff_date": cutoff_date.isoformat(),
            "min_amount": minimum_amount,
            "contract_count": 0,
            "total_award_amount": 0.0,
            "largest_award_amount": None,
            "latest_award_date": None,
            "top_agency": None,
            "items": [],
        }

    sync_government_contracts_from_events(db, symbols=[normalized_symbol])
    availability = get_government_contracts_overlay_availability(db)
    summary = get_government_contracts_summaries_for_symbols(
        db,
        [normalized_symbol],
        lookback_days=lookback_days,
        min_amount=min_amount,
    ).get(normalized_symbol, unavailable_government_contracts_summary())

    if availability.get("status") != "ok":
        return {
            "symbol": normalized_symbol,
            "status": "unavailable",
            "lookback_days": max(1, min(int(lookback_days or DEFAULT_GOVERNMENT_CONTRACTS_LOOKBACK_DAYS), 365 * 3)),
            "cutoff_date": cutoff_date.isoformat(),
            "min_amount": minimum_amount,
            "contract_count": 0,
            "total_award_amount": 0.0,
            "largest_award_amount": None,
            "latest_award_date": None,
            "top_agency": None,
            "items": [],
        }

    rows = db.execute(
        select(GovernmentContract)
        .where(func.upper(GovernmentContract.symbol) == normalized_symbol)
        .where(GovernmentContract.award_date >= cutoff_date)
        .where(GovernmentContract.award_amount >= minimum_amount)
        .order_by(GovernmentContract.award_date.desc(), GovernmentContract.award_amount.desc(), GovernmentContract.id.desc())
        .limit(bounded_limit)
    ).scalars().all()

    items = [
        {
            "award_date": row.award_date.isoformat() if row.award_date else None,
            "award_amount": round(float(row.award_amount), 2),
            "awarding_agency": row.awarding_agency,
            "description": row.description,
            "source": row.source,
        }
        for row in rows
    ]
    return {
        "symbol": normalized_symbol,
        "status": summary.get("status") or "ok",
        "lookback_days": max(1, min(int(lookback_days or DEFAULT_GOVERNMENT_CONTRACTS_LOOKBACK_DAYS), 365 * 3)),
        "cutoff_date": cutoff_date.isoformat(),
        "min_amount": minimum_amount,
        "contract_count": int(summary.get("contract_count") or 0),
        "total_award_amount": float(summary.get("total_award_amount") or 0.0),
        "largest_award_amount": summary.get("largest_award_amount"),
        "latest_award_date": summary.get("latest_award_date"),
        "top_agency": summary.get("top_agency"),
        "items": items,
    }


def sync_government_contracts_from_events(
    db: Session,
    *,
    symbols: list[str] | None = None,
) -> int:
    _ensure_government_contracts_table(db)
    normalized_symbols = sorted({normalize_symbol(symbol) for symbol in (symbols or []) if normalize_symbol(symbol)})
    stmt = (
        select(Event)
        .outerjoin(GovernmentContract, GovernmentContract.event_id == Event.id)
        .where(GovernmentContract.id.is_(None))
        .where(Event.event_type.in_(GOVERNMENT_CONTRACT_EVENT_TYPES))
    )
    if normalized_symbols:
        stmt = stmt.where(Event.symbol.is_not(None)).where(func.upper(Event.symbol).in_(normalized_symbols))

    pending = db.execute(stmt.order_by(Event.id.asc())).scalars().all()
    inserted = 0
    for event in pending:
        contract_row = _government_contract_from_event(event)
        if contract_row is None:
            continue
        db.add(contract_row)
        inserted += 1

    if inserted:
        db.flush()
        logger.info("government_contracts_sync inserted=%s symbols=%s", inserted, normalized_symbols[:10])
    return inserted


def inactive_government_contracts_summary() -> dict[str, Any]:
    return {
        "status": "ok",
        "active": False,
        "contract_count": 0,
        "total_award_amount": 0.0,
        "largest_award_amount": None,
        "latest_award_date": None,
        "top_agency": None,
        "direction": "neutral",
        "score_contribution": 0,
    }


def unavailable_government_contracts_summary() -> dict[str, Any]:
    return {
        "status": "unavailable",
        "active": None,
        "contract_count": None,
        "total_award_amount": None,
        "largest_award_amount": None,
        "latest_award_date": None,
        "top_agency": None,
        "direction": None,
        "score_contribution": 0,
    }


def _government_contract_row_count(db: Session) -> int:
    _ensure_government_contracts_table(db)
    return int(db.execute(select(func.count()).select_from(GovernmentContract)).scalar() or 0)


def _ensure_government_contracts_table(db: Session) -> None:
    GovernmentContract.__table__.create(bind=db.get_bind(), checkfirst=True)


def _cutoff_date(lookback_days: int) -> date:
    bounded_lookback = max(1, min(int(lookback_days or DEFAULT_GOVERNMENT_CONTRACTS_LOOKBACK_DAYS), 365 * 3))
    return (datetime.now(timezone.utc) - timedelta(days=bounded_lookback)).date()


def _government_contract_from_event(event: Event) -> GovernmentContract | None:
    payload = _load_payload(event.payload_json)
    symbol = normalize_symbol(event.symbol or payload.get("symbol"))
    award_date = _contract_date(event, payload)
    award_amount = _contract_amount(event, payload)
    if not symbol or award_date is None or award_amount is None:
        return None
    return GovernmentContract(
        event_id=event.id,
        symbol=symbol,
        award_date=award_date,
        award_amount=round(float(award_amount), 2),
        awarding_agency=_first_text(payload, "awarding_agency", "awardingAgency", "agency", "department", "top_agency"),
        description=_first_text(payload, "description", "summary", "title"),
        source=event.source,
        payload_json=event.payload_json,
    )


def _load_payload(raw: str | None) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _contract_date(event: Event, payload: dict[str, Any]) -> date | None:
    for key in ("award_date", "awardDate", "period_start", "periodStart", "date", "event_date", "report_date", "reportDate"):
        parsed = _parse_date(_payload_value(payload, key))
        if parsed is not None:
            return parsed
    if event.event_date is not None:
        return event.event_date.date()
    if event.ts is not None:
        return event.ts.date()
    return None


def _contract_amount(event: Event, payload: dict[str, Any]) -> float | None:
    for key in ("award_amount", "awardAmount", "amount", "obligated_amount", "obligatedAmount"):
        parsed = _non_negative_float(_payload_value(payload, key))
        if parsed is not None:
            return parsed
    if _non_negative_float(event.amount_max) is not None:
        return float(event.amount_max)
    if _non_negative_float(event.amount_min) is not None:
        return float(event.amount_min)
    return None


def _payload_value(payload: dict[str, Any], key: str) -> Any:
    raw = payload.get("raw") if isinstance(payload.get("raw"), dict) else {}
    nested = payload.get("payload") if isinstance(payload.get("payload"), dict) else {}
    for container in (payload, nested, raw):
        if key in container:
            return container.get(key)
    return None


def _first_text(payload: dict[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = _payload_value(payload, key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _parse_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if not isinstance(value, str) or not value.strip():
        return None
    raw = value.strip()
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).date()
    except ValueError:
        return None


def _non_negative_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        parsed = float(value)
    elif isinstance(value, str):
        cleaned = value.replace("$", "").replace(",", "").strip()
        if not cleaned:
            return None
        try:
            parsed = float(cleaned)
        except ValueError:
            return None
    else:
        return None
    if not isfinite(parsed) or parsed < 0:
        return None
    return parsed
