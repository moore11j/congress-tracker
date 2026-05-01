from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from math import isfinite
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.ingest.government_contracts import government_contracts_table_exists
from app.models import GovernmentContract
from app.utils.symbols import normalize_symbol

DEFAULT_GOVERNMENT_CONTRACTS_MIN_AMOUNT = 1_000_000
DEFAULT_GOVERNMENT_CONTRACTS_LOOKBACK_DAYS = 365
GOVERNMENT_CONTRACTS_SOURCE = "government_contracts"
GOVERNMENT_CONTRACTS_LABEL = "Government Contracts"


def get_government_contracts_overlay_availability(
    db: Session,
    *,
    feature_enabled: bool = True,
) -> dict[str, Any]:
    if not feature_enabled:
        return {
            "enabled": False,
            "status": "disabled",
            "filterable": False,
            "source": "local_index",
            "reason": "feature_disabled",
        }

    if not government_contracts_table_exists(db):
        return {
            "enabled": True,
            "status": "unavailable",
            "filterable": False,
            "source": "local_index",
            "reason": "missing_table",
            "indexed_row_count": 0,
        }

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
    return get_government_contracts_signal(
        db,
        symbol,
        lookback_days=lookback_days,
        min_amount=min_amount,
    )


def get_government_contracts_signal(
    db: Session,
    symbol: str,
    lookback_days: int = DEFAULT_GOVERNMENT_CONTRACTS_LOOKBACK_DAYS,
    min_amount: float | int | None = DEFAULT_GOVERNMENT_CONTRACTS_MIN_AMOUNT,
) -> dict[str, Any]:
    signals = get_government_contracts_signals_for_symbols(
        db,
        [symbol],
        lookback_days=lookback_days,
        min_amount=min_amount,
    )
    normalized = normalize_symbol(symbol)
    return signals.get(normalized or "", unavailable_government_contracts_summary())


def get_government_contracts_summaries_for_symbols(
    db: Session,
    symbols: list[str],
    *,
    lookback_days: int = DEFAULT_GOVERNMENT_CONTRACTS_LOOKBACK_DAYS,
    min_amount: float | int | None = DEFAULT_GOVERNMENT_CONTRACTS_MIN_AMOUNT,
) -> dict[str, dict[str, Any]]:
    return get_government_contracts_signals_for_symbols(
        db,
        symbols,
        lookback_days=lookback_days,
        min_amount=min_amount,
    )


def get_government_contracts_signals_for_symbols(
    db: Session,
    symbols: list[str],
    *,
    lookback_days: int = DEFAULT_GOVERNMENT_CONTRACTS_LOOKBACK_DAYS,
    min_amount: float | int | None = DEFAULT_GOVERNMENT_CONTRACTS_MIN_AMOUNT,
) -> dict[str, dict[str, Any]]:
    normalized_symbols = sorted({normalize_symbol(symbol) for symbol in symbols if normalize_symbol(symbol)})
    if not normalized_symbols:
        return {}

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
        latest_award_date_iso = latest_award_date.isoformat() if isinstance(latest_award_date, date) else None
        top_agency = top_agency_by_symbol.get(symbol)
        score_contribution = _government_contracts_score_contribution(
            total_award_amount=total_award_amount,
            latest_award_date=latest_award_date_iso,
        )
        results[symbol] = {
            "status": "ok",
            "active": contract_count > 0,
            "source": GOVERNMENT_CONTRACTS_SOURCE,
            "label": GOVERNMENT_CONTRACTS_LABEL,
            "summary": _government_contracts_summary_line(
                contract_count=contract_count,
                total_award_amount=total_award_amount,
                active=contract_count > 0,
            ),
            "contract_count": contract_count,
            "total_award_amount": total_award_amount,
            "largest_award_amount": round(largest_award_amount, 2) if largest_award_amount > 0 else None,
            "latest_award_date": latest_award_date_iso,
            "top_agency": top_agency,
            "direction": "bullish" if contract_count > 0 else "neutral",
            "score_contribution": score_contribution if contract_count > 0 else 0,
            "detail": _government_contracts_detail_line(
                contract_count=contract_count,
                total_award_amount=total_award_amount,
                top_agency=top_agency,
                active=contract_count > 0,
            ),
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
            "award_id": row.award_id,
            "award_date": row.award_date.isoformat() if row.award_date else None,
            "award_amount": round(float(row.award_amount), 2),
            "recipient_name": row.recipient_name,
            "raw_recipient_name": row.raw_recipient_name,
            "awarding_agency": row.awarding_agency,
            "awarding_sub_agency": row.awarding_sub_agency,
            "funding_agency": row.funding_agency,
            "funding_sub_agency": row.funding_sub_agency,
            "period_start": row.period_start.isoformat() if row.period_start else None,
            "period_end": row.period_end.isoformat() if row.period_end else None,
            "description": row.description,
            "contract_type": row.contract_type,
            "source_url": row.source_url,
            "source": row.source,
            "mapping_method": row.mapping_method,
            "mapping_confidence": row.mapping_confidence,
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


def inactive_government_contracts_summary() -> dict[str, Any]:
    return {
        "status": "ok",
        "active": False,
        "source": GOVERNMENT_CONTRACTS_SOURCE,
        "label": GOVERNMENT_CONTRACTS_LABEL,
        "summary": "No awards above threshold in selected window.",
        "contract_count": 0,
        "total_award_amount": 0.0,
        "largest_award_amount": None,
        "latest_award_date": None,
        "top_agency": None,
        "direction": "neutral",
        "score_contribution": 0,
        "detail": "No awards above threshold in selected window.",
    }


def unavailable_government_contracts_summary() -> dict[str, Any]:
    return {
        "status": "unavailable",
        "active": None,
        "source": GOVERNMENT_CONTRACTS_SOURCE,
        "label": GOVERNMENT_CONTRACTS_LABEL,
        "summary": "Government contract data unavailable.",
        "contract_count": None,
        "total_award_amount": None,
        "largest_award_amount": None,
        "latest_award_date": None,
        "top_agency": None,
        "direction": None,
        "score_contribution": 0,
        "detail": "Government contract data unavailable.",
    }


def _government_contract_row_count(db: Session) -> int:
    if not government_contracts_table_exists(db):
        return 0
    return int(db.execute(select(func.count()).select_from(GovernmentContract)).scalar() or 0)


def _cutoff_date(lookback_days: int) -> date:
    bounded_lookback = max(1, min(int(lookback_days or DEFAULT_GOVERNMENT_CONTRACTS_LOOKBACK_DAYS), 365 * 3))
    return (datetime.now(timezone.utc) - timedelta(days=bounded_lookback)).date()


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


def _government_contracts_score_contribution(
    *,
    total_award_amount: float,
    latest_award_date: str | None,
) -> int:
    total = max(float(total_award_amount or 0.0), 0.0)
    if total >= 250_000_000:
        base_score = 20
    elif total >= 50_000_000:
        base_score = 15
    elif total >= 10_000_000:
        base_score = 10
    elif total >= 1_000_000:
        base_score = 5
    else:
        base_score = 0

    if base_score <= 0:
        return 0

    recency_boost = 0
    parsed_latest = _parse_iso_date(latest_award_date)
    if parsed_latest is not None:
        age_days = max((datetime.now(timezone.utc).date() - parsed_latest).days, 0)
        if age_days <= 7:
            recency_boost = 5
        elif age_days <= 30:
            recency_boost = 3

    return min(base_score + recency_boost, 20)


def _government_contracts_summary_line(
    *,
    contract_count: int,
    total_award_amount: float,
    active: bool,
) -> str:
    if not active or contract_count <= 0:
        return "No awards above threshold in selected window."
    return (
        f"Government contracts: {_format_currency_compact(total_award_amount)} "
        f"across {contract_count} award{'s' if contract_count != 1 else ''} in the selected window."
    )


def _government_contracts_detail_line(
    *,
    contract_count: int,
    total_award_amount: float,
    top_agency: str | None,
    active: bool,
) -> str:
    if not active or contract_count <= 0:
        return "No awards above threshold in selected window."
    detail = (
        f"{_format_currency_compact(total_award_amount)} across {contract_count} "
        f"award{'s' if contract_count != 1 else ''}"
    )
    if top_agency:
        return f"{detail} · Top agency: {top_agency}"
    return detail


def _format_currency_compact(value: float | int | None) -> str:
    amount = max(float(value or 0.0), 0.0)
    if amount >= 1_000_000_000:
        compact = amount / 1_000_000_000
        suffix = "B"
    elif amount >= 1_000_000:
        compact = amount / 1_000_000
        suffix = "M"
    elif amount >= 1_000:
        compact = amount / 1_000
        suffix = "K"
    else:
        return f"${amount:,.0f}"

    decimals = 0 if compact >= 100 or compact.is_integer() else 1
    return f"${compact:.{decimals}f}{suffix}"


def _parse_iso_date(value: str | None) -> date | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        return date.fromisoformat(value.strip()[:10])
    except ValueError:
        return None
