from __future__ import annotations

import json
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from math import isfinite
from typing import Any

from sqlalchemy import MetaData, Table, func, inspect, or_, select
from sqlalchemy.orm import Session

from app.models import AppSetting, Event, InstitutionalTransaction
from app.services.options_flow import OptionsFlowObservation, summarize_options_flow
from app.services.ticker_events import GOVERNMENT_CONTRACT_EVENT_TYPES
from app.utils.symbols import normalize_symbol

DEFAULT_GOVERNMENT_CONTRACTS_MIN_AMOUNT = 1_000_000
DEFAULT_GOVERNMENT_CONTRACTS_LOOKBACK_DAYS = 365
DEFAULT_OPTIONS_FLOW_LOOKBACK_DAYS = 30
DEFAULT_INSTITUTIONAL_ACTIVITY_LOOKBACK_DAYS = 90


def load_intelligence_feature_flags(db: Session) -> dict[str, bool]:
    return {
        "feature_government_contracts_enabled": _read_bool_setting(db, "feature_government_contracts_enabled", default=True),
        "feature_options_flow_enabled": _read_bool_setting(db, "feature_options_flow_enabled", default=True),
        "feature_institutional_activity_enabled": _read_bool_setting(db, "feature_institutional_activity_enabled", default=True),
        "feature_intelligence_overlays_premium_required": _read_bool_setting(
            db,
            "feature_intelligence_overlays_premium_required",
            default=True,
        ),
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
    return summaries.get(normalized or "", _inactive_government_contracts_summary())


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

    bounded_lookback = max(1, min(int(lookback_days or DEFAULT_GOVERNMENT_CONTRACTS_LOOKBACK_DAYS), 365 * 3))
    minimum_amount = _non_negative_float(min_amount) or 0.0
    since = datetime.now(timezone.utc) - timedelta(days=bounded_lookback + 31)

    rows = db.execute(
        select(Event)
        .where(Event.symbol.is_not(None))
        .where(func.upper(Event.symbol).in_(normalized_symbols))
        .where(Event.event_type.in_(GOVERNMENT_CONTRACT_EVENT_TYPES))
        .where(or_(Event.ts >= since, Event.event_date >= since))
        .order_by(func.upper(Event.symbol), Event.ts.desc(), Event.id.desc())
    ).scalars().all()

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    since_date = since.date()
    for row in rows:
        symbol = normalize_symbol(row.symbol)
        if not symbol:
            continue
        payload = _load_payload(row.payload_json)
        award_date = _contract_event_date(row, payload)
        if award_date is None or award_date < since_date:
            continue
        amount = _contract_amount(row, payload)
        if amount is None or amount < minimum_amount:
            continue
        grouped[symbol].append(
            {
                "award_date": award_date,
                "amount": amount,
                "agency": _contract_agency(payload),
            }
        )

    results = {symbol: _inactive_government_contracts_summary() for symbol in normalized_symbols}
    for symbol, events in grouped.items():
        if not events:
            continue
        total_award_amount = round(sum(event["amount"] for event in events), 2)
        largest_award_amount = max(event["amount"] for event in events)
        latest_event = max(events, key=lambda event: event["award_date"])
        agency_totals: dict[str, float] = defaultdict(float)
        for event in events:
            if event["agency"]:
                agency_totals[str(event["agency"])] += float(event["amount"])
        top_agency = max(agency_totals.items(), key=lambda item: item[1])[0] if agency_totals else None
        score_contribution = max(
            6,
            min(
                24,
                int(
                    round(
                        6
                        + min(total_award_amount / 25_000_000, 10.0)
                        + min(len(events), 6) * 1.5
                        + min(largest_award_amount / 50_000_000, 4.0)
                    )
                ),
            ),
        )
        results[symbol] = {
            "active": True,
            "contract_count": len(events),
            "total_award_amount": total_award_amount,
            "largest_award_amount": round(float(largest_award_amount), 2),
            "latest_award_date": latest_event["award_date"].isoformat(),
            "top_agency": top_agency,
            "direction": "bullish",
            "score_contribution": score_contribution,
        }
    return results


def get_options_flow_summary_local(
    db: Session,
    symbol: str,
    lookback_days: int = DEFAULT_OPTIONS_FLOW_LOOKBACK_DAYS,
) -> dict[str, Any]:
    summaries, _ = get_options_flow_summaries_for_symbols(db, [symbol], lookback_days=lookback_days)
    normalized = normalize_symbol(symbol)
    return summaries.get(normalized or "", _unavailable_options_flow_overlay(""))


def get_options_flow_summaries_for_symbols(
    db: Session,
    symbols: list[str],
    *,
    lookback_days: int = DEFAULT_OPTIONS_FLOW_LOOKBACK_DAYS,
    feature_enabled: bool = True,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    normalized_symbols = sorted({normalize_symbol(symbol) for symbol in symbols if normalize_symbol(symbol)})
    if not normalized_symbols:
        return {}, _options_flow_availability(status="unavailable", enabled=feature_enabled)

    if not feature_enabled:
        unavailable = {symbol: _unavailable_options_flow_overlay(symbol) for symbol in normalized_symbols}
        return unavailable, _options_flow_availability(status="disabled", enabled=False)

    inspector = inspect(db.get_bind())
    if inspector.has_table("options_flow_summary"):
        summaries = _options_flow_from_summary_table(db, normalized_symbols, lookback_days=lookback_days)
        if summaries:
            return summaries, _options_flow_availability(status="ok", enabled=True)

    if inspector.has_table("options_flow_events"):
        summaries = _options_flow_from_events_table(db, normalized_symbols, lookback_days=lookback_days)
        if summaries:
            return summaries, _options_flow_availability(status="ok", enabled=True)

    unavailable = {symbol: _unavailable_options_flow_overlay(symbol) for symbol in normalized_symbols}
    return unavailable, _options_flow_availability(status="unavailable", enabled=True)


def get_institutional_activity_summary(
    db: Session,
    symbol: str,
    lookback_days: int = DEFAULT_INSTITUTIONAL_ACTIVITY_LOOKBACK_DAYS,
) -> dict[str, Any]:
    summaries, _ = get_institutional_activity_summaries_for_symbols(
        db,
        [symbol],
        lookback_days=lookback_days,
    )
    normalized = normalize_symbol(symbol)
    return summaries.get(normalized or "", _not_configured_institutional_summary())


def get_institutional_activity_summaries_for_symbols(
    db: Session,
    symbols: list[str],
    *,
    lookback_days: int = DEFAULT_INSTITUTIONAL_ACTIVITY_LOOKBACK_DAYS,
    feature_enabled: bool = True,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    normalized_symbols = sorted({normalize_symbol(symbol) for symbol in symbols if normalize_symbol(symbol)})
    if not normalized_symbols:
        return {}, _institutional_availability(status="not_configured", enabled=feature_enabled)

    if not feature_enabled:
        disabled = {symbol: _not_configured_institutional_summary() for symbol in normalized_symbols}
        return disabled, _institutional_availability(status="disabled", enabled=False)

    inspector = inspect(db.get_bind())
    if not inspector.has_table(InstitutionalTransaction.__tablename__):
        disabled = {symbol: _not_configured_institutional_summary() for symbol in normalized_symbols}
        return disabled, _institutional_availability(status="not_configured", enabled=True)

    total_rows = db.execute(select(func.count()).select_from(InstitutionalTransaction)).scalar() or 0
    if int(total_rows) <= 0:
        disabled = {symbol: _not_configured_institutional_summary() for symbol in normalized_symbols}
        return disabled, _institutional_availability(status="not_configured", enabled=True)

    bounded_lookback = max(1, min(int(lookback_days or DEFAULT_INSTITUTIONAL_ACTIVITY_LOOKBACK_DAYS), 365))
    since_date = datetime.now(timezone.utc).date() - timedelta(days=bounded_lookback)
    activity_date = func.coalesce(InstitutionalTransaction.report_date, InstitutionalTransaction.filing_date)
    rows = db.execute(
        select(
            func.upper(InstitutionalTransaction.symbol).label("symbol"),
            InstitutionalTransaction.source,
            InstitutionalTransaction.institution_name,
            InstitutionalTransaction.institution_cik,
            InstitutionalTransaction.market_value,
            InstitutionalTransaction.change_in_shares,
            InstitutionalTransaction.filing_date,
            InstitutionalTransaction.report_date,
        )
        .where(InstitutionalTransaction.symbol.is_not(None))
        .where(func.upper(InstitutionalTransaction.symbol).in_(normalized_symbols))
        .where(activity_date >= since_date)
        .order_by(func.upper(InstitutionalTransaction.symbol), activity_date.desc())
    ).all()

    grouped: dict[str, list[Any]] = defaultdict(list)
    for row in rows:
        symbol = normalize_symbol(row.symbol)
        if symbol:
            grouped[symbol].append(row)

    results: dict[str, dict[str, Any]] = {}
    for symbol in normalized_symbols:
        symbol_rows = grouped.get(symbol, [])
        if not symbol_rows:
            results[symbol] = {
                "active": False,
                "direction": "neutral",
                "net_activity": None,
                "institution_count": None,
                "total_value": None,
                "latest_activity_date": None,
                "source": _institutional_source_name(None),
                "status": "ok",
            }
            continue

        net_activity = sum(
            (1.0 if float(row.change_in_shares or 0) >= 0 else -1.0) * float(row.market_value or 0)
            for row in symbol_rows
            if _non_negative_float(row.market_value) is not None
        )
        positive_count = sum(1 for row in symbol_rows if float(row.change_in_shares or 0) > 0)
        negative_count = sum(1 for row in symbol_rows if float(row.change_in_shares or 0) < 0)
        total_value = sum(float(row.market_value or 0) for row in symbol_rows if _non_negative_float(row.market_value) is not None)
        institutions = {
            (row.institution_cik or row.institution_name or "").strip()
            for row in symbol_rows
            if (row.institution_cik or row.institution_name or "").strip()
        }
        latest_activity_date = max(
            (
                row.report_date or row.filing_date
                for row in symbol_rows
                if row.report_date is not None or row.filing_date is not None
            ),
            default=None,
        )
        source = _institutional_source_name(next((row.source for row in symbol_rows if isinstance(row.source, str) and row.source.strip()), None))
        results[symbol] = {
            "active": bool(institutions and total_value > 0),
            "direction": _institutional_direction(net_activity, positive_count, negative_count),
            "net_activity": round(net_activity, 2) if symbol_rows else None,
            "institution_count": len(institutions) if institutions else None,
            "total_value": round(total_value, 2) if total_value > 0 else None,
            "latest_activity_date": latest_activity_date.isoformat() if latest_activity_date else None,
            "source": source,
            "status": "ok",
        }

    return results, _institutional_availability(status="ok", enabled=True)


def _read_bool_setting(db: Session, key: str, *, default: bool) -> bool:
    row = db.get(AppSetting, key)
    value = (row.value or "").strip().lower() if row and row.value else ""
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    return default


def _inactive_government_contracts_summary() -> dict[str, Any]:
    return {
        "active": False,
        "contract_count": 0,
        "total_award_amount": 0.0,
        "largest_award_amount": None,
        "latest_award_date": None,
        "top_agency": None,
        "direction": "neutral",
        "score_contribution": 0,
    }


def _load_payload(raw: str | None) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _contract_event_date(event: Event, payload: dict[str, Any]) -> date | None:
    for key in ("award_date", "awardDate", "period_start", "periodStart", "date", "event_date"):
        parsed = _parse_date(payload.get(key))
        if parsed is not None:
            return parsed
    if event.event_date is not None:
        return event.event_date.date()
    if event.ts is not None:
        return event.ts.date()
    return None


def _contract_amount(event: Event, payload: dict[str, Any]) -> float | None:
    for key in ("award_amount", "awardAmount", "amount", "obligated_amount", "obligatedAmount"):
        parsed = _non_negative_float(payload.get(key))
        if parsed is not None:
            return parsed
    if _non_negative_float(event.amount_max) is not None:
        return float(event.amount_max)
    if _non_negative_float(event.amount_min) is not None:
        return float(event.amount_min)
    return None


def _contract_agency(payload: dict[str, Any]) -> str | None:
    for key in ("awarding_agency", "awardingAgency", "agency", "department", "top_agency"):
        value = payload.get(key)
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
    text = value.strip()
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date()
    except ValueError:
        return None


def _options_flow_availability(*, status: str, enabled: bool) -> dict[str, Any]:
    return {
        "enabled": enabled,
        "status": status,
        "filterable": enabled and status == "ok",
    }


def _institutional_availability(*, status: str, enabled: bool) -> dict[str, Any]:
    return {
        "enabled": enabled,
        "status": status,
        "filterable": enabled and status == "ok",
    }


def _unavailable_options_flow_overlay(symbol: str) -> dict[str, Any]:
    return {
        "active": False,
        "score": None,
        "direction": "neutral",
        "intensity": None,
        "call_put_premium_ratio": None,
        "total_premium": None,
        "latest_flow_date": None,
        "source": None,
        "status": "unavailable",
    }


def _options_flow_from_summary_table(
    db: Session,
    symbols: list[str],
    *,
    lookback_days: int,
) -> dict[str, dict[str, Any]]:
    table = Table("options_flow_summary", MetaData(), autoload_with=db.get_bind())
    columns = set(table.c.keys())
    if "symbol" not in columns:
        return {}

    latest_column = next(
        (table.c[name] for name in ("latest_flow_date", "asof_date", "observed_at", "updated_at") if name in columns),
        None,
    )
    score_column = table.c["score"] if "score" in columns else None
    direction_column = table.c["direction"] if "direction" in columns else None
    intensity_column = table.c["intensity"] if "intensity" in columns else None
    ratio_column = next((table.c[name] for name in ("call_put_premium_ratio", "put_call_premium_ratio") if name in columns), None)
    total_premium_column = table.c["total_premium"] if "total_premium" in columns else None
    source_column = table.c["source"] if "source" in columns else None
    active_column = next((table.c[name] for name in ("active", "is_active") if name in columns), None)
    status_column = table.c["status"] if "status" in columns else None
    if direction_column is None and score_column is None and total_premium_column is None:
        return {}

    since_date = datetime.now(timezone.utc).date() - timedelta(days=max(1, min(int(lookback_days or 30), 365)))
    query = (
        select(table)
        .where(func.upper(table.c.symbol).in_(symbols))
        .order_by(func.upper(table.c.symbol), latest_column.desc() if latest_column is not None else table.c.symbol.asc())
    )
    if latest_column is not None:
        query = query.where(latest_column >= since_date)

    rows = db.execute(query).mappings().all()
    results: dict[str, dict[str, Any]] = {symbol: _unavailable_options_flow_overlay(symbol) for symbol in symbols}
    for row in rows:
        symbol = normalize_symbol(row.get("symbol"))
        if not symbol or results[symbol]["status"] == "ok":
            continue
        direction = _options_direction_value(row.get("direction"))
        score = _positive_int(row.get("score"))
        total_premium = _non_negative_float(row.get("total_premium"))
        ratio = _ratio_value(row.get("call_put_premium_ratio"))
        if ratio is None and "put_call_premium_ratio" in columns:
            ratio = _invert_ratio(_ratio_value(row.get("put_call_premium_ratio")))
        latest_flow_date = _string_date(row.get("latest_flow_date") or row.get("asof_date") or row.get("observed_at") or row.get("updated_at"))
        active = _coerce_bool(row.get("active") if "active" in columns else row.get("is_active")) if active_column is not None else direction in {"bullish", "bearish", "mixed"}
        status = str(row.get("status")).strip().lower() if status_column is not None and row.get("status") else "ok"
        results[symbol] = {
            "active": active,
            "score": score,
            "direction": direction,
            "intensity": _intensity_value(row.get("intensity")),
            "call_put_premium_ratio": ratio,
            "total_premium": round(total_premium, 2) if total_premium is not None else None,
            "latest_flow_date": latest_flow_date,
            "source": _options_source_name(row.get("source") if source_column is not None else None),
            "status": "ok" if status == "ok" else "unavailable",
        }
    return results


def _options_flow_from_events_table(
    db: Session,
    symbols: list[str],
    *,
    lookback_days: int,
) -> dict[str, dict[str, Any]]:
    table = Table("options_flow_events", MetaData(), autoload_with=db.get_bind())
    columns = set(table.c.keys())
    required = {"symbol", "contract_type", "premium"}
    observed_name = next((name for name in ("observed_at", "flow_date", "event_date", "created_at") if name in columns), None)
    volume_name = next((name for name in ("contract_volume", "volume", "contracts") if name in columns), None)
    if not required.issubset(columns) or observed_name is None or volume_name is None:
        return {}

    observed_at = table.c[observed_name]
    since = datetime.now(timezone.utc) - timedelta(days=max(1, min(int(lookback_days or 30), 365)))
    rows = db.execute(
        select(table)
        .where(func.upper(table.c.symbol).in_(symbols))
        .where(observed_at >= since)
        .order_by(func.upper(table.c.symbol), observed_at.desc())
    ).mappings().all()
    if not rows:
        return {}

    grouped: dict[str, list[OptionsFlowObservation]] = defaultdict(list)
    source_by_symbol: dict[str, str | None] = {}
    for row in rows:
        symbol = normalize_symbol(row.get("symbol"))
        if not symbol:
            continue
        contract_type = str(row.get("contract_type") or "").strip().lower()
        if contract_type not in {"call", "put"}:
            continue
        premium = _non_negative_float(row.get("premium"))
        volume = _positive_int(row.get(volume_name))
        if premium is None or volume is None:
            continue
        grouped[symbol].append(
            OptionsFlowObservation(
                contract_type=contract_type,  # type: ignore[arg-type]
                premium=premium,
                contract_volume=volume,
                observed_at=_as_utc_datetime(row.get(observed_name)),
            )
        )
        source_by_symbol.setdefault(symbol, _options_source_name(row.get("source")))

    results: dict[str, dict[str, Any]] = {}
    for symbol in symbols:
        observations = grouped.get(symbol, [])
        if not observations:
            results[symbol] = _unavailable_options_flow_overlay(symbol)
            continue
        summary = summarize_options_flow(
            symbol,
            observations,
            lookback_days=lookback_days,
            provider=source_by_symbol.get(symbol) or "options_flow_events",
        )
        results[symbol] = _options_overlay_from_canonical_summary(summary)
    return results


def _options_overlay_from_canonical_summary(summary: dict[str, Any]) -> dict[str, Any]:
    state = str(summary.get("direction") or summary.get("state") or "neutral").strip().lower()
    direction = state if state in {"bullish", "bearish", "mixed"} else "neutral"
    status = str(summary.get("status") or "ok").strip().lower()
    return {
        "active": summary.get("active") is True or summary.get("is_active") is True,
        "score": _positive_int(summary.get("score")),
        "direction": direction,
        "intensity": _intensity_value(summary.get("intensity")),
        "call_put_premium_ratio": _ratio_value(summary.get("call_put_premium_ratio")),
        "total_premium": _non_negative_float(summary.get("total_premium")),
        "latest_flow_date": _string_date(summary.get("latest_flow_date")),
        "source": _options_source_name(summary.get("source") or summary.get("provider")),
        "status": "unavailable" if status == "unavailable" else "ok",
    }


def _not_configured_institutional_summary() -> dict[str, Any]:
    return {
        "active": False,
        "direction": "neutral",
        "net_activity": None,
        "institution_count": None,
        "total_value": None,
        "latest_activity_date": None,
        "source": None,
        "status": "not_configured",
    }


def _institutional_direction(net_activity: float, positive_count: int, negative_count: int) -> str:
    if positive_count > 0 and negative_count > 0:
        sided_total = positive_count + negative_count
        if sided_total > 0 and abs(positive_count - negative_count) / sided_total < 0.34:
            return "mixed"
    if net_activity > 0:
        return "bullish"
    if net_activity < 0:
        return "bearish"
    if positive_count > 0 and negative_count > 0:
        return "mixed"
    return "neutral"


def _institutional_source_name(value: Any) -> str | None:
    source = str(value or "").strip().lower()
    if "intrinio" in source:
        return "intrinio"
    if source:
        return "fmp"
    return None


def _options_source_name(value: Any) -> str | None:
    source = str(value or "").strip().lower()
    if source in {"massive", "polygon", "intrinio"}:
        return source
    if "massive" in source:
        return "massive"
    if "polygon" in source:
        return "polygon"
    if "intrinio" in source:
        return "intrinio"
    return None


def _options_direction_value(value: Any) -> str:
    direction = str(value or "").strip().lower()
    if direction in {"bullish", "bearish", "mixed"}:
        return direction
    return "neutral"


def _intensity_value(value: Any) -> str | None:
    intensity = str(value or "").strip().lower()
    if intensity in {"low", "medium", "high"}:
        return intensity
    return None


def _ratio_value(value: Any) -> float | None:
    parsed = _non_negative_float(value)
    return round(parsed, 2) if parsed is not None else None


def _invert_ratio(value: float | None) -> float | None:
    if value is None or value == 0:
        return None
    return round(1 / value, 2)


def _string_date(value: Any) -> str | None:
    parsed = _parse_date(value)
    return parsed.isoformat() if parsed else None


def _as_utc_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
    parsed = _parse_date(value)
    if parsed is None:
        return None
    return datetime(parsed.year, parsed.month, parsed.day, tzinfo=timezone.utc)


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "active", "ok"}
    return False


def _positive_int(value: Any) -> int | None:
    try:
        parsed = int(round(float(value)))
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None


def _non_negative_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not isfinite(parsed) or parsed < 0:
        return None
    return parsed
