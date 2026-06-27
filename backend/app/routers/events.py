from __future__ import annotations

import copy
import json
import logging
import os
import re
import threading
from datetime import date, datetime, timedelta, timezone
from time import monotonic, perf_counter
from types import SimpleNamespace
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy import DateTime, Float, Integer, String, and_, bindparam, case, cast, exists, func, or_, select, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from app.auth import current_user, is_admin_user
from app.db import get_db
from app.rate_limit import rate_limit_provider_backed
from app.models import Event, GovernmentContractAction, Member, MonitoringAlert, Security, TickerMeta, TradeOutcome, Watchlist, WatchlistItem
from app.services.ticker_meta import get_cik_meta, get_ticker_meta, normalize_cik
from app.schemas import EventOut, EventsDebug, EventsPage, EventsPageDebug
from app.services.price_lookup import get_close_for_date_or_prior, get_eod_close, get_eod_close_series
from app.services.quote_lookup import get_current_prices_meta_db
from app.services.returns import signed_return_pct
from app.services.member_performance import INSIDER_METHODOLOGY_VERSION
from app.services.congress_assets import (
    CONGRESS_CRYPTO_EVENT_TYPE,
    CONGRESS_DISCLOSURE_EVENT_TYPES,
    CONGRESS_EQUITY_EVENT_TYPE,
    CONGRESS_NON_EQUITY_EVENT_TYPES,
    CONGRESS_TREASURY_EVENT_TYPE,
    canonical_asset_class_value,
)
from app.services.profile_performance_curve import build_normalized_profile_curve, build_timeline_dates, load_profile_price_close_maps
from app.services.replicated_portfolios import latest_replicated_portfolio_payload
from app.services.signal_score import calculate_smart_score
from app.services.confirmation_metrics import ConfirmationMetrics, get_confirmation_metrics_for_symbols
from app.services.event_activity_filters import VISIBLE_INSIDER_TRADE_TYPES, insider_visibility_clause
from app.services.ticker_identity import safe_company_identity_candidate
from app.services.trade_outcome_display import (
    trade_outcome_display_metrics,
    trade_outcome_logical_key,
)
from app.services.trade_outcomes import rank_extreme_trade_outcomes
from app.services.congress_outcome_eligibility import congress_equity_outcome_eligibility
from app.services.ticker_events import GOVERNMENT_CONTRACT_EVENT_TYPES
from app.services.government_departments import DEPARTMENT_ALIASES, canonical_department_name, department_suggestions
from app.services.foreign_trade_normalization import normalize_insider_price, normalization_payload
from app.services.search_suggest import search_suggestions
from app.services.feed_pnl_enrichment import FEED_PNL_PRIORITY_BASE, enqueue_feed_pnl_enrichment_for_events
from app.utils.symbols import normalize_symbol
from app.request_priority import get_request_context

router = APIRouter(tags=["events"])
logger = logging.getLogger(__name__)

DEFAULT_LIMIT = 50
MAX_LIMIT = 200
MAX_SUGGEST_LIMIT = 50
DEFAULT_BASELINE_DAYS = 365
DEFAULT_MIN_BASELINE_COUNT = 3
FEED_OUTCOME_ENQUEUE_LIMIT = int(os.getenv("FEED_OUTCOME_ENQUEUE_LIMIT", "100") or 100)
FEED_OUTCOME_RETRY_STATUSES = {
    "no_current_price",
    "no_data",
    "no_entry_price",
    "no_execution_price",
    "price_unavailable",
    "provider_402",
    "provider_429",
    "provider_unavailable",
    "retry_later",
}
ALLOWED_LOOKBACK_DAYS = {30, 90, 180, 365, 1095}
ALLOWED_LOOKBACK_DAYS_LABEL = ", ".join(str(value) for value in sorted(ALLOWED_LOOKBACK_DAYS))
EVENTS_RESPONSE_CACHE_TTL_SECONDS = int(os.getenv("EVENTS_RESPONSE_CACHE_TTL_SECONDS", "10") or 10)
EVENTS_RESPONSE_DEDUPE_WAIT_SECONDS = float(os.getenv("EVENTS_RESPONSE_DEDUPE_WAIT_SECONDS", "3") or 3)
_EVENTS_RESPONSE_CACHE: dict[str, tuple[float, EventsPage | EventsPageDebug]] = {}
_EVENTS_RESPONSE_INFLIGHT: dict[str, dict[str, object]] = {}
_EVENTS_RESPONSE_CACHE_LOCK = threading.Lock()
INSIDER_SUMMARY_CACHE_TTL_SECONDS = int(os.getenv("INSIDER_SUMMARY_CACHE_TTL_SECONDS", "60") or 60)
INSIDER_SUMMARY_DEDUPE_WAIT_SECONDS = float(os.getenv("INSIDER_SUMMARY_DEDUPE_WAIT_SECONDS", "5") or 5)
_INSIDER_SUMMARY_CACHE: dict[str, tuple[float, dict]] = {}
_INSIDER_SUMMARY_INFLIGHT: dict[str, dict[str, object]] = {}
_INSIDER_SUMMARY_CACHE_LOCK = threading.Lock()
GOVERNMENT_CONTRACT_DEPARTMENT_OPTIONS = (
    "Department of Defense",
    "Department of Health and Human Services",
    "Department of Agriculture",
    "Department of Energy",
    "Department of Homeland Security",
    "Department of Veterans Affairs",
    "National Aeronautics and Space Administration",
    "General Services Administration",
    "Department of Transportation",
    "Department of Justice",
)
BAD_EVENT_IDENTITY_LABELS = {
    "congress_trade",
    "congress_treasury_trade",
    "congress_crypto_trade",
    "insider_trade",
    "institutional_buy",
    "government_contract",
    "event",
    "security",
}


def _log_ticker_events_payload(
    *,
    symbols: list[str],
    items: list[EventOut],
    recent_days: int | None,
    started_at: float,
) -> None:
    if not symbols:
        return
    context = get_request_context() or {}
    logger.info(
        "ticker_content_payload symbol=%s endpoint=events status=%s item_count=%s keys_present=%s window_days=%s updated_at=%s duration_ms=%.1f db_query_count=%s db_checkout_count=%s db_checkout_slow_count=%s",
        ",".join(symbols),
        "ok" if items else "no_data",
        len(items),
        ["items", "limit", "offset"],
        recent_days,
        None,
        (perf_counter() - started_at) * 1000,
        context.get("db_query_count"),
        context.get("db_checkout_count"),
        context.get("db_checkout_slow_count"),
    )


def _log_events_request_summary(
    *,
    started_at: float,
    item_count: int,
    total: int | None,
    include_total: bool,
    enrich_prices: bool,
    limit: int,
    page_size: int | None,
    offset: int,
) -> None:
    context = get_request_context() or {}
    logger.info(
        "events_feed_timing endpoint=/api/events total_duration_ms=%.1f item_count=%s include_total=%s total=%s enrich_prices=%s limit=%s page_size=%s offset=%s db_checkout_count=%s db_checkout_slow_count=%s query_count=%s component=%s route=%s",
        (perf_counter() - started_at) * 1000,
        item_count,
        include_total,
        total,
        enrich_prices,
        limit,
        page_size,
        offset,
        context.get("db_checkout_count"),
        context.get("db_checkout_slow_count"),
        context.get("db_query_count"),
        context.get("walnut_component"),
        context.get("walnut_route"),
    )


MEMBER_NICKNAME_EXPANSIONS = {
    "BILL": ("WILLIAM",),
    "BILLY": ("WILLIAM",),
    "BOB": ("ROBERT",),
    "BOBBY": ("ROBERT",),
    "ROB": ("ROBERT",),
    "ROBBY": ("ROBERT",),
    "MIKE": ("MICHAEL",),
    "MIKEY": ("MICHAEL",),
    "JIM": ("JAMES",),
    "JIMMY": ("JAMES",),
    "TOM": ("THOMAS",),
    "TOMMY": ("THOMAS",),
    "DAVE": ("DAVID",),
    "DAN": ("DANIEL",),
    "DANNY": ("DANIEL",),
    "JOE": ("JOSEPH",),
    "JOEY": ("JOSEPH",),
    "STEVE": ("STEPHEN", "STEVEN"),
    "CHUCK": ("CHARLES",),
    "CHARLIE": ("CHARLES",),
    "RICK": ("RICHARD",),
    "RICKY": ("RICHARD",),
    "DICK": ("RICHARD",),
}
MEMBER_NAME_SUFFIX_TOKENS = {"JR", "SR", "II", "III", "IV", "V"}
MEMBER_NAME_TOKEN_RE = re.compile(r"[A-Z0-9]+")


def _is_production_runtime() -> bool:
    runtime = (os.getenv("APP_ENV") or os.getenv("ENV") or os.getenv("NODE_ENV") or "").strip().lower()
    return runtime in {"prod", "production"}


def _events_debug_enabled(db: Session, request: Request | None, requested: bool | None) -> bool:
    if not requested:
        return False
    if not _is_production_runtime():
        return True
    if request is None:
        return False
    try:
        user = current_user(db, request, required=False)
    except Exception:
        return False
    return is_admin_user(user)


def _events_response_cache_ttl_seconds() -> int:
    return max(0, min(60, EVENTS_RESPONSE_CACHE_TTL_SECONDS))


def _events_response_cache_key(
    *,
    request: Request | None,
    debug_enabled: bool,
    include_total: bool,
    enrich_prices: bool,
    combined_symbols: list[str],
    type_list: list[str],
    tape_value: str | None,
    since: str | None,
    member: str | None,
    member_id: str | None,
    chamber_value: str | None,
    party_value: str | None,
    asset_filter_value: str,
    trade_value: str | None,
    transaction_type: str | None,
    role: str | None,
    ownership: str | None,
    department: str | None,
    min_amount: float | None,
    max_amount: float | None,
    filed_after_max: float | None,
    pnl_min: float | None,
    pnl_max: float | None,
    signal_min: float | None,
    whale: bool | None,
    recent_days: int | None,
    cursor: str | None,
    limit: int,
    page_size: int | None,
    offset: int,
) -> str | None:
    if request is None and not _is_production_runtime():
        return None
    if debug_enabled or include_total:
        return None
    if _events_response_cache_ttl_seconds() <= 0:
        return None
    key_parts = {
        "symbols": tuple(sorted(combined_symbols)),
        "types": tuple(sorted(type_list)),
        "enrich_prices": bool(enrich_prices),
        "tape": tape_value,
        "since": since,
        "member": (member or "").strip().casefold(),
        "member_id": (member_id or "").strip().casefold(),
        "chamber": chamber_value,
        "party": party_value,
        "asset": asset_filter_value.strip().casefold(),
        "trade": trade_value,
        "transaction": (transaction_type or "").strip().casefold(),
        "role": (role or "").strip().casefold(),
        "ownership": (ownership or "").strip().casefold(),
        "department": (department or "").strip().casefold(),
        "min": min_amount,
        "max": max_amount,
        "filed_after": filed_after_max,
        "pnl_min": pnl_min,
        "pnl_max": pnl_max,
        "signal_min": signal_min,
        "whale": bool(whale),
        "recent_days": recent_days,
        "cursor": cursor,
        "limit": limit,
        "page_size": page_size,
        "offset": offset,
    }
    return "events:" + json.dumps(key_parts, sort_keys=True, separators=(",", ":"), default=str)


def _events_response_cache_get(cache_key: str | None) -> EventsPage | EventsPageDebug | None:
    if not cache_key:
        return None
    now = monotonic()
    with _EVENTS_RESPONSE_CACHE_LOCK:
        cached = _EVENTS_RESPONSE_CACHE.get(cache_key)
        if cached is None:
            return None
        expires_at, payload = cached
        if expires_at <= now:
            _EVENTS_RESPONSE_CACHE.pop(cache_key, None)
            return None
        return copy.deepcopy(payload)


def _events_response_inflight_start(cache_key: str | None) -> tuple[dict[str, object] | None, bool]:
    if not cache_key:
        return None, False
    now = monotonic()
    with _EVENTS_RESPONSE_CACHE_LOCK:
        state = _EVENTS_RESPONSE_INFLIGHT.get(cache_key)
        if state is not None and now - float(state.get("started_at") or 0.0) <= EVENTS_RESPONSE_DEDUPE_WAIT_SECONDS:
            return state, False
        state = {"event": threading.Event(), "result": None, "started_at": now}
        _EVENTS_RESPONSE_INFLIGHT[cache_key] = state
        return state, True


def _events_response_cache_finalize(
    cache_key: str | None,
    inflight_state: dict[str, object] | None,
    inflight_leader: bool,
    payload: EventsPage | EventsPageDebug,
) -> EventsPage | EventsPageDebug:
    if not cache_key:
        return payload
    stored = copy.deepcopy(payload)
    with _EVENTS_RESPONSE_CACHE_LOCK:
        _EVENTS_RESPONSE_CACHE[cache_key] = (monotonic() + _events_response_cache_ttl_seconds(), stored)
    if inflight_leader and inflight_state is not None:
        inflight_state["result"] = copy.deepcopy(payload)
        event = inflight_state.get("event")
        if isinstance(event, threading.Event):
            event.set()
        with _EVENTS_RESPONSE_CACHE_LOCK:
            _EVENTS_RESPONSE_INFLIGHT.pop(cache_key, None)
    return payload


def _insider_summary_cache_ttl_seconds() -> int:
    return max(0, min(300, INSIDER_SUMMARY_CACHE_TTL_SECONDS))


def _insider_summary_cache_key(reporting_cik: str, lookback_days: int, issuer: str | None) -> str | None:
    if _insider_summary_cache_ttl_seconds() <= 0:
        return None
    normalized_cik = normalize_cik(reporting_cik)
    if not normalized_cik:
        return None
    issuer_key = normalize_cik(issuer) or normalize_symbol(issuer) or (issuer or "").strip().upper()
    return "insider_summary:" + json.dumps(
        {"cik": normalized_cik, "lookback_days": int(lookback_days), "issuer": issuer_key},
        sort_keys=True,
        separators=(",", ":"),
    )


def _insider_summary_cache_get(cache_key: str | None) -> dict | None:
    if not cache_key:
        return None
    now = monotonic()
    with _INSIDER_SUMMARY_CACHE_LOCK:
        cached = _INSIDER_SUMMARY_CACHE.get(cache_key)
        if cached is None:
            return None
        expires_at, payload = cached
        if expires_at <= now:
            _INSIDER_SUMMARY_CACHE.pop(cache_key, None)
            return None
        return copy.deepcopy(payload)


def _insider_summary_inflight_start(cache_key: str | None) -> tuple[dict[str, object] | None, bool]:
    if not cache_key:
        return None, False
    with _INSIDER_SUMMARY_CACHE_LOCK:
        state = _INSIDER_SUMMARY_INFLIGHT.get(cache_key)
        if state is not None:
            return state, False
        state = {"event": threading.Event(), "result": None, "error": None}
        _INSIDER_SUMMARY_INFLIGHT[cache_key] = state
        return state, True


def _insider_summary_cache_finalize(
    cache_key: str | None,
    inflight_state: dict[str, object] | None,
    inflight_leader: bool,
    payload: dict,
) -> dict:
    if cache_key:
        stored = copy.deepcopy(payload)
        with _INSIDER_SUMMARY_CACHE_LOCK:
            _INSIDER_SUMMARY_CACHE[cache_key] = (monotonic() + _insider_summary_cache_ttl_seconds(), stored)
    if inflight_leader and inflight_state is not None:
        inflight_state["result"] = copy.deepcopy(payload)
        event = inflight_state.get("event")
        if isinstance(event, threading.Event):
            event.set()
        with _INSIDER_SUMMARY_CACHE_LOCK:
            _INSIDER_SUMMARY_INFLIGHT.pop(cache_key or "", None)
    return payload


def _insider_summary_inflight_error(
    cache_key: str | None,
    inflight_state: dict[str, object] | None,
    inflight_leader: bool,
    exc: BaseException,
) -> None:
    if not inflight_leader or inflight_state is None:
        return
    inflight_state["error"] = exc
    event = inflight_state.get("event")
    if isinstance(event, threading.Event):
        event.set()
    with _INSIDER_SUMMARY_CACHE_LOCK:
        _INSIDER_SUMMARY_INFLIGHT.pop(cache_key or "", None)


def _is_legacy_member_alias(member_id: str | None) -> bool:
    return (member_id or "").strip().upper().startswith("FMP_")


def _normalize_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _parse_iso_datetime(value: str) -> datetime:
    cleaned = value.strip()
    if cleaned.endswith("Z"):
        cleaned = cleaned[:-1] + "+00:00"
    parsed = datetime.fromisoformat(cleaned)
    return _normalize_datetime(parsed)


def _parse_since(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return _parse_iso_datetime(value)
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid since datetime.") from exc


def _government_contract_department_clause(department: str, *, include_non_contract_events: bool):
    value = department.strip()
    if not value:
        return None

    payload_lower = func.lower(func.coalesce(Event.payload_json, ""))
    member_lower = func.lower(func.coalesce(Event.member_name, ""))
    contract_type_clause = Event.event_type.in_(GOVERNMENT_CONTRACT_EVENT_TYPES)

    if value.lower() == "other":
        known_values = {
            alias
            for known in GOVERNMENT_CONTRACT_DEPARTMENT_OPTIONS
            for alias in DEPARTMENT_ALIASES.get(canonical_department_name(known) or known, (known,))
        }
        known_clauses = [
            or_(
                payload_lower.like(f"%{known.lower()}%"),
                member_lower.like(f"%{known.lower()}%"),
            )
            for known in known_values
        ]
        contract_clause = and_(contract_type_clause, *[~clause for clause in known_clauses])
    else:
        canonical = canonical_department_name(value) or value
        needles = DEPARTMENT_ALIASES.get(canonical, (canonical,))
        contract_clause = and_(
            contract_type_clause,
            or_(
                *[
                    clause
                    for needle in needles
                    for clause in (
                        payload_lower.like(f"%{needle.lower()}%"),
                        member_lower.like(f"%{needle.lower()}%"),
                    )
                ],
            ),
        )

    if include_non_contract_events:
        return or_(Event.event_type.notin_(GOVERNMENT_CONTRACT_EVENT_TYPES), contract_clause)
    return contract_clause


def _parse_cursor(cursor: str) -> tuple[datetime, int]:
    try:
        ts_str, id_str = cursor.split("|", 1)
        cursor_id = int(id_str)
        cursor_ts = _parse_iso_datetime(ts_str)
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid cursor format. Expected ts|id") from exc
    return cursor_ts, cursor_id


def _parse_optional_payload_datetime(value) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        return _parse_iso_datetime(value)
    except Exception:
        try:
            parsed_date = date.fromisoformat(value.strip())
        except Exception:
            return None
        return datetime.combine(parsed_date, datetime.min.time(), tzinfo=timezone.utc)


def _event_effective_activity_ts(event: Event) -> datetime:
    payload = _parse_event_payload(event)
    raw = payload.get("raw") if isinstance(payload.get("raw"), dict) else {}
    for value in (
        payload.get("filing_date"),
        payload.get("filingDate"),
        payload.get("report_date"),
        payload.get("reportDate"),
        raw.get("filing_date"),
        raw.get("filingDate"),
        raw.get("report_date"),
        raw.get("reportDate"),
    ):
        parsed = _parse_optional_payload_datetime(value)
        if parsed is not None:
            return parsed
    return _normalize_datetime(event.created_at or event.ts or event.event_date)


def _event_effective_activity_ts_expr(db: Session):
    if db.get_bind().dialect.name == "postgresql":
        payload = cast(Event.payload_json, JSONB)
        json_dates = [
            cast(func.nullif(payload[key].astext, ""), DateTime(timezone=True))
            for key in ("filing_date", "filingDate", "report_date", "reportDate")
        ] + [
            cast(func.nullif(payload[("raw", key)].astext, ""), DateTime(timezone=True))
            for key in ("filing_date", "filingDate", "report_date", "reportDate")
        ]
        return func.coalesce(*json_dates, Event.created_at, Event.ts, Event.event_date)
    if db.get_bind().dialect.name != "sqlite":
        return func.coalesce(Event.created_at, Event.ts, Event.event_date)
    json_dates = [
        func.datetime(func.nullif(func.json_extract(Event.payload_json, path), ""))
        for path in (
            "$.filing_date",
            "$.filingDate",
            "$.report_date",
            "$.reportDate",
            "$.raw.filing_date",
            "$.raw.filingDate",
            "$.raw.report_date",
            "$.raw.reportDate",
        )
    ]
    return func.coalesce(*json_dates, Event.created_at, Event.ts, Event.event_date)


def _parse_csv(value: str | None) -> list[str]:
    if not value:
        return []
    return [part.strip() for part in value.split(",") if part.strip()]


def _normalize_event_type_alias(value: str) -> str:
    normalized = value.strip().lower()
    if normalized in {"all", "any"}:
        return ""
    if normalized in {"congress", "congress_trades"}:
        return ",".join(CONGRESS_DISCLOSURE_EVENT_TYPES)
    if normalized in {"insider", "insider_trades"}:
        return "insider_trade"
    if normalized in {"government_contracts", "government_contract_action", "gov_contract"}:
        return "government_contract"
    return normalized


def _expand_event_type_aliases(values: list[str]) -> list[str]:
    expanded: list[str] = []
    for value in values:
        expanded.extend(_normalize_event_type_alias(value).split(","))
    return [value for value in expanded if value]


def _congress_disclosure_clause():
    return Event.event_type.in_(CONGRESS_DISCLOSURE_EVENT_TYPES)


def _asset_class_filter_clause(asset_class: str):
    normalized = asset_class.strip().lower().replace("-", "_")
    if normalized in {"all", "any"}:
        return None
    payload_lower = func.lower(func.coalesce(Event.payload_json, ""))
    fund_text_clause = or_(
        payload_lower.like("%\"asset_class\": \"etf%"),
        payload_lower.like("%\"asset_class\":\"etf%"),
        payload_lower.like("%\"assetclass\": \"etf%"),
        payload_lower.like("%\"assetclass\":\"etf%"),
        payload_lower.like("%\"asset_class\": \"fund%"),
        payload_lower.like("%\"asset_class\":\"fund%"),
        payload_lower.like("%\"assetclass\": \"fund%"),
        payload_lower.like("%\"assetclass\":\"fund%"),
        payload_lower.like("%\"asset_class\": \"mutual fund%"),
        payload_lower.like("%\"asset_class\":\"mutual fund%"),
        payload_lower.like("%\"assetclass\": \"mutual fund%"),
        payload_lower.like("%\"assetclass\":\"mutual fund%"),
        payload_lower.like("% etf%"),
        payload_lower.like("%exchange traded fund%"),
        payload_lower.like("%mutual fund%"),
        payload_lower.like("% index fund%"),
        payload_lower.like("% money market fund%"),
        payload_lower.like("%closed end fund%"),
    )
    public_security_clause = and_(
        Event.event_type.in_([CONGRESS_EQUITY_EVENT_TYPE, "insider_trade"]),
        Event.symbol.is_not(None),
    )
    etf_fund_clause = and_(
        Event.event_type == CONGRESS_EQUITY_EVENT_TYPE,
        fund_text_clause,
    )
    if normalized in {"equity", "equities", "public_equity", "public_equities", "stocks", "stock"}:
        return and_(public_security_clause, ~etf_fund_clause)
    if normalized in {"security", "securities"}:
        return public_security_clause
    if normalized in {"etf", "fund", "etf_fund", "etfs", "funds"}:
        return etf_fund_clause
    if normalized in {"treasury", "treasuries", "treasury_security", "treasury_securities"}:
        return or_(
            Event.event_type == CONGRESS_TREASURY_EVENT_TYPE,
            and_(
                Event.event_type == CONGRESS_EQUITY_EVENT_TYPE,
                payload_lower.like("%treasury%"),
                Event.symbol.is_(None),
                ~fund_text_clause,
            ),
        )
    if normalized in {"crypto", "cryptocurrency", "crypto_asset", "crypto_assets"}:
        return or_(
            Event.event_type == CONGRESS_CRYPTO_EVENT_TYPE,
            and_(
                Event.event_type == CONGRESS_EQUITY_EVENT_TYPE,
                payload_lower.like("%crypto%"),
                Event.symbol.is_(None),
                ~fund_text_clause,
            ),
        )
    if normalized in {"other", "unresolved"}:
        payload_ticker_identity = or_(
            payload_lower.like("%\"symbol\":\"%"),
            payload_lower.like("%\"symbol\": \"%"),
            payload_lower.like("%\"ticker\":\"%"),
            payload_lower.like("%\"ticker\": \"%"),
        )
        exact_public_name_resolution = exists(
            select(Security.id).where(
                Security.symbol.is_not(None),
                Security.name.is_not(None),
                func.length(func.trim(Security.name)) > 0,
                payload_lower.like("%" + func.lower(Security.name) + "%"),
            )
        )
        public_equity_text = or_(
            payload_lower.like("%equity%"),
            payload_lower.like("%stock%"),
            payload_lower.like("%common share%"),
            payload_lower.like("%ordinary share%"),
            payload_lower.like("%etf%"),
            payload_lower.like("%exchange traded fund%"),
            payload_lower.like("%mutual fund%"),
            payload_lower.like("%fund%"),
            payload_lower.like("%treasury%"),
            payload_lower.like("%crypto%"),
        )
        return and_(
            Event.event_type == CONGRESS_EQUITY_EVENT_TYPE,
            Event.symbol.is_(None),
            ~payload_ticker_identity,
            ~exact_public_name_resolution,
            ~public_equity_text,
        )
    raise HTTPException(
        status_code=400,
        detail="Invalid asset_class. Allowed values: all, equity, etf_fund, treasury, crypto, other.",
    )


def _payload_numeric_expr(db: Session, *keys: str):
    if db.get_bind().dialect.name == "postgresql":
        payload = cast(Event.payload_json, JSONB)
        values = [cast(func.nullif(payload[key].astext, ""), Float) for key in keys]
        return func.coalesce(*values)
    if db.get_bind().dialect.name == "sqlite":
        values = [
            cast(func.nullif(func.json_extract(Event.payload_json, f"$.{key}"), ""), Float)
            for key in keys
        ]
        return func.coalesce(*values)
    return None


def _event_pnl_expr(db: Session):
    payload_pnl = _payload_numeric_expr(db, "pnl_pct", "pnlPct", "pnl", "return_pct", "returnPct")
    outcome_return = (
        select(TradeOutcome.return_pct)
        .where(TradeOutcome.event_id == Event.id)
        .correlate(Event)
        .scalar_subquery()
    )
    return func.coalesce(outcome_return, payload_pnl)


def _event_signal_expr(db: Session):
    return _payload_numeric_expr(db, "smart_score", "smartScore", "signal_score", "signalScore", "score")


def _event_filed_after_expr(db: Session):
    explicit_lag = _payload_numeric_expr(
        db,
        "filed_after_days",
        "filedAfterDays",
        "filing_lag_days",
        "filingLagDays",
        "report_lag_days",
        "reportLagDays",
    )
    if db.get_bind().dialect.name == "postgresql":
        payload = cast(Event.payload_json, JSONB)
        trade_date = func.coalesce(
            cast(func.nullif(payload["trade_date"].astext, ""), DateTime(timezone=True)),
            cast(func.nullif(payload["transaction_date"].astext, ""), DateTime(timezone=True)),
            cast(func.nullif(payload["transactionDate"].astext, ""), DateTime(timezone=True)),
            cast(func.nullif(payload[("raw", "transactionDate")].astext, ""), DateTime(timezone=True)),
        )
        filed_date = func.coalesce(
            cast(func.nullif(payload["report_date"].astext, ""), DateTime(timezone=True)),
            cast(func.nullif(payload["reportDate"].astext, ""), DateTime(timezone=True)),
            cast(func.nullif(payload["filing_date"].astext, ""), DateTime(timezone=True)),
            cast(func.nullif(payload["filingDate"].astext, ""), DateTime(timezone=True)),
            cast(func.nullif(payload[("raw", "filingDate")].astext, ""), DateTime(timezone=True)),
        )
        computed_lag = func.extract("epoch", filed_date - trade_date) / 86400.0
        return func.coalesce(explicit_lag, computed_lag)
    if db.get_bind().dialect.name == "sqlite":
        trade_date = func.coalesce(
            func.json_extract(Event.payload_json, "$.trade_date"),
            func.json_extract(Event.payload_json, "$.transaction_date"),
            func.json_extract(Event.payload_json, "$.transactionDate"),
            func.json_extract(Event.payload_json, "$.raw.transactionDate"),
        )
        filed_date = func.coalesce(
            func.json_extract(Event.payload_json, "$.report_date"),
            func.json_extract(Event.payload_json, "$.reportDate"),
            func.json_extract(Event.payload_json, "$.filing_date"),
            func.json_extract(Event.payload_json, "$.filingDate"),
            func.json_extract(Event.payload_json, "$.raw.filingDate"),
        )
        computed_lag = func.julianday(filed_date) - func.julianday(trade_date)
        return func.coalesce(explicit_lag, computed_lag)
    return explicit_lag


def _validate_enum(value: str | None, allowed: set[str], label: str) -> str | None:
    if value is None:
        return None
    normalized = value.strip().lower()
    if normalized not in allowed:
        allowed_list = ", ".join(sorted(allowed))
        raise HTTPException(
            status_code=400,
            detail=f"Invalid {label}. Allowed values: {allowed_list}.",
        )
    return normalized


def _normalize_trade_type(trade_type: str | None) -> str | None:
    if trade_type is None:
        return None
    normalized = trade_type.strip().lower()
    if not normalized:
        return None
    alias_map = {
        "p-purchase": "purchase",
        "s-sale": "sale",
    }
    normalized = alias_map.get(normalized, normalized)

    allowed = {"purchase", "sale", "exchange", "received"}
    if normalized not in allowed:
        raise HTTPException(
            status_code=400,
            detail=(
                "Invalid trade_type. Allowed values: purchase, sale, exchange, received, p-purchase, s-sale."
            ),
        )
    return normalized


def _trade_type_values(trade_type: str) -> list[str]:
    if trade_type == "sale":
        return ["sale", "s-sale"]
    if trade_type == "purchase":
        return ["purchase", "p-purchase"]
    return [trade_type]



def _baseline_avg_subquery(baseline_since: datetime):
    return text(
        """
        SELECT symbol,
               AVG(amount_max) AS median_amount_max,
               COUNT(*) AS baseline_count
        FROM events
        WHERE event_type='congress_trade'
          AND amount_max IS NOT NULL
          AND symbol IS NOT NULL
          AND ts >= :baseline_since
        GROUP BY symbol
        """
    ).bindparams(bindparam("baseline_since", baseline_since)).columns(
        symbol=String,
        median_amount_max=Float,
        baseline_count=Integer,
    ).subquery()


def _congress_baseline_map(
    db: Session,
    events: list[Event],
    *,
    baseline_days: int = DEFAULT_BASELINE_DAYS,
    min_baseline_count: int = DEFAULT_MIN_BASELINE_COUNT,
) -> dict[str, tuple[float, int]]:
    symbols = sorted(
        {
            symbol
            for event in events
            for symbol in [_event_symbol(event, _parse_event_payload(event))]
            if event.event_type == "congress_trade" and event.amount_max is not None and symbol
        }
    )
    if not symbols:
        return {}

    baseline_since = datetime.now(timezone.utc) - timedelta(days=baseline_days)
    baseline_sq = _baseline_avg_subquery(baseline_since)
    baseline_rows = db.execute(
        select(
            baseline_sq.c.symbol,
            baseline_sq.c.median_amount_max,
            baseline_sq.c.baseline_count,
        ).where(baseline_sq.c.symbol.in_(symbols))
    ).all()

    return {
        row.symbol: (float(row.median_amount_max), int(row.baseline_count))
        for row in baseline_rows
        if row.symbol and row.median_amount_max and row.baseline_count >= min_baseline_count
    }



def _actor_net_30d_key(event: Event, payload: dict | None = None) -> str | None:
    if event.event_type == "insider_trade":
        payload = payload if isinstance(payload, dict) else _parse_event_payload(event)
        reporting_cik = _event_reporting_cik(payload)
        if reporting_cik:
            return f"insider:{reporting_cik}"
        name = _insider_display_name(event, payload)
        if name:
            return f"insider_name:{name.strip().casefold()}"
        return None
    if event.member_bioguide_id and event.member_bioguide_id.strip():
        return f"member:{event.member_bioguide_id.strip()}"
    return None


def _member_net_30d_map(db: Session, events: list[Event]) -> dict[str, float]:
    member_ids = sorted(
        {event.member_bioguide_id.strip() for event in events if event.member_bioguide_id and event.member_bioguide_id.strip()}
    )
    insider_ciks = sorted(
        {
            cik
            for event in events
            if event.event_type == "insider_trade"
            for cik in [_event_reporting_cik(_parse_event_payload(event))]
            if cik
        }
    )
    insider_names = sorted(
        {
            name.strip()
            for event in events
            if event.event_type == "insider_trade"
            for name in [_insider_display_name(event, _parse_event_payload(event))]
            if name and name.strip()
        }
    )
    if not member_ids and not insider_names:
        return {}

    cutoff = datetime.now(timezone.utc) - timedelta(days=30)

    net_30d = (
        func.sum(
            case(
                (func.lower(func.trim(func.coalesce(Event.trade_type, ""))).in_(["purchase", "buy"]), Event.amount_max),
                else_=0,
            )
        )
        - func.sum(
            case(
                (func.lower(func.trim(func.coalesce(Event.trade_type, ""))).in_(["sale", "sell"]), Event.amount_max),
                else_=0,
            )
        )
    ).label("net_30d")

    result: dict[str, float] = {}
    if member_ids:
        rows = db.execute(
            select(Event.member_bioguide_id, net_30d)
            .where(Event.ts >= cutoff)
            .where(Event.member_bioguide_id.in_(member_ids))
            .group_by(Event.member_bioguide_id)
        ).all()
        result.update({f"member:{member_id}": float(value or 0) for member_id, value in rows if member_id})

    if insider_names:
        insider_name_keys = [name.lower() for name in insider_names]
        insider_name_expr = func.lower(Event.member_name)
        rows = db.execute(
            select(insider_name_expr.label("insider_name_key"), net_30d)
            .where(Event.event_type == "insider_trade")
            .where(Event.ts >= cutoff)
            .where(insider_name_expr.in_(insider_name_keys))
            .group_by(insider_name_expr)
        ).all()
        result.update(
            {
                f"insider_name:{name_key}": float(value or 0)
                for name_key, value in rows
                if name_key
            }
        )
        for event in events:
            if event.event_type != "insider_trade":
                continue
            payload = _parse_event_payload(event)
            cik = _event_reporting_cik(payload)
            name = _insider_display_name(event, payload)
            if not cik or not name:
                continue
            name_value = result.get(f"insider_name:{name.strip().casefold()}")
            if name_value is not None:
                result[f"insider:{cik}"] = name_value
    elif insider_ciks:
        logger.debug("insider_net_30d_skipped reason=no_indexed_identity cik_count=%s", len(insider_ciks))

    return result


def _symbol_net_30d_map(db: Session, events: list[Event]) -> dict[str, float]:
    symbols = sorted({event.symbol for event in events if event.symbol and event.event_type in {CONGRESS_EQUITY_EVENT_TYPE, "insider_trade"}})
    if not symbols:
        return {}

    cutoff = datetime.now(timezone.utc) - timedelta(days=30)

    buy_amt = func.sum(case((Event.trade_type == "purchase", Event.amount_max), else_=0))
    sell_amt = func.sum(case((Event.trade_type == "sale", Event.amount_max), else_=0))
    net_30d = (func.coalesce(buy_amt, 0) - func.coalesce(sell_amt, 0)).label("net_30d")

    rows = db.execute(
        select(Event.symbol, net_30d)
        .where(Event.event_type.in_([CONGRESS_EQUITY_EVENT_TYPE, "insider_trade"]))
        .where(Event.ts >= cutoff)
        .where(Event.symbol.in_(symbols))
        .where(Event.trade_type.in_(["purchase", "sale"]))
        .group_by(Event.symbol)
    ).all()

    return {symbol: float(net or 0) for symbol, net in rows if symbol}



def _parse_event_payload(event: Event) -> dict:
    if isinstance(event.payload_json, dict):
        return dict(event.payload_json)
    try:
        payload = json.loads(event.payload_json)
        if not isinstance(payload, dict):
            return {}
        return payload
    except Exception:
        return {}


def _congress_symbol_and_trade_date(event: Event, payload: dict) -> tuple[str, str | None]:
    sym = normalize_symbol(event.symbol or payload.get("symbol")) or ""
    trade_date = payload.get("trade_date") or payload.get("transaction_date")
    return sym, trade_date


def _parse_numeric(value) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        parsed = float(value)
        return parsed if parsed == parsed else None
    if isinstance(value, str):
        cleaned = value.replace("$", "").replace(",", "").strip()
        if not cleaned:
            return None
        try:
            return float(cleaned)
        except Exception:
            return None
    return None


def _first_non_empty_text(*values) -> str | None:
    for value in values:
        if isinstance(value, str):
            cleaned = value.strip()
            if cleaned:
                return cleaned
    return None


def _event_source_url(payload: dict) -> str | None:
    raw = payload.get("raw") if isinstance(payload.get("raw"), dict) else {}
    nested = payload.get("payload") if isinstance(payload.get("payload"), dict) else {}
    return _first_non_empty_text(
        payload.get("url"),
        payload.get("source_url"),
        payload.get("sourceUrl"),
        payload.get("filing_url"),
        payload.get("filingUrl"),
        payload.get("report_url"),
        payload.get("reportUrl"),
        payload.get("document_url"),
        payload.get("documentUrl"),
        payload.get("sec_url"),
        payload.get("secUrl"),
        payload.get("finalLink"),
        payload.get("link"),
        nested.get("url"),
        nested.get("source_url"),
        nested.get("sourceUrl"),
        nested.get("filing_url"),
        nested.get("filingUrl"),
        nested.get("report_url"),
        nested.get("reportUrl"),
        nested.get("document_url"),
        nested.get("documentUrl"),
        nested.get("sec_url"),
        nested.get("secUrl"),
        nested.get("finalLink"),
        nested.get("link"),
        raw.get("url"),
        raw.get("source_url"),
        raw.get("sourceUrl"),
        raw.get("filing_url"),
        raw.get("filingUrl"),
        raw.get("report_url"),
        raw.get("reportUrl"),
        raw.get("document_url"),
        raw.get("documentUrl"),
        raw.get("sec_url"),
        raw.get("secUrl"),
        raw.get("finalLink"),
        raw.get("link"),
    )


def _is_bad_event_identity_label(value: object | None) -> bool:
    if not isinstance(value, str):
        return False
    return value.strip().lower() in BAD_EVENT_IDENTITY_LABELS


def _safe_event_identity_text(*values: object) -> str | None:
    for value in values:
        text = _first_non_empty_text(value)
        if text and not _is_bad_event_identity_label(text):
            return text
    return None


def _insider_display_name(event: Event, payload: dict) -> str | None:
    raw = payload.get("raw") if isinstance(payload.get("raw"), dict) else {}
    nested_insider = payload.get("insider") if isinstance(payload.get("insider"), dict) else {}

    return _first_non_empty_text(
        _first_text_field(
            payload,
            "insider_name",
            "insiderName",
            "reporting_name",
            "reportingName",
            "reporting_owner_name",
            "reportingOwnerName",
            "owner_name",
            "ownerName",
        ),
        nested_insider.get("name"),
        raw.get("reportingName"),
        raw.get("reporting_name"),
        raw.get("reportingOwnerName"),
        raw.get("ownerName"),
        raw.get("insiderName"),
        event.member_name,
    )


def _insider_symbol_and_trade_date(event: Event, payload: dict) -> tuple[str, str | None]:
    sym = _event_symbol(event, payload) or ""
    trade_date = payload.get("transaction_date") or payload.get("trade_date")
    return sym, trade_date


def _event_reporting_cik(payload: dict) -> str | None:
    if not isinstance(payload, dict):
        return None
    raw = payload.get("raw") if isinstance(payload.get("raw"), dict) else {}
    return normalize_cik(
        payload.get("reporting_cik")
        or payload.get("reportingCik")
        or raw.get("reportingCik")
        or raw.get("reportingCIK")
        or raw.get("rptOwnerCik")
    )


def _insider_role(payload: dict) -> str | None:
    if not isinstance(payload, dict):
        return None
    raw = payload.get("raw") if isinstance(payload.get("raw"), dict) else {}
    return _first_non_empty_text(
        _first_text_field(payload, "role", "relationship", "title", "typeOfOwner", "officerTitle", "insiderRole", "position"),
        raw.get("typeOfOwner"),
        raw.get("officerTitle"),
        raw.get("insiderRole"),
        raw.get("relationship"),
        raw.get("title"),
        raw.get("position"),
    )


def _normalized_role_needles(role: str) -> list[str]:
    normalized = role.strip().lower()
    if not normalized:
        return []
    alias_map = {
        "ceo": ["ceo", "chief executive officer", "principal executive officer"],
        "cfo": ["cfo", "chief financial officer", "principal financial officer"],
        "coo": ["coo", "chief operating officer"],
        "cto": ["cto", "chief technology officer"],
        "clo": ["clo", "chief legal officer", "general counsel"],
        "cco": ["cco", "chief compliance officer", "chief commercial officer"],
        "cao": ["cao", "chief accounting officer"],
        "director": ["director", "dir"],
        "dir": ["director", "dir"],
        "officer": ["officer", "executive officer"],
        "president": ["president", "pres"],
        "pres": ["president", "pres"],
        "10% owner": ["10% owner", "ten percent owner", "10 percent owner"],
        "10 percent owner": ["10% owner", "ten percent owner", "10 percent owner"],
    }
    return alias_map.get(normalized, [normalized])


def _canonical_role_label(role: str | None) -> str | None:
    cleaned = _clean_suggestion(role)
    if cleaned is None:
        return None
    normalized = cleaned.lower()
    checks = [
        ("CEO", ("ceo", "chief executive officer", "principal executive officer")),
        ("CFO", ("cfo", "chief financial officer", "principal financial officer")),
        ("COO", ("coo", "chief operating officer")),
        ("CTO", ("cto", "chief technology officer")),
        ("CLO", ("clo", "chief legal officer", "general counsel")),
        ("Director", ("director", "dir")),
        ("Officer", ("officer", "executive officer")),
        ("President", ("president", "pres")),
        ("10% Owner", ("10% owner", "ten percent owner", "10 percent owner")),
    ]
    for label, needles in checks:
        if any(needle in normalized for needle in needles):
            return label
    return cleaned


def _insider_role_filter_clause(role: str):
    needles = _normalized_role_needles(role)
    if not needles:
        return None
    payload_lower = func.lower(func.coalesce(Event.payload_json, ""))
    return and_(
        Event.event_type == "insider_trade",
        or_(*[payload_lower.like(f"%{needle}%") for needle in needles]),
    )


def _insider_company_name(event: Event, payload: dict) -> str | None:
    raw = payload.get("raw") if isinstance(payload.get("raw"), dict) else {}
    nested_payload = payload.get("payload") if isinstance(payload.get("payload"), dict) else {}
    symbol = _event_symbol(event, payload)

    def _valid_company_name(*values: object) -> str | None:
        for value in values:
            candidate = safe_company_identity_candidate(_first_non_empty_text(value), symbol)
            if candidate:
                return candidate
        return None

    # Order: enriched payload company fields -> issuer/company fields -> legacy
    # security fields only when they look like a true issuer name.
    return _valid_company_name(
        payload.get("company_name"),
        payload.get("companyName"),
        nested_payload.get("company_name"),
        nested_payload.get("companyName"),
        payload.get("issuer_name"),
        payload.get("issuerName"),
        nested_payload.get("issuer_name"),
        nested_payload.get("issuerName"),
        raw.get("company_name"),
        raw.get("companyName"),
        raw.get("issuer_name"),
        raw.get("issuerName"),
        raw.get("issuer"),
        payload.get("security_name"),
        payload.get("securityName"),
        nested_payload.get("security_name"),
        nested_payload.get("securityName"),
        raw.get("security_name"),
        raw.get("securityName"),
    )


def _insider_security_name(payload: dict) -> str | None:
    return _first_text_field(payload, "security_name", "securityName")


def _insider_event_value_dicts(payload: dict) -> list[dict]:
    if not isinstance(payload, dict):
        return []
    value_dicts: list[dict] = [payload]
    nested_payload = payload.get("payload")
    if isinstance(nested_payload, dict):
        value_dicts.append(nested_payload)
    raw = payload.get("raw")
    if isinstance(raw, dict):
        value_dicts.append(raw)
    return value_dicts


def _first_numeric_field(payload: dict, *keys: str) -> float | None:
    for value_dict in _insider_event_value_dicts(payload):
        for key in keys:
            value = _parse_numeric(value_dict.get(key))
            if value is not None:
                return value
    return None


def _first_text_field(payload: dict, *keys: str) -> str | None:
    for value_dict in _insider_event_value_dicts(payload):
        value = _first_non_empty_text(*[value_dict.get(key) for key in keys])
        if value:
            return value
    return None


def _insider_trade_row(
    event: Event,
    payload: dict,
    outcome: TradeOutcome | None = None,
    fallback_pnl_pct: float | None = None,
    prefer_fallback_pnl: bool = False,
) -> dict:
    raw = payload.get("raw") if isinstance(payload.get("raw"), dict) else {}
    symbol = _event_symbol(event, payload) or normalize_symbol(outcome.symbol if outcome else None)
    company_name = _insider_company_name(event, payload)
    security_name = _insider_security_name(payload)
    if not company_name and outcome is not None:
        company_name = _first_non_empty_text(outcome.symbol)
    transaction_date = _first_text_field(payload, "transaction_date", "transactionDate", "trade_date", "tradeDate")
    if not transaction_date:
        transaction_date = _insider_trade_date(event, payload)
    trade_type = _first_text_field(payload, "trade_type", "tradeType") or event.trade_type
    if not trade_type and outcome is not None:
        trade_type = outcome.trade_type
    normalized_price = normalize_insider_price(symbol=symbol, payload=payload, trade_date=transaction_date)
    price = normalized_price.display_price if normalized_price.is_comparable else None
    if price is None and outcome is not None and outcome.entry_price is not None:
        price = float(outcome.entry_price)
    reported_price = normalized_price.raw_price
    amount_min = _first_numeric_field(payload, "amount_min", "amountMin", "trade_value_min", "tradeValueMin")
    amount_max = _first_numeric_field(payload, "amount_max", "amountMax", "trade_value_max", "tradeValueMax")
    trade_value = _first_numeric_field(
        payload,
        "trade_value",
        "tradeValue",
        "actual_trade_value",
        "actualTradeValue",
        "transactionValue",
        "value",
    )

    if amount_min is None and event.amount_min is not None:
        amount_min = float(event.amount_min)
    if amount_max is None and event.amount_max is not None:
        amount_max = float(event.amount_max)
    if amount_min is None and outcome is not None and outcome.amount_min is not None:
        amount_min = float(outcome.amount_min)
    if amount_max is None and outcome is not None and outcome.amount_max is not None:
        amount_max = float(outcome.amount_max)
    shares = _first_numeric_field(payload, "shares", "transactionShares", "securitiesTransacted")
    if price is not None and shares is not None and shares > 0:
        trade_value = price * shares
    if trade_value is None:
        trade_value = amount_max if amount_max is not None else amount_min

    display_metrics = trade_outcome_display_metrics(outcome)
    payload_pnl_pct = _first_numeric_field(payload, "pnl_pct", "pnlPct", "pnl", "return_pct", "returnPct")
    if payload_pnl_pct is not None:
        pnl_pct = payload_pnl_pct
        pnl_source = "persisted_payload"
    elif display_metrics.return_pct is not None:
        pnl_pct = display_metrics.return_pct
        pnl_source = display_metrics.pnl_source
    elif prefer_fallback_pnl and fallback_pnl_pct is not None:
        pnl_pct = fallback_pnl_pct
        pnl_source = "normalized_filing"
    else:
        pnl_pct = None
        pnl_source = None
    if pnl_pct is None:
        if payload_pnl_pct is not None:
            pnl_pct = payload_pnl_pct
            pnl_source = "persisted_payload"
        elif display_metrics.return_pct is not None:
            pnl_pct = display_metrics.return_pct
            pnl_source = display_metrics.pnl_source
        elif prefer_fallback_pnl and fallback_pnl_pct is not None:
            pnl_pct = fallback_pnl_pct
            pnl_source = "normalized_filing"

    smart_score = _first_numeric_field(payload, "smart_score", "smartScore")
    smart_band = _first_text_field(payload, "smart_band", "smartBand")
    if smart_score is None or smart_band is None:
        try:
            unusual_multiple = _first_numeric_field(payload, "unusual_multiple", "unusualMultiple") or 1.0
        except Exception:
            unusual_multiple = 1.0
        calc_score, calc_band = calculate_smart_score(
            unusual_multiple=unusual_multiple,
            amount_max=event.amount_max,
            ts=event.ts,
        )
        smart_score = smart_score if smart_score is not None else calc_score
        smart_band = smart_band or calc_band
    if pnl_pct is None:
        smart_score = None
        smart_band = None

    return {
        "event_id": event.id,
        "symbol": symbol,
        "company_name": company_name,
        "companyName": company_name,
        "security_name": security_name,
        "securityName": security_name,
        "transaction_date": transaction_date,
        "trade_date": transaction_date,
        "filing_date": payload.get("filing_date") or raw.get("filingDate") or event.ts.isoformat(),
        "trade_type": trade_type,
        "tradeType": trade_type,
        "amount_min": amount_min,
        "amount_max": amount_max,
        "trade_value": trade_value,
        "tradeValue": trade_value,
        "shares": shares,
        "price": price,
        "display_price": price,
        "displayPrice": price,
        "display_price_currency": normalized_price.display_currency,
        "displayPriceCurrency": normalized_price.display_currency,
        "display_share_basis": normalized_price.display_share_basis,
        "displayShareBasis": normalized_price.display_share_basis,
        "reported_price": reported_price,
        "reportedPrice": reported_price,
        "reported_price_currency": normalized_price.raw_currency,
        "reportedPriceCurrency": normalized_price.raw_currency,
        "reported_share_basis": normalized_price.raw_share_basis,
        "reportedShareBasis": normalized_price.raw_share_basis,
        "price_normalization": normalization_payload(normalized_price),
        "priceNormalization": normalization_payload(normalized_price),
        "insider_name": _insider_display_name(event, payload),
        "reporting_cik": _event_reporting_cik(payload),
        "role": _insider_role(payload),
        "external_id": _first_non_empty_text(payload.get("external_id"), raw.get("id"), raw.get("transactionId")),
        "url": _first_non_empty_text(payload.get("url"), payload.get("document_url"), raw.get("url"), raw.get("filingUrl")),
        "pnl_pct": pnl_pct,
        "pnlPct": pnl_pct,
        "pnl": pnl_pct,
        "return_pct": pnl_pct,
        "returnPct": pnl_pct,
        "alpha_pct": display_metrics.alpha_pct,
        "alphaPct": display_metrics.alpha_pct,
        "benchmark_return_pct": display_metrics.benchmark_return_pct,
        "benchmarkReturnPct": display_metrics.benchmark_return_pct,
        "holding_period_days": display_metrics.holding_period_days,
        "holdingPeriodDays": display_metrics.holding_period_days,
        "outcome_horizon": display_metrics.outcome_horizon,
        "outcomeHorizon": display_metrics.outcome_horizon,
        "return_label": display_metrics.outcome_horizon,
        "returnLabel": display_metrics.outcome_horizon,
        "pnl_source": pnl_source,
        "pnlSource": pnl_source,
        "smart_score": smart_score,
        "smartScore": smart_score,
        "smart_band": smart_band,
        "smartBand": smart_band,
    }




def _to_trade_outcome_member_series(
    row: TradeOutcome,
    cumulative_return: float,
    cumulative_alpha: float,
    running_benchmark_return_pct: float | None = None,
) -> dict:
    trade_date = row.trade_date.isoformat() if row.trade_date else None
    return {
        "event_id": row.event_id,
        "symbol": row.symbol,
        "trade_type": row.trade_type,
        "asof_date": trade_date,
        "return_pct": row.return_pct,
        "alpha_pct": row.alpha_pct,
        "benchmark_return_pct": row.benchmark_return_pct,
        "holding_days": row.holding_days,
        "cumulative_return_pct": cumulative_return,
        "running_benchmark_return_pct": running_benchmark_return_pct,
        "cumulative_alpha_pct": cumulative_alpha,
    }


def _to_trade_outcome_trade_view(row: TradeOutcome) -> dict:
    return {
        "event_id": row.event_id,
        "symbol": row.symbol or "—",
        "trade_type": row.trade_type,
        "asof_date": row.trade_date.isoformat() if row.trade_date else None,
        "return_pct": row.return_pct,
        "alpha_pct": row.alpha_pct,
        "holding_days": row.holding_days,
    }

def _insider_filing_date(event: Event, payload: dict) -> str:
    raw = payload.get("raw") if isinstance(payload.get("raw"), dict) else {}
    return (
        _first_non_empty_text(
            payload.get("filing_date"),
            payload.get("filingDate"),
            raw.get("filingDate"),
            raw.get("acceptedDate"),
        )
        or event.ts.isoformat()
    )


def _validated_lookback_days(lookback_days: int) -> int:
    if lookback_days not in ALLOWED_LOOKBACK_DAYS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid lookback_days. Allowed values: {ALLOWED_LOOKBACK_DAYS_LABEL}.",
        )
    return lookback_days




def _insider_reporting_cik_prefilter_clause(normalized_cik: str):
    variants = {normalized_cik}
    stripped = normalized_cik.lstrip("0")
    if stripped:
        variants.add(stripped)

    patterns: list[str] = []
    for cik in variants:
        patterns.extend([
            f'"reporting_cik":"{cik}"',
            f'"reporting_cik": "{cik}"',
            f'"reportingCik":"{cik}"',
            f'"reportingCik": "{cik}"',
            f'"reportingCIK":"{cik}"',
            f'"reportingCIK": "{cik}"',
            f'"rptOwnerCik":"{cik}"',
            f'"rptOwnerCik": "{cik}"',
        ])

    return or_(*[Event.payload_json.contains(pattern) for pattern in patterns])


def _load_insider_events_for_cik(
    db: Session,
    reporting_cik: str,
    lookback_days: int,
    *,
    include_non_market_activity: bool = False,
    issuer: str | None = None,
) -> list[tuple[Event, dict]]:
    lookback = _validated_lookback_days(lookback_days)
    normalized_cik = normalize_cik(reporting_cik)
    if not normalized_cik:
        raise HTTPException(status_code=400, detail="Invalid reporting_cik.")

    since = datetime.now(timezone.utc) - timedelta(days=lookback)
    query = (
        select(Event)
        .where(Event.event_type == "insider_trade")
        .where(Event.ts >= since)
        .where(_insider_reporting_cik_prefilter_clause(normalized_cik))
        .order_by(func.coalesce(Event.event_date, Event.ts).desc(), Event.id.desc())
    )
    if not include_non_market_activity:
        query = query.where(insider_visibility_clause())

    rows = db.execute(query).scalars().all()

    matched: list[tuple[Event, dict]] = []
    issuer_symbol = normalize_symbol(issuer)
    issuer_cik = normalize_cik(issuer)
    for event in rows:
        payload = _parse_event_payload(event)
        if _event_reporting_cik(payload) != normalized_cik:
            continue
        if issuer_symbol and _event_symbol(event, payload) != issuer_symbol:
            continue
        if issuer_cik and _event_cik(payload) != issuer_cik:
            continue
        trade_type = (event.trade_type or "").strip().lower()
        if not include_non_market_activity and trade_type not in VISIBLE_INSIDER_TRADE_TYPES:
            continue
        matched.append((event, payload))

    return matched


def _insider_trade_date(event: Event, payload: dict) -> str | None:
    value = _first_text_field(payload, "transaction_date", "transactionDate", "trade_date", "tradeDate")
    if not value:
        fallback_dt = event.event_date or event.ts
        if fallback_dt is not None:
            value = fallback_dt.date().isoformat()
    return value[:10] if value else None


def _load_insider_trade_outcomes(
    db: Session,
    matched: list[tuple[Event, dict]],
    normalized_cik: str,
    benchmark_symbol: str,
    lookback_days: int,
) -> tuple[dict[int, TradeOutcome], list[TradeOutcome]]:
    if not matched:
        return {}, []

    event_ids = [event.id for event, _ in matched]
    direct = db.execute(
        select(TradeOutcome)
        .where(TradeOutcome.event_id.in_(event_ids))
        .where(TradeOutcome.scoring_status == "ok")
        .where(TradeOutcome.benchmark_symbol == benchmark_symbol)
        .where(TradeOutcome.methodology_version == INSIDER_METHODOLOGY_VERSION)
        .where(TradeOutcome.trade_date.is_not(None))
    ).scalars().all()
    by_event_id: dict[int, TradeOutcome] = {row.event_id: row for row in direct}

    unmatched = [(event, payload) for event, payload in matched if event.id not in by_event_id]
    if not unmatched:
        ordered = sorted(by_event_id.values(), key=lambda row: (row.trade_date, row.event_id))
        return by_event_id, ordered

    cutoff = (datetime.now(timezone.utc) - timedelta(days=lookback_days)).date()
    fallback_query = (
        select(TradeOutcome)
        .where(TradeOutcome.scoring_status == "ok")
        .where(TradeOutcome.benchmark_symbol == benchmark_symbol)
        .where(TradeOutcome.methodology_version == INSIDER_METHODOLOGY_VERSION)
        .where(TradeOutcome.trade_date.is_not(None))
        .where(TradeOutcome.trade_date >= cutoff)
    )
    cik_variants: set[str] = {normalized_cik}
    stripped = normalized_cik.lstrip("0")
    if stripped:
        cik_variants.add(stripped)

    fallback = db.execute(
        fallback_query
        .where(TradeOutcome.member_id.in_(sorted(cik_variants)))
        .order_by(TradeOutcome.trade_date.asc(), TradeOutcome.event_id.asc())
    ).scalars().all()

    fallback_by_logical_key: dict[tuple[str | None, str | None, str | None, int | None, int | None], TradeOutcome] = {}
    for row in fallback:
        logical_key = trade_outcome_logical_key(
            symbol=row.symbol,
            trade_side=row.trade_type,
            trade_date=row.trade_date,
            amount_min=row.amount_min,
            amount_max=row.amount_max,
        )
        if logical_key[0] and logical_key[2]:
            fallback_by_logical_key.setdefault(logical_key, row)

    for event, payload in unmatched:
        sym = _event_symbol(event, payload)
        trade_date = _insider_trade_date(event, payload)
        side = event.trade_type or _first_text_field(payload, "trade_type", "tradeType")
        amount_min = _first_numeric_field(payload, "amount_min", "amountMin", "trade_value_min", "tradeValueMin")
        amount_max = _first_numeric_field(payload, "amount_max", "amountMax", "trade_value_max", "tradeValueMax")
        if amount_min is None and event.amount_min is not None:
            amount_min = float(event.amount_min)
        if amount_max is None and event.amount_max is not None:
            amount_max = float(event.amount_max)

        logical_key = trade_outcome_logical_key(
            symbol=sym,
            trade_side=side,
            trade_date=trade_date,
            amount_min=amount_min,
            amount_max=amount_max,
        )
        row = fallback_by_logical_key.get(logical_key)
        if row:
            by_event_id[event.id] = row

    ordered = sorted({row.id: row for row in by_event_id.values()}.values(), key=lambda row: (row.trade_date, row.event_id))
    return by_event_id, ordered


def _transient_insider_trade_outcomes(
    db: Session,
    matched: list[tuple[Event, dict]],
    outcome_by_event_id: dict[int, TradeOutcome],
    *,
    benchmark_symbol: str,
    benchmark_close_map: dict[str, float],
    benchmark_dates: list[str],
) -> list[SimpleNamespace]:
    missing = [(event, payload) for event, payload in matched if event.id not in outcome_by_event_id]
    if not missing:
        return []

    symbols = sorted(
        {
            symbol
            for event, payload in missing
            for symbol in [_event_symbol(event, payload)]
            if symbol
        }
    )
    current_quote_meta = (
        get_current_prices_meta_db(
            db,
            symbols,
            allow_cache_write=False,
            release_connection_before_fetch=True,
        )
        if symbols
        else {}
    )
    quote_prices = {
        symbol: float(meta["price"])
        for symbol, meta in current_quote_meta.items()
        if isinstance(meta, dict) and meta.get("price") is not None
    }
    price_memo: dict[tuple[str, str], float | None] = {}
    today = datetime.now(timezone.utc).date()
    benchmark_current = (
        get_close_for_date_or_prior(today.isoformat(), benchmark_close_map, benchmark_dates)
        if benchmark_dates
        else None
    )

    rows: list[SimpleNamespace] = []
    for event, payload in missing:
        symbol = _event_symbol(event, payload)
        trade_date_text = _insider_trade_date(event, payload)
        current_price = quote_prices.get(symbol or "")
        entry_price, _ = _insider_entry_price(event, payload, db, price_memo)
        fallback_pnl_pct = None
        if current_price is not None and entry_price is not None and entry_price > 0:
            fallback_pnl_pct = signed_return_pct(
                current_price,
                entry_price,
                event.trade_type or payload.get("trade_type"),
            )

        display_row = _insider_trade_row(
            event,
            payload,
            outcome=None,
            fallback_pnl_pct=fallback_pnl_pct,
            prefer_fallback_pnl=fallback_pnl_pct is not None,
        )
        return_pct = display_row.get("pnl_pct")
        if return_pct is None or not trade_date_text:
            continue

        try:
            trade_day = date.fromisoformat(trade_date_text[:10])
        except Exception:
            continue

        benchmark_return_pct = None
        alpha_pct = None
        benchmark_entry = (
            get_close_for_date_or_prior(trade_day.isoformat(), benchmark_close_map, benchmark_dates)
            if benchmark_dates
            else None
        )
        if benchmark_entry is not None and benchmark_entry > 0 and benchmark_current is not None:
            benchmark_return_pct = float(((benchmark_current - benchmark_entry) / benchmark_entry) * 100)
            alpha_pct = float(return_pct - benchmark_return_pct)

        rows.append(
            SimpleNamespace(
                id=-(event.id or 0),
                event_id=event.id,
                member_id=display_row.get("reporting_cik"),
                member_name=display_row.get("insider_name"),
                symbol=display_row.get("symbol"),
                trade_type=display_row.get("trade_type"),
                source=event.source,
                trade_date=trade_day,
                entry_price=entry_price,
                current_price=current_price,
                benchmark_symbol=benchmark_symbol,
                benchmark_return_pct=benchmark_return_pct,
                return_pct=float(return_pct),
                alpha_pct=alpha_pct,
                holding_days=max((today - trade_day).days, 0),
                amount_min=display_row.get("amount_min"),
                amount_max=display_row.get("amount_max"),
                scoring_status="transient",
                methodology_version=INSIDER_METHODOLOGY_VERSION,
            )
        )

    return rows




def _event_symbol(event: Event, payload: dict) -> str | None:
    if getattr(event, "event_type", None) in CONGRESS_NON_EQUITY_EVENT_TYPES:
        return None
    raw_payload = payload.get("raw") if isinstance(payload, dict) else None
    payload_symbol = payload.get("symbol") if isinstance(payload, dict) else None
    payload_ticker = payload.get("ticker") if isinstance(payload, dict) else None
    raw_symbol = raw_payload.get("symbol") if isinstance(raw_payload, dict) else None
    for candidate in (event.symbol, payload_symbol, payload_ticker, raw_symbol):
        if _is_bad_event_identity_label(candidate):
            continue
        symbol = normalize_symbol(candidate)
        if symbol and not _is_bad_event_identity_label(symbol):
            return symbol
    return None


def _is_government_contract_action_payload(payload: dict) -> bool:
    return (
        isinstance(payload, dict)
        and (
            payload.get("event_subtype") == "funding_action"
            or payload.get("modification_number") is not None
            or payload.get("action_date") is not None
        )
    )


def _government_contract_award_id(payload: dict) -> str | None:
    if not isinstance(payload, dict):
        return None
    raw = payload.get("raw") if isinstance(payload.get("raw"), dict) else {}
    return _first_non_empty_text(
        payload.get("award_id"),
        payload.get("parent_award_id"),
        payload.get("awardId"),
        raw.get("award_id"),
        raw.get("awardId"),
        raw.get("generated_internal_id"),
    )


def _filter_shadowed_government_contract_awards(db: Session, rows: list[Event]) -> list[Event]:
    award_ids = sorted(
        {
            award_id
            for event in rows
            if event.event_type == "government_contract"
            for payload in [_parse_event_payload(event)]
            if not _is_government_contract_action_payload(payload)
            for award_id in [_government_contract_award_id(payload)]
            if award_id
        }
    )
    if not award_ids:
        return rows
    action_parent_ids = set(
        db.execute(
            select(GovernmentContractAction.parent_award_id)
            .where(GovernmentContractAction.parent_award_id.in_(award_ids))
            .distinct()
        ).scalars().all()
    )
    if not action_parent_ids:
        return rows
    return [
        event
        for event in rows
        if not (
            event.event_type == "government_contract"
            and not _is_government_contract_action_payload(_parse_event_payload(event))
            and _government_contract_award_id(_parse_event_payload(event)) in action_parent_ids
        )
    ]


def _government_contract_action_event_id_select():
    return (
        select(GovernmentContractAction.event_id)
        .where(GovernmentContractAction.event_id.is_not(None))
    )


def _government_contract_action_events_only_clause():
    action_event_ids = _government_contract_action_event_id_select()
    return or_(
        Event.event_type.notin_(GOVERNMENT_CONTRACT_EVENT_TYPES),
        Event.id.in_(action_event_ids),
    )


def _event_cik(payload: dict) -> str | None:
    raw_payload = payload.get("raw") if isinstance(payload, dict) else None
    raw_cik = raw_payload.get("companyCik") if isinstance(raw_payload, dict) else None
    if not raw_cik and isinstance(raw_payload, dict):
        raw_cik = raw_payload.get("companyCIK")
    if not raw_cik and isinstance(payload, dict):
        raw_cik = payload.get("companyCik")
    return normalize_cik(raw_cik)


def _should_replace_company_name(existing: str | None, symbol: str | None) -> bool:
    return safe_company_identity_candidate(existing, symbol) is None


def _enrich_payload_company_name(
    event: Event,
    payload: dict,
    ticker_meta: dict[str, dict[str, str | None]],
    cik_names: dict[str, str | None],
) -> dict:
    symbol = _event_symbol(event, payload)
    company_name = None
    meta_name = None
    cik_name = None

    if symbol:
        meta = ticker_meta.get(symbol)
        meta_name = (meta or {}).get("company_name") if meta else None

    if event.event_type == "insider_trade":
        cik = _event_cik(payload)
        if cik:
            cik_name = cik_names.get(cik)

    if event.event_type == "insider_trade":
        company_name = meta_name or cik_name
    else:
        company_name = meta_name

    if not company_name:
        return payload

    if event.event_type != "insider_trade":
        if _should_replace_company_name(payload.get("company_name"), symbol):
            payload["company_name"] = company_name
            payload["companyName"] = company_name
        if _should_replace_company_name(payload.get("issuer_name"), symbol):
            payload["issuer_name"] = company_name
            payload["issuerName"] = company_name
        if _should_replace_company_name(payload.get("security_name"), symbol):
            payload["security_name"] = company_name
            payload["securityName"] = company_name
        if _should_replace_company_name(payload.get("headline"), symbol):
            payload["headline"] = company_name
        return payload

    payload["company_name"] = company_name
    if symbol:
        payload["symbol"] = symbol
    raw = payload.get("raw")
    if not isinstance(raw, dict):
        raw = {}
        payload["raw"] = raw
    if symbol:
        raw["symbol"] = symbol
    raw["companyName"] = company_name

    return payload


def _ensure_insider_payload_company_fields(event: Event, payload: dict) -> dict:
    if event.event_type != "insider_trade" or not isinstance(payload, dict):
        return payload
    company_name = _insider_company_name(event, payload)
    if not company_name:
        return payload
    if not _first_non_empty_text(payload.get("company_name")):
        payload["company_name"] = company_name
    raw = payload.get("raw")
    if not isinstance(raw, dict):
        raw = {}
        payload["raw"] = raw
    if not _first_non_empty_text(raw.get("companyName")):
        raw["companyName"] = company_name
    return payload


def _normalize_congress_payload_identity(event: Event, payload: dict) -> dict:
    if event.event_type not in CONGRESS_DISCLOSURE_EVENT_TYPES or not isinstance(payload, dict):
        return payload

    if event.event_type == CONGRESS_EQUITY_EVENT_TYPE:
        symbol = _event_symbol(event, payload)
        payload["symbol"] = symbol
        payload["ticker"] = symbol
        label = _safe_event_identity_text(
            payload.get("company_name"),
            payload.get("companyName"),
            payload.get("issuer_name"),
            payload.get("issuerName"),
            payload.get("security_name"),
            payload.get("securityName"),
            payload.get("security_description"),
            payload.get("securityDescription"),
            payload.get("description"),
            payload.get("headline"),
            payload.get("summary"),
        )
        if label:
            if _should_replace_company_name(payload.get("company_name"), symbol):
                payload["company_name"] = label
                payload["companyName"] = label
            if _should_replace_company_name(payload.get("issuer_name"), symbol):
                payload["issuer_name"] = label
                payload["issuerName"] = label
            if _is_bad_event_identity_label(payload.get("security_name")) or not _first_non_empty_text(payload.get("security_name")):
                payload["security_name"] = label
                payload["securityName"] = label
        else:
            payload["security_name"] = "Unresolved security"
            payload["securityName"] = "Unresolved security"
        if _is_bad_event_identity_label(payload.get("headline")):
            payload.pop("headline", None)
        asset_class = canonical_asset_class_value(
            event_type=event.event_type,
            asset_class=_first_non_empty_text(payload.get("asset_class"), payload.get("assetClass")),
            instrument_type=_first_non_empty_text(payload.get("instrument_type"), payload.get("instrumentType")),
            symbol=symbol,
            security_description=_first_non_empty_text(
                payload.get("security_description"),
                payload.get("securityDescription"),
                payload.get("description"),
                payload.get("security_name"),
                payload.get("securityName"),
            ),
            company_name=_first_non_empty_text(payload.get("company_name"), payload.get("companyName")),
        )
        payload["asset_class"] = asset_class
        payload["assetClass"] = asset_class
        if not _first_non_empty_text(payload.get("instrument_type")):
            payload["instrument_type"] = "fund" if asset_class == "etf_fund" else "equity"
            payload["instrumentType"] = payload["instrument_type"]
        return payload

    label = _safe_event_identity_text(
        payload.get("issuer_name"),
        payload.get("issuerName"),
        payload.get("security_description"),
        payload.get("securityDescription"),
        payload.get("description"),
        payload.get("security_name"),
        payload.get("securityName"),
    )
    if label and (_is_bad_event_identity_label(payload.get("security_name")) or not _first_non_empty_text(payload.get("security_name"))):
        payload["security_name"] = label
        payload["securityName"] = label
    payload["asset_class"] = canonical_asset_class_value(
        event_type=event.event_type,
        asset_class=_first_non_empty_text(payload.get("asset_class"), payload.get("assetClass")),
        instrument_type=_first_non_empty_text(payload.get("instrument_type"), payload.get("instrumentType")),
        symbol=None,
        security_description=label,
        company_name=_first_non_empty_text(payload.get("issuer_name"), payload.get("issuerName")),
    )
    payload["assetClass"] = payload["asset_class"]
    return payload


def _ticker_meta_with_security_names(
    db: Session,
    symbols: list[str],
    *,
    enqueue_refresh: bool = True,
) -> dict[str, dict[str, str | None]]:
    normalized_symbols = sorted({symbol for raw in symbols for symbol in [normalize_symbol(raw)] if symbol})
    if not normalized_symbols:
        return {}

    ticker_meta = get_ticker_meta(
        db,
        normalized_symbols,
        allow_refresh=False,
        enqueue_refresh=enqueue_refresh,
    )
    security_rows = db.execute(
        select(Security.symbol, Security.name)
        .where(Security.symbol.in_(normalized_symbols))
    ).all()

    for symbol, name in security_rows:
        normalized_symbol = normalize_symbol(symbol)
        company_name = safe_company_identity_candidate(name, normalized_symbol)
        if not normalized_symbol or not company_name or company_name.upper() == normalized_symbol.upper():
            continue
        row = ticker_meta.setdefault(normalized_symbol, {"company_name": None, "exchange": None})
        row["company_name"] = company_name

    return ticker_meta


def _insider_entry_price(
    event: Event,
    payload: dict,
    db: Session,
    price_memo: dict[tuple[str, str], float | None],
) -> tuple[float | None, str]:
    sym, trade_date = _insider_symbol_and_trade_date(event, payload)
    normalized = normalize_insider_price(symbol=sym, payload=payload, trade_date=trade_date)
    if normalized.is_comparable:
        return normalized.display_price, "normalized_filing" if normalized.status == "normalized" else "filing"
    if normalized.ordinary_shares_per_adr is not None:
        return None, "normalization_unavailable"

    if sym and trade_date:
        key = (sym, trade_date)
        if key not in price_memo:
            price_memo[key] = get_eod_close(db, sym, trade_date)
        fallback_price = price_memo[key]
        if fallback_price is not None and fallback_price > 0:
            return fallback_price, "eod"

    return None, "none"


def _safe_outcome_status(status: str | None) -> str | None:
    if not status:
        return None
    if status in FEED_OUTCOME_RETRY_STATUSES or status.startswith("provider_"):
        return None
    if status == "ok":
        return "ok"
    return status


def _outcome_needs_feed_pnl_refresh(outcome: TradeOutcome | None) -> bool:
    if outcome is None:
        return True
    if outcome.return_pct is not None:
        return False
    status = outcome.scoring_status or ""
    return status in FEED_OUTCOME_RETRY_STATUSES or status.startswith("provider_")


def _load_trade_outcomes_for_events(db: Session, event_ids: list[int]) -> dict[int, TradeOutcome]:
    if not event_ids:
        return {}
    try:
        rows = db.execute(select(TradeOutcome).where(TradeOutcome.event_id.in_(event_ids))).scalars().all()
    except OperationalError:
        logger.warning("trade_outcomes table unavailable while serializing events", exc_info=True)
        return {}
    return {row.event_id: row for row in rows}


def _enqueue_missing_trade_outcomes(
    db: Session,
    paged_rows: list[Event],
    outcome_by_event_id: dict[int, TradeOutcome],
) -> None:
    missing = [
        event
        for event in paged_rows
        if event.event_type in {"congress_trade", "insider_trade"}
        and _outcome_needs_feed_pnl_refresh(outcome_by_event_id.get(event.id))
    ]
    if not missing:
        return

    try:
        enqueue_feed_pnl_enrichment_for_events(
            db,
            missing[:FEED_OUTCOME_ENQUEUE_LIMIT],
            source="feed_load",
            reason="missing_trade_outcome",
            priority=FEED_PNL_PRIORITY_BASE,
            use_current_session=True,
        )
        db.commit()
    except Exception:
        db.rollback()
        logger.exception("feed_pnl_batch_enqueue_failed endpoint=/api/events events=%s", len(missing))


def _event_payload(
    event: Event,
    db: Session,
    price_memo: dict[tuple[str, str], float | None],
    current_price_memo: dict[str, float],
    current_quote_meta: dict[str, dict],
    member_net_30d_map: dict[str, float],
    symbol_net_30d_map: dict[str, float],
    confirmation_metrics_map: dict[str, ConfirmationMetrics],
    ticker_meta: dict[str, dict[str, str | None]],
    cik_names: dict[str, str | None],
    baseline_map: dict[str, tuple[float, int]],
    enrich_prices: bool = True,
    outcome: TradeOutcome | None = None,
) -> EventOut:
    payload = _ensure_insider_payload_company_fields(
        event,
        _enrich_payload_company_name(event, _parse_event_payload(event), ticker_meta, cik_names),
    )
    payload = _normalize_congress_payload_identity(event, payload)
    sym_norm = _event_symbol(event, payload)

    baseline_median_amount_max: float | None = None
    baseline_count: int | None = None
    unusual_multiple: float | None = None
    if event.event_type == "congress_trade":
        baseline_stats = baseline_map.get(sym_norm or "")
        if baseline_stats:
            baseline_median_amount_max, baseline_count = baseline_stats
            if event.amount_max is not None and baseline_median_amount_max > 0:
                unusual_multiple = float(event.amount_max) / baseline_median_amount_max
    else:
        try:
            unusual_multiple = float(payload.get("unusual_multiple") or 1.0)
        except Exception:
            unusual_multiple = 1.0

    confirmation_summary = (
        confirmation_metrics_map.get(sym_norm or "").as_dict()
        if sym_norm and sym_norm in confirmation_metrics_map
        else None
    )

    if event.event_type in CONGRESS_NON_EQUITY_EVENT_TYPES:
        smart_score, smart_band = 0, "inactive"
        unusual_multiple = None
        confirmation_summary = None
    else:
        payload_smart_score = _first_numeric_field(payload, "smart_score", "smartScore", "signal_score", "signalScore", "score")
        payload_smart_band = _first_text_field(payload, "smart_band", "smartBand", "signal_band", "signalBand", "band")
        if payload_smart_score is not None:
            smart_score = int(round(payload_smart_score))
            smart_band = payload_smart_band or "active"
        else:
            smart_score, smart_band = calculate_smart_score(
                unusual_multiple=unusual_multiple or 1.0,
                amount_max=event.amount_max,
                ts=event.ts,
                confirmation_30d=confirmation_summary,
            )

    estimated_price = None
    current_price = None
    display_amount_min = event.amount_min
    display_amount_max = event.amount_max
    pnl_pct = None
    pnl_source = "none"
    outcome_status = None
    outcome_skip_reason = None
    alpha_pct = None
    benchmark_return_pct = None
    holding_period_days = None
    outcome_horizon = None
    quote_asof_ts = None
    quote_is_stale = None
    if outcome is not None and event.event_type in {"congress_trade", "insider_trade"}:
        display_metrics = trade_outcome_display_metrics(outcome)
        estimated_price = display_metrics.trade_price
        current_price = display_metrics.current_or_horizon_price
        pnl_pct = display_metrics.return_pct
        alpha_pct = display_metrics.alpha_pct
        benchmark_return_pct = display_metrics.benchmark_return_pct
        holding_period_days = display_metrics.holding_period_days
        outcome_horizon = display_metrics.outcome_horizon
        pnl_source = display_metrics.pnl_source or ("trade_outcome" if pnl_pct is not None else "none")
        outcome_status = _safe_outcome_status(outcome.scoring_status)
        if pnl_pct is None:
            outcome_skip_reason = outcome_status

    if enrich_prices and event.event_type == "congress_trade":
        sym, trade_date = _congress_symbol_and_trade_date(event, payload)
        if outcome is not None:
            display_metrics = trade_outcome_display_metrics(outcome)
            estimated_price = float(outcome.entry_price) if outcome.entry_price is not None else None
            current_price = float(outcome.current_price) if outcome.current_price is not None else None
            pnl_pct = display_metrics.return_pct
            alpha_pct = display_metrics.alpha_pct
            benchmark_return_pct = display_metrics.benchmark_return_pct
            holding_period_days = display_metrics.holding_period_days
            outcome_horizon = display_metrics.outcome_horizon
            pnl_source = "eod" if pnl_pct is not None and estimated_price is not None else display_metrics.pnl_source or "trade_outcome"
            outcome_status = _safe_outcome_status(outcome.scoring_status)
            if pnl_pct is None:
                outcome_skip_reason = outcome_status
        else:
            eligibility = congress_equity_outcome_eligibility(
                event_type=event.event_type,
                symbol=sym,
                payload=payload,
                trade_date=trade_date,
                side=event.trade_type or event.transaction_type,
                amount_min=event.amount_min,
                amount_max=event.amount_max,
            )
            if not eligibility.eligible:
                outcome_skip_reason = eligibility.skip_reason

        q = current_quote_meta.get(sym)
        if q:
            quote_asof_ts = q.get("asof_ts")
            quote_is_stale = q.get("is_stale")
    elif enrich_prices and event.event_type == "insider_trade":
        sym, trade_date = _insider_symbol_and_trade_date(event, payload)
        normalized = normalize_insider_price(symbol=sym, payload=payload, trade_date=trade_date)
        payload["reported_price"] = normalized.raw_price
        payload["reported_price_currency"] = normalized.raw_currency
        payload["reported_share_basis"] = normalized.raw_share_basis
        payload["display_price"] = normalized.display_price if normalized.is_comparable else None
        payload["display_price_currency"] = normalized.display_currency
        payload["display_share_basis"] = normalized.display_share_basis
        payload["price_normalization"] = normalization_payload(normalized)
        display_metrics = trade_outcome_display_metrics(outcome)
        estimated_price = display_metrics.trade_price
        if estimated_price is None and normalized.is_comparable:
            estimated_price = normalized.display_price
        if display_metrics.trade_price is not None:
            estimated_price = display_metrics.trade_price
        shares = _first_numeric_field(payload, "shares", "transactionShares", "securitiesTransacted")
        if estimated_price is not None and shares is not None and shares > 0:
            display_value = int(round(estimated_price * shares))
            display_amount_min = display_value
            display_amount_max = display_value
            payload["display_trade_value"] = display_value
            payload["displayTradeValue"] = display_value
        pnl_source = display_metrics.pnl_source or (
            "normalized_filing" if normalized.status == "normalized" and estimated_price is not None else "none"
        )
        q = current_quote_meta.get(sym)
        if q:
            quote_asof_ts = q.get("asof_ts")
            quote_is_stale = q.get("is_stale")
        current_price = display_metrics.current_or_horizon_price
        if display_metrics.return_pct is not None:
            pnl_pct = display_metrics.return_pct
            alpha_pct = display_metrics.alpha_pct
            benchmark_return_pct = display_metrics.benchmark_return_pct
            holding_period_days = display_metrics.holding_period_days
            outcome_horizon = display_metrics.outcome_horizon
            pnl_source = display_metrics.pnl_source or "trade_outcome"
            payload["alpha_pct"] = display_metrics.alpha_pct
            payload["alphaPct"] = display_metrics.alpha_pct
            payload["benchmark_return_pct"] = display_metrics.benchmark_return_pct
            payload["benchmarkReturnPct"] = display_metrics.benchmark_return_pct
            payload["holding_period_days"] = display_metrics.holding_period_days
            payload["holdingPeriodDays"] = display_metrics.holding_period_days
            payload["outcome_horizon"] = display_metrics.outcome_horizon
            payload["outcomeHorizon"] = display_metrics.outcome_horizon

    resolved_member_name = event.member_name
    if event.event_type == "insider_trade":
        resolved_member_name = _insider_display_name(event, payload)
        if resolved_member_name and not _first_non_empty_text(payload.get("insider_name")):
            payload["insider_name"] = resolved_member_name

    return EventOut(
        id=event.id,
        event_type=event.event_type,
        ts=event.ts,
        symbol=sym_norm,
        source=event.source,
        member_name=resolved_member_name,
        member_bioguide_id=event.member_bioguide_id,
        party=event.party,
        chamber=event.chamber,
        trade_type=event.trade_type,
        url=_event_source_url(payload),
        amount_min=display_amount_min,
        amount_max=display_amount_max,
        impact_score=event.impact_score,
        payload=payload,
        price=estimated_price,
        trade_price=estimated_price,
        estimated_price=estimated_price,
        current_price=current_price,
        pnl_pct=pnl_pct,
        return_pct=pnl_pct,
        alpha_pct=alpha_pct,
        benchmark_return_pct=benchmark_return_pct,
        holding_period_days=holding_period_days,
        outcome_horizon=outcome_horizon,
        return_label=outcome_horizon,
        pnl_source=pnl_source,
        outcome_status=outcome_status,
        outcome_skip_reason=outcome_skip_reason,
        quote_asof_ts=quote_asof_ts,
        quote_is_stale=quote_is_stale,
        smart_score=smart_score,
        smart_band=smart_band,
        baseline_median_amount_max=baseline_median_amount_max,
        baseline_count=baseline_count,
        unusual_multiple=unusual_multiple,
        member_net_30d=member_net_30d_map.get(_actor_net_30d_key(event, payload) or ""),
        symbol_net_30d=symbol_net_30d_map.get(sym_norm or "") if sym_norm else None,
        confirmation_30d=confirmation_summary,
    )


def _symbol_filter_clause(symbols: list[str]):
    return func.upper(Event.symbol).in_(symbols)


def _member_name_tokens(value: str | None) -> list[str]:
    if not value:
        return []
    tokens = MEMBER_NAME_TOKEN_RE.findall(value.upper())
    while len(tokens) > 2 and tokens[-1] in MEMBER_NAME_SUFFIX_TOKENS:
        tokens.pop()
    return tokens


def _member_first_last_key(value: str | None) -> tuple[str, str] | None:
    tokens = _member_name_tokens(value)
    if len(tokens) < 2:
        return None
    return tokens[0], tokens[-1]


def _member_query_keys(value: str | None) -> tuple[set[tuple[str, str]], bool]:
    key = _member_first_last_key(value)
    if key is None:
        return set(), False

    keys = {key}
    nickname_used = False
    for expanded_first in MEMBER_NICKNAME_EXPANSIONS.get(key[0], ()):
        keys.add((expanded_first, key[1]))
        nickname_used = True
    return keys, nickname_used


def _event_member_identity_rows(db: Session, *, congress_only: bool) -> list[tuple[str | None, str]]:
    q = (
        select(Event.member_bioguide_id, Event.member_name)
        .where(insider_visibility_clause())
        .where(_government_contract_action_events_only_clause())
        .where(Event.member_name.is_not(None))
        .where(func.length(func.trim(Event.member_name)) > 0)
        .distinct()
    )
    if congress_only:
        q = q.where(_congress_disclosure_clause())
    return [(row.member_bioguide_id, row.member_name) for row in db.execute(q).all()]


def _insider_member_search_clause(member: str):
    tokens = _search_tokens(member, limit=6)
    if not tokens:
        return None
    insider_blob = func.coalesce(Event.member_name, "") + " " + func.coalesce(Event.symbol, "") + " " + func.coalesce(Event.payload_json, "")
    return and_(
        Event.event_type == "insider_trade",
        *_token_match_clauses(insider_blob, tokens),
    )


def _apply_display_value_filters(
    items: list[EventOut],
    *,
    pnl_min: float | None,
    pnl_max: float | None,
    signal_min: float | None,
) -> list[EventOut]:
    pnl_filter_active = pnl_min is not None or pnl_max is not None
    signal_filter_active = signal_min is not None
    if not pnl_filter_active and not signal_filter_active:
        return items

    filtered: list[EventOut] = []
    for item in items:
        if pnl_filter_active:
            if item.event_type not in {CONGRESS_EQUITY_EVENT_TYPE, "insider_trade"}:
                continue
            if item.pnl_pct is None:
                continue
            pnl_value = float(item.pnl_pct)
            if pnl_min is not None and pnl_value < pnl_min:
                continue
            if pnl_max is not None and pnl_value > pnl_max:
                continue
        if signal_filter_active:
            if item.event_type not in {CONGRESS_EQUITY_EVENT_TYPE, "insider_trade"}:
                continue
            if item.smart_score is None:
                continue
            if float(item.smart_score) < float(signal_min):
                continue
        filtered.append(item)
    return filtered


def _resolve_event_member_filter(
    db: Session,
    member: str | None,
    *,
    congress_only: bool,
) -> tuple[list[str], list[str], bool]:
    member_value = (member or "").strip()
    if not member_value:
        return [], [], False

    requested_keys, nickname_used = _member_query_keys(member_value)
    exact_q = (
        select(Event.member_bioguide_id, Event.member_name)
        .where(insider_visibility_clause())
        .where(_government_contract_action_events_only_clause())
        .where(func.lower(func.trim(Event.member_name)) == member_value.lower())
        .distinct()
    )
    if congress_only:
        exact_q = exact_q.where(_congress_disclosure_clause())
    exact_rows = [(row.member_bioguide_id, row.member_name) for row in db.execute(exact_q).all()]
    if exact_rows and not nickname_used:
        ids = sorted({str(member_id) for member_id, _ in exact_rows if member_id})
        names = sorted({str(name) for _, name in exact_rows if name})
        return ids, names, False

    if not requested_keys:
        return [], [], False

    matched_rows: list[tuple[str | None, str]] = list(exact_rows)
    for member_id, member_name in _event_member_identity_rows(db, congress_only=congress_only):
        member_key = _member_first_last_key(member_name)
        if member_key in requested_keys:
            matched_rows.append((member_id, member_name))

    canonical_ids = sorted({
        str(member_id)
        for member_id, _ in matched_rows
        if member_id and not _is_legacy_member_alias(str(member_id))
    })
    if len(canonical_ids) == 1:
        return canonical_ids, [], False
    if len(canonical_ids) > 1:
        return [], [], True

    identity_keys = {
        f"id:{member_id.strip().lower()}" if member_id and member_id.strip() else f"name:{member_name.strip().lower()}"
        for member_id, member_name in matched_rows
        if member_name and member_name.strip()
    }
    if len(identity_keys) > 1:
        return [], [], True
    if not identity_keys:
        return [], [], False

    ids = sorted({str(member_id) for member_id, _ in matched_rows if member_id})
    names = sorted({str(name) for _, name in matched_rows if name})
    return ids, names, False


def _build_events_query(
    *,
    db: Session,
    symbols: list[str],
    types: list[str],
    since: datetime | None,
    cursor: str | None,
    limit: int,
    extra_filters: list,
    congress_filters: list,
    use_effective_activity_date: bool = False,
):
    q = select(Event)
    sort_ts = _event_effective_activity_ts_expr(db) if use_effective_activity_date else func.coalesce(Event.event_date, Event.ts)
    q = q.where(_government_contract_action_events_only_clause())

    if symbols:
        q = q.where(_symbol_filter_clause(symbols))

    if types:
        q = q.where(Event.event_type.in_(types))

    if since is not None:
        q = q.where(sort_ts >= since)

    for clause in extra_filters:
        q = q.where(clause)

    for clause in congress_filters:
        q = q.where(clause)

    if cursor:
        cursor_ts, cursor_id = _parse_cursor(cursor)
        q = q.where(
            or_(
                sort_ts < cursor_ts,
                and_(sort_ts == cursor_ts, Event.id < cursor_id),
            )
        )

    q = q.order_by(sort_ts.desc(), Event.id.desc()).limit(limit + 1)
    return q


def _fetch_events_page(
    db: Session,
    q,
    limit: int,
    enrich_prices: bool = True,
    use_effective_activity_date: bool = False,
    enqueue_feed_outcomes: bool = True,
    enqueue_metadata_refresh: bool = True,
) -> EventsPage:
    rows = db.execute(q).scalars().all()
    paged_rows = rows[:limit]
    event_ids = [event.id for event in paged_rows]
    outcome_by_event_id = _load_trade_outcomes_for_events(db, event_ids)

    price_memo: dict[tuple[str, str], float | None] = {}
    current_quote_meta: dict[str, dict] = {}
    current_price_memo: dict[str, float] = {}

    ticker_symbols = {
        symbol
        for event in paged_rows
        for symbol in [_event_symbol(event, _parse_event_payload(event))]
        if symbol
    }
    try:
        ticker_meta = _ticker_meta_with_security_names(
            db,
            sorted(ticker_symbols),
            enqueue_refresh=enqueue_metadata_refresh,
        )
    except Exception:
        logger.exception("ticker_meta resolver failed in /api/events")
        ticker_meta = {}

    insider_ciks = {
        cik
        for event in paged_rows
        for cik in [_event_cik(_parse_event_payload(event))]
        if event.event_type == "insider_trade" and cik
    }
    try:
        cik_names = get_cik_meta(
            db,
            sorted(insider_ciks),
            allow_refresh=False,
            enqueue_refresh=enqueue_metadata_refresh,
        )
    except Exception:
        logger.exception("cik_meta resolver failed in /api/events")
        cik_names = {}

    member_net_30d_map = _member_net_30d_map(db, paged_rows)
    symbol_net_30d_map = _symbol_net_30d_map(db, paged_rows)
    confirmation_metrics_map = get_confirmation_metrics_for_symbols(
        db,
        [symbol for event in paged_rows for symbol in [_event_symbol(event, _parse_event_payload(event))] if symbol],
    )
    baseline_map = _congress_baseline_map(db, paged_rows)
    items = [
        _event_payload(
            event,
            db,
            price_memo,
            current_price_memo,
            current_quote_meta,
            member_net_30d_map,
            symbol_net_30d_map,
            confirmation_metrics_map,
            ticker_meta,
            cik_names,
            baseline_map,
            enrich_prices=enrich_prices,
            outcome=outcome_by_event_id.get(event.id),
        )
        for event in paged_rows
    ]

    next_cursor = None
    if len(rows) > limit:
        last = rows[limit - 1]
        cursor_ts = _event_effective_activity_ts(last) if use_effective_activity_date else last.event_date or last.ts
        next_cursor = f"{cursor_ts.isoformat()}|{last.id}"

    if enqueue_feed_outcomes:
        _enqueue_missing_trade_outcomes(db, paged_rows, outcome_by_event_id)

    return EventsPage(items=items, next_cursor=next_cursor)


def _clean_suggestion(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = value.strip()
    return cleaned or None


def _compact_party_label(value: str | None) -> str | None:
    cleaned = _clean_suggestion(value)
    if cleaned is None:
        return None
    normalized = cleaned.lower()
    if normalized.startswith("dem"):
        return "D"
    if normalized.startswith("rep"):
        return "R"
    if normalized.startswith("ind"):
        return "I"
    return cleaned[:1].upper()


def _format_member_selection_label(name: str, party: str | None, state: str | None) -> str:
    badge = "-".join(part for part in [_compact_party_label(party), _clean_suggestion(state)] if part)
    return f"{name} ({badge})" if badge else name


def _search_tokens(value: str, *, limit: int = 4) -> list[str]:
    return [part.lower() for part in value.strip().split() if part.strip()][:limit]


def _token_match_clauses(expression, tokens: list[str]):
    lowered = func.lower(func.coalesce(expression, ""))
    return [lowered.like(f"%{token}%") for token in tokens]


def _global_score(query: str, *values: str | None, exact_bonus: float = 100.0, prefix_bonus: float = 60.0) -> float:
    normalized_query = query.strip().casefold()
    if not normalized_query:
        return 0.0

    best = 0.0
    for value in values:
        cleaned = _clean_suggestion(value)
        if cleaned is None:
            continue
        normalized = cleaned.casefold()
        if normalized == normalized_query:
            best = max(best, exact_bonus)
        elif normalized.startswith(normalized_query):
            best = max(best, prefix_bonus)
        elif normalized_query in normalized:
            best = max(best, 35.0)

    query_tokens = _search_tokens(query)
    if query_tokens:
        haystack = " ".join(value.casefold() for value in values if isinstance(value, str))
        matched = sum(1 for token in query_tokens if token in haystack)
        if matched:
            best = max(best, 10.0 + (matched / len(query_tokens)) * 20.0)
    return best


def _ticker_company_label(symbol: str, *names: str | None) -> str | None:
    for name in names:
        candidate = safe_company_identity_candidate(name, symbol)
        if candidate:
            return candidate
    return None


def _member_route(member_name: str, bioguide_id: str) -> str:
    slug = (
        (member_name or "")
        .strip()
        .upper()
        .replace(".", "")
        .replace(",", "")
        .replace("'", "")
        .replace("-", " ")
    )
    slug = "_".join(part for part in slug.split() if part)
    if slug:
        return f"/member/{slug}"
    return f"/member/{bioguide_id}"


def _insider_slug(name: str | None, reporting_cik: str) -> str:
    cleaned_name = _clean_suggestion(name)
    if not cleaned_name:
        return reporting_cik
    slug = "".join(ch.lower() if ch.isalnum() else "-" for ch in cleaned_name)
    slug = "-".join(part for part in slug.split("-") if part)
    return f"{slug}-{reporting_cik}" if slug else reporting_cik


def _format_insider_name(value: str | None) -> str | None:
    cleaned = _clean_suggestion(value)
    if cleaned is None:
        return None
    if "," in cleaned:
        last, rest = cleaned.split(",", 1)
        reordered = f"{rest.strip()} {last.strip()}".strip()
        return " ".join(part.capitalize() if part.isupper() else part for part in reordered.split())
    parts = cleaned.split()
    if cleaned.isupper() and 2 <= len(parts) <= 4:
        reordered_parts = [*parts[1:], parts[0]]
        return " ".join(part.capitalize() if len(part) > 1 else part for part in reordered_parts)
    return cleaned


def _global_ticker_results(db: Session, query: str, limit: int) -> list[dict[str, str | float | None]]:
    tokens = _search_tokens(query)
    if not tokens:
        return []

    security_blob = (
        func.coalesce(Security.symbol, "")
        + " "
        + func.coalesce(Security.name, "")
        + " "
        + func.coalesce(TickerMeta.company_name, "")
    )
    security_rows = db.execute(
        select(
            Security.symbol,
            Security.name.label("security_name"),
            TickerMeta.company_name.label("metadata_name"),
            TickerMeta.exchange,
        )
        .select_from(Security)
        .outerjoin(TickerMeta, func.upper(func.coalesce(TickerMeta.symbol, "")) == func.upper(func.coalesce(Security.symbol, "")))
        .where(Security.symbol.is_not(None))
        .where(func.length(func.trim(Security.symbol)) > 0)
        .where(and_(*_token_match_clauses(security_blob, tokens)))
        .order_by(func.upper(Security.symbol))
        .limit(limit * 3)
    ).all()

    event_blob = (
        func.coalesce(Event.symbol, "")
        + " "
        + func.coalesce(Security.name, "")
        + " "
        + func.coalesce(TickerMeta.company_name, "")
    )
    event_rows = db.execute(
        select(
            Event.symbol,
            func.max(Security.name).label("security_name"),
            func.max(TickerMeta.company_name).label("metadata_name"),
            func.max(TickerMeta.exchange).label("exchange"),
            func.count(Event.id).label("activity_count"),
        )
        .select_from(Event)
        .outerjoin(Security, func.upper(func.coalesce(Security.symbol, "")) == func.upper(func.coalesce(Event.symbol, "")))
        .outerjoin(TickerMeta, func.upper(func.coalesce(TickerMeta.symbol, "")) == func.upper(func.coalesce(Event.symbol, "")))
        .where(Event.symbol.is_not(None))
        .where(func.length(func.trim(Event.symbol)) > 0)
        .where(and_(*_token_match_clauses(event_blob, tokens)))
        .group_by(Event.symbol)
        .order_by(func.count(Event.id).desc(), func.upper(Event.symbol))
        .limit(limit * 3)
    ).all()

    by_symbol: dict[str, dict[str, str | float | None]] = {}
    for row in [*security_rows, *event_rows]:
        symbol = normalize_symbol(row.symbol)
        if not symbol:
            continue
        company_name = _ticker_company_label(symbol, row.metadata_name, row.security_name)
        exchange = _clean_suggestion(getattr(row, "exchange", None))
        score = _global_score(query, symbol, company_name, exact_bonus=120.0, prefix_bonus=80.0)
        score += min(float(getattr(row, "activity_count", 0) or 0), 25.0) / 5.0
        existing = by_symbol.get(symbol)
        if existing is None or float(existing.get("score") or 0) < score:
            subtitle = " · ".join(part for part in [company_name, exchange] if part)
            by_symbol[symbol] = {
                "type": "ticker",
                "id": symbol,
                "label": symbol,
                "subtitle": subtitle or company_name,
                "symbol": symbol,
                "route": f"/ticker/{symbol}",
                "score": score,
            }

    return sorted(by_symbol.values(), key=lambda item: (-(float(item.get("score") or 0)), str(item.get("label") or "")))[:limit]


def _global_member_results(db: Session, query: str, limit: int) -> list[dict[str, str | float | None]]:
    tokens = _search_tokens(query)
    if not tokens:
        return []

    member_name_expr = func.trim(func.coalesce(Member.first_name, "") + " " + func.coalesce(Member.last_name, ""))
    member_blob = (
        member_name_expr
        + " "
        + func.coalesce(Member.state, "")
        + " "
        + func.coalesce(Member.party, "")
        + " "
        + func.coalesce(Member.chamber, "")
        + " "
        + func.coalesce(Member.bioguide_id, "")
    )
    rows = db.execute(
        select(
            Member.bioguide_id,
            member_name_expr.label("member_name"),
            Member.party,
            Member.state,
            Member.chamber,
        )
        .where(Member.bioguide_id.is_not(None))
        .where(func.length(member_name_expr) > 0)
        .where(and_(*_token_match_clauses(member_blob, tokens)))
        .order_by(func.lower(Member.last_name), func.lower(Member.first_name), func.lower(Member.bioguide_id))
        .limit(limit * 3)
    ).all()

    deduped: dict[tuple[str, str], tuple[str, str, str | None, str | None, str | None]] = {}
    for row in rows:
        bioguide_id = _clean_suggestion(row.bioguide_id)
        name = _clean_suggestion(row.member_name)
        if bioguide_id is None or name is None:
            continue
        dedupe_key = (name.casefold(), (row.chamber or "").strip().casefold())
        existing = deduped.get(dedupe_key)
        if existing is None or (_is_legacy_member_alias(existing[0]) and not _is_legacy_member_alias(bioguide_id)):
            deduped[dedupe_key] = (bioguide_id, name, row.party, row.state, row.chamber)

    results: list[dict[str, str | float | None]] = []
    for bioguide_id, name, party_value, state_value, chamber_value in deduped.values():
        chamber = _clean_suggestion(chamber_value)
        party = _compact_party_label(party_value)
        state = _clean_suggestion(state_value)
        subtitle = " · ".join(part for part in ["Congress member", chamber.title() if chamber else None, party, state] if part)
        results.append(
            {
                "type": "member",
                "id": bioguide_id,
                "label": name,
                "subtitle": subtitle,
                "route": _member_route(name, bioguide_id),
                "score": _global_score(query, name, bioguide_id, exact_bonus=95.0, prefix_bonus=65.0),
            }
        )
        if len(results) >= limit:
            break
    return sorted(results, key=lambda item: (-(float(item.get("score") or 0)), str(item.get("label") or "")))[:limit]


def _global_insider_results(db: Session, query: str, limit: int) -> list[dict[str, str | float | None]]:
    tokens = _search_tokens(query)
    if not tokens:
        return []

    insider_blob = func.coalesce(Event.member_name, "") + " " + func.coalesce(Event.symbol, "") + " " + func.coalesce(Event.payload_json, "")
    rows = db.execute(
        select(Event)
        .where(Event.event_type == "insider_trade")
        .where(and_(*_token_match_clauses(insider_blob, tokens)))
        .order_by(func.coalesce(Event.event_date, Event.ts).desc(), Event.id.desc())
        .limit(limit * 12)
    ).scalars().all()

    results_by_key: dict[str, dict[str, str | float | None]] = {}
    for event in rows:
        payload = _parse_event_payload(event)
        reporting_cik = _event_reporting_cik(payload)
        if reporting_cik is None:
            continue
        name = _format_insider_name(_insider_display_name(event, payload))
        if name is None:
            continue
        symbol = _event_symbol(event, payload)
        company_name = _insider_company_name(event, payload)
        role = _insider_role(payload)
        subtitle_parts = ["Insider", company_name, symbol, role]
        subtitle = " · ".join(part for part in subtitle_parts if part)
        score = _global_score(query, name, symbol, company_name, role, reporting_cik, exact_bonus=90.0, prefix_bonus=60.0)
        issuer_key = symbol or _event_cik(payload) or "unknown"
        result_key = f"{reporting_cik}:{issuer_key}"
        existing = results_by_key.get(result_key)
        if existing is None or float(existing.get("score") or 0) < score:
            route = f"/insider/{_insider_slug(name, reporting_cik)}"
            if symbol:
                route = f"{route}?issuer={symbol}"
            results_by_key[result_key] = {
                "type": "insider",
                "id": result_key,
                "label": name,
                "subtitle": subtitle,
                "symbol": symbol,
                "route": route,
                "score": score,
            }
        if len(results_by_key) >= limit:
            break

    return sorted(results_by_key.values(), key=lambda item: (-(float(item.get("score") or 0)), str(item.get("label") or "")))[:limit]


@router.get("/search/global")
def global_search(
    db: Session = Depends(get_db),
    q: str = "",
    limit: int = Query(8, ge=1, le=12),
):
    query = q.strip()
    if not query:
        return {"results": []}

    results: list[dict[str, str | float | None]] = []
    try:
        results.extend(
            {
                "type": item.get("type"),
                "id": item.get("id"),
                "label": item.get("label"),
                "subtitle": item.get("subtitle"),
                "route": item.get("route"),
                "score": _global_score(query, item.get("label"), exact_bonus=110.0, prefix_bonus=75.0),
            }
            for item in department_suggestions(db, query, limit=limit)
        )
    except Exception:
        logger.exception("global_search_departments_failed query=%s", query)

    for category, loader in (
        ("ticker", _global_ticker_results),
        ("member", _global_member_results),
        ("insider", _global_insider_results),
    ):
        try:
            results.extend(loader(db, query, limit))
        except Exception:
            logger.exception("global_search_%s_failed query=%s", category, query)

    results.sort(key=lambda item: (-(float(item.get("score") or 0)), str(item.get("type") or ""), str(item.get("label") or "")))
    return {"results": results}


@router.get("/search/suggest")
def search_suggest(
    request: Request,
    db: Session = Depends(get_db),
    q: str = "",
    limit: int = Query(8, ge=1, le=20),
):
    user = current_user(db, request, required=False)
    return search_suggestions(db, q, limit=limit, user_id=user.id if user else None)


def _member_suggestions_query(prefix: str, limit: int):
    member_name_sort = func.lower(Event.member_name).label("member_name_sort")
    return (
        select(Event.member_name, member_name_sort)
        .where(Event.event_type == "congress_trade")
        .where(Event.member_name.is_not(None))
        .where(func.length(func.trim(Event.member_name)) > 0)
        .where(func.lower(Event.member_name).like(f"{prefix.lower()}%"))
        .distinct()
        .order_by(member_name_sort)
        .limit(limit)
    )


def _member_insider_event_suggestions_query(pattern: str, limit: int):
    member_name_sort = func.lower(Event.member_name).label("member_name_sort")
    insider_blob = func.lower(func.coalesce(Event.member_name, "") + " " + func.coalesce(Event.symbol, "") + " " + func.coalesce(Event.payload_json, ""))
    return (
        select(Event.member_name, Event.payload_json, Event.symbol, member_name_sort)
        .where(Event.event_type == "insider_trade")
        .where(insider_blob.like(pattern))
        .order_by(member_name_sort)
        .limit(limit * 4)
    )


@router.get("/suggest/symbol")
def suggest_symbol(
    db: Session = Depends(get_db),
    q: str = "",
    limit: int = Query(10, ge=1, le=MAX_SUGGEST_LIMIT),
    tape: str | None = None,
    include_departments: bool = Query(False),
):
    prefix = q.strip()
    if not prefix:
        return {"items": []}

    department_items: list[dict] = []
    tape_value = (tape or "").strip().lower()
    if include_departments and tape_value in {"government_contracts", "government_contract", "all", ""}:
        department_items = department_suggestions(db, prefix, limit=limit)

    query = (
        select(
            Event.symbol.label("symbol"),
            func.max(Security.name).label("company_name"),
        )
        .select_from(Event)
        .outerjoin(
            Security,
            func.upper(func.coalesce(Security.symbol, "")) == func.upper(func.coalesce(Event.symbol, "")),
        )
        .where(Event.symbol.is_not(None))
        .where(func.length(func.trim(Event.symbol)) > 0)
        .where(func.lower(Event.symbol).like(f"{prefix.lower()}%"))
    )

    if tape_value == "congress":
        query = query.where(Event.event_type == "congress_trade")
    elif tape_value == "insider":
        query = query.where(Event.event_type == "insider_trade")
    elif tape_value in {"government_contracts", "government_contract"}:
        query = query.where(Event.event_type == "government_contract")

    rows = db.execute(
        query.group_by(Event.symbol).order_by(func.upper(Event.symbol)).limit(max(limit - len(department_items), 0))
    ).all()
    items = [
        {"symbol": cleaned_symbol, "name": _clean_suggestion(company_name)}
        for raw_symbol, company_name in rows
        if (cleaned_symbol := _clean_suggestion(raw_symbol)) is not None
    ]
    return {"items": [*department_items, *items][:limit]}


@router.get("/suggest/member")
def suggest_member(
    db: Session = Depends(get_db),
    q: str = "",
    limit: int = Query(10, ge=1, le=MAX_SUGGEST_LIMIT),
):
    prefix = q.strip()
    if not prefix:
        return {"items": []}

    try:
        rows = db.execute(_member_suggestions_query(prefix, limit)).all()
    except Exception:
        logger.exception("member_suggest_failed query=%s", prefix)
        return {"items": []}

    items = [name for name in (_clean_suggestion(row.member_name) for row in rows) if name is not None]
    return {"items": items}


@router.get("/suggest/member-insider")
def suggest_member_insider(
    db: Session = Depends(get_db),
    q: str = "",
    limit: int = Query(10, ge=1, le=MAX_SUGGEST_LIMIT),
):
    prefix = q.strip()
    if not prefix:
        return {"items": []}

    pattern = f"%{prefix.lower()}%"
    member_name_expr = func.trim(func.coalesce(Member.first_name, "") + " " + func.coalesce(Member.last_name, ""))
    member_search_blob = func.lower(member_name_expr)
    congress_rows = db.execute(
        select(
            Member.bioguide_id,
            member_name_expr.label("member_name"),
            Member.party,
            Member.state,
            Member.chamber,
        )
        .where(Member.bioguide_id.is_not(None))
        .where(func.length(member_name_expr) > 0)
        .where(member_search_blob.like(pattern))
        .order_by(func.lower(Member.last_name), func.lower(Member.first_name), func.lower(Member.bioguide_id))
        .limit(limit * 4)
    ).all()

    try:
        insider_rows = db.execute(_member_insider_event_suggestions_query(pattern, limit)).all()
    except Exception:
        logger.exception("member_insider_suggest_insider_query_failed query=%s", prefix)
        insider_rows = []

    items: list[dict[str, str | None]] = []
    seen: set[tuple[str, ...]] = set()
    deduped_congress_rows: dict[tuple[str, str], tuple[str, str, str | None, str | None, str | None]] = {}
    for bioguide_id, member_name, party, state, chamber in congress_rows:
        cleaned_bioguide = _clean_suggestion(bioguide_id)
        cleaned_name = _clean_suggestion(member_name)
        if cleaned_name is None or cleaned_bioguide is None:
            continue
        dedupe_key = (cleaned_name.casefold(), (chamber or "").strip().lower())
        existing = deduped_congress_rows.get(dedupe_key)
        if existing is None or (_is_legacy_member_alias(existing[0]) and not _is_legacy_member_alias(cleaned_bioguide)):
            deduped_congress_rows[dedupe_key] = (
                cleaned_bioguide,
                cleaned_name,
                party,
                state,
                chamber,
            )

    for cleaned_bioguide, cleaned_name, party, state, chamber in deduped_congress_rows.values():
        key = (cleaned_bioguide.casefold(), "congress")
        if key in seen:
            continue
        seen.add(key)
        items.append(
            {
                "label": _format_member_selection_label(cleaned_name, party, state),
                "value": cleaned_name,
                "category": "congress",
                "bioguide_id": cleaned_bioguide,
                "party": _compact_party_label(party),
                "state": _clean_suggestion(state),
                "chamber": _clean_suggestion(chamber),
            }
        )
        if len(items) >= limit:
            return {"items": items}

    for name, payload_json, event_symbol, _member_name_sort in insider_rows:
        reporting_cik = None
        payload = {}
        try:
            payload = json.loads(payload_json or "{}")
            if isinstance(payload, dict):
                reporting_cik = normalize_cik(
                    payload.get("reporting_cik")
                    or payload.get("reportingCik")
                    or payload.get("reportingCIK")
                    or payload.get("rptOwnerCik")
                )
        except Exception:
            reporting_cik = None
            payload = {}
        if not isinstance(payload, dict):
            payload = {}

        pseudo_event = SimpleNamespace(event_type="insider_trade", member_name=name, symbol=event_symbol)
        cleaned_name = _clean_suggestion(name) or _format_insider_name(_insider_display_name(pseudo_event, payload))
        if cleaned_name is None:
            continue
        symbol = _event_symbol(pseudo_event, payload)
        company_name = _insider_company_name(pseudo_event, payload)
        role = _insider_role(payload)
        label_parts = [cleaned_name, company_name, symbol, role]
        label = " · ".join(part for part in label_parts if part)

        key = (cleaned_name.casefold(), symbol or reporting_cik or "", "insider")
        if key in seen:
            continue
        seen.add(key)
        items.append(
            {
                "label": label,
                "value": cleaned_name,
                "category": "insider",
                "reporting_cik": reporting_cik,
                "symbol": symbol,
                "company_name": company_name,
                "role": role,
            }
        )
        if len(items) >= limit:
            break

    return {"items": items}


@router.get("/suggest/role")
def suggest_role(
    db: Session = Depends(get_db),
    q: str = "",
    limit: int = Query(10, ge=1, le=MAX_SUGGEST_LIMIT),
):
    prefix = q.strip().lower()
    if not prefix:
        return {"items": []}

    rows = (
        db.execute(
            select(Event.payload_json)
            .where(Event.event_type == "insider_trade")
            .where(Event.payload_json.is_not(None))
            .limit(1000)
        )
        .scalars()
        .all()
    )

    standard_roles = ["CEO", "CFO", "Director", "Officer", "President", "10% Owner", "CLO", "COO", "CTO"]
    found: set[str] = {role for role in standard_roles if role.lower().startswith(prefix)}
    for payload_json in rows:
        try:
            payload = json.loads(payload_json)
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue

        raw = payload.get("raw") if isinstance(payload.get("raw"), dict) else {}
        for value_dict in (payload, raw):
            for key in ("role", "relationship", "title", "typeOfOwner", "officerTitle", "insiderRole", "position"):
                raw_value = value_dict.get(key)
                if not isinstance(raw_value, str):
                    continue
                canonical = _canonical_role_label(raw_value)
                if canonical and (
                    canonical.lower().startswith(prefix)
                    or raw_value.strip().lower().startswith(prefix)
                    or prefix in raw_value.strip().lower()
                ):
                    found.add(canonical)

    items = sorted(found, key=lambda value: (standard_roles.index(value) if value in standard_roles else len(standard_roles), value.lower()))[:limit]
    return {"items": items}


def _member_filter_diagnostics(db: Session, member: str | None) -> dict[str, int | str] | None:
    member_value = (member or "").strip()
    if not member_value:
        return None

    base_query = (
        select(Event.event_type, func.count().label("count"))
        .where(insider_visibility_clause())
        .where(_government_contract_action_events_only_clause())
        .where(Event.member_name.ilike(f"%{member_value}%"))
        .group_by(Event.event_type)
    )
    rows = db.execute(base_query).all()
    by_type = {str(event_type or "unknown"): int(count or 0) for event_type, count in rows}
    return {
        "member_query": member_value,
        "member_name_visible_matches": sum(by_type.values()),
        "member_name_congress_matches": by_type.get("congress_trade", 0),
        "member_name_insider_matches": by_type.get("insider_trade", 0),
    }


@router.get(
    "/events",
    response_model=EventsPageDebug,
    response_model_exclude_none=True,
    dependencies=[Depends(rate_limit_provider_backed)],
)
def list_events(
    request: Request = None,
    db: Session = Depends(get_db),
    symbol: str | None = None,
    ticker: str | None = None,
    event_type: str | None = None,
    types: str | None = None,
    mode: str | None = None,
    tape: str | None = None,
    since: str | None = None,
    member: str | None = None,
    member_id: str | None = None,
    chamber: str | None = None,
    party: str | None = None,
    asset_class: str | None = None,
    asset_type: str | None = None,
    trade_type: str | None = None,
    transaction_type: str | None = None,
    role: str | None = None,
    ownership: str | None = None,
    department: str | None = None,
    min_amount: float | None = Query(None, ge=0),
    max_amount: float | None = Query(None, ge=0),
    filed_after_max: float | None = Query(None, ge=0),
    pnl_min: float | None = None,
    pnl_max: float | None = None,
    signal_min: float | None = Query(None, ge=0),
    whale: bool | None = None,
    recent_days: int | None = Query(None, ge=1),
    cursor: str | None = None,
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=100),
    page_size: int | None = Query(None, ge=1, le=MAX_LIMIT),
    offset: int = Query(0, ge=0),
    include_total: bool = Query(False),
    enrich_prices: bool = Query(True),
    debug: bool | None = None,
):
    started_at = perf_counter()
    # Manual curl checks:
    # curl "http://localhost:8000/api/events?symbol=NVDA"
    # curl "http://localhost:8000/api/events?member=Pelosi"
    # curl "http://localhost:8000/api/events?chamber=house"
    # curl "http://localhost:8000/api/events?min_amount=250000"  # uses amount_max
    # curl "http://localhost:8000/api/events?trade_type=sale"
    # curl "http://localhost:8000/api/events?party=Democrat"
    # curl "http://localhost:8000/api/events?recent_days=30"
    # Smoke checks (after backfill):
    # curl "http://localhost:8000/api/events?limit=1"
    # curl "http://localhost:8000/api/events?event_type=congress_trade&limit=1"
    min_amount = min_amount if isinstance(min_amount, (int, float)) else None
    max_amount = max_amount if isinstance(max_amount, (int, float)) else None
    filed_after_max = filed_after_max if isinstance(filed_after_max, (int, float)) else None
    pnl_min = pnl_min if isinstance(pnl_min, (int, float)) else None
    pnl_max = pnl_max if isinstance(pnl_max, (int, float)) else None
    signal_min = signal_min if isinstance(signal_min, (int, float)) else None
    recent_days = recent_days if isinstance(recent_days, int) else None
    offset = offset if isinstance(offset, int) else 0
    page_size = page_size if isinstance(page_size, int) else None
    include_total = include_total is True
    enrich_prices = enrich_prices is not False
    debug_enabled = _events_debug_enabled(db, request, debug)

    symbol_values = _parse_csv(symbol) + _parse_csv(ticker)
    combined_symbols = [
        normalized
        for value in symbol_values
        for normalized in [normalize_symbol(value)]
        if normalized
    ]
    raw_event_type = event_type if event_type is not None else types
    if raw_event_type is None and mode is not None:
        raw_event_type = mode
    type_list = _expand_event_type_aliases(_parse_csv(raw_event_type))
    if combined_symbols and enrich_prices:
        logger.info(
            "events_price_enrichment_skipped symbols=%s reason=symbol_scoped_base_rows",
            ",".join(combined_symbols),
        )
        enrich_prices = False
    enqueue_feed_outcomes = enrich_prices
    enqueue_metadata_refresh = bool(combined_symbols) or enrich_prices or debug_enabled
    tape_value = None
    if tape is not None:
        tape_value = tape.strip().lower()
        if tape_value not in {"congress", "insider", "government_contracts", "government_contract", "all"}:
            raise HTTPException(status_code=400, detail="Invalid tape. Allowed values: congress, insider, government_contracts, all.")
    since_dt = _parse_since(since)
    recent_since = None
    if recent_days is not None:
        recent_since = datetime.now(timezone.utc) - timedelta(days=recent_days)

    chamber_value = _validate_enum(chamber, {"house", "senate"}, "chamber")
    party_value = _validate_enum(
        party, {"democrat", "republican", "independent", "other"}, "party"
    )
    trade_value = _normalize_trade_type(trade_type)
    asset_filter_value = (asset_class or asset_type or "").strip()

    if whale and (min_amount is None or min_amount < 250_000):
        min_amount = 250_000

    q = select(Event)
    sort_ts = func.coalesce(Event.event_date, Event.ts)
    applied_filters: list[str] = []

    q = q.where(insider_visibility_clause())
    q = q.where(_government_contract_action_events_only_clause())
    applied_filters.append("insider_visibility")

    government_contract_scope = set(type_list).issubset({"government_contract"}) if type_list else False

    if combined_symbols:
        q = q.where(_symbol_filter_clause(combined_symbols))
        applied_filters.append("symbol")

    if type_list:
        q = q.where(Event.event_type.in_(type_list))
        applied_filters.append("types")
    elif tape_value == "congress":
        q = q.where(_congress_disclosure_clause())
        applied_filters.append("tape=congress")
    elif tape_value == "insider":
        q = q.where(Event.event_type == "insider_trade")
        applied_filters.append("tape=insider")

    if since_dt is not None:
        q = q.where(sort_ts >= since_dt)
        applied_filters.append("since")
    if recent_since is not None:
        q = q.where(sort_ts >= recent_since)
        applied_filters.append("recent_days")

    asset_clause = _asset_class_filter_clause(asset_filter_value) if asset_filter_value else None
    if asset_clause is not None:
        q = q.where(asset_clause)
        applied_filters.append("asset_class")

    congress_filter_active = not government_contract_scope and any(
        [
            member_id,
            chamber_value,
            party_value,
        ]
    )
    if congress_filter_active:
        q = q.where(_congress_disclosure_clause())
        applied_filters.append("event_type=congress_disclosure")

    insider_filter_active = not government_contract_scope and any([transaction_type, role, ownership])
    if insider_filter_active:
        q = q.where(Event.event_type == "insider_trade")
        applied_filters.append("event_type=insider_trade")
    if member and not government_contract_scope:
        member_tokens = _member_name_tokens(member)
        member_scope_congress = (
            tape_value == "congress"
            or bool(type_list and set(type_list).issubset(set(CONGRESS_DISCLOSURE_EVENT_TYPES)))
            or (congress_filter_active and not insider_filter_active)
        )
        member_ids, member_names, ambiguous_member = _resolve_event_member_filter(
            db,
            member,
            congress_only=member_scope_congress,
        )
        insider_member_clause = None if member_scope_congress else _insider_member_search_clause(member)
        if member_ids or member_names or insider_member_clause is not None:
            member_clauses = []
            if member_ids:
                member_clauses.append(func.lower(Event.member_bioguide_id).in_([member_id.lower() for member_id in member_ids]))
            if member_names:
                member_clauses.append(func.lower(Event.member_name).in_([name.lower() for name in member_names]))
            if insider_member_clause is not None:
                member_clauses.append(insider_member_clause)
            q = q.where(or_(*member_clauses))
            applied_filters.append("member_alias_or_insider")
        elif ambiguous_member or len(member_tokens) >= 2:
            q = q.where(Event.id == -1)
            applied_filters.append("member_unresolved")
        else:
            member_like = f"%{member.strip()}%"
            q = q.where(Event.member_name.ilike(member_like))
            applied_filters.append("member")
    if member_id and not government_contract_scope:
        q = q.where(func.lower(Event.member_bioguide_id) == member_id.strip().lower())
        applied_filters.append("member_id")
    if chamber_value and not government_contract_scope:
        q = q.where(func.lower(Event.chamber) == chamber_value)
        applied_filters.append("chamber")
    if party_value and not government_contract_scope:
        if party_value == "other":
            q = q.where(or_(Event.party.is_(None), func.lower(Event.party) == party_value))
        else:
            q = q.where(func.lower(Event.party) == party_value)
        applied_filters.append("party")

    if trade_value and not government_contract_scope:
        trade_values = _trade_type_values(trade_value)
        effective_event_scope = "all"
        explicit_event_types = set(type_list)
        if explicit_event_types and explicit_event_types.issubset(set(CONGRESS_DISCLOSURE_EVENT_TYPES)) or tape_value == "congress" or (
            congress_filter_active and not insider_filter_active
        ):
            effective_event_scope = "congress_trade"
        elif explicit_event_types == {"insider_trade"} or tape_value == "insider" or (
            insider_filter_active and not congress_filter_active
        ):
            effective_event_scope = "insider_trade"

        if effective_event_scope == "congress_trade":
            q = q.where(func.lower(Event.trade_type).in_(trade_values))
        elif effective_event_scope == "insider_trade":
            q = q.where(func.lower(Event.trade_type).in_(trade_values))
        else:
            q = q.where(func.lower(Event.trade_type).in_(trade_values))
        applied_filters.append("trade_type")

    if transaction_type and not government_contract_scope:
        q = q.where(func.lower(Event.transaction_type) == transaction_type.strip().lower())
        applied_filters.append("transaction_type")

    payload_lower = func.lower(Event.payload_json)
    if role and not government_contract_scope:
        role_clause = _insider_role_filter_clause(role)
        if role_clause is not None:
            q = q.where(role_clause)
        applied_filters.append("role")
    if ownership and not government_contract_scope:
        ownership_value = ownership.strip().lower()
        q = q.where(payload_lower.like(f'%"ownership"%{ownership_value}%'))
        applied_filters.append("ownership")
    if department and department.strip():
        department_clause = _government_contract_department_clause(
            department,
            include_non_contract_events=False,
        )
        if department_clause is not None:
            q = q.where(department_clause)
            applied_filters.append("department")
    if min_amount is not None:
        q = q.where(Event.amount_max >= min_amount)
        applied_filters.append("min_amount")
    if max_amount is not None:
        q = q.where(Event.amount_min <= max_amount)
        applied_filters.append("max_amount")
    if filed_after_max is not None:
        filed_after_expr = _event_filed_after_expr(db)
        q = q.where(filed_after_expr.is_not(None)).where(filed_after_expr <= filed_after_max)
        applied_filters.append("filed_after_max")
    if pnl_min is not None:
        q = q.where(Event.event_type.in_([CONGRESS_EQUITY_EVENT_TYPE, "insider_trade"]))
        applied_filters.append("pnl_min")
    if pnl_max is not None:
        q = q.where(Event.event_type.in_([CONGRESS_EQUITY_EVENT_TYPE, "insider_trade"]))
        applied_filters.append("pnl_max")
    if signal_min is not None:
        q = q.where(Event.event_type.in_([CONGRESS_EQUITY_EVENT_TYPE, "insider_trade"]))
        applied_filters.append("signal_min")

    display_filter_active = pnl_min is not None or pnl_max is not None or signal_min is not None
    candidate_limit = min(max(limit * 10, limit), 500) if display_filter_active else limit
    response_cache_key = _events_response_cache_key(
        request=request,
        debug_enabled=debug_enabled,
        include_total=include_total,
        enrich_prices=enrich_prices,
        combined_symbols=combined_symbols,
        type_list=type_list,
        tape_value=tape_value,
        since=since,
        member=member,
        member_id=member_id,
        chamber_value=chamber_value,
        party_value=party_value,
        asset_filter_value=asset_filter_value,
        trade_value=trade_value,
        transaction_type=transaction_type,
        role=role,
        ownership=ownership,
        department=department,
        min_amount=min_amount,
        max_amount=max_amount,
        filed_after_max=filed_after_max,
        pnl_min=pnl_min,
        pnl_max=pnl_max,
        signal_min=signal_min,
        whale=whale,
        recent_days=recent_days,
        cursor=cursor,
        limit=limit,
        page_size=page_size,
        offset=offset,
    )
    cached_response = _events_response_cache_get(response_cache_key)
    if cached_response is not None:
        logger.info("events_response_cache_hit symbols=%s limit=%s offset=%s", ",".join(combined_symbols), limit, offset)
        _log_events_request_summary(
            started_at=started_at,
            item_count=len(cached_response.items),
            total=cached_response.total,
            include_total=include_total,
            enrich_prices=enrich_prices,
            limit=limit,
            page_size=page_size,
            offset=offset,
        )
        return cached_response
    inflight_state, inflight_leader = _events_response_inflight_start(response_cache_key)
    if response_cache_key and not inflight_leader and inflight_state is not None:
        event = inflight_state.get("event")
        if isinstance(event, threading.Event) and event.wait(timeout=max(EVENTS_RESPONSE_DEDUPE_WAIT_SECONDS, 0)):
            result = inflight_state.get("result")
            if isinstance(result, (EventsPage, EventsPageDebug)):
                logger.info("events_response_dedupe_hit symbols=%s limit=%s offset=%s", ",".join(combined_symbols), limit, offset)
                return copy.deepcopy(result)
        logger.info("events_response_dedupe_timeout symbols=%s limit=%s offset=%s", ",".join(combined_symbols), limit, offset)

    if cursor:
        cursor_ts, cursor_id = _parse_cursor(cursor)
        q = q.where(
            or_(
                sort_ts < cursor_ts,
                and_(sort_ts == cursor_ts, Event.id < cursor_id),
            )
        )
        applied_filters.append("cursor")

    filtered_query = q.order_by(sort_ts.desc(), Event.id.desc())

    total = None
    if include_total and cursor is None:
        total = db.execute(select(func.count()).select_from(filtered_query.subquery())).scalar()

    if cursor:
        page = _fetch_events_page(
            db,
            filtered_query.limit(candidate_limit + 1),
            candidate_limit,
            enrich_prices=enrich_prices,
            enqueue_feed_outcomes=enqueue_feed_outcomes,
            enqueue_metadata_refresh=enqueue_metadata_refresh,
        )
        if display_filter_active:
            page.items = _apply_display_value_filters(
                page.items,
                pnl_min=pnl_min,
                pnl_max=pnl_max,
                signal_min=signal_min,
            )[:limit]
        if debug_enabled:
            count_query = select(func.count()).select_from(q.subquery())
            count_after_filters = db.execute(count_query).scalar_one()
            diagnostics = _member_filter_diagnostics(db, member)
            debug_payload = EventsDebug(
                received_params={
                    "symbol": symbol,
                    "ticker": ticker,
                    "event_type": event_type,
                    "types": types,
                    "mode": mode,
                    "tape": tape,
                    "member": member,
                    "chamber": chamber,
                    "party": party,
                    "asset_class": asset_class,
                    "asset_type": asset_type,
                    "trade_type": trade_type,
                    "transaction_type": transaction_type,
                    "role": role,
                    "ownership": ownership,
                    "department": department,
                    "min_amount": min_amount,
                    "max_amount": max_amount,
                    "filed_after_max": filed_after_max,
                    "pnl_min": pnl_min,
                    "pnl_max": pnl_max,
                    "signal_min": signal_min,
                    "recent_days": recent_days,
                    "cursor": cursor,
                    "offset": offset,
                    "page_size": page_size,
                    "include_total": include_total,
                    "enrich_prices": enrich_prices,
                },
                applied_filters=applied_filters,
                count_after_filters=count_after_filters,
                diagnostics=diagnostics,
                sql_hint=", ".join(applied_filters) if applied_filters else None,
            )
            _log_ticker_events_payload(symbols=combined_symbols, items=page.items, recent_days=recent_days, started_at=started_at)
            _log_events_request_summary(
                started_at=started_at,
                item_count=len(page.items),
                total=None,
                include_total=include_total,
                enrich_prices=enrich_prices,
                limit=limit,
                page_size=page_size,
                offset=offset,
            )
            return _events_response_cache_finalize(
                response_cache_key,
                inflight_state,
                inflight_leader,
                EventsPageDebug(items=page.items, next_cursor=page.next_cursor, debug=debug_payload),
            )
        _log_ticker_events_payload(symbols=combined_symbols, items=page.items, recent_days=recent_days, started_at=started_at)
        _log_events_request_summary(
            started_at=started_at,
            item_count=len(page.items),
            total=None,
            include_total=include_total,
            enrich_prices=enrich_prices,
            limit=limit,
            page_size=page_size,
            offset=offset,
        )
        return _events_response_cache_finalize(response_cache_key, inflight_state, inflight_leader, page)

    rows = db.execute(filtered_query.offset(offset).limit(candidate_limit)).scalars().all()
    event_ids = [event.id for event in rows]
    outcome_by_event_id = _load_trade_outcomes_for_events(db, event_ids)
    price_memo: dict[tuple[str, str], float | None] = {}
    current_quote_meta: dict[str, dict] = {}
    current_price_memo: dict[str, float] = {}

    ticker_symbols = [_event_symbol(event, _parse_event_payload(event)) for event in rows]
    try:
        ticker_meta = _ticker_meta_with_security_names(
            db,
            [symbol for symbol in ticker_symbols if symbol],
            enqueue_refresh=enqueue_metadata_refresh,
        )
    except Exception:
        logger.exception("ticker_meta resolver failed in /api/events")
        ticker_meta = {}

    insider_ciks = {
        cik
        for event in rows
        for cik in [_event_cik(_parse_event_payload(event))]
        if event.event_type == "insider_trade" and cik
    }
    try:
        cik_names = get_cik_meta(
            db,
            sorted(insider_ciks),
            allow_refresh=False,
            enqueue_refresh=enqueue_metadata_refresh,
        )
    except Exception:
        logger.exception("cik_meta resolver failed in /api/events")
        cik_names = {}

    member_net_30d_map = _member_net_30d_map(db, rows)
    symbol_net_30d_map = _symbol_net_30d_map(db, rows)
    confirmation_metrics_map = get_confirmation_metrics_for_symbols(
        db,
        [symbol for event in rows for symbol in [_event_symbol(event, _parse_event_payload(event))] if symbol],
    )
    baseline_map = _congress_baseline_map(db, rows)
    items = [
        _event_payload(
            event,
            db,
            price_memo,
            current_price_memo,
            current_quote_meta,
            member_net_30d_map,
            symbol_net_30d_map,
            confirmation_metrics_map,
            ticker_meta,
            cik_names,
            baseline_map,
            enrich_prices=enrich_prices,
            outcome=outcome_by_event_id.get(event.id),
        )
        for event in rows
    ]
    if display_filter_active:
        items = _apply_display_value_filters(
            items,
            pnl_min=pnl_min,
            pnl_max=pnl_max,
            signal_min=signal_min,
        )[:limit]

    if enqueue_feed_outcomes:
        _enqueue_missing_trade_outcomes(db, rows, outcome_by_event_id)

    if debug_enabled:
        count_query = select(func.count()).select_from(q.subquery())
        count_after_filters = db.execute(count_query).scalar_one()
        diagnostics = _member_filter_diagnostics(db, member)
        debug_payload = EventsDebug(
            received_params={
                "symbol": symbol,
                "ticker": ticker,
                "event_type": event_type,
                "types": types,
                "mode": mode,
                "tape": tape,
                "member": member,
                "chamber": chamber,
                "party": party,
                "asset_class": asset_class,
                "asset_type": asset_type,
                "trade_type": trade_type,
                "transaction_type": transaction_type,
                "role": role,
                "ownership": ownership,
                "department": department,
                "min_amount": min_amount,
                "max_amount": max_amount,
                "filed_after_max": filed_after_max,
                "pnl_min": pnl_min,
                "pnl_max": pnl_max,
                "signal_min": signal_min,
                "recent_days": recent_days,
                    "cursor": cursor,
                    "offset": offset,
                    "page_size": page_size,
                    "include_total": include_total,
                    "enrich_prices": enrich_prices,
                },
            applied_filters=applied_filters,
            count_after_filters=count_after_filters,
            diagnostics=diagnostics,
            sql_hint=", ".join(applied_filters) if applied_filters else None,
        )
        _log_ticker_events_payload(symbols=combined_symbols, items=items, recent_days=recent_days, started_at=started_at)
        _log_events_request_summary(
            started_at=started_at,
            item_count=len(items),
            total=total,
            include_total=include_total,
            enrich_prices=enrich_prices,
            limit=limit,
            page_size=page_size,
            offset=offset,
        )
        return _events_response_cache_finalize(
            response_cache_key,
            inflight_state,
            inflight_leader,
            EventsPageDebug(items=items, total=total, limit=limit, offset=offset, debug=debug_payload),
        )

    _log_ticker_events_payload(symbols=combined_symbols, items=items, recent_days=recent_days, started_at=started_at)
    _log_events_request_summary(
        started_at=started_at,
        item_count=len(items),
        total=total,
        include_total=include_total,
        enrich_prices=enrich_prices,
        limit=limit,
        page_size=page_size,
        offset=offset,
    )
    return _events_response_cache_finalize(
        response_cache_key,
        inflight_state,
        inflight_leader,
        EventsPageDebug(items=items, total=total, limit=limit, offset=offset),
    )


@router.get("/tickers/{symbol}/events", response_model=EventsPage, dependencies=[Depends(rate_limit_provider_backed)])
def list_ticker_events(
    symbol: str,
    db: Session = Depends(get_db),
    types: str | None = None,
    since: str | None = None,
    cursor: str | None = None,
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
):
    symbol_list = [symbol.strip().upper()]
    type_list = [event_type.strip().lower() for event_type in _parse_csv(types)]
    since_dt = _parse_since(since)

    q = _build_events_query(
        db=db,
        symbols=symbol_list,
        types=type_list,
        since=since_dt,
        cursor=cursor,
        limit=limit,
        extra_filters=[insider_visibility_clause()],
        congress_filters=[],
    )
    return _fetch_events_page(db, q, limit, enrich_prices=False)


@router.get("/watchlists/{id}/events", response_model=EventsPage, dependencies=[Depends(rate_limit_provider_backed)])
def list_watchlist_events(
    id: int,
    request: Request,
    db: Session = Depends(get_db),
    types: str | None = None,
    since: str | None = None,
    recent_days: int | None = Query(None, ge=1),
    cursor: str | None = None,
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
    unread_only: bool = False,
):
    user = current_user(db, request, required=True)
    watchlist = db.execute(
        select(Watchlist).where(Watchlist.id == id, Watchlist.owner_user_id == user.id)
    ).scalar_one_or_none()
    if not watchlist:
        raise HTTPException(status_code=404, detail="Watchlist not found")

    symbols = (
        db.execute(
            select(Security.symbol)
            .join(WatchlistItem, WatchlistItem.security_id == Security.id)
            .where(WatchlistItem.watchlist_id == id)
        )
        .scalars()
        .all()
    )

    if not symbols:
        return EventsPage(items=[], next_cursor=None)

    symbol_list = [symbol.upper() for symbol in symbols if symbol]
    type_list = [event_type.strip().lower() for event_type in _parse_csv(types)]
    since_dt = _parse_since(since)
    recent_days = recent_days if isinstance(recent_days, int) else None
    recent_since = datetime.now(timezone.utc) - timedelta(days=recent_days) if recent_days is not None else None
    effective_since = max([value for value in [since_dt, recent_since] if value is not None], default=None)
    extra_filters = []
    if unread_only:
        unread_event_ids = (
            db.execute(
                select(MonitoringAlert.event_id).where(
                    MonitoringAlert.user_id == user.id,
                    MonitoringAlert.source_type == "watchlist",
                    MonitoringAlert.source_id == str(id),
                    MonitoringAlert.read_at.is_(None),
                )
            )
            .scalars()
            .all()
        )
        if not unread_event_ids:
            return EventsPage(items=[], next_cursor=None)
        extra_filters.append(Event.id.in_([int(event_id) for event_id in unread_event_ids]))

    q = _build_events_query(
        db=db,
        symbols=symbol_list,
        types=type_list,
        since=effective_since,
        cursor=cursor,
        limit=limit,
        extra_filters=extra_filters,
        congress_filters=[],
        use_effective_activity_date=True,
    )
    page = _fetch_events_page(db, q, limit, use_effective_activity_date=True)

    if not _is_production_runtime() or is_admin_user(user):
        oldest_trade_date = min(
            (
                _first_text_field(item.payload or {}, "trade_date", "transaction_date", "tradeDate", "transactionDate")
                for item in page.items
            ),
            default=None,
        )
        oldest_report_date = min(
            (
                _first_text_field(item.payload or {}, "report_date", "filing_date", "reportDate", "filingDate")
                for item in page.items
            ),
            default=None,
        )
        logger.info(
            "watchlist_recent_activity watchlist_id=%s recent_days=%s since=%s effective_since=%s unread_only=%s returned=%s oldest_trade_date=%s oldest_report_date=%s sort_field=effective_activity_date_desc",
            id,
            recent_days,
            since,
            effective_since.isoformat() if effective_since is not None else None,
            unread_only,
            len(page.items),
            oldest_trade_date,
            oldest_report_date,
        )

    return page




@router.get("/insiders/{reporting_cik}/portfolio-performance", dependencies=[Depends(rate_limit_provider_backed)])
def insider_portfolio_performance(
    reporting_cik: str,
    db: Session = Depends(get_db),
    lookback_days: int = Query(1095),
    mode: str = "realistic_disclosure_lag",
    benchmark: str = "^GSPC",
    issuer: str | None = None,
):
    """Read-only replicated portfolio performance from persisted portfolio runs."""
    normalized_cik = normalize_cik(reporting_cik)
    if not normalized_cik:
        raise HTTPException(status_code=400, detail="Invalid reporting_cik.")
    normalized_mode = (mode or "realistic_disclosure_lag").strip()
    if normalized_mode not in {"realistic_disclosure_lag", "theoretical_transaction_date"}:
        raise HTTPException(status_code=400, detail="Unsupported portfolio mode.")
    issuer_cik = normalize_cik(issuer)
    issuer_symbol = normalize_symbol(issuer) if issuer and not issuer_cik else None
    return latest_replicated_portfolio_payload(
        db,
        entity_type="insider",
        entity_id=normalized_cik,
        issuer_cik=issuer_cik,
        issuer_symbol=issuer_symbol,
        lookback_days=lookback_days,
        mode=normalized_mode,
        benchmark=normalize_symbol(benchmark) or "^GSPC",
    )


@router.get("/insiders/{reporting_cik}/alpha-summary", dependencies=[Depends(rate_limit_provider_backed)])
def insider_alpha_summary(
    reporting_cik: str,
    db: Session = Depends(get_db),
    lookback_days: int = Query(90),
    benchmark: str = "^GSPC",
    issuer: str | None = None,
):
    matched = _load_insider_events_for_cik(db, reporting_cik, lookback_days, issuer=issuer)
    normalized_cik = normalize_cik(reporting_cik)
    benchmark_symbol = (benchmark or "^GSPC").strip() or "^GSPC"

    if not matched:
        return {
            "reporting_cik": normalized_cik,
            "lookback_days": lookback_days,
            "benchmark_symbol": benchmark_symbol,
            "metric_definitions": {
                "avg_return_pct": "Arithmetic mean of scored per-trade signed returns in the selected lookback.",
                "avg_alpha_pct": "Arithmetic mean of scored per-trade return minus S&P 500 return.",
                "profile_curve": "Equal-weight scored trade outcome curve, not a capital-constrained portfolio simulation.",
                "date_source": "trade_date",
                "hold_period": "Uses persisted or transient outcome holding_days through the latest/current scored price; no fixed hold_days selector.",
                "backtest_difference": "Backtests use disclosure or filing timing, configurable hold_days, monthly rebalancing, and portfolio CAGR/alpha.",
            },
            "trades_analyzed": 0,
            "avg_return_pct": None,
            "avg_alpha_pct": None,
            "win_rate": None,
            "avg_holding_days": None,
            "best_trades": [],
            "worst_trades": [],
            "member_series": [],
            "benchmark_series": [],
            "performance_series": [],
        }

    outcome_by_event_id, outcomes = _load_insider_trade_outcomes(
        db,
        matched,
        normalized_cik,
        benchmark_symbol,
        lookback_days,
    )

    end_date = datetime.now(timezone.utc).date()
    start_date = end_date - timedelta(days=max(lookback_days, 1))
    benchmark_close_map = get_eod_close_series(
        db=db,
        symbol=benchmark_symbol,
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
    )
    benchmark_dates = sorted(benchmark_close_map.keys())
    timeline_dates = build_timeline_dates(start_date, end_date)
    transient_outcomes = _transient_insider_trade_outcomes(
        db,
        matched,
        outcome_by_event_id,
        benchmark_symbol=benchmark_symbol,
        benchmark_close_map=benchmark_close_map,
        benchmark_dates=benchmark_dates,
    )
    analytics_outcomes = sorted(
        [*outcomes, *transient_outcomes],
        key=lambda row: (row.trade_date, row.event_id),
    )

    scored = [row for row in analytics_outcomes if row.return_pct is not None]
    return_values = [row.return_pct for row in scored if row.return_pct is not None]
    alpha_values = [row.alpha_pct for row in scored if row.alpha_pct is not None]
    holding_day_values = [row.holding_days for row in scored if isinstance(row.holding_days, int)]

    best_trade_rows, worst_trade_rows = rank_extreme_trade_outcomes(scored)
    best_trades = [_to_trade_outcome_trade_view(row) for row in best_trade_rows]
    worst_trades = [_to_trade_outcome_trade_view(row) for row in worst_trade_rows]

    curve = build_normalized_profile_curve(
        outcomes=analytics_outcomes,
        timeline_dates=timeline_dates,
        benchmark_close_map=benchmark_close_map,
        benchmark_dates=benchmark_dates,
        price_close_maps=load_profile_price_close_maps(
            db=db,
            outcomes=analytics_outcomes,
            start_date=start_date,
            end_date=end_date,
        ),
    )

    # Insider profile analytics mirror member profile semantics: cards average scored
    # trade outcomes individually, while the backtest endpoint reports a simulated
    # portfolio with disclosure-timed entries, configurable hold_days, and benchmark alpha.
    return {
        "reporting_cik": normalized_cik,
        "lookback_days": lookback_days,
        "benchmark_symbol": benchmark_symbol,
        "metric_definitions": {
            "avg_return_pct": "Arithmetic mean of scored per-trade signed returns in the selected lookback.",
            "avg_alpha_pct": "Arithmetic mean of scored per-trade return minus S&P 500 return.",
            "profile_curve": "Equal-weight scored trade outcome curve, not a capital-constrained portfolio simulation.",
            "date_source": "trade_date",
            "hold_period": "Uses persisted or transient outcome holding_days through the latest/current scored price; no fixed hold_days selector.",
            "backtest_difference": "Backtests use disclosure or filing timing, configurable hold_days, monthly rebalancing, and portfolio CAGR/alpha.",
        },
        "trades_analyzed": len(scored),
        "avg_return_pct": (sum(return_values) / len(return_values)) if return_values else None,
        "avg_alpha_pct": (sum(alpha_values) / len(alpha_values)) if alpha_values else None,
        "win_rate": (sum(1 for value in return_values if value > 0) / len(scored)) if scored else None,
        "avg_holding_days": (sum(holding_day_values) / len(holding_day_values)) if holding_day_values else None,
        "best_trades": best_trades,
        "worst_trades": worst_trades,
        "member_series": curve.member_series,
        "benchmark_series": curve.benchmark_series,
        "performance_series": curve.member_series,
    }

@router.get("/insiders/{reporting_cik}/summary", dependencies=[Depends(rate_limit_provider_backed)])
def insider_summary(
    reporting_cik: str,
    db: Session = Depends(get_db),
    lookback_days: int = Query(90),
    issuer: str | None = None,
):
    normalized_cik = normalize_cik(reporting_cik)
    cache_key = _insider_summary_cache_key(reporting_cik, lookback_days, issuer)
    cached_response = _insider_summary_cache_get(cache_key)
    if cached_response is not None:
        return cached_response
    inflight_state, inflight_leader = _insider_summary_inflight_start(cache_key)
    if cache_key and not inflight_leader and inflight_state is not None:
        event = inflight_state.get("event")
        if isinstance(event, threading.Event) and event.wait(timeout=max(INSIDER_SUMMARY_DEDUPE_WAIT_SECONDS, 0)):
            if inflight_state.get("error") is not None:
                raise inflight_state["error"]
            result = inflight_state.get("result")
            if isinstance(result, dict):
                return copy.deepcopy(result)
    matched = _load_insider_events_for_cik(db, reporting_cik, lookback_days, include_non_market_activity=True, issuer=issuer)
    if not matched:
        return _insider_summary_cache_finalize(cache_key, inflight_state, inflight_leader, {
            "reporting_cik": normalized_cik,
            "insider_name": None,
            "primary_company_name": None,
            "primary_role": None,
            "primary_symbol": None,
            "lookback_days": lookback_days,
            "total_trades": 0,
            "buy_count": 0,
            "sell_count": 0,
            "unique_tickers": 0,
            "gross_buy_value": 0,
            "gross_sell_value": 0,
            "net_flow": 0,
            "latest_filing_date": None,
            "latest_transaction_date": None,
        })

    buy_count = 0
    sell_count = 0
    gross_buy_value = 0.0
    gross_sell_value = 0.0
    symbol_counts: dict[str, int] = {}
    name_counts: dict[str, int] = {}
    role_counts: dict[str, int] = {}
    latest_transaction_date: str | None = None

    for event, payload in matched:
        trade_type = (event.trade_type or "").strip().lower()
        amount = float(event.amount_max or event.amount_min or 0)
        if trade_type == "purchase":
            buy_count += 1
            gross_buy_value += amount
        elif trade_type == "sale":
            sell_count += 1
            gross_sell_value += amount

        symbol = _event_symbol(event, payload)
        if symbol:
            symbol_counts[symbol] = symbol_counts.get(symbol, 0) + 1

        insider_name = _insider_display_name(event, payload)
        if insider_name:
            name_counts[insider_name] = name_counts.get(insider_name, 0) + 1

        role = _insider_role(payload)
        if role:
            role_counts[role] = role_counts.get(role, 0) + 1

        tx_date = _first_non_empty_text(
            payload.get("transaction_date"),
            payload.get("trade_date"),
            (payload.get("raw") or {}).get("transactionDate") if isinstance(payload.get("raw"), dict) else None,
        )
        if tx_date and (latest_transaction_date is None or tx_date > latest_transaction_date):
            latest_transaction_date = tx_date

    latest_filing_date = _insider_filing_date(matched[0][0], matched[0][1])
    latest_company_name = None
    latest_trade_row_company_name = None
    metadata_company_name = None
    primary_symbol = max(symbol_counts.items(), key=lambda item: item[1])[0] if symbol_counts else None
    if matched:
        insider_symbols = sorted(
            {
                symbol
                for event, payload in matched
                for symbol in [_event_symbol(event, payload)]
                if symbol
            }
        )
        ticker_meta = _ticker_meta_with_security_names(db, insider_symbols) if insider_symbols else {}
        insider_ciks = sorted(
            {
                cik
                for _, payload in matched
                for cik in [_event_cik(payload)]
                if cik
            }
        )
        cik_names = get_cik_meta(db, insider_ciks, allow_refresh=False) if insider_ciks else {}
        if primary_symbol and primary_symbol in ticker_meta:
            metadata_company_name = _first_non_empty_text((ticker_meta.get(primary_symbol) or {}).get("company_name"))
        if not metadata_company_name:
            primary_company_cik = next((cik for cik in insider_ciks if cik), None)
            if primary_company_cik:
                metadata_company_name = _first_non_empty_text(cik_names.get(primary_company_cik))

        latest_event, latest_payload = matched[0]
        resolved = _enrich_payload_company_name(latest_event, latest_payload, ticker_meta, cik_names)
        latest_trade_row_company_name = _first_non_empty_text(_insider_trade_row(latest_event, resolved).get("company_name"))
        latest_company_name = _insider_company_name(latest_event, resolved)

    primary_company_name = latest_company_name or latest_trade_row_company_name or metadata_company_name

    fallback_name = None
    fallback_role = None
    if matched:
        latest_payload = matched[0][1]
        fallback_name = _insider_display_name(matched[0][0], latest_payload)
        fallback_role = _insider_role(latest_payload)

    return _insider_summary_cache_finalize(cache_key, inflight_state, inflight_leader, {
        "reporting_cik": normalized_cik,
        "insider_name": (max(name_counts.items(), key=lambda item: item[1])[0] if name_counts else fallback_name),
        "primary_company_name": primary_company_name,
        "primary_role": (max(role_counts.items(), key=lambda item: item[1])[0] if role_counts else fallback_role),
        "primary_symbol": primary_symbol,
        "lookback_days": lookback_days,
        "total_trades": len(matched),
        "buy_count": buy_count,
        "sell_count": sell_count,
        "unique_tickers": len(symbol_counts),
        "gross_buy_value": round(gross_buy_value, 2),
        "gross_sell_value": round(gross_sell_value, 2),
        "net_flow": round(gross_buy_value - gross_sell_value, 2),
        "latest_filing_date": latest_filing_date,
        "latest_transaction_date": latest_transaction_date,
    })


@router.get("/insiders/{reporting_cik}/trades", dependencies=[Depends(rate_limit_provider_backed)])
def insider_trades(
    reporting_cik: str,
    db: Session = Depends(get_db),
    lookback_days: Annotated[int, Query()] = 90,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    page: Annotated[int, Query(ge=0, le=1000)] = 0,
    issuer: str | None = None,
):
    matched = _load_insider_events_for_cik(db, reporting_cik, lookback_days, include_non_market_activity=True, issuer=issuer)
    normalized_cik = normalize_cik(reporting_cik)
    offset = page * limit
    total = len(matched)
    visible = matched[offset:offset + limit]

    insider_symbols = sorted(
        {
            symbol
            for event, payload in visible
            for symbol in [_event_symbol(event, payload)]
            if symbol
        }
    )
    ticker_meta = _ticker_meta_with_security_names(db, insider_symbols) if insider_symbols else {}
    cik_values = sorted(
        {
            cik
            for event, payload in visible
            for cik in [_event_cik(payload)]
            if cik
        }
    )
    cik_names = get_cik_meta(db, cik_values, allow_refresh=False) if cik_values else {}
    enriched = [
        (event, _enrich_payload_company_name(event, payload, ticker_meta, cik_names))
        for event, payload in visible
    ]

    outcome_by_event_id, _ = _load_insider_trade_outcomes(
        db,
        enriched,
        normalized_cik,
        "^GSPC",
        lookback_days,
    )
    current_quote_meta = (
        get_current_prices_meta_db(
            db,
            insider_symbols,
            allow_cache_write=False,
            release_connection_before_fetch=True,
        )
        if insider_symbols
        else {}
    )
    quote_prices = {
        symbol: float(meta["price"])
        for symbol, meta in current_quote_meta.items()
        if isinstance(meta, dict) and meta.get("price") is not None
    }
    price_memo: dict[tuple[str, str], float | None] = {}

    items = []
    for event, payload in enriched:
        fallback_pnl_pct = None
        symbol = _event_symbol(event, payload)
        current_price = quote_prices.get(symbol or "")
        entry_price, _ = _insider_entry_price(event, payload, db, price_memo)
        if current_price is not None and entry_price is not None and entry_price > 0:
            fallback_pnl_pct = signed_return_pct(current_price, entry_price, event.trade_type or payload.get("trade_type"))
        items.append(
            _insider_trade_row(
                event,
                payload,
                outcome_by_event_id.get(event.id),
                fallback_pnl_pct,
                prefer_fallback_pnl=True,
            )
        )
    return {
        "reporting_cik": normalize_cik(reporting_cik),
        "lookback_days": lookback_days,
        "total": total,
        "page": page,
        "limit": limit,
        "has_next": offset + len(items) < total,
        "items": items,
    }


@router.get("/insiders/{reporting_cik}/top-tickers", dependencies=[Depends(rate_limit_provider_backed)])
def insider_top_tickers(
    reporting_cik: str,
    db: Session = Depends(get_db),
    lookback_days: int = Query(90),
    limit: int = Query(10, ge=1, le=50),
    issuer: str | None = None,
):
    matched = _load_insider_events_for_cik(db, reporting_cik, lookback_days, include_non_market_activity=True, issuer=issuer)
    by_symbol: dict[str, dict] = {}
    for event, payload in matched:
        symbol = _event_symbol(event, payload)
        if not symbol:
            continue
        row = by_symbol.get(symbol)
        if row is None:
            row = {
                "symbol": symbol,
                "company_name": _insider_company_name(event, payload),
                "trades": 0,
                "buy_count": 0,
                "sell_count": 0,
                "net_flow": 0.0,
            }
            by_symbol[symbol] = row
        row["trades"] += 1
        side = (event.trade_type or "").strip().lower()
        amount = float(event.amount_max or event.amount_min or 0)
        if side == "purchase":
            row["buy_count"] += 1
            row["net_flow"] += amount
        elif side == "sale":
            row["sell_count"] += 1
            row["net_flow"] -= amount
        if not row.get("company_name"):
            row["company_name"] = _insider_company_name(event, payload)

    items = sorted(by_symbol.values(), key=lambda row: row["trades"], reverse=True)[:limit]
    return {
        "reporting_cik": normalize_cik(reporting_cik),
        "lookback_days": lookback_days,
        "items": items,
    }
