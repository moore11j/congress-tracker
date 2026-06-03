from __future__ import annotations

import logging
import json
import math
import os
import re
import subprocess
import threading
import time
from contextlib import contextmanager
from statistics import mean, median
from time import perf_counter

from datetime import date, datetime, timezone, timedelta
from pathlib import Path

from fastapi import FastAPI, Depends, Query, HTTPException, Request
from fastapi.responses import JSONResponse
from sqlalchemy import select, func, and_, or_, text, bindparam, String, Float, Integer, case, literal
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError, OperationalError, TimeoutError as SATimeoutError
from pydantic import BaseModel
import requests

from app.db import (
    Base,
    DATABASE_URL,
    SessionLocal,
    engine,
    ensure_email_notification_schema,
    ensure_event_columns,
    ensure_fundamentals_cache_schema,
    ensure_house_annual_disclosure_schema,
    ensure_monitoring_alert_columns,
    ensure_price_cache_volume_columns,
    ensure_trade_outcomes_amount_bigint,
    get_db,
    is_database_locked_error,
)
from app.ingest.government_contracts import ensure_government_contracts_schema
from app.auth import current_user, require_admin_user
from app.entitlements import (
    current_entitlements,
    enforce_limit,
    entitlements_for_user,
    entitlement_payload,
    require_monitored_watchlist_source,
    require_feature,
    monitored_source_ids,
    seed_plan_config,
)
from app.rate_limit import rate_limit_notification_mutation, rate_limit_provider_backed
from app.security.startup_checks import (
    DEFAULT_LOCAL_FRONTEND_ORIGINS as _DEFAULT_LOCAL_FRONTEND_ORIGINS,
    DEFAULT_PRODUCTION_FRONTEND_ORIGINS as _DEFAULT_PRODUCTION_FRONTEND_ORIGINS,
    cors_allowed_origins,
    is_production,
    runtime_environment,
    split_origins,
    validate_startup_security_config,
)
from app.services.email_templates import seed_default_email_templates
from app.models import (
    AppSetting,
    CongressMemberAlias,
    ConfirmationMonitoringEvent,
    ConfirmationMonitoringSnapshot,
    Event,
    Filing,
    Member,
    MonitoringAlert,
    ReplicatedPortfolioRun,
    SavedScreen,
    Security,
    TradeOutcome,
    Transaction,
    UserAccount,
    Watchlist,
    WatchlistItem,
    WatchlistViewState,
)
from app.ingest_congress_recent import CONGRESS_RECENT_STATUS_KEY
from app.routers.accounts import router as accounts_router
from app.routers.backtests import router as backtests_router
from app.routers.debug import router as debug_router
from app.routers.notifications import router as notifications_router
from app.routers.saved_screens import router as saved_screens_router
from app.routers.screener import router as screener_router
from app.routers.events import (
    _enrich_payload_company_name as _enrich_event_payload_company_name,
    _event_cik as _event_payload_cik,
    _event_symbol as _event_payload_symbol,
    _insider_filing_date,
    _insider_trade_row,
    _load_insider_events_for_cik,
    _ticker_meta_with_security_names,
    router as events_router,
)
from app.routers.signals import (
    CONGRESS_SIGNAL_DEFAULTS,
    INSIDER_DEFAULTS,
    _query_unified_signals,
    router as signals_router,
)
from app.clients.fmp import FMP_BASE_URL
from app.services.price_lookup import (
    get_close_for_date_or_prior,
    get_daily_close_series_with_fallback,
    get_daily_volume_series_from_provider,
    get_eod_close,
    get_eod_close_series,
)
from app.services.quote_lookup import get_current_prices, get_current_prices_db
from app.services.government_contracts import get_government_contracts_for_symbol
from app.services.government_departments import get_department_profile, list_departments
from app.services.congress_metadata import get_congress_metadata_resolver
from app.services.congress_assets import (
    CONGRESS_DISCLOSURE_EVENT_TYPES,
    CONGRESS_NON_EQUITY_EVENT_TYPES,
    canonical_asset_class_value,
    classify_congress_disclosure_asset,
)
from app.services.returns import signed_return_pct
from app.services.trade_outcomes import (
    count_member_trade_outcomes,
    dedupe_member_trade_outcomes,
    get_member_trade_outcomes,
)
from app.services.trade_outcome_display import (
    normalize_trade_side,
    trade_outcome_display_metrics,
    trade_outcome_logical_key,
)
from app.services.congress_outcome_eligibility import congress_equity_outcome_eligibility
from app.services.foreign_trade_normalization import normalize_insider_price
from app.services.profile_performance_curve import build_normalized_profile_curve, build_timeline_dates, load_profile_price_close_maps
from app.services.replicated_portfolios import PORTFOLIO_METHODOLOGY_VERSION, latest_replicated_portfolio_payload
from app.services.signal_score import calculate_smart_score
from app.services.confirmation_metrics import get_confirmation_metrics_for_symbols
from app.services.confirmation_score import (
    get_confirmation_score_bundle_for_ticker,
    inactive_confirmation_score_bundle,
)
from app.services.options_flow import get_options_flow_summary, unavailable_options_flow_summary
from app.services.signal_freshness import build_signal_freshness_bundle
from app.services.technical_indicators import build_ticker_technical_indicators
from app.services.ticker_events import (
    GOVERNMENT_CONTRACT_EVENT_TYPES,
    select_visible_ticker_events,
    ticker_event_date_key,
)
from app.services.ticker_identity import resolve_ticker_identity, safe_company_identity_candidate
from app.services.confirmation_monitoring import (
    event_to_dict as confirmation_monitoring_event_to_dict,
    refresh_watchlist_confirmation_monitoring,
)
from app.services.monitoring_alerts import (
    alert_to_dict as monitoring_alert_to_dict,
    ensure_alerts_for_saved_screen_events,
    mark_alert_read,
    dismiss_alerts,
    mark_alerts_read,
    mark_alerts_unread,
    mark_alert_unread,
    mark_watchlist_source_read,
    mark_watchlist_source_unread,
    recent_alerts,
    refresh_watchlist_alerts,
    unread_count,
    unread_count_by_source,
    watchlist_unread_count,
    watchlist_unread_counts,
    watchlist_unread_summary,
)
from app.services.why_now import build_why_now_bundle
from app.services.ticker_meta import get_cik_meta, get_ticker_meta
from app.services.fmp_market_snapshot import get_macro_snapshot
from app.services.fmp_news import get_general_news, get_press_releases, get_sec_filings, get_stock_news
from app.services.ticker_financials import get_ticker_financials
from app.utils.symbols import normalize_symbol

logger = logging.getLogger(__name__)

_CONGRESS_IDENTITY_CACHE: dict[tuple, dict] = {}
_TICKER_QUOTE_SNAPSHOT_CACHE: dict[str, tuple[float, dict]] = {}
_TICKER_RATIOS_TTM_CACHE: dict[str, tuple[float, dict]] = {}
_TICKER_PROFILE_SNAPSHOT_CACHE: dict[str, tuple[float, dict]] = {}
_TICKER_BENCHMARK_SYMBOL = "^GSPC"
_TICKER_BENCHMARK_LABEL = "S&P 500"
_TICKER_IDENTITY_MANUAL_ALIASES = {
    "INFQ": "Infleqtion Inc.",
    "NBIS": "Nebius Group N.V.",
}
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


class _LeaderboardPerfTracker:
    """Lightweight per-request perf tracker for leaderboard stages."""

    def __init__(self, *, mode: str, lookback_days: int, min_trades: int, limit: int):
        self.mode = mode
        self.lookback_days = lookback_days
        self.min_trades = min_trades
        self.limit = limit
        self._t0 = perf_counter()
        self._stage_start = self._t0
        self._stages: list[dict] = []

    def stage(self, name: str, rows: int | None = None) -> None:
        now = perf_counter()
        elapsed_ms = round((now - self._stage_start) * 1000, 2)
        entry = {"stage": name, "elapsed_ms": elapsed_ms}
        if rows is not None:
            entry["rows"] = rows
        self._stages.append(entry)
        logger.info(
            "leaderboard_stage mode=%s lookback_days=%s min_trades=%s limit=%s stage=%s rows=%s elapsed_ms=%.2f",
            self.mode,
            self.lookback_days,
            self.min_trades,
            self.limit,
            name,
            rows if rows is not None else "na",
            elapsed_ms,
        )
        self._stage_start = now

    def finish(self, *, result_rows: int) -> None:
        total_elapsed_ms = round((perf_counter() - self._t0) * 1000, 2)
        logger.info(
            "leaderboard_perf mode=%s lookback_days=%s min_trades=%s limit=%s result_rows=%s total_elapsed_ms=%.2f stages=%s",
            self.mode,
            self.lookback_days,
            self.min_trades,
            self.limit,
            result_rows,
            total_elapsed_ms,
            self._stages,
        )

def _cap_symbols(symbols: set[str]) -> list[str]:
    try:
        limit = int(os.getenv("MAX_SYMBOLS_PER_REQUEST", "25"))
    except ValueError:
        limit = 25
    return sorted(symbols)[: max(limit, 1)]


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


def _estimated_trade_value(amount_min: object, amount_max: object) -> float | None:
    min_value = _parse_numeric(amount_min)
    max_value = _parse_numeric(amount_max)
    if min_value is not None and max_value is not None:
        return (min_value + max_value) / 2
    return max_value if max_value is not None else min_value


def _estimated_shares(amount_min: object, amount_max: object, estimated_price: object) -> float | None:
    price = _parse_numeric(estimated_price)
    trade_value = _estimated_trade_value(amount_min, amount_max)
    if price is None or price <= 0 or trade_value is None or trade_value <= 0:
        return None
    return trade_value / price


def _congress_baseline_map_for_symbols(
    db: Session,
    symbols: list[str],
    *,
    baseline_days: int = 365,
    min_baseline_count: int = 3,
) -> dict[str, tuple[float, int]]:
    normalized_symbols = sorted({symbol.strip().upper() for symbol in symbols if symbol and symbol.strip()})
    if not normalized_symbols:
        return {}

    baseline_since = datetime.now(timezone.utc) - timedelta(days=baseline_days)
    baseline_sq = text(
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

    rows = db.execute(
        select(
            baseline_sq.c.symbol,
            baseline_sq.c.median_amount_max,
            baseline_sq.c.baseline_count,
        ).where(baseline_sq.c.symbol.in_(normalized_symbols))
    ).all()

    return {
        row.symbol: (float(row.median_amount_max), int(row.baseline_count))
        for row in rows
        if row.symbol and row.median_amount_max and int(row.baseline_count or 0) >= min_baseline_count
    }


def _feed_entry_price_for_event(
    db: Session,
    event: Event,
    payload: dict,
    price_memo: dict[tuple[str, str], float | None],
) -> tuple[str, float | None, float | None]:
    if event.event_type in CONGRESS_NON_EQUITY_EVENT_TYPES:
        return "", None, None
    sym = (event.symbol or payload.get("symbol") or "").strip().upper()
    if event.event_type == "congress_trade":
        trade_date = payload.get("trade_date") or payload.get("transaction_date")
        eligibility = congress_equity_outcome_eligibility(
            event_type=event.event_type,
            symbol=sym,
            payload=payload,
            trade_date=trade_date,
            side=event.trade_type or event.transaction_type,
            amount_min=event.amount_min,
            amount_max=event.amount_max,
        )
        if eligibility.eligible and sym and trade_date:
            key = (sym, trade_date)
            if key not in price_memo:
                price_memo[key] = get_eod_close(db, sym, trade_date)
            entry_price = price_memo[key]
        else:
            entry_price = None
        return sym, entry_price, entry_price

    if event.event_type == "insider_trade":
        trade_date = payload.get("transaction_date") or payload.get("trade_date")
        normalized = normalize_insider_price(symbol=sym, payload=payload, trade_date=trade_date)
        if normalized.is_comparable:
            return sym, normalized.display_price, None

        if sym and trade_date:
            key = (sym, trade_date)
            if key not in price_memo:
                price_memo[key] = get_eod_close(db, sym, trade_date)
            entry_price = price_memo[key]
            if entry_price is not None and entry_price > 0:
                return sym, entry_price, None

    return sym, None, None

def _extract_district(member: Member) -> str | None:
    if (member.chamber or "").lower() != "house":
        return None
    bioguide = (member.bioguide_id or "").upper()
    if not bioguide.startswith("FMP_HOUSE_"):
        return None
    suffix = bioguide[len("FMP_HOUSE_"):]
    if len(suffix) < 4:
        return None
    state = suffix[:2]
    district = suffix[2:]
    if not state.isalpha() or not district.isdigit():
        return None
    return district


def _member_payload(member: Member) -> dict:
    return {
        "bioguide_id": member.bioguide_id,
        "member_id": member.id,
        "name": f"{member.first_name or ''} {member.last_name or ''}".strip(),
        "party": member.party,
        "state": member.state,
        "district": _extract_district(member),
        "chamber": member.chamber,
    }

def _top_member_payload(member: Member) -> dict:
    member_identifier = (member.bioguide_id or "").strip()
    payload = {
        "member_id": member_identifier,
        "name": f"{member.first_name or ''} {member.last_name or ''}".strip(),
        "party": member.party,
        "state": member.state,
        "district": _extract_district(member),
        "chamber": member.chamber,
    }
    if member_identifier and not member_identifier.upper().startswith("FMP_"):
        payload["bioguide_id"] = member_identifier
    return payload


def _member_full_name(member: Member) -> str:
    return f"{member.first_name or ''} {member.last_name or ''}".strip()


def _normalize_name(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9 ]+", " ", value.upper())
    return re.sub(r"\s+", " ", cleaned).strip()


def _clean_metadata_value(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = value.strip()
    return cleaned or None


def _normalize_party(value: str | None) -> str | None:
    cleaned = _clean_metadata_value(value)
    if not cleaned:
        return None

    normalized = re.sub(r"[^A-Za-z]", "", cleaned).upper()
    if normalized in {"D", "DEM", "DEMOCRAT", "DEMOCRATIC"}:
        return "DEMOCRAT"
    if normalized in {"R", "REP", "REPUBLICAN"}:
        return "REPUBLICAN"
    if normalized in {"I", "IND", "INDEPENDENT", "INDEPENDENCE"}:
        return "INDEPENDENT"
    return cleaned.upper()


def _merge_member_metadata(
    target: dict,
    chamber: str | None,
    party: str | None,
    state: str | None = None,
) -> None:
    resolved_chamber = _clean_metadata_value(chamber)
    resolved_party = _normalize_party(party)
    resolved_state = _clean_metadata_value(state)

    if not target.get("chamber") and resolved_chamber:
        target["chamber"] = resolved_chamber
    if not target.get("party") and resolved_party:
        target["party"] = resolved_party
    if not target.get("state") and resolved_state:
        target["state"] = resolved_state


def _slug_to_name(slug: str) -> str:
    return _normalize_name(slug.replace("_", " "))


def _legacy_member_identity_parts(member_id: str) -> dict[str, str | None]:
    raw = (member_id or "").strip()
    upper = raw.upper()
    if not upper.startswith("FMP_"):
        return {
            "chamber": None,
            "state": None,
            "house_district": None,
            "full_name": None,
            "first_name": None,
            "last_name": None,
        }

    chunks = [chunk for chunk in upper.split("_") if chunk]
    chamber = chunks[1].lower() if len(chunks) > 1 else None
    state = chunks[2] if len(chunks) > 2 else None
    house_district = None
    first_name = None
    last_name = None
    full_name = None

    if chamber == "house" and state and len(state) >= 4 and state[:2].isalpha() and state[2:].isdigit():
        house_district = state
        state = state[:2]
    elif chamber == "house" and state and len(chunks) > 3 and chunks[3].isdigit():
        house_district = f"{state}{chunks[3]}"

    name_start = 3
    if len(chunks) > 4 and chunks[3].isdigit():
        name_start = 4
    name_tokens = chunks[name_start:]
    if name_tokens:
        titled = [token.title() for token in name_tokens]
        first_name = titled[0]
        last_name = titled[-1] if len(titled) > 1 else titled[0]
        full_name = " ".join(titled)

    return {
        "chamber": chamber,
        "state": state,
        "house_district": house_district,
        "full_name": full_name,
        "first_name": first_name,
        "last_name": last_name,
    }


def _resolve_member_legacy_compat(db: Session, requested_member_id: str) -> Member | None:
    member_id = (requested_member_id or "").strip()
    if not member_id:
        return None

    direct = db.execute(select(Member).where(Member.bioguide_id == member_id)).scalar_one_or_none()
    if direct:
        return direct

    case_insensitive = db.execute(
        select(Member).where(func.lower(Member.bioguide_id) == member_id.lower())
    ).scalar_one_or_none()
    if case_insensitive:
        logger.info(
            "member_profile legacy fallback hit: id_casefold requested=%s resolved=%s",
            member_id,
            case_insensitive.bioguide_id,
        )
        return case_insensitive

    legacy_parts = _legacy_member_identity_parts(member_id)
    if member_id.upper().startswith("FMP_"):
        try:
            metadata = get_congress_metadata_resolver()
            resolved = metadata.resolve(
                bioguide_id=member_id,
                first_name=legacy_parts["first_name"],
                last_name=legacy_parts["last_name"],
                full_name=legacy_parts["full_name"],
                chamber=legacy_parts["chamber"],
                state=legacy_parts["state"],
                house_district=legacy_parts["house_district"],
            )
            if resolved and resolved.bioguide_id:
                canonical = db.execute(
                    select(Member).where(Member.bioguide_id == resolved.bioguide_id)
                ).scalar_one_or_none()
                if canonical:
                    logger.info(
                        "member_profile legacy fallback hit: metadata requested=%s resolved=%s",
                        member_id,
                        canonical.bioguide_id,
                    )
                    return canonical
        except Exception:
            logger.warning(
                "member_profile legacy fallback metadata lookup failed for requested=%s",
                member_id,
                exc_info=True,
            )

    event_hint = db.execute(
        select(Event.member_name, Event.chamber, Event.party)
        .where(Event.member_bioguide_id == member_id)
        .order_by(Event.id.desc())
        .limit(1)
    ).first()
    outcome_hint = db.execute(
        select(TradeOutcome.member_name)
        .where(TradeOutcome.member_id == member_id)
        .order_by(TradeOutcome.id.desc())
        .limit(1)
    ).first()
    hinted_name = (event_hint.member_name if event_hint else None) or (outcome_hint.member_name if outcome_hint else None)
    normalized_name = _normalize_name(hinted_name or "")
    if normalized_name:
        members = db.execute(select(Member)).scalars().all()
        matched = [member for member in members if _normalize_name(_member_full_name(member)) == normalized_name]
        if event_hint and event_hint.chamber:
            narrowed = [m for m in matched if (m.chamber or "").lower() == (event_hint.chamber or "").lower()]
            if narrowed:
                matched = narrowed
        if matched:
            logger.info(
                "member_profile legacy fallback hit: event/outcome hint requested=%s resolved=%s",
                member_id,
                matched[0].bioguide_id,
            )
            return matched[0]

    return None



def _resolve_member_analytics_aliases(db: Session, requested_member_id: str) -> tuple[Member | None, list[str]]:
    requested = (requested_member_id or "").strip()
    if not requested:
        return None, []

    aliases: set[str] = {requested}
    resolved_member = _resolve_member_legacy_compat(db, requested)
    if resolved_member and resolved_member.bioguide_id:
        aliases.add(resolved_member.bioguide_id)

    full_name = _member_full_name(resolved_member) if resolved_member else None
    if full_name:
        lower_name = full_name.lower()

        outcome_alias_rows = db.execute(
            select(TradeOutcome.member_id)
            .where(TradeOutcome.member_id.is_not(None))
            .where(func.lower(TradeOutcome.member_name) == lower_name)
            .group_by(TradeOutcome.member_id)
        ).all()
        for (candidate_id,) in outcome_alias_rows:
            if candidate_id:
                aliases.add(candidate_id)

        event_alias_query = (
            select(Event.member_bioguide_id)
            .where(Event.member_bioguide_id.is_not(None))
            .where(func.lower(Event.member_name) == lower_name)
        )
        if resolved_member and resolved_member.chamber:
            event_alias_query = event_alias_query.where(func.lower(Event.chamber) == (resolved_member.chamber or "").lower())
        event_alias_rows = db.execute(event_alias_query.group_by(Event.member_bioguide_id)).all()
        for (candidate_id,) in event_alias_rows:
            if candidate_id:
                aliases.add(candidate_id)

    alias_list = sorted(aliases)
    if len(alias_list) > 1 or (resolved_member and requested != (resolved_member.bioguide_id or requested)):
        logger.info(
            "member_analytics alias resolution hit: requested=%s canonical=%s aliases=%s",
            requested,
            resolved_member.bioguide_id if resolved_member else None,
            alias_list,
        )

    return resolved_member, alias_list


def _congress_identity_cache_key(db: Session, normalized_chamber: str) -> tuple:
    members_q = select(func.max(Member.id), func.count(Member.id)).where(Member.bioguide_id.is_not(None))
    if normalized_chamber in {"house", "senate"}:
        members_q = members_q.where(func.lower(Member.chamber) == normalized_chamber)
    members_max_id, members_count = db.execute(members_q).one()
    events_max_id = db.execute(
        select(func.max(Event.id)).where(Event.event_type == "congress_trade")
    ).scalar_one()
    outcomes_max_id = db.execute(select(func.max(TradeOutcome.id))).scalar_one()
    return (
        normalized_chamber,
        int(members_count or 0),
        int(members_max_id or 0),
        int(events_max_id or 0),
        int(outcomes_max_id or 0),
    )


def _build_congress_identity_snapshot(db: Session, normalized_chamber: str) -> dict:
    members_q = select(Member).where(Member.bioguide_id.is_not(None))
    if normalized_chamber in {"house", "senate"}:
        members_q = members_q.where(func.lower(Member.chamber) == normalized_chamber)
    members = db.execute(members_q).scalars().all()

    logical_member_aliases: dict[str, set[str]] = {}
    logical_member_profiles: dict[str, Member] = {}
    all_aliases: set[str] = set()
    for member in members:
        resolved_member, aliases = _resolve_member_analytics_aliases(db, member.bioguide_id)
        logical_member_id = (
            resolved_member.bioguide_id
            if resolved_member and resolved_member.bioguide_id
            else member.bioguide_id
        )
        if not logical_member_id:
            continue

        member_choice = _prefer_member_identity(resolved_member, member)
        logical_member_profiles[logical_member_id] = _prefer_member_identity(
            member_choice,
            logical_member_profiles.get(logical_member_id),
        ) or member
        logical_aliases = logical_member_aliases.setdefault(logical_member_id, set())
        if member.bioguide_id:
            logical_aliases.add(member.bioguide_id)
        if resolved_member and resolved_member.bioguide_id:
            logical_aliases.add(resolved_member.bioguide_id)
        logical_aliases.update(aliases or [member.bioguide_id])
        all_aliases.update(logical_aliases)

    merged_logical_aliases: dict[str, set[str]] = {}
    merged_logical_profiles: dict[str, Member] = {}
    alias_to_group_key: dict[str, str] = {}

    for logical_member_id, aliases_set in logical_member_aliases.items():
        aliases = {alias for alias in aliases_set if alias}
        if logical_member_id:
            aliases.add(logical_member_id)
        if not aliases:
            continue

        group_keys = {alias_to_group_key[alias] for alias in aliases if alias in alias_to_group_key}
        if logical_member_id in merged_logical_aliases:
            group_keys.add(logical_member_id)

        if group_keys:
            target_group_key = sorted(
                group_keys,
                key=lambda value: (_is_legacy_fmp_member_id(value), value),
            )[0]
        else:
            target_group_key = logical_member_id

        target_aliases = merged_logical_aliases.setdefault(target_group_key, set())
        target_aliases.update(aliases)

        chosen_profile = _prefer_member_identity(
            logical_member_profiles.get(logical_member_id),
            merged_logical_profiles.get(target_group_key),
        )
        if chosen_profile is not None:
            merged_logical_profiles[target_group_key] = chosen_profile

        for group_key in sorted(group_keys):
            if group_key == target_group_key:
                continue
            existing_aliases = merged_logical_aliases.pop(group_key, set())
            target_aliases.update(existing_aliases)
            existing_profile = merged_logical_profiles.pop(group_key, None)
            preferred_profile = _prefer_member_identity(existing_profile, merged_logical_profiles.get(target_group_key))
            if preferred_profile is not None:
                merged_logical_profiles[target_group_key] = preferred_profile

        for alias in target_aliases:
            alias_to_group_key[alias] = target_group_key

    profile_rows: dict[str, dict[str, str | None]] = {}
    for group_key, member in merged_logical_profiles.items():
        profile_rows[group_key] = {
            "member_name": _member_full_name(member) or group_key,
            "member_slug": group_key,
            "chamber": _clean_metadata_value(member.chamber),
            "party": _normalize_party(member.party),
            "state": _clean_metadata_value(member.state),
        }

    return {
        "candidate_member_count": len(members),
        "logical_member_count": len(logical_member_aliases),
        "merged_group_count": len(merged_logical_aliases),
        "all_aliases": sorted(alias for alias in all_aliases if alias),
        "alias_to_group_key": alias_to_group_key,
        "merged_aliases": {
            key: tuple(sorted(alias for alias in aliases if alias))
            for key, aliases in merged_logical_aliases.items()
        },
        "profiles": profile_rows,
    }


def _get_congress_identity_snapshot(db: Session, normalized_chamber: str) -> tuple[dict, bool]:
    cache_key = _congress_identity_cache_key(db, normalized_chamber)
    cached = _CONGRESS_IDENTITY_CACHE.get(cache_key)
    if cached is not None:
        return cached, True

    snapshot = _build_congress_identity_snapshot(db, normalized_chamber)
    _CONGRESS_IDENTITY_CACHE.clear()
    _CONGRESS_IDENTITY_CACHE[cache_key] = snapshot
    return snapshot, False


def _is_legacy_fmp_member_id(member_id: str | None) -> bool:
    normalized = (member_id or "").strip().upper()
    return normalized.startswith("FMP_")


_ORPHANED_FMP_COMMA_FRAGMENT_MEMBER_IDS = {
    "FMP_SENATE_XX_JUSTICE_II",
    "__JAMES_CONLEY_(SENATOR)",
    "FMP_SENATE_XX_MORENO",
    "_BERNARDO_(SENATOR)",
}


def _is_orphaned_fmp_comma_fragment_member_id(member_id: str | None) -> bool:
    return (member_id or "").strip().upper() in _ORPHANED_FMP_COMMA_FRAGMENT_MEMBER_IDS


def _prefer_member_identity(candidate: Member | None, current: Member | None) -> Member | None:
    if candidate is None:
        return current
    if current is None:
        return candidate

    candidate_is_canonical = not _is_legacy_fmp_member_id(candidate.bioguide_id)
    current_is_canonical = not _is_legacy_fmp_member_id(current.bioguide_id)
    if candidate_is_canonical and not current_is_canonical:
        return candidate
    if current_is_canonical and not candidate_is_canonical:
        return current

    return candidate


def _normalized_trade_side_sql(trade_type_column):
    normalized = func.lower(func.trim(func.coalesce(trade_type_column, "")))
    return case(
        (normalized.in_(["sale", "s-sale", "sell", "s"]), literal("sale")),
        (normalized.in_(["purchase", "p-purchase", "buy", "p"]), literal("purchase")),
        else_=normalized,
    )


def _leaderboard_sort_value_sql(columns, normalized_sort: str):
    if normalized_sort == "trade_count":
        return columns.trade_count_total
    if normalized_sort == "avg_return":
        return func.coalesce(columns.avg_return, float("-inf"))
    if normalized_sort == "win_rate":
        return func.coalesce(columns.win_rate, float("-inf"))
    return func.coalesce(columns.avg_alpha, float("-inf"))


def _normalize_portfolio_leaderboard_sort(sort: str | None) -> str:
    normalized = (sort or "alpha_pct").strip().lower()
    aliases = {
        "total_return": "total_return_pct",
        "return": "total_return_pct",
        "alpha": "alpha_pct",
        "cagr": "cagr_pct",
        "sharpe": "sharpe_ratio",
        "max_drawdown": "max_drawdown_pct",
        "drawdown": "max_drawdown_pct",
        "win_rate": "win_rate_pct",
        "positions": "positions_count",
        "trade_count": "positions_count",
        "skipped_events": "skipped_events_count",
        "skipped": "skipped_events_count",
    }
    normalized = aliases.get(normalized, normalized)
    valid_sorts = {
        "total_return_pct",
        "alpha_pct",
        "cagr_pct",
        "sharpe_ratio",
        "max_drawdown_pct",
        "win_rate_pct",
        "positions_count",
        "skipped_events_count",
    }
    return normalized if normalized in valid_sorts else "alpha_pct"


def _portfolio_sort_lower_is_better(normalized_sort: str) -> bool:
    return normalized_sort in {"max_drawdown_pct", "skipped_events_count"}


def _portfolio_run_curve_quality_status(run: ReplicatedPortfolioRun) -> str:
    if run.status_message:
        try:
            parsed = json.loads(run.status_message)
            diagnostics = parsed.get("curve_diagnostics") if isinstance(parsed, dict) else None
            status = diagnostics.get("curve_quality_status") if isinstance(diagnostics, dict) else None
            normalized = str(status or "").strip().lower()
            if normalized in {"good", "warning", "poor"}:
                return normalized
        except Exception:
            pass
    return "good"


def _portfolio_run_status_payload(run: ReplicatedPortfolioRun) -> dict:
    if not run.status_message:
        return {}
    try:
        parsed = json.loads(run.status_message)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _is_finite_portfolio_number(value: object) -> bool:
    try:
        numeric = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return False
    return math.isfinite(numeric)


def _is_positive_portfolio_number(value: object) -> bool:
    try:
        numeric = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return False
    return math.isfinite(numeric) and numeric > 0


def _portfolio_run_public_safety_flags(run: ReplicatedPortfolioRun) -> list[str]:
    flags: list[str] = []
    payload = _portfolio_run_status_payload(run)
    diagnostics = payload.get("curve_diagnostics") if isinstance(payload, dict) else {}
    warmup = payload.get("warmup_diagnostics") if isinstance(payload, dict) else {}
    effective_window = payload.get("effective_window") if isinstance(payload, dict) else {}
    diagnostics = diagnostics if isinstance(diagnostics, dict) else {}
    warmup = warmup if isinstance(warmup, dict) else {}
    effective_window = effective_window if isinstance(effective_window, dict) else {}

    positions_count = int(run.positions_count or 0)
    starting_value = run.starting_value
    ending_value = run.ending_value
    benchmark_ending_value = run.benchmark_ending_value
    max_single_day_jump = abs(float(diagnostics.get("max_single_day_return_jump_pct") or 0.0))

    if run.status != "ok":
        flags.append("run_status_not_ok")
    if run.methodology_version != PORTFOLIO_METHODOLOGY_VERSION:
        flags.append("stale_methodology")
    if int(run.points_count or 0) <= 0:
        flags.append("no_chart_points")
    if not all(
        _is_finite_portfolio_number(value)
        for value in (run.total_return_pct, run.cagr_pct, run.alpha_pct, run.benchmark_return_pct)
    ):
        flags.append("missing_return_fields")
    if not all(
        _is_positive_portfolio_number(value)
        for value in (starting_value, ending_value, benchmark_ending_value)
    ):
        flags.append("invalid_portfolio_value")
    if bool(effective_window.get("no_active_holdings")) and positions_count > 0:
        flags.append("positions_without_active_curve")
    if max_single_day_jump > 250.0:
        flags.append("single_day_return_jump_outlier")

    return flags


def _portfolio_payload_public_safety_flags(payload: dict) -> list[str]:
    flags: list[str] = []
    summary = payload.get("summary") if isinstance(payload, dict) else {}
    summary = summary if isinstance(summary, dict) else {}
    warmup = payload.get("warmup_diagnostics") if isinstance(payload, dict) else {}
    warmup = warmup if isinstance(warmup, dict) else {}

    points = payload.get("points") if isinstance(payload, dict) else []
    points = points if isinstance(points, list) else []
    positions_count = int(summary.get("positions_count") or len(payload.get("positions") or []))
    starting_value = summary.get("starting_value") or payload.get("starting_value")
    ending_value = summary.get("ending_value") or payload.get("ending_value")
    benchmark_ending_value = summary.get("benchmark_ending_value") or payload.get("benchmark_ending_value")
    max_single_day_jump = abs(float(payload.get("max_single_day_return_jump_pct") or 0.0))

    if payload.get("status") != "ok":
        flags.append("run_status_not_ok")
    if payload.get("methodology_current") is False or payload.get("stale_methodology") is True:
        flags.append("stale_methodology")
    if len(points) <= 0 or int(summary.get("points_count") or 0) <= 0:
        flags.append("no_chart_points")
    if not all(
        _is_finite_portfolio_number(value)
        for value in (
            summary.get("total_return_pct"),
            summary.get("cagr_pct"),
            summary.get("alpha_pct"),
            summary.get("benchmark_return_pct"),
        )
    ):
        flags.append("missing_return_fields")
    if not all(
        _is_positive_portfolio_number(value)
        for value in (starting_value, ending_value, benchmark_ending_value)
    ):
        flags.append("invalid_portfolio_value")
    for point in points:
        if not isinstance(point, dict):
            flags.append("invalid_portfolio_value")
            break
        if not _is_positive_portfolio_number(point.get("strategy_value")):
            flags.append("invalid_portfolio_value")
            break
    if bool(payload.get("no_active_holdings")) and positions_count > 0:
        flags.append("positions_without_active_curve")
    if max_single_day_jump > 250.0:
        flags.append("single_day_return_jump_outlier")

    return flags


def _unavailable_portfolio_payload(payload: dict, flags: list[str]) -> dict:
    safe_payload = dict(payload)
    safe_payload.update(
        {
            "status": "unavailable",
            "summary": None,
            "points": [],
            "positions": [],
            "public_safety_flags": flags,
            "message": "Portfolio simulation is temporarily unavailable while this run is revalidated.",
        }
    )
    return safe_payload


def _load_congress_portfolio_identity_rows(
    db: Session,
    *,
    normalized_chamber: str,
) -> tuple[dict[str, dict], int]:
    alias_metadata: dict[str, dict] = {}
    expected_logical_members = 0

    alias_query = select(CongressMemberAlias)
    if normalized_chamber in {"house", "senate"}:
        alias_query = alias_query.where(func.lower(CongressMemberAlias.chamber) == normalized_chamber)
    alias_rows = db.execute(alias_query).scalars().all()
    if alias_rows:
        expected_logical_members = len({row.group_key for row in alias_rows if row.group_key})
        for row in alias_rows:
            alias_member_id = (row.alias_member_id or "").strip()
            if not alias_member_id:
                continue
            authoritative_member_id = (row.authoritative_member_id or row.group_key or alias_member_id).strip()
            alias_metadata[alias_member_id] = {
                "group_key": row.group_key or authoritative_member_id,
                "member_id": authoritative_member_id,
                "bioguide_id": authoritative_member_id if not _is_legacy_fmp_member_id(authoritative_member_id) else None,
                "member_name": row.member_name or authoritative_member_id,
                "member_slug": row.member_slug or authoritative_member_id,
                "chamber": row.chamber,
                "party": row.party,
                "state": row.state,
            }

    if not alias_metadata:
        identity_snapshot, _ = _get_congress_identity_snapshot(db, normalized_chamber)
        expected_logical_members = int(identity_snapshot.get("logical_member_count", 0) or 0)
        for group_key, aliases in identity_snapshot.get("merged_aliases", {}).items():
            profile = identity_snapshot.get("profiles", {}).get(group_key, {})
            authoritative_member_id = sorted(
                [alias for alias in aliases if alias],
                key=lambda value: (_is_legacy_fmp_member_id(value), value),
            )[0] if aliases else group_key
            for alias in aliases:
                alias_metadata[alias] = {
                    "group_key": group_key,
                    "member_id": authoritative_member_id,
                    "bioguide_id": authoritative_member_id if not _is_legacy_fmp_member_id(authoritative_member_id) else None,
                    "member_name": profile.get("member_name") or authoritative_member_id,
                    "member_slug": profile.get("member_slug") or authoritative_member_id,
                    "chamber": profile.get("chamber"),
                    "party": profile.get("party"),
                    "state": profile.get("state"),
                }

    member_query = select(Member).where(Member.bioguide_id.is_not(None))
    if normalized_chamber in {"house", "senate"}:
        member_query = member_query.where(func.lower(Member.chamber) == normalized_chamber)
    members = db.execute(member_query).scalars().all()
    if not expected_logical_members:
        expected_logical_members = len(members)
    for member in members:
        member_id = (member.bioguide_id or "").strip()
        if not member_id or member_id in alias_metadata:
            continue
        alias_metadata[member_id] = {
            "group_key": member_id,
            "member_id": member_id,
            "bioguide_id": member_id if not _is_legacy_fmp_member_id(member_id) else None,
            "member_name": _member_full_name(member) or member_id,
            "member_slug": member_id,
            "chamber": _clean_metadata_value(member.chamber),
            "party": _normalize_party(member.party),
            "state": _clean_metadata_value(member.state),
        }

    return alias_metadata, expected_logical_members


def _load_congress_portfolio_leaderboard_rows(
    db: Session,
    *,
    normalized_chamber: str,
    benchmark_symbol: str,
    lookback_days: int,
    mode: str,
    limit: int,
    normalized_sort: str,
    include_poor_quality: bool = False,
) -> tuple[list[dict], int, int, list[str]]:
    alias_metadata, expected_logical_members = _load_congress_portfolio_identity_rows(
        db,
        normalized_chamber=normalized_chamber,
    )
    included_quality_statuses = ["good", "warning", "poor"]

    run_rows = db.execute(
        select(ReplicatedPortfolioRun)
        .where(ReplicatedPortfolioRun.entity_type == "congress_member")
        .where(ReplicatedPortfolioRun.lookback_days == lookback_days)
        .where(ReplicatedPortfolioRun.mode == mode)
        .where(ReplicatedPortfolioRun.benchmark_symbol == benchmark_symbol)
        .where(ReplicatedPortfolioRun.methodology_version == PORTFOLIO_METHODOLOGY_VERSION)
        .order_by(ReplicatedPortfolioRun.computed_at.desc(), ReplicatedPortfolioRun.id.desc())
    ).scalars().all()

    latest_by_entity_id: dict[str, ReplicatedPortfolioRun] = {}
    for run in run_rows:
        entity_id = (run.entity_id or "").strip()
        if _is_legacy_fmp_member_id(entity_id):
            continue
        if entity_id and entity_id not in latest_by_entity_id:
            latest_by_entity_id[entity_id] = run

    runs_by_group_key: dict[str, list[ReplicatedPortfolioRun]] = {}
    for entity_id, run in latest_by_entity_id.items():
        metadata = alias_metadata.get(entity_id)
        group_key = (metadata or {}).get("group_key") or entity_id
        runs_by_group_key.setdefault(group_key, []).append(run)

    rows: list[dict] = []
    excluded_poor_quality_count = 0
    for group_key, group_runs in runs_by_group_key.items():
        group_runs = sorted(
            group_runs,
            key=lambda run: (
                _is_legacy_fmp_member_id(run.entity_id),
                -(run.computed_at.timestamp() if run.computed_at else 0),
                -int(run.id or 0),
            ),
        )
        run = group_runs[0]
        curve_quality_status = _portfolio_run_curve_quality_status(run)
        public_safety_flags = _portfolio_run_public_safety_flags(run)
        if not include_poor_quality and public_safety_flags:
            excluded_poor_quality_count += 1
            continue

        entity_id = (run.entity_id or "").strip()
        metadata = alias_metadata.get(entity_id)
        if metadata is None and _is_orphaned_fmp_comma_fragment_member_id(entity_id):
            continue
        if metadata is None:
            metadata = {
                "group_key": group_key,
                "member_id": entity_id,
                "bioguide_id": entity_id if not _is_legacy_fmp_member_id(entity_id) else None,
                "member_name": entity_id,
                "member_slug": entity_id,
                "chamber": None,
                "party": None,
                "state": None,
            }
        if normalized_chamber in {"house", "senate"} and (metadata.get("chamber") or "").strip().lower() != normalized_chamber:
            continue

        rows.append(
            {
                "member_id": metadata.get("member_id") or entity_id,
                "bioguide_id": metadata.get("bioguide_id"),
                "portfolio_entity_id": entity_id,
                "member_name": metadata.get("member_name") or entity_id,
                "member_slug": metadata.get("member_slug") or metadata.get("member_id") or entity_id,
                "chamber": metadata.get("chamber"),
                "party": metadata.get("party"),
                "state": metadata.get("state"),
                "portfolio_run_id": run.id,
                "lookback_days": run.lookback_days,
                "mode": run.mode,
                "benchmark_symbol": run.benchmark_symbol,
                "starting_value": run.starting_value,
                "ending_value": run.ending_value,
                "benchmark_ending_value": run.benchmark_ending_value,
                "total_return_pct": run.total_return_pct,
                "benchmark_return_pct": run.benchmark_return_pct,
                "alpha_pct": run.alpha_pct,
                "cagr_pct": run.cagr_pct,
                "max_drawdown_pct": run.max_drawdown_pct,
                "volatility_pct": run.volatility_pct,
                "sharpe_ratio": run.sharpe_ratio,
                "win_rate_pct": run.win_rate_pct,
                "average_exposure_pct": run.average_exposure_pct,
                "positions_count": int(run.positions_count or 0),
                "skipped_events_count": int(run.skipped_events_count or 0),
                "points_count": int(run.points_count or 0),
                "status": run.status,
                "status_message": run.status_message,
                "curve_quality_status": curve_quality_status,
                "public_safety_flags": public_safety_flags,
                "data_coverage": {
                    "status": run.status,
                    "curve_quality_status": curve_quality_status,
                    "public_safety_flags": public_safety_flags,
                    "points_count": int(run.points_count or 0),
                    "positions_count": int(run.positions_count or 0),
                    "skipped_events_count": int(run.skipped_events_count or 0),
                },
                "run_created_at": run.created_at.isoformat() if run.created_at else None,
                "last_computed_at": run.computed_at.isoformat() if run.computed_at else None,
                "methodology_version": run.methodology_version,
            }
        )

    def sort_key(row: dict):
        raw_value = row.get(normalized_sort)
        value = float(raw_value) if raw_value is not None else None
        if _portfolio_sort_lower_is_better(normalized_sort):
            return (
                value is None,
                value if value is not None else float("inf"),
                -int(row.get("positions_count") or 0),
                str(row.get("member_id") or ""),
            )
        return (
            value is None,
            -(value if value is not None else float("-inf")),
            -int(row.get("positions_count") or 0),
            str(row.get("member_id") or ""),
        )

    groups_with_returned_runs = len(rows)
    rows = sorted(rows, key=sort_key)[:limit]
    for idx, row in enumerate(rows, start=1):
        row["rank"] = idx

    missing_portfolio_runs_count = max(0, int(expected_logical_members or 0) - groups_with_returned_runs)
    return rows, missing_portfolio_runs_count, excluded_poor_quality_count, included_quality_statuses


def _attach_row_medians(rows: list[dict], values_by_key: dict[str, dict[str, list[float]]], *, key_field: str) -> None:
    for row in rows:
        row_values = values_by_key.get(str(row.get(key_field) or ""), {})
        return_values = row_values.get("return_values", [])
        alpha_values = row_values.get("alpha_values", [])
        row["median_return"] = median(return_values) if return_values else None
        row["median_alpha"] = median(alpha_values) if alpha_values else None


def build_congress_member_alias_rows(
    db: Session,
    normalized_chamber: str = "all",
) -> list[dict[str, str | None]]:
    members_q = select(Member).where(Member.bioguide_id.is_not(None))
    if normalized_chamber in {"house", "senate"}:
        members_q = members_q.where(func.lower(Member.chamber) == normalized_chamber)
    members = db.execute(members_q).scalars().all()
    if not members:
        return []

    outcome_alias_rows = db.execute(
        select(
            func.lower(TradeOutcome.member_name).label("normalized_name"),
            TradeOutcome.member_id,
        )
        .where(TradeOutcome.member_id.is_not(None))
        .where(TradeOutcome.member_name.is_not(None))
        .group_by(func.lower(TradeOutcome.member_name), TradeOutcome.member_id)
    ).all()
    outcome_aliases_by_name: dict[str, set[str]] = {}
    for normalized_name, member_id in outcome_alias_rows:
        if normalized_name and member_id:
            outcome_aliases_by_name.setdefault(str(normalized_name), set()).add(str(member_id))

    event_alias_rows = db.execute(
        select(
            func.lower(Event.member_name).label("normalized_name"),
            func.lower(func.coalesce(Event.chamber, "")).label("normalized_chamber"),
            Event.member_bioguide_id,
        )
        .where(Event.event_type == "congress_trade")
        .where(Event.member_bioguide_id.is_not(None))
        .where(Event.member_name.is_not(None))
        .group_by(
            func.lower(Event.member_name),
            func.lower(func.coalesce(Event.chamber, "")),
            Event.member_bioguide_id,
        )
    ).all()
    event_aliases_by_name_chamber: dict[tuple[str, str], set[str]] = {}
    for normalized_name, member_chamber, member_id in event_alias_rows:
        if normalized_name and member_id:
            event_aliases_by_name_chamber.setdefault(
                (str(normalized_name), str(member_chamber or "")),
                set(),
            ).add(str(member_id))

    logical_member_aliases: dict[str, set[str]] = {}
    logical_member_profiles: dict[str, Member] = {}
    for member in members:
        member_id = (member.bioguide_id or "").strip()
        if not member_id:
            continue
        full_name = _member_full_name(member)
        normalized_name = full_name.lower()
        aliases = {member_id}
        if normalized_name:
            aliases.update(outcome_aliases_by_name.get(normalized_name, set()))
            aliases.update(
                event_aliases_by_name_chamber.get(
                    (normalized_name, (member.chamber or "").strip().lower()),
                    set(),
                )
            )
        logical_member_aliases[member_id] = {alias for alias in aliases if alias}
        logical_member_profiles[member_id] = member

    merged_logical_aliases: dict[str, set[str]] = {}
    merged_logical_profiles: dict[str, Member] = {}
    alias_to_group_key: dict[str, str] = {}

    for logical_member_id, aliases_set in logical_member_aliases.items():
        aliases = {alias for alias in aliases_set if alias}
        aliases.add(logical_member_id)
        group_keys = {alias_to_group_key[alias] for alias in aliases if alias in alias_to_group_key}
        if logical_member_id in merged_logical_aliases:
            group_keys.add(logical_member_id)

        target_group_key = (
            sorted(group_keys, key=lambda value: (_is_legacy_fmp_member_id(value), value))[0]
            if group_keys
            else logical_member_id
        )

        target_aliases = merged_logical_aliases.setdefault(target_group_key, set())
        target_aliases.update(aliases)

        chosen_profile = _prefer_member_identity(
            logical_member_profiles.get(logical_member_id),
            merged_logical_profiles.get(target_group_key),
        )
        if chosen_profile is not None:
            merged_logical_profiles[target_group_key] = chosen_profile

        for group_key in sorted(group_keys):
            if group_key == target_group_key:
                continue
            existing_aliases = merged_logical_aliases.pop(group_key, set())
            target_aliases.update(existing_aliases)
            existing_profile = merged_logical_profiles.pop(group_key, None)
            preferred_profile = _prefer_member_identity(existing_profile, merged_logical_profiles.get(target_group_key))
            if preferred_profile is not None:
                merged_logical_profiles[target_group_key] = preferred_profile

        for alias in target_aliases:
            alias_to_group_key[alias] = target_group_key

    rows: list[dict[str, str | None]] = []
    for group_key, aliases in merged_logical_aliases.items():
        member = merged_logical_profiles.get(group_key)
        if member is None:
            continue
        authoritative_member_id = sorted(
            [alias for alias in aliases if alias],
            key=lambda value: (_is_legacy_fmp_member_id(value), value),
        )[0]
        member_name = _member_full_name(member) or authoritative_member_id
        member_slug = group_key
        chamber = _clean_metadata_value(member.chamber)
        party = _normalize_party(member.party)
        state = _clean_metadata_value(member.state)
        for alias in sorted(alias for alias in aliases if alias):
            rows.append(
                {
                    "alias_member_id": alias,
                    "group_key": group_key,
                    "authoritative_member_id": authoritative_member_id,
                    "member_name": member_name,
                    "member_slug": member_slug,
                    "chamber": chamber,
                    "party": party,
                    "state": state,
                }
            )
    return rows


def _has_persisted_congress_member_aliases(db: Session, normalized_chamber: str) -> bool:
    query = select(CongressMemberAlias.alias_member_id)
    if normalized_chamber in {"house", "senate"}:
        query = query.where(func.lower(CongressMemberAlias.chamber) == normalized_chamber)
    return db.execute(query.limit(1)).scalar_one_or_none() is not None


def _load_congress_leaderboard_rows_from_snapshot(
    db: Session,
    *,
    normalized_chamber: str,
    benchmark_symbol: str,
    cutoff_date: date,
    min_trades: int,
    limit: int,
    normalized_sort: str,
) -> list[dict]:
    filters = [
        TradeOutcome.benchmark_symbol == benchmark_symbol,
        TradeOutcome.trade_date.is_not(None),
        TradeOutcome.trade_date >= cutoff_date,
    ]
    if normalized_chamber in {"house", "senate"}:
        filters.append(func.lower(CongressMemberAlias.chamber) == normalized_chamber)

    filtered = (
        select(
            CongressMemberAlias.group_key,
            CongressMemberAlias.authoritative_member_id.label("member_id"),
            CongressMemberAlias.member_name,
            CongressMemberAlias.member_slug,
            CongressMemberAlias.chamber,
            CongressMemberAlias.party,
            CongressMemberAlias.state,
            TradeOutcome.event_id,
            TradeOutcome.symbol,
            TradeOutcome.trade_type,
            TradeOutcome.trade_date,
            TradeOutcome.amount_min,
            TradeOutcome.amount_max,
            TradeOutcome.benchmark_symbol,
            TradeOutcome.scoring_status,
            TradeOutcome.return_pct,
            TradeOutcome.alpha_pct,
            TradeOutcome.computed_at,
        )
        .select_from(TradeOutcome)
        .join(CongressMemberAlias, CongressMemberAlias.alias_member_id == TradeOutcome.member_id)
        .where(*filters)
    ).cte("congress_filtered_outcomes")

    partition_key = (
        filtered.c.group_key,
        func.upper(func.trim(func.coalesce(filtered.c.symbol, ""))),
        filtered.c.trade_date,
        _normalized_trade_side_sql(filtered.c.trade_type),
        filtered.c.amount_min,
        filtered.c.amount_max,
        filtered.c.benchmark_symbol,
    )
    order_key = (filtered.c.computed_at.desc(), filtered.c.event_id.desc())

    total_deduped = (
        select(
            filtered,
            func.row_number().over(partition_by=partition_key, order_by=order_key).label("row_rank"),
        )
    ).cte("congress_total_deduped")

    scored_deduped = (
        select(
            filtered,
            func.row_number().over(partition_by=partition_key, order_by=order_key).label("row_rank"),
        )
        .where(filtered.c.scoring_status == "ok")
    ).cte("congress_scored_deduped")

    total_counts = (
        select(
            total_deduped.c.group_key,
            func.count().label("trade_count_total"),
        )
        .where(total_deduped.c.row_rank == 1)
        .group_by(total_deduped.c.group_key)
    ).cte("congress_total_counts")

    win_rate = func.avg(
        case(
            (scored_deduped.c.return_pct.is_(None), None),
            (scored_deduped.c.return_pct > 0, 1.0),
            else_=0.0,
        )
    ).label("win_rate")

    ranked = (
        select(
            scored_deduped.c.group_key,
            func.max(scored_deduped.c.member_id).label("member_id"),
            func.max(scored_deduped.c.member_name).label("member_name"),
            func.max(scored_deduped.c.member_slug).label("member_slug"),
            func.max(scored_deduped.c.chamber).label("chamber"),
            func.max(scored_deduped.c.party).label("party"),
            func.max(scored_deduped.c.state).label("state"),
            total_counts.c.trade_count_total,
            func.count().label("trade_count_scored"),
            func.avg(scored_deduped.c.return_pct).label("avg_return"),
            func.avg(scored_deduped.c.alpha_pct).label("avg_alpha"),
            win_rate,
        )
        .select_from(scored_deduped)
        .join(total_counts, total_counts.c.group_key == scored_deduped.c.group_key)
        .where(scored_deduped.c.row_rank == 1)
        .group_by(scored_deduped.c.group_key, total_counts.c.trade_count_total)
        .having(func.count() >= min_trades)
    ).cte("congress_ranked_rows")

    sort_value = _leaderboard_sort_value_sql(ranked.c, normalized_sort)
    ranked_rows = db.execute(
        select(ranked)
        .order_by(
            sort_value.desc(),
            ranked.c.trade_count_total.desc(),
            ranked.c.trade_count_scored.desc(),
            ranked.c.member_id.asc(),
        )
        .limit(limit)
    ).all()

    result_rows = [
        {
            "group_key": row.group_key,
            "member_id": row.member_id,
            "member_name": row.member_name,
            "member_slug": row.member_slug,
            "chamber": row.chamber,
            "party": row.party,
            "state": row.state,
            "trade_count_total": int(row.trade_count_total or 0),
            "trade_count_scored": int(row.trade_count_scored or 0),
            "avg_return": float(row.avg_return) if row.avg_return is not None else None,
            "median_return": None,
            "win_rate": float(row.win_rate) if row.win_rate is not None else None,
            "avg_alpha": float(row.avg_alpha) if row.avg_alpha is not None else None,
            "median_alpha": None,
            "benchmark_symbol": benchmark_symbol,
            "pnl_status": "ok",
        }
        for row in ranked_rows
    ]
    if not result_rows:
        return result_rows

    group_keys = [row["group_key"] for row in result_rows if row.get("group_key")]
    median_rows = db.execute(
        select(
            scored_deduped.c.group_key,
            scored_deduped.c.return_pct,
            scored_deduped.c.alpha_pct,
        )
        .where(scored_deduped.c.row_rank == 1)
        .where(scored_deduped.c.group_key.in_(group_keys))
    ).all()
    values_by_key: dict[str, dict[str, list[float]]] = {}
    for group_key, return_pct, alpha_pct in median_rows:
        bucket = values_by_key.setdefault(str(group_key), {"return_values": [], "alpha_values": []})
        if return_pct is not None:
            bucket["return_values"].append(float(return_pct))
        if alpha_pct is not None:
            bucket["alpha_values"].append(float(alpha_pct))
    _attach_row_medians(result_rows, values_by_key, key_field="group_key")
    for row in result_rows:
        row.pop("group_key", None)
    return result_rows


def _load_member_leaderboard_rows(
    db: Session,
    *,
    normalized_source_mode: str,
    normalized_chamber: str,
    insider_market_trade_types: set[str],
    benchmark_symbol: str,
    cutoff_date: date,
    min_trades: int,
    limit: int,
    normalized_sort: str,
) -> list[dict]:
    member_outcome_filters = [
        TradeOutcome.member_id.is_not(None),
        TradeOutcome.trade_date.is_not(None),
        TradeOutcome.trade_date >= cutoff_date,
        TradeOutcome.benchmark_symbol == benchmark_symbol,
    ]

    if normalized_source_mode == "insiders":
        member_outcome_filters.append(Event.event_type == "insider_trade")
        member_outcome_filters.append(func.lower(func.coalesce(Event.trade_type, "")).in_(insider_market_trade_types))
    else:
        member_outcome_filters.append(
            or_(
                Event.event_type == "congress_trade",
                and_(
                    Event.event_type == "insider_trade",
                    func.lower(func.coalesce(Event.trade_type, "")).in_(insider_market_trade_types),
                ),
            )
        )

    if normalized_chamber in {"house", "senate"}:
        member_outcome_filters.append(func.lower(Member.chamber) == normalized_chamber)

    scored_count = func.sum(case((TradeOutcome.scoring_status == "ok", 1), else_=0)).label("trade_count_scored")
    avg_return = func.avg(case((TradeOutcome.scoring_status == "ok", TradeOutcome.return_pct), else_=None)).label("avg_return")
    avg_alpha = func.avg(case((TradeOutcome.scoring_status == "ok", TradeOutcome.alpha_pct), else_=None)).label("avg_alpha")
    win_rate = func.avg(
        case(
            (TradeOutcome.scoring_status != "ok", None),
            (TradeOutcome.return_pct.is_(None), None),
            (TradeOutcome.return_pct > 0, 1.0),
            else_=0.0,
        )
    ).label("win_rate")

    aggregated = (
        select(
            TradeOutcome.member_id.label("member_id"),
            func.max(TradeOutcome.member_name).label("outcome_member_name"),
            func.max(Member.first_name).label("first_name"),
            func.max(Member.last_name).label("last_name"),
            func.coalesce(func.max(Member.chamber), func.max(Event.chamber)).label("chamber"),
            func.coalesce(func.max(Member.party), func.max(Event.party)).label("party"),
            func.max(Member.state).label("state"),
            func.count().label("trade_count_total"),
            scored_count,
            avg_return,
            avg_alpha,
            win_rate,
        )
        .select_from(TradeOutcome)
        .join(Event, Event.id == TradeOutcome.event_id)
        .join(Member, Member.bioguide_id == TradeOutcome.member_id, isouter=True)
        .where(*member_outcome_filters)
        .group_by(TradeOutcome.member_id)
        .having(scored_count >= min_trades)
    ).cte("member_ranked_rows")

    sort_value = _leaderboard_sort_value_sql(aggregated.c, normalized_sort)
    ranked_rows = db.execute(
        select(aggregated)
        .order_by(
            sort_value.desc(),
            aggregated.c.trade_count_total.desc(),
            aggregated.c.trade_count_scored.desc(),
            aggregated.c.member_id.asc(),
        )
        .limit(limit)
    ).all()

    result_rows = []
    for row in ranked_rows:
        resolved_name = f"{row.first_name or ''} {row.last_name or ''}".strip() or (row.outcome_member_name or row.member_id)
        result_rows.append(
            {
                "member_id": row.member_id,
                "member_name": resolved_name,
                "member_slug": row.member_id,
                "chamber": row.chamber,
                "party": row.party,
                "state": row.state,
                "trade_count_total": int(row.trade_count_total or 0),
                "trade_count_scored": int(row.trade_count_scored or 0),
                "avg_return": float(row.avg_return) if row.avg_return is not None else None,
                "median_return": None,
                "win_rate": float(row.win_rate) if row.win_rate is not None else None,
                "avg_alpha": float(row.avg_alpha) if row.avg_alpha is not None else None,
                "median_alpha": None,
                "benchmark_symbol": benchmark_symbol,
                "pnl_status": "ok",
            }
        )

    if not result_rows:
        return result_rows

    top_member_ids = [row["member_id"] for row in result_rows if row.get("member_id")]
    median_rows = db.execute(
        select(
            TradeOutcome.member_id,
            TradeOutcome.return_pct,
            TradeOutcome.alpha_pct,
        )
        .select_from(TradeOutcome)
        .join(Event, Event.id == TradeOutcome.event_id)
        .join(Member, Member.bioguide_id == TradeOutcome.member_id, isouter=True)
        .where(*member_outcome_filters)
        .where(TradeOutcome.scoring_status == "ok")
        .where(TradeOutcome.member_id.in_(top_member_ids))
    ).all()

    values_by_key: dict[str, dict[str, list[float]]] = {}
    for member_id, return_pct, alpha_pct in median_rows:
        key = str(member_id or "")
        bucket = values_by_key.setdefault(key, {"return_values": [], "alpha_values": []})
        if return_pct is not None:
            bucket["return_values"].append(float(return_pct))
        if alpha_pct is not None:
            bucket["alpha_values"].append(float(alpha_pct))
    _attach_row_medians(result_rows, values_by_key, key_field="member_id")
    return result_rows


def _payload_text(payload: dict, *keys: str) -> str | None:
    if not isinstance(payload, dict):
        return None
    candidates = [payload]
    nested = payload.get("payload")
    if isinstance(nested, dict):
        candidates.append(nested)
    raw = payload.get("raw")
    if isinstance(raw, dict):
        candidates.append(raw)
    for candidate in candidates:
        for key in keys:
            value = candidate.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return None


def _safe_outcome_status(status: str | None) -> str | None:
    if not status:
        return None
    if status.startswith("provider_"):
        return "price_unavailable"
    return status


def _safe_identity_text(*values: object) -> str | None:
    for value in values:
        if isinstance(value, str):
            text = value.strip()
            if text and text.lower() not in BAD_EVENT_IDENTITY_LABELS:
                return text
    return None


def _parse_payload_json(payload_json: str | None) -> dict:
    if not payload_json:
        return {}
    try:
        parsed = json.loads(payload_json)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _member_top_tickers(db: Session, member: Member, *, limit: int = 10) -> list[dict]:
    try:
        _, analytics_member_ids = _resolve_member_analytics_aliases(db, member.bioguide_id or "")
    except OperationalError:
        analytics_member_ids = [member.bioguide_id] if member.bioguide_id else []

    normalized_member_ids = [member_id for member_id in sorted(set(analytics_member_ids)) if member_id]
    if normalized_member_ids:
        outcome_rows = db.execute(
            select(TradeOutcome)
            .join(Event, Event.id == TradeOutcome.event_id, isouter=True)
            .where(TradeOutcome.member_id.in_(normalized_member_ids))
            .where(TradeOutcome.benchmark_symbol == "^GSPC")
            .where(or_(Event.id.is_(None), Event.event_type == "congress_trade"))
            .order_by(TradeOutcome.trade_date.asc(), TradeOutcome.event_id.asc())
        ).scalars().all()
        counts: dict[str, dict] = {}
        for row in dedupe_member_trade_outcomes(outcome_rows):
            symbol = (row.symbol or "").strip().upper()
            if not symbol:
                continue
            bucket = counts.setdefault(symbol, {"symbol": symbol, "trades": 0, "notional": 0.0})
            bucket["trades"] += 1
            amount = row.amount_max if row.amount_max is not None else row.amount_min
            if amount is not None:
                bucket["notional"] += float(amount)
        if counts:
            return [
                {"symbol": row["symbol"], "trades": row["trades"]}
                for row in sorted(counts.values(), key=lambda item: (item["trades"], item["notional"], item["symbol"]), reverse=True)[:limit]
            ]

    tx_rows = db.execute(
        select(Security.symbol, func.count(Transaction.id).label("trade_count"))
        .select_from(Transaction)
        .join(Security, Transaction.security_id == Security.id)
        .where(Transaction.member_id == member.id)
        .where(Security.symbol.is_not(None))
        .group_by(Security.symbol)
        .order_by(func.count(Transaction.id).desc(), Security.symbol.asc())
        .limit(limit)
    ).all()
    return [
        {"symbol": str(symbol).strip().upper(), "trades": int(trade_count)}
        for symbol, trade_count in tx_rows
        if symbol and str(symbol).strip()
    ]


def _build_member_profile(db: Session, member: Member) -> dict:
    trades = _member_recent_trades(db, member.id, lookback_days=None, limit=200)

    return {
        "member": _member_payload(member),
        "top_tickers": _member_top_tickers(db, member),
        "trades": trades,
    }


def _member_recent_trades(
    db: Session,
    member_pk: int,
    *,
    lookback_days: int | None,
    limit: int,
) -> list[dict]:
    cutoff: date | None = None
    if lookback_days is not None:
        cutoff = datetime.now(timezone.utc).date() - timedelta(days=max(lookback_days, 1))

    member_bioguide_id = db.execute(
        select(Member.bioguide_id).where(Member.id == member_pk).limit(1)
    ).scalar_one_or_none()
    try:
        _, analytics_member_ids = _resolve_member_analytics_aliases(db, member_bioguide_id or "")
    except OperationalError:
        analytics_member_ids = [member_bioguide_id] if member_bioguide_id else []

    event_member_ids = [member_id for member_id in sorted(set(analytics_member_ids)) if member_id]
    if event_member_ids:
        sort_ts = func.coalesce(Event.event_date, Event.ts)
        event_query = (
            select(Event)
            .where(Event.event_type.in_(CONGRESS_DISCLOSURE_EVENT_TYPES))
            .where(Event.member_bioguide_id.in_(event_member_ids))
            .order_by(sort_ts.desc(), Event.id.desc())
            .limit(limit)
        )
        if cutoff is not None:
            cutoff_dt = datetime.combine(cutoff, datetime.min.time(), tzinfo=timezone.utc)
            event_query = event_query.where(sort_ts >= cutoff_dt)

        try:
            events = db.execute(event_query).scalars().all()
        except OperationalError:
            events = []
        if events:
            event_ids = [event.id for event in events]
            outcomes = db.execute(
                select(TradeOutcome)
                .where(TradeOutcome.event_id.in_(event_ids))
                .where(TradeOutcome.benchmark_symbol == "^GSPC")
            ).scalars().all()
            outcome_by_event_id = {row.event_id: row for row in outcomes}
            event_symbols = [
                symbol
                for event in events
                for payload in [_parse_payload_json(event.payload_json)]
                for symbol in [normalize_symbol(event.symbol or payload.get("symbol") or payload.get("ticker"))]
                if symbol and symbol.lower() not in BAD_EVENT_IDENTITY_LABELS
            ]
            baseline_map = _congress_baseline_map_for_symbols(db, event_symbols) if event_symbols else {}
            confirmation_metrics_map = get_confirmation_metrics_for_symbols(db, event_symbols) if event_symbols else {}
            trades = []
            for event in events:
                payload = _parse_payload_json(event.payload_json)
                display_metrics = trade_outcome_display_metrics(outcome_by_event_id.get(event.id))
                event_outcome = outcome_by_event_id.get(event.id)
                symbol = normalize_symbol(event.symbol or payload.get("symbol") or payload.get("ticker"))
                if symbol and symbol.lower() in BAD_EVENT_IDENTITY_LABELS:
                    symbol = None
                classification = None
                if event.event_type in CONGRESS_NON_EQUITY_EVENT_TYPES:
                    symbol = None
                elif not symbol:
                    classification = classify_congress_disclosure_asset(
                        security_description=_payload_text(payload, "security_description", "securityDescription", "description"),
                        asset_class=_payload_text(payload, "asset_class", "assetClass"),
                        raw_symbol=None,
                    )
                security_name = _safe_identity_text(
                    payload.get("company_name"),
                    payload.get("companyName"),
                    payload.get("issuer_name"),
                    payload.get("issuerName"),
                    payload.get("security_name"),
                    payload.get("securityName"),
                    payload.get("security_description"),
                    payload.get("securityDescription"),
                    payload.get("description"),
                ) or "Unresolved security"
                asset_class = canonical_asset_class_value(
                    event_type=event.event_type,
                    asset_class=_payload_text(payload, "asset_class", "assetClass")
                    or (classification.asset_class if classification else None),
                    instrument_type=_payload_text(payload, "instrument_type", "instrumentType"),
                    symbol=symbol,
                    security_description=security_name,
                    company_name=_payload_text(payload, "company_name", "companyName"),
                )
                trade_date = _payload_text(payload, "trade_date", "transaction_date")
                report_date = _payload_text(payload, "report_date", "filing_date")
                if not report_date and (event.event_date or event.ts):
                    report_date = (event.event_date or event.ts).date().isoformat()
                smart_score = payload.get("smart_score")
                if not isinstance(smart_score, (int, float)):
                    smart_score = payload.get("smartScore")
                smart_band = payload.get("smart_band")
                if not isinstance(smart_band, str):
                    smart_band = payload.get("smartBand")
                if not isinstance(smart_score, (int, float)) or not isinstance(smart_band, str):
                    unusual_multiple = _parse_numeric(payload.get("unusual_multiple") or payload.get("unusualMultiple"))
                    if unusual_multiple is None and symbol:
                        baseline_stats = baseline_map.get(symbol)
                        amount_max = _parse_numeric(event.amount_max)
                        if baseline_stats and amount_max is not None and baseline_stats[0] > 0:
                            unusual_multiple = amount_max / baseline_stats[0]
                    event_ts = event.event_date or event.ts
                    if unusual_multiple is not None and event_ts is not None:
                        confirmation_summary = confirmation_metrics_map.get(symbol or "").as_dict() if symbol and symbol in confirmation_metrics_map else None
                        calc_score, calc_band = calculate_smart_score(
                            unusual_multiple=unusual_multiple,
                            amount_max=_parse_numeric(event.amount_max),
                            ts=event_ts,
                            confirmation_30d=confirmation_summary,
                        )
                        if not isinstance(smart_score, (int, float)):
                            smart_score = calc_score
                        if not isinstance(smart_band, str):
                            smart_band = calc_band
                trades.append({
                    "estimated_trade_value": _estimated_trade_value(event.amount_min, event.amount_max),
                    "estimated_shares": _estimated_shares(
                        event.amount_min,
                        event.amount_max,
                        event_outcome.entry_price if event_outcome is not None else None,
                    ),
                    "id": event.id,
                    "event_id": event.id,
                    "symbol": symbol,
                    "security_name": security_name,
                    "asset_class": asset_class,
                    "instrument_type": _payload_text(payload, "instrument_type", "instrumentType"),
                    "maturity_date": _payload_text(payload, "maturity_date", "maturityDate"),
                    "duration_days": _parse_numeric(payload.get("duration_days") or payload.get("durationDays")),
                    "duration_label": _payload_text(payload, "duration_label", "durationLabel"),
                    "coupon_rate": _parse_numeric(payload.get("coupon_rate") or payload.get("couponRate")),
                    "cusip": _payload_text(payload, "cusip"),
                    "transaction_type": event.transaction_type or _payload_text(payload, "transaction_type", "trade_type") or "",
                    "trade_date": trade_date,
                    "report_date": report_date,
                    "amount_range_min": event.amount_min,
                    "amount_range_max": event.amount_max,
                    "price": outcome_by_event_id.get(event.id).entry_price if outcome_by_event_id.get(event.id) is not None else None,
                    "trade_price": outcome_by_event_id.get(event.id).entry_price if outcome_by_event_id.get(event.id) is not None else None,
                    "estimated_price": outcome_by_event_id.get(event.id).entry_price if outcome_by_event_id.get(event.id) is not None else None,
                    "current_price": outcome_by_event_id.get(event.id).current_price if outcome_by_event_id.get(event.id) is not None else None,
                    "pnl_pct": display_metrics.return_pct,
                    "return_pct": display_metrics.return_pct,
                    "alpha_pct": display_metrics.alpha_pct,
                    "benchmark_return_pct": display_metrics.benchmark_return_pct,
                    "holding_period_days": display_metrics.holding_period_days,
                    "outcome_horizon": display_metrics.outcome_horizon,
                    "return_label": display_metrics.outcome_horizon,
                    "pnl_source": (
                        "eod"
                        if display_metrics.return_pct is not None and event_outcome is not None and event_outcome.entry_price is not None
                        else display_metrics.pnl_source
                    ),
                    "outcome_status": _safe_outcome_status(event_outcome.scoring_status) if event_outcome is not None else "pending",
                    "outcome_skip_reason": (
                        _safe_outcome_status(event_outcome.scoring_status)
                        if event_outcome is not None and display_metrics.return_pct is None
                        else ("no_trade_outcomes_row" if event_outcome is None else None)
                    ),
                    "outcome_methodology": event_outcome.methodology_version if event_outcome is not None else None,
                    "outcome_error": event_outcome.scoring_error if event_outcome is not None else None,
                    "price_basis": "EOD" if event_outcome is not None and event_outcome.entry_price is not None else None,
                    "smart_score": smart_score if isinstance(smart_score, (int, float)) else None,
                    "smart_band": smart_band if isinstance(smart_band, str) else None,
                })
            return trades

    q = (
        select(Transaction, Security)
        .outerjoin(Security, Transaction.security_id == Security.id)
        .where(Transaction.member_id == member_pk)
        .order_by(
            Transaction.trade_date.desc(),
            Transaction.report_date.desc(),
            Transaction.id.desc(),
        )
    )
    if cutoff is not None:
        q = q.where(Transaction.trade_date.is_not(None)).where(Transaction.trade_date >= cutoff)
    q = q.limit(limit)

    rows = db.execute(q).all()

    try:
        _, analytics_member_ids = _resolve_member_analytics_aliases(db, member_bioguide_id or "")
    except OperationalError:
        analytics_member_ids = [member_bioguide_id] if member_bioguide_id else []

    outcome_by_logical_key: dict[tuple, TradeOutcome] = {}
    outcome_by_weak_key: dict[tuple, TradeOutcome] = {}
    event_context_by_id: dict[int, dict] = {}
    outcome_symbols: set[str] = set()
    if analytics_member_ids:
        outcome_query = (
            select(
                TradeOutcome,
                Event.payload_json,
                Event.symbol,
                Event.amount_max,
                Event.ts,
            )
            .join(Event, Event.id == TradeOutcome.event_id)
            .where(TradeOutcome.member_id.in_(analytics_member_ids))
            .where(TradeOutcome.benchmark_symbol == "^GSPC")
            .where(Event.event_type == "congress_trade")
            .order_by(TradeOutcome.trade_date.desc(), TradeOutcome.id.desc())
        )
        if cutoff is not None:
            outcome_query = outcome_query.where(TradeOutcome.trade_date.is_not(None)).where(TradeOutcome.trade_date >= cutoff)

        try:
            outcome_rows = db.execute(outcome_query).all()
        except OperationalError:
            outcome_rows = []
        deduped_outcomes = dedupe_member_trade_outcomes([row for row, *_ in outcome_rows])
        deduped_outcome_ids = {row.id for row in deduped_outcomes}
        for outcome, payload_json, event_symbol, event_amount_max, event_ts in outcome_rows:
            if outcome.id not in deduped_outcome_ids:
                continue
            logical_key = trade_outcome_logical_key(
                symbol=outcome.symbol,
                trade_side=outcome.trade_type,
                trade_date=outcome.trade_date,
                amount_min=outcome.amount_min,
                amount_max=outcome.amount_max,
            )
            if logical_key not in outcome_by_logical_key:
                outcome_by_logical_key[logical_key] = outcome
                payload = _parse_payload_json(payload_json)
                normalized_symbol = (event_symbol or outcome.symbol or "").strip().upper()
                if normalized_symbol:
                    outcome_symbols.add(normalized_symbol)
                event_context_by_id[outcome.event_id] = {
                    "payload": payload,
                    "symbol": normalized_symbol,
                    "amount_max": event_amount_max if event_amount_max is not None else outcome.amount_max,
                    "ts": event_ts,
                }
            weak_key = (
                (outcome.trade_type or "").strip().lower(),
                outcome.trade_date.isoformat() if outcome.trade_date else "",
                outcome.amount_min,
                outcome.amount_max,
            )
            outcome_by_weak_key.setdefault(weak_key, outcome)

    baseline_map = _congress_baseline_map_for_symbols(db, list(outcome_symbols)) if outcome_symbols else {}
    confirmation_metrics_map = (
        get_confirmation_metrics_for_symbols(db, list(outcome_symbols)) if outcome_symbols else {}
    )

    trades = []
    seen_keys: set[tuple] = set()

    for tx, s in rows:
        # Keep a defensive trade-date gate in Python so this section always
        # respects lookback by trade date even if backend SQL behavior varies.
        if cutoff is not None and (tx.trade_date is None or tx.trade_date < cutoff):
            continue

        symbol = s.symbol if s else None
        dedupe_key = (
            symbol or "",
            (tx.transaction_type or "").strip().lower(),
            tx.trade_date.isoformat() if tx.trade_date else "",
            tx.report_date.isoformat() if tx.report_date else "",
            tx.amount_range_min,
            tx.amount_range_max,
        )
        if dedupe_key in seen_keys:
            continue
        seen_keys.add(dedupe_key)

        logical_outcome_key = trade_outcome_logical_key(
            symbol=symbol,
            trade_side=tx.transaction_type,
            trade_date=tx.trade_date,
            amount_min=tx.amount_range_min,
            amount_max=tx.amount_range_max,
        )
        matched_outcome = outcome_by_logical_key.get(logical_outcome_key)
        if matched_outcome is None and not symbol:
            weak_key = (
                (tx.transaction_type or "").strip().lower(),
                tx.trade_date.isoformat() if tx.trade_date else "",
                tx.amount_range_min,
                tx.amount_range_max,
            )
            matched_outcome = outcome_by_weak_key.get(weak_key)
        event_context = event_context_by_id.get(matched_outcome.event_id, {}) if matched_outcome else {}
        outcome_payload = event_context.get("payload", {})
        display_symbol = (
            (symbol or "").strip().upper()
            or str(event_context.get("symbol") or "").strip().upper()
            or ((matched_outcome.symbol or "").strip().upper() if matched_outcome else "")
            or None
        )
        classification = None
        if s is None:
            classification = classify_congress_disclosure_asset(
                security_description=tx.description,
                asset_class=None,
                raw_symbol=display_symbol,
            )
        security_name = (
            (s.name if s and s.name else None)
            or (classification.security_description if classification else None)
            or tx.description
            or _payload_text(
                outcome_payload,
                "security_name",
                "securityName",
                "asset_description",
                "assetDescription",
                "description",
                "ticker_name",
                "tickerName",
            )
            or display_symbol
            or "Security"
        )
        smart_score = outcome_payload.get("smart_score")
        if not isinstance(smart_score, (int, float)):
            smart_score = outcome_payload.get("smartScore")

        smart_band = outcome_payload.get("smart_band")
        if not isinstance(smart_band, str):
            smart_band = outcome_payload.get("smartBand")

        if matched_outcome and (not isinstance(smart_score, (int, float)) or not isinstance(smart_band, str)):
            symbol = event_context.get("symbol") or (matched_outcome.symbol or "").strip().upper()
            unusual_multiple = _parse_numeric(
                outcome_payload.get("unusual_multiple") if isinstance(outcome_payload, dict) else None
            )
            if unusual_multiple is None:
                unusual_multiple = _parse_numeric(
                    outcome_payload.get("unusualMultiple") if isinstance(outcome_payload, dict) else None
                )
            if unusual_multiple is None and symbol:
                baseline_stats = baseline_map.get(symbol)
                amount_max = _parse_numeric(event_context.get("amount_max"))
                if baseline_stats and amount_max is not None and baseline_stats[0] > 0:
                    unusual_multiple = amount_max / baseline_stats[0]

            event_ts = event_context.get("ts")
            if unusual_multiple is not None and isinstance(event_ts, datetime):
                confirmation_summary = None
                if symbol and symbol in confirmation_metrics_map:
                    confirmation_summary = confirmation_metrics_map[symbol].as_dict()

                calc_score, calc_band = calculate_smart_score(
                    unusual_multiple=unusual_multiple,
                    amount_max=_parse_numeric(event_context.get("amount_max")),
                    ts=event_ts,
                    confirmation_30d=confirmation_summary,
                )
                if not isinstance(smart_score, (int, float)):
                    smart_score = calc_score
                if not isinstance(smart_band, str):
                    smart_band = calc_band

        display_metrics = trade_outcome_display_metrics(matched_outcome)

        trades.append({
            "estimated_trade_value": _estimated_trade_value(tx.amount_range_min, tx.amount_range_max),
            "estimated_shares": _estimated_shares(
                tx.amount_range_min,
                tx.amount_range_max,
                matched_outcome.entry_price if matched_outcome else None,
            ),
            "id": tx.id,
            "event_id": matched_outcome.event_id if matched_outcome else None,
            "symbol": display_symbol if s is not None else (classification.symbol if classification and classification.asset_class == "crypto" else None),
            "security_name": security_name,
            "asset_class": s.asset_class if s is not None else (classification.asset_class if classification else "other"),
            "instrument_type": classification.instrument_type if classification else None,
            "maturity_date": classification.maturity_date if classification else None,
            "duration_days": classification.duration_days if classification else None,
            "duration_label": classification.duration_label if classification else None,
            "coupon_rate": classification.coupon_rate if classification else None,
            "cusip": classification.cusip if classification else None,
            "transaction_type": tx.transaction_type,
            "trade_date": tx.trade_date.isoformat() if tx.trade_date else None,
            "report_date": tx.report_date.isoformat() if tx.report_date else None,
            "amount_range_min": tx.amount_range_min,
            "amount_range_max": tx.amount_range_max,
            "price": matched_outcome.entry_price if matched_outcome else None,
            "trade_price": matched_outcome.entry_price if matched_outcome else None,
            "estimated_price": matched_outcome.entry_price if matched_outcome else None,
            "current_price": matched_outcome.current_price if matched_outcome else None,
            "pnl_pct": display_metrics.return_pct,
            "return_pct": display_metrics.return_pct,
            "alpha_pct": display_metrics.alpha_pct,
            "pnl_source": (
                "eod"
                if display_metrics.return_pct is not None and matched_outcome is not None and matched_outcome.entry_price is not None
                else display_metrics.pnl_source
            ),
            "outcome_status": _safe_outcome_status(matched_outcome.scoring_status) if matched_outcome else "pending",
            "outcome_skip_reason": (
                _safe_outcome_status(matched_outcome.scoring_status)
                if matched_outcome is not None and display_metrics.return_pct is None
                else ("no_trade_outcomes_row" if matched_outcome is None else None)
            ),
            "outcome_methodology": matched_outcome.methodology_version if matched_outcome else None,
            "outcome_error": matched_outcome.scoring_error if matched_outcome else None,
            "price_basis": "EOD" if matched_outcome is not None and matched_outcome.entry_price is not None else None,
            "smart_score": smart_score if isinstance(smart_score, (int, float)) else None,
            "smart_band": smart_band if isinstance(smart_band, str) else None,
        })

    return trades


# --- App --------------------------------------------------------------------

app = FastAPI(title="Walnut Market Terminal", version="0.1.0")

_HEAVY_ROUTE_WAIT_SECONDS = float(os.getenv("HEAVY_ROUTE_WAIT_SECONDS", "0.25") or 0.25)
_TICKER_CHART_SEMAPHORE = threading.BoundedSemaphore(int(os.getenv("TICKER_CHART_MAX_CONCURRENCY", "2") or 2))
_TICKER_WIDGET_SEMAPHORE = threading.BoundedSemaphore(int(os.getenv("TICKER_WIDGET_MAX_CONCURRENCY", "3") or 3))


@contextmanager
def _heavy_route_slot(route_name: str, semaphore: threading.BoundedSemaphore):
    acquired = semaphore.acquire(timeout=max(_HEAVY_ROUTE_WAIT_SECONDS, 0))
    if not acquired:
        logger.warning("api_degraded endpoint=%s error=heavy_route_saturated", route_name)
        raise HTTPException(status_code=503, detail="Endpoint temporarily busy; please retry shortly.")
    try:
        yield
    finally:
        semaphore.release()


@app.middleware("http")
async def log_slow_requests(request: Request, call_next):
    started = perf_counter()
    response = await call_next(request)
    elapsed_ms = (perf_counter() - started) * 1000
    threshold_ms = float(os.getenv("API_SLOW_REQUEST_LOG_MS", "2000") or 2000)
    if elapsed_ms >= threshold_ms:
        endpoint = request.scope.get("endpoint")
        endpoint_name = getattr(endpoint, "__name__", None) or request.url.path
        logger.info(
            "api_timing endpoint=%s path=%s status=%s duration_ms=%.1f",
            endpoint_name,
            request.url.path,
            response.status_code,
            elapsed_ms,
        )
    return response


@app.exception_handler(SATimeoutError)
async def handle_db_pool_timeout(request: Request, exc: SATimeoutError):
    endpoint = request.scope.get("endpoint")
    endpoint_name = getattr(endpoint, "__name__", None) or request.url.path
    logger.warning(
        "api_degraded endpoint=%s error=db_pool_timeout detail=%s",
        endpoint_name,
        exc.__class__.__name__,
    )
    return JSONResponse(
        status_code=503,
        content={"detail": "Database temporarily busy; please retry shortly."},
    )


@app.exception_handler(OperationalError)
async def handle_db_operational_error(request: Request, exc: OperationalError):
    if not is_database_locked_error(exc):
        raise exc
    endpoint = request.scope.get("endpoint")
    endpoint_name = getattr(endpoint, "__name__", None) or request.url.path
    logger.warning(
        "api_degraded endpoint=%s error=database_locked",
        endpoint_name,
    )
    detail = (
        "Signals temporarily unavailable, database busy"
        if request.url.path == "/api/signals/all"
        else "Database temporarily busy; please retry shortly."
    )
    return JSONResponse(status_code=503, content={"detail": detail})

from fastapi.middleware.cors import CORSMiddleware


def _runtime_environment() -> str:
    return runtime_environment()


def _is_production_runtime() -> bool:
    return is_production()


def _split_origins(raw: str | None) -> list[str]:
    return split_origins(raw)


def _cors_allowed_origins() -> list[str]:
    return cors_allowed_origins()


app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_allowed_origins(),
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type", "Accept", "X-CT-Entitlement-Tier"],
)


class WatchlistPayload(BaseModel):
    name: str


def _require_account(request: Request, db: Session) -> UserAccount:
    return current_user(db, request, required=True)


def _owned_watchlist_query(user: UserAccount):
    return select(Watchlist).where(Watchlist.owner_user_id == user.id)


def _get_owned_watchlist(db: Session, user: UserAccount, watchlist_id: int) -> Watchlist:
    watchlist = db.execute(
        _owned_watchlist_query(user).where(Watchlist.id == watchlist_id)
    ).scalar_one_or_none()
    if not watchlist:
        raise HTTPException(status_code=404, detail="Watchlist not found")
    return watchlist


def _autoheal_if_empty() -> dict:
    """
    Boot-time self-heal: if DB has 0 transactions, run ingest pipeline.
    This prevents the "machine restarted -> empty feed until I remember token" problem.
    """
    # Allow turning off via env if you ever want it
    if os.getenv("AUTOHEAL_ON_STARTUP", "1").strip() not in ("1", "true", "TRUE", "yes", "YES"):
        return {"status": "skipped", "reason": "AUTOHEAL_ON_STARTUP disabled"}

    db = SessionLocal()
    try:
        tx_count = db.execute(select(func.count()).select_from(Transaction)).scalar_one()
    finally:
        db.close()

    if tx_count and tx_count > 0:
        return {"status": "ok", "did_ingest": False, "transactions": tx_count}

    # Empty -> run ingest chain (same as /admin/ensure_data but no token)
    steps = ["app.ingest_house", "app.ingest_senate", "app.enrich_members", "app.write_last_updated"]
    results = []
    for mod in steps:
        r = _run_module(mod)
        results.append(r)
        if r["returncode"] != 0:
            print("AUTOHEAL FAILED:", {"step": mod, "results": results})
            return {"status": "failed", "step": mod, "results": results}

    # Recount
    db2 = SessionLocal()
    try:
        tx_count2 = db2.execute(select(func.count()).select_from(Transaction)).scalar_one()
    finally:
        db2.close()

    print("AUTOHEAL OK:", {"transactions": tx_count2})
    return {"status": "ok", "did_ingest": True, "transactions": tx_count2, "results": results}


def _needs_event_repair(db: Session) -> bool:
    missing_clause = or_(
        Event.member_name.is_(None),
        Event.member_bioguide_id.is_(None),
        Event.chamber.is_(None),
        Event.party.is_(None),
        Event.trade_type.is_(None),
        Event.amount_min.is_(None),
        Event.amount_max.is_(None),
        Event.event_date.is_(None),
        Event.symbol.is_(None),
    )
    row = db.execute(
        select(Event.id)
        .where(Event.event_type == "congress_trade")
        .where(missing_clause)
        .limit(1)
    ).scalar_one_or_none()
    return row is not None


@app.on_event("startup")
def _startup_create_tables():
    validate_startup_security_config()
    # Creates tables if missing. Does NOT delete or overwrite data.
    Base.metadata.create_all(bind=engine)
    ensure_email_notification_schema(engine)
    ensure_price_cache_volume_columns(engine)
    ensure_fundamentals_cache_schema(engine)
    ensure_event_columns()
    ensure_monitoring_alert_columns()
    ensure_house_annual_disclosure_schema()
    ensure_trade_outcomes_amount_bigint()
    ensure_government_contracts_schema(engine)
    db = SessionLocal()
    try:
        seed_plan_config(db)
    finally:
        db.close()

    db = SessionLocal()
    try:
        seed_default_email_templates(db)
    except Exception:
        logger.exception("email_template_seed_failed")
    finally:
        db.close()

    if os.getenv("AUTO_REPAIR_EVENTS_ON_STARTUP", "1").strip() in ("1", "true", "TRUE", "yes", "YES"):
        db = SessionLocal()
        try:
            if _needs_event_repair(db):
                from app.backfill_events_from_trades import repair_events

                repair_events(db)
        finally:
            db.close()

    # NEW: self-heal if the DB is empty (prevents empty feed after restarts/autostop)
    try:
        _autoheal_if_empty()
    except Exception as e:
        # Don't crash the app on boot — log and keep serving (you can still call /admin/ensure_data)
        print("AUTOHEAL EXCEPTION:", repr(e))

    if os.getenv("AUTO_BACKFILL_EVENTS_ON_STARTUP", "1").strip() in (
        "1",
        "true",
        "TRUE",
        "yes",
        "YES",
    ):
        db = SessionLocal()
        try:
            tx_count = db.execute(select(func.count()).select_from(Transaction)).scalar_one()
            event_count = db.execute(
                select(func.count())
                .select_from(Event)
                .where(Event.event_type == "congress_trade")
            ).scalar_one()
        finally:
            db.close()

        if tx_count > 0 and event_count == 0:
            logger.info("Auto-backfill triggered: transactions=%s events=0", tx_count)
            try:
                from app.backfill_events_from_trades import run_backfill

                results = run_backfill(
                    dry_run=False,
                    limit=None,
                    replace=False,
                    repair=False,
                )
                logger.info(
                    "Auto-backfill done: scanned=%s inserted=%s skipped=%s",
                    results.get("scanned", 0),
                    results.get("inserted", 0),
                    results.get("skipped", 0),
                )
            except Exception:
                logger.exception("Auto-backfill failed")


def _sqlite_path_from_database_url(database_url: str) -> str | None:
    """
    Supports:
      sqlite:////absolute/path.db
      sqlite:///relative-or-absolute/path.db
      sqlite:relative.db
    Returns an absolute-ish path string to the sqlite file, or None if not sqlite.
    """
    if not database_url or not database_url.startswith("sqlite:"):
        return None

    rest = database_url[len("sqlite:"):]

    # sqlite:////data/db.sqlite  -> /data/db.sqlite
    if rest.startswith("////"):
        return rest[3:]  # keep one leading slash

    # sqlite:///app/db.sqlite -> /app/db.sqlite (absolute)
    if rest.startswith("///"):
        return rest[2:]  # keep one leading slash

    # sqlite://relative.db -> relative.db
    if rest.startswith("//"):
        return rest[2:]

    # sqlite:relative.db -> relative.db
    return rest


def _utc_iso_from_mtime(path: str) -> str | None:
    try:
        mtime = os.path.getmtime(path)
    except Exception:
        return None
    dt = datetime.fromtimestamp(mtime, tz=timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/admin/seed-demo")
def seed_demo(request: Request, db: Session = Depends(get_db)):
    if _is_production_runtime():
        raise HTTPException(status_code=404, detail="Not found")
    require_admin_user(db, request)
    existing = db.execute(select(Member).where(Member.bioguide_id == "DEMO0001")).scalar_one_or_none()
    if existing:
        return {"status": "ok", "message": "Demo data already seeded."}

    m = Member(
        bioguide_id="DEMO0001",
        first_name="Demo",
        last_name="Member",
        chamber="house",
        party="I",
        state="CA",
    )
    s = Security(
        symbol="NVDA",
        name="NVIDIA Corporation",
        asset_class="stock",
        sector="Technology",
    )
    db.add_all([m, s])
    db.flush()

    f = Filing(
        member_id=m.id,
        source="house",
        filing_date=date(2026, 1, 9),
        document_url="https://example.com",
        document_hash="demo-1",
    )
    db.add(f)
    db.flush()

    tx = Transaction(
        filing_id=f.id,
        member_id=m.id,
        security_id=s.id,
        owner_type="self",
        transaction_type="buy",
        trade_date=date(2025, 12, 1),
        report_date=date(2026, 1, 9),
        amount_range_min=15000,
        amount_range_max=50000,
        description="Purchase - Demo",
    )
    db.add(tx)
    db.commit()

    return {"status": "ok", "message": "Seeded demo member + NVDA trade."}


@app.get("/api/feed")
def feed(
    db: Session = Depends(get_db),
    limit: int = Query(50, ge=1, le=200),
    cursor: str | None = None,
    tape: str = Query("congress"),
    symbol: str | None = None,
    member: str | None = None,
    chamber: str | None = None,
    transaction_type: str | None = None,
    min_amount: float | None = None,
    whale: int | None = Query(default=None),
    recent_days: int | None = None,
):
    tape_value = (tape or "congress").strip().lower()
    if tape_value not in {"congress", "insider", "all"}:
        raise HTTPException(status_code=400, detail="tape must be one of: congress, insider, all")

    if tape_value == "congress":
        from datetime import timedelta

        price_memo: dict[tuple[str, str], float | None] = {}

        q = (
            select(Transaction, Member, Security)
            .join(Member, Transaction.member_id == Member.id)
            .outerjoin(Security, Transaction.security_id == Security.id)
        )

        if whale:
            min_amount = max(min_amount or 0, 250000)

        if recent_days is not None:
            if recent_days < 1:
                raise HTTPException(status_code=400, detail="recent_days must be >= 1")
            cutoff = date.today() - timedelta(days=recent_days)
            q = q.where(Transaction.report_date >= cutoff)

        if symbol:
            q = q.where(Security.symbol == symbol.strip().upper())
        if chamber:
            q = q.where(Member.chamber == chamber.strip().lower())
        if transaction_type:
            q = q.where(Transaction.transaction_type == transaction_type.strip().lower())
        if min_amount is not None:
            q = q.where(
                or_(
                    Transaction.amount_range_max >= min_amount,
                    and_(Transaction.amount_range_max.is_(None), Transaction.amount_range_min >= min_amount),
                )
            )
        if member:
            term = f"%{member.strip().lower()}%"
            q = q.where(
                or_(
                    Member.first_name.ilike(term),
                    Member.last_name.ilike(term),
                    (Member.first_name + " " + Member.last_name).ilike(term),
                )
            )

        if cursor:
            try:
                cursor_date_str, cursor_id_str = cursor.split("|", 1)
                cursor_id = int(cursor_id_str)
                cursor_date = date.fromisoformat(cursor_date_str)
            except Exception:
                raise HTTPException(status_code=400, detail="Invalid cursor format. Expected YYYY-MM-DD|id")
            q = q.where(
                or_(
                    Transaction.report_date < cursor_date,
                    and_(Transaction.report_date == cursor_date, Transaction.id < cursor_id),
                )
            )

        q = q.order_by(Transaction.report_date.desc(), Transaction.id.desc()).limit(limit + 1)
        rows = db.execute(q).all()

        parsed_rows: list[tuple[Transaction, Member, Security | None, str | None, str | None, float | None]] = []
        quote_symbols: set[str] = set()
        for tx, m, s in rows[:limit]:
            estimated_price: float | None = None
            symbol_value = (s.symbol or "").strip().upper() if s is not None else None
            if not symbol_value:
                symbol_value = None
            trade_date_value = tx.trade_date.isoformat() if tx.trade_date else None
            if symbol_value and trade_date_value:
                memo_key = (symbol_value, trade_date_value)
                if memo_key not in price_memo:
                    price_memo[memo_key] = get_eod_close(db, symbol_value, trade_date_value)
                estimated_price = price_memo[memo_key]
            if symbol_value and estimated_price is not None and estimated_price > 0:
                quote_symbols.add(symbol_value)

            parsed_rows.append((tx, m, s, symbol_value, trade_date_value, estimated_price))

        current_price_memo = get_current_prices(_cap_symbols(quote_symbols)) if quote_symbols else {}

        items = []
        for tx, m, s, symbol_value, trade_date_value, estimated_price in parsed_rows:
            current_price = current_price_memo.get(symbol_value) if symbol_value else None
            pnl_pct = None
            if current_price is not None and estimated_price is not None and estimated_price > 0:
                pnl_pct = signed_return_pct(current_price, estimated_price, tx.transaction_type)

            security_payload = {
                "symbol": symbol_value,
                "name": s.name if s is not None else "Unknown",
                "asset_class": s.asset_class if s is not None else "Unknown",
                "sector": s.sector if s is not None else None,
            }
            items.append(
                {
                    "id": tx.id,
                    "event_type": "congress_trade",
                    "member": {
                        "bioguide_id": m.bioguide_id,
                        "name": f"{m.first_name or ''} {m.last_name or ''}".strip(),
                        "chamber": m.chamber,
                        "party": m.party,
                        "state": m.state,
                    },
                    "security": security_payload,
                    "transaction_type": tx.transaction_type,
                    "owner_type": tx.owner_type,
                    "trade_date": trade_date_value,
                    "report_date": tx.report_date.isoformat() if tx.report_date else None,
                    "amount_range_min": tx.amount_range_min,
                    "amount_range_max": tx.amount_range_max,
                    "is_whale": bool(tx.amount_range_max is not None and tx.amount_range_max >= 250000),
                    "estimated_price": estimated_price,
                    "current_price": current_price,
                    "pnl_pct": pnl_pct,
                }
            )

        next_cursor = None
        if len(rows) > limit:
            tx_last = rows[limit - 1][0]
            if tx_last.report_date:
                next_cursor = f"{tx_last.report_date.isoformat()}|{tx_last.id}"

        return {"items": items, "next_cursor": next_cursor}

    event_types = ["insider_trade"] if tape_value == "insider" else [*CONGRESS_DISCLOSURE_EVENT_TYPES, "insider_trade"]
    sort_ts = func.coalesce(Event.event_date, Event.ts)
    q = select(Event).where(Event.event_type.in_(event_types))

    if symbol:
        q = q.where(func.upper(Event.symbol) == symbol.strip().upper())
    if transaction_type:
        q = q.where(func.lower(Event.transaction_type) == transaction_type.strip().lower())
    if recent_days is not None:
        if recent_days < 1:
            raise HTTPException(status_code=400, detail="recent_days must be >= 1")
        cutoff_dt = datetime.now(timezone.utc) - timedelta(days=recent_days)
        q = q.where(sort_ts >= cutoff_dt)

    if cursor:
        try:
            cursor_ts_str, cursor_id_str = cursor.split("|", 1)
            cursor_id = int(cursor_id_str)
            cursor_ts = datetime.fromisoformat(cursor_ts_str.replace("Z", "+00:00"))
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid cursor format. Expected ISO8601|id")
        q = q.where(or_(sort_ts < cursor_ts, and_(sort_ts == cursor_ts, Event.id < cursor_id)))

    q = q.order_by(sort_ts.desc(), Event.id.desc()).limit(limit + 1)
    rows = db.execute(q).scalars().all()
    event_ids = [event.id for event in rows[:limit]]
    try:
        feed_outcomes = (
            db.execute(select(TradeOutcome).where(TradeOutcome.event_id.in_(event_ids))).scalars().all()
            if event_ids
            else []
        )
    except OperationalError:
        logger.warning("trade_outcomes table unavailable while serializing /api/feed", exc_info=True)
        feed_outcomes = []
    feed_outcome_by_event_id = {row.event_id: row for row in feed_outcomes}

    price_memo: dict[tuple[str, str], float | None] = {}
    parsed_events: list[tuple[Event, dict, str, float | None, float | None]] = []
    quote_symbols: set[str] = set()

    for event in rows[:limit]:
        try:
            payload = json.loads(event.payload_json)
            if not isinstance(payload, dict):
                payload = {}
        except Exception:
            payload = {}

        outcome = feed_outcome_by_event_id.get(event.id)
        if event.event_type == "congress_trade" and outcome is not None:
            symbol_value = (outcome.symbol or event.symbol or payload.get("symbol") or "").strip().upper()
            entry_price = outcome.entry_price
            estimated_price = outcome.entry_price
        else:
            symbol_value, entry_price, estimated_price = _feed_entry_price_for_event(db, event, payload, price_memo)
        if symbol_value and entry_price is not None and entry_price > 0:
            quote_symbols.add(symbol_value)

        parsed_events.append((event, payload, symbol_value, entry_price, estimated_price))

    current_price_memo = get_current_prices_db(db, _cap_symbols(quote_symbols)) if quote_symbols else {}

    insider_symbols = sorted(
        {
            symbol
            for event, _, symbol, _, _ in parsed_events
            if event.event_type == "insider_trade" and symbol
        }
    )
    try:
        ticker_meta = _ticker_meta_with_security_names(db, insider_symbols) if insider_symbols else {}
    except Exception:
        logger.exception("ticker_meta resolver failed in /api/feed")
        ticker_meta = {}

    insider_ciks = sorted(
        {
            cik
            for event, payload, _, _, _ in parsed_events
            for cik in [_event_payload_cik(payload)]
            if event.event_type == "insider_trade" and cik
        }
    )
    try:
        cik_names = get_cik_meta(db, insider_ciks, allow_refresh=False) if insider_ciks else {}
    except Exception:
        logger.exception("cik_meta resolver failed in /api/feed")
        cik_names = {}

    items = []
    for event, payload, symbol_value, entry_price, estimated_price in parsed_events:
        if event.event_type == "insider_trade":
            payload = _enrich_event_payload_company_name(event, payload, ticker_meta, cik_names)
            current_price = current_price_memo.get(symbol_value) if symbol_value else None
            pnl_pct = None
            if current_price is not None and entry_price is not None and entry_price > 0:
                pnl_pct = signed_return_pct(current_price, entry_price, event.transaction_type or event.trade_type)

            canonical_trade = _insider_trade_row(
                event,
                payload,
                outcome=None,
                fallback_pnl_pct=pnl_pct,
                prefer_fallback_pnl=True,
            )
            trade_value = canonical_trade.get("trade_value")
            whale_value = trade_value if trade_value is not None else event.amount_max
            company_name = (
                canonical_trade.get("company_name")
                or canonical_trade.get("security_name")
                or payload.get("company_name")
                or payload.get("security_name")
                or event.symbol
                or "Unknown"
            )
            security_class = canonical_trade.get("security_name") or payload.get("security_name") or "stock"

            items.append(
                {
                    "id": event.id,
                    "event_type": event.event_type,
                    "member": {
                        "bioguide_id": event.member_bioguide_id,
                        "name": canonical_trade.get("insider_name") or event.member_name,
                        "chamber": event.chamber,
                        "party": event.party,
                        "state": None,
                    },
                    "security": {
                        "symbol": canonical_trade.get("symbol") or event.symbol,
                        "name": company_name,
                        "asset_class": security_class,
                        "sector": payload.get("sector"),
                    },
                    "insider": {
                        "name": canonical_trade.get("insider_name") or event.member_name,
                        "ownership": payload.get("owner_type") or payload.get("ownership"),
                        "filing_date": canonical_trade.get("filing_date"),
                        "transaction_date": canonical_trade.get("transaction_date"),
                        "price": canonical_trade.get("price"),
                        "display_price": canonical_trade.get("display_price"),
                        "reported_price": canonical_trade.get("reported_price"),
                        "reported_price_currency": canonical_trade.get("reported_price_currency"),
                        "role": canonical_trade.get("role"),
                        "reporting_cik": canonical_trade.get("reporting_cik"),
                    },
                    "security_name": canonical_trade.get("security_name"),
                    "company_name": canonical_trade.get("company_name"),
                    "transaction_type": canonical_trade.get("trade_type") or event.transaction_type or event.trade_type,
                    "owner_type": payload.get("owner_type") or "insider",
                    "trade_date": canonical_trade.get("transaction_date"),
                    "report_date": canonical_trade.get("filing_date") or payload.get("report_date"),
                    "amount_range_min": trade_value if trade_value is not None else event.amount_min,
                    "amount_range_max": trade_value if trade_value is not None else event.amount_max,
                    "is_whale": bool(whale_value is not None and whale_value >= 250000),
                    "source": event.source,
                    "estimated_price": canonical_trade.get("price"),
                    "current_price": current_price,
                    "display_price": canonical_trade.get("display_price"),
                    "reported_price": canonical_trade.get("reported_price"),
                    "reported_price_currency": canonical_trade.get("reported_price_currency"),
                    "pnl_pct": canonical_trade.get("pnl_pct"),
                    "pnl_source": canonical_trade.get("pnl_source"),
                    "smart_score": canonical_trade.get("smart_score"),
                    "smart_band": canonical_trade.get("smart_band"),
                    "payload": {
                        **payload,
                        "company_name": canonical_trade.get("company_name"),
                        "companyName": canonical_trade.get("companyName"),
                        "security_name": canonical_trade.get("security_name"),
                        "securityName": canonical_trade.get("securityName"),
                        "trade_value": canonical_trade.get("trade_value"),
                        "tradeValue": canonical_trade.get("tradeValue"),
                        "display_price": canonical_trade.get("display_price"),
                        "displayPrice": canonical_trade.get("displayPrice"),
                        "display_price_currency": canonical_trade.get("display_price_currency"),
                        "displayPriceCurrency": canonical_trade.get("displayPriceCurrency"),
                        "display_share_basis": canonical_trade.get("display_share_basis"),
                        "displayShareBasis": canonical_trade.get("displayShareBasis"),
                        "reported_price": canonical_trade.get("reported_price"),
                        "reportedPrice": canonical_trade.get("reportedPrice"),
                        "reported_price_currency": canonical_trade.get("reported_price_currency"),
                        "reportedPriceCurrency": canonical_trade.get("reportedPriceCurrency"),
                        "reported_share_basis": canonical_trade.get("reported_share_basis"),
                        "reportedShareBasis": canonical_trade.get("reportedShareBasis"),
                        "price_normalization": canonical_trade.get("price_normalization"),
                        "priceNormalization": canonical_trade.get("priceNormalization"),
                        "shares": canonical_trade.get("shares"),
                        "insider_name": canonical_trade.get("insider_name"),
                        "reporting_cik": canonical_trade.get("reporting_cik"),
                        "role": canonical_trade.get("role"),
                        "smart_score": canonical_trade.get("smart_score"),
                        "smartScore": canonical_trade.get("smartScore"),
                        "smart_band": canonical_trade.get("smart_band"),
                        "smartBand": canonical_trade.get("smartBand"),
                    },
                }
            )
            continue

        current_price = current_price_memo.get(symbol_value) if symbol_value else None
        pnl_pct = None
        outcome = feed_outcome_by_event_id.get(event.id)
        pnl_source = None
        outcome_status = None
        outcome_skip_reason = None
        if event.event_type == "congress_trade" and outcome is not None:
            display_metrics = trade_outcome_display_metrics(outcome)
            current_price = outcome.current_price if outcome.current_price is not None else current_price
            pnl_pct = display_metrics.return_pct
            pnl_source = "eod" if pnl_pct is not None and outcome.entry_price is not None else display_metrics.pnl_source
            outcome_status = _safe_outcome_status(outcome.scoring_status)
            if pnl_pct is None:
                outcome_skip_reason = outcome_status
        if pnl_pct is None and current_price is not None and entry_price is not None and entry_price > 0:
            pnl_pct = signed_return_pct(current_price, entry_price, event.transaction_type or event.trade_type)
        amount_min = event.amount_min
        amount_max = event.amount_max
        if event.event_type == "insider_trade" and entry_price is not None:
            shares = _parse_numeric(payload.get("shares"))
            if shares is not None and shares > 0:
                normalized_value = int(round(entry_price * shares))
                amount_min = normalized_value
                amount_max = normalized_value

        items.append(
            {
                "id": event.id,
                "event_type": event.event_type,
                "member": {
                    "bioguide_id": event.member_bioguide_id,
                    "name": event.member_name,
                    "chamber": event.chamber,
                    "party": event.party,
                    "state": None,
                },
                "security": {
                    "symbol": event.symbol,
                    "name": payload.get("security_name") or payload.get("insider_name") or event.symbol or "Unknown",
                    "asset_class": payload.get("asset_class") or "stock",
                    "sector": payload.get("sector"),
                },
                "transaction_type": event.transaction_type or event.trade_type,
                "owner_type": payload.get("owner_type") or "insider",
                "trade_date": payload.get("transaction_date") or payload.get("trade_date"),
                "report_date": payload.get("filing_date") or payload.get("report_date"),
                "amount_range_min": amount_min,
                "amount_range_max": amount_max,
                "is_whale": bool(amount_max is not None and amount_max >= 250000),
                "source": event.source,
                "estimated_price": estimated_price,
                "price": estimated_price,
                "trade_price": estimated_price,
                "current_price": current_price,
                "pnl_pct": pnl_pct,
                "return_pct": pnl_pct,
                "pnl_source": pnl_source,
                "outcome_status": outcome_status,
                "outcome_skip_reason": outcome_skip_reason,
            }
        )

    next_cursor = None
    if len(rows) > limit:
        last = rows[limit - 1]
        cursor_ts = last.event_date or last.ts
        next_cursor = f"{cursor_ts.isoformat()}|{last.id}"

    return {"items": items, "next_cursor": next_cursor}



@app.get("/api/meta")
def meta():
    # IMPORTANT: use the same resolved DATABASE_URL the app uses (not env-only),
    # so meta works even when DATABASE_URL isn't explicitly set.
    db_file = _sqlite_path_from_database_url(DATABASE_URL)

    last_updated_utc = None
    if db_file:
        if not db_file.startswith("/"):
            db_file = os.path.abspath(db_file)
        last_updated_utc = _utc_iso_from_mtime(db_file)

    # Fallback if not sqlite OR file missing:
    if last_updated_utc is None:
        db = SessionLocal()
        try:
            latest = db.execute(select(func.max(Filing.filing_date))).scalar_one_or_none()
            if latest:
                dt = datetime(latest.year, latest.month, latest.day, tzinfo=timezone.utc)
                last_updated_utc = dt.isoformat().replace("+00:00", "Z")
        finally:
            db.close()

    return {"last_updated_utc": last_updated_utc}

def _run_module(module: str) -> dict:
    """
    Runs: python3 -m <module>
    Returns stdout/stderr and exit code.
    """
    p = subprocess.run(
        ["python3", "-m", module],
        capture_output=True,
        text=True,
        cwd="/app",
    )
    return {
        "module": module,
        "returncode": p.returncode,
        "stdout": p.stdout[-4000:],  # keep it small
        "stderr": p.stderr[-4000:],
    }


@app.post("/admin/ensure_data")
def ensure_data(request: Request, db: Session = Depends(get_db)):
    """
    If transactions == 0, run ingest_house + ingest_senate + enrich_members + write_last_updated.
    Safe to call repeatedly.
    """
    require_admin_user(db, request)

    tx_count = db.execute(select(func.count()).select_from(Transaction)).scalar_one()
    if tx_count and tx_count > 0:
        return {"status": "ok", "did_ingest": False, "transactions": tx_count}

    # DB empty -> run ingest chain
    results = []
    for mod in ["app.ingest_house", "app.ingest_senate", "app.enrich_members", "app.write_last_updated"]:
        r = _run_module(mod)
        results.append(r)
        if r["returncode"] != 0:
            raise HTTPException(status_code=500, detail={"status": "failed", "step": mod, "results": results})

    # Re-check count
    tx_count2 = db.execute(select(func.count()).select_from(Transaction)).scalar_one()
    return {"status": "ok", "did_ingest": True, "transactions": tx_count2, "results": results}


@app.get("/admin/congress-ingest/freshness")
def congress_ingest_freshness(request: Request, db: Session = Depends(get_db)):
    require_admin_user(db, request)
    row = db.get(AppSetting, CONGRESS_RECENT_STATUS_KEY)
    status = None
    if row and row.value:
        try:
            status = json.loads(row.value)
        except json.JSONDecodeError:
            status = {"raw": row.value}

    latest_by_source = {
        source: latest.isoformat() if latest else None
        for source, latest in db.execute(
            select(Filing.source, func.max(Filing.filing_date))
            .where(Filing.source.in_(("house_fmp", "senate_fmp")))
            .group_by(Filing.source)
        )
    }
    latest_by_chamber = {
        chamber: latest.isoformat() if latest else None
        for chamber, latest in db.execute(
            select(Member.chamber, func.max(Transaction.report_date))
            .join(Member, Member.id == Transaction.member_id)
            .group_by(Member.chamber)
        )
    }
    latest_event_ts = db.execute(
        select(func.max(Event.ts)).where(Event.event_type == "congress_trade")
    ).scalar_one_or_none()
    return {
        "last_recent_ingest": status,
        "latest_house_report_date": latest_by_chamber.get("house") or latest_by_source.get("house_fmp"),
        "latest_senate_report_date": latest_by_chamber.get("senate") or latest_by_source.get("senate_fmp"),
        "latest_congress_event_ts": latest_event_ts.isoformat() if latest_event_ts else None,
    }


@app.get("/api/members/by-slug/{slug}")
def member_profile_by_slug(
    slug: str,
    include_trades: bool = Query(default=True),
    db: Session = Depends(get_db),
):
    slug_value = (slug or "").strip()
    if not slug_value:
        raise HTTPException(status_code=404, detail="Member not found")

    direct = db.execute(select(Member).where(Member.bioguide_id == slug_value)).scalar_one_or_none()
    if direct:
        if include_trades:
            return _build_member_profile(db, direct)
        return {
            "member": _member_payload(direct),
            "top_tickers": _member_top_tickers(db, direct),
            "trades": [],
        }

    normalized = _slug_to_name(slug_value)
    if not normalized:
        raise HTTPException(status_code=404, detail="Member not found")

    members = db.execute(select(Member)).scalars().all()
    matched = [member for member in members if _normalize_name(_member_full_name(member)) == normalized]

    if not matched:
        raise HTTPException(status_code=404, detail="Member not found")

    member = matched[0]
    if include_trades:
        return _build_member_profile(db, member)
    return {
        "member": _member_payload(member),
        "top_tickers": _member_top_tickers(db, member),
        "trades": [],
    }


@app.get("/api/members/{bioguide_id}")
def member_profile(bioguide_id: str, db: Session = Depends(get_db)):
    member = _resolve_member_legacy_compat(db, bioguide_id)

    if not member:
        raise HTTPException(status_code=404, detail="Member not found")

    return _build_member_profile(db, member)

@app.get("/api/members/{member_id}/performance")
def member_performance(member_id: str, lookback_days: int = 365, benchmark: str = "^GSPC", db: Session = Depends(get_db)):
    """Member performance metrics from persisted trade outcomes."""
    resolved_member, analytics_member_ids = _resolve_member_analytics_aliases(db, member_id)
    analytics_member_id = resolved_member.bioguide_id if resolved_member else member_id
    benchmark_symbol = (benchmark or "^GSPC").strip() or "^GSPC"
    rows = get_member_trade_outcomes(
        db=db,
        member_id=analytics_member_id,
        member_ids=analytics_member_ids,
        lookback_days=lookback_days,
        benchmark_symbol=benchmark_symbol,
    )
    total_count = count_member_trade_outcomes(
        db=db,
        member_id=analytics_member_id,
        member_ids=analytics_member_ids,
        lookback_days=lookback_days,
        benchmark_symbol=benchmark_symbol,
    )

    return_values = [row.return_pct for row in rows if row.return_pct is not None]
    alpha_values = [row.alpha_pct for row in rows if row.alpha_pct is not None]
    trade_count_scored = len(rows)

    return {
        "member_id": analytics_member_id,
        "lookback_days": lookback_days,
        "trade_count_total": total_count,
        "trade_count_scored": trade_count_scored,
        "avg_return": mean(return_values) if return_values else None,
        "median_return": median(return_values) if return_values else None,
        "win_rate": (sum(1 for value in return_values if value > 0) / trade_count_scored) if trade_count_scored else None,
        "avg_alpha": mean(alpha_values) if alpha_values else None,
        "median_alpha": median(alpha_values) if alpha_values else None,
        "benchmark_symbol": benchmark_symbol,
        "persisted_only": True,
        "pnl_status": "ok" if trade_count_scored > 0 or total_count == 0 else "unavailable",
    }


@app.get("/api/members/{member_id}/portfolio-performance")
def member_portfolio_performance(
    member_id: str,
    lookback_days: int = 1095,
    mode: str = "realistic_disclosure_lag",
    benchmark: str = "^GSPC",
    db: Session = Depends(get_db),
):
    """Read-only replicated portfolio performance from persisted portfolio runs."""
    resolved_member, _ = _resolve_member_analytics_aliases(db, member_id)
    analytics_member_id = resolved_member.bioguide_id if resolved_member else member_id
    normalized_mode = (mode or "realistic_disclosure_lag").strip()
    if normalized_mode not in {"realistic_disclosure_lag", "theoretical_transaction_date"}:
        raise HTTPException(status_code=400, detail="Unsupported portfolio mode.")
    benchmark_symbol = normalize_symbol(benchmark) or "^GSPC"
    payload = latest_replicated_portfolio_payload(
        db,
        entity_type="congress_member",
        entity_id=analytics_member_id,
        lookback_days=lookback_days,
        mode=normalized_mode,
        benchmark=benchmark_symbol,
    )
    public_safety_flags = _portfolio_payload_public_safety_flags(payload)
    if public_safety_flags:
        return _unavailable_portfolio_payload(payload, public_safety_flags)
    return payload


@app.get("/api/members/{member_id}/trades")
def member_trades(member_id: str, lookback_days: int = 365, limit: int = 100, db: Session = Depends(get_db)):
    member = _resolve_member_legacy_compat(db, member_id)
    if not member:
        raise HTTPException(status_code=404, detail="Member not found")

    safe_limit = min(max(limit, 1), 200)
    items = _member_recent_trades(
        db=db,
        member_pk=member.id,
        lookback_days=lookback_days,
        limit=safe_limit,
    )
    return {
        "member_id": member.bioguide_id,
        "lookback_days": lookback_days,
        "limit": safe_limit,
        "items": items,
    }


@app.get("/api/members/{member_id}/alpha-summary")
def member_alpha_summary(member_id: str, lookback_days: int = 365, benchmark: str = "^GSPC", debug_dates: bool = False, db: Session = Depends(get_db)):
    resolved_member, analytics_member_ids = _resolve_member_analytics_aliases(db, member_id)
    analytics_member_id = resolved_member.bioguide_id if resolved_member else member_id
    benchmark_symbol = (benchmark or "^GSPC").strip() or "^GSPC"
    rows = get_member_trade_outcomes(
        db=db,
        member_id=analytics_member_id,
        member_ids=analytics_member_ids,
        lookback_days=lookback_days,
        benchmark_symbol=benchmark_symbol,
    )

    count = len(rows)
    return_values = [row.return_pct for row in rows if row.return_pct is not None]
    alpha_values = [row.alpha_pct for row in rows if row.alpha_pct is not None]
    holding_day_values = [row.holding_days for row in rows if isinstance(row.holding_days, int)]

    def _trade_view(row: TradeOutcome) -> dict:
        return {
            "event_id": row.event_id,
            "symbol": row.symbol,
            "trade_type": row.trade_type,
            "asof_date": row.trade_date.isoformat() if row.trade_date else None,
            "return_pct": row.return_pct,
            "alpha_pct": row.alpha_pct,
            "holding_days": row.holding_days,
        }

    ranked_rows = [row for row in rows if row.return_pct is not None]
    best_trades = [_trade_view(row) for row in sorted(ranked_rows, key=lambda item: item.return_pct, reverse=True)[:5]]
    worst_trades = [_trade_view(row) for row in sorted(ranked_rows, key=lambda item: item.return_pct)[:5]]

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


    if debug_dates:
        sample_front = [
            {"event_id": row.event_id, "trade_date": row.trade_date.isoformat() if row.trade_date else None}
            for row in rows[:5]
        ]
        sample_back = [
            {"event_id": row.event_id, "trade_date": row.trade_date.isoformat() if row.trade_date else None}
            for row in rows[-5:]
        ]
        benchmark_front = benchmark_dates[:5]
        benchmark_back = benchmark_dates[-5:]
        print(
            "[member_alpha_summary_debug]",
            {
                "member_id": member_id,
                "raw_trade_dates_first": sample_front,
                "raw_trade_dates_last": sample_back,
                "benchmark_dates_first": benchmark_front,
                "benchmark_dates_last": benchmark_back,
            },
        )

    curve = build_normalized_profile_curve(
        outcomes=rows,
        timeline_dates=timeline_dates,
        benchmark_close_map=benchmark_close_map,
        benchmark_dates=benchmark_dates,
        price_close_maps=load_profile_price_close_maps(
            db=db,
            outcomes=rows,
            start_date=start_date,
            end_date=end_date,
        ),
    )

    # Profile analytics summarize persisted scored trade outcomes one row at a time:
    # avg_return_pct is the arithmetic mean of signed per-trade returns from trade_date
    # to the latest/current scored price, and avg_alpha_pct is the arithmetic mean of
    # each trade's return minus S&P 500 return over that same scored trade window.
    # These are not CAGR or portfolio alpha; the backtest endpoint separately simulates
    # capital allocation, disclosure-timed entries, monthly rebalancing, and hold_days.
    return {
        "member_id": analytics_member_id,
        "lookback_days": lookback_days,
        "benchmark_symbol": benchmark_symbol,
        "metric_definitions": {
            "avg_return_pct": "Arithmetic mean of scored per-trade signed returns in the selected lookback.",
            "avg_alpha_pct": "Arithmetic mean of scored per-trade return minus S&P 500 return.",
            "profile_curve": "Equal-weight scored trade outcome curve, not a capital-constrained portfolio simulation.",
            "date_source": "trade_date",
            "hold_period": "Uses persisted outcome holding_days through the latest/current scored price; no fixed hold_days selector.",
            "backtest_difference": "Backtests use disclosure or filing timing, configurable hold_days, monthly rebalancing, and portfolio CAGR/alpha.",
        },
        "trades_analyzed": count,
        "avg_return_pct": mean(return_values) if return_values else None,
        "avg_alpha_pct": mean(alpha_values) if alpha_values else None,
        "win_rate": (sum(1 for value in return_values if value > 0) / count) if count else None,
        "avg_holding_days": mean(holding_day_values) if holding_day_values else None,
        "best_trades": best_trades,
        "worst_trades": worst_trades,
        "member_series": curve.member_series,
        "benchmark_series": curve.benchmark_series,
    }


@app.get("/api/leaderboards/congress-traders")
def congress_trader_leaderboard(
    request: Request,
    lookback_days: int = 365,
    chamber: str = "all",
    source_mode: str = "congress",
    performance_model: str = "trade_outcomes",
    mode: str = "realistic_disclosure_lag",
    sort: str = "avg_alpha",
    min_trades: int = 3,
    limit: int = 100,
    benchmark: str = "^GSPC",
    include_poor_quality: bool = False,
    db: Session = Depends(get_db),
):
    current_user(db, request, required=True)
    require_feature(
        current_entitlements(request, db),
        "leaderboards",
        message="Leaderboards are included with Premium.",
    )
    perf = _LeaderboardPerfTracker(
        mode=(source_mode or "congress").strip().lower() or "congress",
        lookback_days=lookback_days,
        min_trades=min_trades,
        limit=limit,
    )
    benchmark_symbol = (benchmark or "^GSPC").strip() or "^GSPC"
    normalized_chamber = (chamber or "all").strip().lower()
    if normalized_chamber not in {"all", "house", "senate"}:
        normalized_chamber = "all"

    normalized_source_mode = (source_mode or "congress").strip().lower()
    if normalized_source_mode not in {"all", "congress", "insiders"}:
        normalized_source_mode = "congress"

    normalized_performance_model = (performance_model or "trade_outcomes").strip().lower()
    if normalized_performance_model in {"legacy", "trade_outcome", "trade_outcomes", "scored_trades"}:
        normalized_performance_model = "trade_outcomes"
    elif normalized_performance_model != "portfolio":
        normalized_performance_model = "trade_outcomes"

    normalized_portfolio_mode = (mode or "realistic_disclosure_lag").strip().lower()
    if normalized_performance_model == "portfolio" and normalized_portfolio_mode not in {"realistic_disclosure_lag"}:
        raise HTTPException(status_code=400, detail="Unsupported portfolio mode.")

    normalized_sort = (sort or "avg_alpha").strip().lower()
    valid_sorts = {"avg_alpha", "avg_return", "win_rate", "trade_count"}
    if normalized_sort not in valid_sorts:
        normalized_sort = "avg_alpha"

    min_trades = max(min_trades, 1)
    limit = min(max(limit, 1), 250)
    perf.min_trades = min_trades
    perf.limit = limit

    cutoff_dt = datetime.utcnow() - timedelta(days=lookback_days)
    cutoff_date = cutoff_dt.date()

    if normalized_source_mode == "congress" and normalized_performance_model == "portfolio":
        normalized_portfolio_sort = _normalize_portfolio_leaderboard_sort(sort)
        rows, missing_portfolio_runs_count, excluded_poor_quality_count, included_quality_statuses = _load_congress_portfolio_leaderboard_rows(
            db,
            normalized_chamber=normalized_chamber,
            benchmark_symbol=benchmark_symbol,
            lookback_days=lookback_days,
            mode=normalized_portfolio_mode,
            limit=limit,
            normalized_sort=normalized_portfolio_sort,
            include_poor_quality=include_poor_quality,
        )
        perf.stage("portfolio_runs_fetch", rows=len(rows))
        perf.stage("portfolio_alias_logical_identity_grouping", rows=len(rows))
        perf.stage("final_sort_rank_limit", rows=len(rows))
        generated_at = datetime.now(timezone.utc).isoformat()
        metadata = {
            "performance_model": "portfolio",
            "persisted_only": True,
            "lookback_days": lookback_days,
            "mode": normalized_portfolio_mode,
            "sort": normalized_portfolio_sort,
            "rows_returned": len(rows),
            "missing_portfolio_runs_count": missing_portfolio_runs_count,
            "quality_filter_applied": False,
            "excluded_poor_quality_count": excluded_poor_quality_count,
            "included_quality_statuses": included_quality_statuses,
            "generated_at": generated_at,
        }
        response = {
            "lookback_days": lookback_days,
            "chamber": normalized_chamber,
            "source_mode": normalized_source_mode,
            "performance_model": "portfolio",
            "persisted_only": True,
            "mode": normalized_portfolio_mode,
            "sort": normalized_portfolio_sort,
            "limit": limit,
            "benchmark_symbol": benchmark_symbol,
            "quality_filter_applied": False,
            "excluded_poor_quality_count": excluded_poor_quality_count,
            "included_quality_statuses": included_quality_statuses,
            "rows": rows,
            "metadata": metadata,
        }
        if not rows:
            response["status"] = "portfolio_runs_not_populated"
            response["message"] = "No persisted replicated portfolio runs available for the requested filters yet."
        perf.finish(result_rows=len(rows))
        return response

    if normalized_source_mode == "congress":
        if _has_persisted_congress_member_aliases(db, normalized_chamber):
            rows = _load_congress_leaderboard_rows_from_snapshot(
                db,
                normalized_chamber=normalized_chamber,
                benchmark_symbol=benchmark_symbol,
                cutoff_date=cutoff_date,
                min_trades=min_trades,
                limit=limit,
                normalized_sort=normalized_sort,
            )
            perf.stage("candidate_row_fetch", rows=len(rows))
            perf.stage("alias_logical_identity_grouping_snapshot", rows=len(rows))
            perf.stage("trade_outcomes_aggregation", rows=len(rows))
            perf.stage("per_row_enrichment_link_building", rows=len(rows))
            for idx, row in enumerate(rows, start=1):
                row["rank"] = idx
            perf.stage("final_sort_rank_limit", rows=len(rows))

            response = {
                "lookback_days": lookback_days,
                "chamber": normalized_chamber,
                "source_mode": normalized_source_mode,
                "sort": normalized_sort,
                "min_trades": min_trades,
                "limit": limit,
                "benchmark_symbol": benchmark_symbol,
                "rows": rows,
            }
            if not rows:
                response["status"] = "outcomes_not_populated"
                response["message"] = "No persisted trade outcomes available for the requested filters yet."
            perf.finish(result_rows=len(rows))
            return response

        identity_snapshot, identity_cache_hit = _get_congress_identity_snapshot(db, normalized_chamber)
        perf.stage(
            "candidate_row_fetch",
            rows=int(identity_snapshot.get("candidate_member_count", 0)),
        )
        perf.stage(
            "alias_logical_identity_grouping"
            if not identity_cache_hit
            else "alias_logical_identity_grouping_cache_hit",
            rows=int(identity_snapshot.get("logical_member_count", 0)),
        )
        perf.stage("trade_outcomes_aggregation", rows=int(identity_snapshot.get("merged_group_count", 0)))
        outcome_rows = db.execute(
            select(TradeOutcome)
            .where(TradeOutcome.member_id.in_(identity_snapshot["all_aliases"]))
            .where(TradeOutcome.benchmark_symbol == benchmark_symbol)
            .where(TradeOutcome.trade_date.is_not(None))
            .where(TradeOutcome.trade_date >= cutoff_date)
        ).scalars().all()
        perf.stage("trade_outcomes_fetch", rows=len(outcome_rows))

        outcomes_by_member_id: dict[str, list[TradeOutcome]] = {}
        for outcome in outcome_rows:
            if not outcome.member_id:
                continue
            outcomes_by_member_id.setdefault(outcome.member_id, []).append(outcome)

        rows: list[dict] = []
        grouped_outcomes: dict[str, list[TradeOutcome]] = {}
        alias_to_group_key = identity_snapshot["alias_to_group_key"]
        for outcome in outcome_rows:
            group_key = alias_to_group_key.get(outcome.member_id or "")
            if group_key:
                grouped_outcomes.setdefault(group_key, []).append(outcome)

        for group_key, aliases in identity_snapshot["merged_aliases"].items():
            profile = identity_snapshot["profiles"].get(group_key)
            if profile is None:
                continue
            aliases = [alias for alias in aliases if alias]
            if not aliases:
                aliases = [group_key]

            group_outcomes = grouped_outcomes.get(group_key)
            if group_outcomes is None:
                group_outcomes = []
                for alias in aliases:
                    group_outcomes.extend(outcomes_by_member_id.get(alias, []))

            scored_outcomes = dedupe_member_trade_outcomes(
                [row for row in group_outcomes if row.scoring_status == "ok"]
            )
            trade_count_scored = len(scored_outcomes)
            if trade_count_scored < min_trades:
                continue

            trade_count_total = len(dedupe_member_trade_outcomes(group_outcomes))
            return_values = [row.return_pct for row in scored_outcomes if row.return_pct is not None]
            alpha_values = [row.alpha_pct for row in scored_outcomes if row.alpha_pct is not None]
            authoritative_member_id = sorted(
                aliases,
                key=lambda value: (_is_legacy_fmp_member_id(value), value),
            )[0]
            rows.append(
                {
                    "member_id": authoritative_member_id,
                    "member_name": profile["member_name"] or authoritative_member_id,
                    "member_slug": profile["member_slug"] or authoritative_member_id,
                    "chamber": profile["chamber"],
                    "party": profile["party"],
                    "state": profile["state"],
                    "trade_count_total": trade_count_total,
                    "trade_count_scored": trade_count_scored,
                    "avg_return": mean(return_values) if return_values else None,
                    "median_return": median(return_values) if return_values else None,
                    "win_rate": (sum(1 for value in return_values if value > 0) / trade_count_scored) if trade_count_scored else None,
                    "avg_alpha": mean(alpha_values) if alpha_values else None,
                    "median_alpha": median(alpha_values) if alpha_values else None,
                    "benchmark_symbol": benchmark_symbol,
                    "pnl_status": "ok",
                }
            )
        perf.stage("per_row_enrichment_link_building", rows=len(rows))

        def sort_value(row: dict):
            if normalized_sort == "trade_count":
                return row["trade_count_total"]
            if normalized_sort == "avg_return":
                return row["avg_return"] if row["avg_return"] is not None else float("-inf")
            if normalized_sort == "win_rate":
                return row["win_rate"] if row["win_rate"] is not None else float("-inf")
            return row["avg_alpha"] if row["avg_alpha"] is not None else float("-inf")

        rows = sorted(
            rows,
            key=lambda row: (sort_value(row), row["trade_count_total"], row["trade_count_scored"]),
            reverse=True,
        )[:limit]
        for idx, row in enumerate(rows, start=1):
            row["rank"] = idx
        perf.stage("final_sort_rank_limit", rows=len(rows))

        response = {
            "lookback_days": lookback_days,
            "chamber": normalized_chamber,
            "source_mode": normalized_source_mode,
            "sort": normalized_sort,
            "min_trades": min_trades,
            "limit": limit,
            "benchmark_symbol": benchmark_symbol,
            "rows": rows,
        }
        perf.finish(result_rows=len(rows))
        return response

    insider_market_trade_types = {"purchase", "sale", "buy", "sell"}
    rows = _load_member_leaderboard_rows(
        db,
        normalized_source_mode=normalized_source_mode,
        normalized_chamber=normalized_chamber,
        insider_market_trade_types=insider_market_trade_types,
        benchmark_symbol=benchmark_symbol,
        cutoff_date=cutoff_date,
        min_trades=min_trades,
        limit=limit,
        normalized_sort=normalized_sort,
    )
    perf.stage("candidate_row_fetch", rows=len(rows))
    perf.stage("alias_logical_identity_grouping", rows=len(rows))
    perf.stage("trade_outcomes_aggregation", rows=len(rows))
    perf.stage("per_row_enrichment_link_building", rows=len(rows))

    if normalized_source_mode == "insiders" and rows:
        member_ids = [row["member_id"] for row in rows if row.get("member_id")]
        detail_rows = db.execute(
            select(
                TradeOutcome.member_id,
                TradeOutcome.symbol,
                Event.symbol,
                Event.payload_json,
            )
            .select_from(TradeOutcome)
            .join(Event, Event.id == TradeOutcome.event_id)
            .where(TradeOutcome.member_id.in_(member_ids))
            .where(TradeOutcome.benchmark_symbol == benchmark_symbol)
            .where(TradeOutcome.trade_date.is_not(None))
            .where(TradeOutcome.trade_date >= cutoff_dt.date())
            .where(Event.event_type == "insider_trade")
            .where(func.lower(func.coalesce(Event.trade_type, "")).in_(insider_market_trade_types))
            .order_by(TradeOutcome.member_id, TradeOutcome.trade_date.desc(), TradeOutcome.id.desc())
        ).all()

        def _payload_dicts(payload_json: str | None) -> list[dict]:
            if not payload_json:
                return []
            try:
                parsed = json.loads(payload_json)
            except Exception:
                return []
            if not isinstance(parsed, dict):
                return []
            payloads = [parsed]
            nested_payload = parsed.get("payload")
            if isinstance(nested_payload, dict):
                payloads.append(nested_payload)
            raw_payload = parsed.get("raw")
            if isinstance(raw_payload, dict):
                payloads.append(raw_payload)
            return payloads

        def _first_text(payloads: list[dict], keys: list[str]) -> str | None:
            for payload in payloads:
                for key in keys:
                    value = payload.get(key)
                    if not isinstance(value, str):
                        continue
                    cleaned = value.strip()
                    if cleaned:
                        return cleaned
            return None

        insider_detail_by_member: dict[str, dict] = {}
        for member_id, outcome_symbol, event_symbol, payload_json in detail_rows:
            if not member_id or member_id in insider_detail_by_member:
                continue
            payloads = _payload_dicts(payload_json)
            reporting_cik = _first_text(payloads, ["reporting_cik", "reportingCik", "reportingCIK", "rptOwnerCik"])
            company_name = _first_text(
                payloads,
                ["company_name", "companyName", "issuer_name", "issuerName"],
            )
            role = _first_text(payloads, ["role", "typeOfOwner", "officerTitle", "insiderRole", "position"])
            insider_detail_by_member[member_id] = {
                "symbol": (outcome_symbol or event_symbol or "").strip().upper() or None,
                "reporting_cik": reporting_cik,
                "company_name": company_name,
                "role": role,
            }

        for row in rows:
            member_id = row.get("member_id") or ""
            detail = insider_detail_by_member.get(member_id, {})
            row["symbol"] = row.get("symbol") or detail.get("symbol")
            row["reporting_cik"] = row.get("reporting_cik") or detail.get("reporting_cik")
            row["company_name"] = row.get("company_name") or detail.get("company_name")
            row["role"] = row.get("role") or detail.get("role")
            if not row.get("reporting_cik") and re.fullmatch(r"\d{10}", member_id):
                row["reporting_cik"] = member_id
        # NOTE: this enrichment uses a single batched query, avoiding per-row lookups for top insider rows.

    for idx, row in enumerate(rows, start=1):
        row["rank"] = idx
    perf.stage("final_sort_rank_limit", rows=len(rows))

    response = {
        "lookback_days": lookback_days,
        "chamber": normalized_chamber,
        "source_mode": normalized_source_mode,
        "sort": normalized_sort,
        "min_trades": min_trades,
        "limit": limit,
        "benchmark_symbol": benchmark_symbol,
        "rows": rows,
    }
    if not rows:
        response["status"] = "outcomes_not_populated"
        response["message"] = "No persisted trade outcomes available for the requested filters yet."
    perf.finish(result_rows=len(rows))
    return response


@app.get("/api/tickers")
def ticker_profiles(symbols: str | None = Query(None), db: Session = Depends(get_db)):
    with _heavy_route_slot("ticker_profiles", _TICKER_WIDGET_SEMAPHORE):
        return _ticker_profiles_response(symbols, db)


def _ticker_profiles_response(symbols: str | None, db: Session) -> dict:
    if symbols is None or not symbols.strip():
        return {"tickers": {}}

    parsed_symbols: list[str] = []
    seen_symbols: set[str] = set()
    for raw in symbols.split(","):
        sym = raw.strip().upper()
        if not sym or sym in seen_symbols:
            continue
        seen_symbols.add(sym)
        parsed_symbols.append(sym)
        if len(parsed_symbols) >= 50:
            break

    if not parsed_symbols:
        return {"tickers": {}}

    profiles: dict[str, dict] = {}
    for sym in parsed_symbols:
        try:
            profiles[sym] = _build_ticker_profile(sym, db)
        except LookupError:
            event_exists = db.execute(
                select(Event.id)
                .where(Event.symbol == sym)
                .limit(1)
            ).scalar_one_or_none()
            if event_exists is not None:
                profiles[sym] = {"ticker": {"symbol": sym, "name": sym}}

    return {"tickers": profiles}


@app.get("/api/tickers/{symbol}")
def ticker_profile(symbol: str, db: Session = Depends(get_db)):
    with _heavy_route_slot("ticker_profile", _TICKER_WIDGET_SEMAPHORE):
        return _ticker_profile_response(symbol, db)


def _ticker_profile_response(symbol: str, db: Session) -> dict:
    sym = symbol.upper().strip()
    try:
        return _build_ticker_profile(sym, db)
    except LookupError:
        event_exists = db.execute(
            select(Event.id)
            .where(Event.symbol == sym)
            .limit(1)
        ).scalar_one_or_none()
        if event_exists is not None:
            return {"ticker": {"symbol": sym, "name": sym}}
        raise HTTPException(status_code=404, detail="Ticker not found")


@app.get("/api/tickers/{symbol}/government-contracts")
def ticker_government_contracts(
    symbol: str,
    lookback_days: int = Query(365, ge=1, le=1095),
    min_amount: float = Query(1_000_000, ge=0),
    limit: int = Query(10, ge=1, le=100),
    db: Session = Depends(get_db),
):
    with _heavy_route_slot("ticker_government_contracts", _TICKER_WIDGET_SEMAPHORE):
        return get_government_contracts_for_symbol(
            db,
            symbol,
            lookback_days=lookback_days,
            min_amount=min_amount,
            limit=limit,
        )


@app.get("/api/departments")
def government_departments(db: Session = Depends(get_db)):
    return list_departments(db)


@app.get("/api/departments/{slug}")
def government_department_profile(
    slug: str,
    limit: int = Query(10, ge=1, le=50),
    db: Session = Depends(get_db),
):
    profile = get_department_profile(db, slug, limit=limit)
    if profile is None:
        raise HTTPException(status_code=404, detail="Department not found")
    return profile


def _ticker_chart_date_key(value) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    raw = str(value).strip()
    if not raw:
        return None
    day = raw[:10]
    try:
        parsed = datetime.strptime(day, "%Y-%m-%d")
    except ValueError:
        return None
    return parsed.strftime("%Y-%m-%d")


def _ticker_chart_payload(event: Event) -> dict:
    try:
        parsed = json.loads(event.payload_json or "{}")
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _ticker_chart_text(*values) -> str | None:
    for value in values:
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _ticker_chart_numeric(*values) -> float | None:
    for value in values:
        if isinstance(value, (int, float)):
            parsed = float(value)
            if parsed == parsed:
                return parsed
        if isinstance(value, str):
            cleaned = value.replace("$", "").replace(",", "").strip()
            if not cleaned:
                continue
            try:
                parsed = float(cleaned)
            except ValueError:
                continue
            if parsed == parsed:
                return parsed
    return None


def _ticker_chart_contract_details(payload: dict) -> tuple[str | None, float | None, str | None]:
    raw = payload.get("raw") if isinstance(payload.get("raw"), dict) else {}
    nested_payload = payload.get("payload") if isinstance(payload.get("payload"), dict) else {}
    agency = _ticker_chart_text(
        payload.get("awarding_agency"),
        payload.get("awardingAgency"),
        nested_payload.get("awarding_agency"),
        nested_payload.get("awardingAgency"),
        payload.get("agency"),
        nested_payload.get("agency"),
        raw.get("awarding_agency"),
        raw.get("awardingAgency"),
        raw.get("agency"),
    )
    amount = _ticker_chart_numeric(
        payload.get("obligated_amount"),
        payload.get("obligatedAmount"),
        payload.get("transaction_obligated_amount"),
        payload.get("transactionObligatedAmount"),
        payload.get("award_amount"),
        payload.get("awardAmount"),
        nested_payload.get("award_amount"),
        nested_payload.get("awardAmount"),
        payload.get("amount"),
        nested_payload.get("amount"),
        raw.get("award_amount"),
        raw.get("awardAmount"),
        raw.get("amount"),
    )
    description = _ticker_chart_text(
        payload.get("description"),
        nested_payload.get("description"),
        payload.get("summary"),
        nested_payload.get("summary"),
        payload.get("title"),
        nested_payload.get("title"),
        raw.get("description"),
        raw.get("summary"),
        raw.get("title"),
    )
    return agency, amount, description


def _ticker_chart_contract_day(event: Event, payload: dict) -> str | None:
    raw = payload.get("raw") if isinstance(payload.get("raw"), dict) else {}
    nested_payload = payload.get("payload") if isinstance(payload.get("payload"), dict) else {}
    for value in (
        payload.get("action_date"),
        payload.get("actionDate"),
        nested_payload.get("action_date"),
        nested_payload.get("actionDate"),
        raw.get("action_date"),
        raw.get("actionDate"),
        payload.get("report_date"),
        payload.get("reportDate"),
        nested_payload.get("report_date"),
        nested_payload.get("reportDate"),
        raw.get("report_date"),
        raw.get("reportDate"),
        payload.get("award_date"),
        payload.get("awardDate"),
        nested_payload.get("award_date"),
        nested_payload.get("awardDate"),
        raw.get("award_date"),
        raw.get("awardDate"),
        payload.get("period_start"),
        payload.get("periodStart"),
        nested_payload.get("period_start"),
        nested_payload.get("periodStart"),
        raw.get("period_start"),
        raw.get("periodStart"),
        event.event_date,
        event.ts,
    ):
        day = _ticker_chart_date_key(value)
        if day:
            return day
    return None


def _ticker_chart_event_day(event: Event, payload: dict) -> str | None:
    if event.event_type in GOVERNMENT_CONTRACT_EVENT_TYPES:
        return _ticker_chart_contract_day(event, payload)
    return ticker_event_date_key(event)


def _ticker_chart_insider_actor(event: Event, payload: dict) -> str:
    raw = payload.get("raw") if isinstance(payload.get("raw"), dict) else {}
    insider = payload.get("insider") if isinstance(payload.get("insider"), dict) else {}
    return (
        _ticker_chart_text(
            payload.get("insider_name"),
            insider.get("name"),
            raw.get("reportingName"),
            raw.get("reportingOwnerName"),
            raw.get("ownerName"),
            event.member_name,
        )
        or "Unknown insider"
    )


def _ticker_chart_marker_side(trade_type: str | None) -> str | None:
    normalized = normalize_trade_side(trade_type)
    if normalized == "purchase":
        return "buy"
    if normalized == "sale":
        return "sell"
    return normalized


def _ticker_chart_event_marker(event: Event, *, start_key: str, end_key: str) -> dict | None:
    if event.event_type not in {"congress_trade", "insider_trade", *GOVERNMENT_CONTRACT_EVENT_TYPES}:
        return None
    payload = _ticker_chart_payload(event)
    day = _ticker_chart_event_day(event, payload)
    if not day or day < start_key or day > end_key:
        return None

    if event.event_type in GOVERNMENT_CONTRACT_EVENT_TYPES:
        agency, amount, description = _ticker_chart_contract_details(payload)
        is_funding_action = (
            payload.get("event_subtype") == "funding_action"
            or payload.get("modification_number") is not None
            or payload.get("action_date") is not None
        )
        marker_amount = amount
        if marker_amount is None and event.amount_max is not None:
            marker_amount = float(event.amount_max)
        if marker_amount is None and event.amount_min is not None:
            marker_amount = float(event.amount_min)
        return {
            "id": f"government-contract-{event.id}",
            "event_id": event.id,
            "kind": "government_contract",
            "date": day,
            "actor": agency or "Government Contract",
            "action": "funding action" if is_funding_action else "contract award",
            "side": None,
            "amount_min": marker_amount,
            "amount_max": marker_amount,
            "detail": agency,
            "score": None,
            "band": None,
            "label": "Government Contract Funding" if is_funding_action else "Government Contract Award",
            "meta": {
                "agency": agency,
                "amount": marker_amount,
                "description": description,
                "event_subtype": "funding_action" if is_funding_action else "award",
                "report_date": payload.get("report_date") or payload.get("action_date"),
                "modification_number": payload.get("modification_number"),
                "action_type": payload.get("action_type"),
            },
        }

    side = _ticker_chart_marker_side(event.trade_type)
    action = (event.trade_type or "").strip() or "trade"
    kind = "congress" if event.event_type == "congress_trade" else "insider"
    actor = event.member_name or "Unknown member"
    if kind == "insider":
        actor = _ticker_chart_insider_actor(event, payload)

    return {
        "id": f"{kind}-{event.id}",
        "event_id": event.id,
        "kind": kind,
        "date": day,
        "actor": actor,
        "action": action,
        "side": side,
        "amount_min": event.amount_min,
        "amount_max": event.amount_max,
        "detail": event.source,
        "score": None,
        "band": None,
        "label": None,
        "meta": None,
    }


def _ticker_chart_signal_marker(signal, *, start_key: str, end_key: str) -> dict | None:
    day = _ticker_chart_date_key(getattr(signal, "ts", None))
    if not day or day < start_key or day > end_key:
        return None
    event_id = getattr(signal, "event_id", None)
    actor = (
        _ticker_chart_text(getattr(signal, "who", None), getattr(signal, "symbol", None))
        or "Signal"
    )
    band = getattr(signal, "smart_band", None)
    score = getattr(signal, "smart_score", None)
    action = f"{band} signal" if band else "signal"
    return {
        "id": f"signal-{event_id}-{day}-{score or ''}",
        "event_id": event_id,
        "kind": "signals",
        "date": day,
        "actor": actor,
        "action": action,
        "side": _ticker_chart_marker_side(getattr(signal, "trade_type", None)),
        "amount_min": getattr(signal, "amount_min", None),
        "amount_max": getattr(signal, "amount_max", None),
        "detail": getattr(signal, "source", None),
        "score": score,
        "band": band,
        "label": None,
        "meta": None,
    }


def _quote_snapshot_from_fmp(symbol: str) -> dict:
    normalized = symbol.strip().upper()
    cached = _TICKER_QUOTE_SNAPSHOT_CACHE.get(normalized)
    if cached and time.time() < cached[0]:
        return dict(cached[1])

    api_key = os.getenv("FMP_API_KEY", "").strip()
    if not api_key:
        return {}

    try:
        response = requests.get(
            f"{FMP_BASE_URL}/quote",
            params={"symbol": normalized, "apikey": api_key},
            timeout=float(os.getenv("FMP_SNAPSHOT_TIMEOUT_SECONDS", "3") or 3),
        )
        if response.status_code != 200:
            return {}
        payload = response.json()
    except Exception:
        logger.info("ticker_chart quote snapshot failed symbol=%s", normalized, exc_info=True)
        return {}

    row: dict = {}
    if isinstance(payload, list) and payload and isinstance(payload[0], dict):
        row = payload[0]
    elif isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, list) and data and isinstance(data[0], dict):
            row = data[0]
        else:
            row = payload

    if row:
        _TICKER_QUOTE_SNAPSHOT_CACHE[normalized] = (time.time() + 15 * 60, dict(row))
    return row


def _first_payload_row(payload) -> dict:
    if isinstance(payload, list) and payload and isinstance(payload[0], dict):
        return payload[0]
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, list) and data and isinstance(data[0], dict):
            return data[0]
        return payload
    return {}


def _cached_fmp_symbol_row(
    *,
    symbol: str,
    endpoint: str,
    cache: dict[str, tuple[float, dict]],
    log_name: str,
    ttl_seconds: int = 6 * 60 * 60,
) -> dict:
    normalized = symbol.strip().upper()
    cached = cache.get(normalized)
    if cached and time.time() < cached[0]:
        return dict(cached[1])

    api_key = os.getenv("FMP_API_KEY", "").strip()
    if not api_key:
        return {}

    try:
        response = requests.get(
            f"{FMP_BASE_URL}/{endpoint}",
            params={"symbol": normalized, "apikey": api_key},
            timeout=float(os.getenv("FMP_SNAPSHOT_TIMEOUT_SECONDS", "3") or 3),
        )
        if response.status_code != 200:
            return {}
        row = _first_payload_row(response.json())
    except Exception:
        logger.info("ticker_chart %s snapshot failed symbol=%s", log_name, normalized, exc_info=True)
        return {}

    if row:
        cache[normalized] = (time.time() + ttl_seconds, dict(row))
    return row


def _ratios_ttm_from_fmp(symbol: str) -> dict:
    return _cached_fmp_symbol_row(
        symbol=symbol,
        endpoint="ratios-ttm",
        cache=_TICKER_RATIOS_TTM_CACHE,
        log_name="ratios_ttm",
    )


def _company_profile_snapshot_from_fmp(symbol: str) -> dict:
    return _cached_fmp_symbol_row(
        symbol=symbol,
        endpoint="profile",
        cache=_TICKER_PROFILE_SNAPSHOT_CACHE,
        log_name="profile",
    )


def _quote_float(row: dict, *keys: str) -> float | None:
    for key in keys:
        value = row.get(key)
        parsed = _parse_numeric(value)
        if parsed is not None:
            return parsed
    return None


def _average_last_volumes(volume_by_day: dict[str, float], limit: int = 30) -> float | None:
    values = [
        float(value)
        for _, value in sorted(volume_by_day.items(), reverse=True)[:limit]
        if isinstance(value, (int, float)) and value > 0
    ]
    if not values:
        return None
    return sum(values) / len(values)


def _allow_chart_volume_provider_fallback() -> bool:
    return os.getenv("TICKER_CHART_VOLUME_PROVIDER_FALLBACK", "0").strip().lower() in {"1", "true", "yes"}


def _explicit_average_volume_30d(quote_row: dict, profile_row: dict) -> float | None:
    thirty_day_keys = (
        "avgVolume30D",
        "avgVolume30d",
        "averageVolume30D",
        "averageVolume30d",
        "volumeAvg30D",
        "volumeAvg30d",
        "thirtyDayAverageVolume",
        "averageDailyVolume30Day",
        "averageDailyVolume30d",
        "avgDailyVolume30Day",
    )
    return _quote_float(quote_row, *thirty_day_keys) or _quote_float(profile_row, *thirty_day_keys)


def _build_ticker_chart_quote(
    db: Session,
    symbol: str,
    price_points: list[dict],
) -> dict:
    row = _quote_snapshot_from_fmp(symbol)
    ratios_row = _ratios_ttm_from_fmp(symbol)
    profile_row = _company_profile_snapshot_from_fmp(symbol)
    row_price = _quote_float(row, "price", "close")
    quote_map = {} if row_price is not None else get_current_prices_db(db, [symbol])
    cached_price = quote_map.get(symbol)
    latest_close = price_points[-1]["close"] if price_points else None
    prior_close = price_points[-2]["close"] if len(price_points) >= 2 else None

    current_price = row_price
    if current_price is None and cached_price is not None:
        current_price = float(cached_price)
    if current_price is None:
        current_price = latest_close
    previous_close = _quote_float(row, "previousClose", "previous_close", "prevClose")
    if previous_close is None:
        previous_close = prior_close
    day_change = _quote_float(row, "change", "dayChange", "changes")
    if day_change is None and current_price is not None and previous_close not in (None, 0):
        day_change = current_price - previous_close
    day_change_pct = _quote_float(row, "changesPercentage", "changePercentage", "changePercent")
    if day_change_pct is None and day_change is not None and previous_close not in (None, 0):
        day_change_pct = (day_change / previous_close) * 100

    return {
        "current_price": current_price,
        "day_change": day_change,
        "day_change_pct": day_change_pct,
        "market_cap": _quote_float(row, "marketCap", "market_cap", "mktCap"),
        "day_volume": _quote_float(row, "volume"),
        "average_volume": _explicit_average_volume_30d(row, profile_row),
        "trailing_pe": _quote_float(
            ratios_row,
            "priceToEarningsRatioTTM",
            "priceEarningsRatioTTM",
            "priceEarningsRatio",
            "peRatioTTM",
            "peRatio",
            "trailingPE",
            "trailing_pe",
        ),
        "beta": _quote_float(profile_row, "beta"),
        "asof": _ticker_chart_date_key(row.get("timestamp") or row.get("date") or row.get("earningsAnnouncement")),
    }


def _build_ticker_chart_bundle(symbol: str, days: int, db: Session) -> dict:
    sym = symbol.upper().strip()
    if not sym:
        raise HTTPException(status_code=422, detail="Ticker symbol is required")

    end_date = datetime.now(timezone.utc).date()
    start_date = end_date - timedelta(days=max(days - 1, 0))
    start_key = start_date.isoformat()
    end_key = end_date.isoformat()

    ticker_map = get_eod_close_series(db, sym, start_key, end_key)
    benchmark_map = get_eod_close_series(db, _TICKER_BENCHMARK_SYMBOL, start_key, end_key)
    price_points = [{"date": day, "close": close} for day, close in sorted(ticker_map.items())]
    benchmark_points = [{"date": day, "close": close} for day, close in sorted(benchmark_map.items())]

    start_dt = datetime.combine(start_date, datetime.min.time(), tzinfo=timezone.utc)
    events = select_visible_ticker_events(db, symbol=sym, since=start_dt, limit=500)
    markers = [
        marker
        for marker in (
            _ticker_chart_event_marker(event, start_key=start_key, end_key=end_key)
            for event in events
        )
        if marker is not None
    ]
    has_contract_action_markers = any(
        marker.get("kind") == "government_contract"
        and (marker.get("meta") or {}).get("event_subtype") == "funding_action"
        for marker in markers
    )
    if has_contract_action_markers:
        markers = [
            marker
            for marker in markers
            if marker.get("kind") != "government_contract"
            or (marker.get("meta") or {}).get("event_subtype") == "funding_action"
        ]

    try:
        signals = _query_unified_signals(
            db=db,
            mode="all",
            sort="smart",
            limit=150,
            offset=0,
            baseline_days=365,
            congress_recent_days=CONGRESS_SIGNAL_DEFAULTS["recent_days"],
            insider_recent_days=INSIDER_DEFAULTS["recent_days"],
            congress_min_baseline_count=CONGRESS_SIGNAL_DEFAULTS["min_baseline_count"],
            insider_min_baseline_count=INSIDER_DEFAULTS["min_baseline_count"],
            congress_multiple=CONGRESS_SIGNAL_DEFAULTS["multiple"],
            insider_multiple=INSIDER_DEFAULTS["multiple"],
            congress_min_amount=CONGRESS_SIGNAL_DEFAULTS["min_amount"],
            insider_min_amount=INSIDER_DEFAULTS["min_amount"],
            min_smart_score=None,
            side="all",
            symbol=sym,
        )
    except Exception:
        logger.info("ticker_chart signal markers unavailable symbol=%s", sym, exc_info=True)
        signals = []

    seen_marker_ids = {marker["id"] for marker in markers}
    for signal in signals:
        marker = _ticker_chart_signal_marker(signal, start_key=start_key, end_key=end_key)
        if marker is None or marker["id"] in seen_marker_ids:
            continue
        markers.append(marker)
        seen_marker_ids.add(marker["id"])

    markers.sort(key=lambda marker: (marker["date"], marker["kind"], str(marker["id"])))

    db.close()
    quote = _build_ticker_chart_quote(db, sym, price_points)
    if quote.get("average_volume") is None and _allow_chart_volume_provider_fallback():
        db.close()
        volume_by_day = get_daily_volume_series_from_provider(sym, start_key, end_key)
        quote["average_volume"] = _average_last_volumes(volume_by_day, 30)

    return {
        "symbol": sym,
        "resolution": "daily",
        "days": days,
        "start_date": start_key,
        "end_date": end_key,
        "benchmark": {
            "symbol": _TICKER_BENCHMARK_SYMBOL,
            "label": _TICKER_BENCHMARK_LABEL,
            "points": benchmark_points,
        },
        "prices": price_points,
        "markers": markers,
        "quote": quote,
    }


def _insider_stock_chart_marker(event: Event, payload: dict, *, start_key: str, end_key: str) -> dict | None:
    day = _ticker_chart_date_key(
        payload.get("transaction_date")
        or payload.get("trade_date")
        or ((payload.get("raw") or {}).get("transactionDate") if isinstance(payload.get("raw"), dict) else None)
        or event.event_date
        or event.ts
    )
    if not day or day < start_key or day > end_key:
        return None

    row = _insider_trade_row(event, payload)
    side = _ticker_chart_marker_side(row.get("trade_type"))
    filing_date = _ticker_chart_date_key(row.get("filing_date") or _insider_filing_date(event, payload))
    trade_value = row.get("trade_value")
    amount_min = row.get("amount_min")
    amount_max = row.get("amount_max")
    if trade_value is not None:
        amount_min = trade_value
        amount_max = trade_value

    signal_score = row.get("smart_score")
    signal_label = row.get("smart_band")
    return {
        "id": f"insider-{event.id}",
        "event_id": event.id,
        "kind": "insider",
        "date": day,
        "actor": row.get("insider_name") or _ticker_chart_insider_actor(event, payload),
        "action": row.get("trade_type") or event.trade_type or "trade",
        "side": side,
        "amount_min": amount_min,
        "amount_max": amount_max,
        "detail": row.get("company_name") or event.source,
        "score": signal_score,
        "band": signal_label,
        "label": "Insider Buy" if side == "buy" else "Insider Sell" if side == "sell" else "Insider Trade",
        "meta": {
            "transaction_date": day,
            "filing_date": filing_date,
            "shares": row.get("shares"),
            "value": trade_value,
            "price": row.get("price"),
            "signal_score": signal_score,
            "signal_label": signal_label,
            "source_event_id": event.id,
        },
    }


def _build_insider_stock_chart_bundle(
    reporting_cik: str,
    *,
    days: int,
    symbol: str | None,
    db: Session,
) -> dict:
    matched = _load_insider_events_for_cik(
        db,
        reporting_cik,
        days,
        include_non_market_activity=True,
        issuer=symbol,
    )
    symbols: dict[str, int] = {}
    for event, payload in matched:
        event_symbol = _event_payload_symbol(event, payload)
        if event_symbol:
            symbols[event_symbol] = symbols.get(event_symbol, 0) + 1

    requested_symbol = (symbol or "").strip().upper()
    resolved_symbol = requested_symbol or (max(symbols.items(), key=lambda item: item[1])[0] if symbols else None)
    if not resolved_symbol:
        return {
            "symbol": None,
            "company_name": None,
            "resolution": "daily",
            "days": days,
            "start_date": None,
            "end_date": None,
            "benchmark": {"symbol": _TICKER_BENCHMARK_SYMBOL, "label": _TICKER_BENCHMARK_LABEL, "points": []},
            "prices": [],
            "markers": [],
            "quote": {
                "current_price": None,
                "day_change": None,
                "day_change_pct": None,
                "market_cap": None,
                "day_volume": None,
                "average_volume": None,
                "trailing_pe": None,
                "beta": None,
                "asof": None,
            },
            "available_symbols": [],
        }

    scoped = [(event, payload) for event, payload in matched if _event_payload_symbol(event, payload) == resolved_symbol]
    end_date = datetime.now(timezone.utc).date()
    start_date = end_date - timedelta(days=max(days - 1, 0))
    start_key = start_date.isoformat()
    end_key = end_date.isoformat()
    ticker_map = get_eod_close_series(db, resolved_symbol, start_key, end_key)
    benchmark_map = get_eod_close_series(db, _TICKER_BENCHMARK_SYMBOL, start_key, end_key)
    price_points = [{"date": day, "close": close} for day, close in sorted(ticker_map.items())]
    benchmark_points = [{"date": day, "close": close} for day, close in sorted(benchmark_map.items())]

    markers = [
        marker
        for marker in (
            _insider_stock_chart_marker(event, payload, start_key=start_key, end_key=end_key)
            for event, payload in scoped
        )
        if marker is not None
    ]
    markers.sort(key=lambda marker: (marker["date"], str(marker["id"])))

    company_name = None
    if scoped:
        symbol_meta = _ticker_meta_with_security_names(db, [resolved_symbol])
        cik_values = sorted({cik for _, payload in scoped for cik in [_event_payload_cik(payload)] if cik})
        cik_names = get_cik_meta(db, cik_values, allow_refresh=False) if cik_values else {}
        enriched_payload = _enrich_event_payload_company_name(scoped[0][0], dict(scoped[0][1]), symbol_meta, cik_names)
        company_name = _insider_trade_row(scoped[0][0], enriched_payload).get("company_name")

    db.close()
    quote = _build_ticker_chart_quote(db, resolved_symbol, price_points)
    if quote.get("average_volume") is None and _allow_chart_volume_provider_fallback():
        db.close()
        volume_by_day = get_daily_volume_series_from_provider(resolved_symbol, start_key, end_key)
        quote["average_volume"] = _average_last_volumes(volume_by_day, 30)

    return {
        "symbol": resolved_symbol,
        "company_name": company_name,
        "resolution": "daily",
        "days": days,
        "start_date": start_key,
        "end_date": end_key,
        "benchmark": {
            "symbol": _TICKER_BENCHMARK_SYMBOL,
            "label": _TICKER_BENCHMARK_LABEL,
            "points": benchmark_points,
        },
        "prices": price_points,
        "markers": markers,
        "quote": quote,
        "available_symbols": sorted(symbols),
    }


@app.get("/api/tickers/{symbol}/chart-bundle", dependencies=[Depends(rate_limit_provider_backed)])
def ticker_chart_bundle(
    symbol: str,
    days: int = Query(365, ge=30, le=365),
    db: Session = Depends(get_db),
):
    with _heavy_route_slot("ticker_chart_bundle", _TICKER_CHART_SEMAPHORE):
        return _build_ticker_chart_bundle(symbol, days, db)


@app.get("/api/insiders/{reporting_cik}/stock-chart", dependencies=[Depends(rate_limit_provider_backed)])
def insider_stock_chart_bundle(
    reporting_cik: str,
    lookback_days: int = Query(365, ge=30, le=1095),
    symbol: str | None = None,
    db: Session = Depends(get_db),
):
    with _heavy_route_slot("insider_stock_chart_bundle", _TICKER_CHART_SEMAPHORE):
        return _build_insider_stock_chart_bundle(reporting_cik, days=lookback_days, symbol=symbol, db=db)


@app.get("/api/tickers/{symbol}/price-history", dependencies=[Depends(rate_limit_provider_backed)])
def ticker_price_history(
    symbol: str,
    days: int = Query(365, ge=30, le=365),
    db: Session = Depends(get_db),
):
    with _heavy_route_slot("ticker_price_history", _TICKER_CHART_SEMAPHORE):
        return _ticker_price_history_response(symbol, days, db)


def _ticker_price_history_response(symbol: str, days: int, db: Session) -> dict:
    sym = symbol.upper().strip()
    if not sym:
        raise HTTPException(status_code=422, detail="Ticker symbol is required")

    end_date = datetime.now(timezone.utc).date()
    start_date = end_date - timedelta(days=max(days - 1, 0))
    points = get_eod_close_series(db, sym, start_date.isoformat(), end_date.isoformat())

    return {
        "symbol": sym,
        "days": days,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "points": [{"date": day, "close": close} for day, close in sorted(points.items())],
    }


def _build_ticker_profile(symbol: str, db: Session) -> dict:
    sym = symbol.upper().strip()
    if not sym:
        raise LookupError("Ticker not found")

    security = db.execute(
        select(Security).where(func.upper(Security.symbol) == sym)
    ).scalar_one_or_none()

    if not security:
        fallback_profile = _build_ticker_fallback_profile(sym, db)
        if fallback_profile is not None:
            return fallback_profile

        metadata_profile = _build_ticker_metadata_only_profile(sym, db)
        if metadata_profile is not None:
            return metadata_profile

        raise LookupError("Ticker not found")

    q = (
        select(Transaction, Member)
        .join(Member, Transaction.member_id == Member.id)
        .where(Transaction.security_id == security.id)
        .order_by(Transaction.report_date.desc(), Transaction.id.desc())
        .limit(200)
    )

    rows = db.execute(q).all()

    trades = []
    member_counts: dict[int, int] = {}
    members_by_id: dict[int, Member] = {}

    for tx, m in rows:
        member_counts[m.id] = member_counts.get(m.id, 0) + 1
        members_by_id[m.id] = m

        trades.append({
            "id": tx.id,
            "member": {
                "bioguide_id": m.bioguide_id,
                "name": f"{m.first_name or ''} {m.last_name or ''}".strip(),
                "chamber": m.chamber,
                "party": m.party,
                "state": m.state,
            },
            "transaction_type": tx.transaction_type,
            "trade_date": tx.trade_date.isoformat() if tx.trade_date else None,
            "report_date": tx.report_date.isoformat() if tx.report_date else None,
            "amount_range_min": tx.amount_range_min,
            "amount_range_max": tx.amount_range_max,
        })

    top_members = sorted(
        member_counts.items(),
        key=lambda x: x[1],
        reverse=True,
    )[:10]

    options_flow_summary = _ticker_options_flow_summary(sym)
    confirmation_score_bundle = _ticker_confirmation_score_bundle(db, sym, options_flow_summary=options_flow_summary)
    why_now = build_why_now_bundle(sym, confirmation_score_bundle, lookback_days=30)
    signal_freshness = build_signal_freshness_bundle(sym, confirmation_score_bundle, lookback_days=30)
    technical_indicators = _ticker_technical_indicators(db, sym)
    ticker_name = _resolve_ticker_page_name(db, sym, canonical_profile_name=security.name)
    ticker_metadata = _resolve_ticker_company_metadata(db, sym, security=security)

    return {
        "ticker": {
            "symbol": security.symbol,
            "name": ticker_name,
            "asset_class": security.asset_class,
            **ticker_metadata,
        },
        "top_members": [
            {
                **_top_member_payload(members_by_id[member_id]),
                "trade_count": trade_count,
            }
            for member_id, trade_count in top_members
        ],
        "trades": trades,
        "confirmation_score_bundle": confirmation_score_bundle,
        "options_flow_summary": options_flow_summary,
        "why_now": why_now,
        "signal_freshness": signal_freshness,
        "technical_indicators": technical_indicators,
    }


def _ticker_identity_payload_candidates(payload: dict) -> list[str]:
    raw = payload.get("raw") if isinstance(payload.get("raw"), dict) else {}
    nested_payload = payload.get("payload") if isinstance(payload.get("payload"), dict) else {}
    candidates = [
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
    ]
    return [value.strip() for value in candidates if isinstance(value, str) and value.strip()]


def _ticker_identity_event_candidates(events: list[Event]) -> list[str]:
    candidates: list[str] = []
    for event in events:
        candidates.extend(_ticker_identity_payload_candidates(_ticker_chart_payload(event)))
    return candidates


def _resolve_ticker_page_name(
    db: Session,
    sym: str,
    *,
    canonical_profile_name: str | None = None,
    events: list[Event] | None = None,
) -> str:
    candidate_events = events
    if candidate_events is None:
        candidate_events = db.execute(
            select(Event)
            .where(Event.symbol.is_not(None))
            .where(func.upper(Event.symbol) == sym)
            .order_by(func.coalesce(Event.event_date, Event.ts).desc(), Event.id.desc())
            .limit(100)
        ).scalars().all()

    metadata_name = None
    profile_name = None
    try:
        metadata_name = (get_ticker_meta(db, [sym], allow_refresh=True).get(sym) or {}).get("company_name")
    except Exception:
        logger.exception("ticker identity metadata lookup failed symbol=%s", sym)
    try:
        profile_row = _company_profile_snapshot_from_fmp(sym)
        profile_name = _clean_ticker_metadata_text(profile_row.get("companyName")) or _clean_ticker_metadata_text(profile_row.get("name"))
    except Exception:
        logger.exception("ticker identity profile lookup failed symbol=%s", sym)

    return resolve_ticker_identity(
        sym,
        canonical_profile_name=canonical_profile_name,
        issuer_company_names=_ticker_identity_event_candidates(candidate_events),
        metadata_name=metadata_name,
        profile_name=profile_name,
        manual_aliases=_TICKER_IDENTITY_MANUAL_ALIASES,
    )


def _clean_ticker_metadata_text(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned or None


def _resolve_ticker_company_metadata(
    db: Session,
    sym: str,
    *,
    security: Security | None = None,
) -> dict[str, str | None]:
    metadata = get_ticker_meta(db, [sym], allow_refresh=False).get(sym) or {}
    profile_row = _company_profile_snapshot_from_fmp(sym)

    return {
        "sector": _clean_ticker_metadata_text(security.sector if security is not None else None)
        or _clean_ticker_metadata_text(profile_row.get("sector"))
        or _clean_ticker_metadata_text(metadata.get("sector")),
        "industry": _clean_ticker_metadata_text(profile_row.get("industry"))
        or _clean_ticker_metadata_text(metadata.get("industry")),
        "country": _clean_ticker_metadata_text(profile_row.get("country"))
        or _clean_ticker_metadata_text(metadata.get("country")),
        "exchange": _clean_ticker_metadata_text(metadata.get("exchange"))
        or _clean_ticker_metadata_text(profile_row.get("exchangeShortName"))
        or _clean_ticker_metadata_text(profile_row.get("exchange"))
        or _clean_ticker_metadata_text(profile_row.get("stockExchange")),
    }


def _build_ticker_fallback_profile(sym: str, db: Session) -> dict | None:
    events = db.execute(
        select(Event)
        .where(func.upper(Event.symbol) == sym)
        .order_by(Event.event_date.desc(), Event.id.desc())
        .limit(200)
    ).scalars().all()

    if not events:
        return None

    name = _resolve_ticker_page_name(db, sym, events=events)
    options_flow_summary = _ticker_options_flow_summary(sym)
    confirmation_score_bundle = _ticker_confirmation_score_bundle(db, sym, options_flow_summary=options_flow_summary)
    signal_freshness = build_signal_freshness_bundle(sym, confirmation_score_bundle, lookback_days=30)
    technical_indicators = _ticker_technical_indicators(db, sym)
    ticker_metadata = _resolve_ticker_company_metadata(db, sym)

    return {
        "ticker": {
            "symbol": sym,
            "name": name,
            "asset_class": "Equity",
            **ticker_metadata,
        },
        "top_members": [],
        "trades": [],
        "confirmation_score_bundle": confirmation_score_bundle,
        "options_flow_summary": options_flow_summary,
        "why_now": build_why_now_bundle(sym, confirmation_score_bundle, lookback_days=30),
        "signal_freshness": signal_freshness,
        "technical_indicators": technical_indicators,
    }


def _build_ticker_metadata_only_profile(sym: str, db: Session) -> dict | None:
    company_name = _resolve_ticker_page_name(db, sym)
    if not safe_company_identity_candidate(company_name, sym):
        return None

    options_flow_summary = _ticker_options_flow_summary(sym)
    confirmation_score_bundle = _ticker_confirmation_score_bundle(db, sym, options_flow_summary=options_flow_summary)
    signal_freshness = build_signal_freshness_bundle(sym, confirmation_score_bundle, lookback_days=30)
    technical_indicators = _ticker_technical_indicators(db, sym)
    ticker_metadata = _resolve_ticker_company_metadata(db, sym)

    return {
        "ticker": {
            "symbol": sym,
            "name": company_name,
            "asset_class": "Equity",
            **ticker_metadata,
        },
        "top_members": [],
        "trades": [],
        "confirmation_score_bundle": confirmation_score_bundle,
        "options_flow_summary": options_flow_summary,
        "why_now": build_why_now_bundle(sym, confirmation_score_bundle, lookback_days=30),
        "signal_freshness": signal_freshness,
        "technical_indicators": technical_indicators,
    }


def _ticker_confirmation_score_bundle(db: Session, sym: str, *, options_flow_summary: dict | None = None) -> dict:
    try:
        return get_confirmation_score_bundle_for_ticker(db, sym, lookback_days=30, options_flow_summary=options_flow_summary)
    except Exception:
        logger.exception("confirmation_score_bundle failed symbol=%s", sym)
        return inactive_confirmation_score_bundle(sym, lookback_days=30)


def _ticker_options_flow_summary(sym: str) -> dict:
    try:
        return get_options_flow_summary(sym, lookback_days=30)
    except Exception:
        logger.exception("options_flow_summary failed symbol=%s", sym)
        return unavailable_options_flow_summary(sym, 30, provider="massive", reason="provider_error")


def _ticker_technical_indicators(db: Session, sym: str) -> dict:
    try:
        return build_ticker_technical_indicators(
            db,
            sym,
            lookback_days=90,
            release_connection_before_provider=True,
            hydrate_provider=False,
        )
    except Exception:
        logger.exception("technical_indicators failed symbol=%s", sym)
        return {
            "source": "daily_close_history",
            "asof": None,
            "price_points": 0,
            "rsi": {
                "status": "unavailable",
                "signal": "unavailable",
                "message": "RSI temporarily unavailable",
                "reason": "provider_error",
                "value": None,
                "period": 14,
            },
            "macd": {
                "status": "unavailable",
                "signal": "unavailable",
                "message": "MACD temporarily unavailable",
                "reason": "provider_error",
                "value": None,
            },
            "ema_trend": {
                "status": "unavailable",
                "signal": "unavailable",
                "message": "EMA trend temporarily unavailable",
                "reason": "provider_error",
                "value": None,
            },
        }


@app.post("/api/watchlists")
def create_watchlist(
    payload: WatchlistPayload,
    request: Request,
    db: Session = Depends(get_db),
):
    user = _require_account(request, db)
    name = payload.name.strip()
    if not name:
        raise HTTPException(status_code=422, detail="Watchlist name is required")

    existing = db.execute(
        _owned_watchlist_query(user).where(func.lower(Watchlist.name) == name.lower())
    ).scalar_one_or_none()
    if existing:
        raise HTTPException(status_code=409, detail="Watchlist name already exists")

    entitlements = current_entitlements(request, db)
    require_feature(
        entitlements,
        "watchlists",
        message="Watchlist creation is included with Premium.",
    )
    current_count = int(
        db.execute(
            select(func.count()).select_from(Watchlist).where(Watchlist.owner_user_id == user.id)
        ).scalar_one()
        or 0
    )
    enforce_limit(
        entitlements,
        "watchlists",
        current_count=current_count,
        message="Your current plan has reached its watchlist limit. Upgrade to create more.",
    )

    w = Watchlist(name=name, owner_user_id=user.id)
    db.add(w)
    try:
        db.flush()
        db.add(WatchlistViewState(watchlist_id=w.id, last_seen_at=datetime.now(timezone.utc)))
        db.commit()
    except IntegrityError:
        db.rollback()
        raise HTTPException(status_code=409, detail="Watchlist name already exists")
    return {"id": w.id, "name": w.name, "symbols": []}


def _watchlist_symbols(db: Session, watchlist_id: int) -> list[str]:
    symbols = (
        db.execute(
            select(Security.symbol)
            .join(WatchlistItem, WatchlistItem.security_id == Security.id)
            .where(WatchlistItem.watchlist_id == watchlist_id)
        )
        .scalars()
        .all()
    )
    return [symbol.strip().upper() for symbol in symbols if symbol and symbol.strip()]


@app.get("/api/insights/news", dependencies=[Depends(rate_limit_provider_backed)])
def list_insights_news(
    page: int = Query(0, ge=0),
    limit: int = Query(20, ge=1, le=50),
):
    return get_general_news(page=page, limit=limit)


@app.get("/api/insights/macro-snapshot", dependencies=[Depends(rate_limit_provider_backed)])
def insights_macro_snapshot():
    return get_macro_snapshot()


@app.get("/api/tickers/{symbol}/news", dependencies=[Depends(rate_limit_provider_backed)])
def ticker_news(
    symbol: str,
    page: int = Query(0, ge=0),
    limit: int = Query(20, ge=1, le=50),
):
    normalized_symbol = normalize_symbol(symbol)
    if not normalized_symbol:
        raise HTTPException(status_code=422, detail="Ticker symbol is required.")

    payload = get_stock_news(symbol=normalized_symbol, page=page, limit=limit)
    if not payload["items"] and payload.get("status") != "unavailable":
        payload = {**payload, "message": "No recent news found for this ticker."}
    return payload


@app.get("/api/tickers/{symbol}/press-releases", dependencies=[Depends(rate_limit_provider_backed)])
def ticker_press_releases(
    symbol: str,
    page: int = Query(0, ge=0),
    limit: int = Query(20, ge=1, le=50),
):
    normalized_symbol = normalize_symbol(symbol)
    if not normalized_symbol:
        raise HTTPException(status_code=422, detail="Ticker symbol is required.")

    payload = get_press_releases(symbol=normalized_symbol, page=page, limit=limit)
    if not payload["items"] and payload.get("status") != "unavailable":
        payload = {**payload, "message": "No press releases are available for this ticker right now."}
    return payload


@app.get("/api/tickers/{symbol}/financials", dependencies=[Depends(rate_limit_provider_backed)])
def ticker_financials(symbol: str):
    normalized_symbol = normalize_symbol(symbol)
    if not normalized_symbol:
        raise HTTPException(status_code=422, detail="Ticker symbol is required.")
    return get_ticker_financials(normalized_symbol)


@app.get("/api/tickers/{symbol}/sec-filings", dependencies=[Depends(rate_limit_provider_backed)])
def ticker_sec_filings(
    symbol: str,
    from_date: str | None = Query(default=None, alias="from"),
    to_date: str | None = Query(default=None, alias="to"),
    page: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=100),
):
    normalized_symbol = normalize_symbol(symbol)
    if not normalized_symbol:
        raise HTTPException(status_code=422, detail="Ticker symbol is required.")

    payload = get_sec_filings(
        symbol=normalized_symbol,
        from_date=from_date,
        to_date=to_date,
        page=page,
        limit=limit,
    )
    if not payload["items"] and payload.get("status") != "unavailable":
        payload = {**payload, "message": "No recent filings are available for this ticker right now."}
    return payload


def _watchlist_unseen_count(db: Session, watchlist_id: int, last_seen_at: datetime | None) -> int:
    return watchlist_unread_count(db, watchlist_id, last_seen_at)


def _watchlist_view_summary(db: Session, watchlist_id: int) -> dict:
    return watchlist_unread_summary(db, watchlist_id)


@app.get("/api/watchlists")
def list_watchlists(request: Request, db: Session = Depends(get_db)):
    user = _require_account(request, db)
    rows = db.execute(_owned_watchlist_query(user).order_by(Watchlist.name.asc())).scalars().all()
    for watchlist in rows:
        refresh_watchlist_alerts(db, user_id=user.id, watchlist=watchlist, lookback_days=7)
    db.commit()
    watchlist_ids = [w.id for w in rows]
    symbols_by_watchlist: dict[int, list[str]] = {watchlist_id: [] for watchlist_id in watchlist_ids}
    if watchlist_ids:
        symbol_rows = db.execute(
            select(WatchlistItem.watchlist_id, Security.symbol)
            .join(Security, WatchlistItem.security_id == Security.id)
            .where(WatchlistItem.watchlist_id.in_(watchlist_ids))
            .order_by(WatchlistItem.watchlist_id.asc(), Security.symbol.asc())
        ).all()
        for watchlist_id, symbol in symbol_rows:
            normalized_symbol = (symbol or "").strip().upper()
            if normalized_symbol:
                symbols_by_watchlist.setdefault(watchlist_id, []).append(normalized_symbol)
    return [
        {"id": w.id, "name": w.name, "symbols": symbols_by_watchlist.get(w.id, []), **_watchlist_view_summary(db, w.id)}
        for w in rows
    ]


def _monitored_watchlists_for_user(request: Request, db: Session, user: UserAccount) -> list[Watchlist]:
    entitlements = current_entitlements(request, db)
    source_limit = max(int(entitlements.limit("monitoring_sources") or 0), 0)
    if source_limit <= 0:
        return []
    return (
        db.execute(_owned_watchlist_query(user).order_by(Watchlist.name.asc(), Watchlist.id.asc()).limit(source_limit))
        .scalars()
        .all()
    )


def _refresh_monitored_watchlist_alerts(request: Request, db: Session, user: UserAccount) -> list[Watchlist]:
    watchlists = _monitored_watchlists_for_user(request, db, user)
    for watchlist in watchlists:
        refresh_watchlist_alerts(db, user_id=user.id, watchlist=watchlist, lookback_days=7)
    return watchlists


def _refresh_monitored_saved_screen_alerts(request: Request, db: Session, user: UserAccount) -> list[SavedScreen]:
    screens = _monitored_saved_screens_for_user(request, db, user)
    ensure_alerts_for_saved_screen_events(db, user_id=user.id, screens=screens)
    return screens


def _monitoring_watchlist_counts(db: Session, watchlists: list[Watchlist]) -> dict[int, int]:
    return watchlist_unread_counts(db, [watchlist.id for watchlist in watchlists])


def _monitored_saved_screens_for_user(request: Request, db: Session, user: UserAccount) -> list[SavedScreen]:
    entitlements = current_entitlements(request, db)
    if not entitlements.has_feature("screener_monitoring"):
        return []
    allowed_screen_ids = monitored_source_ids(db, user_id=user.id, entitlements=entitlements)["saved_screen_ids"]
    if not allowed_screen_ids:
        return []
    return (
        db.execute(
            select(SavedScreen)
            .where(SavedScreen.user_id == user.id)
            .where(SavedScreen.id.in_(allowed_screen_ids))
        )
        .scalars()
        .all()
    )


def _saved_screen_alert_unread_counts(db: Session, user_id: int) -> dict[tuple[str, str], int]:
    return {
        key: count
        for key, count in unread_count_by_source(db, user_id=user_id).items()
        if key[0] != "watchlist"
    }


def _monitoring_unread_total(request: Request, db: Session, user: UserAccount) -> int:
    watchlists = _monitored_watchlists_for_user(request, db, user)
    watchlist_counts = _monitoring_watchlist_counts(db, watchlists)
    saved_screen_counts = _saved_screen_alert_unread_counts(db, user.id)
    return sum(watchlist_counts.values()) + sum(saved_screen_counts.values())


def _monitoring_counts_payload(
    request: Request,
    db: Session,
    user: UserAccount,
    *,
    watchlists: list[Watchlist] | None = None,
    saved_screens: list[SavedScreen] | None = None,
) -> dict[str, object]:
    resolved_watchlists = watchlists if watchlists is not None else _monitored_watchlists_for_user(request, db, user)
    resolved_saved_screens = saved_screens if saved_screens is not None else _monitored_saved_screens_for_user(request, db, user)
    watchlist_counts = _monitoring_watchlist_counts(db, resolved_watchlists)
    saved_screen_counts = _saved_screen_alert_unread_counts(db, user.id)
    sources = [
        {
            "id": str(watchlist.id),
            "type": "watchlist",
            "name": watchlist.name,
            "unread_count": watchlist_counts.get(watchlist.id, 0),
            "new_count": watchlist_counts.get(watchlist.id, 0),
        }
        for watchlist in resolved_watchlists
    ]
    sources.extend(
        {
            "id": str(screen.id),
            "type": "saved_screen",
            "name": screen.name,
            "unread_count": saved_screen_counts.get(("saved_screen", str(screen.id)), 0),
            "new_count": saved_screen_counts.get(("saved_screen", str(screen.id)), 0),
        }
        for screen in resolved_saved_screens
    )
    total_watchlist_unread = sum(watchlist_counts.values())
    total_saved_screen_unread = sum(saved_screen_counts.get(("saved_screen", str(screen.id)), 0) for screen in resolved_saved_screens)
    return {
        "total_unread": total_watchlist_unread + total_saved_screen_unread,
        "watchlist_unread": total_watchlist_unread,
        "saved_screen_unread": total_saved_screen_unread,
        "unread_sources_count": sum(1 for source in sources if int(source["unread_count"] or 0) > 0),
        "sources": sources,
    }


class MonitoringItemsMutation(BaseModel):
    item_ids: list[int]


@app.get("/api/monitoring/unread-count")
def get_monitoring_unread_count(request: Request, db: Session = Depends(get_db)):
    try:
        user = _require_account(request, db)
        counts = _monitoring_counts_payload(request, db, user)
        total = int(counts["total_unread"])
        return {
            "unread_count": total,
            "total_unread_count": total,
            "unread_watchlist_updates": counts["watchlist_unread"],
            "unread_saved_screen_updates": counts["saved_screen_unread"],
            "unread_sources_count": counts["unread_sources_count"],
            "counts": counts,
        }
    except OperationalError as exc:
        db.rollback()
        if not is_database_locked_error(exc):
            raise
        logger.warning("monitoring_unread_count temporarily_unavailable database_locked")
        return {"unread_count": 0, "status": "temporarily_unavailable"}


@app.get("/api/monitoring/inbox")
def get_monitoring_inbox(request: Request, db: Session = Depends(get_db)):
    user = _require_account(request, db)
    watchlists = _refresh_monitored_watchlist_alerts(request, db, user)
    saved_screens = _refresh_monitored_saved_screen_alerts(request, db, user)
    db.commit()

    counts = _monitoring_counts_payload(request, db, user, watchlists=watchlists, saved_screens=saved_screens)
    alerts = [monitoring_alert_to_dict(alert) for alert in recent_alerts(db, user_id=user.id, unread_only=False, limit=100)]
    unread_alerts = [item for item in alerts if item.get("is_unread")]
    return {
        "unread_total": counts["total_unread"],
        "sources": counts["sources"],
        "counts": counts,
        "screen_changes": [],
        "latest_important": unread_alerts[:8],
        "alerts": alerts,
        "items": alerts,
    }


@app.post("/api/monitoring/items/mark-read", dependencies=[Depends(rate_limit_notification_mutation)])
def mark_monitoring_items_read(payload: MonitoringItemsMutation, request: Request, db: Session = Depends(get_db)):
    user = _require_account(request, db)
    marked = mark_alerts_read(db, user_id=user.id, alert_ids=payload.item_ids)
    db.commit()
    counts = _monitoring_counts_payload(request, db, user)
    return {"item_ids": payload.item_ids, "read": True, "marked_read": marked, "unread_count": counts["total_unread"], "counts": counts}


@app.post("/api/monitoring/items/mark-unread", dependencies=[Depends(rate_limit_notification_mutation)])
def mark_monitoring_items_unread(payload: MonitoringItemsMutation, request: Request, db: Session = Depends(get_db)):
    user = _require_account(request, db)
    marked = mark_alerts_unread(db, user_id=user.id, alert_ids=payload.item_ids)
    db.commit()
    counts = _monitoring_counts_payload(request, db, user)
    return {"item_ids": payload.item_ids, "read": False, "marked_unread": marked, "unread_count": counts["total_unread"], "counts": counts}


@app.post("/api/monitoring/items/dismiss", dependencies=[Depends(rate_limit_notification_mutation)])
def dismiss_monitoring_items(payload: MonitoringItemsMutation, request: Request, db: Session = Depends(get_db)):
    user = _require_account(request, db)
    dismissed = dismiss_alerts(db, user_id=user.id, alert_ids=payload.item_ids)
    db.commit()
    counts = _monitoring_counts_payload(request, db, user)
    return {
        "item_ids": payload.item_ids,
        "dismissed": dismissed,
        "unread_count": counts["total_unread"],
        "counts": counts,
    }


@app.post("/api/monitoring/alerts/{alert_id}/read", dependencies=[Depends(rate_limit_notification_mutation)])
def mark_monitoring_alert_read(alert_id: int, request: Request, db: Session = Depends(get_db)):
    user = _require_account(request, db)
    if not mark_alert_read(db, user_id=user.id, alert_id=alert_id):
        raise HTTPException(status_code=404, detail="Alert not found")
    db.commit()
    counts = _monitoring_counts_payload(request, db, user)
    return {"id": alert_id, "read": True, "unread_count": counts["total_unread"], "counts": counts}


@app.post("/api/monitoring/alerts/{alert_id}/unread", dependencies=[Depends(rate_limit_notification_mutation)])
def mark_monitoring_alert_unread(alert_id: int, request: Request, db: Session = Depends(get_db)):
    user = _require_account(request, db)
    if not mark_alert_unread(db, user_id=user.id, alert_id=alert_id):
        raise HTTPException(status_code=404, detail="Alert not found")
    db.commit()
    counts = _monitoring_counts_payload(request, db, user)
    return {"id": alert_id, "read": False, "unread_count": counts["total_unread"], "counts": counts}


@app.post("/api/monitoring/sources/{source_id}/mark-read", dependencies=[Depends(rate_limit_notification_mutation)])
def mark_monitoring_source_read(source_id: str, request: Request, db: Session = Depends(get_db), source_type: str = "watchlist"):
    user = _require_account(request, db)
    if source_type != "watchlist":
        raise HTTPException(status_code=422, detail="Unsupported source_type")
    watchlist_id = int(source_id) if source_id.isdigit() else -1
    watchlist = _get_owned_watchlist(db, user, watchlist_id)
    marked = mark_watchlist_source_read(db, user_id=user.id, watchlist=watchlist)
    db.commit()
    source_count = watchlist_unread_count(db, watchlist_id)
    counts = _monitoring_counts_payload(request, db, user)
    return {
        "source_id": source_id,
        "source_type": source_type,
        "marked_read": marked,
        "source_unread_count": source_count,
        "unread_count": counts["total_unread"],
        "counts": counts,
    }


@app.post("/api/monitoring/sources/{source_id}/mark-unread", dependencies=[Depends(rate_limit_notification_mutation)])
def mark_monitoring_source_unread(source_id: str, request: Request, db: Session = Depends(get_db), source_type: str = "watchlist"):
    user = _require_account(request, db)
    if source_type != "watchlist":
        raise HTTPException(status_code=422, detail="Unsupported source_type")
    watchlist_id = int(source_id) if source_id.isdigit() else -1
    watchlist = _get_owned_watchlist(db, user, watchlist_id)
    marked = mark_watchlist_source_unread(db, user_id=user.id, watchlist=watchlist)
    db.commit()
    source_count = watchlist_unread_count(db, watchlist_id)
    counts = _monitoring_counts_payload(request, db, user)
    return {
        "source_id": source_id,
        "source_type": source_type,
        "marked_unread": marked,
        "source_unread_count": source_count,
        "unread_count": counts["total_unread"],
        "counts": counts,
    }


@app.get("/api/entitlements")
def get_entitlements(request: Request, db: Session = Depends(get_db)):
    try:
        user = current_user(db, request, required=False)
        entitlements = entitlements_for_user(db, user) if user else current_entitlements(request, None)
        return entitlement_payload(entitlements, user=user)
    except OperationalError as exc:
        db.rollback()
        if not is_database_locked_error(exc):
            raise
        payload = entitlement_payload(current_entitlements(request, None), user=None)
        payload["status"] = "temporarily_unavailable"
        logger.warning("entitlements temporarily_unavailable database_locked")
        return payload


@app.delete("/api/watchlists/{watchlist_id}", status_code=204)
def delete_watchlist(watchlist_id: int, request: Request, db: Session = Depends(get_db)):
    user = _require_account(request, db)
    watchlist = _get_owned_watchlist(db, user, watchlist_id)

    db.execute(
        WatchlistItem.__table__.delete().where(
            WatchlistItem.watchlist_id == watchlist_id
        )
    )
    db.execute(
        WatchlistViewState.__table__.delete().where(
            WatchlistViewState.watchlist_id == watchlist_id
        )
    )
    db.execute(
        ConfirmationMonitoringSnapshot.__table__.delete().where(
            ConfirmationMonitoringSnapshot.watchlist_id == watchlist_id
        )
    )
    db.execute(
        ConfirmationMonitoringEvent.__table__.delete().where(
            ConfirmationMonitoringEvent.watchlist_id == watchlist_id
        )
    )
    db.execute(
        MonitoringAlert.__table__.delete().where(
            and_(
                MonitoringAlert.source_type == "watchlist",
                MonitoringAlert.source_id == str(watchlist_id),
            )
        )
    )
    db.delete(watchlist)
    db.commit()

    return None


@app.put("/api/watchlists/{watchlist_id}")
def rename_watchlist(watchlist_id: int, payload: WatchlistPayload, request: Request, db: Session = Depends(get_db)):
    user = _require_account(request, db)
    name = payload.name.strip()
    if not name:
        raise HTTPException(status_code=422, detail="Watchlist name is required")

    watchlist = _get_owned_watchlist(db, user, watchlist_id)

    existing = db.execute(
        _owned_watchlist_query(user).where(
            and_(func.lower(Watchlist.name) == name.lower(), Watchlist.id != watchlist_id)
        )
    ).scalar_one_or_none()
    if existing:
        raise HTTPException(status_code=409, detail="Watchlist name already exists")

    watchlist.name = name
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        raise HTTPException(status_code=409, detail="Watchlist name already exists")

    return {"id": watchlist.id, "name": watchlist.name, "symbols": _watchlist_symbols(db, watchlist.id)}


def _event_security_fields_for_symbol(db: Session, symbol: str) -> tuple[str | None, str | None]:
    sort_ts = func.coalesce(Event.event_date, Event.ts)
    event = db.execute(
        select(Event)
        .where(Event.symbol.is_not(None))
        .where(func.upper(Event.symbol) == symbol)
        .order_by(sort_ts.desc(), Event.id.desc())
        .limit(1)
    ).scalar_one_or_none()
    if not event:
        return None, None

    payload: dict = {}
    try:
        parsed = json.loads(event.payload_json or "{}")
        if isinstance(parsed, dict):
            payload = parsed
    except Exception:
        payload = {}

    raw_payload = payload.get("raw") if isinstance(payload.get("raw"), dict) else {}
    name = resolve_ticker_identity(
        symbol,
        issuer_company_names=[
            payload.get("company_name"),
            payload.get("companyName"),
            payload.get("issuer_name"),
            payload.get("issuerName"),
            raw_payload.get("companyName"),
            raw_payload.get("issuerName"),
            raw_payload.get("issuer"),
            payload.get("security_name"),
            payload.get("securityName"),
            raw_payload.get("securityName"),
        ],
        manual_aliases=_TICKER_IDENTITY_MANUAL_ALIASES,
    )
    sector = payload.get("sector") or raw_payload.get("sector")
    return (name, str(sector).strip() if sector else None)


def _resolve_watchlist_security(db: Session, raw_symbol: str) -> Security:
    symbol = normalize_symbol(raw_symbol)
    if not symbol:
        raise HTTPException(422, "Ticker symbol is required")

    sec = db.execute(
        select(Security).where(func.upper(Security.symbol) == symbol)
    ).scalar_one_or_none()
    if sec:
        return sec

    event_name, event_sector = _event_security_fields_for_symbol(db, symbol)
    if event_name or db.execute(
        select(Event.id)
        .where(Event.symbol.is_not(None))
        .where(func.upper(Event.symbol) == symbol)
        .limit(1)
    ).scalar_one_or_none():
        sec = Security(
            symbol=symbol,
            name=event_name or symbol,
            asset_class="stock",
            sector=event_sector,
        )
        db.add(sec)
        db.flush()
        return sec

    meta = get_ticker_meta(db, [symbol], allow_refresh=True).get(symbol)
    company_name = safe_company_identity_candidate((meta or {}).get("company_name"), symbol)
    if company_name:
        sec = Security(
            symbol=symbol,
            name=company_name,
            asset_class="stock",
            sector=None,
        )
        db.add(sec)
        db.flush()
        return sec

    raise HTTPException(404, "Ticker not found")


@app.post("/api/watchlists/{watchlist_id}/add")
def add_to_watchlist(
    watchlist_id: int,
    symbol: str,
    request: Request,
    db: Session = Depends(get_db),
):
    user = _require_account(request, db)
    _get_owned_watchlist(db, user, watchlist_id)

    sec = _resolve_watchlist_security(db, symbol)

    existing = db.execute(
        select(WatchlistItem)
        .where(
            and_(
                WatchlistItem.watchlist_id == watchlist_id,
                WatchlistItem.security_id == sec.id,
            )
        )
    ).scalar_one_or_none()
    if existing:
        return {"status": "exists", "symbol": sec.symbol}

    entitlements = current_entitlements(request, db)
    require_feature(
        entitlements,
        "watchlist_tickers",
        message="Adding tickers to watchlists is included with Premium.",
    )
    current_count = int(
        db.execute(
            select(func.count())
            .select_from(WatchlistItem)
            .where(WatchlistItem.watchlist_id == watchlist_id)
        ).scalar_one()
        or 0
    )
    enforce_limit(
        entitlements,
        "watchlist_tickers",
        current_count=current_count,
        message="Your current plan has reached its ticker-per-watchlist limit. Upgrade to add more symbols.",
    )

    item = WatchlistItem(
        watchlist_id=watchlist_id,
        security_id=sec.id,
    )
    db.add(item)
    db.commit()
    return {"status": "added", "symbol": sec.symbol}


@app.delete("/api/watchlists/{watchlist_id}/remove")
def remove_from_watchlist(watchlist_id: int, symbol: str, request: Request, db: Session = Depends(get_db)):
    user = _require_account(request, db)
    _get_owned_watchlist(db, user, watchlist_id)
    sec = db.execute(
        select(Security).where(Security.symbol == symbol.upper())
    ).scalar_one_or_none()

    if not sec:
        raise HTTPException(404, "Ticker not found")

    db.execute(
        WatchlistItem.__table__.delete().where(
            and_(
                WatchlistItem.watchlist_id == watchlist_id,
                WatchlistItem.security_id == sec.id,
            )
        )
    )
    db.execute(
        ConfirmationMonitoringSnapshot.__table__.delete().where(
            and_(
                ConfirmationMonitoringSnapshot.watchlist_id == watchlist_id,
                func.upper(ConfirmationMonitoringSnapshot.ticker) == sec.symbol.upper(),
            )
        )
    )
    db.execute(
        ConfirmationMonitoringEvent.__table__.delete().where(
            and_(
                ConfirmationMonitoringEvent.watchlist_id == watchlist_id,
                func.upper(ConfirmationMonitoringEvent.ticker) == sec.symbol.upper(),
            )
        )
    )
    db.execute(
        MonitoringAlert.__table__.delete().where(
            and_(
                MonitoringAlert.source_type == "watchlist",
                MonitoringAlert.source_id == str(watchlist_id),
                func.upper(MonitoringAlert.symbol) == sec.symbol.upper(),
            )
        )
    )
    db.commit()

    return {"status": "removed", "symbol": symbol.upper()}


@app.post("/api/watchlists/{watchlist_id}/seen")
def mark_watchlist_seen(watchlist_id: int, request: Request, db: Session = Depends(get_db)):
    user = _require_account(request, db)
    watchlist = _get_owned_watchlist(db, user, watchlist_id)

    now = datetime.now(timezone.utc)
    mark_watchlist_source_read(db, user_id=user.id, watchlist=watchlist, now=now)
    db.commit()
    return {"watchlist_id": watchlist_id, "last_seen_at": now, "unseen_count": 0}


@app.get("/api/watchlists/{watchlist_id}")
def get_watchlist(watchlist_id: int, request: Request, db: Session = Depends(get_db)):
    user = _require_account(request, db)
    watchlist = _get_owned_watchlist(db, user, watchlist_id)
    refresh_watchlist_alerts(db, user_id=user.id, watchlist=watchlist, lookback_days=7)
    db.commit()

    q = (
        select(Security.symbol, Security.name)
        .join(WatchlistItem, WatchlistItem.security_id == Security.id)
        .where(WatchlistItem.watchlist_id == watchlist_id)
        .order_by(Security.symbol.asc())
    )

    rows = db.execute(q).all()

    return {
        "watchlist_id": watchlist_id,
        "name": watchlist.name,
        "tickers": [
            {"symbol": s, "name": _resolve_ticker_page_name(db, s, canonical_profile_name=n)} for s, n in rows
        ],
        **_watchlist_view_summary(db, watchlist_id),
    }


def _parse_optional_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        cleaned = value.strip()
        if cleaned.endswith("Z"):
            cleaned = cleaned[:-1] + "+00:00"
        parsed = datetime.fromisoformat(cleaned)
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid datetime.") from exc
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


@app.get("/api/watchlists/{watchlist_id}/confirmation-events")
def list_watchlist_confirmation_events(
    watchlist_id: int,
    request: Request,
    db: Session = Depends(get_db),
    since: str | None = None,
    limit: int = Query(10, ge=1, le=50),
):
    user = _require_account(request, db)
    _get_owned_watchlist(db, user, watchlist_id)
    entitlements = current_entitlements(request, db)
    require_monitored_watchlist_source(
        db,
        user_id=user.id,
        watchlist_id=watchlist_id,
        entitlements=entitlements,
    )
    since_dt = _parse_optional_datetime(since)

    q = (
        select(ConfirmationMonitoringEvent)
        .where(ConfirmationMonitoringEvent.user_id == user.id)
        .where(ConfirmationMonitoringEvent.watchlist_id == watchlist_id)
    )
    if since_dt is not None:
        q = q.where(ConfirmationMonitoringEvent.created_at >= since_dt)
    rows = (
        db.execute(
            q.order_by(
                ConfirmationMonitoringEvent.created_at.desc(),
                ConfirmationMonitoringEvent.id.desc(),
            ).limit(limit)
        )
        .scalars()
        .all()
    )
    return {"items": [confirmation_monitoring_event_to_dict(row) for row in rows]}


@app.delete("/api/watchlists/{watchlist_id}/confirmation-events")
def clear_watchlist_confirmation_events(
    watchlist_id: int,
    request: Request,
    db: Session = Depends(get_db),
):
    user = _require_account(request, db)
    _get_owned_watchlist(db, user, watchlist_id)
    entitlements = current_entitlements(request, db)
    require_monitored_watchlist_source(
        db,
        user_id=user.id,
        watchlist_id=watchlist_id,
        entitlements=entitlements,
    )
    result = db.execute(
        ConfirmationMonitoringEvent.__table__.delete().where(
            ConfirmationMonitoringEvent.user_id == user.id,
            ConfirmationMonitoringEvent.watchlist_id == watchlist_id,
        )
    )
    db.commit()
    return {"cleared": int(result.rowcount or 0)}


@app.delete("/api/watchlists/{watchlist_id}/confirmation-events/{event_id}")
def clear_watchlist_confirmation_event(
    watchlist_id: int,
    event_id: int,
    request: Request,
    db: Session = Depends(get_db),
):
    user = _require_account(request, db)
    _get_owned_watchlist(db, user, watchlist_id)
    entitlements = current_entitlements(request, db)
    require_monitored_watchlist_source(
        db,
        user_id=user.id,
        watchlist_id=watchlist_id,
        entitlements=entitlements,
    )
    result = db.execute(
        ConfirmationMonitoringEvent.__table__.delete().where(
            ConfirmationMonitoringEvent.user_id == user.id,
            ConfirmationMonitoringEvent.watchlist_id == watchlist_id,
            ConfirmationMonitoringEvent.id == event_id,
        )
    )
    db.commit()
    return {"cleared": int(result.rowcount or 0)}


@app.post("/api/watchlists/{watchlist_id}/confirmation-monitoring/refresh")
def refresh_watchlist_confirmation_monitoring_endpoint(
    watchlist_id: int,
    request: Request,
    db: Session = Depends(get_db),
):
    user = _require_account(request, db)
    _get_owned_watchlist(db, user, watchlist_id)
    entitlements = current_entitlements(request, db)
    require_monitored_watchlist_source(
        db,
        user_id=user.id,
        watchlist_id=watchlist_id,
        entitlements=entitlements,
    )
    symbols = _watchlist_symbols(db, watchlist_id)
    result = refresh_watchlist_confirmation_monitoring(
        db,
        user_id=user.id,
        watchlist_id=watchlist_id,
        tickers=symbols,
        lookback_days=30,
    )
    db.commit()
    return result


@app.get("/api/watchlists/{watchlist_id}/feed")
def watchlist_feed(
    watchlist_id: int,
    request: Request,
    db: Session = Depends(get_db),
    limit: int = Query(50, ge=1, le=200),
    cursor: str | None = None,

    # allow same filters as /api/feed
    whale: int | None = Query(default=None),
    recent_days: int | None = None,
):
    """
    Feed filtered to tickers inside a watchlist.

    IMPORTANT: WatchlistItem stores security_id (not symbol), so we join:
      WatchlistItem -> Security -> Transaction
    """

    user = _require_account(request, db)
    _get_owned_watchlist(db, user, watchlist_id)

    # 1) Get security_ids in this watchlist
    watch_security_ids = db.execute(
        select(WatchlistItem.security_id).where(WatchlistItem.watchlist_id == watchlist_id)
    ).scalars().all()

    if not watch_security_ids:
        return {"items": [], "next_cursor": None}

    # 2) Build same base query shape as /api/feed
    q = (
        select(Transaction, Member, Security)
        .join(Member, Transaction.member_id == Member.id)
        .outerjoin(Security, Transaction.security_id == Security.id)
        .where(Transaction.security_id.in_(watch_security_ids))
    )

    # 3) Apply whale + recent_days shortcuts (same logic style as /api/feed)
    if whale == 1:
        # "big trades" shortcut; tune the threshold as you like
        q = q.where(
            or_(
                Transaction.amount_range_max >= 100000,
                and_(
                    Transaction.amount_range_max.is_(None),
                    Transaction.amount_range_min >= 100000,
                ),
            )
        )

    if recent_days is not None:
        # filter by report_date (safe, since your ordering uses report_date)
        cutoff = date.today() - timedelta(days=int(recent_days))
        q = q.where(Transaction.report_date.is_not(None)).where(Transaction.report_date >= cutoff)

    # 4) Cursor pagination (report_date DESC, id DESC)
    if cursor:
        try:
            cursor_date_str, cursor_id_str = cursor.split("|", 1)
            cursor_id = int(cursor_id_str)
            cursor_date = date.fromisoformat(cursor_date_str)
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid cursor format. Expected YYYY-MM-DD|id")

        q = q.where(
            or_(
                Transaction.report_date < cursor_date,
                and_(
                    Transaction.report_date == cursor_date,
                    Transaction.id < cursor_id,
                ),
            )
        )

    q = q.order_by(Transaction.report_date.desc(), Transaction.id.desc()).limit(limit + 1)
    rows = db.execute(q).all()

    items = []
    for tx, m, s in rows[:limit]:
        if s is not None:
            security_payload = {
                "symbol": s.symbol,
                "name": s.name,
                "asset_class": s.asset_class,
                "sector": s.sector,
            }
        else:
            security_payload = {
                "symbol": None,
                "name": "Unknown",
                "asset_class": "Unknown",
                "sector": None,
            }

        items.append(
            {
                "id": tx.id,
                "member": {
                    "bioguide_id": m.bioguide_id,
                    "name": f"{m.first_name or ''} {m.last_name or ''}".strip(),
                    "chamber": m.chamber,
                    "party": m.party,
                    "state": m.state,
                },
                "security": security_payload,
                "transaction_type": tx.transaction_type,
                "owner_type": tx.owner_type,
                "trade_date": tx.trade_date.isoformat() if tx.trade_date else None,
                "report_date": tx.report_date.isoformat() if tx.report_date else None,
                "amount_range_min": tx.amount_range_min,
                "amount_range_max": tx.amount_range_max,
                "is_whale": bool(
                    tx.amount_range_max is not None and tx.amount_range_max >= 100000
                ) or bool(
                    tx.amount_range_max is None and tx.amount_range_min is not None and tx.amount_range_min >= 100000
                ),
            }
        )

    next_cursor = None
    if len(rows) > limit:
        tx_last = rows[limit - 1][0]
        if tx_last.report_date:
            next_cursor = f"{tx_last.report_date.isoformat()}|{tx_last.id}"

    return {"items": items, "next_cursor": next_cursor}


app.include_router(events_router, prefix="/api")
app.include_router(signals_router, prefix="/api")
app.include_router(screener_router, prefix="/api")
app.include_router(backtests_router, prefix="/api")
app.include_router(debug_router, prefix="/api")
app.include_router(notifications_router, prefix="/api")
app.include_router(saved_screens_router, prefix="/api")
app.include_router(accounts_router, prefix="/api")
