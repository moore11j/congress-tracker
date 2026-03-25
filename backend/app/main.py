from __future__ import annotations

import logging
import json
import os
import re
import subprocess
from statistics import mean, median
from time import perf_counter

from datetime import date, datetime, timezone, timedelta
from pathlib import Path

from fastapi import FastAPI, Depends, Query, HTTPException
from sqlalchemy import select, func, and_, or_
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError, OperationalError
from pydantic import BaseModel

from app.db import Base, DATABASE_URL, SessionLocal, engine, ensure_event_columns, get_db
from app.models import Event, Filing, Member, Security, TradeOutcome, Transaction, Watchlist, WatchlistItem
from app.routers.debug import router as debug_router
from app.routers.events import router as events_router
from app.routers.signals import router as signals_router
from app.services.price_lookup import get_close_for_date_or_prior, get_eod_close, get_eod_close_series
from app.services.quote_lookup import get_current_prices, get_current_prices_db
from app.services.congress_metadata import get_congress_metadata_resolver
from app.services.returns import signed_return_pct
from app.services.trade_outcomes import (
    dedupe_member_trade_outcomes,
    ensure_member_congress_trade_outcomes,
)

logger = logging.getLogger(__name__)

_CONGRESS_IDENTITY_CACHE: dict[tuple, dict] = {}


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


def _feed_entry_price_for_event(
    db: Session,
    event: Event,
    payload: dict,
    price_memo: dict[tuple[str, str], float | None],
) -> tuple[str, float | None, float | None]:
    sym = (event.symbol or payload.get("symbol") or "").strip().upper()
    if event.event_type == "congress_trade":
        trade_date = payload.get("trade_date") or payload.get("transaction_date")
        if sym and trade_date:
            key = (sym, trade_date)
            if key not in price_memo:
                price_memo[key] = get_eod_close(db, sym, trade_date)
            entry_price = price_memo[key]
        else:
            entry_price = None
        return sym, entry_price, entry_price

    if event.event_type == "insider_trade":
        filing_price = _parse_numeric(payload.get("price"))
        if filing_price is not None and filing_price > 0:
            return sym, filing_price, None

        trade_date = payload.get("transaction_date") or payload.get("trade_date")
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


def _normalize_trade_side(value: str | None) -> str | None:
    if not value:
        return None
    normalized = value.strip().lower()
    if normalized in {"sale", "s-sale", "sell", "s"}:
        return "sale"
    if normalized in {"purchase", "p-purchase", "buy", "p"}:
        return "purchase"
    return normalized or None


def _merge_member_metadata(target: dict, chamber: str | None, party: str | None) -> None:
    resolved_chamber = _clean_metadata_value(chamber)
    resolved_party = _normalize_party(party)

    if not target.get("chamber") and resolved_chamber:
        target["chamber"] = resolved_chamber
    if not target.get("party") and resolved_party:
        target["party"] = resolved_party


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
            "chamber": _clean_metadata_value(member.chamber),
            "party": _normalize_party(member.party),
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


def _build_member_profile(db: Session, member: Member) -> dict:
    trades = _member_recent_trades(db, member.id, lookback_days=None, limit=200)
    ticker_counts = {}
    for trade in trades:
        symbol = trade.get("symbol")
        if symbol:
            ticker_counts[symbol] = ticker_counts.get(symbol, 0) + 1

    top_tickers = sorted(
        ticker_counts.items(),
        key=lambda x: x[1],
        reverse=True
    )[:10]

    return {
        "member": _member_payload(member),
        "top_tickers": [{"symbol": s, "trades": n} for s, n in top_tickers],
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

    member_bioguide_id = db.execute(
        select(Member.bioguide_id).where(Member.id == member_pk).limit(1)
    ).scalar_one_or_none()
    try:
        _, analytics_member_ids = _resolve_member_analytics_aliases(db, member_bioguide_id or "")
    except OperationalError:
        analytics_member_ids = [member_bioguide_id] if member_bioguide_id else []

    outcome_by_logical_key: dict[tuple, TradeOutcome] = {}
    event_payload_by_id: dict[int, dict] = {}
    if analytics_member_ids:
        outcome_query = (
            select(TradeOutcome, Event.payload_json)
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
        deduped_outcomes = dedupe_member_trade_outcomes([row for row, _ in outcome_rows])
        deduped_outcome_ids = {row.id for row in deduped_outcomes}
        for outcome, payload_json in outcome_rows:
            if outcome.id not in deduped_outcome_ids:
                continue
            logical_key = (
                (outcome.symbol or "").strip().upper(),
                _normalize_trade_side(outcome.trade_type),
                outcome.trade_date.isoformat() if outcome.trade_date else "",
                outcome.amount_min,
                outcome.amount_max,
            )
            if logical_key not in outcome_by_logical_key:
                outcome_by_logical_key[logical_key] = outcome
                payload: dict = {}
                if payload_json:
                    try:
                        parsed = json.loads(payload_json)
                        if isinstance(parsed, dict):
                            payload = parsed
                    except Exception:
                        payload = {}
                event_payload_by_id[outcome.event_id] = payload

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

        logical_outcome_key = (
            (symbol or "").strip().upper(),
            _normalize_trade_side(tx.transaction_type),
            tx.trade_date.isoformat() if tx.trade_date else "",
            tx.amount_range_min,
            tx.amount_range_max,
        )
        matched_outcome = outcome_by_logical_key.get(logical_outcome_key)
        outcome_payload = event_payload_by_id.get(matched_outcome.event_id, {}) if matched_outcome else {}
        smart_score = outcome_payload.get("smart_score")
        smart_band = outcome_payload.get("smart_band")

        trades.append({
            "id": tx.id,
            "event_id": matched_outcome.event_id if matched_outcome else None,
            "symbol": symbol,
            "security_name": s.name if s else "Unknown",
            "transaction_type": tx.transaction_type,
            "trade_date": tx.trade_date.isoformat() if tx.trade_date else None,
            "report_date": tx.report_date.isoformat() if tx.report_date else None,
            "amount_range_min": tx.amount_range_min,
            "amount_range_max": tx.amount_range_max,
            "pnl_pct": matched_outcome.return_pct if matched_outcome else None,
            "pnl_source": "trade_outcome" if matched_outcome and matched_outcome.return_pct is not None else None,
            "smart_score": smart_score if isinstance(smart_score, (int, float)) else None,
            "smart_band": smart_band if isinstance(smart_band, str) else None,
        })

    return trades


# --- App --------------------------------------------------------------------

app = FastAPI(title="Congress Tracker API", version="0.1.0")

from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


class WatchlistPayload(BaseModel):
    name: str

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
    # Creates tables if missing. Does NOT delete or overwrite data.
    Base.metadata.create_all(bind=engine)
    ensure_event_columns()

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
def seed_demo(db: Session = Depends(get_db)):
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

    event_types = ["insider_trade"] if tape_value == "insider" else ["congress_trade", "insider_trade"]
    _ = benchmark
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

        symbol_value, entry_price, estimated_price = _feed_entry_price_for_event(db, event, payload, price_memo)
        if symbol_value and entry_price is not None and entry_price > 0:
            quote_symbols.add(symbol_value)

        parsed_events.append((event, payload, symbol_value, entry_price, estimated_price))

    current_price_memo = get_current_prices_db(db, _cap_symbols(quote_symbols)) if quote_symbols else {}

    items = []
    for event, payload, symbol_value, entry_price, estimated_price in parsed_events:
        current_price = current_price_memo.get(symbol_value) if symbol_value else None
        pnl_pct = None
        if current_price is not None and entry_price is not None and entry_price > 0:
            pnl_pct = signed_return_pct(current_price, entry_price, event.transaction_type or event.trade_type)

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
                "amount_range_min": event.amount_min,
                "amount_range_max": event.amount_max,
                "is_whale": bool(event.amount_max is not None and event.amount_max >= 250000),
                "source": event.source,
                "estimated_price": estimated_price,
                "current_price": current_price,
                "pnl_pct": pnl_pct,
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

ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "")

def _require_admin(token: str | None):
    if not ADMIN_TOKEN:
        raise HTTPException(status_code=500, detail="ADMIN_TOKEN not configured")
    if not token or token != ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")


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
def ensure_data(token: str | None = Query(default=None), db: Session = Depends(get_db)):
    """
    If transactions == 0, run ingest_house + ingest_senate + enrich_members + write_last_updated.
    Safe to call repeatedly.
    """
    _require_admin(token)

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


@app.get("/api/members/by-slug/{slug}")
def member_profile_by_slug(slug: str, db: Session = Depends(get_db)):
    slug_value = (slug or "").strip()
    if not slug_value:
        raise HTTPException(status_code=404, detail="Member not found")

    direct = db.execute(select(Member).where(Member.bioguide_id == slug_value)).scalar_one_or_none()
    if direct:
        return _build_member_profile(db, direct)

    normalized = _slug_to_name(slug_value)
    if not normalized:
        raise HTTPException(status_code=404, detail="Member not found")

    members = db.execute(select(Member)).scalars().all()
    matched = [member for member in members if _normalize_name(_member_full_name(member)) == normalized]

    if not matched:
        raise HTTPException(status_code=404, detail="Member not found")

    member = matched[0]
    return _build_member_profile(db, member)


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
    ensure_member_congress_trade_outcomes(
        db=db,
        member_ids=analytics_member_ids or [analytics_member_id],
        lookback_days=lookback_days,
        benchmark_symbol=benchmark_symbol,
    )
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
        "pnl_status": "ok" if trade_count_scored > 0 or total_count == 0 else "unavailable",
    }


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
    ensure_member_congress_trade_outcomes(
        db=db,
        member_ids=analytics_member_ids or [analytics_member_id],
        lookback_days=lookback_days,
        benchmark_symbol=benchmark_symbol,
    )
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
    benchmark_base = benchmark_close_map.get(benchmark_dates[0]) if benchmark_dates else None

    benchmark_series: list[dict] = []
    if benchmark_base is not None and benchmark_base > 0:
        for asof_date in benchmark_dates:
            close_value = benchmark_close_map.get(asof_date)
            if close_value is None or close_value <= 0:
                continue
            benchmark_series.append(
                {
                    "asof_date": asof_date,
                    "cumulative_return_pct": float(((close_value - benchmark_base) / benchmark_base) * 100),
                }
            )


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

    cumulative_return = 0.0
    cumulative_alpha = 0.0
    member_series: list[dict] = []
    for row in sorted(rows, key=lambda item: (item.trade_date or date.min, item.event_id)):
        if row.return_pct is not None:
            cumulative_return += row.return_pct
        if row.alpha_pct is not None:
            cumulative_alpha += row.alpha_pct

        trade_date = row.trade_date.isoformat() if row.trade_date else None
        running_benchmark_return_pct = None
        if benchmark_base is not None and benchmark_base > 0 and trade_date:
            benchmark_close = get_close_for_date_or_prior(trade_date, benchmark_close_map, benchmark_dates)
            if benchmark_close is not None and benchmark_close > 0:
                running_benchmark_return_pct = float(((benchmark_close - benchmark_base) / benchmark_base) * 100)

        member_series.append(
            {
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
        )

    return {
        "member_id": analytics_member_id,
        "lookback_days": lookback_days,
        "benchmark_symbol": benchmark_symbol,
        "trades_analyzed": count,
        "avg_return_pct": mean(return_values) if return_values else None,
        "avg_alpha_pct": mean(alpha_values) if alpha_values else None,
        "win_rate": (sum(1 for value in return_values if value > 0) / count) if count else None,
        "avg_holding_days": mean(holding_day_values) if holding_day_values else None,
        "best_trades": best_trades,
        "worst_trades": worst_trades,
        "member_series": member_series,
        "benchmark_series": benchmark_series,
        "performance_series": member_series,
    }


@app.get("/api/leaderboards/congress-traders")
def congress_trader_leaderboard(
    lookback_days: int = 365,
    chamber: str = "all",
    source_mode: str = "congress",
    sort: str = "avg_alpha",
    min_trades: int = 3,
    limit: int = 100,
    benchmark: str = "^GSPC",
    db: Session = Depends(get_db),
):
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

    normalized_sort = (sort or "avg_alpha").strip().lower()
    valid_sorts = {"avg_alpha", "avg_return", "win_rate", "trade_count"}
    if normalized_sort not in valid_sorts:
        normalized_sort = "avg_alpha"

    min_trades = max(min_trades, 1)
    limit = min(max(limit, 1), 250)
    perf.min_trades = min_trades
    perf.limit = limit

    cutoff_dt = datetime.utcnow() - timedelta(days=lookback_days)

    if normalized_source_mode == "congress":
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
            .where(TradeOutcome.trade_date >= cutoff_dt.date())
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
                    "chamber": profile["chamber"],
                    "party": profile["party"],
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

    member_outcome_filters = [
        TradeOutcome.member_id.is_not(None),
        TradeOutcome.trade_date.is_not(None),
        TradeOutcome.trade_date >= cutoff_dt.date(),
        TradeOutcome.benchmark_symbol == benchmark_symbol,
    ]

    insider_market_trade_types = {"purchase", "sale", "buy", "sell"}

    if normalized_source_mode == "congress":
        member_outcome_filters.append(Event.event_type == "congress_trade")
    elif normalized_source_mode == "insiders":
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

    total_count_rows = db.execute(
        select(TradeOutcome.member_id, func.count(TradeOutcome.id))
        .select_from(TradeOutcome)
        .join(Event, Event.id == TradeOutcome.event_id)
        .join(Member, Member.bioguide_id == TradeOutcome.member_id, isouter=True)
        .where(*member_outcome_filters)
        .group_by(TradeOutcome.member_id)
    ).all()
    perf.stage("candidate_row_fetch", rows=len(total_count_rows))
    total_count_by_member = {
        member_id: int(count)
        for member_id, count in total_count_rows
        if member_id
    }

    scored_rows = db.execute(
        select(
            TradeOutcome.member_id,
            TradeOutcome.member_name,
            TradeOutcome.return_pct,
            TradeOutcome.alpha_pct,
            Member.first_name,
            Member.last_name,
            Member.chamber,
            Member.party,
        )
        .select_from(TradeOutcome)
        .join(Event, Event.id == TradeOutcome.event_id)
        .join(Member, Member.bioguide_id == TradeOutcome.member_id, isouter=True)
        .where(*member_outcome_filters)
        .where(TradeOutcome.scoring_status == "ok")
        .order_by(TradeOutcome.trade_date.desc(), TradeOutcome.id.desc())
    ).all()
    perf.stage("alias_logical_identity_grouping", rows=len(scored_rows))

    grouped_rows: dict[str, dict] = {}
    member_name_by_id: dict[str, str] = {}
    for (
        member_id,
        outcome_member_name,
        return_pct,
        alpha_pct,
        first_name,
        last_name,
        member_chamber,
        member_party,
    ) in scored_rows:
        if not member_id:
            continue
        existing = grouped_rows.get(member_id)
        if not existing:
            resolved_name = f"{first_name or ''} {last_name or ''}".strip() or (outcome_member_name or member_id)
            existing = {
                "member_id": member_id,
                "member_name": resolved_name,
                "chamber": None,
                "party": None,
                "return_values": [],
                "alpha_values": [],
                "scored_count": 0,
                "win_count": 0,
            }
            grouped_rows[member_id] = existing
            member_name_by_id[member_id] = outcome_member_name or resolved_name

        _merge_member_metadata(existing, member_chamber, member_party)

        existing["scored_count"] += 1
        if return_pct is not None:
            existing["return_values"].append(return_pct)
            if return_pct > 0:
                existing["win_count"] += 1
        if alpha_pct is not None:
            existing["alpha_values"].append(alpha_pct)
    perf.stage("trade_outcomes_aggregation", rows=len(grouped_rows))

    member_ids = list(grouped_rows.keys())
    unresolved_ids = {
        member_id
        for member_id, grouped in grouped_rows.items()
        if not grouped.get("party") or not grouped.get("chamber")
    }

    if unresolved_ids:
        canonical_rows = db.execute(
            select(Member.bioguide_id, Member.chamber, Member.party)
            .where(Member.bioguide_id.in_(member_ids))
        ).all()
        for member_id, member_chamber, member_party in canonical_rows:
            target = grouped_rows.get(member_id)
            if not target:
                continue
            _merge_member_metadata(target, member_chamber, member_party)

    unresolved_ids = {
        member_id
        for member_id, grouped in grouped_rows.items()
        if not grouped.get("party") or not grouped.get("chamber")
    }

    if unresolved_ids:
        name_candidates: dict[str, list[str]] = {}
        for member_id, name in member_name_by_id.items():
            if member_id not in unresolved_ids:
                continue
            normalized_name = _normalize_name(name)
            if not normalized_name:
                continue
            name_candidates.setdefault(normalized_name, []).append(member_id)
        if name_candidates:
            members = db.execute(
                select(Member.first_name, Member.last_name, Member.chamber, Member.party)
            ).all()
            canonical_by_name: dict[str, tuple[str | None, str | None] | str] = {}
            for first_name, last_name, member_chamber, member_party in members:
                normalized_name = _normalize_name(f"{first_name or ''} {last_name or ''}")
                if not normalized_name or normalized_name not in name_candidates:
                    continue
                existing = canonical_by_name.get(normalized_name)
                value = (member_chamber, member_party)
                if existing is None:
                    canonical_by_name[normalized_name] = value
                else:
                    canonical_by_name[normalized_name] = "ambiguous"

            for normalized_name, canonical in canonical_by_name.items():
                if canonical == "ambiguous":
                    continue
                for member_id in name_candidates[normalized_name]:
                    target = grouped_rows.get(member_id)
                    if not target:
                        continue
                    _merge_member_metadata(target, canonical[0], canonical[1])

    unresolved_ids = {
        member_id
        for member_id, grouped in grouped_rows.items()
        if not grouped.get("party") or not grouped.get("chamber")
    }

    if unresolved_ids:
        fallback_rows = db.execute(
            select(TradeOutcome.member_id, Event.chamber, Event.party)
            .select_from(TradeOutcome)
            .join(Event, Event.id == TradeOutcome.event_id)
            .where(TradeOutcome.member_id.in_(unresolved_ids))
            .where(
                or_(
                    Event.chamber.is_not(None),
                    Event.party.is_not(None),
                )
            )
            .order_by(TradeOutcome.trade_date.desc(), TradeOutcome.id.desc())
        ).all()
        for member_id, event_chamber, event_party in fallback_rows:
            target = grouped_rows.get(member_id)
            if not target:
                continue
            _merge_member_metadata(target, event_chamber, event_party)
    perf.stage("per_row_enrichment_link_building", rows=len(grouped_rows))

    rows: list[dict] = []
    for member_id, grouped in grouped_rows.items():
        return_values = grouped["return_values"]
        alpha_values = grouped["alpha_values"]
        trade_count_scored = grouped["scored_count"]
        if trade_count_scored < min_trades:
            continue

        rows.append(
            {
                "member_id": member_id,
                "member_name": grouped["member_name"],
                "chamber": grouped["chamber"],
                "party": grouped["party"],
                "trade_count_total": total_count_by_member.get(member_id, trade_count_scored),
                "trade_count_scored": trade_count_scored,
                "avg_return": mean(return_values) if return_values else None,
                "median_return": median(return_values) if return_values else None,
                "win_rate": (grouped["win_count"] / trade_count_scored) if trade_count_scored else None,
                "avg_alpha": mean(alpha_values) if alpha_values else None,
                "median_alpha": median(alpha_values) if alpha_values else None,
                "benchmark_symbol": benchmark_symbol,
                "pnl_status": "ok",
            }
        )

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
    if not scored_rows:
        response["status"] = "outcomes_not_populated"
        response["message"] = "No persisted trade outcomes available for the requested filters yet."
    perf.finish(result_rows=len(rows))
    return response


@app.get("/api/tickers")
def ticker_profiles(symbols: str | None = Query(None), db: Session = Depends(get_db)):
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


@app.get("/api/tickers/{symbol}/price-history")
def ticker_price_history(
    symbol: str,
    days: int = Query(365, ge=30, le=365),
    db: Session = Depends(get_db),
):
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

    security = db.execute(select(Security).where(Security.symbol == sym)).scalar_one_or_none()

    if not security:
        fallback_profile = _build_ticker_fallback_profile(sym, db)
        if fallback_profile is None:
            raise LookupError("Ticker not found")
        return fallback_profile

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

    return {
        "ticker": {
            "symbol": security.symbol,
            "name": security.name,
            "asset_class": security.asset_class,
            "sector": security.sector,
        },
        "top_members": [
            {
                **_top_member_payload(members_by_id[member_id]),
                "trade_count": trade_count,
            }
            for member_id, trade_count in top_members
        ],
        "trades": trades,
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

    name = sym
    for event in events:
        try:
            payload = json.loads(event.payload_json or "{}")
            if not isinstance(payload, dict):
                payload = {}
        except Exception:
            payload = {}

        raw = payload.get("raw") if isinstance(payload.get("raw"), dict) else {}
        candidate_name = (
            raw.get("companyName")
            or payload.get("company_name")
            or payload.get("companyName")
        )
        if candidate_name and candidate_name.strip().upper() != sym:
            name = candidate_name.strip()
            break

    return {
        "ticker": {
            "symbol": sym,
            "name": name,
            "asset_class": "Equity",
            "sector": None,
        },
        "top_members": [],
        "trades": [],
    }


@app.post("/api/watchlists")
def create_watchlist(payload: WatchlistPayload, db: Session = Depends(get_db)):
    name = payload.name.strip()
    if not name:
        raise HTTPException(status_code=422, detail="Watchlist name is required")

    existing = db.execute(select(Watchlist).where(Watchlist.name == name)).scalar_one_or_none()
    if existing:
        raise HTTPException(status_code=409, detail="Watchlist name already exists")

    w = Watchlist(name=name)
    db.add(w)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        raise HTTPException(status_code=409, detail="Watchlist name already exists")
    return {"id": w.id, "name": w.name}


@app.get("/api/watchlists")
def list_watchlists(db: Session = Depends(get_db)):
    rows = db.execute(select(Watchlist)).scalars().all()
    return [{"id": w.id, "name": w.name} for w in rows]


@app.delete("/api/watchlists/{watchlist_id}", status_code=204)
def delete_watchlist(watchlist_id: int, db: Session = Depends(get_db)):
    watchlist = db.execute(
        select(Watchlist).where(Watchlist.id == watchlist_id)
    ).scalar_one_or_none()

    if not watchlist:
        raise HTTPException(status_code=404, detail="Watchlist not found")

    db.execute(
        WatchlistItem.__table__.delete().where(
            WatchlistItem.watchlist_id == watchlist_id
        )
    )
    db.delete(watchlist)
    db.commit()

    return None


@app.put("/api/watchlists/{watchlist_id}")
def rename_watchlist(watchlist_id: int, payload: WatchlistPayload, db: Session = Depends(get_db)):
    name = payload.name.strip()
    if not name:
        raise HTTPException(status_code=422, detail="Watchlist name is required")

    watchlist = db.execute(
        select(Watchlist).where(Watchlist.id == watchlist_id)
    ).scalar_one_or_none()
    if not watchlist:
        raise HTTPException(status_code=404, detail="Watchlist not found")

    existing = db.execute(
        select(Watchlist).where(and_(Watchlist.name == name, Watchlist.id != watchlist_id))
    ).scalar_one_or_none()
    if existing:
        raise HTTPException(status_code=409, detail="Watchlist name already exists")

    watchlist.name = name
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        raise HTTPException(status_code=409, detail="Watchlist name already exists")

    return {"id": watchlist.id, "name": watchlist.name}


@app.post("/api/watchlists/{watchlist_id}/add")
def add_to_watchlist(watchlist_id: int, symbol: str, db: Session = Depends(get_db)):
    sec = db.execute(
        select(Security).where(Security.symbol == symbol.upper())
    ).scalar_one_or_none()

    if not sec:
        raise HTTPException(404, "Ticker not found")

    item = WatchlistItem(
        watchlist_id=watchlist_id,
        security_id=sec.id,
    )
    db.add(item)
    db.commit()
    return {"status": "added", "symbol": symbol.upper()}


@app.delete("/api/watchlists/{watchlist_id}/remove")
def remove_from_watchlist(watchlist_id: int, symbol: str, db: Session = Depends(get_db)):
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
    db.commit()

    return {"status": "removed", "symbol": symbol.upper()}


@app.get("/api/watchlists/{watchlist_id}")
def get_watchlist(watchlist_id: int, db: Session = Depends(get_db)):
    q = (
        select(Security.symbol, Security.name)
        .join(WatchlistItem, WatchlistItem.security_id == Security.id)
        .where(WatchlistItem.watchlist_id == watchlist_id)
    )

    rows = db.execute(q).all()

    return {
        "watchlist_id": watchlist_id,
        "tickers": [
            {"symbol": s, "name": n} for s, n in rows
        ],
    }


@app.get("/api/watchlists/{watchlist_id}/feed")
def watchlist_feed(
    watchlist_id: int,
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
app.include_router(debug_router, prefix="/api")
