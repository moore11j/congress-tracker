from __future__ import annotations

import argparse
import json
import logging
import re
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from statistics import median
from types import SimpleNamespace

from sqlalchemy import delete, func, or_, select

from app.db import Base, SessionLocal, engine
from app.models import (
    CongressMemberAlias,
    Event,
    Member,
    PriceCache,
    ReplicatedPortfolioPoint,
    ReplicatedPortfolioPosition,
    ReplicatedPortfolioRun,
)
from app.services.backtesting.queries import parse_payload
from app.services.replicated_portfolios import (
    SUPPORTED_MODES,
    curve_debug_daily_payload,
    curve_diagnostics_payload,
    default_warmup_days_for_lookback,
    effective_window_payload,
    inspect_replicated_portfolio_event,
    latest_replicated_portfolio_payload,
    load_replicated_portfolio_events,
    normalize_skip_reason,
    persist_replicated_portfolio_run,
    run_replicated_portfolio_simulation,
    skip_reason_summary,
    warmup_diagnostics_payload,
)
from app.services.price_lookup import _fetch_provider_eod_close_series, _safe_cache_upsert
from app.services.ticker_meta import normalize_cik
from app.utils.symbols import normalize_symbol, symbol_variants

logger = logging.getLogger(__name__)
STANDARD_LOOKBACK_DAYS = [30, 90, 180, 365, 1095]
_REPORTING_CIK_TEXT_RE = re.compile(
    r'"(?:reporting_cik|reportingCik|reportingCIK|rptOwnerCik)"\s*:\s*"?(\d+)"?',
    re.IGNORECASE,
)
DEFAULT_PRICE_PREFLIGHT_MAX_PASSES = 2
DEFAULT_ALL_CONGRESS_PRICE_PREFLIGHT_MAX_PASSES = 4
DEFAULT_PRICE_PREFLIGHT_MAX_SYMBOLS = 10
DEFAULT_MIN_AVG_PRICED_INVESTED_VALUE_PCT = 85.0
DEFAULT_MAX_PCT_INVESTED_VALUE_WITH_PRICE_GAPS = 15.0
ALL_CONGRESS_DEFAULT_LOOKBACK_DAYS = 365
ALL_CONGRESS_SUPPORTED_LOOKBACK_DAYS = (30, 90, 180, 365, 1095)
ALL_CONGRESS_DEFAULT_BATCH_SIZE_BY_LOOKBACK = {
    30: 75,
    90: 75,
    180: 75,
    365: 10,
    1095: 25,
}
ALL_CONGRESS_LOOKBACK_DAYS = ALL_CONGRESS_DEFAULT_LOOKBACK_DAYS
ALL_CONGRESS_MODE = "realistic_disclosure_lag"
ALL_CONGRESS_ENTITY_TYPE = "congress_member"
FMP_COMMA_FRAGMENT_CANONICAL_MEMBER_IDS = {
    "FMP_SENATE_XX_JUSTICE_II": "J000312",
    "__JAMES_CONLEY_(SENATOR)": "J000312",
    "FMP_SENATE_XX_MORENO": "M001242",
    "_BERNARDO_(SENATOR)": "M001242",
}
_LEGACY_CONGRESS_MEMBER_ID_RE = re.compile(r"^(?:FMP_|_).+", re.IGNORECASE)


@dataclass(frozen=True)
class CandidateSelection:
    entity_ids: list[str]
    candidates_scanned: int = 0
    candidates_selected: int = 0
    events_prefiltered: int = 0
    events_parsed: int = 0
    candidate_scan_limit_hit: bool = False
    candidate_metrics: dict[str, dict] | None = None

    def asdict(self) -> dict[str, int | bool]:
        return {
            "candidates_scanned": self.candidates_scanned,
            "candidates_selected": self.candidates_selected,
            "events_prefiltered": self.events_prefiltered,
            "events_parsed": self.events_parsed,
            "candidate_scan_limit_hit": self.candidate_scan_limit_hit,
        }

    def metrics_for(self, entity_id: str) -> dict:
        if not self.candidate_metrics:
            return {}
        return self.candidate_metrics.get(entity_id, {})


def _event_reporting_cik(payload: dict) -> str | None:
    raw = payload.get("raw") if isinstance(payload.get("raw"), dict) else {}
    for source in (payload, raw):
        for key in ("reporting_cik", "reportingCik", "reportingCIK", "rptOwnerCik"):
            value = source.get(key)
            if isinstance(value, str) and value.strip():
                return normalize_cik(value)
    return None


def _first_payload_text(payload: dict, *keys: str) -> str | None:
    raw = payload.get("raw") if isinstance(payload.get("raw"), dict) else {}
    for source in (payload, raw):
        for key in keys:
            value = source.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return None


def _event_reporting_name(payload: dict) -> str | None:
    return _first_payload_text(
        payload,
        "reportingOwnerName",
        "reporting_owner_name",
        "ownerName",
        "insiderName",
        "insider_name",
        "name",
    )


def _event_reporting_cik_from_payload_text(payload_json: str | None) -> str | None:
    if not payload_json:
        return None
    match = _REPORTING_CIK_TEXT_RE.search(payload_json)
    if not match:
        return None
    return normalize_cik(match.group(1))


def _symbol_price_count(db, *, symbol: str | None, start_date: date, end_date: date, cache: dict[str, int]) -> int:
    normalized_symbol = normalize_symbol(symbol)
    if not normalized_symbol:
        return 0
    if normalized_symbol not in cache:
        cache[normalized_symbol] = int(
            db.scalar(
                select(func.count())
                .select_from(PriceCache)
                .where(PriceCache.symbol == normalized_symbol)
                .where(PriceCache.date >= start_date.isoformat())
                .where(PriceCache.date <= end_date.isoformat())
            )
            or 0
        )
    return cache[normalized_symbol]


def _candidate_congress_members(db, *, limit: int, lookback_days: int) -> list[str]:
    cutoff = datetime.now(timezone.utc) - timedelta(days=max(lookback_days, 1) + 14)
    rows = db.execute(
        select(Event.member_bioguide_id)
        .where(Event.event_type == "congress_trade")
        .where(Event.ts >= cutoff)
        .where(Event.member_bioguide_id.is_not(None))
        .group_by(Event.member_bioguide_id)
        .order_by(func.count(Event.id).desc(), Event.member_bioguide_id.asc())
        .limit(limit)
    ).all()
    return [str(member_id) for (member_id,) in rows if member_id]


def _insider_reporting_cik_prefilter_clause(normalized_cik: str):
    variants = {normalized_cik}
    stripped = normalized_cik.lstrip("0")
    if stripped:
        variants.add(stripped)

    patterns: list[str] = []
    for cik in variants:
        patterns.extend(
            [
                f'"reporting_cik":"{cik}"',
                f'"reporting_cik": "{cik}"',
                f'"reportingCik":"{cik}"',
                f'"reportingCik": "{cik}"',
                f'"reportingCIK":"{cik}"',
                f'"reportingCIK": "{cik}"',
                f'"rptOwnerCik":"{cik}"',
                f'"rptOwnerCik": "{cik}"',
            ]
        )

    return or_(*[Event.payload_json.contains(pattern) for pattern in patterns])


def _insider_likely_side_clause():
    payload_lower = func.lower(func.coalesce(Event.payload_json, ""))
    trade_type_lower = func.lower(func.coalesce(Event.trade_type, ""))
    transaction_type_lower = func.lower(func.coalesce(Event.transaction_type, ""))
    return or_(
        trade_type_lower.in_(("purchase", "buy", "sale", "sell", "p", "s", "a", "d")),
        transaction_type_lower.in_(("purchase", "buy", "sale", "sell", "p", "s", "a", "d")),
        payload_lower.like("%purchase%"),
        payload_lower.like("%sale%"),
        payload_lower.like("%transactioncode%"),
        payload_lower.like("%transaction_code%"),
        payload_lower.like("%acquireddisposed%"),
        payload_lower.like("%acquired_disposed%"),
        payload_lower.like("%acquisition_or_disposition%"),
    )


def _insider_likely_reporting_cik_clause():
    payload_lower = func.lower(func.coalesce(Event.payload_json, ""))
    return or_(
        payload_lower.like('%"reporting_cik"%'),
        payload_lower.like('%"reportingcik"%'),
        payload_lower.like('%"rptownercik"%'),
    )


def _insider_base_candidate_query(*, cutoff: datetime, now_dt: datetime, issuer_symbol: str | None):
    query = (
        select(Event)
        .where(Event.event_type == "insider_trade")
        .where(Event.ts >= cutoff)
        .where(Event.ts <= now_dt)
        .where(or_(Event.event_date.is_(None), Event.event_date <= now_dt))
        .where(Event.symbol.is_not(None))
        .where(Event.symbol != "")
        .where(Event.payload_json.is_not(None))
        .where(_insider_likely_reporting_cik_clause())
        .where(_insider_likely_side_clause())
    )
    if issuer_symbol:
        query = query.where(func.upper(Event.symbol) == issuer_symbol.upper())
    return query


def _insider_issuer_payload_clause(normalized_issuer_cik: str):
    variants = {normalized_issuer_cik}
    stripped = normalized_issuer_cik.lstrip("0")
    if stripped:
        variants.add(stripped)

    patterns: list[str] = []
    for cik in variants:
        patterns.extend(
            [
                f'"companyCik":"{cik}"',
                f'"companyCik": "{cik}"',
                f'"companyCIK":"{cik}"',
                f'"companyCIK": "{cik}"',
                f'"issuer_cik":"{cik}"',
                f'"issuer_cik": "{cik}"',
                f'"issuerCik":"{cik}"',
                f'"issuerCik": "{cik}"',
            ]
        )
    return or_(*[Event.payload_json.contains(pattern) for pattern in patterns])


def _insider_candidate_rows_query(
    *,
    cutoff: datetime,
    now_dt: datetime,
    issuer_cik: str | None,
    issuer_symbol: str | None,
    row_limit: int,
):
    query = _insider_base_candidate_query(
        cutoff=cutoff,
        now_dt=now_dt,
        issuer_symbol=issuer_symbol,
    )
    if issuer_cik:
        query = query.where(_insider_issuer_payload_clause(issuer_cik))
    return query.order_by(Event.ts.desc(), Event.id.desc()).limit(row_limit)


def _candidate_insiders(
    db,
    *,
    limit: int,
    lookback_days: int,
    candidate_scan_limit: int = 500,
    max_events_per_candidate: int = 100,
    issuer_cik: str | None = None,
    issuer_symbol: str | None = None,
) -> CandidateSelection:
    cutoff = datetime.now(timezone.utc) - timedelta(days=max(lookback_days, 1) + 14)
    now_dt = datetime.now(timezone.utc)
    start_date = now_dt.date() - timedelta(days=max(lookback_days, 1))
    end_date = now_dt.date()
    normalized_issuer_cik = normalize_cik(issuer_cik)
    normalized_issuer_symbol = normalize_symbol(issuer_symbol)
    scan_limit = max(candidate_scan_limit, 1)
    per_candidate_limit = max(max_events_per_candidate, 1)
    row_limit = scan_limit * per_candidate_limit
    candidate_query = _insider_candidate_rows_query(
        cutoff=cutoff,
        now_dt=now_dt,
        issuer_cik=normalized_issuer_cik,
        issuer_symbol=normalized_issuer_symbol,
        row_limit=row_limit,
    )
    rows = db.execute(candidate_query).scalars().all()

    seen: set[str] = set()
    inspected_by_cik: dict[str, int] = {}
    stats_by_cik: dict[str, dict] = {}
    price_count_cache: dict[str, int] = {}
    events_parsed = 0
    scan_limit_hit = len(rows) >= row_limit
    for event in rows:
        cik = _event_reporting_cik_from_payload_text(event.payload_json)
        if not cik:
            continue
        if cik not in seen:
            if len(seen) >= scan_limit:
                scan_limit_hit = True
                break
            seen.add(cik)
        if inspected_by_cik.get(cik, 0) >= per_candidate_limit:
            continue
        inspected_by_cik[cik] = inspected_by_cik.get(cik, 0) + 1
        inspected = inspect_replicated_portfolio_event(event, entity_type="insider", entity_id="")
        events_parsed += 1
        stats = stats_by_cik.setdefault(
            cik,
            {
                "entity_name": None,
                "candidate_valid_side_events": 0,
                "candidate_priceable_event_estimate": 0,
                "candidate_non_market_events": 0,
                "candidate_missing_price_events": 0,
                "candidate_inspected_events": 0,
                "candidate_name_found": False,
                "candidate_quality_score": 0.0,
            },
        )
        stats["candidate_inspected_events"] += 1
        if not stats["entity_name"]:
            event_name = _event_reporting_name(parse_payload(event.payload_json))
            if event_name:
                stats["entity_name"] = event_name
                stats["candidate_name_found"] = True
        if inspected.get("reporting_cik") != cik:
            continue
        if normalized_issuer_cik and inspected.get("issuer_cik") != normalized_issuer_cik:
            continue
        if normalized_issuer_symbol and inspected.get("symbol") != normalized_issuer_symbol:
            continue
        if inspected.get("skip_reason") == "insider_non_market":
            stats["candidate_non_market_events"] += 1
            continue
        if inspected.get("skip_reason") is not None or inspected.get("normalized_side") not in {"purchase", "sale"}:
            continue
        stats["candidate_valid_side_events"] += 1
        price_count = _symbol_price_count(
            db,
            symbol=inspected.get("symbol"),
            start_date=start_date,
            end_date=end_date,
            cache=price_count_cache,
        )
        if price_count > 0:
            stats["candidate_priceable_event_estimate"] += 1
        else:
            stats["candidate_missing_price_events"] += 1

    ranked_candidates: list[tuple[float, int, int, bool, str]] = []
    for cik, stats in stats_by_cik.items():
        valid_side_events = int(stats["candidate_valid_side_events"])
        priceable_events = int(stats["candidate_priceable_event_estimate"])
        missing_price_events = int(stats["candidate_missing_price_events"])
        non_market_events = int(stats["candidate_non_market_events"])
        if valid_side_events <= 0 or priceable_events <= 0:
            continue
        score = (
            priceable_events * 10.0
            + valid_side_events * 2.0
            + (3.0 if stats["candidate_name_found"] else 0.0)
            - missing_price_events * 2.0
            - non_market_events * 3.0
        )
        stats["candidate_quality_score"] = round(score, 4)
        ranked_candidates.append(
            (
                score,
                priceable_events,
                valid_side_events,
                bool(stats["candidate_name_found"]),
                cik,
            )
        )

    ranked_candidates.sort(key=lambda item: (-item[0], -item[1], -item[2], not item[3], item[4]))
    out = [cik for _, _, _, _, cik in ranked_candidates[:limit]]
    candidate_metrics = {
        cik: {
            "entity_name": stats.get("entity_name"),
            "candidate_quality_score": stats.get("candidate_quality_score", 0.0),
            "candidate_valid_side_events": stats.get("candidate_valid_side_events", 0),
            "candidate_priceable_event_estimate": stats.get("candidate_priceable_event_estimate", 0),
            "candidate_name_found": bool(stats.get("candidate_name_found")),
        }
        for cik, stats in stats_by_cik.items()
    }

    return CandidateSelection(
        entity_ids=out,
        candidates_scanned=len(seen),
        candidates_selected=len(out),
        events_prefiltered=len(rows),
        events_parsed=events_parsed,
        candidate_scan_limit_hit=scan_limit_hit,
        candidate_metrics=candidate_metrics,
    )


def _normalize_entity_type(value: str) -> str:
    normalized = (value or "").strip().lower()
    if normalized == "congress":
        return "congress_member"
    if normalized in {"congress_member", "insider"}:
        return normalized
    raise ValueError("entity-type must be congress, congress_member, or insider")


def _parse_lookback_days(value: int | str | list[int] | tuple[int, ...] | None) -> list[int]:
    if value is None:
        return [1095]
    if isinstance(value, int):
        items = [value]
    elif isinstance(value, (list, tuple)):
        items = [int(item) for item in value]
    else:
        parts = [part.strip() for part in str(value).split(",") if part.strip()]
        if not parts:
            raise ValueError("lookback-days must include at least one integer")
        items = [int(part) for part in parts]
    out: list[int] = []
    seen: set[int] = set()
    for item in items:
        if item <= 0:
            raise ValueError("lookback-days values must be positive integers")
        if item not in seen:
            out.append(item)
            seen.add(item)
    return out


def _resolve_lookback_days(*, lookback_days: int | str | list[int] | tuple[int, ...] | None, lookback_set: str | None) -> list[int]:
    normalized_set = (lookback_set or "").strip().lower()
    if normalized_set:
        if normalized_set != "standard":
            raise ValueError("lookback-set must be standard")
        return list(STANDARD_LOOKBACK_DAYS)
    return _parse_lookback_days(lookback_days)


def _parse_debug_date_range(value: str | None) -> tuple[date, date] | None:
    if not value:
        return None
    parts = [part.strip() for part in value.split(":", 1)]
    if len(parts) != 2 or not parts[0] or not parts[1]:
        raise ValueError("--debug-date-range must be formatted as YYYY-MM-DD:YYYY-MM-DD")
    start = date.fromisoformat(parts[0])
    end = date.fromisoformat(parts[1])
    if end < start:
        raise ValueError("--debug-date-range end date must be on or after start date")
    return start, end


def _parse_entity_ids(
    value: str | list[str] | tuple[str, ...] | None,
    *,
    entity_type: str,
    split_strings: bool = True,
) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        raw_items = value.split(",") if split_strings else [value]
    else:
        raw_items = list(value)
    out: list[str] = []
    seen: set[str] = set()
    for raw in raw_items:
        item = str(raw).strip()
        if not item:
            continue
        normalized = normalize_cik(item) if entity_type == "insider" else item
        if normalized and normalized not in seen:
            out.append(normalized)
            seen.add(normalized)
    return out


def _entity_name(db, *, entity_type: str, entity_id: str) -> str | None:
    if entity_type == "congress_member":
        member = db.execute(select(Member).where(Member.bioguide_id == entity_id)).scalar_one_or_none()
        if member is not None:
            return " ".join(part for part in [member.first_name, member.last_name] if part) or entity_id
        row = db.execute(
            select(Event.member_name)
            .where(Event.event_type == "congress_trade")
            .where(func.lower(func.coalesce(Event.member_bioguide_id, "")) == entity_id.lower())
            .where(Event.member_name.is_not(None))
            .order_by(Event.ts.desc())
            .limit(1)
        ).first()
        return str(row[0]) if row and row[0] else None

    target_cik = normalize_cik(entity_id)
    rows = db.execute(
        select(Event)
        .where(Event.event_type == "insider_trade")
        .order_by(Event.ts.desc(), Event.id.desc())
        .limit(1000)
    ).scalars().all()
    for event in rows:
        payload = parse_payload(event.payload_json)
        if _event_reporting_cik(payload) != target_cik:
            continue
        for key in ("insider_name", "insiderName", "reportingOwnerName", "ownerName"):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        raw = payload.get("raw") if isinstance(payload.get("raw"), dict) else {}
        for key in ("insiderName", "reportingOwnerName", "ownerName"):
            value = raw.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return None


def _member_display_name(member: Member) -> str:
    name = " ".join(part for part in [member.first_name, member.last_name] if part)
    return name or member.bioguide_id


def _is_legacy_congress_member_id(member_id: str | None) -> bool:
    normalized = (member_id or "").strip()
    return bool(normalized and _LEGACY_CONGRESS_MEMBER_ID_RE.match(normalized))


def _all_congress_member_candidates(db) -> list[dict[str, str | None]]:
    rows = db.execute(
        select(Member)
        .where(Member.bioguide_id.is_not(None))
        .where(Member.bioguide_id != "")
        .order_by(Member.bioguide_id.asc())
    ).scalars().all()
    members_by_id = {(member.bioguide_id or "").strip(): member for member in rows if member.bioguide_id}
    alias_rows = db.execute(
        select(CongressMemberAlias)
        .where(CongressMemberAlias.alias_member_id.is_not(None))
        .where(CongressMemberAlias.alias_member_id != "")
    ).scalars().all()
    aliases_by_id = {(row.alias_member_id or "").strip(): row for row in alias_rows if row.alias_member_id}
    candidates: list[dict[str, str | None]] = []
    seen: set[str] = set()
    for member in rows:
        raw_member_id = (member.bioguide_id or "").strip()
        if not raw_member_id:
            continue
        alias = aliases_by_id.get(raw_member_id)
        alias_member_id = ((alias.authoritative_member_id or alias.group_key or "").strip() if alias else "")
        member_id = FMP_COMMA_FRAGMENT_CANONICAL_MEMBER_IDS.get(raw_member_id) or alias_member_id or raw_member_id
        if _is_legacy_congress_member_id(member_id):
            if _is_legacy_congress_member_id(raw_member_id):
                continue
            member_id = raw_member_id
        resolved_member = members_by_id.get(member_id)
        if raw_member_id in FMP_COMMA_FRAGMENT_CANONICAL_MEMBER_IDS and resolved_member is None:
            continue
        if member_id in seen:
            continue
        seen.add(member_id)
        candidate_name = (
            _member_display_name(resolved_member)
            if resolved_member is not None
            else (alias.member_name if alias and alias.member_name else _member_display_name(member))
        )
        candidates.append(
            {
                "entity_id": member_id,
                "entity_name": candidate_name,
                "chamber": (resolved_member.chamber if resolved_member is not None else (alias.chamber if alias else member.chamber)),
                "party": (resolved_member.party if resolved_member is not None else (alias.party if alias else member.party)),
                "state": (resolved_member.state if resolved_member is not None else (alias.state if alias else member.state)),
            }
        )
    return candidates


def _top_skip_reasons(skips: list, *, limit: int = 6) -> dict[str, int]:
    return dict(list(skip_reason_summary(skips).items())[:limit])


def _normalize_persisted_skip_reason(position: ReplicatedPortfolioPosition) -> str:
    return normalize_skip_reason(SimpleNamespace(reason=position.skip_reason, detail=None))


def _top_skip_reasons_from_positions(positions: list[ReplicatedPortfolioPosition], *, limit: int = 5) -> dict[str, int]:
    counts: dict[str, int] = {}
    for position in positions:
        if position.status != "skipped" or not position.skip_reason:
            continue
        reason = _normalize_persisted_skip_reason(position)
        counts[reason] = counts.get(reason, 0) + 1
    sorted_counts = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    return dict(sorted_counts[:limit])


def _curve_diagnostics_from_run(run: ReplicatedPortfolioRun) -> dict:
    if not run.status_message:
        return {}
    try:
        parsed = json.loads(run.status_message)
    except (TypeError, json.JSONDecodeError):
        return {}
    diagnostics = parsed.get("curve_diagnostics") if isinstance(parsed, dict) else None
    return diagnostics if isinstance(diagnostics, dict) else {}


def _curve_quality_status_from_run(run: ReplicatedPortfolioRun) -> str | None:
    status = _curve_diagnostics_from_run(run).get("curve_quality_status")
    return str(status).strip().lower() if status else None


def _compact_curve_quality_fields_from_run(run: ReplicatedPortfolioRun) -> dict:
    diagnostics = _curve_diagnostics_from_run(run)
    fields: dict[str, object] = {}
    for key in (
        "curve_quality_status",
        "curve_quality_notes",
        "avg_priced_invested_value_pct",
        "pct_invested_value_with_price_gaps",
        "pct_days_with_price_gaps",
    ):
        if key in diagnostics:
            fields[key] = diagnostics[key]
    return fields


def _missing_price_symbol_summary_from_positions(
    positions: list[ReplicatedPortfolioPosition],
    *,
    limit: int | None = 10,
) -> tuple[int, dict[str, int]]:
    counts: dict[str, int] = {}
    for position in positions:
        if position.status != "skipped" or _normalize_persisted_skip_reason(position) != "missing_price":
            continue
        if not position.symbol:
            continue
        counts[str(position.symbol)] = counts.get(str(position.symbol), 0) + 1
    sorted_counts = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    top = dict(sorted_counts if limit is None else sorted_counts[:limit])
    return len(counts), top


def _count_skip(skips: list, reason: str) -> int:
    return sum(1 for skip in skips if normalize_skip_reason(skip) == reason)


def _missing_price_symbol_summary(skips: list, *, limit: int | None = 10) -> tuple[int, dict[str, int]]:
    counts: dict[str, int] = {}
    for skip in skips:
        if normalize_skip_reason(skip) != "missing_price":
            continue
        symbol = getattr(skip, "symbol", None)
        if not symbol:
            continue
        counts[str(symbol)] = counts.get(str(symbol), 0) + 1
    sorted_counts = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    top = dict(sorted_counts if limit is None else sorted_counts[:limit])
    return len(counts), top


def _symbol_coverage_summary(coverage, *, limit: int | None = 10) -> list[dict]:
    rows = []
    sorted_items = sorted(coverage.symbol_points_loaded.items(), key=lambda item: (item[1], item[0]))
    for symbol, points in sorted_items if limit is None else sorted_items[:limit]:
        rows.append(
            {
                "symbol": symbol,
                "points_loaded": points,
                "first_date": coverage.symbol_first_dates.get(symbol),
                "last_date": coverage.symbol_last_dates.get(symbol),
            }
        )
    return rows


def _curve_price_gap_status(simulation) -> dict[str, object]:
    diagnostics = simulation.curve_diagnostics
    return {
        "curve_quality_status": diagnostics.curve_quality_status,
        "avg_priced_invested_value_pct": diagnostics.avg_priced_invested_value_pct,
        "pct_invested_value_with_price_gaps": diagnostics.pct_invested_value_with_price_gaps,
        "suggested_backfill_symbols": list(diagnostics.suggested_backfill_symbols or []),
        "suggested_backfill_start_date": diagnostics.suggested_backfill_start_date,
        "suggested_backfill_end_date": diagnostics.suggested_backfill_end_date,
    }


def _price_preflight_stop_reason(
    simulation,
    *,
    min_avg_priced_invested_value_pct: float,
    max_pct_invested_value_with_price_gaps: float,
) -> str | None:
    diagnostics = simulation.curve_diagnostics
    if diagnostics.curve_quality_status in {"good", "warning"}:
        return f"curve_quality_{diagnostics.curve_quality_status}"
    if float(diagnostics.avg_priced_invested_value_pct or 0.0) >= min_avg_priced_invested_value_pct:
        return "avg_priced_invested_value_threshold_met"
    if float(diagnostics.pct_invested_value_with_price_gaps or 0.0) <= max_pct_invested_value_with_price_gaps:
        return "price_gap_value_threshold_met"
    return None


def _should_price_preflight_attempt(
    simulation,
    *,
    min_avg_priced_invested_value_pct: float,
    max_pct_invested_value_with_price_gaps: float,
) -> bool:
    diagnostics = simulation.curve_diagnostics
    if diagnostics.curve_quality_status != "poor":
        return False
    if not diagnostics.suggested_backfill_symbols:
        return False
    return _price_preflight_stop_reason(
        simulation,
        min_avg_priced_invested_value_pct=min_avg_priced_invested_value_pct,
        max_pct_invested_value_with_price_gaps=max_pct_invested_value_with_price_gaps,
    ) is None


def _missing_execution_price_symbols(simulation, *, limit: int) -> list[str]:
    counts: dict[str, int] = {}
    for skip in getattr(simulation, "skipped", []) or []:
        reason = getattr(skip, "reason", None)
        if reason not in {"missing_price_history", "no_execution_price", "missing_trading_calendar"}:
            continue
        symbol = normalize_symbol(getattr(skip, "symbol", None))
        if not symbol:
            continue
        counts[symbol] = counts.get(symbol, 0) + 1
    ranked = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    return [symbol for symbol, _ in ranked[: max(limit, 0)]]


def _is_share_class_symbol(symbol: str | None) -> bool:
    normalized_symbol = normalize_symbol(symbol)
    if not normalized_symbol:
        return False
    return any(separator in normalized_symbol for separator in (".", "/", "-")) and len(symbol_variants(normalized_symbol)) > 1


def _retryable_missing_execution_price_symbols(simulation, *, limit: int) -> list[str]:
    return [
        symbol
        for symbol in _missing_execution_price_symbols(simulation, limit=limit)
        if _is_share_class_symbol(symbol)
    ][: max(limit, 0)]


def _existing_price_dates(db, *, symbol: str, start_date: str, end_date: str) -> set[str]:
    rows = db.execute(
        select(PriceCache.date)
        .where(PriceCache.symbol == symbol)
        .where(PriceCache.date >= start_date)
        .where(PriceCache.date <= end_date)
    ).all()
    return {str(row[0]) for row in rows}


def _backfill_price_cache_for_preflight(
    db,
    *,
    symbols: list[str],
    start_date: date,
    end_date: date,
    dry_run: bool,
) -> dict:
    start = start_date.isoformat()
    end = end_date.isoformat()
    report_rows: list[dict] = []
    for symbol in symbols:
        normalized_symbol = normalize_symbol(symbol)
        if not normalized_symbol:
            continue
        existing = _existing_price_dates(db, symbol=normalized_symbol, start_date=start, end_date=end)
        provider_map: dict[str, float] = {}
        provider_symbol = None
        failure = None
        try:
            provider_map, provider_symbol = _fetch_provider_eod_close_series(normalized_symbol, start, end)
        except Exception as exc:
            failure = exc.__class__.__name__
            logger.warning("price preflight provider failure symbol=%s error=%s", normalized_symbol, failure)

        provider_dates = set(provider_map.keys())
        missing_provider_dates = sorted(provider_dates - existing)
        inserted_or_updated = 0
        if not dry_run and provider_map:
            cache_symbols = list(dict.fromkeys([normalized_symbol, provider_symbol or normalized_symbol]))
            for day, close in sorted(provider_map.items()):
                for cache_symbol in cache_symbols:
                    if _safe_cache_upsert(db, cache_symbol, day, close):
                        inserted_or_updated += 1
            db.commit()

        report_rows.append(
            {
                "symbol": normalized_symbol,
                "provider_symbol": provider_symbol,
                "start_date": start,
                "end_date": end,
                "dry_run": dry_run,
                "rows_existing": len(existing),
                "rows_provider": len(provider_map),
                "rows_missing": len(missing_provider_dates),
                "rows_inserted_or_updated": inserted_or_updated,
                "first_provider_date": min(provider_dates) if provider_dates else None,
                "last_provider_date": max(provider_dates) if provider_dates else None,
                "failure": failure,
            }
        )
    return {
        "dry_run": dry_run,
        "symbols": symbols,
        "start_date": start,
        "end_date": end,
        "rows": report_rows,
    }


def _date_from_preflight_value(value) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _run_price_preflight(
    db,
    *,
    initial_simulation,
    simulate,
    max_passes: int,
    max_symbols: int,
    min_avg_priced_invested_value_pct: float,
    max_pct_invested_value_with_price_gaps: float,
    allow_backfill_writes: bool,
    dry_run: bool,
) -> tuple[object, dict]:
    initial = _curve_price_gap_status(initial_simulation)
    final_simulation = initial_simulation
    backfilled_symbols: list[str] = []
    suggested_passes: list[dict] = []
    backfill_reports: list[dict] = []
    terminal_symbols: dict[str, str | None] = {}
    terminal_notes: list[str] = []
    passes_attempted = 0

    stop_reason = _price_preflight_stop_reason(
        final_simulation,
        min_avg_priced_invested_value_pct=min_avg_priced_invested_value_pct,
        max_pct_invested_value_with_price_gaps=max_pct_invested_value_with_price_gaps,
    )
    if _retryable_missing_execution_price_symbols(final_simulation, limit=max_symbols):
        stop_reason = None
    if stop_reason is None and not _should_price_preflight_attempt(
        final_simulation,
        min_avg_priced_invested_value_pct=min_avg_priced_invested_value_pct,
        max_pct_invested_value_with_price_gaps=max_pct_invested_value_with_price_gaps,
    ) and not _retryable_missing_execution_price_symbols(final_simulation, limit=max_symbols):
        stop_reason = "not_poor_due_to_value_weighted_price_gaps"

    while stop_reason is None and passes_attempted < max(max_passes, 0):
        diagnostics = final_simulation.curve_diagnostics
        coverage = final_simulation.coverage
        start = _date_from_preflight_value(diagnostics.suggested_backfill_start_date) or (
            coverage.warmup_start_date or coverage.requested_start_date
        )
        end = _date_from_preflight_value(diagnostics.suggested_backfill_end_date) or coverage.requested_end_date
        missing_execution_symbols = _retryable_missing_execution_price_symbols(final_simulation, limit=max_symbols)
        ranked_symbols = list(missing_execution_symbols)
        ranked_symbols.extend(normalize_symbol(symbol) for symbol in diagnostics.suggested_backfill_symbols or [])
        ranked_symbols = [symbol for symbol in ranked_symbols if symbol]
        ranked_symbols = list(dict.fromkeys(ranked_symbols))
        candidate_symbols = [symbol for symbol in ranked_symbols if symbol not in terminal_symbols][: max(max_symbols, 0)]
        suggested_passes.append(
            {
                "pass": passes_attempted + 1,
                "symbols": candidate_symbols,
                "start_date": start.isoformat() if start else None,
                "end_date": end.isoformat() if end else None,
                "would_write_price_cache": allow_backfill_writes,
            }
        )
        if not candidate_symbols:
            stop_reason = "no_retryable_suggested_symbols"
            break
        if start is None or end is None:
            stop_reason = "missing_suggested_backfill_date_range"
            break
        if not allow_backfill_writes:
            stop_reason = "dry_run_no_price_writes" if dry_run else "price_preflight_backfill_not_enabled"
            break

        passes_attempted += 1
        report = _backfill_price_cache_for_preflight(
            db,
            symbols=candidate_symbols,
            start_date=start,
            end_date=end,
            dry_run=False,
        )
        backfill_reports.append(report)
        for row in report.get("rows", []):
            symbol = str(row.get("symbol") or "")
            if symbol and int(row.get("rows_inserted_or_updated") or 0) > 0:
                backfilled_symbols.append(symbol)
            last_provider_date = row.get("last_provider_date")
            rows_provider = int(row.get("rows_provider") or 0)
            if rows_provider == 0:
                terminal_symbols[symbol] = None
                terminal_notes.append(f"{symbol} returned no provider rows for {start.isoformat()}:{end.isoformat()}; skipping retries.")
            elif last_provider_date and str(last_provider_date) < end.isoformat():
                terminal_symbols[symbol] = str(last_provider_date)
                terminal_notes.append(
                    f"{symbol} provider history ended at {last_provider_date} before requested {end.isoformat()}; skipping retries."
                )

        final_simulation = simulate()
        stop_reason = _price_preflight_stop_reason(
            final_simulation,
            min_avg_priced_invested_value_pct=min_avg_priced_invested_value_pct,
            max_pct_invested_value_with_price_gaps=max_pct_invested_value_with_price_gaps,
        )
        if _retryable_missing_execution_price_symbols(final_simulation, limit=max_symbols):
            stop_reason = None
        if stop_reason is None and not _should_price_preflight_attempt(
            final_simulation,
            min_avg_priced_invested_value_pct=min_avg_priced_invested_value_pct,
            max_pct_invested_value_with_price_gaps=max_pct_invested_value_with_price_gaps,
        ) and not _retryable_missing_execution_price_symbols(final_simulation, limit=max_symbols):
            stop_reason = "not_poor_due_to_value_weighted_price_gaps"

    if stop_reason is None:
        stop_reason = "max_passes_reached"

    final = _curve_price_gap_status(final_simulation)
    preflight = {
        "initial_curve_quality_status": initial["curve_quality_status"],
        "final_curve_quality_status": final["curve_quality_status"],
        "initial_avg_priced_invested_value_pct": initial["avg_priced_invested_value_pct"],
        "final_avg_priced_invested_value_pct": final["avg_priced_invested_value_pct"],
        "initial_pct_invested_value_with_price_gaps": initial["pct_invested_value_with_price_gaps"],
        "final_pct_invested_value_with_price_gaps": final["pct_invested_value_with_price_gaps"],
        "preflight_passes_attempted": passes_attempted,
        "preflight_symbols_backfilled": list(dict.fromkeys(backfilled_symbols)),
        "preflight_stopped_reason": stop_reason,
        "preflight_suggested_passes": suggested_passes,
        "preflight_backfill_reports": backfill_reports,
        "preflight_terminal_provider_notes": terminal_notes,
    }
    return final_simulation, preflight


def _curve_quality_fields_from_simulation(simulation, *, include_segments: bool = False) -> dict:
    payload = curve_diagnostics_payload(simulation.curve_diagnostics)
    fields = {
        "flat_segment_count": payload["flat_segment_count"],
        "longest_flat_segment_days": payload["longest_flat_segment_days"],
        "longest_problematic_flat_segment_days": payload["longest_problematic_flat_segment_days"],
        "average_exposure_pct": payload["average_exposure_pct"],
        "min_exposure_pct": payload["min_exposure_pct"],
        "max_exposure_pct": payload["max_exposure_pct"],
        "days_with_zero_exposure": payload["days_with_zero_exposure"],
        "days_with_active_positions_but_zero_exposure": payload["days_with_active_positions_but_zero_exposure"],
        "days_with_active_positions_but_no_valued_positions": payload["days_with_active_positions_but_no_valued_positions"],
        "pct_position_days_with_price_gaps": payload["pct_position_days_with_price_gaps"],
        "pct_invested_value_with_price_gaps": payload["pct_invested_value_with_price_gaps"],
        "avg_priced_invested_value_pct": payload["avg_priced_invested_value_pct"],
        "min_priced_invested_value_pct": payload["min_priced_invested_value_pct"],
        "days_below_90pct_priced_value": payload["days_below_90pct_priced_value"],
        "days_below_75pct_priced_value": payload["days_below_75pct_priced_value"],
        "days_below_50pct_priced_value": payload["days_below_50pct_priced_value"],
        "stale_price_fill_count": payload["stale_price_fill_count"],
        "missing_price_fill_count": payload["missing_price_fill_count"],
        "positions_marked_to_market_count": payload["positions_marked_to_market_count"],
        "positions_using_stale_price_count": payload["positions_using_stale_price_count"],
        "pct_days_with_price_gaps": payload["pct_days_with_price_gaps"],
        "curve_quality_status": payload["curve_quality_status"],
        "curve_quality_notes": payload["curve_quality_notes"][:5],
    }
    if include_segments:
        fields["flat_segments"] = payload["flat_segments"]
        fields["suggested_backfill_symbols"] = payload["suggested_backfill_symbols"]
        fields["suggested_backfill_start_date"] = payload["suggested_backfill_start_date"]
        fields["suggested_backfill_end_date"] = payload["suggested_backfill_end_date"]
        if payload["suggested_backfill_symbols"] and payload["suggested_backfill_start_date"] and payload["suggested_backfill_end_date"]:
            fields["suggested_price_backfill_command"] = (
                "python -m app.backfill_price_cache --symbols "
                + ",".join(payload["suggested_backfill_symbols"][:10])
                + f" --start-date {payload['suggested_backfill_start_date']}"
                + f" --end-date {payload['suggested_backfill_end_date']} --dry-run"
            )
    return fields


def _compact_result(
    *,
    db,
    entity_type: str,
    entity_id: str,
    issuer_cik: str | None,
    issuer_symbol: str | None,
    benchmark_symbol: str,
    start_date,
    end_date,
    simulation,
    events_considered: int,
    events_used: int,
    candidate_diagnostics: dict[str, int | bool] | None = None,
    entity_name_override: str | None = None,
    verbose: bool = False,
) -> dict:
    coverage = simulation.coverage
    summary = simulation.summary
    item_limit = None if verbose else 10
    missing_price_symbols_count, top_missing_price_symbols = _missing_price_symbol_summary(
        simulation.skipped,
        limit=item_limit,
    )
    coverage_limitations = coverage.limitations if verbose else coverage.limitations[:10]
    result = {
        "entity_id": entity_id,
        "entity_name": entity_name_override or _entity_name(db, entity_type=entity_type, entity_id=entity_id),
        "issuer_cik": issuer_cik,
        "issuer_symbol": issuer_symbol,
        "requested_start_date": start_date.isoformat(),
        "requested_end_date": end_date.isoformat(),
        **effective_window_payload(simulation.effective_window),
        "actual_start_date": coverage.actual_start_date.isoformat() if coverage.actual_start_date else None,
        "actual_end_date": coverage.actual_end_date.isoformat() if coverage.actual_end_date else None,
        "points_count": summary.points_count,
        "benchmark_symbol": benchmark_symbol,
        "benchmark_points_loaded": coverage.benchmark_points_loaded,
        "calendar_source": coverage.calendar_source,
        "events_considered": events_considered,
        "events_used": events_used,
        "valid_candidate_events": events_used,
        "invalid_future_date_events": _count_skip(simulation.skipped, "future_transaction_date"),
        "invalid_side_events": _count_skip(simulation.skipped, "missing_transaction_code_or_side")
        + _count_skip(simulation.skipped, "unsupported_side"),
        "positions_count": summary.positions_count,
        "skipped_events_count": summary.skipped_events_count,
        "top_skip_reasons": _top_skip_reasons(simulation.skipped),
        "missing_price_symbols_count": missing_price_symbols_count,
        "top_missing_price_symbols": top_missing_price_symbols,
        "symbol_coverage_summary": _symbol_coverage_summary(coverage, limit=item_limit),
        "total_return_pct": summary.total_return_pct,
        "benchmark_return_pct": summary.benchmark_return_pct,
        "alpha_pct": summary.alpha_pct,
        "coverage_limitations_count": len(coverage.limitations),
        "coverage_limitations": coverage_limitations,
        **_curve_quality_fields_from_simulation(simulation, include_segments=verbose),
    }
    if candidate_diagnostics:
        result.update(candidate_diagnostics)
    return result


def _compact_apply_result(
    *,
    db,
    run_id: int,
    entity_type: str,
    entity_id: str,
    entity_name: str | None,
    issuer_cik: str | None,
    issuer_symbol: str | None,
    lookback_days: int,
    mode: str,
    benchmark_symbol: str,
    start_date,
    end_date,
    simulation,
) -> dict:
    summary = simulation.summary
    coverage = simulation.coverage
    missing_price_symbols_count, top_missing_price_symbols = _missing_price_symbol_summary(
        simulation.skipped,
        limit=10,
    )
    return {
        "run_id": run_id,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "entity_name": entity_name or _entity_name(db, entity_type=entity_type, entity_id=entity_id),
        "issuer_cik": issuer_cik,
        "issuer_symbol": issuer_symbol,
        "lookback_days": lookback_days,
        "mode": mode,
        "benchmark_symbol": benchmark_symbol,
        "persisted_points": summary.points_count,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        **effective_window_payload(simulation.effective_window),
        "total_return_pct": summary.total_return_pct,
        "cagr_pct": summary.cagr_pct,
        "alpha_pct": summary.alpha_pct,
        "benchmark_return_pct": summary.benchmark_return_pct,
        "positions_count": summary.positions_count,
        "skipped_events_count": summary.skipped_events_count,
        "top_skip_reasons": _top_skip_reasons(simulation.skipped),
        "missing_price_symbols_count": missing_price_symbols_count,
        "top_missing_price_symbols": top_missing_price_symbols,
        "coverage_limitations_count": len(coverage.limitations),
        "coverage_limitations": coverage.limitations[:10],
        **_curve_quality_fields_from_simulation(simulation),
    }


def _portfolio_run_lookup_query(
    *,
    entity_type: str,
    entity_id: str,
    lookback_days: int,
    mode: str,
    benchmark_symbol: str,
    issuer_cik: str | None,
    issuer_symbol: str | None,
):
    query = (
        select(ReplicatedPortfolioRun)
        .where(ReplicatedPortfolioRun.entity_type == entity_type)
        .where(ReplicatedPortfolioRun.entity_id == entity_id)
        .where(ReplicatedPortfolioRun.lookback_days == lookback_days)
        .where(ReplicatedPortfolioRun.mode == mode)
        .where(ReplicatedPortfolioRun.benchmark_symbol == benchmark_symbol)
    )
    if issuer_cik:
        return query.where(ReplicatedPortfolioRun.issuer_cik == issuer_cik)
    if issuer_symbol:
        return query.where(ReplicatedPortfolioRun.issuer_symbol == issuer_symbol)
    return query.where(ReplicatedPortfolioRun.issuer_cik.is_(None)).where(ReplicatedPortfolioRun.issuer_symbol.is_(None))


def _latest_portfolio_run(
    db,
    *,
    entity_type: str,
    entity_id: str,
    lookback_days: int,
    mode: str,
    benchmark_symbol: str,
    issuer_cik: str | None,
    issuer_symbol: str | None,
) -> ReplicatedPortfolioRun | None:
    query = _portfolio_run_lookup_query(
        entity_type=entity_type,
        entity_id=entity_id,
        lookback_days=lookback_days,
        mode=mode,
        benchmark_symbol=benchmark_symbol,
        issuer_cik=issuer_cik,
        issuer_symbol=issuer_symbol,
    )
    return db.execute(query.order_by(ReplicatedPortfolioRun.computed_at.desc(), ReplicatedPortfolioRun.id.desc())).scalars().first()


def _matching_portfolio_runs(
    db,
    *,
    entity_type: str,
    entity_id: str,
    lookback_days: int,
    mode: str,
    benchmark_symbol: str,
    issuer_cik: str | None,
    issuer_symbol: str | None,
) -> list[ReplicatedPortfolioRun]:
    query = _portfolio_run_lookup_query(
        entity_type=entity_type,
        entity_id=entity_id,
        lookback_days=lookback_days,
        mode=mode,
        benchmark_symbol=benchmark_symbol,
        issuer_cik=issuer_cik,
        issuer_symbol=issuer_symbol,
    )
    return list(db.execute(query).scalars().all())


def _delete_portfolio_runs(db, runs: list[ReplicatedPortfolioRun]) -> int:
    run_ids = [run.id for run in runs if run.id is not None]
    if not run_ids:
        return 0
    db.execute(delete(ReplicatedPortfolioPoint).where(ReplicatedPortfolioPoint.run_id.in_(run_ids)))
    db.execute(delete(ReplicatedPortfolioPosition).where(ReplicatedPortfolioPosition.run_id.in_(run_ids)))
    db.execute(delete(ReplicatedPortfolioRun).where(ReplicatedPortfolioRun.id.in_(run_ids)))
    return len(run_ids)


def _compact_planned_result_from_simulation(
    *,
    db,
    entity_type: str,
    entity_id: str,
    entity_name: str | None,
    lookback_days: int,
    mode: str,
    status: str,
    simulation,
    run_id: int | None = None,
    include_segments: bool = False,
) -> dict:
    summary = simulation.summary
    missing_price_symbols_count, top_missing_price_symbols = _missing_price_symbol_summary(simulation.skipped, limit=10)
    result = {
        "entity_type": entity_type,
        "entity_id": entity_id,
        "entity_name": entity_name or _entity_name(db, entity_type=entity_type, entity_id=entity_id),
        "lookback_days": lookback_days,
        "mode": mode,
        "status": status,
        "points_count": summary.points_count,
        "total_return_pct": summary.total_return_pct,
        "benchmark_return_pct": summary.benchmark_return_pct,
        "alpha_pct": summary.alpha_pct,
        "positions_count": summary.positions_count,
        "skipped_events_count": summary.skipped_events_count,
        **effective_window_payload(simulation.effective_window),
        "warmup_diagnostics": warmup_diagnostics_payload(simulation.warmup_diagnostics),
        "missing_price_symbols_count": missing_price_symbols_count,
        "top_missing_price_symbols": top_missing_price_symbols,
        "top_skip_reasons": _top_skip_reasons(simulation.skipped, limit=5),
        **_curve_quality_fields_from_simulation(simulation, include_segments=include_segments),
    }
    if run_id is not None:
        result["run_id"] = run_id
        result["persisted_points"] = summary.points_count
    return result


def _compact_planned_result_from_run(
    *,
    db,
    run: ReplicatedPortfolioRun,
    entity_name: str | None,
    status: str,
) -> dict:
    positions = db.execute(
        select(ReplicatedPortfolioPosition).where(ReplicatedPortfolioPosition.run_id == run.id)
    ).scalars().all()
    missing_price_symbols_count, top_missing_price_symbols = _missing_price_symbol_summary_from_positions(positions, limit=10)
    result = {
        "entity_type": run.entity_type,
        "entity_id": run.entity_id,
        "entity_name": entity_name or _entity_name(db, entity_type=run.entity_type, entity_id=run.entity_id),
        "lookback_days": run.lookback_days,
        "mode": run.mode,
        "status": status,
        "run_id": run.id,
        "points_count": run.points_count,
        "persisted_points": run.points_count,
        "total_return_pct": run.total_return_pct,
        "benchmark_return_pct": run.benchmark_return_pct,
        "alpha_pct": run.alpha_pct,
        "positions_count": run.positions_count,
        "skipped_events_count": run.skipped_events_count,
        "warmup_diagnostics": (latest_replicated_portfolio_payload(
            db,
            entity_type=run.entity_type,
            entity_id=run.entity_id,
            lookback_days=run.lookback_days,
            mode=run.mode,
            benchmark=run.benchmark_symbol,
            issuer_cik=run.issuer_cik,
            issuer_symbol=run.issuer_symbol,
        ).get("warmup_diagnostics")),
        "missing_price_symbols_count": missing_price_symbols_count,
        "top_missing_price_symbols": top_missing_price_symbols,
        "top_skip_reasons": _top_skip_reasons_from_positions(positions, limit=5),
    }
    result.update(_compact_curve_quality_fields_from_run(run))
    return result


def _failed_planned_result(
    *,
    db,
    entity_type: str,
    entity_id: str,
    entity_name: str | None,
    lookback_days: int,
    mode: str,
    error: Exception,
    verbose: bool,
    stage: str = "compute",
) -> dict:
    result = {
        "entity_type": entity_type,
        "entity_id": entity_id,
        "entity_name": entity_name or _entity_name(db, entity_type=entity_type, entity_id=entity_id),
        "lookback_days": lookback_days,
        "mode": mode,
        "status": "failed",
        "points_count": 0,
        "total_return_pct": None,
        "benchmark_return_pct": None,
        "alpha_pct": None,
        "positions_count": 0,
        "skipped_events_count": 0,
        "missing_price_symbols_count": 0,
        "top_skip_reasons": {},
        "error": str(error),
        "stage": stage,
    }
    if verbose:
        result["error_type"] = type(error).__name__
    return result


def _status_summary(results: list[dict], *, entities_requested: int, lookbacks_requested: int) -> dict[str, int]:
    counts = {
        "entities_requested": entities_requested,
        "lookbacks_requested": lookbacks_requested,
        "runs_planned": len(results),
        "would_create": 0,
        "created": 0,
        "skipped_existing": 0,
        "failed": 0,
    }
    for result in results:
        status = result.get("status")
        if status in {"would_create", "created", "skipped_existing", "failed"}:
            counts[status] += 1
    return counts


def _weekdays(start_date: date, end_date: date) -> list[date]:
    if end_date < start_date:
        return []
    days = []
    cursor = start_date
    while cursor <= end_date:
        if cursor.weekday() < 5:
            days.append(cursor)
        cursor += timedelta(days=1)
    return days


def _missing_weekday_ranges(*, start_date: date, end_date: date, cached_dates: set[str], limit: int = 8) -> list[dict[str, object]]:
    ranges: list[tuple[date, date, int]] = []
    current_start: date | None = None
    current_end: date | None = None
    current_count = 0
    for day in _weekdays(start_date, end_date):
        if day.isoformat() not in cached_dates:
            if current_start is None:
                current_start = day
            current_end = day
            current_count += 1
            continue
        if current_start is not None and current_end is not None:
            ranges.append((current_start, current_end, current_count))
        current_start = None
        current_end = None
        current_count = 0
    if current_start is not None and current_end is not None:
        ranges.append((current_start, current_end, current_count))
    ranges.sort(key=lambda item: item[2], reverse=True)
    return [
        {"start": start.isoformat(), "end": end.isoformat(), "missing_weekdays": count}
        for start, end, count in ranges[:limit]
    ]


def run_coverage_only(*, benchmark: str, lookback_days: int) -> dict:
    Base.metadata.create_all(bind=engine)
    benchmark_symbol = normalize_symbol(benchmark) or "^GSPC"
    end_date = datetime.now(timezone.utc).date()
    start_date = end_date - timedelta(days=max(lookback_days, 1))
    with SessionLocal() as db:
        all_row = db.execute(
            select(func.min(PriceCache.date), func.max(PriceCache.date), func.count())
            .where(PriceCache.symbol == benchmark_symbol)
        ).first()
        window_rows_raw = db.execute(
            select(PriceCache.date)
            .where(PriceCache.symbol == benchmark_symbol)
            .where(PriceCache.date >= start_date.isoformat())
            .where(PriceCache.date <= end_date.isoformat())
            .order_by(PriceCache.date.asc())
        ).all()
        window_dates = [str(row[0]) for row in window_rows_raw]
        window_row = db.execute(
            select(func.min(PriceCache.date), func.max(PriceCache.date), func.count())
            .where(PriceCache.symbol == benchmark_symbol)
            .where(PriceCache.date >= start_date.isoformat())
            .where(PriceCache.date <= end_date.isoformat())
        ).first()
    expected_weekdays = len(_weekdays(start_date, end_date))
    missing_weekdays_estimate = max(expected_weekdays - len(set(window_dates)), 0)
    return {
        "benchmark_symbol": benchmark_symbol,
        "lookback_days": lookback_days,
        "requested_start_date": start_date.isoformat(),
        "requested_end_date": end_date.isoformat(),
        "cache_first_date": all_row[0] if all_row else None,
        "cache_last_date": all_row[1] if all_row else None,
        "cache_rows_total": int(all_row[2] or 0) if all_row else 0,
        "window_first_date": window_row[0] if window_row else None,
        "window_last_date": window_row[1] if window_row else None,
        "window_rows": int(window_row[2] or 0) if window_row else 0,
        "expected_weekdays": expected_weekdays,
        "expected_trading_days_estimate": expected_weekdays,
        "missing_weekdays_estimate": missing_weekdays_estimate,
        "largest_missing_date_ranges": _missing_weekday_ranges(
            start_date=start_date,
            end_date=end_date,
            cached_dates=set(window_dates),
        ),
        "is_sparse": bool(expected_weekdays and len(set(window_dates)) < expected_weekdays * 0.85),
    }


def run_inspect_events(
    *,
    entity_type: str,
    lookback_days: int,
    limit: int,
    entity_id: str | None = None,
    issuer_cik: str | None = None,
    issuer_symbol: str | None = None,
) -> dict:
    Base.metadata.create_all(bind=engine)
    normalized_entity_type = _normalize_entity_type(entity_type)
    if normalized_entity_type != "insider":
        raise ValueError("--inspect-events currently supports insider mode.")
    normalized_entity_id = normalize_cik(entity_id) if entity_id else None
    normalized_issuer_cik = normalize_cik(issuer_cik)
    normalized_issuer_symbol = normalize_symbol(issuer_symbol)
    cutoff = datetime.now(timezone.utc) - timedelta(days=max(lookback_days, 1) + 14)
    with SessionLocal() as db:
        rows = db.execute(
            select(Event)
            .where(Event.event_type == "insider_trade")
            .where(Event.ts >= cutoff)
            .order_by(Event.ts.desc(), Event.id.desc())
            .limit(max(limit * 50, limit))
        ).scalars().all()
        items = []
        for event in rows:
            payload = parse_payload(event.payload_json)
            reporting_cik = _event_reporting_cik(payload)
            if normalized_entity_id and reporting_cik != normalized_entity_id:
                continue
            inspected = inspect_replicated_portfolio_event(
                event,
                entity_type="insider",
                entity_id=reporting_cik or normalized_entity_id or "",
            )
            if normalized_issuer_cik and inspected.get("issuer_cik") != normalized_issuer_cik:
                continue
            if normalized_issuer_symbol and inspected.get("symbol") != normalized_issuer_symbol:
                continue
            items.append(inspected)
            if len(items) >= limit:
                break
    return {
        "entity_type": normalized_entity_type,
        "entity_id": normalized_entity_id,
        "issuer_cik": normalized_issuer_cik,
        "issuer_symbol": normalized_issuer_symbol,
        "lookback_days": lookback_days,
        "items": items,
    }


def run_compute(
    *,
    entity_type: str,
    lookback_days: int | str | list[int] | tuple[int, ...],
    mode: str,
    limit: int,
    dry_run: bool,
    benchmark: str,
    entity_id: str | None = None,
    entity_ids: str | list[str] | tuple[str, ...] | None = None,
    lookback_set: str | None = None,
    replace_existing: bool = False,
    issuer: str | None = None,
    issuer_cik: str | None = None,
    issuer_symbol: str | None = None,
    summary_only: bool = False,
    verbose: bool = False,
    curve_diagnostics: bool = False,
    debug_date_range: str | None = None,
    candidate_scan_limit: int = 500,
    max_events_per_candidate: int = 100,
    price_preflight: bool = False,
    price_preflight_backfill: bool = False,
    price_preflight_max_passes: int = DEFAULT_PRICE_PREFLIGHT_MAX_PASSES,
    price_preflight_max_symbols: int = DEFAULT_PRICE_PREFLIGHT_MAX_SYMBOLS,
    min_avg_priced_invested_value_pct: float = DEFAULT_MIN_AVG_PRICED_INVESTED_VALUE_PCT,
    max_pct_invested_value_with_price_gaps: float = DEFAULT_MAX_PCT_INVESTED_VALUE_WITH_PRICE_GAPS,
) -> dict:
    Base.metadata.create_all(bind=engine)
    normalized_entity_type = _normalize_entity_type(entity_type)
    if mode not in SUPPORTED_MODES:
        raise ValueError(f"mode must be one of {', '.join(sorted(SUPPORTED_MODES))}")
    if price_preflight and normalized_entity_type != "congress_member":
        raise ValueError("--price-preflight currently supports congress/congress_member only")
    if price_preflight_backfill and not price_preflight:
        raise ValueError("--price-preflight-backfill requires --price-preflight")
    if price_preflight_backfill and dry_run:
        raise ValueError("--price-preflight-backfill requires --apply")
    lookback_values = _resolve_lookback_days(lookback_days=lookback_days, lookback_set=lookback_set)
    debug_range = _parse_debug_date_range(debug_date_range)

    end_date = datetime.now(timezone.utc).date()
    benchmark_symbol = normalize_symbol(benchmark) or "^GSPC"

    with SessionLocal() as db:
        normalized_issuer_cik = normalize_cik(issuer_cik or issuer)
        normalized_issuer_symbol = normalize_symbol(issuer_symbol or (issuer if issuer and not normalized_issuer_cik else None))
        issuer_filter = normalized_issuer_cik or normalized_issuer_symbol
        explicit_entity_ids = _parse_entity_ids(entity_ids, entity_type=normalized_entity_type)
        single_entity_ids = _parse_entity_ids(entity_id, entity_type=normalized_entity_type, split_strings=False)
        if explicit_entity_ids and single_entity_ids:
            raise ValueError("Use either entity_id or entity_ids, not both")
        requested_entity_ids = explicit_entity_ids or single_entity_ids
        if requested_entity_ids:
            selected_entity_ids = requested_entity_ids
            candidate_selection = CandidateSelection(
                entity_ids=[item for item in selected_entity_ids if item],
                candidates_scanned=len([item for item in selected_entity_ids if item]),
                candidates_selected=len([item for item in selected_entity_ids if item]),
            )
        elif normalized_entity_type == "congress_member":
            selected_entity_ids = _candidate_congress_members(db, limit=limit, lookback_days=max(lookback_values))
            candidate_selection = CandidateSelection(
                entity_ids=[item for item in selected_entity_ids if item],
                candidates_scanned=len([item for item in selected_entity_ids if item]),
                candidates_selected=len([item for item in selected_entity_ids if item]),
            )
        else:
            candidate_selection = _candidate_insiders(
                db,
                limit=limit,
                lookback_days=max(lookback_values),
                candidate_scan_limit=candidate_scan_limit,
                max_events_per_candidate=max_events_per_candidate,
                issuer_cik=normalized_issuer_cik,
                issuer_symbol=normalized_issuer_symbol,
            )
            selected_entity_ids = candidate_selection.entity_ids

        results: list[dict] = []
        candidate_diagnostics = candidate_selection.asdict()
        for current_entity_id in [item for item in selected_entity_ids if item]:
            candidate_metrics = candidate_selection.metrics_for(current_entity_id)
            candidate_entity_name = candidate_metrics.get("entity_name") if candidate_metrics else None
            candidate_result_diagnostics = {
                **candidate_diagnostics,
                **{key: value for key, value in candidate_metrics.items() if key != "entity_name"},
            }
            for current_lookback_days in lookback_values:
                start_date = end_date - timedelta(days=max(current_lookback_days, 1))
                existing_run = _latest_portfolio_run(
                    db,
                    entity_type=normalized_entity_type,
                    entity_id=current_entity_id,
                    lookback_days=current_lookback_days,
                    mode=mode,
                    benchmark_symbol=benchmark_symbol,
                    issuer_cik=normalized_issuer_cik,
                    issuer_symbol=normalized_issuer_symbol,
                )
                if existing_run is not None and not replace_existing and not curve_diagnostics and not price_preflight:
                    row = _compact_planned_result_from_run(
                        db=db,
                        run=existing_run,
                        entity_name=candidate_entity_name,
                        status="skipped_existing",
                    )
                    if verbose:
                        row.update(candidate_result_diagnostics)
                    results.append(row)
                    continue

                try:
                    operation_stage = "candidate_load"
                    loaded_events, loader_skips = load_replicated_portfolio_events(
                        db,
                        entity_type=normalized_entity_type,
                        entity_id=current_entity_id,
                        lookback_days=current_lookback_days,
                        issuer=issuer_filter,
                        end_date=end_date,
                        warmup_days=default_warmup_days_for_lookback(current_lookback_days),
                    )
                    operation_stage = "compute"
                    simulation = run_replicated_portfolio_simulation(
                        db,
                        entity_type=normalized_entity_type,
                        entity_id=current_entity_id,
                        lookback_days=current_lookback_days,
                        mode=mode,
                        benchmark=benchmark_symbol,
                        issuer=issuer_filter,
                        end_date=end_date,
                    )
                    preflight_result: dict | None = None
                    if price_preflight:
                        operation_stage = "preflight"
                        def _rerun_simulation():
                            return run_replicated_portfolio_simulation(
                                db,
                                entity_type=normalized_entity_type,
                                entity_id=current_entity_id,
                                lookback_days=current_lookback_days,
                                mode=mode,
                                benchmark=benchmark_symbol,
                                issuer=issuer_filter,
                                end_date=end_date,
                            )

                        simulation, preflight_result = _run_price_preflight(
                            db,
                            initial_simulation=simulation,
                            simulate=_rerun_simulation,
                            max_passes=price_preflight_max_passes,
                            max_symbols=price_preflight_max_symbols,
                            min_avg_priced_invested_value_pct=min_avg_priced_invested_value_pct,
                            max_pct_invested_value_with_price_gaps=max_pct_invested_value_with_price_gaps,
                            allow_backfill_writes=price_preflight_backfill and not dry_run,
                            dry_run=dry_run,
                        )
                    operation_stage = "compute"
                    events_considered = len(loaded_events) + len(loader_skips)
                    events_used = len(loaded_events)
                    status = "would_create" if dry_run else "created"
                    if not verbose:
                        result = _compact_planned_result_from_simulation(
                            db=db,
                            entity_type=normalized_entity_type,
                            entity_id=current_entity_id,
                            entity_name=candidate_entity_name,
                            lookback_days=current_lookback_days,
                            mode=mode,
                            status=status,
                            simulation=simulation,
                            include_segments=curve_diagnostics,
                        )
                        result["events_considered"] = events_considered
                        result["events_used"] = events_used
                        if candidate_result_diagnostics:
                            result.update(candidate_result_diagnostics)
                        if preflight_result:
                            result.update(preflight_result)
                    else:
                        result = {
                            "entity_type": normalized_entity_type,
                            "entity_id": current_entity_id,
                            "entity_name": candidate_entity_name or _entity_name(
                                db,
                                entity_type=normalized_entity_type,
                                entity_id=current_entity_id,
                            ),
                            "issuer_cik": normalized_issuer_cik,
                            "issuer_symbol": normalized_issuer_symbol,
                            "requested_start_date": start_date.isoformat(),
                            "requested_end_date": end_date.isoformat(),
                            "lookback_days": current_lookback_days,
                            "mode": mode,
                            "status": status,
                            "benchmark_symbol": benchmark_symbol,
                            "dry_run": dry_run,
                            "events_considered": events_considered,
                            "events_used": events_used,
                            "valid_candidate_events": events_used,
                            "invalid_future_date_events": _count_skip(simulation.skipped, "future_transaction_date"),
                            "invalid_side_events": _count_skip(simulation.skipped, "missing_transaction_code_or_side")
                            + _count_skip(simulation.skipped, "unsupported_side"),
                            "summary": simulation.summary.__dict__,
                            "warmup_diagnostics": warmup_diagnostics_payload(simulation.warmup_diagnostics),
                            "coverage": asdict(simulation.coverage),
                            "coverage_limitations_count": len(simulation.coverage.limitations),
                            "coverage_limitations": simulation.coverage.limitations,
                            "skip_reason_summary": skip_reason_summary(simulation.skipped),
                            **_curve_quality_fields_from_simulation(simulation, include_segments=curve_diagnostics),
                            **candidate_result_diagnostics,
                        }
                        if preflight_result:
                            result.update(preflight_result)
                        missing_price_symbols_count, top_missing_price_symbols = _missing_price_symbol_summary(simulation.skipped)
                        result["missing_price_symbols_count"] = missing_price_symbols_count
                        result["top_missing_price_symbols"] = top_missing_price_symbols
                        result["symbol_coverage_summary"] = _symbol_coverage_summary(simulation.coverage, limit=None)
                        result["skipped"] = [skip.__dict__ for skip in simulation.skipped[:100]]

                    if debug_range is not None:
                        result["debug_date_range"] = {
                            "start_date": debug_range[0].isoformat(),
                            "end_date": debug_range[1].isoformat(),
                            "limit": 100,
                        }
                        result["daily_curve_diagnostics"] = curve_debug_daily_payload(
                            simulation,
                            start_date=debug_range[0],
                            end_date=debug_range[1],
                            limit=100,
                        )

                    if not dry_run:
                        operation_stage = "persist"
                        if replace_existing:
                            _delete_portfolio_runs(
                                db,
                                _matching_portfolio_runs(
                                    db,
                                    entity_type=normalized_entity_type,
                                    entity_id=current_entity_id,
                                    lookback_days=current_lookback_days,
                                    mode=mode,
                                    benchmark_symbol=benchmark_symbol,
                                    issuer_cik=normalized_issuer_cik,
                                    issuer_symbol=normalized_issuer_symbol,
                                ),
                            )
                        run = persist_replicated_portfolio_run(
                            db,
                            simulation=simulation,
                            entity_type=normalized_entity_type,
                            entity_id=current_entity_id,
                            lookback_days=current_lookback_days,
                            mode=mode,
                            benchmark=benchmark_symbol,
                            issuer_cik=normalized_issuer_cik,
                            issuer_symbol=normalized_issuer_symbol,
                            start_date=start_date,
                            end_date=end_date,
                        )
                        result["run_id"] = run.id
                        result["persisted_points"] = simulation.summary.points_count
                    elif verbose:
                        existing = latest_replicated_portfolio_payload(
                            db,
                            entity_type=normalized_entity_type,
                            entity_id=current_entity_id,
                            lookback_days=current_lookback_days,
                            mode=mode,
                            benchmark=benchmark_symbol,
                            issuer_cik=normalized_issuer_cik,
                            issuer_symbol=normalized_issuer_symbol,
                        )
                        result["existing_run_status"] = existing.get("status")
                    results.append(result)
                except Exception as exc:
                    db.rollback()
                    results.append(
                        _failed_planned_result(
                            db=db,
                            entity_type=normalized_entity_type,
                            entity_id=current_entity_id,
                            entity_name=candidate_entity_name,
                            lookback_days=current_lookback_days,
                            mode=mode,
                            error=exc,
                            verbose=verbose,
                            stage=operation_stage,
                        )
                    )

    summary = _status_summary(
        results,
        entities_requested=len([item for item in selected_entity_ids if item]),
        lookbacks_requested=len(lookback_values),
    )
    return {
        "entity_type": normalized_entity_type,
        "lookback_days": lookback_values[0] if len(lookback_values) == 1 else lookback_values,
        "lookbacks_requested": lookback_values,
        "mode": mode,
        "benchmark_symbol": benchmark_symbol,
        "dry_run": dry_run,
        "replace_existing": replace_existing,
        "price_preflight": price_preflight,
        "price_preflight_backfill": price_preflight_backfill,
        "price_preflight_max_passes": price_preflight_max_passes,
        "price_preflight_max_symbols": price_preflight_max_symbols,
        "min_avg_priced_invested_value_pct": min_avg_priced_invested_value_pct,
        "max_pct_invested_value_with_price_gaps": max_pct_invested_value_with_price_gaps,
        **candidate_diagnostics,
        "summary": summary,
        "results": results,
    }


def _batch_action_for_existing_run(
    existing_run: ReplicatedPortfolioRun | None,
    *,
    replace_existing: bool,
    replace_quality: str | None,
) -> str:
    if existing_run is None:
        return "create"
    if replace_existing:
        return "replace"
    normalized_replace_quality = (replace_quality or "").strip().lower()
    if normalized_replace_quality and _curve_quality_status_from_run(existing_run) == normalized_replace_quality:
        return "replace"
    return "skip_existing"


def _result_quality_status(row: dict) -> str | None:
    status = row.get("final_curve_quality_status") or row.get("curve_quality_status")
    normalized = str(status).strip().lower() if status else None
    return normalized if normalized in {"good", "warning", "poor"} else None


def _result_avg_priced(row: dict) -> float | None:
    value = row.get("final_avg_priced_invested_value_pct", row.get("avg_priced_invested_value_pct"))
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _batch_failure_logs(results: list[dict]) -> list[dict]:
    logs = []
    for row in results:
        if row.get("status") != "failed":
            continue
        logs.append(
            {
                "entity_id": row.get("entity_id"),
                "entity_name": row.get("entity_name"),
                "error": row.get("error"),
                "stage": row.get("stage") or "compute",
            }
        )
    return logs


def _batch_summary(*, entities_planned: int, results: list[dict]) -> dict:
    qualities = [_result_quality_status(row) for row in results]
    avg_priced_values = [value for value in (_result_avg_priced(row) for row in results) if value is not None]
    backfilled_symbols: set[str] = set()
    provider_terminal_notes_count = 0
    for row in results:
        backfilled_symbols.update(str(symbol) for symbol in row.get("preflight_symbols_backfilled") or [] if symbol)
        provider_terminal_notes_count += len(row.get("preflight_terminal_provider_notes") or [])

    summary = {
        "entities_planned": entities_planned,
        "entities_processed": sum(1 for row in results if row.get("status") not in {"skipped_existing", "would_skip_existing"}),
        "would_create": sum(1 for row in results if row.get("status") == "would_create"),
        "would_replace": sum(1 for row in results if row.get("status") == "would_replace"),
        "created": sum(1 for row in results if row.get("status") == "created"),
        "skipped_existing": sum(1 for row in results if row.get("status") in {"skipped_existing", "would_skip_existing"}),
        "replaced": sum(1 for row in results if row.get("status") == "replaced"),
        "failed": sum(1 for row in results if row.get("status") == "failed"),
        "final_good": sum(1 for status in qualities if status == "good"),
        "final_warning": sum(1 for status in qualities if status == "warning"),
        "final_poor": sum(1 for status in qualities if status == "poor"),
        "avg_priced_invested_value_pct": {
            "average": (sum(avg_priced_values) / len(avg_priced_values)) if avg_priced_values else None,
            "median": median(avg_priced_values) if avg_priced_values else None,
        },
        "price_backfill_symbols_count": len(backfilled_symbols),
        "provider_terminal_notes_count": provider_terminal_notes_count,
    }
    return summary


def run_all_congress_portfolio_batch(
    *,
    batch_size: int = 10,
    batch_offset: int = 0,
    max_batches: int | None = None,
    dry_run: bool,
    lookback_days: int = ALL_CONGRESS_DEFAULT_LOOKBACK_DAYS,
    benchmark: str = "^GSPC",
    resume: bool = False,
    quality_target: str = "warning",
    replace_existing: bool = False,
    replace_quality: str | None = None,
    price_preflight_max_passes: int = DEFAULT_ALL_CONGRESS_PRICE_PREFLIGHT_MAX_PASSES,
    price_preflight_max_symbols: int = DEFAULT_PRICE_PREFLIGHT_MAX_SYMBOLS,
    min_avg_priced_invested_value_pct: float = DEFAULT_MIN_AVG_PRICED_INVESTED_VALUE_PCT,
    max_pct_invested_value_with_price_gaps: float = DEFAULT_MAX_PCT_INVESTED_VALUE_WITH_PRICE_GAPS,
    verbose: bool = False,
) -> dict:
    Base.metadata.create_all(bind=engine)
    normalized_quality_target = (quality_target or "warning").strip().lower()
    if normalized_quality_target not in {"good", "warning", "poor"}:
        raise ValueError("--quality-target must be good, warning, or poor")
    normalized_replace_quality = (replace_quality or "").strip().lower() or None
    if normalized_replace_quality and normalized_replace_quality not in {"good", "warning", "poor"}:
        raise ValueError("--replace-quality must be good, warning, or poor")
    if isinstance(lookback_days, (list, tuple)):
        raise ValueError("--all-entities accepts exactly one --lookback-days value")
    normalized_lookback_days = int(lookback_days)
    if normalized_lookback_days not in ALL_CONGRESS_SUPPORTED_LOOKBACK_DAYS:
        supported = ", ".join(str(item) for item in ALL_CONGRESS_SUPPORTED_LOOKBACK_DAYS)
        raise ValueError(f"--all-entities supports only these lookbacks for now: {supported}")

    limit = max(int(batch_size or 1), 1)
    offset = max(int(batch_offset or 0), 0)
    batch_count = max(int(max_batches), 1) if max_batches is not None else 1
    planned_limit = limit * batch_count
    benchmark_symbol = normalize_symbol(benchmark) or "^GSPC"

    with SessionLocal() as db:
        all_candidates = _all_congress_member_candidates(db)
        planned_entities = all_candidates[offset : offset + planned_limit]

    results: list[dict] = []
    for candidate in planned_entities:
        entity_id = str(candidate["entity_id"])
        entity_name = candidate.get("entity_name")
        try:
            with SessionLocal() as db:
                existing_run = _latest_portfolio_run(
                    db,
                    entity_type=ALL_CONGRESS_ENTITY_TYPE,
                    entity_id=entity_id,
                    lookback_days=normalized_lookback_days,
                    mode=ALL_CONGRESS_MODE,
                    benchmark_symbol=benchmark_symbol,
                    issuer_cik=None,
                    issuer_symbol=None,
                )
                action = _batch_action_for_existing_run(
                    existing_run,
                    replace_existing=replace_existing,
                    replace_quality=normalized_replace_quality,
                )
                if action == "skip_existing" and existing_run is not None:
                    row = _compact_planned_result_from_run(
                        db=db,
                        run=existing_run,
                        entity_name=entity_name,
                        status="would_skip_existing" if dry_run else "skipped_existing",
                    )
                    row["planned_action"] = "skip_existing"
                    row["quality_target"] = normalized_quality_target
                    results.append(row)
                    continue

            report = run_compute(
                entity_type="congress",
                entity_ids=[entity_id],
                lookback_days=normalized_lookback_days,
                mode=ALL_CONGRESS_MODE,
                limit=1,
                dry_run=dry_run,
                benchmark=benchmark_symbol,
                replace_existing=(action == "replace"),
                summary_only=True,
                verbose=verbose,
                price_preflight=True,
                price_preflight_backfill=not dry_run,
                price_preflight_max_passes=price_preflight_max_passes,
                price_preflight_max_symbols=price_preflight_max_symbols,
                min_avg_priced_invested_value_pct=min_avg_priced_invested_value_pct,
                max_pct_invested_value_with_price_gaps=max_pct_invested_value_with_price_gaps,
            )
            row = (report.get("results") or [{}])[0]
            row["planned_action"] = action
            row["quality_target"] = normalized_quality_target
            if action == "replace":
                if row.get("status") == "would_create":
                    row["status"] = "would_replace"
                elif row.get("status") == "created":
                    row["status"] = "replaced"
            results.append(row)
        except Exception as exc:
            results.append(
                {
                    "entity_type": ALL_CONGRESS_ENTITY_TYPE,
                    "entity_id": entity_id,
                    "entity_name": entity_name,
                    "lookback_days": normalized_lookback_days,
                    "mode": ALL_CONGRESS_MODE,
                    "status": "failed",
                    "planned_action": "unknown",
                    "quality_target": normalized_quality_target,
                    "error": str(exc),
                    "stage": "candidate_load",
                }
            )

    summary = _batch_summary(entities_planned=len(planned_entities), results=results)
    return {
        "entity_type": ALL_CONGRESS_ENTITY_TYPE,
        "lookback_days": normalized_lookback_days,
        "mode": ALL_CONGRESS_MODE,
        "benchmark_symbol": benchmark_symbol,
        "dry_run": dry_run,
        "resume": resume,
        "batch_size": limit,
        "batch_offset": offset,
        "max_batches": max_batches,
        "quality_target": normalized_quality_target,
        "replace_existing": replace_existing,
        "replace_quality": normalized_replace_quality,
        "price_preflight": True,
        "price_preflight_backfill": not dry_run,
        "price_preflight_max_passes": price_preflight_max_passes,
        "summary_only": True,
        "summary": summary,
        "failure_logs": _batch_failure_logs(results),
        "results": results,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Compute persisted replicated portfolio simulations.")
    parser.add_argument("--entity-type", help="congress, congress_member, or insider")
    parser.add_argument("--entity-id", help="Optional single member bioguide ID or insider reporting CIK")
    parser.add_argument("--entity-ids", help="Optional comma-separated member bioguide IDs or insider reporting CIKs")
    parser.add_argument("--all-entities", action="store_true", help="Compute a safe all-Congress Portfolio Mode batch.")
    parser.add_argument("--batch-size", type=int, help="All-Congress batch size. Defaults to 75 for 30D/90D/180D, 10 for 365D, and 25 for 1095D.")
    parser.add_argument("--batch-offset", type=int, default=0, help="All-Congress entity offset.")
    parser.add_argument("--max-batches", type=int, help="Optional number of all-Congress batches to process from the offset.")
    parser.add_argument("--resume", action="store_true", help="Resume all-Congress batches by skipping existing runs.")
    parser.add_argument("--quality-target", default="warning", choices=["good", "warning", "poor"], help="Target curve quality for batch reporting.")
    parser.add_argument("--replace-quality", choices=["good", "warning", "poor"], help="Replace only existing runs with this curve quality.")
    parser.add_argument("--issuer", help="Optional insider issuer CIK or symbol scope")
    parser.add_argument("--issuer-cik", help="Optional insider issuer CIK scope")
    parser.add_argument("--issuer-symbol", help="Optional insider issuer symbol scope")
    parser.add_argument("--lookback-days", help="Single lookback or comma-separated lookbacks")
    parser.add_argument("--lookback-set", choices=["standard"], help="Named lookback set. standard expands to 30,90,180,365,1095.")
    parser.add_argument("--mode", default="realistic_disclosure_lag", choices=sorted(SUPPORTED_MODES))
    parser.add_argument("--benchmark", default="^GSPC")
    parser.add_argument("--limit", type=int, default=5)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--replace-existing", action="store_true", help="Replace matching persisted runs instead of skipping them.")
    parser.add_argument("--summary-only", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--candidate-scan-limit", type=int, default=500)
    parser.add_argument("--max-events-per-candidate", type=int, default=100)
    parser.add_argument("--inspect-events", action="store_true")
    parser.add_argument("--coverage-only", action="store_true")
    parser.add_argument("--show-gaps", action="store_true", help="Include benchmark cache gap diagnostics with --coverage-only.")
    parser.add_argument("--curve-diagnostics", action="store_true", help="Include flat-segment and price-gap diagnostics for computed curves.")
    parser.add_argument("--debug-date-range", help="Optional YYYY-MM-DD:YYYY-MM-DD range for capped daily value-weighted curve diagnostics.")
    parser.add_argument("--price-preflight", action="store_true", help="Run congress price-coverage preflight before portfolio persistence.")
    parser.add_argument("--price-preflight-backfill", action="store_true", help="Allow price preflight to write price_cache rows. Requires --apply.")
    parser.add_argument("--price-preflight-max-passes", type=int)
    parser.add_argument("--price-preflight-max-symbols", type=int, default=DEFAULT_PRICE_PREFLIGHT_MAX_SYMBOLS)
    parser.add_argument("--min-avg-priced-invested-value-pct", type=float, default=DEFAULT_MIN_AVG_PRICED_INVESTED_VALUE_PCT)
    parser.add_argument("--max-pct-invested-value-with-price-gaps", type=float, default=DEFAULT_MAX_PCT_INVESTED_VALUE_WITH_PRICE_GAPS)
    args = parser.parse_args()
    if args.all_entities and args.lookback_days is None and not args.lookback_set:
        lookback_values = [ALL_CONGRESS_DEFAULT_LOOKBACK_DAYS]
    else:
        lookback_values = _resolve_lookback_days(lookback_days=args.lookback_days, lookback_set=args.lookback_set)

    if args.coverage_only:
        if len(lookback_values) != 1:
            raise SystemExit("--coverage-only accepts a single --lookback-days value.")
        print(json.dumps(run_coverage_only(benchmark=args.benchmark, lookback_days=lookback_values[0]), indent=2, sort_keys=True, default=str))
        return

    if args.all_entities:
        if args.entity_type and _normalize_entity_type(args.entity_type) != "congress_member":
            raise SystemExit("--all-entities supports congress/congress_member only.")
        if args.entity_id or args.entity_ids:
            raise SystemExit("--all-entities cannot be combined with --entity-id or --entity-ids.")
        if args.issuer or args.issuer_cik or args.issuer_symbol:
            raise SystemExit("--all-entities does not support insider issuer filters.")
        if args.lookback_set:
            raise SystemExit("--all-entities accepts one explicit --lookback-days value, not --lookback-set.")
        if len(lookback_values) != 1:
            raise SystemExit("--all-entities accepts exactly one --lookback-days value.")
        all_entities_lookback_days = lookback_values[0]
        if all_entities_lookback_days not in ALL_CONGRESS_SUPPORTED_LOOKBACK_DAYS:
            supported = ", ".join(str(item) for item in ALL_CONGRESS_SUPPORTED_LOOKBACK_DAYS)
            raise SystemExit(f"--all-entities supports only these lookbacks for now: {supported}.")
        if args.mode != ALL_CONGRESS_MODE:
            raise SystemExit(f"--all-entities supports only --mode {ALL_CONGRESS_MODE}.")
        if args.dry_run and args.apply:
            raise SystemExit("Choose only one of --dry-run or --apply.")
        if not args.dry_run and not args.apply:
            raise SystemExit("Pass --dry-run to preview or --apply to persist a run.")
        if args.price_preflight_backfill and not args.apply:
            raise SystemExit("--price-preflight-backfill requires --apply.")
        logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
        all_entities_batch_size = (
            args.batch_size
            if args.batch_size is not None
            else ALL_CONGRESS_DEFAULT_BATCH_SIZE_BY_LOOKBACK[all_entities_lookback_days]
        )
        report = run_all_congress_portfolio_batch(
            batch_size=all_entities_batch_size,
            batch_offset=args.batch_offset,
            max_batches=args.max_batches,
            dry_run=args.dry_run,
            lookback_days=all_entities_lookback_days,
            benchmark=args.benchmark,
            resume=args.resume,
            quality_target=args.quality_target,
            replace_existing=args.replace_existing,
            replace_quality=args.replace_quality,
            price_preflight_max_passes=args.price_preflight_max_passes
            if args.price_preflight_max_passes is not None
            else DEFAULT_ALL_CONGRESS_PRICE_PREFLIGHT_MAX_PASSES,
            price_preflight_max_symbols=args.price_preflight_max_symbols,
            min_avg_priced_invested_value_pct=args.min_avg_priced_invested_value_pct,
            max_pct_invested_value_with_price_gaps=args.max_pct_invested_value_with_price_gaps,
            verbose=args.verbose,
        )
        print(json.dumps(report, indent=2, sort_keys=True, default=str))
        return

    if not args.entity_type:
        raise SystemExit("--entity-type is required unless --coverage-only is used.")

    if args.inspect_events:
        if len(lookback_values) != 1:
            raise SystemExit("--inspect-events accepts a single --lookback-days value.")
        report = run_inspect_events(
            entity_type=args.entity_type,
            entity_id=args.entity_id,
            issuer_cik=args.issuer_cik,
            issuer_symbol=args.issuer_symbol,
            lookback_days=lookback_values[0],
            limit=max(args.limit, 1),
        )
        print(json.dumps(report, indent=2, sort_keys=True, default=str))
        return

    if (args.curve_diagnostics or args.debug_date_range) and not args.dry_run and not args.apply:
        args.dry_run = True
    if not args.dry_run and not args.apply:
        raise SystemExit("Pass --dry-run to preview or --apply to persist a run.")
    if args.dry_run and args.apply:
        raise SystemExit("Choose only one of --dry-run or --apply.")
    if args.price_preflight and _normalize_entity_type(args.entity_type) != "congress_member":
        raise SystemExit("--price-preflight currently supports congress/congress_member only.")
    if args.price_preflight_backfill and not args.price_preflight:
        raise SystemExit("--price-preflight-backfill requires --price-preflight.")
    if args.price_preflight_backfill and not args.apply:
        raise SystemExit("--price-preflight-backfill requires --apply.")

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    report = run_compute(
        entity_type=args.entity_type,
        entity_id=args.entity_id,
        entity_ids=args.entity_ids,
        issuer=args.issuer,
        lookback_days=lookback_values,
        lookback_set=None,
        replace_existing=args.replace_existing,
        mode=args.mode,
        limit=max(args.limit, 1),
        dry_run=args.dry_run,
        benchmark=args.benchmark,
        issuer_cik=args.issuer_cik,
        issuer_symbol=args.issuer_symbol,
        summary_only=args.summary_only,
        verbose=args.verbose,
        curve_diagnostics=args.curve_diagnostics,
        debug_date_range=args.debug_date_range,
        candidate_scan_limit=args.candidate_scan_limit,
        max_events_per_candidate=args.max_events_per_candidate,
        price_preflight=args.price_preflight,
        price_preflight_backfill=args.price_preflight_backfill,
        price_preflight_max_passes=args.price_preflight_max_passes
        if args.price_preflight_max_passes is not None
        else DEFAULT_PRICE_PREFLIGHT_MAX_PASSES,
        price_preflight_max_symbols=args.price_preflight_max_symbols,
        min_avg_priced_invested_value_pct=args.min_avg_priced_invested_value_pct,
        max_pct_invested_value_with_price_gaps=args.max_pct_invested_value_with_price_gaps,
    )
    print(json.dumps(report, indent=2, sort_keys=True, default=str))


if __name__ == "__main__":
    main()
