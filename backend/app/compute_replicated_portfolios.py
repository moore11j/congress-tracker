from __future__ import annotations

import argparse
import json
import logging
import re
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace

from sqlalchemy import delete, func, or_, select

from app.db import Base, SessionLocal, engine
from app.models import Event, Member, PriceCache, ReplicatedPortfolioPoint, ReplicatedPortfolioPosition, ReplicatedPortfolioRun
from app.services.backtesting.queries import parse_payload
from app.services.replicated_portfolios import (
    SUPPORTED_MODES,
    curve_diagnostics_payload,
    inspect_replicated_portfolio_event,
    latest_replicated_portfolio_payload,
    load_replicated_portfolio_events,
    normalize_skip_reason,
    persist_replicated_portfolio_run,
    run_replicated_portfolio_simulation,
    skip_reason_summary,
)
from app.services.ticker_meta import normalize_cik
from app.utils.symbols import normalize_symbol

logger = logging.getLogger(__name__)
STANDARD_LOOKBACK_DAYS = [30, 90, 180, 365, 1095]
_REPORTING_CIK_TEXT_RE = re.compile(
    r'"(?:reporting_cik|reportingCik|reportingCIK|rptOwnerCik)"\s*:\s*"?(\d+)"?',
    re.IGNORECASE,
)


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


def _parse_entity_ids(value: str | list[str] | tuple[str, ...] | None, *, entity_type: str) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        raw_items = value.split(",")
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


def _curve_quality_fields_from_simulation(simulation, *, include_segments: bool = False) -> dict:
    payload = curve_diagnostics_payload(simulation.curve_diagnostics)
    fields = {
        "flat_segment_count": payload["flat_segment_count"],
        "longest_flat_segment_days": payload["longest_flat_segment_days"],
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
    return {
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
        "missing_price_symbols_count": missing_price_symbols_count,
        "top_missing_price_symbols": top_missing_price_symbols,
        "top_skip_reasons": _top_skip_reasons_from_positions(positions, limit=5),
    }


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
    candidate_scan_limit: int = 500,
    max_events_per_candidate: int = 100,
) -> dict:
    Base.metadata.create_all(bind=engine)
    normalized_entity_type = _normalize_entity_type(entity_type)
    if mode not in SUPPORTED_MODES:
        raise ValueError(f"mode must be one of {', '.join(sorted(SUPPORTED_MODES))}")
    lookback_values = _resolve_lookback_days(lookback_days=lookback_days, lookback_set=lookback_set)

    end_date = datetime.now(timezone.utc).date()
    benchmark_symbol = normalize_symbol(benchmark) or "^GSPC"

    with SessionLocal() as db:
        normalized_issuer_cik = normalize_cik(issuer_cik or issuer)
        normalized_issuer_symbol = normalize_symbol(issuer_symbol or (issuer if issuer and not normalized_issuer_cik else None))
        issuer_filter = normalized_issuer_cik or normalized_issuer_symbol
        explicit_entity_ids = _parse_entity_ids(entity_ids, entity_type=normalized_entity_type)
        single_entity_ids = _parse_entity_ids(entity_id, entity_type=normalized_entity_type)
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
                if existing_run is not None and not replace_existing and not curve_diagnostics:
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
                    loaded_events, loader_skips = load_replicated_portfolio_events(
                        db,
                        entity_type=normalized_entity_type,
                        entity_id=current_entity_id,
                        lookback_days=current_lookback_days,
                        issuer=issuer_filter,
                        end_date=end_date,
                    )
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
                            "coverage": asdict(simulation.coverage),
                            "coverage_limitations_count": len(simulation.coverage.limitations),
                            "coverage_limitations": simulation.coverage.limitations,
                            "skip_reason_summary": skip_reason_summary(simulation.skipped),
                            **_curve_quality_fields_from_simulation(simulation, include_segments=curve_diagnostics),
                            **candidate_result_diagnostics,
                        }
                        missing_price_symbols_count, top_missing_price_symbols = _missing_price_symbol_summary(simulation.skipped)
                        result["missing_price_symbols_count"] = missing_price_symbols_count
                        result["top_missing_price_symbols"] = top_missing_price_symbols
                        result["symbol_coverage_summary"] = _symbol_coverage_summary(simulation.coverage, limit=None)
                        result["skipped"] = [skip.__dict__ for skip in simulation.skipped[:100]]

                    if not dry_run:
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
        **candidate_diagnostics,
        "summary": summary,
        "results": results,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Compute persisted replicated portfolio simulations.")
    parser.add_argument("--entity-type", help="congress, congress_member, or insider")
    parser.add_argument("--entity-id", help="Optional single member bioguide ID or insider reporting CIK")
    parser.add_argument("--entity-ids", help="Optional comma-separated member bioguide IDs or insider reporting CIKs")
    parser.add_argument("--issuer", help="Optional insider issuer CIK or symbol scope")
    parser.add_argument("--issuer-cik", help="Optional insider issuer CIK scope")
    parser.add_argument("--issuer-symbol", help="Optional insider issuer symbol scope")
    parser.add_argument("--lookback-days", default="1095", help="Single lookback or comma-separated lookbacks")
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
    args = parser.parse_args()
    lookback_values = _resolve_lookback_days(lookback_days=args.lookback_days, lookback_set=args.lookback_set)

    if args.coverage_only:
        if len(lookback_values) != 1:
            raise SystemExit("--coverage-only accepts a single --lookback-days value.")
        print(json.dumps(run_coverage_only(benchmark=args.benchmark, lookback_days=lookback_values[0]), indent=2, sort_keys=True, default=str))
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

    if args.curve_diagnostics and not args.dry_run and not args.apply:
        args.dry_run = True
    if not args.dry_run and not args.apply:
        raise SystemExit("Pass --dry-run to preview or --apply to persist a run.")
    if args.dry_run and args.apply:
        raise SystemExit("Choose only one of --dry-run or --apply.")

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
        candidate_scan_limit=args.candidate_scan_limit,
        max_events_per_candidate=args.max_events_per_candidate,
    )
    print(json.dumps(report, indent=2, sort_keys=True, default=str))


if __name__ == "__main__":
    main()
