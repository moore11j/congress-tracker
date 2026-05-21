from __future__ import annotations

import argparse
import json
import logging
from dataclasses import asdict
from datetime import date, datetime, timedelta, timezone

from sqlalchemy import func, select

from app.db import Base, SessionLocal, engine
from app.models import Event, Member, PriceCache
from app.services.backtesting.queries import parse_payload
from app.services.replicated_portfolios import (
    SUPPORTED_MODES,
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


def _event_reporting_cik(payload: dict) -> str | None:
    raw = payload.get("raw") if isinstance(payload.get("raw"), dict) else {}
    for source in (payload, raw):
        for key in ("reporting_cik", "reportingCik", "reportingCIK", "rptOwnerCik"):
            value = source.get(key)
            if isinstance(value, str) and value.strip():
                return normalize_cik(value)
    return None


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


def _candidate_insiders(db, *, limit: int, lookback_days: int) -> list[str]:
    cutoff = datetime.now(timezone.utc) - timedelta(days=max(lookback_days, 1) + 14)
    rows = db.execute(
        select(Event)
        .where(Event.event_type == "insider_trade")
        .where(Event.ts >= cutoff)
        .order_by(Event.ts.desc(), Event.id.desc())
        .limit(max(limit * 100, limit))
    ).scalars().all()
    seen: set[str] = set()
    out: list[str] = []
    for event in rows:
        cik = _event_reporting_cik(parse_payload(event.payload_json))
        if not cik or cik in seen:
            continue
        portfolio_events, _ = load_replicated_portfolio_events(
            db,
            entity_type="insider",
            entity_id=cik,
            lookback_days=lookback_days,
        )
        if not portfolio_events:
            continue
        out.append(cik)
        seen.add(cik)
        if len(out) >= limit:
            break
    return out


def _normalize_entity_type(value: str) -> str:
    normalized = (value or "").strip().lower()
    if normalized == "congress":
        return "congress_member"
    if normalized in {"congress_member", "insider"}:
        return normalized
    raise ValueError("entity-type must be congress, congress_member, or insider")


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


def _count_skip(skips: list, reason: str) -> int:
    return sum(1 for skip in skips if normalize_skip_reason(skip) == reason)


def _missing_price_symbol_summary(skips: list, *, limit: int = 10) -> tuple[int, dict[str, int]]:
    counts: dict[str, int] = {}
    for skip in skips:
        if normalize_skip_reason(skip) != "missing_price":
            continue
        symbol = getattr(skip, "symbol", None)
        if not symbol:
            continue
        counts[str(symbol)] = counts.get(str(symbol), 0) + 1
    top = dict(sorted(counts.items(), key=lambda item: (-item[1], item[0]))[:limit])
    return len(counts), top


def _symbol_coverage_summary(coverage, *, limit: int = 10) -> list[dict]:
    rows = []
    for symbol, points in sorted(coverage.symbol_points_loaded.items(), key=lambda item: (item[1], item[0]))[:limit]:
        rows.append(
            {
                "symbol": symbol,
                "points_loaded": points,
                "first_date": coverage.symbol_first_dates.get(symbol),
                "last_date": coverage.symbol_last_dates.get(symbol),
            }
        )
    return rows


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
) -> dict:
    coverage = simulation.coverage
    summary = simulation.summary
    missing_price_symbols_count, top_missing_price_symbols = _missing_price_symbol_summary(simulation.skipped)
    return {
        "entity_id": entity_id,
        "entity_name": _entity_name(db, entity_type=entity_type, entity_id=entity_id),
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
        "symbol_coverage_summary": _symbol_coverage_summary(coverage),
        "total_return_pct": summary.total_return_pct,
        "benchmark_return_pct": summary.benchmark_return_pct,
        "alpha_pct": summary.alpha_pct,
        "coverage_limitations": coverage.limitations,
    }


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
    lookback_days: int,
    mode: str,
    limit: int,
    dry_run: bool,
    benchmark: str,
    entity_id: str | None = None,
    issuer: str | None = None,
    issuer_cik: str | None = None,
    issuer_symbol: str | None = None,
    summary_only: bool = False,
    verbose: bool = False,
) -> dict:
    Base.metadata.create_all(bind=engine)
    normalized_entity_type = _normalize_entity_type(entity_type)
    if mode not in SUPPORTED_MODES:
        raise ValueError(f"mode must be one of {', '.join(sorted(SUPPORTED_MODES))}")

    end_date = datetime.now(timezone.utc).date()
    start_date = end_date - timedelta(days=max(lookback_days, 1))
    benchmark_symbol = normalize_symbol(benchmark) or "^GSPC"

    with SessionLocal() as db:
        if entity_id:
            entity_ids = [normalize_cik(entity_id) if normalized_entity_type == "insider" else entity_id]
        elif normalized_entity_type == "congress_member":
            entity_ids = _candidate_congress_members(db, limit=limit, lookback_days=lookback_days)
        else:
            entity_ids = _candidate_insiders(db, limit=limit, lookback_days=lookback_days)

        results: list[dict] = []
        normalized_issuer_cik = normalize_cik(issuer_cik or issuer)
        normalized_issuer_symbol = normalize_symbol(issuer_symbol or (issuer if issuer and not normalized_issuer_cik else None))
        issuer_filter = normalized_issuer_cik or normalized_issuer_symbol
        for current_entity_id in [item for item in entity_ids if item]:
            loaded_events, loader_skips = load_replicated_portfolio_events(
                db,
                entity_type=normalized_entity_type,
                entity_id=current_entity_id,
                lookback_days=lookback_days,
                issuer=issuer_filter,
                end_date=end_date,
            )
            simulation = run_replicated_portfolio_simulation(
                db,
                entity_type=normalized_entity_type,
                entity_id=current_entity_id,
                lookback_days=lookback_days,
                mode=mode,
                benchmark=benchmark_symbol,
                issuer=issuer_filter,
                end_date=end_date,
            )
            events_considered = len(loaded_events) + len(loader_skips)
            events_used = len(loaded_events)
            if summary_only:
                results.append(
                    _compact_result(
                        db=db,
                        entity_type=normalized_entity_type,
                        entity_id=current_entity_id,
                        issuer_cik=normalized_issuer_cik,
                        issuer_symbol=normalized_issuer_symbol,
                        benchmark_symbol=benchmark_symbol,
                        start_date=start_date,
                        end_date=end_date,
                        simulation=simulation,
                        events_considered=events_considered,
                        events_used=events_used,
                    )
                )
                continue
            result = {
                "entity_type": normalized_entity_type,
                "entity_id": current_entity_id,
                "entity_name": _entity_name(db, entity_type=normalized_entity_type, entity_id=current_entity_id),
                "issuer_cik": normalized_issuer_cik,
                "issuer_symbol": normalized_issuer_symbol,
                "requested_start_date": start_date.isoformat(),
                "requested_end_date": end_date.isoformat(),
                "lookback_days": lookback_days,
                "mode": mode,
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
                "skip_reason_summary": skip_reason_summary(simulation.skipped),
            }
            missing_price_symbols_count, top_missing_price_symbols = _missing_price_symbol_summary(simulation.skipped)
            result["missing_price_symbols_count"] = missing_price_symbols_count
            result["top_missing_price_symbols"] = top_missing_price_symbols
            result["symbol_coverage_summary"] = _symbol_coverage_summary(simulation.coverage)
            if verbose:
                result["skipped"] = [skip.__dict__ for skip in simulation.skipped[:100]]
            if not dry_run:
                run = persist_replicated_portfolio_run(
                    db,
                    simulation=simulation,
                    entity_type=normalized_entity_type,
                    entity_id=current_entity_id,
                    lookback_days=lookback_days,
                    mode=mode,
                    benchmark=benchmark_symbol,
                    issuer_cik=normalized_issuer_cik,
                    issuer_symbol=normalized_issuer_symbol,
                    start_date=start_date,
                    end_date=end_date,
                )
                result["run_id"] = run.id
                result["persisted_points"] = simulation.summary.points_count
            else:
                existing = latest_replicated_portfolio_payload(
                    db,
                    entity_type=normalized_entity_type,
                    entity_id=current_entity_id,
                    lookback_days=lookback_days,
                    mode=mode,
                    benchmark=benchmark_symbol,
                    issuer_cik=normalized_issuer_cik,
                    issuer_symbol=normalized_issuer_symbol,
                )
                result["existing_run_status"] = existing.get("status")
            results.append(result)

    return {
        "entity_type": normalized_entity_type,
        "lookback_days": lookback_days,
        "mode": mode,
        "benchmark_symbol": benchmark_symbol,
        "dry_run": dry_run,
        "results": results,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Compute persisted replicated portfolio simulations.")
    parser.add_argument("--entity-type", help="congress, congress_member, or insider")
    parser.add_argument("--entity-id", help="Optional single member bioguide ID or insider reporting CIK")
    parser.add_argument("--issuer", help="Optional insider issuer CIK or symbol scope")
    parser.add_argument("--issuer-cik", help="Optional insider issuer CIK scope")
    parser.add_argument("--issuer-symbol", help="Optional insider issuer symbol scope")
    parser.add_argument("--lookback-days", type=int, default=1095)
    parser.add_argument("--mode", default="realistic_disclosure_lag", choices=sorted(SUPPORTED_MODES))
    parser.add_argument("--benchmark", default="^GSPC")
    parser.add_argument("--limit", type=int, default=5)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--summary-only", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--inspect-events", action="store_true")
    parser.add_argument("--coverage-only", action="store_true")
    parser.add_argument("--show-gaps", action="store_true", help="Include benchmark cache gap diagnostics with --coverage-only.")
    args = parser.parse_args()

    if args.coverage_only:
        print(json.dumps(run_coverage_only(benchmark=args.benchmark, lookback_days=args.lookback_days), indent=2, sort_keys=True, default=str))
        return

    if not args.entity_type:
        raise SystemExit("--entity-type is required unless --coverage-only is used.")

    if args.inspect_events:
        report = run_inspect_events(
            entity_type=args.entity_type,
            entity_id=args.entity_id,
            issuer_cik=args.issuer_cik,
            issuer_symbol=args.issuer_symbol,
            lookback_days=args.lookback_days,
            limit=max(args.limit, 1),
        )
        print(json.dumps(report, indent=2, sort_keys=True, default=str))
        return

    if not args.dry_run and not args.apply:
        raise SystemExit("Pass --dry-run to preview or --apply to persist a run.")
    if args.dry_run and args.apply:
        raise SystemExit("Choose only one of --dry-run or --apply.")

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    report = run_compute(
        entity_type=args.entity_type,
        entity_id=args.entity_id,
        issuer=args.issuer,
        lookback_days=args.lookback_days,
        mode=args.mode,
        limit=max(args.limit, 1),
        dry_run=args.dry_run,
        benchmark=args.benchmark,
        issuer_cik=args.issuer_cik,
        issuer_symbol=args.issuer_symbol,
        summary_only=args.summary_only,
        verbose=args.verbose,
    )
    print(json.dumps(report, indent=2, sort_keys=True, default=str))


if __name__ == "__main__":
    main()
