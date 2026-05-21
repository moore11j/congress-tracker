from __future__ import annotations

import argparse
import json
import logging
from dataclasses import asdict
from datetime import datetime, timedelta, timezone

from sqlalchemy import func, select

from app.db import Base, SessionLocal, engine
from app.models import Event
from app.services.backtesting.queries import parse_payload
from app.services.replicated_portfolios import (
    SUPPORTED_MODES,
    latest_replicated_portfolio_payload,
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
        issuer_cik = normalize_cik(issuer)
        issuer_symbol = normalize_symbol(issuer) if issuer and not issuer_cik else None
        for current_entity_id in [item for item in entity_ids if item]:
            simulation = run_replicated_portfolio_simulation(
                db,
                entity_type=normalized_entity_type,
                entity_id=current_entity_id,
                lookback_days=lookback_days,
                mode=mode,
                benchmark=benchmark_symbol,
                issuer=issuer,
                end_date=end_date,
            )
            result = {
                "entity_type": normalized_entity_type,
                "entity_id": current_entity_id,
                "issuer_cik": issuer_cik,
                "issuer_symbol": issuer_symbol,
                "requested_start_date": start_date.isoformat(),
                "requested_end_date": end_date.isoformat(),
                "lookback_days": lookback_days,
                "mode": mode,
                "benchmark_symbol": benchmark_symbol,
                "dry_run": dry_run,
                "summary": simulation.summary.__dict__,
                "coverage": asdict(simulation.coverage),
                "skip_reason_summary": skip_reason_summary(simulation.skipped),
                "skipped": [skip.__dict__ for skip in simulation.skipped[:20]],
            }
            if not dry_run:
                run = persist_replicated_portfolio_run(
                    db,
                    simulation=simulation,
                    entity_type=normalized_entity_type,
                    entity_id=current_entity_id,
                    lookback_days=lookback_days,
                    mode=mode,
                    benchmark=benchmark_symbol,
                    issuer_cik=issuer_cik,
                    issuer_symbol=issuer_symbol,
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
                    issuer_cik=issuer_cik,
                    issuer_symbol=issuer_symbol,
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
    parser.add_argument("--entity-type", required=True, help="congress, congress_member, or insider")
    parser.add_argument("--entity-id", help="Optional single member bioguide ID or insider reporting CIK")
    parser.add_argument("--issuer", help="Optional insider issuer CIK or symbol scope")
    parser.add_argument("--lookback-days", type=int, default=1095)
    parser.add_argument("--mode", default="realistic_disclosure_lag", choices=sorted(SUPPORTED_MODES))
    parser.add_argument("--benchmark", default="^GSPC")
    parser.add_argument("--limit", type=int, default=5)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

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
    )
    print(json.dumps(report, indent=2, sort_keys=True, default=str))


if __name__ == "__main__":
    main()
