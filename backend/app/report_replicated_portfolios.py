from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from typing import Any

from sqlalchemy import select

from app.db import SessionLocal
from app.models import Member, ReplicatedPortfolioPosition, ReplicatedPortfolioRun


LOOKBACKS = [30, 90, 180, 365, 1095]
NON_EQUITY_REASONS = {
    "options",
    "municipal_bond",
    "corporate_bond",
    "private_fund",
    "unsupported_asset_class",
    "not_equity_outcome_eligible",
    "non_equity_or_unpriced_asset",
}
MISSING_PRICE_REASONS = {"missing_price_history", "no_execution_price", "missing_trading_calendar"}
UNRESOLVED_REASONS = {"no_symbol", "invalid_symbol", "unsupported_symbol"}
SALE_WITHOUT_POSITION_REASONS = {
    "unmatched_sell",
    "sale_without_known_prior_position",
    "sale_without_position_during_warmup",
    "sale_without_position",
}


def _status_payload(run: ReplicatedPortfolioRun) -> dict[str, Any]:
    try:
        parsed = json.loads(run.status_message or "{}")
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _warmup(run: ReplicatedPortfolioRun) -> dict[str, Any]:
    payload = _status_payload(run).get("warmup_diagnostics")
    return payload if isinstance(payload, dict) else {}


def _quality(run: ReplicatedPortfolioRun) -> str:
    diagnostics = _status_payload(run).get("curve_diagnostics")
    if isinstance(diagnostics, dict):
        quality = diagnostics.get("curve_quality_status")
        if quality:
            return str(quality)
    return "unknown"


def _latest_runs(all_runs: list[ReplicatedPortfolioRun]) -> list[ReplicatedPortfolioRun]:
    latest: dict[tuple[Any, ...], ReplicatedPortfolioRun] = {}
    for run in all_runs:
        key = (
            run.entity_type,
            run.entity_id,
            run.lookback_days,
            run.mode,
            run.benchmark_symbol,
            run.issuer_cik or "",
            run.issuer_symbol or "",
        )
        current = latest.get(key)
        current_sort = (current.computed_at, current.id) if current else None
        run_sort = (run.computed_at, run.id)
        if current is None or run_sort > current_sort:
            latest[key] = run
    return list(latest.values())


def _is_legacy_fmp_entity(entity_id: str | None) -> bool:
    return (entity_id or "").strip().upper().startswith("FMP_")


def _duplicate_entity_windows(all_runs: list[ReplicatedPortfolioRun], lookback_days: int) -> int:
    counts: Counter[tuple[Any, ...]] = Counter()
    for run in all_runs:
        if run.lookback_days != lookback_days:
            continue
        counts[
            (
                run.entity_id,
                run.lookback_days,
                run.mode,
                run.benchmark_symbol,
                run.issuer_cik or "",
                run.issuer_symbol or "",
            )
        ] += 1
    return sum(1 for count in counts.values() if count > 1)


def _estimated_value(position: ReplicatedPortfolioPosition) -> float:
    if position.amount_min is not None and position.amount_max is not None:
        return float(position.amount_min + position.amount_max) / 2.0
    if position.amount_max is not None:
        return float(position.amount_max)
    if position.amount_min is not None:
        return float(position.amount_min)
    return 0.0


def _window_report(
    *,
    latest_runs: list[ReplicatedPortfolioRun],
    all_runs: list[ReplicatedPortfolioRun],
    positions_by_run: dict[int, list[ReplicatedPortfolioPosition]],
    member_ids: set[str],
    lookback_days: int,
) -> dict[str, Any]:
    runs = [run for run in latest_runs if run.lookback_days == lookback_days]
    canonical_runs = [
        run
        for run in runs
        if (run.entity_id in member_ids and not _is_legacy_fmp_entity(run.entity_id))
    ]
    positions = [position for run in runs for position in positions_by_run.get(int(run.id), [])]
    canonical_positions = [
        position
        for run in canonical_runs
        for position in positions_by_run.get(int(run.id), [])
    ]
    skipped = [position for position in canonical_positions if position.status == "skipped"]
    qualities = Counter(_quality(run) for run in canonical_runs)
    estimated_positions = [position for position in canonical_positions if position.side == "estimated_opening_position"]
    return {
        "latest_run_count": len(canonical_runs),
        "distinct_canonical_members": len({run.entity_id for run in canonical_runs}),
        "public_helper_row_count": sum(1 for run in canonical_runs if run.status == "ok"),
        "persisted_point_count": sum(int(run.points_count or 0) for run in canonical_runs),
        "persisted_position_count": sum(int(run.positions_count or 0) for run in canonical_runs),
        "quality_distribution": dict(sorted(qualities.items())),
        "total_excluded_count": sum(int(run.skipped_events_count or 0) for run in canonical_runs),
        "non_equity_asset_count": sum(1 for position in skipped if position.skip_reason in NON_EQUITY_REASONS),
        "missing_execution_price_count": sum(1 for position in skipped if position.skip_reason in MISSING_PRICE_REASONS),
        "unresolved_symbol_count": sum(1 for position in skipped if position.skip_reason in UNRESOLVED_REASONS),
        "sale_without_position_count": sum(1 for position in skipped if position.skip_reason in SALE_WITHOUT_POSITION_REASONS),
        "estimated_opening_positions_count": sum(
            int(_warmup(run).get("estimated_opening_positions_count") or 0) for run in canonical_runs
        )
        or len(estimated_positions),
        "estimated_opening_positions_value": sum(
            float(_warmup(run).get("estimated_opening_positions_value") or 0.0) for run in canonical_runs
        )
        or sum(_estimated_value(position) for position in estimated_positions),
        "opening_positions_count": sum(int(_warmup(run).get("opening_positions_count") or 0) for run in canonical_runs),
        "buy_marker_count": sum(1 for position in canonical_positions if position.status != "skipped" and position.entry_date is not None),
        "sell_marker_count": sum(1 for position in canonical_positions if position.status != "skipped" and position.exit_date is not None),
        "duplicate_entity_window_rows": _duplicate_entity_windows(all_runs, lookback_days),
        "orphan_fragment_rows": sum(
            1 for run in canonical_runs if run.entity_id not in member_ids or _is_legacy_fmp_entity(run.entity_id)
        ),
        "stale_legacy_fmp_latest_rows": sum(1 for run in runs if _is_legacy_fmp_entity(run.entity_id)),
    }


def _dwight_report(
    *,
    latest_runs: list[ReplicatedPortfolioRun],
    positions_by_run: dict[int, list[ReplicatedPortfolioPosition]],
    members: list[Member],
) -> dict[str, Any] | None:
    member = next(
        (
            item
            for item in members
            if (item.first_name or "").strip().lower() == "dwight"
            and (item.last_name or "").strip().lower() == "evans"
        ),
        None,
    )
    if member is None:
        return None
    report: dict[str, Any] = {
        "bioguide_id": member.bioguide_id,
        "name": " ".join(part for part in [member.first_name, member.last_name] if part),
    }
    for lookback_days in LOOKBACKS:
        run = next((item for item in latest_runs if item.entity_id == member.bioguide_id and item.lookback_days == lookback_days), None)
        if run is None:
            continue
        positions = positions_by_run.get(int(run.id), [])
        sale_skips: Counter[tuple[str | None, str | None]] = Counter(
            (position.symbol, position.skip_reason)
            for position in positions
            if position.status == "skipped" and position.skip_reason in SALE_WITHOUT_POSITION_REASONS
        )
        estimates: dict[str, dict[str, Any]] = {}
        for position in positions:
            if position.side != "estimated_opening_position":
                continue
            symbol = position.symbol or "UNKNOWN"
            entry = estimates.setdefault(symbol, {"symbol": symbol, "count": 0, "value": 0.0})
            entry["count"] += 1
            entry["value"] += _estimated_value(position)
        report[str(lookback_days)] = {
            "sale_without_position_by_symbol": [
                {"symbol": symbol, "skip_reason": reason, "count": count}
                for (symbol, reason), count in sorted(sale_skips.items())
            ],
            "estimated_opening_by_symbol": list(sorted(estimates.values(), key=lambda item: item["symbol"])),
            "warmup_diagnostics": _warmup(run),
        }
    return report


def build_report() -> dict[str, Any]:
    db = SessionLocal()
    try:
        all_runs = db.execute(
            select(ReplicatedPortfolioRun)
            .where(ReplicatedPortfolioRun.entity_type == "congress_member")
            .where(ReplicatedPortfolioRun.mode == "realistic_disclosure_lag")
            .where(ReplicatedPortfolioRun.issuer_cik.is_(None))
            .where(ReplicatedPortfolioRun.issuer_symbol.is_(None))
        ).scalars().all()
        latest_runs = _latest_runs(all_runs)
        run_ids = [int(run.id) for run in latest_runs]
        positions = (
            db.execute(select(ReplicatedPortfolioPosition).where(ReplicatedPortfolioPosition.run_id.in_(run_ids))).scalars().all()
            if run_ids
            else []
        )
        positions_by_run: dict[int, list[ReplicatedPortfolioPosition]] = defaultdict(list)
        for position in positions:
            positions_by_run[int(position.run_id)].append(position)
        members = db.execute(select(Member)).scalars().all()
        member_ids = {str(member.bioguide_id) for member in members if member.bioguide_id}
        report = {
            str(lookback): _window_report(
                latest_runs=latest_runs,
                all_runs=all_runs,
                positions_by_run=positions_by_run,
                member_ids=member_ids,
                lookback_days=lookback,
            )
            for lookback in LOOKBACKS
        }
        report["dwight_evans"] = _dwight_report(latest_runs=latest_runs, positions_by_run=positions_by_run, members=members)
        return report
    finally:
        db.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Report latest Congress replicated portfolio run diagnostics.")
    parser.parse_args()
    print(json.dumps(build_report(), sort_keys=True))


if __name__ == "__main__":
    main()
