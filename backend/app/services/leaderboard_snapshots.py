from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import InstitutionalHolder, InstitutionalHolderPerformanceMetric, LeaderboardSnapshot

CONGRESS_LEADERBOARD_KEY = "congress_members"
INSIDER_LEADERBOARD_KEY = "insiders"
INSTITUTION_LEADERBOARD_KEY = "institutions"
LEADERBOARD_KEYS = {CONGRESS_LEADERBOARD_KEY, INSIDER_LEADERBOARD_KEY, INSTITUTION_LEADERBOARD_KEY}


def read_leaderboard_snapshot(db: Session, key: str) -> dict[str, Any]:
    if key not in LEADERBOARD_KEYS:
        raise KeyError(key)
    row = db.execute(select(LeaderboardSnapshot).where(LeaderboardSnapshot.leaderboard_key == key)).scalar_one_or_none()
    if row is None:
        return _empty_snapshot(key)
    try:
        payload = json.loads(row.payload_json)
    except (TypeError, ValueError):
        return _empty_snapshot(key)
    return payload if isinstance(payload, dict) else _empty_snapshot(key)


def refresh_performance_leaderboard_snapshots(db: Session, *, now: datetime | None = None) -> dict[str, dict[str, Any]]:
    generated_at = _utc(now or datetime.now(timezone.utc))
    payloads = {
        CONGRESS_LEADERBOARD_KEY: _build_congress_payload(db, generated_at),
        INSIDER_LEADERBOARD_KEY: _build_insider_payload(generated_at),
        INSTITUTION_LEADERBOARD_KEY: _build_institution_payload(db, generated_at),
    }
    for key, payload in payloads.items():
        _store_snapshot(db, key, generated_at, payload)
    db.commit()
    return payloads


def _build_congress_payload(db: Session, generated_at: datetime) -> dict[str, Any]:
    # This is the existing, point-in-time-safe portfolio leaderboard builder.
    # Importing it here keeps its eligibility, quality, and ranking logic intact.
    from app.main import _load_congress_portfolio_leaderboard_rows

    rows, missing_runs, excluded_runs, included_quality = _load_congress_portfolio_leaderboard_rows(
        db,
        normalized_chamber="all",
        benchmark_symbol="SPY",
        lookback_days=1095,
        mode="realistic_disclosure_lag",
        limit=10,
        normalized_sort="alpha_pct",
    )
    items = [
        {
            "rank": row.get("rank"),
            "name": row.get("member_name"),
            "party": row.get("party"),
            "chamber": row.get("chamber"),
            "total_return_pct": row.get("total_return_pct"),
            "cagr_pct": row.get("cagr_pct"),
            "alpha_pct": row.get("alpha_pct"),
            "sharpe_ratio": row.get("sharpe_ratio"),
            "max_drawdown_pct": row.get("max_drawdown_pct"),
            "win_rate_pct": row.get("win_rate_pct"),
            "positions_count": row.get("positions_count"),
            "href": f"/member/{row.get('member_slug') or row.get('member_id')}",
        }
        for row in rows
    ]
    return {
        "key": CONGRESS_LEADERBOARD_KEY,
        "items": items,
        "generated_at": _iso(generated_at),
        "timeframe_label": "3Y",
        "sort": "alpha_pct",
        "methodology": "Simulated portfolios use publicly available congressional disclosure timing.",
        "metadata": {"missing_portfolio_runs": missing_runs, "excluded_quality_runs": excluded_runs, "included_quality_statuses": included_quality},
    }


def _build_insider_payload(generated_at: datetime) -> dict[str, Any]:
    # Existing portfolio runs aggregate by reporting issuer CIK rather than an
    # individual insider identity. Do not relabel that as an insider leaderboard.
    return {
        "key": INSIDER_LEADERBOARD_KEY,
        "items": [],
        "generated_at": _iso(generated_at),
        "timeframe_label": "—",
        "sort": None,
        "methodology": "A personal-insider, point-in-time portfolio leaderboard is not yet available.",
        "empty_message": "Historical performance rankings are still being built as more qualifying Form 4 records mature.",
    }


def _build_institution_payload(db: Session, generated_at: datetime) -> dict[str, Any]:
    rows = db.execute(
        select(InstitutionalHolderPerformanceMetric, InstitutionalHolder)
        .join(InstitutionalHolder, InstitutionalHolder.cik == InstitutionalHolderPerformanceMetric.cik)
        .where(InstitutionalHolderPerformanceMetric.metric_key == "three_year")
        .where(InstitutionalHolderPerformanceMetric.status == "ok")
        .where(InstitutionalHolderPerformanceMetric.return_pct.is_not(None))
        .order_by(InstitutionalHolderPerformanceMetric.return_pct.desc(), InstitutionalHolderPerformanceMetric.position_count.desc())
        .limit(10)
    ).all()
    items = [
        {
            "rank": index,
            "name": metric.holder_name or holder.holder_name or metric.cik,
            "cik": metric.cik,
            "total_return_pct": metric.return_pct,
            "positions_count": metric.position_count,
            "coverage_pct": metric.coverage_pct,
            "report_period": metric.report_period,
            "href": f"/institution/{metric.cik}",
        }
        for index, (metric, holder) in enumerate(rows, start=1)
    ]
    return {
        "key": INSTITUTION_LEADERBOARD_KEY,
        "items": items,
        "generated_at": _iso(generated_at),
        "timeframe_label": "3Y reported holdings",
        "sort": "total_return_pct",
        "methodology": "Returns are based on public 13F holdings and point-in-time filing availability.",
        "empty_message": "Historical performance rankings are still being built as more qualifying filings mature.",
    }


def _store_snapshot(db: Session, key: str, generated_at: datetime, payload: dict[str, Any]) -> None:
    row = db.execute(select(LeaderboardSnapshot).where(LeaderboardSnapshot.leaderboard_key == key)).scalar_one_or_none()
    serialized = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    if row is None:
        db.add(LeaderboardSnapshot(leaderboard_key=key, generated_at=generated_at, payload_json=serialized))
    else:
        row.generated_at = generated_at
        row.payload_json = serialized


def _empty_snapshot(key: str) -> dict[str, Any]:
    return {"key": key, "items": [], "generated_at": None, "empty_message": "Historical performance rankings are still being built as more qualifying records mature."}


def _utc(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


def _iso(value: datetime) -> str:
    return _utc(value).astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
