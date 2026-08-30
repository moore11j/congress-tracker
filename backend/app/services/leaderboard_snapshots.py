from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import Event, InstitutionalHolder, InstitutionalHolderPerformanceMetric, LeaderboardSnapshot, TradeOutcome

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
        INSIDER_LEADERBOARD_KEY: _build_insider_payload(db, generated_at),
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


def _build_insider_payload(db: Session, generated_at: datetime) -> dict[str, Any]:
    """Build the existing insider trade-outcomes leaderboard during the daily job."""
    from datetime import timedelta
    from app.main import _load_member_leaderboard_rows

    rows = _load_member_leaderboard_rows(
        db,
        normalized_source_mode="insiders",
        normalized_chamber="all",
        insider_market_trade_types={"purchase", "sale", "buy", "sell"},
        benchmark_symbol="SPY",
        cutoff_date=(generated_at - timedelta(days=365)).date(),
        min_trades=3,
        limit=10,
        normalized_sort="avg_alpha",
    )
    details = _insider_details(db, [str(row.get("member_id") or "") for row in rows])
    items = []
    for index, row in enumerate(rows, start=1):
        member_id = str(row.get("member_id") or "")
        detail = details.get(member_id, {})
        raw_name = str(row.get("member_name") or "").strip()
        name = detail.get("insider_name") or (raw_name if raw_name and not raw_name.isdigit() else None) or detail.get("company_name") or member_id
        items.append(
            {
                "rank": index,
                "name": name,
                "company_name": detail.get("company_name"),
                "role": detail.get("role"),
                "symbol": detail.get("symbol"),
                "reporting_cik": detail.get("reporting_cik") or member_id,
                "avg_return_pct": row.get("avg_return"),
                "avg_alpha_pct": row.get("avg_alpha"),
                "win_rate_pct": (float(row["win_rate"]) * 100) if row.get("win_rate") is not None else None,
                "trade_count": row.get("trade_count_scored"),
            }
        )
    return {
        "key": INSIDER_LEADERBOARD_KEY,
        "items": items,
        "generated_at": _iso(generated_at),
        "timeframe_label": "1Y trade outcomes",
        "sort": "avg_alpha_pct",
        "methodology": "Ranked by average scored trade alpha versus SPY across qualifying insider purchase and sale disclosures; this is not a CAGR or portfolio simulation.",
        "empty_message": "No qualifying insider trade outcomes are available in the current one-year snapshot.",
    }


def _insider_details(db: Session, member_ids: list[str]) -> dict[str, dict[str, str | None]]:
    normalized_ids = [member_id for member_id in member_ids if member_id]
    if not normalized_ids:
        return {}
    rows = db.execute(
        select(TradeOutcome.member_id, TradeOutcome.symbol, Event.symbol, Event.payload_json)
        .select_from(TradeOutcome)
        .join(Event, Event.id == TradeOutcome.event_id)
        .where(TradeOutcome.member_id.in_(normalized_ids))
        .where(Event.event_type == "insider_trade")
        .order_by(TradeOutcome.member_id, TradeOutcome.trade_date.desc(), TradeOutcome.id.desc())
    ).all()
    details: dict[str, dict[str, str | None]] = {}
    for member_id, outcome_symbol, event_symbol, payload_json in rows:
        key = str(member_id or "")
        if not key or key in details:
            continue
        payload = _payload_dict(payload_json)
        details[key] = {
            "symbol": str(outcome_symbol or event_symbol or "").strip().upper() or None,
            "reporting_cik": _payload_text(payload, "reporting_cik", "reportingCik", "reportingCIK", "rptOwnerCik"),
            "insider_name": _payload_text(payload, "insider_name", "insiderName", "reporting_owner_name", "reportingOwnerName", "owner_name", "ownerName"),
            "company_name": _payload_text(payload, "company_name", "companyName", "issuer_name", "issuerName"),
            "role": _payload_text(payload, "role", "typeOfOwner", "officerTitle", "insiderRole", "position"),
        }
    return details


def _payload_dict(raw: str | None) -> dict[str, Any]:
    try:
        payload = json.loads(raw or "{}")
    except (TypeError, ValueError):
        return {}
    if not isinstance(payload, dict):
        return {}
    # Historical Form 4 events have used both a top-level payload and nested
    # `payload`/`raw` envelopes. Preserve every known shape for the daily cache.
    result = dict(payload)
    for key in ("raw", "payload"):
        nested = payload.get(key)
        if isinstance(nested, dict):
            result.update(nested)
    return result


def _payload_text(payload: dict[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


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
