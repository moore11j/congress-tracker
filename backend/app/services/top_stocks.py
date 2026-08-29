from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import LeaderboardSnapshot
from app.services.screener import MAX_FETCH_ROWS, ScreenerParams, build_screener_rows

TOP_STOCKS_LEADERBOARD_KEY = "top_stocks"
TOP_STOCKS_PARAMS = ScreenerParams(
    page=1,
    page_size=10,
    sort="confirmation_score",
    sort_dir="desc",
    lookback_days=30,
    confirmation_score_min=60,
    confirmation_direction="bullish",
    confirmation_band="strong_plus",
)


def build_top_stocks_response(db: Session) -> dict[str, Any]:
    """Read the daily Top Stocks snapshot; this path is intentionally read-only."""
    snapshot = db.execute(
        select(LeaderboardSnapshot).where(LeaderboardSnapshot.leaderboard_key == TOP_STOCKS_LEADERBOARD_KEY)
    ).scalar_one_or_none()
    if snapshot is None:
        return _empty_response()
    payload = _payload(snapshot.payload_json)
    return payload if payload is not None else _empty_response()


def refresh_top_stocks_leaderboard(db: Session, *, now: datetime | None = None) -> dict[str, Any]:
    """Build the once-daily cache from the canonical Bullish Confirmation screener.

    The public API/page never invokes this builder: score assembly, cached source
    reads, and any enrichment work remain confined to the scheduled job.
    """
    generated_at = _utc(now or datetime.now(timezone.utc))
    rows = build_screener_rows(db, TOP_STOCKS_PARAMS, requested_rows=MAX_FETCH_ROWS)
    top_rows = rows[:10]
    payload = {
        "items": [
            _item_from_screener_row(
                row,
                rank=index,
                updated_at=_iso(generated_at),
            )
            for index, row in enumerate(top_rows, start=1)
        ],
        "returned": len(top_rows),
        "generated_at": _iso(generated_at),
        "source": "bullish_confirmation_screener_daily_cache",
        "qualification": _qualification(),
    }
    snapshot = db.execute(
        select(LeaderboardSnapshot).where(LeaderboardSnapshot.leaderboard_key == TOP_STOCKS_LEADERBOARD_KEY)
    ).scalar_one_or_none()
    serialized = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    if snapshot is None:
        db.add(
            LeaderboardSnapshot(
                leaderboard_key=TOP_STOCKS_LEADERBOARD_KEY,
                generated_at=generated_at,
                payload_json=serialized,
            )
        )
    else:
        snapshot.generated_at = generated_at
        snapshot.payload_json = serialized
    db.commit()
    return payload


def _item_from_screener_row(
    row: dict[str, Any],
    *,
    rank: int,
    updated_at: str,
) -> dict[str, Any]:
    confirmation = row.get("confirmation") if isinstance(row.get("confirmation"), dict) else {}
    symbol = str(row.get("symbol") or "").strip().upper()
    return {
        "rank": rank,
        "symbol": symbol,
        "company_name": str(row.get("company_name") or symbol),
        "confirmation_score": confirmation.get("score"),
        "confirmation_band": confirmation.get("band") or "inactive",
        "confirmation_direction": confirmation.get("direction") or "neutral",
        "price": row.get("price"),
        "key_drivers": _drivers_from_screener_row(row),
        "updated_at": updated_at,
        "ticker_url": str(row.get("ticker_url") or f"/ticker/{symbol}"),
    }


def _empty_response() -> dict[str, Any]:
    return {
        "items": [],
        "returned": 0,
        "generated_at": None,
        "source": "bullish_confirmation_screener_daily_cache",
        "qualification": _qualification(),
    }


def _qualification() -> dict[str, Any]:
    return {
        "confirmation_score_min": TOP_STOCKS_PARAMS.confirmation_score_min,
        "confirmation_direction": TOP_STOCKS_PARAMS.confirmation_direction,
        "confirmation_band": TOP_STOCKS_PARAMS.confirmation_band,
        "lookback_days": TOP_STOCKS_PARAMS.lookback_days,
    }


def _drivers_from_screener_row(row: dict[str, Any]) -> list[str]:
    """Use the same cached screener outputs that qualified this row, without re-scoring it."""
    drivers: list[str] = []
    if isinstance(row.get("analyst_consensus"), dict) and row["analyst_consensus"].get("active") is True:
        drivers.append("Analysts")
    if row.get("government_contracts_active") is True:
        drivers.append("Government contracts")
    if row.get("institutional_activity_active") is True:
        drivers.append("Institutions")
    if row.get("options_flow_active") is True:
        drivers.append("Options flow")
    if isinstance(row.get("congress_activity"), dict) and row["congress_activity"].get("present") is True:
        drivers.append("Congress")
    if isinstance(row.get("insider_activity"), dict) and row["insider_activity"].get("present") is True:
        drivers.append("Insiders")
    if not drivers:
        drivers.append("Confirmation Score")
    return drivers[:4]


def _payload(raw: str | None) -> dict[str, Any] | None:
    try:
        parsed = json.loads(raw or "{}")
    except (TypeError, ValueError):
        return None
    return parsed if isinstance(parsed, dict) else None


def _utc(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


def _iso(value: datetime) -> str:
    return _utc(value).astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
