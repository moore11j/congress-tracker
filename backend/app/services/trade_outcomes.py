from __future__ import annotations

from datetime import datetime, timedelta

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models import TradeOutcome


def get_member_trade_outcomes(
    db: Session,
    member_id: str,
    lookback_days: int,
    benchmark_symbol: str = "^GSPC",
    member_ids: list[str] | None = None,
) -> list[TradeOutcome]:
    cutoff_dt = datetime.utcnow() - timedelta(days=lookback_days)
    candidate_member_ids = [member_id]
    if member_ids:
        candidate_member_ids = [value for value in member_ids if value]
        if not candidate_member_ids:
            candidate_member_ids = [member_id]

    return db.execute(
        select(TradeOutcome)
        .where(TradeOutcome.member_id.in_(candidate_member_ids))
        .where(TradeOutcome.benchmark_symbol == benchmark_symbol)
        .where(TradeOutcome.scoring_status == "ok")
        .where(TradeOutcome.trade_date.is_not(None))
        .where(TradeOutcome.trade_date >= cutoff_dt.date())
        .order_by(TradeOutcome.trade_date.asc(), TradeOutcome.event_id.asc())
    ).scalars().all()


def summarize_trade_outcome_statuses(db: Session) -> dict[str, int]:
    rows = db.execute(
        select(TradeOutcome.scoring_status, func.count())
        .group_by(TradeOutcome.scoring_status)
    ).all()
    return {status: int(count) for status, count in rows if status}


def count_member_trade_outcomes(
    db: Session,
    member_id: str,
    lookback_days: int,
    benchmark_symbol: str = "^GSPC",
    member_ids: list[str] | None = None,
) -> int:
    cutoff_dt = datetime.utcnow() - timedelta(days=lookback_days)
    candidate_member_ids = [member_id]
    if member_ids:
        candidate_member_ids = [value for value in member_ids if value]
        if not candidate_member_ids:
            candidate_member_ids = [member_id]

    return int(
        db.execute(
            select(func.count(TradeOutcome.id))
            .where(TradeOutcome.member_id.in_(candidate_member_ids))
            .where(TradeOutcome.benchmark_symbol == benchmark_symbol)
            .where(TradeOutcome.trade_date.is_not(None))
            .where(TradeOutcome.trade_date >= cutoff_dt.date())
        ).scalar_one()
        or 0
    )
