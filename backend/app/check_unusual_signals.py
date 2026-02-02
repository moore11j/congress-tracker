from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import func, select

from app.db import SessionLocal
from app.models import Event
from app.routers.signals import (
    DEFAULT_BASELINE_DAYS,
    DEFAULT_MIN_AMOUNT,
    DEFAULT_MULTIPLE,
    DEFAULT_RECENT_DAYS,
    _baseline_median_subquery,
)

MIN_BASELINE_COUNT = 10
LIMIT = 5


def main() -> None:
    now = datetime.now(timezone.utc)
    recent_since = now - timedelta(days=DEFAULT_RECENT_DAYS)
    baseline_since = now - timedelta(days=DEFAULT_BASELINE_DAYS)

    with SessionLocal() as db:
        total_events = db.execute(select(func.count()).select_from(Event)).scalar_one()
        print(f"total_events={total_events}")

        median_subquery = _baseline_median_subquery(baseline_since)
        median_rows = db.execute(select(func.count()).select_from(median_subquery)).scalar_one()
        print(f"median_rows={median_rows}")

        unusual_multiple = (Event.amount_max / median_subquery.c.median_amount_max).label(
            "unusual_multiple"
        )

        query = (
            select(
                Event.id.label("event_id"),
                Event.ts,
                Event.symbol,
                Event.member_name,
                Event.member_bioguide_id,
                Event.party,
                Event.chamber,
                Event.trade_type,
                Event.amount_min,
                Event.amount_max,
                Event.source,
                median_subquery.c.median_amount_max.label("baseline_median_amount_max"),
                median_subquery.c.baseline_count,
                unusual_multiple,
            )
            .join(median_subquery, median_subquery.c.symbol == Event.symbol)
            .where(Event.event_type == "congress_trade")
            .where(Event.ts >= recent_since)
            .where(Event.amount_max.is_not(None))
            .where(Event.amount_max >= DEFAULT_MIN_AMOUNT)
            .where(median_subquery.c.median_amount_max.is_not(None))
            .where(median_subquery.c.median_amount_max > 0)
            .where(median_subquery.c.baseline_count >= MIN_BASELINE_COUNT)
            .where(unusual_multiple >= DEFAULT_MULTIPLE)
            .order_by(unusual_multiple.desc(), Event.ts.desc())
            .limit(LIMIT)
        )

        rows = db.execute(query).all()
        print("top_rows=")
        for row in rows:
            print(row)


if __name__ == "__main__":
    main()
