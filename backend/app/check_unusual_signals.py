from __future__ import annotations

from sqlalchemy import func, select

from app.db import SessionLocal
from app.models import Event
from app.routers.signals import _query_unusual_signals

BASELINE_DAYS = 365
RECENT_DAYS = 365
MULTIPLE = 1.2
MIN_AMOUNT = 0
MIN_BASELINE_COUNT = 1
LIMIT = 5


def main() -> None:
    with SessionLocal() as db:
        total_events = db.execute(select(func.count()).select_from(Event)).scalar_one()
        print(f"total_events={total_events}")

        items, counts = _query_unusual_signals(
            db=db,
            recent_days=RECENT_DAYS,
            baseline_days=BASELINE_DAYS,
            min_baseline_count=MIN_BASELINE_COUNT,
            multiple=MULTIPLE,
            min_amount=MIN_AMOUNT,
            limit=LIMIT,
        )

        print(
            "counts baseline_events={baseline_events_count} median_rows={median_rows_count} "
            "recent_events={recent_events_count} final_hits={final_hits_count}".format(
                **counts
            )
        )
        if (
            total_events > 0
            and counts["baseline_events_count"] > 0
            and counts["median_rows_count"] > 0
            and counts["recent_events_count"] > 0
        ):
            assert counts["final_hits_count"] > 0, "Expected unusual signal hits"

        print("top_rows=")
        for item in items:
            print(item)


if __name__ == "__main__":
    main()
