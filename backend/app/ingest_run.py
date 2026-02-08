import json
import logging
import os

from app.backfill_events_from_trades import run_backfill
from app.ingest_house import ingest_house
from app.ingest_senate import ingest_senate


def _is_truthy(value: str | None) -> bool:
    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "yes"}


def _run_backfill_mode() -> str:
    rebuild_requested = _is_truthy(os.getenv("REBUILD_EVENTS", "0"))
    repair_requested = _is_truthy(os.getenv("BACKFILL_EVENTS", "0"))

    if rebuild_requested:
        logging.basicConfig(level=logging.INFO)
        run_backfill(replace=True)
        return "replace"

    if repair_requested:
        logging.basicConfig(level=logging.INFO)
        run_backfill(repair=True)
        return "repair"

    return "none"


if __name__ == "__main__":
    pages = int(os.getenv("INGEST_PAGES", "3"))
    limit = int(os.getenv("INGEST_LIMIT", "200"))

    house_result = ingest_house(pages=pages, limit=limit)
    senate_result = ingest_senate(pages=pages, limit=limit)
    backfill_mode = _run_backfill_mode()

    print(
        json.dumps(
            {
                "house": house_result,
                "senate": senate_result,
                "backfill": backfill_mode,
            }
        )
    )
