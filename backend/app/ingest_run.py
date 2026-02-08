import json
import os
import subprocess
import sys

from app.ingest_house import ingest_house
from app.ingest_senate import ingest_senate


def _is_truthy(value: str | None) -> bool:
    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "yes"}


def _run_backfill(rebuild: bool) -> str:
    cmd = [
        sys.executable,
        "-m",
        "app.backfill_events_from_trades",
        "--log-level",
        "INFO",
    ]
    if rebuild:
        cmd.append("--replace")
    subprocess.run(cmd, check=True)
    return "rebuild" if rebuild else "repair"


if __name__ == "__main__":
    pages = int(os.getenv("INGEST_PAGES", "3"))
    limit = int(os.getenv("INGEST_LIMIT", "200"))

    house_result = ingest_house(pages=pages, limit=limit)
    senate_result = ingest_senate(pages=pages, limit=limit)

    backfill_requested = _is_truthy(os.getenv("BACKFILL_EVENTS", "0"))
    rebuild_requested = _is_truthy(os.getenv("REBUILD_EVENTS", "0"))

    backfill_mode = "none"
    if backfill_requested:
        backfill_mode = _run_backfill(rebuild=rebuild_requested)

    print(
        json.dumps(
            {
                "house": house_result,
                "senate": senate_result,
                "backfill_ran": backfill_mode != "none",
                "backfill_mode": backfill_mode,
            }
        )
    )
