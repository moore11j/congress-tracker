import json
import logging
import os
from pathlib import Path

from app.backfill_events_from_trades import run_backfill
from app.ingest_house import ingest_house
from app.ingest_senate import ingest_senate


logger = logging.getLogger(__name__)


def _is_truthy(value: str | None) -> bool:
    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "yes"}


def _require_data_mount_writable(path: str = "/data") -> None:
    data_path = Path(path)
    if not data_path.exists() or not data_path.is_dir():
        raise RuntimeError(f"Required data mount is missing: {path}")

    probe = data_path / ".ingest_write_probe"
    try:
        probe.write_text("ok")
        probe.unlink(missing_ok=True)
    except Exception as exc:
        raise RuntimeError(f"Required data mount is not writable: {path}") from exc


def _log_startup_config(config: dict[str, object]) -> None:
    logger.info("ingest startup config: %s", json.dumps(config, sort_keys=True))


def _run_backfill_if_requested(do_backfill: bool, limit: int) -> str:
    if not do_backfill:
        return "none"
    run_backfill(limit=limit)
    return "run"


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    _require_data_mount_writable()

    do_house = _is_truthy(os.getenv("INGEST_DO_HOUSE", "1"))
    do_senate = _is_truthy(os.getenv("INGEST_DO_SENATE", "1"))
    do_backfill = _is_truthy(os.getenv("INGEST_BACKFILL", "0"))

    pages = int(os.getenv("INGEST_PAGES", "3"))
    limit = int(os.getenv("INGEST_LIMIT", "200"))
    sleep_s = float(os.getenv("INGEST_SLEEP_S", "0.25"))

    config = {
        "INGEST_DO_HOUSE": do_house,
        "INGEST_DO_SENATE": do_senate,
        "INGEST_BACKFILL": do_backfill,
        "INGEST_PAGES": pages,
        "INGEST_LIMIT": limit,
        "INGEST_SLEEP_S": sleep_s,
    }
    _log_startup_config(config)

    house_result = {"status": "skipped"}
    senate_result = {"status": "skipped"}

    if do_house:
        house_result = ingest_house(pages=pages, limit=limit, sleep_s=sleep_s)

    if do_senate:
        senate_result = ingest_senate(pages=pages, limit=limit, sleep_s=sleep_s)

    backfill_mode = _run_backfill_if_requested(do_backfill=do_backfill, limit=limit)

    print(
        json.dumps(
            {
                "house": house_result,
                "senate": senate_result,
                "backfill": backfill_mode,
            }
        )
    )
