import json
import logging
import os
import subprocess
import sys
from pathlib import Path

import requests
from sqlalchemy import func, select

from app.db import SessionLocal
from app.ingest_house import ingest_house
from app.models import Event
from app.ingest_senate import ingest_senate
from app.ingest_insider_trades import insider_ingest_run


logger = logging.getLogger(__name__)


def _check_insider_freshness() -> str | None:
    """
    Fetch latest insider filing date from FMP stable endpoint.
    Returns date string or None.
    """
    key = os.getenv("FMP_API_KEY")
    if not key:
        logger.warning("FMP_API_KEY not set; skipping insider freshness check")
        return None

    url = (
        "https://financialmodelingprep.com/stable/insider-trading/latest"
        f"?page=0&limit=5&apikey={key}"
    )

    try:
        response = requests.get(url, timeout=30)
        if response.status_code != 200:
            logger.warning("FMP insider latest returned %s", response.status_code)
            return None

        data = response.json()
        if isinstance(data, list) and data:
            dates = [item.get("filingDate") for item in data if item.get("filingDate")]
            return max(dates) if dates else None
    except Exception as exc:
        logger.warning("Insider freshness check failed: %s", exc)

    return None


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


def _inserted_count(result: dict[str, object]) -> int:
    inserted = result.get("inserted")
    return inserted if isinstance(inserted, int) else 0


def _run_backfill() -> str:
    logger.info("Starting congress events backfill")
    subprocess.run(
        [
            sys.executable,
            "-m",
            "app.backfill_events_from_trades",
            "--log-level",
            "INFO",
        ],
        check=True,
    )
    logger.info("Finished congress events backfill")
    return "run"


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    _require_data_mount_writable()

    do_house = _is_truthy(os.getenv("INGEST_DO_HOUSE", "1"))
    do_senate = _is_truthy(os.getenv("INGEST_DO_SENATE", "1"))
    do_backfill = _is_truthy(os.getenv("INGEST_BACKFILL", "0"))
    do_insider = _is_truthy(os.getenv("INGEST_DO_INSIDER", "1"))

    pages = int(os.getenv("INGEST_PAGES", "3"))
    limit = int(os.getenv("INGEST_LIMIT", "200"))
    sleep_s = float(os.getenv("INGEST_SLEEP_S", "0.25"))
    insider_days = int(os.getenv("INGEST_INSIDER_DAYS", "30"))

    config = {
        "INGEST_DO_HOUSE": do_house,
        "INGEST_DO_SENATE": do_senate,
        "INGEST_BACKFILL": do_backfill,
        "INGEST_DO_INSIDER": do_insider,
        "INGEST_PAGES": pages,
        "INGEST_LIMIT": limit,
        "INGEST_SLEEP_S": sleep_s,
        "INGEST_INSIDER_DAYS": insider_days,
    }
    _log_startup_config(config)

    house_result = {"status": "skipped"}
    senate_result = {"status": "skipped"}
    insider_result = {"status": "skipped"}

    if do_house:
        house_result = ingest_house(pages=pages, limit=limit, sleep_s=sleep_s)

    if do_senate:
        senate_result = ingest_senate(pages=pages, limit=limit, sleep_s=sleep_s)

    if do_insider:
        insider_result = insider_ingest_run(pages=pages, limit=limit, days=insider_days)
        latest_fmp_date = _check_insider_freshness()

        latest_db_date = None
        try:
            with SessionLocal() as db:
                latest_db_date = db.execute(
                    select(func.max(Event.event_date)).where(Event.event_type == "insider_trade")
                ).scalar()
        except Exception as exc:
            logger.warning("Failed to check DB insider freshness: %s", exc)

        logger.info("FMP latest insider filingDate: %s", latest_fmp_date)
        logger.info("DB latest insider event_date: %s", latest_db_date)

    congress_inserted = _inserted_count(house_result) + _inserted_count(senate_result)
    should_run_backfill = do_backfill or congress_inserted > 0
    backfill_mode = "none"
    if should_run_backfill:
        backfill_mode = _run_backfill()

    print(
        json.dumps(
            {
                "house": house_result,
                "senate": senate_result,
                "insider": insider_result,
                "backfill": backfill_mode,
            }
        )
    )
