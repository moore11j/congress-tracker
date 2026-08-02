from __future__ import annotations

import argparse
import json
import time
from collections import deque
from typing import Any

from sqlalchemy import select

from app.db import SessionLocal, engine, ensure_institutional_activity_schema
from app.models import InstitutionalHolder
from app.services.institutional_activity import normalize_cik, refresh_holder_performance_cache


class CallsPerMinuteThrottle:
    def __init__(self, calls_per_minute: int):
        self.calls_per_minute = max(1, int(calls_per_minute))
        self.calls: deque[float] = deque()

    def wait(self) -> None:
        now = time.monotonic()
        while self.calls and now - self.calls[0] >= 60:
            self.calls.popleft()
        if len(self.calls) >= self.calls_per_minute:
            time.sleep(max(0.0, 60 - (now - self.calls[0])) + 0.05)
            self.wait()
            return
        self.calls.append(time.monotonic())


def _holder_ciks(*, cik: str | None, limit: int | None) -> list[str]:
    normalized = normalize_cik(cik)
    if normalized:
        return [normalized]
    query = (
        select(InstitutionalHolder.cik)
        .where(InstitutionalHolder.latest_report_year.is_not(None))
        .where(InstitutionalHolder.latest_report_quarter.is_not(None))
        .order_by(InstitutionalHolder.latest_filing_date.desc().nullslast(), InstitutionalHolder.cik.asc())
    )
    if limit is not None:
        query = query.limit(max(0, int(limit)))
    with SessionLocal() as db:
        return [row[0] for row in db.execute(query).all() if normalize_cik(row[0])]


def run_refresh(*, cik: str | None, limit: int | None, calls_per_minute: int, progress_every: int) -> dict[str, Any]:
    ensure_institutional_activity_schema(engine)
    ciks = _holder_ciks(cik=cik, limit=limit)
    stats: dict[str, Any] = {
        "status": "ok",
        "ciks_total": len(ciks),
        "ciks_seen": 0,
        "ciks_updated": 0,
        "rows_updated": 0,
        "errors": 0,
    }
    throttle = CallsPerMinuteThrottle(calls_per_minute)
    progress_interval = max(1, int(progress_every or 25))
    with SessionLocal() as db:
        for index, holder_cik in enumerate(ciks, start=1):
            throttle.wait()
            stats["ciks_seen"] += 1
            try:
                result = refresh_holder_performance_cache(
                    db,
                    holder_cik,
                    note="Performance is calculated from reported holdings and cached adjusted end-of-day prices.",
                )
                db.commit()
            except Exception as exc:
                db.rollback()
                stats["errors"] += 1
                print({"cik": holder_cik, "status": "failed", "error": exc.__class__.__name__, "progress": f"{index}/{len(ciks)}", **stats})
                continue
            updated = int(result.get("updated") or 0)
            stats["rows_updated"] += updated
            if updated:
                stats["ciks_updated"] += 1
            if index == 1 or index % progress_interval == 0 or index == len(ciks):
                print({"cik": holder_cik, "progress": f"{index}/{len(ciks)}", "last_result": result, **stats})
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description="Refresh cached institutional performance metrics.")
    parser.add_argument("--cik", default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--calls-per-minute", type=int, default=20)
    parser.add_argument("--progress-every", type=int, default=25)
    args = parser.parse_args()
    print(
        json.dumps(
            run_refresh(
                cik=args.cik,
                limit=args.limit,
                calls_per_minute=args.calls_per_minute,
                progress_every=args.progress_every,
            ),
            sort_keys=True,
            default=str,
        )
    )


if __name__ == "__main__":
    main()
