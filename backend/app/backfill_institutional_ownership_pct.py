from __future__ import annotations

import argparse
import math
import time
from collections import deque
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import distinct, func, select, update

from app.clients.fmp import fetch_shares_float
from app.db import SessionLocal
from app.models import InstitutionalPosition
from app.utils.symbols import normalize_symbol


def _parse_number(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        parsed = float(value)
        return parsed if math.isfinite(parsed) else None
    if isinstance(value, str):
        cleaned = value.replace("$", "").replace(",", "").replace("%", "").strip()
        if not cleaned:
            return None
        try:
            parsed = float(cleaned)
        except ValueError:
            return None
        return parsed if math.isfinite(parsed) else None
    return None


def _first_number(row: dict[str, Any], *keys: str) -> float | None:
    lower = {str(key).lower(): value for key, value in row.items()}
    for key in keys:
        value = row.get(key)
        if value in (None, ""):
            value = lower.get(key.lower())
        parsed = _parse_number(value)
        if parsed is not None:
            return parsed
    return None


def _shares_denominator(rows: list[dict[str, Any]]) -> tuple[float | None, str | None]:
    for row in rows:
        if not isinstance(row, dict):
            continue
        float_shares = _first_number(row, "floatShares", "sharesFloat", "freeFloatShares", "publicFloatShares", "publicFloat", "float")
        if float_shares and float_shares > 0:
            return float_shares, "floatShares"
        outstanding = _first_number(row, "outstandingShares", "sharesOutstanding", "weightedAverageShsOut", "weightedAverageShsOutDil", "sharesOut")
        if outstanding and outstanding > 0:
            return outstanding, "sharesOutstanding"
    return None, None


class CallsPerMinuteThrottle:
    def __init__(self, calls_per_minute: int):
        self.calls_per_minute = max(1, int(calls_per_minute))
        self.calls: deque[float] = deque()

    def wait(self) -> None:
        now = time.monotonic()
        while self.calls and now - self.calls[0] >= 60:
            self.calls.popleft()
        if len(self.calls) >= self.calls_per_minute:
            sleep_for = max(0.0, 60 - (now - self.calls[0])) + 0.05
            time.sleep(sleep_for)
            self.wait()
            return
        self.calls.append(time.monotonic())


def _symbol_query(db, *, cik: str | None, latest_only: bool, overwrite: bool):
    query = select(distinct(InstitutionalPosition.normalized_symbol)).where(
        InstitutionalPosition.normalized_symbol.is_not(None),
        InstitutionalPosition.shares.is_not(None),
        InstitutionalPosition.shares > 0,
    )
    if not overwrite:
        query = query.where(InstitutionalPosition.ownership_pct.is_(None))
    if cik:
        query = query.where(InstitutionalPosition.cik == cik)
    if latest_only:
        latest_periods = (
            select(
                InstitutionalPosition.cik.label("cik"),
                func.max(InstitutionalPosition.report_year * 10 + InstitutionalPosition.report_quarter).label("period_key"),
            )
            .group_by(InstitutionalPosition.cik)
            .subquery()
        )
        query = query.join(
            latest_periods,
            (latest_periods.c.cik == InstitutionalPosition.cik)
            & (latest_periods.c.period_key == InstitutionalPosition.report_year * 10 + InstitutionalPosition.report_quarter),
        )
    return query.order_by(InstitutionalPosition.normalized_symbol.asc())


def _update_symbol_positions(db, symbol: str, denominator: float, *, cik: str | None, latest_only: bool, overwrite: bool, dry_run: bool) -> int:
    filters = [
        InstitutionalPosition.normalized_symbol == symbol,
        InstitutionalPosition.shares.is_not(None),
        InstitutionalPosition.shares > 0,
    ]
    if cik:
        filters.append(InstitutionalPosition.cik == cik)
    if not overwrite:
        filters.append(InstitutionalPosition.ownership_pct.is_(None))
    if latest_only:
        latest_period = (
            select(func.max(InstitutionalPosition.report_year * 10 + InstitutionalPosition.report_quarter))
            .where(InstitutionalPosition.normalized_symbol == symbol)
        )
        if cik:
            latest_period = latest_period.where(InstitutionalPosition.cik == cik)
        latest_key = db.execute(latest_period).scalar()
        if latest_key is None:
            return 0
        filters.append((InstitutionalPosition.report_year * 10 + InstitutionalPosition.report_quarter) == int(latest_key))

    rows = db.execute(select(InstitutionalPosition.id, InstitutionalPosition.shares).where(*filters)).all()
    if dry_run:
        return len(rows)
    updated = 0
    now = datetime.now(timezone.utc)
    for row_id, shares in rows:
        pct = max(0.0, min((float(shares) / float(denominator)) * 100.0, 100.0))
        db.execute(
            update(InstitutionalPosition)
            .where(InstitutionalPosition.id == row_id)
            .values(ownership_pct=round(pct, 6), updated_at=now)
        )
        updated += 1
    return updated


def run_backfill(
    *,
    limit_symbols: int | None,
    calls_per_minute: int,
    cik: str | None,
    latest_only: bool,
    overwrite: bool,
    dry_run: bool,
) -> dict[str, Any]:
    throttle = CallsPerMinuteThrottle(calls_per_minute)
    normalized_cik = "".join(ch for ch in str(cik or "") if ch.isdigit()).zfill(10) if cik else None
    stats: dict[str, Any] = {
        "status": "ok",
        "dry_run": dry_run,
        "latest_only": latest_only,
        "overwrite": overwrite,
        "cik": normalized_cik,
        "calls_per_minute": calls_per_minute,
        "symbols_seen": 0,
        "symbols_updated": 0,
        "symbols_without_denominator": 0,
        "symbols_failed": 0,
        "positions_updated": 0,
    }
    with SessionLocal() as db:
        symbols = [
            symbol
            for (symbol,) in db.execute(_symbol_query(db, cik=normalized_cik, latest_only=latest_only, overwrite=overwrite)).all()
            if normalize_symbol(symbol)
        ]
        if limit_symbols is not None:
            symbols = symbols[: max(0, int(limit_symbols))]
        stats["symbols_total"] = len(symbols)
        for index, raw_symbol in enumerate(symbols, start=1):
            symbol = normalize_symbol(raw_symbol)
            if not symbol:
                continue
            stats["symbols_seen"] += 1
            try:
                throttle.wait()
                denominator, denominator_source = _shares_denominator(fetch_shares_float(symbol=symbol))
            except Exception as exc:
                db.rollback()
                stats["symbols_failed"] += 1
                print({"symbol": symbol, "status": "failed", "error": str(exc)})
                continue
            if not denominator or denominator <= 0:
                stats["symbols_without_denominator"] += 1
                print({"symbol": symbol, "status": "no_denominator", "progress": f"{index}/{len(symbols)}"})
                continue
            updated = _update_symbol_positions(
                db,
                symbol,
                denominator,
                cik=normalized_cik,
                latest_only=latest_only,
                overwrite=overwrite,
                dry_run=dry_run,
            )
            if not dry_run:
                db.commit()
            stats["positions_updated"] += updated
            if updated:
                stats["symbols_updated"] += 1
            print(
                {
                    "symbol": symbol,
                    "status": "updated" if updated else "skipped",
                    "positions_updated": updated,
                    "denominator": round(float(denominator), 4),
                    "denominator_source": denominator_source,
                    "progress": f"{index}/{len(symbols)}",
                }
            )
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill institutional position ownership percentages from shares float/outstanding.")
    parser.add_argument("--limit-symbols", type=int, default=None)
    parser.add_argument("--calls-per-minute", type=int, default=500)
    parser.add_argument("--cik", default=None)
    parser.add_argument("--latest-only", action="store_true")
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    print(
        run_backfill(
            limit_symbols=args.limit_symbols,
            calls_per_minute=args.calls_per_minute,
            cik=args.cik,
            latest_only=args.latest_only,
            overwrite=args.overwrite,
            dry_run=args.dry_run,
        )
    )


if __name__ == "__main__":
    main()
