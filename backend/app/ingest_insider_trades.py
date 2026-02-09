from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import sys
from datetime import date, datetime, timedelta, timezone
from typing import Any

from sqlalchemy import select

from app.clients.fmp import FMPClientError, fetch_insider_trades
from app.db import SessionLocal
from app.models import Event, InsiderTransaction

logger = logging.getLogger(__name__)


def _parse_date(value: Any) -> date | None:
    if not value:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        cleaned = value.strip()
        if not cleaned:
            return None
        try:
            return datetime.fromisoformat(cleaned.replace("Z", "+00:00")).date()
        except Exception:
            try:
                return datetime.strptime(cleaned[:10], "%Y-%m-%d").date()
            except Exception:
                return None
    return None


def _as_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except Exception:
        return None


def _as_str(value: Any) -> str | None:
    if value is None:
        return None
    parsed = str(value).strip()
    return parsed if parsed else None


def _external_id(row: dict[str, Any]) -> str:
    key_parts = [
        _as_str(row.get("symbol")) or "",
        _as_str(row.get("filingDate")) or "",
        _as_str(row.get("transactionDate")) or "",
        _as_str(row.get("reportingCik")) or "",
        _as_str(row.get("insiderName")) or "",
        _as_str(row.get("transactionType")) or "",
        str(_as_float(row.get("securitiesTransacted")) or ""),
        str(_as_float(row.get("price")) or ""),
    ]
    return hashlib.sha1("|".join(key_parts).encode("utf-8")).hexdigest()


def _event_ts(transaction_date: date | None, filing_date: date | None) -> datetime:
    selected = transaction_date or filing_date or datetime.now(timezone.utc).date()
    return datetime(selected.year, selected.month, selected.day, tzinfo=timezone.utc)


def ingest_insider_trades(*, days: int = 30, page_limit: int = 3, per_page: int = 200) -> dict[str, int]:
    cutoff = date.today() - timedelta(days=days)
    scanned = inserted_raw = inserted_events = skipped = 0

    db = SessionLocal()
    try:
        for page in range(page_limit):
            rows = fetch_insider_trades(page=page, limit=per_page)
            if not rows:
                break

            for row in rows:
                scanned += 1
                filing_date = _parse_date(row.get("filingDate"))
                transaction_date = _parse_date(row.get("transactionDate"))
                reference_date = transaction_date or filing_date
                if reference_date and reference_date < cutoff:
                    continue

                external_id = _external_id(row)
                existing_raw = db.execute(
                    select(InsiderTransaction).where(InsiderTransaction.external_id == external_id)
                ).scalar_one_or_none()
                if existing_raw:
                    skipped += 1
                    continue

                symbol = _as_str(row.get("symbol"))
                payload_json = json.dumps(row, sort_keys=True)
                insider = InsiderTransaction(
                    source="fmp",
                    external_id=external_id,
                    symbol=symbol.upper() if symbol else None,
                    reporting_cik=_as_str(row.get("reportingCik")),
                    insider_name=_as_str(row.get("insiderName")),
                    transaction_type=_as_str(row.get("transactionType")),
                    role=_as_str(row.get("officerTitle") or row.get("insiderRole") or row.get("position")),
                    ownership=_as_str(row.get("ownershipType") or row.get("ownership")),
                    transaction_date=transaction_date,
                    filing_date=filing_date,
                    shares=_as_float(row.get("securitiesTransacted")),
                    price=_as_float(row.get("price")),
                    payload_json=payload_json,
                )
                db.add(insider)
                db.flush()
                inserted_raw += 1

                event_payload = {
                    "external_id": external_id,
                    "symbol": insider.symbol,
                    "insider_name": insider.insider_name,
                    "reporting_cik": insider.reporting_cik,
                    "transaction_type": insider.transaction_type,
                    "transaction_date": insider.transaction_date.isoformat() if insider.transaction_date else None,
                    "role": insider.role,
                    "ownership": insider.ownership,
                    "filing_date": insider.filing_date.isoformat() if insider.filing_date else None,
                    "shares": insider.shares,
                    "price": insider.price,
                    "source": "fmp",
                    "raw": row,
                }

                event = Event(
                    event_type="insider_trade",
                    ts=_event_ts(insider.transaction_date, insider.filing_date),
                    event_date=_event_ts(insider.transaction_date, insider.filing_date),
                    symbol=insider.symbol,
                    source="fmp",
                    member_name=None,
                    member_bioguide_id=None,
                    chamber=None,
                    party=None,
                    trade_type=insider.transaction_type.lower() if insider.transaction_type else None,
                    transaction_type=insider.transaction_type,
                    amount_min=None,
                    amount_max=None,
                    impact_score=0.0,
                    payload_json=json.dumps(event_payload, sort_keys=True),
                )
                db.add(event)
                inserted_events += 1

            db.commit()

        return {
            "status": "ok",
            "scanned": scanned,
            "inserted_raw": inserted_raw,
            "inserted_events": inserted_events,
            "skipped": skipped,
        }
    finally:
        db.close()




def insider_ingest_run(*, pages: int, limit: int, days: int = 30) -> dict[str, int | str]:
    return ingest_insider_trades(days=days, page_limit=pages, per_page=limit)

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest insider trades from FMP into events tape.")
    parser.add_argument("--days", type=int, default=30, help="Only ingest trades for the most recent N days.")
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--page-limit", type=int, default=3)
    parser.add_argument("--per-page", type=int, default=200)
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    logging.basicConfig(level=getattr(logging, str(args.log_level).upper(), logging.INFO))
    try:
        result = ingest_insider_trades(days=args.days, page_limit=args.page_limit, per_page=args.per_page)
    except FMPClientError as exc:
        raise SystemExit(str(exc))
    logger.info("Ingest completed: %s", result)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        main()
    else:
        pages = int(os.getenv("INGEST_PAGES", "3"))
        limit = int(os.getenv("INGEST_LIMIT", "200"))
        days = int(os.getenv("INGEST_INSIDER_DAYS", "30"))
        print(insider_ingest_run(pages=pages, limit=limit, days=days))
