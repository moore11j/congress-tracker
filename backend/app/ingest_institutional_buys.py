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

from app.clients.fmp import FMPClientError, fetch_institutional_buys
from app.db import SessionLocal
from app.models import Event, InstitutionalTransaction
from app.utils.symbols import canonical_symbol

logger = logging.getLogger(__name__)


SOURCE_LABEL = "fmp_institutional"


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


def _first_non_empty(row: dict[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = _as_str(row.get(key))
        if value:
            return value
    return None


def _first_float(row: dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        value = _as_float(row.get(key))
        if value is not None:
            return value
    return None


def _external_id(row: dict[str, Any]) -> str:
    key_parts = [
        _first_non_empty(row, "symbol", "ticker") or "",
        _first_non_empty(row, "holder", "institutionName", "investorName") or "",
        _first_non_empty(row, "cik", "institutionCik", "holderCik") or "",
        _first_non_empty(row, "filingDate", "date") or "",
        _first_non_empty(row, "reportDate", "periodOfReport") or "",
        str(_first_float(row, "shares", "sharesNumber", "shareChange") or ""),
        str(_first_float(row, "marketValue", "marketValueUsd", "value") or ""),
    ]
    return hashlib.sha1("|".join(key_parts).encode("utf-8")).hexdigest()


def _event_ts(filing_date: date | None, report_date: date | None) -> datetime:
    selected = filing_date or report_date or datetime.now(timezone.utc).date()
    return datetime(selected.year, selected.month, selected.day, tzinfo=timezone.utc)


def ingest_institutional_buys(*, days: int = 30, page_limit: int = 3, per_page: int = 200) -> dict[str, int]:
    cutoff = date.today() - timedelta(days=days)
    scanned = inserted_raw = inserted_events = skipped = 0

    db = SessionLocal()
    try:
        for page in range(page_limit):
            rows = fetch_institutional_buys(page=page, limit=per_page)
            if not rows:
                break

            for row in rows:
                scanned += 1
                filing_date = _parse_date(_first_non_empty(row, "filingDate", "date", "acceptedDate"))
                report_date = _parse_date(_first_non_empty(row, "reportDate", "periodOfReport"))
                reference_date = filing_date or report_date
                if reference_date and reference_date < cutoff:
                    continue

                shares = _first_float(row, "shares", "sharesNumber", "sharesHeld")
                change_in_shares = _first_float(row, "changeInShares", "shareChange", "sharesChange")
                if shares is not None and shares <= 0:
                    skipped += 1
                    continue
                if change_in_shares is not None and change_in_shares <= 0:
                    skipped += 1
                    continue

                symbol = canonical_symbol(_first_non_empty(row, "symbol", "ticker"))
                if not symbol:
                    skipped += 1
                    continue

                external_id = _external_id(row)
                existing_raw = db.execute(
                    select(InstitutionalTransaction).where(InstitutionalTransaction.external_id == external_id)
                ).scalar_one_or_none()
                if existing_raw:
                    skipped += 1
                    continue

                market_value = _first_float(row, "marketValue", "marketValueUsd", "value")
                avg_price = _first_float(row, "avgPricePaid", "averagePrice", "price")
                if market_value is None and shares and avg_price and avg_price > 0:
                    market_value = shares * avg_price

                institution = InstitutionalTransaction(
                    source=SOURCE_LABEL,
                    external_id=external_id,
                    symbol=symbol,
                    institution_name=_first_non_empty(row, "holder", "institutionName", "investorName", "entityName"),
                    institution_cik=_first_non_empty(row, "cik", "institutionCik", "holderCik", "investorCik"),
                    filing_date=filing_date,
                    report_date=report_date,
                    shares=shares,
                    market_value=market_value,
                    avg_price=avg_price,
                    change_in_shares=change_in_shares,
                    change_pct=_first_float(row, "changePercent", "changePct", "sharesChangePercent"),
                    payload_json=json.dumps(row, sort_keys=True),
                )
                db.add(institution)
                db.flush()
                inserted_raw += 1

                event_payload = {
                    "external_id": external_id,
                    "symbol": symbol,
                    "institution_name": institution.institution_name,
                    "institution_cik": institution.institution_cik,
                    "filing_date": institution.filing_date.isoformat() if institution.filing_date else None,
                    "report_date": institution.report_date.isoformat() if institution.report_date else None,
                    "shares": institution.shares,
                    "market_value": institution.market_value,
                    "avg_price": institution.avg_price,
                    "change_in_shares": institution.change_in_shares,
                    "change_pct": institution.change_pct,
                    "trade_type": "purchase",
                    "transaction_type": "holding_increase",
                    "data_semantics": "institutional_filing_holding_increase",
                    "timing_note": "Institutional filing data can be delayed versus execution date.",
                    "source": SOURCE_LABEL,
                    "raw": row,
                }

                event_dt = _event_ts(institution.filing_date, institution.report_date)
                estimated_value = int(round(institution.market_value)) if institution.market_value and institution.market_value > 0 else None

                event = Event(
                    event_type="institutional_buy",
                    ts=event_dt,
                    event_date=event_dt,
                    symbol=institution.symbol,
                    source=SOURCE_LABEL,
                    member_name=institution.institution_name,
                    member_bioguide_id=institution.institution_cik,
                    chamber=None,
                    party=None,
                    trade_type="purchase",
                    transaction_type="Holding Increase",
                    amount_min=estimated_value,
                    amount_max=estimated_value,
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


def institutional_ingest_run(*, pages: int, limit: int, days: int = 30) -> dict[str, int | str]:
    return ingest_institutional_buys(days=days, page_limit=pages, per_page=limit)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest institutional buy events from FMP into events tape.")
    parser.add_argument("--days", type=int, default=30, help="Only ingest filings for the most recent N days.")
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--page-limit", type=int, default=3)
    parser.add_argument("--per-page", type=int, default=200)
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    logging.basicConfig(level=getattr(logging, str(args.log_level).upper(), logging.INFO))
    try:
        result = ingest_institutional_buys(days=args.days, page_limit=args.page_limit, per_page=args.per_page)
    except FMPClientError as exc:
        raise SystemExit(str(exc))
    logger.info("Institutional ingest completed: %s", result)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        main()
    else:
        pages = int(os.getenv("INGEST_PAGES", "3"))
        limit = int(os.getenv("INGEST_LIMIT", "200"))
        days = int(os.getenv("INGEST_INSTITUTIONAL_DAYS", "30"))
        print(institutional_ingest_run(pages=pages, limit=limit, days=days))
