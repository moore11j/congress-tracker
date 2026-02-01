from __future__ import annotations

import argparse
import hashlib
import json
import logging
from datetime import datetime, time, timezone

from sqlalchemy import func, select

from app.db import DATABASE_URL, SessionLocal
from app.models import Event, Filing, Member, Security, Transaction

logger = logging.getLogger(__name__)


def _event_ts(trade_date, report_date) -> datetime:
    use_date = trade_date or report_date
    if use_date:
        return datetime.combine(use_date, time.min, tzinfo=timezone.utc)
    return datetime.now(timezone.utc)


def _build_backfill_id(payload: dict) -> str:
    key_fields = {
        "symbol": payload.get("symbol"),
        "member_bioguide_id": payload.get("member", {}).get("bioguide_id"),
        "transaction_type": payload.get("transaction_type"),
        "amount_range_min": payload.get("amount_range_min"),
        "amount_range_max": payload.get("amount_range_max"),
        "trade_date": payload.get("trade_date"),
        "source": payload.get("source"),
    }
    normalized = json.dumps(key_fields, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--replace", action="store_true")
    p.add_argument("--limit", type=int)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--log-level", default="INFO")
    return p.parse_args()


def main():
    args = _parse_args()
    level_name = str(args.log_level).upper()
    logging.basicConfig(level=getattr(logging, level_name, logging.INFO))


    db = SessionLocal()

    try:
        logger.info("Backfill starting")
        logger.info("DB: %s", DATABASE_URL)

        legacy_count = db.execute(
            select(func.count()).select_from(Transaction)
        ).scalar_one()

        events_count = db.execute(
            select(func.count()).select_from(Event)
        ).scalar_one()

        logger.info("Legacy trades: %s", legacy_count)
        logger.info("Existing events: %s", events_count)

        if legacy_count == 0:
            logger.warning("No trades found — nothing to backfill")
            return

        if args.replace:
            deleted = db.query(Event).filter(
                Event.event_type == "congress_trade"
            ).delete(synchronize_session=False)

            db.commit()
            logger.info("Deleted %s old events", deleted)

        existing_ids: set[str] = set()

        rows = db.execute(
            select(Event.payload_json).where(Event.event_type == "congress_trade")
        ).all()

        for (payload_json,) in rows:
            try:
                payload = json.loads(payload_json)
                bid = payload.get("backfill_id")
                if bid:
                    existing_ids.add(bid)
            except Exception:
                continue

        q = (
            select(Transaction, Member, Security, Filing)
            .join(Member, Transaction.member_id == Member.id)
            .outerjoin(Security, Transaction.security_id == Security.id)
            .join(Filing, Transaction.filing_id == Filing.id)
            .order_by(Transaction.id)
        )

        if args.limit:
            q = q.limit(args.limit)

        scanned = inserted = skipped = 0

        for tx, member, security, filing in db.execute(q):
            scanned += 1

            if not security or not security.symbol:
                skipped += 1
                continue

            symbol = security.symbol.upper()
            source = filing.source or member.chamber

            payload = {
                "transaction_id": tx.id,
                "filing_id": tx.filing_id,
                "member_id": tx.member_id,
                "security_id": tx.security_id,
                "owner_type": tx.owner_type,
                "transaction_type": tx.transaction_type,
                "trade_date": tx.trade_date.isoformat() if tx.trade_date else None,
                "report_date": tx.report_date.isoformat() if tx.report_date else None,
                "amount_range_min": tx.amount_range_min,
                "amount_range_max": tx.amount_range_max,
                "description": tx.description,
                "symbol": symbol,
                "security_name": security.name if security else None,
                "asset_class": security.asset_class if security else None,
                "sector": security.sector if security else None,
                "member": {
                    "bioguide_id": member.bioguide_id,
                    "name": f"{member.first_name or ''} {member.last_name or ''}".strip(),
                    "chamber": member.chamber,
                    "party": member.party,
                    "state": member.state,
                },
                "source": source,
                "filing_source": filing.source,
                "filing_date": filing.filing_date.isoformat() if filing.filing_date else None,
                "document_url": filing.document_url,
            }

            backfill_id = _build_backfill_id(payload)

            if backfill_id in existing_ids:
                skipped += 1
                continue

            payload["backfill_id"] = backfill_id
            existing_ids.add(backfill_id)

            event = Event(
                event_type="congress_trade",
                ts=_event_ts(tx.trade_date, tx.report_date),

                symbol=symbol,
                source=source or "unknown",

                member_name=f"{member.first_name or ''} {member.last_name or ''}".strip(),
                member_bioguide_id=member.bioguide_id,

                party=member.party,
                chamber=member.chamber,

                trade_type=tx.transaction_type,
                amount_min=tx.amount_range_min,
                amount_max=tx.amount_range_max,

                impact_score=0.0,
                payload_json=json.dumps(payload, sort_keys=True),
            )

            if not args.dry_run:
                db.add(event)

            inserted += 1

        if not args.dry_run:
            db.commit()

        logger.info("Scanned: %s", scanned)
        logger.info("Inserted: %s", inserted)
        logger.info("Skipped: %s", skipped)

    finally:
        db.close()


if __name__ == "__main__":
    main()
