from __future__ import annotations

import argparse
import hashlib
import json
import logging
from datetime import date, datetime, time, timezone

from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from app.db import DATABASE_URL, SessionLocal
from app.models import Event, Filing, Member, Security, Transaction
from app.routers.events import list_events

logger = logging.getLogger(__name__)

ALLOWED_TRADE_TYPES = {"purchase", "sale", "exchange", "received"}


def _event_ts(trade_date, report_date) -> datetime:
    use_date = trade_date or report_date
    if use_date:
        return datetime.combine(use_date, time.min, tzinfo=timezone.utc)
    return datetime.now(timezone.utc)


def _normalize_trade_type(value: str | None) -> str | None:
    if not value:
        return None
    normalized = value.strip().lower()
    if normalized in ALLOWED_TRADE_TYPES:
        return normalized
    if "purchase" in normalized or "buy" in normalized or "acquisition" in normalized:
        return "purchase"
    if "sale" in normalized or "sell" in normalized or "dispose" in normalized:
        return "sale"
    if "exchange" in normalized:
        return "exchange"
    if "receive" in normalized or "gift" in normalized or "award" in normalized:
        return "received"
    return None


def _parse_iso_date(value: str | None) -> date | None:
    if not value:
        return None
    cleaned = value.strip()
    if cleaned.endswith("Z"):
        cleaned = cleaned[:-1]
    try:
        parsed = datetime.fromisoformat(cleaned)
    except ValueError:
        try:
            return date.fromisoformat(cleaned)
        except ValueError:
            return None
    return parsed.date()


def _to_event_datetime(value: date | datetime | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    return datetime.combine(value, time.min, tzinfo=timezone.utc)


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
    mode_group = p.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--replace",
        action="store_true",
        help="Rebuild trade events from legacy trades (safe dedupe).",
    )
    mode_group.add_argument(
        "--repair",
        action="store_true",
        help="Repair NULL filter columns on existing events without inserting new rows",
    )
    p.add_argument("--limit", type=int)
    p.add_argument(
        "--dry-run", action="store_true", help="Show counts without writing changes."
    )
    p.add_argument("--log-level", default="INFO")
    return p.parse_args()


def _payload_member_name(payload: dict) -> str | None:
    member = payload.get("member")
    if isinstance(member, dict):
        name = member.get("name")
        if name:
            return name.strip()
    return None


def _parse_payload_fields(payload: dict) -> dict[str, object | None]:
    member = payload.get("member") if isinstance(payload.get("member"), dict) else {}
    return {
        "symbol": payload.get("symbol"),
        "member_name": _payload_member_name(payload),
        "member_bioguide_id": member.get("bioguide_id"),
        "chamber": member.get("chamber"),
        "party": member.get("party"),
        "transaction_type": payload.get("transaction_type"),
        "trade_type": payload.get("trade_type"),
        "amount_min": payload.get("amount_range_min"),
        "amount_max": payload.get("amount_range_max"),
        "trade_date": _parse_iso_date(payload.get("trade_date")),
        "report_date": _parse_iso_date(payload.get("report_date")),
    }


def _resolve_from_transaction_row(
    tx: Transaction,
    member: Member,
    security: Security | None,
    filing: Filing,
) -> dict[str, object | None]:
    return {
        "symbol": security.symbol if security else None,
        "member_name": f"{member.first_name or ''} {member.last_name or ''}".strip() or None,
        "member_bioguide_id": member.bioguide_id,
        "chamber": member.chamber,
        "party": member.party,
        "transaction_type": tx.transaction_type,
        "amount_min": tx.amount_range_min,
        "amount_max": tx.amount_range_max,
        "trade_date": tx.trade_date,
        "report_date": tx.report_date,
    }


def _resolve_from_transaction(db: Session, transaction_id: int) -> dict[str, object | None] | None:
    row = (
        db.execute(
            select(Transaction, Member, Security, Filing)
            .join(Member, Transaction.member_id == Member.id)
            .outerjoin(Security, Transaction.security_id == Security.id)
            .join(Filing, Transaction.filing_id == Filing.id)
            .where(Transaction.id == transaction_id)
        )
        .mappings()
        .first()
    )
    if not row:
        return None
    tx: Transaction = row["Transaction"]
    member: Member = row["Member"]
    security: Security | None = row.get("Security")
    filing: Filing = row["Filing"]
    return _resolve_from_transaction_row(tx, member, security, filing)


def _parse_int(value: object | None) -> int | None:
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip().isdigit():
        return int(value.strip())
    return None


def _extract_transaction_id(payload: dict) -> int | None:
    direct = _parse_int(payload.get("transaction_id"))
    if direct is not None:
        return direct
    alt = _parse_int(payload.get("transactionId"))
    if alt is not None:
        return alt
    if isinstance(payload.get("transaction"), dict):
        return _parse_int(payload["transaction"].get("id"))
    return None


def _resolve_from_payload(db: Session, payload: dict) -> dict[str, object | None] | None:
    tx_id = _extract_transaction_id(payload)
    if tx_id is not None:
        resolved = _resolve_from_transaction(db, tx_id)
        if resolved:
            return resolved

    member_id = _parse_int(payload.get("member_id"))
    filing_id = _parse_int(payload.get("filing_id"))
    security_id = _parse_int(payload.get("security_id"))
    trade_date = _parse_iso_date(payload.get("trade_date"))
    report_date = _parse_iso_date(payload.get("report_date"))

    if member_id is None and filing_id is None and security_id is None:
        return None

    q = (
        select(Transaction, Member, Security, Filing)
        .join(Member, Transaction.member_id == Member.id)
        .outerjoin(Security, Transaction.security_id == Security.id)
        .join(Filing, Transaction.filing_id == Filing.id)
    )
    if member_id is not None:
        q = q.where(Transaction.member_id == member_id)
    if filing_id is not None:
        q = q.where(Transaction.filing_id == filing_id)
    if security_id is not None:
        q = q.where(Transaction.security_id == security_id)
    if trade_date is not None:
        q = q.where(Transaction.trade_date == trade_date)
    if report_date is not None:
        q = q.where(Transaction.report_date == report_date)

    row = db.execute(q.order_by(Transaction.id.desc()).limit(1)).mappings().first()
    if not row:
        return None
    return _resolve_from_transaction_row(
        row["Transaction"], row["Member"], row.get("Security"), row["Filing"]
    )


def _merge_value(current: object | None, incoming: object | None) -> object | None:
    if current is None or (isinstance(current, str) and not current.strip()):
        return incoming
    return current


def verify_event_filters(db: Session) -> None:
    empty_symbol = list_events(
        db=db,
        symbol="ZZZZZZ",
        limit=50,
        min_amount=None,
        max_amount=None,
        whale=None,
        recent_days=None,
    )
    if len(empty_symbol.items) != 0:
        raise RuntimeError("symbol=ZZZZZZ should return zero results")

    empty_amount = list_events(
        db=db,
        min_amount=999_999_999,
        max_amount=None,
        limit=50,
        whale=None,
        recent_days=None,
    )
    if len(empty_amount.items) != 0:
        raise RuntimeError("min_amount=999999999 should return zero results")

    recent_page = list_events(
        db=db,
        recent_days=1,
        limit=50,
        min_amount=None,
        max_amount=None,
        whale=None,
    )
    wide_page = list_events(
        db=db,
        recent_days=30,
        limit=50,
        min_amount=None,
        max_amount=None,
        whale=None,
    )
    if len(recent_page.items) > len(wide_page.items):
        raise RuntimeError("recent_days=1 should return <= recent_days=30")

    logger.info("Event filter checks passed.")


def repair_events(db: Session, limit: int | None = None, dry_run: bool = False) -> int:
    missing_clause = or_(
        Event.symbol.is_(None),
        Event.member_name.is_(None),
        Event.member_bioguide_id.is_(None),
        Event.chamber.is_(None),
        Event.party.is_(None),
        Event.trade_type.is_(None),
        Event.amount_min.is_(None),
        Event.amount_max.is_(None),
        Event.transaction_type.is_(None),
        Event.event_date.is_(None),
    )

    q = select(Event).where(Event.event_type == "congress_trade").where(missing_clause)
    if limit:
        q = q.limit(limit)

    scanned = updated = skipped = missing_source = 0
    for event in db.execute(q).scalars():
        scanned += 1
        try:
            payload = json.loads(event.payload_json or "{}")
            if not isinstance(payload, dict):
                payload = {}
        except Exception:
            payload = {}

        tx_data = _resolve_from_payload(db, payload)

        payload_data = _parse_payload_fields(payload)
        resolved = tx_data or {}
        has_payload_data = any(
            value not in (None, "")
            for value in payload_data.values()
            if not isinstance(value, date)
        ) or any(
            value is not None
            for key, value in payload_data.items()
            if key in {"trade_date", "report_date"}
        )
        if not resolved and not has_payload_data:
            missing_source += 1

        candidate_symbol = _merge_value(resolved.get("symbol"), payload_data.get("symbol"))
        if candidate_symbol:
            candidate_symbol = str(candidate_symbol).upper()

        candidate_member_name = _merge_value(
            resolved.get("member_name"), payload_data.get("member_name")
        )
        candidate_member_id = _merge_value(
            resolved.get("member_bioguide_id"), payload_data.get("member_bioguide_id")
        )
        candidate_chamber = _merge_value(resolved.get("chamber"), payload_data.get("chamber"))
        candidate_party = _merge_value(resolved.get("party"), payload_data.get("party"))
        candidate_transaction_type = _merge_value(
            resolved.get("transaction_type"), payload_data.get("transaction_type")
        )
        trade_type_source = _merge_value(
            resolved.get("transaction_type"),
            payload_data.get("trade_type") or payload_data.get("transaction_type"),
        )
        candidate_trade_type = _normalize_trade_type(
            str(trade_type_source) if trade_type_source else None
        )
        candidate_amount_min = _merge_value(
            resolved.get("amount_min"), payload_data.get("amount_min")
        )
        candidate_amount_max = _merge_value(
            resolved.get("amount_max"), payload_data.get("amount_max")
        )

        trade_date = _merge_value(resolved.get("trade_date"), payload_data.get("trade_date"))
        report_date = _merge_value(resolved.get("report_date"), payload_data.get("report_date"))
        candidate_event_date = _to_event_datetime(trade_date or report_date)

        updated_fields = {}
        if candidate_symbol and event.symbol is None:
            updated_fields["symbol"] = candidate_symbol
        if candidate_member_name and event.member_name is None:
            updated_fields["member_name"] = candidate_member_name
        if candidate_member_id and event.member_bioguide_id is None:
            updated_fields["member_bioguide_id"] = candidate_member_id
        if candidate_chamber and event.chamber is None:
            updated_fields["chamber"] = candidate_chamber
        if candidate_party and event.party is None:
            updated_fields["party"] = candidate_party
        if candidate_transaction_type and event.transaction_type is None:
            updated_fields["transaction_type"] = candidate_transaction_type
        if candidate_trade_type and event.trade_type is None:
            updated_fields["trade_type"] = candidate_trade_type
        if candidate_amount_min is not None and event.amount_min is None:
            updated_fields["amount_min"] = candidate_amount_min
        if candidate_amount_max is not None and event.amount_max is None:
            updated_fields["amount_max"] = candidate_amount_max
        if candidate_event_date and event.event_date is None:
            updated_fields["event_date"] = candidate_event_date

        if not updated_fields:
            skipped += 1
            continue

        for key, value in updated_fields.items():
            setattr(event, key, value)

        updated += 1

    if updated and not dry_run:
        db.commit()
    elif dry_run:
        db.rollback()

    logger.info("Scanned: %s", scanned)
    logger.info("Updated: %s", updated)
    logger.info("Skipped: %s", skipped)
    logger.info("Missing source: %s", missing_source)
    return updated


def main():
    args = _parse_args()
    level_name = str(args.log_level).upper()
    logging.basicConfig(level=getattr(logging, level_name, logging.INFO))

    db = SessionLocal()

    try:
        if args.repair:
            repaired = repair_events(db, limit=args.limit, dry_run=args.dry_run)
            logger.info("Repair complete. Rows updated: %s", repaired)
            if not args.dry_run:
                verify_event_filters(db)
            return

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
