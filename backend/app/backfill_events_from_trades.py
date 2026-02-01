from __future__ import annotations

import argparse
import hashlib
import json
import logging
from datetime import date, datetime, time, timezone

from sqlalchemy import func, or_, select, text

from app.db import DATABASE_URL, SessionLocal, ensure_event_columns
from app.models import Event, Filing, Member, Security, Transaction

logger = logging.getLogger(__name__)


def _format_amount(value: float | None) -> str | None:
    if value is None:
        return None
    if value >= 1_000_000:
        amount = value / 1_000_000
        formatted = f"{amount:.1f}M" if amount % 1 else f"{int(amount)}M"
    elif value >= 1_000:
        amount = value / 1_000
        formatted = f"{amount:.1f}k" if amount % 1 else f"{int(amount)}k"
    else:
        formatted = f"{int(value)}"
    return f"${formatted}"


def _format_amount_range(min_value: float | None, max_value: float | None) -> str:
    min_label = _format_amount(min_value)
    max_label = _format_amount(max_value)
    if min_label and max_label:
        return f"{min_label}–{max_label}"
    if min_label:
        return f"{min_label}+"
    if max_label:
        return max_label
    return "Unknown amount"


def _title_source(value: str | None) -> str:
    if not value:
        return "Unknown"
    cleaned = value.strip()
    if not cleaned:
        return "Unknown"
    if "_" not in cleaned:
        return cleaned.capitalize()
    parts = [part for part in cleaned.split("_") if part]
    if len(parts) == 1:
        return parts[0].capitalize()
    head = parts[0].capitalize()
    tail = "_".join(part.lower() for part in parts[1:])
    return f"{head}_{tail}"


def _event_ts(trade_date, report_date) -> datetime:
    use_date = trade_date or report_date
    if use_date:
        return datetime.combine(use_date, time.min, tzinfo=timezone.utc)
    return datetime.now(timezone.utc)


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        try:
            return datetime.fromisoformat(value).date()
        except ValueError:
            return None


def _build_backfill_id(
    *,
    source: str | None,
    filing_id: int | None,
    transaction_id: int | None,
    symbol: str | None,
    trade_date: str | None,
    transaction_type: str | None,
    amount_range_min: float | None,
    amount_range_max: float | None,
) -> str:
    key_fields = {
        "source": source,
        "filing_id": filing_id,
        "transaction_id": transaction_id,
        "symbol": symbol,
        "trade_date": trade_date,
        "transaction_type": transaction_type,
        "amount_range_min": amount_range_min,
        "amount_range_max": amount_range_max,
    }
    normalized = json.dumps(key_fields, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _normalize_party(value: str | None) -> str:
    if not value:
        return "unknown"
    normalized = value.strip().lower()
    if normalized in {"democrat", "republican", "independent", "other", "unknown"}:
        return normalized
    return "other"


def _normalize_chamber(value: str | None) -> str | None:
    if not value:
        return None
    normalized = value.strip().lower()
    if normalized in {"house", "senate"}:
        return normalized
    return None


def _normalize_transaction_type(value: str | None) -> str:
    if not value:
        return "other"
    normalized = value.strip().lower()
    if normalized in {"purchase", "sale", "exchange", "received", "other"}:
        return normalized
    return "other"


def _normalize_amount(value: float | int | str | None) -> int | None:
    if value is None:
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _supports_json_extract(db) -> bool:
    try:
        db.execute(text("SELECT json_extract('{\"a\":1}', '$.a')"))
    except Exception:
        return False
    return True


def _load_existing_transaction_ids(db) -> set[int]:
    rows = db.execute(
        text(
            "SELECT json_extract(payload_json, '$.transaction_id') "
            "FROM events WHERE event_type = :event_type"
        ),
        {"event_type": "congress_trade"},
    ).fetchall()
    existing_ids: set[int] = set()
    for (value,) in rows:
        if value is None:
            continue
        try:
            existing_ids.add(int(value))
        except (TypeError, ValueError):
            continue
    return existing_ids


def _load_existing_backfill_ids(db) -> set[str]:
    existing_ids: set[str] = set()
    existing_rows = db.execute(
        select(Event.payload_json).where(Event.event_type == "congress_trade")
    ).all()
    for (payload_json,) in existing_rows:
        try:
            payload = json.loads(payload_json)
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        backfill_id = payload.get("backfill_id")
        if not backfill_id:
            backfill_id = _build_backfill_id(
                source=payload.get("source") or payload.get("filing_source"),
                filing_id=payload.get("filing_id"),
                transaction_id=payload.get("transaction_id"),
                symbol=payload.get("symbol"),
                trade_date=payload.get("trade_date"),
                transaction_type=payload.get("transaction_type"),
                amount_range_min=payload.get("amount_range_min"),
                amount_range_max=payload.get("amount_range_max"),
            )
        existing_ids.add(backfill_id)
    return existing_ids


def _repair_events(db) -> None:
    logger.info("Repairing congress_trade events with missing filter columns...")
    q = select(Event).where(
        Event.event_type == "congress_trade",
        or_(
            Event.member_name.is_(None),
            Event.member_bioguide_id.is_(None),
            Event.chamber.is_(None),
            Event.party.is_(None),
            Event.trade_type.is_(None),
            Event.amount_min.is_(None),
            Event.amount_max.is_(None),
            Event.event_date.is_(None),
            Event.symbol.is_(None),
        ),
    )
    events = db.execute(q).scalars().all()
    scanned = 0
    updated = 0
    skipped = 0
    missing_source = 0
    for event in events:
        scanned += 1
        try:
            payload = json.loads(event.payload_json)
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        transaction_id = payload.get("transaction_id")
        tx = None
        member = None
        security = None
        filing = None
        if transaction_id is not None:
            try:
                tx_id = int(transaction_id)
            except (TypeError, ValueError):
                tx_id = None
            if tx_id is not None:
                row = db.execute(
                    select(Transaction, Member, Security, Filing)
                    .join(Member, Transaction.member_id == Member.id)
                    .outerjoin(Security, Transaction.security_id == Security.id)
                    .join(Filing, Transaction.filing_id == Filing.id)
                    .where(Transaction.id == tx_id)
                ).first()
                if row:
                    tx, member, security, filing = row
                else:
                    missing_source += 1
        else:
            missing_source += 1

        payload_member = payload.get("member") or {}
        member_name = None
        bioguide_id = None
        chamber = None
        party = None
        if member:
            member_name = f"{member.first_name or ''} {member.last_name or ''}".strip() or None
            bioguide_id = member.bioguide_id
            chamber = _normalize_chamber(member.chamber)
            party = _normalize_party(member.party)
        else:
            member_name = payload_member.get("name") or None
            bioguide_id = payload_member.get("bioguide_id") or None
            chamber = _normalize_chamber(payload_member.get("chamber"))
            party = _normalize_party(payload_member.get("party"))

        transaction_type = _normalize_transaction_type(
            (tx.transaction_type if tx else None) or payload.get("transaction_type")
        )
        amount_min = _normalize_amount(
            (tx.amount_range_min if tx else None) or payload.get("amount_range_min")
        )
        amount_max = _normalize_amount(
            (tx.amount_range_max if tx else None) or payload.get("amount_range_max")
        )
        symbol = (
            (security.symbol if security and security.symbol else None)
            or payload.get("symbol")
            or event.ticker
        )
        symbol = symbol.strip().upper() if symbol else None
        trade_date = tx.trade_date if tx else _parse_date(payload.get("trade_date"))
        report_date = tx.report_date if tx else _parse_date(payload.get("report_date"))
        event_date = _event_ts(trade_date, report_date) if trade_date or report_date else event.ts

        updated_fields = False
        if event.member_name is None and member_name:
            event.member_name = member_name
            updated_fields = True
        if event.member_bioguide_id is None and bioguide_id:
            event.member_bioguide_id = bioguide_id
            updated_fields = True
        if event.chamber is None and chamber:
            event.chamber = chamber
            updated_fields = True
        if event.party is None and party:
            event.party = party
            updated_fields = True
        if event.trade_type is None and transaction_type:
            event.trade_type = transaction_type
            if event.transaction_type is None:
                event.transaction_type = transaction_type
            updated_fields = True
        if event.amount_min is None and amount_min is not None:
            event.amount_min = amount_min
            updated_fields = True
        if event.amount_max is None and amount_max is not None:
            event.amount_max = amount_max
            updated_fields = True
        if event.symbol is None and symbol:
            event.symbol = symbol
            updated_fields = True
        if event.event_date is None and event_date:
            event.event_date = event_date
            updated_fields = True
        if updated_fields:
            updated += 1
        else:
            skipped += 1

    if updated:
        db.commit()
    logger.info("Repair scan complete.")
    logger.info("Scanned: %s", scanned)
    logger.info("Updated: %s", updated)
    logger.info("Skipped: %s", skipped)
    logger.info("Missing source: %s", missing_source)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill congress trade events.")
    parser.add_argument("--dry-run", action="store_true", help="Do not write to the DB.")
    parser.add_argument("--limit", type=int, default=None, help="Limit transactions processed.")
    parser.add_argument(
        "--replace",
        action="store_true",
        help="Delete existing congress_trade events before inserting.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity.",
    )
    parser.add_argument(
        "--repair",
        action="store_true",
        help="Repair existing congress_trade events with missing filter columns.",
    )
    return parser.parse_args()


def run_backfill(
    *,
    dry_run: bool,
    limit: int | None,
    replace: bool,
    repair: bool,
) -> tuple[int, int, int]:
    db = SessionLocal()
    try:
        logger.info("Backfill starting.")
        logger.info("Database URL: %s", DATABASE_URL)
        if DATABASE_URL.startswith("sqlite"):
            sqlite_path = DATABASE_URL.replace("sqlite:///", "", 1)
            logger.info("Database file: /%s", sqlite_path.lstrip("/"))

        legacy_trade_count = db.execute(select(func.count()).select_from(Transaction)).scalar_one()
        events_count = db.execute(select(func.count()).select_from(Event)).scalar_one()
        logger.info("Legacy trades: %s", legacy_trade_count)
        logger.info("Events: %s", events_count)

        ensure_event_columns()

        if repair:
            _repair_events(db)

        if legacy_trade_count == 0:
            logger.warning("No trades found; backfill skipped.")
            return 0, 0, 0

        if replace:
            existing_count = db.execute(
                select(func.count()).select_from(Event).where(Event.event_type == "congress_trade")
            ).scalar_one()
            if dry_run:
                logger.info("Dry run: would delete %s congress_trade events.", existing_count)
            else:
                db.query(Event).filter(Event.event_type == "congress_trade").delete(
                    synchronize_session=False
                )
                db.commit()
                logger.info("Deleted %s congress_trade events.", existing_count)

        use_json_extract = _supports_json_extract(db)
        if replace and dry_run:
            logger.info("Dry run: ignoring existing events because --replace was set.")
            existing_ids = set()
        elif use_json_extract:
            logger.debug("Using json_extract for deduplication by transaction_id.")
            existing_ids = _load_existing_transaction_ids(db)
        else:
            logger.warning(
                "json_extract unavailable; falling back to backfill_id dedupe key."
            )
            existing_ids = _load_existing_backfill_ids(db)

        q = (
            select(Transaction, Member, Security, Filing)
            .join(Member, Transaction.member_id == Member.id)
            .outerjoin(Security, Transaction.security_id == Security.id)
            .join(Filing, Transaction.filing_id == Filing.id)
            .order_by(Transaction.id.asc())
        )
        if limit is not None:
            q = q.limit(limit)

        scanned = 0
        inserted = 0
        skipped = 0

        for tx, member, security, filing in db.execute(q).all():
            scanned += 1
            symbol = security.symbol if security and security.symbol else None
            symbol_upper = symbol.strip().upper() if symbol else None
            ticker = symbol_upper or "UNKNOWN"
            source = filing.source or member.chamber
            member_name = f"{member.first_name or ''} {member.last_name or ''}".strip() or None
            trade_date = tx.trade_date.isoformat() if tx.trade_date else None
            report_date = tx.report_date.isoformat() if tx.report_date else None

            payload = {
                "transaction_id": tx.id,
                "filing_id": tx.filing_id,
                "member_id": tx.member_id,
                "security_id": tx.security_id,
                "owner_type": tx.owner_type,
                "transaction_type": tx.transaction_type,
                "trade_date": trade_date,
                "report_date": report_date,
                "amount_range_min": tx.amount_range_min,
                "amount_range_max": tx.amount_range_max,
                "description": tx.description,
                "symbol": symbol,
                "security_name": security.name if security else None,
                "asset_class": security.asset_class if security else None,
                "sector": security.sector if security else None,
                "member": {
                    "bioguide_id": member.bioguide_id,
                    "name": member_name,
                    "chamber": member.chamber,
                    "party": member.party,
                    "state": member.state,
                },
                "source": source,
                "filing_source": filing.source,
                "filing_date": filing.filing_date.isoformat() if filing.filing_date else None,
                "document_url": filing.document_url,
            }

            backfill_id = _build_backfill_id(
                source=source,
                filing_id=tx.filing_id,
                transaction_id=tx.id,
                symbol=symbol,
                trade_date=trade_date,
                transaction_type=tx.transaction_type,
                amount_range_min=tx.amount_range_min,
                amount_range_max=tx.amount_range_max,
            )
            payload["backfill_id"] = backfill_id

            dedupe_key = tx.id if use_json_extract else backfill_id
            if dedupe_key in existing_ids:
                skipped += 1
                continue

            if use_json_extract:
                existing_ids.add(tx.id)
            else:
                existing_ids.add(backfill_id)

            amount_text = _format_amount_range(tx.amount_range_min, tx.amount_range_max)
            transaction_label = (tx.transaction_type or "unknown").upper()
            headline = (
                f"{_title_source(source)} trade: "
                f"{transaction_label} {ticker} {amount_text}"
            )
            event_date = _event_ts(tx.trade_date, tx.report_date)

            event = Event(
                event_type="congress_trade",
                ts=event_date,
                event_date=event_date,
                ticker=ticker,
                symbol=symbol_upper,
                source=source or "unknown",
                headline=headline,
                summary=None,
                url=filing.document_url,
                payload_json=json.dumps(payload, sort_keys=True),
                member_name=member_name,
                member_bioguide_id=member.bioguide_id,
                chamber=_normalize_chamber(member.chamber),
                party=_normalize_party(member.party),
                transaction_type=_normalize_transaction_type(tx.transaction_type),
                trade_type=_normalize_transaction_type(tx.transaction_type),
                amount_min=_normalize_amount(tx.amount_range_min),
                amount_max=_normalize_amount(tx.amount_range_max),
            )

            if not dry_run:
                db.add(event)
            inserted += 1

        if dry_run:
            logger.info("Dry run: would insert %s events.", inserted)
        else:
            if replace or inserted:
                db.commit()
            logger.info("Inserted %s events.", inserted)

        logger.info("Scanned: %s", scanned)
        logger.info("Skipped: %s", skipped)
        return scanned, inserted, skipped
    finally:
        db.close()


def main() -> None:
    args = _parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level))
    run_backfill(
        dry_run=args.dry_run,
        limit=args.limit,
        replace=args.replace,
        repair=args.repair,
    )


if __name__ == "__main__":
    main()
