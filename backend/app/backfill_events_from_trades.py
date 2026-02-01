from __future__ import annotations

import hashlib
import json
import sys
from datetime import datetime, time, timezone

from sqlalchemy import func, or_, select

from app.db import DATABASE_URL, SessionLocal, ensure_event_columns
from app.models import Event, Filing, Member, Security, Transaction


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
    return cleaned.capitalize()


def _event_ts(trade_date, report_date) -> datetime:
    use_date = trade_date or report_date
    if use_date:
        return datetime.combine(use_date, time.min, tzinfo=timezone.utc)
    return datetime.now(timezone.utc)


def _build_backfill_id(payload: dict) -> str:
    key_fields = {
        "symbol": payload.get("symbol"),
        "member_bioguide_id": payload.get("member", {}).get("bioguide_id"),
        "member_name": payload.get("member", {}).get("name"),
        "transaction_type": payload.get("transaction_type"),
        "owner_type": payload.get("owner_type"),
        "amount_range_min": payload.get("amount_range_min"),
        "amount_range_max": payload.get("amount_range_max"),
        "trade_date": payload.get("trade_date"),
        "report_date": payload.get("report_date"),
        "source": payload.get("source"),
    }
    normalized = json.dumps(key_fields, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _normalize_party(value: str | None) -> str:
    if not value:
        return "unknown"
    normalized = value.strip().lower()
    if normalized in {"democrat", "republican", "other", "unknown"}:
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


def _repair_events(db) -> None:
    print("Repairing congress_trade events with missing filter columns...")
    q = select(Event).where(
        Event.event_type == "congress_trade",
        or_(
            Event.member_name.is_(None),
            Event.member_bioguide_id.is_(None),
            Event.chamber.is_(None),
            Event.party.is_(None),
            Event.transaction_type.is_(None),
            Event.amount_min.is_(None),
            Event.amount_max.is_(None),
        ),
    )
    events = db.execute(q).scalars().all()
    updated = 0
    for event in events:
        try:
            payload = json.loads(event.payload_json)
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        member = payload.get("member") or {}
        member_name = member.get("name") or None
        bioguide_id = member.get("bioguide_id") or None
        chamber = _normalize_chamber(member.get("chamber"))
        party = _normalize_party(member.get("party"))
        transaction_type = _normalize_transaction_type(payload.get("transaction_type"))
        amount_min = payload.get("amount_range_min")
        amount_max = payload.get("amount_range_max")

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
        if event.transaction_type is None and transaction_type:
            event.transaction_type = transaction_type
            updated_fields = True
        if event.amount_min is None and amount_min is not None:
            event.amount_min = amount_min
            updated_fields = True
        if event.amount_max is None and amount_max is not None:
            event.amount_max = amount_max
            updated_fields = True
        if updated_fields:
            updated += 1

    if updated:
        db.commit()
    print(f"Repaired events: {updated}")


def main() -> None:
    db = SessionLocal()
    try:
        print("Backfill starting....")
        print(f"Database URL: {DATABASE_URL}")
        if DATABASE_URL.startswith("sqlite"):
            sqlite_path = DATABASE_URL.replace("sqlite:///", "", 1)
            print(f"Database file: /{sqlite_path.lstrip('/')}")

        legacy_trade_count = db.execute(select(func.count()).select_from(Transaction)).scalar_one()
        events_count = db.execute(select(func.count()).select_from(Event)).scalar_one()
        print(f"Legacy trades: {legacy_trade_count}")
        print(f"Events: {events_count}")

        ensure_event_columns()

        run_backfill = True
        run_repair = False
        if "--repair" in sys.argv:
            run_repair = True
            run_backfill = "--backfill" in sys.argv

        if run_repair:
            _repair_events(db)
            if not run_backfill:
                return

        if legacy_trade_count == 0:
            print("No legacy trades found; backfill cannot run")
            sys.exit(1)

        existing_ids: set[str] = set()
        existing_rows = db.execute(
            select(Event.payload_json).where(Event.event_type == "congress_trade")
        ).all()
        for (payload_json,) in existing_rows:
            try:
                payload = json.loads(payload_json)
            except Exception:
                continue
            if isinstance(payload, dict):
                backfill_id = payload.get("backfill_id")
                if backfill_id:
                    existing_ids.add(backfill_id)

        q = (
            select(Transaction, Member, Security, Filing)
            .join(Member, Transaction.member_id == Member.id)
            .outerjoin(Security, Transaction.security_id == Security.id)
            .join(Filing, Transaction.filing_id == Filing.id)
            .order_by(Transaction.id.asc())
        )

        scanned = 0
        inserted = 0
        skipped = 0

        for tx, member, security, filing in db.execute(q).all():
            scanned += 1
            symbol = security.symbol if security and security.symbol else None
            ticker = (symbol or "UNKNOWN").upper()
            source = filing.source or member.chamber
            member_name = f"{member.first_name or ''} {member.last_name or ''}".strip() or None
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

            backfill_id = _build_backfill_id(payload)
            if backfill_id in existing_ids:
                skipped += 1
                continue

            payload["backfill_id"] = backfill_id
            existing_ids.add(backfill_id)

            amount_text = _format_amount_range(tx.amount_range_min, tx.amount_range_max)
            headline = (
                f"{_title_source(source)} trade: "
                f"{tx.transaction_type.upper()} {ticker} {amount_text}"
            )

            event = Event(
                event_type="congress_trade",
                ts=_event_ts(tx.trade_date, tx.report_date),
                ticker=ticker,
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
                amount_min=tx.amount_range_min,
                amount_max=tx.amount_range_max,
            )
            db.add(event)
            inserted += 1

        if inserted:
            db.commit()

        print(f"Scanned: {scanned}")
        print(f"Inserted: {inserted}")
        print(f"Skipped: {skipped}")

        if inserted == 0:
            print("Inserted 0 events; check dedupe key logic or target DB")
            sys.exit(1)
    finally:
        db.close()


if __name__ == "__main__":
    main()
