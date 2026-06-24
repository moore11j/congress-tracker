from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

from sqlalchemy import select

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.backfill_events_from_trades import (  # noqa: E402
    _congress_event_from_transaction,
    _congress_event_payload,
    _existing_congress_event_identities,
)
from app.db import SessionLocal  # noqa: E402
from app.models import Event, Filing, Member, Security, Transaction  # noqa: E402
from app.services.official_congress import normalize_congress_symbol  # noqa: E402


DOCUMENT_URL = "https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2026/8221322.pdf"
SCRIPT_VERSION = "ro_khanna_sandisk_2026_01_29_v1"


@dataclass(frozen=True)
class VerifiedCongressRow:
    document_id: str
    source_line_ref: str
    member_bioguide_id: str
    first_name: str
    last_name: str
    chamber: str
    party: str
    state: str
    filing_date: date
    document_url: str
    owner_type: str
    transaction_type: str
    trade_date: date
    amount_min: float
    amount_max: float
    raw_asset_name: str
    raw_symbol: str | None


VERIFIED_ROWS = (
    VerifiedCongressRow(
        document_id="8221322",
        source_line_ref="official House PTR 8221322.pdf page 31",
        member_bioguide_id="K000389",
        first_name="Ro",
        last_name="Khanna",
        chamber="house",
        party="Democrat",
        state="CA",
        filing_date=date(2026, 2, 6),
        document_url=DOCUMENT_URL,
        owner_type="spouse",
        transaction_type="purchase",
        trade_date=date(2026, 1, 29),
        amount_min=1001.0,
        amount_max=15000.0,
        raw_asset_name="SANDISK LLC CMN",
        raw_symbol=None,
    ),
)


def _find_or_create_member(db, row: VerifiedCongressRow) -> Member:
    member = db.execute(
        select(Member)
        .where(Member.bioguide_id == row.member_bioguide_id)
        .limit(1)
    ).scalar_one_or_none()
    if member is None:
        member = Member(
            bioguide_id=row.member_bioguide_id,
            first_name=row.first_name,
            last_name=row.last_name,
            chamber=row.chamber,
            party=row.party,
            state=row.state,
        )
        db.add(member)
        db.flush()
        return member

    member.first_name = member.first_name or row.first_name
    member.last_name = member.last_name or row.last_name
    member.chamber = member.chamber or row.chamber
    member.party = member.party or row.party
    member.state = member.state or row.state
    return member


def _find_or_create_security(db, row: VerifiedCongressRow) -> Security:
    symbol, status = normalize_congress_symbol(row.raw_symbol, row.raw_asset_name, db)
    if symbol != "SNDK" or status != "resolved":
        raise RuntimeError(
            f"Refusing to backfill {row.raw_asset_name!r}; resolver returned symbol={symbol!r} status={status!r}"
        )
    security = db.execute(select(Security).where(Security.symbol == symbol)).scalar_one_or_none()
    if security is None:
        security = Security(symbol=symbol, name=row.raw_asset_name, asset_class="Stock", sector=None)
        db.add(security)
        db.flush()
    else:
        security.name = security.name or row.raw_asset_name
        security.asset_class = security.asset_class or "Stock"
    return security


def _find_or_create_filing(db, row: VerifiedCongressRow, member: Member) -> Filing:
    filing = db.execute(
        select(Filing)
        .where(
            (Filing.document_url == row.document_url)
            | (Filing.document_hash.in_([f"fmp:house:{row.document_id}", f"official_house:ptr:{row.document_id}"]))
        )
        .order_by(Filing.id.asc())
        .limit(1)
    ).scalar_one_or_none()
    if filing is None:
        filing = Filing(
            member_id=member.id,
            source="house_fmp",
            filing_date=row.filing_date,
            document_url=row.document_url,
            document_hash=f"fmp:house:{row.document_id}",
        )
        db.add(filing)
        db.flush()
    else:
        filing.member_id = filing.member_id or member.id
        filing.filing_date = filing.filing_date or row.filing_date
        filing.document_url = filing.document_url or row.document_url
        filing.document_hash = filing.document_hash or f"fmp:house:{row.document_id}"
    return filing


def _find_matching_transaction(db, row: VerifiedCongressRow, member: Member, security: Security) -> Transaction | None:
    return db.execute(
        select(Transaction)
        .join(Filing, Filing.id == Transaction.filing_id)
        .where(Transaction.member_id == member.id)
        .where(Transaction.security_id == security.id)
        .where(Transaction.owner_type == row.owner_type)
        .where(Transaction.transaction_type == row.transaction_type)
        .where(Transaction.trade_date == row.trade_date)
        .where(Transaction.amount_range_min == row.amount_min)
        .where(Transaction.amount_range_max == row.amount_max)
        .where(Filing.document_url == row.document_url)
        .limit(1)
    ).scalar_one_or_none()


def _upsert_transaction(db, row: VerifiedCongressRow, filing: Filing, member: Member, security: Security) -> tuple[Transaction, bool]:
    existing = _find_matching_transaction(db, row, member, security)
    if existing is not None:
        return existing, False
    tx = Transaction(
        filing_id=filing.id,
        member_id=member.id,
        security_id=security.id,
        owner_type=row.owner_type,
        transaction_type=row.transaction_type,
        trade_date=row.trade_date,
        report_date=row.filing_date,
        amount_range_min=row.amount_min,
        amount_range_max=row.amount_max,
        description=row.raw_asset_name,
    )
    db.add(tx)
    db.flush()
    return tx, True


def _event_exists(db, tx: Transaction, filing: Filing, member: Member, security: Security) -> bool:
    payload = _congress_event_payload(tx, filing, member, security)
    if payload is None:
        return False
    external_ids, transaction_ids, backfill_ids = _existing_congress_event_identities(db)
    return (
        str(payload["external_id"]) in external_ids
        or tx.id in transaction_ids
        or str(payload["backfill_id"]) in backfill_ids
    )


def _insert_event(db, tx: Transaction, filing: Filing, member: Member, security: Security, row: VerifiedCongressRow) -> Event:
    event = _congress_event_from_transaction(tx, filing, member, security)
    payload = json.loads(event.payload_json)
    payload["source_line_ref"] = row.source_line_ref
    payload["parser_version"] = SCRIPT_VERSION
    event.payload_json = json.dumps(payload, sort_keys=True)
    event.data_source = "congress"
    event.source_provider = filing.source
    event.source_filing_id = f"{row.document_id}:{row.trade_date.isoformat()}:{security.symbol}:{row.owner_type}"
    event.source_document_url = row.document_url
    event.parser_version = SCRIPT_VERSION
    event.provider_priority = 10
    db.add(event)
    db.flush()
    return event


def run(*, apply: bool) -> dict[str, Any]:
    db = SessionLocal()
    try:
        results = []
        for row in VERIFIED_ROWS:
            member = _find_or_create_member(db, row)
            security = _find_or_create_security(db, row)
            filing = _find_or_create_filing(db, row, member)
            tx, transaction_inserted = _upsert_transaction(db, row, filing, member, security)
            event_exists = _event_exists(db, tx, filing, member, security)
            event_inserted = False
            if not event_exists:
                _insert_event(db, tx, filing, member, security, row)
                event_inserted = True
            results.append(
                {
                    "document_id": row.document_id,
                    "source_line_ref": row.source_line_ref,
                    "member": f"{member.first_name or ''} {member.last_name or ''}".strip(),
                    "member_bioguide_id": member.bioguide_id,
                    "symbol": security.symbol,
                    "raw_asset_name": row.raw_asset_name,
                    "owner": row.owner_type,
                    "transaction_type": row.transaction_type,
                    "transaction_date": row.trade_date.isoformat(),
                    "filing_date": row.filing_date.isoformat(),
                    "amount_min": row.amount_min,
                    "amount_max": row.amount_max,
                    "transaction_inserted": transaction_inserted,
                    "event_inserted": event_inserted,
                    "event_existing": event_exists,
                }
            )
        if apply:
            db.commit()
        else:
            db.rollback()
        return {
            "apply": apply,
            "rows": results,
            "transactions_to_insert": sum(1 for item in results if item["transaction_inserted"]),
            "events_to_insert": sum(1 for item in results if item["event_inserted"]),
        }
    finally:
        db.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill verified Ro Khanna SanDisk House PTR row only.")
    parser.add_argument("--apply", action="store_true", help="Write the verified transaction and event. Defaults to dry-run.")
    args = parser.parse_args()
    print(json.dumps(run(apply=args.apply), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
